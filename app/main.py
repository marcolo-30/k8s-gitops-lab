# app/main.py
# QoS basado en:
#   1. Throttling de cgroups (señal primaria)  — requiere limits.cpu en el deployment
#   2. EMA de iterations/sec vs baseline       — señal secundaria / respaldo
# Métricas exportadas: cpu, iter/s, temp, QoS, jitter, throttle_rate, señales individuales

import os
import time
import math
import threading
import psutil
import multiprocessing
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
SERVICE_NAME   = os.getenv("SERVICE_NAME", "main-app")
OTEL_ENDPOINT  = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT",
                            "http://otel-collector.observability.svc.cluster.local:4318")
WORKLOAD_SIZE  = int(os.getenv("WORKLOAD_SIZE", "200000"))
NUM_WORKERS    = int(os.getenv("NUM_WORKERS", str(multiprocessing.cpu_count())))
TEMP_FILE_PATH = "/sys/class/thermal/thermal_zone0/temp"

# Autocalibración de baseline de iterations/sec
CALIBRATION_SECONDS = int(os.getenv("CALIBRATION_SECONDS", "30"))

# Pesos QoS compuesto (deben sumar 1.0)
WEIGHT_THROTTLE = float(os.getenv("QOS_WEIGHT_THROTTLE", "0.70"))
WEIGHT_ITERS    = float(os.getenv("QOS_WEIGHT_ITERS",    "0.30"))

# EMA alpha para suavizar iterations/sec (0.1 suave, 0.3 reactivo)
EMA_ALPHA = float(os.getenv("EMA_ALPHA", "0.15"))

# Cgroups paths (v1 primero, v2 como fallback)
CGROUP_STAT_PATHS = [
    "/sys/fs/cgroup/cpu/cpu.stat",  # cgroups v1
    "/sys/fs/cgroup/cpu.stat",      # cgroups v2
]

print(f"[INFO]  [startup] service={SERVICE_NAME}, workers={NUM_WORKERS}, workload_size={WORKLOAD_SIZE}")
print(f"[INFO]  [startup] calibration={CALIBRATION_SECONDS}s | "
      f"weights: throttle={WEIGHT_THROTTLE} iters={WEIGHT_ITERS}")

# ---------------------------------------------------------------------------
# OTEL Setup
# ---------------------------------------------------------------------------
resource = Resource(attributes={"service.name": SERVICE_NAME})
reader   = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"),
    export_interval_millis=2000,
)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("main-app.meter")

# ---------------------------------------------------------------------------
# Estado compartido
# ---------------------------------------------------------------------------
_current_cpu                     = 0.0
_total_worker_iterations_per_sec = 0.0   # raw (calculado en loop principal)
_ema_iterations_per_sec          = 0.0   # suavizado
_node_temperature_celsius        = 0.0
_throttle_rate_pct               = 0.0   # % de tiempo throttled en la ventana
_app_qos                         = 100.0

# Señales individuales expuestas como métricas
_qos_signal_throttle             = 100.0
_qos_signal_iters                = 100.0

# Baseline autocalibrado
_baseline_iters                  = None
_is_calibrating                  = True

# ---------------------------------------------------------------------------
# Callbacks OTEL
# ---------------------------------------------------------------------------
def cb_cpu(o: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

def cb_iters(o: CallbackOptions):
    yield Observation(_total_worker_iterations_per_sec, {"service": SERVICE_NAME})

def cb_iters_ema(o: CallbackOptions):
    yield Observation(_ema_iterations_per_sec, {"service": SERVICE_NAME})

def cb_temp(o: CallbackOptions):
    yield Observation(_node_temperature_celsius, {"service": SERVICE_NAME})

def cb_throttle(o: CallbackOptions):
    yield Observation(_throttle_rate_pct, {"service": SERVICE_NAME})

def cb_qos(o: CallbackOptions):
    yield Observation(_app_qos, {"service": SERVICE_NAME})

def cb_qos_throttle(o: CallbackOptions):
    yield Observation(_qos_signal_throttle, {"service": SERVICE_NAME})

def cb_qos_iters(o: CallbackOptions):
    yield Observation(_qos_signal_iters, {"service": SERVICE_NAME})

def cb_calibrating(o: CallbackOptions):
    yield Observation(1.0 if _is_calibrating else 0.0, {"service": SERVICE_NAME})

def cb_baseline(o: CallbackOptions):
    yield Observation(_baseline_iters or 0.0, {"service": SERVICE_NAME})

# Registro de métricas
meter.create_observable_gauge("main_app.cpu_percent",
    callbacks=[cb_cpu], description="CPU % del proceso main (psutil).")
meter.create_observable_gauge("main_app.total_worker_iterations_per_sec",
    callbacks=[cb_iters], description="Iteraciones/s raw de los workers.")
meter.create_observable_gauge("main_app.ema_iterations_per_sec",
    callbacks=[cb_iters_ema], description="Iteraciones/s suavizadas con EMA.")
meter.create_observable_gauge("main_app.node_temperature_celsius",
    callbacks=[cb_temp], description="Temperatura del nodo en °C.")
meter.create_observable_gauge("main_app.throttle_rate_pct",
    callbacks=[cb_throttle], description="% del tiempo que el pod fue throttled (requiere limits.cpu).")
meter.create_observable_gauge("main_app.qos",
    callbacks=[cb_qos], description="QoS compuesto 0-100.")
meter.create_observable_gauge("main_app.qos_signal_throttle",
    callbacks=[cb_qos_throttle], description="Señal QoS individual: throttling (0-100).")
meter.create_observable_gauge("main_app.qos_signal_iters",
    callbacks=[cb_qos_iters], description="Señal QoS individual: iterations/s vs baseline (0-100).")
meter.create_observable_gauge("main_app.is_calibrating",
    callbacks=[cb_calibrating], description="1 mientras el pod está autocalibrandose, 0 después.")
meter.create_observable_gauge("main_app.baseline_iters_per_sec",
    callbacks=[cb_baseline], description="Baseline de iter/s medido durante la calibración.")

print("[INFO]  [startup] Métricas registradas.")

# ---------------------------------------------------------------------------
# Helper: leer throttling desde cgroups
# ---------------------------------------------------------------------------
def _read_cgroup_throttle():
    """
    Retorna (nr_throttled, throttled_time_ns) desde cpu.stat.
    Retorna (0, 0) si no encuentra el archivo o no hay limits.
    """
    for path in CGROUP_STAT_PATHS:
        try:
            with open(path, "r") as f:
                content = f.read()
            nr_throttled    = 0
            throttled_time  = 0
            for line in content.splitlines():
                if line.startswith("nr_throttled"):
                    nr_throttled = int(line.split()[1])
                elif line.startswith("throttled_time"):
                    throttled_time = int(line.split()[1])
            return nr_throttled, throttled_time
        except FileNotFoundError:
            continue
        except Exception as e:
            print(f"[WARN]  [throttle] Error leyendo {path}: {e}")
            continue
    return 0, 0

# ---------------------------------------------------------------------------
# Thread: tracking de throttling
# ---------------------------------------------------------------------------
def track_throttle():
    """
    Mide la tasa de throttling comparando deltas de throttled_time
    entre intervalos de 2 segundos. Resultado en porcentaje (0-100).
    """
    global _throttle_rate_pct, _qos_signal_throttle, _app_qos

    INTERVAL = 2.0
    _, prev_throttled_time = _read_cgroup_throttle()
    prev_ts = time.time()

    while True:
        time.sleep(INTERVAL)

        _, curr_throttled_time = _read_cgroup_throttle()
        curr_ts = time.time()

        elapsed_ns      = (curr_ts - prev_ts) * 1e9
        throttled_delta = curr_throttled_time - prev_throttled_time

        if elapsed_ns > 0:
            # Porcentaje del tiempo del intervalo que el pod estuvo throttled
            _throttle_rate_pct = min(100.0, (throttled_delta / elapsed_ns) * 100.0)
        else:
            _throttle_rate_pct = 0.0

        # Señal QoS: 100 cuando no hay throttling, 0 cuando está throttled el 100% del tiempo
        _qos_signal_throttle = max(0.0, 100.0 - _throttle_rate_pct)

        prev_throttled_time = curr_throttled_time
        prev_ts             = curr_ts

# ---------------------------------------------------------------------------
# Thread: tracking de CPU del proceso (main + workers)
# ---------------------------------------------------------------------------
def track_cpu():
    global _current_cpu

    main_process = psutil.Process()
    main_process.cpu_percent()  # primer llamado inicializa el baseline interno
    time.sleep(0.5)

    while True:
        try:
            all_procs = [main_process]
            try:
                all_procs += main_process.children(recursive=True)
            except psutil.NoSuchProcess:
                pass

            total_raw = 0.0
            for proc in all_procs:
                try:
                    total_raw += proc.cpu_percent(interval=1)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

            _current_cpu = max(0.0, min(100.0, total_raw / multiprocessing.cpu_count()))

        except Exception as e:
            print(f"[ERROR] [cpu-tracker] {e}")
            _current_cpu = 0.0

# ---------------------------------------------------------------------------
# Thread: tracking de temperatura
# ---------------------------------------------------------------------------
def track_temperature():
    global _node_temperature_celsius
    while True:
        try:
            with open(TEMP_FILE_PATH, "r") as f:
                _node_temperature_celsius = int(f.read().strip()) / 1000.0
        except FileNotFoundError:
            _node_temperature_celsius = 0.0
        except Exception as e:
            print(f"[ERROR] [temp-tracker] {e}")
            _node_temperature_celsius = 0.0
        time.sleep(2)

# ---------------------------------------------------------------------------
# Función QoS: recalcula el QoS compuesto
# ---------------------------------------------------------------------------
def _recalculate_qos():
    global _qos_signal_iters, _app_qos

    # Señal de iterations/sec vs baseline
    if _is_calibrating or _baseline_iters is None or _baseline_iters == 0:
        _qos_signal_iters = 100.0
    else:
        ratio = _ema_iterations_per_sec / _baseline_iters
        # ratio=1.0 → QoS_iters=100, ratio=0.0 → QoS_iters=0
        _qos_signal_iters = max(0.0, min(100.0, ratio * 100.0))

    # QoS compuesto ponderado
    _app_qos = (
        WEIGHT_THROTTLE * _qos_signal_throttle +
        WEIGHT_ITERS    * _qos_signal_iters
    )
    _app_qos = max(0.0, min(100.0, _app_qos))

# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------
def burn_cpu(shared_iterations_counter):
    while True:
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
        shared_iterations_counter.value += 1

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if NUM_WORKERS > multiprocessing.cpu_count():
        NUM_WORKERS = multiprocessing.cpu_count()

    print(f"[INFO]  [main] Iniciando {NUM_WORKERS} workers.")

    manager = multiprocessing.Manager()
    shared_iterations_counter = manager.Value("L", 0)

    pool = multiprocessing.Pool(processes=NUM_WORKERS)
    pool.map_async(burn_cpu, [shared_iterations_counter] * NUM_WORKERS)

    # Iniciar threads de sensores
    threading.Thread(target=track_cpu,         daemon=True).start()
    threading.Thread(target=track_temperature, daemon=True).start()
    threading.Thread(target=track_throttle,    daemon=True).start()

    last_total_iterations = 0
    last_time             = time.time()
    calibration_samples   = []

    print(f"[INFO]  [main] Autocalibración por {CALIBRATION_SECONDS}s — no lanzar carga todavía.")

    while True:
        loop_start = time.time()
        time.sleep(1.0)
        loop_elapsed = time.time() - loop_start

        current_time          = time.time()
        elapsed_time          = current_time - last_time
        current_total         = shared_iterations_counter.value
        delta_iterations      = current_total - last_total_iterations

        # Calcular raw iter/s
        if elapsed_time > 0:
            _total_worker_iterations_per_sec = delta_iterations / elapsed_time
        else:
            _total_worker_iterations_per_sec = 0.0

        # Actualizar EMA
        if _ema_iterations_per_sec == 0.0:
            _ema_iterations_per_sec = _total_worker_iterations_per_sec
        else:
            _ema_iterations_per_sec = (
                EMA_ALPHA * _total_worker_iterations_per_sec +
                (1 - EMA_ALPHA) * _ema_iterations_per_sec
            )

        # Autocalibración
        if _is_calibrating:
            if _total_worker_iterations_per_sec > 0:
                calibration_samples.append(_total_worker_iterations_per_sec)

            elapsed_since_start = current_time - (last_time - len(calibration_samples))
            if len(calibration_samples) >= CALIBRATION_SECONDS:
                # Descartar el 10% más bajo para ignorar el warmup inicial
                sorted_samples  = sorted(calibration_samples)
                trim            = max(1, len(sorted_samples) // 10)
                trimmed         = sorted_samples[trim:]
                _baseline_iters = sum(trimmed) / len(trimmed)
                _is_calibrating = False
                print(f"[INFO]  [calibration] Baseline establecido: {_baseline_iters:.2f} iter/s "
                      f"(muestras={len(trimmed)})")
        else:
            _recalculate_qos()

        last_total_iterations = current_total
        last_time             = current_time

        status = "CALIBRANDO" if _is_calibrating else "OK"
        print(
            f"[INFO] [main] [{status}] "
            f"cpu={_current_cpu:.1f}% | "
            f"iter/s={_total_worker_iterations_per_sec:.1f} | "
            f"ema={_ema_iterations_per_sec:.1f} | "
            f"baseline={f'{_baseline_iters:.1f}' if _baseline_iters is not None else 'N/A'} | "
            f"throttle={_throttle_rate_pct:.1f}% | "
            f"temp={_node_temperature_celsius:.1f}°C | "
            f"QoS={_app_qos:.1f} "
            f"[thr={_qos_signal_throttle:.0f} iter={_qos_signal_iters:.0f}]"
        )