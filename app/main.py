# app/main.py
# QoS basado en:
#   1. Node load5 con curva de degradación exponencial (señal primaria) — NO requiere limits.cpu
#   2. EMA de iterations/sec vs baseline                                (señal secundaria)
# Métricas exportadas: cpu, iter/s, temp, QoS, node_load_pct, señales individuales

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
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, MetricExportResult
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

CALIBRATION_SECONDS = int(os.getenv("CALIBRATION_SECONDS", "30"))

LOAD_THRESHOLD = float(os.getenv("QOS_LOAD_THRESHOLD", "70"))
QOS_FLOOR      = float(os.getenv("QOS_FLOOR", "5"))
WEIGHT_LOAD    = float(os.getenv("QOS_WEIGHT_LOAD",  "0.70"))
WEIGHT_ITERS   = float(os.getenv("QOS_WEIGHT_ITERS", "0.30"))
EMA_ALPHA      = float(os.getenv("EMA_ALPHA", "0.15"))

# ---------------------------------------------------------------------------
# Estado compartido — solo el proceso __main__ escribe estos valores.
# Los procesos worker (Pool) no llegan a este punto porque burn_cpu()
# se ejecuta en un proceso separado sin re-importar el módulo completo.
# ---------------------------------------------------------------------------
_current_cpu                     = 0.0
_total_worker_iterations_per_sec = 0.0
_ema_iterations_per_sec          = 0.0
_node_temperature_celsius        = 0.0
_node_load1                      = 0.0
_node_load5                      = 0.0
_node_load15                     = 0.0
_node_load_pct                   = 0.0
_app_qos                         = 100.0
_qos_signal_load                 = 100.0
_qos_signal_iters                = 100.0
_baseline_iters                  = None
_is_calibrating                  = True

# ---------------------------------------------------------------------------
# LoggingMetricExporter
# Definido a nivel de módulo (requerido para ser serializable por pickle),
# pero INSTANCIADO solo dentro de __main__ — después del fork de Pool —
# para que ningún proceso hijo tenga su propio MeterProvider activo.
# Esto elimina los [PUSH] duplicados con QoS=100/load=0 que venían del fork.
# ---------------------------------------------------------------------------
class LoggingMetricExporter(OTLPMetricExporter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._push_count = 0

    def export(self, metrics_data, **kwargs):
        self._push_count += 1
        try:
            result = super().export(metrics_data, **kwargs)
            status = "OK   " if result == MetricExportResult.SUCCESS else "FAIL "
            print(
                f"[PUSH] [{self._push_count:05d}] {status} | "
                f"endpoint={OTEL_ENDPOINT} | "
                f"QoS={_app_qos:.1f} load_pct={_node_load_pct:.1f}% "
                f"load_sig={_qos_signal_load:.0f} iter_sig={_qos_signal_iters:.0f}",
                flush=True
            )
            return result
        except Exception as e:
            print(
                f"[PUSH] [{self._push_count:05d}] ERROR | "
                f"endpoint={OTEL_ENDPOINT} | exception={e}",
                flush=True
            )
            return MetricExportResult.FAILURE

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

def cb_node_load_pct(o: CallbackOptions):
    yield Observation(_node_load_pct, {"service": SERVICE_NAME})

def cb_node_load1(o: CallbackOptions):
    yield Observation(_node_load1, {"service": SERVICE_NAME})

def cb_node_load5(o: CallbackOptions):
    yield Observation(_node_load5, {"service": SERVICE_NAME})

def cb_qos(o: CallbackOptions):
    yield Observation(_app_qos, {"service": SERVICE_NAME})

def cb_qos_load(o: CallbackOptions):
    yield Observation(_qos_signal_load, {"service": SERVICE_NAME})

def cb_qos_iters(o: CallbackOptions):
    yield Observation(_qos_signal_iters, {"service": SERVICE_NAME})

def cb_calibrating(o: CallbackOptions):
    yield Observation(1.0 if _is_calibrating else 0.0, {"service": SERVICE_NAME})

def cb_baseline(o: CallbackOptions):
    yield Observation(_baseline_iters or 0.0, {"service": SERVICE_NAME})

# ---------------------------------------------------------------------------
# QoS: curva de degradación exponencial
# load_pct <= LOAD_THRESHOLD  →  QoS = 100 (zona sana)
# load_pct == 100%            →  QoS = QOS_FLOOR (zona colapsada)
# ---------------------------------------------------------------------------
def _compute_load_qos_signal(load_pct: float) -> float:
    if load_pct <= LOAD_THRESHOLD:
        return 100.0
    t = min(1.0, max(0.0, (load_pct - LOAD_THRESHOLD) / (100.0 - LOAD_THRESHOLD)))
    degraded = math.pow(1.0 - t, 2.5)
    return max(QOS_FLOOR, QOS_FLOOR + degraded * (100.0 - QOS_FLOOR))

# ---------------------------------------------------------------------------
# Thread: node load via /proc/loadavg (no namespaciado — carga real del nodo)
# ---------------------------------------------------------------------------
def track_node_load():
    global _node_load1, _node_load5, _node_load15, _node_load_pct, _qos_signal_load
    NUM_CPUS = multiprocessing.cpu_count()
    print(f"[INFO]  [load-tracker] Iniciando. num_cpus={NUM_CPUS} "
          f"threshold={LOAD_THRESHOLD}% floor={QOS_FLOOR}", flush=True)
    while True:
        time.sleep(2.0)
        try:
            load1, load5, load15 = os.getloadavg()
            _node_load1      = load1
            _node_load5      = load5
            _node_load15     = load15
            _node_load_pct   = min(100.0, (load5 / NUM_CPUS) * 100.0)
            _qos_signal_load = _compute_load_qos_signal(_node_load_pct)
        except Exception as e:
            print(f"[ERROR] [load-tracker] {e}", flush=True)

# ---------------------------------------------------------------------------
# Thread: CPU del proceso (main + workers)
# ---------------------------------------------------------------------------
def track_cpu():
    global _current_cpu
    main_process = psutil.Process()
    main_process.cpu_percent()
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
            print(f"[ERROR] [cpu-tracker] {e}", flush=True)
            _current_cpu = 0.0

# ---------------------------------------------------------------------------
# Thread: temperatura
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
            print(f"[ERROR] [temp-tracker] {e}", flush=True)
            _node_temperature_celsius = 0.0
        time.sleep(2)

# ---------------------------------------------------------------------------
# QoS compuesto
# ---------------------------------------------------------------------------
def _recalculate_qos():
    global _qos_signal_iters, _app_qos
    if _is_calibrating or _baseline_iters is None or _baseline_iters == 0:
        _qos_signal_iters = 100.0
    else:
        ratio = _ema_iterations_per_sec / _baseline_iters
        _qos_signal_iters = max(0.0, min(100.0, ratio * 100.0))
    _app_qos = max(0.0, min(100.0,
        WEIGHT_LOAD  * _qos_signal_load +
        WEIGHT_ITERS * _qos_signal_iters
    ))

# ---------------------------------------------------------------------------
# Worker — proceso hijo, no toca OTEL ni globals del padre
# ---------------------------------------------------------------------------
def burn_cpu(shared_iterations_counter):
    while True:
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
        shared_iterations_counter.value += 1

# ---------------------------------------------------------------------------
# Main
# OTEL se inicializa AQUÍ, después del fork de multiprocessing.Pool,
# garantizando que solo existe UN MeterProvider en UN proceso.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if NUM_WORKERS > multiprocessing.cpu_count():
        NUM_WORKERS = multiprocessing.cpu_count()

    print(f"[INFO]  [startup] service={SERVICE_NAME}, workers={NUM_WORKERS}, workload_size={WORKLOAD_SIZE}", flush=True)
    print(f"[INFO]  [startup] calibration={CALIBRATION_SECONDS}s", flush=True)
    print(f"[INFO]  [startup] QoS config: threshold={LOAD_THRESHOLD}% floor={QOS_FLOOR} "
          f"weights: load={WEIGHT_LOAD} iters={WEIGHT_ITERS}", flush=True)

    # 1) Fork workers PRIMERO — proceso más limpio, sin sockets OTEL abiertos
    print(f"[INFO]  [main] Iniciando {NUM_WORKERS} workers.", flush=True)
    manager = multiprocessing.Manager()
    shared_iterations_counter = manager.Value("L", 0)
    pool = multiprocessing.Pool(processes=NUM_WORKERS)
    pool.map_async(burn_cpu, [shared_iterations_counter] * NUM_WORKERS)

    # 2) OTEL setup post-fork — solo este proceso tiene el exporter
    print("[INFO]  [main] Inicializando OTEL (post-fork)...", flush=True)
    resource       = Resource(attributes={"service.name": SERVICE_NAME})
    reader         = PeriodicExportingMetricReader(
        LoggingMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"),
        export_interval_millis=2000,
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meter_provider)
    meter = metrics.get_meter("main-app.meter")

    meter.create_observable_gauge("main_app.cpu_percent",
        callbacks=[cb_cpu], description="CPU % del proceso main (psutil).")
    meter.create_observable_gauge("main_app.total_worker_iterations_per_sec",
        callbacks=[cb_iters], description="Iteraciones/s raw de los workers.")
    meter.create_observable_gauge("main_app.ema_iterations_per_sec",
        callbacks=[cb_iters_ema], description="Iteraciones/s suavizadas con EMA.")
    meter.create_observable_gauge("main_app.node_temperature_celsius",
        callbacks=[cb_temp], description="Temperatura del nodo en °C.")
    meter.create_observable_gauge("main_app.node_load_pct",
        callbacks=[cb_node_load_pct], description="Node load5 normalizado a % respecto a num CPUs.")
    meter.create_observable_gauge("main_app.node_load1",
        callbacks=[cb_node_load1], description="Node load average 1 minuto (raw).")
    meter.create_observable_gauge("main_app.node_load5",
        callbacks=[cb_node_load5], description="Node load average 5 minutos (raw).")
    meter.create_observable_gauge("main_app.qos",
        callbacks=[cb_qos], description="QoS compuesto 0-100.")
    meter.create_observable_gauge("main_app.qos_signal_load",
        callbacks=[cb_qos_load], description="Señal QoS individual: node load degradation (0-100).")
    meter.create_observable_gauge("main_app.qos_signal_iters",
        callbacks=[cb_qos_iters], description="Señal QoS individual: iterations/s vs baseline (0-100).")
    meter.create_observable_gauge("main_app.is_calibrating",
        callbacks=[cb_calibrating], description="1 mientras el pod está autocalibrandose, 0 después.")
    meter.create_observable_gauge("main_app.baseline_iters_per_sec",
        callbacks=[cb_baseline], description="Baseline de iter/s medido durante la calibración.")

    print("[INFO]  [main] Métricas registradas.", flush=True)

    # 3) Threads de sensores
    threading.Thread(target=track_cpu,         daemon=True).start()
    threading.Thread(target=track_temperature, daemon=True).start()
    threading.Thread(target=track_node_load,   daemon=True).start()

    last_total_iterations = 0
    last_time             = time.time()
    calibration_samples   = []

    print(f"[INFO]  [main] Autocalibración por {CALIBRATION_SECONDS}s — no lanzar carga todavía.", flush=True)

    while True:
        time.sleep(1.0)

        current_time     = time.time()
        elapsed_time     = current_time - last_time
        current_total    = shared_iterations_counter.value
        delta_iterations = current_total - last_total_iterations

        if elapsed_time > 0:
            _total_worker_iterations_per_sec = delta_iterations / elapsed_time
        else:
            _total_worker_iterations_per_sec = 0.0

        if _ema_iterations_per_sec == 0.0:
            _ema_iterations_per_sec = _total_worker_iterations_per_sec
        else:
            _ema_iterations_per_sec = (
                EMA_ALPHA * _total_worker_iterations_per_sec +
                (1 - EMA_ALPHA) * _ema_iterations_per_sec
            )

        if _is_calibrating:
            if _total_worker_iterations_per_sec > 0:
                calibration_samples.append(_total_worker_iterations_per_sec)
            if len(calibration_samples) >= CALIBRATION_SECONDS:
                sorted_samples  = sorted(calibration_samples)
                trim            = max(1, len(sorted_samples) // 10)
                trimmed         = sorted_samples[trim:]
                _baseline_iters = sum(trimmed) / len(trimmed)
                _is_calibrating = False
                print(f"[INFO]  [calibration] Baseline establecido: {_baseline_iters:.2f} iter/s "
                      f"(muestras={len(trimmed)})", flush=True)
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
            f"load1={_node_load1:.2f} load5={_node_load5:.2f} load_pct={_node_load_pct:.1f}% | "
            f"temp={_node_temperature_celsius:.1f}°C | "
            f"QoS={_app_qos:.1f} "
            f"[load_sig={_qos_signal_load:.0f} iter={_qos_signal_iters:.0f}]",
            flush=True
        )