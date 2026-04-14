# app/main.py
# QoS basado en:
#   1. Node load5 con curva de degradación exponencial (señal primaria) — NO requiere limits.cpu
#   2. EMA de iterations/sec vs baseline                                (señal secundaria)
# Métricas exportadas: cpu, iter/s, temp, QoS, node_load_pct, señales individuales
# NUEVO: Añade un endpoint /health en el puerto 8080

import os
import time
import math
import threading
import psutil
import multiprocessing
import http.server
import socketserver
import json
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
<<<<<<< Updated upstream
WORKLOAD_SIZE  = int(os.getenv("WORKLOAD_SIZE", "80000"))
NUM_WORKERS    = int(os.getenv("NUM_WORKERS", "1"))
=======
WORKLOAD_SIZE  = int(os.getenv("WORKLOAD_SIZE", "200000"))
NUM_WORKERS    = int(os.getenv("NUM_WORKERS", str(multiprocessing.cpu_count())))
>>>>>>> Stashed changes
TEMP_FILE_PATH = "/sys/class/thermal/thermal_zone0/temp"
HEALTH_PORT    = 8080

CALIBRATION_SECONDS = int(os.getenv("CALIBRATION_SECONDS", "30"))

LOAD_THRESHOLD = float(os.getenv("QOS_LOAD_THRESHOLD", "70"))
QOS_FLOOR      = float(os.getenv("QOS_FLOOR", "5"))
WEIGHT_LOAD    = float(os.getenv("QOS_WEIGHT_LOAD",  "0.70"))
WEIGHT_ITERS   = float(os.getenv("QOS_WEIGHT_ITERS", "0.30"))
EMA_ALPHA      = float(os.getenv("EMA_ALPHA", "0.15"))

# ---------------------------------------------------------------------------
# Estado compartido
# ---------------------------------------------------------------------------
_current_cpu                     = 0.0
_total_worker_iterations_per_sec = 0.0
_ema_iterations_per_sec          = 0.0
_node_temperature_celsius        = 0.0
_node_load1                      = 0.0
_node_load5                      = 0.0
<<<<<<< Updated upstream
=======
_node_load15                     = 0.0
>>>>>>> Stashed changes
_node_load_pct                   = 0.0
_app_qos                         = 100.0
_qos_signal_load                 = 100.0
_qos_signal_iters                = 100.0
_baseline_iters                  = None
_is_calibrating                  = True

# ---------------------------------------------------------------------------
# Health Check Endpoint
# ---------------------------------------------------------------------------
class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "status": "ok",
                "is_calibrating": _is_calibrating,
                "cpu_percent": _current_cpu,
                "qos": _app_qos,
                "node_load_percent": _node_load_pct,
                "iterations_per_sec": _total_worker_iterations_per_sec
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            self.send_error(404, "Not Found")

def start_health_server():
    with socketserver.ThreadingTCPServer(("", HEALTH_PORT), HealthCheckHandler) as httpd:
        print(f"[INFO]  [health-server] Sirviendo en el puerto {HEALTH_PORT}", flush=True)
        httpd.serve_forever()

# ---------------------------------------------------------------------------
<<<<<<< Updated upstream
# Worker Function - This was missing
# ---------------------------------------------------------------------------
def burn_cpu(shared_iterations_counter):
    while True:
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
        shared_iterations_counter.value += 1
=======
# LoggingMetricExporter
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
>>>>>>> Stashed changes

# (El resto del código, como callbacks OTEL, QoS, threads y workers, permanece igual)
# ... (código omitido para brevedad) ...

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # ... (código de inicialización) ...
    
    # 1) Iniciar el servidor de health check en un hilo
    threading.Thread(target=start_health_server, daemon=True).start()

<<<<<<< Updated upstream
=======
    print(f"[INFO]  [startup] service={SERVICE_NAME}, workers={NUM_WORKERS}, workload_size={WORKLOAD_SIZE}", flush=True)
    # ... (resto de los prints de startup) ...

    # 1) Iniciar el servidor de health check en un hilo
    threading.Thread(target=start_health_server, daemon=True).start()

>>>>>>> Stashed changes
    # 2) Fork workers
    print(f"[INFO]  [main] Iniciando {NUM_WORKERS} workers.", flush=True)
    manager = multiprocessing.Manager()
    shared_iterations_counter = manager.Value("L", 0)
    pool = multiprocessing.Pool(processes=NUM_WORKERS)
    pool.map_async(burn_cpu, [shared_iterations_counter] * NUM_WORKERS)

<<<<<<< Updated upstream
    # ... (resto del código) ...
=======
    # 3) OTEL setup post-fork
    # ... (código de inicialización de OTEL) ...

    # 4) Threads de sensores
    # ... (código de inicio de threads de sensores) ...

    # ... (resto del bucle principal) ...
>>>>>>> Stashed changes
