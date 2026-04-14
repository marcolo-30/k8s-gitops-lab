# app/main.py
# Goal: A multi-process app with a health check endpoint to measure responsiveness under load.

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

# --- Configuration ---
SERVICE_NAME = os.getenv("SERVICE_NAME", "main-app")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4318")
WORKLOAD_SIZE = int(os.getenv("WORKLOAD_SIZE", "200000"))
NUM_WORKERS = int(os.getenv("NUM_WORKERS", multiprocessing.cpu_count()))
HEALTH_PORT = 8080

print(f"[INFO]  [startup] service={SERVICE_NAME}, workers={NUM_WORKERS}, health_port={HEALTH_PORT}")

# --- OTEL Setup ---
inner_exporter = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
_original_export = inner_exporter.export

def _logged_export(metrics_data, timeout_millis=10000, **kwargs):
    try:
        result = _original_export(metrics_data, timeout_millis=timeout_millis, **kwargs)
        if result == MetricExportResult.SUCCESS:
            print(f"[INFO]  [otel-export] Push OK -> {OTEL_ENDPOINT}/v1/metrics")
        else:
            print(f"[ERROR] [otel-export] Push FAILED -> {OTEL_ENDPOINT}/v1/metrics")
        return result
    except Exception as e:
        print(f"[ERROR] [otel-export] Push EXCEPTION -> {e}")
        return MetricExportResult.FAILURE

inner_exporter.export = _logged_export

resource = Resource(attributes={"service.name": SERVICE_NAME})
reader = PeriodicExportingMetricReader(inner_exporter, export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("main-app.meter")

# --- Metrics State & Callbacks ---
_current_cpu = 0.0
_app_qos = 100.0

def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

def get_app_qos(options: CallbackOptions):
    yield Observation(_app_qos, {"service": SERVICE_NAME})

meter.create_observable_gauge("main_app.cpu_percent", callbacks=[get_cpu], description="Total CPU usage of the pod.")
meter.create_observable_gauge("main_app.qos", callbacks=[get_app_qos], description="Application Quality of Service (0-100).")

print("[INFO]  [startup] Metrics registered.")

# --- Health Check Endpoint ---
class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {"status": "ok", "cpu_percent": _current_cpu, "qos": _app_qos}
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            self.send_error(404, "Not Found")

def start_health_server():
    with socketserver.ThreadingTCPServer(("", HEALTH_PORT), HealthCheckHandler) as httpd:
        print(f"[INFO]  [health-server] Serving at port {HEALTH_PORT}")
        httpd.serve_forever()

# --- Sensor Thread ---
def track_cpu():
    global _current_cpu, _app_qos
    process = psutil.Process()
    QOS_CPU_HEALTHY_THRESHOLD = float(os.getenv("QOS_CPU_HEALTHY_THRESHOLD", "70.0"))
    QOS_CPU_CRITICAL_THRESHOLD = float(os.getenv("QOS_CPU_CRITICAL_THRESHOLD", "20.0"))
    
    while True:
        _current_cpu = process.cpu_percent(interval=1)
        if _current_cpu >= QOS_CPU_HEALTHY_THRESHOLD:
            _app_qos = 100.0
        elif _current_cpu <= QOS_CPU_CRITICAL_THRESHOLD:
            _app_qos = 0.0
        else:
            _app_qos = ((_current_cpu - QOS_CPU_CRITICAL_THRESHOLD) / 
                        (QOS_CPU_HEALTHY_THRESHOLD - QOS_CPU_CRITICAL_THRESHOLD)) * 100.0
        _app_qos = max(0.0, min(100.0, _app_qos))

# --- Worker Function ---
def burn_cpu(_):
    while True:
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))

# --- Main Execution Block ---
if __name__ == "__main__":
    if NUM_WORKERS > multiprocessing.cpu_count():
        NUM_WORKERS = multiprocessing.cpu_count()

    # Start Health Check Server in a separate thread
    health_thread = threading.Thread(target=start_health_server, daemon=True)
    health_thread.start()

    # Start CPU tracker thread
    cpu_thread = threading.Thread(target=track_cpu, daemon=True)
    cpu_thread.start()

    # Start CPU burner worker processes
    print(f"[INFO]  [main] Starting {NUM_WORKERS} worker processes.")
    pool = multiprocessing.Pool(processes=NUM_WORKERS)
    pool.map_async(burn_cpu, range(NUM_WORKERS))

    # Main process now just reports metrics and keeps the pod alive
    while True:
        time.sleep(5)
        print(f"[INFO] [main-process] cpu={_current_cpu:.1f}%, QoS={_app_qos:.1f}")
