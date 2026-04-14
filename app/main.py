# app/main.py
# Final, consolidated version with all features.

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
# Configuration
# ---------------------------------------------------------------------------
SERVICE_NAME   = os.getenv("SERVICE_NAME", "main-app")
OTEL_ENDPOINT  = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4318")
WORKLOAD_SIZE  = int(os.getenv("WORKLOAD_SIZE", "80000"))
NUM_WORKERS    = int(os.getenv("NUM_WORKERS", "1"))
TEMP_FILE_PATH = "/sys/class/thermal/thermal_zone0/temp"
HEALTH_PORT    = 8080
CALIBRATION_SECONDS = int(os.getenv("CALIBRATION_SECONDS", "30"))

# QoS Configuration
LOAD_THRESHOLD = float(os.getenv("QOS_LOAD_THRESHOLD", "70"))
QOS_FLOOR      = float(os.getenv("QOS_FLOOR", "5"))
WEIGHT_LOAD    = float(os.getenv("QOS_WEIGHT_LOAD",  "0.70"))
WEIGHT_ITERS   = float(os.getenv("QOS_WEIGHT_ITERS", "0.30"))
EMA_ALPHA      = float(os.getenv("EMA_ALPHA", "0.15"))

# ---------------------------------------------------------------------------
# Shared State (for main process)
# ---------------------------------------------------------------------------
_current_cpu = 0.0
_total_worker_iterations_per_sec = 0.0
_ema_iterations_per_sec = 0.0
_node_temperature_celsius = 0.0
_node_load1, _node_load5, _node_load15 = 0.0, 0.0, 0.0
_node_load_pct = 0.0
_app_qos = 100.0
_qos_signal_load, _qos_signal_iters = 100.0, 100.0
_baseline_iters = None
_is_calibrating = True

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
                "status": "ok", "is_calibrating": _is_calibrating, "cpu_percent": _current_cpu,
                "qos": _app_qos, "node_load_percent": _node_load_pct, "iterations_per_sec": _total_worker_iterations_per_sec
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            self.send_error(404, "Not Found")

def start_health_server():
    with socketserver.ThreadingTCPServer(("", HEALTH_PORT), HealthCheckHandler) as httpd:
        httpd.serve_forever()

# ---------------------------------------------------------------------------
# Worker Function (for child processes)
# ---------------------------------------------------------------------------
def burn_cpu(shared_iterations_counter):
    while True:
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
        shared_iterations_counter.value += 1

# ---------------------------------------------------------------------------
# Sensor Threads (for main process)
# ---------------------------------------------------------------------------
def track_sensors():
    global _current_cpu, _node_temperature_celsius, _node_load1, _node_load5, _node_load15, _node_load_pct
    process = psutil.Process()
    num_cores = psutil.cpu_count()
    while True:
        _current_cpu = process.cpu_percent(interval=1)
        try:
            with open(TEMP_FILE_PATH, 'r') as f:
                _node_temperature_celsius = int(f.read().strip()) / 1000.0
        except Exception: _node_temperature_celsius = 0.0
        try:
            _node_load1, _node_load5, _node_load15 = psutil.getloadavg()
            _node_load_pct = (_node_load5 / num_cores) * 100
        except Exception: _node_load_pct = 0.0

# ---------------------------------------------------------------------------
# Main Execution Block
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"[INFO]  [startup] service={SERVICE_NAME}, workers={NUM_WORKERS}, workload_size={WORKLOAD_SIZE}", flush=True)

    # 1. Start worker processes
    manager = multiprocessing.Manager()
    shared_iterations_counter = manager.Value("L", 0)
    pool = multiprocessing.Pool(processes=NUM_WORKERS)
    pool.map_async(burn_cpu, [shared_iterations_counter] * NUM_WORKERS)

    # 2. Start sensor and health threads
    threading.Thread(target=track_sensors, daemon=True).start()
    threading.Thread(target=start_health_server, daemon=True).start()
    
    # 3. Initialize OTEL (post-fork)
    resource = Resource(attributes={"service.name": SERVICE_NAME})
    reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"), export_interval_millis=2000)
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meter_provider)
    meter = metrics.get_meter("main-app.meter")
    
    # Define and register all metrics
    def cb(obs, val): yield Observation(val, {"service": SERVICE_NAME})
    meter.create_observable_gauge("main_app.cpu_percent", lambda o: cb(o, _current_cpu))
    meter.create_observable_gauge("main_app.qos", lambda o: cb(o, _app_qos))
    meter.create_observable_gauge("main_app.node_load_percent", lambda o: cb(o, _node_load_pct))
    meter.create_observable_gauge("main_app.total_worker_iterations_per_sec", lambda o: cb(o, _total_worker_iterations_per_sec))
    meter.create_observable_gauge("main_app.node_temperature_celsius", lambda o: cb(o, _node_temperature_celsius))
    print("[INFO]  [main] Metrics registered.", flush=True)

    # 4. Main logic loop (calibration and QoS calculation)
    last_total_iterations, last_time = 0, time.time()
    calibration_samples = []
    print(f"[INFO]  [main] Calibrating for {CALIBRATION_SECONDS}s...", flush=True)

    while True:
        time.sleep(1.0)
        current_time, elapsed_time = time.time(), time.time() - last_time
        current_total, delta_iterations = shared_iterations_counter.value, shared_iterations_counter.value - last_total_iterations
        
        if elapsed_time > 0: _total_worker_iterations_per_sec = delta_iterations / elapsed_time
        else: _total_worker_iterations_per_sec = 0.0

        if _ema_iterations_per_sec == 0.0: _ema_iterations_per_sec = _total_worker_iterations_per_sec
        else: _ema_iterations_per_sec = (EMA_ALPHA * _total_worker_iterations_per_sec) + ((1 - EMA_ALPHA) * _ema_iterations_per_sec)

        if _is_calibrating:
            if _total_worker_iterations_per_sec > 0: calibration_samples.append(_total_worker_iterations_per_sec)
            if len(calibration_samples) >= CALIBRATION_SECONDS:
                sorted_samples, trim = sorted(calibration_samples), max(1, len(calibration_samples) // 10)
                _baseline_iters = sum(sorted_samples[trim:]) / len(sorted_samples[trim:])
                _is_calibrating = False
                print(f"[INFO]  [calibration] Baseline established: {_baseline_iters:.2f} iter/s", flush=True)
        else:
            if _node_load_pct < LOAD_THRESHOLD: load_signal = 100.0
            else: load_signal = max(QOS_FLOOR, 100 - (((_node_load_pct - LOAD_THRESHOLD) / (100 - LOAD_THRESHOLD)) * (100 - QOS_FLOOR)))
            iter_signal = max(0.0, min(100.0, (_ema_iterations_per_sec / _baseline_iters) * 100.0)) if _baseline_iters > 0 else 100.0
            _app_qos = (load_signal * WEIGHT_LOAD) + (iter_signal * WEIGHT_ITERS)

        last_total_iterations, last_time = current_total, current_time
        status = "CALIBRANDO" if _is_calibrating else ("OK" if _app_qos > 50 else ("WARN" if _app_qos > 20 else "CRIT"))
        print(f"[INFO] [main] [{status}] cpu={_current_cpu:.1f}% | iter/s={_total_worker_iterations_per_sec:.1f} | ema={_ema_iterations_per_sec:.1f} | "
              f"baseline={f'{_baseline_iters:.1f}' if _baseline_iters is not None else 'N/A'} | load5={_node_load5:.2f} load_pct={_node_load_pct:.1f}% | "
              f"temp={_node_temperature_celsius:.1f}°C | QoS={_app_qos:.1f}", flush=True)
