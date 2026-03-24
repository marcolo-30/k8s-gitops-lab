# app/main.py
# Goal: A multi-process app that allows configuring CPU core usage.

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

# --- Configuration ---
SERVICE_NAME = os.getenv("SERVICE_NAME", "main-app")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4318")
WORKLOAD_SIZE = int(os.getenv("WORKLOAD_SIZE", "200000"))
# Allow configuring the number of workers. Defaults to all cores if not set.
NUM_WORKERS = int(os.getenv("NUM_WORKERS", multiprocessing.cpu_count()))

print(f"[INFO]  [startup] service={SERVICE_NAME}, workers={NUM_WORKERS}, workload_size={WORKLOAD_SIZE}")

# --- OTEL Setup ---
resource = Resource(attributes={"service.name": SERVICE_NAME})
reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"), export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("main-app.meter")

# --- Metrics State & Callbacks ---
_current_cpu = 0.0
_iterations_per_sec = 0.0
_task_duration_ms = 0.0

def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

def get_iterations(options: CallbackOptions):
    yield Observation(_iterations_per_sec, {"service": SERVICE_NAME})

def get_task_duration(options: CallbackOptions):
    yield Observation(_task_duration_ms, {"service": SERVICE_NAME})

meter.create_observable_gauge("main_app.cpu_percent", callbacks=[get_cpu], description="Total CPU usage of the pod (main + workers).")
meter.create_observable_gauge("main_app.iterations_per_sec", callbacks=[get_iterations], description="Iterations per second of the main process.")
meter.create_observable_gauge("main_app.task_duration_ms", callbacks=[get_task_duration], description="Task duration of the main process.")

print("[INFO]  [startup] Metrics registered.")

# --- CPU Sampler Thread ---
def track_cpu():
    global _current_cpu
    process = psutil.Process()
    while True:
        _current_cpu = process.cpu_percent(interval=1)

threading.Thread(target=track_cpu, daemon=True).start()

# --- Worker Function ---
def burn_cpu(_):
    while True:
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))

# --- Main Execution Block ---
if __name__ == "__main__":
    # Ensure we don't try to use more workers than available cores
    if NUM_WORKERS > multiprocessing.cpu_count():
        print(f"[WARN]  [main] NUM_WORKERS ({NUM_WORKERS}) is greater than available cores ({multiprocessing.cpu_count()}). Capping to max available.")
        NUM_WORKERS = multiprocessing.cpu_count()

    print(f"[INFO]  [main] Starting {NUM_WORKERS} worker processes.")
    
    pool = multiprocessing.Pool(processes=NUM_WORKERS)
    pool.map_async(burn_cpu, range(NUM_WORKERS))

    window_start, window_iters = time.time(), 0
    while True:
        time.sleep(1.0)
        window_iters += 1
        
        elapsed = time.time() - window_start
        if elapsed >= 1.0:
            _iterations_per_sec = window_iters / elapsed
            print(f"[INFO] [main-process] cpu={_current_cpu:.1f}% (Pod Total)")
            window_start, window_iters = time.time(), 0
