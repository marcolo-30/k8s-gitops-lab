# app/main.py
# Goal: A multi-process app that allows configuring CPU core usage and reports node temperature.

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
NUM_WORKERS = int(os.getenv("NUM_WORKERS", multiprocessing.cpu_count()))
TEMP_FILE_PATH = "/sys/class/thermal/thermal_zone0/temp"

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
_node_temperature_celsius = 0.0

def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

def get_iterations(options: CallbackOptions):
    yield Observation(_iterations_per_sec, {"service": SERVICE_NAME})

def get_task_duration(options: CallbackOptions):
    yield Observation(_task_duration_ms, {"service": SERVICE_NAME})

def get_temperature(options: CallbackOptions):
    yield Observation(_node_temperature_celsius, {"service": SERVICE_NAME})

meter.create_observable_gauge("main_app.cpu_percent", callbacks=[get_cpu], description="Total CPU usage of the pod (main + workers).")
meter.create_observable_gauge("main_app.iterations_per_sec", callbacks=[get_iterations], description="Iterations per second of the main process.")
meter.create_observable_gauge("main_app.task_duration_ms", callbacks=[get_task_duration], description="Task duration of the main process.")
meter.create_observable_gauge("main_app.node_temperature_celsius", callbacks=[get_temperature], description="Temperature of the node's CPU in Celsius.")

print("[INFO]  [startup] Metrics registered.")

# --- Sensor Threads ---
def track_cpu():
    global _current_cpu
    process = psutil.Process()
    while True:
        _current_cpu = process.cpu_percent(interval=1)

def track_temperature():
    global _node_temperature_celsius
    while True:
        try:
            with open(TEMP_FILE_PATH, 'r') as f:
                # The value is in milli-Celsius, e.g., 45000 -> 45.0 C
                _node_temperature_celsius = int(f.read().strip()) / 1000.0
        except FileNotFoundError:
            # Silently fail if the file doesn't exist (e.g., not on a RPi)
            _node_temperature_celsius = 0.0
        except Exception as e:
            print(f"[ERROR] [temp-tracker] Could not read temperature: {e}")
            _node_temperature_celsius = 0.0
        time.sleep(2) # Read temperature every 2 seconds

threading.Thread(target=track_cpu, daemon=True).start()
threading.Thread(target=track_temperature, daemon=True).start()

# --- Worker Function ---
def burn_cpu(_):
    while True:
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))

# --- Main Execution Block ---
if __name__ == "__main__":
    if NUM_WORKERS > multiprocessing.cpu_count():
        NUM_WORKERS = multiprocessing.cpu_count()

    print(f"[INFO]  [main] Starting {NUM_WORKERS} worker processes.")
    
    pool = multiprocessing.Pool(processes=NUM_WORKERS)
    pool.map_async(burn_cpu, range(NUM_WORKERS))

    while True:
        time.sleep(5) # Log pod status every 5 seconds
        print(f"[INFO] [main-process] cpu={_current_cpu:.1f}%, temp={_node_temperature_celsius:.1f}°C")
