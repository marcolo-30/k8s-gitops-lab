# app/main.py
# Goal: A multi-process app that saturates all available CPU cores and reports total worker iterations.

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
_total_worker_iterations_per_sec = 0.0 # New metric for total work done
_node_temperature_celsius = 0.0

def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

def get_total_worker_iterations(options: CallbackOptions):
    yield Observation(_total_worker_iterations_per_sec, {"service": SERVICE_NAME})

def get_temperature(options: CallbackOptions):
    yield Observation(_node_temperature_celsius, {"service": SERVICE_NAME})

meter.create_observable_gauge("main_app.cpu_percent", callbacks=[get_cpu], description="Total CPU usage of the pod (main + workers).")
meter.create_observable_gauge("main_app.total_worker_iterations_per_sec", callbacks=[get_total_worker_iterations], description="Total work units completed by all workers per second.")
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
                _node_temperature_celsius = int(f.read().strip()) / 1000.0
        except FileNotFoundError:
            _node_temperature_celsius = 0.0
        except Exception as e:
            print(f"[ERROR] [temp-tracker] Could not read temperature: {e}")
            _node_temperature_celsius = 0.0
        time.sleep(2)

threading.Thread(target=track_cpu, daemon=True).start()
threading.Thread(target=track_temperature, daemon=True).start()

# --- Worker Function ---
# This function will be run by each worker process to burn CPU and count its iterations.
def burn_cpu(shared_iterations_counter):
    while True:
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
        shared_iterations_counter.value += 1 # Increment shared counter

# --- Main Execution Block ---
if __name__ == "__main__":
    if NUM_WORKERS > multiprocessing.cpu_count():
        NUM_WORKERS = multiprocessing.cpu_count()

    print(f"[INFO]  [main] Starting {NUM_WORKERS} worker processes.")
    
    # Use a Manager to create a shared counter for total iterations
    manager = multiprocessing.Manager()
    shared_iterations_counter = manager.Value('L', 0) # 'L' for long integer

    # Create and start the pool of worker processes, passing the shared counter
    pool = multiprocessing.Pool(processes=NUM_WORKERS)
    # map_async expects an iterable for the second argument, so we pass a list of the counter for each worker
    pool.map_async(burn_cpu, [shared_iterations_counter] * NUM_WORKERS)

    last_total_iterations = 0
    last_time = time.time()

    while True:
        time.sleep(1.0) # Report metrics every second
        
        current_time = time.time()
        elapsed_time = current_time - last_time
        
        current_total_iterations = shared_iterations_counter.value
        delta_iterations = current_total_iterations - last_total_iterations
        
        if elapsed_time > 0:
            _total_worker_iterations_per_sec = delta_iterations / elapsed_time
        else:
            _total_worker_iterations_per_sec = 0.0

        print(f"[INFO] [main-process] cpu={_current_cpu:.1f}%, "
              f"total_iter/s={_total_worker_iterations_per_sec:.1f}, "
              f"temp={_node_temperature_celsius:.1f}°C")
        
        last_total_iterations = current_total_iterations
        last_time = current_time
