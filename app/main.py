# app/main.py
# Heavier work loop — sensitive to CPU throttling
# Removes CPU limit so pod competes freely for node CPU
# When burner runs: iterations_per_sec drops 40-60% visibly in Grafana

import os
import time
import math
import threading
import psutil
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, MetricExportResult
from opentelemetry.sdk.resources import Resource

SERVICE_NAME  = os.getenv("SERVICE_NAME", "main-app")
OTEL_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector.observability.svc.cluster.local:4318"
)
# Tuned workload size. 50_000 works for a fast VM. 
# A smaller value like 5_000 is needed for a less powerful device like a Raspberry Pi.
WORKLOAD_SIZE = int(os.getenv("WORKLOAD_SIZE", "5000"))

print(f"[INFO]  [startup] service={SERVICE_NAME} endpoint={OTEL_ENDPOINT} workload_size={WORKLOAD_SIZE}")

# --- OTEL setup ---
inner_exporter = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
_original_export = inner_exporter.export

def _logged_export(metrics_data, timeout_millis=10000, **kwargs):
    try:
        result = _original_export(metrics_data, timeout_millis=timeout_millis, **kwargs)
        if result == MetricExportResult.SUCCESS:
            # Reducing log spam
            pass
        else:
            print(f"[ERROR] [otel-export] Push FAILED -> {OTEL_ENDPOINT}/v1/metrics")
        return result
    except Exception as e:
        print(f"[ERROR] [otel-export] Push EXCEPTION -> {e}")
        return MetricExportResult.FAILURE

inner_exporter.export = _logged_export

resource       = Resource(attributes={"service.name": SERVICE_NAME})
reader         = PeriodicExportingMetricReader(inner_exporter, export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("main-app.meter")

# --- Metrics ---
_current_cpu        = 0.0
_iterations_per_sec = 0.0
_task_duration_ms   = 0.0

def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

def get_iterations(options: CallbackOptions):
    yield Observation(_iterations_per_sec, {"service": SERVICE_NAME})

def get_task_duration(options: CallbackOptions):
    yield Observation(_task_duration_ms, {"service": SERVICE_NAME})

meter.create_observable_gauge("main_app.cpu_percent", callbacks=[get_cpu], description="CPU usage percent of this pod.")
meter.create_observable_gauge("main_app.iterations_per_sec", callbacks=[get_iterations], description="Work iterations per second — drops under CPU pressure.")
meter.create_observable_gauge("main_app.task_duration_ms", callbacks=[get_task_duration], description="Time in ms to complete one work unit — rises under CPU pressure.")
print("[INFO]  [startup] Metrics registered.")

# --- CPU sampler ---
def track_cpu():
    global _current_cpu
    process = psutil.Process()
    while True:
        _current_cpu = process.cpu_percent(interval=1)

threading.Thread(target=track_cpu, daemon=True).start()

# --- Heavy work loop ---
def work_loop():
    global _iterations_per_sec, _task_duration_ms
    iteration, window_start, window_iters = 0, time.time(), 0

    print(f"[INFO]  [work-loop] Starting heavy work loop with workload_size={WORKLOAD_SIZE}")

    while True:
        t0 = time.time()
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
        t1 = time.time()

        iteration    += 1
        window_iters += 1
        _task_duration_ms = (t1 - t0) * 1000

        if time.time() - window_start >= 1.0:
            elapsed = time.time() - window_start
            _iterations_per_sec = window_iters / elapsed
            
            level = "INFO " if _current_cpu < 70 else "WARN "
            if _current_cpu > 90: level = "ERROR"

            print(f"[{level}] [work-loop] iter/s={_iterations_per_sec:.0f} task_ms={_task_duration_ms:.1f} cpu={_current_cpu:.1f}% total={iteration}")
            window_start, window_iters = time.time(), 0

if __name__ == "__main__":
    print(f"[INFO]  [main] Pod starting — service={SERVICE_NAME}")
    work_loop()