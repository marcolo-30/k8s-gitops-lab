# app/main.py
# Runs a continuous CPU work loop and measures iterations/second
# When the burner pod steals CPU, iterations/sec drops visibly in Grafana

import os
import time
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

print(f"[INFO]  [startup] service={SERVICE_NAME} endpoint={OTEL_ENDPOINT}")

# --- OTEL setup ---
inner_exporter = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
_original_export = inner_exporter.export

def _logged_export(metrics_data, timeout_millis=10000, **kwargs):
    try:
        result = _original_export(metrics_data, timeout_millis=timeout_millis, **kwargs)
        if result == MetricExportResult.SUCCESS:
            print(f"[INFO]  [otel-export] Push SUCCESS -> {OTEL_ENDPOINT}/v1/metrics")
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
_current_cpu  = 0.0
_iterations_per_sec = 0.0

def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

def get_iterations(options: CallbackOptions):
    yield Observation(_iterations_per_sec, {"service": SERVICE_NAME})

meter.create_observable_gauge(
    "main_app.cpu_percent",
    callbacks=[get_cpu],
    description="CPU usage percent of this pod."
)
meter.create_observable_gauge(
    "main_app.iterations_per_sec",
    callbacks=[get_iterations],
    description="Work iterations per second — drops when CPU is stolen by another pod."
)
print("[INFO]  [startup] Metrics registered: main_app.cpu_percent, main_app.iterations_per_sec")

# --- CPU sampler ---
def track_cpu():
    global _current_cpu
    process = psutil.Process()
    while True:
        _current_cpu = process.cpu_percent(interval=1)

threading.Thread(target=track_cpu, daemon=True).start()

# --- Work loop — runs on main thread, competes for CPU ---
def work_loop():
    global _iterations_per_sec
    iteration  = 0
    window_start = time.time()
    window_iters = 0

    print("[INFO]  [work-loop] Starting — this loop competes for CPU with the burner pod")

    while True:
        # Real CPU work — math loop
        _ = sum(i * i for i in range(5_000))
        iteration  += 1
        window_iters += 1

        # Every second, calculate iterations/sec and log it
        elapsed = time.time() - window_start
        if elapsed >= 1.0:
            _iterations_per_sec = window_iters / elapsed
            cpu = _current_cpu

            # Log level based on CPU pressure
            if cpu < 30:
                level = "INFO "
            elif cpu < 70:
                level = "WARN "
            else:
                level = "ERROR"

            print(f"[{level}] [work-loop] iter/s={_iterations_per_sec:.0f} cpu={cpu:.1f}% total={iteration}")

            if cpu > 70:
                print(f"[ERROR] [work-loop] CPU above 70% — burner is stealing resources!")
            elif cpu > 30:
                print(f"[WARN ] [work-loop] CPU above 30% — starting to feel pressure")

            window_start = time.time()
            window_iters = 0

if __name__ == "__main__":
    print(f"[INFO]  [main] Pod starting — service={SERVICE_NAME}")
    print(f"[INFO]  [main] psutil={psutil.__version__}")
    print("[INFO]  [main] Watch main_app_iterations_per_sec in Grafana — it drops under CPU pressure")
    print("[INFO]  [main] To stress: kubectl run cpu-burner --image=python:3.12-slim -n myapp -- python3 -c '...'")
    work_loop()  # runs forever on main thread