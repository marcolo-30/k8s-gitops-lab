# app/main.py
import os
import time
import threading
import psutil
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

SERVICE_NAME  = os.getenv("SERVICE_NAME", "main-app")
OTEL_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector.observability.svc.cluster.local:4318"
)

# --- OpenTelemetry setup ---
print(f"[INFO]  [startup] Initializing OpenTelemetry — service={SERVICE_NAME}")
print(f"[INFO]  [startup] OTEL endpoint: {OTEL_ENDPOINT}")

resource       = Resource(attributes={"service.name": SERVICE_NAME})
exporter       = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
reader         = PeriodicExportingMetricReader(exporter, export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("main-app.meter")
print("[INFO]  [startup] OpenTelemetry initialized — exporting every 2s")

# --- Metric ---
_current_cpu = 0.0

def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

meter.create_observable_gauge(
    "main_app.cpu_percent",
    callbacks=[get_cpu],
    description="CPU percent of this pod. Watch it change when the burner pod runs."
)
print("[INFO]  [startup] Metric registered: main_app.cpu_percent")

# --- CPU thresholds for log levels ---
def cpu_level(cpu):
    if cpu < 30:
        return "INFO "
    elif cpu < 70:
        return "WARN "
    else:
        return "ERROR"

_prev_level = None

def track_cpu():
    global _current_cpu, _prev_level
    process = psutil.Process()
    iteration = 0

    print("[INFO]  [cpu-tracker] Starting CPU sampling loop")

    while True:
        _current_cpu = process.cpu_percent(interval=1)
        level = cpu_level(_current_cpu)
        iteration += 1

        # Always print the current reading
        print(f"[{level}] [cpu-tracker] cpu={_current_cpu:.1f}% iteration={iteration}")

        # Log a warning when crossing thresholds
        if _prev_level != level:
            if level == "WARN ":
                print(f"[WARN ] [cpu-tracker] CPU crossed 30% threshold — possible external pressure")
            elif level == "ERROR":
                print(f"[ERROR] [cpu-tracker] CPU crossed 70% threshold — node under heavy load!")
            elif level == "INFO " and _prev_level is not None:
                print(f"[INFO ] [cpu-tracker] CPU back to normal levels — pressure relieved")
            _prev_level = level

        # Every 30 iterations print a summary
        if iteration % 30 == 0:
            print(f"[INFO]  [cpu-tracker] ---- 30s summary: avg_cpu={_current_cpu:.1f}% total_iterations={iteration} ----")

if __name__ == "__main__":
    print(f"[INFO]  [main] Pod starting up — service={SERVICE_NAME}")
    print(f"[INFO]  [main] psutil version: {psutil.__version__}")

    threading.Thread(target=track_cpu, daemon=True).start()
    print("[INFO]  [main] CPU tracker thread started")
    print("[INFO]  [main] Ready — pushing metrics to OTEL every 2s")
    print("[INFO]  [main] To stress test run: kubectl apply -f burner/cpu-burner-job.yaml")

    while True:
        time.sleep(5)