# app/main.py
# Runs forever — measures its own cpu_percent and pushes it to OTEL every 2 seconds
# This is the "victim" pod — watch its metric degrade when the burner pod runs

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
resource       = Resource(attributes={"service.name": SERVICE_NAME})
exporter       = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
reader         = PeriodicExportingMetricReader(exporter, export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("main-app.meter")

# --- Metric ---
_current_cpu = 0.0

def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

meter.create_observable_gauge(
    "main_app.cpu_percent",
    callbacks=[get_cpu],
    description="CPU percent of this pod. Watch it change when the burner pod runs."
)

# --- Background CPU sampler ---
def track_cpu():
    global _current_cpu
    process = psutil.Process()
    while True:
        _current_cpu = process.cpu_percent(interval=1)
        print(f"[cpu] {_current_cpu:.1f}%")

if __name__ == "__main__":
    threading.Thread(target=track_cpu, daemon=True).start()
    print("[main] Running. Pushing cpu_percent metric every 2s...")
    while True:
        time.sleep(5)