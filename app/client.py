# client.py
# A client that makes requests and pushes its own RTT metric to OpenTelemetry.

import os
import requests
import time
import json
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# --- Configuration ---
SERVICE_NAME = os.getenv("SERVICE_NAME", "api-client")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4318")
SERVICE_ENDPOINT = os.getenv("SERVICE_ENDPOINT", "http://localhost:8080")

# --- OTEL Setup for the Client ---
resource = Resource(attributes={'service.name': SERVICE_NAME})
reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"), export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter('api-client.meter')

# This histogram will record the RTT of each request.
rtt_histogram = meter.create_histogram(
    name="client_rtt_seconds",
    description="Round-Trip Time for requests from the client's perspective.",
    unit="s",
)

def make_request():
    url = f'{SERVICE_ENDPOINT}/process'
    print(f'--> [CLIENT] Sending request to {url}', flush=True)
    try:
        start_time = time.time()
        response = requests.post(url, timeout=30)
        duration = time.time() - start_time
        
        # Record the RTT in our histogram metric
        rtt_histogram.record(duration)

        if response.status_code == 200:
            data = response.json()
            print(f'<-- [CLIENT] SUCCESS | Status: {response.status_code} | App Duration: {data.get("processing_time_seconds", 0):.2f}s | Total RTT: {duration:.2f}s', flush=True)
        else:
            print(f'<-- [CLIENT] FAILED  | Status: {response.status_code} | Total RTT: {duration:.2f}s', flush=True)
    except requests.exceptions.Timeout:
        print(f'<-- [CLIENT] TIMEOUT | Request took more than 30 seconds.', flush=True)
    except requests.exceptions.RequestException as e:
        print(f'<-- [CLIENT] ERROR   | Could not connect to service: {e}', flush=True)

if __name__ == "__main__":
    if "localhost" in SERVICE_ENDPOINT:
        print("\n[WARN] SERVICE_ENDPOINT is not set. Using default localhost.")
        print("       For in-cluster, set to: http://image-processor-svc:8080")
        print("       For external, set to: http://<node-ip>:<node-port>\n")

    print(f'[CLIENT] Client started. Targeting endpoint: {SERVICE_ENDPOINT}. Pushing metrics as {SERVICE_NAME}.', flush=True)
    while True:
        make_request()
        time.sleep(2)
