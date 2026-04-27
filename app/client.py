# client.py
# A client that makes requests and pushes both a Histogram and a Gauge for RTT.

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
_latest_rtt = 0.0 # Variable to hold the latest RTT for the gauge
resource = Resource(attributes={'service.name': SERVICE_NAME})
reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"), export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter('api-client.meter')

# --- Metric 1: Histogram (for statistical analysis) ---
rtt_histogram = meter.create_histogram(
    name="client_rtt_seconds",
    description="Round-Trip Time for requests from the client's perspective (Histogram).",
    unit="s",
)

# --- Metric 2: Gauge (for direct visualization) ---
def rtt_gauge_callback(options):
    yield metrics.Observation(_latest_rtt, {})

meter.create_observable_gauge(
    name='client_rtt_seconds_gauge',
    callbacks=[rtt_gauge_callback],
    description='The most recent RTT value from the client (Gauge).'
)

print('[CLIENT] Both Histogram and Gauge metrics are registered.', flush=True)

def make_request():
    global _latest_rtt
    url = f'{SERVICE_ENDPOINT}/process'
    print(f'--> [CLIENT] Sending request to {url}', flush=True)
    try:
        start_time = time.time()
        response = requests.post(url, timeout=30)
        duration = time.time() - start_time
        
        # Update both the gauge's variable and record in the histogram
        _latest_rtt = duration
        rtt_histogram.record(duration)

        if response.status_code == 200:
            data = response.json()
            print(f'<-- [CLIENT] SUCCESS | RTT: {duration:.2f}s | App Duration: {data.get("processing_time_seconds", 0):.2f}s', flush=True)
        else:
            print(f'<-- [CLIENT] FAILED  | RTT: {duration:.2f}s | Status: {response.status_code}', flush=True)
    except requests.exceptions.Timeout:
        _latest_rtt = 30.0 # Use a high value for timeout
        rtt_histogram.record(30.0)
        print(f'<-- [CLIENT] TIMEOUT after 30s.', flush=True)
    except requests.exceptions.RequestException as e:
        _latest_rtt = -1.0 # Use a negative value for connection error
        print(f'<-- [CLIENT] ERROR: {e}', flush=True)

if __name__ == "__main__":
    if "localhost" in SERVICE_ENDPOINT:
        print("\n[WARN] SERVICE_ENDPOINT is not set. Using default localhost.")
        print("       For in-cluster, set to: http://image-processor-svc:8080")
        print("       For external, set to: http://<node-ip>:<node-port>\n")

    print(f'[CLIENT] Client started. Targeting: {SERVICE_ENDPOINT}.', flush=True)
    while True:
        make_request()
        time.sleep(2)
