# client.py
# A client that validates the server's response using a token.

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

# --- OTEL Setup ---
_latest_rtt = 0.0
resource = Resource(attributes={'service.name': SERVICE_NAME})
reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"), export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter('api-client.meter')

def rtt_gauge_callback(options):
    yield metrics.Observation(_latest_rtt, {})

meter.create_observable_gauge('client_rtt_seconds_gauge', [rtt_gauge_callback], description='The most recent RTT value.')

def make_request():
    global _latest_rtt
    url = f'{SERVICE_ENDPOINT}/process'
    
    # Create a unique token for this request
    request_token = time.time()
    payload = {"token": request_token}

    print(f'--> [CLIENT] Sending request with token {request_token}', flush=True)
    try:
        start_time = time.time()
        response = requests.post(url, json=payload, timeout=30)
        duration = time.time() - start_time
        
        _latest_rtt = duration

        if response.status_code == 200:
            data = response.json()
            response_token = data.get("token")

            # --- Validation Logic ---
            if response_token == request_token:
                print(f'<-- [CLIENT] SUCCESS (Validated) | RTT: {duration:.2f}s | App Duration: {data.get("processing_time_seconds", 0):.2f}s', flush=True)
            else:
                print(f'<-- [CLIENT] FAILED (Token Mismatch) | Expected {request_token}, Got {response_token}', flush=True)
        else:
            print(f'<-- [CLIENT] FAILED (HTTP Error) | RTT: {duration:.2f}s | Status: {response.status_code}', flush=True)
    except requests.exceptions.Timeout:
        _latest_rtt = 30.0
        print(f'<-- [CLIENT] TIMEOUT after 30s.', flush=True)
    except requests.exceptions.RequestException as e:
        _latest_rtt = -1.0
        print(f'<-- [CLIENT] ERROR: {e}', flush=True)

if __name__ == "__main__":
    print(f'[CLIENT] Client started. Targeting: {SERVICE_ENDPOINT}.', flush=True)
    while True:
        make_request()
        time.sleep(2)
