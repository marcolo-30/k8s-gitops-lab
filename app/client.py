# client.py
# A resilient client that calculates and exposes Network & Queuing Time.

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
RETRY_DELAY_SECONDS = 2

# --- OTEL Setup ---
_latest_rtt = 0.0
_latest_network_time = 0.0 # Variable for the new metric
resource = Resource(attributes={'service.name': SERVICE_NAME})
reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"), export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter('api-client.meter')

# --- Métricas del Cliente ---
def rtt_gauge_callback(options):
    yield metrics.Observation(_latest_rtt, {})
meter.create_observable_gauge('client_rtt_seconds_gauge', [rtt_gauge_callback], description='The total RTT for a job, including retries.')

# --- NUEVA MÉTRICA: Network & Queuing Time ---
def network_time_gauge_callback(options):
    yield metrics.Observation(_latest_network_time, {})
meter.create_observable_gauge(
    name='client_network_and_queue_time_seconds',
    callbacks=[network_time_gauge_callback],
    description='Network latency + server queue time (Total RTT - App Duration).'
)
print('[CLIENT] All metrics registered.', flush=True)


def process_job_with_retries():
    global _latest_rtt, _latest_network_time
    url = f'{SERVICE_ENDPOINT}/process'
    
    request_token = time.time()
    payload = {"token": request_token}
    
    first_attempt_time = time.time()
    attempts = 0

    print(f'--> [JOB {request_token}] Starting new job.', flush=True)

    while True:
        attempts += 1
        print(f'--> [JOB {request_token}] Attempt #{attempts}: Sending request...', flush=True)
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                response_token = data.get("token")

                if response_token == request_token:
                    total_duration = time.time() - first_attempt_time
                    _latest_rtt = total_duration
                    
                    # --- CÁLCULO DEL TIEMPO DE RED Y COLA ---
                    server_processing_time = data.get("processing_time_seconds", 0)
                    network_and_queue_time = max(0, total_duration - server_processing_time)
                    _latest_network_time = network_and_queue_time

                    print(f'<-- [JOB {request_token}] SUCCESS (Validated) | Total RTT: {total_duration:.2f}s | App Duration: {server_processing_time:.2f}s | Network Time: {network_and_queue_time:.2f}s', flush=True)
                    return
                else:
                    print(f'<-- [JOB {request_token}] FAILED (Token Mismatch) | Retrying in {RETRY_DELAY_SECONDS}s...', flush=True)
            else:
                print(f'<-- [JOB {request_token}] FAILED (HTTP {response.status_code}) | Retrying in {RETRY_DELAY_SECONDS}s...', flush=True)

        except requests.exceptions.Timeout:
            print(f'<-- [JOB {request_token}] TIMEOUT | Retrying in {RETRY_DELAY_SECONDS}s...', flush=True)
        except requests.exceptions.RequestException:
            print(f'<-- [JOB {request_token}] CONNECTION ERROR | Retrying in {RETRY_DELAY_SECONDS}s...', flush=True)
        
        time.sleep(RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    print(f'[CLIENT] Resilient client started. Targeting: {SERVICE_ENDPOINT}.', flush=True)
    while True:
        process_job_with_retries()
        time.sleep(1)
