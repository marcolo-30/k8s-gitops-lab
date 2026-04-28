# client.py
# Resilient client with:
#   - UUID token per job (idempotency on retry)
#   - Uninterrupted wall-clock timer across retries
#   - OpenTelemetry push: total_time + network_latency

import os
import uuid
import requests
import time

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SERVICE_ENDPOINT  = os.getenv("SERVICE_ENDPOINT",  "http://localhost:8080")
OTEL_ENDPOINT     = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT",
                               "http://otel-collector.observability.svc.cluster.local:4318")
SERVICE_NAME      = os.getenv("SERVICE_NAME", "image-processor-client")
RETRY_DELAY_SECONDS = 2
REQUEST_TIMEOUT     = 10          # seconds – tune to be > max expected server processing time

# ---------------------------------------------------------------------------
# OpenTelemetry – client side
# ---------------------------------------------------------------------------
resource = Resource(attributes={"service.name": SERVICE_NAME})
reader   = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"),
    export_interval_millis=2000,
)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)

meter = metrics.get_meter("image-processor-client.meter")

# Histogram: full wall-clock time per job (includes all retries)
job_total_time_histogram = meter.create_histogram(
    name="job_total_time_seconds",
    description="Wall-clock time from first attempt to successful 200 OK (includes retries).",
    unit="s",
)

# Histogram: pure network round-trip latency (total_time – server_processing_time)
job_network_latency_histogram = meter.create_histogram(
    name="job_network_latency_seconds",
    description="Estimated network latency: total_time minus server-reported processing_time.",
    unit="s",
)

# ---------------------------------------------------------------------------
# Core job loop
# ---------------------------------------------------------------------------
def process_one_job():
    """
    Generates a single token, keeps retrying until the server confirms
    the token was processed, then publishes latency metrics to OTEL.
    """
    url   = f"{SERVICE_ENDPOINT}/process"
    token = str(uuid.uuid4())           # stable across all retries for this job

    first_attempt_time = time.time()
    attempts = 0

    print(f"\n--- Starting New Job ---", flush=True)
    print(f"    Token: {token}", flush=True)

    while True:
        attempts += 1
        print(f"--> [Attempt #{attempts}] Sending token {token[:8]}...", flush=True)

        try:
            response = requests.post(
                url,
                json={"token": token},   # server echoes this back for validation
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 200:
                t2   = time.time()
                data = response.json()

                # Validate the server echoed our token
                returned_token = data.get("token")
                if returned_token != token:
                    print(f"    WARNING: token mismatch! sent={token} got={returned_token}",
                          flush=True)

                server_proc_time = float(data.get("processing_time_seconds", 0))
                total_time       = t2 - first_attempt_time
                network_latency  = max(0.0, total_time - server_proc_time)

                # --- Publish to OpenTelemetry ---
                attrs = {"token": token, "attempts": str(attempts)}
                job_total_time_histogram.record(total_time,      attrs)
                job_network_latency_histogram.record(network_latency, attrs)

                print(f"<-- SUCCESS: Job confirmed by server.", flush=True)
                print(f"    Token            : {token}", flush=True)
                print(f"    Attempts         : {attempts}", flush=True)
                print(f"    Total time       : {total_time:.3f}s", flush=True)
                print(f"    Server proc time : {server_proc_time:.3f}s", flush=True)
                print(f"    Network latency  : {network_latency:.3f}s", flush=True)
                print(f"    QoS              : {data.get('qos', 'n/a')}", flush=True)
                return

            else:
                # Server returned an error (e.g. 500 checksum mismatch)
                print(f"<-- FAILED (HTTP {response.status_code}): will retry same token...",
                      flush=True)

        except requests.exceptions.Timeout:
            # Pod may have been migrated/killed mid-processing
            print(f"<-- FAILED (Timeout after {REQUEST_TIMEOUT}s): "
                  f"pod may have migrated – retrying same token...", flush=True)

        except requests.exceptions.RequestException as exc:
            print(f"<-- FAILED (Connection error: {exc}): retrying same token...", flush=True)

        # Brief back-off before the next attempt; timer is NOT reset
        time.sleep(RETRY_DELAY_SECONDS)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"[CLIENT] Started. Endpoint: {SERVICE_ENDPOINT}  OTEL: {OTEL_ENDPOINT}",
          flush=True)
    while True:
        process_one_job()
        print("----------------------------------------------------", flush=True)
        time.sleep(1)