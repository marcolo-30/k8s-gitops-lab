# app/main.py
# Server with:
#   - Token reading from request body (idempotency support)
#   - Token echoed back in response (client-side validation)
#   - OTEL push: processing_time_seconds + app_qos

import http.server
import socketserver
import json
import time
import math
import os

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SERVICE_NAME             = os.getenv("SERVICE_NAME", "image-processor")
OTEL_ENDPOINT            = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT",
                                      "http://otel-collector.observability.svc.cluster.local:4318")
PORT                     = 8080
WORKLOAD_SIZE            = int(os.getenv("WORKLOAD_SIZE", "2000000"))
LATENCY_HEALTHY_SECONDS  = float(os.getenv("LATENCY_HEALTHY_SECONDS",  "4.0"))
LATENCY_CRITICAL_SECONDS = float(os.getenv("LATENCY_CRITICAL_SECONDS", "10.0"))

# ---------------------------------------------------------------------------
# Self-calibration – computed once at startup
# ---------------------------------------------------------------------------
_self_validation_checksum = None   # set in __main__ before the server starts

# ---------------------------------------------------------------------------
# OpenTelemetry – server side
# ---------------------------------------------------------------------------
_app_qos = 100.0

resource = Resource(attributes={"service.name": SERVICE_NAME})
reader   = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"),
    export_interval_millis=2000,
)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)

meter = metrics.get_meter("image-processor.meter")

# Observable gauge – QoS (existing metric, kept as-is)
def qos_callback(options):
    yield metrics.Observation(_app_qos, {})

meter.create_observable_gauge(
    "app_qos",
    [qos_callback],
    description="QoS derived from processing latency (100 = healthy, 0 = critical).",
)

# Histogram – server-side processing time per request
processing_time_histogram = meter.create_histogram(
    name="processing_time_seconds",
    description="CPU-bound Proof-of-Work duration measured on the server.",
    unit="s",
)

# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------
class APIHandler(http.server.SimpleHTTPRequestHandler):

    # Silence the default per-request log line (keep our own prints cleaner)
    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        global _app_qos

        if self.path != "/process":
            self.send_error(404, "Not Found")
            return

        # --- Read request body (JSON with token) ---
        content_length = int(self.headers.get("Content-Length", 0))
        body           = self.rfile.read(content_length) if content_length else b"{}"
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {}

        token = payload.get("token", "unknown")
        print(f"[SERVER] /process received  token={token[:8]}...", flush=True)

        # --- CPU-bound Proof-of-Work (deterministic; time varies with CPU pressure) ---
        start_time  = time.time()
        actual_result = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
        duration    = time.time() - start_time

        # --- Proof-of-Work validation ---
        if int(actual_result) != _self_validation_checksum:
            error_msg = (
                f"Work Incomplete: checksum mismatch. "
                f"Expected {_self_validation_checksum}, got {int(actual_result)}"
            )
            print(f"[SERVER] ERROR: {error_msg}", flush=True)
            self.send_error(500, error_msg)
            return

        # --- Update QoS ---
        if duration <= LATENCY_HEALTHY_SECONDS:
            _app_qos = 100.0
        elif duration >= LATENCY_CRITICAL_SECONDS:
            _app_qos = 0.0
        else:
            _app_qos = 100.0 - (
                (duration - LATENCY_HEALTHY_SECONDS)
                / (LATENCY_CRITICAL_SECONDS - LATENCY_HEALTHY_SECONDS)
                * 100.0
            )

        # --- Publish server-side metrics to OTEL ---
        attrs = {"token": token}
        processing_time_histogram.record(duration, attrs)
        # _app_qos is published via the observable gauge callback automatically

        print(
            f"[SERVER] Work VERIFIED  token={token[:8]}...  "
            f"duration={duration:.3f}s  QoS={_app_qos:.1f}",
            flush=True,
        )

        # --- Respond with token + processing time (client needs both) ---
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        response = {
            "status":                   "ok",
            "token":                    token,          # echoed for client validation
            "processing_time_seconds":  duration,
            "qos":                      _app_qos,
        }
        self.wfile.write(json.dumps(response).encode("utf-8"))

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')
        else:
            self.send_error(404, "Not Found")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Self-calibration: compute the expected checksum once for this environment
    print("[SERVER] Self-calibrating (computing expected checksum)...", flush=True)
    _self_validation_checksum = int(
        sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
    )
    print(
        f"[SERVER] Calibration complete. "
        f"Checksum={_self_validation_checksum}  WORKLOAD_SIZE={WORKLOAD_SIZE}",
        flush=True,
    )

    with socketserver.ThreadingTCPServer(("", PORT), APIHandler) as httpd:
        print(
            f"[SERVER] Listening on port {PORT}  "
            f"OTEL={OTEL_ENDPOINT}",
            flush=True,
        )
        httpd.serve_forever()