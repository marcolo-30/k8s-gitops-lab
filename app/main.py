# app/main.py
# Professional version with Proof-of-Work using a validation checksum.

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

# --- Configuration ---
SERVICE_NAME = os.getenv("SERVICE_NAME", "image-processor")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4318")
PORT = 8080
WORKLOAD_SIZE = int(os.getenv("WORKLOAD_SIZE", "2000000"))
LATENCY_HEALTHY_SECONDS = float(os.getenv("LATENCY_HEALTHY_SECONDS", "4.0"))
LATENCY_CRITICAL_SECONDS = float(os.getenv("LATENCY_CRITICAL_SECONDS", "10.0"))

# --- Proof-of-Work Checksum ---
# The correct integer result for the calculation with WORKLOAD_SIZE = 2,000,000
VALIDATION_CHECKSUM = 13419921163

# --- OTEL Setup ---
_app_qos = 100.0
resource = Resource(attributes={'service.name': SERVICE_NAME})
reader = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"), export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter('image-processor.meter')

def qos_callback(options):
    yield metrics.Observation(_app_qos, {})

meter.create_observable_gauge('app_qos', [qos_callback], description='QoS based on processing latency.')

# --- API Handler ---
class APIHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        global _app_qos
        if self.path == '/process':
            print(f'[SERVER] Received request for /process.', flush=True)
            start_time = time.time()
            
            # Perform the work and get the actual result
            actual_result = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
            duration = time.time() - start_time
            
            # --- Proof-of-Work Validation ---
            if int(actual_result) != VALIDATION_CHECKSUM:
                # The work was interrupted and is incomplete. Fail hard.
                error_message = f"Work Incomplete: Checksum Mismatch. Expected {VALIDATION_CHECKSUM} but got {int(actual_result)}"
                print(f'[SERVER] ERROR: {error_message}', flush=True)
                self.send_error(500, error_message)
                return

            # If we reach here, the work is validated. Proceed normally.
            if duration <= LATENCY_HEALTHY_SECONDS:
                _app_qos = 100.0
            elif duration >= LATENCY_CRITICAL_SECONDS:
                _app_qos = 0.0
            else:
                _app_qos = 100 - (((duration - LATENCY_HEALTHY_SECONDS) / (LATENCY_CRITICAL_SECONDS - LATENCY_HEALTHY_SECONDS)) * 100)
            
            print(f'[SERVER] Work VERIFIED in {duration:.2f}s. QoS is now {_app_qos:.1f}', flush=True)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'status': 'ok', 'processing_time_seconds': duration, 'qos': _app_qos}
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            self.send_error(404, 'Not Found')
    
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{\"status\": \"ok\"}')
        else:
            self.send_error(404, 'Not Found')

# --- Main Execution ---
if __name__ == "__main__":
    with socketserver.ThreadingTCPServer(('', PORT), APIHandler) as httpd:
        print(f'[SERVER] Serving at port {PORT} with WORKLOAD_SIZE={WORKLOAD_SIZE}', flush=True)
        httpd.serve_forever()
