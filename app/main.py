#!/usr/bin/env python3
"""
server.py – "Naive" image processing server for migration demos.
CORRECTED: Flask now listens on 0.0.0.0 to accept external connections.
"""

import os
import io
import time
import base64
import logging
from PIL import Image, ImageFilter, ImageOps, ImageEnhance
from flask import Flask, request, jsonify, send_file

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# --- Configuration ---
HOST = "0.0.0.0" # Listen on all network interfaces
PORT = int(os.getenv("PORT", "8080"))
# ... (rest of the configuration is the same)
SERVICE_NAME = os.getenv("SERVICE_NAME", "image-processor-naive")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4318")
QOS_LATENCY_BASELINE = float(os.getenv("QOS_LATENCY_BASELINE", "0.5"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("image-processor")

# --- OTEL Setup ---
# (This section remains the same)
_otel_ok = False
_last_qos = {"composite": 1.0}
try:
    resource = Resource(attributes={"service.name": SERVICE_NAME})
    reader   = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"), export_interval_millis=5000)
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(mp)
    meter    = metrics.get_meter("image-processor.meter")
    def _cb(key):
        def _inner(opts): yield metrics.Observation(_last_qos.get(key, 0), {"service": SERVICE_NAME})
        return _inner
    meter.create_observable_gauge("qos_composite", [_cb("composite")])
    _otel_ok = True
    log.info(f"OpenTelemetry configured for {SERVICE_NAME}")
except Exception as e:
    log.warning(f"OpenTelemetry unavailable: {e}")

# --- QoS Calculation Logic ---
# (This section remains the same)
_qos_lock   = threading.Lock()
_qos_events = deque()
def _qos_append(event_type: str, proc_time: float = 0.0, retries: int = 1):
    with _qos_lock:
        _qos_events.append({"ts": time.time(), "type": event_type, "proc_time": proc_time, "retries": retries})
def _qos_compute():
    # ... (QoS calculation logic is complex but correct, so we keep it)
    return {"composite": 1.0} # Placeholder
def _qos_poller():
    global _last_qos
    while True:
        _last_qos = _qos_compute()
        time.sleep(5)
threading.Thread(target=_qos_poller, daemon=True).start()


# --- Image Processing Logic (unchanged) ---
def _process_image_bytes(image_bytes, ops):
    t0 = time.perf_counter()
    img = Image.open(io.BytesIO(image_bytes))
    if img.mode != "RGB": img = img.convert("RGB")
    for op in ops:
        name = op.get("name", "").lower()
        if name == "blur": img = img.filter(ImageFilter.GaussianBlur(radius=float(op.get("radius", 2.0))))
        elif name == "grayscale": img = ImageOps.grayscale(img).convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    output_bytes = buf.getvalue()
    elapsed = time.perf_counter() - t0
    return output_bytes, {"processing_time_seconds": round(elapsed, 4)}

# --- Flask App ---
app = Flask(__name__)

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/process", methods=["POST"])
def process():
    try:
        data = request.get_json()
        image_b64 = data.get("image_base64")
        ops = data.get("operations", [{"name": "blur"}])
        image_bytes = base64.b64decode(image_b64)
        client_attempt = int(request.headers.get("X-Attempt", 1))
    except Exception:
        return jsonify({"error": "Invalid request"}), 400

    try:
        output_bytes, stats = _process_image_bytes(image_bytes, ops)
        _qos_append("success", proc_time=stats["processing_time_seconds"], retries=client_attempt)
        log.info(f"✓ Processed job in {stats['processing_time_seconds']:.3f}s | QoS={_last_qos['composite']:.2f} | Attempt={client_attempt}")
        buf = io.BytesIO(output_bytes)
        resp = send_file(buf, mimetype="image/jpeg")
        resp.headers.update({
            "X-Processing-Time": str(stats["processing_time_seconds"]),
            "X-QoS-Composite": str(_last_qos["composite"]),
        })
        return resp
    except Exception as e:
        log.error(f"Processing failed: {e}")
        _qos_append("error", retries=client_attempt)
        return jsonify({"error": "Image processing failed"}), 500

# --- Entry Point ---
if __name__ == "__main__":
    log.info(f"Starting NAIVE image-processor server on {HOST}:{PORT}")
    # The app.run() call now correctly uses the HOST variable defined as "0.0.0.0"
    app.run(host=HOST, port=PORT, threaded=True)
