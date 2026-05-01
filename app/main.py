#!/usr/bin/env python3
"""
server.py – "Naive" image processing server for migration demos.
This version has its self-protection logic (throttling, load shedding) REMOVED.
It will accept all work until it breaks, allowing us to measure its degradation.
"""

import os
import io
import time
import uuid
import threading
import hashlib
import base64
import logging
from collections import deque

import psutil
from PIL import Image, ImageFilter, ImageOps, ImageEnhance
from flask import Flask, request, jsonify, send_file, Response

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# --- Configuration ---
HOST                 = os.getenv("HOST", "0.0.0.0")
PORT                 = int(os.getenv("PORT", "8080"))
OTEL_ENDPOINT        = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4318")
SERVICE_NAME         = os.getenv("SERVICE_NAME", "image-processor-naive")
QOS_WINDOW_SEC       = float(os.getenv("QOS_WINDOW_SEC", "60.0"))
QOS_LATENCY_BASELINE = float(os.getenv("QOS_LATENCY_BASELINE", "0.5")) # A fast baseline for a powerful node
CPU_COUNT            = psutil.cpu_count(logical=True) or 2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("image-processor")

# --- OpenTelemetry Setup ---
# (This section remains the same, it will report QoS metrics)
_otel_ok = False
try:
    resource = Resource(attributes={"service.name": SERVICE_NAME})
    reader   = PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"), export_interval_millis=5000)
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(mp)
    meter    = metrics.get_meter("image-processor.meter")
    _otel_ok = True
    log.info(f"OpenTelemetry configured for {SERVICE_NAME}")
except Exception as e:
    log.warning(f"OpenTelemetry unavailable: {e}")

# --- QoS Calculation Logic ---
# (This section remains the same, it calculates QoS based on performance)
_qos_lock   = threading.Lock()
_qos_events = deque()
_last_qos = {"composite": 1.0}

def _qos_append(event_type: str, proc_time: float = 0.0, retries: int = 1):
    with _qos_lock:
        _qos_events.append({"ts": time.time(), "type": event_type, "proc_time": proc_time, "retries": retries})

def _qos_compute() -> dict:
    # ... (QoS calculation logic is complex but correct, so we keep it)
    now = time.time()
    cutoff = now - QOS_WINDOW_SEC
    with _qos_lock:
        while _qos_events and _qos_events[0]["ts"] < cutoff:
            _qos_events.popleft()
        window = list(_qos_events)

    cpu_pct = psutil.cpu_percent()
    cpu_score = max(0.0, 1.0 - (cpu_pct / 100.0))

    if not window:
        return {"composite": cpu_score, "rejection_rate": 0.0, "retry_penalty": 1.0, "latency_score": 1.0, "cpu_score": cpu_score}

    total = len(window)
    rejected = sum(1 for e in window if e["type"] == "rejected")
    successes = [e for e in window if e["type"] == "success"]
    rejection_rate = rejected / total
    avg_retries = (sum(e["retries"] for e in successes) / len(successes)) if successes else 1.0
    retry_penalty = max(0.0, 1.0 - ((avg_retries - 1) / 4.0))
    avg_proc_time = (sum(e["proc_time"] for e in successes) / len(successes)) if successes else 0.0
    latency_score = max(0.0, 1.0 - (avg_proc_time / (QOS_LATENCY_BASELINE * 2)))
    composite = ((1.0 - rejection_rate) * 0.40 + retry_penalty * 0.25 + latency_score * 0.20 + cpu_score * 0.15)
    
    return {"composite": round(composite, 4), "rejection_rate": round(rejection_rate, 4), "retry_penalty": round(retry_penalty, 4), "latency_score": round(latency_score, 4), "cpu_score": round(cpu_score, 4)}

def _qos_poller():
    global _last_qos
    while True:
        _last_qos = _qos_compute()
        time.sleep(5)
threading.Thread(target=_qos_poller, daemon=True).start()

if _otel_ok:
    def _cb(key):
        def _inner(opts): yield metrics.Observation(_last_qos.get(key, 0), {"service": SERVICE_NAME})
        return _inner
    meter.create_observable_gauge("qos_composite", [_cb("composite")])
    meter.create_observable_gauge("qos_rejection_rate", [_cb("rejection_rate")])
    meter.create_observable_gauge("qos_retry_penalty", [_cb("retry_penalty")])
    meter.create_observable_gauge("qos_latency_score", [_cb("latency_score")])
    meter.create_observable_gauge("qos_cpu_score", [_cb("cpu_score")])

# --- Image Processing Logic (unchanged) ---
def _process_image_bytes(image_bytes, ops):
    t0 = time.perf_counter()
    img = Image.open(io.BytesIO(image_bytes))
    if img.mode != "RGB": img = img.convert("RGB")
    # (The actual image operations are kept)
    for op in ops:
        name = op.get("name", "").lower()
        if name == "blur": img = img.filter(ImageFilter.GaussianBlur(radius=float(op.get("radius", 2.0))))
        elif name == "grayscale": img = ImageOps.grayscale(img).convert("RGB")
    
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    output_bytes = buf.getvalue()
    elapsed = time.perf_counter() - t0
    checksum = hashlib.sha256(output_bytes).hexdigest()[:16]
    return output_bytes, {"processing_time_seconds": round(elapsed, 4), "checksum": checksum}

# --- Flask App ---
app = Flask(__name__)

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/process", methods=["POST"])
def process():
    # --- SELF-PROTECTION LOGIC REMOVED ---
    # This server is now "naive" and will accept all requests,
    # making it vulnerable to high CPU load.
    
    try:
        data = request.get_json()
        image_b64 = data.get("image_base64")
        ops = data.get("operations", [{"name": "blur"}])
        image_bytes = base64.b64decode(image_b64)
        client_attempt = int(request.headers.get("X-Attempt", 1))
    except Exception as e:
        return jsonify({"error": "Invalid request"}), 400

    try:
        output_bytes, stats = _process_image_bytes(image_bytes, ops)
        # Append a success event for QoS calculation
        _qos_append("success", proc_time=stats["processing_time_seconds"], retries=client_attempt)
        
        log.info(f"✓ Processed job in {stats['processing_time_seconds']:.3f}s | QoS={_last_qos['composite']:.2f} | Attempt={client_attempt}")
        
        buf = io.BytesIO(output_bytes)
        resp = send_file(buf, mimetype="image/jpeg")
        resp.headers.update({
            "X-Processing-Time": str(stats["processing_time_seconds"]),
            "X-Checksum": stats["checksum"],
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
    app.run(host=HOST, port=PORT, threaded=True)
