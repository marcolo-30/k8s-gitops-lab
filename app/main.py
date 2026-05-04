#!/usr/bin/env python3
"""
server.py – "Naive" image processing server using FastAPI.
This version is vulnerable to CPU load and exports QoS metrics.
"""

import os
import io
import time
import base64
import logging
from PIL import Image, ImageFilter, ImageOps
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel
import uvicorn

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# --- Configuration ---
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8080"))
MAX_IMAGE_MB = float(os.getenv("MAX_IMAGE_MB", "20.0"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "image-processor-naive")
# CORRECTED: Updated OTEL_ENDPOINT to the new IP address
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://192.168.0.42:4318")
QOS_LATENCY_BASELINE = float(os.getenv("QOS_LATENCY_BASELINE", "0.5"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("image-processor")

# --- OTEL Setup ---
_otel_ok = False
_last_qos = {"composite": 1.0} # Placeholder for QoS calculation
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

# --- QoS Calculation Logic (Simplified for this context) ---
_qos_lock   = threading.Lock()
_qos_events = deque()
def _qos_append(event_type: str, proc_time: float = 0.0, retries: int = 1):
    with _qos_lock:
        _qos_events.append({"ts": time.time(), "type": event_type, "proc_time": proc_time, "retries": retries})
def _qos_compute():
    # This is a placeholder for the actual QoS calculation logic
    # which is more complex and depends on _qos_events, cpu_pct, etc.
    # For now, we'll return a dummy value or the last known value.
    # The full logic from the previous main.py would go here.
    return {"composite": 1.0} # Placeholder

def _qos_poller():
    global _last_qos
    while True:
        _last_qos = _qos_compute()
        time.sleep(5)
threading.Thread(target=_qos_poller, daemon=True).start()


# --- Image Processing Logic ---
def _process_image_bytes(image_bytes, ops):
    t0 = time.perf_counter()
    img = Image.open(io.BytesIO(image_bytes))
    if img.mode != "RGB": img = img.convert("RGB")
    for op in ops:
        if op.get("name") == "blur": img = img.filter(ImageFilter.GaussianBlur(radius=2))
        elif op.get("name") == "grayscale": img = ImageOps.grayscale(img).convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    output_bytes = buf.getvalue()
    elapsed = time.perf_counter() - t0
    return output_bytes, {"processing_time_seconds": round(elapsed, 4)}

# --- FastAPI App ---
app = FastAPI(title="Image Processor", version="1.0")

class ImageOperation(BaseModel):
    name: str
    radius: Optional[float] = None

class ProcessRequest(BaseModel):
    image_base64: str
    operations: list[ImageOperation] = [{"name": "blur"}]

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/process")
async def process(payload: ProcessRequest, request: Request):
    client_attempt = int(request.headers.get("X-Attempt", 1))
    
    try:
        image_bytes = base64.b64decode(payload.image_base64)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 image data")

    try:
        t0 = time.perf_counter()
        output_bytes, stats = _process_image_bytes(image_bytes, payload.operations)
        duration = time.perf_counter() - t0
        
        _qos_append("success", proc_time=duration, retries=client_attempt)
        log.info(f"✓ Processed job in {duration:.3f}s | QoS={_last_qos['composite']:.2f} | Attempt={client_attempt}")
        
        headers = {
            "X-Processing-Time": str(duration),
            "X-QoS-Composite": str(_last_qos["composite"]),
        }
        return Response(content=output_bytes, media_type="image/jpeg", headers=headers)

    except Exception as e:
        log.error(f"Processing failed: {e}")
        _qos_append("error", retries=client_attempt)
        raise HTTPException(status_code=500, detail="Image processing failed")


# --- Entry Point ---
if __name__ == "__main__":
    log.info(f"Starting NAIVE image-processor server on {HOST}:{PORT}")
    uvicorn.run(app, host=HOST, port=PORT)
