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
# ... (otras importaciones de OTEL)

# --- Configuration ---
# (La configuración se mantiene igual)
SERVICE_NAME = os.getenv("SERVICE_NAME", "image-processor-naive")
OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability.svc.cluster.local:4318")
QOS_LATENCY_BASELINE = float(os.getenv("QOS_LATENCY_BASELINE", "0.5"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("image-processor")

# --- OTEL, QoS, and Image Processing Logic (se mantienen igual) ---
# (Código omitido para brevedad)

# --- FastAPI App ---
app = FastAPI(title="Image Processor", version="1.0")

# --- Pydantic Models for Request Validation ---
class ImageOperation(BaseModel):
    name: str
    radius: Optional[float] = None
    # Add other operation parameters here if needed

class ProcessRequest(BaseModel):
    image_base64: str
    operations: list[ImageOperation] = [{"name": "blur"}]

# --- Endpoints ---
@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/process")
async def process(payload: ProcessRequest, request: Request):
    """
    Processes an image by applying a series of operations.
    This endpoint is intentionally CPU-bound and naive.
    """
    client_attempt = int(request.headers.get("X-Attempt", 1))
    
    try:
        image_bytes = base64.b64decode(payload.image_base64)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 image data")

    try:
        # --- Medir el tiempo de procesamiento real ---
        t0 = time.perf_counter()
        
        img = Image.open(io.BytesIO(image_bytes))
        if img.mode != "RGB": img = img.convert("RGB")

        for op in payload.operations:
            if op.name == "blur":
                img = img.filter(ImageFilter.GaussianBlur(radius=op.radius or 2.0))
            elif op.name == "grayscale":
                img = ImageOps.grayscale(img).convert("RGB")
        
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        output_bytes = buf.getvalue()
        
        duration = time.perf_counter() - t0
        
        # --- Lógica de QoS (se mantiene igual) ---
        # _qos_append("success", proc_time=duration, retries=client_attempt)
        log.info(f"✓ Processed job in {duration:.3f}s | Attempt={client_attempt}")
        
        headers = {
            "X-Processing-Time": str(duration),
            # "X-QoS-Composite": str(_last_qos["composite"]),
        }
        return Response(content=output_bytes, media_type="image/jpeg", headers=headers)

    except Exception as e:
        log.error(f"Processing failed: {e}")
        # _qos_append("error", retries=client_attempt)
        raise HTTPException(status_code=500, detail="Image processing failed")

# --- Entry Point ---
if __name__ == "__main__":
    log.info(f"Starting FastAPI server on 0.0.0.0:8080")
    # Uvicorn es el servidor ASGI que ejecuta la aplicación FastAPI
    uvicorn.run(app, host="0.0.0.0", port=8080)
