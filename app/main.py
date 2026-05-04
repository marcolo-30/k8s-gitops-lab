#!/usr/bin/env python3
"""
main.py – Image processing server with multi-dimensional QoS metrics.

QoS scores (all in [0.0 – 1.0], higher = better):
  qos_latency_score   – baseline_latency / avg_processing_time   (most CPU-sensitive)
  qos_cpu_score       – 1 - cpu_percent/100                      (direct CPU pressure)
  qos_retry_score     – 1 / avg_retries_per_job                  (slow server → timeouts → retries)
  qos_rejection_rate  – error_count / total in window            (0 = good)
  qos_composite       – weighted combination of all four

These are the metrics you want to compare under CPU wave load.
Under contention: latency and cpu scores react fastest (~5s);
retry and rejection lag by ~15-30s.
"""

import os, io, time, base64, hashlib, logging, threading
from collections import deque
from typing import Optional, List

import psutil
from PIL import Image, ImageFilter, ImageOps, ImageEnhance
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
import uvicorn

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# ── Configuration ──────────────────────────────────────────────────────────────
HOST                 = "0.0.0.0"
PORT                 = int(os.getenv("PORT", "8080"))
MAX_IMAGE_MB         = float(os.getenv("MAX_IMAGE_MB", "20.0"))
SERVICE_NAME         = os.getenv("SERVICE_NAME", "image-processor-naive")
OTEL_ENDPOINT        = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://192.168.0.42:4318")
QOS_LATENCY_BASELINE = float(os.getenv("QOS_LATENCY_BASELINE", "0.5"))  # idle baseline in seconds
QOS_WINDOW_SECONDS   = float(os.getenv("QOS_WINDOW_SECONDS", "60.0"))   # sliding window

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("image-processor")

# ── QoS event store ────────────────────────────────────────────────────────────
_qos_lock:   threading.Lock = threading.Lock()
_qos_events: deque          = deque()
_last_qos: dict = {
    "composite": 1.0, "rejection": 0.0,
    "cpu": 1.0, "latency": 1.0, "retry": 1.0,
    "_cpu_pct": 0.0, "_avg_proc_time": 0.0, "_avg_retries": 1.0, "_window_events": 0,
}

def _qos_append(event_type: str, proc_time: float = 0.0, retries: int = 1) -> None:
    with _qos_lock:
        _qos_events.append({
            "ts": time.time(), "type": event_type,
            "proc_time": proc_time, "retries": retries,
        })

def _qos_compute() -> dict:
    now = time.time()
    with _qos_lock:
        while _qos_events and now - _qos_events[0]["ts"] > QOS_WINDOW_SECONDS:
            _qos_events.popleft()
        events = list(_qos_events)

    total = len(events)

    # Rejection score
    if total == 0:
        rejection_rate  = 0.0
        rejection_score = 1.0
    else:
        errors          = sum(1 for e in events if e["type"] == "error")
        rejection_rate  = errors / total
        rejection_score = 1.0 - rejection_rate

    # CPU score — direct reading
    try:
        cpu_pct   = psutil.cpu_percent(interval=0.1)
        cpu_score = max(0.0, 1.0 - cpu_pct / 100.0)
    except Exception:
        cpu_pct, cpu_score = 0.0, 1.0

    # Latency score — most CPU-sensitive metric
    proc_times = [e["proc_time"] for e in events
                  if e["type"] == "success" and e["proc_time"] > 0]
    if proc_times:
        avg_proc      = sum(proc_times) / len(proc_times)
        latency_score = min(1.0, QOS_LATENCY_BASELINE / avg_proc)
    else:
        avg_proc      = 0.0
        latency_score = 1.0

    # Retry score — lags CPU by ~15s (client has to exhaust retries first)
    retry_vals  = [e["retries"] for e in events]
    avg_retries = (sum(retry_vals) / len(retry_vals)) if retry_vals else 1.0
    retry_score = 1.0 / max(1.0, avg_retries)

    # Composite
    composite = (
        0.35 * latency_score  +
        0.30 * cpu_score      +
        0.20 * retry_score    +
        0.15 * rejection_score
    )

    return {
        "composite":       round(composite,      4),
        "rejection":       round(rejection_rate, 4),
        "cpu":             round(cpu_score,      4),
        "latency":         round(latency_score,  4),
        "retry":           round(retry_score,    4),
        "_cpu_pct":        round(cpu_pct,        2),
        "_avg_proc_time":  round(avg_proc,       4),
        "_avg_retries":    round(avg_retries,    3),
        "_window_events":  total,
    }

def _qos_poller() -> None:
    global _last_qos
    while True:
        _last_qos = _qos_compute()
        time.sleep(5)

threading.Thread(target=_qos_poller, daemon=True).start()

# ── OpenTelemetry ──────────────────────────────────────────────────────────────
_otel_ok = False
processing_time_hist = image_input_size_hist = None
try:
    resource = Resource(attributes={"service.name": SERVICE_NAME})
    reader   = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"),
        export_interval_millis=5_000,
    )
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(mp)
    meter = metrics.get_meter("image-processor.meter")

    processing_time_hist = meter.create_histogram(
        "server_processing_time_seconds", unit="s",
        description="Per-request image processing time.")
    image_input_size_hist = meter.create_histogram(
        "server_input_bytes", unit="By",
        description="Input image size in bytes.")

    def _gauge_cb(key):
        def _inner(opts):
            yield metrics.Observation(_last_qos.get(key, 0), {"service": SERVICE_NAME})
        return _inner

    # One gauge per QoS dimension → separate Grafana panels
    meter.create_observable_gauge("qos_composite",      [_gauge_cb("composite")])
    meter.create_observable_gauge("qos_latency_score",  [_gauge_cb("latency")])
    meter.create_observable_gauge("qos_cpu_score",      [_gauge_cb("cpu")])
    meter.create_observable_gauge("qos_retry_score",    [_gauge_cb("retry")])
    meter.create_observable_gauge("qos_rejection_rate", [_gauge_cb("rejection")])
    meter.create_observable_gauge("qos_cpu_percent",    [_gauge_cb("_cpu_pct")])
    meter.create_observable_gauge("qos_avg_proc_time",  [_gauge_cb("_avg_proc_time")])

    _otel_ok = True
    log.info(f"OpenTelemetry configured → {OTEL_ENDPOINT}")
except Exception as exc:
    log.warning(f"OpenTelemetry unavailable: {exc}")

# ── Image processing ───────────────────────────────────────────────────────────
def _apply_ops(img: Image.Image, ops: list) -> Image.Image:
    for op in ops:
        name = op.get("name", "")
        if name == "blur":
            img = img.filter(ImageFilter.GaussianBlur(radius=float(op.get("radius", 2))))
        elif name == "grayscale":
            img = ImageOps.grayscale(img).convert("RGB")
        elif name == "sharpen":
            img = ImageEnhance.Sharpness(img).enhance(float(op.get("factor", 2.0)))
        elif name == "resize":
            img = img.resize((int(op.get("width", 800)), int(op.get("height", 600))),
                             Image.LANCZOS)
        elif name == "thumbnail":
            s = int(op.get("size", 256))
            img.thumbnail((s, s), Image.LANCZOS)
        elif name == "brightness":
            img = ImageEnhance.Brightness(img).enhance(float(op.get("factor", 1.0)))
        elif name == "contrast":
            img = ImageEnhance.Contrast(img).enhance(float(op.get("factor", 1.0)))
        elif name == "rotate":
            img = img.rotate(float(op.get("angle", 90)), expand=True)
        elif name == "flip":
            if op.get("direction", "horizontal") == "horizontal":
                img = ImageOps.mirror(img)
            else:
                img = ImageOps.flip(img)
        # "compress" is a no-op here; quality applied at save
    return img

def _process_image_bytes(image_bytes: bytes, ops: list) -> tuple:
    t0  = time.perf_counter()
    img = Image.open(io.BytesIO(image_bytes))
    if img.mode != "RGB":
        img = img.convert("RGB")
    img = _apply_ops(img, ops)
    quality = next((int(op.get("quality", 75)) for op in ops if op.get("name") == "compress"), 85)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality)
    return buf.getvalue(), time.perf_counter() - t0

# ── FastAPI ────────────────────────────────────────────────────────────────────
app = FastAPI(title="Image Processor (Naive)", version="2.0")

class ImageOperation(BaseModel):
    name:      str
    radius:    Optional[float] = None
    factor:    Optional[float] = None
    width:     Optional[int]   = None
    height:    Optional[int]   = None
    size:      Optional[int]   = None
    quality:   Optional[int]   = None
    angle:     Optional[float] = None
    direction: Optional[str]   = None

class ProcessRequest(BaseModel):
    token:        Optional[str]        = None
    image_base64: str
    operations:   List[ImageOperation] = [ImageOperation(name="blur")]

@app.get("/health")
async def health():
    q = _last_qos
    return {"status": "ok", "qos": q, "cpu_pct": q.get("_cpu_pct", 0)}

@app.get("/qos")
async def qos_snapshot():
    """Poll current QoS state without processing an image."""
    return _last_qos

@app.post("/process")
async def process(payload: ProcessRequest, request: Request):
    client_attempt = int(request.headers.get("X-Attempt", 1))
    token          = payload.token or "unknown"

    try:
        image_bytes = base64.b64decode(payload.image_base64)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 image data")

    if len(image_bytes) > MAX_IMAGE_MB * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"Image exceeds {MAX_IMAGE_MB}MB limit")

    ops = [op.model_dump(exclude_none=True) for op in payload.operations]

    try:
        output_bytes, duration = _process_image_bytes(image_bytes, ops)
    except Exception as exc:
        log.error(f"Processing error: {exc}")
        _qos_append("error", retries=client_attempt)
        raise HTTPException(status_code=500, detail="Image processing failed")

    _qos_append("success", proc_time=duration, retries=client_attempt)

    if _otel_ok:
        attrs = {"service": SERVICE_NAME}
        processing_time_hist.record(duration, attrs)
        image_input_size_hist.record(len(image_bytes), attrs)

    checksum = hashlib.sha256(output_bytes).hexdigest()[:16]
    q        = _last_qos

    log.info(
        f"✓ {token[:8]} | {duration*1000:.1f}ms | "
        f"QoS={q['composite']:.2f} (lat={q['latency']:.2f} cpu={q['cpu']:.2f} "
        f"retry={q['retry']:.2f} rej={q['rejection']:.2f}) | attempt={client_attempt}"
    )

    headers = {
        "X-Token":           token,
        "X-Processing-Time": str(round(duration, 6)),
        "X-Output-Bytes":    str(len(output_bytes)),
        "X-Checksum":        checksum,
        "X-Operations":      ",".join(op.get("name", "?") for op in ops),
        "X-Cached":          "false",
        "X-QoS-Composite":   str(q["composite"]),
        "X-QoS-Rejection":   str(q["rejection"]),
        "X-QoS-CPU":         str(q["cpu"]),
        "X-QoS-Latency":     str(q["latency"]),
        "X-QoS-Retry":       str(q["retry"]),
    }
    return Response(content=output_bytes, media_type="image/jpeg", headers=headers)



if __name__ == "__main__":
    log.info(f"Starting image-processor on {HOST}:{PORT}")
    log.info(f"  QoS latency baseline : {QOS_LATENCY_BASELINE}s")
    log.info(f"  QoS window           : {QOS_WINDOW_SECONDS}s")
    log.info(f"  OTEL endpoint        : {OTEL_ENDPOINT}")
    uvicorn.run(app, host=HOST, port=PORT)