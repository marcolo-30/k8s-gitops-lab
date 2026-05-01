#!/usr/bin/env python3
"""
server.py – CPU-aware image processing server with QoS metrics.

QoS signals pushed to OTEL (all as gauges, 0.0→1.0 or raw):
  qos_rejection_rate      – 503s / total requests in last 60s window
  qos_retry_penalty       – 1.0 - min(avg_retries/5, 1.0)
  qos_latency_score       – 1.0 - min(avg_proc_time/BASELINE, 1.0)
  qos_cpu_score           – 1.0 - (cpu% / 100)
  qos_composite           – weighted combination of all four
  qos_503_count           – raw counter of 503s in last 60s
  qos_success_count       – raw counter of successes in last 60s
  qos_avg_proc_time       – raw avg processing time in last 60s
  qos_avg_retries         – raw avg retry count per job in last 60s
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HOST                 = os.getenv("HOST", "0.0.0.0")
PORT                 = int(os.getenv("PORT", "8080"))
CPU_LOW_THRESH       = float(os.getenv("CPU_LOW_THRESH", "50.0"))
CPU_HIGH_THRESH      = float(os.getenv("CPU_HIGH_THRESH", "80.0"))
CPU_POLL_SEC         = float(os.getenv("CPU_POLL_SEC", "1.0"))
MAX_IMAGE_MB         = float(os.getenv("MAX_IMAGE_MB", "20.0"))
MAX_QUEUE_DEPTH      = int(os.getenv("MAX_QUEUE_DEPTH", "32"))
OTEL_ENDPOINT        = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT",
                                  "http://otel-collector.observability.svc.cluster.local:4318")
SERVICE_NAME         = os.getenv("SERVICE_NAME", "image-processor-server")
QOS_WINDOW_SEC       = float(os.getenv("QOS_WINDOW_SEC", "60.0"))
QOS_LATENCY_BASELINE = float(os.getenv("QOS_LATENCY_BASELINE", "2.0"))  # "good" proc time in seconds
CPU_COUNT            = psutil.cpu_count(logical=True) or 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("image-processor")

# ---------------------------------------------------------------------------
# OpenTelemetry
# ---------------------------------------------------------------------------
_otel_ok = False
try:
    resource = Resource(attributes={"service.name": SERVICE_NAME})
    reader   = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"),
        export_interval_millis=5000,
    )
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(mp)
    meter    = metrics.get_meter("image-processor-server.meter")
    _otel_ok = True
    log.info(f"OpenTelemetry → {OTEL_ENDPOINT}")
except Exception as e:
    log.warning(f"OpenTelemetry unavailable: {e}")

# ---------------------------------------------------------------------------
# CPU state
# ---------------------------------------------------------------------------
_cpu_lock  = threading.Lock()
_cpu_state = {"percent": 0.0, "mode": "full", "workers": CPU_COUNT, "updated_at": time.time()}

def _cpu_mode(pct):
    if pct < CPU_LOW_THRESH:  return "full", CPU_COUNT
    if pct < CPU_HIGH_THRESH: return "throttled", max(1, CPU_COUNT // 2)
    return "overloaded", 0

def _cpu_poller():
    while True:
        pct = psutil.cpu_percent(interval=CPU_POLL_SEC)
        mode, workers = _cpu_mode(pct)
        with _cpu_lock:
            _cpu_state.update({"percent": pct, "mode": mode,
                               "workers": workers, "updated_at": time.time()})

threading.Thread(target=_cpu_poller, daemon=True, name="cpu-poller").start()

def _get_cpu():
    with _cpu_lock:
        return dict(_cpu_state)

# ---------------------------------------------------------------------------
# QoS event log – rolling window
# ---------------------------------------------------------------------------
_qos_lock   = threading.Lock()
_qos_events = deque()
# Each event: {"ts": float, "type": "success"|"rejected"|"error",
#              "proc_time": float, "retries": int}

def _qos_append(event_type: str, proc_time: float = 0.0, retries: int = 1):
    with _qos_lock:
        _qos_events.append({
            "ts": time.time(), "type": event_type,
            "proc_time": proc_time, "retries": retries,
        })

def _qos_compute() -> dict:
    now    = time.time()
    cutoff = now - QOS_WINDOW_SEC
    with _qos_lock:
        while _qos_events and _qos_events[0]["ts"] < cutoff:
            _qos_events.popleft()
        window = list(_qos_events)

    cpu_pct   = _cpu_state["percent"]
    cpu_score = max(0.0, 1.0 - (cpu_pct / 100.0))

    if not window:
        return {
            "rejection_rate": 0.0, "retry_penalty": 1.0,
            "latency_score":  1.0, "cpu_score": cpu_score,
            "composite": cpu_score,
            "count_503": 0, "count_success": 0,
            "avg_proc_time": 0.0, "avg_retries": 0.0,
        }

    total      = len(window)
    rejected   = sum(1 for e in window if e["type"] == "rejected")
    successes  = [e for e in window if e["type"] == "success"]

    # 1. Rejection rate (0=none rejected, 1=all rejected)
    rejection_rate = rejected / total

    # 2. Retry penalty: 1 retry=1.0, 5+ retries=0.0
    avg_retries   = (sum(e["retries"] for e in successes) / len(successes)) if successes else 5.0
    retry_penalty = max(0.0, 1.0 - ((avg_retries - 1) / 4.0))

    # 3. Latency score vs baseline
    avg_proc_time = (sum(e["proc_time"] for e in successes) / len(successes)) if successes else QOS_LATENCY_BASELINE * 2
    latency_score = max(0.0, 1.0 - (avg_proc_time / (QOS_LATENCY_BASELINE * 2)))

    # 4. Composite weighted
    composite = (
        (1.0 - rejection_rate) * 0.40 +
        retry_penalty          * 0.25 +
        latency_score          * 0.20 +
        cpu_score              * 0.15
    )

    return {
        "rejection_rate": round(rejection_rate, 4),
        "retry_penalty":  round(retry_penalty,  4),
        "latency_score":  round(latency_score,  4),
        "cpu_score":      round(cpu_score,       4),
        "composite":      round(composite,       4),
        "count_503":      rejected,
        "count_success":  len(successes),
        "avg_proc_time":  round(avg_proc_time,   4),
        "avg_retries":    round(avg_retries,     4),
    }

# ---------------------------------------------------------------------------
# QoS background poller + OTEL gauges
# ---------------------------------------------------------------------------
_last_qos = {
    "rejection_rate": 0.0, "retry_penalty": 1.0, "latency_score": 1.0,
    "cpu_score": 1.0, "composite": 1.0, "count_503": 0, "count_success": 0,
    "avg_proc_time": 0.0, "avg_retries": 0.0,
}

def _qos_poller():
    global _last_qos
    while True:
        _last_qos = _qos_compute()
        time.sleep(5)

threading.Thread(target=_qos_poller, daemon=True, name="qos-poller").start()

if _otel_ok:
    def _cb(key):
        def _inner(opts):
            yield metrics.Observation(_last_qos[key], {"service": SERVICE_NAME})
        return _inner

    # Individual QoS signals – your policies can pick any of these
    meter.create_observable_gauge(
        "qos_rejection_rate", [_cb("rejection_rate")],
        description=f"Fraction of requests rejected (503) in last {QOS_WINDOW_SEC}s. 0=none rejected, 1=all rejected.")
    meter.create_observable_gauge(
        "qos_retry_penalty", [_cb("retry_penalty")],
        description="QoS score from retry count. 1.0=always first-try success, 0.0=5+ retries per job.")
    meter.create_observable_gauge(
        "qos_latency_score", [_cb("latency_score")],
        description=f"QoS score from avg processing time vs {QOS_LATENCY_BASELINE}s baseline. 1.0=fast, 0.0=very slow.")
    meter.create_observable_gauge(
        "qos_cpu_score", [_cb("cpu_score")],
        description="QoS score from CPU usage. 1.0=idle, 0.0=100% CPU.")
    meter.create_observable_gauge(
        "qos_composite", [_cb("composite")],
        description="Weighted composite QoS (0.0=critical/migrate, 1.0=healthy). rejection=40% retry=25% latency=20% cpu=15%.")
    # Raw counters
    meter.create_observable_gauge(
        "qos_503_count", [_cb("count_503")],
        description=f"Raw count of 503 rejections in last {QOS_WINDOW_SEC}s.")
    meter.create_observable_gauge(
        "qos_success_count", [_cb("count_success")],
        description=f"Raw count of successful jobs in last {QOS_WINDOW_SEC}s.")
    meter.create_observable_gauge(
        "qos_avg_proc_time", [_cb("avg_proc_time")],
        description=f"Avg processing time (seconds) of successful jobs in last {QOS_WINDOW_SEC}s.")
    meter.create_observable_gauge(
        "qos_avg_retries", [_cb("avg_retries")],
        description=f"Avg retry count per successful job in last {QOS_WINDOW_SEC}s.")

# ---------------------------------------------------------------------------
# Job registry
# ---------------------------------------------------------------------------
_job_lock     = threading.Lock()
_job_registry = {}
_recent_jobs  = deque(maxlen=50)
_queue_depth  = 0
_queue_lock   = threading.Lock()

# ---------------------------------------------------------------------------
# Image processing pipeline
# ---------------------------------------------------------------------------
def _apply_operations(img, ops):
    for op in ops:
        name = op.get("name", "").lower()
        if name == "resize":
            img = img.resize((int(op.get("width", img.width)),
                              int(op.get("height", img.height))), Image.LANCZOS)
        elif name == "thumbnail":
            img.thumbnail((int(op.get("size", 256)),) * 2, Image.LANCZOS)
        elif name == "grayscale":
            img = ImageOps.grayscale(img).convert("RGB")
        elif name == "blur":
            img = img.filter(ImageFilter.GaussianBlur(radius=float(op.get("radius", 2.0))))
        elif name == "sharpen":
            img = ImageEnhance.Sharpness(img).enhance(float(op.get("factor", 2.0)))
        elif name == "brightness":
            img = ImageEnhance.Brightness(img).enhance(float(op.get("factor", 1.2)))
        elif name == "contrast":
            img = ImageEnhance.Contrast(img).enhance(float(op.get("factor", 1.2)))
        elif name == "flip":
            img = (ImageOps.mirror(img) if op.get("direction", "horizontal") == "horizontal"
                   else ImageOps.flip(img))
        elif name == "rotate":
            img = img.rotate(float(op.get("angle", 90)), expand=bool(op.get("expand", True)))
        elif name == "compress":
            pass
        else:
            log.warning(f"Unknown op '{name}' – skipped.")
    return img

def _encode_image(img, ops, fmt="JPEG"):
    quality = next((int(op.get("quality", 60))
                    for op in ops if op.get("name", "").lower() == "compress"), 85)
    buf = io.BytesIO()
    img.save(buf, format=fmt, quality=quality, optimize=True)
    return buf.getvalue()

def _process_image_bytes(image_bytes, ops):
    t0  = time.perf_counter()
    img = Image.open(io.BytesIO(image_bytes))
    original_size, original_mode = img.size, img.mode
    if img.mode == "RGBA":
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[3])
        img = bg
    elif img.mode not in ("RGB", "L"):
        img = img.convert("RGB")
    img          = _apply_operations(img, ops)
    output_bytes = _encode_image(img, ops)
    elapsed      = time.perf_counter() - t0
    checksum     = hashlib.sha256(output_bytes).hexdigest()[:16]
    return output_bytes, {
        "processing_time_seconds": round(elapsed, 4),
        "original_size": original_size, "output_size": img.size,
        "original_mode": original_mode, "output_bytes": len(output_bytes),
        "checksum": checksum,
        "operations_applied": [op.get("name") for op in ops],
    }

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = int(MAX_IMAGE_MB * 1024 * 1024)

@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/qos")
def qos_endpoint():
    """All QoS signals as JSON – orchestrator can poll this to decide migration."""
    q   = _qos_compute()
    cpu = _get_cpu()
    q.update({
        "cpu_percent":    cpu["percent"],
        "cpu_mode":       cpu["mode"],
        "window_seconds": QOS_WINDOW_SEC,
        "migrate_now":    q["composite"] < 0.3,   # convenience flag
    })
    return jsonify(q)

@app.route("/status")
def status():
    cpu = _get_cpu()
    with _queue_lock:
        qd = _queue_depth
    return jsonify({
        "cpu_percent": cpu["percent"], "cpu_mode": cpu["mode"],
        "active_workers": cpu["workers"], "max_workers": CPU_COUNT,
        "queue_depth": qd,
        "qos": _last_qos,
        "recent_jobs": list(_recent_jobs),
    })

@app.route("/process", methods=["POST"])
def process():
    global _queue_depth
    token = (request.form.get("token") or
             (request.get_json(silent=True) or {}).get("token") or
             str(uuid.uuid4()))

    # Idempotency
    with _job_lock:
        if token in _job_registry:
            entry = _job_registry[token]
            if entry["status"] == "done":
                return _build_response(entry, token, cached=True)
            if entry["status"] == "processing":
                return jsonify({"error": "Job in progress", "token": token}), 202

    cpu = _get_cpu()

    # CPU gate
    if cpu["mode"] == "overloaded":
        _qos_append("rejected")
        log.warning(f"[{token[:8]}] CPU={cpu['percent']:.1f}% – shedding load (503)")
        return jsonify({
            "error": "Server overloaded", "cpu_percent": cpu["percent"],
            "token": token, "retry_after": 5, "qos": _last_qos,
        }), 503, {"Retry-After": "5"}

    with _queue_lock:
        if _queue_depth >= MAX_QUEUE_DEPTH:
            _qos_append("rejected")
            return jsonify({"error": "Queue full", "token": token}), 429
        _queue_depth += 1

    # Parse image
    image_bytes = None
    if "image" in request.files:
        image_bytes = request.files["image"].read()
    elif request.is_json:
        b64 = (request.get_json() or {}).get("image_base64")
        if b64:
            try:
                image_bytes = base64.b64decode(b64)
            except Exception:
                with _queue_lock: _queue_depth = max(0, _queue_depth - 1)
                return jsonify({"error": "Invalid base64"}), 400

    if not image_bytes:
        with _queue_lock: _queue_depth = max(0, _queue_depth - 1)
        return jsonify({"error": "No image provided"}), 400

    # Parse operations
    if request.content_type and "multipart" in request.content_type:
        import json as _j
        ops = _j.loads(request.form.get("operations", "[]"))
    else:
        ops = (request.get_json(silent=True) or {}).get("operations", [])
    if not ops:
        ops = [{"name": "compress", "quality": 85}]

    # Client sends attempt number in header so server can track retries in QoS
    client_attempt = int(request.headers.get("X-Attempt", 1))

    with _job_lock:
        _job_registry[token] = {"status": "processing", "started_at": time.time()}

    if cpu["mode"] == "throttled":
        delay = 0.5 + (cpu["percent"] - CPU_LOW_THRESH) / (CPU_HIGH_THRESH - CPU_LOW_THRESH) * 2.0
        log.info(f"[{token[:8]}] Throttled ({cpu['percent']:.1f}%) → +{delay:.2f}s delay")
        time.sleep(delay)

    try:
        output_bytes, stats = _process_image_bytes(image_bytes, ops)
    except Exception as exc:
        log.error(f"[{token[:8]}] Failed: {exc}")
        _qos_append("error", retries=client_attempt)
        with _job_lock: _job_registry[token] = {"status": "error", "error": str(exc)}
        with _queue_lock: _queue_depth = max(0, _queue_depth - 1)
        return jsonify({"error": str(exc), "token": token}), 500
    finally:
        with _queue_lock: _queue_depth = max(0, _queue_depth - 1)

    _qos_append("success", proc_time=stats["processing_time_seconds"], retries=client_attempt)

    entry = {
        "status": "done", "token": token, "stats": stats,
        "output": output_bytes, "cpu_at_submit": cpu["percent"],
        "completed_at": time.time(),
    }
    with _job_lock:
        _job_registry[token] = entry

    _recent_jobs.appendleft({
        "token": token[:8], "operations": stats["operations_applied"],
        "proc_time": stats["processing_time_seconds"],
        "output_kb": round(stats["output_bytes"] / 1024, 1),
        "cpu_at_submit": round(cpu["percent"], 1),
        "completed_at": entry["completed_at"],
        "retries": client_attempt,
        "qos_composite": _last_qos["composite"],
    })

    log.info(
        f"[{token[:8]}] ✓ {stats['processing_time_seconds']:.3f}s | "
        f"CPU={cpu['percent']:.1f}% | QoS={_last_qos['composite']:.2f} | attempt={client_attempt}"
    )
    return _build_response(entry, token, cached=False)

def _build_response(entry, token, cached):
    stats = entry["stats"]
    buf   = io.BytesIO(entry["output"])
    buf.seek(0)
    resp  = send_file(buf, mimetype="image/jpeg")
    resp.headers.update({
        "X-Token":           token,
        "X-Cached":          str(cached).lower(),
        "X-Processing-Time": str(stats["processing_time_seconds"]),
        "X-Checksum":        stats["checksum"],
        "X-Output-Bytes":    str(stats["output_bytes"]),
        "X-Operations":      ",".join(str(o) for o in stats["operations_applied"]),
        "X-QoS-Composite":   str(_last_qos["composite"]),
        "X-QoS-Rejection":   str(_last_qos["rejection_rate"]),
        "X-QoS-CPU":         str(_last_qos["cpu_score"]),
        "X-QoS-Latency":     str(_last_qos["latency_score"]),
        "X-QoS-Retry":       str(_last_qos["retry_penalty"]),
    })
    return resp

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Image Processor – Live Dashboard</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Syne:wght@700;800&display=swap');
  :root{--bg:#0a0a0f;--surface:#13131a;--border:#22223a;--green:#00ff88;--yellow:#ffcc00;--red:#ff4466;--text:#e0e0f0;--muted:#666688}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'JetBrains Mono',monospace;min-height:100vh;padding:24px}
  h1{font-family:'Syne',sans-serif;font-size:2rem;letter-spacing:-.03em;color:var(--green);margin-bottom:4px}
  .subtitle{color:var(--muted);font-size:.75rem;margin-bottom:24px}
  .grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px}
  .card-label{font-size:.6rem;text-transform:uppercase;letter-spacing:.12em;color:var(--muted);margin-bottom:6px}
  .card-value{font-size:1.8rem;font-weight:700;line-height:1}
  .bar-wrap{background:#1a1a2a;border-radius:4px;height:6px;margin-top:10px;overflow:hidden}
  .bar{height:100%;border-radius:4px;transition:width .5s ease,background .3s}
  .mode-badge{display:inline-block;padding:2px 8px;border-radius:20px;font-size:.65rem;font-weight:700;text-transform:uppercase;margin-top:8px}
  .badge-full{background:#00ff8822;color:var(--green);border:1px solid var(--green)}
  .badge-throttled{background:#ffcc0022;color:var(--yellow);border:1px solid var(--yellow)}
  .badge-overloaded{background:#ff446622;color:var(--red);border:1px solid var(--red)}
  .section-title{color:var(--muted);font-size:.6rem;text-transform:uppercase;letter-spacing:.12em;margin:16px 0 8px}
  table{width:100%;border-collapse:collapse;font-size:.75rem}
  th{color:var(--muted);font-weight:400;text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);font-size:.6rem;text-transform:uppercase;letter-spacing:.08em}
  td{padding:7px 8px;border-bottom:1px solid #1a1a28}
  tr:hover td{background:#16162a}
  .op-tag{background:#1e1e3a;border:1px solid #33336a;border-radius:4px;padding:1px 5px;font-size:.6rem;margin-right:2px}
  #dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:8px;background:var(--green);animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  .migrate-alert{background:#ff446611;border:1px solid var(--red);border-radius:8px;padding:10px 16px;color:var(--red);font-weight:700;margin-bottom:16px;display:none}
</style>
</head>
<body>
<h1>⚡ Image Processor</h1>
<p class="subtitle"><span id="dot"></span>Live · refresh 2s · QoS window 60s</p>
<div class="migrate-alert" id="migrate-alert">⚠ MIGRATE NOW — QoS composite below 0.3</div>

<div class="grid">
  <div class="card" id="cpu-card">
    <div class="card-label">CPU</div>
    <div class="card-value" id="cpu-val">–</div>
    <div class="bar-wrap"><div class="bar" id="cpu-bar"></div></div>
    <span class="mode-badge" id="mode-badge">–</span>
  </div>
  <div class="card">
    <div class="card-label">Workers</div>
    <div class="card-value" id="workers-val">–</div>
    <div style="color:var(--muted);font-size:.7rem;margin-top:6px" id="workers-sub"></div>
  </div>
  <div class="card">
    <div class="card-label">Queue</div>
    <div class="card-value" id="queue-val">–</div>
    <div style="color:var(--muted);font-size:.7rem;margin-top:6px">max 32</div>
  </div>
  <div class="card">
    <div class="card-label">QoS Composite ⚖</div>
    <div class="card-value" id="qos-composite">–</div>
    <div class="bar-wrap"><div class="bar" id="qos-bar"></div></div>
    <div style="color:var(--muted);font-size:.6rem;margin-top:6px">rejection×0.4 + retry×0.25 + latency×0.2 + cpu×0.15</div>
  </div>
</div>

<p class="section-title">QoS Signals — each usable independently by your migration policies</p>
<div class="grid">
  <div class="card">
    <div class="card-label">Rejection Rate (503s)</div>
    <div class="card-value" id="qos-rejection">–</div>
    <div style="color:var(--muted);font-size:.65rem;margin-top:4px">
      <span id="cnt-503">–</span> 503s / <span id="cnt-total">–</span> reqs in 60s
    </div>
  </div>
  <div class="card">
    <div class="card-label">Retry Penalty</div>
    <div class="card-value" id="qos-retry">–</div>
    <div style="color:var(--muted);font-size:.65rem;margin-top:4px">avg <span id="avg-retry">–</span> retries/job</div>
  </div>
  <div class="card">
    <div class="card-label">Latency Score</div>
    <div class="card-value" id="qos-latency">–</div>
    <div style="color:var(--muted);font-size:.65rem;margin-top:4px">avg <span id="avg-proc">–</span>s proc time</div>
  </div>
  <div class="card">
    <div class="card-label">CPU Score</div>
    <div class="card-value" id="qos-cpu">–</div>
    <div style="color:var(--muted);font-size:.65rem;margin-top:4px">✅ <span id="cnt-ok">–</span> jobs succeeded</div>
  </div>
</div>

<div class="card" style="margin-bottom:16px">
  <div class="card-label" style="margin-bottom:12px">Recent Jobs</div>
  <table>
    <thead>
      <tr>
        <th>Token</th><th>Operations</th><th>Proc Time</th>
        <th>Output KB</th><th>Attempt #</th><th>CPU</th><th>QoS</th><th>Time</th>
      </tr>
    </thead>
    <tbody id="jobs-body">
      <tr><td colspan="8" style="color:var(--muted);text-align:center;padding:20px">No jobs yet</td></tr>
    </tbody>
  </table>
</div>

<script>
function qosColor(v){return v>=0.7?'#00ff88':v>=0.4?'#ffcc00':'#ff4466'}
function pct(v){return (v*100).toFixed(0)+'%'}

async function refresh(){
  try{
    const r=await fetch('/status'),d=await r.json();
    const cpu=d.cpu_percent;
    document.getElementById('cpu-val').textContent=cpu.toFixed(1)+'%';
    const cpuBar=document.getElementById('cpu-bar');
    cpuBar.style.width=Math.min(cpu,100)+'%';
    cpuBar.style.background=cpu<50?'#00ff88':cpu<80?'#ffcc00':'#ff4466';
    document.getElementById('mode-badge').textContent=d.cpu_mode;
    document.getElementById('mode-badge').className='mode-badge badge-'+d.cpu_mode;
    document.getElementById('workers-val').textContent=d.active_workers;
    document.getElementById('workers-sub').textContent='of '+d.max_workers+' max';
    document.getElementById('queue-val').textContent=d.queue_depth;

    const q=d.qos||{};
    const comp=q.composite??1;

    // Composite
    const cv=document.getElementById('qos-composite');
    cv.textContent=pct(comp); cv.style.color=qosColor(comp);
    const qb=document.getElementById('qos-bar');
    qb.style.width=(comp*100)+'%'; qb.style.background=qosColor(comp);

    // Migrate alert
    document.getElementById('migrate-alert').style.display=comp<0.3?'block':'none';

    // Individual signals
    const rr=1-(q.rejection_rate??0);
    const rej=document.getElementById('qos-rejection');
    rej.textContent=pct(rr); rej.style.color=qosColor(rr);

    const rp=document.getElementById('qos-retry');
    rp.textContent=pct(q.retry_penalty??1); rp.style.color=qosColor(q.retry_penalty??1);

    const ls=document.getElementById('qos-latency');
    ls.textContent=pct(q.latency_score??1); ls.style.color=qosColor(q.latency_score??1);

    const cs=document.getElementById('qos-cpu');
    cs.textContent=pct(q.cpu_score??1); cs.style.color=qosColor(q.cpu_score??1);

    document.getElementById('cnt-503').textContent=q.count_503??0;
    document.getElementById('cnt-total').textContent=(q.count_503??0)+(q.count_success??0);
    document.getElementById('cnt-ok').textContent=q.count_success??0;
    document.getElementById('avg-proc').textContent=(q.avg_proc_time??0).toFixed(3);
    document.getElementById('avg-retry').textContent=(q.avg_retries??1).toFixed(1);

    const tbody=document.getElementById('jobs-body');
    if(d.recent_jobs&&d.recent_jobs.length>0){
      tbody.innerHTML=d.recent_jobs.map(j=>`
        <tr>
          <td style="color:#8888cc;font-family:monospace">${j.token}</td>
          <td>${(j.operations||[]).map(o=>`<span class="op-tag">${o}</span>`).join('')}</td>
          <td>${(j.proc_time*1000).toFixed(0)}ms</td>
          <td>${j.output_kb}</td>
          <td style="color:${(j.retries||1)>1?'#ffcc00':'#00ff88'}">#${j.retries||1}</td>
          <td style="color:${j.cpu_at_submit<50?'#00ff88':j.cpu_at_submit<80?'#ffcc00':'#ff4466'}">${j.cpu_at_submit}%</td>
          <td style="color:${qosColor(j.qos_composite??1)}">${((j.qos_composite??1)*100).toFixed(0)}%</td>
          <td style="color:var(--muted)">${new Date(j.completed_at*1000).toLocaleTimeString()}</td>
        </tr>`).join('');
    }
  }catch(e){console.error(e)}
}
refresh(); setInterval(refresh,2000);
</script>
</body>
</html>"""

@app.route("/dashboard")
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html")

if __name__ == "__main__":
    log.info(f"Starting on {HOST}:{PORT} | cores={CPU_COUNT} | Low={CPU_LOW_THRESH}% High={CPU_HIGH_THRESH}%")
    log.info(f"QoS window={QOS_WINDOW_SEC}s | latency baseline={QOS_LATENCY_BASELINE}s | OTEL={'ok' if _otel_ok else 'unavailable'}")
    app.run(host=HOST, port=PORT, threaded=True, debug=False)