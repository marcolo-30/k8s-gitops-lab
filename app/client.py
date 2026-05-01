#!/usr/bin/env python3
"""
client.py – Resilient image-processing client.

Sends X-Attempt header on every request so the server can track
retry counts as part of its QoS calculation.

OTEL metrics pushed from client side:
  job_total_time_seconds        – wall-clock per job (includes all retries)
  job_network_latency_seconds   – total_time - server_processing_time
  job_output_bytes              – output image size
  job_total_time_seconds_gauge  – last raw value
  job_network_latency_seconds_gauge – last raw value
"""

import os
import io
import time
import uuid
import base64
import hashlib
import argparse
import logging
import random
from pathlib import Path
from typing import Optional

import requests
from PIL import Image, ImageDraw

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SERVICE_ENDPOINT   = os.getenv("SERVICE_ENDPOINT", "http://localhost:8080")
OTEL_ENDPOINT      = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT",
                                "http://otel-collector.observability.svc.cluster.local:4318")
SERVICE_NAME       = os.getenv("SERVICE_NAME", "image-processor-client")
OUTPUT_DIR         = Path(os.getenv("OUTPUT_DIR", "./processed"))
MAX_RETRY_DELAY    = float(os.getenv("MAX_RETRY_DELAY", "30.0"))
REQUEST_TIMEOUT    = float(os.getenv("REQUEST_TIMEOUT", "60.0"))
BASE_RETRY_DELAY   = float(os.getenv("BASE_RETRY_DELAY", "1.0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("image-client")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# OpenTelemetry
# ---------------------------------------------------------------------------
_otel_ok = False
try:
    resource = Resource(attributes={"service.name": SERVICE_NAME})
    reader   = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"),
        export_interval_millis=2000,
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meter_provider)
    meter = metrics.get_meter("image-processor-client.meter")

    job_total_time_histogram     = meter.create_histogram("job_total_time_seconds",
        description="Wall-clock time from first attempt to success (includes retries).", unit="s")
    job_network_latency_histogram = meter.create_histogram("job_network_latency_seconds",
        description="Estimated network latency: total_time minus server processing_time.", unit="s")
    job_image_size_histogram      = meter.create_histogram("job_output_bytes",
        description="Output image size in bytes.", unit="By")

    _last_total_time      = 0.0
    _last_network_latency = 0.0

    def _total_cb(opts):
        yield metrics.Observation(_last_total_time, {})
    def _net_cb(opts):
        yield metrics.Observation(_last_network_latency, {})

    meter.create_observable_gauge("job_total_time_seconds_gauge",      [_total_cb])
    meter.create_observable_gauge("job_network_latency_seconds_gauge", [_net_cb])

    _otel_ok = True
    log.info(f"OpenTelemetry → {OTEL_ENDPOINT}")
except Exception as e:
    log.warning(f"OpenTelemetry unavailable ({e}) – metrics skipped.")
    job_total_time_histogram = job_network_latency_histogram = job_image_size_histogram = None

def _record(total_time, net_latency, output_bytes, attrs):
    if not _otel_ok:
        return
    global _last_total_time, _last_network_latency
    _last_total_time      = total_time
    _last_network_latency = net_latency
    job_total_time_histogram.record(total_time,     attrs)
    job_network_latency_histogram.record(net_latency, attrs)
    job_image_size_histogram.record(output_bytes,   attrs)

# ---------------------------------------------------------------------------
# Synthetic test image generator
# ---------------------------------------------------------------------------
def _generate_test_image(width: int = 1024, height: int = 768) -> bytes:
    img  = Image.new("RGB", (width, height))
    draw = ImageDraw.Draw(img)
    for y in range(height):
        r = int(255 * y / height)
        g = int(128 + 127 * (y / height))
        b = int(200 - 100 * (y / height))
        draw.line([(0, y), (width, y)], fill=(r, g, b))
    rng = random.Random()
    for _ in range(30):
        cx, cy, r = rng.randint(0, width), rng.randint(0, height), rng.randint(20, 150)
        color = (rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255))
        draw.ellipse([cx-r, cy-r, cx+r, cy+r], fill=color, outline=(255, 255, 255), width=2)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()

# ---------------------------------------------------------------------------
# Core job function
# ---------------------------------------------------------------------------
def process_image_job(
    image_path: Optional[str] = None,
    operations: Optional[list] = None,
    token:      Optional[str]  = None,
) -> dict:
    token      = token or str(uuid.uuid4())
    operations = operations or [{"name": "compress", "quality": 75}]
    url        = f"{SERVICE_ENDPOINT}/process"

    if image_path:
        with open(image_path, "rb") as f:
            image_bytes = f.read()
        log.info(f"[{token[:8]}] Loaded image: {image_path} ({len(image_bytes)//1024}KB)")
    else:
        image_bytes = _generate_test_image()
        log.info(f"[{token[:8]}] Generated synthetic test image ({len(image_bytes)//1024}KB)")

    image_b64          = base64.b64encode(image_bytes).decode()
    first_attempt_time = time.time()
    attempt            = 0
    retry_delay        = BASE_RETRY_DELAY

    log.info(f"\n{'='*60}")
    log.info(f"[{token[:8]}] Starting job | ops: {[o['name'] for o in operations]}")
    log.info(f"{'='*60}")

    while True:
        attempt += 1
        log.info(f"[{token[:8]}] → Attempt #{attempt}")

        try:
            resp = requests.post(
                url,
                json={
                    "token":        token,
                    "image_base64": image_b64,
                    "operations":   operations,
                },
                headers={
                    "X-Attempt": str(attempt),   # server uses this for QoS retry tracking
                },
                timeout=REQUEST_TIMEOUT,
            )

            # ── 200 OK ────────────────────────────────────────────────────
            if resp.status_code == 200:
                t2 = time.time()

                server_proc_time = float(resp.headers.get("X-Processing-Time", 0))
                returned_token   = resp.headers.get("X-Token", "")
                checksum         = resp.headers.get("X-Checksum", "")
                output_bytes_len = int(resp.headers.get("X-Output-Bytes", len(resp.content)))
                ops_applied      = resp.headers.get("X-Operations", "")
                cached           = resp.headers.get("X-Cached", "false") == "true"
                qos_composite    = float(resp.headers.get("X-QoS-Composite", 1.0))
                qos_rejection    = float(resp.headers.get("X-QoS-Rejection", 0.0))
                qos_cpu          = float(resp.headers.get("X-QoS-CPU", 1.0))
                qos_latency      = float(resp.headers.get("X-QoS-Latency", 1.0))
                qos_retry        = float(resp.headers.get("X-QoS-Retry", 1.0))

                if returned_token and returned_token != token:
                    log.warning(f"[{token[:8]}] Token mismatch – retrying")
                    time.sleep(retry_delay)
                    continue

                actual_checksum = hashlib.sha256(resp.content).hexdigest()[:16]
                if checksum and actual_checksum != checksum:
                    log.error(f"[{token[:8]}] Checksum mismatch – retrying")
                    time.sleep(retry_delay)
                    continue

                total_time  = t2 - first_attempt_time
                net_latency = max(0.0, total_time - server_proc_time)

                out_file = OUTPUT_DIR / f"{token[:8]}_{attempt}.jpg"
                out_file.write_bytes(resp.content)

                attrs = {"token": token, "attempts": str(attempt), "cached": str(cached)}
                _record(total_time, net_latency, output_bytes_len, attrs)

                result = {
                    "token":            token,
                    "attempts":         attempt,
                    "total_time":       round(total_time, 3),
                    "server_proc_time": round(server_proc_time, 3),
                    "network_latency":  round(net_latency, 3),
                    "output_bytes":     output_bytes_len,
                    "checksum":         checksum,
                    "cached":           cached,
                    "operations":       ops_applied,
                    "saved_to":         str(out_file),
                    "qos": {
                        "composite":     qos_composite,
                        "rejection_rate": qos_rejection,
                        "cpu_score":     qos_cpu,
                        "latency_score": qos_latency,
                        "retry_penalty": qos_retry,
                    },
                }

                log.info(f"[{token[:8]}] ✓ SUCCESS {'(cached)' if cached else ''}")
                log.info(f"  Attempts         : {attempt}")
                log.info(f"  Total time       : {total_time:.3f}s")
                log.info(f"  Server proc time : {server_proc_time:.3f}s")
                log.info(f"  Network latency  : {net_latency:.3f}s")
                log.info(f"  Output size      : {output_bytes_len // 1024}KB")
                log.info(f"  QoS composite    : {qos_composite:.2f} | rejection={qos_rejection:.2f} cpu={qos_cpu:.2f} latency={qos_latency:.2f} retry={qos_retry:.2f}")
                log.info(f"  Saved to         : {out_file}")
                return result

            # ── 503 overloaded ────────────────────────────────────────────
            elif resp.status_code == 503:
                retry_after = float(resp.headers.get("Retry-After", retry_delay))
                try:
                    body = resp.json()
                    cpu  = body.get("cpu_percent", "?")
                    qos  = body.get("qos", {}).get("composite", "?")
                except Exception:
                    cpu, qos = "?", "?"
                log.warning(f"[{token[:8]}] ← 503 overloaded (CPU={cpu}% QoS={qos}) – back off {retry_after:.1f}s")
                time.sleep(retry_after)
                continue

            # ── 429 queue full ────────────────────────────────────────────
            elif resp.status_code == 429:
                log.warning(f"[{token[:8]}] ← 429 Queue full – back off {retry_delay:.1f}s")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, MAX_RETRY_DELAY)
                continue

            # ── 202 in progress ───────────────────────────────────────────
            elif resp.status_code == 202:
                log.info(f"[{token[:8]}] ← 202 in progress – retry in {retry_delay:.1f}s")
                time.sleep(retry_delay)
                continue

            else:
                log.error(f"[{token[:8]}] ← HTTP {resp.status_code} – retry in {retry_delay:.1f}s")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
                continue

        except requests.exceptions.Timeout:
            log.warning(f"[{token[:8]}] ← Timeout after {REQUEST_TIMEOUT}s – retrying same token...")
        except requests.exceptions.ConnectionError as exc:
            log.warning(f"[{token[:8]}] ← Connection error: {exc} – retrying...")
        except requests.exceptions.RequestException as exc:
            log.error(f"[{token[:8]}] ← Request error: {exc} – retrying...")

        time.sleep(retry_delay)
        retry_delay = min(retry_delay * 1.5, MAX_RETRY_DELAY)

# ---------------------------------------------------------------------------
# Rotating demo pipelines
# ---------------------------------------------------------------------------
_PIPELINES = [
    [{"name": "resize", "width": 800, "height": 600}],
    [{"name": "grayscale"}, {"name": "sharpen", "factor": 2.5}],
    [{"name": "thumbnail", "size": 256}],
    [{"name": "blur", "radius": 3}, {"name": "compress", "quality": 60}],
    [{"name": "brightness", "factor": 1.4}, {"name": "contrast", "factor": 1.2}],
    [{"name": "rotate", "angle": 90}, {"name": "resize", "width": 512, "height": 512}],
    [{"name": "flip", "direction": "horizontal"}, {"name": "grayscale"}],
]

if __name__ == "__main__":
    import json as _json
    p = argparse.ArgumentParser()
    p.add_argument("--image",      help="Path to input image (default: synthetic)")
    p.add_argument("--ops",        help='JSON ops array e.g. \'[{"name":"resize","width":640}]\'')
    p.add_argument("--continuous", action="store_true", help="Run continuously with rotating pipelines")
    p.add_argument("--server",     default=SERVICE_ENDPOINT)
    args = p.parse_args()
    SERVICE_ENDPOINT = args.server

    log.info(f"[CLIENT] Image Processor Client")
    log.info(f"  Server : {SERVICE_ENDPOINT}")
    log.info(f"  Output : {OUTPUT_DIR}")
    log.info(f"  OTEL   : {OTEL_ENDPOINT} ({'ok' if _otel_ok else 'unavailable'})")

    if args.continuous:
        log.info("Running continuously – Ctrl+C to stop.\n")
        idx = 0
        while True:
            ops = _PIPELINES[idx % len(_PIPELINES)]
            idx += 1
            process_image_job(image_path=args.image, operations=ops)
            print("-" * 60, flush=True)
            time.sleep(1)
    else:
        ops = _json.loads(args.ops) if args.ops else None
        process_image_job(image_path=args.image, operations=ops)