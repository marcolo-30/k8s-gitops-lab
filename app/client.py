#!/usr/bin/env python3
"""
client.py - Image-processor client with Prometheus/OTEL latency metrics.

Sends synthetic images to the FastAPI image-processor server in a continuous
loop, measures the end-to-end request latency and exports metrics to the
in-cluster OpenTelemetry Collector via OTLP/HTTP.

Exported metrics (all labeled with service=<SERVICE_NAME>):
  client_request_latency_seconds   (Histogram) - per-request latency
  client_request_duration_gauge    (Gauge)     - last request latency (live)
  client_requests_total            (Counter)   - total requests, label status=success|error
  client_request_errors_total      (Counter)   - failed requests, label reason=<HTTP code|exception>
  client_retries_total             (Counter)   - total retry attempts performed
  client_image_bytes               (Histogram) - input image size (bytes)
"""

import argparse
import base64
import io
import logging
import os
import random
import signal
import sys
import threading
import time
from typing import Optional

import requests
from PIL import Image, ImageDraw

# ── OpenTelemetry ──────────────────────────────────────────────────────────────
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

# ── Configuration ──────────────────────────────────────────────────────────────
SERVICE_ENDPOINT  = os.getenv("SERVICE_ENDPOINT",  "http://image-processor-naive-svc:8080")
OTEL_ENDPOINT     = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT",
                              "http://otel-collector.observability:4318")
SERVICE_NAME      = os.getenv("SERVICE_NAME",      "image-processor-client")
REQUEST_TIMEOUT   = float(os.getenv("REQUEST_TIMEOUT",   "60.0"))
MAX_RETRY_DELAY   = float(os.getenv("MAX_RETRY_DELAY",   "30.0"))
MAX_RETRIES       = int(os.getenv("MAX_RETRIES",         "5"))
REQUEST_INTERVAL  = float(os.getenv("REQUEST_INTERVAL",  "2.0"))
IMAGE_WIDTH       = int(os.getenv("IMAGE_WIDTH",         "800"))
IMAGE_HEIGHT      = int(os.getenv("IMAGE_HEIGHT",        "600"))
EXPORT_INTERVAL_MS = int(os.getenv("OTEL_EXPORT_INTERVAL_MS", "5000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("image-client")

# ── OTEL setup ─────────────────────────────────────────────────────────────────
_otel_ok = False
latency_hist = req_counter = err_counter = retry_counter = bytes_hist = None
_last_latency = {"value": 0.0}

try:
    resource = Resource(attributes={"service.name": SERVICE_NAME})
    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics"),
        export_interval_millis=EXPORT_INTERVAL_MS,
    )
    mp = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(mp)
    meter = metrics.get_meter("image-processor.client")

    latency_hist = meter.create_histogram(
        "client_request_latency_seconds",
        unit="s",
        description="End-to-end request latency observed by the client.",
    )
    bytes_hist = meter.create_histogram(
        "client_image_bytes",
        unit="By",
        description="Size in bytes of images sent by the client.",
    )
    req_counter = meter.create_counter(
        "client_requests_total",
        description="Total number of requests issued by the client.",
    )
    err_counter = meter.create_counter(
        "client_request_errors_total",
        description="Total number of failed requests.",
    )
    retry_counter = meter.create_counter(
        "client_retries_total",
        description="Total number of retry attempts performed.",
    )

    def _last_latency_cb(opts):
        yield metrics.Observation(_last_latency["value"], {"service": SERVICE_NAME})

    meter.create_observable_gauge(
        "client_request_duration_gauge",
        callbacks=[_last_latency_cb],
        unit="s",
        description="Latency of the most recent successful request (gauge).",
    )

    _otel_ok = True
    log.info(f"OpenTelemetry configured → {OTEL_ENDPOINT}")
except Exception as exc:
    log.warning(f"OpenTelemetry unavailable: {exc}")

# ── Helpers ────────────────────────────────────────────────────────────────────
_running = True

def _stop(*_):
    global _running
    log.info("Shutdown signal received, stopping after current request...")
    _running = False

signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def _generate_image() -> bytes:
    """Generate a simple synthetic image to send to the server."""
    img = Image.new(
        "RGB",
        (IMAGE_WIDTH, IMAGE_HEIGHT),
        color=(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)),
    )
    draw = ImageDraw.Draw(img)
    for _ in range(20):
        x0, y0 = random.randint(0, IMAGE_WIDTH), random.randint(0, IMAGE_HEIGHT)
        x1, y1 = random.randint(0, IMAGE_WIDTH), random.randint(0, IMAGE_HEIGHT)
        color = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        draw.rectangle([min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)],
                       outline=color, width=3)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def _build_payload() -> dict:
    image_bytes = _generate_image()
    if _otel_ok and bytes_hist is not None:
        bytes_hist.record(len(image_bytes), {"service": SERVICE_NAME})
    return {
        "token": f"client-{int(time.time() * 1000)}",
        "image_base64": base64.b64encode(image_bytes).decode("ascii"),
        "operations": [
            {"name": "blur",       "radius": 2.0},
            {"name": "grayscale"},
            {"name": "resize",     "width": 640, "height": 480},
            {"name": "compress",   "quality": 80},
        ],
    }


def _send_once(session: requests.Session, payload: dict) -> bool:
    """
    Send one logical request, retrying with exponential backoff up to MAX_RETRIES.
    Returns True on success, False otherwise.
    """
    url = f"{SERVICE_ENDPOINT.rstrip('/')}/process"
    attempt = 1
    delay = 1.0
    attrs = {"service": SERVICE_NAME}
    request_start = time.perf_counter()

    while _running and attempt <= MAX_RETRIES:
        headers = {"X-Attempt": str(attempt), "Content-Type": "application/json"}
        try:
            t0 = time.perf_counter()
            resp = session.post(url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
            elapsed = time.perf_counter() - t0
        except requests.exceptions.RequestException as exc:
            elapsed = time.perf_counter() - t0 if 't0' in locals() else 0.0
            log.warning(f"  attempt {attempt} failed: {exc.__class__.__name__}: {exc}")
            if _otel_ok:
                err_counter.add(1, {**attrs, "reason": exc.__class__.__name__})
                retry_counter.add(1, attrs)
            attempt += 1
            time.sleep(min(delay, MAX_RETRY_DELAY))
            delay *= 2
            continue

        if 200 <= resp.status_code < 300:
            total = time.perf_counter() - request_start
            _last_latency["value"] = total
            if _otel_ok:
                latency_hist.record(total, attrs)
                req_counter.add(1, {**attrs, "status": "success"})
            srv_qos = resp.headers.get("X-QoS-Composite", "?")
            srv_proc = resp.headers.get("X-Processing-Time", "?")
            log.info(
                f"✓ {payload['token'][:14]} | total={total*1000:.1f}ms "
                f"server={srv_proc}s | attempts={attempt} | server_qos={srv_qos}"
            )
            return True

        # HTTP error response
        log.warning(f"  attempt {attempt} HTTP {resp.status_code}: {resp.text[:120]}")
        if _otel_ok:
            err_counter.add(1, {**attrs, "reason": f"http_{resp.status_code}"})
        # 4xx (client error) → don't retry except 408/429
        if 400 <= resp.status_code < 500 and resp.status_code not in (408, 429):
            if _otel_ok:
                req_counter.add(1, {**attrs, "status": "error"})
            return False
        if _otel_ok:
            retry_counter.add(1, attrs)
        attempt += 1
        time.sleep(min(delay, MAX_RETRY_DELAY))
        delay *= 2

    if _otel_ok:
        req_counter.add(1, {**attrs, "status": "error"})
    log.error(f"✗ {payload.get('token', '?')[:14]} | exhausted {attempt-1} attempts")
    return False


def _wait_for_server(session: requests.Session) -> None:
    """Poll /health until the server is reachable."""
    url = f"{SERVICE_ENDPOINT.rstrip('/')}/health"
    backoff = 1.0
    while _running:
        try:
            r = session.get(url, timeout=5)
            if r.status_code == 200:
                log.info(f"Server is healthy → {url}")
                return
            log.info(f"Server health = {r.status_code}, waiting...")
        except requests.exceptions.RequestException as exc:
            log.info(f"Server not ready ({exc.__class__.__name__}), waiting...")
        time.sleep(min(backoff, 10.0))
        backoff *= 1.5


def run_continuous() -> None:
    log.info("=" * 60)
    log.info("Image-processor client starting")
    log.info(f"  Target endpoint  : {SERVICE_ENDPOINT}")
    log.info(f"  OTEL endpoint    : {OTEL_ENDPOINT}")
    log.info(f"  Request interval : {REQUEST_INTERVAL}s")
    log.info(f"  Image size       : {IMAGE_WIDTH}x{IMAGE_HEIGHT}")
    log.info(f"  Max retries      : {MAX_RETRIES}")
    log.info(f"  Request timeout  : {REQUEST_TIMEOUT}s")
    log.info("=" * 60)

    session = requests.Session()
    _wait_for_server(session)

    while _running:
        payload = _build_payload()
        _send_once(session, payload)
        # Sleep between requests (interruptible)
        slept = 0.0
        while _running and slept < REQUEST_INTERVAL:
            time.sleep(min(0.5, REQUEST_INTERVAL - slept))
            slept += 0.5

    log.info("Client stopped.")


def run_once() -> int:
    session = requests.Session()
    _wait_for_server(session)
    payload = _build_payload()
    ok = _send_once(session, payload)
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Image processor client")
    parser.add_argument("--continuous", action="store_true",
                        help="Run forever, sending requests every REQUEST_INTERVAL seconds.")
    args = parser.parse_args()

    if args.continuous:
        run_continuous()
        return 0
    return run_once()


if __name__ == "__main__":
    sys.exit(main())
