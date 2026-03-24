# app/main.py
# Goal: ~50% CPU baseline, visible drop in iter/s when burner runs

import os
import time
import math
import threading
import psutil
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader, MetricExportResult
from opentelemetry.sdk.resources import Resource

SERVICE_NAME  = os.getenv("SERVICE_NAME", "main-app")
OTEL_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector.observability.svc.cluster.local:4318"
)
# The workload size to achieve a work/sleep balance
WORKLOAD_SIZE = int(os.getenv("WORKLOAD_SIZE", "5000"))
# The sleep duration to create a stable baseline
SLEEP_DURATION = float(os.getenv("SLEEP_DURATION", "0.05"))


print(f"[INFO]  [startup] service={SERVICE_NAME} workload_size={WORKLOAD_SIZE} sleep_duration={SLEEP_DURATION}s endpoint={OTEL_ENDPOINT}")

# --- OTEL setup ---
inner_exporter = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
_original_export = inner_exporter.export

def _logged_export(metrics_data, timeout_millis=10000, **kwargs):
    try:
        result = _original_export(metrics_data, timeout_millis=timeout_millis, **kwargs)
        if result == MetricExportResult.SUCCESS:
            print(f"[INFO]  [otel-export] Push OK -> {OTEL_ENDPOINT}/v1/metrics")
        else:
            print(f"[ERROR] [otel-export] Push FAILED -> {OTEL_ENDPOINT}/v1/metrics")
        return result
    except Exception as e:
        print(f"[ERROR] [otel-export] Push EXCEPTION -> {e}")
        return MetricExportResult.FAILURE

inner_exporter.export = _logged_export

resource       = Resource(attributes={"service.name": SERVICE_NAME})
reader         = PeriodicExportingMetricReader(inner_exporter, export_interval_millis=2000)
meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("main-app.meter")

# --- Metrics state ---
_current_cpu        = 0.0
_iterations_per_sec = 0.0
_task_duration_ms   = 0.0

def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME})

def get_iterations(options: CallbackOptions):
    yield Observation(_iterations_per_sec, {"service": SERVICE_NAME})

def get_task_duration(options: CallbackOptions):
    yield Observation(_task_duration_ms, {"service": SERVICE_NAME})

meter.create_observable_gauge(
    "main_app.cpu_percent",
    callbacks=[get_cpu],
    description="CPU usage percent of this pod process."
)
meter.create_observable_gauge(
    "main_app.iterations_per_sec",
    callbacks=[get_iterations],
    description="Work iterations per second — drops under CPU pressure."
)
meter.create_observable_gauge(
    "main_app.task_duration_ms",
    callbacks=[get_task_duration],
    description="Time in ms per work unit — rises under CPU pressure."
)

print("[INFO]  [startup] Metrics registered: cpu_percent, iterations_per_sec, task_duration_ms")

# --- CPU sampler thread ---
def track_cpu():
    global _current_cpu
    process = psutil.Process()
    while True:
        _current_cpu = process.cpu_percent(interval=1)

threading.Thread(target=track_cpu, daemon=True).start()

# --- Work loop ---
def work_loop():
    global _iterations_per_sec, _task_duration_ms

    iteration    = 0
    window_start = time.time()
    window_iters = 0

    print(f"[INFO]  [work-loop] Starting — workload_size={WORKLOAD_SIZE}, sleep_duration={SLEEP_DURATION}s")
    print(f"[INFO]  [work-loop] GOAL: Tune WORKLOAD_SIZE until task_ms is close to {SLEEP_DURATION * 1000:.0f}ms for a ~50% CPU baseline.")

    while True:
        # 1. WORK
        t0 = time.time()
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(WORKLOAD_SIZE))
        t1 = time.time()

        _task_duration_ms = (t1 - t0) * 1000
        
        # 2. SLEEP
        time.sleep(SLEEP_DURATION)

        iteration    += 1
        window_iters += 1

        elapsed = time.time() - window_start
        if elapsed >= 1.0:
            _iterations_per_sec = window_iters / elapsed
            cpu = _current_cpu

            if cpu < 40:
                level = "INFO "
            elif cpu < 75:
                level = "WARN "
            else:
                level = "ERROR"

            print(
                f"[{level}] [work-loop]"
                f"  iter/s={_iterations_per_sec:.1f}"
                f"  task_ms={_task_duration_ms:.2f}"
                f"  cpu={cpu:.1f}%"
                f"  total={iteration}"
            )

            window_start = time.time()
            window_iters = 0


if __name__ == "__main__":
    print(f"[INFO]  [main] Starting — service={SERVICE_NAME}")
    print(f"[INFO]  [main] psutil={psutil.__version__}")
    print(f"[INFO]  [main] Tune WORKLOAD_SIZE env var to make task_ms match sleep_duration for ~50% CPU.")
    work_loop()