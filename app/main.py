# app/main.py
# Heavier work loop — sensitive to CPU throttling
# Removes CPU limit so pod competes freely for node CPU
# When burner runs: iterations_per_sec drops 40-60% visibly in Grafana

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
NODE_NAME     = os.getenv("NODE_NAME", "unknown")
NODE_COST     = float(os.getenv("NODE_COST", "1.0"))  # RPi=1.0, VM=2.5
OTEL_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector.observability.svc.cluster.local:4318"
)

# How long to sleep between iterations — controls CPU baseline
# 0.05 = ~30-40% CPU at rest, visible drop when burner kicks in
SLEEP_BETWEEN_ITERATIONS = float(os.getenv("SLEEP_BETWEEN_ITERATIONS", "0.05"))

print(f"[INFO]  [startup] service={SERVICE_NAME} node={NODE_NAME} cost={NODE_COST} endpoint={OTEL_ENDPOINT}")
print(f"[INFO]  [startup] sleep_between_iterations={SLEEP_BETWEEN_ITERATIONS}s")

# --- OTEL setup ---
inner_exporter = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
_original_export = inner_exporter.export

def _logged_export(metrics_data, timeout_millis=10000, **kwargs):
    try:
        result = _original_export(metrics_data, timeout_millis=timeout_millis, **kwargs)
        if result == MetricExportResult.SUCCESS:
            print(f"[INFO]  [otel-export] Push SUCCESS -> {OTEL_ENDPOINT}/v1/metrics")
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

# --- Observable gauges ---
def get_cpu(options: CallbackOptions):
    yield Observation(_current_cpu, {"service": SERVICE_NAME, "node": NODE_NAME})

def get_iterations(options: CallbackOptions):
    yield Observation(_iterations_per_sec, {"service": SERVICE_NAME, "node": NODE_NAME})

def get_task_duration(options: CallbackOptions):
    yield Observation(_task_duration_ms, {"service": SERVICE_NAME, "node": NODE_NAME})

def get_node_cost(options: CallbackOptions):
    yield Observation(NODE_COST, {"service": SERVICE_NAME, "node": NODE_NAME})

meter.create_observable_gauge(
    "main_app.cpu_percent",
    callbacks=[get_cpu],
    description="CPU usage percent of this pod."
)
meter.create_observable_gauge(
    "main_app.iterations_per_sec",
    callbacks=[get_iterations],
    description="Work iterations per second — drops under CPU pressure."
)
meter.create_observable_gauge(
    "main_app.task_duration_ms",
    callbacks=[get_task_duration],
    description="Time in ms to complete one work unit — rises under CPU pressure."
)
meter.create_observable_gauge(
    "main_app.node_cost",
    callbacks=[get_node_cost],
    description="Relative cost of running on this node (RPi=1.0, VM=2.5)."
)

print("[INFO]  [startup] Metrics registered: cpu_percent, iterations_per_sec, task_duration_ms, node_cost")

# --- CPU sampler thread ---
def track_cpu():
    global _current_cpu
    process = psutil.Process()
    while True:
        _current_cpu = process.cpu_percent(interval=1)

threading.Thread(target=track_cpu, daemon=True).start()

# --- Heavy work loop ---
# Uses sqrt + log so the CPU actually has to work for each iteration
# Sleep between iterations creates a stable baseline (~30-40% CPU)
# so the burner's impact is clearly visible in Grafana
def work_loop():
    global _iterations_per_sec, _task_duration_ms
    iteration    = 0
    window_start = time.time()
    window_iters = 0

    print("[INFO]  [work-loop] Starting heavy work loop")
    print("[INFO]  [work-loop] Each iteration: sum(sqrt(i) * log(i+1)) for 5,000 elements")
    print(f"[INFO]  [work-loop] Sleep between iterations: {SLEEP_BETWEEN_ITERATIONS}s")
    print(f"[INFO]  [work-loop] Expected baseline: ~{int(1/SLEEP_BETWEEN_ITERATIONS)} iter/s at low CPU")

    while True:
        # Heavy math
        t0 = time.time()
        _ = sum(math.sqrt(i) * math.log(i + 1) for i in range(500))
        t1 = time.time()

        _task_duration_ms = (t1 - t0) * 1000  # ms per work unit

        # 👇 controlled rest — this is what creates the observable baseline
        # When burner runs, the sleep gets skipped by the scheduler
        # so iter/s drops and task_ms rises — visible in Grafana
        time.sleep(SLEEP_BETWEEN_ITERATIONS)

        iteration    += 1
        window_iters += 1

        elapsed = time.time() - window_start
        if elapsed >= 1.0:
            _iterations_per_sec = window_iters / elapsed
            cpu = _current_cpu

            if cpu < 30:
                level = "INFO "
            elif cpu < 70:
                level = "WARN "
            else:
                level = "ERROR"

            print(
                f"[{level}] [work-loop] "
                f"iter/s={_iterations_per_sec:.1f}  "
                f"task_ms={_task_duration_ms:.2f}  "
                f"cpu={cpu:.1f}%  "
                f"node={NODE_NAME}  "
                f"cost={NODE_COST}  "
                f"total={iteration}"
            )

            if cpu > 70:
                print(f"[ERROR] [work-loop] CPU above 70% — burner is stealing resources!")
            elif cpu > 30:
                print(f"[WARN ] [work-loop] CPU above 30% — starting to feel pressure")

            window_start = time.time()
            window_iters = 0


if __name__ == "__main__":
    print(f"[INFO]  [main] Pod starting — service={SERVICE_NAME} node={NODE_NAME}")
    print(f"[INFO]  [main] psutil={psutil.__version__}")
    print(f"[INFO]  [main] NODE_COST={NODE_COST} (RPi=1.0 cheap, VM=2.5 expensive)")
    print("[INFO]  [main] Watch: iterations_per_sec DROP + task_duration_ms RISE under pressure")
    print("[INFO]  [main] To stress: kubectl run cpu-wave --image=python:3.12-slim ...")
    work_loop()