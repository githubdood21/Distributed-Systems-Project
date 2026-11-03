"""Worker configuration. Values can be overridden via environment variables."""
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TASKS_TOPIC = "ocr-tasks"
RESULTS_TOPIC = "ocr-results"
METRICS_TOPIC = "worker-metrics"
CONSUMER_GROUP = "ocr-workers"

# Unique id for this worker instance (used in metrics + results).
WORKER_ID = os.getenv("WORKER_ID", "worker-1")

# How often to publish system metrics, in seconds.
METRICS_INTERVAL_S = 5
