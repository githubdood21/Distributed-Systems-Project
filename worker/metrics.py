"""System usage reporting (CPU / RAM). Published to the `worker-metrics` topic."""
import psutil


def snapshot() -> dict:
    """Return a small dict of current system usage for this worker's host."""
    return {
        "cpu_percent": psutil.cpu_percent(interval=None),
        "ram_percent": psutil.virtual_memory().percent,
    }
