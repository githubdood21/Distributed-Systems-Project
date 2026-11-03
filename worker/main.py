"""OCR worker entry point.

Stub — connect to Kafka and poll for tasks. Fill in the TODOs in later phases.
"""
import logging
import confluent_kafka
import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("worker")


def main() -> None:
    consumer = confluent_kafka.Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([config.TASKS_TOPIC])
    log.info("Worker %s subscribed to %s", config.WORKER_ID, config.TASKS_TOPIC)

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            log.warning("Consumer error: %s", msg.error())
            continue
        # TODO: decode task, run OCR engine, publish result to ocr-results.


if __name__ == "__main__":
    main()
