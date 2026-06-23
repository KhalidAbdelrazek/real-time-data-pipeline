"""
Kafka producer for the local retail analytics platform.

Run:
    python producer/producer.py --rate 50
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.config import (  # noqa: E402
    DEFAULT_BAD_RECORD_RATE,
    DEFAULT_DUPLICATE_RATE,
    DEFAULT_EVENTS_PER_SECOND,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    LOG_DIR,
    PRODUCER_LOG_FILE,
)
from .data_generator import RetailEventGenerator  # noqa: E402


def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(PRODUCER_LOG_FILE, encoding="utf-8"),
        ],
    )
    return logging.getLogger("retail_producer")


logger = configure_logging()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously stream retail events to Kafka.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--rate", type=float, default=DEFAULT_EVENTS_PER_SECOND)
    parser.add_argument("--bad-record-rate", type=float, default=DEFAULT_BAD_RECORD_RATE)
    parser.add_argument("--duplicate-rate", type=float, default=DEFAULT_DUPLICATE_RATE)
    return parser.parse_args()


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=10,
        retry_backoff_ms=500,
        linger_ms=25,
        request_timeout_ms=30000,
        max_in_flight_requests_per_connection=1,
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )


def main() -> None:
    args = parse_args()
    delay = 1.0 / args.rate if args.rate > 0 else 0.05
    generator = RetailEventGenerator(
        bad_record_rate=args.bad_record_rate,
        duplicate_rate=args.duplicate_rate,
    )

    logger.info("Connecting to Kafka at %s", args.bootstrap_servers)
    producer = build_producer(args.bootstrap_servers)
    logger.info("Streaming to topic '%s' at about %.2f events/sec", args.topic, args.rate)

    sent = 0
    try:
        while True:
            event = generator.generate_event()
            key = event.get("customer_id") or event.get("session_id") or event.get("event_id")
            try:
                future = producer.send(args.topic, key=key, value=event)
                future.get(timeout=10)
                sent += 1
                if sent % 100 == 0:
                    logger.info(
                        "Sent %d events. Latest type=%s timestamp=%s",
                        sent,
                        event.get("event_type"),
                        event.get("event_timestamp"),
                    )
            except KafkaError:
                logger.exception("Kafka delivery failed. The producer will retry on the next event.")
            time.sleep(delay)
    except KeyboardInterrupt:
        logger.info("Graceful shutdown requested.")
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
        logger.info("Producer closed after sending %d events.", sent)


if __name__ == "__main__":
    main()
