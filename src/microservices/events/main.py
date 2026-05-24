import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [events-service] %(message)s",
)

logger = logging.getLogger("events-service")

app = FastAPI(title="CinemaAbyss Events Service")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

TOPICS = {
    "movie": "movie-events",
    "user": "user-events",
    "payment": "payment-events",
}

producer: KafkaProducer | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_event(event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "type": event_type,
        "timestamp": now_iso(),
        "payload": payload,
    }


def create_producer_with_retry() -> KafkaProducer:
    """
    Creates Kafka producer with retry because Docker depends_on starts containers,
    but does not guarantee that Kafka is already ready to accept connections.
    """
    last_error = None

    for attempt in range(1, 31):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8") if value else None,
                acks="all",
                retries=3,
            )

            logger.info("Kafka producer connected to %s", KAFKA_BROKERS)
            return kafka_producer

        except NoBrokersAvailable as error:
            last_error = error
            logger.warning(
                "Kafka broker is not ready yet. Attempt %s/30. Broker: %s",
                attempt,
                KAFKA_BROKERS,
            )
            time.sleep(2)

    raise RuntimeError(f"Could not connect Kafka producer to {KAFKA_BROKERS}: {last_error}")


def consume_topic(topic: str) -> None:
    """
    Background Kafka consumer.
    Reads events from one topic and writes processed events to service logs.
    """
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BROKERS,
                group_id=f"events-service-{topic}",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )

            logger.info("Kafka consumer subscribed to topic '%s'", topic)

            for message in consumer:
                logger.info(
                    "Processed event from topic=%s partition=%s offset=%s value=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                    message.value,
                )

        except Exception as error:
            logger.exception(
                "Kafka consumer for topic '%s' failed: %s. Restarting in 5 seconds.",
                topic,
                error,
            )
            time.sleep(5)


@app.on_event("startup")
def startup() -> None:
    global producer

    producer = create_producer_with_retry()

    for topic in TOPICS.values():
        thread = threading.Thread(
            target=consume_topic,
            args=(topic,),
            daemon=True,
        )
        thread.start()

    logger.info("Events service started. Kafka brokers: %s", KAFKA_BROKERS)


@app.on_event("shutdown")
def shutdown() -> None:
    global producer

    if producer is not None:
        producer.flush(timeout=5)
        producer.close(timeout=5)
        logger.info("Kafka producer closed")


@app.get("/api/events/health")
def health() -> Dict[str, bool]:
    return {"status": True}


def publish_event(event_type: str, payload: Dict[str, Any]) -> JSONResponse:
    global producer

    if event_type not in TOPICS:
        raise HTTPException(status_code=400, detail=f"Unsupported event type: {event_type}")

    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer is not initialized")

    topic = TOPICS[event_type]
    event = create_event(event_type=event_type, payload=payload)

    try:
        future = producer.send(
            topic,
            key=event["id"],
            value=event,
        )

        record_metadata = future.get(timeout=10)
        producer.flush(timeout=5)

        logger.info(
            "Published event id=%s type=%s topic=%s partition=%s offset=%s",
            event["id"],
            event_type,
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

        return JSONResponse(
            status_code=201,
            content={
                "status": "success",
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
                "event": event,
            },
        )

    except KafkaError as error:
        logger.exception("Failed to publish event to Kafka: %s", error)
        raise HTTPException(status_code=500, detail="Failed to publish event to Kafka")


@app.post("/api/events/movie")
async def create_movie_event(request: Request) -> JSONResponse:
    payload = await request.json()
    return publish_event("movie", payload)


@app.post("/api/events/user")
async def create_user_event(request: Request) -> JSONResponse:
    payload = await request.json()
    return publish_event("user", payload)


@app.post("/api/events/payment")
async def create_payment_event(request: Request) -> JSONResponse:
    payload = await request.json()
    return publish_event("payment", payload)