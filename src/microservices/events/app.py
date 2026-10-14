import json
import logging
import os
import threading
from typing import Any, Dict

from flask import Flask, request, jsonify
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "http://localhost:9092")
KAFKA_USER_TOPIC = "user-events"
KAFKA_MOVIE_TOPIC = "movie-events"
KAFKA_PAYMENT_TOPIC = "payment-events"
KAFKA_GROUP_ID = "event-logger-group"

PORT = int(os.getenv("PORT", "8082"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
})


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def send_to_kafka(event: Dict[str, Any], topic: str) -> None:
    data = json.dumps(event).encode("utf-8")
    try:
        producer.produce(
            topic=topic,
            value=data,
            callback=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        logger.exception(f"Error producing message: {e}")
        raise


def kafka_consumer_loop() -> None:
    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_MOVIE_TOPIC, KAFKA_PAYMENT_TOPIC, KAFKA_USER_TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
                logger.info(f"Received event from Kafka: {event}")
            except Exception as e:
                logger.exception(f"Failed to parse message from Kafka: {e}")
    finally:
        consumer.close()


consumer_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
consumer_thread.start()


@app.route("/api/events/health", methods=["GET"])
def health():
    return jsonify({
        "status": True,
        "service": "events"
    }), 200


@app.route("/api/events/user", methods=["POST"])
def post_user_event():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    payload = request.get_json()
    if payload is None:
        return jsonify({"error": "Invalid JSON"}), 400

    if not isinstance(payload, dict):
        return jsonify({"error": "Payload must be a JSON object"}), 400

    send_to_kafka(payload, KAFKA_USER_TOPIC)

    return jsonify({"status": "success", "message": "Event sent to Kafka"}), 201


@app.route("/api/events/payment", methods=["POST"])
def post_payment_event():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    payload = request.get_json()
    if payload is None:
        return jsonify({"error": "Invalid JSON"}), 400

    if not isinstance(payload, dict):
        return jsonify({"error": "Payload must be a JSON object"}), 400

    send_to_kafka(payload, KAFKA_PAYMENT_TOPIC)

    return jsonify({"status": "success", "message": "Event sent to Kafka"}), 201


@app.route("/api/events/movie", methods=["POST"])
def post_movie_event():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    payload = request.get_json()
    if payload is None:
        return jsonify({"error": "Invalid JSON"}), 400

    if not isinstance(payload, dict):
        return jsonify({"error": "Payload must be a JSON object"}), 400

    send_to_kafka(payload, KAFKA_MOVIE_TOPIC)

    return jsonify({"status": "success", "message": "Event sent to Kafka"}), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
