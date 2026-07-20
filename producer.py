"""
Event producer for the `user_activity_queue`.

Run standalone to simulate a realistic stream of user activity:

    docker compose exec app_service python producer.py

Or import `build_event` / `publish_event` / `get_channel` from other
scripts (e.g. tests/integration_test.py) to publish specific events.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "user_activity_queue")
DLX_EXCHANGE = os.getenv("DLX_EXCHANGE", "dlx_exchange")
DLQ_ROUTING_KEY = os.getenv("DLQ_ROUTING_KEY", "dlq")

EVENT_TYPES = ["view", "click", "purchase", "add_to_cart", "login", "logout"]


def get_channel():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    # Declare defensively in case the producer runs before the consumer
    # has started (queue declaration is idempotent as long as arguments
    # match).
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
        },
    )
    return connection, channel


def build_event(user_id, event_type, payload, ts=None):
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": str(user_id),
        "event_type": event_type,
        "timestamp": (ts or datetime.now(timezone.utc)).isoformat(),
        "payload": payload,
    }


def publish_event(channel, event):
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(event),
        properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
    )
    print(f" [x] Sent '{event['event_type']}' for user {event['user_id']} (event_id={event['event_id']})")


def generate_sample_payload(event_type):
    if event_type == "purchase":
        return {"item_id": str(uuid.uuid4()), "value": round(random.uniform(5, 500), 2)}
    if event_type == "view":
        return {"page": random.choice(["/home", "/products", "/cart", "/profile"])}
    if event_type == "click":
        return {"element": random.choice(["banner", "nav_link", "cta_button"])}
    if event_type == "add_to_cart":
        return {"item_id": str(uuid.uuid4()), "quantity": random.randint(1, 5)}
    return {}


def simulate(num_users=3, events_per_user=6, include_malformed=True):
    """Publish a realistic stream: >=5 events across >=3 users, plus
    one intentionally malformed event to demonstrate DLQ routing."""
    connection, channel = get_channel()
    try:
        for _ in range(num_users):
            user_id = uuid.uuid4()
            base_time = datetime.now(timezone.utc) - timedelta(minutes=events_per_user)
            for i in range(events_per_user):
                event_type = random.choice(EVENT_TYPES)
                payload = generate_sample_payload(event_type)
                ts = base_time + timedelta(minutes=i)
                event = build_event(user_id, event_type, payload, ts)
                publish_event(channel, event)
                time.sleep(0.05)

        if include_malformed:
            bad_event = {"event_id": "not-a-uuid", "user_id": str(uuid.uuid4())}  # missing fields
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(bad_event),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            print(" [x] Sent malformed event (should be routed to the DLQ)")
    finally:
        connection.close()


if __name__ == "__main__":
    simulate(num_users=3, events_per_user=6, include_malformed=True)
    print("Done. Check RabbitMQ management UI (http://localhost:15672) and "
          "the user_profiles table to see the results.")
