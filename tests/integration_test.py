"""
End-to-end integration test.

Verifies the full pipeline: producer publishes -> RabbitMQ delivers ->
consumer processes -> PostgreSQL user_profiles is updated. Also
publishes a malformed event and confirms it is routed to the DLQ.

This test talks to real RabbitMQ/PostgreSQL services, so it must be
run against the running docker-compose stack:

    docker compose exec app_service python -m tests.integration_test

(Not collected by `pytest`; see pytest.ini which restricts collection
to test_*.py so this doesn't run as part of the unit test suite.)
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pika
import psycopg2

import producer
from app import config


def wait_for_profile(cursor, user_id, expected_total, timeout=20):
    deadline = time.time() + timeout
    row = None
    while time.time() < deadline:
        cursor.execute(
            "SELECT total_events, event_counts, last_event_type "
            "FROM user_profiles WHERE user_id = %s",
            (str(user_id),),
        )
        row = cursor.fetchone()
        if row and row[0] >= expected_total:
            return row
        time.sleep(0.5)
    return row


def dlq_message_count():
    credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(host=config.RABBITMQ_HOST, port=config.RABBITMQ_PORT, credentials=credentials)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    result = channel.queue_declare(queue=config.DLQ_NAME, durable=True, passive=True)
    count = result.method.message_count
    connection.close()
    return count


def wait_for_dlq_message(timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if dlq_message_count() > 0:
            return True
        time.sleep(1)
    return False


def main():
    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # --- 1. Publish a realistic sequence of events for one user --------
    connection, channel = producer.get_channel()
    user_id = uuid.uuid4()
    events = [
        ("view", {"page": "/home"}),
        ("click", {"element": "cta_button"}),
        ("add_to_cart", {"item_id": str(uuid.uuid4()), "quantity": 2}),
        ("purchase", {"item_id": str(uuid.uuid4()), "value": 42.5}),
        ("logout", {}),
    ]
    now = datetime.now(timezone.utc)
    for i, (event_type, payload) in enumerate(events):
        event = producer.build_event(user_id, event_type, payload, now)
        producer.publish_event(channel, event)
    connection.close()

    # --- 2. Verify user_profiles reflects them ---------------------------
    row = wait_for_profile(cursor, user_id, expected_total=len(events))
    assert row is not None, "user_profiles was not updated in time"
    total_events, event_counts, last_event_type = row
    assert total_events == len(events), f"expected {len(events)} total_events, got {total_events}"
    assert isinstance(event_counts, dict)
    assert sum(event_counts.values()) == len(events)
    print(f"PASS: user_profiles correctly updated for user {user_id} "
          f"(total_events={total_events}, event_counts={event_counts})")

    # --- 3. Idempotency: redeliver one of the same events ----------------
    duplicate_event = producer.build_event(user_id, "view", {"page": "/home"}, now)
    # Publish the *same* event twice to simulate redelivery.
    connection, channel = producer.get_channel()
    producer.publish_event(channel, duplicate_event)
    producer.publish_event(channel, duplicate_event)
    connection.close()

    time.sleep(3)
    cursor.execute("SELECT total_events FROM user_profiles WHERE user_id = %s", (str(user_id),))
    total_after_dupe = cursor.fetchone()[0]
    assert total_after_dupe == len(events) + 1, (
        f"expected exactly one increment for the duplicated event_id, "
        f"got total_events={total_after_dupe}"
    )
    print("PASS: duplicate event_id was processed idempotently")

    # --- 4. DLQ: malformed event should be routed there -------------------
    before = dlq_message_count()
    connection, channel = producer.get_channel()
    bad_event = {"event_id": "not-a-uuid", "user_id": str(uuid.uuid4())}  # missing fields too
    channel.basic_publish(
        exchange="",
        routing_key=config.QUEUE_NAME,
        body=json.dumps(bad_event),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    connection.close()

    assert wait_for_dlq_message(), "malformed event was not routed to the DLQ"
    after = dlq_message_count()
    assert after >= before + 1
    print("PASS: malformed event correctly routed to the DLQ")

    cursor.close()
    conn.close()
    print("ALL INTEGRATION TESTS PASSED")


if __name__ == "__main__":
    main()
