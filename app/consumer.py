"""
RabbitMQ consumer for the `user_activity_queue`.

Topology:
  dlx_exchange (direct)  --routing_key='dlq'-->  user_activity_dlq
  user_activity_queue     (durable, x-dead-letter-exchange=dlx_exchange,
                            x-dead-letter-routing-key='dlq')

Any message that basic_nack's with requeue=False is automatically
routed by RabbitMQ to the DLQ via the queue's dead-letter arguments.
Messages are only ack'd after the database update has been committed,
so a crash mid-processing results in redelivery (handled idempotently)
rather than data loss.
"""
import json
import logging
import time

import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from . import config
from .db import SessionLocal
from .profile_service import process_event
from .schema import ValidationError, validate_event

logger = logging.getLogger("consumer")


def get_connection():
    credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30,
    )
    return pika.BlockingConnection(params)


def declare_topology(channel):
    # Dead Letter Exchange + Dead Letter Queue, declared first.
    channel.exchange_declare(exchange=config.DLX_EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=config.DLQ_NAME, durable=True)
    channel.queue_bind(queue=config.DLQ_NAME, exchange=config.DLX_EXCHANGE, routing_key=config.DLQ_ROUTING_KEY)

    # Main queue: unacknowledged messages are capped at 1 per consumer
    # (prevents one worker from hoarding messages) and failed messages
    # are routed to the DLX above.
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(
        queue=config.QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": config.DLX_EXCHANGE,
            "x-dead-letter-routing-key": config.DLQ_ROUTING_KEY,
        },
    )


def make_callback():
    def callback(ch, method, properties, body):
        # --- Parse ----------------------------------------------------
        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            logger.error(json.dumps({"event": "parse_error", "error": str(exc)}))
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        # --- Validate ---------------------------------------------------
        try:
            validated = validate_event(data)
        except ValidationError as exc:
            logger.error(json.dumps({
                "event": "validation_error",
                "error": str(exc),
                "raw_event_id": data.get("event_id") if isinstance(data, dict) else None,
            }))
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        # --- Process (idempotent) + persist ------------------------------
        session = SessionLocal()
        try:
            applied = process_event(session, validated)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(json.dumps({
                "event": "processed" if applied else "duplicate_skipped",
                "event_id": validated["event_id"],
                "user_id": validated["user_id"],
                "event_type": validated["event_type"],
            }))
        except Exception as exc:  # noqa: BLE001 - must not crash the consumer loop
            session.rollback()
            logger.error(json.dumps({
                "event": "processing_error",
                "error": str(exc),
                "event_id": validated.get("event_id"),
            }))
            # Do not requeue: a DB error on this message will likely
            # recur immediately, so send straight to the DLQ instead of
            # spinning. Operators can inspect + manually replay from
            # there.
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        finally:
            session.close()

    return callback


def run_consumer(stop_event=None):
    """Blocking consume loop with automatic reconnection.

    Runs forever (intended to be started in a background thread) until
    `stop_event` is set, reconnecting with a backoff delay if the
    connection to RabbitMQ drops.
    """
    while stop_event is None or not stop_event.is_set():
        try:
            connection = get_connection()
            channel = connection.channel()
            declare_topology(channel)
            channel.basic_consume(queue=config.QUEUE_NAME, on_message_callback=make_callback())
            logger.info(json.dumps({"event": "consumer_started", "queue": config.QUEUE_NAME}))
            channel.start_consuming()
        except (AMQPConnectionError, AMQPChannelError) as exc:
            logger.error(json.dumps({"event": "amqp_connection_error", "error": str(exc)}))
            time.sleep(config.CONSUMER_RECONNECT_DELAY_SECONDS)
        except Exception as exc:  # noqa: BLE001
            logger.error(json.dumps({"event": "consumer_unexpected_error", "error": str(exc)}))
            time.sleep(config.CONSUMER_RECONNECT_DELAY_SECONDS)
