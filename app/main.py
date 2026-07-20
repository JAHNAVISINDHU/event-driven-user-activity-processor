import logging
import threading

import pika
from fastapi import FastAPI, Response
from sqlalchemy import text

from . import config
from .consumer import run_consumer
from .db import SessionLocal, init_db
from .logging_config import setup_logging

setup_logging()
logger = logging.getLogger("app")

app = FastAPI(title="Event-Driven User Activity Processor")

_stop_event = threading.Event()
_consumer_thread = None


@app.on_event("startup")
def startup():
    # ORM-based migration safety net (init.sql already creates the
    # tables when the postgres container first starts).
    init_db()

    global _consumer_thread
    _consumer_thread = threading.Thread(target=run_consumer, args=(_stop_event,), daemon=True)
    _consumer_thread.start()
    logger.info("Application startup complete: consumer thread launched")


@app.on_event("shutdown")
def shutdown():
    _stop_event.set()
    logger.info("Application shutdown: stop signal sent to consumer")


@app.get("/health")
def health():
    """Returns 200 only if both PostgreSQL and RabbitMQ are reachable."""
    db_ok = False
    mq_ok = False

    try:
        session = SessionLocal()
        try:
            session.execute(text("SELECT 1"))
        finally:
            session.close()
        db_ok = True
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Health check: database unreachable: {exc}")

    try:
        credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD)
        params = pika.ConnectionParameters(
            host=config.RABBITMQ_HOST,
            port=config.RABBITMQ_PORT,
            credentials=credentials,
            blocked_connection_timeout=3,
            socket_timeout=3,
        )
        connection = pika.BlockingConnection(params)
        connection.close()
        mq_ok = True
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Health check: RabbitMQ unreachable: {exc}")

    if db_ok and mq_ok:
        return {"status": "ok", "database": "connected", "rabbitmq": "connected"}

    return Response(
        content='{"status": "error", "database": %s, "rabbitmq": %s}'
        % (str(db_ok).lower(), str(mq_ok).lower()),
        status_code=500,
        media_type="application/json",
    )
