"""
Central configuration module. All environment-dependent values are read
here from environment variables so that no configuration is hardcoded
elsewhere in the codebase (see .env.example for the full list).
"""
import os

# --- RabbitMQ ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

QUEUE_NAME = os.getenv("QUEUE_NAME", "user_activity_queue")
DLX_EXCHANGE = os.getenv("DLX_EXCHANGE", "dlx_exchange")
DLQ_NAME = os.getenv("DLQ_NAME", "user_activity_dlq")
DLQ_ROUTING_KEY = os.getenv("DLQ_ROUTING_KEY", "dlq")

# --- PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "activity_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "activity_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "activity_pass")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
)

# --- App ---
APP_PORT = int(os.getenv("APP_PORT", "8000"))
CONSUMER_RECONNECT_DELAY_SECONDS = int(os.getenv("CONSUMER_RECONNECT_DELAY_SECONDS", "5"))
