from sqlalchemy import JSON, Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from .db import Base
from .types import GUID


class UserProfile(Base):
    """Aggregated, real-time view of a user's activity."""

    __tablename__ = "user_profiles"

    user_id = Column(GUID(), primary_key=True)
    last_activity_at = Column(DateTime(timezone=True), nullable=False)
    total_events = Column(Integer, nullable=False, default=0)
    last_event_type = Column(String(255), nullable=False)
    # JSONB on PostgreSQL, plain JSON elsewhere (e.g. SQLite in unit tests).
    event_counts = Column(JSON().with_variant(JSONB, "postgresql"), nullable=False, default=dict)


class ProcessedEvent(Base):
    """Records every event_id that has been successfully processed.

    This is the backbone of consumer idempotency: before applying an
    event's effects to a user_profile, the consumer checks whether its
    event_id already exists here. If it does, the event is a redelivery
    and is skipped (but still acknowledged) rather than double-counted.
    """

    __tablename__ = "processed_events"

    event_id = Column(GUID(), primary_key=True)
    processed_at = Column(DateTime(timezone=True), server_default=func.now())
