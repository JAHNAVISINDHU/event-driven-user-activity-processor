"""
Core business logic: applying a validated event to the user_profiles
aggregate. Kept free of any RabbitMQ/FastAPI concerns so it can be unit
tested in isolation against a plain SQLAlchemy Session.
"""
import uuid
from datetime import timezone

from sqlalchemy.exc import IntegrityError

from .models import ProcessedEvent, UserProfile


def _comparable(dt):
    """Return a tz-aware UTC datetime for safe comparison.

    Some backends (notably SQLite, used in unit tests) don't preserve
    tzinfo on round-trip even for a `DateTime(timezone=True)` column, so
    a value read back from the DB can come back naive while the
    freshly-parsed incoming event timestamp is aware. Treat naive values
    as UTC to make comparisons well-defined everywhere.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def process_event(session, event):
    """Apply a validated event to the user_profiles table, idempotently.

    `event` must already be normalized by app.schema.validate_event
    (i.e. event["timestamp"] is a datetime object).

    Returns True if the event was newly applied, False if it was a
    duplicate (already-processed event_id) and therefore skipped.
    """
    event_id = uuid.UUID(str(event["event_id"]))
    user_id = uuid.UUID(str(event["user_id"]))
    event_type = event["event_type"]
    timestamp = event["timestamp"]

    # --- Idempotency check -------------------------------------------------
    # If this event_id has already been recorded, this is a redelivery
    # (e.g. after a consumer crash before ack). Skip re-applying its
    # effects so total_events / event_counts are never double-counted.
    if session.get(ProcessedEvent, event_id) is not None:
        return False

    profile = session.get(UserProfile, user_id)
    if profile is None:
        profile = UserProfile(
            user_id=user_id,
            last_activity_at=timestamp,
            total_events=1,
            last_event_type=event_type,
            event_counts={event_type: 1},
        )
        session.add(profile)
    else:
        counts = dict(profile.event_counts or {})
        counts[event_type] = counts.get(event_type, 0) + 1
        profile.event_counts = counts
        profile.total_events = (profile.total_events or 0) + 1

        # Only move last_activity_at / last_event_type forward if this
        # event is actually newer than what's stored, guarding against
        # out-of-order delivery overwriting more recent state.
        if profile.last_activity_at is None or _comparable(timestamp) >= _comparable(profile.last_activity_at):
            profile.last_activity_at = timestamp
            profile.last_event_type = event_type

    session.add(ProcessedEvent(event_id=event_id))

    try:
        session.commit()
    except IntegrityError:
        # Race: another consumer/redelivery committed the same event_id
        # concurrently. Treat as a duplicate rather than an error.
        session.rollback()
        return False

    return True
