import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import ProcessedEvent, UserProfile
from app.profile_service import process_event


def _as_utc(dt):
    """SQLite doesn't round-trip tzinfo even for DateTime(timezone=True)
    columns, so normalize before comparing in these SQLite-backed tests
    (PostgreSQL, used in production/integration tests, does not have
    this issue)."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


@pytest.fixture()
def session():
    """Fresh in-memory SQLite DB per test -- fast, no external services
    required for these unit tests."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    yield db
    db.close()


def make_event(user_id, event_type, ts, event_id=None, payload=None):
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "user_id": str(user_id),
        "event_type": event_type,
        "timestamp": ts,
        "payload": payload or {},
    }


def test_creates_new_profile_on_first_event(session):
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    applied = process_event(session, make_event(user_id, "view", now))

    assert applied is True
    profile = session.get(UserProfile, user_id)
    assert profile is not None
    assert profile.total_events == 1
    assert profile.last_event_type == "view"
    assert profile.event_counts == {"view": 1}
    assert _as_utc(profile.last_activity_at) == now


def test_updates_existing_profile_and_increments_counts(session):
    user_id = uuid.uuid4()
    t1 = datetime.now(timezone.utc)
    t2 = t1 + timedelta(minutes=1)

    process_event(session, make_event(user_id, "view", t1))
    process_event(session, make_event(user_id, "click", t2))
    process_event(session, make_event(user_id, "click", t2 + timedelta(minutes=1)))

    profile = session.get(UserProfile, user_id)
    assert profile.total_events == 3
    assert profile.event_counts == {"view": 1, "click": 2}
    assert profile.last_event_type == "click"


def test_duplicate_event_id_is_idempotent(session):
    user_id = uuid.uuid4()
    event_id = str(uuid.uuid4())
    ts = datetime.now(timezone.utc)
    event = make_event(user_id, "purchase", ts, event_id=event_id)

    first = process_event(session, dict(event))
    second = process_event(session, dict(event))  # simulate redelivery

    assert first is True
    assert second is False

    profile = session.get(UserProfile, user_id)
    assert profile.total_events == 1
    assert profile.event_counts == {"purchase": 1}
    assert session.get(ProcessedEvent, uuid.UUID(event_id)) is not None


def test_out_of_order_event_does_not_regress_last_activity(session):
    user_id = uuid.uuid4()
    later = datetime.now(timezone.utc)
    earlier = later - timedelta(minutes=10)

    process_event(session, make_event(user_id, "click", later))
    process_event(session, make_event(user_id, "view", earlier))  # arrives late

    profile = session.get(UserProfile, user_id)
    # total_events / event_counts still reflect both events...
    assert profile.total_events == 2
    assert profile.event_counts == {"click": 1, "view": 1}
    # ...but last_activity_at / last_event_type stay at the newer event.
    assert _as_utc(profile.last_activity_at) == later
    assert profile.last_event_type == "click"


def test_multiple_users_are_independent(session):
    user_a = uuid.uuid4()
    user_b = uuid.uuid4()
    now = datetime.now(timezone.utc)

    process_event(session, make_event(user_a, "view", now))
    process_event(session, make_event(user_b, "purchase", now))

    profile_a = session.get(UserProfile, user_a)
    profile_b = session.get(UserProfile, user_b)

    assert profile_a.total_events == 1
    assert profile_b.total_events == 1
    assert profile_a.event_counts == {"view": 1}
    assert profile_b.event_counts == {"purchase": 1}
