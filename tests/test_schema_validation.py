import uuid
from datetime import datetime, timezone

import pytest

from app.schema import ValidationError, validate_event


def make_valid_event(**overrides):
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "event_type": "view",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {"page": "/home"},
    }
    event.update(overrides)
    return event


def test_valid_event_passes_and_normalizes_timestamp():
    event = make_valid_event()
    result = validate_event(event)
    assert result["event_type"] == "view"
    assert isinstance(result["timestamp"], datetime)


def test_valid_event_with_z_suffix_timestamp():
    event = make_valid_event(timestamp="2026-01-01T10:00:00Z")
    result = validate_event(event)
    assert result["timestamp"].year == 2026


def test_missing_field_raises():
    event = make_valid_event()
    del event["user_id"]
    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_event_id_uuid_raises():
    event = make_valid_event(event_id="not-a-uuid")
    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_user_id_uuid_raises():
    event = make_valid_event(user_id="not-a-uuid")
    with pytest.raises(ValidationError):
        validate_event(event)


def test_empty_event_type_raises():
    event = make_valid_event(event_type="   ")
    with pytest.raises(ValidationError):
        validate_event(event)


def test_non_string_event_type_raises():
    event = make_valid_event(event_type=123)
    with pytest.raises(ValidationError):
        validate_event(event)


def test_invalid_timestamp_raises():
    event = make_valid_event(timestamp="not-a-date")
    with pytest.raises(ValidationError):
        validate_event(event)


def test_non_dict_payload_raises():
    event = make_valid_event(payload="not-a-dict")
    with pytest.raises(ValidationError):
        validate_event(event)


def test_non_dict_input_raises():
    with pytest.raises(ValidationError):
        validate_event("not-a-dict")


def test_non_dict_input_list_raises():
    with pytest.raises(ValidationError):
        validate_event(["event_id", "user_id"])
