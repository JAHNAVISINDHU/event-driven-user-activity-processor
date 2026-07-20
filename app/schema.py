"""
Validation for the `user_activity` event schema:

{
  "event_id": "UUID",
  "user_id": "UUID",
  "event_type": "string",
  "timestamp": "ISO 8601 string",
  "payload": {}
}
"""
import uuid
from datetime import datetime

REQUIRED_FIELDS = {"event_id", "user_id", "event_type", "timestamp", "payload"}


class ValidationError(Exception):
    """Raised when an incoming event does not conform to the schema."""


def _parse_timestamp(raw):
    ts = raw
    if isinstance(ts, str) and ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts)
    except (TypeError, ValueError) as exc:
        raise ValidationError("timestamp must be an ISO 8601 string") from exc


def validate_event(data):
    """Validate a raw decoded event dict and return a normalized version.

    Raises ValidationError with a descriptive message on any schema
    violation. Normalizes `timestamp` into a `datetime` object so
    downstream business logic never has to re-parse it.
    """
    if not isinstance(data, dict):
        raise ValidationError("Event payload must be a JSON object")

    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        raise ValidationError(f"Missing required field(s): {sorted(missing)}")

    try:
        uuid.UUID(str(data["event_id"]))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValidationError("event_id must be a valid UUID") from exc

    try:
        uuid.UUID(str(data["user_id"]))
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValidationError("user_id must be a valid UUID") from exc

    if not isinstance(data["event_type"], str) or not data["event_type"].strip():
        raise ValidationError("event_type must be a non-empty string")

    timestamp = _parse_timestamp(data["timestamp"])

    if not isinstance(data["payload"], dict):
        raise ValidationError("payload must be a JSON object")

    return {
        "event_id": str(data["event_id"]),
        "user_id": str(data["user_id"]),
        "event_type": data["event_type"],
        "timestamp": timestamp,
        "payload": data["payload"],
    }
