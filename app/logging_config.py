"""
Structured (JSON) logging configuration.

Log calls elsewhere in the app pass a JSON string as the log message
(e.g. logger.info(json.dumps({"event": "processed", ...}))); this
formatter merges that structured payload into a single JSON log line
along with standard fields (timestamp, level, logger name).
"""
import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
        }
        message = record.getMessage()
        try:
            parsed = json.loads(message)
            if isinstance(parsed, dict):
                payload.update(parsed)
            else:
                payload["message"] = message
        except (json.JSONDecodeError, TypeError):
            payload["message"] = message

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload)


def setup_logging(level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
