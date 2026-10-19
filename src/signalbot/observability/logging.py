from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any

# Structured one-line events (for example a handled candle gap) carry a ``fields``
# mapping. Its values are masked before they are written so an operational log can
# never become a secret sink.
_SECRET_KEY = re.compile(r"(secret|token|password|passwd|webhook|api[_-]?key|authorization)", re.I)
_WEBHOOK_URL = re.compile(r"https?://[^\s\"']*webhooks?/[^\s\"']*", re.I)
_LONG_TOKEN = re.compile(r"\b[A-Za-z0-9_\-]{40,}\b")
MASK = "***"


def mask_value(value: Any, *, key: str = "") -> Any:
    if key and _SECRET_KEY.search(key):
        return MASK
    if isinstance(value, dict):
        return {str(k): mask_value(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [mask_value(item) for item in value]
    if isinstance(value, str):
        return _LONG_TOKEN.sub(MASK, _WEBHOOK_URL.sub(MASK, value))
    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("market", "symbol", "event_id", "stream", "attempt"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload["fields"] = mask_value(fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
