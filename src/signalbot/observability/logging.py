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


# Discord webhook URLs carry the secret token as the last path segment:
# /api/webhooks/<id>/<token> (optionally /api/v10/webhooks/...).
_WEBHOOK_SECRET_PATTERN = re.compile(r"(/webhooks/\d+/)[^\s/?#\"'<>)\]}]+")
_REDACTED = "<redacted>"
_NOISY_TRANSPORT_LOGGERS = ("httpx", "httpcore")


def redact_webhook_secrets(text: str) -> str:
    return _WEBHOOK_SECRET_PATTERN.sub(rf"\g<1>{_REDACTED}", text)


def _redact_arg(arg: object) -> object:
    if arg is None or isinstance(arg, bool | int | float):
        return arg
    return redact_webhook_secrets(arg if isinstance(arg, str) else str(arg))


class WebhookSecretRedactingFilter(logging.Filter):
    """Redacts Discord webhook tokens from message, args and exception text."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_webhook_secrets(record.msg)
        else:
            record.msg = redact_webhook_secrets(str(record.msg))
        if isinstance(record.args, tuple):
            record.args = tuple(_redact_arg(arg) for arg in record.args)
        elif isinstance(record.args, dict):
            record.args = {key: _redact_arg(value) for key, value in record.args.items()}
        if record.exc_info and record.exc_info[0] is not None:
            rendered = logging.Formatter().formatException(record.exc_info)
            record.exc_text = redact_webhook_secrets(rendered)
        elif record.exc_text:
            record.exc_text = redact_webhook_secrets(record.exc_text)
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_webhook_secrets(record.getMessage()),
        }
        for key in ("market", "symbol", "event_id", "stream", "attempt"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload["fields"] = mask_value(fields)
        if record.exc_info:
            exception_text = record.exc_text or self.formatException(record.exc_info)
            payload["exception"] = redact_webhook_secrets(exception_text)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(WebhookSecretRedactingFilter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # httpx logs every request URL at INFO, which includes the webhook token.
    for name in _NOISY_TRANSPORT_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
