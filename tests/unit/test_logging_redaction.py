from __future__ import annotations

import io
import logging
from collections.abc import Iterator

import httpx
import pytest

from signalbot.observability.logging import (
    WebhookSecretRedactingFilter,
    configure_logging,
    redact_webhook_secrets,
)

TOKEN = "SECRETtoken_abc-123.xyz"
WEBHOOK = f"https://discord.com/api/webhooks/123456789012345678/{TOKEN}"


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    saved = {n: logging.getLogger(n).level for n in ("httpx", "httpcore")}
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    for name, level in saved.items():
        logging.getLogger(name).setLevel(level)


def _post_webhook() -> None:
    transport = httpx.MockTransport(lambda request: httpx.Response(204))
    with httpx.Client(transport=transport) as client:
        client.post(f"{WEBHOOK}?wait=true", json={"content": "x"})


@pytest.mark.parametrize("level", ["INFO", "DEBUG"])
def test_configured_logging_never_emits_webhook_token(
    level: str, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging(level)
    _post_webhook()
    out = capsys.readouterr().out
    assert TOKEN not in out
    assert "123456789012345678/" not in out or "<redacted>" in out


def test_httpx_and_httpcore_loggers_are_warning_or_higher() -> None:
    configure_logging("DEBUG")
    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING
    assert logging.getLogger("httpcore").getEffectiveLevel() >= logging.WARNING


def test_filter_redacts_message_args_and_exception(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("INFO")
    log = logging.getLogger("signalbot.test")
    log.info("posting to %s", WEBHOOK)
    log.info(f"literal {WEBHOOK}")
    try:
        raise RuntimeError(f"boom {WEBHOOK}?wait=true")
    except RuntimeError:
        log.error("failed", exc_info=True)
    out = capsys.readouterr().out
    assert TOKEN not in out
    assert out.count("<redacted>") >= 3


def test_non_webhook_record_is_unchanged(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    logging.getLogger("signalbot.test").info(
        "plain %s %d https://discord.com/api/channels/1", "text", 7
    )
    out = capsys.readouterr().out
    assert "plain text 7 https://discord.com/api/channels/1" in out
    assert "<redacted>" not in out


def test_redact_function_boundaries() -> None:
    assert redact_webhook_secrets("") == ""
    assert redact_webhook_secrets("/api/v10/webhooks/1/abc") == (
        "/api/v10/webhooks/1/<redacted>"
    )
    assert redact_webhook_secrets("/webhooks/12/") == "/webhooks/12/"
    assert redact_webhook_secrets("/webhooks/abc/def") == "/webhooks/abc/def"


def test_filter_is_reusable_on_any_handler() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(WebhookSecretRedactingFilter())
    logger = logging.getLogger("position_guardian.reuse")
    logger.propagate = False
    logger.addHandler(handler)
    try:
        logger.warning("sent %s", WEBHOOK)
    finally:
        logger.removeHandler(handler)
        logger.propagate = True
    assert TOKEN not in stream.getvalue()
    assert "<redacted>" in stream.getvalue()
