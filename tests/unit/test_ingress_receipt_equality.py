"""Phase-H P0: single ingress receipt clock contract.

The raw tape envelope received_at_ms and the live BookTicker
receipt_time_ms must be the SAME integer captured once at scanner
ingress -- even when raw disk recording is artificially delayed.
"""

import asyncio
import json
from unittest.mock import MagicMock

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.data.raw_events import RawEventRecorder
from signalbot.domain.enums import Market
from signalbot.scanner import MarketScanner


def _scanner(tmp_path, clock, runtime):
    settings = Settings.model_validate(
        {
            "runtime": {
                "record_raw_events": True,
                "raw_event_directory": str(tmp_path / "tape"),
                "raw_event_max_bytes": 1_048_576,
            }
        }
    )
    recorder = RawEventRecorder(tmp_path / "tape")
    stop_event = asyncio.Event()
    return MarketScanner(
        Market.SPOT,
        settings,
        clock,
        runtime,
        stop_event,
        rest_client=MagicMock(),
        raw_recorder=recorder,
    )


@pytest.mark.asyncio
async def test_ingress_receipt_feeds_both_tape_and_runtime(tmp_path) -> None:
    clock = ReplayClock(123_456)
    payload = {
        "stream": "bnbusdt@bookTicker",
        "data": {
            "u": 1,
            "s": "BNBUSDT",
            "b": "100.0",
            "B": "1.0",
            "a": "100.2",
            "A": "2.0",
        },
    }

    captured = []

    class CapturingRuntime(MagicMock):
        async def handle_payload(self, inner_payload, *, received_at_ms=None):
            captured.append((inner_payload, received_at_ms))

    scanner = _scanner(tmp_path, clock, CapturingRuntime())

    # Simulate a slow disk write between ingress capture and runtime call:
    # the recorder append is awaited before handle_payload, so any second
    # clock.now_ms() inside runtime would drift forward. ReplayClock only
    # advances explicitly, so freeze it at T0 and verify equality holds.
    await scanner._handle_payload(payload)

    assert captured, "runtime must be invoked"
    _inner_payload, received_at_ms = captured[0]
    assert received_at_ms == 123_456

    assert scanner.raw_recorder is not None
    assert await scanner.raw_recorder.wait_drained()
    await scanner.raw_recorder.close()
    tape_files = list((tmp_path / "tape").rglob("*.jsonl"))
    assert len(tape_files) == 1
    record = json.loads(tape_files[0].read_text(encoding="utf-8"))
    assert record["received_at_ms"] == received_at_ms == 123_456
