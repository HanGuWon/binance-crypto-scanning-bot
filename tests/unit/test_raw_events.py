import asyncio
import json
from unittest.mock import MagicMock

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.data.raw_events import RawEventCapacityError, RawEventRecorder
from signalbot.domain.enums import Market
from signalbot.scanner import MarketScanner


@pytest.mark.asyncio
async def test_raw_event_recorder_writes_replayable_jsonl(tmp_path) -> None:
    recorder = RawEventRecorder(tmp_path)
    await recorder.append(Market.SPOT, {"e": "test", "value": 1}, 1_710_000_000_000)
    files = list(tmp_path.rglob("*.jsonl"))
    assert len(files) == 1
    record = json.loads(files[0].read_text(encoding="utf-8"))
    assert record == {
        "market": "spot",
        "received_at_ms": 1_710_000_000_000,
        "payload": {"e": "test", "value": 1},
    }


@pytest.mark.asyncio
async def test_raw_event_recorder_fails_before_exceeding_hard_quota(tmp_path) -> None:
    recorder = RawEventRecorder(tmp_path, maximum_total_bytes=1)
    with pytest.raises(RawEventCapacityError, match="hard byte quota"):
        await recorder.append(Market.SPOT, {"e": "test"}, 1)
    assert list(tmp_path.rglob("*.jsonl")) == []


@pytest.mark.asyncio
async def test_raw_event_recorder_honors_exact_byte_boundary(tmp_path) -> None:
    record = {
        "market": "spot",
        "received_at_ms": 1,
        "payload": {"e": "test"},
    }
    expected_size = len(
        json.dumps(
            record,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ) + 1
    recorder = RawEventRecorder(tmp_path, maximum_total_bytes=expected_size)

    await recorder.append(Market.SPOT, {"e": "test"}, 1)

    files = list(tmp_path.rglob("*.jsonl"))
    assert len(files) == 1
    assert files[0].stat().st_size == expected_size
    with pytest.raises(RawEventCapacityError, match="hard byte quota"):
        await recorder.append(Market.SPOT, {"e": "test"}, 1)


def test_market_scanners_share_one_global_raw_event_quota(tmp_path) -> None:
    settings = Settings.model_validate(
        {
            "runtime": {
                "record_raw_events": True,
                "raw_event_directory": str(tmp_path),
                "raw_event_max_bytes": 1_048_576,
            }
        }
    )
    recorder = RawEventRecorder(tmp_path, maximum_total_bytes=1_048_576)
    stop_event = asyncio.Event()

    spot = MarketScanner(
        Market.SPOT,
        settings,
        ReplayClock(0),
        MagicMock(),
        stop_event,
        rest_client=MagicMock(),
        raw_recorder=recorder,
    )
    futures = MarketScanner(
        Market.FUTURES,
        settings,
        ReplayClock(0),
        MagicMock(),
        stop_event,
        rest_client=MagicMock(),
        raw_recorder=recorder,
    )

    assert spot.raw_recorder is recorder
    assert futures.raw_recorder is recorder
