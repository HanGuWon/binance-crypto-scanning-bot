from __future__ import annotations

import json

import pytest

from signalbot.clock import ReplayClock
from signalbot.domain.enums import Market
from signalbot.prospective.raw_tape import (
    ProspectiveRawTapeReplay,
    RawTapeReplayError,
)


class _Runtime:
    def __init__(self, clock: ReplayClock) -> None:
        self.clock = clock
        self.receipts: list[tuple[int, dict[str, object]]] = []

    async def handle_payload(self, payload: object) -> None:
        assert isinstance(payload, dict)
        self.receipts.append((self.clock.now_ms(), payload))


@pytest.mark.asyncio
async def test_raw_tape_replay_uses_outer_receipt_not_exchange_event(tmp_path) -> None:
    path = tmp_path / "spot.jsonl"
    path.write_text(
        json.dumps(
            {
                "market": "spot",
                "received_at_ms": 300_005,
                "payload": {"e": "bookTicker", "E": 299_000, "T": 299_001},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    clock = ReplayClock(300_000)
    runtime = _Runtime(clock)
    replay = ProspectiveRawTapeReplay(runtime, clock, market=Market.SPOT)

    assert await replay.replay_file(path) == 1
    assert runtime.receipts == [
        (300_005, {"e": "bookTicker", "E": 299_000, "T": 299_001})
    ]


@pytest.mark.asyncio
async def test_raw_tape_replay_rejects_backwards_receipt_and_wrong_market(tmp_path) -> None:
    path = tmp_path / "bad.jsonl"
    path.write_text(
        "\n".join(
            [
                json.dumps({"market": "spot", "received_at_ms": 300_005, "payload": {}}),
                json.dumps({"market": "spot", "received_at_ms": 300_004, "payload": {}}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    clock = ReplayClock(0)
    replay = ProspectiveRawTapeReplay(_Runtime(clock), clock, market=Market.SPOT)
    with pytest.raises(RawTapeReplayError, match="moved backwards"):
        await replay.replay_file(path)

    wrong = tmp_path / "wrong.jsonl"
    wrong.write_text(
        json.dumps({"market": "futures", "received_at_ms": 1, "payload": {}}) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(RawTapeReplayError, match="market mismatch"):
        await replay.replay_file(wrong)
