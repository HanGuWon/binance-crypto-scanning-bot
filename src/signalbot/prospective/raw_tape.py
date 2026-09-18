"""Receipt-time-aware replay for the prospective raw market-data tape."""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Protocol

from signalbot.clock import ReplayClock
from signalbot.domain.enums import Market
from signalbot.prospective.segmented_replay import iter_raw_tape


class RawTapePayloadRuntime(Protocol):
    async def handle_payload(self, payload: Any) -> None: ...


class RawTapeReplayError(ValueError):
    """Raised when receipt-time evidence is malformed or non-causal."""


class ProspectiveRawTapeReplay:
    """Feed durable raw envelopes using recorded local receipt time as clock."""

    def __init__(
        self,
        runtime: RawTapePayloadRuntime,
        clock: ReplayClock,
        *,
        market: Market,
    ) -> None:
        self.runtime = runtime
        self.clock = clock
        self.market = market
        self.records_read = 0

    async def replay_file(self, path: str | Path) -> int:
        """Replay one JSONL file in durable line order without re-sorting events."""

        count = 0
        def _lines():
            with Path(path).open(encoding="utf-8") as handle:
                yield from handle

        for line_number, line in enumerate(_lines(), start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RawTapeReplayError(
                    f"invalid raw tape JSON at {path}:{line_number}"
                ) from exc
            if not isinstance(record, dict):
                raise RawTapeReplayError(
                    f"raw tape record must be an object at {path}:{line_number}"
                )
            if record.get("market") != self.market.value:
                raise RawTapeReplayError(
                    f"raw tape market mismatch at {path}:{line_number}"
                )
            received_at_ms = record.get("received_at_ms")
            if isinstance(received_at_ms, bool) or not isinstance(received_at_ms, int):
                raise RawTapeReplayError(
                    f"raw tape receipt clock missing/invalid at {path}:{line_number}"
                )
            if received_at_ms < self.clock.now_ms():
                raise RawTapeReplayError(
                    f"raw tape receipt clock moved backwards at {path}:{line_number}"
                )
            if "payload" not in record:
                raise RawTapeReplayError(
                    f"raw tape payload missing at {path}:{line_number}"
                )
            self.clock.advance_to(received_at_ms)
            await self.runtime.handle_payload(record["payload"])
            count += 1
            self.records_read += 1
        return count

    async def replay_files(self, paths: Iterable[str | Path]) -> int:
        total = 0
        for path in paths:
            total += await self.replay_file(path)
        return total

    async def replay_directory(self, directory: str | Path) -> int:
        """Stream-replay a whole raw-tape directory (legacy or segmented V2).

        Bounded memory via the unified reader; validates segment chain,
        hashes, and per-market receipt monotonicity while streaming.
        """
        total = 0
        for record in iter_raw_tape(Path(directory)):
            if record.get("market") != self.market.value:
                raise RawTapeReplayError(
                    "raw tape market mismatch during directory replay"
                )
            received_at_ms = record.get("received_at_ms")
            if isinstance(received_at_ms, bool) or not isinstance(
                received_at_ms, int
            ):
                raise RawTapeReplayError("raw tape receipt clock invalid")
            self.clock.advance_to(received_at_ms)
            await self.runtime.handle_payload(record["payload"])
            total += 1
            self.records_read += 1
        return total
