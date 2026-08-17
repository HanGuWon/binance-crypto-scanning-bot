from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from signalbot.clock import ReplayClock
from signalbot.domain.models import SignalDecision
from signalbot.runtime import MarketRuntime


@dataclass(frozen=True, slots=True)
class ReplayResult:
    events_read: int
    decisions: tuple[SignalDecision, ...]
    parse_errors: int


class ReplayEngine:
    def __init__(self, runtime: MarketRuntime, clock: ReplayClock) -> None:
        self.runtime = runtime
        self.clock = clock

    async def run_file(self, path: str | Path) -> ReplayResult:
        text = await asyncio.to_thread(Path(path).read_text, encoding="utf-8")
        count = 0
        for line_number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            try:
                record = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_number}") from exc
            payload = record.get("payload", record) if isinstance(record, dict) else record
            timestamp = _event_time_ms(payload)
            if timestamp is not None:
                self.clock.advance_to(max(timestamp, self.clock.now_ms()))
            await self.runtime.handle_payload(payload)
            count += 1
        decisions = tuple(
            sorted(
                self.runtime.repository.recent_signals(10_000),
                key=lambda item: item.event_time_ms,
            )
        )
        return ReplayResult(count, decisions, self.runtime.parse_error_count)


def _event_time_ms(payload: Any) -> int | None:
    if isinstance(payload, dict) and "data" in payload:
        return _event_time_ms(payload["data"])
    if isinstance(payload, list):
        times = [value for item in payload if (value := _event_time_ms(item)) is not None]
        return max(times) if times else None
    if not isinstance(payload, dict):
        return None
    if payload.get("E") is not None:
        return int(payload["E"])
    kline = payload.get("k")
    return int(kline["T"]) if isinstance(kline, dict) and kline.get("T") is not None else None
