from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from signalbot.domain.enums import Market


class RawEventCapacityError(RuntimeError):
    """Raised before a write would exceed the configured evidence quota."""


class RawEventRecorder:
    def __init__(
        self,
        directory: str | Path,
        maximum_total_bytes: int = 10_737_418_240,
    ) -> None:
        if maximum_total_bytes < 1:
            raise ValueError("maximum_total_bytes must be positive")
        self.directory = Path(directory)
        self.maximum_total_bytes = maximum_total_bytes
        self._lock = asyncio.Lock()
        self._known_bytes: int | None = None

    async def append(self, market: Market, payload: Any, event_time_ms: int) -> None:
        day = datetime.fromtimestamp(event_time_ms / 1000, tz=UTC).strftime("%Y-%m-%d")
        path = self.directory / market.value / f"{day}.jsonl"
        line = json.dumps(
            {"market": market.value, "received_at_ms": event_time_ms, "payload": payload},
            separators=(",", ":"),
            ensure_ascii=False,
        )
        encoded_bytes = len(line.encode("utf-8")) + 1
        async with self._lock:
            if self._known_bytes is None:
                self._known_bytes = await asyncio.to_thread(self._directory_size)
            if self._known_bytes + encoded_bytes > self.maximum_total_bytes:
                raise RawEventCapacityError(
                    "raw-event directory reached its configured hard byte quota"
                )
            path.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, path, line)
            self._known_bytes += encoded_bytes

    def _directory_size(self) -> int:
        if not self.directory.exists():
            return 0
        return sum(
            path.stat().st_size
            for path in self.directory.rglob("*")
            if path.is_file()
        )

    @staticmethod
    def _append_line(path: Path, line: str) -> None:
        # Pin the on-disk terminator to one byte so the pre-write quota
        # calculation is exact on Windows as well as POSIX systems.
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(line + "\n")
