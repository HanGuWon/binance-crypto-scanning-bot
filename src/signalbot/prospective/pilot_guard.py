"""Operational admission limits, independent of candidate formulas and pending recovery."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path

from sqlalchemy.engine import make_url

from signalbot.config import MarketWatchSettings


class ShadowPilotGuard:
    """Latch a campaign stop without stopping production or discarding pending rows."""

    def __init__(self, watch: MarketWatchSettings, database_url: str) -> None:
        self.watch = watch
        url = make_url(database_url)
        self.database = (
            Path(url.database).resolve()
            if url.drivername.startswith("sqlite") and url.database not in (None, ":memory:")
            else None
        )
        campaign = str(watch.public_structure_flow_campaign_id)
        suffix = hashlib.sha256(campaign.encode()).hexdigest()[:24]
        self.marker = (
            self.database.with_name(f"shadow-stop-{suffix}.json") if self.database else None
        )
        self.reason: str | None = None

    def admission_allowed(self, now_ms: int) -> bool:
        if self.reason is not None:
            return False
        if self.marker is not None and self.marker.exists():
            self.reason = "persisted_campaign_stop"
            return False
        reason: str | None = None
        if self.watch.public_structure_flow_stop_ms is not None:
            if now_ms >= self.watch.public_structure_flow_stop_ms:
                reason = "pilot_window_complete"
        disk_floor = self.watch.public_structure_flow_min_free_disk_bytes
        size_ceiling = self.watch.public_structure_flow_max_database_bytes
        if disk_floor or size_ceiling:
            if self.database is None:
                reason = reason or "unverifiable_storage_budget"
            else:
                paths = [self.database, Path(str(self.database) + "-wal"),
                         Path(str(self.database) + "-journal")]
                size = sum(p.stat().st_size for p in paths if p.exists())
                if size_ceiling and size >= size_ceiling:
                    reason = reason or "database_size_limit"
                if disk_floor and shutil.disk_usage(self.database.parent).free <= disk_floor:
                    reason = reason or "free_disk_floor"
        if reason is None:
            return True
        self.reason = reason
        if self.marker is not None:
            try:
                with self.marker.open("x", encoding="utf-8") as handle:
                    json.dump({"campaign_id": self.watch.public_structure_flow_campaign_id,
                               "stopped_at_ms": now_ms, "reason": reason}, handle)
                    handle.flush()
                    os.fsync(handle.fileno())
            except FileExistsError:
                pass
        return False
