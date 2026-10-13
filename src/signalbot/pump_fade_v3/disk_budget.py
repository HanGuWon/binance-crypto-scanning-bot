from __future__ import annotations

import logging
import os
import shutil
import stat
from collections.abc import Callable
from pathlib import Path

from signalbot.capture.errors import CaptureStorageCapacityError
from signalbot.capture.path_safety import inspect_link_free_path

LOGGER = logging.getLogger(__name__)


class DiskBudget:
    """Fail-closed DATA-filesystem free-space guard; never removes capture data."""

    def __init__(
        self,
        data_directory: str | Path,
        *,
        maximum_total_bytes: int,
        minimum_free_bytes: int,
        event_depth_shed: Callable[[], None],
        expected_mount: str | Path,
        emergency_reserve_bytes: int = 0,
    ) -> None:
        self.data_directory = Path(data_directory)
        if maximum_total_bytes <= 0 or minimum_free_bytes <= 0:
            raise ValueError("disk budget byte limits must be positive")
        if minimum_free_bytes >= maximum_total_bytes:
            raise ValueError("minimum free space must be below the hard cap")
        if not 0 <= emergency_reserve_bytes < maximum_total_bytes:
            raise ValueError("emergency reserve must fit inside the collector cap")
        if not callable(event_depth_shed):
            raise TypeError("event_depth_shed callback must be callable")
        self.maximum_total_bytes = maximum_total_bytes
        self.minimum_free_bytes = minimum_free_bytes
        self.emergency_reserve_bytes = emergency_reserve_bytes
        self._event_depth_shed = event_depth_shed
        self._expected_mount = Path(expected_mount)
        self._tripped = False
        inspection = inspect_link_free_path(self.data_directory, "collector data")
        if inspection.final_status is None or not self.data_directory.is_dir():
            raise ValueError("collector data directory must already exist")
        mount_inspection = inspect_link_free_path(self._expected_mount, "expected DATA mount")
        if mount_inspection.final_status is None or not os.path.ismount(self._expected_mount):
            raise ValueError("expected DATA path must be an existing mount point")
        if not self.data_directory.is_relative_to(self._expected_mount):
            raise ValueError("collector data directory is outside the DATA mount")
        if os.stat(self.data_directory).st_dev != os.stat(self._expected_mount).st_dev:
            raise ValueError("collector data directory is not on the DATA filesystem")
        # The budget covers all retained generations, not just the current
        # SegmentedCaptureWriter directory. Approved writes are conservatively
        # counted before IO; a failed write may consume budget but never exceed it.
        self._accounted_total_bytes = self._safe_tree_bytes()

    def _safe_tree_bytes(self) -> int:
        """Reject links, reparse points, devices and nested mounts before accounting."""
        total = 0
        expected_device = os.stat(self._expected_mount).st_dev
        pending = [self.data_directory]
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    info = entry.stat(follow_symlinks=False)
                    if (
                        entry.is_symlink()
                        or bool(
                            getattr(info, "st_file_attributes", 0)
                            & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
                        )
                        or (os.name != "nt" and info.st_dev != expected_device)
                    ):
                        raise ValueError("collector DATA contains unsafe link/reparse/mount")
                    if stat.S_ISDIR(info.st_mode):
                        pending.append(Path(entry.path))
                    elif stat.S_ISREG(info.st_mode) and (
                        os.name == "nt" or info.st_nlink == 1
                    ):
                        total += info.st_size
                    else:
                        raise ValueError("collector DATA contains non-regular or hardlinked entry")
        return total

    @property
    def tripped(self) -> bool:
        return self._tripped

    @property
    def accounted_total_bytes(self) -> int:
        return self._accounted_total_bytes

    def reconcile(self) -> int:
        """Reset conservative reservations to the files actually retained on DATA."""

        if self._tripped:
            raise CaptureStorageCapacityError("collector disk budget already tripped")
        actual = self._safe_tree_bytes()
        disk_usage = shutil.disk_usage(self.data_directory)
        if actual + self.emergency_reserve_bytes > self.maximum_total_bytes:
            self._trip("hard data cap reached during reconciliation")
        if disk_usage.free < self.minimum_free_bytes:
            self._trip("DATA free-space floor reached during reconciliation")
        self._accounted_total_bytes = actual
        return actual

    def ensure_capacity(self, current_bytes: int, incoming_bytes: int) -> None:
        """Check both the total capture cap and filesystem free-space floor."""

        if self._tripped:
            raise CaptureStorageCapacityError("collector disk budget already tripped")
        if current_bytes < 0 or incoming_bytes < 0:
            raise ValueError("disk usage byte counts cannot be negative")
        disk_usage = shutil.disk_usage(self.data_directory)
        candidate = max(self._accounted_total_bytes, current_bytes) + incoming_bytes
        over_total_cap = candidate + self.emergency_reserve_bytes > self.maximum_total_bytes
        below_free_floor = disk_usage.free - incoming_bytes < self.minimum_free_bytes
        if over_total_cap or below_free_floor:
            reason = "hard data cap reached" if over_total_cap else "DATA free-space floor reached"
            self._trip(reason)
        self._accounted_total_bytes = candidate

    def _trip(self, reason: str) -> None:
        self._tripped = True
        try:
            self._event_depth_shed()
        except Exception:
            LOGGER.exception("collector depth shedding callback failed")
        LOGGER.critical("collector disk guard tripped (%s); all writes stopped", reason)
        raise CaptureStorageCapacityError(f"collector disk guard tripped: {reason}")
