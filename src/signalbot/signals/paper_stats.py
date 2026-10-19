from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

COMPLETED = "completed"
TRACKING_INTERRUPTED = "tracking_interrupted"


def classify_paper_exit(metadata: Mapping[str, Any]) -> str:
    """``tracking_interrupted`` for a data_gap end, ``completed`` for every real exit.

    Works on rows written before and after the labelling change: the old rows have
    only ``exit_reason == "data_gap"``.
    """

    if metadata.get("exit_class") == TRACKING_INTERRUPTED:
        return TRACKING_INTERRUPTED
    if metadata.get("exit_reason") == "data_gap":
        return TRACKING_INTERRUPTED
    return COMPLETED


def summarize_paper_exits(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Counts only. A censored (interrupted) track is never mixed into completed exits.

    No returns, hit rates or other performance figure is computed here.
    """

    completed: Counter[str] = Counter()
    interrupted: Counter[str] = Counter()
    for metadata in rows:
        if metadata.get("paper_only") is not True:
            continue
        reason = str(metadata.get("exit_reason", "unknown"))
        if classify_paper_exit(metadata) == TRACKING_INTERRUPTED:
            interrupted[reason] += 1
        else:
            completed[reason] += 1
    return {
        "completed_total": sum(completed.values()),
        "completed_by_reason": dict(sorted(completed.items())),
        "tracking_interrupted_total": sum(interrupted.values()),
        "tracking_interrupted_by_reason": dict(sorted(interrupted.items())),
        "all_tracks": sum(completed.values()) + sum(interrupted.values()),
    }
