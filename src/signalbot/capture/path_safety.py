from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from pathlib import Path

FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400


@dataclass(frozen=True, slots=True)
class LinkFreePathInspection:
    """A fail-closed lstat snapshot of a path and all of its present components."""

    absolute_path: Path
    final_status: os.stat_result | None
    first_missing_component: Path | None


def is_link_or_reparse_status(status: os.stat_result) -> bool:
    """Recognize POSIX links and every Windows reparse-point class, including junctions."""

    file_attributes = int(getattr(status, "st_file_attributes", 0))
    return stat.S_ISLNK(status.st_mode) or bool(file_attributes & FILE_ATTRIBUTE_REPARSE_POINT)


def inspect_link_free_path(
    path: str | Path,
    field: str,
    *,
    allow_missing_tail: bool = False,
) -> LinkFreePathInspection:
    """Inspect every path component with lstat and reject links/reparse points.

    A missing tail is allowed only for callers that intentionally accept a not-yet-created
    output path. Any other inspection error is converted to a fail-closed ``ValueError``.
    ``lstat`` is deliberate: it also exposes broken symbolic links instead of following them.
    """

    absolute = Path(os.path.abspath(os.fspath(path)))
    components = [*reversed(absolute.parents), absolute]
    final_status: os.stat_result | None = None
    for component in components:
        try:
            status = os.lstat(component)
        except FileNotFoundError as exc:
            if allow_missing_tail:
                return LinkFreePathInspection(
                    absolute_path=absolute,
                    final_status=None,
                    first_missing_component=component,
                )
            raise ValueError(f"{field} path component does not exist: {component}") from exc
        except OSError as exc:
            raise ValueError(f"{field} path inspection failed at {component}: {exc}") from exc
        if is_link_or_reparse_status(status):
            raise ValueError(
                f"{field} must not contain symbolic-link or reparse-point components: {component}"
            )
        final_status = status
    return LinkFreePathInspection(
        absolute_path=absolute,
        final_status=final_status,
        first_missing_component=None,
    )
