"""Boundary tests for the frozen INFRA_SMOKE qualification contract."""

from __future__ import annotations

from typing import Any, cast

from signalbot.prospective.smoke_audit import _qualification_report


class _FakeRepository:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def list_shadow_coverage(self, *, campaign_id: str, campaign_manifest_sha256: str):
        return self._rows


_MANIFEST = "a" * 64
_CAMPAIGN = "campaign"


def _cell(market: str, close_ms: int, status: str = "SEALED", complete: int = 1):
    return {
        "market": market,
        "decision_close_ms": close_ms,
        "status": status,
        "complete": complete,
    }


def _cells(markets: list[str], counts: list[int], *, start_ms: int = 0) -> list[dict[str, Any]]:
    rows = []
    for market, count in zip(markets, counts, strict=True):
        for i in range(count):
            rows.append(_cell(market, start_ms + i * 300_000))
    return rows


def test_empty_campaign_is_not_run() -> None:
    repo = _FakeRepository([])
    report = _qualification_report(
        cast(Any, repo),
        campaign_id=_CAMPAIGN,
        manifest_sha256=_MANIFEST,
        markets=["spot", "futures"],
        integrity_errors=[],
    )
    assert report["infra_smoke"] == "NOT_RUN"


def test_seventeen_consecutive_cells_is_insufficient() -> None:
    repo = _FakeRepository(_cells(["spot", "futures"], [17, 17]))
    report = _qualification_report(
        cast(Any, repo),
        campaign_id=_CAMPAIGN,
        manifest_sha256=_MANIFEST,
        markets=["spot", "futures"],
        integrity_errors=[],
    )
    assert report["infra_smoke"] == "INCONCLUSIVE_INSUFFICIENT_RUNTIME"


def test_eighteen_consecutive_cells_per_market_passes() -> None:
    repo = _FakeRepository(_cells(["spot", "futures"], [18, 18]))
    report = _qualification_report(
        cast(Any, repo),
        campaign_id=_CAMPAIGN,
        manifest_sha256=_MANIFEST,
        markets=["spot", "futures"],
        integrity_errors=[],
    )
    assert report["infra_smoke"] == "PASS"


def test_gap_splitting_runs_fails_even_with_nineteen_cells() -> None:
    cells = [
        _cell("futures", i * 300_000) for i in range(10)
    ] + [
        # gap of one missing cell splits the run
        _cell("futures", (11 + i) * 300_000) for i in range(9)
    ]
    cells += [_cell("spot", i * 300_000) for i in range(18)]
    repo = _FakeRepository(cells)
    report = _qualification_report(
        cast(Any, repo),
        campaign_id=_CAMPAIGN,
        manifest_sha256=_MANIFEST,
        markets=["spot", "futures"],
        integrity_errors=[],
    )
    assert report["infra_smoke"] == "INCONCLUSIVE_INSUFFICIENT_RUNTIME"
    assert report["maximum_consecutive_primary_cells_by_market"]["futures"] == 10


def test_spot_eighteen_futures_seventeen_does_not_pass() -> None:
    repo = _FakeRepository(_cells(["spot", "futures"], [18, 17]))
    report = _qualification_report(
        cast(Any, repo),
        campaign_id=_CAMPAIGN,
        manifest_sha256=_MANIFEST,
        markets=["spot", "futures"],
        integrity_errors=[],
    )
    assert report["infra_smoke"] == "INCONCLUSIVE_INSUFFICIENT_RUNTIME"


def test_open_cell_after_shutdown_fails() -> None:
    cells = _cells(["spot", "futures"], [18, 18])
    cells.append(_cell("spot", 99 * 300_000, status="OPEN"))
    repo = _FakeRepository(cells)
    report = _qualification_report(
        cast(Any, repo),
        campaign_id=_CAMPAIGN,
        manifest_sha256=_MANIFEST,
        markets=["spot", "futures"],
        integrity_errors=[],
    )
    assert report["infra_smoke"] == "FAIL"


def test_integrity_p0_fails_qualification() -> None:
    repo = _FakeRepository(_cells(["spot", "futures"], [18, 18]))
    report = _qualification_report(
        cast(Any, repo),
        campaign_id=_CAMPAIGN,
        manifest_sha256=_MANIFEST,
        markets=["spot", "futures"],
        integrity_errors=["hash mismatch"],
    )
    assert report["infra_smoke"] == "FAIL"
