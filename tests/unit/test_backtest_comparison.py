import csv

import pytest

from signalbot.backtest.comparison import (
    OpportunityObservation,
    TradeObservation,
    align_common_opportunities,
    compare_common_opportunity_panels,
    compare_strategy_runs,
    read_opportunity_observations,
)

DAY_MS = 86_400_000


def _observations(long_return: float, short_return: float) -> list[TradeObservation]:
    return [
        item
        for block in range(4)
        for item in (
            TradeObservation("spot", "long", block * DAY_MS + 1, long_return),
            TradeObservation("futures", "short", block * DAY_MS + 1, short_return),
        )
    ]


def _opportunity(
    opportunity_id: str,
    market: str,
    direction: str,
    decision_time_ms: int,
    outcome: float | None,
    *,
    eligible: bool = True,
    available: bool = True,
    analysis_eligible: bool = True,
) -> OpportunityObservation:
    return OpportunityObservation(
        opportunity_id=opportunity_id,
        market=market,
        direction=direction,
        decision_time_ms=decision_time_ms,
        eligible=eligible,
        analysis_eligible=analysis_eligible,
        volume_feature_available=available,
        forward_return_12=outcome,
    )


def _frozen_opportunity_panels() -> dict[str, list[OpportunityObservation]]:
    rows = (
        ("s1", "spot", "long", 1, 0.10),
        ("f1", "futures", "short", 1, 0.20),
        ("s2", "spot", "long", 7 * DAY_MS + 1, 0.30),
        ("f2", "futures", "short", 7 * DAY_MS + 1, 0.40),
    )
    return {
        "C0": [_opportunity(*row) for row in rows],
        "G2": [
            _opportunity(*row, eligible=row[1] == "futures", available=row[1] == "futures")
            for row in rows
        ],
        "G4": [
            _opportunity(*row, eligible=row[1] == "spot", available=row[1] == "spot")
            for row in rows
        ],
    }


def test_paired_comparison_uses_shared_fixed_calendar_blocks() -> None:
    result = compare_strategy_runs(
        {
            "b0": _observations(0.00, 0.00),
            "b3": _observations(0.01, -0.01),
            "b2": _observations(0.02, -0.02),
            "headline": _observations(0.03, -0.03),
        },
        evaluation_start_ms=0,
        evaluation_end_ms=4 * DAY_MS,
        samples=100,
        block_days=1,
        seed=7,
    )

    assert result["calendar_blocks"] == 4
    assert result["family_tests"] == 8
    rows = {
        (row["contrast"], row["market"]): row for row in result["rows"]
    }
    long_delta = rows[("headline_minus_b2", "spot")]
    short_zero = rows[("headline_vs_zero", "futures")]
    assert long_delta["effect"] == pytest.approx(0.01)
    assert long_delta["simultaneous_low"] == pytest.approx(0.01)
    assert short_zero["effect"] == pytest.approx(-0.03)
    assert short_zero["probability_positive"] == 0.0


def test_comparison_rejects_non_frozen_run_order() -> None:
    with pytest.raises(ValueError, match="insertion-ordered"):
        compare_strategy_runs(
            {"headline": [], "b0": [], "b3": [], "b2": []},
            evaluation_start_ms=0,
            evaluation_end_ms=DAY_MS,
            samples=100,
            block_days=1,
            seed=1,
        )


def test_comparison_validates_sample_boundary() -> None:
    with pytest.raises(ValueError, match="at least 100"):
        compare_strategy_runs(
            {"b0": [], "b3": [], "b2": [], "headline": []},
            evaluation_start_ms=0,
            evaluation_end_ms=DAY_MS,
            samples=99,
            block_days=1,
            seed=1,
        )


def test_opportunity_csv_reader_returns_typed_observations(tmp_path) -> None:
    path = tmp_path / "opportunities.csv"
    fields = [
        "opportunity_id",
        "market",
        "direction",
        "decision_time_ms",
        "eligible",
        "analysis_eligible",
        "analysis_eligible_12",
        "volume_feature_available",
        "analysis_eligible_3",
        "analysis_eligible_72",
        "forward_return_3",
        "forward_return_12",
        "forward_return_72",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerow(
            {
                "opportunity_id": "abc",
                "market": "SPOT",
                "direction": "LONG",
                "decision_time_ms": "123",
                "eligible": "True",
                "analysis_eligible": "1",
                "analysis_eligible_12": "True",
                "volume_feature_available": "False",
                "analysis_eligible_3": "False",
                "analysis_eligible_72": "0",
                "forward_return_3": "",
                "forward_return_12": "0.125",
                "forward_return_72": "",
            }
        )

    assert read_opportunity_observations(path) == [
        _opportunity("abc", "spot", "long", 123, 0.125, available=False)
    ]


def test_common_opportunity_alignment_rejects_nonidentical_id_sets() -> None:
    common = _opportunity("common", "spot", "long", 20, 0.1)
    panels = {
        "C0": [_opportunity("c0-only", "spot", "long", 10, 0.2), common],
        "G2": [common, _opportunity("g2-only", "spot", "long", 30, 0.3)],
        "G4": [_opportunity("g4-only", "spot", "long", 40, 0.4), common],
    }

    with pytest.raises(ValueError, match="opportunity_id sets differ"):
        align_common_opportunities(panels)


def test_common_opportunity_alignment_rejects_identity_and_future_outcome_mismatch() -> None:
    baseline = _opportunity("same", "spot", "long", 20, 0.1)
    with pytest.raises(ValueError, match="identity mismatch"):
        align_common_opportunities(
            {
                "C0": [baseline],
                "G2": [baseline],
                "G4": [_opportunity("same", "futures", "short", 20, 0.1)],
            }
        )
    with pytest.raises(ValueError, match="future label mismatch"):
        align_common_opportunities(
            {
                "C0": [baseline],
                "G2": [baseline],
                "G4": [_opportunity("same", "spot", "long", 20, 0.1000001)],
            }
        )


def test_common_opportunity_alignment_rejects_empty_or_different_sets() -> None:
    with pytest.raises(ValueError, match="opportunity_id sets differ"):
        align_common_opportunities(
            {
                "C0": [_opportunity("c0", "spot", "long", 1, 0.1)],
                "G2": [_opportunity("g2", "spot", "long", 1, 0.1)],
                "G4": [_opportunity("g4", "spot", "long", 1, 0.1)],
            }
        )


def test_common_panel_reports_conditional_and_unconditional_h12() -> None:
    result = compare_common_opportunity_panels(
        _frozen_opportunity_panels(),
        evaluation_start_ms=0,
        evaluation_end_ms=14 * DAY_MS,
        samples=100,
        seed=17,
    )

    summaries = {
        (row["variant"], row["market"]): row for row in result["summary_rows"]
    }
    g2_spot = summaries[("G2", "spot")]
    c0_spot = summaries[("C0", "spot")]
    assert result["calendar_days"] == 14
    assert result["blocks_per_draw"] == 2
    assert result["family_tests"] == 4
    assert g2_spot["availability_rate"] == 0.0
    assert g2_spot["conditional_h12_mean"] is None
    assert g2_spot["unconditional_contribution"] == 0.0
    assert c0_spot["conditional_h12_mean"] == pytest.approx(0.20)
    assert c0_spot["unconditional_contribution"] == pytest.approx(0.20)

    contrasts = {
        (row["contrast"], row["market"]): row for row in result["rows"]
    }
    assert contrasts[("G2-C0", "spot")]["effect"] == pytest.approx(-0.20)
    assert contrasts[("G4-C0", "spot")]["effect"] == 0.0
    assert contrasts[("G2-C0", "futures")]["effect"] == 0.0
    assert "p_value" not in str(result)


def test_common_panel_shared_bootstrap_is_seed_deterministic() -> None:
    first = compare_common_opportunity_panels(
        _frozen_opportunity_panels(),
        evaluation_start_ms=0,
        evaluation_end_ms=14 * DAY_MS,
        samples=100,
        seed=1234,
    )
    second = compare_common_opportunity_panels(
        _frozen_opportunity_panels(),
        evaluation_start_ms=0,
        evaluation_end_ms=14 * DAY_MS,
        samples=100,
        seed=1234,
    )

    assert first == second


def test_common_panel_validates_sample_boundary() -> None:
    with pytest.raises(ValueError, match="at least 100"):
        compare_common_opportunity_panels(
            {"C0": [], "G2": [], "G4": []}, samples=99, seed=1
        )
