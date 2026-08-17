import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from conftest import make_candle, make_decision, make_feature
from signalbot.backtest import alert_replay
from signalbot.backtest.alert_filter import (
    alert_filter_snapshot_at,
    compute_alert_filter_series,
)
from signalbot.backtest.alert_replay import (
    RecommendationOutcomeRow,
    _event_row,
    _is_index_recommendation,
    _selection,
    _shared_calendar_bootstrap,
    _status_for_margin,
    _summary_row,
)
from signalbot.backtest.config import BacktestAsset, load_backtest_spec
from signalbot.backtest.engine import ResearchBacktester
from signalbot.config import SignalSettings, load_settings
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import ChartStructureSnapshot
from signalbot.signals.rules import SignalRuleEngine
from signalbot.signals.state_machine import SignalStateMachine

ROOT = Path(__file__).resolve().parents[2]


def _outcome(event_id: str, *, net: float, status: str) -> RecommendationOutcomeRow:
    return RecommendationOutcomeRow(
        event_id=event_id,
        horizon_bars=12,
        horizon_minutes=60,
        evaluable=True,
        exclusion_reason="",
        entry_time_ms=1,
        exit_time_ms=2,
        entry_price=100.0,
        exit_price=101.0,
        raw_close_return=0.01,
        maximum_rise=0.02,
        maximum_drop=-0.01,
        gross_return=net + 0.002,
        slippage_return=0.001,
        fee_return=0.001,
        funding_return=0.0,
        net_return=net,
        mfe=0.02,
        mae=-0.01,
        hit_status_5bps=status,
        hit_status_0bps=status,
        hit_status_10bps=status,
        hit_status_25bps=status,
        one_r_path_status="target_first",
        one_r_target_price=102.0,
        one_r_risk_fraction=0.02,
        observed_until_ms=2,
    )


def _event(*, event_id: str, timestamp_ms: int, informational: bool):
    family = SignalFamily.PULLBACK_LONG if informational else SignalFamily.BREAKOUT_LONG
    stage = SignalStage.SETUP if informational else SignalStage.CONFIRMED
    decision = make_decision(
        event_id=event_id,
        family=family,
        direction=Direction.LONG,
        stage=stage,
        event_time_ms=timestamp_ms,
        metadata={"informational_only": True} if informational else {},
    )
    feature = make_feature(event_time_ms=timestamp_ms)
    return _event_row(
        BacktestAsset(
            asset="BTC",
            cohort="anchor",
            spot_symbol="BTCUSDT",
            futures_symbol="BTCUSDT",
        ),
        decision,
        feature,
        "retrospective_test",
    )


def test_index_recommendations_keep_confirmed_and_information_only_setup_separate() -> None:
    confirmed = make_decision()
    pullback_setup = make_decision(
        family=SignalFamily.PULLBACK_LONG,
        stage=SignalStage.SETUP,
        metadata={"informational_only": True},
    )
    pullback_watch = pullback_setup.model_copy(update={"stage": SignalStage.WATCH})
    ordinary_setup = confirmed.model_copy(update={"stage": SignalStage.SETUP})

    assert _is_index_recommendation(confirmed)
    assert _is_index_recommendation(pullback_setup)
    assert not _is_index_recommendation(pullback_watch)
    assert not _is_index_recommendation(ordinary_setup)


def test_event_row_persists_experimental_indicator_evidence_without_rescoring() -> None:
    candles = []
    for index in range(50):
        close = Decimal(str(100 + index * 0.5))
        candles.append(
            make_candle(index, close=float(close)).model_copy(
                update={
                    "open": close,
                    "high": close + Decimal("0.8"),
                    "low": close - Decimal("0.7"),
                    "close": close,
                }
            )
        )
    last = candles[-1]
    feature = make_feature(
        market=last.market,
        symbol=last.symbol,
        interval=last.interval,
        event_time_ms=last.close_time_ms,
        atr=2.0,
        macd_histogram=0.4,
        macd_histogram_previous=0.1,
        taker_delta_3=0.2,
        volume_zscore=1.5,
        chart_structure=ChartStructureSnapshot(
            pullback_range_ratio=0.6,
            pullback_quote_volume_ratio=0.75,
        ),
    )
    decision = make_decision(
        market=last.market,
        symbol=last.symbol,
        direction=Direction.LONG,
        score=100,
        event_time_ms=last.close_time_ms,
    )
    snapshot = alert_filter_snapshot_at(
        compute_alert_filter_series(candles),
        feature,
        decision.direction,
        len(candles) - 1,
    )

    event = _event_row(
        BacktestAsset(
            asset="BTC",
            cohort="anchor",
            spot_symbol="BTCUSDT",
            futures_symbol="BTCUSDT",
        ),
        decision,
        feature,
        "development",
        snapshot,
    )

    assert event.score == 100
    assert event.protocol_version == "alert_replay_v3_2026-07-20_indicator_discriminator"
    assert event.efficiency_ratio_20 == pytest.approx(1.0)
    assert event.directional_macd_delta_atr == pytest.approx(0.15)
    assert event.directional_taker_delta == pytest.approx(0.2)
    assert event.pullback_range_contraction == pytest.approx(0.4)
    assert event.pullback_volume_contraction == pytest.approx(0.25)


def test_margin_status_has_a_symmetric_ambiguous_zone() -> None:
    assert _status_for_margin(0.0006, 5) == "hit"
    assert _status_for_margin(-0.0006, 5) == "miss"
    assert _status_for_margin(0.0005, 5) == "ambiguous"
    assert _status_for_margin(-0.0005, 5) == "ambiguous"


def test_selection_accepts_declared_split_and_rejects_unknown_name() -> None:
    spec = load_backtest_spec(ROOT / "config" / "backtest.5m.research.yaml")
    selected = _selection(spec, ["retrospective_test"])

    assert selected.split_names == ("retrospective_test",)
    assert selected.start_ms < selected.end_ms
    with pytest.raises(ValueError, match="unknown backtest split"):
        _selection(spec, ["not-a-split"])


def test_strict_hit_rate_keeps_ambiguous_and_unevaluable_events_in_denominator() -> None:
    first = _event(event_id="a", timestamp_ms=1_800_000_000_000, informational=False)
    second = replace(first, event_id="b", decision_time_ms=1_800_000_300_000)
    third = replace(first, event_id="c", decision_time_ms=1_800_000_600_000)
    excluded = replace(
        _outcome("c", net=0.0, status="ambiguous"),
        evaluable=False,
        exclusion_reason="horizon_crosses_split",
        net_return=None,
    )

    summary = _summary_row(
        [
            (first, _outcome("a", net=0.01, status="hit")),
            (second, _outcome("b", net=0.0, status="ambiguous")),
            (third, excluded),
        ],
        evaluation_days=1,
    )

    assert summary["events"] == 3
    assert summary["coverage"] == pytest.approx(2 / 3)
    assert summary["strict_hit_rate"] == pytest.approx(1 / 3)
    assert summary["resolved_accuracy"] == 1.0


def test_shared_calendar_bootstrap_is_deterministic_across_panels() -> None:
    day_ms = 86_400_000
    start_ms = 1_800_000_000_000 // day_ms * day_ms
    events = [
        _event(event_id="paper", timestamp_ms=start_ms, informational=False),
        _event(
            event_id="shadow",
            timestamp_ms=start_ms + day_ms,
            informational=True,
        ),
    ]
    pairs = [
        (events[0], _outcome("paper", net=0.01, status="hit")),
        (events[1], _outcome("shadow", net=-0.01, status="miss")),
    ]
    spec = load_backtest_spec(ROOT / "config" / "backtest.5m.research.yaml")
    selection = _selection(spec, ["retrospective_test"])
    shifted = replace(
        selection,
        start_ms=start_ms,
        end_ms=start_ms + 2 * day_ms,
    )

    first = _shared_calendar_bootstrap(
        pairs,
        shifted,
        samples=100,
        seed=7,
        block_days=1,
    )
    second = _shared_calendar_bootstrap(
        pairs,
        shifted,
        samples=100,
        seed=7,
        block_days=1,
    )

    assert first == second
    assert first["shared_draw_schedule_sha256"]


def test_replay_uses_spec_rule_contract_across_gap_reset_and_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_spec = load_backtest_spec(ROOT / "config" / "backtest.5m.highvol-direction.yaml")
    spec = base_spec.model_copy(
        update={
            "assets": [base_spec.assets[0]],
            "bootstrap": base_spec.bootstrap.model_copy(update={"samples": 100}),
            "costs": base_spec.costs.model_copy(update={"include_funding": False}),
            "rule_version": "spec-derived-v9",
        }
    )
    settings = load_settings(ROOT / "config" / "settings.example.yaml").model_copy(
        update={"rule_version": "settings-must-not-own-replay"}
    )
    expected = ResearchBacktester(settings, spec).rule_settings
    assert settings.signals.gate_use_participation is False
    assert settings.signals.gate_use_crowding is False
    assert expected.gate_use_participation is True
    assert expected.gate_use_crowding is True

    rule_settings_seen: list[SignalSettings] = []
    state_contracts_seen: list[tuple[SignalSettings, str]] = []

    class RecordingRuleEngine(SignalRuleEngine):
        def __init__(self, rule_settings: SignalSettings) -> None:
            rule_settings_seen.append(rule_settings)
            super().__init__(rule_settings)

    class RecordingStateMachine(SignalStateMachine):
        def __init__(
            self,
            rule_settings: SignalSettings,
            rule_version: str,
        ) -> None:
            state_contracts_seen.append((rule_settings, rule_version))
            super().__init__(rule_settings, rule_version)

    selection = _selection(spec, ["retrospective_test"])
    first_open_ms = selection.start_ms - 10 * 86_400_000
    second_open_ms = selection.start_ms

    def read_dataset(path: str | Path) -> SimpleNamespace:
        market = Market(Path(path).parent.name)
        symbol = (
            spec.assets[0].spot_symbol if market is Market.SPOT else spec.assets[0].futures_symbol
        )
        candles = tuple(
            make_candle(0, market=market, symbol=symbol).model_copy(
                update={
                    "open_time_ms": open_time_ms,
                    "close_time_ms": open_time_ms + 300_000 - 1,
                }
            )
            for open_time_ms in (first_open_ms, second_open_ms)
        )
        return SimpleNamespace(candles=candles)

    def verify_dataset(*_args: object, **_kwargs: object) -> None:
        return None

    def dataset_manifest(path: str | Path) -> SimpleNamespace:
        return SimpleNamespace(sha256=f"fixture:{Path(path).as_posix()}")

    monkeypatch.setattr(alert_replay, "SignalRuleEngine", RecordingRuleEngine)
    monkeypatch.setattr(alert_replay, "SignalStateMachine", RecordingStateMachine)
    monkeypatch.setattr(alert_replay, "read_kline_csv", read_dataset)
    monkeypatch.setattr(alert_replay, "verify_dataset_manifest", verify_dataset)
    monkeypatch.setattr(alert_replay, "build_dataset_manifest", dataset_manifest)

    spec_path = tmp_path / "spec.yaml"
    config_path = tmp_path / "settings.yaml"
    spec_path.write_text("fixture: spec\n", encoding="utf-8")
    config_path.write_text("fixture: settings\n", encoding="utf-8")
    output_dir = tmp_path / "output"
    alert_replay.run_alert_replay(
        settings,
        spec,
        tmp_path / "data",
        output_dir,
        workspace_root=tmp_path,
        spec_path=spec_path,
        config_path=config_path,
        split_names=["retrospective_test"],
    )

    assert rule_settings_seen == [expected, expected]
    assert state_contracts_seen == [
        (expected, spec.rule_version),
        (expected, spec.rule_version),
        (expected, spec.rule_version),
        (expected, spec.rule_version),
    ]
    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert results["rule_version"] == spec.rule_version
    assert manifest["rule_version"] == spec.rule_version
    assert results["evaluation_contract"]["horizons_bars"] == [1, 3, 6, 12, 72]
    assert set(results["horizons_by_panel"]) == {"1", "3", "6", "12", "72"}
    report = (output_dir / "report_ko.md").read_text(encoding="utf-8")
    assert "## 구간별 비용 반영 결과" in report
    assert "5분(1봉)" in report
    assert f"고정 {len(spec.assets)}종 연구 universe" in report
    assert "고정 8종 연구 universe" not in report
    assert "기술적 청산" in report
