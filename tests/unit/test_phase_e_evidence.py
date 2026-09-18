from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.data.microstructure import BookState
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import (
    BookTicker,
    Candle,
    ComparatorCandidate,
    FeatureSnapshot,
    MarketRegime,
    ObservedBboSnapshot,
)
from signalbot.persistence.repository import SqlRepository
from signalbot.prospective.observer import ShadowObserver, build_observation_payload
from signalbot.prospective.retest import (
    arm_from_candidate,
    build_ready_snapshot,
    serialize_lifecycle,
)
from signalbot.prospective.retest_observer import CausalRetestObserver
from signalbot.prospective.retest_outcomes import (
    OutcomeStatus,
    RetestOutcomePolicy,
    RetestReference,
    evaluate_reference,
    reference_from_raw_observation,
)
from signalbot.prospective.retest_parity import RetestParityCase, run_retest_adapter_parity
from signalbot.prospective.retest_replay import (
    CausalRetestReplayAdapter,
    CausalRetestReplayInput,
    ReplayBar,
    RepositoryLiveLikeAdapter,
)
from signalbot.prospective.source_freeze import freeze_source
from signalbot.signals.rules import SignalRuleEngine


def _book() -> BookTicker:
    return BookTicker(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=1_000,
        exchange_event_time_ms=999,
        receipt_time_ms=1_005,
        bid_price=Decimal("100.00"),
        bid_quantity=Decimal("2.50"),
        ask_price=Decimal("100.10"),
        ask_quantity=Decimal("3.00"),
        update_id=7,
    )


def _candle(open_ms: int, close: str, high: str, low: str) -> Candle:
    return Candle(
        market=Market.SPOT,
        symbol="BTCUSDT",
        interval="5m",
        open_time_ms=open_ms,
        close_time_ms=open_ms + 299_999,
        open=Decimal(close),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=10,
        taker_buy_base_volume=Decimal("5"),
        taker_buy_quote_volume=Decimal("500"),
    )


def test_one_book_snapshot_preserves_exact_raw_bbo_and_derived_fields() -> None:
    state = BookState()
    state.update(_book())
    snapshot = state.snapshot(Market.SPOT, "BTCUSDT", as_of_ms=1_005, maximum_age_ms=2_000)
    assert snapshot is not None
    assert snapshot.observed_bbo.bid_price == Decimal("100.00")
    assert snapshot.observed_bbo.ask_quantity == Decimal("3.00")
    assert snapshot.observed_bbo.update_id == 7
    assert snapshot.observed_bbo.age_ms == snapshot.age_ms == 0
    assert snapshot.bid_quote_capacity == 250.0
    assert snapshot.ask_quote_capacity == 300.3


def test_v2_raw_c0_payload_freezes_exact_bbo_and_directional_reference() -> None:
    settings = Settings.model_validate(
        {
            "binance": {"markets": ["spot"], "top_n": 1, "surveillance_n": 1,
                        "intervals": ["5m", "15m", "1h"], "primary_interval": "5m"},
            "signals": {"entry_policy": "r2_pit_htf_exec", "gate_enabled": True},
            "storage": {"url": "sqlite:///:memory:"},
            "shadow": {
                "observation_enabled": True,
                "observation_schema_version": "shadow_observation_v2",
                "campaign_id": "smoke-v2",
                "source_identity": "test",
                "campaign_created_at_ms": 1,
            },
        }
    )
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    observer = ShadowObserver(settings, SignalRuleEngine(settings.signals, settings.shadow), repo,
                             clock=ReplayClock(1_000))
    feature = FeatureSnapshot(
        market=Market.SPOT, symbol="BTCUSDT", interval="5m", event_time_ms=1_000,
        price=100.0, previous_close=99.0, ema9=100.0, ema20=99.0, ema50=98.0,
        rsi=50.0, rsi_previous=49.0, macd_histogram=1.0,
        macd_histogram_previous=0.5, macd_histogram_previous2=0.1, atr=1.0,
        atr_percent=1.0, adx=25.0, bollinger_width=1.0,
        bollinger_width_percentile=50.0, relative_volume=2.0, recent_high=99.5,
        recent_low=95.0, upper_wick_ratio=0.1, lower_wick_ratio=0.1,
        bearish_divergence=False, bullish_divergence=False, taker_buy_ratio=0.5,
        spread_bps=2.0, book_age_ms=10, bid_quote_capacity=1000.0,
        ask_quote_capacity=1000.0,
        observed_bbo=ObservedBboSnapshot(
            bid_price=Decimal("99.9"), bid_quantity=Decimal("10"),
            ask_price=Decimal("100.1"), ask_quantity=Decimal("10"),
            exchange_event_time_ms=990, receipt_time_ms=995, update_id=9, age_ms=5,
        ), regime=MarketRegime(),
    )
    candidate = ComparatorCandidate(
        market=Market.SPOT, symbol="BTCUSDT", family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG, decision_time_ms=1_000, primary_interval="5m",
        raw_c0_triggered=True, raw_score=80, r2_passed=False,
        shadow_passed=True,
    )
    payload = build_observation_payload(observer, candidate, feature, {}, "opp")
    evidence = payload["execution_evidence"]
    assert evidence["observed_bbo"]["ask_price"] == "100.1"
    assert evidence["executable_bbo_reference_price"] == 100.1


def test_ready_snapshot_uses_ask_for_long_and_bid_for_short() -> None:
    feature = FeatureSnapshot(
        market=Market.SPOT, symbol="BTCUSDT", interval="5m", event_time_ms=2_000,
        price=100.0, previous_close=99.0, ema9=100.0, ema20=99.0, ema50=98.0,
        rsi=50.0, rsi_previous=49.0, macd_histogram=1.0,
        macd_histogram_previous=0.5, macd_histogram_previous2=0.1, atr=1.0,
        atr_percent=1.0, adx=25.0, bollinger_width=1.0,
        bollinger_width_percentile=50.0, relative_volume=2.0, recent_high=99.5,
        recent_low=95.0, upper_wick_ratio=0.1, lower_wick_ratio=0.1,
        bearish_divergence=False, bullish_divergence=False, taker_buy_ratio=0.5,
        spread_bps=2.0, book_age_ms=10, bid_quote_capacity=1000.0,
        ask_quote_capacity=1000.0,
        observed_bbo=ObservedBboSnapshot(
            bid_price=Decimal("99.9"), bid_quantity=Decimal("10"),
            ask_price=Decimal("100.1"), ask_quantity=Decimal("10"), age_ms=5,
        ), regime=MarketRegime(),
    )
    contexts = {
        "15m": feature.model_copy(update={"interval": "15m", "event_time_ms": 1_000}),
        "1h": feature.model_copy(update={"interval": "1h", "event_time_ms": 1_000}),
    }
    long_candidate = ComparatorCandidate(
        market=Market.SPOT, symbol="BTCUSDT", family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG, decision_time_ms=1_000, primary_interval="5m",
        raw_c0_triggered=True, raw_score=80, r2_passed=False, shadow_passed=True,
    )
    settings = Settings.model_validate({"signals": {"maximum_spread_bps": 15}})
    long_lifecycle = arm_from_candidate(
        long_candidate, feature.model_copy(update={"event_time_ms": 1_000}),
        campaign_id="c", campaign_manifest_sha256="m" * 64,
        opportunity_id="long", retest_horizon_bars=72,
    )
    ready = build_ready_snapshot(long_lifecycle, feature, contexts, settings, bar_close_ms=2_000)
    assert ready.executable_bbo_reference_price == 100.1
    assert ready.bbo.raw_bbo is not None
    assert ready.bbo.raw_bbo.ask_price == Decimal("100.1")
    long_lifecycle.ready_snapshot = ready
    json.dumps(serialize_lifecycle(long_lifecycle))
    futures_feature = feature.model_copy(update={"market": Market.FUTURES, "recent_low": 101.0})
    futures_contexts = {
        key: value.model_copy(update={"market": Market.FUTURES})
        for key, value in contexts.items()
    }
    short_candidate = long_candidate.model_copy(
        update={"market": Market.FUTURES, "direction": Direction.SHORT,
                "family": SignalFamily.BREAKDOWN_SHORT}
    )
    short_lifecycle = arm_from_candidate(
        short_candidate, futures_feature.model_copy(update={"event_time_ms": 1_000}),
        campaign_id="c", campaign_manifest_sha256="m" * 64,
        opportunity_id="short", retest_horizon_bars=72,
    )
    short_ready = build_ready_snapshot(
        short_lifecycle,
        futures_feature,
        futures_contexts,
        settings,
        bar_close_ms=2_000,
    )
    assert short_ready.executable_bbo_reference_price == 99.9


def test_source_freeze_is_deterministic_and_content_sensitive() -> None:
    root = Path(__file__).parents[2]
    first = freeze_source(root)
    second = freeze_source(root)
    assert first.source_root_sha256 == second.source_root_sha256
    assert [item.path for item in first.files] == sorted(item.path for item in first.files)


def test_source_freeze_changes_when_one_source_byte_changes(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    source = tmp_path / "src" / "signalbot"
    source.mkdir(parents=True)
    target = source / "__init__.py"
    target.write_text("VALUE = 1\n", encoding="utf-8")
    first = freeze_source(tmp_path)
    target.write_text("VALUE = 2\n", encoding="utf-8")
    second = freeze_source(tmp_path)
    assert first.source_root_sha256 != second.source_root_sha256


def test_source_freeze_rejects_missing_runtime_source(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    with pytest.raises(FileNotFoundError):
        freeze_source(tmp_path)


def test_source_freeze_rejects_unsafe_required_file(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    source = tmp_path / "src" / "signalbot"
    source.mkdir(parents=True)
    (source / "__init__.py").write_text("\n", encoding="utf-8")
    try:
        (source / "unsafe.py").symlink_to(tmp_path / "uv.lock")
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
    with pytest.raises(ValueError, match=r"symlink|reparse"):
        freeze_source(tmp_path)


def test_prospective_retest_runtime_freeze_and_candle_persistence_guards() -> None:
    root = Path(__file__).parents[2]
    source_identity = freeze_source(root).source_identity

    def settings_for(
        identity: str,
        *,
        persist_candles: bool = True,
        campaign_mode: str = "prospective",
        observation_schema_version: str = "shadow_observation_v2",
    ) -> Settings:
        return Settings.model_validate(
            {
                "binance": {
                    "markets": ["spot"],
                    "top_n": 1,
                    "surveillance_n": 1,
                    "intervals": ["5m", "15m", "1h"],
                    "primary_interval": "5m",
                },
                "signals": {
                    "entry_policy": "r2_pit_htf_exec",
                    "confirmation_mode": "explicit_trigger",
                    "gate_enabled": True,
                },
                "runtime": {
                    "persist_candles": persist_candles,
                    "record_raw_events": True,
                },
                "shadow": {
                    "observation_enabled": True,
                    "observation_schema_version": observation_schema_version,
                    "campaign_mode": campaign_mode,
                    "campaign_id": (
                        "freeze-guard"
                        if campaign_mode == "prospective"
                        else "smoke-freeze-guard"
                    ),
                    "source_identity": identity,
                    "campaign_created_at_ms": 1,
                    "activation_ms": 2,
                    "retest_observation_enabled": True,
                },
            }
        )

    mismatch = settings_for("worktree-source-v1:" + "0" * 64)
    mismatch_repo = SqlRepository("sqlite:///:memory:")
    mismatch_repo.initialize()
    with pytest.raises(RuntimeError, match="source freeze mismatch"):
        ShadowObserver(
            mismatch,
            SignalRuleEngine(mismatch.signals, mismatch.shadow),
            mismatch_repo,
            clock=ReplayClock(1),
        )
    assert mismatch_repo.get_shadow_campaign("freeze-guard") is None

    exact = settings_for(source_identity)
    exact_repo = SqlRepository("sqlite:///:memory:")
    exact_repo.initialize()
    ShadowObserver(
        exact,
        SignalRuleEngine(exact.signals, exact.shadow),
        exact_repo,
        clock=ReplayClock(1),
    )
    assert exact_repo.get_shadow_campaign("freeze-guard") is not None

    with pytest.raises(ValueError, match=r"runtime\.persist_candles"):
        settings_for(source_identity, persist_candles=False)

    with pytest.raises(ValueError, match="shadow_observation_v2"):
        settings_for(source_identity, observation_schema_version="shadow_observation_v1")

    smoke = settings_for(source_identity, campaign_mode="smoke")
    smoke_repo = SqlRepository("sqlite:///:memory:")
    smoke_repo.initialize()
    ShadowObserver(
        smoke,
        SignalRuleEngine(smoke.signals, smoke.shadow),
        smoke_repo,
        clock=ReplayClock(1),
    )
    assert smoke_repo.get_shadow_campaign("smoke-freeze-guard") is not None

    with pytest.raises(ValueError, match="worktree-source-v1"):
        settings_for("not-a-source-id", campaign_mode="smoke")

    wrong_smoke = settings_for(
        "worktree-source-v1:" + "0" * 64,
        campaign_mode="smoke",
    )
    wrong_repo = SqlRepository("sqlite:///:memory:")
    wrong_repo.initialize()
    with pytest.raises(RuntimeError, match="source freeze mismatch"):
        ShadowObserver(
            wrong_smoke,
            SignalRuleEngine(wrong_smoke.signals, wrong_smoke.shadow),
            wrong_repo,
            clock=ReplayClock(1),
        )
    assert wrong_repo.get_shadow_campaign("smoke-freeze-guard") is None


def test_v1_observation_cannot_masquerade_as_exact_bbo() -> None:
    with pytest.raises(ValueError, match="shadow_observation_v2"):
        reference_from_raw_observation(
            {
                "provenance": {"observation_schema_version": "shadow_observation_v1"},
                "common_causal_input": {},
                "execution_evidence": {},
                "shadow": {},
            },
            campaign_id="c",
            campaign_manifest_sha256="m" * 64,
            retest_policy_sha256="r" * 64,
        )


def _reference(direction: Direction, bbo: float | None = 99.5) -> RetestReference:
    raw = ObservedBboSnapshot(
        bid_price=Decimal("99.5"),
        bid_quantity=Decimal("2"),
        ask_price=Decimal("100.5"),
        ask_quantity=Decimal("2"),
        receipt_time_ms=1,
        age_ms=10,
    )
    return RetestReference(
        opportunity_id="opp",
        campaign_id="camp",
        campaign_manifest_sha256="m" * 64,
        retest_policy_sha256="r" * 64,
        reference_kind="RAW_C0",
        direction=direction,
        market=Market.SPOT,
        symbol="BTCUSDT",
        reference_decision_time_ms=0,
        signal_close_reference_price=100.0,
        executable_bbo_reference_price=bbo,
        observed_bbo=raw if bbo is not None else None,
    )


def test_outcome_excludes_reference_bar_and_reports_frozen_horizons() -> None:
    step = 300_000
    candles = [_candle(step * i, "101", "103", "99") for i in range(1, 13)]
    outcomes = evaluate_reference(_reference(Direction.LONG), candles)
    assert [row.horizon_bars for row in outcomes] == [1, 3, 6, 12]
    assert all(row.status is OutcomeStatus.COMPLETE for row in outcomes)
    assert outcomes[0].time_to_mfe_bars == 1
    assert outcomes[0].directional_mfe == pytest.approx(0.03)
    bbo_return = outcomes[0].bbo_entry_terminal_return
    adjusted_return = outcomes[0].cost_model_adjusted_research_return
    assert bbo_return == pytest.approx(101 / 99.5 - 1)
    assert adjusted_return is not None
    assert bbo_return is not None
    assert adjusted_return < bbo_return


def test_outcome_policy_rejects_post_hoc_horizon_change() -> None:
    with pytest.raises(ValueError, match="horizons are frozen"):
        RetestOutcomePolicy(horizons_bars=(1, 2, 3, 4))


def test_outcome_missing_bar_is_explicit_data_gap_and_short_is_symmetric() -> None:
    step = 300_000
    candles = [_candle(step, "99", "100", "97"), _candle(step * 3, "98", "99", "95")]
    outcomes = evaluate_reference(_reference(Direction.SHORT, bbo=None), candles)
    assert outcomes[0].status is OutcomeStatus.COMPLETE
    assert outcomes[0].directional_mfe == pytest.approx(0.03)
    assert outcomes[1].status is OutcomeStatus.EXCLUDED
    assert outcomes[1].reason == "DATA_GAP"
    assert outcomes[1].descriptive_terminal_return is None


def test_outcome_discards_partial_bar_after_bbo_receipt_and_records_effective_time() -> None:
    base = _reference(Direction.LONG)
    assert base.observed_bbo is not None
    reference = replace(
        base,
        reference_decision_time_ms=299_999,
        observed_bbo=base.observed_bbo.model_copy(update={"receipt_time_ms": 300_005}),
    )
    step = 300_000
    candles = [
        _candle(step, "101", "103", "99"),
        *[_candle(step * i, "102", "104", "100") for i in range(2, 14)],
    ]
    outcomes = evaluate_reference(reference, candles)
    assert outcomes[0].status is OutcomeStatus.COMPLETE
    assert outcomes[0].effective_reference_time_ms == 300_005
    assert outcomes[0].observed_until_ms == step * 2 + 299_999
    assert outcomes[0].bbo_unavailable_reason is None


def test_outcome_exact_effective_boundary_is_strict_and_missing_receipt_is_fail_closed() -> None:
    base = _reference(Direction.LONG)
    assert base.observed_bbo is not None
    boundary = replace(
        base,
        reference_decision_time_ms=299_999,
        observed_bbo=base.observed_bbo.model_copy(
            update={"receipt_time_ms": 600_000}
        ),
    )
    candles = [
        _candle(300_000, "101", "103", "99"),
        _candle(600_000, "102", "104", "100"),
        *[_candle(300_000 * i, "102", "104", "100") for i in range(3, 14)],
    ]
    boundary_outcomes = evaluate_reference(boundary, candles)
    assert boundary_outcomes[0].effective_reference_time_ms == 600_000
    assert boundary_outcomes[0].observed_until_ms == 900_000 + 299_999

    missing_receipt = replace(
        base,
        observed_bbo=base.observed_bbo.model_copy(update={"receipt_time_ms": None}),
    )
    missing_outcomes = evaluate_reference(missing_receipt, [_candle(300_000, "101", "103", "99")])
    assert missing_outcomes[0].bbo_entry_terminal_return is None
    assert missing_outcomes[0].bbo_unavailable_reason == "BBO_RECEIPT_TIME_UNAVAILABLE"
    assert missing_outcomes[0].descriptive_terminal_return == pytest.approx(0.01)


def test_independent_replay_adapter_matches_durable_live_like_output() -> None:
    bbo = ObservedBboSnapshot(
        bid_price=Decimal("99.9"), bid_quantity=Decimal("10"),
        ask_price=Decimal("100.1"), ask_quantity=Decimal("10"), age_ms=5,
    )
    arm_feature = FeatureSnapshot(
        market=Market.SPOT, symbol="BTCUSDT", interval="5m", event_time_ms=1_000,
        price=106.0, previous_close=104.0, ema9=105.0, ema20=102.0, ema50=99.0,
        rsi=50.0, rsi_previous=49.0, macd_histogram=1.0,
        macd_histogram_previous=0.5, macd_histogram_previous2=0.1, atr=2.0,
        atr_percent=2.0, adx=25.0, bollinger_width=1.0,
        bollinger_width_percentile=50.0, relative_volume=2.0, recent_high=105.0,
        recent_low=95.0, upper_wick_ratio=0.1, lower_wick_ratio=0.1,
        bearish_divergence=False, bullish_divergence=False, taker_buy_ratio=0.5,
        spread_bps=2.0, book_age_ms=10, bid_quote_capacity=1000.0,
        ask_quote_capacity=1000.0, observed_bbo=bbo, regime=MarketRegime(),
    )
    touch = arm_feature.model_copy(update={"event_time_ms": 2_000, "price": 104.0})
    recover = arm_feature.model_copy(update={"event_time_ms": 3_000, "price": 106.0})
    contexts = {
        "15m": arm_feature.model_copy(update={"interval": "15m", "event_time_ms": 1_500}),
        "1h": arm_feature.model_copy(update={"interval": "1h", "event_time_ms": 1_500}),
    }
    candidate = ComparatorCandidate(
        market=Market.SPOT, symbol="BTCUSDT", family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG, decision_time_ms=1_000, primary_interval="5m",
        raw_c0_triggered=True, raw_score=80, r2_passed=False, shadow_passed=True,
    )
    settings = Settings.model_validate({"signals": {"maximum_spread_bps": 15}})
    item = CausalRetestReplayInput(
        candidate=candidate, arm_feature=arm_feature, settings=settings,
        campaign_id="c", campaign_manifest_sha256="m" * 64,
        retest_horizon_bars=72,
        bars=(ReplayBar(touch, contexts, 2_000), ReplayBar(recover, contexts, 3_000)),
    )
    case = RetestParityCase(
        opportunity_id="opp", input_payload={},
        historical_decision_time_bbo={"bid": "99.9", "ask": "100.1"},
    )
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    repo.register_shadow_campaign(
        campaign_schema_version="shadow_campaign_v1",
        campaign_id="c",
        campaign_mode="smoke",
        source_identity="test-source",
        rule_version="test-rule",
        policy_name="r2_pit_htf_exec",
        policy_version="r2",
        policy_sha256="p" * 64,
        config_sha256="q" * 64,
        observation_schema_version="shadow_observation_v2",
        primary_interval="5m",
        markets=["spot"],
        families={"spot": ["BREAKOUT_LONG"]},
        activation_ms=None,
        created_at_ms=1,
        status="ACTIVE",
    )
    campaign = repo.get_shadow_campaign("c")
    assert campaign is not None
    manifest_sha = campaign["manifest_sha256"]
    item = replace(item, campaign_manifest_sha256=manifest_sha)
    repo.save_shadow_observation(
        observation_id="obs-opp",
        campaign_id="c",
        opportunity_id="opp",
        market="spot",
        symbol="BTCUSDT",
        family="BREAKOUT_LONG",
        direction="long",
        decision_time_ms=1_000,
        primary_interval="5m",
        payload={"opportunity_id": "opp", "source": "independent-live-fixture"},
        policy_sha256="p" * 64,
        campaign_manifest_sha256=manifest_sha,
        created_at_ms=1_000,
    )
    live_settings = settings.model_copy(
        update={"shadow": settings.shadow.model_copy(update={"retest_observation_enabled": True})}
    )
    live_observer = CausalRetestObserver(
        live_settings,
        repo,
        campaign_id="c",
        campaign_manifest_sha256=manifest_sha,
        clock=ReplayClock(1_000),
    )
    live_observer.arm(candidate, arm_feature, opportunity_id="opp")
    live_observer.advance(touch, contexts)
    live_observer.advance(recover, contexts)
    replay = CausalRetestReplayAdapter({"opp": item})
    live = RepositoryLiveLikeAdapter(
        repo, campaign_id="c", campaign_manifest_sha256=manifest_sha
    )
    result = run_retest_adapter_parity([case], live_adapter=live, replay_adapter=replay)
    assert result.status.value == "PASS", result
    assert result.mismatches == ()
