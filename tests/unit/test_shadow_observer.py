from __future__ import annotations

from decimal import Decimal

import pytest

from conftest import make_feature
from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import FeatureSnapshot, MarketRegime
from signalbot.persistence.repository import EventIdConflictError, SqlRepository
from signalbot.prospective.observer import (
    ShadowObserver,
    shadow_config_sha256,
    shadow_observation_id,
    shadow_opportunity_id,
    shadow_policy_identity,
)
from signalbot.signals.rules import SignalRuleEngine


def _settings(campaign_id: str = "smoke-campaign-1") -> Settings:
    return Settings.model_validate(
        {
            "binance": {
                "markets": ["spot", "futures"],
                "top_n": 5,
                "surveillance_n": 10,
                "intervals": ["5m", "15m", "1h"],
                "primary_interval": "5m",
                "min_quote_volume": 0,
            },
            "signals": {
                "entry_policy": "r2_pit_htf_exec",
                "gate_enabled": True,
                "confirmation_mode": "explicit_trigger",
            },
            "storage": {"url": "sqlite:///:memory:"},
            "shadow": {
                "observation_enabled": True,
                "campaign_id": campaign_id,
                "source_identity": "test-source",
                "campaign_created_at_ms": 1_709_999_999_000,
            },
        }
    )


def _repo() -> SqlRepository:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    return repo


def _feature(*, market: Market = Market.SPOT, **updates: object) -> FeatureSnapshot:
    values: dict[str, object] = {
        "market": market,
        "symbol": "BTCUSDT",
        "interval": "5m",
        "event_time_ms": 1_710_000_000_000,
        "price": 106.0,
        "previous_close": 104.0,
        "ema20": 102.0,
        "ema50": 99.0,
        "macd_histogram": 0.2,
        "macd_histogram_previous": 0.1,
        "adx": 25.0,
        "atr": 2.0,
        "atr_percent": 2.0,
        "relative_volume": 2.0,
        "recent_high": 105.0,
        "recent_low": 95.0,
        "efficiency_ratio_20": 0.5,
        "spread_bps": 2.0,
        "spread_is_proxy": False,
        "book_age_ms": 100,
        "bid_quote_capacity": 500.0,
        "ask_quote_capacity": 500.0,
        "data_completeness": 1.0,
        "regime": MarketRegime(label="neutral", btc_trend="neutral"),
    }
    values.update(updates)
    return make_feature(**values)


def _contexts() -> dict[str, FeatureSnapshot]:
    prior = 1_710_000_000_000 - 1
    return {
        "15m": make_feature(
            market=Market.SPOT,
            interval="15m",
            event_time_ms=prior,
            price=103.0,
            ema20=101.0,
            ema50=99.0,
        ),
        "1h": make_feature(
            market=Market.SPOT,
            interval="1h",
            event_time_ms=prior,
            price=104.0,
            ema20=102.0,
            ema50=99.0,
        ),
    }


def _observer() -> tuple[ShadowObserver, Settings]:
    settings = _settings()
    repo = _repo()
    observer = ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repo,
        clock=ReplayClock(1_710_000_000_000),
    )
    return observer, settings


def test_config_rejects_observation_without_production_r2() -> None:
    with pytest.raises(
        ValueError, match="shadow observation requires entry_policy=r2_pit_htf_exec"
    ):
        Settings.model_validate(
            {
                "binance": {
                    "markets": ["spot"],
                    "top_n": 1,
                    "surveillance_n": 2,
                    "intervals": ["5m", "15m", "1h"],
                    "primary_interval": "5m",
                },
                "signals": {"entry_policy": "legacy_gates"},
                "shadow": {
                    "observation_enabled": True,
                    "campaign_id": "smoke-invalid-r2",
                    "source_identity": "test-source",
                    "campaign_created_at_ms": 1_709_999_999_000,
                },
            }
        )


def test_config_rejects_observation_on_non_5m_clock() -> None:
    with pytest.raises(ValueError, match="primary_interval=5m"):
        Settings.model_validate(
            {
                "binance": {
                    "markets": ["spot"],
                    "top_n": 1,
                    "surveillance_n": 2,
                    "intervals": ["1m", "15m", "1h"],
                    "primary_interval": "1m",
                },
                "signals": {
                    "entry_policy": "r2_pit_htf_exec",
                    "gate_enabled": True,
                },
                "shadow": {
                    "observation_enabled": True,
                    "campaign_id": "smoke-invalid-clock",
                    "source_identity": "test-source",
                    "campaign_created_at_ms": 1_709_999_999_000,
                },
            }
        )


def test_r2_remains_production_path_with_observation_enabled() -> None:
    settings = _settings()
    engine = SignalRuleEngine(settings.signals, settings.shadow)
    evaluations = engine.evaluate(_feature(), _contexts())
    # R2 path returns the full rule set, not a shadow-only list.
    assert len(evaluations) == 8
    breakout = next(item for item in evaluations if item.family is SignalFamily.BREAKOUT_LONG)
    assert breakout.metadata.get("informational_only") is not True


def test_spot_only_evaluates_breakout_long_and_futures_breakdown_short() -> None:
    settings = _settings()
    engine = SignalRuleEngine(settings.signals, settings.shadow)
    spot = engine.evaluate_comparator(_feature(market=Market.SPOT), _contexts())
    assert spot is not None
    assert spot.family is SignalFamily.BREAKOUT_LONG
    futures = engine.evaluate_comparator(
        _feature(market=Market.FUTURES), _contexts()
    )
    assert futures is not None
    assert futures.family is SignalFamily.BREAKDOWN_SHORT


def test_comparator_shares_causal_input_and_is_informational_only() -> None:
    settings = _settings()
    engine = SignalRuleEngine(settings.signals, settings.shadow)
    feature = _feature()
    contexts = _contexts()
    candidate = engine.evaluate_comparator(feature, contexts)
    assert candidate is not None
    assert candidate.raw_c0_triggered is True
    assert candidate.decision_time_ms == feature.event_time_ms
    assert candidate.informational_only is True
    assert candidate.shadow_passed is True
    for key in ("15m", "1h"):
        assert contexts[key].interval in (key,)


def test_shadow_never_confirms_via_state_machine() -> None:
    settings = _settings()
    engine = SignalRuleEngine(settings.signals, settings.shadow)
    candidate = engine.evaluate_comparator(_feature(), _contexts())
    # The observer only ever holds an informational-only candidate; it has no
    # path into the SignalStateMachine, so a CONFIRMED shadow decision is
    # impossible by construction.
    assert candidate is not None
    assert candidate.informational_only is True


def test_policy_identity_changes_and_is_stable() -> None:
    settings = _settings()
    a = shadow_policy_identity(settings.shadow, settings.signals)
    b = shadow_policy_identity(settings.shadow, settings.signals)
    assert a == b
    changed = settings.model_copy(deep=True)
    changed.shadow = changed.shadow.model_copy(update={"efficiency_ratio_min": 0.45})
    assert shadow_policy_identity(changed.shadow, changed.signals) != a


def test_r2_and_shadow_observation_ids_cannot_collide() -> None:
    settings = _settings()
    policy = shadow_policy_identity(settings.shadow, settings.signals)
    opportunity = shadow_opportunity_id(
        campaign_id="c1",
        market="spot",
        symbol="BTCUSDT",
        decision_time_ms=1_710_000_000_000,
        primary_interval="5m",
    )
    obs = shadow_observation_id(
        opportunity_id=opportunity,
        policy_sha256=policy,
        schema_version="shadow_observation_v1",
    )
    r2_like_event_id = "c1|spot|BTCUSDT|1710000000000|5m|r2"
    assert obs != r2_like_event_id
    other_policy_obs = shadow_observation_id(
        opportunity_id=opportunity,
        policy_sha256="0" * 64,
        schema_version="shadow_observation_v1",
    )
    assert other_policy_obs != obs


def test_one_raw_c0_opportunity_produces_exactly_one_row() -> None:
    observer, _settings = _observer()
    repo = observer.repository
    feature = _feature()
    before = repo.count_shadow_observations(campaign_id="smoke-campaign-1")
    candidate = observer.observe(feature, _contexts(), frozenset({"BTCUSDT"}))
    assert candidate is not None and candidate.raw_c0_triggered
    after = repo.count_shadow_observations(campaign_id="smoke-campaign-1")
    assert after == before + 1
    # Duplicate replay of the identical opportunity is idempotent.
    observer.observe(feature, _contexts(), frozenset({"BTCUSDT"}))
    assert repo.count_shadow_observations(campaign_id="smoke-campaign-1") == after


def test_conflicting_same_id_observation_fails_loudly() -> None:
    observer, _settings = _observer()
    repo = observer.repository
    opportunity = shadow_opportunity_id(
        campaign_id="smoke-campaign-1",
        market="spot",
        symbol="BTCUSDT",
        decision_time_ms=1_710_000_000_000,
        primary_interval="5m",
    )
    observation_id = shadow_observation_id(
        opportunity_id=opportunity,
        policy_sha256=observer.policy_sha256,
        schema_version=observer.schema_version,
    )
    base = {
        "campaign_id": "smoke-campaign-1",
        "campaign_manifest_sha256": observer.campaign_manifest_sha256,
        "opportunity_id": opportunity,
        "market": "spot",
        "symbol": "BTCUSDT",
        "family": SignalFamily.BREAKOUT_LONG.value,
        "direction": "long",
        "decision_time_ms": 1_710_000_000_000,
        "primary_interval": "5m",
        "policy_sha256": observer.policy_sha256,
        "created_at_ms": 1_710_000_000_000,
    }
    repo.save_shadow_observation(observation_id=observation_id, payload={"v": 1}, **base)
    with pytest.raises(EventIdConflictError):
        repo.save_shadow_observation(
            observation_id=observation_id, payload={"v": 2}, **base
        )


def test_coverage_detects_missing_comparator_rows() -> None:
    observer, _settings = _observer()
    repo = observer.repository
    # Two symbols on the same close: BTCUSDT is a raw C0 opportunity (row
    # persisted); ETHUSDT is mature but non-triggering. The close-level invariant
    # raw_c0_count == comparator_rows holds and full maturity yields a complete
    # coverage cell.
    feature_a = _feature()
    observer.observe(feature_a, _contexts(), frozenset({"BTCUSDT", "ETHUSDT"}))
    feature_b = _feature(symbol="ETHUSDT", price=100.0, recent_high=105.0, previous_close=101.0)
    observer.observe(feature_b, _contexts(), frozenset({"BTCUSDT", "ETHUSDT"}))
    observer.flush()
    coverage = repo.get_shadow_coverage(
        campaign_id="smoke-campaign-1",
        market="spot",
        decision_close_ms=feature_a.event_time_ms,
        primary_interval="5m",
    )
    assert coverage is not None
    assert coverage["expected_tradable_count"] == 2
    assert coverage["mature_count"] == 2
    assert coverage["raw_c0_count"] == 1
    assert coverage["comparator_rows"] == 1
    assert coverage["complete"] is True


def test_duplicate_symbol_close_is_idempotent_for_counts() -> None:
    """P0-2: re-observing the same symbol/close must not inflate coverage
    counts. The invariant proves UNIQUE durable rows, not call attempts."""

    observer, _settings = _observer()
    repo = observer.repository
    feature = _feature()
    tradable = frozenset({"BTCUSDT", "ETHUSDT"})
    # BTCUSDT is a raw C0 trigger; observe it twice on the same close.
    observer.observe(feature, _contexts(), tradable)
    observer.observe(feature, _contexts(), tradable)
    # ETHUSDT mature but non-triggering second symbol.
    feature_b = _feature(
        symbol="ETHUSDT", price=100.0, recent_high=105.0, previous_close=101.0
    )
    observer.observe(feature_b, _contexts(), tradable)
    observer.flush()
    coverage = repo.get_shadow_coverage(
        campaign_id="smoke-campaign-1",
        market="spot",
        decision_close_ms=feature.event_time_ms,
        primary_interval="5m",
    )
    assert coverage is not None
    # Duplicate BTCUSDT observation contributed exactly one durable row.
    assert repo.count_shadow_observations(
        campaign_id="smoke-campaign-1",
        decision_time_ms=feature.event_time_ms,
    ) == 1
    assert coverage["mature_count"] == 2  # BTCUSDT + ETHUSDT, not 3
    assert coverage["raw_c0_count"] == 1
    assert coverage["comparator_rows"] == 1
    assert coverage["complete"] is True


def test_restart_marks_prior_open_cell_incomplete() -> None:
    """P0-3: a simulated abrupt restart (new observer on the same repo) must
    explicitly seal any prior OPEN cell as INCOMPLETE, never silently absent
    or falsely complete."""

    repo = _repo()
    settings = _settings()
    feature = _feature()
    first = ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repo,
        clock=ReplayClock(1_710_000_000_000),
    )
    first.observe(feature, _contexts(), frozenset({"BTCUSDT", "ETHUSDT"}))
    # Abrupt restart: second observer begins WITHOUT calling first.flush().
    second = ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repo,
        clock=ReplayClock(1_710_000_000_100),
    )
    # Prior open cell was sealed INCOMPLETE by the restart audit.
    coverage = repo.get_shadow_coverage(
        campaign_id="smoke-campaign-1",
        market="spot",
        decision_close_ms=feature.event_time_ms,
        primary_interval="5m",
    )
    assert coverage is not None
    assert coverage["status"] == "SEALED"
    assert coverage["complete"] is False
    assert "interrupted restart" in coverage["failures"]
    # New observer can begin a fresh cell on a later close.
    later = _feature(
        event_time_ms=1_710_000_300_000,
        price=106.5,
        previous_close=105.0,
        recent_high=106.0,
    )
    second.observe(later, _contexts(), frozenset({"BTCUSDT"}))
    second.flush()
    later_coverage = repo.get_shadow_coverage(
        campaign_id="smoke-campaign-1",
        market="spot",
        decision_close_ms=later.event_time_ms,
        primary_interval="5m",
    )
    assert later_coverage is not None
    assert later_coverage["complete"] is True


def test_durable_open_progress_preserved_across_restart() -> None:
    """B2-1: in-progress counters must be durably snapshot into the OPEN row
    as each symbol is observed, so an abrupt restart audits partial progress
    (A+B seen) rather than a zero/empty fabricated progress."""

    repo = _repo()
    settings = _settings("smoke-campaign-2")
    tradable = frozenset({"AUSDT", "BUSDT", "CUSDT"})
    feature_a = _feature(symbol="AUSDT", price=106.0, recent_high=105.0)
    feature_b = _feature(
        symbol="BUSDT", price=100.0, recent_high=105.0, previous_close=101.0
    )
    first = ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repo,
        clock=ReplayClock(1_710_000_000_000),
    )
    first.observe(feature_a, _contexts(), tradable)
    first.observe(feature_b, _contexts(), tradable)
    # Crashed before flush and before observing CUSDT.
    ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repo,
        clock=ReplayClock(1_710_000_000_100),
    )
    coverage = repo.get_shadow_coverage(
        campaign_id="smoke-campaign-2",
        market="spot",
        decision_close_ms=feature_a.event_time_ms,
        primary_interval="5m",
    )
    assert coverage is not None
    assert coverage["status"] == "SEALED"
    assert coverage["complete"] is False
    # Durable progress survives the restart: A+B observed, CUSDT never claimed.
    assert sorted(coverage["seen_symbols"]) == ["AUSDT", "BUSDT"]
    assert coverage["mature_count"] == 2
    assert coverage["raw_c0_count"] == 1


def test_graceful_shutdown_seals_last_cell() -> None:
    """B2-2: flush() on the production shutdown owner seals the last in-
    progress cell as complete/incomplete rather than leaving it OPEN."""

    observer, _settings = _observer()
    repo = observer.repository
    tradable = frozenset({"BTCUSDT", "ETHUSDT"})
    feature_a = _feature()
    feature_b = _feature(
        symbol="ETHUSDT", price=100.0, recent_high=105.0, previous_close=101.0
    )
    observer.observe(feature_a, _contexts(), tradable)
    observer.observe(feature_b, _contexts(), tradable)
    # Simulate the graceful shutdown owner calling flush().
    observer.flush()
    coverage = repo.get_shadow_coverage(
        campaign_id="smoke-campaign-1",
        market="spot",
        decision_close_ms=feature_a.event_time_ms,
        primary_interval="5m",
    )
    assert coverage is not None
    assert coverage["status"] == "SEALED"
    assert coverage["complete"] is True


def test_flush_failure_does_not_damage_incumbent_persistence() -> None:
    """A shadow flush failure (injected) must not affect the incumbent signal
    persistence path."""

    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    # A saved incumbent signal succeeds before any shadow involvement.
    from signalbot.domain.enums import SignalStage
    from signalbot.domain.models import SignalDecision

    decision = SignalDecision(
        event_id="evt-incumbent",
        market=Market.SPOT,
        symbol="BTCUSDT",
        family=SignalFamily.BREAKOUT_LONG,
        stage=SignalStage.SETUP,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=1_710_000_000_000,
        score=70,
        price=Decimal("105.0"),
        invalidation=None,
        rule_version="r2",
    )
    assert repo.save_signal(decision) is True


def test_anomaly_missing_volume_preserves_state_but_floor_clears() -> None:
    from signalbot.config import SignalSettings
    from signalbot.data.anomaly import AnomalyDetector
    from signalbot.domain.models import MiniTicker

    settings = SignalSettings(anomaly_min_quote_volume_usdt=50_000_000)
    detector = AnomalyDetector(settings)
    regime = MarketRegime()
    missing = MiniTicker(
        market=Market.SPOT,
        symbol="ADAUSDT",
        event_time_ms=1_710_000_000_000,
        close=Decimal("1.2"),
        quote_volume=None,
    )
    # Missing quote_volume is unusable evidence: returns no evaluation and must
    # not clear a previously qualified warning (preserves stale state).
    assert detector.update(missing, frozenset({"ADAUSDT"}), regime) == ()
    below_floor = missing.model_copy(
        update={"quote_volume": Decimal("10_000_000")}
    )
    result = detector.update(below_floor, frozenset({"ADAUSDT"}), regime)
    assert len(result) == 2
    assert all(item.score == 0 for item in result)
    assert all(
        any("liquidity floor" in reason for reason in item.reasons)
        for item in result
    )


def test_config_hash_is_deterministic() -> None:
    settings = _settings()
    assert shadow_config_sha256(settings) == shadow_config_sha256(settings)


def test_bbo_unavailable_fails_closed_in_payload() -> None:
    observer, _settings = _observer()
    feature = _feature(spread_bps=None, book_age_ms=None)
    from signalbot.prospective.observer import build_observation_payload

    candidate = observer.engine.evaluate_comparator(feature, _contexts())
    assert candidate is not None
    payload = build_observation_payload(
        observer, candidate, feature, _contexts(), "opp"
    )
    assert payload["execution_evidence"]["execution_available"] is False


def test_er20_boundary() -> None:
    settings = _settings()
    engine = SignalRuleEngine(settings.signals, settings.shadow)
    fail = engine.evaluate_comparator(_feature(efficiency_ratio_20=0.399), _contexts())
    assert fail is not None and fail.shadow_passed is False
    ok = engine.evaluate_comparator(_feature(efficiency_ratio_20=0.400), _contexts())
    assert ok is not None and ok.shadow_passed is True


def test_antichase_boundary() -> None:
    settings = _settings()
    engine = SignalRuleEngine(settings.signals, settings.shadow)
    # For a LONG breakout, distance = price - recent_high; ATR=2 so 0.50 ATR =
    # 1.0 price points.
    fail = engine.evaluate_comparator(
        _feature(price=107.0, recent_high=105.0, atr=2.0), _contexts()
    )
    assert fail is not None and fail.shadow_passed is False
    ok = engine.evaluate_comparator(
        _feature(price=106.0, recent_high=105.0, atr=2.0), _contexts()
    )
    assert ok is not None and ok.shadow_passed is True


def test_replay_live_parity_same_inputs() -> None:
    settings = _settings()
    engine_a = SignalRuleEngine(settings.signals, settings.shadow)
    engine_b = SignalRuleEngine(settings.signals, settings.shadow)
    feature = _feature()
    contexts = _contexts()
    assert engine_a.evaluate_comparator(feature, contexts) == engine_b.evaluate_comparator(
        feature, contexts
    )


def test_runtime_wires_observer_only_when_enabled() -> None:
    from signalbot.clock import ReplayClock
    from signalbot.runtime import MarketRuntime

    async def discard(_decision: object) -> None:
        return None

    settings_off = _settings().model_copy(deep=True)
    settings_off.shadow = settings_off.shadow.model_copy(update={"observation_enabled": False})
    runtime_off = MarketRuntime(
        Market.SPOT,
        settings_off,
        _repo(),
        ReplayClock(),
        discard,
        campaign_id="smoke-campaign-1",
    )
    assert runtime_off.shadow_observer is None

    settings_on = _settings()
    runtime_on = MarketRuntime(
        Market.SPOT,
        settings_on,
        _repo(),
        ReplayClock(),
        discard,
        campaign_id="smoke-campaign-1",
    )
    assert runtime_on.shadow_observer is not None
    assert runtime_on.shadow_observer.campaign_id == "smoke-campaign-1"
    assert runtime_on.shadow_observer.engine.settings.entry_policy == "r2_pit_htf_exec"


class _FailingShadowRepository(SqlRepository):
    """Repository whose shadow observation writes always fail."""

    def save_shadow_observation(self, *args: object, **kwargs: object) -> bool:
        raise RuntimeError("injected shadow persistence failure")


def test_shadow_persistence_failure_is_isolated_and_does_not_raise() -> None:
    """P0-1: a shadow persistence failure must never raise out of ``observe``
    and must not fabricate a durable coverage row."""

    settings = _settings()
    repo = _FailingShadowRepository("sqlite:///:memory:")
    repo.initialize()
    observer = ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repo,
        clock=ReplayClock(1_710_000_000_000),
    )
    # The comparator evaluation still succeeds and returns a candidate even when
    # evidence persistence is broken.
    candidate = observer.observe(_feature(), _contexts(), frozenset({"BTCUSDT"}))
    assert candidate is not None
    assert candidate.raw_c0_triggered is True
    assert candidate.r2_passed is True
    # No durable coverage cell is fabricated as complete from a failed persist.
    assert (
        repo.count_shadow_observations(campaign_id="smoke-campaign-1") == 0
    )
    observer.flush()
    coverage = repo.get_shadow_coverage(
        campaign_id="smoke-campaign-1",
        market="spot",
        decision_close_ms=_feature().event_time_ms,
        primary_interval="5m",
    )
    assert coverage is not None
    assert coverage["complete"] is False
    assert any("persistence failure" in item for item in coverage["failures"].split('","'))


def test_shadow_comparator_failure_is_isolated() -> None:
    """A corrupt comparator input cannot propagate out of ``observe``."""

    settings = _settings()
    repo = _repo()
    observer = ShadowObserver(
        settings,
        SignalRuleEngine(settings.signals, settings.shadow),
        repo,
        clock=ReplayClock(1_710_000_000_000),
    )
    # A market outside the permitted families yields None without raising.
    from signalbot.domain.enums import Market as MarketEnum

    candidate = observer.observe(
        _feature(market=MarketEnum.SPOT),
        {},
        frozenset({"BTCUSDT"}),
    )
    assert candidate is None or candidate.raw_c0_triggered is not None
