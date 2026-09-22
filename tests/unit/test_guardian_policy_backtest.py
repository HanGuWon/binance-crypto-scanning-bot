from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
from pathlib import Path

import pytest

from signalbot.backtest.engine import FundingRate, Trade, candle_from_values
from signalbot.backtest.guardian_policy import (
    FROZEN_L60_02_PARENT_CONTRACT_SHA256,
    FROZEN_L60_02_V2_CONTRACT_SHA256,
    GuardianBarContext,
    GuardianEntryRow,
    GuardianExitPhase,
    GuardianPolicyContractError,
    GuardianPolicyEpisode,
    bootstrap_draw_day_indices,
    compute_guardian_pair_metrics,
    compute_guardian_stratified_metrics,
    derive_guardian_historical_spec,
    familywise_guardian_bootstrap,
    freeze_guardian_entry_row,
    is_premature_stop,
    load_guardian_policy_contract,
    replay_guardian_episode,
    run_guardian_policy_backtest,
    validate_guardian_historical_settings,
)
from signalbot.config import load_settings
from signalbot.domain.enums import Direction, Market, SignalFamily

ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = ROOT / "config" / "guardian-policy-selection.v2.json"
GUARDIAN_POLICY_SOURCE = ROOT / "src" / "signalbot" / "backtest" / "guardian_policy.py"
EXPECTED_GUARDIAN_POLICY_SOURCE_SHA256 = (
    "354824d2162b77f46b140e727a648b69a57817147dfb8fdaf413fa3a6a04b6d9"
)
STEP_MS = 300_000


def _contract() -> dict[str, object]:
    value = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def test_guardian_policy_implementation_identity_is_frozen_before_outcomes() -> None:
    source = GUARDIAN_POLICY_SOURCE.read_bytes().replace(b"\r\n", b"\n")

    assert hashlib.sha256(source).hexdigest() == EXPECTED_GUARDIAN_POLICY_SOURCE_SHA256


def _candle(
    index: int,
    *,
    open_price: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.0,
    direction: Direction = Direction.LONG,
):
    symbol = "LONGUSDT" if direction is Direction.LONG else "SHORTUSDT"
    return candle_from_values(
        market=Market.FUTURES,
        symbol=symbol,
        interval="5m",
        open_time_ms=index * STEP_MS,
        open_price=open_price,
        high=high,
        low=low,
        close=close,
    )


def _entry(
    candles,
    *,
    direction: Direction = Direction.LONG,
    initial_stop: Decimal | None = None,
    position_id: str | None = None,
) -> GuardianEntryRow:
    stop = initial_stop or (Decimal("98") if direction is Direction.LONG else Decimal("102"))
    return GuardianEntryRow(
        position_id=position_id or f"position-{direction.value}",
        asset="LONG" if direction is Direction.LONG else "SHORT",
        cohort="major",
        symbol=candles[1].symbol,
        direction=direction,
        family=(
            SignalFamily.BREAKOUT_LONG
            if direction is Direction.LONG
            else SignalFamily.BREAKDOWN_SHORT
        ),
        score=90,
        split="development",
        regime="neutral",
        rule_version="test-rule",
        entry_signal_id=f"signal-{direction.value}",
        entry_signal_time_ms=candles[0].close_time_ms,
        entry_time_ms=candles[1].open_time_ms,
        entry_index=1,
        entry_price=Decimal("100"),
        entry_execution_price=(
            Decimal("100.03") if direction is Direction.LONG else Decimal("99.97")
        ),
        initial_stop=stop,
        notional_usdt=Decimal("100"),
        quantity=Decimal("1"),
        trend_state="mixed",
        entry_atr_percent=Decimal("0.75"),
    )


def _contexts(
    candles,
    *,
    atr: Decimal = Decimal("1"),
    support_by_index: dict[int, Decimal] | None = None,
    resistance_by_index: dict[int, Decimal] | None = None,
    weakened_long_indices: set[int] | None = None,
    weakened_short_indices: set[int] | None = None,
) -> dict[int, GuardianBarContext]:
    support_by_index = support_by_index or {}
    resistance_by_index = resistance_by_index or {}
    weakened_long_indices = weakened_long_indices or set()
    weakened_short_indices = weakened_short_indices or set()
    return {
        candle.close_time_ms: GuardianBarContext(
            candle_close_time_ms=candle.close_time_ms,
            atr=atr,
            confirmed_swing_support=support_by_index.get(index),
            confirmed_swing_resistance=resistance_by_index.get(index),
            momentum_weakened_long=index in weakened_long_indices,
            momentum_weakened_short=index in weakened_short_indices,
        )
        for index, candle in enumerate(candles)
    }


def _source_trade() -> Trade:
    return Trade(
        trade_id="outcome-dependent-a",
        opportunity_id="opportunity",
        protocol_version="protocol",
        rule_version="rule",
        asset="BTC",
        cohort="anchor",
        market="futures",
        symbol="BTCUSDT",
        direction="long",
        family="breakout_long",
        score=90,
        split="development",
        split_contained=True,
        regime="neutral",
        entry_signal_id="signal",
        entry_signal_time_ms=299_999,
        entry_time_ms=300_000,
        exit_time_ms=600_000,
        entry_price=100.0,
        exit_price=101.0,
        entry_execution_price=100.03,
        exit_execution_price=100.97,
        initial_stop=98.0,
        exit_reason="time_exit",
        bars_held=2,
        gross_return=0.01,
        slippage_return=0.0006,
        fee_return=0.001,
        funding_return=0.0,
        net_return=0.0084,
        gross_pnl_usdt=1.0,
        slippage_usdt=0.06,
        fees_usdt=0.1,
        funding_pnl_usdt=0.0,
        net_pnl_usdt=0.84,
        mfe=0.02,
        mae=-0.01,
        net_r_multiple=0.42,
    )


def _episode(
    entry: GuardianEntryRow,
    policy_id: str,
    *,
    after_cost_bps: Decimal | None,
    valid: bool = True,
    exit_time_ms: int | None = 600_000,
) -> GuardianPolicyEpisode:
    return GuardianPolicyEpisode(
        position_id=entry.position_id,
        policy_id=policy_id,
        symbol=entry.symbol,
        direction=entry.direction,
        entry_time_ms=entry.entry_time_ms,
        exit_time_ms=exit_time_ms,
        exit_bar_index=2 if exit_time_ms is not None else None,
        exit_phase=GuardianExitPhase.CLOSE if exit_time_ms is not None else None,
        exit_price=Decimal("101") if exit_time_ms is not None else None,
        exit_atr=Decimal("1") if exit_time_ms is not None else None,
        bars_held=2,
        valid_primary=valid,
        censor_reason=None if valid else "test_censor",
        stop_update_count=1,
        gap_through_slippage_bps=Decimal("0"),
        same_bar_ambiguity_count=0,
        directional_return_bps=Decimal("100") if valid else None,
        realized_signed_funding_bps=Decimal("0") if valid else None,
        after_cost_return_bps=after_cost_bps if valid else None,
        mfe_bps=Decimal("200") if valid else None,
        mfe_giveback_fraction=Decimal("0.5") if valid else None,
    )


def test_final_contract_and_derived_historical_spec_are_bound() -> None:
    contract = load_guardian_policy_contract(CONTRACT_PATH, workspace_root=ROOT)

    assert FROZEN_L60_02_V2_CONTRACT_SHA256 == (
        "23140ebd342ccf5e2b6c1ba9a6f8b180ece420cf8277db7fe944c0b3790fefb7"
    )
    assert FROZEN_L60_02_PARENT_CONTRACT_SHA256 == (
        "2e19fd7a597dd78a0372753da50fd54dcfacceea6e9482bb34aac606c507a923"
    )
    spec = derive_guardian_historical_spec(contract, workspace_root=ROOT)
    assert spec.direction_scope == "futures_bidirectional"
    assert spec.interval == "5m"
    assert spec.exits.max_holding_bars == 72


def test_real_runner_fails_before_data_when_config_path_is_not_frozen(tmp_path: Path) -> None:
    settings = load_settings(ROOT / "config" / "settings.example.yaml")

    with pytest.raises(GuardianPolicyContractError, match="config path"):
        run_guardian_policy_backtest(
            settings,
            CONTRACT_PATH,
            tmp_path / "missing-data",
            tmp_path / "output",
            workspace_root=ROOT,
            config_path=tmp_path / "wrong-settings.yaml",
        )

    assert not (tmp_path / "output").exists()


def test_effective_settings_drift_fails_before_data(tmp_path: Path) -> None:
    settings = load_settings(ROOT / "config" / "settings.example.yaml")
    drifted = settings.model_copy(
        update={"signals": settings.signals.model_copy(update={"breakout_lookback": 21})}
    )
    contract = load_guardian_policy_contract(CONTRACT_PATH, workspace_root=ROOT)

    with pytest.raises(GuardianPolicyContractError, match="supplied Settings differ"):
        validate_guardian_historical_settings(
            contract,
            drifted,
            config_path=ROOT / "config" / "settings.example.yaml",
            workspace_root=ROOT,
        )


def test_frozen_entry_identity_ignores_source_exit_outcome_fields() -> None:
    first = _source_trade()
    second = replace(
        first,
        trade_id="outcome-dependent-b",
        exit_time_ms=9_999_999,
        exit_price=50.0,
        exit_reason="initial_stop",
        gross_return=-0.5,
        net_return=-0.6,
        mfe=9.0,
        mae=-9.0,
    )

    row_a = freeze_guardian_entry_row(
        first,
        entry_index=1,
        notional_usdt=Decimal("100"),
        trend_state="mixed",
    )
    row_b = freeze_guardian_entry_row(
        second,
        entry_index=1,
        notional_usdt=Decimal("100"),
        trend_state="mixed",
    )

    assert row_a == row_b
    assert row_a.quantity == Decimal("1")


def test_delayed_atr_stop_is_not_retroactive_inside_its_source_bar() -> None:
    candles = [
        _candle(0),
        _candle(1, high=103.0, low=99.0, close=102.5),
        _candle(2, open_price=102.0, high=102.5, low=100.5, close=101.5),
    ]
    entry = _entry(candles)

    episode = replay_guardian_episode(
        entry,
        policy_id="delayed_atr_trail_v1",
        candles=candles,
        contexts_by_close=_contexts(candles),
        funding=(),
        contract=_contract(),
    )

    assert episode.valid_primary is True
    assert episode.exit_bar_index == 2
    assert episode.exit_phase is GuardianExitPhase.INTRABAR
    assert episode.exit_price == Decimal("101.0")
    assert episode.stop_update_count == 1
    with localcontext(Context(prec=34, rounding=ROUND_HALF_EVEN)):
        expected_mfe = (Decimal("103") - Decimal("100.03")) / Decimal("100.03") * Decimal("10000")
    assert episode.mfe_bps == expected_mfe


def test_confirmed_swing_cannot_bypass_atr_activation_but_weakening_can() -> None:
    candles = [
        _candle(0),
        _candle(1, high=101.0, low=99.0, close=100.5),
        _candle(2, open_price=100.5, high=100.8, low=99.4, close=100.0),
    ]
    entry = _entry(candles)
    contexts = _contexts(
        candles,
        support_by_index={1: Decimal("99.5")},
        weakened_long_indices={1},
    )

    swing = replay_guardian_episode(
        entry,
        policy_id="confirmed_swing_atr_trail_v1",
        candles=candles,
        contexts_by_close=contexts,
        funding=(),
        contract=_contract(),
    )
    weakening = replay_guardian_episode(
        entry,
        policy_id="weakening_sensitive_adaptive_trail_v1",
        candles=candles,
        contexts_by_close=contexts,
        funding=(),
        contract=_contract(),
    )

    assert swing.valid_primary is False
    assert swing.censor_reason == "end_of_data_before_terminal"
    assert swing.stop_update_count == 0
    assert weakening.valid_primary is True
    assert weakening.exit_bar_index == 2
    assert weakening.exit_price == Decimal("99.5")
    assert weakening.stop_update_count == 1


def test_data_gap_censors_without_inferred_stop_fill() -> None:
    candles = [
        _candle(0),
        _candle(1, high=101.0, low=99.0, close=100.0),
        candle_from_values(
            market=Market.FUTURES,
            symbol="LONGUSDT",
            interval="5m",
            open_time_ms=3 * STEP_MS,
            open_price=97.0,
            high=98.0,
            low=96.0,
            close=97.0,
        ),
    ]
    entry = _entry(candles)

    episode = replay_guardian_episode(
        entry,
        policy_id="initial_stop_only_v1",
        candles=candles,
        contexts_by_close=_contexts(candles),
        funding=(),
        contract=_contract(),
    )

    assert episode.valid_primary is False
    assert episode.censor_reason == "data_gap"
    assert episode.exit_price is None


def test_candidate_stop_missing_immediately_prior_atr_is_primary_censored() -> None:
    candles = [_candle(0), _candle(1, high=101.0, low=97.0, close=99.0)]
    entry = _entry(candles)
    contexts = _contexts(candles)
    del contexts[candles[0].close_time_ms]

    episode = replay_guardian_episode(
        entry,
        policy_id="initial_stop_only_v1",
        candles=candles,
        contexts_by_close=contexts,
        funding=(),
        contract=_contract(),
    )

    assert episode.valid_primary is False
    assert episode.censor_reason == "missing_candidate_exit_atr"
    assert episode.exit_phase is GuardianExitPhase.INTRABAR
    assert episode.exit_atr is None


def test_candidate_stop_does_not_carry_stale_atr_across_missing_prior_context() -> None:
    candles = [
        _candle(0),
        _candle(1, high=101.0, low=99.0, close=100.0),
        _candle(2, high=101.0, low=97.0, close=99.0),
    ]
    entry = _entry(candles)
    contexts = _contexts(candles)
    del contexts[candles[1].close_time_ms]

    episode = replay_guardian_episode(
        entry,
        policy_id="initial_stop_only_v1",
        candles=candles,
        contexts_by_close=contexts,
        funding=(),
        contract=_contract(),
    )

    assert episode.valid_primary is False
    assert episode.censor_reason == "missing_candidate_exit_atr"
    assert episode.exit_bar_index == 2
    assert episode.exit_atr is None


def test_candidate_stop_nonpositive_prior_atr_is_primary_censored() -> None:
    candles = [_candle(0), _candle(1, high=101.0, low=97.0, close=99.0)]
    entry = _entry(candles)
    contexts = _contexts(candles, atr=Decimal("0"))

    episode = replay_guardian_episode(
        entry,
        policy_id="initial_stop_only_v1",
        candles=candles,
        contexts_by_close=contexts,
        funding=(),
        contract=_contract(),
    )

    assert episode.valid_primary is False
    assert episode.censor_reason == "missing_candidate_exit_atr"


def test_intrabar_stop_mfe_excludes_unknown_later_high_and_counts_target_collision() -> None:
    candles = [_candle(0), _candle(1, high=150.0, low=97.0, close=120.0)]
    entry = _entry(candles)

    episode = replay_guardian_episode(
        entry,
        policy_id="initial_stop_only_v1",
        candles=candles,
        contexts_by_close=_contexts(candles),
        funding=(),
        contract=_contract(),
    )

    assert episode.valid_primary is True
    assert episode.exit_phase is GuardianExitPhase.INTRABAR
    assert episode.mfe_bps == Decimal("0")
    assert episode.same_bar_ambiguity_count == 1


def test_funding_is_strict_interior_and_boundary_event_censors() -> None:
    candles = [_candle(0), _candle(1, high=101.0, low=97.0, close=99.0)]
    entry = _entry(candles)
    strict_interior = FundingRate(
        funding_time_ms=450_000,
        rate=0.001,
        mark_price=110.0,
    )

    valid = replay_guardian_episode(
        entry,
        policy_id="initial_stop_only_v1",
        candles=candles,
        contexts_by_close=_contexts(candles),
        funding=(strict_interior,),
        contract=_contract(),
    )
    boundary = replay_guardian_episode(
        entry,
        policy_id="initial_stop_only_v1",
        candles=candles,
        contexts_by_close=_contexts(candles),
        funding=(FundingRate(candles[1].close_time_ms, 0.001, 110.0),),
        contract=_contract(),
    )

    assert valid.valid_primary is True
    assert valid.realized_signed_funding_bps == Decimal("-11.0000")
    assert boundary.valid_primary is False
    assert boundary.censor_reason == "funding_boundary_or_authority"


def test_short_funding_sign_and_nonpositive_mark_falls_back_to_entry() -> None:
    candles = [
        _candle(0, direction=Direction.SHORT),
        _candle(1, high=103.0, low=99.0, close=101.0, direction=Direction.SHORT),
    ]
    entry = _entry(candles, direction=Direction.SHORT)

    episode = replay_guardian_episode(
        entry,
        policy_id="initial_stop_only_v1",
        candles=candles,
        contexts_by_close=_contexts(candles),
        funding=(FundingRate(450_000, 0.001, -1.0),),
        contract=_contract(),
    )

    assert episode.valid_primary is True
    assert episode.realized_signed_funding_bps == Decimal("10.000")


def test_stop_wins_over_max_holding_close_on_bar_72() -> None:
    candles = [_candle(0)]
    candles.extend(_candle(index) for index in range(1, 72))
    candles.append(_candle(72, high=101.0, low=97.0, close=100.0))
    entry = _entry(candles)

    episode = replay_guardian_episode(
        entry,
        policy_id="initial_stop_only_v1",
        candles=candles,
        contexts_by_close=_contexts(candles),
        funding=(),
        contract=_contract(),
    )

    assert episode.valid_primary is True
    assert episode.bars_held == 72
    assert episode.exit_bar_index == 72
    assert episode.exit_phase is GuardianExitPhase.INTRABAR
    assert episode.exit_price == Decimal("98.0")


def test_pair_metrics_use_all_admitted_for_censor_and_paired_rows_for_metrics() -> None:
    candles = [_candle(0), _candle(1), _candle(2)]
    first = _entry(candles, position_id="first")
    second = _entry(candles, position_id="second")
    episodes = {
        ("first", "delayed_atr_trail_v1"): _episode(
            first, "delayed_atr_trail_v1", after_cost_bps=Decimal("10")
        ),
        ("first", "initial_stop_only_v1"): _episode(
            first, "initial_stop_only_v1", after_cost_bps=Decimal("20")
        ),
        ("second", "delayed_atr_trail_v1"): _episode(
            second, "delayed_atr_trail_v1", after_cost_bps=Decimal("30")
        ),
        ("second", "initial_stop_only_v1"): _episode(
            second,
            "initial_stop_only_v1",
            after_cost_bps=None,
            valid=False,
            exit_time_ms=None,
        ),
    }

    metrics = compute_guardian_pair_metrics(
        [first, second],
        episodes,
        challenger_policy_id="initial_stop_only_v1",
        candles_by_symbol={first.symbol: candles},
    )

    assert metrics.admitted_positions == 2
    assert metrics.paired_valid_positions == 1
    assert metrics.censor_fraction == Decimal("0.5")
    assert metrics.mean_after_cost_delta_bps == Decimal("10")
    assert metrics.candidate_total_stop_update_count == 1
    assert metrics.baseline_total_stop_update_count == 1
    assert metrics.candidate_mean_gap_through_slippage_bps == Decimal("0")
    assert metrics.candidate_same_bar_ambiguity_count == 0
    assert metrics.evidence_total_positions == 1


def test_same_bar_intrabar_exits_are_unordered_for_premature_diagnostic() -> None:
    candles = [_candle(0), _candle(1), _candle(2)]
    entry = _entry(candles)
    candidate = replace(
        _episode(entry, "initial_stop_only_v1", after_cost_bps=Decimal("1")),
        exit_phase=GuardianExitPhase.INTRABAR,
        exit_atr=Decimal("1"),
    )
    baseline = replace(
        _episode(entry, "delayed_atr_trail_v1", after_cost_bps=Decimal("1")),
        exit_phase=GuardianExitPhase.INTRABAR,
        exit_atr=Decimal("1"),
    )

    assert is_premature_stop(candidate, baseline, candles=candles) is False


def test_report_only_strata_use_same_paired_valid_population_and_fixed_bins() -> None:
    candles = [_candle(0), _candle(1), _candle(2)]
    entry = _entry(candles, position_id="stratum-entry")
    episodes = {
        (entry.position_id, "delayed_atr_trail_v1"): _episode(
            entry, "delayed_atr_trail_v1", after_cost_bps=Decimal("10")
        ),
        (entry.position_id, "initial_stop_only_v1"): _episode(
            entry, "initial_stop_only_v1", after_cost_bps=Decimal("15")
        ),
    }

    rows = compute_guardian_stratified_metrics(
        [entry],
        episodes,
        challenger_policy_id="initial_stop_only_v1",
        candles_by_symbol={entry.symbol: candles},
    )

    assert {(row.dimension, row.stratum) for row in rows} == {
        ("direction", "LONG"),
        ("regime", "neutral"),
        ("time_of_day_utc", "utc_00_08"),
        ("volatility_atr_percent", "medium_0_50_to_1_00"),
    }
    assert all(row.paired_valid_positions == 1 for row in rows)
    assert all(row.mean_after_cost_delta_bps == Decimal("5") for row in rows)


def test_bootstrap_draw_is_deterministic_and_familywise_schedule_is_shared() -> None:
    assert bootstrap_draw_day_indices(
        calendar_day_count=10,
        seed=20_260_921,
        replicate_ordinal=0,
    ) == (0, 1, 2, 3, 4, 5, 6, 6, 7, 8)

    candles = [_candle(0), _candle(1), _candle(2)]
    entries = [
        replace(
            _entry(candles, position_id=f"p-{day}"),
            entry_time_ms=(20_000 + day) * 86_400_000,
        )
        for day in range(3)
    ]
    episodes: dict[tuple[str, str], GuardianPolicyEpisode] = {}
    for index, entry in enumerate(entries):
        baseline = _episode(
            entry,
            "delayed_atr_trail_v1",
            after_cost_bps=Decimal(str(index)),
            exit_time_ms=entry.entry_time_ms + STEP_MS,
        )
        episodes[(entry.position_id, "delayed_atr_trail_v1")] = baseline
        for offset, policy_id in enumerate(
            (
                "initial_stop_only_v1",
                "confirmed_swing_atr_trail_v1",
                "weakening_sensitive_adaptive_trail_v1",
            ),
            start=1,
        ):
            episodes[(entry.position_id, policy_id)] = _episode(
                entry,
                policy_id,
                after_cost_bps=Decimal(str(index + offset)),
                exit_time_ms=entry.entry_time_ms + STEP_MS,
            )

    result = familywise_guardian_bootstrap(
        entries,
        episodes,
        samples=20,
        minimum_valid_replicates=1,
    )

    assert result["valid_replicates"] == 20
    assert result["calendar_day_count"] == 3
    assert set(result["point_estimates_bps"]) == {
        "initial_stop_only_v1",
        "confirmed_swing_atr_trail_v1",
        "weakening_sensitive_adaptive_trail_v1",
    }
