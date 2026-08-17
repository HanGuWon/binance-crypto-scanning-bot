# pyright: reportPrivateUsage=false

from __future__ import annotations

import gzip
import hashlib
import json
import os
from dataclasses import replace
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Protocol, cast

import pytest

from signalbot.backtest import d1_scefb_historical_development as subject
from signalbot.backtest.d1_scefb_historical_math import (
    D1HistoricalFeeCellV0,
    build_d1_historical_funding_point_v0,
)
from signalbot.backtest.dataset import DatasetManifest
from signalbot.backtest.downstream_code_freeze import create_downstream_code_freeze_v1
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.d1_scefb import (
    D1EntryStatusV0,
    D1ExitReasonV0,
    D1FiveMinuteBarV0,
    D1SideV0,
    build_d1_entry_input_v0,
    build_d1_five_minute_bar_v0,
    build_d1_hourly_bar_v0,
    evaluate_d1_entry_v0,
)

_FIVE_MINUTE_MS = 300_000
_HOUR_MS = 3_600_000
_DAY_MS = 86_400_000
_T = 1_800_000_000_000
_SHA = "1" * 64


class _FakeWin32Function:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[object, ...]] = []
        self.argtypes: object = None
        self.restype: object = None

    def __call__(self, *args: object) -> object:
        self.calls.append(args)
        if callable(self.result):
            return self.result(*args)
        return self.result


class _UnicodeBuffer(Protocol):
    value: str


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _sealed_bar(
    *,
    open_ms: int,
    open_price: Decimal = Decimal("100"),
    high: Decimal = Decimal("101"),
    low: Decimal = Decimal("99"),
    close: Decimal = Decimal("100"),
    quote_volume: Decimal = Decimal("200000"),
    imbalance: Decimal = Decimal("0"),
    receipt_ms: int | None = None,
    is_closed: bool = True,
) -> D1FiveMinuteBarV0:
    with localcontext(protocol_decimal_context_v2()):
        taker_buy = quote_volume * (Decimal(1) + imbalance) / Decimal(2)
    return build_d1_five_minute_bar_v0(
        open_ms=open_ms,
        open_price=open_price,
        high_price=high,
        low_price=low,
        close_price=close,
        quote_volume=quote_volume,
        taker_buy_quote_volume=taker_buy,
        receipt_ms=receipt_ms,
        is_closed=is_closed,
    )


def _candle_from_bar(symbol: str, value: D1FiveMinuteBarV0) -> Candle:
    return Candle(
        market=Market.FUTURES,
        symbol=symbol,
        interval="5m",
        open_time_ms=value.open_ms,
        close_time_ms=value.close_ms,
        open=value.open_price,
        high=value.high_price,
        low=value.low_price,
        close=value.close_price,
        volume=Decimal("1"),
        quote_volume=value.quote_volume,
        trade_count=1,
        taker_buy_base_volume=Decimal("0.5"),
        taker_buy_quote_volume=value.taker_buy_quote_volume,
        is_closed=value.is_closed,
    )


def _hourly_candle(*, open_ms: int, close: Decimal, symbol: str = "BTCUSDT") -> Candle:
    return Candle(
        market=Market.FUTURES,
        symbol=symbol,
        interval="1h",
        open_time_ms=open_ms,
        close_time_ms=open_ms + _HOUR_MS - 1,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=Decimal("1"),
        quote_volume=Decimal("100000"),
        trade_count=1,
        taker_buy_base_volume=Decimal("0.5"),
        taker_buy_quote_volume=Decimal("50000"),
        is_closed=True,
    )


def _positive_prior() -> tuple[D1FiveMinuteBarV0, ...]:
    first_open = _T - 289 * _FIVE_MINUTE_MS
    bars = [
        _sealed_bar(
            open_ms=first_open,
            high=Decimal("100.5"),
            low=Decimal("99.5"),
        )
    ]
    for index in range(288):
        if index < 216:
            width = Decimal("1")
        elif index < 276:
            width = Decimal("2")
        else:
            width = Decimal("1")
        imbalance = Decimal("-0.05") if index % 2 == 0 else Decimal("0.05")
        bars.append(
            _sealed_bar(
                open_ms=first_open + (index + 1) * _FIVE_MINUTE_MS,
                high=Decimal("100") + width / Decimal(2),
                low=Decimal("100") - width / Decimal(2),
                imbalance=imbalance,
            )
        )
    return tuple(bars)


def _positive_current(side: D1SideV0) -> D1FiveMinuteBarV0:
    if side is D1SideV0.LONG:
        return _sealed_bar(
            open_ms=_T,
            high=Decimal("101.7"),
            low=Decimal("98.3"),
            close=Decimal("101.5"),
            quote_volume=Decimal("500000"),
            imbalance=Decimal("0.40"),
        )
    return _sealed_bar(
        open_ms=_T,
        high=Decimal("101.7"),
        low=Decimal("98.3"),
        close=Decimal("98.5"),
        quote_volume=Decimal("500000"),
        imbalance=Decimal("-0.40"),
    )


def _hourly_bars(side: D1SideV0) -> tuple:
    first_open = _T - 250 * _HOUR_MS
    return tuple(
        build_d1_hourly_bar_v0(
            open_ms=first_open + index * _HOUR_MS,
            close_price=(
                Decimal("100") + Decimal(index) / Decimal(10)
                if side is D1SideV0.LONG
                else Decimal("200") - Decimal(index) / Decimal(10)
            ),
        )
        for index in range(250)
    )


def _positive_decision(side: D1SideV0):
    decision = evaluate_d1_entry_v0(
        build_d1_entry_input_v0(
            attempt_id="synthetic",
            symbol="BTCUSDT",
            venue=VenueV2.USDM_FUTURES,
            source_root_sha256=_SHA,
            prior_bars=_positive_prior(),
            current_bar=_positive_current(side),
            hourly_bars=_hourly_bars(side),
        )
    )
    assert decision.status is D1EntryStatusV0.SIGNAL
    return decision


@pytest.mark.parametrize(
    ("side", "expected"),
    [
        (D1SideV0.LONG, subject.D1HistoricalPrefilterStatusV0.CANDIDATE_LONG),
        (D1SideV0.SHORT, subject.D1HistoricalPrefilterStatusV0.CANDIDATE_SHORT),
    ],
)
def test_prefilter_never_drops_a_sealed_positive_signal(
    side: D1SideV0,
    expected: subject.D1HistoricalPrefilterStatusV0,
) -> None:
    _positive_decision(side)
    result = subject.evaluate_d1_historical_prefilter_v0(
        prior_channel_bars=_positive_prior()[-24:],
        current_bar=_positive_current(side),
    )

    assert result.status is expected


@pytest.mark.parametrize(
    ("side", "expected"),
    [
        (D1SideV0.LONG, subject.D1HistoricalPrefilterStatusV0.CANDIDATE_LONG),
        (D1SideV0.SHORT, subject.D1HistoricalPrefilterStatusV0.CANDIDATE_SHORT),
    ],
)
def test_prefilter_exact_volume_flow_and_close_location_boundaries_are_inclusive(
    side: D1SideV0,
    expected: subject.D1HistoricalPrefilterStatusV0,
) -> None:
    prior = tuple(_sealed_bar(open_ms=_T - (24 - index) * _FIVE_MINUTE_MS) for index in range(24))
    current = (
        _sealed_bar(
            open_ms=_T,
            high=Decimal("103"),
            low=Decimal("99"),
            close=Decimal("102"),
            imbalance=Decimal("0.20"),
        )
        if side is D1SideV0.LONG
        else _sealed_bar(
            open_ms=_T,
            high=Decimal("101"),
            low=Decimal("97"),
            close=Decimal("98"),
            imbalance=Decimal("-0.20"),
        )
    )

    result = subject.evaluate_d1_historical_prefilter_v0(
        prior_channel_bars=prior,
        current_bar=current,
    )

    assert result.status is expected


def test_prefilter_distinguishes_proven_false_from_invalid_input() -> None:
    prior = tuple(_sealed_bar(open_ms=_T - (24 - index) * _FIVE_MINUTE_MS) for index in range(24))
    no_break = _sealed_bar(open_ms=_T, close=Decimal("101"))
    zero_volume = _sealed_bar(
        open_ms=_T,
        high=Decimal("103"),
        low=Decimal("99"),
        close=Decimal("102"),
        quote_volume=Decimal(0),
    )

    assert (
        subject.evaluate_d1_historical_prefilter_v0(
            prior_channel_bars=prior,
            current_bar=no_break,
        ).status
        is subject.D1HistoricalPrefilterStatusV0.NECESSARY_GATE_FALSE
    )
    assert (
        subject.evaluate_d1_historical_prefilter_v0(
            prior_channel_bars=prior,
            current_bar=zero_volume,
        ).status
        is subject.D1HistoricalPrefilterStatusV0.INVALID_INPUT_INCONCLUSIVE
    )
    assert (
        subject.evaluate_d1_historical_prefilter_v0(
            prior_channel_bars=prior[:-1],
            current_bar=no_break,
        ).status
        is subject.D1HistoricalPrefilterStatusV0.INVALID_INPUT_INCONCLUSIVE
    )


def _engine_data(
    *,
    funding=(),
    exact_standard_8h_development_funding_coverage: bool = True,
    trigger_profit: bool = True,
    second_signal: bool = False,
) -> subject.D1HistoricalReplaySymbolInputV0:
    symbol = "BTCUSDT"
    bars = list(_positive_prior())
    bars.append(_positive_current(D1SideV0.LONG))
    bars.append(
        _sealed_bar(
            open_ms=_T + _FIVE_MINUTE_MS,
            open_price=Decimal("100") if second_signal else Decimal("101.5"),
            high=Decimal("102.2") if second_signal else Decimal("101.7"),
            low=Decimal("99.4") if second_signal else Decimal("101.3"),
            close=Decimal("102.1") if second_signal else Decimal("101.5"),
            quote_volume=Decimal("500000") if second_signal else Decimal("100000"),
            imbalance=Decimal("0.40") if second_signal else Decimal(0),
        )
    )
    entry_close = Decimal("106") if trigger_profit else Decimal("102")
    bars.append(
        _sealed_bar(
            open_ms=_T + 2 * _FIVE_MINUTE_MS,
            open_price=Decimal("101.5"),
            high=Decimal("107") if trigger_profit else Decimal("102.2"),
            low=Decimal("101.3"),
            close=entry_close,
            quote_volume=Decimal("100000"),
        )
    )
    for offset in range(3, 31):
        if not trigger_profit:
            open_price = close = Decimal("102")
            high = Decimal("102.2")
            low = Decimal("101.8")
        elif offset == 4:
            open_price = high = close = Decimal("106")
            low = Decimal("101.3")
        else:
            open_price = close = Decimal("101.5")
            high = Decimal("101.7")
            low = Decimal("101.3")
        bars.append(
            _sealed_bar(
                open_ms=_T + offset * _FIVE_MINUTE_MS,
                open_price=open_price,
                high=high,
                low=low,
                close=close,
                quote_volume=Decimal("100000"),
            )
        )
    hourly = tuple(
        _hourly_candle(
            open_ms=_T - (250 - index) * _HOUR_MS,
            close=Decimal("100") + Decimal(index) / Decimal(10),
        )
        for index in range(250)
    )
    return subject.D1HistoricalReplaySymbolInputV0(
        symbol=symbol,
        five_minute_manifest_sha256="2" * 64,
        higher_timeframe_source_sha256="3" * 64,
        funding_file_sha256="4" * 64,
        source_root_sha256="5" * 64,
        five_minute=tuple(_candle_from_bar(symbol, value) for value in bars),
        hourly=hourly,
        funding=tuple(funding),
        exact_standard_8h_development_funding_coverage=(
            exact_standard_8h_development_funding_coverage
        ),
    )


def _run_synthetic(
    data: subject.D1HistoricalReplaySymbolInputV0,
    *,
    end_offset: int,
) -> subject._SymbolRunResultV0:
    return subject._run_symbol_development_v0(
        data=data,
        run_id="synthetic-run",
        decision_start_ms=_T,
        decision_end_ms=_T + end_offset * _FIVE_MINUTE_MS,
    )


def test_engine_uses_t_plus_2_entry_entry_bar_exit_and_j_plus_2_reference() -> None:
    result = _run_synthetic(_engine_data(), end_offset=10)

    assert len(result.episodes) == 1
    episode = result.episodes[0]
    assert episode.entry_reference_time_ms == _T + 2 * _FIVE_MINUTE_MS
    assert episode.entry_reference_price == Decimal("101.5")
    assert episode.exit_observation_open_ms == episode.entry_reference_time_ms
    assert episode.exit_reason is D1ExitReasonV0.PROFIT_CLOSE
    assert episode.exit_reference_time_ms == _T + 4 * _FIVE_MINUTE_MS
    assert episode.exit_reference_price == Decimal("106")
    assert episode.funding_evaluable
    assert episode.funding_inconclusive_reason is None
    assert result.counters.entered_position_count == 1


@pytest.mark.parametrize(
    ("funding_time", "mark", "reason"),
    [
        (
            _T + 2 * _FIVE_MINUTE_MS,
            Decimal("101"),
            subject.D1HistoricalFundingInconclusiveReasonV0.FUNDING_ENDPOINT_EQUALITY,
        ),
        (
            _T + 3 * _FIVE_MINUTE_MS,
            None,
            subject.D1HistoricalFundingInconclusiveReasonV0.MISSING_INTERIOR_FUNDING_MARK,
        ),
    ],
)
def test_engine_separates_funding_endpoint_and_missing_interior_mark(
    funding_time: int,
    mark: Decimal | None,
    reason: subject.D1HistoricalFundingInconclusiveReasonV0,
) -> None:
    funding = build_d1_historical_funding_point_v0(
        funding_time_ms=funding_time,
        rate=Decimal("0.0001"),
        mark_price=mark,
    )

    episode = _run_synthetic(_engine_data(funding=(funding,)), end_offset=10).episodes[0]

    assert not episode.funding_evaluable
    assert episode.funding_inconclusive_reason is reason
    assert all(value.net_return is None for value in episode.projections)


def test_missing_mark_outside_episode_is_ignored() -> None:
    outside = build_d1_historical_funding_point_v0(
        funding_time_ms=_T - _FIVE_MINUTE_MS,
        rate=Decimal("0.0001"),
        mark_price=None,
    )

    episode = _run_synthetic(_engine_data(funding=(outside,)), end_offset=10).episodes[0]

    assert episode.funding_evaluable
    assert episode.funding_event_count == 0


def test_nonstandard_or_incomplete_symbol_funding_marks_every_episode_unavailable() -> None:
    episode = _run_synthetic(
        _engine_data(exact_standard_8h_development_funding_coverage=False),
        end_offset=10,
    ).episodes[0]

    assert not episode.funding_evaluable
    assert episode.funding_inconclusive_reason is (
        subject.D1HistoricalFundingInconclusiveReasonV0.FUNDING_COVERAGE_UNAVAILABLE
    )
    assert all(value.net_return is None for value in episode.projections)


@pytest.mark.parametrize(
    ("end_offset", "trigger_profit", "stage"),
    [
        (2, True, subject.D1HistoricalCensorStageV0.ENTRY_REFERENCE),
        (4, True, subject.D1HistoricalCensorStageV0.EXIT_REFERENCE),
        (6, False, subject.D1HistoricalCensorStageV0.EXIT_OBSERVATION),
    ],
)
def test_right_edge_is_censored_without_forced_close(
    end_offset: int,
    trigger_profit: bool,
    stage: subject.D1HistoricalCensorStageV0,
) -> None:
    result = _run_synthetic(
        _engine_data(trigger_profit=trigger_profit),
        end_offset=end_offset,
    )

    assert result.episodes == ()
    assert len(result.censors) == 1
    assert result.censors[0].stage is stage


def test_only_actual_full_signal_is_counted_as_pending_suppression() -> None:
    result = _run_synthetic(_engine_data(second_signal=True), end_offset=10)

    assert result.counters.full_signal_count >= 2
    assert result.counters.pending_or_active_suppressed_signal_count >= 1
    assert len(result.episodes) == 1


def test_hard_horizon_is_the_exact_twenty_fourth_entry_containing_bar() -> None:
    result = _run_synthetic(_engine_data(trigger_profit=False), end_offset=30)

    assert len(result.episodes) == 1
    episode = result.episodes[0]
    assert episode.exit_reason is D1ExitReasonV0.HARD_HORIZON
    assert episode.exit_observation_open_ms == _T + 25 * _FIVE_MINUTE_MS
    assert episode.exit_reference_time_ms == _T + 27 * _FIVE_MINUTE_MS


def test_entry_admission_applies_adverse_e_at_inclusive_half_atr_boundary() -> None:
    decision = _positive_decision(D1SideV0.LONG)
    assert decision.signal_close is not None
    assert decision.frozen_atr is not None
    pending = subject._PendingEntryStateV0(
        decision=decision,
        signal_index=289,
        entry_reference_index=291,
    )
    with localcontext(protocol_decimal_context_v2()):
        boundary_e = decision.signal_close + Decimal("0.50") * decision.frozen_atr
        boundary_raw = boundary_e / Decimal("1.0008")
        outside_e = decision.signal_close + Decimal("0.500001") * decision.frozen_atr
        outside_raw = outside_e / Decimal("1.0008")

    def entry_candle(price: Decimal) -> Candle:
        return Candle(
            market=Market.FUTURES,
            symbol="BTCUSDT",
            interval="5m",
            open_time_ms=_T + 2 * _FIVE_MINUTE_MS,
            close_time_ms=_T + 3 * _FIVE_MINUTE_MS - 1,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=Decimal(1),
            quote_volume=Decimal("100000"),
            trade_count=1,
            taker_buy_base_volume=Decimal("0.5"),
            taker_buy_quote_volume=Decimal("50000"),
            is_closed=True,
        )

    accepted_counter = subject._RunCountersV0()
    rejected_counter = subject._RunCountersV0()
    accepted = subject._attempt_historical_entry_v0(
        data=_engine_data(),
        pending=pending,
        entry_reference_candle=entry_candle(boundary_raw),
        counters=accepted_counter,
    )
    rejected = subject._attempt_historical_entry_v0(
        data=_engine_data(),
        pending=pending,
        entry_reference_candle=entry_candle(outside_raw),
        counters=rejected_counter,
    )

    assert isinstance(accepted, subject._ActivePositionStateV0)
    assert accepted.position.entry_vwap == boundary_e
    assert accepted_counter.entered_position_count == 1
    assert rejected is None
    assert rejected_counter.entry_distance_rejection_count == 1


def test_resume_cutoff_is_strictly_later_than_exit_fill() -> None:
    assert not subject._signal_cutoff_is_after_fill_v0(fill_ms=1_000, cutoff_ms=1_000)
    assert subject._signal_cutoff_is_after_fill_v0(fill_ms=1_000, cutoff_ms=1_001)
    assert not subject._signal_cutoff_is_after_fill_v0(fill_ms=1_000, cutoff_ms=999)


def _projection(
    *,
    unit_id: str,
    notional: Decimal,
    multiplier: Decimal,
    net: Decimal | None,
) -> subject.D1HistoricalProjectionCellV0:
    fee_cell = dict(subject._FEE_CELLS)[multiplier]
    return subject.D1HistoricalProjectionCellV0(
        statistical_unit_id=unit_id,
        notional_usdt=notional,
        fee_multiplier=multiplier,
        fee_rate_per_side=fee_cell.rate_per_side,
        gross_return=Decimal("0.002"),
        executable_return_before_fee_funding=Decimal("0.0004"),
        slippage_return=Decimal("0.0016"),
        fee_return=Decimal("0.001"),
        funding_return=None if net is None else Decimal(0),
        net_return=net,
        projected_net_pnl_usdt=None if net is None else net * notional,
        _factory_token=subject._PROJECTION_FACTORY_TOKEN,
    )


def _episode(
    index: int,
    *,
    primary: Decimal | None,
    stress: Decimal | None,
    side: D1SideV0 | None = None,
    symbol: str | None = None,
    entry_time_ms: int | None = None,
    exit_time_ms: int | None = None,
) -> subject.D1HistoricalEpisodeV0:
    unit_id = _hash(f"unit:{index}")
    selected_side = side or (D1SideV0.LONG if index % 2 == 0 else D1SideV0.SHORT)
    selected_symbol = symbol or subject.D1_HISTORICAL_UNIVERSE_V0[index % 10]
    entry = (
        _T + (index % 45) * _DAY_MS + (index // 45) * _HOUR_MS
        if entry_time_ms is None
        else entry_time_ms
    )
    exit_time = entry + 2 * _FIVE_MINUTE_MS if exit_time_ms is None else exit_time_ms
    evaluable = primary is not None and stress is not None
    reason = (
        None
        if evaluable
        else subject.D1HistoricalFundingInconclusiveReasonV0.FUNDING_ENDPOINT_EQUALITY
    )
    projections = tuple(
        _projection(
            unit_id=unit_id,
            notional=notional,
            multiplier=multiplier,
            net=(primary if multiplier == Decimal("1.0") else stress),
        )
        for notional in (Decimal("100"), Decimal("1000"))
        for multiplier, _fee_cell in subject._FEE_CELLS
    )
    return subject.D1HistoricalEpisodeV0(
        statistical_unit_id=unit_id,
        symbol=selected_symbol,
        side=selected_side,
        signal_event_id=_hash(f"signal:{index}"),
        signal_payload_sha256=_hash(f"signal-payload:{index}"),
        signal_bar_open_ms=entry - 2 * _FIVE_MINUTE_MS,
        signal_decision_cutoff_ms=entry - _FIVE_MINUTE_MS + 2_000,
        entry_reference_time_ms=entry,
        entry_reference_price=Decimal("100"),
        entry_executable_price=(
            Decimal("100.08") if selected_side is D1SideV0.LONG else Decimal("99.92")
        ),
        exit_observation_open_ms=entry,
        exit_observation_close_ms=entry + _FIVE_MINUTE_MS - 1,
        exit_decision_event_id=_hash(f"exit:{index}"),
        exit_decision_payload_sha256=_hash(f"exit-payload:{index}"),
        exit_reason=D1ExitReasonV0.PROFIT_CLOSE,
        exit_reference_time_ms=exit_time,
        exit_reference_price=Decimal("101"),
        exit_executable_price=(
            Decimal("100.9192") if selected_side is D1SideV0.LONG else Decimal("101.0808")
        ),
        funding_event_count=0,
        funding_evaluable=evaluable,
        funding_inconclusive_reason=reason,
        projections=projections,
        five_minute_manifest_sha256="6" * 64,
        hourly_manifest_sha256="7" * 64,
        funding_file_sha256="8" * 64,
        _factory_token=subject._EPISODE_FACTORY_TOKEN,
    )


def _economically_valid_episode(
    index: int,
    *,
    side: D1SideV0 | None = None,
    symbol: str = "BTCUSDT",
    entry_time_ms: int | None = None,
) -> subject.D1HistoricalEpisodeV0:
    selected_side = side or (D1SideV0.LONG if index % 2 == 0 else D1SideV0.SHORT)
    entry_time = (
        subject.D1_HISTORICAL_DEVELOPMENT_START_MS_V0 + 2 * _FIVE_MINUTE_MS + index * _DAY_MS
        if entry_time_ms is None
        else entry_time_ms
    )
    exit_time = entry_time + 2 * _FIVE_MINUTE_MS
    signal_event_id = _hash(f"economic-signal:{index}")
    exit_decision_event_id = _hash(f"economic-exit:{index}")
    statistical_unit_id = subject._hash_document(
        subject._STATISTICAL_UNIT_ID_DOMAIN,
        {
            "entry_reference_time_ms": entry_time,
            "exit_decision_event_id": exit_decision_event_id,
            "exit_reference_time_ms": exit_time,
            "signal_event_id": signal_event_id,
            "symbol": symbol,
        },
    )
    executions = {
        fee_cell: subject.calculate_d1_historical_execution_v0(
            side=selected_side,
            fee_cell=fee_cell,
            entry_time_ms=entry_time,
            exit_time_ms=exit_time,
            entry_reference_price=Decimal("100"),
            exit_reference_price=Decimal("101"),
            funding_points=(),
        )
        for _multiplier, fee_cell in subject._FEE_CELLS
    }
    projections = tuple(
        subject.D1HistoricalProjectionCellV0(
            statistical_unit_id=statistical_unit_id,
            notional_usdt=notional,
            fee_multiplier=multiplier,
            fee_rate_per_side=fee_cell.rate_per_side,
            gross_return=executions[fee_cell].gross_return,
            executable_return_before_fee_funding=(
                executions[fee_cell].execution_return_before_fee_and_funding
            ),
            slippage_return=executions[fee_cell].slippage_return,
            fee_return=executions[fee_cell].fee_return,
            funding_return=executions[fee_cell].funding_return,
            net_return=executions[fee_cell].net_return,
            projected_net_pnl_usdt=subject.project_d1_historical_pnl_v0(
                executions[fee_cell],
                notional_usdt=notional,
            ),
            _factory_token=subject._PROJECTION_FACTORY_TOKEN,
        )
        for notional in subject._NOTIONALS
        for multiplier, fee_cell in subject._FEE_CELLS
    )
    primary = executions[D1HistoricalFeeCellV0.PRIMARY_1_0]
    return subject.D1HistoricalEpisodeV0(
        statistical_unit_id=statistical_unit_id,
        symbol=symbol,
        side=selected_side,
        signal_event_id=signal_event_id,
        signal_payload_sha256=_hash(f"economic-signal-payload:{index}"),
        signal_bar_open_ms=entry_time - 2 * _FIVE_MINUTE_MS,
        signal_decision_cutoff_ms=entry_time - _FIVE_MINUTE_MS + 2_000,
        entry_reference_time_ms=entry_time,
        entry_reference_price=Decimal("100"),
        entry_executable_price=primary.entry_execution_price,
        exit_observation_open_ms=entry_time,
        exit_observation_close_ms=entry_time + _FIVE_MINUTE_MS - 1,
        exit_decision_event_id=exit_decision_event_id,
        exit_decision_payload_sha256=_hash(f"economic-exit-payload:{index}"),
        exit_reason=D1ExitReasonV0.PROFIT_CLOSE,
        exit_reference_time_ms=exit_time,
        exit_reference_price=Decimal("101"),
        exit_executable_price=primary.exit_execution_price,
        funding_event_count=0,
        funding_evaluable=True,
        funding_inconclusive_reason=None,
        projections=projections,
        five_minute_manifest_sha256="6" * 64,
        hourly_manifest_sha256="7" * 64,
        funding_file_sha256="8" * 64,
        _factory_token=subject._EPISODE_FACTORY_TOKEN,
    )


def _summary(episodes: tuple[subject.D1HistoricalEpisodeV0, ...]):
    return subject._summarize_development_v0(
        episodes=episodes,
        censors=(),
        counters=subject._RunCountersV0(
            full_signal_count=len(episodes),
            entered_position_count=len(episodes),
            prefilter_candidate_count=len(episodes),
        ),
        funding_coverage_status_by_symbol=tuple(
            (
                symbol,
                subject.D1HistoricalFundingCoverageStatusV0.EXACT_STANDARD_8H_DEVELOPMENT_COVERAGE.value,
            )
            for symbol in subject.D1_HISTORICAL_UNIVERSE_V0
        ),
    )


def test_disposition_uses_evaluable_n_not_total_episode_count() -> None:
    episodes = tuple(
        _episode(
            index,
            primary=None if index == 0 else Decimal("-0.001"),
            stress=None if index == 0 else Decimal("-0.0015"),
        )
        for index in range(150)
    )

    summary = _summary(episodes)

    assert summary.episode_count == 150
    assert summary.evaluable_episode_count == 149
    assert summary.disposition is (subject.D1HistoricalDispositionV0.INCONCLUSIVE_LOW_INFORMATION)


def test_primary_negative_mean_and_profit_factor_below_one_rejects_at_150() -> None:
    episodes = tuple(
        _episode(index, primary=Decimal("-0.001"), stress=Decimal("-0.0015"))
        for index in range(150)
    )

    summary = _summary(episodes)

    assert summary.disposition is subject.D1HistoricalDispositionV0.RETROSPECTIVE_PROXY_REJECT
    assert summary.fee_aggregates[0].evaluable_episode_count == 150
    assert summary.fee_aggregates[0].profit_factor == Decimal(0)


def test_global_nonoverlap_guard_prevents_correlated_overlap_from_unlocking_reject() -> None:
    episodes = tuple(
        _episode(
            index,
            primary=Decimal("-0.001"),
            stress=Decimal("-0.0015"),
            entry_time_ms=_T,
            exit_time_ms=_T + 2 * _FIVE_MINUTE_MS,
        )
        for index in range(150)
    )

    summary = _summary(episodes)

    assert summary.evaluable_episode_count == 150
    assert summary.global_nonoverlap_evaluable_count == 1
    assert summary.disposition is subject.D1HistoricalDispositionV0.INCONCLUSIVE_LOW_INFORMATION


def test_global_earliest_exit_schedule_accepts_equality_and_is_deterministic() -> None:
    first = _episode(
        0,
        primary=Decimal("0.001"),
        stress=Decimal("0.0008"),
        entry_time_ms=_T,
        exit_time_ms=_T + 2 * _FIVE_MINUTE_MS,
    )
    overlaps = _episode(
        1,
        primary=Decimal("0.001"),
        stress=Decimal("0.0008"),
        entry_time_ms=_T + _FIVE_MINUTE_MS,
        exit_time_ms=_T + 3 * _FIVE_MINUTE_MS,
    )
    equality = _episode(
        2,
        primary=Decimal("0.001"),
        stress=Decimal("0.0008"),
        entry_time_ms=_T + 2 * _FIVE_MINUTE_MS,
        exit_time_ms=_T + 4 * _FIVE_MINUTE_MS,
    )

    forward = subject._select_global_nonoverlap_evaluable_v0((first, overlaps, equality))
    reverse = subject._select_global_nonoverlap_evaluable_v0((equality, overlaps, first))

    assert tuple(value.statistical_unit_id for value in forward) == (
        first.statistical_unit_id,
        equality.statistical_unit_id,
    )
    assert tuple(value.statistical_unit_id for value in reverse) == tuple(
        value.statistical_unit_id for value in forward
    )


def test_screen_pass_requires_both_fee_cells_and_concentration_survival() -> None:
    passing = tuple(
        _episode(index, primary=Decimal("0.001"), stress=Decimal("0.0008")) for index in range(500)
    )
    stressed_failure = tuple(
        _episode(index, primary=Decimal("0.001"), stress=Decimal("0.0004")) for index in range(500)
    )

    passed = _summary(passing)
    failed = _summary(stressed_failure)

    assert passed.disposition is (
        subject.D1HistoricalDispositionV0.RETROSPECTIVE_PROXY_SCREEN_PASS_INCONCLUSIVE
    )
    assert failed.disposition is (
        subject.D1HistoricalDispositionV0.INCONCLUSIVE_MIXED_PROXY_EVIDENCE
    )
    assert len(passed.breakdowns) == 2 * (1 + 10 + 2 + 20 + 5)
    overall = passed.breakdowns[0]
    assert overall.kind is subject.D1HistoricalBreakdownKindV0.OVERALL
    assert overall.evaluable_episode_count == 500
    assert overall.projected_total_pnl_100_usdt == Decimal("50.000")


def _authority_bindings() -> tuple[subject.D1HistoricalKlineManifestBindingV0, ...]:
    return tuple(
        subject.D1HistoricalKlineManifestBindingV0(
            symbol=symbol,
            interval=interval,
            relative_manifest_path=f"klines/{symbol}-{interval}.json",
            manifest_sha256=_hash(f"{symbol}:{interval}"),
        )
        for symbol in subject.D1_HISTORICAL_UNIVERSE_V0
        for interval in ("5m", "1h")
    )


def _funding_bindings() -> tuple[subject.D1HistoricalFundingFileBindingV0, ...]:
    return tuple(
        subject.D1HistoricalFundingFileBindingV0(
            symbol=symbol,
            relative_path=f"funding/{symbol}.csv.gz",
            sha256=_hash(f"funding:{symbol}"),
        )
        for symbol in subject.D1_HISTORICAL_UNIVERSE_V0
    )


def _empty_replay_inputs() -> tuple[subject.D1HistoricalReplaySymbolInputV0, ...]:
    return tuple(
        subject.D1HistoricalReplaySymbolInputV0(
            symbol=symbol,
            five_minute_manifest_sha256="2" * 64,
            higher_timeframe_source_sha256="3" * 64,
            funding_file_sha256="4" * 64,
            source_root_sha256=_hash(f"source:{symbol}"),
            five_minute=(),
            hourly=(),
            funding=(),
            exact_standard_8h_development_funding_coverage=index % 2 == 0,
        )
        for index, symbol in enumerate(subject.D1_HISTORICAL_UNIVERSE_V0)
    )


def _result_bundle(
    episodes: tuple[subject.D1HistoricalEpisodeV0, ...],
) -> tuple[
    subject.D1HistoricalDevelopmentResultV0,
    subject.D1HistoricalInputAuthorityV0,
    subject.D1HistoricalDevelopmentFreezeV0,
]:
    authority = subject.build_d1_historical_input_authority_v0(
        kline_manifests=_authority_bindings(),
        funding_manifest_relative_path="funding/authority.json",
        funding_manifest_sha256="9" * 64,
    )
    freeze = subject.D1HistoricalDevelopmentFreezeV0(
        manifest_sha256="a" * 64,
        manifest_created_at_ms=_T - 1,
        input_authority_sha256=authority.authority_sha256,
        preregistration_sha256="b" * 64,
        frozen_file_count=20,
        _factory_token=subject._FREEZE_FACTORY_TOKEN,
    )
    summary = _summary(episodes)
    result = subject.D1HistoricalDevelopmentResultV0(
        run_id="synthetic-run",
        run_started_at_ms=_T,
        input_authority_sha256=authority.authority_sha256,
        code_freeze_receipt_sha256=freeze.receipt_sha256,
        code_freeze_manifest_sha256=freeze.manifest_sha256,
        preregistration_sha256=freeze.preregistration_sha256,
        episodes=episodes,
        censors=(),
        summary=summary,
        _factory_token=subject._RESULT_FACTORY_TOKEN,
    )
    return result, authority, freeze


def _serialized_document(raw: bytes) -> dict[str, object]:
    value = json.loads(raw)
    assert isinstance(value, dict)
    return value


def _rehash_serialized_document(
    document: dict[str, object],
    *,
    hash_field: str,
    domain: bytes,
) -> bytes:
    body = dict(document)
    body.pop(hash_field, None)
    document[hash_field] = subject._hash_document(domain, body)
    return canonical_json_line(document)


def _coherent_serialized_verifier_fixture() -> tuple[
    list[dict[str, object]],
    dict[str, object],
    dict[str, object],
    subject.D1HistoricalDevelopmentResultV0,
    subject.D1HistoricalInputAuthorityV0,
    subject.D1HistoricalDevelopmentFreezeV0,
]:
    episodes = (
        _economically_valid_episode(
            0,
            symbol="BTCUSDT",
        ),
        _economically_valid_episode(
            1,
            symbol="BTCUSDT",
        ),
    )
    result, authority, freeze = _result_bundle(episodes)
    return (
        [
            _serialized_document(subject.canonical_d1_historical_episode_v0(value))
            for value in episodes
        ],
        _serialized_document(subject.canonical_d1_historical_summary_v0(result.summary)),
        _serialized_document(subject.canonical_d1_historical_development_result_v0(result)),
        result,
        authority,
        freeze,
    )


def _verify_coherently_rehashed_fixture(
    *,
    episode_documents: list[dict[str, object]],
    summary_document: dict[str, object],
    result_document: dict[str, object],
    result: subject.D1HistoricalDevelopmentResultV0,
    authority: subject.D1HistoricalInputAuthorityV0,
    freeze: subject.D1HistoricalDevelopmentFreezeV0,
) -> subject.D1HistoricalSerializedArtifactsVerificationV0:
    episode_lines = tuple(
        _rehash_serialized_document(
            value,
            hash_field="episode_sha256",
            domain=subject._EPISODE_HASH_DOMAIN,
        )
        for value in episode_documents
    )
    episode_hashes = tuple(str(value["episode_sha256"]) for value in episode_documents)
    summary_raw = _rehash_serialized_document(
        summary_document,
        hash_field="summary_sha256",
        domain=subject._SUMMARY_HASH_DOMAIN,
    )
    result_document["episode_count"] = len(episode_documents)
    result_document["episode_sequence_root_sha256"] = subject._ordered_hash_root(
        subject._EPISODE_SEQUENCE_ROOT_DOMAIN,
        episode_hashes,
    )
    result_document["summary_sha256"] = summary_document["summary_sha256"]
    result_index_raw = _rehash_serialized_document(
        result_document,
        hash_field="result_sha256",
        domain=subject._RESULT_HASH_DOMAIN,
    )
    return subject.verify_d1_historical_serialized_artifacts_v0(
        episode_lines=episode_lines,
        censor_lines=(),
        summary_raw=summary_raw,
        result_index_raw=result_index_raw,
        expected_run_id=result.run_id,
        expected_run_started_at_ms=result.run_started_at_ms,
        expected_input_authority_sha256=authority.authority_sha256,
        expected_code_freeze_manifest_sha256=freeze.manifest_sha256,
        expected_code_freeze_receipt_sha256=freeze.receipt_sha256,
        expected_preregistration_sha256=freeze.preregistration_sha256,
    )


def test_serialized_artifact_verifier_recomputes_exact_domains_roots_and_bindings() -> None:
    episode = _economically_valid_episode(0)
    result, authority, freeze = _result_bundle((episode,))

    verified = subject.verify_d1_historical_serialized_artifacts_v0(
        episode_lines=(subject.canonical_d1_historical_episode_v0(episode),),
        censor_lines=(),
        summary_raw=subject.canonical_d1_historical_summary_v0(result.summary),
        result_index_raw=subject.canonical_d1_historical_development_result_v0(result),
        expected_run_id=result.run_id,
        expected_run_started_at_ms=result.run_started_at_ms,
        expected_input_authority_sha256=authority.authority_sha256,
        expected_code_freeze_manifest_sha256=freeze.manifest_sha256,
        expected_code_freeze_receipt_sha256=freeze.receipt_sha256,
        expected_preregistration_sha256=freeze.preregistration_sha256,
    )

    assert verified.result_sha256 == result.result_sha256
    assert verified.summary_sha256 == result.summary.summary_sha256
    assert verified.episode_count == 1
    assert verified.censor_count == 0


def test_serialized_artifact_verifier_rejects_minimal_or_domain_tampered_result() -> None:
    episode = _economically_valid_episode(0)
    result, authority, freeze = _result_bundle((episode,))
    common = {
        "episode_lines": (subject.canonical_d1_historical_episode_v0(episode),),
        "censor_lines": (),
        "summary_raw": subject.canonical_d1_historical_summary_v0(result.summary),
        "expected_run_id": result.run_id,
        "expected_run_started_at_ms": result.run_started_at_ms,
        "expected_input_authority_sha256": authority.authority_sha256,
        "expected_code_freeze_manifest_sha256": freeze.manifest_sha256,
        "expected_code_freeze_receipt_sha256": freeze.receipt_sha256,
        "expected_preregistration_sha256": freeze.preregistration_sha256,
    }
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="fields"):
        subject.verify_d1_historical_serialized_artifacts_v0(
            **common,
            result_index_raw=canonical_json_line({"result_sha256": result.result_sha256}),
        )

    raw_episode = subject.canonical_d1_historical_episode_v0(episode)
    tampered_episode = raw_episode.replace(b'"side":"LONG"', b'"side":"SHORT"')
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="domain hash"):
        subject.verify_d1_historical_serialized_artifacts_v0(
            **{**common, "episode_lines": (tampered_episode,)},
            result_index_raw=subject.canonical_d1_historical_development_result_v0(result),
        )


def test_serialized_verifier_rejects_coherently_rehashed_invalid_decimal() -> None:
    episode_documents, summary, result_index, result, authority, freeze = (
        _coherent_serialized_verifier_fixture()
    )
    episode_documents[0]["entry_reference_price"] = "NOT_A_DECIMAL"

    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="finite decimal string",
    ):
        _verify_coherently_rehashed_fixture(
            episode_documents=episode_documents,
            summary_document=summary,
            result_document=result_index,
            result=result,
            authority=authority,
            freeze=freeze,
        )


@pytest.mark.parametrize(
    ("attack", "error"),
    (
        ("reordered", "strict symbol/time order"),
        ("duplicate", "duplicate identity"),
    ),
)
def test_serialized_verifier_rejects_coherently_rehashed_sequence_attacks(
    attack: str,
    error: str,
) -> None:
    episode_documents, summary, result_index, result, authority, freeze = (
        _coherent_serialized_verifier_fixture()
    )
    if attack == "reordered":
        episode_documents.reverse()
    else:
        episode_documents[1] = dict(episode_documents[0])

    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match=error):
        _verify_coherently_rehashed_fixture(
            episode_documents=episode_documents,
            summary_document=summary,
            result_document=result_index,
            result=result,
            authority=authority,
            freeze=freeze,
        )


def test_serialized_verifier_rejects_coherently_rehashed_projection_order() -> None:
    episode_documents, summary, result_index, result, authority, freeze = (
        _coherent_serialized_verifier_fixture()
    )
    projections = episode_documents[0]["projections"]
    assert isinstance(projections, list)
    assert isinstance(projections[0], dict)
    projections[1] = dict(projections[0])

    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="projection cells must be exact, ordered",
    ):
        _verify_coherently_rehashed_fixture(
            episode_documents=episode_documents,
            summary_document=summary,
            result_document=result_index,
            result=result,
            authority=authority,
            freeze=freeze,
        )


def test_serialized_verifier_rejects_coherently_rehashed_summary_aggregate() -> None:
    episode_documents, summary, result_index, result, authority, freeze = (
        _coherent_serialized_verifier_fixture()
    )
    fee_aggregates = summary["fee_aggregates"]
    assert isinstance(fee_aggregates, list)
    assert isinstance(fee_aggregates[0], dict)
    fee_aggregates[0]["total_net_return"] = "999999999"

    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="differs from the exact episode reducer",
    ):
        _verify_coherently_rehashed_fixture(
            episode_documents=episode_documents,
            summary_document=summary,
            result_document=result_index,
            result=result,
            authority=authority,
            freeze=freeze,
        )


@pytest.mark.parametrize(
    ("attack", "error"),
    (
        ("executable_price", "executable prices differ"),
        ("gross", "return decomposition differs"),
        ("fee", "fee arithmetic differs"),
        ("funding_divergence", "funding return diverges"),
        ("net", "net-return arithmetic differs"),
        ("pnl", "PnL arithmetic differs"),
        ("statistical_unit", "statistical unit differs"),
    ),
)
def test_serialized_verifier_rejects_coherently_rehashed_economic_math_attacks(
    attack: str,
    error: str,
) -> None:
    episode_documents, summary, result_index, result, authority, freeze = (
        _coherent_serialized_verifier_fixture()
    )
    episode = episode_documents[0]
    projections = episode["projections"]
    assert isinstance(projections, list)
    assert all(isinstance(value, dict) for value in projections)
    first_projection = projections[0]
    assert isinstance(first_projection, dict)
    if attack == "executable_price":
        episode["entry_executable_price"] = "100.09"
    elif attack == "gross":
        first_projection["gross_return"] = "0.02"
    elif attack == "fee":
        first_projection["fee_return"] = "0.002"
    elif attack == "funding_divergence":
        first_projection["funding_return"] = "0.0001"
    elif attack == "net":
        first_projection["net_return"] = "0.123"
    elif attack == "pnl":
        first_projection["projected_net_pnl_usdt"] = "999"
    else:
        forged_unit_id = _hash("coherently-forged-statistical-unit")
        episode["statistical_unit_id"] = forged_unit_id
        for projection in projections:
            assert isinstance(projection, dict)
            projection["statistical_unit_id"] = forged_unit_id

    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match=error):
        _verify_coherently_rehashed_fixture(
            episode_documents=episode_documents,
            summary_document=summary,
            result_document=result_index,
            result=result,
            authority=authority,
            freeze=freeze,
        )


def test_serialized_verifier_rejects_coherently_rehashed_timing_attack() -> None:
    episode_documents, summary, result_index, result, authority, freeze = (
        _coherent_serialized_verifier_fixture()
    )
    cutoff = episode_documents[0]["signal_decision_cutoff_ms"]
    assert isinstance(cutoff, int)
    episode_documents[0]["signal_decision_cutoff_ms"] = cutoff + 1

    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="signal cutoff differs",
    ):
        _verify_coherently_rehashed_fixture(
            episode_documents=episode_documents,
            summary_document=summary,
            result_document=result_index,
            result=result,
            authority=authority,
            freeze=freeze,
        )


def test_serialized_verifier_rejects_coherently_rehashed_funding_coverage_attack() -> None:
    episode_documents, summary, result_index, result, authority, freeze = (
        _coherent_serialized_verifier_fixture()
    )
    coverage = summary["funding_coverage_status_by_symbol"]
    assert isinstance(coverage, list)
    assert isinstance(coverage[0], list)
    coverage[0][1] = subject.D1HistoricalFundingCoverageStatusV0.FUNDING_COVERAGE_UNAVAILABLE.value

    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="funding coverage and episode reason differ",
    ):
        _verify_coherently_rehashed_fixture(
            episode_documents=episode_documents,
            summary_document=summary,
            result_document=result_index,
            result=result,
            authority=authority,
            freeze=freeze,
        )


def test_serialized_verifier_rejects_coherently_rehashed_cross_role_event_id() -> None:
    episode_documents, summary, result_index, result, authority, freeze = (
        _coherent_serialized_verifier_fixture()
    )
    first_exit_event_id = episode_documents[0]["exit_decision_event_id"]
    assert isinstance(first_exit_event_id, str)
    second = episode_documents[1]
    second["signal_event_id"] = first_exit_event_id
    second_unit_id = subject._hash_document(
        subject._STATISTICAL_UNIT_ID_DOMAIN,
        {
            "entry_reference_time_ms": second["entry_reference_time_ms"],
            "exit_decision_event_id": second["exit_decision_event_id"],
            "exit_reference_time_ms": second["exit_reference_time_ms"],
            "signal_event_id": second["signal_event_id"],
            "symbol": second["symbol"],
        },
    )
    second["statistical_unit_id"] = second_unit_id
    projections = second["projections"]
    assert isinstance(projections, list)
    for projection in projections:
        assert isinstance(projection, dict)
        projection["statistical_unit_id"] = second_unit_id

    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="duplicate event identity",
    ):
        _verify_coherently_rehashed_fixture(
            episode_documents=episode_documents,
            summary_document=summary,
            result_document=result_index,
            result=result,
            authority=authority,
            freeze=freeze,
        )


def test_serialized_verifier_rejects_coherently_rehashed_manifest_drift() -> None:
    episode_documents, summary, result_index, result, authority, freeze = (
        _coherent_serialized_verifier_fixture()
    )
    episode_documents[1]["five_minute_manifest_sha256"] = "c" * 64

    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="manifests vary within one symbol",
    ):
        _verify_coherently_rehashed_fixture(
            episode_documents=episode_documents,
            summary_document=summary,
            result_document=result_index,
            result=result,
            authority=authority,
            freeze=freeze,
        )


def test_input_and_funding_authorities_require_exact_order_and_are_canonical() -> None:
    authority = subject.build_d1_historical_input_authority_v0(
        kline_manifests=_authority_bindings(),
        funding_manifest_relative_path="funding/authority.json",
        funding_manifest_sha256="9" * 64,
    )
    funding = tuple(
        subject.D1HistoricalFundingFileBindingV0(
            symbol=symbol,
            relative_path=f"funding/{symbol}.csv.gz",
            sha256=_hash(symbol),
        )
        for symbol in subject.D1_HISTORICAL_UNIVERSE_V0
    )

    assert subject.canonical_d1_historical_input_authority_v0(authority).endswith(b"\n")
    assert subject.canonical_d1_historical_funding_authority_manifest_v0(funding).endswith(b"\n")
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="ordered"):
        subject.build_d1_historical_input_authority_v0(
            kline_manifests=tuple(reversed(_authority_bindings())),
            funding_manifest_relative_path="funding/authority.json",
            funding_manifest_sha256="9" * 64,
        )
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="ordered"):
        subject.canonical_d1_historical_funding_authority_manifest_v0(tuple(reversed(funding)))


def test_public_authenticated_loader_wrappers_preserve_verified_payloads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _authority_bindings()[0]
    candle = _candle_from_bar(
        "BTCUSDT",
        _sealed_bar(open_ms=_T, receipt_ms=_T + _FIVE_MINUTE_MS),
    )
    loaded_kline = subject._LoadedKlineFileV0(
        symbol="BTCUSDT",
        interval="5m",
        manifest_sha256=binding.manifest_sha256,
        data_sha256="d" * 64,
        candles=(candle,),
    )
    observed_kline_roots: list[Path] = []

    def fake_load_kline(
        *,
        root: Path,
        binding: subject.D1HistoricalKlineManifestBindingV0,
    ) -> subject._LoadedKlineFileV0:
        observed_kline_roots.append(root)
        assert binding.interval == "5m"
        return loaded_kline

    monkeypatch.setattr(subject, "_load_authenticated_kline_v0", fake_load_kline)
    five = subject.load_d1_historical_authenticated_five_minute_v0(
        data_root=tmp_path,
        binding=binding,
    )

    assert five == subject.D1HistoricalAuthenticatedFiveMinuteV0(
        symbol="BTCUSDT",
        manifest_sha256=binding.manifest_sha256,
        data_sha256="d" * 64,
        candles=(candle,),
        _factory_token=subject._AUTHENTICATED_FIVE_MINUTE_FACTORY_TOKEN,
    )
    assert observed_kline_roots == [tmp_path.resolve()]
    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="exact 5m binding",
    ):
        subject.load_d1_historical_authenticated_five_minute_v0(
            data_root=tmp_path,
            binding=_authority_bindings()[1],
        )

    funding_bindings = _funding_bindings()
    funding_manifest_raw = subject.canonical_d1_historical_funding_authority_manifest_v0(
        funding_bindings
    )
    funding_manifest_relative_path = "funding/authority.json"
    funding_manifest_path = tmp_path / funding_manifest_relative_path
    funding_manifest_path.parent.mkdir(parents=True)
    funding_manifest_path.write_bytes(funding_manifest_raw)
    funding_manifest_sha256 = hashlib.sha256(funding_manifest_raw).hexdigest()
    authority = subject.build_d1_historical_input_authority_v0(
        kline_manifests=_authority_bindings(),
        funding_manifest_relative_path=funding_manifest_relative_path,
        funding_manifest_sha256=funding_manifest_sha256,
    )
    funding = tuple(
        subject.D1HistoricalAuthenticatedFundingV0(
            symbol=value.symbol,
            file_sha256=value.sha256,
            start_time_ms=subject.D1_HISTORICAL_DATA_START_MS_V0,
            end_time_ms=subject.D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1,
            points=(),
            exact_standard_8h_development_coverage=False,
            _factory_token=subject._AUTHENTICATED_FUNDING_FACTORY_TOKEN,
        )
        for value in funding_bindings
    )
    observed_funding_roots: list[Path] = []
    observed_funding_bindings: list[subject.D1HistoricalFundingFileBindingV0] = []

    def fake_load_funding(
        *,
        root: Path,
        binding: subject.D1HistoricalFundingFileBindingV0,
    ) -> subject.D1HistoricalAuthenticatedFundingV0:
        observed_funding_roots.append(root)
        observed_funding_bindings.append(binding)
        return funding[subject.D1_HISTORICAL_UNIVERSE_V0.index(binding.symbol)]

    monkeypatch.setattr(subject, "_load_authenticated_funding_file_v0", fake_load_funding)

    assert subject.load_d1_historical_authenticated_funding_bindings_v0(
        data_root=tmp_path,
        funding_manifest_relative_path=funding_manifest_relative_path,
        funding_manifest_sha256=funding_manifest_sha256,
        funding_files=funding_bindings,
    ) == funding
    assert subject.load_d1_historical_authenticated_funding_v0(
        data_root=tmp_path,
        authority=authority,
    ) == funding
    assert observed_funding_roots == [tmp_path.resolve()] * 20
    assert tuple(observed_funding_bindings) == funding_bindings * 2


def test_generalized_funding_loader_rejects_binding_or_manifest_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bindings = _funding_bindings()
    raw = subject.canonical_d1_historical_funding_authority_manifest_v0(bindings)
    relative_path = "funding/authority.json"
    path = tmp_path / relative_path
    path.parent.mkdir(parents=True)
    path.write_bytes(raw)
    manifest_sha256 = hashlib.sha256(raw).hexdigest()
    load_count = 0

    def fail_if_loaded(
        *,
        root: Path,
        binding: subject.D1HistoricalFundingFileBindingV0,
    ) -> subject.D1HistoricalAuthenticatedFundingV0:
        nonlocal load_count
        load_count += 1
        raise AssertionError(f"unexpected funding load from {root}: {binding.symbol}")

    monkeypatch.setattr(subject, "_load_authenticated_funding_file_v0", fail_if_loaded)
    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="exact ordered D1 universe",
    ):
        subject.load_d1_historical_authenticated_funding_bindings_v0(
            data_root=tmp_path,
            funding_manifest_relative_path=relative_path,
            funding_manifest_sha256=manifest_sha256,
            funding_files=tuple(reversed(bindings)),
        )

    drifted = (replace(bindings[0], sha256="f" * 64), *bindings[1:])
    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="differs from supplied bindings",
    ):
        subject.load_d1_historical_authenticated_funding_bindings_v0(
            data_root=tmp_path,
            funding_manifest_relative_path=relative_path,
            funding_manifest_sha256=manifest_sha256,
            funding_files=drifted,
        )
    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="manifest hash differs",
    ):
        subject.load_d1_historical_authenticated_funding_bindings_v0(
            data_root=tmp_path,
            funding_manifest_relative_path=relative_path,
            funding_manifest_sha256="0" * 64,
            funding_files=bindings,
        )
    assert load_count == 0


def test_replay_core_preserves_legacy_orchestration_canonical_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _empty_replay_inputs()
    _unused_result, authority, freeze = _result_bundle(())
    calls: list[tuple[str, str, int, int]] = []

    def fake_run_symbol(
        *,
        data: subject.D1HistoricalReplaySymbolInputV0,
        run_id: str,
        decision_start_ms: int,
        decision_end_ms: int,
    ) -> subject._SymbolRunResultV0:
        calls.append((data.symbol, run_id, decision_start_ms, decision_end_ms))
        return subject._SymbolRunResultV0(
            symbol=data.symbol,
            exact_standard_8h_development_funding_coverage=(
                data.exact_standard_8h_development_funding_coverage
            ),
            episodes=(),
            censors=(),
            counters=subject._RunCountersV0(),
        )

    monkeypatch.setattr(subject, "_run_symbol_development_v0", fake_run_symbol)
    core = subject.run_d1_historical_replay_core_v0(
        symbol_inputs=inputs,
        run_id="synthetic-run",
        decision_start_ms=subject.D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
        decision_end_ms=subject.D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
    )

    def fake_input_panel(
        *,
        data_root: str | Path,
        authority: subject.D1HistoricalInputAuthorityV0,
    ):
        assert data_root == "unused"
        assert authority.authority_sha256
        return iter(inputs)

    monkeypatch.setattr(subject, "_iter_authenticated_input_panel_v0", fake_input_panel)
    legacy = subject.run_d1_historical_development_v0(
        data_root="unused",
        input_authority=authority,
        code_freeze=freeze,
        run_id="synthetic-run",
        run_started_at_ms=_T,
    )
    raw = subject.canonical_d1_historical_development_result_v0(legacy)

    assert legacy.episodes == core.episodes
    assert legacy.censors == core.censors
    assert legacy.summary == core.summary
    assert legacy.summary.summary_sha256 == (
        "56c47eaa56928a83f9a1eeee763d8020f9cc7a4d24d05e640254d67953c4a8ea"
    )
    assert legacy.result_sha256 == (
        "3b35fb1f0bcbc8da7cc294e5174f3d4a39b90ccc9da5e994901eeee00b2702b5"
    )
    assert hashlib.sha256(raw).hexdigest() == (
        "967ed562aed04ee3ed8f1bbf52eca287150654585a776ddb8b5128f7977c280c"
    )
    assert len(raw) == 1532
    assert tuple(value[0] for value in calls) == subject.D1_HISTORICAL_UNIVERSE_V0 * 2
    assert all(
        value[1:] == (
            "synthetic-run",
            subject.D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
            subject.D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
        )
        for value in calls
    )

    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="exact ordered D1 universe",
    ):
        subject.run_d1_historical_replay_core_v0(
            symbol_inputs=inputs[:-1],
            run_id="synthetic-run",
            decision_start_ms=subject.D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
            decision_end_ms=subject.D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
        )


def test_extracted_replay_core_matches_nonempty_pre_extraction_golden() -> None:
    """Lock extraction equivalence to the immutable, pre-extraction state machine.

    The literal hashes below were generated in a clean interpreter with
    ``frozen-failure-evidence.zip/workspace/src`` first on ``sys.path``.  That
    archive has SHA-256 ``f44e4c38aefeb5542c8875e3625ab01e82cde1fd4ff7738e26684b9895a25592``;
    its pre-extraction module member has SHA-256
    ``5174299fb8430bf61a70f69574c69b675814f2ce2077efb1eeeea6e35b8176ec``.
    The archived ``_run_symbol_development_v0`` ran this deterministic panel
    for every symbol, after which its own counter combiner and summarizer made
    the recorded episode, censor, and summary canonical hashes.  No market
    data, outcome data, current-core output, or patched state-machine function
    participated in golden generation.
    """

    decision_start_ms = subject.D1_HISTORICAL_DEVELOPMENT_START_MS_V0
    timestamp_shift_ms = decision_start_ms - _T

    def replay_input(
        *,
        symbol: str,
        mode: str,
        exact_funding_coverage: bool,
    ) -> subject.D1HistoricalReplaySymbolInputV0:
        source = _engine_data(trigger_profit=mode == "episode")

        def shifted_candle(value: Candle, *, neutral: bool = False) -> Candle:
            neutral_decision_bar = neutral and value.open_time_ms >= _T
            return Candle(
                market=value.market,
                symbol=symbol,
                interval=value.interval,
                open_time_ms=value.open_time_ms + timestamp_shift_ms,
                close_time_ms=value.close_time_ms + timestamp_shift_ms,
                open=Decimal("100") if neutral_decision_bar else value.open,
                high=Decimal("100.5") if neutral_decision_bar else value.high,
                low=Decimal("99.5") if neutral_decision_bar else value.low,
                close=Decimal("100") if neutral_decision_bar else value.close,
                volume=value.volume,
                quote_volume=(
                    Decimal("100000") if neutral_decision_bar else value.quote_volume
                ),
                trade_count=value.trade_count,
                taker_buy_base_volume=value.taker_buy_base_volume,
                taker_buy_quote_volume=(
                    Decimal("50000")
                    if neutral_decision_bar
                    else value.taker_buy_quote_volume
                ),
                is_closed=value.is_closed,
            )

        return subject.D1HistoricalReplaySymbolInputV0(
            symbol=symbol,
            five_minute_manifest_sha256="2" * 64,
            higher_timeframe_source_sha256="3" * 64,
            funding_file_sha256="4" * 64,
            source_root_sha256=_hash(f"source:{symbol}"),
            five_minute=tuple(
                shifted_candle(value, neutral=mode == "neutral")
                for value in source.five_minute
            ),
            hourly=tuple(shifted_candle(value) for value in source.hourly),
            funding=(),
            exact_standard_8h_development_funding_coverage=exact_funding_coverage,
        )

    modes = ("episode", "censor", *("neutral" for _ in range(8)))
    inputs = tuple(
        replay_input(
            symbol=symbol,
            mode=mode,
            exact_funding_coverage=index % 2 == 0,
        )
        for index, (symbol, mode) in enumerate(
            zip(subject.D1_HISTORICAL_UNIVERSE_V0, modes, strict=True)
        )
    )

    core = subject.run_d1_historical_replay_core_v0(
        symbol_inputs=inputs,
        run_id="synthetic-golden-run",
        decision_start_ms=decision_start_ms,
        decision_end_ms=decision_start_ms + 10 * _FIVE_MINUTE_MS,
    )

    assert len(core.episodes) == 1
    assert core.episodes[0].exit_reason is D1ExitReasonV0.PROFIT_CLOSE
    assert len(core.censors) == 1
    assert core.censors[0].stage is subject.D1HistoricalCensorStageV0.EXIT_OBSERVATION
    assert core.summary.entered_position_count == 2
    assert core.summary.full_signal_count == 2
    assert core.summary.right_edge_censor_count == 1

    episode_raw = subject.canonical_d1_historical_episode_v0(core.episodes[0])
    censor_raw = subject.canonical_d1_historical_censor_v0(core.censors[0])
    summary_raw = subject.canonical_d1_historical_summary_v0(core.summary)
    assert core.episodes[0].episode_sha256 == (
        "3f56ccb91f3e6ce0dee7e905b7d7faaf568ebe80eaa25816ccbee7d742ed2ea6"
    )
    assert hashlib.sha256(episode_raw).hexdigest() == (
        "23b7f554fd454edf77b5aaf351e59402357ae65ad67a860cf106c05e012178e9"
    )
    assert core.censors[0].censor_sha256 == (
        "b0c0a44e8fc6fc9dcb6e03cbde5a73c18682716a9143f9c7af0b5ab97143cc77"
    )
    assert hashlib.sha256(censor_raw).hexdigest() == (
        "65965f4945a25d885b5543beb34daa8331f9cb17e46a56c5765d281d1c22ff2d"
    )
    assert core.summary.summary_sha256 == (
        "a8631c9a3af7a328951e203043eccfc76d366de9e6d8179ba6c09e893534d2e6"
    )
    assert hashlib.sha256(summary_raw).hexdigest() == (
        "299d24125c5cc616b72d47367075dc3bd8a052fbd27cdfba4052a0361eeb2471"
    )


def _downstream_freeze_authority(
    *,
    input_authority_sha256: str,
    preregistration_sha256: str,
    frozen_preregistration_sha256: str | None,
):
    file_sha256 = {
        subject._RUNNER_RELATIVE_PATH: _hash("runner"),
        subject._RULE_RELATIVE_PATH: _hash("rule"),
        subject.D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_RELATIVE_PATH_V0: (
            subject.D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0
        ),
    }
    if frozen_preregistration_sha256 is not None:
        file_sha256[subject._PREREGISTRATION_RELATIVE_PATH] = frozen_preregistration_sha256
    return subject.DownstreamCodeFreezeAuthorityV1(
        manifest_path=Path("freeze.json"),
        manifest_sha256=_hash("freeze-manifest"),
        created_at_utc="2026-07-21T00:00:00+00:00",
        purpose=subject.D1_DEVELOPMENT_FREEZE_PURPOSE_V0,
        include_trees=subject.D1_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0,
        include_files=subject.D1_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0,
        included_suffixes=subject.D1_DEVELOPMENT_FREEZE_SUFFIXES_V0,
        upstream_sha256={
            "d1_input_authority": input_authority_sha256,
            "d1_predecessor_freeze_001": (
                subject.D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0
            ),
            "d1_preregistration": preregistration_sha256,
        },
        file_sha256=file_sha256,
        file_size_bytes={name: 1 for name in file_sha256},
    )


def test_freeze_requires_prereg_binding_to_equal_actual_frozen_file_hash() -> None:
    input_hash = _hash("input-authority")
    prereg_hash = _hash("preregistration")
    authority = _downstream_freeze_authority(
        input_authority_sha256=input_hash,
        preregistration_sha256=prereg_hash,
        frozen_preregistration_sha256=prereg_hash,
    )

    receipt = subject._validate_freeze_authority(
        authority,
        input_authority_sha256=input_hash,
        preregistration_sha256=prereg_hash,
    )

    assert receipt.preregistration_sha256 == prereg_hash


def test_real_generic_freeze_round_trip_matches_canonical_d1_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner_relative = "src/runner.py"
    rule_relative = "src/rule.py"
    predecessor_relative = "predecessor.json"
    preregistration_relative = "prereg.md"
    include_files = (
        ".python-version",
        "correction.md",
        predecessor_relative,
        preregistration_relative,
    )
    payloads = {
        ".python-version": b"3.12\n",
        "correction.md": b"outcome-blind correction\n",
        predecessor_relative: b"retired freeze\n",
        preregistration_relative: b"fixed preregistration\n",
        runner_relative: b"RUNNER = 1\n",
        rule_relative: b"RULE = 1\n",
    }
    for relative, raw in payloads.items():
        path = tmp_path.joinpath(*relative.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    predecessor_sha256 = hashlib.sha256(payloads[predecessor_relative]).hexdigest()
    preregistration_sha256 = hashlib.sha256(
        payloads[preregistration_relative]
    ).hexdigest()
    monkeypatch.setattr(subject, "D1_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0", ("src",))
    monkeypatch.setattr(subject, "D1_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0", include_files)
    monkeypatch.setattr(subject, "D1_DEVELOPMENT_FREEZE_SUFFIXES_V0", (".py",))
    monkeypatch.setattr(subject, "_RUNNER_RELATIVE_PATH", runner_relative)
    monkeypatch.setattr(subject, "_RULE_RELATIVE_PATH", rule_relative)
    monkeypatch.setattr(
        subject,
        "_PREREGISTRATION_RELATIVE_PATH",
        preregistration_relative,
    )
    monkeypatch.setattr(
        subject,
        "D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_RELATIVE_PATH_V0",
        predecessor_relative,
    )
    monkeypatch.setattr(
        subject,
        "D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0",
        predecessor_sha256,
    )

    input_authority = subject.build_d1_historical_input_authority_v0(
        kline_manifests=_authority_bindings(),
        funding_manifest_relative_path="funding/authority.json",
        funding_manifest_sha256="9" * 64,
    )
    upstream_sha256 = {
        "d1_input_authority": input_authority.authority_sha256,
        "d1_predecessor_freeze_001": predecessor_sha256,
        "d1_preregistration": preregistration_sha256,
    }
    manifest = tmp_path / "artifacts/freeze.json"
    generic = create_downstream_code_freeze_v1(
        workspace_root=tmp_path,
        manifest_path=manifest,
        purpose=subject.D1_DEVELOPMENT_FREEZE_PURPOSE_V0,
        include_trees=("src",),
        include_files=include_files,
        included_suffixes=(".py",),
        upstream_sha256=upstream_sha256,
    )

    loaded = subject.load_d1_historical_development_freeze_v0(
        manifest,
        workspace_root=tmp_path,
        expected_manifest_sha256=generic.manifest_sha256,
        input_authority=input_authority,
        preregistration_sha256=preregistration_sha256,
    )

    assert loaded.manifest_sha256 == generic.manifest_sha256
    assert loaded.frozen_file_count == len(payloads)


@pytest.mark.parametrize("frozen_hash", [_hash("different-preregistration"), None])
def test_freeze_rejects_mismatched_or_missing_frozen_prereg_file_hash(
    frozen_hash: str | None,
) -> None:
    input_hash = _hash("input-authority")
    prereg_hash = _hash("preregistration")
    authority = _downstream_freeze_authority(
        input_authority_sha256=input_hash,
        preregistration_sha256=prereg_hash,
        frozen_preregistration_sha256=frozen_hash,
    )

    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="policy"):
        subject._validate_freeze_authority(
            authority,
            input_authority_sha256=input_hash,
            preregistration_sha256=prereg_hash,
        )


def test_manifest_validator_enforces_exact_alias_and_interval_specific_range() -> None:
    binding = _authority_bindings()[0]
    manifest = DatasetManifest(
        schema_version=2,
        data_file="BTC__BTCUSDT__5m.csv.gz",
        sha256="a" * 64,
        market=Market.FUTURES.value,
        symbol="BTCUSDT",
        alias="BTC",
        interval="5m",
        request_start_time_ms=subject.D1_HISTORICAL_DATA_START_MS_V0,
        request_end_time_ms=subject.D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1,
        row_count=subject.D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0,
        first_open_time_ms=subject.D1_HISTORICAL_DATA_START_MS_V0,
        last_close_time_ms=subject.D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1,
        gap_count=0,
        missing_intervals=0,
    )

    subject._validate_kline_manifest_contract_v0(manifest, binding=binding)
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="range"):
        subject._validate_kline_manifest_contract_v0(
            replace(manifest, alias="BTCUSDT"),
            binding=binding,
        )

    hourly_binding = _authority_bindings()[1]
    hourly = replace(
        manifest,
        data_file="BTC__BTCUSDT__1h.csv.gz",
        interval="1h",
        request_start_time_ms=subject.D1_HISTORICAL_DATA_START_MS_V0 - _HOUR_MS,
        row_count=subject.D1_HISTORICAL_MAX_HOURLY_SOURCE_ROWS_V0,
        first_open_time_ms=subject.D1_HISTORICAL_DATA_START_MS_V0 - _HOUR_MS,
    )
    subject._validate_kline_manifest_contract_v0(hourly, binding=hourly_binding)
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="range"):
        subject._validate_kline_manifest_contract_v0(
            replace(hourly, row_count=subject.D1_HISTORICAL_MAX_HOURLY_SOURCE_ROWS_V0 + 1),
            binding=hourly_binding,
        )


@pytest.mark.parametrize(
    ("gap_count", "missing_intervals"),
    [(False, 0), (0, False)],
)
def test_manifest_zero_gap_fields_reject_boolean_aliases(
    gap_count: int,
    missing_intervals: int,
) -> None:
    binding = _authority_bindings()[0]
    manifest = DatasetManifest(
        schema_version=2,
        data_file="BTC__BTCUSDT__5m.csv.gz",
        sha256="a" * 64,
        market=Market.FUTURES.value,
        symbol="BTCUSDT",
        alias="BTC",
        interval="5m",
        request_start_time_ms=subject.D1_HISTORICAL_DATA_START_MS_V0,
        request_end_time_ms=subject.D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1,
        row_count=subject.D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0,
        first_open_time_ms=subject.D1_HISTORICAL_DATA_START_MS_V0,
        last_close_time_ms=subject.D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1,
        gap_count=0,
        missing_intervals=0,
    )

    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="range"):
        subject._validate_kline_manifest_contract_v0(
            replace(
                manifest,
                gap_count=gap_count,
                missing_intervals=missing_intervals,
            ),
            binding=binding,
        )


def test_every_kline_row_must_stay_inside_its_declared_request() -> None:
    assert subject._kline_row_is_within_declared_request_v0(
        open_time_ms=100,
        close_time_ms=199,
        request_start_time_ms=100,
        request_end_time_ms=199,
    )
    assert not subject._kline_row_is_within_declared_request_v0(
        open_time_ms=99,
        close_time_ms=199,
        request_start_time_ms=100,
        request_end_time_ms=199,
    )
    assert not subject._kline_row_is_within_declared_request_v0(
        open_time_ms=100,
        close_time_ms=200,
        request_start_time_ms=100,
        request_end_time_ms=199,
    )


def _write_gzip_payload(path: Path, payload: bytes) -> str:
    compressed = gzip.compress(payload, mtime=0)
    path.write_bytes(compressed)
    return hashlib.sha256(compressed).hexdigest()


def test_streaming_csv_enforces_exact_row_and_decompressed_byte_caps(
    tmp_path: Path,
) -> None:
    source = tmp_path / "bounded.csv.gz"
    payload = b"a,b\n1,2\n3,4\n"
    digest = _write_gzip_payload(source, payload)
    consumed: list[dict[str, str]] = []

    assert subject._stream_authenticated_gzip_csv_v0(
        path=source,
        expected_sha256=digest,
        expected_columns=("a", "b"),
        maximum_rows=2,
        maximum_decompressed_bytes=len(payload),
        label="bounded fixture",
        consume_row=lambda row, _line: consumed.append(row),
    ) == (digest, 2)
    assert consumed == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]

    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="row cap"):
        subject._stream_authenticated_gzip_csv_v0(
            path=source,
            expected_sha256=digest,
            expected_columns=("a", "b"),
            maximum_rows=1,
            maximum_decompressed_bytes=len(payload),
            label="bounded fixture",
            consume_row=lambda _row, _line: None,
        )
    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="decompressed-byte cap",
    ):
        subject._stream_authenticated_gzip_csv_v0(
            path=source,
            expected_sha256=digest,
            expected_columns=("a", "b"),
            maximum_rows=2,
            maximum_decompressed_bytes=len(payload) - 1,
            label="bounded fixture",
            consume_row=lambda _row, _line: None,
        )


@pytest.mark.parametrize("row", [b"1\n", b"1,2,3\n"])
def test_streaming_csv_rejects_missing_and_extra_cells(tmp_path: Path, row: bytes) -> None:
    source = tmp_path / "shape.csv.gz"
    payload = b"a,b\n" + row
    digest = _write_gzip_payload(source, payload)

    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="extra or missing"):
        subject._stream_authenticated_gzip_csv_v0(
            path=source,
            expected_sha256=digest,
            expected_columns=("a", "b"),
            maximum_rows=1,
            maximum_decompressed_bytes=len(payload),
            label="shape fixture",
            consume_row=lambda _row, _line: None,
        )


def test_streaming_csv_stops_at_byte_cap_before_allocating_a_giant_line(
    tmp_path: Path,
) -> None:
    source = tmp_path / "giant.csv.gz"
    payload = b"a,b\n" + b"x" * (subject._MAX_CSV_LINE_BYTES + 1) + b"\n"
    digest = _write_gzip_payload(source, payload)

    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="decompressed-byte cap",
    ):
        subject._stream_authenticated_gzip_csv_v0(
            path=source,
            expected_sha256=digest,
            expected_columns=("a", "b"),
            maximum_rows=1,
            maximum_decompressed_bytes=len(b"a,b\n") + 16,
            label="giant fixture",
            consume_row=lambda _row, _line: None,
        )


def test_manifest_reads_are_bounded_at_exact_cap(tmp_path: Path) -> None:
    source = tmp_path / "authority.json"
    source.write_bytes(b"1234")

    assert (
        subject._read_exact_regular_file(
            source,
            "authority fixture",
            maximum_bytes=4,
        )
        == b"1234"
    )
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="byte cap"):
        subject._read_exact_regular_file(
            source,
            "authority fixture",
            maximum_bytes=3,
        )


def test_manifest_reader_loops_over_short_reads_and_rejects_early_eof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "short-read.json"
    source.write_bytes(b"1234")
    opened = source.stat()

    class ShortReader:
        def __init__(self, *, early_eof: bool) -> None:
            self._chunks = iter((b"1", b"2", b"" if early_eof else b"3", b"4", b""))

        def read(self, _size: int = -1) -> bytes:
            return next(self._chunks, b"")

        def close(self) -> None:
            return None

    monkeypatch.setattr(subject, "_verify_open_file_stability_v0", lambda **_kwargs: None)
    monkeypatch.setattr(
        subject,
        "_open_stable_regular_binary_v0",
        lambda *_args, **_kwargs: (ShortReader(early_eof=False), opened),
    )
    assert (
        subject._read_exact_regular_file(
            source,
            "short-read fixture",
            maximum_bytes=4,
        )
        == b"1234"
    )

    monkeypatch.setattr(
        subject,
        "_open_stable_regular_binary_v0",
        lambda *_args, **_kwargs: (ShortReader(early_eof=True), opened),
    )
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="opened size"):
        subject._read_exact_regular_file(
            source,
            "early-eof fixture",
            maximum_bytes=4,
        )


def test_manifest_reader_rejects_path_swap_or_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "authority.json"
    replacement = tmp_path / "replacement.json"
    source.write_bytes(b"1234")
    replacement.write_bytes(b"5678")
    real_verify = subject._verify_open_file_stability_v0

    def swapping_verify(**kwargs) -> None:
        try:
            os.replace(replacement, source)
        except PermissionError:
            pytest.skip("host cannot replace a pathname while its descriptor is open")
        real_verify(**kwargs)

    monkeypatch.setattr(subject, "_verify_open_file_stability_v0", swapping_verify)
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="changed"):
        subject._read_exact_regular_file(
            source,
            "swapped authority fixture",
            maximum_bytes=4,
        )


def test_gzip_hash_and_parse_use_one_descriptor_and_fail_closed_on_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "same-fd.csv.gz"
    payload = b"a,b\n1,2\n"
    digest = _write_gzip_payload(source, payload)
    real_open = os.open
    target_open_count = 0

    def counted_open(path, flags, *args):
        nonlocal target_open_count
        if Path(path) == source:
            target_open_count += 1
        return real_open(path, flags, *args)

    monkeypatch.setattr(subject.os, "open", counted_open)
    subject._stream_authenticated_gzip_csv_v0(
        path=source,
        expected_sha256=digest,
        expected_columns=("a", "b"),
        maximum_rows=1,
        maximum_decompressed_bytes=len(payload),
        label="same descriptor fixture",
        consume_row=lambda _row, _line: None,
    )
    assert target_open_count == 1

    target_open_count = 0

    def mutating_open(path, flags, *args):
        nonlocal target_open_count
        descriptor = real_open(path, flags, *args)
        if Path(path) == source:
            target_open_count += 1
            source.write_bytes(gzip.compress(b"a,b\n100,200\n", mtime=0))
        return descriptor

    monkeypatch.setattr(subject.os, "open", mutating_open)
    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match=r"identity changed|identity or size changed|hash differs",
    ):
        subject._stream_authenticated_gzip_csv_v0(
            path=source,
            expected_sha256=digest,
            expected_columns=("a", "b"),
            maximum_rows=1,
            maximum_decompressed_bytes=100,
            label="same descriptor fixture",
            consume_row=lambda _row, _line: None,
        )
    assert target_open_count == 1


def _funding_grid_points() -> tuple:
    step = subject._STANDARD_FUNDING_INTERVAL_MS
    first = ((subject.D1_HISTORICAL_DEVELOPMENT_START_MS_V0 + step - 1) // step) * step
    last = ((subject.D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1) // step) * step
    return tuple(
        build_d1_historical_funding_point_v0(
            funding_time_ms=value,
            rate=Decimal("0.0001"),
            mark_price=Decimal("100"),
        )
        for value in range(first, last + 1, step)
    )


@pytest.mark.parametrize("removed", [0, 1, -1])
def test_standard_8h_funding_coverage_rejects_edge_and_interior_gaps(removed: int) -> None:
    complete = _funding_grid_points()
    assert subject._has_exact_standard_8h_development_funding_coverage_v0(complete)

    missing = complete[:removed] + complete[removed + 1 :] if removed >= 0 else complete[:-1]
    assert not subject._has_exact_standard_8h_development_funding_coverage_v0(missing)


def test_standard_8h_funding_coverage_rejects_adjusted_non_grid_event() -> None:
    complete = list(_funding_grid_points())
    middle = len(complete) // 2
    original = complete[middle]
    complete[middle] = build_d1_historical_funding_point_v0(
        funding_time_ms=original.funding_time_ms + _HOUR_MS,
        rate=original.rate,
        mark_price=original.mark_price,
    )

    assert not subject._has_exact_standard_8h_development_funding_coverage_v0(tuple(complete))


def test_summary_reports_failed_symbol_coverage_even_with_zero_episodes() -> None:
    available = (
        subject.D1HistoricalFundingCoverageStatusV0.EXACT_STANDARD_8H_DEVELOPMENT_COVERAGE.value
    )
    statuses = tuple(
        (
            symbol,
            (
                subject.D1HistoricalFundingCoverageStatusV0.FUNDING_COVERAGE_UNAVAILABLE.value
                if symbol == "BTCUSDT"
                else available
            ),
        )
        for symbol in subject.D1_HISTORICAL_UNIVERSE_V0
    )
    summary = subject._summarize_development_v0(
        episodes=(),
        censors=(),
        counters=subject._RunCountersV0(),
        funding_coverage_status_by_symbol=statuses,
    )

    assert summary.episode_count == 0
    assert summary.funding_inconclusive_counts[-1][1] == 0
    assert summary.funding_coverage_status_by_symbol[0] == (
        "BTCUSDT",
        subject.D1HistoricalFundingCoverageStatusV0.FUNDING_COVERAGE_UNAVAILABLE.value,
    )
    assert b"FUNDING_COVERAGE_UNAVAILABLE" in (subject.canonical_d1_historical_summary_v0(summary))


def test_every_hour_close_is_crosschecked_against_twelfth_five_minute_close() -> None:
    five = tuple(
        _candle_from_bar(
            "BTCUSDT",
            _sealed_bar(
                open_ms=_T + index * _FIVE_MINUTE_MS,
                close=Decimal("100") + Decimal(index),
                high=Decimal("100") + Decimal(index),
                low=Decimal("99"),
            ),
        )
        for index in range(12)
    )
    hourly = (_hourly_candle(open_ms=_T, close=five[-1].close),)

    subject._validate_hourly_close_rows_v0(
        symbol="BTCUSDT",
        five_minute=five,
        hourly=hourly,
    )
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="does not equal"):
        subject._validate_hourly_close_rows_v0(
            symbol="BTCUSDT",
            five_minute=five,
            hourly=(_hourly_candle(open_ms=_T, close=five[-1].close + Decimal(1)),),
        )


def test_episode_and_summary_canonical_hashes_detect_post_factory_tampering() -> None:
    episode = _episode(0, primary=Decimal("0.001"), stress=Decimal("0.0008"))
    summary = _summary((episode,))

    assert subject.canonical_d1_historical_episode_v0(episode).endswith(b"\n")
    assert subject.canonical_d1_historical_summary_v0(summary).endswith(b"\n")
    object.__setattr__(episode, "symbol", "ETHUSDT")
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="hash"):
        subject.canonical_d1_historical_episode_v0(episode)


def test_result_canonicalization_independently_detects_censor_tampering() -> None:
    result, _authority, _freeze = _result_bundle(
        (_episode(0, primary=Decimal("0.001"), stress=Decimal("0.0008")),)
    )
    censor = subject.D1HistoricalCensorV0(
        symbol="BTCUSDT",
        signal_event_id=_hash("censored-signal"),
        signal_bar_open_ms=_T,
        stage=subject.D1HistoricalCensorStageV0.EXIT_REFERENCE,
        reason="RIGHT_EDGE_EXIT_REFERENCE_UNAVAILABLE",
        _factory_token=subject._CENSOR_FACTORY_TOKEN,
    )
    assert subject.canonical_d1_historical_censor_v0(censor).endswith(b"\n")
    object.__setattr__(result, "censors", (censor,))
    object.__setattr__(censor, "reason", "TAMPERED_CENSOR_REASON")

    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="censor hash"):
        subject.canonical_d1_historical_development_result_v0(result)


def test_all_historical_claim_and_order_flags_remain_false() -> None:
    episode = _episode(0, primary=Decimal("0.001"), stress=Decimal("0.0008"))
    summary = _summary((episode,))

    assert episode.status == subject.D1_HISTORICAL_RESULT_STATUS_V0
    assert not episode.historical_bbo_available
    assert not episode.paper_fill_claim
    assert not episode.execution_conclusive
    assert not episode.probability_claim
    assert not episode.efficacy_claim
    assert not episode.promoting
    assert not episode.prospective
    assert not episode.production_order_placement
    assert not summary.probability_claim
    assert not summary.efficacy_claim
    assert not summary.promoting


def test_result_index_and_atomic_artifacts_are_byte_deterministic(tmp_path) -> None:
    bundle = _result_bundle((_episode(0, primary=Decimal("0.001"), stress=Decimal("0.0008")),))
    result, authority, freeze = bundle

    first = subject.write_d1_historical_development_artifacts_v0(
        result=result,
        input_authority=authority,
        code_freeze=freeze,
        output_dir=tmp_path / "first",
    )
    second = subject.write_d1_historical_development_artifacts_v0(
        result=result,
        input_authority=authority,
        code_freeze=freeze,
        output_dir=tmp_path / "second",
    )

    assert first.manifest_sha256 == second.manifest_sha256
    assert first.output_file_sha256 == second.output_file_sha256
    assert subject.canonical_d1_historical_development_result_v0(result).endswith(b"\n")
    first_files = {value.name: value.read_bytes() for value in first.output_dir.iterdir()}
    second_files = {value.name: value.read_bytes() for value in second.output_dir.iterdir()}
    assert first_files == second_files
    assert set(first_files) == {
        "censors.jsonl",
        "code-freeze-receipt.jsonl",
        "episodes.jsonl",
        "input-authority.jsonl",
        "manifest.jsonl",
        "report.md",
        "result-index.jsonl",
        "summary.jsonl",
    }
    assert b"INCONCLUSIVE_NO_HISTORICAL_BBO" in first_files["report.md"]
    manifest = json.loads(first_files["manifest.jsonl"])
    assert manifest["durability_contract"] == (
        subject.d1_historical_artifact_durability_contract_v0()
    )


def test_artifact_writer_rejects_overwrite_and_cap_without_partial_publish(
    tmp_path: Path,
) -> None:
    result, authority, freeze = _result_bundle(
        (_episode(0, primary=Decimal("0.001"), stress=Decimal("0.0008")),)
    )
    existing = tmp_path / "existing"
    existing.mkdir()
    capped = tmp_path / "capped"

    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="fresh absent"):
        subject.write_d1_historical_development_artifacts_v0(
            result=result,
            input_authority=authority,
            code_freeze=freeze,
            output_dir=existing,
        )
    with pytest.raises(subject.D1HistoricalDevelopmentContractErrorV0, match="byte cap"):
        subject.write_d1_historical_development_artifacts_v0(
            result=result,
            input_authority=authority,
            code_freeze=freeze,
            output_dir=capped,
            maximum_total_bytes=100,
        )

    assert not capped.exists()
    assert not tuple(tmp_path.glob(".capped.tmp-*"))


def test_artifact_writer_flushes_staging_and_parent_directory_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, authority, freeze = _result_bundle(
        (_episode(0, primary=Decimal("0.001"), stress=Decimal("0.0008")),)
    )
    observed: list[Path] = []
    monkeypatch.setattr(
        subject,
        "_fsync_directory_if_supported_v0",
        lambda path: observed.append(path),
    )

    subject.write_d1_historical_development_artifacts_v0(
        result=result,
        input_authority=authority,
        code_freeze=freeze,
        output_dir=tmp_path / "durable",
    )

    assert len(observed) == 3
    assert observed[0].name.startswith(".durable.tmp-")
    assert observed[1] == tmp_path / "durable"
    assert observed[2] == tmp_path


def test_artifact_durability_contract_is_exact_and_never_disclaims_windows_flush() -> None:
    assert subject.D1_HISTORICAL_WINDOWS_ARTIFACT_DURABILITY_CONTRACT_V0 == (
        "WINDOWS_LOCAL_FIXED_NTFS_FILE_STAGING_OUTPUT_AND_PARENT_DIRECTORY_FLUSH_"
        "ATOMIC_RENAME_NOREPLACE_V0"
    )
    assert "WITHOUT_DIRECTORY_FLUSH" not in (
        subject.D1_HISTORICAL_WINDOWS_ARTIFACT_DURABILITY_CONTRACT_V0
    )
    expected = (
        subject.D1_HISTORICAL_WINDOWS_ARTIFACT_DURABILITY_CONTRACT_V0
        if os.name == "nt"
        else subject.D1_HISTORICAL_POSIX_ARTIFACT_DURABILITY_CONTRACT_V0
    )
    assert subject.d1_historical_artifact_durability_contract_v0() == expected


def test_artifact_volume_qualification_explicitly_rejects_refs_identity_truncation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def set_volume_path(_path: object, buffer: object, _length: object) -> int:
        cast(_UnicodeBuffer, buffer).value = "Z:\\"
        return 1

    def set_refs_filesystem(*arguments: object) -> int:
        cast(_UnicodeBuffer, arguments[6]).value = "ReFS"
        return 1

    apis = {
        "GetVolumePathNameW": _FakeWin32Function(set_volume_path),
        "GetDriveTypeW": _FakeWin32Function(3),
        "GetVolumeInformationW": _FakeWin32Function(set_refs_filesystem),
    }
    monkeypatch.setattr(subject, "_windows_api_v0", lambda name: apis[name])

    with pytest.raises(
        subject.D1HistoricalArtifactDurabilityErrorV0,
        match=r"local fixed NTFS; ReFS.*64-bit directory identity",
    ):
        subject._windows_local_volume_identity_v0(tmp_path)

    assert len(apis["GetVolumeInformationW"].calls) == 1


@pytest.mark.skipif(os.name != "nt", reason="requires the local Win32 NTFS contract")
def test_artifact_volume_qualification_accepts_real_host_ntfs(tmp_path: Path) -> None:
    volume_identity, serial = subject._windows_local_volume_identity_v0(tmp_path)

    assert serial >= 0
    assert "|NTFS|" in volume_identity


def test_win32_directory_open_uses_exact_durable_no_reparse_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_file = _FakeWin32Function(41)

    class FakeKernel32:
        CreateFileW = create_file

    monkeypatch.setattr(subject, "_windows_kernel32_v0", lambda: FakeKernel32())

    assert subject._windows_open_directory_handle_v0(tmp_path) == 41
    assert len(create_file.calls) == 1
    arguments = create_file.calls[0]
    assert arguments[0] == os.fspath(tmp_path)
    assert arguments[1] == 0x40000000
    assert arguments[2] == 0x00000007
    assert arguments[4] == 3
    assert arguments[5] == 0x82200000


def test_win32_close_handle_failure_is_a_durability_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_handle = _FakeWin32Function(0)

    class FakeKernel32:
        CloseHandle = close_handle

    monkeypatch.setattr(subject, "_windows_kernel32_v0", lambda: FakeKernel32())
    monkeypatch.setattr(subject, "_windows_last_error_v0", lambda: 995)

    with pytest.raises(
        subject.D1HistoricalArtifactDurabilityErrorV0,
        match=r"CloseHandle.*995",
    ):
        subject._windows_close_handle_v0(41)

    assert len(close_handle.calls) == 1


def test_staging_directory_flush_failure_leaves_no_public_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, authority, freeze = _result_bundle(
        (_episode(0, primary=Decimal("0.001"), stress=Decimal("0.0008")),)
    )
    target = tmp_path / "staging-flush-failure"

    def fail_staging_flush(path: Path) -> None:
        if path.name.startswith(".staging-flush-failure.tmp-"):
            raise subject.D1HistoricalArtifactDurabilityErrorV0(
                "injected staging directory flush failure"
            )

    monkeypatch.setattr(
        subject,
        "_fsync_directory_if_supported_v0",
        fail_staging_flush,
    )

    with pytest.raises(
        subject.D1HistoricalArtifactDurabilityErrorV0,
        match="injected staging directory flush failure",
    ):
        subject.write_d1_historical_development_artifacts_v0(
            result=result,
            input_authority=authority,
            code_freeze=freeze,
            output_dir=target,
        )

    assert not target.exists()
    assert not tuple(tmp_path.glob(".staging-flush-failure.tmp-*"))


@pytest.mark.parametrize("failure_point", ["output", "parent"])
def test_post_commit_directory_flush_failure_is_ambiguous_and_preserves_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
) -> None:
    result, authority, freeze = _result_bundle(
        (_episode(0, primary=Decimal("0.001"), stress=Decimal("0.0008")),)
    )
    target = tmp_path / f"{failure_point}-flush-failure"
    real_sync = subject._fsync_directory_if_supported_v0

    def fail_selected_flush(path: Path) -> None:
        if (failure_point == "output" and path == target) or (
            failure_point == "parent" and path == tmp_path
        ):
            raise subject.D1HistoricalArtifactDurabilityErrorV0(
                f"injected {failure_point} directory flush failure"
            )
        real_sync(path)

    monkeypatch.setattr(
        subject,
        "_fsync_directory_if_supported_v0",
        fail_selected_flush,
    )

    with pytest.raises(
        subject.D1HistoricalArtifactDurabilityErrorV0,
        match=r"durability-ambiguous.*do not retry, delete, or replace",
    ):
        subject.write_d1_historical_development_artifacts_v0(
            result=result,
            input_authority=authority,
            code_freeze=freeze,
            output_dir=target,
        )

    assert target.is_dir()
    assert (target / "manifest.jsonl").is_file()
    assert not tuple(tmp_path.glob(f".{target.name}.tmp-*"))


def test_final_same_descriptor_revalidation_failure_is_ambiguous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, authority, freeze = _result_bundle(
        (_episode(0, primary=Decimal("0.001"), stress=Decimal("0.0008")),)
    )
    target = tmp_path / "final-revalidation-failure"
    real_revalidate = subject._revalidate_published_artifacts_v0

    def tamper_before_revalidation(
        *,
        target: Path,
        output_metadata: dict[str, tuple[str, int]],
    ) -> None:
        (target / "report.md").write_bytes(b"tampered after directory flush\n")
        real_revalidate(target=target, output_metadata=output_metadata)

    monkeypatch.setattr(
        subject,
        "_revalidate_published_artifacts_v0",
        tamper_before_revalidation,
    )

    with pytest.raises(
        subject.D1HistoricalArtifactDurabilityErrorV0,
        match=r"durability-ambiguous.*inspect it read-only",
    ):
        subject.write_d1_historical_development_artifacts_v0(
            result=result,
            input_authority=authority,
            code_freeze=freeze,
            output_dir=target,
        )

    assert (target / "report.md").read_bytes() == b"tampered after directory flush\n"
    assert not tuple(tmp_path.glob(".final-revalidation-failure.tmp-*"))


def test_artifact_directory_commit_rejects_concurrent_empty_target_without_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / "staging"
    target = tmp_path / "target"
    staging.mkdir()
    (staging / "payload").write_bytes(b"new")
    real_rename = subject._rename_directory_no_replace_v0

    def racing_rename(*, staging: Path, target: Path) -> None:
        target.mkdir()
        (target / "sentinel").write_bytes(b"original")
        real_rename(staging=staging, target=target)

    monkeypatch.setattr(subject, "_rename_directory_no_replace_v0", racing_rename)
    with pytest.raises(
        subject.D1HistoricalDevelopmentContractErrorV0,
        match="target appeared",
    ):
        subject._publish_staging_no_replace(staging=staging, target=target)

    assert (target / "sentinel").read_bytes() == b"original"
    assert (staging / "payload").read_bytes() == b"new"


def test_artifact_directory_commit_reports_close_failure_after_rename_without_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / "staging"
    target = tmp_path / "target"
    staging.mkdir()
    (staging / "payload").write_bytes(b"committed")
    real_open = subject.os.open
    real_close = subject.os.close
    lock_descriptor: int | None = None

    def tracking_open(path, flags, mode=0o777):
        nonlocal lock_descriptor
        descriptor = real_open(path, flags, mode)
        if str(path).endswith(".target.publish.lock"):
            lock_descriptor = descriptor
        return descriptor

    def failing_close(descriptor: int) -> None:
        if descriptor == lock_descriptor:
            real_close(descriptor)
            raise OSError("injected post-rename close failure")
        real_close(descriptor)

    monkeypatch.setattr(subject.os, "open", tracking_open)
    monkeypatch.setattr(subject.os, "close", failing_close)

    with pytest.raises(
        subject.D1HistoricalArtifactDurabilityErrorV0,
        match=r"durability-ambiguous.*do not retry, delete, or replace",
    ):
        subject._publish_staging_no_replace(staging=staging, target=target)

    assert (target / "payload").read_bytes() == b"committed"
    assert not staging.exists()


def test_artifact_directory_commit_reports_lock_unlink_failure_after_rename_without_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging = tmp_path / "staging"
    target = tmp_path / "target"
    staging.mkdir()
    (staging / "payload").write_bytes(b"committed")
    real_unlink = Path.unlink

    def failing_unlink(path: Path, *args, **kwargs) -> None:
        if path.name == ".target.publish.lock":
            raise OSError("injected post-rename unlink failure")
        real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", failing_unlink)

    with pytest.raises(
        subject.D1HistoricalArtifactDurabilityErrorV0,
        match=r"durability-ambiguous.*do not retry, delete, or replace",
    ):
        subject._publish_staging_no_replace(staging=staging, target=target)

    assert (target / "payload").read_bytes() == b"committed"
    assert not staging.exists()
    assert (tmp_path / ".target.publish.lock").is_file()


def test_fee_cell_constants_remain_exact() -> None:
    assert D1HistoricalFeeCellV0.PRIMARY_1_0.rate_per_side == Decimal("0.0005")
    assert D1HistoricalFeeCellV0.STRESS_1_5.rate_per_side == Decimal("0.00075")
    assert subject.D1_HISTORICAL_MAX_FIVE_MINUTE_ROWS_V0 == 245_376
    assert subject.D1_HISTORICAL_MAX_FIVE_MINUTE_DECOMPRESSED_BYTES_V0 == 256 * 1024 * 1024
    assert subject.D1_HISTORICAL_MAX_HOURLY_SOURCE_ROWS_V0 == 30_000
    assert subject.D1_HISTORICAL_MAX_HOURLY_DECOMPRESSED_BYTES_V0 == 64 * 1024 * 1024
    assert subject.D1_HISTORICAL_MAX_FUNDING_ROWS_V0 == 10_000
    assert subject.D1_HISTORICAL_MAX_FUNDING_DECOMPRESSED_BYTES_V0 == 16 * 1024 * 1024
