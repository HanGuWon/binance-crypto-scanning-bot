"""Causal, budget, storage and endpoint regression tests: no external I/O."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import zipfile
from collections.abc import Coroutine
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from signalbot.backtest.dataset import (
    KlineDataset,
    KlineDatasetRequest,
    build_dataset_manifest,
    write_dataset_manifest,
    write_kline_csv,
)
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    RestEnvelopeV2,
    RestErrorCategory,
    record_to_json_line,
)
from signalbot.capture.storage import SegmentedCaptureWriter
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.pump_fade_v2.execution_replay import (
    AddOpportunity,
    ExposureClass,
    FundingCoverage,
    FundingSettlement,
    ParentPath,
    PathBar,
    compare_parent_paths,
    summarize_arm_results,
)
from signalbot.pump_fade_v2.execution_replay import (
    ExecutableQuote as ReplayQuote,
)
from signalbot.pump_fade_v2.forward_gate import evaluate_forward_gate
from signalbot.pump_fade_v2.labels import (
    AppendOnlyOutcomeStore,
    ExecutableQuote,
    LabelStatus,
    MarkBar,
    label_outcome,
    load_primary_squeeze_threshold,
)
from signalbot.pump_fade_v2.ladder import (
    Fill,
    FrozenLadder,
    Side,
    assess_ladder,
    supplied_liquidation_advisory,
)
from signalbot.pump_fade_v2.materializer import (
    PumpInputMaterializer,
)
from signalbot.pump_fade_v2.offline_lifecycle import OfflinePumpLifecycle
from signalbot.pump_fade_v2.private_replay import aggregate_private_additions
from signalbot.pump_fade_v2.public_capture import (
    evidence_to_predicted_funding,
    normalize_closed_kline_frame,
    normalize_funding_metadata,
    normalize_liquidation,
    normalize_oi_history,
    normalize_premium_index,
    normalize_public_frame,
    pump_public_plans,
    pump_rest_poll_plan,
)
from signalbot.pump_fade_v2.release import (
    build_release_evidence,
    source_closure_manifest,
)
from signalbot.pump_fade_v2.replay import (
    day_block_interval,
    find_proxy_events,
    holm_adjust,
    parent_release_accounting,
    run_public_kline_feasibility,
)
from signalbot.pump_fade_v2.runtime import (
    PumpCapturePlanLedger,
    PumpCaptureRuntime,
    PumpCaptureSettings,
    PumpRestScheduler,
    next_utc_5m_poll_ms,
    pump_capture_plan_sha256,
)
from signalbot.pump_fade_v2.shadow import (
    ShadowEpisodeEngine,
    ShadowPreviewStore,
    preview_payload,
)
from signalbot.pump_fade_v2.state import (
    MINUTE_MS,
    BookQuality,
    ClosedBar,
    Event,
    FundingCap,
    InputPanel,
    Observation,
    State,
    admit_pump,
    derive_fifteen_minute,
    start_episode,
    step_episode,
)

BASE = 1_800_000_000_000
STEP = 300_000
POLICY_PATH = Path(__file__).resolve().parents[2] / "config/pump_fade_v2/policy_v2d.json"
PRIMARY_SQUEEZE_PCT = load_primary_squeeze_threshold(POLICY_PATH)
def _run_inline[T](coroutine: Coroutine[Any, Any, T]) -> T:
    """Drive no-I/O test coroutines without creating a sandbox-blocked socket loop."""

    iterator = coroutine.__await__()
    while True:
        try:
            yielded = iterator.send(None)
        except StopIteration as completed:
            return completed.value
        if yielded is not None:
            raise RuntimeError("inline coroutine attempted event-loop I/O")


def make_bar(index: int, *, price: float = 100, high: float | None = None,
             qv: float = 10_000) -> ClosedBar:
    ts = BASE + (index + 1) * STEP - 1
    return ClosedBar(ts, ts + 1, price, high if high is not None else price,
                     price, price, 10, qv)


def make_event_panel(*, price: float = 130) -> tuple[Event, InputPanel, int]:
    historical = tuple(make_bar(i) for i in range(288))
    event_bar = make_bar(288, price=price, high=price + 1)
    future = tuple(make_bar(i, price=price, high=price + 0.5)
                   for i in range(289, 341))
    all_bars = (*historical, event_bar, *future)
    event_ms = event_bar.close_ms
    start = BASE - 20 * 24 * 60 * MINUTE_MS
    event = admit_pump("ABCUSDT", all_bars[:289], now_ms=event_ms + 1,
                       latest_admitted_event_ms=None, listing_open_ms=start,
                       listing_observed_ms=event_ms - 100_000)
    assert event is not None
    now_ms = all_bars[-1].received_ms
    panel = InputPanel(
        five_minute=all_bars, fifteen_minute=(),
        oi=(Observation(event_ms, event_ms + 1, 100),
            Observation(now_ms - 600_001, now_ms - 600_000, 100),
            Observation(now_ms - 10_001, now_ms - 10_000, 100)),
        predicted_funding=(Observation(now_ms - 101, now_ms - 100, 0.0001),),
        caps=(FundingCap(now_ms - 100, 0.025, 8),),
        book=BookQuality(now_ms - 100, price - 0.1, price + 0.1,
                         150_000, 150_000, now_ms - 100),
    )
    return event, panel, now_ms


def test_event_admission_threshold_quote_volume_cooldown_and_future_receipt() -> None:
    event, panel, _ = make_event_panel()
    assert event.event_id and event.policy_version
    now = panel.five_minute[288].received_ms
    assert admit_pump("ABCUSDT", panel.five_minute[:289], now_ms=now,
                      latest_admitted_event_ms=event.event_ms,
                      listing_open_ms=event.listing_open_ms,
                      listing_observed_ms=event.listing_observed_ms) is None
    assert admit_pump("ABCUSDT", (*panel.five_minute[:288],
                                  replace(panel.five_minute[288], received_ms=now + 10_000)),
        now_ms=now, latest_admitted_event_ms=None,
        listing_open_ms=event.listing_open_ms,
        listing_observed_ms=event.listing_observed_ms) is None
    below = replace(panel.five_minute[288], close=129.99, low=129.99, high=130)
    assert admit_pump("ABCUSDT", (*panel.five_minute[:288], below),
                      now_ms=now, latest_admitted_event_ms=None,
                      listing_open_ms=event.listing_open_ms,
                      listing_observed_ms=event.listing_observed_ms) is None
    low_volume = tuple(replace(b, quote_volume=1) for b in panel.five_minute[:289])
    assert admit_pump("ABCUSDT", low_volume, now_ms=now,
                      latest_admitted_event_ms=None,
                      listing_open_ms=event.listing_open_ms,
                      listing_observed_ms=event.listing_observed_ms) is None


def test_causal_wait_missing_inputs_release_and_terminal_skip() -> None:
    event, panel, now = make_event_panel()
    initial = start_episode(event)
    _, missing = step_episode(initial, replace(panel, book=None), now_ms=now)
    assert missing.state is State.WAIT_UNAVAILABLE
    _, ready = step_episode(initial, panel, now_ms=now)
    assert ready.state is State.FADE_CANDIDATE
    assert "R1_NO_HIGH_60M" in ready.releases
    new = replace(event, listing_open_ms=event.event_ms - 2 * 24 * 3600_000)
    skipped, skipped_decision = step_episode(start_episode(new), panel, now_ms=now)
    assert skipped_decision.state is State.SKIP
    _, terminal = step_episode(skipped, panel, now_ms=now + 10_000)
    assert terminal.state is State.SKIP
    missing_event_anchor = replace(panel,
        five_minute=tuple(x for x in panel.five_minute
                          if x.close_ms != event.event_ms))
    _, cannot_claim = step_episode(initial, missing_event_anchor, now_ms=now)
    assert cannot_claim.state is State.WAIT_UNAVAILABLE


def test_policy_return_and_volume_boundaries_are_strict_and_inclusive() -> None:
    event, panel, now = make_event_panel()
    bars = list(panel.five_minute)
    baseline_price = bars[-49].close
    exact = baseline_price * 1.15
    for index in range(len(bars) - 48, len(bars)):
        bars[index] = replace(bars[index], open=exact, high=exact, low=exact, close=exact)
    exact_panel = replace(panel, five_minute=tuple(bars))
    _, exact_decision = step_episode(start_episode(event), exact_panel, now_ms=now)
    assert "4h_return_gt_15pct" not in exact_decision.reasons
    above = list(bars)
    for index in range(len(above) - 12, len(above)):
        above[index] = replace(above[index], open=exact + 0.01, high=exact + 0.01,
                               low=exact + 0.01, close=exact + 0.01)
    _, above_decision = step_episode(start_episode(event),
        replace(panel, five_minute=tuple(above)), now_ms=now)
    assert "4h_return_gt_15pct" in above_decision.reasons
    baseline = tuple(replace(bar, quote_volume=10_000) for bar in panel.five_minute)
    doubled = list(baseline)
    for index in range(len(doubled) - 12, len(doubled)):
        doubled[index] = replace(doubled[index], quote_volume=20_000)
    _, volume_decision = step_episode(start_episode(event),
        replace(panel, five_minute=tuple(doubled)), now_ms=now)
    assert "1h_volume_at_least_2x_prior12h_median" in volume_decision.reasons


def test_closed_fifteen_aggregation_and_stale_book_does_not_skip() -> None:
    event, panel, now = make_event_panel()
    fifteen = derive_fifteen_minute(panel.five_minute, now)
    assert fifteen
    assert all((bar.close_ms + 1) % 900_000 == 0 for bar in fifteen)
    assert all(bar.received_ms > bar.close_ms for bar in fifteen)
    future_data = replace(panel.five_minute[-1], received_ms=now + 100_000)
    assert all(bar.received_ms <= now for bar in derive_fifteen_minute(
        (*panel.five_minute[:-1], future_data), now))
    assert panel.book is not None
    stale_wide = replace(panel.book, bid=100, ask=130,
                         received_ms=now - 5_001, depth_received_ms=now - 5_001)
    _, decision = step_episode(start_episode(event), replace(panel, book=stale_wide),
                               now_ms=now)
    assert decision.state is State.WAIT_UNAVAILABLE


def test_fully_closed_15m_failed_retest_and_post_event_anchor() -> None:
    event, panel, now = make_event_panel()
    aligned_last = ((now + 1) // 900_000) * 900_000 - 1
    prices = ((129.0, 129.4, 129.2, 129.2),
              (128.5, 129.0, 128.8, 128.8),
              (126.8, 128.0, 127.0, 127.0),
              (126.8, 128.4, 127.0, 127.0))
    fifteen = tuple(ClosedBar(
        close_ms=aligned_last - (3 - i) * 900_000,
        received_ms=aligned_last - (3 - i) * 900_000 + 1,
        open=opened, high=hi, low=low, close=closed,
        base_volume=10, quote_volume=1_000)
        for i, (low, hi, opened, closed) in enumerate(prices))
    _, decision = step_episode(start_episode(event), replace(panel, fifteen_minute=fifteen),
                               now_ms=now)
    assert "R2_15M_BREAK_FAILED_RETEST" in decision.releases
    stale = tuple(replace(bar, close_ms=bar.close_ms - 2 * 900_000,
                          received_ms=bar.received_ms - 2 * 900_000) for bar in fifteen)
    _, old = step_episode(start_episode(event), replace(panel, fifteen_minute=stale),
                          now_ms=now)
    assert "R2_15M_BREAK_FAILED_RETEST" not in old.releases


def test_funding_normalization_clears_hit_but_interval_change_waits() -> None:
    event, panel, now = make_event_panel()
    caps = (FundingCap(event.event_ms - 100, 0.025, 8),
            FundingCap(now - 100, 0.025, 8))
    rates = (Observation(event.event_ms, event.event_ms + 1, 0.025),
             Observation(now - 180_001, now - 180_000, 0.023),
             Observation(now - 120_001, now - 120_000, 0.023),
             Observation(now - 60_001, now - 60_000, 0.023))
    _, normalized = step_episode(start_episode(event), replace(
        panel, caps=caps, predicted_funding=rates), now_ms=now)
    assert normalized.state is State.FADE_CANDIDATE
    assert "R5_PREDICTED_FUNDING_NORMALIZED" in normalized.releases
    shortened = (caps[0], FundingCap(now - 600_000, 0.025, 4),
                 FundingCap(now - 100, 0.025, 4))
    _, still_risk = step_episode(start_episode(event), replace(
        panel, caps=shortened, predicted_funding=rates), now_ms=now)
    assert still_risk.state is State.WAIT
    restored = (caps[0], FundingCap(now - 600_000, 0.025, 4),
                FundingCap(now - 100, 0.025, 8))
    _, recovery = step_episode(start_episode(event), replace(
        panel, caps=restored, predicted_funding=rates), now_ms=now)
    assert recovery.state is State.FADE_CANDIDATE
    stepped_shortening = (
        caps[0], FundingCap(now - 900_000, 0.025, 4),
        FundingCap(now - 600_000, 0.025, 2),
        FundingCap(now - 100, 0.025, 4),
    )
    _, partial_restore = step_episode(start_episode(event), replace(
        panel, caps=stepped_shortening, predicted_funding=rates), now_ms=now)
    assert partial_restore.state is State.WAIT
    pre_event_hit = (Observation(event.event_ms - 600_000, event.event_ms - 599_999,
                                 0.025), *rates[1:])
    observed_before_hit = (FundingCap(event.event_ms - 700_000, 0.025, 8), caps[1])
    _, pre_event_cleared = step_episode(start_episode(event), replace(
        panel, caps=observed_before_hit, predicted_funding=pre_event_hit), now_ms=now)
    assert pre_event_cleared.state is State.FADE_CANDIDATE
    _, insufficient_streak = step_episode(start_episode(event), replace(
        panel, caps=observed_before_hit, predicted_funding=pre_event_hit[:2]), now_ms=now)
    assert insufficient_streak.state is State.WAIT
    rapid = (
        pre_event_hit[0],
        Observation(now - 180_000, now - 179_999, 0.023),
        Observation(now - 150_000, now - 149_999, 0.023),
        Observation(now - 120_000, now - 119_999, 0.023),
    )
    _, underspaced = step_episode(start_episode(event), replace(
        panel, caps=observed_before_hit, predicted_funding=rapid), now_ms=now)
    assert underspaced.state is State.WAIT
    repeated_hit = (*rates, Observation(now - 30_000, now - 29_999, 0.025))
    repeat_caps = (caps[0], FundingCap(now - 40_000, 0.025, 8))
    _, reset_streak = step_episode(start_episode(event), replace(
        panel, caps=repeat_caps, predicted_funding=repeated_hit), now_ms=now)
    assert reset_streak.state is State.WAIT
    _, no_cap_history = step_episode(start_episode(event), replace(
        panel, caps=()), now_ms=now)
    assert no_cap_history.state is State.WAIT_UNAVAILABLE
    stale_caps = (FundingCap(now - 300_001, 0.025, 8),)
    _, stale_cap_state = step_episode(start_episode(event), replace(
        panel, caps=stale_caps), now_ms=now)
    assert stale_cap_state.state is State.WAIT_UNAVAILABLE


def test_cap_risk_clearance_is_separate_from_event_specific_r5_release() -> None:
    event, panel, now = make_event_panel()
    caps = (
        FundingCap(event.event_ms - 300_000, 0.025, 8),
        FundingCap(now - 5_000, 0.025, 8),
    )
    rates = (
        Observation(event.event_ms - 240_000, event.event_ms - 239_999, 0.025),
        Observation(event.event_ms - 180_000, event.event_ms - 179_999, 0.023),
        Observation(event.event_ms - 120_000, event.event_ms - 119_999, 0.023),
        Observation(event.event_ms - 60_000, event.event_ms - 59_999, 0.023),
        Observation(now - 5_000, now - 4_999, 0.023),
    )

    _, decision = step_episode(
        start_episode(event), replace(panel, caps=caps, predicted_funding=rates),
        now_ms=now,
    )

    assert "observed_funding_cap_or_interval_risk" not in decision.reasons
    assert "R5_PREDICTED_FUNDING_NORMALIZED" not in decision.releases


def test_lagged_oi_release_and_wait_overrides_release() -> None:
    event, panel, now = make_event_panel()
    samples = (Observation(event.event_ms, event.event_ms + 1, 100),
               Observation(event.event_ms + STEP, event.event_ms + STEP + 1, 120),
               Observation(now - 600_001, now - 600_000, 110),
               Observation(now - 1_001, now - 1_000, 110))
    _, decision = step_episode(start_episode(event), replace(panel, oi=samples), now_ms=now)
    assert "R3_OI_FALL_5PCT_LAG" in decision.releases
    # Last row excluded: the decline in the final as-of row cannot trigger R3.
    not_yet = (*samples[:2], Observation(now - 1_001, now - 1_000, 110))
    _, other = step_episode(start_episode(event), replace(panel, oi=not_yet), now_ms=now)
    assert "R3_OI_FALL_5PCT_LAG" not in other.releases
    _, funding_wait = step_episode(start_episode(event), replace(panel,
        predicted_funding=(Observation(now - 100, now - 50, 0.025),)), now_ms=now)
    assert funding_wait.state is State.WAIT


def test_funding_cap_observation_normalization_and_liquidation_censoring() -> None:
    cap = normalize_funding_metadata([
        {"symbol": "ABCUSDT", "adjustedFundingRateCap": "0.025",
         "fundingIntervalHours": 4}], symbol="ABCUSDT", received_ms=BASE)
    assert cap == FundingCap(BASE, 0.025, 4)
    assert normalize_funding_metadata([], symbol="ABCUSDT", received_ms=BASE) is None
    with pytest.raises(ValueError, match="decimal fraction"):
        normalize_funding_metadata([{
            "symbol": "ABCUSDT", "adjustedFundingRateCap": "2.5",
            "fundingIntervalHours": 8}], symbol="ABCUSDT", received_ms=BASE)
    cap_and_floor = normalize_funding_metadata([
        {"symbol": "ABCUSDT", "adjustedFundingRateCap": "0.025",
         "adjustedFundingRateFloor": "-0.025", "fundingIntervalHours": 4}],
        symbol="ABCUSDT", received_ms=BASE)
    assert cap_and_floor is not None and cap_and_floor.floor == -0.025
    plans = pump_public_plans(("ABCUSDT",))
    assert any("!forceOrder@arr" in plan.streams for plan in plans)
    assert all("/market/" in p.url or "/public/" in p.url for p in plans)
    rest = pump_rest_poll_plan(("ABCUSDT",))
    assert len(rest) == 8
    assert {entry.path for entry in rest} == {
        "/fapi/v1/depth", "/fapi/v1/exchangeInfo", "/fapi/v1/fundingInfo",
        "/fapi/v1/klines", "/fapi/v1/openInterest", "/fapi/v1/premiumIndex",
        "/futures/data/openInterestHist",
    }
    assert {entry.trigger for entry in rest} == {
        "interval", "bootstrap", "depth_resync", "utc_5m",
    }
    assert all(entry.maximum_attempts == 2 for entry in rest)
    sample = {"e": "forceOrder", "E": BASE, "st": 1,
              "o": {"s": "ABCUSDT", "S": "BUY", "ap": "100", "l": "1", "T": BASE}}
    evidence = normalize_public_frame(sample, received_ms=BASE + 10,
                                      admitted_symbols=frozenset({"ABCUSDT"}))
    assert evidence is not None and evidence.censored_sample
    assert normalize_liquidation(evidence).is_short_liquidation
    assert normalize_public_frame({**sample, "st": 2}, received_ms=BASE + 10,
                                  admitted_symbols=frozenset({"ABCUSDT"})) is None
    with pytest.raises(ValueError, match="receipt precedes"):
        normalize_public_frame(sample, received_ms=BASE - 1,
                               admitted_symbols=frozenset({"ABCUSDT"}))


def test_capture_normalizers_closed_bar_oi_and_predicted_funding() -> None:
    closed = {
        "e": "kline", "E": BASE + STEP, "s": "ABCUSDT", "st": 1,
        "k": {"i": "5m", "T": BASE + STEP - 1, "x": True,
              "o": "100", "h": "131", "l": "99", "c": "130",
              "v": "10", "q": "100000"},
    }
    normalized = normalize_closed_kline_frame(
        closed, received_ms=BASE + STEP,
        admitted_symbols=frozenset({"ABCUSDT"}),
    )
    assert normalized is not None
    symbol, bar = normalized
    assert symbol == "ABCUSDT" and bar.close == 130 and bar.received_ms == BASE + STEP
    assert normalize_closed_kline_frame(
        {**closed, "k": {**closed["k"], "x": False}},
        received_ms=BASE + STEP,
        admitted_symbols=frozenset({"ABCUSDT"}),
    ) is None
    oi = normalize_oi_history([
        {"timestamp": BASE + STEP, "sumOpenInterest": "12"},
        {"timestamp": BASE, "sumOpenInterest": "10"},
    ], received_ms=BASE + 2 * STEP)
    assert [row.value for row in oi] == [10, 12]
    with pytest.raises(ValueError, match="duplicate"):
        normalize_oi_history([
            {"timestamp": BASE, "sumOpenInterest": "10"},
            {"timestamp": BASE, "sumOpenInterest": "11"},
        ], received_ms=BASE + 1)
    premium = normalize_premium_index(
        {"time": BASE, "lastFundingRate": "0.0002"}, received_ms=BASE + 1)
    assert premium == Observation(BASE, BASE + 1, 0.0002)
    mark = normalize_public_frame(
        {"e": "markPriceUpdate", "E": BASE, "s": "ABCUSDT", "st": 1,
         "p": "100", "r": "0.0003", "T": BASE + 10_000},
        received_ms=BASE + 1, admitted_symbols=frozenset({"ABCUSDT"}),
    )
    assert mark is not None
    assert evidence_to_predicted_funding(mark) == Observation(BASE, BASE + 1, 0.0003)


def _rest_envelope(*, status: int, attempt: int, role: str,
                   query: tuple[tuple[str, str], ...]) -> RestEnvelopeV2:
    error = None if 200 <= status < 300 else RestErrorCategory.HTTP_STATUS
    detail = None if error is None else "captured non-success status"
    return RestEnvelopeV2(
        request_started_at_ms=BASE,
        request_started_monotonic_ns=1,
        response_first_byte_at_ms=BASE + 1,
        response_first_byte_monotonic_ns=2,
        response_completed_at_ms=BASE + 2,
        response_completed_monotonic_ns=3,
        plan_sha256="a" * 64,
        process_boot_id="test-boot",
        request_role=role,
        correlation_id="test-correlation",
        attempt=attempt,
        ingest_seq=attempt,
        market=Market.FUTURES,
        endpoint_path="/fapi/v1/openInterest",
        canonical_query=query,
        response_status=status,
        response_headers=(("retry-after", "0"),) if status == 429 else (),
        payload_complete=True,
        raw_payload="{}",
        error_category=error,
        error_detail=detail,
    )


def test_pump_rest_scheduler_bounded_retry_and_disabled_runtime(tmp_path: Path) -> None:
    plan = tuple(entry for entry in pump_rest_poll_plan(("ABCUSDT",))
                 if entry.role == "oi_ABCUSDT")
    assert len(plan) == 1

    class FakeAdapter:
        def __init__(self) -> None:
            self.calls: list[int] = []

        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            attempt_value = kwargs["attempt"]
            assert isinstance(attempt_value, int)
            attempt = attempt_value
            self.calls.append(attempt)
            status = 429 if attempt == 1 else 200
            return _rest_envelope(status=status, attempt=attempt,
                                  role=plan[0].role, query=plan[0].query)

    adapter = FakeAdapter()
    scheduler = PumpRestScheduler(plan, adapter, maximum_retry_after_seconds=0)
    result = _run_inline(scheduler.capture_entry(plan[0], scheduled_at_ms=BASE))
    assert result.response_status == 200
    assert adapter.calls == [1, 2]
    cancelled_adapter = FakeAdapter()
    cancelled_scheduler = PumpRestScheduler(plan, cancelled_adapter,
                                            maximum_retry_after_seconds=0)
    stop = asyncio.Event()
    stop.set()
    with pytest.raises(asyncio.CancelledError):
        _run_inline(cancelled_scheduler.capture_entry(
            plan[0], scheduled_at_ms=BASE, stop_event=stop))
    assert cancelled_adapter.calls == []
    digest = pump_capture_plan_sha256(("ABCUSDT",), "b" * 64)
    assert len(digest) == 64
    runtime = PumpCaptureRuntime(
        symbols=("ABCUSDT",), policy_sha256="b" * 64,
        output_directory=tmp_path / "disabled",
        settings=PumpCaptureSettings(enabled=False),
    )
    with pytest.raises(RuntimeError, match="disabled"):
        _run_inline(runtime.run_smoke(duration_seconds=10))
    assert not (tmp_path / "disabled").exists()


def test_pump_rest_scheduler_does_not_retry_ip_ban() -> None:
    plan = tuple(entry for entry in pump_rest_poll_plan(("ABCUSDT",))
                 if entry.role == "oi_ABCUSDT")

    class BanAdapter:
        def __init__(self) -> None:
            self.calls: list[int] = []

        async def capture_attempt(self, **kwargs: object) -> RestEnvelopeV2:
            attempt_value = kwargs["attempt"]
            assert isinstance(attempt_value, int)
            self.calls.append(attempt_value)
            return _rest_envelope(status=418, attempt=attempt_value,
                                  role=plan[0].role, query=plan[0].query)

    adapter = BanAdapter()
    scheduler = PumpRestScheduler(plan, adapter, maximum_retry_after_seconds=0)
    result = _run_inline(scheduler.capture_entry(plan[0], scheduled_at_ms=BASE))
    assert result.response_status == 418
    assert adapter.calls == [1]
    assert next_utc_5m_poll_ms(600_001) == 901_000
    with pytest.raises(ValueError, match="clock"):
        next_utc_5m_poll_ms(-1)


def test_capture_generation_ledger_is_restart_idempotent_and_append_only(tmp_path: Path) -> None:
    path = tmp_path / "plan-ledger.sqlite"
    ledger = PumpCapturePlanLedger(path, maximum_generations=2)
    first, created = ledger.register(
        ("ABCUSDT",), policy_sha256="b" * 64, observed_at_ms=BASE,
    )
    assert created and first.generation == 1
    same, created = PumpCapturePlanLedger(path, maximum_generations=2).register(
        ("ABCUSDT",), policy_sha256="b" * 64, observed_at_ms=BASE + 1,
    )
    assert not created and same == first
    second, created = ledger.register(
        ("ABCUSDT", "XYZUSDT"), policy_sha256="b" * 64,
        observed_at_ms=BASE + 2,
    )
    assert created and second.generation == 2 and second.plan_sha256 != first.plan_sha256
    assert ledger.latest() == second
    with pytest.raises(OverflowError, match="bound"):
        ledger.register(("XYZUSDT",), policy_sha256="b" * 64, observed_at_ms=BASE + 3)


def _captured_rest(
    *, ingest_seq: int, role: str, path: str, payload: object,
    received_ms: int, query: tuple[tuple[str, str], ...] = (),
) -> RestEnvelopeV2:
    return RestEnvelopeV2(
        request_started_at_ms=received_ms - 2,
        request_started_monotonic_ns=ingest_seq * 10,
        response_first_byte_at_ms=received_ms - 1,
        response_first_byte_monotonic_ns=ingest_seq * 10 + 1,
        response_completed_at_ms=received_ms,
        response_completed_monotonic_ns=ingest_seq * 10 + 2,
        plan_sha256="a" * 64,
        process_boot_id="pump-materializer-test",
        request_role=role,
        correlation_id=f"correlation-{ingest_seq}",
        attempt=1,
        ingest_seq=ingest_seq,
        market=Market.FUTURES,
        endpoint_path=path,
        canonical_query=query,
        response_status=200,
        response_headers=(),
        payload_complete=True,
        raw_payload=json.dumps(payload, separators=(",", ":")),
    )


def _captured_ws(
    *, ingest_seq: int, stream: str, payload: object, received_ms: int,
    route: str = "market",
) -> CaptureEnvelopeV1:
    return CaptureEnvelopeV1(
        received_at_ms=received_ms,
        received_monotonic_ns=ingest_seq * 10,
        plan_sha256="a" * 64,
        process_boot_id="pump-materializer-test",
        connection_id=f"connection-{route}",
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        market=Market.FUTURES,
        route=route,
        stream=stream,
        subscription_streams=(stream,),
        raw_payload=json.dumps(payload, separators=(",", ":")),
    )


def test_raw_materializer_bootstrap_listing_and_shadow_decision(tmp_path: Path) -> None:
    engine = ShadowEpisodeEngine(tmp_path / "materialized.sqlite")
    materializer = PumpInputMaterializer(("ABCUSDT",), engine)
    start = BASE - 289 * STEP
    listing_open = BASE - 20 * 24 * 3_600_000
    listing = {"symbols": [{
        "symbol": "ABCUSDT", "onboardDate": listing_open, "status": "TRADING",
    }]}
    materializer.consume(_captured_rest(
        ingest_seq=1, role="exchange_info_all", path="/fapi/v1/exchangeInfo",
        payload=listing, received_ms=BASE,
    ))
    klines = []
    for i in range(289):
        open_ms = start + i * STEP
        klines.append([
            open_ms, "100", "100", "100", "100", "100",
            open_ms + STEP - 1, "10000", 10, "50", "5000", "0",
        ])
    materializer.consume(_captured_rest(
        ingest_seq=2, role="bootstrap_5m_ABCUSDT", path="/fapi/v1/klines",
        payload=klines, received_ms=BASE + 1,
        query=(("interval", "5m"), ("limit", "289"), ("symbol", "ABCUSDT")),
    ))
    materializer.consume(_captured_rest(
        ingest_seq=3, role="funding_info_all", path="/fapi/v1/fundingInfo",
        payload=[{"symbol": "ABCUSDT", "adjustedFundingRateCap": "0.025",
                  "adjustedFundingRateFloor": "-0.025", "fundingIntervalHours": 8}],
        received_ms=BASE + STEP + 10,
    ))
    event_close = BASE + STEP - 1
    kline_event = {
        "e": "kline", "E": BASE + STEP, "s": "ABCUSDT", "st": 1,
        "k": {"i": "5m", "T": event_close, "x": True,
              "o": "100", "h": "130", "l": "100", "c": "130",
              "v": "100", "q": "100000"},
    }
    admitted = materializer.consume(_captured_ws(
        ingest_seq=4, stream="abcusdt@kline_5m", payload=kline_event,
        received_ms=BASE + STEP,
    ))
    assert len(admitted.admitted_event_ids) == 1
    assert admitted.advances and admitted.advances[0].payload["state"] == "WAIT_UNAVAILABLE"
    materializer.consume(_captured_rest(
        ingest_seq=5, role="oi_ABCUSDT", path="/fapi/v1/openInterest",
        payload={"symbol": "ABCUSDT", "openInterest": "100", "time": event_close},
        received_ms=BASE + STEP + 20, query=(("symbol", "ABCUSDT"),),
    ))
    materializer.consume(_captured_rest(
        ingest_seq=6, role="premium_ABCUSDT", path="/fapi/v1/premiumIndex",
        payload={"symbol": "ABCUSDT", "lastFundingRate": "0.0001",
                 "time": event_close},
        received_ms=BASE + STEP + 30, query=(("symbol", "ABCUSDT"),),
    ))
    materializer.consume(_captured_ws(
        ingest_seq=7, stream="abcusdt@bookTicker",
        payload={"e": "bookTicker", "E": BASE + STEP + 35, "s": "ABCUSDT",
                 "st": 1, "b": "129.9", "B": "1000", "a": "130.1", "A": "1000"},
        received_ms=BASE + STEP + 40, route="public",
    ))
    depth = {
        "lastUpdateId": 1,
        "bids": [["129.9", "1000"], ["129.5", "1000"]],
        "asks": [["130.1", "1000"], ["130.5", "1000"]],
    }
    ready = materializer.consume(_captured_rest(
        ingest_seq=8, role="quality_depth_ABCUSDT", path="/fapi/v1/depth",
        payload=depth, received_ms=BASE + STEP + 50,
        query=(("limit", "1000"), ("symbol", "ABCUSDT")),
    ))
    assert ready.advances
    assert ready.advances[0].payload["state"] == "WAIT"
    assert ready.advances[0].payload["delivery_enabled"] is False
    assert materializer.panel("ABCUSDT").book is not None


def test_materializer_out_of_order_funding_metadata_does_not_rewind(tmp_path: Path) -> None:
    materializer = PumpInputMaterializer(
        ("ABCUSDT",), ShadowEpisodeEngine(tmp_path / "cap-order.sqlite"))
    state = materializer._states["ABCUSDT"]
    PumpInputMaterializer._merge_cap(state, FundingCap(BASE + 20, 0.025, 4))
    PumpInputMaterializer._merge_cap(state, FundingCap(BASE + 10, 0.025, 8))
    assert state.cap_latest == FundingCap(BASE + 20, 0.025, 4)
    assert tuple(item.observed_ms for item in state.cap_changes) == (BASE + 10, BASE + 20)
    with pytest.raises(ValueError, match="same observed timestamp"):
        PumpInputMaterializer._merge_cap(state, FundingCap(BASE + 20, 0.03, 4))


def test_terminal_shadow_archive_is_hash_acknowledged_before_prune(tmp_path: Path) -> None:
    event, panel, now = make_event_panel()
    db_path = tmp_path / "archive.sqlite"
    engine = ShadowEpisodeEngine(db_path)
    receipt = engine.advance(event, panel, now_ms=now)
    assert receipt.payload["state"] == "FADE-CANDIDATE"
    terminal = engine.advance(event, panel, now_ms=event.event_ms + 24 * 3_600_000 + 1)
    assert terminal.payload["state"] == "EXPIRED"
    lifecycle = OfflinePumpLifecycle(
        engine, AppendOnlyOutcomeStore(db_path), clock_ms=lambda: now,
    )
    archive_path = tmp_path / "archive" / "terminal.json"
    blocked = lifecycle.archive_terminal(archive_path)
    assert blocked.event_count == 0
    assert engine.checkpoint(event.event_id) is not None
    pending = lifecycle.label_horizon(
        event_id=event.event_id, origin="alert", origin_ms=now,
        origin_mark=event.event_price, horizon_hours=4, marks=(),
    )
    assert pending.status is LabelStatus.PENDING
    requirements = sorted(
        ((origin_ms + horizon * 3_600_000 + 1, origin, origin_ms, horizon)
         for origin, origin_ms in (("event", event.event_ms), ("alert", now))
         for horizon in (4, 24)),
    )
    assert len({item[0] for item in requirements}) == 4
    for index, (labeled_at, origin, origin_ms, horizon) in enumerate(requirements):
        lifecycle = OfflinePumpLifecycle(
            engine, AppendOnlyOutcomeStore(db_path), clock_ms=lambda at=labeled_at: at,
        )
        outcome = lifecycle.label_horizon(
            event_id=event.event_id, origin=origin, origin_ms=origin_ms,
            origin_mark=event.event_price, horizon_hours=horizon,
            marks=(),
        )
        assert outcome.status is LabelStatus.CENSORED
        assert not lifecycle.persist_outcome(outcome)
        if index < len(requirements) - 1:
            still_blocked = lifecycle.archive_terminal(archive_path)
            assert still_blocked.event_count == 0
            assert engine.checkpoint(event.event_id) is not None
    reopened = ShadowEpisodeEngine(db_path)
    resumed = OfflinePumpLifecycle(
        reopened, AppendOnlyOutcomeStore(db_path),
        clock_ms=lambda: now + 48 * 3_600_000 + 1,
    )
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    (archive_path.parent / (archive_path.name + ".partial")).write_text(
        "unverified partial", encoding="utf-8",
    )
    with pytest.raises(FileExistsError, match="partial requires operator review"):
        resumed.archive_terminal(archive_path)
    assert reopened.checkpoint(event.event_id) is not None
    (archive_path.parent / (archive_path.name + ".partial")).unlink()
    constrained = OfflinePumpLifecycle(
        ShadowEpisodeEngine(db_path, maximum_archive_bytes=1),
        AppendOnlyOutcomeStore(db_path), clock_ms=lambda: now + 48 * 3_600_000 + 1,
    )
    with pytest.raises(OverflowError, match="terminal archive capacity"):
        constrained.archive_terminal(archive_path)
    assert reopened.checkpoint(event.event_id) is not None
    archive = resumed.archive_terminal(archive_path)
    assert archive.event_count == 1 and archive.decision_count == 2
    assert len(archive.sha256) == 64
    assert reopened.checkpoint(event.event_id) is None
    payload = json.loads(Path(archive.path).read_text(encoding="utf-8"))
    assert payload["schema_version"] == "pump_fade_v2_terminal_archive_v1"


def test_verified_capture_directory_replays_into_materializer(tmp_path: Path) -> None:
    capture = tmp_path / "capture"
    record = _captured_rest(
        ingest_seq=1,
        role="exchange_info_all",
        path="/fapi/v1/exchangeInfo",
        payload={"symbols": [{
            "symbol": "ABCUSDT",
            "onboardDate": BASE - 20 * 24 * 3_600_000,
            "status": "TRADING",
        }]},
        received_ms=BASE,
    )
    writer = SegmentedCaptureWriter(
        capture,
        plan_sha256="a" * 64,
        process_boot_id="pump-materializer-test",
        maximum_total_bytes=4 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    writer.append(record, record_to_json_line(record))
    writer.close()
    (capture / "pump-v2-capture-receipt.json").write_text(
        json.dumps({
            "plan_sha256": "a" * 64,
            "process_boot_id": "pump-materializer-test",
            "symbols": ["ABCUSDT"],
        }),
        encoding="utf-8",
    )
    db_path = tmp_path / "replay.sqlite"
    engine = ShadowEpisodeEngine(db_path)
    materializer = PumpInputMaterializer(("ABCUSDT",), engine)
    lifecycle = OfflinePumpLifecycle(
        engine, AppendOnlyOutcomeStore(db_path), clock_ms=lambda: BASE + 10,
    )
    receipt = lifecycle.replay_capture(capture, materializer)
    assert receipt == {
        "schema_version": "pump_fade_v2_materialization_receipt_v1",
        "evidence_class": "LOCAL_PUBLIC_CAPTURE_REPLAY_NOT_STRATEGY_CONFIRMATION",
        "segment_count": 1,
        "records": 1,
        "admitted_events": 0,
        "posted_previews": 0,
        "orchestration_asof_ms": BASE + 10,
        "outcome_store": str(db_path),
    }


def test_private_addition_accounting_uses_risk_increases_not_total_orders(
    tmp_path: Path,
) -> None:
    source = tmp_path / "orders.csv"
    source.write_text(
        "Uid,Time,Order No,Symbol,Type,Side,Price,Average Price,Amount,"
        "Executed Amount,Executed Quote Amount,Stop Price,Status,Update Time\n"
        "x,2026-01-01 00:00:00,1,ABCUSDT,MARKET,SELL,0,100,1,1,100,0,FILLED,"
        "2026-01-01 00:00:00\n"
        "x,2026-01-01 00:01:00,2,ABCUSDT,MARKET,SELL,0,105,0.5,0.5,52.5,0,FILLED,"
        "2026-01-01 00:01:00\n"
        "x,2026-01-01 00:02:00,3,ABCUSDT,MARKET,BUY,0,90,1.5,1.5,135,0,FILLED,"
        "2026-01-01 00:02:00\n"
        "x,2026-01-01 00:03:00,4,XYZUSDT,MARKET,BUY,0,100,1,1,100,0,FILLED,"
        "2026-01-01 00:03:00\n"
        "x,2026-01-01 00:04:00,5,XYZUSDT,MARKET,BUY,0,95,0.6,0.6,57,0,FILLED,"
        "2026-01-01 00:04:00\n"
        "x,2026-01-01 00:05:00,6,XYZUSDT,MARKET,SELL,0,110,1.6,1.6,176,0,FILLED,"
        "2026-01-01 00:05:00\n",
        encoding="utf-8",
    )
    result = aggregate_private_additions(source)
    assert result["executed_order_rows"] == 6
    assert result["reconstructed_position_episodes"] == 2
    assert result["addition_order_count_distribution"] == {"1": 2}
    assert result["episodes_structurally_within_primary_quantity_limits"] == 1
    assert result["official_r2_gate"] == "UNAVAILABLE"


def test_ladder_partial_fill_risk_budget_and_contra_stop() -> None:
    original = Fill("f0", "initial", 1.0, 100, BASE)
    plan = FrozenLadder(Side.SHORT, original, invalidation_price=120,
                        max_loss_usdt=30)
    part = Fill("f1", "add1", 0.25, 110, BASE + STEP)
    allowed = assess_ladder(plan, (), proposed=part,
                            renewed_confirmation=True)
    assert allowed.allowed
    assert allowed.maximum_proposed_quantity is not None
    assert allowed.maximum_proposed_quantity >= part.quantity
    second_partial = Fill("f2", "add1", 0.25, 110, BASE + 2 * STEP)
    accepted = assess_ladder(plan, (part,), proposed=second_partial,
                             renewed_confirmation=True)
    assert accepted.allowed and accepted.admitted_additions == 1
    no_confirmation = assess_ladder(plan, (part,), proposed=second_partial)
    assert not no_confirmation.allowed
    risk_block = assess_ladder(replace(plan, max_loss_usdt=21), (),
                               proposed=part, renewed_confirmation=True)
    assert not risk_block.allowed and "frozen_loss_budget_exceeded" in risk_block.reasons
    assert risk_block.maximum_proposed_quantity is not None
    assert risk_block.maximum_proposed_quantity < part.quantity
    wait_block = assess_ladder(plan, (), proposed=part, candidate_state=State.WAIT,
                               renewed_confirmation=True)
    assert not wait_block.allowed
    with pytest.raises(ValueError, match="adverse"):
        FrozenLadder(Side.SHORT, original, invalidation_price=99, max_loss_usdt=30)
    with pytest.raises(ValueError, match="duplicate"):
        assess_ladder(plan, (part, part))


def test_executable_ladder_comparators_replay_same_parent_and_partial_tranches() -> None:
    entry_quote = ReplayQuote(BASE, BASE, 100.0, 100.1)
    additions = []
    for fill_id, tranche, at_ms, price, quantity in (
        ("a1", "add1", BASE + 100, 105.0, 0.2),
        ("a1-part", "add1", BASE + 200, 105.1, 0.1),
        ("a2", "add2", BASE + 300, 111.0, 0.2),
        ("a3", "add3", BASE + 400, 117.0, 0.2),
    ):
        quote = ReplayQuote(at_ms, at_ms, price, price + 0.1)
        additions.append(AddOpportunity(
            Fill(fill_id, tranche, quantity, price, at_ms), at_ms, quote,
            State.FADE_CANDIDATE, False, True,
        ))
    horizon = BASE + 1_000
    path = ParentPath(
        parent_id="parent-1", event_id="event-1", event_at_ms=BASE - 1,
        decision_available_ms=BASE, exposure_class=ExposureClass.PUBLIC_SYNTHETIC,
        ladder=FrozenLadder(Side.SHORT, Fill("initial", "initial", 1.0, 100.0, BASE),
                            invalidation_price=120.0, max_loss_usdt=50.0),
        entry_quote=entry_quote,
        quotes=(entry_quote, *(item.quote for item in additions),
                ReplayQuote(horizon, horizon, 90.0, 90.1)),
        bars=(PathBar(horizon - 100, horizon - 99, 115.0, 90.0),),
        additions=tuple(additions),
        funding=(FundingSettlement(BASE + 500, BASE + 501, 0.0001, 100.0),),
        funding_coverage=FundingCoverage(BASE, horizon, (BASE + 500,)),
        horizon_end_ms=horizon, asof_ms=horizon + 1,
    )

    results = compare_parent_paths((path,))
    by_arm = {row.arm: row for row in results}

    assert by_arm["primary_two_add"].status == "COMPLETE"
    assert by_arm["primary_two_add"].accepted_additions == 2
    assert by_arm["primary_two_add"].filled_quantity == pytest.approx(1.5)
    assert by_arm["first_fill_only"].filled_quantity == 1.0
    assert by_arm["legacy_eight_add_comparator"].accepted_additions == 3
    assert by_arm["three_add_sensitivity"].accepted_additions == 3
    for row in by_arm.values():  # every add precedes the BASE+500 settlement
        assert row.funding_status == "VERIFIED"
        assert row.funding_usdt == pytest.approx(row.filled_quantity * 100.0 * 0.0001)
    summary = summarize_arm_results(results)
    primary_summary = summary["primary_two_add"]
    assert isinstance(primary_summary, dict)
    assert primary_summary["q99_status"] == "UNAVAILABLE_INSUFFICIENT_OBSERVED_TAIL"


def test_executable_ladder_censors_missing_exit_and_does_not_zero_missing_funding() -> None:
    entry = ReplayQuote(BASE, BASE, 100.0, 100.1)
    initial = Fill("initial", "initial", 1.0, 100.0, BASE)
    frozen = FrozenLadder(Side.SHORT, initial, 120.0, 50.0)
    censored = ParentPath(
        parent_id="parent-2", event_id="event-2", event_at_ms=BASE - 1,
        decision_available_ms=BASE, exposure_class=ExposureClass.PUBLIC_SYNTHETIC,
        ladder=frozen, entry_quote=entry, quotes=(entry,), bars=(), additions=(),
        funding=(), funding_coverage=None,
        horizon_end_ms=BASE + 1_000, asof_ms=BASE + 1_001,
    )
    assert compare_parent_paths((censored,))[0].status == "CENSORED_NO_EXECUTABLE_EXIT"
    complete = ParentPath(
        parent_id="parent-2", event_id="event-2", event_at_ms=BASE - 1,
        decision_available_ms=BASE, exposure_class=ExposureClass.PUBLIC_SYNTHETIC,
        ladder=frozen, entry_quote=entry,
        quotes=(entry, ReplayQuote(BASE + 1_000, BASE + 1_000, 99.0, 99.1)),
        bars=(), additions=(), funding=(), funding_coverage=None,
        horizon_end_ms=BASE + 1_000, asof_ms=BASE + 1_001,
    )
    first = compare_parent_paths((complete,))[0]
    assert first.status == "COMPLETE_FUNDING_UNAVAILABLE"
    assert first.gross_pnl_usdt is not None and first.costs_usdt is not None
    assert first.funding_usdt is None and first.net_pnl_usdt is None


def test_executable_comparator_supports_clustered_inference_and_observed_q99() -> None:
    entry = ReplayQuote(BASE, BASE, 100.0, 100.1)
    exit_quote = ReplayQuote(BASE + 1_000, BASE + 1_000, 124.9, 125.0)
    paths = tuple(
        ParentPath(
            parent_id=f"support-{index}", event_id=f"support-event-{index}",
            event_at_ms=BASE - (20 - index % 20) * 86_400_000,
            decision_available_ms=BASE, exposure_class=ExposureClass.PUBLIC_SYNTHETIC,
            ladder=FrozenLadder(
                Side.SHORT, Fill(f"initial-{index}", "initial", 1.0, 100.0, BASE),
                140.0, 50.0,
            ),
            entry_quote=entry, quotes=(entry, exit_quote), bars=(), additions=(),
            funding=(), funding_coverage=FundingCoverage(BASE, BASE + 1_000, ()),
            horizon_end_ms=BASE + 1_000, asof_ms=BASE + 1_001,
        )
        for index in range(100)
    )
    summary = summarize_arm_results(compare_parent_paths(paths))
    primary = summary["primary_two_add"]
    assert isinstance(primary, dict)
    assert primary["q99_status"] == "OBSERVED_POSITIVE_LOSS_TAIL_N_GE_100"
    comparisons = summary["paired_comparisons_vs_first_fill_only"]
    assert isinstance(comparisons, dict)
    for name in ("primary_two_add", "legacy_eight_add_comparator",
                 "three_add_sensitivity"):
        comparison = comparisons[name]
        assert isinstance(comparison, dict)
        assert comparison["paired_net_parent_n"] == 100
        assert comparison["utc_day_clusters_n"] == 20
        assert comparison["bootstrap_replicates"] == 10_000
        assert isinstance(comparison["holm_adjusted_p"], float)


def test_executable_ladder_stop_precedes_later_add_and_rejected_confirmation() -> None:
    entry = ReplayQuote(BASE, BASE, 100.0, 100.1)
    late = ReplayQuote(BASE + 600, BASE + 600, 110.0, 110.1)
    add = AddOpportunity(
        Fill("late-add", "add1", 0.2, 110.0, BASE + 600), BASE + 600, late,
        State.FADE_CANDIDATE, False, True,
    )
    stop_quote = ReplayQuote(BASE + 501, BASE + 501, 120.0, 120.2)
    path = ParentPath(
        parent_id="parent-stop", event_id="event-stop", event_at_ms=BASE - 1,
        decision_available_ms=BASE, exposure_class=ExposureClass.PUBLIC_SYNTHETIC,
        ladder=FrozenLadder(Side.SHORT, Fill("initial-stop", "initial", 1, 100, BASE),
                            120, 50),
        entry_quote=entry, quotes=(entry, stop_quote, late, ReplayQuote(
            BASE + 1_000, BASE + 1_000, 115, 115.2,
        )), bars=(PathBar(BASE + 500, BASE + 501, 121, 95),),
        additions=(add,), funding=(), funding_coverage=None,
        horizon_end_ms=BASE + 1_000, asof_ms=BASE + 1_001,
    )
    result = {row.arm: row for row in compare_parent_paths((path,))}
    primary = result["primary_two_add"]
    assert primary.status == "COMPLETE_FUNDING_UNAVAILABLE"
    assert primary.accepted_additions == 0
    assert primary.net_pnl_usdt is None

    blocked = replace(add, fill=Fill("blocked-add", "add1", 0.2, 110, BASE + 600),
                      renewed_confirmation=False)
    no_stop = replace(path, parent_id="parent-no-confirm", event_id="event-no-confirm",
                      bars=(), additions=(blocked,))
    blocked_result = {row.arm: row for row in compare_parent_paths((no_stop,))}
    assert blocked_result["primary_two_add"].accepted_additions == 0


def test_manual_liquidation_requires_supplied_price_and_fresh_mark() -> None:
    unknown = supplied_liquidation_advisory(Side.SHORT, liquidation_price=None,
        mark_price=100, mark_received_ms=BASE, now_ms=BASE + 1)
    assert unknown.status == "UNKNOWN"
    advisory = supplied_liquidation_advisory(Side.SHORT, liquidation_price=125,
        mark_price=100, mark_received_ms=BASE, now_ms=BASE + 1)
    assert advisory.adverse_distance_pct == 25
    stale = supplied_liquidation_advisory(Side.SHORT, liquidation_price=125,
        mark_price=100, mark_received_ms=BASE, now_ms=BASE + 5001)
    assert stale.status == "UNKNOWN"


def test_outcome_horizon_censor_and_append_only_identity(tmp_path) -> None:
    marks = tuple(MarkBar(BASE + (i + 1) * STEP, BASE + (i + 1) * STEP + 1,
                          125 if i == 1 else 101, 98, 100) for i in range(48))
    start_quote = ExecutableQuote(BASE, BASE + 1, 99.9, 100.1)
    end = BASE + 4 * 3_600_000
    end_quote = ExecutableQuote(end, end + 1, 99.9, 100.1)
    pending = label_outcome(event_id="a", origin="event", origin_ms=BASE,
                            origin_mark=100, horizon_hours=4,
                            decision_now_ms=end - 1, marks=marks,
                            squeeze_adverse_pct=PRIMARY_SQUEEZE_PCT)
    assert pending.status is LabelStatus.PENDING and pending.net_short_bps is None
    complete = label_outcome(event_id="a", origin="event", origin_ms=BASE,
                             origin_mark=100, horizon_hours=4,
                             decision_now_ms=end + 1, marks=marks,
                             entry_quote=start_quote, exit_quote=end_quote,
                             verified_funding_coverage=True,
                             squeeze_adverse_pct=PRIMARY_SQUEEZE_PCT)
    assert complete.status is LabelStatus.COMPLETE
    assert complete.squeeze_primary is True
    assert complete.squeeze_primary_threshold_pct == 15.0
    assert complete.mark_mae_pct == 25
    assert complete.net_short_bps is not None
    assert complete.triple_barrier_first == "STOP"
    store = AppendOnlyOutcomeStore(tmp_path / "outcomes.sqlite")
    assert store.append(pending)
    assert store.append(complete)
    assert not AppendOnlyOutcomeStore(tmp_path / "outcomes.sqlite").append(complete)
    with pytest.raises(ValueError, match="identity conflict"):
        store.append(replace(complete, mark_mae_pct=24))
    censored = label_outcome(event_id="b", origin="alert", origin_ms=BASE,
                             origin_mark=100, horizon_hours=4,
                             decision_now_ms=end + 1, marks=marks[:-1],
                             squeeze_adverse_pct=PRIMARY_SQUEEZE_PCT)
    assert censored.status is LabelStatus.CENSORED
    ambiguous_marks = tuple(MarkBar(
        BASE + (i + 1) * STEP, BASE + (i + 1) * STEP + 1,
        125 if i == 1 else 101, 85 if i == 1 else 98, 100)
        for i in range(48))
    ambiguous = label_outcome(event_id="d", origin="event", origin_ms=BASE,
        origin_mark=100, horizon_hours=4, decision_now_ms=end + 1,
        marks=ambiguous_marks, squeeze_adverse_pct=PRIMARY_SQUEEZE_PCT)
    assert ambiguous.triple_barrier_first == "AMBIGUOUS_SAME_BAR"
    boundary_marks = tuple(MarkBar(
        BASE + (i + 1) * STEP, BASE + (i + 1) * STEP + 1,
        115 if i == 1 else 101, 98, 100) for i in range(48))
    boundary = label_outcome(event_id="c", origin="event", origin_ms=BASE,
        origin_mark=100, horizon_hours=4, decision_now_ms=end + 1,
        marks=boundary_marks, squeeze_adverse_pct=PRIMARY_SQUEEZE_PCT)
    assert boundary.squeeze_primary is True  # exact 15% is inclusive


def test_shadow_preview_storage_restart_no_discord(tmp_path) -> None:
    event, panel, now = make_event_panel()
    _, decision = step_episode(start_episode(event), panel, now_ms=now)
    preview = preview_payload(decision, spread_bps=10)
    assert preview["delivery_enabled"] is False
    assert isinstance(preview["decision_time"], dict)
    assert "asia_seoul" in preview["decision_time"]
    queue = ShadowPreviewStore(tmp_path / "shadow.sqlite")
    assert queue.enqueue(preview)
    assert not ShadowPreviewStore(tmp_path / "shadow.sqlite").enqueue(preview)
    with pytest.raises(ValueError, match="preview-only"):
        queue.enqueue({**preview, "delivery_enabled": True})


def test_shadow_episode_transaction_restart_conflict_and_bounded_capacity(tmp_path) -> None:
    event, panel, now = make_event_panel()
    path = tmp_path / "durable.sqlite"
    engine = ShadowEpisodeEngine(path, max_records=1, max_episodes=1)
    first = engine.advance(event, panel, now_ms=now)
    assert first.posted and first.payload["state"] == "FADE-CANDIDATE"
    rebooted = ShadowEpisodeEngine(path, max_records=1, max_episodes=1)
    repeat = rebooted.advance(event, panel, now_ms=now)
    assert repeat.posted is False
    assert repeat.payload == first.payload
    with pytest.raises(ValueError, match="different evidence"):
        rebooted.advance(event, replace(panel, book=None), now_ms=now)
    with pytest.raises(OverflowError, match="preview capacity"):
        rebooted.advance(event, panel, now_ms=now + 1)
    with pytest.raises(ValueError, match="cannot move backwards"):
        rebooted.advance(event, panel, now_ms=now - 1)
    different = replace(event, event_id="different-opportunity")
    with pytest.raises(OverflowError, match="episode capacity"):
        rebooted.advance(different, panel, now_ms=now)
    assert rebooted.advance(event, panel, now_ms=now).posted is False


def test_statistics_block_and_holm_boundary() -> None:
    assert day_block_interval((("day1", 1.0),))["replicates"] == 0
    sampled = day_block_interval((("day1", 1.0), ("day1", 2.0), ("day2", 3.0)),
                                 replicates=100)
    assert sampled["clusters"] == 2 and sampled["replicates"] == 100
    assert holm_adjust({"a": 0.02, "b": 0.04}) == {"a": 0.04, "b": 0.04}
    assert len({"day1"}) == 1  # 10k resamples cannot manufacture another cluster
    abstention = parent_release_accounting(
        release_return_bps=None, favorable_excursion_pct=12.0)
    assert abstention["parent_in_denominator"] is True
    assert abstention["trade_created"] is False
    assert abstention["missed_fade_indicator"] is True
    assert abstention["net_trade_pnl"] == "NOT_APPLICABLE"
    gate = evaluate_forward_gate(
        days_since_eligible_start=56, valid_parent_alerts=150,
        clustered_power_passed=True, net_mean_bps=4.0,
        one_sided_cluster_lower_bound_bps=-1.0)
    assert gate.duration_gate and gate.parent_count_gate
    assert not gate.positive_lower_bound_gate and not gate.eligible_for_human_review


def test_replay_synthetic_closed_panel_with_hashed_manifest(tmp_path: Path) -> None:
    """Engineering fixture only; a passing result is never market evidence."""

    candles = []
    for i in range(577):
        px = Decimal("100") if i < 288 else (
            Decimal("130") if i == 288 else Decimal("125"))
        candles.append(Candle(
            market=Market.FUTURES, symbol="TESTUSDT", interval="5m",
            open_time_ms=BASE + i * STEP,
            close_time_ms=BASE + (i + 1) * STEP - 1,
            open=px, high=px, low=px, close=px,
            volume=Decimal("100"), quote_volume=Decimal("10000"),
            trade_count=10, taker_buy_base_volume=Decimal("50"),
            taker_buy_quote_volume=Decimal("5000"), is_closed=True,
        ))
    rows = tuple(candles)
    found = find_proxy_events(rows)
    assert len(found) == 1
    assert found[0].baseline_24h_short_return_bps is not None
    assert found[0].first_release_minutes["R1_NO_HIGH_60M"] is not None
    assert found[0].first_release_minutes["R3_OI_FALL_5PCT_LAG"] is None
    assert found[0].kline_proxy_24h_squeeze_20pct_sensitivity is False
    request = KlineDatasetRequest(
        Market.FUTURES, "TESTUSDT", "5m", rows[0].open_time_ms,
        rows[-1].close_time_ms,
    )
    dataset = KlineDataset(request, rows)
    source_dir = tmp_path / "futures"
    source_dir.mkdir()
    path = write_kline_csv(dataset, source_dir / "TEST__TESTUSDT__5m.csv.gz")
    write_dataset_manifest(build_dataset_manifest(path), str(path) + ".manifest.json")
    execution_start = BASE + 288 * STEP
    execution_entry = ReplayQuote(execution_start + 1, execution_start + 1, 130, 130.1)
    execution_parent = ParentPath(
        parent_id="synthetic-parent", event_id="synthetic-event",
        event_at_ms=execution_start, decision_available_ms=execution_start + 1,
        exposure_class=ExposureClass.PUBLIC_SYNTHETIC,
        ladder=FrozenLadder(
            Side.SHORT,
            Fill("synthetic-entry", "initial", 1.0, 130, execution_start + 1),
            invalidation_price=150, max_loss_usdt=40,
        ),
        entry_quote=execution_entry,
        quotes=(execution_entry, ReplayQuote(
            execution_start + 60_000, execution_start + 60_000, 128, 128.1,
        )),
        bars=(), additions=(), funding=(),
        funding_coverage=FundingCoverage(execution_start + 1, execution_start + 60_000, ()),
        horizon_end_ms=execution_start + 60_000, asof_ms=execution_start + 60_001,
    )
    output = run_public_kline_feasibility(source_dir, policy_file=POLICY_PATH,
                                           output_dir=tmp_path / "result",
                                           execution_paths=(execution_parent,))
    assert output["prefilter_events_n"] == 1
    assert output["complete_24h_kline_proxy_events_n"] == 1
    assert "joint_25_8" in output["r3_skip_joint_cell_proxy_hits"]
    assert "unconditional_40" in output["r3_skip_joint_cell_proxy_hits"]
    assert "signal_n" in output["r4_continuation_long_new_high_wait_kline_proxy"]
    assert "does not identify causal" in output["r3_estimand"]
    assert output["historical_funding_cap_change_chronology"].startswith("UNAVAILABLE")
    assert output["registered_arm_status"]["P1_prior_pump_validation"].startswith("EXPOSED")
    assert "R2_three_addition_sensitivity" in output["registered_arm_status"]
    assert "bootstrap draws do not create tail observations" in (
        output["registered_arm_status"]["R2_q99_loss_reduction"]
    )
    assert output["confirmed_positive_expectancy"] is False
    assert output["official_holm_p_values"] is None
    assert output["r2_executable_scenario_comparison"]["status"] == (
        "EXECUTABLE_SCENARIO_COMPARISON_NOT_HISTORICAL_EFFICACY"
    )
    assert "primary_two_add" in output["r2_executable_scenario_comparison"]["arms"]
    assert (tmp_path / "result/data_manifest.json").is_file()


def test_release_manifest_hashed_rules_and_local_example_only(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    result = build_release_evidence(repo_root, tmp_path / "release")
    assert result["status"] == "LOCAL_PREPARED_NO_DEPLOYMENT"
    assert result["forward_eligible"] is False
    assert result["production_orders_enabled"] is False
    assert result["capture_enabled"] is False
    assert result["executable_source_closure_sha256"]
    assert result["research_evidence_tree_sha256"]
    package_path = tmp_path / "release/pump_fade_v2d_package.zip"
    assert hashlib.sha256(package_path.read_bytes()).hexdigest() == (
        result["executable_package_sha256"]
    )
    extracted = tmp_path / "isolated"
    with zipfile.ZipFile(package_path) as package:
        package.extractall(extracted)
    code = (
        f"import sys; sys.path.insert(0, {str(extracted / 'src')!r}); "
        "import signalbot; import signalbot.pump_fade_v2.release; "
        "print(signalbot.__file__)"
    )
    child_env = {key: value for key, value in os.environ.items()
                 if key not in {"PYTHONPATH", "PYTHONHOME"}}
    isolated = subprocess.run(
        [sys.executable, "-I", "-c", code], cwd=tmp_path, env=child_env,
        check=False, capture_output=True, text=True,
    )
    assert isolated.returncode == 0, isolated.stderr
    assert Path(isolated.stdout.strip()).resolve().is_relative_to(extracted / "src")
    example = (tmp_path / "release/sample_shadow_payload.json").read_text(encoding="utf-8")
    assert "SYNTHETIC_EXAMPLE_ONLY_DO_NOT_TRADE" in example


def test_release_import_closure_detects_changes_and_missing_local_dependencies(
    tmp_path: Path,
) -> None:
    root = tmp_path / "mini"
    for name in (
        "src/signalbot/pump_fade_v2",
        "src/signalbot/capture",
        "src/signalbot/domain",
    ):
        (root / name).mkdir(parents=True)
    files = {
        "src/signalbot/__init__.py": "",
        "src/signalbot/pump_fade_v2/__init__.py": "",
        "src/signalbot/pump_fade_v2/runtime.py": (
            "from signalbot.capture.models import CaptureModel\n"
            "from signalbot.capture.receipts import Receipt\n"
            "from signalbot.domain.enums import Market\n"
        ),
        "src/signalbot/capture/__init__.py": "",
        "src/signalbot/capture/models.py": "class CaptureModel: pass\n",
        "src/signalbot/capture/receipts.py": "class Receipt: pass\n",
        "src/signalbot/domain/__init__.py": "",
        "src/signalbot/domain/enums.py": "class Market: pass\n",
    }
    for name, contents in files.items():
        (root / name).write_text(contents, encoding="utf-8")

    before = source_closure_manifest(root)
    assert "src/signalbot/capture/models.py" in before
    assert "src/signalbot/capture/receipts.py" in before
    (root / "src/signalbot/capture/models.py").write_text(
        "class CaptureModel:\n    revision = 2\n", encoding="utf-8",
    )
    after = source_closure_manifest(root)
    assert before["src/signalbot/capture/models.py"] != (
        after["src/signalbot/capture/models.py"]
    )
    (root / "src/signalbot/capture/receipts.py").unlink()
    with pytest.raises(ValueError, match="unresolved local execution dependency"):
        source_closure_manifest(root)


# --------------------------------------------------------------------------
# F5: per-arm counterfactual funding (open quantity at each settlement)
# --------------------------------------------------------------------------

FUNDING_HORIZON = BASE + 1_000


def _funding_path(
    *,
    side: Side = Side.SHORT,
    additions: tuple[tuple[str, str, int, float, float], ...] = (),
    exit_at: int = FUNDING_HORIZON,
    settlements: tuple[tuple[int, float, float], ...] = (),
    scheduled: tuple[int, ...] | None = None,
    coverage: FundingCoverage | None | str = "default",
    stop_bar_close: int | None = None,
    parent_id: str = "funding-parent",
) -> ParentPath:
    """Synthetic same-parent path with an explicit funding schedule."""

    short = side is Side.SHORT
    entry = ReplayQuote(BASE, BASE, 100.0, 100.1)
    initial_price = entry.bid if short else entry.ask
    opportunities = []
    quotes = [entry]
    for fill_id, tranche, at_ms, price, quantity in additions:
        quote = ReplayQuote(at_ms, at_ms, price, price + 0.1) if short else ReplayQuote(
            at_ms, at_ms, price - 0.1, price)
        fill_price = quote.bid if short else quote.ask
        quotes.append(quote)
        opportunities.append(AddOpportunity(
            Fill(fill_id, tranche, quantity, fill_price, at_ms), at_ms, quote,
            State.FADE_CANDIDATE, False, True,
        ))
    quotes.append(ReplayQuote(exit_at, exit_at, 99.0, 99.1))
    bars = ()
    if stop_bar_close is not None:
        stop_price = 130.0 if short else 70.0
        bars = (PathBar(stop_bar_close, stop_bar_close + 1,
                        stop_price if short else 101.0, 99.0 if short else stop_price),)
    schedule = tuple(item[0] for item in settlements) if scheduled is None else scheduled
    if coverage == "default":
        coverage = FundingCoverage(BASE, FUNDING_HORIZON, schedule)
    assert coverage is None or isinstance(coverage, FundingCoverage)
    return ParentPath(
        parent_id=parent_id, event_id=f"event-{parent_id}", event_at_ms=BASE - 1,
        decision_available_ms=BASE, exposure_class=ExposureClass.PUBLIC_SYNTHETIC,
        ladder=FrozenLadder(
            side, Fill("initial", "initial", 1.0, initial_price, BASE),
            invalidation_price=120.0 if short else 80.0, max_loss_usdt=50.0,
        ),
        entry_quote=entry, quotes=tuple(sorted(quotes, key=lambda q: q.at_ms)), bars=bars,
        additions=tuple(opportunities),
        funding=tuple(FundingSettlement(at, at + 1, rate, mark) for at, rate, mark in settlements),
        funding_coverage=coverage, horizon_end_ms=FUNDING_HORIZON,
        asof_ms=FUNDING_HORIZON + 1,
    )


def _by_arm(path: ParentPath) -> dict[str, Any]:
    return {row.arm: row for row in compare_parent_paths((path,))}


TWO_ADDS = (
    ("a1", "add1", BASE + 100, 105.0, 0.2),
    ("a2", "add2", BASE + 300, 111.0, 0.3),
)


def test_f5_stop_exit_before_settlement_has_verified_zero_funding_for_every_arm() -> None:
    """Regression: the old code added the same cash after the simulated exit."""

    path = _funding_path(
        additions=TWO_ADDS[:1], stop_bar_close=BASE + 250, exit_at=BASE + 300,
        settlements=((BASE + 1_000, 0.0005, 100.0),),
    )
    rows = _by_arm(path)
    assert {row.accepted_additions for row in rows.values()} >= {0, 1}
    for row in rows.values():
        assert row.funding_status == "VERIFIED"
        assert row.funding_usdt == 0.0  # verified none, not an unavailable placeholder
        assert row.net_pnl_usdt == pytest.approx(row.gross_pnl_usdt - row.costs_usdt)


def test_f5_unequal_quantities_receive_unequal_funding() -> None:
    path = _funding_path(
        additions=TWO_ADDS, settlements=((BASE + 500, 0.0002, 100.0),),
    )
    rows = _by_arm(path)
    quantities = {name: row.filled_quantity for name, row in rows.items()}
    assert len(set(quantities.values())) > 1
    for row in rows.values():
        assert row.funding_usdt == pytest.approx(row.filled_quantity * 100.0 * 0.0002)
    assert rows["first_fill_only"].funding_usdt == pytest.approx(1.0 * 100.0 * 0.0002)
    assert rows["first_fill_only"].funding_usdt != rows["primary_two_add"].funding_usdt


def test_f5_addition_after_settlement_is_not_exposed_to_it() -> None:
    # settlement between the two additions: only initial + the first add are open
    path = _funding_path(additions=TWO_ADDS, settlements=((BASE + 200, 0.001, 50.0),))
    primary = _by_arm(path)["primary_two_add"]
    assert primary.filled_quantity == pytest.approx(1.5)
    assert primary.funding_usdt == pytest.approx((1.0 + 0.2) * 50.0 * 0.001)
    # and a settlement after both additions sees the full position
    later = _by_arm(_funding_path(additions=TWO_ADDS, settlements=((BASE + 400, 0.001, 50.0),)))
    assert later["primary_two_add"].funding_usdt == pytest.approx(1.5 * 50.0 * 0.001)


def test_f5_multiple_settlements_use_each_instants_own_rate_and_valuation() -> None:
    path = _funding_path(
        additions=TWO_ADDS,
        settlements=((BASE + 50, 0.0001, 100.0), (BASE + 200, -0.0003, 110.0),
                     (BASE + 600, 0.0002, 120.0)),
    )
    primary = _by_arm(path)["primary_two_add"]
    expected = (1.0 * 100.0 * 0.0001) + (1.2 * 110.0 * -0.0003) + (1.5 * 120.0 * 0.0002)
    assert primary.funding_usdt == pytest.approx(expected)


def test_f5_side_and_sign_conventions() -> None:
    settlements = ((BASE + 500, 0.001, 100.0),)
    short = _by_arm(_funding_path(side=Side.SHORT, settlements=settlements))
    long = _by_arm(_funding_path(side=Side.LONG, settlements=settlements))
    assert short["first_fill_only"].funding_usdt == pytest.approx(+0.1)  # shorts receive
    assert long["first_fill_only"].funding_usdt == pytest.approx(-0.1)  # longs pay
    negative = ((BASE + 500, -0.001, 100.0),)
    assert _by_arm(_funding_path(side=Side.SHORT, settlements=negative))[
        "first_fill_only"].funding_usdt == pytest.approx(-0.1)
    assert _by_arm(_funding_path(side=Side.LONG, settlements=negative))[
        "first_fill_only"].funding_usdt == pytest.approx(+0.1)


@pytest.mark.parametrize(("settled_at", "exposed"), [
    (BASE, False),            # entry fill at the same instant: not yet exposed
    (BASE + 1, True),         # one millisecond after entry
    (FUNDING_HORIZON - 1, True),
    (FUNDING_HORIZON, True),  # exit at exactly the settlement still settles
])
def test_f5_entry_and_exit_equality_boundaries(settled_at: int, exposed: bool) -> None:
    path = _funding_path(settlements=((settled_at, 0.001, 100.0),))
    row = _by_arm(path)["first_fill_only"]
    assert row.funding_status == "VERIFIED"
    assert row.funding_usdt == pytest.approx(0.1 if exposed else 0.0)


def test_f5_settlement_after_exit_by_one_millisecond_is_excluded() -> None:
    path = _funding_path(
        exit_at=BASE + 600, stop_bar_close=BASE + 550,
        settlements=((BASE + 601, 0.001, 100.0),),
    )
    row = _by_arm(path)["first_fill_only"]
    assert row.funding_usdt == 0.0 and row.funding_status == "VERIFIED"
    at_exit = _funding_path(
        exit_at=BASE + 600, stop_bar_close=BASE + 550,
        settlements=((BASE + 600, 0.001, 100.0),),
    )
    assert _by_arm(at_exit)["first_fill_only"].funding_usdt == pytest.approx(0.1)


@pytest.mark.parametrize(("add_at", "counted"), [(BASE + 499, True), (BASE + 500, False),
                                                  (BASE + 501, False)])
def test_f5_addition_fill_boundary_at_the_settlement_instant(add_at: int, counted: bool) -> None:
    path = _funding_path(
        additions=(("edge", "add1", add_at, 105.0, 0.2),),
        settlements=((BASE + 500, 0.001, 100.0),),
    )
    row = _by_arm(path)["primary_two_add"]
    assert row.accepted_additions == 1
    assert row.funding_usdt == pytest.approx((1.2 if counted else 1.0) * 100.0 * 0.001)


def test_f5_no_scheduled_settlement_is_verified_zero_not_missing() -> None:
    row = _by_arm(_funding_path(settlements=()))["primary_two_add"]
    assert row.funding_usdt == 0.0 and row.funding_status == "VERIFIED"
    assert row.net_pnl_usdt is not None and row.status == "COMPLETE"


def test_f5_missing_coverage_makes_net_unavailable_not_zero() -> None:
    rows = _by_arm(_funding_path(
        coverage=None, settlements=(), scheduled=(),
    ))
    for row in rows.values():
        assert row.status == "COMPLETE_FUNDING_UNAVAILABLE"
        assert row.funding_status == "UNAVAILABLE_NO_VERIFIED_COVERAGE_FOR_HOLDING_WINDOW"
        assert row.funding_usdt is None and row.net_pnl_usdt is None and row.loss_r is None
        assert row.gross_pnl_usdt is not None  # executable gross is still reported


@pytest.mark.parametrize(("start", "end"), [
    (BASE + 1, FUNDING_HORIZON),       # starts after the first fill
    (BASE, FUNDING_HORIZON - 1),       # ends before the simulated exit
])
def test_f5_coverage_must_span_the_arms_holding_window(start: int, end: int) -> None:
    path = _funding_path(coverage=FundingCoverage(start, end, ()))
    row = _by_arm(path)["first_fill_only"]
    assert row.funding_usdt is None and row.net_pnl_usdt is None
    assert row.funding_status.startswith("UNAVAILABLE_NO_VERIFIED_COVERAGE")


def test_f5_scheduled_settlement_without_rate_or_valuation_is_unavailable() -> None:
    path = _funding_path(settlements=(), scheduled=(BASE + 500,))
    row = _by_arm(path)["primary_two_add"]
    assert row.funding_status == "UNAVAILABLE_EXPOSED_SETTLEMENT_WITHOUT_RATE_AND_VALUATION"
    assert row.funding_usdt is None and row.net_pnl_usdt is None


def test_f5_missing_settlement_outside_an_arms_exposure_does_not_block_that_arm() -> None:
    # exits at BASE+300, so the unobserved settlement at BASE+700 never applied
    path = _funding_path(
        exit_at=BASE + 300, stop_bar_close=BASE + 250, settlements=(), scheduled=(BASE + 700,),
    )
    row = _by_arm(path)["first_fill_only"]
    assert row.funding_usdt == 0.0 and row.funding_status == "VERIFIED"


def test_f5_funding_inputs_are_validated() -> None:
    with pytest.raises(ValueError, match="rate and valuation"):
        FundingSettlement(BASE, BASE + 1, float("nan"), 100.0)
    with pytest.raises(ValueError, match="rate and valuation"):
        FundingSettlement(BASE, BASE + 1, 0.001, 0.0)
    with pytest.raises(ValueError, match="rate and valuation"):
        FundingSettlement(BASE, BASE + 1, 2.0, 100.0)
    with pytest.raises(ValueError, match="rate and valuation"):
        FundingSettlement(BASE, BASE - 1, 0.001, 100.0)
    with pytest.raises(ValueError, match="strictly ascending"):
        FundingCoverage(BASE, BASE + 10, (BASE + 5, BASE + 5))
    with pytest.raises(ValueError, match="outside the verified coverage"):
        FundingCoverage(BASE, BASE + 10, (BASE + 11,))
    with pytest.raises(ValueError, match="explicit verified coverage"):
        _funding_path(coverage=None, settlements=((BASE + 500, 0.001, 100.0),))
    with pytest.raises(ValueError, match="not in the verified schedule"):
        _funding_path(settlements=((BASE + 500, 0.001, 100.0),), scheduled=(BASE + 400,))
    with pytest.raises(ValueError, match="beyond the observed horizon"):
        _funding_path(coverage=FundingCoverage(BASE, FUNDING_HORIZON + 1, ()))
    with pytest.raises(ValueError, match="duplicate funding"):
        _funding_path(
            settlements=((BASE + 500, 0.001, 100.0), (BASE + 500, 0.002, 100.0)),
            scheduled=(BASE + 500,),
        )


def test_f5_unavailable_net_never_looks_complete_downstream() -> None:
    covered = tuple(_funding_path(parent_id=f"c{i}") for i in range(3))
    uncovered = _funding_path(parent_id="u0", coverage=None, settlements=(), scheduled=())
    summary = summarize_arm_results(compare_parent_paths((*covered, uncovered)))
    primary = summary["primary_two_add"]
    assert isinstance(primary, dict)
    assert primary["complete_executable_paths"] == 4
    assert primary["net_complete_paths"] == 3
    assert primary["funding_unavailable_paths"] == 1
    assert primary["net_inference_status"] == "UNAVAILABLE_FUNDING_COVERAGE_INCOMPLETE"
    assert primary["mean_net_pnl_usdt"] is None  # not the mean of the lucky subset
    complete = summarize_arm_results(compare_parent_paths(covered))["primary_two_add"]
    assert isinstance(complete, dict)
    assert complete["net_inference_status"] == "COMPLETE"
    assert complete["mean_net_pnl_usdt"] is not None


def test_f5_paired_inference_is_unavailable_when_any_paired_parent_lacks_funding() -> None:
    entry = ReplayQuote(BASE, BASE, 100.0, 100.1)
    exit_quote = ReplayQuote(FUNDING_HORIZON, FUNDING_HORIZON, 124.9, 125.0)

    def parent(index: int, covered: bool) -> ParentPath:
        return ParentPath(
            parent_id=f"pair-{index}", event_id=f"pair-event-{index}",
            event_at_ms=BASE - (20 - index % 20) * 86_400_000, decision_available_ms=BASE,
            exposure_class=ExposureClass.PUBLIC_SYNTHETIC,
            ladder=FrozenLadder(Side.SHORT, Fill(f"i{index}", "initial", 1.0, 100.0, BASE),
                                140.0, 50.0),
            entry_quote=entry, quotes=(entry, exit_quote), bars=(), additions=(),
            funding=(),
            funding_coverage=FundingCoverage(BASE, FUNDING_HORIZON, ()) if covered else None,
            horizon_end_ms=FUNDING_HORIZON, asof_ms=FUNDING_HORIZON + 1,
        )

    full = summarize_arm_results(compare_parent_paths(tuple(parent(i, True) for i in range(100))))
    comparison = full["paired_comparisons_vs_first_fill_only"]["primary_two_add"]  # type: ignore[index]
    assert comparison["net_noninferiority_status"] == "CLUSTERED_BOOTSTRAP_SUPPORTED"
    assert comparison["paired_funding_unavailable_n"] == 0

    partial = summarize_arm_results(compare_parent_paths(
        tuple(parent(i, i != 7) for i in range(100))))
    comparison = partial["paired_comparisons_vs_first_fill_only"]["primary_two_add"]  # type: ignore[index]
    assert comparison["paired_funding_unavailable_n"] == 1
    assert comparison["net_noninferiority_status"] == (
        "UNAVAILABLE_FUNDING_COVERAGE_INCOMPLETE_FOR_PAIRED_PARENTS"
    )
    assert comparison["net_noninferior"] is None and comparison["holm_adjusted_p"] is None
    assert comparison["q99_loss_reduction_fraction"] is None
    primary = partial["primary_two_add"]  # type: ignore[index]
    assert primary["q99_status"] == "UNAVAILABLE_FUNDING_COVERAGE_INCOMPLETE"  # type: ignore[index]


def test_stop_exit_waits_for_the_stop_bar_to_be_received() -> None:
    """A quote stamped between the bar's close and its receipt was not actionable yet."""

    entry = ReplayQuote(BASE, BASE, 100.0, 100.1)
    premature = ReplayQuote(BASE + 255, BASE + 255, 121.0, 121.1)  # before receipt at +260
    actionable = ReplayQuote(BASE + 300, BASE + 300, 125.0, 125.1)
    path = ParentPath(
        parent_id="causal-exit", event_id="event-causal-exit", event_at_ms=BASE - 1,
        decision_available_ms=BASE, exposure_class=ExposureClass.PUBLIC_SYNTHETIC,
        ladder=FrozenLadder(Side.SHORT, Fill("initial", "initial", 1.0, 100.0, BASE),
                            120.0, 50.0),
        entry_quote=entry,
        quotes=(entry, premature, actionable, ReplayQuote(BASE + 1_000, BASE + 1_000, 90, 90.1)),
        bars=(PathBar(BASE + 250, BASE + 260, 121.0, 95.0),), additions=(),
        funding=(), funding_coverage=FundingCoverage(BASE, BASE + 1_000, ()),
        horizon_end_ms=BASE + 1_000, asof_ms=BASE + 1_001,
    )
    row = {r.arm: r for r in compare_parent_paths((path,))}["first_fill_only"]
    # short exits at the ask: 125.1 (BASE+300), not the premature 121.1 (BASE+255)
    assert row.gross_pnl_usdt == pytest.approx(100.0 - 125.1)


def test_release_revision_names_a_separate_candidate_package(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    base = build_release_evidence(repo_root, tmp_path / "base")
    revised = build_release_evidence(repo_root, tmp_path / "revised", revision="-r1")
    assert base["executable_package_path"] == "pump_fade_v2d_package.zip"
    assert revised["executable_package_path"] == "pump_fade_v2d-r1_package.zip"
    assert revised["release_revision"] == "-r1" and base["release_revision"] == "initial"
    assert (tmp_path / "revised/pump_fade_v2d-r1_package.zip").is_file()
    assert not (tmp_path / "revised/pump_fade_v2d_package.zip").exists()
    # identical bytes in, identical identities out (deterministic archive)
    assert revised["executable_tree_sha256"] == base["executable_tree_sha256"]
    assert revised["executable_package_sha256"] == base["executable_package_sha256"]
    with pytest.raises(ValueError, match="release revision"):
        build_release_evidence(repo_root, tmp_path / "bad", revision="../escape")


def test_release_identities_are_line_ending_independent(tmp_path: Path) -> None:
    from signalbot.pump_fade_v2.release import _canonical_bytes, _sha256_text_file

    lf = tmp_path / "lf.txt"
    crlf = tmp_path / "crlf.txt"
    lf.write_bytes(b"a\nb\n")
    crlf.write_bytes(b"a\r\nb\r\n")
    assert _sha256_text_file(lf) == _sha256_text_file(crlf)
    assert _canonical_bytes(crlf) == b"a\nb\n"
    lone_cr = tmp_path / "cr.txt"
    lone_cr.write_bytes(b"a\rb\n")
    assert _canonical_bytes(lone_cr) == b"a\rb\n"  # only CRLF pairs are normalized
    repo_root = Path(__file__).resolve().parents[2]
    result = build_release_evidence(repo_root, tmp_path / "release")
    assert "CRLF normalized to LF" in result["hash_canonicalization"]
    with zipfile.ZipFile(tmp_path / "release/pump_fade_v2d_package.zip") as archive:
        assert all(b"\r\n" not in archive.read(name) for name in archive.namelist())
