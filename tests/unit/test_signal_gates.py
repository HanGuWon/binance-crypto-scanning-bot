import pytest

from conftest import make_feature
from signalbot.config import SignalSettings
from signalbot.domain.enums import Direction
from signalbot.signals.gates import evaluate_entry_gates, evaluate_strict_prior_htf


def _settings() -> SignalSettings:
    return SignalSettings(
        gate_enabled=True,
        gate_use_higher_timeframes=True,
        reversal_intervals=["1h", "4h"],
    )


def _feature(**updates: object):
    values: dict[str, object] = {
        "price": 103.0,
        "ema20": 102.0,
        "ema50": 100.0,
        "ema20_slope_atr": 0.5,
        "adx": 25.0,
        "volume_zscore": 1.0,
        "trade_count_zscore": 0.5,
        "taker_imbalance": 0.10,
        "cvd_pressure": 0.05,
        "funding_zscore": 0.0,
        "spread_bps": 11.25,
        "spread_is_proxy": True,
    }
    values.update(updates)
    return make_feature(**values)


def _contexts():
    return {
        "15m": _feature(interval="15m", event_time_ms=1_709_999_000_000),
        "1h": _feature(interval="1h", event_time_ms=1_709_996_000_000),
    }


def test_all_four_entry_gates_can_pass_with_proxy_execution_at_boundary() -> None:
    gate = evaluate_entry_gates(
        _feature(), Direction.LONG, _contexts(), _settings()
    )

    assert gate.passed
    assert gate.execution_score == _settings().execution_gate == 65
    assert gate.proxy_fields == ("spread_bps",)


def test_missing_strict_higher_timeframe_context_fails_closed() -> None:
    gate = evaluate_entry_gates(_feature(), Direction.LONG, {}, _settings())

    assert not gate.passed
    assert any("15m" in failure for failure in gate.failures)
    assert any("1h" in failure for failure in gate.failures)


def test_crowding_cap_is_exclusive_boundary() -> None:
    blocked = evaluate_entry_gates(
        _feature(funding_zscore=2.0), Direction.LONG, _contexts(), _settings()
    )
    allowed = evaluate_entry_gates(
        _feature(funding_zscore=1.999), Direction.LONG, _contexts(), _settings()
    )

    assert blocked.crowding_risk_score == _settings().crowding_risk_cap == 75
    assert not blocked.passed
    assert allowed.crowding_risk_score == 50
    assert allowed.passed


def test_missing_or_stale_execution_data_fails_closed() -> None:
    missing = evaluate_entry_gates(
        _feature(spread_bps=None, spread_is_proxy=False),
        Direction.LONG,
        _contexts(),
        _settings(),
    )
    too_wide = evaluate_entry_gates(
        _feature(spread_bps=15.0001, spread_is_proxy=False),
        Direction.LONG,
        _contexts(),
        _settings(),
    )

    assert not missing.passed
    assert missing.execution_score == 0
    assert not too_wide.passed
    assert too_wide.execution_score == 0


def test_missing_futures_funding_fails_closed_but_spot_is_not_applicable() -> None:
    futures = evaluate_entry_gates(
        _feature(funding_zscore=None), Direction.LONG, _contexts(), _settings()
    )
    spot = evaluate_entry_gates(
        _feature(market="spot", funding_zscore=None),
        Direction.LONG,
        _contexts(),
        _settings(),
    )

    assert not futures.passed
    assert futures.crowding_risk_score == 100
    assert any("missing futures funding" in failure for failure in futures.failures)
    assert spot.passed
    assert spot.crowding_risk_score == 0


def test_closed_kline_taker_delta_gate_uses_direction_and_fixed_boundary() -> None:
    settings = _settings().model_copy(
        update={"gate_use_crowding": False, "volume_feature_set": "kline_taker_delta"}
    )
    boundary = evaluate_entry_gates(
        _feature(taker_delta_3=0.10, taker_delta_12=0.0),
        Direction.LONG,
        _contexts(),
        settings,
    )
    below = evaluate_entry_gates(
        _feature(taker_delta_3=0.099999, taker_delta_12=0.0),
        Direction.LONG,
        _contexts(),
        settings,
    )
    short = evaluate_entry_gates(
        _feature(
            price=97.0,
            ema20=98.0,
            ema50=100.0,
            ema20_slope_atr=-0.5,
            taker_imbalance=-0.10,
            cvd_pressure=-0.05,
            taker_delta_3=-0.10,
            taker_delta_12=-0.01,
        ),
        Direction.SHORT,
        {
            "15m": _feature(
                interval="15m",
                event_time_ms=1_709_999_000_000,
                price=97.0,
                ema20=98.0,
                ema50=100.0,
            ),
            "1h": _feature(
                interval="1h",
                event_time_ms=1_709_996_000_000,
                price=97.0,
                ema20=98.0,
                ema50=100.0,
            ),
        },
        settings,
    )

    assert boundary.passed and boundary.volume_policy_score == 100
    assert not below.passed and below.volume_policy_score == 0
    assert short.passed


def test_normalized_vpci_gate_requires_all_directional_components() -> None:
    settings = _settings().model_copy(
        update={"gate_use_crowding": False, "volume_feature_set": "normalized_vpci"}
    )
    passing = evaluate_entry_gates(
        _feature(
            normalized_vpci=0.30,
            normalized_vpci_signal=0.20,
            normalized_vpci_slope_3=0.05,
        ),
        Direction.LONG,
        _contexts(),
        settings,
    )
    zero_slope = evaluate_entry_gates(
        _feature(
            normalized_vpci=0.30,
            normalized_vpci_signal=0.20,
            normalized_vpci_slope_3=0.0,
        ),
        Direction.LONG,
        _contexts(),
        settings,
    )
    missing = evaluate_entry_gates(
        _feature(normalized_vpci=None),
        Direction.LONG,
        _contexts(),
        settings,
    )

    assert passing.passed
    assert not zero_slope.passed
    assert not missing.passed


def test_participation_fails_when_canonical_closed_kline_flow_is_unavailable() -> None:
    gate = evaluate_entry_gates(
        _feature(closed_kline_flow_available=False),
        Direction.LONG,
        _contexts(),
        _settings(),
    )

    assert not gate.passed
    assert gate.participation_score == 0
    assert any("closed-kline flow unavailable" in item for item in gate.failures)


def test_strict_prior_htf_requires_both_noncompensating_contexts() -> None:
    accepted = evaluate_strict_prior_htf(
        _feature(),
        Direction.LONG,
        _contexts(),
    )
    missing = evaluate_strict_prior_htf(
        _feature(),
        Direction.LONG,
        {"15m": _contexts()["15m"]},
    )

    assert accepted.accepted and accepted.failures == ()
    assert not missing.accepted
    assert any("1h" in failure for failure in missing.failures)


def test_strict_prior_htf_rejects_equal_or_same_close_context() -> None:
    decision = _feature(event_time_ms=1_710_000_000_000)
    equality = evaluate_strict_prior_htf(
        decision,
        Direction.LONG,
        {
            **_contexts(),
            "15m": _feature(
                interval="15m",
                event_time_ms=1_709_999_000_000,
                price=102.0,
                ema20=102.0,
                ema50=100.0,
            ),
        },
    )
    same_close = evaluate_strict_prior_htf(
        decision,
        Direction.LONG,
        {
            **_contexts(),
            "1h": _feature(
                interval="1h",
                event_time_ms=decision.event_time_ms,
            ),
        },
    )

    assert not equality.accepted
    assert any("aligned" in failure for failure in equality.failures)
    assert not same_close.accepted
    assert any("strictly prior" in failure for failure in same_close.failures)


def test_strict_prior_htf_mirrors_the_short_direction() -> None:
    short_contexts = {
        interval: context.model_copy(
            update={"price": 97.0, "ema20": 98.0, "ema50": 100.0}
        )
        for interval, context in _contexts().items()
    }

    result = evaluate_strict_prior_htf(
        _feature(price=97.0, ema20=98.0, ema50=100.0),
        Direction.SHORT,
        short_contexts,
    )

    assert result.accepted


def test_r2_gate_requires_strict_htf_and_executable_observed_bbo() -> None:
    settings = SignalSettings(
        gate_enabled=True,
        entry_policy="r2_pit_htf_exec",
        gate_use_participation=True,
        gate_use_crowding=True,
        gate_use_higher_timeframes=True,
        execution_notional_usdt=100.0,
    )
    feature = _feature(
        market="spot",
        spread_bps=10.0,
        spread_is_proxy=False,
        book_age_ms=2_000,
        ask_quote_capacity=100.0,
        bid_quote_capacity=99.0,
        closed_kline_flow_available=False,
        funding_zscore=None,
    )
    accepted = evaluate_entry_gates(
        feature,
        Direction.LONG,
        _contexts(),
        settings,
    )
    insufficient = evaluate_entry_gates(
        feature.model_copy(update={"ask_quote_capacity": 99.999}),
        Direction.LONG,
        _contexts(),
        settings,
    )
    missing_htf = evaluate_entry_gates(feature, Direction.LONG, {}, settings)
    proxy = evaluate_entry_gates(
        feature.model_copy(update={"spread_is_proxy": True}),
        Direction.LONG,
        _contexts(),
        settings,
    )

    assert accepted.passed
    assert accepted.participation_score == 100
    assert accepted.crowding_risk_score == 0
    assert not insufficient.passed
    assert any("quote capacity" in item for item in insufficient.failures)
    assert not missing_htf.passed
    assert not proxy.passed


@pytest.mark.parametrize(
    ("book_age_ms", "expected_pass"),
    [
        pytest.param(None, False, id="missing"),
        pytest.param(-1, False, id="negative"),
        pytest.param(0, True, id="zero-boundary"),
        pytest.param(2_000, True, id="maximum-boundary"),
        pytest.param(2_001, False, id="above-maximum"),
    ],
)
def test_r2_gate_requires_bbo_age_within_closed_freshness_interval(
    book_age_ms: int | None,
    expected_pass: bool,
) -> None:
    settings = SignalSettings(
        gate_enabled=True,
        entry_policy="r2_pit_htf_exec",
        gate_use_higher_timeframes=True,
        execution_notional_usdt=100.0,
        book_maximum_age_ms=2_000,
    )
    feature = _feature(
        market="spot",
        spread_bps=10.0,
        spread_is_proxy=False,
        book_age_ms=book_age_ms,
        ask_quote_capacity=100.0,
        bid_quote_capacity=100.0,
        funding_zscore=None,
    )

    gate = evaluate_entry_gates(feature, Direction.LONG, _contexts(), settings)

    assert gate.passed is expected_pass
    assert (gate.execution_score > 0) is expected_pass
