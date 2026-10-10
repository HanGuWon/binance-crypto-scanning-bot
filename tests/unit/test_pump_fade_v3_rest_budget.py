from __future__ import annotations

import pytest

from signalbot.pump_fade_v3.rest_budget import RestCircuitOpen, RestWeightBudget


def test_rolling_admission_and_window_expiry() -> None:
    budget = RestWeightBudget(local_limit=2)
    assert budget.admit(weight=1, now=0)
    assert budget.admit(weight=1, now=0)
    assert not budget.admit(weight=1, now=59.99)
    assert budget.admit(weight=1, now=60)


def test_shared_ip_header_blocks_requests() -> None:
    budget = RestWeightBudget(shared_limit=2400)
    budget.observe(status=200, headers={"X-MBX-USED-WEIGHT-1M": "2399"}, now=10)
    assert budget.admit(weight=1, now=10)
    assert not budget.admit(weight=1, now=11)


def test_ban_and_bounded_retry() -> None:
    budget = RestWeightBudget()
    assert budget.retry(status=429, attempt=1, headers={"Retry-After": "999"}).delay_seconds == 120
    assert budget.retry(status=503, attempt=2, headers={}).delay_seconds == 2
    assert not budget.retry(status=None, attempt=3, headers={}).retry
    with pytest.raises(RestCircuitOpen):
        budget.observe(status=418, headers={}, now=0)
        budget.admit(weight=1, now=0)


def test_third_429_opens_permanent_circuit() -> None:
    budget = RestWeightBudget()
    for attempt, expected in ((1, 120.0), (2, 1.0)):
        decision = budget.retry(
            status=429, attempt=attempt,
            headers={"Retry-After": "999" if attempt == 1 else "0"},
        )
        assert decision.retry and decision.delay_seconds == expected
        assert not budget.banned
    with pytest.raises(RestCircuitOpen, match="429 retry exhaustion"):
        budget.retry(status=429, attempt=3, headers={})
    assert budget.banned
    with pytest.raises(RestCircuitOpen):
        budget.admit(weight=1, now=0)


def test_minute_summary_logs_request_count_weight_and_last_header(
    caplog: pytest.LogCaptureFixture,
) -> None:
    budget = RestWeightBudget()
    budget.record_request(weight=10, now_ms=1_000)
    budget.record_request(weight=1, now_ms=2_000)
    budget.observe(
        status=200,
        headers={"X-MBX-USED-WEIGHT-1M": "27"},
        now=2,
    )

    with caplog.at_level("INFO", logger="signalbot.pump_fade_v3.rest_budget"):
        budget.flush_minute(now_ms=60_000)

    assert "requests=2 weight=11 last_x_mbx_used_weight_1m=27" in caplog.text
