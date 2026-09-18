from __future__ import annotations

import asyncio
from decimal import Decimal

import httpx
import pytest
from pydantic import ValidationError

from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.data.funding import (
    FundingRateCapacityError,
    FundingRatePayloadError,
    FundingRatePoint,
    FundingRateTracker,
)
from signalbot.domain.enums import Market
from signalbot.exchange.binance.rest import BinanceRestClient
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime
from signalbot.scanner import MarketScanner


def _payload(time_ms: int, rate: str, symbol: str = "BTCUSDT") -> dict[str, object]:
    return {"symbol": symbol, "fundingTime": time_ms, "fundingRate": rate}


def test_point_in_time_snapshot_ignores_future_and_uses_strict_prior_sample() -> None:
    tracker = FundingRateTracker(maximum_points=4, minimum_history=2, maximum_symbols=1)
    tracker.ingest_payloads(
        "BTCUSDT",
        [
            _payload(100, "0.001"),
            _payload(200, "0.002"),
            _payload(400, "0.100"),
            _payload(300, "0.004"),
        ],
    )

    snapshot = tracker.snapshot("btcusdt", as_of_ms=301, maximum_age_ms=1)

    assert snapshot is not None
    assert snapshot.funding_time_ms == 300
    assert snapshot.rate == pytest.approx(0.004)
    assert snapshot.zscore == pytest.approx(5.0)
    assert snapshot.prior_sample_size == 2


def test_funding_freshness_boundary_and_missing_history_fail_closed() -> None:
    tracker = FundingRateTracker(maximum_points=3, minimum_history=2, maximum_symbols=1)
    tracker.ingest_payloads("BTCUSDT", [_payload(100, "0.001"), _payload(200, "0.002")])
    assert tracker.snapshot("BTCUSDT", 201, 101) is None

    tracker.ingest_payloads("BTCUSDT", [_payload(300, "0.003")])
    assert tracker.snapshot("BTCUSDT", 350, 50) is not None
    assert tracker.snapshot("BTCUSDT", 351, 50) is None


def test_funding_zscore_lookback_is_a_time_window_with_inclusive_boundary() -> None:
    inside = FundingRateTracker(
        maximum_points=4,
        minimum_history=2,
        maximum_symbols=1,
        lookback_ms=100,
    )
    inside.ingest_payloads(
        "BTCUSDT",
        [_payload(0, "0"), _payload(1, "2"), _payload(100, "3")],
    )
    snapshot = inside.snapshot("BTCUSDT", 101, 1)
    assert snapshot is not None and snapshot.zscore == pytest.approx(2.0)

    outside = FundingRateTracker(
        maximum_points=4,
        minimum_history=2,
        maximum_symbols=1,
        lookback_ms=100,
    )
    outside.ingest_payloads(
        "BTCUSDT",
        [_payload(0, "0"), _payload(1, "2"), _payload(101, "3")],
    )
    assert outside.snapshot("BTCUSDT", 102, 1) is None


def test_payload_ingest_is_atomic_and_caches_are_bounded() -> None:
    tracker = FundingRateTracker(maximum_points=3, minimum_history=2, maximum_symbols=1)
    with pytest.raises(FundingRatePayloadError, match="fundingRate"):
        tracker.ingest_payloads("BTCUSDT", [_payload(100, "0.001"), _payload(200, "not-a-number")])
    assert tracker.latest_time_ms("BTCUSDT") is None

    points = [
        FundingRatePoint("btcusdt", timestamp, Decimal(str(timestamp / 1_000_000)))
        for timestamp in (100, 200, 300, 400)
    ]
    assert all(tracker.update(point) for point in points)
    assert not tracker.update(points[-1])
    snapshot = tracker.snapshot("BTCUSDT", 401, 1)
    assert snapshot is not None
    assert snapshot.prior_sample_size == 2
    with pytest.raises(FundingRateCapacityError):
        tracker.update(FundingRatePoint("ETHUSDT", 400, Decimal("0.001")))


def test_funding_rotation_prunes_stale_history_and_releases_capacity() -> None:
    tracker = FundingRateTracker(maximum_points=3, minimum_history=2, maximum_symbols=1)
    tracker.ingest_payloads(
        "BTCUSDT",
        [_payload(100, "0.001"), _payload(200, "0.002"), _payload(300, "0.003")],
    )
    assert tracker.retain_symbols(frozenset({"ETHUSDT"})) == 1
    assert tracker.latest_time_ms("BTCUSDT") is None
    assert tracker.update(FundingRatePoint("ETHUSDT", 400, Decimal("0.001")))


def test_funding_configuration_rejects_insufficient_bounded_history() -> None:
    with pytest.raises(ValidationError, match="funding_history_points"):
        Settings.model_validate(
            {
                "binance": {"funding_history_points": 20},
                "signals": {"funding_zscore_minimum_history": 20},
            }
        )


@pytest.mark.asyncio
async def test_scanner_bootstraps_recent_funding_then_requests_only_new_timestamps() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            assert "startTime" not in request.url.params
            assert "endTime" not in request.url.params
            return httpx.Response(
                200,
                json=[
                    _payload(100, "0.001"),
                    _payload(200, "0.002"),
                    _payload(300, "0.003"),
                ],
            )
        assert request.url.params["startTime"] == "301"
        assert request.url.params["endTime"] == "400"
        return httpx.Response(200, json=[_payload(400, "0.004")])

    http = httpx.AsyncClient(
        base_url="https://fapi.binance.com", transport=httpx.MockTransport(handler)
    )
    rest = BinanceRestClient(Market.FUTURES, client=http)
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["futures"],
                "top_n": 1,
                "funding_history_points": 3,
                "funding_refresh_seconds": 30,
            },
            "signals": {
                "funding_zscore_minimum_history": 2,
                "funding_maximum_age_ms": 100,
            },
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repository = SqlRepository(settings.storage.url)
    repository.initialize()
    clock = ReplayClock(300)

    async def discard(_decision: object) -> None:
        return None

    runtime = MarketRuntime(Market.FUTURES, settings, repository, clock, discard)
    scanner = MarketScanner(
        Market.FUTURES,
        settings,
        clock,
        runtime,
        asyncio.Event(),
        rest_client=rest,
    )
    try:
        await scanner._refresh_funding(["BTCUSDT"], bootstrap=True)
        assert runtime.funding.snapshot("BTCUSDT", 301, 1) is not None

        clock.advance_to(400)
        await scanner._refresh_funding(["BTCUSDT"], bootstrap=False)
        latest = runtime.funding.snapshot("BTCUSDT", 401, 1)
        assert latest is not None
        assert latest.funding_time_ms == 400

        scanner.stop_event.set()
        await asyncio.wait_for(scanner._funding_refresh_loop(), timeout=0.1)
        assert len(requests) == 2
    finally:
        repository.close()
        await http.aclose()


def test_runtime_copies_only_fresh_funding_into_futures_features() -> None:
    from conftest import make_feature

    settings = Settings.model_validate(
        {
            "binance": {"top_n": 1, "funding_history_points": 3},
            "signals": {
                "funding_zscore_minimum_history": 2,
                "funding_maximum_age_ms": 50,
            },
            "storage": {"url": "sqlite:///:memory:"},
        }
    )
    repository = SqlRepository(settings.storage.url)
    repository.initialize()

    async def discard(_decision: object) -> None:
        return None

    runtime = MarketRuntime(Market.FUTURES, settings, repository, ReplayClock(), discard)
    runtime.funding.ingest_payloads(
        "TESTUSDT",
        [
            _payload(100, "0.001", "TESTUSDT"),
            _payload(200, "0.002", "TESTUSDT"),
            _payload(300, "0.004", "TESTUSDT"),
        ],
    )
    try:
        fresh = runtime._with_funding(make_feature(event_time_ms=350))
        stale = runtime._with_funding(make_feature(event_time_ms=351))
        assert fresh.funding_rate == pytest.approx(0.004)
        assert fresh.funding_zscore == pytest.approx(5.0)
        assert stale.funding_rate is None
        assert stale.funding_zscore is None
    finally:
        repository.close()
