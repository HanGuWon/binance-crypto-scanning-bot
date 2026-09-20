from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from position_guardian.domain import ManagedPositionIdentity
from position_guardian.exchange.binance_user_stream import (
    BoundedReconnectBackoff,
    BoundedUserEventBuffer,
    UserStreamPayloadError,
    build_user_stream_url,
    parse_user_stream_event,
)
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.reconcile import ReconciliationRequest, apply_stream_health

FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "binance_user_stream"


def _fixture(name: str) -> str:
    return (FIXTURE_ROOT / name).read_text(encoding="utf-8")


def test_production_user_stream_url_escapes_listen_key() -> None:
    assert build_user_stream_url(" synthetic/key ") == (
        "wss://fstream.binance.com/private/ws/synthetic%2Fkey"
    )


def test_recorded_account_and_order_events_parse_with_ordering_fields() -> None:
    account = parse_user_stream_event(_fixture("account_update.json"))
    order = parse_user_stream_event(_fixture("order_trade_update.json"))

    assert account.event_type == "ACCOUNT_UPDATE"
    assert account.event_time_ms == 1700000001000
    assert account.transaction_time_ms == 1700000000999
    assert order.event_type == "ORDER_TRADE_UPDATE"
    assert order.ordering_key == (1700000001999, 1700000002000)
    assert account.event_id != order.event_id


def test_malformed_and_unsupported_payloads_fail_closed() -> None:
    with pytest.raises(UserStreamPayloadError):
        parse_user_stream_event("not-json")
    with pytest.raises(UserStreamPayloadError):
        parse_user_stream_event(json.dumps({"e": "UNKNOWN", "E": 1}))


def test_duplicate_and_out_of_order_events_degrade_buffer() -> None:
    buffer = BoundedUserEventBuffer(max_queue_size=4, max_dedupe_size=4)
    first = parse_user_stream_event(_fixture("account_update.json"))
    duplicate = buffer.ingest(first)
    replay = buffer.ingest(first)
    older_payload = json.loads(_fixture("account_update.json"))
    older_payload["E"] = 1699999999000
    older_payload["T"] = 1699999998999
    older = buffer.ingest(parse_user_stream_event(json.dumps(older_payload)))

    assert duplicate.status == "ACCEPTED"
    assert replay.status == "DUPLICATE"
    assert older.status == "OUT_OF_ORDER"
    assert buffer.health == "DEGRADED"
    assert buffer.rest_resync_required


def test_listen_key_expiry_requires_resync() -> None:
    buffer = BoundedUserEventBuffer()
    result = buffer.ingest_raw(_fixture("listen_key_expired.json"))

    assert result.status == "ACCEPTED"
    assert buffer.health == "EXPIRED"
    assert buffer.rest_resync_required
    assert len(buffer.drain()) == 1
    buffer.mark_rest_resync_complete()
    assert buffer.health == "HEALTHY"


def test_queue_overflow_is_bounded_and_degraded() -> None:
    buffer = BoundedUserEventBuffer(max_queue_size=1, max_dedupe_size=1)
    first = parse_user_stream_event(_fixture("account_update.json"))
    second_payload = json.loads(_fixture("order_trade_update.json"))
    second = parse_user_stream_event(json.dumps(second_payload))

    assert buffer.ingest(first).status == "ACCEPTED"
    overflow = buffer.ingest(second)
    assert overflow.status == "OVERFLOW"
    assert buffer.pending_count == 1
    assert buffer.dedupe_count == 1
    assert len(buffer.drain()) == 1
    assert buffer.health == "DEGRADED"


def test_disconnect_is_visible_and_requires_rest_resync() -> None:
    buffer = BoundedUserEventBuffer()

    buffer.mark_disconnected()

    assert buffer.health == "DISCONNECTED"
    assert buffer.rest_resync_required


def test_dedupe_cache_and_queue_have_finite_limits() -> None:
    buffer = BoundedUserEventBuffer(max_queue_size=3, max_dedupe_size=1)
    payload = json.loads(_fixture("account_update.json"))
    for event_time in (1000, 2000, 3000):
        payload["E"] = event_time
        payload["T"] = event_time
        assert buffer.ingest(parse_user_stream_event(json.dumps(payload))).status == "ACCEPTED"

    assert len(buffer.drain()) == 3
    buffer.mark_rest_resync_complete()
    payload["E"] = 1000
    payload["T"] = 1000
    assert buffer.ingest(parse_user_stream_event(json.dumps(payload))).status == "ACCEPTED"


def test_reconnect_backoff_is_bounded_and_resettable() -> None:
    backoff = BoundedReconnectBackoff((0.0, 0.25))

    assert backoff.next_delay() == 0.0
    assert backoff.next_delay() == 0.25
    assert backoff.next_delay() is None
    backoff.reset()
    assert backoff.next_delay() == 0.0


def test_stream_health_drives_existing_reconciliation_uncertainty_gate() -> None:
    request = ReconciliationRequest(
        identity=ManagedPositionIdentity("manual-main", "BTCUSDT", "LONG", 1),
        position=PositionSnapshot(
            symbol="BTCUSDT",
            position_side="BOTH",
            position_amount=Decimal("0.01"),
            entry_price=Decimal("60000"),
            mark_price=Decimal("61000"),
            unrealized_profit=Decimal("10"),
            update_time_ms=1,
        ),
        snapshot_event_id="snapshot",
        event_time_ms=1,
        created_at_ms=2,
        protective_order_confirmed=True,
    )

    degraded = apply_stream_health(request, "DISCONNECTED")
    healthy = apply_stream_health(degraded, "HEALTHY")
    assert degraded.uncertainty_state == "DEGRADED"
    assert healthy.uncertainty_state == "CERTAIN"
