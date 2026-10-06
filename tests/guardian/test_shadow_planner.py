from __future__ import annotations

import json
from collections.abc import Generator
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from position_guardian.context_client import (
    ClosedCandleCursor,
    ProtectionContextClient,
    ProtectionContextRejected,
    validate_protection_context,
)
from position_guardian.domain import (
    AdoptionCandidate,
    ManagedPositionIdentity,
    ProtectiveOrderReference,
)
from position_guardian.persistence.repository import GuardianRepository
from position_guardian.planner import ShadowPlanningError, plan_shadow_stop
from position_guardian.runtime import (
    ShadowPlanningRequest,
    managed_position_ref,
    plan_shadow_once,
)
from signalbot.domain.enums import Direction, Market
from signalbot.signals.position_management import ManagedPositionSnapshot
from signalbot.signals.protection_context import ProtectionContext

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "protection_context"


def _payload(name: str = "protection_context_v1.json") -> dict[str, object]:
    value = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _context_payload(**updates: object) -> dict[str, object]:
    payload = _payload()
    payload.update(updates)
    payload["context_id"] = ProtectionContext.deterministic_context_id(
        {key: value for key, value in payload.items() if key != "context_id"}
    )
    return payload


def _identity(*, side: str = "LONG") -> ManagedPositionIdentity:
    return ManagedPositionIdentity(
        account_alias="manual-main",
        symbol="BTCUSDT",
        position_side=side,  # type: ignore[arg-type]
        adoption_generation=1,
    )


def _candidate() -> AdoptionCandidate:
    return AdoptionCandidate(
        identity=_identity(),
        quantity=Decimal("0.01"),
        entry_price=Decimal("40000"),
        mark_price=Decimal("42000"),
        source_update_time_ms=1700000000000,
        original_risk_stop=Decimal("39000"),
        protection_floor=Decimal("39000"),
        protection_source="exchange_stop",
        protective_order=ProtectiveOrderReference(
            source="open_order",
            order_id=11,
            trigger_price=Decimal("39000"),
        ),
    )


def _snapshot(**updates: object) -> ManagedPositionSnapshot:
    values: dict[str, object] = {
        "position_ref": managed_position_ref(_identity()),
        "market": Market.FUTURES,
        "symbol": "BTCUSDT",
        "direction": Direction.LONG,
        "entry_price": 40000.0,
        "initial_stop": 39000.0,
        "original_risk_stop": 39000.0,
        "protection_floor": 39000.0,
        "active_stop": 39000.0,
        "reference_price": 42000.0,
        "highest_price": 42400.0,
        "lowest_price": 39900.0,
        "observed_at_ms": 1700000299999,
    }
    values.update(updates)
    return ManagedPositionSnapshot(**values)  # type: ignore[arg-type]


@pytest.fixture
def repository() -> Generator[GuardianRepository, None, None]:
    with GuardianRepository("sqlite://") as repository:
        repository.record_adoption(
            event_id="adopt-1",
            event_time_ms=1700000000000,
            created_at_ms=1700000000001,
            candidate=_candidate(),
        )
        yield repository


def _validate(
    payload: dict[str, object] | None = None,
    *,
    now_ms: int = 1700000301000,
    max_age_ms: int = 5000,
    previous_cursor: str | None = None,
):
    return validate_protection_context(
        payload or _payload(),
        expected_market=Market.FUTURES,
        expected_symbol="BTCUSDT",
        expected_primary_interval="5m",
        now_ms=now_ms,
        max_age_ms=max_age_ms,
        previous_cursor=previous_cursor,
    )


def _request(
    *,
    payload: dict[str, object] | None = None,
    snapshot: ManagedPositionSnapshot | None = None,
    now_ms: int = 1700000301000,
    max_age_ms: int = 5000,
) -> ShadowPlanningRequest:
    return ShadowPlanningRequest(
        identity=_identity(),
        snapshot=snapshot or _snapshot(),
        context_payload=payload or _payload(),
        now_ms=now_ms,
        max_context_age_ms=max_age_ms,
    )


def _fixture_close_time_ms() -> int:
    value = _payload()["candle_close_time_ms"]
    assert isinstance(value, int) and not isinstance(value, bool)
    return value


def test_supported_major_fresh_closed_context_is_accepted() -> None:
    validated = _validate()

    assert validated.context.context_version == "protection-context-v1"
    assert validated.context.source_candle_closed is True
    assert validated.age_ms == 1001


@pytest.mark.parametrize(
    "fixture_name",
    ["protection_context_v0.json", "protection_context_v2.json"],
)
def test_unsupported_major_versions_fail_before_policy(fixture_name: str) -> None:
    with pytest.raises(ProtectionContextRejected) as exc_info:
        _validate(_payload(fixture_name))

    assert exc_info.value.reason == "UNSUPPORTED_MAJOR_VERSION"


def test_exact_freshness_boundary_is_accepted_but_one_ms_stale_is_rejected() -> None:
    close_time = _fixture_close_time_ms()
    assert _validate(now_ms=close_time + 5000, max_age_ms=5000).age_ms == 5000

    with pytest.raises(ProtectionContextRejected) as exc_info:
        _validate(now_ms=close_time + 5001, max_age_ms=5000)

    assert exc_info.value.reason == "STALE_CONTEXT"


def test_source_freshness_field_is_also_fail_closed() -> None:
    payload = _context_payload(context_freshness_ms=5001)

    with pytest.raises(ProtectionContextRejected) as exc_info:
        _validate(payload, max_age_ms=5000)

    assert exc_info.value.reason == "STALE_CONTEXT"


def test_open_or_current_candle_is_rejected() -> None:
    open_payload = _context_payload(source_candle_closed=False)
    with pytest.raises(ProtectionContextRejected) as open_exc:
        _validate(open_payload)
    assert open_exc.value.reason == "NOT_CLOSED_CANDLE"

    close_time = _fixture_close_time_ms()
    with pytest.raises(ProtectionContextRejected) as current_exc:
        _validate(now_ms=close_time)
    assert current_exc.value.reason == "FUTURE_OR_UNCLOSED_CURSOR"

    with pytest.raises(ProtectionContextRejected) as future_exc:
        _validate(now_ms=close_time - 1)
    assert future_exc.value.reason == "FUTURE_OR_UNCLOSED_CURSOR"


def test_symbol_mismatch_fails_closed() -> None:
    with pytest.raises(ProtectionContextRejected) as exc_info:
        validate_protection_context(
            _payload(),
            expected_market=Market.FUTURES,
            expected_symbol="ETHUSDT",
            expected_primary_interval="5m",
            now_ms=1700000301000,
            max_age_ms=5000,
        )

    assert exc_info.value.reason == "SYMBOL_MISMATCH"


@pytest.mark.parametrize("field", ["atr", "candle_close_time_ms"])
def test_missing_required_context_field_fails_closed(field: str) -> None:
    payload = _payload()
    payload.pop(field)

    with pytest.raises(ProtectionContextRejected) as exc_info:
        _validate(payload)

    assert exc_info.value.reason == "MALFORMED_CONTEXT"


def test_tampered_deterministic_context_id_fails_closed() -> None:
    payload = _payload()
    payload["context_id"] = "0" * 64

    with pytest.raises(ProtectionContextRejected) as exc_info:
        _validate(payload)

    assert exc_info.value.reason == "MALFORMED_CONTEXT"


def test_closed_candle_cursor_duplicate_is_allowed_but_regression_and_conflict_fail() -> None:
    validated = _validate()
    serialized = validated.cursor.serialize()
    assert _validate(previous_cursor=serialized).cursor == validated.cursor

    future_cursor = ClosedCandleCursor(
        candle_close_time_ms=validated.cursor.candle_close_time_ms + 300_000,
        context_id="future-context",
    ).serialize()
    with pytest.raises(ProtectionContextRejected) as regression_exc:
        _validate(previous_cursor=future_cursor)
    assert regression_exc.value.reason == "CURSOR_REGRESSION"

    conflicting = ClosedCandleCursor(
        candle_close_time_ms=validated.cursor.candle_close_time_ms,
        context_id="different-context",
    ).serialize()
    with pytest.raises(ProtectionContextRejected) as conflict_exc:
        _validate(previous_cursor=conflicting)
    assert conflict_exc.value.reason == "CURSOR_CONFLICT"


def test_read_only_context_client_uses_get_and_fails_closed_on_stale_status() -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.url.params.get("symbol") == "STALEUSDT":
            return httpx.Response(200, json={"status": "STALE_CONTEXT", "context": _payload()})
        return httpx.Response(200, json={"status": "OK", "context": _payload()})

    with httpx.Client(
        transport=httpx.MockTransport(handler),
        base_url="http://scanner.test",
    ) as client:
        context_client = ProtectionContextClient(client)
        assert context_client.fetch_latest_payload(
            market=Market.FUTURES,
            symbol="BTCUSDT",
            primary_interval="5m",
            max_age_ms=5000,
        )["context_id"] == _payload()["context_id"]
        with pytest.raises(ProtectionContextRejected) as exc_info:
            context_client.fetch_latest_payload(
                market=Market.FUTURES,
                symbol="STALEUSDT",
                primary_interval="5m",
                max_age_ms=5000,
            )

    assert exc_info.value.reason == "STALE_CONTEXT"
    assert methods == ["GET", "GET"]


def test_pure_planner_is_deterministic_for_same_snapshot_and_context() -> None:
    validated = _validate()

    first = plan_shadow_stop(_snapshot(), validated)
    second = plan_shadow_stop(_snapshot(), validated)

    assert first == second
    assert first.intent is not None
    assert second.intent is not None
    assert first.intent.intent_id == second.intent.intent_id
    assert first.intent.order_placed is False
    assert first.intent.effective_from_next_candle is True


def test_changed_closed_candle_snapshot_changes_logical_intent_id() -> None:
    first = plan_shadow_stop(_snapshot(), _validate())
    shifted_payload = _context_payload(
        candle_open_time_ms=1700000300000,
        candle_close_time_ms=1700000599999,
        source_decision_clock_id="futures:BTCUSDT:5m:1700000599999",
        close=42300.0,
        atr=200.0,
    )
    shifted = _validate(shifted_payload, now_ms=1700000601000)
    second = plan_shadow_stop(
        _snapshot(
            reference_price=42300.0,
                highest_price=42600.0,
            observed_at_ms=1700000599999,
        ),
        shifted,
    )

    assert first.intent is not None and second.intent is not None
    assert first.intent.intent_id != second.intent.intent_id


def test_snapshot_and_context_must_share_closed_candle_cursor() -> None:
    with pytest.raises(ShadowPlanningError, match="same closed-candle cursor"):
        plan_shadow_stop(_snapshot(observed_at_ms=1700000299998), _validate())


def test_policy_no_update_produces_explicit_no_intent() -> None:
    plan = plan_shadow_stop(
        _snapshot(highest_price=40500.0, reference_price=40500.0),
        _validate(),
    )

    assert plan.disposition == "NO_STOP_UPDATE"
    assert plan.intent is None


def test_runtime_persists_intent_and_cursor_idempotently(repository: GuardianRepository) -> None:
    request = _request()

    first = plan_shadow_once(repository, request)
    second = plan_shadow_once(repository, request)

    assert first.disposition == second.disposition == "STOP_UPDATE_INTENT"
    assert first.intent is not None and second.intent is not None
    assert first.intent.intent_id == second.intent.intent_id
    assert first.intent_event_inserted is True
    assert first.cursor_event_inserted is True
    assert second.intent_event_inserted is False
    assert second.cursor_event_inserted is False
    assert first.exchange_write_calls == second.exchange_write_calls == 0
    events = repository.list_events(identity=_identity())
    assert [event.event_type for event in events] == ["ADOPTION", "PLANNED_INTENT"]
    assert len(repository.list_events()) == 3


def test_restart_replay_with_same_database_does_not_duplicate_intent(tmp_path: Path) -> None:
    database = tmp_path / "guardian.db"
    database_url = f"sqlite:///{database.as_posix()}"
    with GuardianRepository(database_url) as first_repository:
        first_repository.record_adoption(
            event_id="adopt-1",
            event_time_ms=1700000000000,
            created_at_ms=1700000000001,
            candidate=_candidate(),
        )
        first = plan_shadow_once(first_repository, _request())
        assert first.intent_event_inserted is True

    with GuardianRepository(database_url) as restarted_repository:
        second = plan_shadow_once(restarted_repository, _request())
        assert second.intent_event_inserted is False
        assert second.cursor_event_inserted is False
        assert second.intent is not None and first.intent is not None
        assert second.intent.intent_id == first.intent.intent_id
        assert len(restarted_repository.list_events(identity=_identity())) == 2


def test_stale_context_persists_restart_authoritative_alert_source_only(
    repository: GuardianRepository,
) -> None:
    result = plan_shadow_once(
        repository,
        _request(now_ms=1700000305000, max_age_ms=4000),
    )

    assert result.disposition == "CONTEXT_REJECTED"
    assert result.reason == "STALE_CONTEXT"
    assert result.intent is None
    assert result.intent_event_inserted is False
    assert result.cursor_event_inserted is False
    assert result.exchange_write_calls == 0
    assert [event.event_type for event in repository.list_events()] == [
        "ADOPTION",
        "SHADOW_ALERT_SOURCE",
    ]
    outbox = repository.list_guardian_alert_outbox()
    assert len(outbox) == 1
    assert outbox[0].status == "disabled"
    assert '"code":"STALE_CONTEXT"' in outbox[0].payload_json


def test_uncertain_snapshot_produces_no_intent_but_consumes_valid_context(
    repository: GuardianRepository,
) -> None:
    result = plan_shadow_once(
        repository,
        _request(snapshot=_snapshot(context_state="STALE_OR_UNCERTAIN")),
    )

    assert result.disposition == "NO_STOP_UPDATE"
    assert result.intent is None
    assert result.cursor_event_inserted is True
    assert result.exchange_write_calls == 0


def test_cursor_regression_after_newer_context_is_rejected_without_new_ledger_row(
    repository: GuardianRepository,
) -> None:
    newer_payload = _context_payload(
        candle_open_time_ms=1700000300000,
        candle_close_time_ms=1700000599999,
        source_decision_clock_id="futures:BTCUSDT:5m:1700000599999",
        close=42300.0,
        atr=200.0,
    )
    newer_request = _request(
        payload=newer_payload,
        snapshot=_snapshot(
            reference_price=42300.0,
            highest_price=42600.0,
            observed_at_ms=1700000599999,
        ),
        now_ms=1700000601000,
    )
    assert plan_shadow_once(repository, newer_request).disposition == "STOP_UPDATE_INTENT"
    event_count = len(repository.list_events())

    regressed = plan_shadow_once(repository, _request(now_ms=1700000601000, max_age_ms=400000))

    assert regressed.disposition == "CONTEXT_REJECTED"
    assert regressed.reason == "CURSOR_REGRESSION"
    assert len(repository.list_events()) == event_count


def test_runtime_rejects_wrong_managed_identity_before_context_or_policy(
    repository: GuardianRepository,
) -> None:
    request = ShadowPlanningRequest(
        identity=_identity(side="SHORT"),
        snapshot=_snapshot(),
        context_payload=_payload(),
        now_ms=1700000301000,
        max_context_age_ms=5000,
    )

    result = plan_shadow_once(repository, request)

    assert result.disposition == "SNAPSHOT_REJECTED"
    assert result.reason == "POSITION_NOT_MANAGED"
    assert result.exchange_write_calls == 0


def test_runtime_rejects_position_ref_from_another_account_or_generation(
    repository: GuardianRepository,
) -> None:
    result = plan_shadow_once(
        repository,
        _request(snapshot=_snapshot(position_ref="other-account:BTCUSDT:LONG:99")),
    )

    assert result.disposition == "SNAPSHOT_REJECTED"
    assert result.reason == "MANAGED_IDENTITY_POSITION_REF_MISMATCH"
    assert result.intent is None
    assert len(repository.list_events()) == 1


def test_persisted_uncertain_context_cursor_blocks_shadow_planning(
    repository: GuardianRepository,
) -> None:
    validated = _validate()
    cursor_name = "protection-context:futures:BTCUSDT:5m"
    assert repository.record_reconciliation_cursor(
        event_id="degraded-context-cursor",
        event_time_ms=validated.context.candle_close_time_ms,
        created_at_ms=1700000300000,
        cursor_name=cursor_name,
        cursor_value=validated.cursor.serialize(),
        uncertainty_state="DEGRADED",
        detail="uncertain replay boundary",
    )

    result = plan_shadow_once(repository, _request())

    assert result.disposition == "CONTEXT_REJECTED"
    assert result.reason == "CURSOR_UNCERTAIN"
    assert result.intent is None
    assert result.intent_event_inserted is False
    assert result.cursor_event_inserted is False
    assert result.exchange_write_calls == 0
