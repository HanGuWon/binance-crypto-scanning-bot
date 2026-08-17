from __future__ import annotations

import copy
import pickle
from dataclasses import dataclass, fields, replace
from typing import Any, cast

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    build_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.rest_depth import (
    PublicDepthRestTerminalObservationV8,
    PublicDepthSnapshotTriggerV8,
    public_depth_rest_plan_sha256_v8,
    public_depth_rest_source_logical_key_v8,
)
from signalbot.r4b_v2.capture.rest_depth_scheduler import (
    PublicDepthRestRegisteredCycleV8,
    PublicDepthRestRegistrationDispositionV8,
    PublicDepthRestScheduleAuthorityV8,
    PublicDepthRestScheduledAttemptOwnershipErrorV8,
    PublicDepthRestScheduledAttemptTokenV8,
    acknowledge_public_depth_rest_terminal_admission_v8,
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8,
    consume_public_depth_rest_scheduled_attempt_token_v8,
    create_public_depth_rest_schedule_authority_v8,
    public_depth_rest_registration_disposition_v8,
    validate_public_depth_rest_registered_cycle_v8,
    validate_public_depth_rest_schedule_authority_v8,
    validate_public_depth_rest_scheduled_attempt_token_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicDepthRestAdmissionReceiptV8,
    SharedWebSocketIngressV2,
)

_MAX_SIGNED_INT64 = (1 << 63) - 1
_SESSION_ID = "depth-rest-schedule-session"
_PROTOCOL_HASH = "0" * 64
_BUILT_REQUEST_HEADERS = (
    ("accept", "application/json"),
    ("accept-encoding", "identity"),
    ("connection", "keep-alive"),
    ("host", "fapi.binance.com"),
    ("user-agent", "binance-signalbot-r4b-v2-capture/1"),
)


def _connection_id(generation: int) -> str:
    return f"depth-rest-schedule-g{generation:06d}"


def _advance(
    authority: PublicDepthRestScheduleAuthorityV8,
    generation: int,
    *,
    session_id: str = _SESSION_ID,
    protocol_hash: str = _PROTOCOL_HASH,
    connection_id: str | None = None,
) -> None:
    authority.advance_connection_generation(
        generation,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=_connection_id(generation) if connection_id is None else connection_id,
    )


def _retire(
    authority: PublicDepthRestScheduleAuthorityV8,
    generation: int,
    *,
    session_id: str = _SESSION_ID,
    protocol_hash: str = _PROTOCOL_HASH,
    connection_id: str | None = None,
) -> None:
    authority.retire_current_generation(
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=_connection_id(generation) if connection_id is None else connection_id,
        connection_generation=generation,
    )


def _plan(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
) -> ProvisionalDepthRestQualificationPlanV8:
    plans = build_provisional_promoting_capture_plans_v8(symbols)
    [plan] = [item for item in plans if type(item) is ProvisionalDepthRestQualificationPlanV8]
    return plan


def _authority(
    plan: ProvisionalDepthRestQualificationPlanV8 | None = None,
    *,
    generation: int | None = 1,
) -> PublicDepthRestScheduleAuthorityV8:
    authority = create_public_depth_rest_schedule_authority_v8(plan or _plan())
    if generation is not None:
        _advance(authority, generation)
    return authority


def _register(
    authority: PublicDepthRestScheduleAuthorityV8,
    *,
    symbol: str = "BTCUSDT",
    trigger: PublicDepthSnapshotTriggerV8 = "startup",
    connection_generation: int = 1,
    first_buffered_u: int = 100,
) -> tuple[PublicDepthRestRegisteredCycleV8, ...]:
    watermarks = (
        ((symbol, first_buffered_u),)
        if trigger == "sequence_gap"
        else tuple((item, first_buffered_u) for item in authority.symbol_census)
    )
    return authority.register_trigger(
        trigger=trigger,
        connection_generation=connection_generation,
        symbol_watermarks=watermarks,
    )


def _issue(
    authority: PublicDepthRestScheduleAuthorityV8,
    registration: PublicDepthRestRegisteredCycleV8,
    *,
    bridge_attempt: int = 1,
) -> PublicDepthRestScheduledAttemptTokenV8:
    return authority.issue_attempt(
        registration=registration,
        bridge_attempt=bridge_attempt,
    )


def _consume(
    token: PublicDepthRestScheduledAttemptTokenV8,
    authority: PublicDepthRestScheduleAuthorityV8,
) -> None:
    consume_public_depth_rest_scheduled_attempt_token_v8(
        token,
        plan=authority.plan,
        schedule_authority=authority,
    )


@dataclass(slots=True)
class _FixedClock:
    receipt: ReceiptTimestamp

    def capture(self) -> ReceiptTimestamp:
        return self.receipt


@dataclass(slots=True)
class _AdmissionOfferer:
    handoff: BoundedBatchHandoffV2

    def offer(self, record: RawRecordV2) -> QueuedRawRecordV2:
        return self.handoff.offer(record)

    def offer_with_admission_receipt(
        self,
        record: RawRecordV2,
    ) -> CaptureQueueAdmissionReceiptV2:
        return self.handoff.offer_with_admission_receipt(record)

    def validate_queue_admission_receipt_v2(
        self,
        receipt: CaptureQueueAdmissionReceiptV2,
    ) -> QueuedRawRecordV2:
        return self.handoff.validate_queue_admission_receipt_v2(receipt)


async def _actual_receipt(
    token: PublicDepthRestScheduledAttemptTokenV8,
    *,
    trigger_seq: int | None = None,
    first_buffered_u: int | None = None,
) -> PublicDepthRestAdmissionReceiptV8:
    handoff = BoundedBatchHandoffV2(
        BatchPolicyV2(
            max_records=4,
            max_encoded_bytes=2_000_000,
            max_linger_us=1_000,
            queue_max_events=8,
            queue_max_encoded_bytes=4_000_000,
            low_water_events=0,
            low_water_encoded_bytes=0,
            qualification_id="depth-rest-schedule-admission-test",
        ),
        expected_first_ingest_seq=1,
    )
    ingress = SharedWebSocketIngressV2(
        _AdmissionOfferer(handoff),
        recovered_wal_tail_ingest_seq=0,
    )
    observation = PublicDepthRestTerminalObservationV8.for_plan(
        token.plan,
        session_id=token.session_id,
        protocol_hash=token.protocol_hash,
        connection_id=token.connection_id,
        method="GET",
        base_url="https://fapi.binance.com",
        endpoint="/fapi/v1/depth",
        symbol=token.symbol,
        canonical_query=(("limit", "1000"), ("symbol", token.symbol)),
        request_headers=_BUILT_REQUEST_HEADERS,
        trigger=token.trigger,
        trigger_seq=token.trigger_seq if trigger_seq is None else trigger_seq,
        connection_generation=token.connection_generation,
        first_buffered_u=(token.first_buffered_u if first_buffered_u is None else first_buffered_u),
        symbol_ordinal=token.symbol_ordinal,
        bridge_attempt=token.bridge_attempt,
        request_started_wall_ms=1_001,
        request_started_monotonic_ns=10_001,
        response_first_header_wall_ms=1_002,
        response_first_header_monotonic_ns=10_002,
        attempt_ended_wall_ms=1_003,
        attempt_ended_monotonic_ns=10_003,
        response_status=200,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        body=b"{}",
    )
    return await ingress.offer_depth_https_attempt_v8(
        plan=token.plan,
        session_id=token.session_id,
        protocol_hash=token.protocol_hash,
        connection_id=token.connection_id,
        generation=token.connection_generation,
        symbol=token.symbol,
        clock=_FixedClock(ReceiptTimestamp(1_004, 10_004)),
        observation=observation,
        source_logical_key=public_depth_rest_source_logical_key_v8(token.symbol),
    )


async def _claim_and_ack_terminal(
    token: PublicDepthRestScheduledAttemptTokenV8,
    authority: PublicDepthRestScheduleAuthorityV8,
) -> None:
    _consume(token, authority)
    acknowledge_public_depth_rest_terminal_admission_v8(
        token,
        await _actual_receipt(token),
        plan=authority.plan,
        schedule_authority=authority,
    )


def _by_symbol(
    registrations: tuple[PublicDepthRestRegisteredCycleV8, ...],
    symbol: str,
) -> PublicDepthRestRegisteredCycleV8:
    return next(item for item in registrations if item.symbol == symbol)


def _copy_all_dataclass_fields[T](value: T) -> T:
    copied = cast(T, object.__new__(type(value)))
    for model_field in fields(cast(Any, value)):
        object.__setattr__(copied, model_field.name, getattr(value, model_field.name))
    return copied


def test_factory_binds_exact_plan_hash_and_symbol_census() -> None:
    plan = _plan()
    authority = _authority(plan)

    assert authority.plan is plan
    assert authority.plan_sha256 == public_depth_rest_plan_sha256_v8(plan)
    assert authority.symbol_census == ("BTCUSDT", "ETHUSDT")
    assert authority.retained_registration_count == 0
    with pytest.raises(TypeError, match="exact factory"):
        PublicDepthRestScheduleAuthorityV8(plan=plan)


def test_registration_requires_explicit_generation_advance() -> None:
    authority = _authority(generation=None)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="not open for registration",
    ):
        _register(authority)

    _advance(authority, 1)
    [btc, eth] = _register(authority)
    assert btc.connection_generation == eth.connection_generation == 1


def test_equal_generation_replay_is_rejected_and_registration_retains_lineage() -> None:
    authority = _authority()
    [btc, _] = _register(authority)

    assert btc.session_id == _SESSION_ID
    assert btc.protocol_hash == _PROTOCOL_HASH
    assert btc.connection_id == _connection_id(1)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="strictly advance",
    ):
        _advance(authority, 1, connection_id="different-connection")


@pytest.mark.parametrize(
    "overrides",
    [
        {"session_id": "different-session"},
        {"protocol_hash": "f" * 64},
    ],
)
def test_higher_generation_cannot_cross_session_or_protocol_lineage(
    overrides: dict[str, str],
) -> None:
    authority = _authority()
    _retire(authority, 1)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable across connection generations",
    ):
        _advance(authority, 2, **overrides)
    assert not authority.generation_open
    _advance(authority, 2)
    [registration, *_] = _register(authority, connection_generation=2)
    assert registration.connection_generation == 2
    assert registration.session_id == _SESSION_ID
    assert registration.protocol_hash == _PROTOCOL_HASH


def test_higher_generation_requires_a_new_connection_id() -> None:
    authority = _authority()
    _retire(authority, 1)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="requires a new connection_id",
    ):
        _advance(authority, 2, connection_id=_connection_id(1))
    _advance(authority, 2)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"session_id": ""}, "session_id"),
        ({"session_id": " x"}, "session_id"),
        ({"protocol_hash": "ABC"}, "protocol_hash"),
        ({"connection_id": "x\n"}, "connection_id"),
    ],
)
def test_generation_lineage_rejects_invalid_material(
    kwargs: dict[str, str],
    message: str,
) -> None:
    authority = _authority(generation=None)

    with pytest.raises(ValueError, match=message):
        _advance(authority, 1, **kwargs)  # type: ignore[arg-type]


def test_startup_and_reconnect_atomically_register_exact_sorted_census() -> None:
    authority = _authority()
    registrations = authority.register_trigger(
        trigger="startup",
        connection_generation=1,
        symbol_watermarks=(("BTCUSDT", 100), ("ETHUSDT", 200)),
    )

    assert tuple(item.symbol for item in registrations) == authority.symbol_census
    assert tuple(item.first_buffered_u for item in registrations) == (100, 200)
    assert {item.trigger_seq for item in registrations} == {1}
    assert authority.retained_registration_count == 2
    for registration in registrations:
        validate_public_depth_rest_registered_cycle_v8(
            registration,
            plan=authority.plan,
            schedule_authority=authority,
        )

    reconnect = authority.register_trigger(
        trigger="reconnect",
        connection_generation=1,
        symbol_watermarks=(("BTCUSDT", 101), ("ETHUSDT", 201)),
    )
    assert {item.trigger_seq for item in reconnect} == {2}
    for stale in registrations:
        with pytest.raises(
            PublicDepthRestScheduledAttemptOwnershipErrorV8,
            match="no longer current",
        ):
            validate_public_depth_rest_registered_cycle_v8(
                stale,
                plan=authority.plan,
                schedule_authority=authority,
            )


@pytest.mark.parametrize("trigger", ["startup", "reconnect", "sequence_gap"])
def test_exact_snapshot_trigger_registration_is_accepted(
    trigger: PublicDepthSnapshotTriggerV8,
) -> None:
    [registration, *_] = _register(_authority(), trigger=trigger)

    assert registration.trigger == trigger
    assert registration.trigger_seq == 1


@pytest.mark.parametrize(
    ("trigger", "watermarks", "message"),
    [
        (
            "startup",
            (("ETHUSDT", 1), ("BTCUSDT", 1)),
            "exact sorted symbol census",
        ),
        ("startup", (("BTCUSDT", 1),), "exact sorted symbol census"),
        ("reconnect", (), "exact sorted symbol census"),
        (
            "sequence_gap",
            (("BTCUSDT", 1), ("ETHUSDT", 2)),
            "exactly one symbol",
        ),
        ("sequence_gap", (("SOLUSDT", 1),), "outside the census"),
        ("sequence_gap", (("BTCUSDT", -1),), "first_buffered_u"),
        ("sequence_gap", (("BTCUSDT", True),), "first_buffered_u"),
    ],
)
def test_registration_shape_rejects_partial_or_ambiguous_callbacks(
    trigger: PublicDepthSnapshotTriggerV8,
    watermarks: tuple[tuple[str, int], ...],
    message: str,
) -> None:
    authority = _authority()

    with pytest.raises(ValueError, match=message):
        authority.register_trigger(
            trigger=trigger,
            connection_generation=1,
            symbol_watermarks=watermarks,
        )
    assert authority.retained_registration_count == 0


def test_registration_rejects_nonexact_container_and_unknown_trigger() -> None:
    authority = _authority()

    with pytest.raises(ValueError, match="exact symbol/value tuples"):
        authority.register_trigger(
            trigger="startup",
            connection_generation=1,
            symbol_watermarks=[("BTCUSDT", 1), ("ETHUSDT", 1)],  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="trigger"):
        authority.register_trigger(
            trigger=cast(PublicDepthSnapshotTriggerV8, "periodic"),
            connection_generation=1,
            symbol_watermarks=(("BTCUSDT", 1),),
        )


@pytest.mark.parametrize("generation", [0, False, _MAX_SIGNED_INT64 + 1])
def test_registration_rejects_invalid_generation(generation: int) -> None:
    authority = _authority()

    with pytest.raises(ValueError, match="connection_generation"):
        _register(authority, connection_generation=generation)


def test_registration_requires_exact_current_generation() -> None:
    authority = _authority(generation=3)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="pre-advanced",
    ):
        _register(authority, connection_generation=2)


def test_full_census_registration_preflights_atomically_without_consuming_seq() -> None:
    authority = _authority()
    initial = authority.register_trigger(
        trigger="startup",
        connection_generation=1,
        symbol_watermarks=(("BTCUSDT", 100), ("ETHUSDT", 200)),
    )

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="strictly advance",
    ):
        authority.register_trigger(
            trigger="reconnect",
            connection_generation=1,
            symbol_watermarks=(("BTCUSDT", 101), ("ETHUSDT", 200)),
        )

    assert authority.retained_registration_count == 2
    for registration in initial:
        validate_public_depth_rest_registered_cycle_v8(
            registration,
            plan=authority.plan,
            schedule_authority=authority,
        )
    [next_registration] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )
    assert next_registration.trigger_seq == 2


def test_one_registration_issues_one_current_token_per_exact_symbol() -> None:
    authority = _authority()
    registrations = _register(authority)
    eth = _issue(authority, registrations[1])
    btc = _issue(authority, registrations[0])

    assert btc.plan_sha256 == authority.plan_sha256
    assert btc.symbol == "BTCUSDT"
    assert eth.symbol_ordinal == 1
    assert btc.trigger_seq == eth.trigger_seq == 1
    assert btc.registration is registrations[0]
    assert authority.retained_token_count == 2


@pytest.mark.asyncio
async def test_delayed_btc_seq2_remains_issuable_after_eth_seq3_registration() -> None:
    authority = _authority()
    startup = _register(authority)
    btc_seq1 = _issue(authority, startup[0])
    _consume(btc_seq1, authority)

    [btc_seq2] = _register(
        authority,
        trigger="sequence_gap",
        symbol="BTCUSDT",
        first_buffered_u=101,
    )
    [eth_seq3] = _register(
        authority,
        trigger="sequence_gap",
        symbol="ETHUSDT",
        first_buffered_u=201,
    )
    eth_token = _issue(authority, eth_seq3)

    assert btc_seq2.trigger_seq == 2
    assert eth_seq3.trigger_seq == 3
    assert eth_token.trigger_seq == 3
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="pending",
    ):
        _issue(authority, btc_seq2)

    acknowledge_public_depth_rest_terminal_admission_v8(
        btc_seq1,
        await _actual_receipt(btc_seq1),
        plan=authority.plan,
        schedule_authority=authority,
    )
    delayed = _issue(authority, btc_seq2)
    assert delayed.trigger_seq == 2
    assert delayed.bridge_attempt == 1


@pytest.mark.asyncio
async def test_claimed_token_is_never_overwritten_and_ack_promotes_latest_pending() -> None:
    authority = _authority()
    [btc, _] = _register(authority)
    claimed = _issue(authority, btc)
    _consume(claimed, authority)

    [pending2] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )
    [pending3] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=102,
    )

    assert authority.claimed_token_count == 1
    assert authority.pending_registration_count == 1
    assert authority.retained_registration_count == 3
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
        claimed,
        plan=authority.plan,
        schedule_authority=authority,
    )
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="no longer current",
    ):
        validate_public_depth_rest_registered_cycle_v8(
            pending2,
            plan=authority.plan,
            schedule_authority=authority,
        )

    acknowledge_public_depth_rest_terminal_admission_v8(
        claimed,
        await _actual_receipt(claimed),
        plan=authority.plan,
        schedule_authority=authority,
    )

    assert authority.claimed_token_count == 0
    assert authority.pending_registration_count == 0
    promoted = _issue(authority, pending3)
    assert promoted.bridge_attempt == 1
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="no longer current",
    ):
        validate_public_depth_rest_scheduled_attempt_token_v8(
            claimed,
            plan=authority.plan,
            schedule_authority=authority,
        )


@pytest.mark.asyncio
async def test_registration_disposition_exposes_normal_supersession_without_exceptions() -> None:
    authority = _authority()
    [active, _] = _register(authority)
    claimed = _issue(authority, active)
    _consume(claimed, authority)
    [replaced_pending] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )
    [latest_pending] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=102,
    )

    def disposition(
        value: PublicDepthRestRegisteredCycleV8,
    ) -> PublicDepthRestRegistrationDispositionV8:
        return public_depth_rest_registration_disposition_v8(
            value,
            plan=authority.plan,
            schedule_authority=authority,
        )

    assert disposition(active) is PublicDepthRestRegistrationDispositionV8.ACTIVE_CLAIMED
    assert disposition(replaced_pending) is PublicDepthRestRegistrationDispositionV8.SUPERSEDED
    assert disposition(latest_pending) is PublicDepthRestRegistrationDispositionV8.PENDING

    acknowledge_public_depth_rest_terminal_admission_v8(
        claimed,
        await _actual_receipt(claimed),
        plan=authority.plan,
        schedule_authority=authority,
    )

    assert disposition(active) is PublicDepthRestRegistrationDispositionV8.SUPERSEDED
    assert disposition(latest_pending) is PublicDepthRestRegistrationDispositionV8.ACTIVE_READY


@pytest.mark.asyncio
async def test_registration_disposition_exposes_each_active_lifecycle() -> None:
    authority = _authority()
    [registration, _] = _register(authority)

    def disposition() -> PublicDepthRestRegistrationDispositionV8:
        return public_depth_rest_registration_disposition_v8(
            registration,
            plan=authority.plan,
            schedule_authority=authority,
        )

    assert disposition() is PublicDepthRestRegistrationDispositionV8.ACTIVE_READY

    token = _issue(authority, registration)
    assert disposition() is PublicDepthRestRegistrationDispositionV8.ACTIVE_ISSUED

    _consume(token, authority)
    assert disposition() is PublicDepthRestRegistrationDispositionV8.ACTIVE_CLAIMED

    acknowledge_public_depth_rest_terminal_admission_v8(
        token,
        await _actual_receipt(token),
        plan=authority.plan,
        schedule_authority=authority,
    )
    assert disposition() is PublicDepthRestRegistrationDispositionV8.ACTIVE_TERMINAL_ADMITTED

    retry = _issue(authority, registration, bridge_attempt=2)
    assert retry.bridge_attempt == 2
    assert disposition() is PublicDepthRestRegistrationDispositionV8.ACTIVE_ISSUED


def test_ready_disposition_is_observational_and_issue_is_atomic_transition() -> None:
    authority = _authority()
    [registration, _] = _register(authority)

    first_observation = public_depth_rest_registration_disposition_v8(
        registration,
        plan=authority.plan,
        schedule_authority=authority,
    )
    second_observation = public_depth_rest_registration_disposition_v8(
        registration,
        plan=authority.plan,
        schedule_authority=authority,
    )
    assert first_observation is second_observation
    assert first_observation is PublicDepthRestRegistrationDispositionV8.ACTIVE_READY

    _issue(authority, registration)
    assert (
        public_depth_rest_registration_disposition_v8(
            registration,
            plan=authority.plan,
            schedule_authority=authority,
        )
        is PublicDepthRestRegistrationDispositionV8.ACTIVE_ISSUED
    )
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="duplicated or replayed",
    ):
        _issue(authority, registration)


def test_registration_disposition_reports_prior_generation_as_superseded() -> None:
    authority = _authority()
    [registration, _] = _register(authority)

    _retire(authority, 1)
    _advance(authority, 2)

    assert (
        public_depth_rest_registration_disposition_v8(
            registration,
            plan=authority.plan,
            schedule_authority=authority,
        )
        is PublicDepthRestRegistrationDispositionV8.SUPERSEDED
    )


def test_registration_disposition_rejects_foreign_issuer_and_mutation() -> None:
    plan = _plan()
    authority = _authority(plan)
    [registration, _] = _register(authority)
    foreign_authority = _authority(plan)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="different issuer",
    ):
        public_depth_rest_registration_disposition_v8(
            registration,
            plan=plan,
            schedule_authority=foreign_authority,
        )

    object.__setattr__(registration, "symbol", "ETHUSDT")
    with pytest.raises(
        (ValueError, PublicDepthRestScheduledAttemptOwnershipErrorV8),
        match="immutable material",
    ):
        public_depth_rest_registration_disposition_v8(
            registration,
            plan=plan,
            schedule_authority=authority,
        )


@pytest.mark.parametrize(
    ("field_name", "tampered_value"),
    [
        ("first_buffered_u", 999),
        ("trigger", "reconnect"),
    ],
)
def test_registration_disposition_rejects_valid_shape_active_tamper(
    field_name: str,
    tampered_value: object,
) -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    object.__setattr__(registration, field_name, tampered_value)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable material",
    ):
        public_depth_rest_registration_disposition_v8(
            registration,
            plan=authority.plan,
            schedule_authority=authority,
        )


def test_registration_disposition_rejects_valid_shape_pending_tamper() -> None:
    authority = _authority()
    [active, _] = _register(authority)
    token = _issue(authority, active)
    _consume(token, authority)
    [pending] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )
    object.__setattr__(pending, "first_buffered_u", 999)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable material",
    ):
        public_depth_rest_registration_disposition_v8(
            pending,
            plan=authority.plan,
            schedule_authority=authority,
        )


def test_registration_disposition_rejects_retargeted_foreign_capability() -> None:
    plan = _plan()
    authority = _authority(plan)
    foreign_authority = _authority(plan)
    [local_registration, _] = _register(authority)
    [foreign_registration, _] = _register(foreign_authority)
    foreign_token = _issue(foreign_authority, foreign_registration)

    object.__setattr__(foreign_registration, "schedule_authority", authority)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="foreign authority capability",
    ):
        public_depth_rest_registration_disposition_v8(
            foreign_registration,
            plan=plan,
            schedule_authority=authority,
        )

    object.__setattr__(foreign_token, "schedule_authority", authority)
    object.__setattr__(foreign_token, "registration", local_registration)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="foreign authority capability",
    ):
        validate_public_depth_rest_scheduled_attempt_token_v8(
            foreign_token,
            plan=plan,
            schedule_authority=authority,
        )

    object.__setattr__(
        foreign_token,
        "_authority_capability_seal",
        authority._mint_capability,
    )
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable material",
    ):
        validate_public_depth_rest_scheduled_attempt_token_v8(
            foreign_token,
            plan=plan,
            schedule_authority=authority,
        )

    object.__setattr__(
        foreign_registration,
        "_authority_capability_seal",
        authority._mint_capability,
    )
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable material",
    ):
        public_depth_rest_registration_disposition_v8(
            foreign_registration,
            plan=plan,
            schedule_authority=authority,
        )


def test_pending_supersession_requires_strictly_advancing_watermark() -> None:
    authority = _authority()
    [btc, _] = _register(authority)
    claimed = _issue(authority, btc)
    _consume(claimed, authority)
    [pending] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )

    for watermark in (100, 101):
        with pytest.raises(
            PublicDepthRestScheduledAttemptOwnershipErrorV8,
            match="strictly advance",
        ):
            _register(
                authority,
                trigger="sequence_gap",
                first_buffered_u=watermark,
            )
    assert authority.pending_registration_count == 1
    assert authority.retained_registration_count == 3
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="pending",
    ):
        _issue(authority, pending)


def test_unclaimed_issued_token_is_safely_superseded_and_fails_closed() -> None:
    authority = _authority()
    startup = _register(authority)
    stale_token = _issue(authority, startup[0])
    [current] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="no longer current",
    ):
        _consume(stale_token, authority)
    current_token = _issue(authority, current)
    _consume(current_token, authority)
    validate_public_depth_rest_registered_cycle_v8(
        startup[1],
        plan=authority.plan,
        schedule_authority=authority,
    )


def test_unissued_registration_can_be_superseded_but_not_reused() -> None:
    authority = _authority()
    [stale, _] = _register(authority)
    [current] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="no longer current",
    ):
        _issue(authority, stale)
    assert _issue(authority, current).bridge_attempt == 1


@pytest.mark.asyncio
async def test_terminal_cycle_allows_only_three_contiguous_admitted_attempts() -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    first = _issue(authority, registration)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="terminal",
    ):
        _issue(authority, registration, bridge_attempt=2)
    _consume(first, authority)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="terminal",
    ):
        _issue(authority, registration, bridge_attempt=2)

    acknowledge_public_depth_rest_terminal_admission_v8(
        first,
        await _actual_receipt(first),
        plan=authority.plan,
        schedule_authority=authority,
    )
    second = _issue(authority, registration, bridge_attempt=2)
    await _claim_and_ack_terminal(second, authority)
    third = _issue(authority, registration, bridge_attempt=3)

    with pytest.raises(ValueError, match="bound"):
        _issue(authority, registration, bridge_attempt=4)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="duplicated",
    ):
        _issue(authority, registration, bridge_attempt=3)
    _consume(third, authority)


@pytest.mark.asyncio
async def test_new_registration_after_terminal_admission_revokes_old_retry() -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    first = _issue(authority, registration)
    await _claim_and_ack_terminal(first, authority)
    [newer] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="no longer current",
    ):
        _issue(authority, registration, bridge_attempt=2)
    assert _issue(authority, newer).bridge_attempt == 1


@pytest.mark.asyncio
async def test_terminal_admission_requires_claim_and_exact_payload_identity() -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    token = _issue(authority, registration)
    exact_receipt = await _actual_receipt(token)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="must be claimed",
    ):
        acknowledge_public_depth_rest_terminal_admission_v8(
            token,
            exact_receipt,
            plan=authority.plan,
            schedule_authority=authority,
        )
    _consume(token, authority)
    for receipt in (
        await _actual_receipt(token, trigger_seq=token.trigger_seq + 1),
        await _actual_receipt(token, first_buffered_u=token.first_buffered_u + 1),
    ):
        with pytest.raises(
            PublicDepthRestScheduledAttemptOwnershipErrorV8,
            match="identity differs",
        ):
            acknowledge_public_depth_rest_terminal_admission_v8(
                token,
                receipt,
                plan=authority.plan,
                schedule_authority=authority,
            )
    acknowledge_public_depth_rest_terminal_admission_v8(
        token,
        exact_receipt,
        plan=authority.plan,
        schedule_authority=authority,
    )
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="already acknowledged",
    ):
        acknowledge_public_depth_rest_terminal_admission_v8(
            token,
            exact_receipt,
            plan=authority.plan,
            schedule_authority=authority,
        )


def test_one_shot_claim_and_replay_fail_closed() -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    token = _issue(authority, registration)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="not been claimed",
    ):
        assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
            token,
            plan=authority.plan,
            schedule_authority=authority,
        )
    _consume(token, authority)
    assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
        token,
        plan=authority.plan,
        schedule_authority=authority,
    )
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="already claimed",
    ):
        _consume(token, authority)


@pytest.mark.asyncio
async def test_generation_retirement_blocks_claimed_then_clears_promoted_pending() -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    claimed = _issue(authority, registration)
    _consume(claimed, authority)
    [pending] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="must drain",
    ):
        _retire(authority, 1)
    acknowledge_public_depth_rest_terminal_admission_v8(
        claimed,
        await _actual_receipt(claimed),
        plan=authority.plan,
        schedule_authority=authority,
    )
    validate_public_depth_rest_registered_cycle_v8(
        pending,
        plan=authority.plan,
        schedule_authority=authority,
    )

    _retire(authority, 1)
    assert authority.retained_token_count == 0
    assert authority.retained_registration_count == 0
    assert authority.pending_registration_count == 0
    assert not authority.generation_open
    _advance(authority, 2)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="stale connection generation",
    ):
        validate_public_depth_rest_registered_cycle_v8(
            pending,
            plan=authority.plan,
            schedule_authority=authority,
        )
    reconnect = _register(
        authority,
        trigger="reconnect",
        connection_generation=2,
        first_buffered_u=200,
    )
    assert reconnect[0].trigger_seq == 3


def test_generation_retirement_clears_unclaimed_tokens_and_registrations() -> None:
    authority = _authority()
    registrations = _register(authority)
    stale = _issue(authority, registrations[0])

    _retire(authority, 1)

    assert authority.retained_token_count == 0
    assert authority.retained_registration_count == 0
    assert not authority.generation_open
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="retired depth REST generation",
    ):
        _consume(stale, authority)


@pytest.mark.asyncio
async def test_exact_retirement_clears_issued_and_terminal_state_and_closes_mutations() -> None:
    authority = _authority()
    registrations = _register(authority)
    issued = _issue(authority, registrations[0])
    terminal = _issue(authority, registrations[1])
    _consume(terminal, authority)
    terminal_receipt = await _actual_receipt(terminal)
    acknowledge_public_depth_rest_terminal_admission_v8(
        terminal,
        terminal_receipt,
        plan=authority.plan,
        schedule_authority=authority,
    )

    assert authority.generation_open
    assert authority.current_connection_generation == 1
    assert authority.retained_registration_count == 2
    assert authority.retained_token_count == 2
    assert authority.claimed_token_count == 0

    _retire(authority, 1)

    assert not authority.generation_open
    assert authority.current_connection_generation == 1
    assert authority.retained_registration_count == 0
    assert authority.pending_registration_count == 0
    assert authority.retained_token_count == 0
    assert authority.claimed_token_count == 0
    for operation in (
        lambda: _register(authority),
        lambda: _issue(authority, registrations[0]),
        lambda: _consume(issued, authority),
        lambda: acknowledge_public_depth_rest_terminal_admission_v8(
            terminal,
            terminal_receipt,
            plan=authority.plan,
            schedule_authority=authority,
        ),
    ):
        with pytest.raises(
            PublicDepthRestScheduledAttemptOwnershipErrorV8,
            match=r"not open|retired depth REST generation",
        ):
            operation()
    assert (
        public_depth_rest_registration_disposition_v8(
            registrations[0],
            plan=authority.plan,
            schedule_authority=authority,
        )
        is PublicDepthRestRegistrationDispositionV8.SUPERSEDED
    )


def test_generation_retirement_rejects_zero_stale_future_foreign_and_replay() -> None:
    authority = _authority(generation=None)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="generation zero",
    ):
        _retire(authority, 1)

    _advance(authority, 1)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="future",
    ):
        _retire(authority, 2)
    for overrides in (
        {"session_id": "foreign-session"},
        {"protocol_hash": "1" * 64},
        {"connection_id": "foreign-connection"},
    ):
        with pytest.raises(
            PublicDepthRestScheduledAttemptOwnershipErrorV8,
            match="exact current lineage",
        ):
            _retire(authority, 1, **overrides)
    assert authority.generation_open

    _retire(authority, 1)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="already retired",
    ):
        _retire(authority, 1)
    _advance(authority, 2)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="stale",
    ):
        _retire(authority, 1)


def test_next_generation_requires_retirement_higher_cursor_and_immutable_lineage() -> None:
    authority = _authority()
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="must be retired",
    ):
        _advance(authority, 2)
    _retire(authority, 1)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="strictly advance",
    ):
        _advance(authority, 1)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable",
    ):
        _advance(authority, 2, session_id="changed-session")
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable",
    ):
        _advance(authority, 2, protocol_hash="1" * 64)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="new connection_id",
    ):
        _advance(authority, 2, connection_id=_connection_id(1))

    _advance(authority, 2)
    assert authority.generation_open
    assert authority.current_connection_generation == 2


def test_generation_boundaries_reject_regression_and_signed_int64_overflow() -> None:
    authority = _authority(generation=2)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="backwards",
    ):
        _advance(authority, 1)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="must be retired",
    ):
        _advance(authority, _MAX_SIGNED_INT64)
    _retire(authority, 2)
    _advance(authority, _MAX_SIGNED_INT64)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="strictly advance",
    ):
        _advance(authority, _MAX_SIGNED_INT64)
    with pytest.raises(ValueError, match="connection_generation"):
        _advance(authority, _MAX_SIGNED_INT64 + 1)


def test_first_buffered_u_accepts_signed_int64_maximum() -> None:
    authority = _authority(generation=_MAX_SIGNED_INT64)
    [registration] = _register(
        authority,
        trigger="sequence_gap",
        connection_generation=_MAX_SIGNED_INT64,
        first_buffered_u=_MAX_SIGNED_INT64,
    )

    assert registration.first_buffered_u == _MAX_SIGNED_INT64


def test_trigger_sequence_exhaustion_fails_without_mutating_slots() -> None:
    authority = _authority()
    state = authority._state
    state.current_trigger_seq = _MAX_SIGNED_INT64

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="sequence is exhausted",
    ):
        _register(authority)
    assert authority.retained_registration_count == 0


@pytest.mark.asyncio
async def test_fixed_slots_bound_active_and_latest_pending_registrations() -> None:
    authority = _authority()
    registrations = _register(authority)
    claimed = _issue(authority, registrations[0])
    _consume(claimed, authority)
    stale_pending: PublicDepthRestRegisteredCycleV8 | None = None

    for watermark in range(101, 151):
        [latest] = _register(
            authority,
            trigger="sequence_gap",
            first_buffered_u=watermark,
        )
        if stale_pending is not None:
            with pytest.raises(
                PublicDepthRestScheduledAttemptOwnershipErrorV8,
                match="no longer current",
            ):
                validate_public_depth_rest_registered_cycle_v8(
                    stale_pending,
                    plan=authority.plan,
                    schedule_authority=authority,
                )
        stale_pending = latest
        assert authority.pending_registration_count == 1
        assert authority.retained_registration_count == 3
        assert authority.retained_token_count == 1

    assert stale_pending is not None
    acknowledge_public_depth_rest_terminal_admission_v8(
        claimed,
        await _actual_receipt(claimed),
        plan=authority.plan,
        schedule_authority=authority,
    )
    assert _issue(authority, stale_pending).first_buffered_u == 150


@pytest.mark.asyncio
async def test_full_reconnect_retains_at_most_active_plus_pending_per_symbol() -> None:
    authority = _authority()
    active = _register(authority)
    tokens = tuple(_issue(authority, registration) for registration in active)
    for token in tokens:
        _consume(token, authority)

    pending = authority.register_trigger(
        trigger="reconnect",
        connection_generation=1,
        symbol_watermarks=(("BTCUSDT", 101), ("ETHUSDT", 101)),
    )
    assert authority.claimed_token_count == 2
    assert authority.pending_registration_count == 2
    assert authority.retained_registration_count == 4

    for token in tokens:
        acknowledge_public_depth_rest_terminal_admission_v8(
            token,
            await _actual_receipt(token),
            plan=authority.plan,
            schedule_authority=authority,
        )
    assert authority.pending_registration_count == 0
    assert authority.retained_registration_count == 2
    for registration in pending:
        assert _issue(authority, registration).bridge_attempt == 1


def test_foreign_authority_and_equivalent_plan_objects_are_rejected() -> None:
    plan = _plan()
    authority = _authority(plan)
    foreign_authority = _authority(plan)
    equivalent_plan = _plan()
    [registration, _] = _register(authority)
    token = _issue(authority, registration)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="different issuer",
    ):
        validate_public_depth_rest_registered_cycle_v8(
            registration,
            plan=plan,
            schedule_authority=foreign_authority,
        )
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="different plan",
    ):
        validate_public_depth_rest_scheduled_attempt_token_v8(
            token,
            plan=equivalent_plan,
            schedule_authority=authority,
        )


def test_authority_detects_post_factory_plan_hash_or_census_drift() -> None:
    plan = _plan()
    authority = _authority(plan)
    object.__setattr__(plan, "symbols", ("BTCUSDT", "SOLUSDT"))

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="plan hash",
    ):
        validate_public_depth_rest_schedule_authority_v8(authority, plan=plan)


def test_authority_rejects_tampered_active_registration_before_generation_clear() -> None:
    authority = _authority()
    [active, _] = _register(authority)
    object.__setattr__(active, "first_buffered_u", -1)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="foreign or tampered registration",
    ):
        validate_public_depth_rest_schedule_authority_v8(
            authority,
            plan=authority.plan,
        )
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="foreign or tampered registration",
    ):
        _advance(authority, 2)


def test_authority_rejects_tampered_pending_registration_lineage() -> None:
    authority = _authority()
    [active, _] = _register(authority)
    token = _issue(authority, active)
    _consume(token, authority)
    [pending] = _register(
        authority,
        trigger="sequence_gap",
        first_buffered_u=101,
    )
    object.__setattr__(pending, "connection_id", "tampered-connection")

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="foreign or tampered registration",
    ):
        validate_public_depth_rest_schedule_authority_v8(
            authority,
            plan=authority.plan,
        )


@pytest.mark.parametrize("invalid_seq", [-1, True, _MAX_SIGNED_INT64 + 1])
def test_authority_rejects_invalid_internal_trigger_sequence(invalid_seq: int) -> None:
    authority = _authority()
    authority._state.current_trigger_seq = invalid_seq

    with pytest.raises(ValueError, match="current_trigger_seq"):
        validate_public_depth_rest_schedule_authority_v8(
            authority,
            plan=authority.plan,
        )


@pytest.mark.parametrize("foreign_lifecycle", ["issued", "claimed", "terminal_admitted"])
def test_authority_rejects_string_lookalike_attempt_lifecycle(
    foreign_lifecycle: str,
) -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    token = _issue(authority, registration)
    _consume(token, authority)
    slot = authority._state.symbols[registration.symbol_ordinal]
    slot.lifecycle = cast(Any, foreign_lifecycle)

    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="lifecycle has a foreign type",
    ):
        validate_public_depth_rest_schedule_authority_v8(
            authority,
            plan=authority.plan,
        )
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="lifecycle has a foreign type",
    ):
        _advance(authority, 2)


def test_capabilities_reject_copy_deepcopy_pickle_replace_and_construction() -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    token = _issue(authority, registration)

    for operation in (
        lambda: copy.copy(authority),
        lambda: copy.deepcopy(authority),
        lambda: pickle.dumps(authority),
        lambda: copy.copy(registration),
        lambda: copy.deepcopy(registration),
        lambda: pickle.dumps(registration),
        lambda: copy.copy(token),
        lambda: copy.deepcopy(token),
        lambda: pickle.dumps(token),
    ):
        with pytest.raises(PublicDepthRestScheduledAttemptOwnershipErrorV8):
            operation()
    with pytest.raises(TypeError, match="minted by its authority"):
        replace(registration, first_buffered_u=101)
    with pytest.raises(TypeError, match="minted by its authority"):
        PublicDepthRestRegisteredCycleV8(
            plan=authority.plan,
            plan_sha256=authority.plan_sha256,
            schedule_authority=authority,
            session_id=_SESSION_ID,
            protocol_hash=_PROTOCOL_HASH,
            connection_id=_connection_id(1),
            symbol="BTCUSDT",
            symbol_ordinal=0,
            trigger="startup",
            trigger_seq=1,
            connection_generation=1,
            first_buffered_u=100,
        )
    with pytest.raises(TypeError, match="issued by its authority"):
        replace(token, bridge_attempt=2)
    with pytest.raises(TypeError, match="issued by its authority"):
        PublicDepthRestScheduledAttemptTokenV8(
            plan=authority.plan,
            plan_sha256=authority.plan_sha256,
            schedule_authority=authority,
            registration=registration,
            session_id=_SESSION_ID,
            protocol_hash=_PROTOCOL_HASH,
            connection_id=_connection_id(1),
            symbol="BTCUSDT",
            symbol_ordinal=0,
            trigger="startup",
            trigger_seq=1,
            connection_generation=1,
            first_buffered_u=100,
            bridge_attempt=1,
        )


def test_material_seals_reject_objects_with_all_private_fields_copied() -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    token = _issue(authority, registration)

    authority_copy = _copy_all_dataclass_fields(authority)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable material",
    ):
        validate_public_depth_rest_schedule_authority_v8(
            authority_copy,
            plan=authority.plan,
        )

    registration_copy = _copy_all_dataclass_fields(registration)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable material",
    ):
        public_depth_rest_registration_disposition_v8(
            registration_copy,
            plan=authority.plan,
            schedule_authority=authority,
        )

    token_copy = _copy_all_dataclass_fields(token)
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable material",
    ):
        validate_public_depth_rest_scheduled_attempt_token_v8(
            token_copy,
            plan=authority.plan,
            schedule_authority=authority,
        )


def test_authority_and_token_material_seals_reject_valid_shape_tamper() -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    token = _issue(authority, registration)

    object.__setattr__(token, "trigger", "reconnect")
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable material",
    ):
        validate_public_depth_rest_scheduled_attempt_token_v8(
            token,
            plan=authority.plan,
            schedule_authority=authority,
        )

    object.__setattr__(authority, "_mint_capability", object())
    with pytest.raises(
        PublicDepthRestScheduledAttemptOwnershipErrorV8,
        match="immutable material",
    ):
        validate_public_depth_rest_schedule_authority_v8(
            authority,
            plan=authority.plan,
        )


def test_scheduler_has_no_transport_io_or_promotion_surface() -> None:
    authority = _authority()
    [registration, _] = _register(authority)
    token = _issue(authority, registration)

    for capability in (authority, registration, token):
        for forbidden_attribute in (
            "http_attempt",
            "body",
            "ingress",
            "book_bridge_certified",
            "m2_certified",
            "paper",
            "order_execution_enabled",
        ):
            assert not hasattr(capability, forbidden_attribute)
