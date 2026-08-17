from __future__ import annotations

import re
from dataclasses import InitVar, dataclass, field
from enum import StrEnum
from typing import NoReturn, SupportsIndex

from signalbot.r4b_v2.capture.plans import ProvisionalDepthRestQualificationPlanV8
from signalbot.r4b_v2.capture.rest_depth import (
    PublicDepthRestAttemptPayloadV8,
    PublicDepthSnapshotTriggerV8,
    public_depth_rest_plan_sha256_v8,
    validate_public_depth_rest_plan_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicDepthRestAdmissionReceiptV8,
    validate_public_depth_rest_admission_receipt_v8,
)

_PUBLIC_DEPTH_REST_SCHEDULE_AUTHORITY_FACTORY_TOKEN_V8 = object()
_PUBLIC_DEPTH_REST_REGISTERED_CYCLE_FACTORY_TOKEN_V8 = object()
_PUBLIC_DEPTH_REST_SCHEDULED_ATTEMPT_FACTORY_TOKEN_V8 = object()
_MAX_SIGNED_INT64 = (1 << 63) - 1
_MAX_IDENTITY_LENGTH = 256
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class PublicDepthRestScheduledAttemptOwnershipErrorV8(RuntimeError):
    """A depth schedule capability was foreign, stale, unclaimed, or replayed."""


class PublicDepthRestRegistrationDispositionV8(StrEnum):
    """Observed scheduler state of one valid registered-cycle capability.

    This is not a worker lease.  A coordinator still owns exactly one worker
    per symbol, and ``issue_attempt`` is the atomic READY-to-ISSUED transition.
    """

    ACTIVE_READY = "active_ready"
    ACTIVE_ISSUED = "active_issued"
    ACTIVE_CLAIMED = "active_claimed"
    ACTIVE_TERMINAL_ADMITTED = "active_terminal_admitted"
    PENDING = "pending"
    SUPERSEDED = "superseded"


class _PublicDepthRestAttemptLifecycleV8(StrEnum):
    ISSUED = "issued"
    CLAIMED = "claimed"
    TERMINAL_ADMITTED = "terminal_admitted"


@dataclass(slots=True)
class _PublicDepthRestSymbolScheduleStateV8:
    registration: PublicDepthRestRegisteredCycleV8 | None = None
    pending_registration: PublicDepthRestRegisteredCycleV8 | None = None
    last_bridge_attempt: int = 0
    token: PublicDepthRestScheduledAttemptTokenV8 | None = None
    lifecycle: _PublicDepthRestAttemptLifecycleV8 | None = None

    def clear(self) -> None:
        self.registration = None
        self.pending_registration = None
        self.last_bridge_attempt = 0
        self.token = None
        self.lifecycle = None


@dataclass(slots=True)
class _PublicDepthRestScheduleStateV8:
    current_trigger_seq: int
    current_session_id: str | None
    current_protocol_hash: str | None
    current_connection_id: str | None
    current_connection_generation: int
    generation_open: bool
    symbols: list[_PublicDepthRestSymbolScheduleStateV8]


@dataclass(frozen=True, slots=True, eq=False)
class PublicDepthRestScheduleAuthorityV8:
    """Process-local issuer for bounded, non-promoting depth REST attempts."""

    plan: ProvisionalDepthRestQualificationPlanV8 = field(repr=False)
    plan_sha256: str = field(init=False)
    symbol_census: tuple[str, ...] = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    _mint_capability: object = field(init=False, repr=False, compare=False)
    _material_seal: tuple[object, ...] = field(
        init=False,
        repr=False,
        compare=False,
    )
    _state: _PublicDepthRestScheduleStateV8 = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_DEPTH_REST_SCHEDULE_AUTHORITY_FACTORY_TOKEN_V8:
            raise TypeError(
                "PublicDepthRestScheduleAuthorityV8 must be created by its exact factory"
            )
        validate_public_depth_rest_plan_v8(self.plan)
        object.__setattr__(self, "plan_sha256", public_depth_rest_plan_sha256_v8(self.plan))
        object.__setattr__(self, "symbol_census", self.plan.symbols)
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_DEPTH_REST_SCHEDULE_AUTHORITY_FACTORY_TOKEN_V8,
        )
        object.__setattr__(self, "_mint_capability", object())
        object.__setattr__(
            self,
            "_state",
            _PublicDepthRestScheduleStateV8(
                current_trigger_seq=0,
                current_session_id=None,
                current_protocol_hash=None,
                current_connection_id=None,
                current_connection_generation=0,
                generation_open=False,
                symbols=[_PublicDepthRestSymbolScheduleStateV8() for _ in self.symbol_census],
            ),
        )
        object.__setattr__(
            self,
            "_material_seal",
            _public_depth_rest_schedule_authority_material_v8(self),
        )

    @property
    def retained_token_count(self) -> int:
        """Return the fixed-census number of currently retained token identities."""

        return sum(slot.token is not None for slot in self._state.symbols)

    @property
    def claimed_token_count(self) -> int:
        """Return the bounded number of attempts that must drain before advance."""

        return sum(
            slot.lifecycle is _PublicDepthRestAttemptLifecycleV8.CLAIMED
            for slot in self._state.symbols
        )

    @property
    def retained_registration_count(self) -> int:
        """Return active plus pending registrations retained in fixed symbol slots."""

        return sum(
            int(slot.registration is not None) + int(slot.pending_registration is not None)
            for slot in self._state.symbols
        )

    @property
    def pending_registration_count(self) -> int:
        """Return the fixed-census number of claimed cycles with a pending successor."""

        return sum(slot.pending_registration is not None for slot in self._state.symbols)

    @property
    def generation_open(self) -> bool:
        """Return whether the exact current connection generation accepts work."""

        return self._state.generation_open

    @property
    def current_connection_generation(self) -> int:
        """Return the current generation cursor without fabricating a successor."""

        return self._state.current_connection_generation

    def advance_connection_generation(
        self,
        generation: int,
        *,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
    ) -> None:
        """Advance exact lineage after all claimed attempts have terminally drained."""

        validate_public_depth_rest_schedule_authority_v8(self, plan=self.plan)
        _require_positive_int(generation, "connection_generation")
        _require_bounded_identity(session_id, "session_id")
        _require_sha256(protocol_hash, "protocol_hash")
        _require_bounded_identity(connection_id, "connection_id")
        state = self._state
        if generation < state.current_connection_generation:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST connection_generation moved backwards"
            )
        if generation == state.current_connection_generation:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST connection generation must strictly advance"
            )
        if state.current_connection_generation != 0:
            if state.generation_open:
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "current depth REST connection generation must be retired before advance"
                )
            if (
                state.current_session_id != session_id
                or state.current_protocol_hash != protocol_hash
            ):
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "depth REST schedule authority session and protocol lineage "
                    "are immutable across connection generations"
                )
            if state.current_connection_id == connection_id:
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "a new depth REST connection generation requires a new connection_id"
                )
        elif state.generation_open:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "uninitialized depth REST generation state is unexpectedly open"
            )
        for symbol_state in state.symbols:
            symbol_state.clear()
        state.current_session_id = session_id
        state.current_protocol_hash = protocol_hash
        state.current_connection_id = connection_id
        state.current_connection_generation = generation
        state.generation_open = True

    def retire_current_generation(
        self,
        *,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        connection_generation: int,
    ) -> None:
        """Synchronously retire the exact drained generation once.

        The caller owns worker and adapter draining.  This boundary proves that
        no claimed schedule token remains, clears every other bounded cycle and
        token slot, and closes the generation against later registration,
        issuance, claim, or terminal acknowledgement.
        """

        validate_public_depth_rest_schedule_authority_v8(self, plan=self.plan)
        _require_bounded_identity(session_id, "session_id")
        _require_sha256(protocol_hash, "protocol_hash")
        _require_bounded_identity(connection_id, "connection_id")
        _require_positive_int(connection_generation, "connection_generation")
        state = self._state
        current_generation = state.current_connection_generation
        if current_generation == 0:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST generation zero cannot be retired"
            )
        if connection_generation < current_generation:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "stale depth REST connection generation cannot be retired"
            )
        if connection_generation > current_generation:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "future depth REST connection generation cannot be retired"
            )
        if (
            state.current_session_id != session_id
            or state.current_protocol_hash != protocol_hash
            or state.current_connection_id != connection_id
        ):
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST generation retirement requires exact current lineage"
            )
        if not state.generation_open:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST connection generation was already retired"
            )
        if self.claimed_token_count:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "claimed depth REST attempts must drain before generation retirement"
            )
        for symbol_state in state.symbols:
            symbol_state.clear()
        state.generation_open = False

    def register_trigger(
        self,
        *,
        trigger: PublicDepthSnapshotTriggerV8,
        connection_generation: int,
        symbol_watermarks: tuple[tuple[str, int], ...],
    ) -> tuple[PublicDepthRestRegisteredCycleV8, ...]:
        """Atomically register one ordered callback as bounded symbol cycles.

        Startup and reconnect callbacks cover the exact sorted symbol census in one
        registration sequence. A sequence gap covers exactly one symbol. The
        authority allocates the contiguous sequence, so workers can only issue a
        capability minted by this callback-order owner.
        """

        validate_public_depth_rest_schedule_authority_v8(self, plan=self.plan)
        state = self._state
        if state.current_connection_generation == 0 or not state.generation_open:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST connection generation is not open for registration"
            )
        _require_positive_int(connection_generation, "connection_generation")
        if connection_generation != state.current_connection_generation:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST registration requires the exact pre-advanced connection_generation"
            )
        validated_watermarks = _validate_trigger_registration_v8(
            plan=self.plan,
            trigger=trigger,
            symbol_watermarks=symbol_watermarks,
        )
        if state.current_trigger_seq == _MAX_SIGNED_INT64:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST trigger sequence is exhausted"
            )
        trigger_seq = state.current_trigger_seq + 1
        registrations = tuple(
            PublicDepthRestRegisteredCycleV8(
                plan=self.plan,
                plan_sha256=self.plan_sha256,
                schedule_authority=self,
                session_id=_require_current_lineage_member(
                    state.current_session_id,
                    "session_id",
                ),
                protocol_hash=_require_current_lineage_member(
                    state.current_protocol_hash,
                    "protocol_hash",
                ),
                connection_id=_require_current_lineage_member(
                    state.current_connection_id,
                    "connection_id",
                ),
                symbol=symbol,
                symbol_ordinal=symbol_ordinal,
                trigger=trigger,
                trigger_seq=trigger_seq,
                connection_generation=connection_generation,
                first_buffered_u=first_buffered_u,
                _factory_token=_PUBLIC_DEPTH_REST_REGISTERED_CYCLE_FACTORY_TOKEN_V8,
                _authority_capability=self._mint_capability,
            )
            for symbol_ordinal, (symbol, first_buffered_u) in validated_watermarks
        )

        # Preflight every affected slot before mutating any slot or the global seq.
        for registration in registrations:
            slot = state.symbols[registration.symbol_ordinal]
            latest = slot.pending_registration or slot.registration
            if (
                latest is not None
                and latest.connection_generation == connection_generation
                and registration.first_buffered_u <= latest.first_buffered_u
            ):
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "depth REST registered cycle did not strictly advance its first_buffered_u"
                )

        for registration in registrations:
            _install_registered_cycle_v8(
                state.symbols[registration.symbol_ordinal],
                registration,
            )
        state.current_trigger_seq = trigger_seq
        return registrations

    def issue_attempt(
        self,
        *,
        registration: PublicDepthRestRegisteredCycleV8,
        bridge_attempt: int,
    ) -> PublicDepthRestScheduledAttemptTokenV8:
        """Issue one attempt for an exact active registered cycle."""

        validate_public_depth_rest_registered_cycle_v8(
            registration,
            plan=self.plan,
            schedule_authority=self,
        )
        _require_generation_open_v8(self._state, operation="attempt issuance")
        _require_positive_int(bridge_attempt, "bridge_attempt")
        if bridge_attempt > self.plan.bridge_maximum_attempts:
            raise ValueError("depth REST bridge_attempt exceeds the exact plan bound")
        state = self._state
        slot = state.symbols[registration.symbol_ordinal]
        if slot.token is None:
            expected_bridge_attempt = 1
        else:
            if bridge_attempt <= slot.last_bridge_attempt:
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "depth REST bridge attempt was duplicated or replayed"
                )
            if slot.lifecycle is not _PublicDepthRestAttemptLifecycleV8.TERMINAL_ADMITTED:
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "previous depth REST attempt lacks terminal shared-ingress admission"
                )
            expected_bridge_attempt = slot.last_bridge_attempt + 1
        if bridge_attempt != expected_bridge_attempt:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST bridge attempts must be contiguous per trigger and symbol"
            )

        token = PublicDepthRestScheduledAttemptTokenV8(
            plan=self.plan,
            plan_sha256=self.plan_sha256,
            schedule_authority=self,
            registration=registration,
            session_id=registration.session_id,
            protocol_hash=registration.protocol_hash,
            connection_id=registration.connection_id,
            symbol=registration.symbol,
            symbol_ordinal=registration.symbol_ordinal,
            trigger=registration.trigger,
            trigger_seq=registration.trigger_seq,
            connection_generation=registration.connection_generation,
            first_buffered_u=registration.first_buffered_u,
            bridge_attempt=bridge_attempt,
            _factory_token=_PUBLIC_DEPTH_REST_SCHEDULED_ATTEMPT_FACTORY_TOKEN_V8,
            _authority_capability=self._mint_capability,
        )
        slot.last_bridge_attempt = bridge_attempt
        slot.token = token
        slot.lifecycle = _PublicDepthRestAttemptLifecycleV8.ISSUED
        return token

    def __copy__(self) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority cannot be copied"
        )

    def __deepcopy__(self, _memo: dict[int, object]) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority cannot be deep-copied"
        )

    def __reduce__(self) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority cannot be serialized"
        )

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority cannot be serialized"
        )


@dataclass(frozen=True, slots=True, eq=False)
class PublicDepthRestRegisteredCycleV8:
    """Factory-sealed proof that one callback registered one symbol cycle."""

    plan: ProvisionalDepthRestQualificationPlanV8 = field(repr=False)
    plan_sha256: str
    schedule_authority: PublicDepthRestScheduleAuthorityV8 = field(
        repr=False,
        compare=False,
    )
    session_id: str
    protocol_hash: str
    connection_id: str
    symbol: str
    symbol_ordinal: int
    trigger: PublicDepthSnapshotTriggerV8
    trigger_seq: int
    connection_generation: int
    first_buffered_u: int
    _factory_token: InitVar[object | None] = None
    _authority_capability: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    _authority_capability_seal: object = field(
        init=False,
        repr=False,
        compare=False,
    )
    _material_seal: tuple[object, ...] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(
        self,
        _factory_token: object | None,
        _authority_capability: object | None,
    ) -> None:
        if _factory_token is not _PUBLIC_DEPTH_REST_REGISTERED_CYCLE_FACTORY_TOKEN_V8:
            raise TypeError("PublicDepthRestRegisteredCycleV8 must be minted by its authority")
        if (
            type(self.schedule_authority) is not PublicDepthRestScheduleAuthorityV8
            or _authority_capability is not self.schedule_authority._mint_capability
        ):
            raise TypeError(
                "PublicDepthRestRegisteredCycleV8 requires its authority's exact mint capability"
            )
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_DEPTH_REST_REGISTERED_CYCLE_FACTORY_TOKEN_V8,
        )
        object.__setattr__(
            self,
            "_authority_capability_seal",
            _authority_capability,
        )
        _validate_registered_cycle_material_v8(self)
        object.__setattr__(
            self,
            "_material_seal",
            _public_depth_rest_registered_cycle_material_seal_v8(self),
        )

    def __copy__(self) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered-cycle capability cannot be copied"
        )

    def __deepcopy__(self, _memo: dict[int, object]) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered-cycle capability cannot be deep-copied"
        )

    def __reduce__(self) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered-cycle capability cannot be serialized"
        )

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered-cycle capability cannot be serialized"
        )


@dataclass(frozen=True, slots=True, eq=False)
class PublicDepthRestScheduledAttemptTokenV8:
    """One-shot capability for one trigger, symbol, and bridge attempt."""

    plan: ProvisionalDepthRestQualificationPlanV8 = field(repr=False)
    plan_sha256: str
    schedule_authority: PublicDepthRestScheduleAuthorityV8 = field(
        repr=False,
        compare=False,
    )
    registration: PublicDepthRestRegisteredCycleV8 = field(
        repr=False,
        compare=False,
    )
    session_id: str
    protocol_hash: str
    connection_id: str
    symbol: str
    symbol_ordinal: int
    trigger: PublicDepthSnapshotTriggerV8
    trigger_seq: int
    connection_generation: int
    first_buffered_u: int
    bridge_attempt: int
    _factory_token: InitVar[object | None] = None
    _authority_capability: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    _authority_capability_seal: object = field(
        init=False,
        repr=False,
        compare=False,
    )
    _material_seal: tuple[object, ...] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(
        self,
        _factory_token: object | None,
        _authority_capability: object | None,
    ) -> None:
        if _factory_token is not _PUBLIC_DEPTH_REST_SCHEDULED_ATTEMPT_FACTORY_TOKEN_V8:
            raise TypeError(
                "PublicDepthRestScheduledAttemptTokenV8 must be issued by its authority"
            )
        if (
            type(self.schedule_authority) is not PublicDepthRestScheduleAuthorityV8
            or _authority_capability is not self.schedule_authority._mint_capability
        ):
            raise TypeError(
                "PublicDepthRestScheduledAttemptTokenV8 requires its authority's "
                "exact mint capability"
            )
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_DEPTH_REST_SCHEDULED_ATTEMPT_FACTORY_TOKEN_V8,
        )
        object.__setattr__(
            self,
            "_authority_capability_seal",
            _authority_capability,
        )
        _validate_token_material_v8(self)
        object.__setattr__(
            self,
            "_material_seal",
            _public_depth_rest_scheduled_attempt_material_seal_v8(self),
        )

    def __copy__(self) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token cannot be copied"
        )

    def __deepcopy__(self, _memo: dict[int, object]) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token cannot be deep-copied"
        )

    def __reduce__(self) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token cannot be serialized"
        )

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token cannot be serialized"
        )


def create_public_depth_rest_schedule_authority_v8(
    plan: ProvisionalDepthRestQualificationPlanV8,
) -> PublicDepthRestScheduleAuthorityV8:
    """Create one exact-plan, process-local depth schedule issuer."""

    return PublicDepthRestScheduleAuthorityV8(
        plan=plan,
        _factory_token=_PUBLIC_DEPTH_REST_SCHEDULE_AUTHORITY_FACTORY_TOKEN_V8,
    )


def validate_public_depth_rest_schedule_authority_v8(
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
) -> None:
    """Fail unless the exact factory created this authority for this plan object."""

    if type(schedule_authority) is not PublicDepthRestScheduleAuthorityV8:
        raise TypeError("depth REST scheduling requires an exact schedule authority")
    if (
        getattr(schedule_authority, "_factory_seal", None)
        is not _PUBLIC_DEPTH_REST_SCHEDULE_AUTHORITY_FACTORY_TOKEN_V8
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority lacks factory provenance"
        )
    if schedule_authority.plan is not plan:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority belongs to a different plan"
        )
    validate_public_depth_rest_plan_v8(plan)
    if schedule_authority.plan_sha256 != public_depth_rest_plan_sha256_v8(plan):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority plan hash drifted"
        )
    if schedule_authority.symbol_census != plan.symbols:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority symbol census drifted"
        )
    if type(getattr(schedule_authority, "_mint_capability", None)) is not object:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority lacks its mint capability"
        )
    state = getattr(schedule_authority, "_state", None)
    if type(state) is not _PublicDepthRestScheduleStateV8:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority lacks bounded process state"
        )
    if getattr(
        schedule_authority, "_material_seal", None
    ) != _public_depth_rest_schedule_authority_material_v8(schedule_authority):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority immutable material changed"
        )
    if len(state.symbols) != len(schedule_authority.symbol_census) or any(
        type(slot) is not _PublicDepthRestSymbolScheduleStateV8 for slot in state.symbols
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST schedule authority symbol state differs from its census"
        )
    if type(state.generation_open) is not bool:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST generation-open state must be an exact boolean"
        )
    _require_nonnegative_int(state.current_trigger_seq, "current_trigger_seq")
    lineage_members = (
        state.current_session_id,
        state.current_protocol_hash,
        state.current_connection_id,
    )
    if state.current_connection_generation == 0:
        if state.generation_open:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "unadvanced depth REST authority cannot have an open generation"
            )
        if any(member is not None for member in lineage_members):
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "unadvanced depth REST authority unexpectedly retains lineage"
            )
    else:
        _require_positive_int(
            state.current_connection_generation,
            "current_connection_generation",
        )
        _require_bounded_identity(
            _require_current_lineage_member(state.current_session_id, "session_id"),
            "session_id",
        )
        _require_sha256(
            _require_current_lineage_member(
                state.current_protocol_hash,
                "protocol_hash",
            ),
            "protocol_hash",
        )
        _require_bounded_identity(
            _require_current_lineage_member(
                state.current_connection_id,
                "connection_id",
            ),
            "connection_id",
        )
    _validate_bounded_symbol_states_v8(schedule_authority)
    if not state.generation_open and any(
        slot.registration is not None
        or slot.pending_registration is not None
        or slot.token is not None
        or slot.lifecycle is not None
        or slot.last_bridge_attempt != 0
        for slot in state.symbols
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "retired depth REST generation retains schedule state"
        )


def validate_public_depth_rest_registered_cycle_v8(
    registration: PublicDepthRestRegisteredCycleV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
) -> None:
    """Fail unless this sealed registration is the symbol's active cycle."""

    if type(registration) is not PublicDepthRestRegisteredCycleV8:
        raise TypeError("depth REST issuance requires an exact registered cycle")
    _validate_registered_cycle_seals_v8(registration)
    if registration.plan is not plan:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle belongs to a different plan"
        )
    validate_public_depth_rest_schedule_authority_v8(
        schedule_authority,
        plan=plan,
    )
    if registration.schedule_authority is not schedule_authority:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle belongs to a different issuer"
        )
    _validate_registered_cycle_material_v8(registration)
    state = schedule_authority._state
    _require_generation_open_v8(state, operation="registered-cycle validation")
    if state.current_connection_generation != registration.connection_generation:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle belongs to a stale connection generation"
        )
    if (
        state.current_session_id != registration.session_id
        or state.current_protocol_hash != registration.protocol_hash
        or state.current_connection_id != registration.connection_id
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle lineage is no longer current"
        )
    slot = state.symbols[registration.symbol_ordinal]
    if slot.registration is not registration:
        if slot.pending_registration is registration:
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST registered cycle is pending terminal active-cycle admission"
            )
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle is no longer current"
        )


def public_depth_rest_registration_disposition_v8(
    registration: PublicDepthRestRegisteredCycleV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
) -> PublicDepthRestRegistrationDispositionV8:
    """Classify one authentic registration without treating supersession as corruption.

    Structural, factory, plan, and issuer mismatches still fail closed.  Only a
    capability that was genuinely minted by this authority can be reported as
    superseded.  This gives the bridge coordinator an explicit race outcome
    instead of forcing it to infer normal supersession from an exception.
    """

    if type(registration) is not PublicDepthRestRegisteredCycleV8:
        raise TypeError("depth REST disposition requires an exact registered cycle")
    _validate_registered_cycle_seals_v8(registration)
    if registration.plan is not plan:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle belongs to a different plan"
        )
    validate_public_depth_rest_schedule_authority_v8(
        schedule_authority,
        plan=plan,
    )
    if registration.schedule_authority is not schedule_authority:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle belongs to a different issuer"
        )
    _validate_registered_cycle_material_v8(registration)
    state = schedule_authority._state
    lineage_is_current = (
        state.current_connection_generation == registration.connection_generation
        and state.current_session_id == registration.session_id
        and state.current_protocol_hash == registration.protocol_hash
        and state.current_connection_id == registration.connection_id
    )
    if not lineage_is_current:
        return PublicDepthRestRegistrationDispositionV8.SUPERSEDED
    slot = state.symbols[registration.symbol_ordinal]
    if slot.registration is registration:
        if slot.lifecycle is None:
            return PublicDepthRestRegistrationDispositionV8.ACTIVE_READY
        if slot.lifecycle is _PublicDepthRestAttemptLifecycleV8.ISSUED:
            return PublicDepthRestRegistrationDispositionV8.ACTIVE_ISSUED
        if slot.lifecycle is _PublicDepthRestAttemptLifecycleV8.CLAIMED:
            return PublicDepthRestRegistrationDispositionV8.ACTIVE_CLAIMED
        if slot.lifecycle is _PublicDepthRestAttemptLifecycleV8.TERMINAL_ADMITTED:
            return PublicDepthRestRegistrationDispositionV8.ACTIVE_TERMINAL_ADMITTED
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST active registration has an unknown lifecycle"
        )
    if slot.pending_registration is registration:
        return PublicDepthRestRegistrationDispositionV8.PENDING
    return PublicDepthRestRegistrationDispositionV8.SUPERSEDED


def validate_public_depth_rest_scheduled_attempt_token_v8(
    token: PublicDepthRestScheduledAttemptTokenV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
) -> None:
    """Fail unless this is the issuer's exact current token for one symbol."""

    if type(token) is not PublicDepthRestScheduledAttemptTokenV8:
        raise TypeError("depth REST attempt requires an exact scheduled-attempt token")
    _validate_scheduled_attempt_token_seals_v8(token)
    if token.plan is not plan:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token belongs to a different plan"
        )
    validate_public_depth_rest_schedule_authority_v8(
        schedule_authority,
        plan=plan,
    )
    if token.schedule_authority is not schedule_authority:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token belongs to a different issuer"
        )
    _validate_token_material_v8(token)
    state = schedule_authority._state
    _require_generation_open_v8(state, operation="scheduled-attempt validation")
    if state.current_connection_generation != token.connection_generation:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token belongs to a stale connection generation"
        )
    if (
        state.current_session_id != token.session_id
        or state.current_protocol_hash != token.protocol_hash
        or state.current_connection_id != token.connection_id
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token lineage is no longer current"
        )
    slot = state.symbols[token.symbol_ordinal]
    if (
        slot.token is not token
        or slot.registration is not token.registration
        or slot.last_bridge_attempt != token.bridge_attempt
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token is no longer current"
        )


def consume_public_depth_rest_scheduled_attempt_token_v8(
    token: PublicDepthRestScheduledAttemptTokenV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
) -> None:
    """Atomically retire one issued schedule capability into claimed state."""

    validate_public_depth_rest_scheduled_attempt_token_v8(
        token,
        plan=plan,
        schedule_authority=schedule_authority,
    )
    slot = schedule_authority._state.symbols[token.symbol_ordinal]
    if slot.lifecycle is not _PublicDepthRestAttemptLifecycleV8.ISSUED:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt capability was already claimed or admitted"
        )
    slot.lifecycle = _PublicDepthRestAttemptLifecycleV8.CLAIMED


def assert_public_depth_rest_scheduled_attempt_token_consumed_v8(
    token: PublicDepthRestScheduledAttemptTokenV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
) -> None:
    """Fail unless this exact current one-shot capability has been claimed."""

    validate_public_depth_rest_scheduled_attempt_token_v8(
        token,
        plan=plan,
        schedule_authority=schedule_authority,
    )
    lifecycle = schedule_authority._state.symbols[token.symbol_ordinal].lifecycle
    if lifecycle not in (
        _PublicDepthRestAttemptLifecycleV8.CLAIMED,
        _PublicDepthRestAttemptLifecycleV8.TERMINAL_ADMITTED,
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt capability has not been claimed"
        )


def acknowledge_public_depth_rest_terminal_admission_v8(
    token: PublicDepthRestScheduledAttemptTokenV8,
    receipt: PublicDepthRestAdmissionReceiptV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
) -> None:
    """Acknowledge exact terminal queue admission for one claimed attempt."""

    validate_public_depth_rest_scheduled_attempt_token_v8(
        token,
        plan=plan,
        schedule_authority=schedule_authority,
    )
    slot = schedule_authority._state.symbols[token.symbol_ordinal]
    if slot.lifecycle is _PublicDepthRestAttemptLifecycleV8.ISSUED:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST capability must be claimed before terminal admission"
        )
    if slot.lifecycle is not _PublicDepthRestAttemptLifecycleV8.CLAIMED:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST terminal admission was already acknowledged"
        )
    record = validate_public_depth_rest_admission_receipt_v8(receipt, plan=plan)
    if receipt.plan is not plan:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST terminal admission belongs to a different plan authority"
        )
    payload = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
        record.payload_bytes(),
        plan=plan,
    )
    if (
        record.session_id != token.session_id
        or record.protocol_hash != token.protocol_hash
        or record.connection_id != token.connection_id
        or record.generation != token.connection_generation
        or payload.session_id != token.session_id
        or payload.protocol_hash != token.protocol_hash
        or payload.connection_id != token.connection_id
        or payload.symbol != token.symbol
        or payload.trigger != token.trigger
        or payload.trigger_seq != token.trigger_seq
        or payload.connection_generation != token.connection_generation
        or payload.first_buffered_u != token.first_buffered_u
        or payload.symbol_ordinal != token.symbol_ordinal
        or payload.bridge_attempt != token.bridge_attempt
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST terminal admission identity differs from its schedule token"
        )
    slot.lifecycle = _PublicDepthRestAttemptLifecycleV8.TERMINAL_ADMITTED
    if slot.pending_registration is not None:
        slot.registration = slot.pending_registration
        slot.pending_registration = None
        slot.last_bridge_attempt = 0
        slot.token = None
        slot.lifecycle = None


def _validate_registered_cycle_seals_v8(
    registration: PublicDepthRestRegisteredCycleV8,
) -> None:
    if (
        getattr(registration, "_factory_seal", None)
        is not _PUBLIC_DEPTH_REST_REGISTERED_CYCLE_FACTORY_TOKEN_V8
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle lacks authority provenance"
        )
    authority = registration.schedule_authority
    if type(authority) is not PublicDepthRestScheduleAuthorityV8 or getattr(
        registration, "_authority_capability_seal", None
    ) is not getattr(authority, "_mint_capability", None):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle has a foreign authority capability"
        )
    if getattr(
        registration, "_material_seal", None
    ) != _public_depth_rest_registered_cycle_material_seal_v8(registration):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle immutable material changed"
        )


def _validate_scheduled_attempt_token_seals_v8(
    token: PublicDepthRestScheduledAttemptTokenV8,
) -> None:
    if (
        getattr(token, "_factory_seal", None)
        is not _PUBLIC_DEPTH_REST_SCHEDULED_ATTEMPT_FACTORY_TOKEN_V8
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token lacks issuer provenance"
        )
    authority = token.schedule_authority
    if type(authority) is not PublicDepthRestScheduleAuthorityV8 or getattr(
        token, "_authority_capability_seal", None
    ) is not getattr(authority, "_mint_capability", None):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token has a foreign authority capability"
        )
    if getattr(
        token, "_material_seal", None
    ) != _public_depth_rest_scheduled_attempt_material_seal_v8(token):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token immutable material changed"
        )


def _validate_token_material_v8(token: PublicDepthRestScheduledAttemptTokenV8) -> None:
    if type(token.plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("depth REST scheduled-attempt token requires the exact v8 plan")
    validate_public_depth_rest_schedule_authority_v8(
        token.schedule_authority,
        plan=token.plan,
    )
    if token.plan_sha256 != public_depth_rest_plan_sha256_v8(token.plan):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST scheduled-attempt token plan hash drifted"
        )
    _validate_registered_cycle_seals_v8(token.registration)
    _validate_registered_cycle_material_v8(token.registration)
    if (
        token.registration.plan is not token.plan
        or token.registration.schedule_authority is not token.schedule_authority
        or token.registration.session_id != token.session_id
        or token.registration.protocol_hash != token.protocol_hash
        or token.registration.connection_id != token.connection_id
        or token.registration.symbol != token.symbol
        or token.registration.symbol_ordinal != token.symbol_ordinal
        or token.registration.trigger != token.trigger
        or token.registration.trigger_seq != token.trigger_seq
        or token.registration.connection_generation != token.connection_generation
        or token.registration.first_buffered_u != token.first_buffered_u
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST attempt identity differs from its registered cycle"
        )
    _require_bounded_identity(token.session_id, "session_id")
    _require_sha256(token.protocol_hash, "protocol_hash")
    _require_bounded_identity(token.connection_id, "connection_id")
    _validate_attempt_identity_v8(
        plan=token.plan,
        symbol=token.symbol,
        symbol_ordinal=token.symbol_ordinal,
        trigger=token.trigger,
        trigger_seq=token.trigger_seq,
        connection_generation=token.connection_generation,
        first_buffered_u=token.first_buffered_u,
        bridge_attempt=token.bridge_attempt,
    )


def _validate_registered_cycle_material_v8(
    registration: PublicDepthRestRegisteredCycleV8,
) -> None:
    if type(registration.plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("depth REST registered cycle requires the exact v8 plan")
    validate_public_depth_rest_schedule_authority_v8(
        registration.schedule_authority,
        plan=registration.plan,
    )
    if registration.plan_sha256 != public_depth_rest_plan_sha256_v8(registration.plan):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST registered cycle plan hash drifted"
        )
    _require_bounded_identity(registration.session_id, "session_id")
    _require_sha256(registration.protocol_hash, "protocol_hash")
    _require_bounded_identity(registration.connection_id, "connection_id")
    _validate_cycle_identity_v8(
        plan=registration.plan,
        symbol=registration.symbol,
        symbol_ordinal=registration.symbol_ordinal,
        trigger=registration.trigger,
        trigger_seq=registration.trigger_seq,
        connection_generation=registration.connection_generation,
        first_buffered_u=registration.first_buffered_u,
    )


def _public_depth_rest_schedule_authority_material_v8(
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
) -> tuple[object, ...]:
    return (
        id(schedule_authority),
        id(schedule_authority._factory_seal),
        id(schedule_authority._mint_capability),
        id(schedule_authority.plan),
        schedule_authority.plan_sha256,
        schedule_authority.symbol_census,
        id(schedule_authority._state),
    )


def _public_depth_rest_registered_cycle_material_seal_v8(
    registration: PublicDepthRestRegisteredCycleV8,
) -> tuple[object, ...]:
    return (
        id(registration),
        id(registration._factory_seal),
        id(registration._authority_capability_seal),
        id(registration.plan),
        registration.plan_sha256,
        id(registration.schedule_authority),
        registration.session_id,
        registration.protocol_hash,
        registration.connection_id,
        registration.symbol,
        registration.symbol_ordinal,
        registration.trigger,
        registration.trigger_seq,
        registration.connection_generation,
        registration.first_buffered_u,
    )


def _public_depth_rest_scheduled_attempt_material_seal_v8(
    token: PublicDepthRestScheduledAttemptTokenV8,
) -> tuple[object, ...]:
    return (
        id(token),
        id(token._factory_seal),
        id(token._authority_capability_seal),
        id(token.plan),
        token.plan_sha256,
        id(token.schedule_authority),
        id(token.registration),
        token.session_id,
        token.protocol_hash,
        token.connection_id,
        token.symbol,
        token.symbol_ordinal,
        token.trigger,
        token.trigger_seq,
        token.connection_generation,
        token.first_buffered_u,
        token.bridge_attempt,
    )


def _validate_attempt_identity_v8(
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    symbol: str,
    symbol_ordinal: int,
    trigger: PublicDepthSnapshotTriggerV8,
    trigger_seq: int,
    connection_generation: int,
    first_buffered_u: int,
    bridge_attempt: int,
) -> None:
    _validate_cycle_identity_v8(
        plan=plan,
        symbol=symbol,
        symbol_ordinal=symbol_ordinal,
        trigger=trigger,
        trigger_seq=trigger_seq,
        connection_generation=connection_generation,
        first_buffered_u=first_buffered_u,
    )
    _require_positive_int(bridge_attempt, "bridge_attempt")
    if bridge_attempt > plan.bridge_maximum_attempts:
        raise ValueError("depth REST bridge_attempt exceeds the exact plan bound")


def _validate_cycle_identity_v8(
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    symbol: str,
    symbol_ordinal: int,
    trigger: PublicDepthSnapshotTriggerV8,
    trigger_seq: int,
    connection_generation: int,
    first_buffered_u: int,
) -> None:
    if type(trigger) is not str or trigger not in plan.snapshot_triggers:
        raise ValueError("depth REST trigger is outside the exact plan")
    _require_positive_int(trigger_seq, "trigger_seq")
    _require_positive_int(connection_generation, "connection_generation")
    _require_nonnegative_int(first_buffered_u, "first_buffered_u")
    _require_nonnegative_int(symbol_ordinal, "symbol_ordinal")
    if symbol_ordinal >= len(plan.symbols) or plan.symbols[symbol_ordinal] != symbol:
        raise ValueError("depth REST symbol differs from its exact plan ordinal")


def _validate_trigger_registration_v8(
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    trigger: PublicDepthSnapshotTriggerV8,
    symbol_watermarks: tuple[tuple[str, int], ...],
) -> tuple[tuple[int, tuple[str, int]], ...]:
    if type(trigger) is not str or trigger not in plan.snapshot_triggers:
        raise ValueError("depth REST trigger is outside the exact plan")
    if type(symbol_watermarks) is not tuple or any(
        type(item) is not tuple or len(item) != 2 for item in symbol_watermarks
    ):
        raise ValueError("depth REST symbol_watermarks must be exact symbol/value tuples")
    for item in symbol_watermarks:
        symbol, first_buffered_u = item
        if type(symbol) is not str:
            raise ValueError("depth REST watermark symbol must be a string")
        _require_nonnegative_int(first_buffered_u, "first_buffered_u")
    if trigger in ("startup", "reconnect"):
        if tuple(symbol for symbol, _ in symbol_watermarks) != plan.symbols:
            raise ValueError(
                "startup/reconnect depth REST registration requires the exact sorted symbol census"
            )
    elif trigger == "sequence_gap":
        if len(symbol_watermarks) != 1:
            raise ValueError("sequence_gap depth REST registration requires exactly one symbol")
        if symbol_watermarks[0][0] not in plan.symbols:
            raise ValueError("sequence_gap depth REST registration symbol is outside the census")
    else:  # pragma: no cover - exact plan validation makes this unreachable
        raise ValueError("depth REST trigger has no registration policy")
    ordinal_by_symbol = {symbol: ordinal for ordinal, symbol in enumerate(plan.symbols)}
    return tuple(
        (ordinal_by_symbol[symbol], (symbol, first_buffered_u))
        for symbol, first_buffered_u in symbol_watermarks
    )


def _install_registered_cycle_v8(
    slot: _PublicDepthRestSymbolScheduleStateV8,
    registration: PublicDepthRestRegisteredCycleV8,
) -> None:
    if slot.lifecycle is _PublicDepthRestAttemptLifecycleV8.CLAIMED:
        slot.pending_registration = registration
        return
    slot.registration = registration
    slot.pending_registration = None
    slot.last_bridge_attempt = 0
    slot.token = None
    slot.lifecycle = None


def _validate_bounded_symbol_states_v8(
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
) -> None:
    state = schedule_authority._state
    for symbol_ordinal, slot in enumerate(state.symbols):
        if slot.registration is None:
            if (
                slot.pending_registration is not None
                or slot.token is not None
                or slot.lifecycle is not None
                or slot.last_bridge_attempt != 0
            ):
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "empty depth REST symbol slot retains cycle state"
                )
            continue
        _validate_registered_cycle_material_without_authority_v8(
            slot.registration,
            schedule_authority,
            expected_symbol_ordinal=symbol_ordinal,
        )
        if (
            slot.lifecycle is not None
            and type(slot.lifecycle) is not _PublicDepthRestAttemptLifecycleV8
        ):
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST symbol lifecycle has a foreign type"
            )
        if (slot.token is None) != (slot.lifecycle is None):
            raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                "depth REST symbol token and lifecycle state diverged"
            )
        if slot.token is None:
            if slot.last_bridge_attempt != 0:
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "unissued depth REST cycle retains a bridge attempt"
                )
        else:
            _validate_token_material_without_authority_v8(
                slot.token,
                schedule_authority,
                slot.registration,
                expected_bridge_attempt=slot.last_bridge_attempt,
            )
        if slot.pending_registration is not None:
            _validate_registered_cycle_material_without_authority_v8(
                slot.pending_registration,
                schedule_authority,
                expected_symbol_ordinal=symbol_ordinal,
            )
            if slot.lifecycle is not _PublicDepthRestAttemptLifecycleV8.CLAIMED:
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "pending depth REST cycle requires one claimed active attempt"
                )
            if (
                slot.pending_registration.first_buffered_u <= slot.registration.first_buffered_u
                or slot.pending_registration.trigger_seq <= slot.registration.trigger_seq
            ):
                raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
                    "pending depth REST cycle identity did not advance"
                )


def _validate_registered_cycle_material_without_authority_v8(
    registration: PublicDepthRestRegisteredCycleV8,
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
    *,
    expected_symbol_ordinal: int,
) -> None:
    if (
        type(registration) is not PublicDepthRestRegisteredCycleV8
        or getattr(registration, "_factory_seal", None)
        is not _PUBLIC_DEPTH_REST_REGISTERED_CYCLE_FACTORY_TOKEN_V8
        or registration.plan is not schedule_authority.plan
        or registration.schedule_authority is not schedule_authority
        or registration._authority_capability_seal is not schedule_authority._mint_capability
        or registration._material_seal
        != _public_depth_rest_registered_cycle_material_seal_v8(registration)
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST symbol slot retains a foreign or tampered registration"
        )
    state = schedule_authority._state
    if registration.plan_sha256 != schedule_authority.plan_sha256:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST symbol registration plan hash drifted"
        )
    if (
        registration.session_id != state.current_session_id
        or registration.protocol_hash != state.current_protocol_hash
        or registration.connection_id != state.current_connection_id
        or registration.connection_generation != state.current_connection_generation
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST symbol registration lineage drifted"
        )
    _validate_cycle_identity_v8(
        plan=schedule_authority.plan,
        symbol=registration.symbol,
        symbol_ordinal=registration.symbol_ordinal,
        trigger=registration.trigger,
        trigger_seq=registration.trigger_seq,
        connection_generation=registration.connection_generation,
        first_buffered_u=registration.first_buffered_u,
    )
    if (
        registration.symbol_ordinal != expected_symbol_ordinal
        or registration.trigger_seq > state.current_trigger_seq
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST symbol registration differs from its bounded slot"
        )


def _validate_token_material_without_authority_v8(
    token: PublicDepthRestScheduledAttemptTokenV8,
    schedule_authority: PublicDepthRestScheduleAuthorityV8,
    registration: PublicDepthRestRegisteredCycleV8,
    *,
    expected_bridge_attempt: int,
) -> None:
    if (
        type(token) is not PublicDepthRestScheduledAttemptTokenV8
        or getattr(token, "_factory_seal", None)
        is not _PUBLIC_DEPTH_REST_SCHEDULED_ATTEMPT_FACTORY_TOKEN_V8
        or token.plan is not schedule_authority.plan
        or token.plan_sha256 != schedule_authority.plan_sha256
        or token.schedule_authority is not schedule_authority
        or token._authority_capability_seal is not schedule_authority._mint_capability
        or token._material_seal != _public_depth_rest_scheduled_attempt_material_seal_v8(token)
        or token.registration is not registration
        or token.bridge_attempt != expected_bridge_attempt
        or token.session_id != registration.session_id
        or token.protocol_hash != registration.protocol_hash
        or token.connection_id != registration.connection_id
        or token.symbol != registration.symbol
        or token.symbol_ordinal != registration.symbol_ordinal
        or token.trigger != registration.trigger
        or token.trigger_seq != registration.trigger_seq
        or token.connection_generation != registration.connection_generation
        or token.first_buffered_u != registration.first_buffered_u
    ):
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST symbol token differs from its active cycle"
        )
    _require_positive_int(expected_bridge_attempt, "last_bridge_attempt")
    if expected_bridge_attempt > schedule_authority.plan.bridge_maximum_attempts:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            "depth REST symbol token exceeds its attempt bound"
        )


def _require_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or not 0 <= value <= _MAX_SIGNED_INT64:
        raise ValueError(f"{field_name} must be a nonnegative signed 64-bit integer")


def _require_positive_int(value: int, field_name: str) -> None:
    if type(value) is not int or not 1 <= value <= _MAX_SIGNED_INT64:
        raise ValueError(f"{field_name} must be a positive signed 64-bit integer")


def _require_bounded_identity(value: str, field_name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field_name} must be a bounded normalized identity")


def _require_sha256(value: str, field_name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")


def _require_current_lineage_member(value: str | None, field_name: str) -> str:
    if value is None:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            f"current depth REST lineage lacks {field_name}"
        )
    return value


def _require_generation_open_v8(
    state: _PublicDepthRestScheduleStateV8,
    *,
    operation: str,
) -> None:
    if not state.generation_open:
        raise PublicDepthRestScheduledAttemptOwnershipErrorV8(
            f"retired depth REST generation forbids {operation}"
        )
