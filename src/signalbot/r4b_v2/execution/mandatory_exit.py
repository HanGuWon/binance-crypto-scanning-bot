from __future__ import annotations

import hashlib
import json
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
from enum import StrEnum
from fractions import Fraction
from typing import Final

from signalbot.r4b_v2.alerts.actionability import (
    AlertTransportTimesV2,
    PromotingFamilyV2,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.fees import (
    FeeVersionResolutionV2,
    canonical_fee_version_resolution_v2,
)
from signalbot.r4b_v2.execution.paper_fok import (
    CausalMarkPriceEvidenceV2,
    CommonQuantityGridV2,
    ContinuousBookHealthEvidenceV2,
    DepthLevelV2,
    FuturesDepthContinuityWitnessV2,
    FuturesDepthSnapshotV2,
    FuturesExchangeInfoEvidenceV2,
    FuturesFrozenBookV2,
    FuturesStandardDepthEventV2,
    PaperFokClosureEvidenceV2,
    PaperFokClosureMethodV2,
    PaperFokFullFillCertificateV2,
    PaperFokLineageV2,
    PaperFokSideV2,
    QuietRestSnapshotEvidenceV2,
    RawQuantityFilterV2,
    canonical_paper_fok_full_fill_certificate_v2,
    classify_futures_book_closure_v2,
    decimal_fraction_v2,
    finite_base10_fraction_v2,
    futures_level_passes_official_bounds_v2,
    intersect_quantity_filters_v2,
    is_price_tick_aligned_v2,
    multiply_protocol_decimals_exact_v2,
    reconstruct_futures_standard_book_v2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2

MANDATORY_EXIT_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.3.1_MANDATORY_USDM_POST_ENTRY_EXIT"
)
EXIT_ACK_TARGET_DELAY_MS_V2: Final = 10_000
EXIT_MISSING_ACK_EMERGENCY_DELAY_MS_V2: Final = 15_000
EXIT_RETRY_WINDOW_MS_V2: Final = 30_000
PRIMARY_EXIT_DEPTH_HAIRCUT_V2: Final = Decimal("0.50")

_TARGET_EVIDENCE_DOMAIN: Final = b"R4B_MANDATORY_EXIT_TARGET_V2\0"
_POSITION_ID_DOMAIN: Final = b"R4B_MANDATORY_EXIT_POSITION_V2\0"
_INTENT_ID_DOMAIN: Final = b"R4B_MANDATORY_EXIT_INTENT_V2\0"
_INTENT_PAYLOAD_DOMAIN: Final = b"R4B_MANDATORY_EXIT_INTENT_PAYLOAD_V2\0"
_GENERATION_ID_DOMAIN: Final = b"R4B_MANDATORY_EXIT_GENERATION_V2\0"
_GENERATION_EVIDENCE_DOMAIN: Final = b"R4B_MANDATORY_EXIT_EVIDENCE_V2\0"
_ATTEMPT_PAYLOAD_DOMAIN: Final = b"R4B_MANDATORY_EXIT_ATTEMPT_V2\0"
_TERMINAL_ID_DOMAIN: Final = b"R4B_MANDATORY_EXIT_TERMINAL_V2\0"
_TERMINAL_PAYLOAD_DOMAIN: Final = b"R4B_MANDATORY_EXIT_TERMINAL_PAYLOAD_V2\0"
_REPLAY_ROOT_DOMAIN: Final = b"R4B_MANDATORY_EXIT_REPLAY_ROOT_V2\0"
_STATE_ROOT_DOMAIN: Final = b"R4B_MANDATORY_EXIT_STATE_ROOT_V2\0"
_CHECKPOINT_DOMAIN: Final = b"R4B_MANDATORY_EXIT_CHECKPOINT_V2\0"
_FEE_CERTIFICATE_DOMAIN: Final = b"R4B_MANDATORY_EXIT_FEE_CERTIFICATE_V2\0"
_STATE_SCHEMA: Final = "r4b_mandatory_exit_ledger_state_v2"
_FEE_CERTIFICATE_SCHEMA: Final = "r4b_mandatory_exit_fee_certificate_v2"
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_TARGET_FACTORY_TOKEN: Final = object()
_POSITION_FACTORY_TOKEN: Final = object()
_INTENT_FACTORY_TOKEN: Final = object()
_ATTEMPT_FACTORY_TOKEN: Final = object()
_TERMINAL_FACTORY_TOKEN: Final = object()
_FEE_CERTIFICATE_FACTORY_TOKEN: Final = object()


class MandatoryExitContractErrorV2(ValueError):
    """Raised when post-entry PAPER liquidation evidence is contradictory."""


class MandatoryExitPositionSideV2(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class MandatoryExitTargetModeV2(StrEnum):
    ACK_PLUS_10000 = "ACK_PLUS_10000"
    MISSING_ACK_EMERGENCY_PLUS_15000 = "MISSING_ACK_EMERGENCY_PLUS_15000"


class MandatoryExitAttemptStatusV2(StrEnum):
    CLOSURE_PENDING = "CLOSURE_PENDING"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    INCONCLUSIVE_FILTER = "INCONCLUSIVE_FILTER"
    NO_FILL = "NO_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    FULL_FILL = "FULL_FILL"


class MandatoryExitTerminalStatusV2(StrEnum):
    EXITED_FULL = "EXITED_FULL"
    DUST_RESIDUAL_RETAINED = "DUST_RESIDUAL_RETAINED"
    POST_ENTRY_UNRESOLVED_EXIT = "POST_ENTRY_UNRESOLVED_EXIT"


class MandatoryExitRegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


@dataclass(frozen=True, slots=True)
class MandatoryExitTargetCursorV2:
    """Clock- and transport-bound target for a mandatory exit action."""

    exit_decision_cutoff_ms: int
    transport_times: AlertTransportTimesV2
    transport_ledger_checkpoint_sha256: str
    target_venue_ms: int
    prior_local_cursor_ms: int
    prior_venue_lower_bound_ms: int
    target_local_cursor_ms: int
    target_venue_lower_bound_ms: int
    clock_segment_root_sha256: str
    contiguous_cursor_evidence: bool
    _factory_token: InitVar[object] = None
    mode: MandatoryExitTargetModeV2 = field(init=False)
    cursor_evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _TARGET_FACTORY_TOKEN:
            raise MandatoryExitContractErrorV2(
                "exit target cursor must be created from typed transport evidence"
            )
        for name in (
            "exit_decision_cutoff_ms",
            "target_venue_ms",
            "prior_local_cursor_ms",
            "prior_venue_lower_bound_ms",
            "target_local_cursor_ms",
            "target_venue_lower_bound_ms",
        ):
            _validate_nonnegative_int(getattr(self, name), name)
        if not isinstance(self.transport_times, AlertTransportTimesV2):
            raise MandatoryExitContractErrorV2(
                "transport_times must be AlertTransportTimesV2"
            )
        _validate_sha256(
            self.transport_ledger_checkpoint_sha256,
            "transport_ledger_checkpoint_sha256",
        )
        _validate_sha256(
            self.clock_segment_root_sha256,
            "clock_segment_root_sha256",
        )
        if type(self.contiguous_cursor_evidence) is not bool:
            raise MandatoryExitContractErrorV2(
                "contiguous_cursor_evidence must be boolean"
            )
        if not self.contiguous_cursor_evidence:
            raise MandatoryExitContractErrorV2(
                "exit target requires contiguous online clock evidence"
            )
        ack_ms = self.transport_times.provider_acceptance_completion_ms
        if ack_ms is None:
            mode = MandatoryExitTargetModeV2.MISSING_ACK_EMERGENCY_PLUS_15000
            expected_target = (
                self.exit_decision_cutoff_ms
                + EXIT_MISSING_ACK_EMERGENCY_DELAY_MS_V2
            )
        else:
            if ack_ms < self.exit_decision_cutoff_ms:
                raise MandatoryExitContractErrorV2(
                    "provider acceptance cannot precede the exit decision"
                )
            mode = MandatoryExitTargetModeV2.ACK_PLUS_10000
            expected_target = ack_ms + EXIT_ACK_TARGET_DELAY_MS_V2
        if self.target_venue_ms != expected_target:
            raise MandatoryExitContractErrorV2(
                "exit target differs from the frozen ack/emergency timing rule"
            )
        if self.prior_local_cursor_ms >= self.target_local_cursor_ms:
            raise MandatoryExitContractErrorV2(
                "prior local cursor must strictly precede target cursor"
            )
        if not (
            self.prior_venue_lower_bound_ms
            < self.target_venue_ms
            <= self.target_venue_lower_bound_ms
        ):
            raise MandatoryExitContractErrorV2(
                "clock cursor must straddle the exact exit target"
            )
        object.__setattr__(self, "mode", mode)
        object.__setattr__(
            self,
            "cursor_evidence_sha256",
            hashlib.sha256(
                _TARGET_EVIDENCE_DOMAIN
                + canonical_json_line(_target_cursor_document(self))
            ).hexdigest(),
        )

    @property
    def missing_ack_makes_family_inconclusive(self) -> bool:
        return (
            self.mode
            is MandatoryExitTargetModeV2.MISSING_ACK_EMERGENCY_PLUS_15000
        )


def build_mandatory_exit_target_cursor_v2(
    *,
    exit_decision_cutoff_ms: int,
    transport_times: AlertTransportTimesV2,
    transport_ledger_checkpoint_sha256: str,
    target_venue_ms: int,
    prior_local_cursor_ms: int,
    prior_venue_lower_bound_ms: int,
    target_local_cursor_ms: int,
    target_venue_lower_bound_ms: int,
    clock_segment_root_sha256: str,
    contiguous_cursor_evidence: bool = True,
) -> MandatoryExitTargetCursorV2:
    """Build the exact ack+10s or missing-ack emergency+15s target."""

    return MandatoryExitTargetCursorV2(
        exit_decision_cutoff_ms=exit_decision_cutoff_ms,
        transport_times=transport_times,
        transport_ledger_checkpoint_sha256=(
            transport_ledger_checkpoint_sha256
        ),
        target_venue_ms=target_venue_ms,
        prior_local_cursor_ms=prior_local_cursor_ms,
        prior_venue_lower_bound_ms=prior_venue_lower_bound_ms,
        target_local_cursor_ms=target_local_cursor_ms,
        target_venue_lower_bound_ms=target_venue_lower_bound_ms,
        clock_segment_root_sha256=clock_segment_root_sha256,
        contiguous_cursor_evidence=contiguous_cursor_evidence,
        _factory_token=_TARGET_FACTORY_TOKEN,
    )


@dataclass(frozen=True, slots=True)
class MandatoryExitPositionV2:
    attempt_id: str
    family: PromotingFamilyV2
    entry_signal_event_id: str
    entry_execution_event_id: str
    entry_execution_payload_sha256: str
    entry_execution_evidence_sha256: str
    entry_execution_certificate_sha256: str
    entry_registry_replay_root_sha256: str
    entry_registry_checkpoint_sha256: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    side: MandatoryExitPositionSideV2
    entry_target_venue_ms: int
    initial_quantity: Decimal
    entry_vwap: Decimal
    entry_notional: Decimal
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _POSITION_FACTORY_TOKEN:
            raise MandatoryExitContractErrorV2(
                "mandatory exit position requires a PAPER full-fill certificate"
            )
        _validate_identity(self.attempt_id, "attempt_id")
        if not isinstance(self.family, PromotingFamilyV2):
            raise MandatoryExitContractErrorV2("family must be A, B, or C")
        for value, name in (
            (self.entry_signal_event_id, "entry_signal_event_id"),
            (self.entry_execution_event_id, "entry_execution_event_id"),
            (
                self.entry_execution_payload_sha256,
                "entry_execution_payload_sha256",
            ),
            (
                self.entry_execution_evidence_sha256,
                "entry_execution_evidence_sha256",
            ),
            (
                self.entry_execution_certificate_sha256,
                "entry_execution_certificate_sha256",
            ),
            (
                self.entry_registry_replay_root_sha256,
                "entry_registry_replay_root_sha256",
            ),
            (
                self.entry_registry_checkpoint_sha256,
                "entry_registry_checkpoint_sha256",
            ),
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
        ):
            _validate_sha256(value, name)
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise MandatoryExitContractErrorV2(
                "mandatory exit position must remain USD-M Futures"
            )
        if not isinstance(self.side, MandatoryExitPositionSideV2):
            raise MandatoryExitContractErrorV2("position side must be LONG or SHORT")
        _validate_nonnegative_int(
            self.entry_target_venue_ms,
            "entry_target_venue_ms",
        )
        for value, name in (
            (self.initial_quantity, "initial_quantity"),
            (self.entry_vwap, "entry_vwap"),
            (self.entry_notional, "entry_notional"),
        ):
            _validate_positive_decimal(value, name)
        expected_notional = multiply_protocol_decimals_exact_v2(
            self.initial_quantity,
            self.entry_vwap,
        )
        if self.entry_notional != expected_notional:
            raise MandatoryExitContractErrorV2(
                "entry certificate quantity, VWAP, and notional disagree"
            )
        object.__setattr__(
            self,
            "event_id",
            hashlib.sha256(
                _POSITION_ID_DOMAIN + canonical_json_line(_position_document(self))
            ).hexdigest(),
        )

    @property
    def exit_side(self) -> PaperFokSideV2:
        return (
            PaperFokSideV2.SELL
            if self.side is MandatoryExitPositionSideV2.LONG
            else PaperFokSideV2.BUY
        )


def mandatory_exit_position_from_certificate_v2(
    certificate: PaperFokFullFillCertificateV2,
    *,
    family: PromotingFamilyV2,
) -> MandatoryExitPositionV2:
    """Freeze an opened position from the registry-issued entry certificate."""

    canonical_paper_fok_full_fill_certificate_v2(certificate)
    if not isinstance(family, PromotingFamilyV2):
        raise MandatoryExitContractErrorV2("family must be A, B, or C")
    side = (
        MandatoryExitPositionSideV2.LONG
        if certificate.side is PaperFokSideV2.BUY
        else MandatoryExitPositionSideV2.SHORT
    )
    return MandatoryExitPositionV2(
        attempt_id=certificate.attempt_id,
        family=family,
        entry_signal_event_id=certificate.signal_event_id,
        entry_execution_event_id=certificate.decision_event_id,
        entry_execution_payload_sha256=certificate.decision_payload_sha256,
        entry_execution_evidence_sha256=certificate.evidence_sha256,
        entry_execution_certificate_sha256=certificate.certificate_sha256,
        entry_registry_replay_root_sha256=(
            certificate.terminal_registry_replay_root_sha256
        ),
        entry_registry_checkpoint_sha256=(
            certificate.terminal_registry_checkpoint_sha256
        ),
        symbol=certificate.symbol,
        venue=certificate.venue,
        promoting_plan_sha256=certificate.promoting_plan_sha256,
        side=side,
        entry_target_venue_ms=certificate.target_venue_ms,
        initial_quantity=certificate.filled_quantity,
        entry_vwap=certificate.executable_vwap,
        entry_notional=certificate.executable_notional,
        _factory_token=_POSITION_FACTORY_TOKEN,
    )


@dataclass(frozen=True, slots=True)
class MandatoryExitIntentV2:
    position_event_id: str
    entry_signal_event_id: str
    attempt_id: str
    family: PromotingFamilyV2
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    exit_decision_event_id: str
    exit_decision_payload_sha256: str
    canonical_exit_decision_sha256: str
    family_exit_registry_checkpoint_sha256: str
    family_rule_version: str
    exit_reason: str
    exit_decision_cutoff_ms: int
    target_cursor: MandatoryExitTargetCursorV2
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _INTENT_FACTORY_TOKEN:
            raise MandatoryExitContractErrorV2(
                "mandatory exit intent requires canonical family authority"
            )
        for value, name in (
            (self.position_event_id, "position_event_id"),
            (self.entry_signal_event_id, "entry_signal_event_id"),
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
            (self.exit_decision_event_id, "exit_decision_event_id"),
            (
                self.exit_decision_payload_sha256,
                "exit_decision_payload_sha256",
            ),
            (
                self.canonical_exit_decision_sha256,
                "canonical_exit_decision_sha256",
            ),
            (
                self.family_exit_registry_checkpoint_sha256,
                "family_exit_registry_checkpoint_sha256",
            ),
        ):
            _validate_sha256(value, name)
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_identity(self.family_rule_version, "family_rule_version")
        _validate_identity(self.exit_reason, "exit_reason")
        if not isinstance(self.family, PromotingFamilyV2):
            raise MandatoryExitContractErrorV2("family must be A, B, or C")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise MandatoryExitContractErrorV2("exit intent must be USD-M Futures")
        _validate_nonnegative_int(
            self.exit_decision_cutoff_ms,
            "exit_decision_cutoff_ms",
        )
        if not isinstance(self.target_cursor, MandatoryExitTargetCursorV2):
            raise MandatoryExitContractErrorV2(
                "target_cursor must be MandatoryExitTargetCursorV2"
            )
        if (
            self.target_cursor.exit_decision_cutoff_ms
            != self.exit_decision_cutoff_ms
        ):
            raise MandatoryExitContractErrorV2(
                "exit decision cutoff differs from target cursor"
            )
        identity = _intent_identity_document(self)
        object.__setattr__(
            self,
            "event_id",
            hashlib.sha256(
                _INTENT_ID_DOMAIN + canonical_json_line(identity)
            ).hexdigest(),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            hashlib.sha256(
                _INTENT_PAYLOAD_DOMAIN
                + canonical_json_line(_intent_document(self, include_payload=False))
            ).hexdigest(),
        )

    @property
    def retry_deadline_venue_ms(self) -> int:
        return self.target_cursor.target_venue_ms + EXIT_RETRY_WINDOW_MS_V2


def build_mandatory_exit_intent_v2(
    position: MandatoryExitPositionV2,
    *,
    exit_decision_event_id: str,
    exit_decision_payload_sha256: str,
    canonical_exit_decision: bytes,
    family_exit_registry_checkpoint_sha256: str,
    family_rule_version: str,
    exit_reason: str,
    exit_decision_cutoff_ms: int,
    target_cursor: MandatoryExitTargetCursorV2,
) -> MandatoryExitIntentV2:
    """Bind a canonical family exit decision to one certified position.

    The external family registry checkpoint is retained because this module is
    not the authoritative A/B/C decision-ledger owner.
    """

    if not isinstance(position, MandatoryExitPositionV2):
        raise MandatoryExitContractErrorV2("position has the wrong type")
    if type(canonical_exit_decision) is not bytes or not canonical_exit_decision:
        raise MandatoryExitContractErrorV2(
            "canonical_exit_decision must be nonempty bytes"
        )
    if len(canonical_exit_decision) > 1_000_000:
        raise MandatoryExitContractErrorV2("canonical exit decision is too large")
    return MandatoryExitIntentV2(
        position_event_id=position.event_id,
        entry_signal_event_id=position.entry_signal_event_id,
        attempt_id=position.attempt_id,
        family=position.family,
        symbol=position.symbol,
        venue=position.venue,
        promoting_plan_sha256=position.promoting_plan_sha256,
        exit_decision_event_id=exit_decision_event_id,
        exit_decision_payload_sha256=exit_decision_payload_sha256,
        canonical_exit_decision_sha256=hashlib.sha256(
            canonical_exit_decision
        ).hexdigest(),
        family_exit_registry_checkpoint_sha256=(
            family_exit_registry_checkpoint_sha256
        ),
        family_rule_version=family_rule_version,
        exit_reason=exit_reason,
        exit_decision_cutoff_ms=exit_decision_cutoff_ms,
        target_cursor=target_cursor,
        _factory_token=_INTENT_FACTORY_TOKEN,
    )


def build_mandatory_exit_intent_from_family_decision_v2(
    position: MandatoryExitPositionV2,
    decision: object,
    *,
    family_exit_registry_checkpoint_sha256: str,
    target_cursor: MandatoryExitTargetCursorV2,
) -> MandatoryExitIntentV2:
    """Canonical adapter for the existing A/B/C exit decision owners."""

    from signalbot.r4b_v2.strategy.family_a import (
        FamilyAExitDecisionV2,
        canonical_family_a_exit_decision_v2,
    )
    from signalbot.r4b_v2.strategy.family_b import (
        FamilyBExitDecisionV2,
        canonical_family_b_exit_decision_v2,
    )
    from signalbot.r4b_v2.strategy.family_c import (
        FamilyCExitDecisionV2,
        canonical_family_c_exit_decision_v2,
    )

    if isinstance(decision, FamilyAExitDecisionV2):
        family = PromotingFamilyV2.A
        canonical = canonical_family_a_exit_decision_v2(decision)
    elif isinstance(decision, FamilyBExitDecisionV2):
        family = PromotingFamilyV2.B
        canonical = canonical_family_b_exit_decision_v2(decision)
    elif isinstance(decision, FamilyCExitDecisionV2):
        family = PromotingFamilyV2.C
        canonical = canonical_family_c_exit_decision_v2(decision)
    else:
        raise MandatoryExitContractErrorV2(
            "decision must be an existing Family A/B/C exit decision"
        )
    if family is not position.family:
        raise MandatoryExitContractErrorV2("exit decision family differs from position")
    if not decision.exits_position:
        raise MandatoryExitContractErrorV2("HOLD cannot schedule a mandatory exit")
    if decision.entry_event_id != position.entry_signal_event_id:
        raise MandatoryExitContractErrorV2(
            "family exit decision refers to a different entry signal"
        )
    identity = (
        decision.attempt_id,
        decision.symbol,
        decision.venue,
        decision.promoting_plan_sha256,
    )
    expected = (
        position.attempt_id,
        position.symbol,
        position.venue,
        position.promoting_plan_sha256,
    )
    if identity != expected:
        raise MandatoryExitContractErrorV2(
            "family exit decision identity differs from certified position"
        )
    return build_mandatory_exit_intent_v2(
        position,
        exit_decision_event_id=decision.event_id,
        exit_decision_payload_sha256=decision.payload_sha256,
        canonical_exit_decision=canonical,
        family_exit_registry_checkpoint_sha256=(
            family_exit_registry_checkpoint_sha256
        ),
        family_rule_version=decision.rule_version,
        exit_reason=decision.reason.value,
        exit_decision_cutoff_ms=decision.decision_cutoff_ms,
        target_cursor=target_cursor,
    )


@dataclass(frozen=True, slots=True)
class MandatoryExitBookGenerationV2:
    """All causal public evidence for one target or one later depth generation."""

    position_event_id: str
    intent_event_id: str
    lineage: PaperFokLineageV2
    generation_venue_ms: int
    generation_local_cursor_ms: int
    target_state_last_ingest_seq: int
    snapshot: FuturesDepthSnapshotV2
    pre_generation_depth_events: tuple[FuturesStandardDepthEventV2, ...]
    closure: PaperFokClosureEvidenceV2
    mark: CausalMarkPriceEvidenceV2
    exchange_info: FuturesExchangeInfoEvidenceV2
    fee_resolution: FeeVersionResolutionV2 | None = None
    event_id: str = field(init=False)
    evidence_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _validate_sha256(self.position_event_id, "position_event_id")
        _validate_sha256(self.intent_event_id, "intent_event_id")
        if not isinstance(self.lineage, PaperFokLineageV2):
            raise MandatoryExitContractErrorV2("lineage must be PaperFokLineageV2")
        _validate_nonnegative_int(
            self.generation_venue_ms,
            "generation_venue_ms",
        )
        _validate_nonnegative_int(
            self.generation_local_cursor_ms,
            "generation_local_cursor_ms",
        )
        _validate_nonnegative_int(
            self.target_state_last_ingest_seq,
            "target_state_last_ingest_seq",
        )
        if not isinstance(self.snapshot, FuturesDepthSnapshotV2):
            raise MandatoryExitContractErrorV2(
                "snapshot must be FuturesDepthSnapshotV2"
            )
        if type(self.pre_generation_depth_events) is not tuple or any(
            not isinstance(value, FuturesStandardDepthEventV2)
            for value in self.pre_generation_depth_events
        ):
            raise MandatoryExitContractErrorV2(
                "pre_generation_depth_events must be an immutable depth tuple"
            )
        if not isinstance(self.closure, PaperFokClosureEvidenceV2):
            raise MandatoryExitContractErrorV2("closure has the wrong type")
        if not isinstance(self.mark, CausalMarkPriceEvidenceV2):
            raise MandatoryExitContractErrorV2("mark has the wrong type")
        if not isinstance(self.exchange_info, FuturesExchangeInfoEvidenceV2):
            raise MandatoryExitContractErrorV2("exchange_info has the wrong type")
        if self.fee_resolution is not None and not isinstance(
            self.fee_resolution,
            FeeVersionResolutionV2,
        ):
            raise MandatoryExitContractErrorV2(
                "fee_resolution must be typed or absent"
            )
        identity = {
            "generation_venue_ms": self.generation_venue_ms,
            "intent_event_id": self.intent_event_id,
            "position_event_id": self.position_event_id,
            "role": "MANDATORY_EXIT_BOOK_GENERATION",
            "target_state_last_ingest_seq": self.target_state_last_ingest_seq,
        }
        object.__setattr__(
            self,
            "event_id",
            hashlib.sha256(
                _GENERATION_ID_DOMAIN + canonical_json_line(identity)
            ).hexdigest(),
        )
        object.__setattr__(
            self,
            "evidence_sha256",
            hashlib.sha256(
                _GENERATION_EVIDENCE_DOMAIN
                + canonical_json_line(_generation_evidence_document(self))
            ).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class MandatoryExitLevelFillV2:
    price: Decimal
    quantity: Decimal
    level_revision_ingest_seq: int

    def __post_init__(self) -> None:
        _validate_positive_decimal(self.price, "price")
        _validate_positive_decimal(self.quantity, "quantity")
        _validate_nonnegative_int(
            self.level_revision_ingest_seq,
            "level_revision_ingest_seq",
        )


@dataclass(frozen=True, slots=True)
class MandatoryExitAttemptV2:
    position_event_id: str
    intent_event_id: str
    generation_event_id: str
    generation_evidence_sha256: str
    generation_venue_ms: int
    generation_local_cursor_ms: int
    target_state_last_ingest_seq: int
    terminal_book_update_id: int | None
    status: MandatoryExitAttemptStatusV2
    closure_method: PaperFokClosureMethodV2
    exit_side: PaperFokSideV2
    residual_before: Decimal
    requested_order_quantity: Decimal
    filled_quantity: Decimal
    residual_after: Decimal
    executable_vwap: Decimal | None
    gross_notional: Decimal
    signed_gross_cashflow: Decimal
    level_fills: tuple[MandatoryExitLevelFillV2, ...]
    residual_is_filter_dust: bool
    primary_target_generation_missing: bool
    fee_resolution_event_id: str | None
    fee_resolution_payload_sha256: str | None
    fee_resolution_status: str | None
    reasons: tuple[str, ...]
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ATTEMPT_FACTORY_TOKEN:
            raise MandatoryExitContractErrorV2(
                "exit attempts must be created by the causal executor"
            )
        for value, name in (
            (self.position_event_id, "position_event_id"),
            (self.intent_event_id, "intent_event_id"),
            (self.generation_event_id, "generation_event_id"),
            (self.generation_evidence_sha256, "generation_evidence_sha256"),
        ):
            _validate_sha256(value, name)
        for name in (
            "generation_venue_ms",
            "generation_local_cursor_ms",
            "target_state_last_ingest_seq",
        ):
            _validate_nonnegative_int(getattr(self, name), name)
        if self.terminal_book_update_id is not None:
            _validate_nonnegative_int(
                self.terminal_book_update_id,
                "terminal_book_update_id",
            )
        if not isinstance(self.status, MandatoryExitAttemptStatusV2):
            raise MandatoryExitContractErrorV2("attempt status has the wrong type")
        if not isinstance(self.closure_method, PaperFokClosureMethodV2):
            raise MandatoryExitContractErrorV2("closure method has the wrong type")
        if not isinstance(self.exit_side, PaperFokSideV2):
            raise MandatoryExitContractErrorV2("exit side must be BUY or SELL")
        for value, name in (
            (self.residual_before, "residual_before"),
            (self.requested_order_quantity, "requested_order_quantity"),
            (self.filled_quantity, "filled_quantity"),
            (self.residual_after, "residual_after"),
            (self.gross_notional, "gross_notional"),
        ):
            _validate_nonnegative_decimal(value, name)
        if type(self.signed_gross_cashflow) is not Decimal or not (
            self.signed_gross_cashflow.is_finite()
        ):
            raise MandatoryExitContractErrorV2(
                "signed_gross_cashflow must be finite Decimal"
            )
        with localcontext(protocol_decimal_context_v2()):
            if self.residual_before != self.filled_quantity + self.residual_after:
                raise MandatoryExitContractErrorV2(
                    "attempt violates inventory conservation"
                )
        if self.requested_order_quantity > self.residual_before:
            raise MandatoryExitContractErrorV2(
                "requested exit quantity exceeds remaining inventory"
            )
        if self.filled_quantity > self.requested_order_quantity:
            raise MandatoryExitContractErrorV2(
                "filled exit quantity exceeds requested order quantity"
            )
        if type(self.level_fills) is not tuple or any(
            not isinstance(value, MandatoryExitLevelFillV2)
            for value in self.level_fills
        ):
            raise MandatoryExitContractErrorV2(
                "level_fills must be an immutable typed tuple"
            )
        with localcontext(protocol_decimal_context_v2()):
            level_quantity = sum(
                (value.quantity for value in self.level_fills),
                Decimal(0),
            )
            level_notional = sum(
                (value.price * value.quantity for value in self.level_fills),
                Decimal(0),
            )
        if level_quantity != self.filled_quantity:
            raise MandatoryExitContractErrorV2(
                "level fills do not conserve filled quantity"
            )
        if level_notional != self.gross_notional:
            raise MandatoryExitContractErrorV2(
                "level fills do not conserve gross notional"
            )
        if self.filled_quantity == 0:
            if any(
                value is not None
                for value in (self.executable_vwap, *self.fee_binding)
            ):
                raise MandatoryExitContractErrorV2(
                    "zero fill cannot expose VWAP or fee binding"
                )
            if self.gross_notional != 0 or self.signed_gross_cashflow != 0:
                raise MandatoryExitContractErrorV2(
                    "zero fill must have zero gross cashflow"
                )
        else:
            _validate_positive_decimal(self.executable_vwap, "executable_vwap")
            assert self.executable_vwap is not None
            with localcontext(protocol_decimal_context_v2()):
                expected_vwap = self.gross_notional / self.filled_quantity
            if self.executable_vwap != expected_vwap:
                raise MandatoryExitContractErrorV2(
                    "VWAP differs from filled notional over quantity"
                )
            expected_cashflow = (
                self.gross_notional
                if self.exit_side is PaperFokSideV2.SELL
                else -self.gross_notional
            )
            if self.signed_gross_cashflow != expected_cashflow:
                raise MandatoryExitContractErrorV2(
                    "signed gross cashflow differs from exit side"
                )
            if any(value is None for value in self.fee_binding) and not all(
                value is None for value in self.fee_binding
            ):
                raise MandatoryExitContractErrorV2(
                    "fee binding fields must be all present or all absent"
                )
        if type(self.residual_is_filter_dust) is not bool:
            raise MandatoryExitContractErrorV2(
                "residual_is_filter_dust must be boolean"
            )
        if type(self.primary_target_generation_missing) is not bool:
            raise MandatoryExitContractErrorV2(
                "primary_target_generation_missing must be boolean"
            )
        _validate_reasons(self.reasons)
        object.__setattr__(
            self,
            "event_id",
            hashlib.sha256(
                _GENERATION_ID_DOMAIN
                + canonical_json_line(
                    {
                        "generation_event_id": self.generation_event_id,
                        "intent_event_id": self.intent_event_id,
                        "position_event_id": self.position_event_id,
                        "role": "MANDATORY_EXIT_ATTEMPT",
                    }
                )
            ).hexdigest(),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            hashlib.sha256(
                _ATTEMPT_PAYLOAD_DOMAIN
                + canonical_json_line(_attempt_document(self, include_payload=False))
            ).hexdigest(),
        )

    @property
    def fee_binding(self) -> tuple[str | None, str | None, str | None]:
        return (
            self.fee_resolution_event_id,
            self.fee_resolution_payload_sha256,
            self.fee_resolution_status,
        )


@dataclass(frozen=True, slots=True)
class MandatoryExitTerminalV2:
    position_event_id: str
    intent_event_id: str
    terminal_status: MandatoryExitTerminalStatusV2
    finalized_at_venue_ms: int
    residual_quantity: Decimal
    family_inconclusive: bool
    reasons: tuple[str, ...]
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _TERMINAL_FACTORY_TOKEN:
            raise MandatoryExitContractErrorV2(
                "terminal exit states must be created by the ledger"
            )
        _validate_sha256(self.position_event_id, "position_event_id")
        _validate_sha256(self.intent_event_id, "intent_event_id")
        if not isinstance(self.terminal_status, MandatoryExitTerminalStatusV2):
            raise MandatoryExitContractErrorV2("terminal status has the wrong type")
        _validate_nonnegative_int(
            self.finalized_at_venue_ms,
            "finalized_at_venue_ms",
        )
        _validate_nonnegative_decimal(
            self.residual_quantity,
            "residual_quantity",
        )
        if type(self.family_inconclusive) is not bool:
            raise MandatoryExitContractErrorV2(
                "family_inconclusive must be boolean"
            )
        if (
            self.terminal_status is MandatoryExitTerminalStatusV2.EXITED_FULL
            and self.residual_quantity != 0
        ):
            raise MandatoryExitContractErrorV2(
                "EXITED_FULL requires zero residual inventory"
            )
        if (
            self.terminal_status
            is not MandatoryExitTerminalStatusV2.EXITED_FULL
            and self.residual_quantity <= 0
        ):
            raise MandatoryExitContractErrorV2(
                "residual terminal states must retain positive inventory"
            )
        _validate_reasons(self.reasons)
        identity = {
            "intent_event_id": self.intent_event_id,
            "position_event_id": self.position_event_id,
            "role": "MANDATORY_EXIT_TERMINAL",
        }
        object.__setattr__(
            self,
            "event_id",
            hashlib.sha256(
                _TERMINAL_ID_DOMAIN + canonical_json_line(identity)
            ).hexdigest(),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            hashlib.sha256(
                _TERMINAL_PAYLOAD_DOMAIN
                + canonical_json_line(_terminal_document(self, include_payload=False))
            ).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class MandatoryExitStateV2:
    position: MandatoryExitPositionV2
    intent: MandatoryExitIntentV2 | None
    attempts: tuple[MandatoryExitAttemptV2, ...]
    terminal: MandatoryExitTerminalV2 | None

    def __post_init__(self) -> None:
        if not isinstance(self.position, MandatoryExitPositionV2):
            raise MandatoryExitContractErrorV2("state position has the wrong type")
        if self.intent is not None and not isinstance(
            self.intent,
            MandatoryExitIntentV2,
        ):
            raise MandatoryExitContractErrorV2("state intent has the wrong type")
        if type(self.attempts) is not tuple or any(
            not isinstance(value, MandatoryExitAttemptV2)
            for value in self.attempts
        ):
            raise MandatoryExitContractErrorV2(
                "state attempts must be an immutable typed tuple"
            )
        if self.terminal is not None and not isinstance(
            self.terminal,
            MandatoryExitTerminalV2,
        ):
            raise MandatoryExitContractErrorV2("state terminal has the wrong type")
        if self.intent is None and (self.attempts or self.terminal is not None):
            raise MandatoryExitContractErrorV2(
                "attempts and terminal require a scheduled intent"
            )
        if self.intent is not None:
            _validate_state_identity(self)
        ordered = tuple(
            sorted(
                self.attempts,
                key=lambda value: (
                    value.generation_venue_ms,
                    value.target_state_last_ingest_seq,
                    value.event_id,
                ),
            )
        )
        if ordered != self.attempts:
            raise MandatoryExitContractErrorV2(
                "attempts must use canonical chronological order"
            )
        if len({value.event_id for value in self.attempts}) != len(self.attempts):
            raise MandatoryExitContractErrorV2("attempt event IDs must be unique")
        if self.terminal is not None and self.terminal.position_event_id != (
            self.position.event_id
        ):
            raise MandatoryExitContractErrorV2(
                "terminal refers to a different position"
            )
        with localcontext(protocol_decimal_context_v2()):
            total_filled = sum(
                (value.filled_quantity for value in self.attempts),
                Decimal(0),
            )
        if total_filled + self.residual_quantity != self.position.initial_quantity:
            raise MandatoryExitContractErrorV2(
                "state violates lifetime inventory conservation"
            )
        prior_residual = self.position.initial_quantity
        for attempt in self.attempts:
            if attempt.residual_before != prior_residual:
                raise MandatoryExitContractErrorV2(
                    "attempt residual chain is discontinuous"
                )
            prior_residual = attempt.residual_after
        if self.terminal is not None and (
            self.terminal.residual_quantity != self.residual_quantity
        ):
            raise MandatoryExitContractErrorV2(
                "terminal residual differs from attempt ledger"
            )

    @property
    def residual_quantity(self) -> Decimal:
        if not self.attempts:
            return self.position.initial_quantity
        return self.attempts[-1].residual_after

    @property
    def total_filled_quantity(self) -> Decimal:
        with localcontext(protocol_decimal_context_v2()):
            return sum(
                (value.filled_quantity for value in self.attempts),
                Decimal(0),
            )

    @property
    def gross_exit_notional(self) -> Decimal:
        with localcontext(protocol_decimal_context_v2()):
            return sum(
                (value.gross_notional for value in self.attempts),
                Decimal(0),
            )

    @property
    def signed_gross_exit_cashflow(self) -> Decimal:
        with localcontext(protocol_decimal_context_v2()):
            return sum(
                (value.signed_gross_cashflow for value in self.attempts),
                Decimal(0),
            )

    @property
    def family_inconclusive(self) -> bool:
        if self.terminal is not None:
            return self.terminal.family_inconclusive
        if self.intent is None:
            return False
        return self.intent.target_cursor.missing_ack_makes_family_inconclusive or any(
            value.primary_target_generation_missing
            or value.status
            in (
                MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA,
                MandatoryExitAttemptStatusV2.INCONCLUSIVE_FILTER,
            )
            for value in self.attempts
        )

    @property
    def fee_resolution_bindings_present(self) -> bool:
        """Whether every fill has a fee-version input, not a final fee cost."""

        return all(
            attempt.filled_quantity == 0
            or attempt.fee_resolution_event_id is not None
            for attempt in self.attempts
        )

    @property
    def final_fee_cost_complete(self) -> bool:
        """Final both-leg fee owner is not yet attached to this execution ledger."""

        return False


@dataclass(frozen=True, slots=True)
class MandatoryExitLedgerCheckpointV2:
    attempt_id: str
    promoting_plan_sha256: str
    replay_root_sha256: str
    state_root_sha256: str
    event_count: int
    position_count: int
    maximum_events: int
    maximum_positions: int
    checkpoint_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        for value, name in (
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
            (self.replay_root_sha256, "replay_root_sha256"),
            (self.state_root_sha256, "state_root_sha256"),
        ):
            _validate_sha256(value, name)
        for name in ("event_count", "position_count"):
            _validate_nonnegative_int(getattr(self, name), name)
        for name in ("maximum_events", "maximum_positions"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise MandatoryExitContractErrorV2(f"{name} must be positive")
        if self.event_count > self.maximum_events:
            raise MandatoryExitContractErrorV2("checkpoint exceeds event capacity")
        if self.position_count > self.maximum_positions:
            raise MandatoryExitContractErrorV2("checkpoint exceeds position capacity")
        object.__setattr__(
            self,
            "checkpoint_sha256",
            hashlib.sha256(
                _CHECKPOINT_DOMAIN
                + canonical_json_line(_checkpoint_document(self))
            ).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class MandatoryExitFeeCertificateV2:
    """Derived ledger certificate for exact entry and 0..N exit fee slices."""

    position: MandatoryExitPositionV2
    filled_exit_attempts: tuple[MandatoryExitAttemptV2, ...]
    terminal: MandatoryExitTerminalV2 | None
    source_state_sha256: str
    ledger_checkpoint: MandatoryExitLedgerCheckpointV2
    _factory_token: InitVar[object] = None
    certificate_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FEE_CERTIFICATE_FACTORY_TOKEN:
            raise MandatoryExitContractErrorV2(
                "fee certificates must be issued by MandatoryExitLedgerV2"
            )
        if not isinstance(self.position, MandatoryExitPositionV2):
            raise MandatoryExitContractErrorV2(
                "fee certificate position has the wrong type"
            )
        if type(self.filled_exit_attempts) is not tuple or any(
            not isinstance(value, MandatoryExitAttemptV2)
            for value in self.filled_exit_attempts
        ):
            raise MandatoryExitContractErrorV2(
                "fee certificate exit attempts must be an immutable typed tuple"
            )
        if any(value.filled_quantity <= 0 for value in self.filled_exit_attempts):
            raise MandatoryExitContractErrorV2(
                "fee certificate may contain only positive exit fills"
            )
        if any(
            value.position_event_id != self.position.event_id
            for value in self.filled_exit_attempts
        ):
            raise MandatoryExitContractErrorV2(
                "fee certificate exit slice refers to a different position"
            )
        ordered = tuple(
            sorted(
                self.filled_exit_attempts,
                key=lambda value: (
                    value.generation_venue_ms,
                    value.target_state_last_ingest_seq,
                    value.event_id,
                ),
            )
        )
        if ordered != self.filled_exit_attempts:
            raise MandatoryExitContractErrorV2(
                "fee certificate exit slices must use canonical chronological order"
            )
        if len({value.event_id for value in self.filled_exit_attempts}) != len(
            self.filled_exit_attempts
        ):
            raise MandatoryExitContractErrorV2(
                "fee certificate exit slice event IDs must be unique"
            )
        if self.terminal is not None:
            if not isinstance(self.terminal, MandatoryExitTerminalV2):
                raise MandatoryExitContractErrorV2(
                    "fee certificate terminal has the wrong type"
                )
            if self.terminal.position_event_id != self.position.event_id:
                raise MandatoryExitContractErrorV2(
                    "fee certificate terminal refers to a different position"
                )
        _validate_sha256(self.source_state_sha256, "source_state_sha256")
        if not isinstance(self.ledger_checkpoint, MandatoryExitLedgerCheckpointV2):
            raise MandatoryExitContractErrorV2(
                "fee certificate ledger checkpoint has the wrong type"
            )
        if (
            self.ledger_checkpoint.attempt_id != self.position.attempt_id
            or self.ledger_checkpoint.promoting_plan_sha256
            != self.position.promoting_plan_sha256
            or self.ledger_checkpoint.position_count < 1
        ):
            raise MandatoryExitContractErrorV2(
                "fee certificate checkpoint differs from the position scope"
            )
        with localcontext(protocol_decimal_context_v2()):
            filled_quantity = sum(
                (value.filled_quantity for value in self.filled_exit_attempts),
                Decimal(0),
            )
            residual_quantity = self.position.initial_quantity - filled_quantity
        if residual_quantity < 0:
            raise MandatoryExitContractErrorV2(
                "fee certificate exits exceed certified entry inventory"
            )
        if self.terminal is not None and (
            self.terminal.residual_quantity != residual_quantity
        ):
            raise MandatoryExitContractErrorV2(
                "fee certificate terminal residual contradicts exit slices"
            )
        object.__setattr__(
            self,
            "certificate_sha256",
            hashlib.sha256(
                _FEE_CERTIFICATE_DOMAIN
                + canonical_json_line(
                    _mandatory_exit_fee_certificate_document(
                        self,
                        include_certificate_sha256=False,
                    )
                )
            ).hexdigest(),
        )

    @property
    def residual_quantity(self) -> Decimal:
        with localcontext(protocol_decimal_context_v2()):
            return self.position.initial_quantity - sum(
                (value.filled_quantity for value in self.filled_exit_attempts),
                Decimal(0),
            )

    @property
    def full_exit(self) -> bool:
        return bool(
            self.terminal is not None
            and self.terminal.terminal_status
            is MandatoryExitTerminalStatusV2.EXITED_FULL
            and self.residual_quantity == 0
        )


def canonical_mandatory_exit_fee_certificate_v2(
    certificate: MandatoryExitFeeCertificateV2,
) -> bytes:
    """Verify and export exact execution slices without creating fee authority."""

    if not isinstance(certificate, MandatoryExitFeeCertificateV2):
        raise MandatoryExitContractErrorV2(
            "certificate must be MandatoryExitFeeCertificateV2"
        )
    expected = hashlib.sha256(
        _FEE_CERTIFICATE_DOMAIN
        + canonical_json_line(
            _mandatory_exit_fee_certificate_document(
                certificate,
                include_certificate_sha256=False,
            )
        )
    ).hexdigest()
    if certificate.certificate_sha256 != expected:
        raise MandatoryExitContractErrorV2(
            "mandatory exit fee certificate hash mismatch"
        )
    return canonical_json_line(
        _mandatory_exit_fee_certificate_document(
            certificate,
            include_certificate_sha256=True,
        )
    )


@dataclass(slots=True)
class _MutablePositionStateV2:
    position: MandatoryExitPositionV2
    intent: MandatoryExitIntentV2 | None = None
    attempts: list[MandatoryExitAttemptV2] = field(default_factory=list)
    terminal: MandatoryExitTerminalV2 | None = None

    def frozen(self) -> MandatoryExitStateV2:
        return MandatoryExitStateV2(
            position=self.position,
            intent=self.intent,
            attempts=tuple(self.attempts),
            terminal=self.terminal,
        )


class MandatoryExitLedgerV2:
    """Bounded, restart-safe owner of mandatory USD-M PAPER exits.

    The ledger performs no network call and cannot place an exchange order.
    Cross-position replay roots are canonical rather than arrival-order based.
    """

    def __init__(
        self,
        *,
        maximum_events: int,
        maximum_positions: int,
        attempt_id: str,
        promoting_plan_sha256: str,
    ) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise MandatoryExitContractErrorV2("maximum_events must be positive")
        if type(maximum_positions) is not int or maximum_positions < 1:
            raise MandatoryExitContractErrorV2(
                "maximum_positions must be positive"
            )
        _validate_identity(attempt_id, "attempt_id")
        _validate_sha256(promoting_plan_sha256, "promoting_plan_sha256")
        self._maximum_events = maximum_events
        self._maximum_positions = maximum_positions
        self._attempt_id = attempt_id
        self._promoting_plan_sha256 = promoting_plan_sha256
        self._states: dict[str, _MutablePositionStateV2] = {}
        self._event_payload_by_id: dict[str, bytes] = {}
        self._event_order_key_by_id: dict[str, tuple[int, str, int, str]] = {}

    @property
    def event_count(self) -> int:
        return len(self._event_payload_by_id)

    @property
    def position_count(self) -> int:
        return len(self._states)

    @property
    def maximum_events(self) -> int:
        return self._maximum_events

    @property
    def maximum_positions(self) -> int:
        return self._maximum_positions

    @property
    def replay_root_sha256(self) -> str:
        digest = hashlib.sha256(_REPLAY_ROOT_DOMAIN)
        for event_id in sorted(
            self._event_payload_by_id,
            key=lambda value: self._event_order_key_by_id[value],
        ):
            digest.update(bytes.fromhex(event_id))
            digest.update(self._event_payload_by_id[event_id])
        return digest.hexdigest()

    @property
    def state_root_sha256(self) -> str:
        digest = hashlib.sha256(_STATE_ROOT_DOMAIN)
        for position_event_id in sorted(self._states):
            digest.update(
                canonical_json_line(
                    _state_document(self._states[position_event_id].frozen())
                )
            )
        return digest.hexdigest()

    def terminal_checkpoint_v2(self) -> MandatoryExitLedgerCheckpointV2:
        return MandatoryExitLedgerCheckpointV2(
            attempt_id=self._attempt_id,
            promoting_plan_sha256=self._promoting_plan_sha256,
            replay_root_sha256=self.replay_root_sha256,
            state_root_sha256=self.state_root_sha256,
            event_count=self.event_count,
            position_count=self.position_count,
            maximum_events=self.maximum_events,
            maximum_positions=self.maximum_positions,
        )

    def register_position_v2(
        self,
        position: MandatoryExitPositionV2,
    ) -> MandatoryExitRegistryDispositionV2:
        if not isinstance(position, MandatoryExitPositionV2):
            raise MandatoryExitContractErrorV2("position has the wrong type")
        self._validate_scope(position.attempt_id, position.promoting_plan_sha256)
        payload = canonical_json_line(
            {
                "kind": "POSITION_OPEN",
                "position": _position_document(position),
                "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
            }
        )
        existing = self._states.get(position.event_id)
        if existing is not None:
            if existing.position != position:
                raise MandatoryExitContractErrorV2(
                    "conflicting position shares deterministic event ID"
                )
            self._verify_duplicate_event(position.event_id, payload)
            return MandatoryExitRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if self.position_count >= self.maximum_positions:
            raise MandatoryExitContractErrorV2("mandatory exit position capacity reached")
        self._reserve_event_capacity(1)
        self._states[position.event_id] = _MutablePositionStateV2(position)
        self._append_event(
            position.event_id,
            payload,
            order_key=(
                position.entry_target_venue_ms,
                position.event_id,
                0,
                position.event_id,
            ),
        )
        return MandatoryExitRegistryDispositionV2.NEW

    def schedule_intent_v2(
        self,
        intent: MandatoryExitIntentV2,
    ) -> MandatoryExitRegistryDispositionV2:
        if not isinstance(intent, MandatoryExitIntentV2):
            raise MandatoryExitContractErrorV2("intent has the wrong type")
        self._validate_scope(intent.attempt_id, intent.promoting_plan_sha256)
        state = self._require_state(intent.position_event_id)
        _validate_position_intent_match(state.position, intent)
        payload = canonical_json_line(
            {
                "intent": _intent_document(intent, include_payload=True),
                "kind": "EXIT_INTENT",
                "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
            }
        )
        if state.intent is not None:
            if state.intent != intent:
                raise MandatoryExitContractErrorV2(
                    "position already has a different immutable exit intent"
                )
            self._verify_duplicate_event(intent.event_id, payload)
            return MandatoryExitRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if state.terminal is not None:
            raise MandatoryExitContractErrorV2("cannot schedule a terminal position")
        self._reserve_event_capacity(1)
        state.intent = intent
        self._append_event(
            intent.event_id,
            payload,
            order_key=(
                intent.exit_decision_cutoff_ms,
                intent.position_event_id,
                1,
                intent.event_id,
            ),
        )
        return MandatoryExitRegistryDispositionV2.NEW

    def apply_generation_v2(
        self,
        generation: MandatoryExitBookGenerationV2,
    ) -> MandatoryExitAttemptV2:
        if not isinstance(generation, MandatoryExitBookGenerationV2):
            raise MandatoryExitContractErrorV2("generation has the wrong type")
        state = self._require_state(generation.position_event_id)
        if state.intent is None:
            raise MandatoryExitContractErrorV2(
                "exit generation requires a scheduled intent"
            )
        if generation.intent_event_id != state.intent.event_id:
            raise MandatoryExitContractErrorV2(
                "generation refers to a different exit intent"
            )
        existing = next(
            (
                value
                for value in state.attempts
                if value.generation_event_id == generation.event_id
            ),
            None,
        )
        if existing is not None:
            if existing.generation_evidence_sha256 != generation.evidence_sha256:
                raise MandatoryExitContractErrorV2(
                    "conflicting generation evidence would rewrite an exit attempt"
                )
            return existing
        if state.terminal is not None:
            raise MandatoryExitContractErrorV2(
                "terminal position cannot consume another book generation"
            )
        attempt = _evaluate_exit_generation(state.frozen(), generation)
        if attempt.status is MandatoryExitAttemptStatusV2.CLOSURE_PENDING:
            return attempt
        terminal = _terminal_after_attempt(state.frozen(), attempt)
        required_events = 1 + (1 if terminal is not None else 0)
        self._reserve_event_capacity(required_events)
        state.attempts.append(attempt)
        self._append_attempt_event(attempt)
        if terminal is not None:
            state.terminal = terminal
            self._append_terminal_event(terminal)
        return attempt

    def finalize_retry_window_v2(
        self,
        position_event_id: str,
        *,
        finalized_at_venue_ms: int,
    ) -> MandatoryExitTerminalV2:
        _validate_sha256(position_event_id, "position_event_id")
        _validate_nonnegative_int(finalized_at_venue_ms, "finalized_at_venue_ms")
        state = self._require_state(position_event_id)
        if state.intent is None:
            raise MandatoryExitContractErrorV2(
                "cannot finalize before an exit intent is scheduled"
            )
        if state.terminal is not None:
            return state.terminal
        if finalized_at_venue_ms <= state.intent.retry_deadline_venue_ms:
            raise MandatoryExitContractErrorV2(
                "retry window remains open through target plus 30000 ms"
            )
        frozen = state.frozen()
        is_dust = bool(
            frozen.attempts and frozen.attempts[-1].residual_is_filter_dust
        )
        status = (
            MandatoryExitTerminalStatusV2.DUST_RESIDUAL_RETAINED
            if is_dust
            else MandatoryExitTerminalStatusV2.POST_ENTRY_UNRESOLVED_EXIT
        )
        reasons = (
            ("FILTER_DUST_RETAINED_FOR_EXTERNAL_NAV_MARK",)
            if is_dust
            else ("NON_DUST_RESIDUAL_AFTER_RETRY_WINDOW",)
        )
        terminal = MandatoryExitTerminalV2(
            position_event_id=position_event_id,
            intent_event_id=state.intent.event_id,
            terminal_status=status,
            finalized_at_venue_ms=finalized_at_venue_ms,
            residual_quantity=frozen.residual_quantity,
            family_inconclusive=(
                frozen.family_inconclusive
                or status
                is MandatoryExitTerminalStatusV2.POST_ENTRY_UNRESOLVED_EXIT
            ),
            reasons=reasons,
            _factory_token=_TERMINAL_FACTORY_TOKEN,
        )
        self._reserve_event_capacity(1)
        state.terminal = terminal
        self._append_terminal_event(terminal)
        return terminal

    def state_for_position_v2(
        self,
        position_event_id: str,
    ) -> MandatoryExitStateV2:
        _validate_sha256(position_event_id, "position_event_id")
        return self._require_state(position_event_id).frozen()

    def states_v2(self) -> tuple[MandatoryExitStateV2, ...]:
        return tuple(self._states[key].frozen() for key in sorted(self._states))

    def issue_fee_certificate_v2(
        self,
        position_event_id: str,
    ) -> MandatoryExitFeeCertificateV2:
        """Issue a derived certificate pinned to this ledger's current checkpoint."""

        _validate_sha256(position_event_id, "position_event_id")
        state = self._require_state(position_event_id).frozen()
        state_payload = canonical_json_line(_state_document(state))
        return MandatoryExitFeeCertificateV2(
            position=state.position,
            filled_exit_attempts=tuple(
                value for value in state.attempts if value.filled_quantity > 0
            ),
            terminal=state.terminal,
            source_state_sha256=hashlib.sha256(state_payload).hexdigest(),
            ledger_checkpoint=self.terminal_checkpoint_v2(),
            _factory_token=_FEE_CERTIFICATE_FACTORY_TOKEN,
        )

    def export_state_v2(self) -> bytes:
        checkpoint = self.terminal_checkpoint_v2()
        return canonical_json_line(
            {
                "attempt_id": self._attempt_id,
                "checkpoint": {
                    **_checkpoint_document(checkpoint),
                    "checkpoint_sha256": checkpoint.checkpoint_sha256,
                },
                "maximum_events": self.maximum_events,
                "maximum_positions": self.maximum_positions,
                "promoting_plan_sha256": self._promoting_plan_sha256,
                "schema_version": _STATE_SCHEMA,
                "states": [
                    _state_document(state) for state in self.states_v2()
                ],
            }
        )

    @classmethod
    def from_state_v2(
        cls,
        payload: bytes,
        *,
        expected_attempt_id: str,
        expected_promoting_plan_sha256: str,
        expected_replay_root_sha256: str,
        expected_state_root_sha256: str,
        expected_event_count: int,
        expected_position_count: int,
        expected_maximum_events: int,
        expected_maximum_positions: int,
        expected_checkpoint_sha256: str,
    ) -> MandatoryExitLedgerV2:
        """Restore only against a complete out-of-band checkpoint/census pin."""

        if type(payload) is not bytes or not payload or len(payload) > 128_000_000:
            raise MandatoryExitContractErrorV2(
                "ledger state must be bounded nonempty bytes"
            )
        _validate_identity(expected_attempt_id, "expected_attempt_id")
        for value, name in (
            (
                expected_promoting_plan_sha256,
                "expected_promoting_plan_sha256",
            ),
            (expected_replay_root_sha256, "expected_replay_root_sha256"),
            (expected_state_root_sha256, "expected_state_root_sha256"),
            (expected_checkpoint_sha256, "expected_checkpoint_sha256"),
        ):
            _validate_sha256(value, name)
        for value, name in (
            (expected_event_count, "expected_event_count"),
            (expected_position_count, "expected_position_count"),
        ):
            _validate_nonnegative_int(value, name)
        for value, name in (
            (expected_maximum_events, "expected_maximum_events"),
            (expected_maximum_positions, "expected_maximum_positions"),
        ):
            if type(value) is not int or value < 1:
                raise MandatoryExitContractErrorV2(f"{name} must be positive")
        try:
            raw = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MandatoryExitContractErrorV2(
                "ledger state is not canonical JSON"
            ) from exc
        document = _require_mapping(raw, "ledger state")
        _require_exact_keys(
            document,
            {
                "attempt_id",
                "checkpoint",
                "maximum_events",
                "maximum_positions",
                "promoting_plan_sha256",
                "schema_version",
                "states",
            },
            "ledger state",
        )
        if document["schema_version"] != _STATE_SCHEMA:
            raise MandatoryExitContractErrorV2("ledger state schema differs")
        if (
            document["attempt_id"] != expected_attempt_id
            or document["promoting_plan_sha256"]
            != expected_promoting_plan_sha256
            or document["maximum_events"] != expected_maximum_events
            or document["maximum_positions"] != expected_maximum_positions
        ):
            raise MandatoryExitContractErrorV2(
                "ledger state differs from external scope or capacity"
            )
        raw_states = _require_list(document["states"], "states")
        restored_states = tuple(
            _state_from_document(_require_mapping(value, "state"))
            for value in raw_states
        )
        if len(restored_states) != expected_position_count:
            raise MandatoryExitContractErrorV2(
                "restored position census differs from external count"
            )
        ledger = cls(
            maximum_events=expected_maximum_events,
            maximum_positions=expected_maximum_positions,
            attempt_id=expected_attempt_id,
            promoting_plan_sha256=expected_promoting_plan_sha256,
        )
        for state in restored_states:
            ledger.register_position_v2(state.position)
            mutable = ledger._states[state.position.event_id]
            if state.intent is not None:
                ledger.schedule_intent_v2(state.intent)
            for attempt in state.attempts:
                if attempt.status is MandatoryExitAttemptStatusV2.CLOSURE_PENDING:
                    raise MandatoryExitContractErrorV2(
                        "pending closure cannot be persisted as a ledger event"
                    )
                ledger._reserve_event_capacity(1)
                mutable.attempts.append(attempt)
                ledger._append_attempt_event(attempt)
            if state.terminal is not None:
                ledger._reserve_event_capacity(1)
                mutable.terminal = state.terminal
                ledger._append_terminal_event(state.terminal)
            mutable.frozen()
        checkpoint = ledger.terminal_checkpoint_v2()
        embedded = _checkpoint_from_document(
            _require_mapping(document["checkpoint"], "checkpoint")
        )
        if checkpoint != embedded:
            raise MandatoryExitContractErrorV2(
                "embedded checkpoint differs from reconstructed ledger"
            )
        if (
            checkpoint.replay_root_sha256 != expected_replay_root_sha256
            or checkpoint.state_root_sha256 != expected_state_root_sha256
            or checkpoint.event_count != expected_event_count
            or checkpoint.position_count != expected_position_count
            or checkpoint.checkpoint_sha256 != expected_checkpoint_sha256
        ):
            raise MandatoryExitContractErrorV2(
                "restored ledger differs from external checkpoint"
            )
        return ledger

    def _validate_scope(self, attempt_id: str, plan: str) -> None:
        if attempt_id != self._attempt_id or plan != self._promoting_plan_sha256:
            raise MandatoryExitContractErrorV2(
                "event differs from the ledger attempt or promoting plan"
            )

    def _require_state(self, position_event_id: str) -> _MutablePositionStateV2:
        state = self._states.get(position_event_id)
        if state is None:
            raise MandatoryExitContractErrorV2(
                "mandatory exit position is absent from the ledger"
            )
        return state

    def _reserve_event_capacity(self, count: int) -> None:
        if self.event_count + count > self.maximum_events:
            raise MandatoryExitContractErrorV2("mandatory exit event capacity reached")

    def _verify_duplicate_event(self, event_id: str, payload: bytes) -> None:
        if self._event_payload_by_id.get(event_id) != payload:
            raise MandatoryExitContractErrorV2(
                "deterministic event ID has conflicting canonical payload"
            )

    def _append_event(
        self,
        event_id: str,
        payload: bytes,
        *,
        order_key: tuple[int, str, int, str],
    ) -> None:
        existing = self._event_payload_by_id.get(event_id)
        if existing is not None:
            if existing != payload:
                raise MandatoryExitContractErrorV2(
                    "deterministic event ID has conflicting payload"
                )
            return
        self._event_payload_by_id[event_id] = payload
        self._event_order_key_by_id[event_id] = order_key

    def _append_attempt_event(self, attempt: MandatoryExitAttemptV2) -> None:
        self._append_event(
            attempt.event_id,
            canonical_json_line(
                {
                    "attempt": _attempt_document(attempt, include_payload=True),
                    "kind": "EXIT_ATTEMPT",
                    "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
                }
            ),
            order_key=(
                attempt.generation_venue_ms,
                attempt.position_event_id,
                2,
                attempt.event_id,
            ),
        )

    def _append_terminal_event(self, terminal: MandatoryExitTerminalV2) -> None:
        self._append_event(
            terminal.event_id,
            canonical_json_line(
                {
                    "kind": "EXIT_TERMINAL",
                    "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
                    "terminal": _terminal_document(terminal, include_payload=True),
                }
            ),
            order_key=(
                terminal.finalized_at_venue_ms,
                terminal.position_event_id,
                3,
                terminal.event_id,
            ),
        )


def _evaluate_exit_generation(
    state: MandatoryExitStateV2,
    generation: MandatoryExitBookGenerationV2,
) -> MandatoryExitAttemptV2:
    assert state.intent is not None
    position = state.position
    intent = state.intent
    _validate_generation_identity(position, intent, generation)
    target_missing, timing_reason = _validate_generation_timing(state, generation)
    if timing_reason is not None:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA,
            closure_method=PaperFokClosureMethodV2.INVALID,
            reason=timing_reason,
            primary_target_missing=target_missing,
        )
    provenance_reason = _validate_generation_provenance(
        position,
        generation,
    )
    if provenance_reason is not None:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA,
            closure_method=PaperFokClosureMethodV2.INVALID,
            reason=provenance_reason,
            primary_target_missing=target_missing,
        )
    book, book_reason = reconstruct_futures_standard_book_v2(
        snapshot=generation.snapshot,
        pre_target_depth_events=generation.pre_generation_depth_events,
        target_venue_ms=generation.generation_venue_ms,
        target_local_cursor_ms=generation.generation_local_cursor_ms,
        target_state_last_ingest_seq=generation.target_state_last_ingest_seq,
    )
    if book is None:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA,
            closure_method=PaperFokClosureMethodV2.INVALID,
            reason=book_reason,
            primary_target_missing=target_missing,
        )
    closure_method, closure_reason = classify_futures_book_closure_v2(
        closure=generation.closure,
        target_local_cursor_ms=generation.generation_local_cursor_ms,
        target_state_last_ingest_seq=generation.target_state_last_ingest_seq,
        prior_u=book.prior_u,
    )
    if closure_method is PaperFokClosureMethodV2.PENDING:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.CLOSURE_PENDING,
            closure_method=closure_method,
            reason=closure_reason,
            primary_target_missing=target_missing,
            terminal_book_update_id=book.prior_u,
        )
    if closure_method is PaperFokClosureMethodV2.INVALID:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA,
            closure_method=closure_method,
            reason=closure_reason,
            primary_target_missing=target_missing,
            terminal_book_update_id=book.prior_u,
        )
    mark_reason = _validate_mark_at_generation(generation)
    if mark_reason is not None:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA,
            closure_method=closure_method,
            reason=mark_reason,
            primary_target_missing=target_missing,
            terminal_book_update_id=book.prior_u,
        )
    rules_reason = _validate_rules_at_generation(generation)
    if rules_reason is not None:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.INCONCLUSIVE_FILTER,
            closure_method=closure_method,
            reason=rules_reason,
            primary_target_missing=target_missing,
            terminal_book_update_id=book.prior_u,
        )
    try:
        grid = intersect_quantity_filters_v2(
            generation.exchange_info.lot_size,
            generation.exchange_info.market_lot_size,
        )
    except ValueError as exc:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.INCONCLUSIVE_FILTER,
            closure_method=closure_method,
            reason=f"INCONCLUSIVE_FILTER_GRID:{exc}",
            primary_target_missing=target_missing,
            terminal_book_update_id=book.prior_u,
        )
    return _walk_exit_generation(
        state,
        generation,
        book=book,
        grid=grid,
        closure_method=closure_method,
        closure_reason=closure_reason,
        primary_target_missing=target_missing,
    )


def _validate_generation_identity(
    position: MandatoryExitPositionV2,
    intent: MandatoryExitIntentV2,
    generation: MandatoryExitBookGenerationV2,
) -> None:
    if generation.position_event_id != position.event_id:
        raise MandatoryExitContractErrorV2(
            "generation position differs from ledger position"
        )
    if generation.intent_event_id != intent.event_id:
        raise MandatoryExitContractErrorV2(
            "generation intent differs from scheduled intent"
        )


def _validate_generation_timing(
    state: MandatoryExitStateV2,
    generation: MandatoryExitBookGenerationV2,
) -> tuple[bool, str | None]:
    assert state.intent is not None
    target = state.intent.target_cursor
    venue_ms = generation.generation_venue_ms
    if venue_ms < target.target_venue_ms:
        return False, "GENERATION_PRECEDES_MANDATORY_EXIT_TARGET"
    if venue_ms > state.intent.retry_deadline_venue_ms:
        raise MandatoryExitContractErrorV2(
            "generation after target plus 30000 ms cannot change inventory"
        )
    target_missing = not state.attempts and venue_ms > target.target_venue_ms
    if venue_ms == target.target_venue_ms:
        if state.attempts:
            return False, "PRIMARY_TARGET_GENERATION_IS_NOT_FIRST"
        if generation.generation_local_cursor_ms != target.target_local_cursor_ms:
            return False, "PRIMARY_TARGET_LOCAL_CURSOR_MISMATCH"
        return False, None
    if generation.generation_local_cursor_ms <= target.target_local_cursor_ms:
        return target_missing, "RETRY_LOCAL_CURSOR_DID_NOT_FOLLOW_TARGET"
    ordered = sorted(
        generation.pre_generation_depth_events,
        key=lambda value: value.ingest_seq,
    )
    if not ordered:
        return target_missing, "RETRY_GENERATION_HAS_NO_DEPTH_TRANSITION"
    last = ordered[-1]
    if (
        last.ingest_seq != generation.target_state_last_ingest_seq
        or last.transaction_time_ms != generation.generation_venue_ms
        or last.receipt_completion_ms != generation.generation_local_cursor_ms
    ):
        return target_missing, "RETRY_CURSOR_IS_NOT_THE_APPLIED_DEPTH_TRANSITION"
    if state.attempts:
        previous = state.attempts[-1]
        if not (
            generation.generation_venue_ms >= previous.generation_venue_ms
            and generation.target_state_last_ingest_seq
            > previous.target_state_last_ingest_seq
            and last.previous_same_stream_ingest_seq
            == previous.target_state_last_ingest_seq
        ):
            return False, "RETRY_OMITS_AN_INTERMEDIATE_SAME_STREAM_GENERATION"
    return target_missing, None


def _validate_generation_provenance(
    position: MandatoryExitPositionV2,
    generation: MandatoryExitBookGenerationV2,
) -> str | None:
    lineage = generation.lineage
    if lineage.promoting_plan_sha256 != position.promoting_plan_sha256:
        return "GENERATION_PROMOTING_PLAN_MISMATCH"
    rows: list[tuple[object, str]] = [
        (generation.snapshot, lineage.depth_snapshot_schema_sha256),
        (generation.mark, lineage.mark_schema_sha256),
        (generation.exchange_info, lineage.exchange_info_schema_sha256),
    ]
    rows.extend(
        (value, lineage.standard_depth_schema_sha256)
        for value in generation.pre_generation_depth_events
    )
    successors = _material_successors(generation.closure)
    rows.extend(
        (value, lineage.standard_depth_schema_sha256) for value in successors
    )
    if not successors and generation.closure.quiet_rest_snapshot is not None:
        rows.append(
            (
                generation.closure.quiet_rest_snapshot,
                lineage.depth_snapshot_schema_sha256,
            )
        )
    if not successors and generation.closure.continuous_health is not None:
        rows.append(
            (
                generation.closure.continuous_health,
                lineage.health_schema_sha256,
            )
        )
    for row, schema in rows:
        if (
            getattr(row, "symbol", None) != position.symbol
            or getattr(row, "venue", None) is not position.venue
            or getattr(row, "promoting_plan_sha256", None)
            != lineage.promoting_plan_sha256
            or getattr(row, "source_root_sha256", None)
            != lineage.source_root_sha256
            or getattr(row, "schema_sha256", None) != schema
        ):
            return "ROW_SYMBOL_VENUE_PLAN_SOURCE_OR_SCHEMA_ROOT_MISMATCH"
    routed = [*generation.pre_generation_depth_events, generation.mark, *successors]
    for row in routed:
        if getattr(row, "routing_status", None) != 1:
            return "USD_M_ROUTING_STATUS_NOT_ONE"
        if getattr(row, "pair", None) != position.symbol:
            return "USD_M_ROUTING_PAIR_MISMATCH"
    return None


def _validate_mark_at_generation(
    generation: MandatoryExitBookGenerationV2,
) -> str | None:
    mark = generation.mark
    if mark.event_time_ms > generation.generation_venue_ms:
        return "MARK_EVENT_TIME_AFTER_EXIT_GENERATION"
    if mark.receipt_completion_ms > generation.generation_local_cursor_ms:
        return "MARK_RECEIPT_AFTER_EXIT_GENERATION"
    if generation.generation_venue_ms - mark.event_time_ms > 2_000:
        return "MARK_PRICE_STALE_OVER_2000MS"
    return None


def _validate_rules_at_generation(
    generation: MandatoryExitBookGenerationV2,
) -> str | None:
    rules = generation.exchange_info
    if not rules.applicable_filter_inventory_complete:
        return "APPLICABLE_FILTER_INVENTORY_IS_INCOMPLETE"
    if rules.response_completion_ms > generation.generation_local_cursor_ms:
        return "EXCHANGE_INFO_RESPONSE_AFTER_EXIT_GENERATION"
    if not (
        rules.version_valid_from_local_ms
        <= generation.generation_local_cursor_ms
        <= rules.version_valid_through_local_ms
    ):
        return "EXCHANGE_INFO_VERSION_NOT_CERTAIN_AT_EXIT_GENERATION"
    return None


def _walk_exit_generation(
    state: MandatoryExitStateV2,
    generation: MandatoryExitBookGenerationV2,
    *,
    book: FuturesFrozenBookV2,
    grid: CommonQuantityGridV2,
    closure_method: PaperFokClosureMethodV2,
    closure_reason: str,
    primary_target_missing: bool,
) -> MandatoryExitAttemptV2:
    position = state.position
    residual = state.residual_quantity
    requested = grid.floor_legal_total(residual)
    rules = generation.exchange_info
    mark = generation.mark.mark_price
    if requested == 0:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.NO_FILL,
            closure_method=closure_method,
            reason="RESIDUAL_BELOW_COMMON_QUANTITY_GRID",
            primary_target_missing=primary_target_missing,
            terminal_book_update_id=book.prior_u,
            residual_is_filter_dust=True,
        )
    mark_notional = multiply_protocol_decimals_exact_v2(mark, requested)
    if rules.min_notional > 0 and mark_notional < rules.min_notional:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.NO_FILL,
            closure_method=closure_method,
            reason="RESIDUAL_BELOW_ENABLED_MIN_NOTIONAL",
            primary_target_missing=primary_target_missing,
            terminal_book_update_id=book.prior_u,
            requested_order_quantity=requested,
            residual_is_filter_dust=True,
        )
    if rules.max_notional > 0 and mark_notional > rules.max_notional:
        with localcontext(protocol_decimal_context_v2()):
            max_quantity = rules.max_notional / mark
        requested = grid.floor_legal_total(min(requested, max_quantity))
        if requested == 0:
            return _empty_attempt(
                state,
                generation,
                status=MandatoryExitAttemptStatusV2.INCONCLUSIVE_FILTER,
                closure_method=closure_method,
                reason="MAX_NOTIONAL_LEAVES_NO_LEGAL_EXIT_QUANTITY",
                primary_target_missing=primary_target_missing,
                terminal_book_update_id=book.prior_u,
            )
    levels = book.bids if position.exit_side is PaperFokSideV2.SELL else book.asks
    with localcontext(protocol_decimal_context_v2()):
        percent_lower = (
            None
            if rules.percent_price_multiplier_down is None
            else mark * rules.percent_price_multiplier_down
        )
        percent_upper = (
            None
            if rules.percent_price_multiplier_up is None
            else mark * rules.percent_price_multiplier_up
        )
        market_bound = (
            mark * (Decimal(1) + rules.market_take_bound)
            if position.exit_side is PaperFokSideV2.BUY
            else mark * (Decimal(1) - rules.market_take_bound)
        )
    reservations = _shadow_reservations(state)
    capacities: list[tuple[DepthLevelV2, Decimal, int]] = []
    total_capacity = Decimal(0)
    for level in levels:
        if not futures_level_passes_official_bounds_v2(
            position.exit_side,
            level.price,
            rules=rules,
            market_bound=market_bound,
            percent_lower=percent_lower,
            percent_upper=percent_upper,
        ):
            continue
        if not is_price_tick_aligned_v2(level.price, rules.tick_size):
            return _empty_attempt(
                state,
                generation,
                status=MandatoryExitAttemptStatusV2.INCONCLUSIVE_FILTER,
                closure_method=closure_method,
                reason="PERMISSIBLE_EXIT_LEVEL_IS_OFF_PRICE_TICK",
                primary_target_missing=primary_target_missing,
                terminal_book_update_id=book.prior_u,
            )
        haircutted = multiply_protocol_decimals_exact_v2(
            level.quantity,
            PRIMARY_EXIT_DEPTH_HAIRCUT_V2,
        )
        capacity = grid.floor_capacity_per_level(haircutted)
        revision = _level_revision_ingest_seq(
            generation.pre_generation_depth_events,
            side=position.exit_side,
            price=level.price,
        )
        reserved = reservations.get(
            (position.exit_side, level.price, revision),
            Decimal(0),
        )
        with localcontext(protocol_decimal_context_v2()):
            available = max(Decimal(0), capacity - reserved)
        if available > 0:
            capacities.append((level, available, revision))
            with localcontext(protocol_decimal_context_v2()):
                total_capacity += available
    fillable = grid.floor_legal_total(min(requested, total_capacity))
    if fillable == 0:
        return _empty_attempt(
            state,
            generation,
            status=MandatoryExitAttemptStatusV2.NO_FILL,
            closure_method=closure_method,
            reason="NO_NEW_HAIRCUT_CAPACITY_IN_PERMISSIBLE_CAPTURED_DEPTH",
            primary_target_missing=primary_target_missing,
            terminal_book_update_id=book.prior_u,
            requested_order_quantity=requested,
        )
    remaining = fillable
    level_fills: list[MandatoryExitLevelFillV2] = []
    exact_notional = Fraction(0, 1)
    for level, available, revision in capacities:
        quantity = min(remaining, available)
        if quantity <= 0:
            continue
        level_fills.append(
            MandatoryExitLevelFillV2(
                price=level.price,
                quantity=quantity,
                level_revision_ingest_seq=revision,
            )
        )
        exact_notional += decimal_fraction_v2(level.price) * decimal_fraction_v2(
            quantity
        )
        with localcontext(protocol_decimal_context_v2()):
            remaining -= quantity
        if remaining == 0:
            break
    if remaining != 0:
        raise MandatoryExitContractErrorV2(
            "internal exit capacity could not allocate certified fill"
        )
    exact_notional_decimal = finite_base10_fraction_v2(exact_notional)
    with localcontext(protocol_decimal_context_v2()):
        notional = +exact_notional_decimal
        vwap = notional / fillable
        residual_after = residual - fillable
    fee_event_id, fee_payload, fee_status = _fee_binding(
        position,
        generation,
    )
    reasons = [
        closure_reason,
        "ALL_CAPTURED_OFFICIAL_BOUND_LEVELS_WALKED",
        "PRIMARY_PER_LEVEL_VISIBLE_DEPTH_HAIRCUT_0_50",
        "FEATURE_SPREAD_AND_10BP_COMPLETENESS_GATES_IGNORED",
    ]
    if primary_target_missing:
        reasons.append("PRIMARY_TARGET_GENERATION_MISSING_FAMILY_INCONCLUSIVE")
    if fee_event_id is None:
        reasons.append("EXIT_FEE_BINDING_MISSING_NO_NUMERIC_FALLBACK")
    elif fee_status != "RESOLVED":
        reasons.append("EXIT_FEE_BINDING_UNRESOLVED_NO_NUMERIC_FALLBACK")
    status = (
        MandatoryExitAttemptStatusV2.FULL_FILL
        if residual_after == 0
        else MandatoryExitAttemptStatusV2.PARTIAL_FILL
    )
    return MandatoryExitAttemptV2(
        position_event_id=position.event_id,
        intent_event_id=state.intent.event_id if state.intent is not None else "",
        generation_event_id=generation.event_id,
        generation_evidence_sha256=generation.evidence_sha256,
        generation_venue_ms=generation.generation_venue_ms,
        generation_local_cursor_ms=generation.generation_local_cursor_ms,
        target_state_last_ingest_seq=generation.target_state_last_ingest_seq,
        terminal_book_update_id=book.prior_u,
        status=status,
        closure_method=closure_method,
        exit_side=position.exit_side,
        residual_before=residual,
        requested_order_quantity=requested,
        filled_quantity=fillable,
        residual_after=residual_after,
        executable_vwap=vwap,
        gross_notional=notional,
        signed_gross_cashflow=(
            notional if position.exit_side is PaperFokSideV2.SELL else -notional
        ),
        level_fills=tuple(level_fills),
        residual_is_filter_dust=False,
        primary_target_generation_missing=primary_target_missing,
        fee_resolution_event_id=fee_event_id,
        fee_resolution_payload_sha256=fee_payload,
        fee_resolution_status=fee_status,
        reasons=tuple(reasons),
        _factory_token=_ATTEMPT_FACTORY_TOKEN,
    )


def _empty_attempt(
    state: MandatoryExitStateV2,
    generation: MandatoryExitBookGenerationV2,
    *,
    status: MandatoryExitAttemptStatusV2,
    closure_method: PaperFokClosureMethodV2,
    reason: str,
    primary_target_missing: bool,
    terminal_book_update_id: int | None = None,
    requested_order_quantity: Decimal = Decimal(0),
    residual_is_filter_dust: bool = False,
) -> MandatoryExitAttemptV2:
    assert state.intent is not None
    reasons = [reason]
    if primary_target_missing:
        reasons.append("PRIMARY_TARGET_GENERATION_MISSING_FAMILY_INCONCLUSIVE")
    return MandatoryExitAttemptV2(
        position_event_id=state.position.event_id,
        intent_event_id=state.intent.event_id,
        generation_event_id=generation.event_id,
        generation_evidence_sha256=generation.evidence_sha256,
        generation_venue_ms=generation.generation_venue_ms,
        generation_local_cursor_ms=generation.generation_local_cursor_ms,
        target_state_last_ingest_seq=generation.target_state_last_ingest_seq,
        terminal_book_update_id=terminal_book_update_id,
        status=status,
        closure_method=closure_method,
        exit_side=state.position.exit_side,
        residual_before=state.residual_quantity,
        requested_order_quantity=requested_order_quantity,
        filled_quantity=Decimal(0),
        residual_after=state.residual_quantity,
        executable_vwap=None,
        gross_notional=Decimal(0),
        signed_gross_cashflow=Decimal(0),
        level_fills=(),
        residual_is_filter_dust=residual_is_filter_dust,
        primary_target_generation_missing=primary_target_missing,
        fee_resolution_event_id=None,
        fee_resolution_payload_sha256=None,
        fee_resolution_status=None,
        reasons=tuple(reasons),
        _factory_token=_ATTEMPT_FACTORY_TOKEN,
    )


def _fee_binding(
    position: MandatoryExitPositionV2,
    generation: MandatoryExitBookGenerationV2,
) -> tuple[str | None, str | None, str | None]:
    resolution = generation.fee_resolution
    if resolution is None:
        return None, None, None
    canonical_fee_version_resolution_v2(resolution)
    if (
        resolution.scope.attempt_id != position.attempt_id
        or resolution.venue is not position.venue
        or resolution.symbol != position.symbol
        or resolution.position_event_id != position.event_id
        or resolution.target_ms != generation.generation_venue_ms
    ):
        raise MandatoryExitContractErrorV2(
            "fee resolution scope, position, or target differs from exit fill"
        )
    return resolution.event_id, resolution.payload_sha256, resolution.status.value


def _shadow_reservations(
    state: MandatoryExitStateV2,
) -> dict[tuple[PaperFokSideV2, Decimal, int], Decimal]:
    reservations: dict[tuple[PaperFokSideV2, Decimal, int], Decimal] = {}
    with localcontext(protocol_decimal_context_v2()):
        for attempt in state.attempts:
            for fill in attempt.level_fills:
                key = (
                    attempt.exit_side,
                    fill.price,
                    fill.level_revision_ingest_seq,
                )
                reservations[key] = reservations.get(key, Decimal(0)) + fill.quantity
    return reservations


def _level_revision_ingest_seq(
    events: tuple[FuturesStandardDepthEventV2, ...],
    *,
    side: PaperFokSideV2,
    price: Decimal,
) -> int:
    revision = 0
    for event in events:
        levels = event.bids if side is PaperFokSideV2.SELL else event.asks
        if any(value.price == price for value in levels):
            revision = max(revision, event.ingest_seq)
    return revision


def _terminal_after_attempt(
    prior_state: MandatoryExitStateV2,
    attempt: MandatoryExitAttemptV2,
) -> MandatoryExitTerminalV2 | None:
    if attempt.residual_after != 0:
        return None
    assert prior_state.intent is not None
    family_inconclusive = (
        prior_state.family_inconclusive
        or prior_state.intent.target_cursor.missing_ack_makes_family_inconclusive
        or attempt.primary_target_generation_missing
        or attempt.status
        in (
            MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA,
            MandatoryExitAttemptStatusV2.INCONCLUSIVE_FILTER,
        )
    )
    return MandatoryExitTerminalV2(
        position_event_id=prior_state.position.event_id,
        intent_event_id=prior_state.intent.event_id,
        terminal_status=MandatoryExitTerminalStatusV2.EXITED_FULL,
        finalized_at_venue_ms=attempt.generation_venue_ms,
        residual_quantity=Decimal(0),
        family_inconclusive=family_inconclusive,
        reasons=("FULL_POSITION_INVENTORY_EXITED",),
        _factory_token=_TERMINAL_FACTORY_TOKEN,
    )


def _target_cursor_document(value: MandatoryExitTargetCursorV2) -> dict[str, object]:
    transport = value.transport_times
    return {
        "clock_segment_root_sha256": value.clock_segment_root_sha256,
        "contiguous_cursor_evidence": value.contiguous_cursor_evidence,
        "exit_decision_cutoff_ms": value.exit_decision_cutoff_ms,
        "mode": value.mode.value,
        "prior_local_cursor_ms": value.prior_local_cursor_ms,
        "prior_venue_lower_bound_ms": value.prior_venue_lower_bound_ms,
        "target_local_cursor_ms": value.target_local_cursor_ms,
        "target_venue_lower_bound_ms": value.target_venue_lower_bound_ms,
        "target_venue_ms": value.target_venue_ms,
        "transport_ledger_checkpoint_sha256": (
            value.transport_ledger_checkpoint_sha256
        ),
        "transport_times": {
            "durable_outbox_enqueue_ms": transport.durable_outbox_enqueue_ms,
            "observable_delivery_or_ack_ms": (
                transport.observable_delivery_or_ack_ms
            ),
            "provider_acceptance_completion_ms": (
                transport.provider_acceptance_completion_ms
            ),
            "request_completion_ms": transport.request_completion_ms,
            "response_first_byte_ms": transport.response_first_byte_ms,
            "send_start_ms": transport.send_start_ms,
        },
    }


def _position_document(value: MandatoryExitPositionV2) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "entry_execution_certificate_sha256": (
            value.entry_execution_certificate_sha256
        ),
        "entry_execution_event_id": value.entry_execution_event_id,
        "entry_execution_evidence_sha256": (
            value.entry_execution_evidence_sha256
        ),
        "entry_execution_payload_sha256": (
            value.entry_execution_payload_sha256
        ),
        "entry_notional": str(value.entry_notional),
        "entry_registry_checkpoint_sha256": (
            value.entry_registry_checkpoint_sha256
        ),
        "entry_registry_replay_root_sha256": (
            value.entry_registry_replay_root_sha256
        ),
        "entry_signal_event_id": value.entry_signal_event_id,
        "entry_target_venue_ms": value.entry_target_venue_ms,
        "entry_vwap": str(value.entry_vwap),
        "family": value.family.value,
        "initial_quantity": str(value.initial_quantity),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "role": "MANDATORY_EXIT_POSITION",
        "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
        "side": value.side.value,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _intent_identity_document(value: MandatoryExitIntentV2) -> dict[str, object]:
    return {
        "exit_decision_event_id": value.exit_decision_event_id,
        "position_event_id": value.position_event_id,
        "role": "MANDATORY_EXIT_INTENT",
    }


def _intent_document(
    value: MandatoryExitIntentV2,
    *,
    include_payload: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "attempt_id": value.attempt_id,
        "canonical_exit_decision_sha256": value.canonical_exit_decision_sha256,
        "entry_signal_event_id": value.entry_signal_event_id,
        "event_id": value.event_id,
        "exit_decision_cutoff_ms": value.exit_decision_cutoff_ms,
        "exit_decision_event_id": value.exit_decision_event_id,
        "exit_decision_payload_sha256": value.exit_decision_payload_sha256,
        "exit_reason": value.exit_reason,
        "family": value.family.value,
        "family_exit_registry_checkpoint_sha256": (
            value.family_exit_registry_checkpoint_sha256
        ),
        "family_rule_version": value.family_rule_version,
        "position_event_id": value.position_event_id,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "role": "MANDATORY_EXIT_INTENT",
        "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
        "symbol": value.symbol,
        "target_cursor": {
            **_target_cursor_document(value.target_cursor),
            "cursor_evidence_sha256": value.target_cursor.cursor_evidence_sha256,
        },
        "venue": value.venue.value,
    }
    if include_payload:
        document["payload_sha256"] = value.payload_sha256
    return document


def _generation_evidence_document(
    value: MandatoryExitBookGenerationV2,
) -> dict[str, object]:
    successors = _material_successors(value.closure)
    closure = value.closure
    if successors:
        finalized_through = value.generation_local_cursor_ms
    elif closure.finalized_through_local_ms < closure.closure_grace_end_local_ms:
        finalized_through = closure.finalized_through_local_ms
    else:
        finalized_through = closure.closure_grace_end_local_ms
    fee_document: object = None
    if value.fee_resolution is not None:
        fee_document = json.loads(
            canonical_fee_version_resolution_v2(value.fee_resolution)
        )
    return {
        "closure": {
            "closure_grace_end_local_ms": closure.closure_grace_end_local_ms,
            "continuous_health": (
                None
                if successors or closure.continuous_health is None
                else _health_document(closure.continuous_health)
            ),
            "finalization_grace_binding_sha256": (
                closure.finalization_grace_binding_sha256
            ),
            "finalized_through_local_ms": finalized_through,
            "quiet_rest_snapshot": (
                None
                if successors or closure.quiet_rest_snapshot is None
                else _quiet_document(closure.quiet_rest_snapshot)
            ),
            "successor_candidates": [_witness_document(item) for item in successors],
        },
        "exchange_info": _exchange_info_document(value.exchange_info),
        "fee_resolution": fee_document,
        "generation_local_cursor_ms": value.generation_local_cursor_ms,
        "generation_venue_ms": value.generation_venue_ms,
        "intent_event_id": value.intent_event_id,
        "lineage": _lineage_document(value.lineage),
        "mark": _mark_document(value.mark),
        "position_event_id": value.position_event_id,
        "pre_generation_depth_events": _canonical_depth_documents(
            value.pre_generation_depth_events
        ),
        "role": "MANDATORY_EXIT_BOOK_GENERATION_EVIDENCE",
        "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
        "snapshot": _snapshot_document(value.snapshot),
        "target_state_last_ingest_seq": value.target_state_last_ingest_seq,
    }


def _attempt_document(
    value: MandatoryExitAttemptV2,
    *,
    include_payload: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "closure_method": value.closure_method.value,
        "event_id": value.event_id,
        "executable_vwap": (
            None if value.executable_vwap is None else str(value.executable_vwap)
        ),
        "exit_side": value.exit_side.value,
        "fee_resolution_event_id": value.fee_resolution_event_id,
        "fee_resolution_payload_sha256": value.fee_resolution_payload_sha256,
        "fee_resolution_status": value.fee_resolution_status,
        "filled_quantity": str(value.filled_quantity),
        "generation_event_id": value.generation_event_id,
        "generation_evidence_sha256": value.generation_evidence_sha256,
        "generation_local_cursor_ms": value.generation_local_cursor_ms,
        "generation_venue_ms": value.generation_venue_ms,
        "gross_notional": str(value.gross_notional),
        "intent_event_id": value.intent_event_id,
        "level_fills": [
            {
                "level_revision_ingest_seq": item.level_revision_ingest_seq,
                "price": str(item.price),
                "quantity": str(item.quantity),
            }
            for item in value.level_fills
        ],
        "position_event_id": value.position_event_id,
        "primary_target_generation_missing": (
            value.primary_target_generation_missing
        ),
        "reasons": list(value.reasons),
        "requested_order_quantity": str(value.requested_order_quantity),
        "residual_after": str(value.residual_after),
        "residual_before": str(value.residual_before),
        "residual_is_filter_dust": value.residual_is_filter_dust,
        "role": "MANDATORY_EXIT_ATTEMPT",
        "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
        "signed_gross_cashflow": str(value.signed_gross_cashflow),
        "status": value.status.value,
        "target_state_last_ingest_seq": value.target_state_last_ingest_seq,
        "terminal_book_update_id": value.terminal_book_update_id,
    }
    if include_payload:
        document["payload_sha256"] = value.payload_sha256
    return document


def _terminal_document(
    value: MandatoryExitTerminalV2,
    *,
    include_payload: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "event_id": value.event_id,
        "family_inconclusive": value.family_inconclusive,
        "finalized_at_venue_ms": value.finalized_at_venue_ms,
        "intent_event_id": value.intent_event_id,
        "position_event_id": value.position_event_id,
        "reasons": list(value.reasons),
        "residual_quantity": str(value.residual_quantity),
        "role": "MANDATORY_EXIT_TERMINAL",
        "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
        "terminal_status": value.terminal_status.value,
    }
    if include_payload:
        document["payload_sha256"] = value.payload_sha256
    return document


def _state_document(value: MandatoryExitStateV2) -> dict[str, object]:
    return {
        "attempts": [
            _attempt_document(item, include_payload=True) for item in value.attempts
        ],
        "intent": (
            None
            if value.intent is None
            else _intent_document(value.intent, include_payload=True)
        ),
        "position": {
            **_position_document(value.position),
            "event_id": value.position.event_id,
        },
        "terminal": (
            None
            if value.terminal is None
            else _terminal_document(value.terminal, include_payload=True)
        ),
    }


def _checkpoint_document(value: MandatoryExitLedgerCheckpointV2) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "event_count": value.event_count,
        "maximum_events": value.maximum_events,
        "maximum_positions": value.maximum_positions,
        "position_count": value.position_count,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "replay_root_sha256": value.replay_root_sha256,
        "state_root_sha256": value.state_root_sha256,
    }


def _mandatory_exit_fee_certificate_document(
    value: MandatoryExitFeeCertificateV2,
    *,
    include_certificate_sha256: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "filled_exit_attempts": [
            _attempt_document(item, include_payload=True)
            for item in value.filled_exit_attempts
        ],
        "ledger_checkpoint": {
            **_checkpoint_document(value.ledger_checkpoint),
            "checkpoint_sha256": value.ledger_checkpoint.checkpoint_sha256,
        },
        "position": {
            **_position_document(value.position),
            "event_id": value.position.event_id,
        },
        "residual_quantity": str(value.residual_quantity),
        "schema_version": _FEE_CERTIFICATE_SCHEMA,
        "source_state_sha256": value.source_state_sha256,
        "terminal": (
            None
            if value.terminal is None
            else _terminal_document(value.terminal, include_payload=True)
        ),
    }
    if include_certificate_sha256:
        document["certificate_sha256"] = value.certificate_sha256
    return document


def _lineage_document(value: PaperFokLineageV2) -> dict[str, object]:
    return {
        "depth_snapshot_schema_sha256": value.depth_snapshot_schema_sha256,
        "exchange_info_schema_sha256": value.exchange_info_schema_sha256,
        "health_schema_sha256": value.health_schema_sha256,
        "lineage_sha256": value.lineage_sha256,
        "mark_schema_sha256": value.mark_schema_sha256,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "source_root_sha256": value.source_root_sha256,
        "standard_depth_schema_sha256": value.standard_depth_schema_sha256,
    }


def _levels_document(values: tuple[DepthLevelV2, ...]) -> list[dict[str, str]]:
    return [{"price": str(item.price), "quantity": str(item.quantity)} for item in values]


def _snapshot_document(value: FuturesDepthSnapshotV2) -> dict[str, object]:
    return {
        "asks": _levels_document(value.asks),
        "bids": _levels_document(value.bids),
        "depth_limit": value.depth_limit,
        "last_update_id": value.last_update_id,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "response_completion_ms": value.response_completion_ms,
        "schema_sha256": value.schema_sha256,
        "source_kind": value.source_kind,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _depth_event_document(value: FuturesStandardDepthEventV2) -> dict[str, object]:
    return {
        "asks": _levels_document(value.asks),
        "bids": _levels_document(value.bids),
        "event_time_ms": value.event_time_ms,
        "final_update_id": value.final_update_id,
        "first_update_id": value.first_update_id,
        "ingest_seq": value.ingest_seq,
        "pair": value.pair,
        "previous_final_update_id": value.previous_final_update_id,
        "previous_same_stream_ingest_seq": value.previous_same_stream_ingest_seq,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "receipt_completion_ms": value.receipt_completion_ms,
        "routing_status": value.routing_status,
        "schema_sha256": value.schema_sha256,
        "source_kind": value.source_kind,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "transaction_time_ms": value.transaction_time_ms,
        "venue": value.venue.value,
    }


def _canonical_depth_documents(
    values: tuple[FuturesStandardDepthEventV2, ...],
) -> list[dict[str, object]]:
    by_bytes: dict[bytes, dict[str, object]] = {}
    for value in values:
        document = _depth_event_document(value)
        by_bytes.setdefault(canonical_json_line(document), document)
    return [by_bytes[key] for key in sorted(by_bytes)]


def _material_successors(
    closure: PaperFokClosureEvidenceV2,
) -> tuple[FuturesDepthContinuityWitnessV2, ...]:
    if not closure.successor_candidates:
        return ()
    first_ingest = min(item.ingest_seq for item in closure.successor_candidates)
    by_bytes: dict[bytes, FuturesDepthContinuityWitnessV2] = {}
    for item in closure.successor_candidates:
        if item.ingest_seq == first_ingest:
            document = _witness_document(item)
            by_bytes.setdefault(canonical_json_line(document), item)
    return tuple(by_bytes[key] for key in sorted(by_bytes))


def _witness_document(value: FuturesDepthContinuityWitnessV2) -> dict[str, object]:
    return {
        "event_time_ms": value.event_time_ms,
        "final_update_id": value.final_update_id,
        "first_update_id": value.first_update_id,
        "ingest_seq": value.ingest_seq,
        "pair": value.pair,
        "previous_final_update_id": value.previous_final_update_id,
        "previous_same_stream_ingest_seq": value.previous_same_stream_ingest_seq,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "receipt_completion_ms": value.receipt_completion_ms,
        "routing_status": value.routing_status,
        "schema_sha256": value.schema_sha256,
        "source_kind": value.source_kind,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "transaction_time_ms": value.transaction_time_ms,
        "venue": value.venue.value,
    }


def _quiet_document(value: QuietRestSnapshotEvidenceV2) -> dict[str, object]:
    return {
        "last_update_id": value.last_update_id,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "response_completion_ms": value.response_completion_ms,
        "schema_sha256": value.schema_sha256,
        "source_kind": value.source_kind,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _health_document(value: ContinuousBookHealthEvidenceV2) -> dict[str, object]:
    return {
        "disconnect_count": value.disconnect_count,
        "generation": value.generation,
        "interval_end_local_ms": value.interval_end_local_ms,
        "interval_start_local_ms": value.interval_start_local_ms,
        "parser_error_count": value.parser_error_count,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "queue_drop_count": value.queue_drop_count,
        "schema_sha256": value.schema_sha256,
        "sequence_gap_count": value.sequence_gap_count,
        "source_kind": value.source_kind,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _mark_document(value: CausalMarkPriceEvidenceV2) -> dict[str, object]:
    return {
        "event_time_ms": value.event_time_ms,
        "mark_price": str(value.mark_price),
        "pair": value.pair,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "receipt_completion_ms": value.receipt_completion_ms,
        "routing_status": value.routing_status,
        "schema_sha256": value.schema_sha256,
        "source_kind": value.source_kind,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _filter_document(value: RawQuantityFilterV2 | None) -> dict[str, str] | None:
    if value is None:
        return None
    return {
        "max_qty": str(value.max_qty),
        "min_qty": str(value.min_qty),
        "step_size": str(value.step_size),
    }


def _exchange_info_document(
    value: FuturesExchangeInfoEvidenceV2,
) -> dict[str, object]:
    return {
        "applicable_filter_inventory_complete": (
            value.applicable_filter_inventory_complete
        ),
        "lot_size": _filter_document(value.lot_size),
        "market_lot_size": _filter_document(value.market_lot_size),
        "market_take_bound": str(value.market_take_bound),
        "max_notional": str(value.max_notional),
        "max_price": str(value.max_price),
        "min_notional": str(value.min_notional),
        "min_price": str(value.min_price),
        "percent_price_multiplier_down": (
            None
            if value.percent_price_multiplier_down is None
            else str(value.percent_price_multiplier_down)
        ),
        "percent_price_multiplier_up": (
            None
            if value.percent_price_multiplier_up is None
            else str(value.percent_price_multiplier_up)
        ),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "response_completion_ms": value.response_completion_ms,
        "schema_sha256": value.schema_sha256,
        "source_kind": value.source_kind,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "tick_size": str(value.tick_size),
        "venue": value.venue.value,
        "version_valid_from_local_ms": value.version_valid_from_local_ms,
        "version_valid_through_local_ms": value.version_valid_through_local_ms,
    }


def _validate_state_identity(state: MandatoryExitStateV2) -> None:
    assert state.intent is not None
    _validate_position_intent_match(state.position, state.intent)
    for attempt in state.attempts:
        if (
            attempt.position_event_id != state.position.event_id
            or attempt.intent_event_id != state.intent.event_id
        ):
            raise MandatoryExitContractErrorV2(
                "attempt refers to a different position or intent"
            )
    if state.terminal is not None and state.terminal.intent_event_id != (
        state.intent.event_id
    ):
        raise MandatoryExitContractErrorV2(
            "terminal refers to a different intent"
        )


def _validate_position_intent_match(
    position: MandatoryExitPositionV2,
    intent: MandatoryExitIntentV2,
) -> None:
    if (
        intent.position_event_id != position.event_id
        or intent.entry_signal_event_id != position.entry_signal_event_id
        or intent.attempt_id != position.attempt_id
        or intent.family is not position.family
        or intent.symbol != position.symbol
        or intent.venue is not position.venue
        or intent.promoting_plan_sha256 != position.promoting_plan_sha256
    ):
        raise MandatoryExitContractErrorV2(
            "exit intent identity differs from certified position"
        )


def _state_from_document(document: dict[str, object]) -> MandatoryExitStateV2:
    _require_exact_keys(document, {"attempts", "intent", "position", "terminal"}, "state")
    position = _position_from_document(
        _require_mapping(document["position"], "position")
    )
    raw_intent = document["intent"]
    intent = (
        None
        if raw_intent is None
        else _intent_from_document(_require_mapping(raw_intent, "intent"))
    )
    attempts = tuple(
        _attempt_from_document(_require_mapping(value, "attempt"))
        for value in _require_list(document["attempts"], "attempts")
    )
    raw_terminal = document["terminal"]
    terminal = (
        None
        if raw_terminal is None
        else _terminal_from_document(_require_mapping(raw_terminal, "terminal"))
    )
    return MandatoryExitStateV2(
        position=position,
        intent=intent,
        attempts=attempts,
        terminal=terminal,
    )


def _position_from_document(document: dict[str, object]) -> MandatoryExitPositionV2:
    expected_keys = set(_position_document_keys()) | {"event_id"}
    _require_exact_keys(document, expected_keys, "position")
    _require_role_and_rule(document, "MANDATORY_EXIT_POSITION")
    position = MandatoryExitPositionV2(
        attempt_id=_require_str(document["attempt_id"], "attempt_id"),
        family=PromotingFamilyV2(_require_str(document["family"], "family")),
        entry_signal_event_id=_require_str(
            document["entry_signal_event_id"],
            "entry_signal_event_id",
        ),
        entry_execution_event_id=_require_str(
            document["entry_execution_event_id"],
            "entry_execution_event_id",
        ),
        entry_execution_payload_sha256=_require_str(
            document["entry_execution_payload_sha256"],
            "entry_execution_payload_sha256",
        ),
        entry_execution_evidence_sha256=_require_str(
            document["entry_execution_evidence_sha256"],
            "entry_execution_evidence_sha256",
        ),
        entry_execution_certificate_sha256=_require_str(
            document["entry_execution_certificate_sha256"],
            "entry_execution_certificate_sha256",
        ),
        entry_registry_replay_root_sha256=_require_str(
            document["entry_registry_replay_root_sha256"],
            "entry_registry_replay_root_sha256",
        ),
        entry_registry_checkpoint_sha256=_require_str(
            document["entry_registry_checkpoint_sha256"],
            "entry_registry_checkpoint_sha256",
        ),
        symbol=_require_str(document["symbol"], "symbol"),
        venue=VenueV2(_require_str(document["venue"], "venue")),
        promoting_plan_sha256=_require_str(
            document["promoting_plan_sha256"],
            "promoting_plan_sha256",
        ),
        side=MandatoryExitPositionSideV2(
            _require_str(document["side"], "side")
        ),
        entry_target_venue_ms=_require_int(
            document["entry_target_venue_ms"],
            "entry_target_venue_ms",
        ),
        initial_quantity=_decimal_from_document(
            document["initial_quantity"],
            "initial_quantity",
        ),
        entry_vwap=_decimal_from_document(document["entry_vwap"], "entry_vwap"),
        entry_notional=_decimal_from_document(
            document["entry_notional"],
            "entry_notional",
        ),
        _factory_token=_POSITION_FACTORY_TOKEN,
    )
    if position.event_id != document["event_id"]:
        raise MandatoryExitContractErrorV2("position event ID differs on restore")
    return position


def _position_document_keys() -> tuple[str, ...]:
    return (
        "attempt_id",
        "entry_execution_certificate_sha256",
        "entry_execution_event_id",
        "entry_execution_evidence_sha256",
        "entry_execution_payload_sha256",
        "entry_notional",
        "entry_registry_checkpoint_sha256",
        "entry_registry_replay_root_sha256",
        "entry_signal_event_id",
        "entry_target_venue_ms",
        "entry_vwap",
        "family",
        "initial_quantity",
        "promoting_plan_sha256",
        "role",
        "rule_version",
        "side",
        "symbol",
        "venue",
    )


def _intent_from_document(document: dict[str, object]) -> MandatoryExitIntentV2:
    _require_exact_keys(
        document,
        {
            "attempt_id",
            "canonical_exit_decision_sha256",
            "entry_signal_event_id",
            "event_id",
            "exit_decision_cutoff_ms",
            "exit_decision_event_id",
            "exit_decision_payload_sha256",
            "exit_reason",
            "family",
            "family_exit_registry_checkpoint_sha256",
            "family_rule_version",
            "payload_sha256",
            "position_event_id",
            "promoting_plan_sha256",
            "role",
            "rule_version",
            "symbol",
            "target_cursor",
            "venue",
        },
        "intent",
    )
    _require_role_and_rule(document, "MANDATORY_EXIT_INTENT")
    cursor = _target_cursor_from_document(
        _require_mapping(document["target_cursor"], "target_cursor")
    )
    intent = MandatoryExitIntentV2(
        position_event_id=_require_str(
            document["position_event_id"],
            "position_event_id",
        ),
        entry_signal_event_id=_require_str(
            document["entry_signal_event_id"],
            "entry_signal_event_id",
        ),
        attempt_id=_require_str(document["attempt_id"], "attempt_id"),
        family=PromotingFamilyV2(_require_str(document["family"], "family")),
        symbol=_require_str(document["symbol"], "symbol"),
        venue=VenueV2(_require_str(document["venue"], "venue")),
        promoting_plan_sha256=_require_str(
            document["promoting_plan_sha256"],
            "promoting_plan_sha256",
        ),
        exit_decision_event_id=_require_str(
            document["exit_decision_event_id"],
            "exit_decision_event_id",
        ),
        exit_decision_payload_sha256=_require_str(
            document["exit_decision_payload_sha256"],
            "exit_decision_payload_sha256",
        ),
        canonical_exit_decision_sha256=_require_str(
            document["canonical_exit_decision_sha256"],
            "canonical_exit_decision_sha256",
        ),
        family_exit_registry_checkpoint_sha256=_require_str(
            document["family_exit_registry_checkpoint_sha256"],
            "family_exit_registry_checkpoint_sha256",
        ),
        family_rule_version=_require_str(
            document["family_rule_version"],
            "family_rule_version",
        ),
        exit_reason=_require_str(document["exit_reason"], "exit_reason"),
        exit_decision_cutoff_ms=_require_int(
            document["exit_decision_cutoff_ms"],
            "exit_decision_cutoff_ms",
        ),
        target_cursor=cursor,
        _factory_token=_INTENT_FACTORY_TOKEN,
    )
    if (
        intent.event_id != document["event_id"]
        or intent.payload_sha256 != document["payload_sha256"]
    ):
        raise MandatoryExitContractErrorV2(
            "intent event or payload hash differs on restore"
        )
    return intent


def _target_cursor_from_document(
    document: dict[str, object],
) -> MandatoryExitTargetCursorV2:
    _require_exact_keys(
        document,
        {
            "clock_segment_root_sha256",
            "contiguous_cursor_evidence",
            "cursor_evidence_sha256",
            "exit_decision_cutoff_ms",
            "mode",
            "prior_local_cursor_ms",
            "prior_venue_lower_bound_ms",
            "target_local_cursor_ms",
            "target_venue_lower_bound_ms",
            "target_venue_ms",
            "transport_ledger_checkpoint_sha256",
            "transport_times",
        },
        "target_cursor",
    )
    transport_document = _require_mapping(
        document["transport_times"],
        "transport_times",
    )
    _require_exact_keys(
        transport_document,
        {
            "durable_outbox_enqueue_ms",
            "observable_delivery_or_ack_ms",
            "provider_acceptance_completion_ms",
            "request_completion_ms",
            "response_first_byte_ms",
            "send_start_ms",
        },
        "transport_times",
    )
    transport = AlertTransportTimesV2(
        durable_outbox_enqueue_ms=_require_int(
            transport_document["durable_outbox_enqueue_ms"],
            "durable_outbox_enqueue_ms",
        ),
        send_start_ms=_require_int(
            transport_document["send_start_ms"],
            "send_start_ms",
        ),
        response_first_byte_ms=_optional_int(
            transport_document["response_first_byte_ms"],
            "response_first_byte_ms",
        ),
        provider_acceptance_completion_ms=_optional_int(
            transport_document["provider_acceptance_completion_ms"],
            "provider_acceptance_completion_ms",
        ),
        request_completion_ms=_optional_int(
            transport_document["request_completion_ms"],
            "request_completion_ms",
        ),
        observable_delivery_or_ack_ms=_optional_int(
            transport_document["observable_delivery_or_ack_ms"],
            "observable_delivery_or_ack_ms",
        ),
    )
    cursor = build_mandatory_exit_target_cursor_v2(
        exit_decision_cutoff_ms=_require_int(
            document["exit_decision_cutoff_ms"],
            "exit_decision_cutoff_ms",
        ),
        transport_times=transport,
        transport_ledger_checkpoint_sha256=_require_str(
            document["transport_ledger_checkpoint_sha256"],
            "transport_ledger_checkpoint_sha256",
        ),
        target_venue_ms=_require_int(document["target_venue_ms"], "target_venue_ms"),
        prior_local_cursor_ms=_require_int(
            document["prior_local_cursor_ms"],
            "prior_local_cursor_ms",
        ),
        prior_venue_lower_bound_ms=_require_int(
            document["prior_venue_lower_bound_ms"],
            "prior_venue_lower_bound_ms",
        ),
        target_local_cursor_ms=_require_int(
            document["target_local_cursor_ms"],
            "target_local_cursor_ms",
        ),
        target_venue_lower_bound_ms=_require_int(
            document["target_venue_lower_bound_ms"],
            "target_venue_lower_bound_ms",
        ),
        clock_segment_root_sha256=_require_str(
            document["clock_segment_root_sha256"],
            "clock_segment_root_sha256",
        ),
        contiguous_cursor_evidence=_require_bool(
            document["contiguous_cursor_evidence"],
            "contiguous_cursor_evidence",
        ),
    )
    if (
        cursor.mode.value != document["mode"]
        or cursor.cursor_evidence_sha256 != document["cursor_evidence_sha256"]
    ):
        raise MandatoryExitContractErrorV2(
            "target mode or evidence hash differs on restore"
        )
    return cursor


def _attempt_from_document(document: dict[str, object]) -> MandatoryExitAttemptV2:
    _require_exact_keys(
        document,
        {
            "closure_method",
            "event_id",
            "executable_vwap",
            "exit_side",
            "fee_resolution_event_id",
            "fee_resolution_payload_sha256",
            "fee_resolution_status",
            "filled_quantity",
            "generation_event_id",
            "generation_evidence_sha256",
            "generation_local_cursor_ms",
            "generation_venue_ms",
            "gross_notional",
            "intent_event_id",
            "level_fills",
            "payload_sha256",
            "position_event_id",
            "primary_target_generation_missing",
            "reasons",
            "requested_order_quantity",
            "residual_after",
            "residual_before",
            "residual_is_filter_dust",
            "role",
            "rule_version",
            "signed_gross_cashflow",
            "status",
            "target_state_last_ingest_seq",
            "terminal_book_update_id",
        },
        "attempt",
    )
    _require_role_and_rule(document, "MANDATORY_EXIT_ATTEMPT")
    fills = tuple(
        _level_fill_from_document(_require_mapping(value, "level_fill"))
        for value in _require_list(document["level_fills"], "level_fills")
    )
    raw_vwap = document["executable_vwap"]
    attempt = MandatoryExitAttemptV2(
        position_event_id=_require_str(
            document["position_event_id"],
            "position_event_id",
        ),
        intent_event_id=_require_str(document["intent_event_id"], "intent_event_id"),
        generation_event_id=_require_str(
            document["generation_event_id"],
            "generation_event_id",
        ),
        generation_evidence_sha256=_require_str(
            document["generation_evidence_sha256"],
            "generation_evidence_sha256",
        ),
        generation_venue_ms=_require_int(
            document["generation_venue_ms"],
            "generation_venue_ms",
        ),
        generation_local_cursor_ms=_require_int(
            document["generation_local_cursor_ms"],
            "generation_local_cursor_ms",
        ),
        target_state_last_ingest_seq=_require_int(
            document["target_state_last_ingest_seq"],
            "target_state_last_ingest_seq",
        ),
        terminal_book_update_id=_optional_int(
            document["terminal_book_update_id"],
            "terminal_book_update_id",
        ),
        status=MandatoryExitAttemptStatusV2(
            _require_str(document["status"], "status")
        ),
        closure_method=PaperFokClosureMethodV2(
            _require_str(document["closure_method"], "closure_method")
        ),
        exit_side=PaperFokSideV2(
            _require_str(document["exit_side"], "exit_side")
        ),
        residual_before=_decimal_from_document(
            document["residual_before"],
            "residual_before",
        ),
        requested_order_quantity=_decimal_from_document(
            document["requested_order_quantity"],
            "requested_order_quantity",
        ),
        filled_quantity=_decimal_from_document(
            document["filled_quantity"],
            "filled_quantity",
        ),
        residual_after=_decimal_from_document(
            document["residual_after"],
            "residual_after",
        ),
        executable_vwap=(
            None
            if raw_vwap is None
            else _decimal_from_document(raw_vwap, "executable_vwap")
        ),
        gross_notional=_decimal_from_document(
            document["gross_notional"],
            "gross_notional",
        ),
        signed_gross_cashflow=_decimal_from_document(
            document["signed_gross_cashflow"],
            "signed_gross_cashflow",
            allow_negative=True,
        ),
        level_fills=fills,
        residual_is_filter_dust=_require_bool(
            document["residual_is_filter_dust"],
            "residual_is_filter_dust",
        ),
        primary_target_generation_missing=_require_bool(
            document["primary_target_generation_missing"],
            "primary_target_generation_missing",
        ),
        fee_resolution_event_id=_optional_str(
            document["fee_resolution_event_id"],
            "fee_resolution_event_id",
        ),
        fee_resolution_payload_sha256=_optional_str(
            document["fee_resolution_payload_sha256"],
            "fee_resolution_payload_sha256",
        ),
        fee_resolution_status=_optional_str(
            document["fee_resolution_status"],
            "fee_resolution_status",
        ),
        reasons=_string_tuple(document["reasons"], "reasons"),
        _factory_token=_ATTEMPT_FACTORY_TOKEN,
    )
    if (
        attempt.event_id != document["event_id"]
        or attempt.payload_sha256 != document["payload_sha256"]
    ):
        raise MandatoryExitContractErrorV2(
            "attempt event or payload hash differs on restore"
        )
    return attempt


def _level_fill_from_document(
    document: dict[str, object],
) -> MandatoryExitLevelFillV2:
    _require_exact_keys(
        document,
        {"level_revision_ingest_seq", "price", "quantity"},
        "level_fill",
    )
    return MandatoryExitLevelFillV2(
        price=_decimal_from_document(document["price"], "price"),
        quantity=_decimal_from_document(document["quantity"], "quantity"),
        level_revision_ingest_seq=_require_int(
            document["level_revision_ingest_seq"],
            "level_revision_ingest_seq",
        ),
    )


def _terminal_from_document(document: dict[str, object]) -> MandatoryExitTerminalV2:
    _require_exact_keys(
        document,
        {
            "event_id",
            "family_inconclusive",
            "finalized_at_venue_ms",
            "intent_event_id",
            "payload_sha256",
            "position_event_id",
            "reasons",
            "residual_quantity",
            "role",
            "rule_version",
            "terminal_status",
        },
        "terminal",
    )
    _require_role_and_rule(document, "MANDATORY_EXIT_TERMINAL")
    terminal = MandatoryExitTerminalV2(
        position_event_id=_require_str(
            document["position_event_id"],
            "position_event_id",
        ),
        intent_event_id=_require_str(document["intent_event_id"], "intent_event_id"),
        terminal_status=MandatoryExitTerminalStatusV2(
            _require_str(document["terminal_status"], "terminal_status")
        ),
        finalized_at_venue_ms=_require_int(
            document["finalized_at_venue_ms"],
            "finalized_at_venue_ms",
        ),
        residual_quantity=_decimal_from_document(
            document["residual_quantity"],
            "residual_quantity",
        ),
        family_inconclusive=_require_bool(
            document["family_inconclusive"],
            "family_inconclusive",
        ),
        reasons=_string_tuple(document["reasons"], "reasons"),
        _factory_token=_TERMINAL_FACTORY_TOKEN,
    )
    if (
        terminal.event_id != document["event_id"]
        or terminal.payload_sha256 != document["payload_sha256"]
    ):
        raise MandatoryExitContractErrorV2(
            "terminal event or payload hash differs on restore"
        )
    return terminal


def _checkpoint_from_document(
    document: dict[str, object],
) -> MandatoryExitLedgerCheckpointV2:
    _require_exact_keys(
        document,
        {
            "attempt_id",
            "checkpoint_sha256",
            "event_count",
            "maximum_events",
            "maximum_positions",
            "position_count",
            "promoting_plan_sha256",
            "replay_root_sha256",
            "state_root_sha256",
        },
        "checkpoint",
    )
    checkpoint = MandatoryExitLedgerCheckpointV2(
        attempt_id=_require_str(document["attempt_id"], "attempt_id"),
        promoting_plan_sha256=_require_str(
            document["promoting_plan_sha256"],
            "promoting_plan_sha256",
        ),
        replay_root_sha256=_require_str(
            document["replay_root_sha256"],
            "replay_root_sha256",
        ),
        state_root_sha256=_require_str(
            document["state_root_sha256"],
            "state_root_sha256",
        ),
        event_count=_require_int(document["event_count"], "event_count"),
        position_count=_require_int(document["position_count"], "position_count"),
        maximum_events=_require_int(document["maximum_events"], "maximum_events"),
        maximum_positions=_require_int(
            document["maximum_positions"],
            "maximum_positions",
        ),
    )
    if checkpoint.checkpoint_sha256 != document["checkpoint_sha256"]:
        raise MandatoryExitContractErrorV2(
            "checkpoint hash differs from canonical content"
        )
    return checkpoint


def _require_mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise MandatoryExitContractErrorV2(f"{name} must be an object")
    return value


def _require_list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise MandatoryExitContractErrorV2(f"{name} must be a list")
    return value


def _require_exact_keys(
    value: dict[str, object],
    expected: set[str],
    name: str,
) -> None:
    if set(value) != expected:
        raise MandatoryExitContractErrorV2(f"{name} keys differ from schema")


def _require_str(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise MandatoryExitContractErrorV2(f"{name} must be a string")
    return value


def _optional_str(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _require_str(value, name)


def _require_int(value: object, name: str) -> int:
    if type(value) is not int:
        raise MandatoryExitContractErrorV2(f"{name} must be an integer")
    return value


def _optional_int(value: object, name: str) -> int | None:
    if value is None:
        return None
    return _require_int(value, name)


def _require_bool(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise MandatoryExitContractErrorV2(f"{name} must be boolean")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    items = _require_list(value, name)
    if any(not isinstance(item, str) for item in items):
        raise MandatoryExitContractErrorV2(f"{name} must contain strings")
    return tuple(item for item in items if isinstance(item, str))


def _decimal_from_document(
    value: object,
    name: str,
    *,
    allow_negative: bool = False,
) -> Decimal:
    text = _require_str(value, name)
    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise MandatoryExitContractErrorV2(f"{name} is not Decimal text") from exc
    if not result.is_finite() or (not allow_negative and result < 0):
        raise MandatoryExitContractErrorV2(f"{name} is outside Decimal bounds")
    return result


def _require_role_and_rule(document: dict[str, object], role: str) -> None:
    if (
        document.get("role") != role
        or document.get("rule_version") != MANDATORY_EXIT_RULE_VERSION_V2
    ):
        raise MandatoryExitContractErrorV2("role or rule version differs")


def _validate_nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int or value < 0:
        raise MandatoryExitContractErrorV2(f"{name} must be a nonnegative integer")


def _validate_sha256(value: object, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise MandatoryExitContractErrorV2(f"{name} must be lowercase SHA-256")


def _validate_identity(value: object, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 256
        or any(ord(character) < 32 for character in value)
    ):
        raise MandatoryExitContractErrorV2(f"{name} must be a bounded identity")


def _validate_symbol(value: object) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise MandatoryExitContractErrorV2(
            "symbol must be a normalized USDT symbol"
        )


def _validate_nonnegative_decimal(value: object, name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value < 0:
        raise MandatoryExitContractErrorV2(
            f"{name} must be a nonnegative finite Decimal"
        )


def _validate_positive_decimal(value: object, name: str) -> None:
    _validate_nonnegative_decimal(value, name)
    assert isinstance(value, Decimal)
    if value == 0:
        raise MandatoryExitContractErrorV2(
            f"{name} must be a positive finite Decimal"
        )


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values:
        raise MandatoryExitContractErrorV2("reasons must be a nonempty tuple")
    for value in values:
        _validate_identity(value, "reason")
