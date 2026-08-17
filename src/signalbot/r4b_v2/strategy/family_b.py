from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from fractions import Fraction
from threading import RLock
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    PRIMARY_PAPER_TARGET_DELAY_MS_V2,
    PaperFokDecisionRegistryV2,
    PaperFokEntryDecisionV2,
    PaperFokEntryStatusV2,
    PaperFokFullFillCertificateV2,
    PaperFokSideV2,
    canonical_paper_fok_entry_decision_v2,
    canonical_paper_fok_full_fill_certificate_v2,
    issue_paper_fok_full_fill_certificate_v2,
)
from signalbot.r4b_v2.protocol import decision_clock as _decision_clock
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.family_b_features import (
    FamilyBExitFeatureEvidenceV2,
    FamilyBFeatureEvidenceV2,
    FamilyBFeatureReadinessV2,
    canonical_family_b_exit_feature_evidence_v2,
    canonical_family_b_feature_evidence_v2,
)

FAMILY_B_RULE_VERSION_V2 = "R4B_CAUSAL_V2.3.0_FAMILY_B"
FIVE_MINUTE_MS_V2 = _decision_clock.FIVE_MINUTE_MS_V2
DECISION_DELAY_MS_V2 = _decision_clock.DECISION_DELAY_MS_V2
FAMILY_B_HARD_HORIZON_BARS_V2 = 3

_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_RZ_FLOW_MIN = Decimal("2.0")
_B1_ALIGNED_RETURN_MIN = Decimal("1.0")
_B2_ALIGNED_RETURN_MAX = Decimal("0.25")
_B2_ABSOLUTE_RETURN_MAX = Decimal("0.75")
_SPREAD95_MAX_BPS = Decimal("20")
_EXIT_FLOW_REVERSAL = Decimal("-0.30")
_DECISION_ID_DOMAIN = b"R4B_FAMILY_B_DECISION_V2\0"
_EXIT_ID_DOMAIN = b"R4B_FAMILY_B_EXIT_V2\0"
_ENTRY_PAYLOAD_DOMAIN: Final = b"R4B_FAMILY_B_ENTRY_PAYLOAD_V2\0"
_EXIT_PAYLOAD_DOMAIN: Final = b"R4B_FAMILY_B_EXIT_PAYLOAD_V2\0"
_REGISTRY_REPLAY_DOMAIN: Final = b"R4B_FAMILY_B_REGISTRY_REPLAY_V2\0"
_REGISTRY_STATE_SCHEMA: Final = "r4b_family_b_atomic_episode_registry_state_v3"
_ENTRY_INPUT_DOMAIN: Final = b"R4B_FAMILY_B_ENTRY_INPUT_V2\0"
_EXIT_INPUT_DOMAIN: Final = b"R4B_FAMILY_B_EXIT_INPUT_V2\0"
_POSITION_PAYLOAD_DOMAIN: Final = b"R4B_FAMILY_B_POSITION_PAYLOAD_V2\0"
_POSITION_FACTORY_TOKEN: Final = object()
_ENTRY_PREVIEW_FACTORY_TOKEN: Final = object()
_ENTRY_COMMIT_RECEIPT_FACTORY_TOKEN: Final = object()
_EXIT_DECISION_FACTORY_TOKEN: Final = object()
_ADMISSION_RECEIPT_FACTORY_TOKEN: Final = object()
_EXIT_MUTATION_RECEIPT_FACTORY_TOKEN: Final = object()


class FamilyBContractError(ValueError):
    """Raised when a caller violates an immutable Family B domain contract."""


class FamilyBSideV2(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class FamilyBChildV2(StrEnum):
    B1 = "B1"
    B2 = "B2"


class FamilyBEntryStatusV2(StrEnum):
    SIGNAL = "SIGNAL"
    NO_SIGNAL = "NO_SIGNAL"
    NOT_ADMITTED_ACTIVE_POSITION = "NOT_ADMITTED_ACTIVE_POSITION"
    FEATURE_NOT_READY = "FEATURE_NOT_READY"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    DATA_INVALID = "DATA_INVALID"
    DATA_INVALID_RULE_INVARIANT = "DATA_INVALID_RULE_INVARIANT"


class FamilyBEntryCommitDispositionV2(StrEnum):
    NEW_BY_THIS_TRANSACTION = "NEW_BY_THIS_TRANSACTION"
    PREEXISTING = "PREEXISTING"


class FamilyBAdmissionDispositionV2(StrEnum):
    NEW_BY_THIS_TRANSACTION = "NEW_BY_THIS_TRANSACTION"
    PREEXISTING = "PREEXISTING"


class FamilyBExitDispositionV2(StrEnum):
    NEW_BY_THIS_TRANSACTION = "NEW_BY_THIS_TRANSACTION"
    PREEXISTING = "PREEXISTING"


class FamilyBExitActionV2(StrEnum):
    HOLD = "HOLD"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"


class FamilyBExitReasonV2(StrEnum):
    HOLD = "HOLD"
    MANDATORY_DATA_EMERGENCY = "MANDATORY_DATA_EMERGENCY"
    MANDATORY_TERMINAL_EMERGENCY = "MANDATORY_TERMINAL_EMERGENCY"
    ADVERSE_INVALIDATION = "ADVERSE_INVALIDATION"
    FLOW_REVERSAL = "FLOW_REVERSAL"
    HARD_HORIZON = "HARD_HORIZON"


class FamilyBMandatoryExitV2(StrEnum):
    DATA = "DATA"
    TERMINAL = "TERMINAL"


@dataclass(frozen=True, slots=True)
class FamilyBChildResolutionV2:
    """Fail-closed result of enforcing the B1/B2 mutual-exclusion invariant."""

    status: FamilyBEntryStatusV2
    child: FamilyBChildV2 | None

    def __post_init__(self) -> None:
        if self.status is FamilyBEntryStatusV2.SIGNAL:
            if self.child is None:
                raise FamilyBContractError("SIGNAL child resolution requires B1 or B2")
            return
        if self.child is not None:
            raise FamilyBContractError("non-signal child resolution cannot expose a child")
        if self.status not in (
            FamilyBEntryStatusV2.NO_SIGNAL,
            FamilyBEntryStatusV2.DATA_INVALID_RULE_INVARIANT,
        ):
            raise FamilyBContractError("unsupported Family B child resolution status")


@dataclass(frozen=True, slots=True)
class FamilyBEntryInputV2:
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    feature_evidence: FamilyBFeatureEvidenceV2

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyBContractError("Family B entry requires USD-M Futures provenance")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_bar_times(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        if not isinstance(self.feature_evidence, FamilyBFeatureEvidenceV2):
            raise FamilyBContractError(
                "feature_evidence must come from the causal Family B factory"
            )
        evidence_identity = (
            self.feature_evidence.attempt_id,
            self.feature_evidence.symbol,
            self.feature_evidence.venue,
            self.feature_evidence.promoting_plan_sha256,
            self.feature_evidence.bar_open_ms,
            self.feature_evidence.bar_close_ms,
            self.feature_evidence.decision_cutoff_ms,
        )
        input_identity = (
            self.attempt_id,
            self.symbol,
            self.venue,
            self.promoting_plan_sha256,
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        if evidence_identity != input_identity:
            raise FamilyBContractError("entry identity differs from its bound feature evidence")

    @property
    def closed_bar(self) -> bool:
        return True

    @property
    def causal_inputs_complete(self) -> bool:
        return True

    @property
    def complete_10bp_band_for_full_bar(self) -> bool:
        return True

    @property
    def positive_duration_in_every_window(self) -> bool:
        return True

    @property
    def flow_imbalance_current(self) -> Decimal:
        return self.feature_evidence.flow_imbalance_current

    @property
    def rz_flow_imbalance_current(self) -> Decimal | None:
        return self.feature_evidence.rz_flow_imbalance_current

    @property
    def rz_bar_return_current(self) -> Decimal | None:
        return self.feature_evidence.rz_bar_return_current

    @property
    def d_start(self) -> Decimal:
        return self.feature_evidence.d_start

    @property
    def d_low(self) -> Decimal:
        return self.feature_evidence.d_low

    @property
    def d_end(self) -> Decimal:
        return self.feature_evidence.d_end

    @property
    def spread95_bps(self) -> Decimal:
        return self.feature_evidence.spread95_bps

    @property
    def high_current(self) -> Decimal:
        return self.feature_evidence.high_current

    @property
    def low_current(self) -> Decimal:
        return self.feature_evidence.low_current

    @property
    def previous_close(self) -> Decimal:
        return self.feature_evidence.previous_close


@dataclass(frozen=True, slots=True)
class FamilyBEntryDecisionV2:
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    feature_evidence_sha256: str
    feature_source_root_sha256: str
    status: FamilyBEntryStatusV2
    child: FamilyBChildV2 | None
    side: FamilyBSideV2 | None
    reasons: tuple[str, ...]
    invalidation: str
    flow_sign: int
    event_true_range: Decimal | None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FAMILY_B_RULE_VERSION_V2)

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyBContractError("Family B decision must retain USD-M Futures provenance")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_bar_times(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        _validate_sha256(self.feature_evidence_sha256, "feature_evidence_sha256")
        _validate_sha256(self.feature_source_root_sha256, "feature_source_root_sha256")
        _validate_entry_decision_state(self)
        event_id = hashlib.sha256(
            _DECISION_ID_DOMAIN + canonical_json_line(_entry_identity_document(self))
        ).hexdigest()
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = hashlib.sha256(
            _ENTRY_PAYLOAD_DOMAIN
            + canonical_json_line(_entry_decision_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def emitted_signal(self) -> bool:
        return self.status is FamilyBEntryStatusV2.SIGNAL


@dataclass(frozen=True, slots=True)
class FamilyBEntryPreviewV2:
    """Factory-sealed, non-mutating snapshot for one transactional entry."""

    input_sha256: str
    pre_replay_root_sha256: str
    pre_event_count: int
    decision: FamilyBEntryDecisionV2
    already_committed: bool
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ENTRY_PREVIEW_FACTORY_TOKEN:
            raise FamilyBContractError("Family B entry previews must be created by the registry")
        _validate_sha256(self.input_sha256, "input_sha256")
        _validate_sha256(self.pre_replay_root_sha256, "pre_replay_root_sha256")
        _validate_nonnegative_int(self.pre_event_count, "pre_event_count")
        if not isinstance(self.decision, FamilyBEntryDecisionV2):
            raise FamilyBContractError("preview decision must be FamilyBEntryDecisionV2")
        canonical_family_b_entry_decision_v2(self.decision)
        if type(self.already_committed) is not bool:
            raise FamilyBContractError("already_committed must be boolean")


@dataclass(frozen=True, slots=True)
class FamilyBEntryCommitReceiptV2:
    """Ephemeral capability proving which transaction created an entry."""

    input_sha256: str
    event_id: str
    decision: FamilyBEntryDecisionV2
    preview_already_committed: bool
    pre_root_sha256: str
    pre_event_count: int
    post_root_sha256: str
    post_event_count: int
    disposition: FamilyBEntryCommitDispositionV2
    _owner_token: object = field(repr=False, compare=False)
    _rollback_capability: object = field(repr=False, compare=False)
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ENTRY_COMMIT_RECEIPT_FACTORY_TOKEN:
            raise FamilyBContractError(
                "Family B entry commit receipts must be created by the registry"
            )
        _validate_sha256(self.input_sha256, "input_sha256")
        _validate_sha256(self.event_id, "event_id")
        _validate_sha256(self.pre_root_sha256, "pre_root_sha256")
        _validate_sha256(self.post_root_sha256, "post_root_sha256")
        _validate_nonnegative_int(self.pre_event_count, "pre_event_count")
        _validate_nonnegative_int(self.post_event_count, "post_event_count")
        if not isinstance(self.decision, FamilyBEntryDecisionV2):
            raise FamilyBContractError("receipt decision must be FamilyBEntryDecisionV2")
        canonical_family_b_entry_decision_v2(self.decision)
        if self.event_id != self.decision.event_id:
            raise FamilyBContractError("receipt event differs from its decision")
        if type(self.preview_already_committed) is not bool:
            raise FamilyBContractError("preview_already_committed must be boolean")
        if not isinstance(self.disposition, FamilyBEntryCommitDispositionV2):
            raise FamilyBContractError("disposition must be FamilyBEntryCommitDispositionV2")
        if self.disposition is FamilyBEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION:
            if self.preview_already_committed:
                raise FamilyBContractError("pre-existing preview cannot claim a new commit")
            if (
                self.post_event_count != self.pre_event_count + 1
                or self.post_root_sha256 == self.pre_root_sha256
            ):
                raise FamilyBContractError("new commit receipt has invalid post-state")
            return
        if self.preview_already_committed:
            if (
                self.post_event_count != self.pre_event_count
                or self.post_root_sha256 != self.pre_root_sha256
            ):
                raise FamilyBContractError("pre-existing replay receipt must preserve its state")
            return
        if (
            self.post_event_count != self.pre_event_count + 1
            or self.post_root_sha256 == self.pre_root_sha256
        ):
            raise FamilyBContractError("concurrent pre-existing receipt has invalid post-state")


@dataclass(frozen=True, slots=True)
class FamilyBPositionV2:
    """Frozen rule state admitted only by a registry-pinned full PAPER fill."""

    entry_event_id: str
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    feature_evidence_sha256: str
    feature_source_root_sha256: str
    admission_evidence_sha256: str
    paper_decision_event_id: str
    paper_decision_payload_sha256: str
    paper_registry_root_sha256: str
    paper_registry_event_count: int
    paper_registry_checkpoint_sha256: str
    paper_requested_quantity: Decimal
    paper_filled_quantity: Decimal
    paper_executable_notional: Decimal
    child: FamilyBChildV2
    side: FamilyBSideV2
    flow_sign: int
    signal_bar_open_ms: int
    entry_vwap: Decimal
    event_true_range: Decimal
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _POSITION_FACTORY_TOKEN:
            raise FamilyBContractError(
                "Family B position requires a registry-pinned full PAPER fill"
            )
        _validate_sha256(self.entry_event_id, "entry_event_id")
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyBContractError("Family B position must retain USD-M Futures provenance")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_sha256(self.feature_evidence_sha256, "feature_evidence_sha256")
        _validate_sha256(self.feature_source_root_sha256, "feature_source_root_sha256")
        for value, field_name in (
            (self.admission_evidence_sha256, "admission_evidence_sha256"),
            (self.paper_decision_event_id, "paper_decision_event_id"),
            (
                self.paper_decision_payload_sha256,
                "paper_decision_payload_sha256",
            ),
            (self.paper_registry_root_sha256, "paper_registry_root_sha256"),
            (
                self.paper_registry_checkpoint_sha256,
                "paper_registry_checkpoint_sha256",
            ),
        ):
            _validate_sha256(value, field_name)
        _validate_nonnegative_int(
            self.paper_registry_event_count,
            "paper_registry_event_count",
        )
        if self.paper_registry_event_count < 1:
            raise FamilyBContractError("paper registry checkpoint cannot be empty")
        if not all(
            _is_positive_finite(value)
            for value in (
                self.paper_requested_quantity,
                self.paper_filled_quantity,
                self.paper_executable_notional,
            )
        ):
            raise FamilyBContractError(
                "PAPER quantities and executable notional must be positive finite"
            )
        if self.paper_requested_quantity != self.paper_filled_quantity:
            raise FamilyBContractError("position requires requested equals full fill")
        if not isinstance(self.child, FamilyBChildV2):
            raise FamilyBContractError("child must be B1 or B2")
        if not isinstance(self.side, FamilyBSideV2):
            raise FamilyBContractError("side must be LONG or SHORT")
        if self.flow_sign not in (-1, 1):
            raise FamilyBContractError("flow_sign must be -1 or 1")
        expected_position_sign = (
            self.flow_sign if self.child is FamilyBChildV2.B1 else -self.flow_sign
        )
        if self.position_sign != expected_position_sign:
            raise FamilyBContractError("position side differs from child direction rule")
        _validate_nonnegative_int(self.signal_bar_open_ms, "signal_bar_open_ms")
        if self.signal_bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
            raise FamilyBContractError("signal bar must be aligned to a 5m UTC boundary")
        if not _is_positive_finite(self.entry_vwap):
            raise FamilyBContractError("entry_vwap must be positive finite Decimal")
        with localcontext(protocol_decimal_context_v2()):
            if self.paper_filled_quantity * self.entry_vwap != self.paper_executable_notional:
                raise FamilyBContractError("PAPER quantity, VWAP, and executable notional disagree")
        if not _is_nonnegative_finite(self.event_true_range):
            raise FamilyBContractError("event_true_range must be nonnegative finite Decimal")

    @property
    def position_sign(self) -> int:
        return 1 if self.side is FamilyBSideV2.LONG else -1


@dataclass(frozen=True, slots=True)
class FamilyBExitInputV2:
    position: FamilyBPositionV2
    mandatory_exit: FamilyBMandatoryExitV2 | None
    exit_feature_evidence: FamilyBExitFeatureEvidenceV2

    def __post_init__(self) -> None:
        if not isinstance(self.position, FamilyBPositionV2):
            raise FamilyBContractError("position must be a FamilyBPositionV2")
        if self.mandatory_exit is not None and not isinstance(
            self.mandatory_exit, FamilyBMandatoryExitV2
        ):
            raise FamilyBContractError("mandatory_exit has an unsupported value")
        if not isinstance(self.exit_feature_evidence, FamilyBExitFeatureEvidenceV2):
            raise FamilyBContractError(
                "exit_feature_evidence must come from the causal exit factory"
            )
        evidence_identity = (
            self.exit_feature_evidence.attempt_id,
            self.exit_feature_evidence.symbol,
            self.exit_feature_evidence.venue,
            self.exit_feature_evidence.promoting_plan_sha256,
        )
        position_identity = (
            self.position.attempt_id,
            self.position.symbol,
            self.position.venue,
            self.position.promoting_plan_sha256,
        )
        if evidence_identity != position_identity:
            raise FamilyBContractError("exit evidence identity differs from its bound position")
        if self.bar_open_ms <= self.position.signal_bar_open_ms:
            raise FamilyBContractError("exit evaluation must follow the signal bar")

    @property
    def bar_open_ms(self) -> int:
        return self.exit_feature_evidence.bar_open_ms

    @property
    def bar_close_ms(self) -> int:
        return self.exit_feature_evidence.bar_close_ms

    @property
    def decision_cutoff_ms(self) -> int:
        return self.exit_feature_evidence.decision_cutoff_ms

    @property
    def close_price(self) -> Decimal:
        return self.exit_feature_evidence.close_price

    @property
    def flow_imbalance_current(self) -> Decimal:
        return self.exit_feature_evidence.flow_imbalance_current


@dataclass(frozen=True, slots=True)
class FamilyBExitDecisionV2:
    entry_event_id: str
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    position_side: FamilyBSideV2
    exit_evidence_sha256: str
    exit_source_root_sha256: str
    action: FamilyBExitActionV2
    reason: FamilyBExitReasonV2
    reasons: tuple[str, ...]
    invalidation: str
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FAMILY_B_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object) -> None:
        _validate_sha256(self.entry_event_id, "entry_event_id")
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyBContractError("Family B exit must retain USD-M Futures provenance")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_bar_times(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        if not isinstance(self.position_side, FamilyBSideV2):
            raise FamilyBContractError("position_side must be LONG or SHORT")
        _validate_sha256(self.exit_evidence_sha256, "exit_evidence_sha256")
        _validate_sha256(self.exit_source_root_sha256, "exit_source_root_sha256")
        _validate_exit_decision_state(self)
        event_id = hashlib.sha256(
            _EXIT_ID_DOMAIN + canonical_json_line(_exit_identity_document(self))
        ).hexdigest()
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = hashlib.sha256(
            _EXIT_PAYLOAD_DOMAIN
            + canonical_json_line(_exit_decision_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)
        if _factory_token is not _EXIT_DECISION_FACTORY_TOKEN:
            raise FamilyBContractError("Family B exit decisions must be created by the evaluator")

    @property
    def exits_position(self) -> bool:
        return self.action is not FamilyBExitActionV2.HOLD


@dataclass(frozen=True, slots=True)
class FamilyBAdmissionReceiptV2:
    """Ephemeral exact-owner proof for one PAPER-backed position admission."""

    input_sha256: str
    decision: FamilyBEntryDecisionV2
    position: FamilyBPositionV2
    position_sha256: str
    paper_decision: PaperFokEntryDecisionV2
    paper_certificate: PaperFokFullFillCertificateV2
    paper_registry_root_sha256: str
    paper_registry_event_count: int
    paper_registry_maximum_events: int
    paper_registry_checkpoint_sha256: str
    pre_root_sha256: str
    pre_event_count: int
    post_root_sha256: str
    post_event_count: int
    disposition: FamilyBAdmissionDispositionV2
    _owner_token: object = field(repr=False, compare=False)
    _rollback_capability: object = field(repr=False, compare=False)
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ADMISSION_RECEIPT_FACTORY_TOKEN:
            raise FamilyBContractError(
                "Family B admission receipts must be created by the registry"
            )
        for value, field_name in (
            (self.input_sha256, "input_sha256"),
            (self.position_sha256, "position_sha256"),
            (self.paper_registry_root_sha256, "paper_registry_root_sha256"),
            (
                self.paper_registry_checkpoint_sha256,
                "paper_registry_checkpoint_sha256",
            ),
            (self.pre_root_sha256, "pre_root_sha256"),
            (self.post_root_sha256, "post_root_sha256"),
        ):
            _validate_sha256(value, field_name)
        for value, field_name in (
            (self.paper_registry_event_count, "paper_registry_event_count"),
            (self.pre_event_count, "pre_event_count"),
            (self.post_event_count, "post_event_count"),
        ):
            _validate_nonnegative_int(value, field_name)
        if (
            type(self.paper_registry_maximum_events) is not int
            or self.paper_registry_maximum_events < 1
            or self.paper_registry_event_count > self.paper_registry_maximum_events
        ):
            raise FamilyBContractError("PAPER registry receipt count/capacity is invalid")
        if not isinstance(self.decision, FamilyBEntryDecisionV2):
            raise FamilyBContractError("admission receipt decision must be FamilyBEntryDecisionV2")
        if not isinstance(self.position, FamilyBPositionV2):
            raise FamilyBContractError("admission receipt position must be FamilyBPositionV2")
        if not isinstance(self.paper_decision, PaperFokEntryDecisionV2):
            raise FamilyBContractError("admission receipt requires a PAPER decision")
        if not isinstance(self.paper_certificate, PaperFokFullFillCertificateV2):
            raise FamilyBContractError("admission receipt requires a PAPER certificate")
        canonical_family_b_entry_decision_v2(self.decision)
        canonical_paper_fok_entry_decision_v2(self.paper_decision)
        canonical_paper_fok_full_fill_certificate_v2(self.paper_certificate)
        if self.position_sha256 != _position_sha256(self.position):
            raise FamilyBContractError("admission receipt position hash differs")
        if (
            self.position.entry_event_id != self.decision.event_id
            or self.position.paper_decision_event_id != self.paper_decision.event_id
            or self.position.paper_decision_payload_sha256 != self.paper_decision.payload_sha256
            or self.position.admission_evidence_sha256 != self.paper_certificate.certificate_sha256
            or self.paper_certificate.signal_event_id != self.decision.event_id
            or self.paper_certificate.decision_event_id != self.paper_decision.event_id
            or self.paper_certificate.decision_payload_sha256 != self.paper_decision.payload_sha256
            or not (
                self.paper_registry_root_sha256
                == self.position.paper_registry_root_sha256
                == self.paper_certificate.terminal_registry_replay_root_sha256
            )
            or not (
                self.paper_registry_event_count
                == self.position.paper_registry_event_count
                == self.paper_certificate.terminal_registry_event_count
            )
            or self.paper_registry_maximum_events
            != self.paper_certificate.terminal_registry_maximum_events
            or not (
                self.paper_registry_checkpoint_sha256
                == self.position.paper_registry_checkpoint_sha256
                == self.paper_certificate.terminal_registry_checkpoint_sha256
            )
        ):
            raise FamilyBContractError(
                "admission receipt PAPER, decision, position, or checkpoint evidence differs"
            )
        if not isinstance(self.disposition, FamilyBAdmissionDispositionV2):
            raise FamilyBContractError("admission disposition has the wrong type")
        if self.disposition is FamilyBAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION:
            if (
                self.post_event_count != self.pre_event_count
                or self.post_root_sha256 == self.pre_root_sha256
            ):
                raise FamilyBContractError("new admission receipt has invalid post-state")
            return
        if (
            self.post_event_count != self.pre_event_count
            or self.post_root_sha256 != self.pre_root_sha256
        ):
            raise FamilyBContractError("pre-existing admission receipt must preserve state")


@dataclass(frozen=True, slots=True)
class FamilyBExitMutationReceiptV2:
    """Ephemeral exact-owner proof for one exit-ledger mutation."""

    input_sha256: str
    entry_event_id: str
    position: FamilyBPositionV2
    position_sha256: str
    decision: FamilyBExitDecisionV2
    pre_root_sha256: str
    pre_event_count: int
    pre_terminal: bool
    pre_active_entry_event_id: str | None
    post_root_sha256: str
    post_event_count: int
    post_terminal: bool
    post_active_entry_event_id: str | None
    disposition: FamilyBExitDispositionV2
    _owner_token: object = field(repr=False, compare=False)
    _rollback_capability: object = field(repr=False, compare=False)
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _EXIT_MUTATION_RECEIPT_FACTORY_TOKEN:
            raise FamilyBContractError(
                "Family B exit mutation receipts must be created by the registry"
            )
        for value, field_name in (
            (self.input_sha256, "input_sha256"),
            (self.entry_event_id, "entry_event_id"),
            (self.position_sha256, "position_sha256"),
            (self.pre_root_sha256, "pre_root_sha256"),
            (self.post_root_sha256, "post_root_sha256"),
        ):
            _validate_sha256(value, field_name)
        _validate_nonnegative_int(self.pre_event_count, "pre_event_count")
        _validate_nonnegative_int(self.post_event_count, "post_event_count")
        if type(self.pre_terminal) is not bool or type(self.post_terminal) is not bool:
            raise FamilyBContractError("exit receipt terminal flags must be boolean")
        for value, field_name in (
            (self.pre_active_entry_event_id, "pre_active_entry_event_id"),
            (self.post_active_entry_event_id, "post_active_entry_event_id"),
        ):
            if value is not None:
                _validate_sha256(value, field_name)
        if not isinstance(self.position, FamilyBPositionV2):
            raise FamilyBContractError("exit receipt position must be FamilyBPositionV2")
        if not isinstance(self.decision, FamilyBExitDecisionV2):
            raise FamilyBContractError("exit receipt decision must be FamilyBExitDecisionV2")
        canonical_family_b_exit_decision_v2(self.decision)
        if (
            self.position_sha256 != _position_sha256(self.position)
            or self.entry_event_id != self.position.entry_event_id
            or self.decision.entry_event_id != self.entry_event_id
            or not _exit_decision_matches_position(self.decision, self.position)
        ):
            raise FamilyBContractError("exit receipt decision or position identity differs")
        if not isinstance(self.disposition, FamilyBExitDispositionV2):
            raise FamilyBContractError("exit disposition has the wrong type")
        if self.disposition is FamilyBExitDispositionV2.NEW_BY_THIS_TRANSACTION:
            if (
                self.pre_terminal
                or self.pre_active_entry_event_id != self.entry_event_id
                or self.post_event_count != self.pre_event_count + 1
                or self.post_root_sha256 == self.pre_root_sha256
                or self.post_terminal != self.decision.exits_position
                or self.post_active_entry_event_id
                != (None if self.decision.exits_position else self.entry_event_id)
            ):
                raise FamilyBContractError("new exit receipt has invalid state transition")
            return
        if (
            self.post_event_count != self.pre_event_count
            or self.post_root_sha256 != self.pre_root_sha256
            or self.post_terminal != self.pre_terminal
            or self.post_active_entry_event_id != self.pre_active_entry_event_id
        ):
            raise FamilyBContractError("pre-existing exit receipt must preserve state")


@dataclass(slots=True)
class _FamilyBEpisodeStateV2:
    position: FamilyBPositionV2
    position_sha256: str
    terminal: bool = False


class FamilyBDecisionRegistryV2:
    """Single bounded atomic owner for B decisions, episodes, and restart state."""

    def __init__(self, *, maximum_events: int) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise FamilyBContractError("maximum_events must be a positive integer")
        self._maximum_events = maximum_events
        self._entry_results: dict[
            str,
            tuple[str, FamilyBEntryDecisionV2],
        ] = {}
        self._exit_results: dict[
            str,
            tuple[str, FamilyBExitDecisionV2],
        ] = {}
        self._episodes: dict[str, _FamilyBEpisodeStateV2] = {}
        self._active_by_key: dict[tuple[str, VenueV2, str], str] = {}
        self._entry_commit_lock = RLock()
        self._entry_commit_owner_token = object()
        self._entry_rollback_capabilities: dict[str, object] = {}
        self._lifecycle_owner_token = object()
        self._admission_rollback_capabilities: dict[str, object] = {}
        self._exit_rollback_capabilities: dict[str, object] = {}
        self._prospective_authority_token: object | None = None

    @property
    def event_count(self) -> int:
        return len(self._entry_results) + len(self._exit_results)

    @property
    def maximum_events(self) -> int:
        return self._maximum_events

    @property
    def replay_root_sha256(self) -> str:
        return self._replay_root_sha256_with_entries(self._entry_results)

    def _claim_prospective_decision_authority_v2(self) -> object:
        """Exclusively gate mutations for one fresh prospective attempt."""

        with self._entry_commit_lock:
            if self._prospective_authority_token is not None:
                raise FamilyBContractError(
                    "Family B prospective decision authority is already held"
                )
            genesis = FamilyBDecisionRegistryV2(maximum_events=self._maximum_events)
            if (
                self.event_count != 0
                or self.replay_root_sha256 != genesis.replay_root_sha256
                or self._entry_results
                or self._exit_results
                or self._episodes
                or self._active_by_key
                or self._entry_rollback_capabilities
                or self._admission_rollback_capabilities
                or self._exit_rollback_capabilities
            ):
                raise FamilyBContractError(
                    "Family B prospective authority requires exact genesis state"
                )
            authority = object()
            self._prospective_authority_token = authority
            return authority

    def _release_unconsumed_prospective_decision_authority_v2(
        self,
        authority: object,
    ) -> None:
        """Release only a still-empty claim before the WAL claim is consumed."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(authority)
            genesis = FamilyBDecisionRegistryV2(maximum_events=self._maximum_events)
            if (
                self.event_count != 0
                or self.replay_root_sha256 != genesis.replay_root_sha256
                or self._entry_results
                or self._exit_results
                or self._episodes
                or self._active_by_key
                or self._entry_rollback_capabilities
                or self._admission_rollback_capabilities
                or self._exit_rollback_capabilities
            ):
                raise FamilyBContractError(
                    "cannot release a non-genesis Family B prospective authority"
                )
            self._prospective_authority_token = None

    def _assert_prospective_mutation_authority_v2(
        self,
        authority: object | None,
    ) -> None:
        held = self._prospective_authority_token
        if held is None:
            if authority is not None:
                raise FamilyBContractError("Family B prospective authority was not claimed")
            return
        if authority is not held:
            raise FamilyBContractError(
                "Family B mutation requires the held prospective decision authority"
            )

    def _replay_root_sha256_with_entries(
        self,
        entry_results: dict[str, tuple[str, FamilyBEntryDecisionV2]],
    ) -> str:
        events, episodes, active = self._state_rows(entry_results=entry_results)
        return _registry_replay_root(events, episodes, active)

    def is_active(
        self,
        *,
        promoting_plan_sha256: str,
        venue: VenueV2,
        symbol: str,
    ) -> bool:
        return (promoting_plan_sha256, venue, symbol) in self._active_by_key

    def evaluate_entry(
        self,
        item: FamilyBEntryInputV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyBEntryDecisionV2:
        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            preview = self.preview_entry(item)
            return self.commit_entry_preview(
                item,
                preview,
                _prospective_authority=_prospective_authority,
            )

    def preview_entry(self, item: FamilyBEntryInputV2) -> FamilyBEntryPreviewV2:
        """Evaluate against current owner state without mutating that state."""

        with self._entry_commit_lock:
            if not isinstance(item, FamilyBEntryInputV2):
                raise FamilyBContractError("item must be FamilyBEntryInputV2")
            canonical_family_b_feature_evidence_v2(item.feature_evidence)
            input_sha256 = _entry_input_sha256(item)
            logical_event_id = _entry_logical_event_id(item)
            prior = self._entry_results.get(logical_event_id)
            if prior is not None:
                if prior[0] != input_sha256:
                    raise FamilyBContractError(
                        "same Family B entry event received conflicting causal input"
                    )
                decision = prior[1]
                already_committed = True
            else:
                self._require_capacity()
                active_key = (item.promoting_plan_sha256, item.venue, item.symbol)
                decision = _evaluate_family_b_entry_unsequenced_v2(
                    item,
                    active_position=active_key in self._active_by_key,
                )
                if decision.event_id != logical_event_id:
                    raise FamilyBContractError("entry evaluator changed its logical event ID")
                already_committed = False
            return FamilyBEntryPreviewV2(
                input_sha256=input_sha256,
                pre_replay_root_sha256=self.replay_root_sha256,
                pre_event_count=self.event_count,
                decision=decision,
                already_committed=already_committed,
                _factory_token=_ENTRY_PREVIEW_FACTORY_TOKEN,
            )

    def commit_entry_preview(
        self,
        item: FamilyBEntryInputV2,
        preview: FamilyBEntryPreviewV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyBEntryDecisionV2:
        """Compatibility API returning the decision from a receipt-backed commit."""

        return self.commit_entry_preview_with_receipt(
            item,
            preview,
            _prospective_authority=_prospective_authority,
        ).decision

    def commit_entry_preview_with_receipt(
        self,
        item: FamilyBEntryInputV2,
        preview: FamilyBEntryPreviewV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyBEntryCommitReceiptV2:
        """Commit exactly one preview and identify which call created it."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            logical_event_id, input_sha256 = self._validate_entry_preview(item, preview)
            prior = self._entry_results.get(logical_event_id)
            if preview.already_committed:
                if (
                    self.event_count != preview.pre_event_count
                    or self.replay_root_sha256 != preview.pre_replay_root_sha256
                    or prior != (input_sha256, preview.decision)
                ):
                    raise FamilyBContractError("Family B entry preview state drifted before commit")
                return self._entry_commit_receipt(
                    preview,
                    FamilyBEntryCommitDispositionV2.PREEXISTING,
                    object(),
                )
            if prior is not None:
                if prior != (input_sha256, preview.decision):
                    raise FamilyBContractError(
                        "Family B entry preview conflicts with committed input"
                    )
                entries_without_target = dict(self._entry_results)
                del entries_without_target[logical_event_id]
                if (
                    self.event_count != preview.pre_event_count + 1
                    or self._replay_root_sha256_with_entries(entries_without_target)
                    != preview.pre_replay_root_sha256
                ):
                    raise FamilyBContractError("Family B entry preview state drifted before commit")
                return self._entry_commit_receipt(
                    preview,
                    FamilyBEntryCommitDispositionV2.PREEXISTING,
                    object(),
                )
            if (
                self.event_count != preview.pre_event_count
                or self.replay_root_sha256 != preview.pre_replay_root_sha256
            ):
                raise FamilyBContractError("Family B entry preview state drifted before commit")
            self._require_capacity()
            active_key = (item.promoting_plan_sha256, item.venue, item.symbol)
            expected = _evaluate_family_b_entry_unsequenced_v2(
                item,
                active_position=active_key in self._active_by_key,
            )
            if expected != preview.decision or expected.event_id != logical_event_id:
                raise FamilyBContractError("Family B entry preview decision drifted before commit")
            self._entry_results[logical_event_id] = (input_sha256, preview.decision)
            rollback_capability = object()
            self._entry_rollback_capabilities[logical_event_id] = rollback_capability
            return self._entry_commit_receipt(
                preview,
                FamilyBEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION,
                rollback_capability,
            )

    def rollback_entry_preview(
        self,
        item: FamilyBEntryInputV2,
        preview: FamilyBEntryPreviewV2,
        receipt: FamilyBEntryCommitReceiptV2,
        *,
        _prospective_authority: object | None = None,
    ) -> bool:
        """Consume an exact NEW receipt and restore its untouched pre-state."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            logical_event_id, input_sha256 = self._validate_entry_preview(item, preview)
            self._validate_entry_commit_receipt(preview, receipt)
            if receipt.disposition is not FamilyBEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION:
                raise FamilyBContractError("cannot roll back a pre-existing Family B entry")
            if (
                self._entry_rollback_capabilities.get(logical_event_id)
                is not receipt._rollback_capability
            ):
                raise FamilyBContractError("Family B entry receipt does not own the current commit")
            if (
                self.event_count != receipt.post_event_count
                or self.replay_root_sha256 != receipt.post_root_sha256
            ):
                raise FamilyBContractError("Family B entry preview state drifted before rollback")
            prior = self._entry_results.get(logical_event_id)
            if prior is None or prior != (input_sha256, preview.decision):
                raise FamilyBContractError("Family B entry preview conflicts with rollback target")
            entries_without_target = dict(self._entry_results)
            del entries_without_target[logical_event_id]
            if (
                self._replay_root_sha256_with_entries(entries_without_target)
                != receipt.pre_root_sha256
            ):
                raise FamilyBContractError("Family B entry preview state drifted before rollback")
            del self._entry_results[logical_event_id]
            if (
                self.event_count != receipt.pre_event_count
                or self.replay_root_sha256 != receipt.pre_root_sha256
            ):
                self._entry_results[logical_event_id] = prior
                raise FamilyBContractError(
                    "Family B entry rollback failed to restore its checkpoint"
                )
            del self._entry_rollback_capabilities[logical_event_id]
            return True

    def _entry_commit_receipt(
        self,
        preview: FamilyBEntryPreviewV2,
        disposition: FamilyBEntryCommitDispositionV2,
        rollback_capability: object,
    ) -> FamilyBEntryCommitReceiptV2:
        return FamilyBEntryCommitReceiptV2(
            input_sha256=preview.input_sha256,
            event_id=preview.decision.event_id,
            decision=preview.decision,
            preview_already_committed=preview.already_committed,
            pre_root_sha256=preview.pre_replay_root_sha256,
            pre_event_count=preview.pre_event_count,
            post_root_sha256=self.replay_root_sha256,
            post_event_count=self.event_count,
            disposition=disposition,
            _owner_token=self._entry_commit_owner_token,
            _rollback_capability=rollback_capability,
            _factory_token=_ENTRY_COMMIT_RECEIPT_FACTORY_TOKEN,
        )

    def _validate_entry_commit_receipt(
        self,
        preview: FamilyBEntryPreviewV2,
        receipt: FamilyBEntryCommitReceiptV2,
    ) -> None:
        if not isinstance(receipt, FamilyBEntryCommitReceiptV2):
            raise FamilyBContractError("receipt must be FamilyBEntryCommitReceiptV2")
        if receipt._owner_token is not self._entry_commit_owner_token:
            raise FamilyBContractError("Family B entry receipt belongs to another registry")
        if (
            receipt.input_sha256 != preview.input_sha256
            or receipt.event_id != preview.decision.event_id
            or receipt.decision != preview.decision
            or receipt.preview_already_committed != preview.already_committed
            or receipt.pre_root_sha256 != preview.pre_replay_root_sha256
            or receipt.pre_event_count != preview.pre_event_count
        ):
            raise FamilyBContractError("Family B entry receipt differs from exact preview")

    def _validate_entry_preview(
        self,
        item: FamilyBEntryInputV2,
        preview: FamilyBEntryPreviewV2,
    ) -> tuple[str, str]:
        if not isinstance(item, FamilyBEntryInputV2):
            raise FamilyBContractError("item must be FamilyBEntryInputV2")
        if not isinstance(preview, FamilyBEntryPreviewV2):
            raise FamilyBContractError("preview must be FamilyBEntryPreviewV2")
        canonical_family_b_feature_evidence_v2(item.feature_evidence)
        canonical_family_b_entry_decision_v2(preview.decision)
        logical_event_id = _entry_logical_event_id(item)
        input_sha256 = _entry_input_sha256(item)
        if preview.input_sha256 != input_sha256 or preview.decision.event_id != logical_event_id:
            raise FamilyBContractError("Family B entry preview differs from exact input")
        return logical_event_id, input_sha256

    def admit_position(
        self,
        item: FamilyBEntryInputV2,
        decision: FamilyBEntryDecisionV2,
        *,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        paper_registry: PaperFokDecisionRegistryV2,
        _prospective_authority: object | None = None,
    ) -> FamilyBPositionV2:
        """Compatibility API returning the receipt-backed admitted position."""

        return self.admit_position_with_receipt(
            item,
            decision,
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=paper_registry,
            _prospective_authority=_prospective_authority,
        ).position

    def admit_position_with_receipt(
        self,
        item: FamilyBEntryInputV2,
        decision: FamilyBEntryDecisionV2,
        *,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        paper_registry: PaperFokDecisionRegistryV2,
        _prospective_authority: object | None = None,
    ) -> FamilyBAdmissionReceiptV2:
        """Admit exactly one PAPER-backed position and prove mutation ownership."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            return self._admit_position_with_receipt_guarded(
                item,
                decision,
                paper_decision=paper_decision,
                certificate=certificate,
                paper_registry=paper_registry,
            )

    def _admit_position_with_receipt_guarded(
        self,
        item: FamilyBEntryInputV2,
        decision: FamilyBEntryDecisionV2,
        *,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        paper_registry: PaperFokDecisionRegistryV2,
    ) -> FamilyBAdmissionReceiptV2:
        if not isinstance(item, FamilyBEntryInputV2):
            raise FamilyBContractError("item must be FamilyBEntryInputV2")
        canonical_family_b_entry_decision_v2(decision)
        input_sha256 = _entry_input_sha256(item)
        prior = self._entry_results.get(decision.event_id)
        if prior is None or prior[0] != input_sha256 or prior[1] != decision:
            raise FamilyBContractError(
                "signal decision is not the ledgered result of this exact input"
            )
        position = _position_from_paper_admission(
            item,
            decision,
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=paper_registry,
        )
        position_sha256 = _position_sha256(position)
        checkpoint = paper_registry.terminal_checkpoint_v2()
        if (
            checkpoint.replay_root_sha256 != position.paper_registry_root_sha256
            or checkpoint.event_count != position.paper_registry_event_count
            or checkpoint.maximum_events != certificate.terminal_registry_maximum_events
            or checkpoint.checkpoint_sha256 != position.paper_registry_checkpoint_sha256
        ):
            raise FamilyBContractError(
                "PAPER registry changed while Family B admission evidence was captured"
            )
        active_key = (item.promoting_plan_sha256, item.venue, item.symbol)
        pre_root_sha256 = self.replay_root_sha256
        pre_event_count = self.event_count
        state = self._episodes.get(decision.event_id)
        if state is not None:
            if (
                state.position != position
                or state.position_sha256 != position_sha256
                or state.position_sha256 != _position_sha256(state.position)
            ):
                raise FamilyBContractError("conflicting Family B PAPER admission replay")
            active_event_id = self._active_by_key.get(active_key)
            if state.terminal:
                if active_event_id is not None:
                    raise FamilyBContractError(
                        "terminal admission replay conflicts with an active position"
                    )
            elif active_event_id != decision.event_id:
                raise FamilyBContractError("conflicting Family B PAPER admission replay")
            position = state.position
            position_sha256 = state.position_sha256
            return self._admission_receipt(
                input_sha256=input_sha256,
                decision=decision,
                position=position,
                position_sha256=position_sha256,
                paper_decision=paper_decision,
                certificate=certificate,
                checkpoint_root_sha256=checkpoint.replay_root_sha256,
                checkpoint_event_count=checkpoint.event_count,
                checkpoint_maximum_events=checkpoint.maximum_events,
                checkpoint_sha256=checkpoint.checkpoint_sha256,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                disposition=FamilyBAdmissionDispositionV2.PREEXISTING,
                rollback_capability=object(),
            )
        if active_key in self._active_by_key:
            raise FamilyBContractError(
                "another Family B position is already active for this plan and symbol"
            )
        if len(self._episodes) >= self._maximum_events:
            raise FamilyBContractError("bounded Family B episode capacity exhausted")
        self._episodes[decision.event_id] = _FamilyBEpisodeStateV2(
            position=position,
            position_sha256=position_sha256,
        )
        self._active_by_key[active_key] = decision.event_id
        rollback_capability = object()
        self._admission_rollback_capabilities[decision.event_id] = rollback_capability
        receipt: FamilyBAdmissionReceiptV2 | None = None
        try:
            receipt = self._admission_receipt(
                input_sha256=input_sha256,
                decision=decision,
                position=position,
                position_sha256=position_sha256,
                paper_decision=paper_decision,
                certificate=certificate,
                checkpoint_root_sha256=checkpoint.replay_root_sha256,
                checkpoint_event_count=checkpoint.event_count,
                checkpoint_maximum_events=checkpoint.maximum_events,
                checkpoint_sha256=checkpoint.checkpoint_sha256,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                disposition=FamilyBAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION,
                rollback_capability=rollback_capability,
            )
        finally:
            if receipt is None:
                self._admission_rollback_capabilities.pop(decision.event_id, None)
                self._active_by_key.pop(active_key, None)
                self._episodes.pop(decision.event_id, None)
        assert receipt is not None
        return receipt

    def _admission_receipt(
        self,
        *,
        input_sha256: str,
        decision: FamilyBEntryDecisionV2,
        position: FamilyBPositionV2,
        position_sha256: str,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        checkpoint_root_sha256: str,
        checkpoint_event_count: int,
        checkpoint_maximum_events: int,
        checkpoint_sha256: str,
        pre_root_sha256: str,
        pre_event_count: int,
        disposition: FamilyBAdmissionDispositionV2,
        rollback_capability: object,
    ) -> FamilyBAdmissionReceiptV2:
        return FamilyBAdmissionReceiptV2(
            input_sha256=input_sha256,
            decision=decision,
            position=position,
            position_sha256=position_sha256,
            paper_decision=paper_decision,
            paper_certificate=certificate,
            paper_registry_root_sha256=checkpoint_root_sha256,
            paper_registry_event_count=checkpoint_event_count,
            paper_registry_maximum_events=checkpoint_maximum_events,
            paper_registry_checkpoint_sha256=checkpoint_sha256,
            pre_root_sha256=pre_root_sha256,
            pre_event_count=pre_event_count,
            post_root_sha256=self.replay_root_sha256,
            post_event_count=self.event_count,
            disposition=disposition,
            _owner_token=self._lifecycle_owner_token,
            _rollback_capability=rollback_capability,
            _factory_token=_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )

    def rollback_position_admission(
        self,
        item: FamilyBEntryInputV2,
        decision: FamilyBEntryDecisionV2,
        receipt: FamilyBAdmissionReceiptV2,
        *,
        _prospective_authority: object | None = None,
    ) -> bool:
        """Consume an exact NEW admission receipt and restore its pre-state."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            self._validate_admission_receipt(item, decision, receipt)
            if receipt.disposition is not FamilyBAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION:
                raise FamilyBContractError("cannot roll back a pre-existing Family B admission")
            if (
                self._admission_rollback_capabilities.get(decision.event_id)
                is not receipt._rollback_capability
            ):
                raise FamilyBContractError(
                    "Family B admission receipt does not own the current mutation"
                )
            if (
                self.event_count != receipt.post_event_count
                or self.replay_root_sha256 != receipt.post_root_sha256
            ):
                raise FamilyBContractError("Family B admission state drifted before rollback")
            state = self._episodes.get(decision.event_id)
            active_key = (item.promoting_plan_sha256, item.venue, item.symbol)
            if (
                state is None
                or state.position != receipt.position
                or state.position_sha256 != receipt.position_sha256
                or state.terminal
                or self._active_by_key.get(active_key) != decision.event_id
                or any(
                    value.entry_event_id == decision.event_id
                    for _, value in self._exit_results.values()
                )
            ):
                raise FamilyBContractError("Family B admission target drifted before rollback")
            del self._active_by_key[active_key]
            del self._episodes[decision.event_id]
            if (
                self.event_count != receipt.pre_event_count
                or self.replay_root_sha256 != receipt.pre_root_sha256
            ):
                self._episodes[decision.event_id] = state
                self._active_by_key[active_key] = decision.event_id
                raise FamilyBContractError(
                    "Family B admission rollback failed to restore its checkpoint"
                )
            del self._admission_rollback_capabilities[decision.event_id]
            return True

    def _validate_admission_receipt(
        self,
        item: FamilyBEntryInputV2,
        decision: FamilyBEntryDecisionV2,
        receipt: FamilyBAdmissionReceiptV2,
    ) -> None:
        if not isinstance(item, FamilyBEntryInputV2):
            raise FamilyBContractError("item must be FamilyBEntryInputV2")
        if not isinstance(decision, FamilyBEntryDecisionV2):
            raise FamilyBContractError("decision must be FamilyBEntryDecisionV2")
        if not isinstance(receipt, FamilyBAdmissionReceiptV2):
            raise FamilyBContractError("receipt must be FamilyBAdmissionReceiptV2")
        if receipt._owner_token is not self._lifecycle_owner_token:
            raise FamilyBContractError("Family B admission receipt belongs to another registry")
        canonical_family_b_entry_decision_v2(decision)
        if (
            receipt.input_sha256 != _entry_input_sha256(item)
            or receipt.decision != decision
            or receipt.position.entry_event_id != decision.event_id
        ):
            raise FamilyBContractError("Family B admission receipt differs from exact input")
        FamilyBAdmissionReceiptV2(
            input_sha256=receipt.input_sha256,
            decision=receipt.decision,
            position=receipt.position,
            position_sha256=receipt.position_sha256,
            paper_decision=receipt.paper_decision,
            paper_certificate=receipt.paper_certificate,
            paper_registry_root_sha256=receipt.paper_registry_root_sha256,
            paper_registry_event_count=receipt.paper_registry_event_count,
            paper_registry_maximum_events=receipt.paper_registry_maximum_events,
            paper_registry_checkpoint_sha256=(receipt.paper_registry_checkpoint_sha256),
            pre_root_sha256=receipt.pre_root_sha256,
            pre_event_count=receipt.pre_event_count,
            post_root_sha256=receipt.post_root_sha256,
            post_event_count=receipt.post_event_count,
            disposition=receipt.disposition,
            _owner_token=receipt._owner_token,
            _rollback_capability=receipt._rollback_capability,
            _factory_token=_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )

    def evaluate_exit(
        self,
        item: FamilyBExitInputV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyBExitDecisionV2:
        """Compatibility API returning the receipt-backed exit decision."""

        return self.evaluate_exit_with_receipt(
            item,
            _prospective_authority=_prospective_authority,
        ).decision

    def evaluate_exit_with_receipt(
        self,
        item: FamilyBExitInputV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyBExitMutationReceiptV2:
        """Evaluate exactly one exit input and prove mutation ownership."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            return self._evaluate_exit_with_receipt_guarded(item)

    def _evaluate_exit_with_receipt_guarded(
        self,
        item: FamilyBExitInputV2,
    ) -> FamilyBExitMutationReceiptV2:
        if not isinstance(item, FamilyBExitInputV2):
            raise FamilyBContractError("item must be FamilyBExitInputV2")
        canonical_family_b_exit_feature_evidence_v2(item.exit_feature_evidence)
        input_sha256 = _exit_input_sha256(item)
        logical_event_id = _exit_logical_event_id(item)
        state = self._episodes.get(item.position.entry_event_id)
        if state is None or state.position != item.position:
            raise FamilyBContractError("exit position is absent from its episode registry")
        if state.position_sha256 != _position_sha256(item.position):
            raise FamilyBContractError("exit position differs from admitted payload")
        active_key = (
            item.position.promoting_plan_sha256,
            item.position.venue,
            item.position.symbol,
        )
        pre_root_sha256 = self.replay_root_sha256
        pre_event_count = self.event_count
        pre_terminal = state.terminal
        pre_active_entry_event_id = self._active_by_key.get(active_key)
        prior = self._exit_results.get(logical_event_id)
        if prior is not None:
            if prior[0] != input_sha256:
                raise FamilyBContractError(
                    "same Family B exit event received conflicting causal input"
                )
            canonical_family_b_exit_decision_v2(prior[1])
            if not _exit_decision_matches_position(prior[1], item.position):
                raise FamilyBContractError("stored Family B exit differs from its position")
            return self._exit_mutation_receipt(
                input_sha256=input_sha256,
                item=item,
                decision=prior[1],
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                pre_terminal=pre_terminal,
                pre_active_entry_event_id=pre_active_entry_event_id,
                disposition=FamilyBExitDispositionV2.PREEXISTING,
                rollback_capability=object(),
            )
        self._require_capacity()
        if state.terminal:
            raise FamilyBContractError("Family B episode is already terminal")
        if pre_active_entry_event_id != item.position.entry_event_id:
            raise FamilyBContractError("active episode index differs from exit position")
        decision = _evaluate_family_b_exit_unsequenced_v2(item)
        if decision.event_id != logical_event_id:
            raise FamilyBContractError("exit evaluator changed its logical event ID")
        self._exit_results[logical_event_id] = (input_sha256, decision)
        if decision.exits_position:
            state.terminal = True
            del self._active_by_key[active_key]
        rollback_capability = object()
        self._exit_rollback_capabilities[logical_event_id] = rollback_capability
        receipt: FamilyBExitMutationReceiptV2 | None = None
        try:
            receipt = self._exit_mutation_receipt(
                input_sha256=input_sha256,
                item=item,
                decision=decision,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                pre_terminal=pre_terminal,
                pre_active_entry_event_id=pre_active_entry_event_id,
                disposition=FamilyBExitDispositionV2.NEW_BY_THIS_TRANSACTION,
                rollback_capability=rollback_capability,
            )
        finally:
            if receipt is None:
                self._exit_rollback_capabilities.pop(logical_event_id, None)
                self._exit_results.pop(logical_event_id, None)
                state.terminal = pre_terminal
                if pre_active_entry_event_id is None:
                    self._active_by_key.pop(active_key, None)
                else:
                    self._active_by_key[active_key] = pre_active_entry_event_id
        assert receipt is not None
        return receipt

    def _exit_mutation_receipt(
        self,
        *,
        input_sha256: str,
        item: FamilyBExitInputV2,
        decision: FamilyBExitDecisionV2,
        pre_root_sha256: str,
        pre_event_count: int,
        pre_terminal: bool,
        pre_active_entry_event_id: str | None,
        disposition: FamilyBExitDispositionV2,
        rollback_capability: object,
    ) -> FamilyBExitMutationReceiptV2:
        active_key = (
            item.position.promoting_plan_sha256,
            item.position.venue,
            item.position.symbol,
        )
        state = self._episodes[item.position.entry_event_id]
        return FamilyBExitMutationReceiptV2(
            input_sha256=input_sha256,
            entry_event_id=item.position.entry_event_id,
            position=item.position,
            position_sha256=_position_sha256(item.position),
            decision=decision,
            pre_root_sha256=pre_root_sha256,
            pre_event_count=pre_event_count,
            pre_terminal=pre_terminal,
            pre_active_entry_event_id=pre_active_entry_event_id,
            post_root_sha256=self.replay_root_sha256,
            post_event_count=self.event_count,
            post_terminal=state.terminal,
            post_active_entry_event_id=self._active_by_key.get(active_key),
            disposition=disposition,
            _owner_token=self._lifecycle_owner_token,
            _rollback_capability=rollback_capability,
            _factory_token=_EXIT_MUTATION_RECEIPT_FACTORY_TOKEN,
        )

    def rollback_exit(
        self,
        item: FamilyBExitInputV2,
        receipt: FamilyBExitMutationReceiptV2,
        *,
        _prospective_authority: object | None = None,
    ) -> bool:
        """Consume an exact NEW exit receipt and restore its episode checkpoint."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            logical_event_id, input_sha256 = self._validate_exit_mutation_receipt(
                item,
                receipt,
            )
            if receipt.disposition is not FamilyBExitDispositionV2.NEW_BY_THIS_TRANSACTION:
                raise FamilyBContractError("cannot roll back a pre-existing Family B exit")
            if (
                self._exit_rollback_capabilities.get(logical_event_id)
                is not receipt._rollback_capability
            ):
                raise FamilyBContractError(
                    "Family B exit receipt does not own the current mutation"
                )
            if (
                self.event_count != receipt.post_event_count
                or self.replay_root_sha256 != receipt.post_root_sha256
            ):
                raise FamilyBContractError("Family B exit state drifted before rollback")
            state = self._episodes.get(receipt.entry_event_id)
            active_key = (
                receipt.position.promoting_plan_sha256,
                receipt.position.venue,
                receipt.position.symbol,
            )
            if (
                state is None
                or state.position != receipt.position
                or state.position_sha256 != receipt.position_sha256
                or state.terminal != receipt.post_terminal
                or self._active_by_key.get(active_key) != receipt.post_active_entry_event_id
                or self._exit_results.get(logical_event_id) != (input_sha256, receipt.decision)
            ):
                raise FamilyBContractError("Family B exit target drifted before rollback")
            del self._exit_results[logical_event_id]
            state.terminal = receipt.pre_terminal
            if receipt.pre_active_entry_event_id is None:
                self._active_by_key.pop(active_key, None)
            else:
                self._active_by_key[active_key] = receipt.pre_active_entry_event_id
            if (
                self.event_count != receipt.pre_event_count
                or self.replay_root_sha256 != receipt.pre_root_sha256
            ):
                self._exit_results[logical_event_id] = (input_sha256, receipt.decision)
                state.terminal = receipt.post_terminal
                if receipt.post_active_entry_event_id is None:
                    self._active_by_key.pop(active_key, None)
                else:
                    self._active_by_key[active_key] = receipt.post_active_entry_event_id
                raise FamilyBContractError(
                    "Family B exit rollback failed to restore its checkpoint"
                )
            del self._exit_rollback_capabilities[logical_event_id]
            return True

    def _validate_exit_mutation_receipt(
        self,
        item: FamilyBExitInputV2,
        receipt: FamilyBExitMutationReceiptV2,
    ) -> tuple[str, str]:
        if not isinstance(item, FamilyBExitInputV2):
            raise FamilyBContractError("item must be FamilyBExitInputV2")
        if not isinstance(receipt, FamilyBExitMutationReceiptV2):
            raise FamilyBContractError("receipt must be FamilyBExitMutationReceiptV2")
        if receipt._owner_token is not self._lifecycle_owner_token:
            raise FamilyBContractError("Family B exit receipt belongs to another registry")
        canonical_family_b_exit_feature_evidence_v2(item.exit_feature_evidence)
        logical_event_id = _exit_logical_event_id(item)
        input_sha256 = _exit_input_sha256(item)
        if (
            receipt.input_sha256 != input_sha256
            or receipt.entry_event_id != item.position.entry_event_id
            or receipt.position != item.position
            or receipt.position_sha256 != _position_sha256(item.position)
            or receipt.decision.event_id != logical_event_id
        ):
            raise FamilyBContractError("Family B exit receipt differs from exact input")
        FamilyBExitMutationReceiptV2(
            input_sha256=receipt.input_sha256,
            entry_event_id=receipt.entry_event_id,
            position=receipt.position,
            position_sha256=receipt.position_sha256,
            decision=receipt.decision,
            pre_root_sha256=receipt.pre_root_sha256,
            pre_event_count=receipt.pre_event_count,
            pre_terminal=receipt.pre_terminal,
            pre_active_entry_event_id=receipt.pre_active_entry_event_id,
            post_root_sha256=receipt.post_root_sha256,
            post_event_count=receipt.post_event_count,
            post_terminal=receipt.post_terminal,
            post_active_entry_event_id=receipt.post_active_entry_event_id,
            disposition=receipt.disposition,
            _owner_token=receipt._owner_token,
            _rollback_capability=receipt._rollback_capability,
            _factory_token=_EXIT_MUTATION_RECEIPT_FACTORY_TOKEN,
        )
        return logical_event_id, input_sha256

    def export_state_v2(self) -> bytes:
        events, episodes, active = self._state_rows()
        return canonical_json_line(
            {
                "active": active,
                "episodes": episodes,
                "events": events,
                "maximum_events": self._maximum_events,
                "replay_root_sha256": _registry_replay_root(
                    events,
                    episodes,
                    active,
                ),
                "schema_version": _REGISTRY_STATE_SCHEMA,
            }
        )

    @classmethod
    def from_state_v2(
        cls,
        payload: bytes,
        *,
        expected_replay_root_sha256: str,
        expected_event_count: int,
        expected_maximum_events: int,
    ) -> FamilyBDecisionRegistryV2:
        _validate_sha256(expected_replay_root_sha256, "expected_replay_root_sha256")
        _validate_nonnegative_int(expected_event_count, "expected_event_count")
        if type(expected_maximum_events) is not int or expected_maximum_events < 1:
            raise FamilyBContractError("expected_maximum_events must be positive")
        if type(payload) is not bytes or not payload:
            raise FamilyBContractError("registry state must be non-empty bytes")
        try:
            document = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise FamilyBContractError("registry state is not valid UTF-8 JSON") from exc
        if not isinstance(document, dict) or canonical_json_line(document) != payload:
            raise FamilyBContractError("registry state must be canonical JSONL")
        if (
            set(document)
            != {
                "active",
                "episodes",
                "events",
                "maximum_events",
                "replay_root_sha256",
                "schema_version",
            }
            or document.get("schema_version") != _REGISTRY_STATE_SCHEMA
        ):
            raise FamilyBContractError("registry state schema is unsupported")
        maximum_events = document.get("maximum_events")
        raw_events = document.get("events")
        raw_episodes = document.get("episodes")
        raw_active = document.get("active")
        if (
            type(maximum_events) is not int
            or maximum_events != expected_maximum_events
            or not isinstance(raw_events, list)
            or not isinstance(raw_episodes, list)
            or not isinstance(raw_active, list)
            or len(raw_events) != expected_event_count
            or len(raw_events) > maximum_events
            or len(raw_episodes) > maximum_events
        ):
            raise FamilyBContractError(
                "registry state count or capacity differs from external checkpoint"
            )
        registry = cls(maximum_events=maximum_events)
        prior_key: tuple[int, int, str, str] | None = None
        for raw_row in raw_events:
            row, input_sha256, decision = _parse_registry_state_row(raw_row)
            order_key = _registry_state_row_sort_key(row)
            if prior_key is not None and order_key <= prior_key:
                raise FamilyBContractError(
                    "registry state rows must use strict deterministic replay order"
                )
            prior_key = order_key
            if isinstance(decision, FamilyBEntryDecisionV2):
                if decision.event_id in registry._entry_results:
                    raise FamilyBContractError("registry state contains a duplicate event ID")
                registry._entry_results[decision.event_id] = (
                    input_sha256,
                    decision,
                )
            else:
                if decision.event_id in registry._exit_results:
                    raise FamilyBContractError("registry state contains a duplicate event ID")
                registry._exit_results[decision.event_id] = (
                    input_sha256,
                    decision,
                )
        for raw_episode in raw_episodes:
            entry_event_id, state = _parse_episode_state_row(raw_episode)
            if entry_event_id in registry._episodes:
                raise FamilyBContractError("registry state contains a duplicate episode")
            entry = registry._entry_results.get(entry_event_id)
            if (
                entry is None
                or not entry[1].emitted_signal
                or state.position.entry_event_id != entry_event_id
            ):
                raise FamilyBContractError("registry episode lacks its exact SIGNAL decision")
            registry._episodes[entry_event_id] = state
        registry._validate_restored_episode_semantics()
        expected_active = registry._active_rows()
        if raw_active != expected_active:
            raise FamilyBContractError("registry active index differs from episodes")
        registry._active_by_key = {
            (
                state.position.promoting_plan_sha256,
                state.position.venue,
                state.position.symbol,
            ): entry_event_id
            for entry_event_id, state in registry._episodes.items()
            if not state.terminal
        }
        observed_root = document.get("replay_root_sha256")
        _validate_sha256_value(observed_root, "replay_root_sha256")
        if (
            observed_root != registry.replay_root_sha256
            or observed_root != expected_replay_root_sha256
        ):
            raise FamilyBContractError("registry replay root differs from external checkpoint")
        return registry

    def _validate_restored_episode_semantics(self) -> None:
        exits_by_entry: dict[str, list[FamilyBExitDecisionV2]] = {}
        for _, decision in self._exit_results.values():
            exits_by_entry.setdefault(decision.entry_event_id, []).append(decision)
        active_keys: set[tuple[str, VenueV2, str]] = set()
        for entry_event_id, state in self._episodes.items():
            entry_record = self._entry_results.get(entry_event_id)
            if entry_record is None or not _position_matches_entry_decision(
                state.position,
                entry_record[1],
            ):
                raise FamilyBContractError(
                    "registry position differs from its exact SIGNAL decision"
                )
            if not state.terminal:
                active_key = (
                    state.position.promoting_plan_sha256,
                    state.position.venue,
                    state.position.symbol,
                )
                if active_key in active_keys:
                    raise FamilyBContractError(
                        "registry contains conflicting active Family B positions"
                    )
                active_keys.add(active_key)
        for entry_event_id, decisions in exits_by_entry.items():
            state = self._episodes.get(entry_event_id)
            if state is None:
                raise FamilyBContractError("registry exit lacks an admitted episode")
            if any(
                not _exit_decision_matches_position(decision, state.position)
                for decision in decisions
            ):
                raise FamilyBContractError("registry exit differs from its admitted position")
            ordered = sorted(decisions, key=lambda value: value.bar_open_ms)
            terminal_indexes = [
                index for index, decision in enumerate(ordered) if decision.exits_position
            ]
            if terminal_indexes not in ([], [len(ordered) - 1]):
                raise FamilyBContractError("registry has an exit after terminal state")
            if state.terminal != bool(terminal_indexes):
                raise FamilyBContractError(
                    "registry terminal episode state differs from exit decisions"
                )

    def _state_rows(
        self,
        *,
        entry_results: dict[str, tuple[str, FamilyBEntryDecisionV2]] | None = None,
    ) -> tuple[
        list[dict[str, object]],
        list[dict[str, object]],
        list[dict[str, object]],
    ]:
        events = [
            _registry_state_row(decision, input_sha256=input_sha256)
            for input_sha256, decision in (
                *(self._entry_results if entry_results is None else entry_results).values(),
                *self._exit_results.values(),
            )
        ]
        events.sort(key=_registry_state_row_sort_key)
        episodes = [
            _episode_state_row(entry_event_id, state)
            for entry_event_id, state in sorted(self._episodes.items())
        ]
        return events, episodes, self._active_rows()

    def _active_rows(self) -> list[dict[str, object]]:
        return [
            {
                "entry_event_id": entry_event_id,
                "promoting_plan_sha256": state.position.promoting_plan_sha256,
                "symbol": state.position.symbol,
                "venue": state.position.venue.value,
            }
            for entry_event_id, state in sorted(self._episodes.items())
            if not state.terminal
        ]

    def _require_capacity(self) -> None:
        if self.event_count >= self._maximum_events:
            raise FamilyBContractError("bounded Family B decision registry capacity exhausted")


def resolve_family_b_child_matches_v2(
    *,
    b1_matches: bool,
    b2_matches: bool,
) -> FamilyBChildResolutionV2:
    """Enforce that B1 and B2 can never signal simultaneously."""

    if type(b1_matches) is not bool or type(b2_matches) is not bool:
        raise FamilyBContractError("B1/B2 match flags must be boolean")
    if b1_matches and b2_matches:
        return FamilyBChildResolutionV2(
            status=FamilyBEntryStatusV2.DATA_INVALID_RULE_INVARIANT,
            child=None,
        )
    if b1_matches:
        return FamilyBChildResolutionV2(
            status=FamilyBEntryStatusV2.SIGNAL,
            child=FamilyBChildV2.B1,
        )
    if b2_matches:
        return FamilyBChildResolutionV2(
            status=FamilyBEntryStatusV2.SIGNAL,
            child=FamilyBChildV2.B2,
        )
    return FamilyBChildResolutionV2(
        status=FamilyBEntryStatusV2.NO_SIGNAL,
        child=None,
    )


def event_true_range_v2(
    *,
    high: Decimal,
    low: Decimal,
    previous_close: Decimal,
) -> Decimal:
    """Compute frozen TR_t from the closed event bar and previous close."""

    if not all(_is_positive_finite(value) for value in (high, low, previous_close)):
        raise FamilyBContractError("true-range prices must be positive finite Decimal")
    if low > high:
        raise FamilyBContractError("true-range low cannot exceed high")
    with localcontext(protocol_decimal_context_v2()):
        return max(high - low, abs(high - previous_close), abs(low - previous_close))


def evaluate_family_b_entry_v2(
    item: FamilyBEntryInputV2,
    registry: FamilyBDecisionRegistryV2,
) -> FamilyBEntryDecisionV2:
    """Atomically evaluate and ledger one causal closed-5m Family B decision."""

    if not isinstance(item, FamilyBEntryInputV2):
        raise FamilyBContractError("item must be FamilyBEntryInputV2")
    if not isinstance(registry, FamilyBDecisionRegistryV2):
        raise FamilyBContractError("registry must be FamilyBDecisionRegistryV2")
    return registry.evaluate_entry(item)


def _evaluate_family_b_entry_unsequenced_v2(
    item: FamilyBEntryInputV2,
    *,
    active_position: bool,
) -> FamilyBEntryDecisionV2:
    if type(active_position) is not bool:
        raise FamilyBContractError("active_position must be registry-derived boolean")

    flow_sign = _sign(item.flow_imbalance_current)
    if flow_sign == 0:
        return _entry_no_action(
            item,
            FamilyBEntryStatusV2.NO_SIGNAL,
            ("FLOW_IMBALANCE_ZERO",),
            "NO_POSITION_NO_INVALIDATION",
        )
    if item.feature_evidence.readiness is not FamilyBFeatureReadinessV2.READY:
        return _entry_no_action(
            item,
            FamilyBEntryStatusV2.FEATURE_NOT_READY,
            (item.feature_evidence.readiness.value,),
            "FEATURE_NOT_READY_DO_NOT_ACT",
        )
    assert item.rz_flow_imbalance_current is not None
    assert item.rz_bar_return_current is not None
    event_true_range = event_true_range_v2(
        high=item.high_current,
        low=item.low_current,
        previous_close=item.previous_close,
    )
    b1_matches, b2_matches = _child_matches(item, flow_sign)
    resolution = resolve_family_b_child_matches_v2(
        b1_matches=b1_matches,
        b2_matches=b2_matches,
    )
    if resolution.status is FamilyBEntryStatusV2.DATA_INVALID_RULE_INVARIANT:
        return _entry_no_action(
            item,
            FamilyBEntryStatusV2.DATA_INVALID_RULE_INVARIANT,
            ("SIMULTANEOUS_B1_AND_B2_FORBIDDEN",),
            "DATA_INVALID_DO_NOT_ACT",
        )
    if resolution.status is FamilyBEntryStatusV2.NO_SIGNAL:
        return _entry_decision(
            item,
            status=FamilyBEntryStatusV2.NO_SIGNAL,
            child=None,
            side=None,
            reasons=(
                *_failed_b1_conditions(item, flow_sign),
                *_failed_b2_conditions(item, flow_sign),
            ),
            invalidation="NO_POSITION_NO_INVALIDATION",
            flow_sign=flow_sign,
            event_true_range=None,
        )
    assert resolution.child is not None
    if active_position:
        return _entry_decision(
            item,
            status=FamilyBEntryStatusV2.NOT_ADMITTED_ACTIVE_POSITION,
            child=None,
            side=None,
            reasons=("FAMILY_SYMBOL_POSITION_ALREADY_OPEN",),
            invalidation="ACTIVE_POSITION_UNCHANGED",
            flow_sign=flow_sign,
            event_true_range=None,
        )
    side = _side_for_child(resolution.child, flow_sign)
    invalidation = (
        "close_j <= entry_VWAP - TR_t"
        if side is FamilyBSideV2.LONG
        else "close_j >= entry_VWAP + TR_t"
    )
    return _entry_decision(
        item,
        status=FamilyBEntryStatusV2.SIGNAL,
        child=resolution.child,
        side=side,
        reasons=(
            f"{resolution.child.value}_CONDITIONS_MET",
            (
                "DIRECTION_EQUALS_FLOW_SIGN"
                if resolution.child is FamilyBChildV2.B1
                else "DIRECTION_OPPOSES_FLOW_SIGN"
            ),
            f"ACTION_{side.value}",
        ),
        invalidation=invalidation,
        flow_sign=flow_sign,
        event_true_range=event_true_range,
    )


def position_from_family_b_signal_v2(
    item: FamilyBEntryInputV2,
    decision: FamilyBEntryDecisionV2,
    registry: FamilyBDecisionRegistryV2,
    *,
    paper_decision: PaperFokEntryDecisionV2,
    certificate: PaperFokFullFillCertificateV2,
    paper_registry: PaperFokDecisionRegistryV2,
) -> FamilyBPositionV2:
    """Atomically admit rule state from an exact full PAPER execution."""

    if not decision.emitted_signal or decision.side is None or decision.child is None:
        raise FamilyBContractError("only an admitted Family B signal can create rule state")
    if not isinstance(registry, FamilyBDecisionRegistryV2):
        raise FamilyBContractError("registry must be FamilyBDecisionRegistryV2")
    return registry.admit_position(
        item,
        decision,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )


def _position_from_paper_admission(
    item: FamilyBEntryInputV2,
    decision: FamilyBEntryDecisionV2,
    *,
    paper_decision: PaperFokEntryDecisionV2,
    certificate: PaperFokFullFillCertificateV2,
    paper_registry: PaperFokDecisionRegistryV2,
) -> FamilyBPositionV2:
    if not decision.emitted_signal or decision.side is None or decision.child is None:
        raise FamilyBContractError("position requires a SIGNAL decision")
    if not isinstance(paper_decision, PaperFokEntryDecisionV2):
        raise FamilyBContractError("paper_decision must be concrete PAPER evidence")
    if not isinstance(certificate, PaperFokFullFillCertificateV2):
        raise FamilyBContractError("certificate must be concrete PAPER evidence")
    if not isinstance(paper_registry, PaperFokDecisionRegistryV2):
        raise FamilyBContractError("paper_registry must be the concrete PAPER registry")
    if (
        paper_decision.status is not PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY
        or not paper_decision.executed_full_quantity
    ):
        raise FamilyBContractError("zero, partial, rejected, or pending PAPER entry is not a fill")
    expected_paper_side = (
        PaperFokSideV2.BUY if decision.side is FamilyBSideV2.LONG else PaperFokSideV2.SELL
    )
    expected_paper_identity = (
        item.attempt_id,
        decision.event_id,
        item.symbol,
        item.venue,
        item.promoting_plan_sha256,
        item.bar_open_ms,
        item.bar_close_ms,
        item.decision_cutoff_ms,
        item.decision_cutoff_ms + PRIMARY_PAPER_TARGET_DELAY_MS_V2,
        expected_paper_side,
    )
    paper_identity = (
        paper_decision.attempt_id,
        paper_decision.signal_event_id,
        paper_decision.symbol,
        paper_decision.venue,
        paper_decision.promoting_plan_sha256,
        paper_decision.bar_open_ms,
        paper_decision.bar_close_ms,
        paper_decision.decision_cutoff_ms,
        paper_decision.target_venue_ms,
        paper_decision.side,
    )
    expected_certificate_identity = (
        item.attempt_id,
        decision.event_id,
        item.symbol,
        item.venue,
        item.promoting_plan_sha256,
        item.decision_cutoff_ms,
        item.decision_cutoff_ms + PRIMARY_PAPER_TARGET_DELAY_MS_V2,
        expected_paper_side,
    )
    certificate_identity = (
        certificate.attempt_id,
        certificate.signal_event_id,
        certificate.symbol,
        certificate.venue,
        certificate.promoting_plan_sha256,
        certificate.decision_cutoff_ms,
        certificate.target_venue_ms,
        certificate.side,
    )
    if (
        paper_identity != expected_paper_identity
        or certificate_identity != expected_certificate_identity
    ):
        raise FamilyBContractError("PAPER evidence identity differs from Family B signal")
    if not paper_registry.contains_exact_v2(paper_decision):
        raise FamilyBContractError("PAPER decision is absent from its registry checkpoint")
    checkpoint = paper_registry.terminal_checkpoint_v2()
    expected_certificate = issue_paper_fok_full_fill_certificate_v2(
        paper_decision,
        registry=paper_registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    if certificate != expected_certificate:
        raise FamilyBContractError("PAPER certificate differs from its sealed decision")
    if (
        paper_decision.filled_quantity is None
        or paper_decision.executable_vwap is None
        or paper_decision.executable_notional is None
        or paper_decision.requested_quantity != paper_decision.filled_quantity
        or certificate.filled_quantity != paper_decision.requested_quantity
        or certificate.executable_vwap != paper_decision.executable_vwap
        or certificate.executable_notional != paper_decision.executable_notional
    ):
        raise FamilyBContractError("PAPER requested, filled, VWAP, or notional evidence differs")
    assert decision.event_true_range is not None
    return FamilyBPositionV2(
        entry_event_id=decision.event_id,
        attempt_id=item.attempt_id,
        symbol=item.symbol,
        venue=item.venue,
        promoting_plan_sha256=item.promoting_plan_sha256,
        feature_evidence_sha256=item.feature_evidence.feature_evidence_sha256,
        feature_source_root_sha256=item.feature_evidence.feature_source_root_sha256,
        admission_evidence_sha256=certificate.certificate_sha256,
        paper_decision_event_id=paper_decision.event_id,
        paper_decision_payload_sha256=paper_decision.payload_sha256,
        paper_registry_root_sha256=checkpoint.replay_root_sha256,
        paper_registry_event_count=checkpoint.event_count,
        paper_registry_checkpoint_sha256=checkpoint.checkpoint_sha256,
        paper_requested_quantity=paper_decision.requested_quantity,
        paper_filled_quantity=paper_decision.filled_quantity,
        paper_executable_notional=paper_decision.executable_notional,
        child=decision.child,
        side=decision.side,
        flow_sign=decision.flow_sign,
        signal_bar_open_ms=item.bar_open_ms,
        entry_vwap=paper_decision.executable_vwap,
        event_true_range=decision.event_true_range,
        _factory_token=_POSITION_FACTORY_TOKEN,
    )


def evaluate_family_b_exit_v2(
    item: FamilyBExitInputV2,
    registry: FamilyBDecisionRegistryV2,
) -> FamilyBExitDecisionV2:
    """Atomically evaluate an admitted position exit in frozen priority order."""

    if not isinstance(item, FamilyBExitInputV2):
        raise FamilyBContractError("item must be FamilyBExitInputV2")
    if not isinstance(registry, FamilyBDecisionRegistryV2):
        raise FamilyBContractError("registry must be FamilyBDecisionRegistryV2")
    return registry.evaluate_exit(item)


def _evaluate_family_b_exit_unsequenced_v2(
    item: FamilyBExitInputV2,
) -> FamilyBExitDecisionV2:

    exit_action = (
        FamilyBExitActionV2.EXIT_LONG
        if item.position.side is FamilyBSideV2.LONG
        else FamilyBExitActionV2.EXIT_SHORT
    )
    if item.mandatory_exit is FamilyBMandatoryExitV2.DATA:
        return _exit(
            item,
            exit_action,
            FamilyBExitReasonV2.MANDATORY_DATA_EMERGENCY,
            "EXACT_DATA_EMERGENCY_REQUIRES_EXIT",
        )
    if item.mandatory_exit is FamilyBMandatoryExitV2.TERMINAL:
        return _exit(
            item,
            exit_action,
            FamilyBExitReasonV2.MANDATORY_TERMINAL_EMERGENCY,
            "TERMINAL_BOUNDARY_REQUIRES_EXIT",
        )
    with localcontext(protocol_decimal_context_v2()):
        adverse_long = item.position.entry_vwap - item.position.event_true_range
        adverse_short = item.position.entry_vwap + item.position.event_true_range
    if (item.position.side is FamilyBSideV2.LONG and item.close_price <= adverse_long) or (
        item.position.side is FamilyBSideV2.SHORT and item.close_price >= adverse_short
    ):
        return _exit(
            item,
            exit_action,
            FamilyBExitReasonV2.ADVERSE_INVALIDATION,
            "ADVERSE_ONE_TRUE_RANGE_BOUNDARY_REACHED",
        )
    aligned_flow = (
        item.flow_imbalance_current
        if item.position.position_sign == 1
        else -item.flow_imbalance_current
    )
    if aligned_flow <= _EXIT_FLOW_REVERSAL:
        return _exit(
            item,
            exit_action,
            FamilyBExitReasonV2.FLOW_REVERSAL,
            "POSITION_SIGN_TIMES_FLOW_LE_NEG_0_30",
        )
    hard_horizon_ms = (
        item.position.signal_bar_open_ms + FAMILY_B_HARD_HORIZON_BARS_V2 * FIVE_MINUTE_MS_V2
    )
    if item.bar_open_ms == hard_horizon_ms:
        return _exit(
            item,
            exit_action,
            FamilyBExitReasonV2.HARD_HORIZON,
            "HARD_HORIZON_EXACT",
        )
    if item.bar_open_ms > hard_horizon_ms:
        return _exit(
            item,
            exit_action,
            FamilyBExitReasonV2.MANDATORY_TERMINAL_EMERGENCY,
            "HARD_HORIZON_OVERDUE_FAIL_CLOSED",
        )
    return _exit_decision(
        item,
        action=FamilyBExitActionV2.HOLD,
        reason=FamilyBExitReasonV2.HOLD,
        reasons=("NO_EXIT_CONDITION_MET",),
        invalidation=(
            "close_j <= entry_VWAP - TR_t"
            if item.position.side is FamilyBSideV2.LONG
            else "close_j >= entry_VWAP + TR_t"
        ),
    )


def _child_matches(item: FamilyBEntryInputV2, flow_sign: int) -> tuple[bool, bool]:
    return (
        not _failed_b1_conditions(item, flow_sign),
        not _failed_b2_conditions(item, flow_sign),
    )


def _failed_b1_conditions(
    item: FamilyBEntryInputV2,
    flow_sign: int,
) -> tuple[str, ...]:
    assert isinstance(item.rz_flow_imbalance_current, Decimal)
    assert isinstance(item.rz_bar_return_current, Decimal)
    assert isinstance(item.d_start, Decimal)
    assert isinstance(item.d_low, Decimal)
    assert isinstance(item.d_end, Decimal)
    assert isinstance(item.spread95_bps, Decimal)
    aligned_return = item.rz_bar_return_current if flow_sign == 1 else -item.rz_bar_return_current
    checks = (
        (
            abs(item.rz_flow_imbalance_current) >= _RZ_FLOW_MIN,
            "B1_ABS_RZ_I_LT_2_0",
        ),
        (aligned_return >= _B1_ALIGNED_RETURN_MIN, "B1_ALIGNED_RETURN_RZ_LT_1_0"),
        (_scaled_le(item.d_low, 2, item.d_start, 1), "B1_DEPLETION_RATIO_GT_0_50"),
        (_scaled_lt(item.d_end, 5, item.d_low, 6), "B1_RECOVERY_RATIO_GTE_1_20"),
        (item.spread95_bps <= _SPREAD95_MAX_BPS, "B1_SPREAD95_GT_20"),
    )
    return tuple(reason for passed, reason in checks if not passed)


def _failed_b2_conditions(
    item: FamilyBEntryInputV2,
    flow_sign: int,
) -> tuple[str, ...]:
    assert isinstance(item.rz_flow_imbalance_current, Decimal)
    assert isinstance(item.rz_bar_return_current, Decimal)
    assert isinstance(item.d_low, Decimal)
    assert isinstance(item.d_end, Decimal)
    assert isinstance(item.spread95_bps, Decimal)
    aligned_return = item.rz_bar_return_current if flow_sign == 1 else -item.rz_bar_return_current
    checks = (
        (
            abs(item.rz_flow_imbalance_current) >= _RZ_FLOW_MIN,
            "B2_ABS_RZ_I_LT_2_0",
        ),
        (aligned_return <= _B2_ALIGNED_RETURN_MAX, "B2_ALIGNED_RETURN_RZ_GT_0_25"),
        (
            abs(item.rz_bar_return_current) <= _B2_ABSOLUTE_RETURN_MAX,
            "B2_ABS_RETURN_RZ_GT_0_75",
        ),
        (
            _scaled_ge(item.d_end, 2, item.d_low, 3),
            "B2_REPLENISHMENT_RATIO_LT_1_50",
        ),
        (item.spread95_bps <= _SPREAD95_MAX_BPS, "B2_SPREAD95_GT_20"),
    )
    return tuple(reason for passed, reason in checks if not passed)


def _side_for_child(child: FamilyBChildV2, flow_sign: int) -> FamilyBSideV2:
    position_sign = flow_sign if child is FamilyBChildV2.B1 else -flow_sign
    return FamilyBSideV2.LONG if position_sign == 1 else FamilyBSideV2.SHORT


def _entry_no_action(
    item: FamilyBEntryInputV2,
    status: FamilyBEntryStatusV2,
    reasons: tuple[str, ...],
    invalidation: str,
) -> FamilyBEntryDecisionV2:
    return _entry_decision(
        item,
        status=status,
        child=None,
        side=None,
        reasons=reasons,
        invalidation=invalidation,
        flow_sign=0,
        event_true_range=None,
    )


def _entry_decision(
    item: FamilyBEntryInputV2,
    *,
    status: FamilyBEntryStatusV2,
    child: FamilyBChildV2 | None,
    side: FamilyBSideV2 | None,
    reasons: tuple[str, ...],
    invalidation: str,
    flow_sign: int,
    event_true_range: Decimal | None,
) -> FamilyBEntryDecisionV2:
    return FamilyBEntryDecisionV2(
        attempt_id=item.attempt_id,
        symbol=item.symbol,
        venue=item.venue,
        promoting_plan_sha256=item.promoting_plan_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        feature_evidence_sha256=item.feature_evidence.feature_evidence_sha256,
        feature_source_root_sha256=item.feature_evidence.feature_source_root_sha256,
        status=status,
        child=child,
        side=side,
        reasons=reasons,
        invalidation=invalidation,
        flow_sign=flow_sign,
        event_true_range=event_true_range,
    )


def _exit(
    item: FamilyBExitInputV2,
    action: FamilyBExitActionV2,
    reason: FamilyBExitReasonV2,
    detail: str,
) -> FamilyBExitDecisionV2:
    return _exit_decision(
        item,
        action=action,
        reason=reason,
        reasons=(detail,),
        invalidation="POSITION_EXIT_REQUIRED",
    )


def _exit_decision(
    item: FamilyBExitInputV2,
    *,
    action: FamilyBExitActionV2,
    reason: FamilyBExitReasonV2,
    reasons: tuple[str, ...],
    invalidation: str,
) -> FamilyBExitDecisionV2:
    return FamilyBExitDecisionV2(
        entry_event_id=item.position.entry_event_id,
        attempt_id=item.position.attempt_id,
        symbol=item.position.symbol,
        venue=item.position.venue,
        promoting_plan_sha256=item.position.promoting_plan_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        position_side=item.position.side,
        exit_evidence_sha256=(item.exit_feature_evidence.exit_evidence_sha256),
        exit_source_root_sha256=(item.exit_feature_evidence.exit_source_root_sha256),
        action=action,
        reason=reason,
        reasons=reasons,
        invalidation=invalidation,
        _factory_token=_EXIT_DECISION_FACTORY_TOKEN,
    )


def canonical_family_b_entry_decision_v2(
    decision: FamilyBEntryDecisionV2,
) -> bytes:
    if not isinstance(decision, FamilyBEntryDecisionV2):
        raise FamilyBContractError("decision must be FamilyBEntryDecisionV2")
    expected = hashlib.sha256(
        _ENTRY_PAYLOAD_DOMAIN
        + canonical_json_line(_entry_decision_document(decision, include_payload_hash=False))
    ).hexdigest()
    if decision.payload_sha256 != expected:
        raise FamilyBContractError("entry payload hash differs from canonical decision")
    return canonical_json_line(_entry_decision_document(decision, include_payload_hash=True))


def parse_canonical_family_b_entry_decision_v2(
    payload: bytes,
) -> FamilyBEntryDecisionV2:
    """Restore one entry decision only from its exact canonical JSONL bytes."""

    if not isinstance(payload, bytes):
        raise FamilyBContractError("entry decision payload must be bytes")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FamilyBContractError("entry decision payload is invalid JSON") from exc
    if not isinstance(document, dict) or canonical_json_line(document) != payload:
        raise FamilyBContractError("entry decision payload must be canonical JSONL")
    event_id = document.get("event_id")
    bar_open_ms = document.get("bar_open_ms")
    symbol = document.get("symbol")
    _validate_sha256_value(event_id, "event_id")
    if type(bar_open_ms) is not int or bar_open_ms < 0:
        raise FamilyBContractError("entry decision bar_open_ms is invalid")
    if not isinstance(symbol, str):
        raise FamilyBContractError("entry decision symbol is invalid")
    _validate_symbol(symbol)
    assert isinstance(event_id, str)
    try:
        decision = _decision_from_registry_payload(
            payload,
            event_id=event_id,
            order_key=(bar_open_ms, 0, symbol, event_id),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, FamilyBContractError):
            raise
        raise FamilyBContractError("entry decision payload fields are invalid") from exc
    if not isinstance(decision, FamilyBEntryDecisionV2):
        raise FamilyBContractError("entry decision payload has a non-entry role")
    return decision


def canonical_family_b_exit_decision_v2(
    decision: FamilyBExitDecisionV2,
) -> bytes:
    if not isinstance(decision, FamilyBExitDecisionV2):
        raise FamilyBContractError("decision must be FamilyBExitDecisionV2")
    expected = hashlib.sha256(
        _EXIT_PAYLOAD_DOMAIN
        + canonical_json_line(_exit_decision_document(decision, include_payload_hash=False))
    ).hexdigest()
    if decision.payload_sha256 != expected:
        raise FamilyBContractError("exit payload hash differs from canonical decision")
    return canonical_json_line(_exit_decision_document(decision, include_payload_hash=True))


def _registry_order_key(
    decision: FamilyBEntryDecisionV2 | FamilyBExitDecisionV2,
) -> tuple[int, int, str, str]:
    role_rank = 0 if isinstance(decision, FamilyBEntryDecisionV2) else 1
    return (decision.bar_open_ms, role_rank, decision.symbol, decision.event_id)


def _registry_state_row(
    decision: FamilyBEntryDecisionV2 | FamilyBExitDecisionV2,
    *,
    input_sha256: str,
) -> dict[str, object]:
    _validate_sha256(input_sha256, "input_sha256")
    payload = (
        canonical_family_b_entry_decision_v2(decision)
        if isinstance(decision, FamilyBEntryDecisionV2)
        else canonical_family_b_exit_decision_v2(decision)
    )
    return {
        "event_id": decision.event_id,
        "input_sha256": input_sha256,
        "order_key": list(_registry_order_key(decision)),
        "payload_base64": base64.b64encode(payload).decode("ascii"),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
    }


def _registry_state_row_sort_key(
    row: dict[str, object],
) -> tuple[int, int, str, str]:
    order_key = row["order_key"]
    if not isinstance(order_key, list) or len(order_key) != 4:
        raise FamilyBContractError("internal registry order key is malformed")
    bar_open_ms, role_rank, symbol, event_id = order_key
    if (
        type(bar_open_ms) is not int
        or type(role_rank) is not int
        or not isinstance(symbol, str)
        or not isinstance(event_id, str)
    ):
        raise FamilyBContractError("internal registry order key types are malformed")
    return bar_open_ms, role_rank, symbol, event_id


def _registry_replay_root(
    events: list[dict[str, object]],
    episodes: list[dict[str, object]],
    active: list[dict[str, object]],
) -> str:
    return hashlib.sha256(
        _REGISTRY_REPLAY_DOMAIN
        + canonical_json_line(
            {
                "active": active,
                "episodes": episodes,
                "events": events,
                "schema_version": "r4b_family_b_atomic_episode_replay_v3",
            }
        )
    ).hexdigest()


def _parse_registry_state_row(
    raw_row: object,
) -> tuple[
    dict[str, object],
    str,
    FamilyBEntryDecisionV2 | FamilyBExitDecisionV2,
]:
    if not isinstance(raw_row, dict) or set(raw_row) != {
        "event_id",
        "input_sha256",
        "order_key",
        "payload_base64",
        "payload_sha256",
    }:
        raise FamilyBContractError("registry state row schema is unsupported")
    event_id = raw_row.get("event_id")
    _validate_sha256_value(event_id, "event_id")
    assert isinstance(event_id, str)
    input_sha256 = raw_row.get("input_sha256")
    _validate_sha256_value(input_sha256, "input_sha256")
    assert isinstance(input_sha256, str)
    order_key_raw = raw_row.get("order_key")
    if not isinstance(order_key_raw, list) or len(order_key_raw) != 4:
        raise FamilyBContractError("registry state order key must have four fields")
    bar_open_ms, role_rank, symbol, key_event_id = order_key_raw
    if type(bar_open_ms) is not int or bar_open_ms < 0:
        raise FamilyBContractError("registry order bar_open_ms is invalid")
    if type(role_rank) is not int or role_rank not in (0, 1):
        raise FamilyBContractError("registry order role rank is invalid")
    if not isinstance(symbol, str):
        raise FamilyBContractError("registry order symbol is invalid")
    _validate_symbol(symbol)
    if key_event_id != event_id:
        raise FamilyBContractError("registry order key event ID differs from row")
    payload_base64 = raw_row.get("payload_base64")
    if not isinstance(payload_base64, str):
        raise FamilyBContractError("registry payload_base64 must be text")
    try:
        decision_payload = base64.b64decode(payload_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise FamilyBContractError("registry payload_base64 is invalid") from exc
    if base64.b64encode(decision_payload).decode("ascii") != payload_base64:
        raise FamilyBContractError("registry payload_base64 is not canonical")
    payload_sha256 = raw_row.get("payload_sha256")
    _validate_sha256_value(payload_sha256, "payload_sha256")
    if payload_sha256 != hashlib.sha256(decision_payload).hexdigest():
        raise FamilyBContractError("registry row payload hash mismatch")
    decision = _decision_from_registry_payload(
        decision_payload,
        event_id=event_id,
        order_key=(bar_open_ms, role_rank, symbol, event_id),
    )
    row = _registry_state_row(
        decision,
        input_sha256=input_sha256,
    )
    if row != raw_row:
        raise FamilyBContractError("registry state row is not canonical")
    return row, input_sha256, decision


def _decision_from_registry_payload(
    payload: bytes,
    *,
    event_id: str,
    order_key: tuple[int, int, str, str],
) -> FamilyBEntryDecisionV2 | FamilyBExitDecisionV2:
    try:
        document = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise FamilyBContractError("registry decision payload is invalid JSON") from exc
    if not isinstance(document, dict) or canonical_json_line(document) != payload:
        raise FamilyBContractError("registry decision payload is not canonical JSONL")
    bar_open_ms, role_rank, symbol, _ = order_key
    role = "ENTRY_DECISION" if role_rank == 0 else "EXIT_DECISION"
    if (
        document.get("event_id") != event_id
        or document.get("bar_open_ms") != bar_open_ms
        or document.get("symbol") != symbol
        or document.get("role") != role
        or document.get("family") != "B"
        or document.get("rule_version") != FAMILY_B_RULE_VERSION_V2
        or document.get("venue") != VenueV2.USDM_FUTURES.value
    ):
        raise FamilyBContractError("registry decision payload differs from its ordered identity")
    internal_payload_hash = document.get("payload_sha256")
    _validate_sha256_value(internal_payload_hash, "decision payload_sha256")
    without_hash = dict(document)
    del without_hash["payload_sha256"]
    payload_domain = _ENTRY_PAYLOAD_DOMAIN if role_rank == 0 else _EXIT_PAYLOAD_DOMAIN
    if (
        internal_payload_hash
        != hashlib.sha256(payload_domain + canonical_json_line(without_hash)).hexdigest()
    ):
        raise FamilyBContractError("registry decision internal payload hash mismatch")
    if role_rank == 0:
        identity = {
            "attempt_id": document.get("attempt_id"),
            "bar_open_ms": bar_open_ms,
            "family": "B",
            "promoting_plan_sha256": document.get("promoting_plan_sha256"),
            "role": role,
            "rule_version": FAMILY_B_RULE_VERSION_V2,
            "symbol": symbol,
            "venue": VenueV2.USDM_FUTURES.value,
        }
        event_domain = _DECISION_ID_DOMAIN
    else:
        identity = {
            "attempt_id": document.get("attempt_id"),
            "bar_open_ms": bar_open_ms,
            "entry_event_id": document.get("entry_event_id"),
            "family": "B",
            "promoting_plan_sha256": document.get("promoting_plan_sha256"),
            "role": role,
            "rule_version": FAMILY_B_RULE_VERSION_V2,
            "symbol": symbol,
            "venue": VenueV2.USDM_FUTURES.value,
        }
        event_domain = _EXIT_ID_DOMAIN
    if event_id != hashlib.sha256(event_domain + canonical_json_line(identity)).hexdigest():
        raise FamilyBContractError("registry decision event ID mismatch")
    converted = dict(document)
    for field_name in (
        "event_id",
        "family",
        "payload_sha256",
        "role",
        "rule_version",
    ):
        converted.pop(field_name)
    converted["venue"] = VenueV2(converted["venue"])
    converted["reasons"] = tuple(converted["reasons"])
    if role_rank == 0:
        converted["status"] = FamilyBEntryStatusV2(converted["status"])
        converted["child"] = (
            None if converted["child"] is None else FamilyBChildV2(converted["child"])
        )
        converted["side"] = None if converted["side"] is None else FamilyBSideV2(converted["side"])
        converted["event_true_range"] = (
            None
            if converted["event_true_range"] is None
            else Decimal(converted["event_true_range"])
        )
        decision: FamilyBEntryDecisionV2 | FamilyBExitDecisionV2 = FamilyBEntryDecisionV2(
            **converted
        )  # type: ignore[arg-type]
        if canonical_family_b_entry_decision_v2(decision) != payload:
            raise FamilyBContractError("restored entry decision differs from state bytes")
        return decision
    converted["position_side"] = FamilyBSideV2(converted["position_side"])
    converted["action"] = FamilyBExitActionV2(converted["action"])
    converted["reason"] = FamilyBExitReasonV2(converted["reason"])
    decision = FamilyBExitDecisionV2(  # type: ignore[arg-type]
        **converted,
        _factory_token=_EXIT_DECISION_FACTORY_TOKEN,
    )
    if canonical_family_b_exit_decision_v2(decision) != payload:
        raise FamilyBContractError("restored exit decision differs from state bytes")
    return decision


def _entry_logical_event_id(item: FamilyBEntryInputV2) -> str:
    return hashlib.sha256(
        _DECISION_ID_DOMAIN
        + canonical_json_line(
            {
                "attempt_id": item.attempt_id,
                "bar_open_ms": item.bar_open_ms,
                "family": "B",
                "promoting_plan_sha256": item.promoting_plan_sha256,
                "role": "ENTRY_DECISION",
                "rule_version": FAMILY_B_RULE_VERSION_V2,
                "symbol": item.symbol,
                "venue": item.venue.value,
            }
        )
    ).hexdigest()


def _exit_logical_event_id(item: FamilyBExitInputV2) -> str:
    return hashlib.sha256(
        _EXIT_ID_DOMAIN
        + canonical_json_line(
            {
                "attempt_id": item.position.attempt_id,
                "bar_open_ms": item.bar_open_ms,
                "entry_event_id": item.position.entry_event_id,
                "family": "B",
                "promoting_plan_sha256": item.position.promoting_plan_sha256,
                "role": "EXIT_DECISION",
                "rule_version": FAMILY_B_RULE_VERSION_V2,
                "symbol": item.position.symbol,
                "venue": item.position.venue.value,
            }
        )
    ).hexdigest()


def _entry_input_sha256(item: FamilyBEntryInputV2) -> str:
    canonical_family_b_feature_evidence_v2(item.feature_evidence)
    return hashlib.sha256(
        _ENTRY_INPUT_DOMAIN
        + canonical_json_line(
            {
                "attempt_id": item.attempt_id,
                "bar_close_ms": item.bar_close_ms,
                "bar_open_ms": item.bar_open_ms,
                "decision_cutoff_ms": item.decision_cutoff_ms,
                "feature_evidence_sha256": (item.feature_evidence.feature_evidence_sha256),
                "feature_source_root_sha256": (item.feature_evidence.feature_source_root_sha256),
                "promoting_plan_sha256": item.promoting_plan_sha256,
                "symbol": item.symbol,
                "venue": item.venue.value,
            }
        )
    ).hexdigest()


def _exit_input_sha256(item: FamilyBExitInputV2) -> str:
    canonical_family_b_exit_feature_evidence_v2(item.exit_feature_evidence)
    return hashlib.sha256(
        _EXIT_INPUT_DOMAIN
        + canonical_json_line(
            {
                "bar_close_ms": item.bar_close_ms,
                "bar_open_ms": item.bar_open_ms,
                "decision_cutoff_ms": item.decision_cutoff_ms,
                "exit_evidence_sha256": (item.exit_feature_evidence.exit_evidence_sha256),
                "exit_source_root_sha256": (item.exit_feature_evidence.exit_source_root_sha256),
                "mandatory_exit": (
                    None if item.mandatory_exit is None else item.mandatory_exit.value
                ),
                "position_sha256": _position_sha256(item.position),
            }
        )
    ).hexdigest()


def _position_document(position: FamilyBPositionV2) -> dict[str, object]:
    return {
        "admission_evidence_sha256": position.admission_evidence_sha256,
        "attempt_id": position.attempt_id,
        "child": position.child.value,
        "entry_event_id": position.entry_event_id,
        "entry_vwap": str(position.entry_vwap),
        "event_true_range": str(position.event_true_range),
        "feature_evidence_sha256": position.feature_evidence_sha256,
        "feature_source_root_sha256": position.feature_source_root_sha256,
        "flow_sign": position.flow_sign,
        "paper_decision_event_id": position.paper_decision_event_id,
        "paper_decision_payload_sha256": position.paper_decision_payload_sha256,
        "paper_executable_notional": str(position.paper_executable_notional),
        "paper_filled_quantity": str(position.paper_filled_quantity),
        "paper_registry_checkpoint_sha256": (position.paper_registry_checkpoint_sha256),
        "paper_registry_event_count": position.paper_registry_event_count,
        "paper_registry_root_sha256": position.paper_registry_root_sha256,
        "paper_requested_quantity": str(position.paper_requested_quantity),
        "promoting_plan_sha256": position.promoting_plan_sha256,
        "schema_version": "r4b_family_b_paper_admitted_position_v2",
        "side": position.side.value,
        "signal_bar_open_ms": position.signal_bar_open_ms,
        "symbol": position.symbol,
        "venue": position.venue.value,
    }


def _position_sha256(position: FamilyBPositionV2) -> str:
    return hashlib.sha256(
        _POSITION_PAYLOAD_DOMAIN + canonical_json_line(_position_document(position))
    ).hexdigest()


def _position_matches_entry_decision(
    position: FamilyBPositionV2,
    decision: FamilyBEntryDecisionV2,
) -> bool:
    return (
        decision.emitted_signal
        and decision.child is not None
        and decision.side is not None
        and decision.event_true_range is not None
        and (
            position.entry_event_id,
            position.attempt_id,
            position.symbol,
            position.venue,
            position.promoting_plan_sha256,
            position.feature_evidence_sha256,
            position.feature_source_root_sha256,
            position.child,
            position.side,
            position.flow_sign,
            position.signal_bar_open_ms,
            position.event_true_range,
        )
        == (
            decision.event_id,
            decision.attempt_id,
            decision.symbol,
            decision.venue,
            decision.promoting_plan_sha256,
            decision.feature_evidence_sha256,
            decision.feature_source_root_sha256,
            decision.child,
            decision.side,
            decision.flow_sign,
            decision.bar_open_ms,
            decision.event_true_range,
        )
    )


def _exit_decision_matches_position(
    decision: FamilyBExitDecisionV2,
    position: FamilyBPositionV2,
) -> bool:
    return (
        decision.entry_event_id,
        decision.attempt_id,
        decision.symbol,
        decision.venue,
        decision.promoting_plan_sha256,
        decision.position_side,
    ) == (
        position.entry_event_id,
        position.attempt_id,
        position.symbol,
        position.venue,
        position.promoting_plan_sha256,
        position.side,
    )


def _position_from_document(document: object) -> FamilyBPositionV2:
    if not isinstance(document, dict):
        raise FamilyBContractError("registry position state must be an object")
    expected_fields = {
        "admission_evidence_sha256",
        "attempt_id",
        "child",
        "entry_event_id",
        "entry_vwap",
        "event_true_range",
        "feature_evidence_sha256",
        "feature_source_root_sha256",
        "flow_sign",
        "paper_decision_event_id",
        "paper_decision_payload_sha256",
        "paper_executable_notional",
        "paper_filled_quantity",
        "paper_registry_checkpoint_sha256",
        "paper_registry_event_count",
        "paper_registry_root_sha256",
        "paper_requested_quantity",
        "promoting_plan_sha256",
        "schema_version",
        "side",
        "signal_bar_open_ms",
        "symbol",
        "venue",
    }
    if (
        set(document) != expected_fields
        or document.get("schema_version") != "r4b_family_b_paper_admitted_position_v2"
    ):
        raise FamilyBContractError("registry position state schema is unsupported")
    converted = dict(document)
    converted.pop("schema_version")
    try:
        converted["venue"] = VenueV2(converted["venue"])
        converted["child"] = FamilyBChildV2(converted["child"])
        converted["side"] = FamilyBSideV2(converted["side"])
        for field_name in (
            "entry_vwap",
            "event_true_range",
            "paper_executable_notional",
            "paper_filled_quantity",
            "paper_requested_quantity",
        ):
            converted[field_name] = Decimal(converted[field_name])
        return FamilyBPositionV2(
            **converted,  # type: ignore[arg-type]
            _factory_token=_POSITION_FACTORY_TOKEN,
        )
    except (TypeError, ValueError) as exc:
        raise FamilyBContractError("registry position state is invalid") from exc


def _episode_state_row(
    entry_event_id: str,
    state: _FamilyBEpisodeStateV2,
) -> dict[str, object]:
    return {
        "entry_event_id": entry_event_id,
        "position": _position_document(state.position),
        "position_sha256": state.position_sha256,
        "terminal": state.terminal,
    }


def _parse_episode_state_row(
    raw: object,
) -> tuple[str, _FamilyBEpisodeStateV2]:
    if not isinstance(raw, dict) or set(raw) != {
        "entry_event_id",
        "position",
        "position_sha256",
        "terminal",
    }:
        raise FamilyBContractError("registry episode state schema is unsupported")
    entry_event_id = raw.get("entry_event_id")
    position_sha256 = raw.get("position_sha256")
    terminal = raw.get("terminal")
    _validate_sha256_value(entry_event_id, "entry_event_id")
    _validate_sha256_value(position_sha256, "position_sha256")
    if type(terminal) is not bool:
        raise FamilyBContractError("registry episode terminal must be boolean")
    assert isinstance(entry_event_id, str)
    assert isinstance(position_sha256, str)
    position = _position_from_document(raw.get("position"))
    if position.entry_event_id != entry_event_id or _position_sha256(position) != position_sha256:
        raise FamilyBContractError("registry episode position hash differs")
    state = _FamilyBEpisodeStateV2(
        position=position,
        position_sha256=position_sha256,
        terminal=terminal,
    )
    if _episode_state_row(entry_event_id, state) != raw:
        raise FamilyBContractError("registry episode state is not canonical")
    return entry_event_id, state


def _entry_identity_document(decision: FamilyBEntryDecisionV2) -> dict[str, object]:
    return {
        "attempt_id": decision.attempt_id,
        "bar_open_ms": decision.bar_open_ms,
        "family": "B",
        "promoting_plan_sha256": decision.promoting_plan_sha256,
        "role": "ENTRY_DECISION",
        "rule_version": decision.rule_version,
        "symbol": decision.symbol,
        "venue": decision.venue.value,
    }


def _exit_identity_document(decision: FamilyBExitDecisionV2) -> dict[str, object]:
    return {
        "attempt_id": decision.attempt_id,
        "bar_open_ms": decision.bar_open_ms,
        "entry_event_id": decision.entry_event_id,
        "family": "B",
        "promoting_plan_sha256": decision.promoting_plan_sha256,
        "role": "EXIT_DECISION",
        "rule_version": decision.rule_version,
        "symbol": decision.symbol,
        "venue": decision.venue.value,
    }


def _entry_decision_document(
    decision: FamilyBEntryDecisionV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_entry_identity_document(decision),
        "bar_close_ms": decision.bar_close_ms,
        "child": None if decision.child is None else decision.child.value,
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "event_id": decision.event_id,
        "event_true_range": (
            None if decision.event_true_range is None else str(decision.event_true_range)
        ),
        "feature_evidence_sha256": decision.feature_evidence_sha256,
        "feature_source_root_sha256": decision.feature_source_root_sha256,
        "flow_sign": decision.flow_sign,
        "invalidation": decision.invalidation,
        "reasons": list(decision.reasons),
        "side": None if decision.side is None else decision.side.value,
        "status": decision.status.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = decision.payload_sha256
    return document


def _exit_decision_document(
    decision: FamilyBExitDecisionV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_exit_identity_document(decision),
        "action": decision.action.value,
        "bar_close_ms": decision.bar_close_ms,
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "event_id": decision.event_id,
        "exit_evidence_sha256": decision.exit_evidence_sha256,
        "exit_source_root_sha256": decision.exit_source_root_sha256,
        "invalidation": decision.invalidation,
        "position_side": decision.position_side.value,
        "reason": decision.reason.value,
        "reasons": list(decision.reasons),
    }
    if include_payload_hash:
        document["payload_sha256"] = decision.payload_sha256
    return document


def _validate_entry_decision_state(decision: FamilyBEntryDecisionV2) -> None:
    if not isinstance(decision.status, FamilyBEntryStatusV2):
        raise FamilyBContractError("status must be FamilyBEntryStatusV2")
    _validate_reasons(decision.reasons)
    _validate_identity(decision.invalidation, "invalidation")
    if decision.flow_sign not in (-1, 0, 1):
        raise FamilyBContractError("flow_sign must be -1, 0, or 1")
    if decision.status is FamilyBEntryStatusV2.SIGNAL:
        if (
            not isinstance(decision.child, FamilyBChildV2)
            or not isinstance(decision.side, FamilyBSideV2)
            or decision.flow_sign not in (-1, 1)
            or not _is_nonnegative_finite(decision.event_true_range)
        ):
            raise FamilyBContractError("SIGNAL decision requires child, side, sign, and finite TR")
        expected_sign = (
            decision.flow_sign if decision.child is FamilyBChildV2.B1 else -decision.flow_sign
        )
        observed_sign = 1 if decision.side is FamilyBSideV2.LONG else -1
        if observed_sign != expected_sign:
            raise FamilyBContractError("SIGNAL side contradicts the B1/B2 flow direction rule")
        return
    if (
        decision.child is not None
        or decision.side is not None
        or decision.event_true_range is not None
    ):
        raise FamilyBContractError("non-signal decision cannot expose child, side, or event TR")


def _validate_exit_decision_state(decision: FamilyBExitDecisionV2) -> None:
    if not isinstance(decision.action, FamilyBExitActionV2) or not isinstance(
        decision.reason,
        FamilyBExitReasonV2,
    ):
        raise FamilyBContractError("exit action and reason must use Family B enums")
    _validate_reasons(decision.reasons)
    _validate_identity(decision.invalidation, "invalidation")
    if (decision.action is FamilyBExitActionV2.HOLD) != (
        decision.reason is FamilyBExitReasonV2.HOLD
    ):
        raise FamilyBContractError("HOLD action and reason must agree")
    expected_exit = (
        FamilyBExitActionV2.EXIT_LONG
        if decision.position_side is FamilyBSideV2.LONG
        else FamilyBExitActionV2.EXIT_SHORT
    )
    if decision.action not in (FamilyBExitActionV2.HOLD, expected_exit):
        raise FamilyBContractError("exit action contradicts the bound position side")
    expected_invalidation = (
        "POSITION_EXIT_REQUIRED"
        if decision.action is expected_exit
        else "close_j <= entry_VWAP - TR_t"
        if decision.position_side is FamilyBSideV2.LONG
        else "close_j >= entry_VWAP + TR_t"
    )
    if decision.invalidation != expected_invalidation:
        raise FamilyBContractError("exit invalidation contradicts action and position side")


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 32:
        raise FamilyBContractError("reasons must be a non-empty bounded tuple")
    for value in values:
        _validate_identity(value, "reason")


def _scaled_le(left: Decimal, left_multiplier: int, right: Decimal, right_multiplier: int) -> bool:
    return Fraction(left) * left_multiplier <= Fraction(right) * right_multiplier


def _scaled_lt(left: Decimal, left_multiplier: int, right: Decimal, right_multiplier: int) -> bool:
    return Fraction(left) * left_multiplier < Fraction(right) * right_multiplier


def _scaled_ge(left: Decimal, left_multiplier: int, right: Decimal, right_multiplier: int) -> bool:
    return Fraction(left) * left_multiplier >= Fraction(right) * right_multiplier


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _is_finite_decimal(value: Decimal | None) -> bool:
    return type(value) is Decimal and value.is_finite()


def _is_positive_finite(value: Decimal | None) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0


def _is_nonnegative_finite(value: Decimal | None) -> bool:
    return type(value) is Decimal and value.is_finite() and value >= 0


def _validate_identity(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value or value.strip() != value or len(value) > 256:
        raise FamilyBContractError(f"{field_name} must be a bounded normalized identity")


def _validate_symbol(symbol: str) -> None:
    if not isinstance(symbol, str) or _SYMBOL_RE.fullmatch(symbol) is None:
        raise FamilyBContractError("symbol must be a normalized USDT symbol")


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise FamilyBContractError(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_sha256_value(value: object, field_name: str) -> None:
    if not isinstance(value, str):
        raise FamilyBContractError(f"{field_name} must be a lowercase SHA-256 digest")
    _validate_sha256(value, field_name)


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise FamilyBContractError(f"{field_name} must be a nonnegative integer")


def _validate_bar_times(bar_open_ms: int, bar_close_ms: int, decision_cutoff_ms: int) -> None:
    try:
        _decision_clock.validate_decision_bar_v2(
            bar_open_ms,
            bar_close_ms,
            decision_cutoff_ms,
        )
    except _decision_clock.DecisionClockContractErrorV2 as exc:
        raise FamilyBContractError(str(exc)) from exc
