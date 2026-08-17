from __future__ import annotations

import hashlib
import json
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from threading import RLock

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    PaperFokDecisionRegistryV2,
    PaperFokEntryDecisionV2,
    PaperFokEntryStatusV2,
    PaperFokFullFillCertificateV2,
    PaperFokSideV2,
    canonical_paper_fok_entry_decision_v2,
    canonical_paper_fok_full_fill_certificate_v2,
    issue_paper_fok_full_fill_certificate_v2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2 as _DECISION_DELAY_MS_V2,
)
from signalbot.r4b_v2.protocol.decision_clock import (
    FIVE_MINUTE_MS_V2,
    validate_decision_bar_v2,
)
from signalbot.r4b_v2.strategy.family_a_features import (
    FamilyAEntryFeatureEvidenceV2,
    FamilyAExitFeatureEvidenceV2,
    FamilyAFeatureReadinessV2,
)

FAMILY_A_RULE_VERSION_V2 = "R4B_CAUSAL_V2.2.0_FAMILY_A"
DECISION_DELAY_MS_V2 = _DECISION_DELAY_MS_V2
FAMILY_A_HARD_HORIZON_BARS_V2 = 12
FAMILY_A_PAPER_TARGET_DELAY_MS_V2 = 10_000

_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_PRE_R12 = Decimal("1.5")
_PRE_DOI12 = Decimal("1.5")
_PRE_BASIS = Decimal("1.5")
_PRE_FUNDING = Decimal("1.0")
_TRIGGER_R1 = Decimal("-0.5")
_TRIGGER_DOI1 = Decimal("-1.0")
_TRIGGER_FLOW = Decimal("-0.35")
_EXIT_FLOW = Decimal("0.20")
_DECISION_ID_DOMAIN = b"R4B_FAMILY_A_DECISION_V2\0"
_EXIT_ID_DOMAIN = b"R4B_FAMILY_A_EXIT_V2\0"
_ENTRY_PAYLOAD_DOMAIN = b"R4B_FAMILY_A_ENTRY_PAYLOAD_V2\0"
_EXIT_PAYLOAD_DOMAIN = b"R4B_FAMILY_A_EXIT_PAYLOAD_V2\0"
_INPUT_DOMAIN = b"R4B_FAMILY_A_INPUT_V2\0"
_REGISTRY_ROOT_DOMAIN = b"R4B_FAMILY_A_REGISTRY_ROOT_V2\0"
_LEDGER_ROOT_DOMAIN = b"R4B_FAMILY_A_EPISODE_LEDGER_V2\0"
_POSITION_FACTORY_TOKEN = object()
_DECISION_FACTORY_TOKEN = object()
_ENTRY_PREVIEW_FACTORY_TOKEN = object()
_ENTRY_COMMIT_RECEIPT_FACTORY_TOKEN = object()
_ADMISSION_RECEIPT_FACTORY_TOKEN = object()
_EXIT_MUTATION_RECEIPT_FACTORY_TOKEN = object()


class FamilyAContractError(ValueError):
    """Raised when a caller violates an immutable Family A domain contract."""


class FamilyASideV2(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class FamilyAEntryStatusV2(StrEnum):
    SIGNAL = "SIGNAL"
    NO_SIGNAL = "NO_SIGNAL"
    NOT_ADMITTED_ACTIVE_POSITION = "NOT_ADMITTED_ACTIVE_POSITION"
    FEATURE_NOT_READY = "FEATURE_NOT_READY"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    DATA_INVALID = "DATA_INVALID"


class FamilyAExitActionV2(StrEnum):
    HOLD = "HOLD"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"


class FamilyAExitReasonV2(StrEnum):
    HOLD = "HOLD"
    MANDATORY_DATA_EMERGENCY = "MANDATORY_DATA_EMERGENCY"
    ADVERSE_INVALIDATION = "ADVERSE_INVALIDATION"
    BASIS_NORMALIZATION = "BASIS_NORMALIZATION"
    TWO_BAR_FLOW_REVERSAL = "TWO_BAR_FLOW_REVERSAL"
    HARD_HORIZON = "HARD_HORIZON"


class FamilyAIntervalStatusV2(StrEnum):
    COMPLETE = "COMPLETE"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"


class FamilyARegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


class FamilyAEntryCommitDispositionV2(StrEnum):
    NEW_BY_THIS_TRANSACTION = "NEW_BY_THIS_TRANSACTION"
    PREEXISTING = "PREEXISTING"


class FamilyAAdmissionDispositionV2(StrEnum):
    NEW_BY_THIS_TRANSACTION = "NEW_BY_THIS_TRANSACTION"
    PREEXISTING = "PREEXISTING"


class FamilyAExitDispositionV2(StrEnum):
    NEW_BY_THIS_TRANSACTION = "NEW_BY_THIS_TRANSACTION"
    PREEXISTING = "PREEXISTING"


@dataclass(frozen=True, slots=True)
class FamilyAEntryInputV2:
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    feature_evidence: FamilyAEntryFeatureEvidenceV2

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyAContractError("Family A entry requires USD-M Futures")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_bar_times(
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )
        if not isinstance(self.feature_evidence, FamilyAEntryFeatureEvidenceV2):
            raise FamilyAContractError(
                "feature_evidence must come from the causal Family A factory"
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
        if evidence_identity != self.identity:
            raise FamilyAContractError("entry identity differs from feature evidence")

    @property
    def identity(self) -> tuple[str, str, VenueV2, str, int, int, int]:
        return (
            self.attempt_id,
            self.symbol,
            self.venue,
            self.promoting_plan_sha256,
            self.bar_open_ms,
            self.bar_close_ms,
            self.decision_cutoff_ms,
        )


@dataclass(frozen=True, slots=True)
class FamilyAEntryDecisionV2:
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    feature_evidence_sha256: str
    feature_source_root_sha256: str
    status: FamilyAEntryStatusV2
    side: FamilyASideV2 | None
    reasons: tuple[str, ...]
    invalidation: str
    crowd_sign: int
    crowded_long_high: Decimal | None
    crowded_short_low: Decimal | None
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FAMILY_A_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _DECISION_FACTORY_TOKEN:
            raise FamilyAContractError("Family A entry decisions must be created by the evaluator")
        _validate_decision_identity(self)
        _validate_sha256(self.feature_evidence_sha256, "feature_evidence_sha256")
        _validate_sha256(
            self.feature_source_root_sha256,
            "feature_source_root_sha256",
        )
        _validate_entry_decision_state(self)
        object.__setattr__(
            self,
            "event_id",
            _hash_document(_DECISION_ID_DOMAIN, _entry_identity_document(self)),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(
                _ENTRY_PAYLOAD_DOMAIN,
                _entry_decision_document(self, include_payload_hash=False),
            ),
        )

    @property
    def emitted_signal(self) -> bool:
        return self.status is FamilyAEntryStatusV2.SIGNAL


@dataclass(frozen=True, slots=True)
class FamilyAEntryPreviewV2:
    """Factory-sealed, non-mutating snapshot for one transactional entry."""

    input_sha256: str
    pre_root_sha256: str
    pre_event_count: int
    decision: FamilyAEntryDecisionV2
    already_committed: bool
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ENTRY_PREVIEW_FACTORY_TOKEN:
            raise FamilyAContractError("Family A entry previews must be created by the ledger")
        _validate_sha256(self.input_sha256, "input_sha256")
        _validate_sha256(self.pre_root_sha256, "pre_root_sha256")
        _validate_nonnegative_int(self.pre_event_count, "pre_event_count")
        if not isinstance(self.decision, FamilyAEntryDecisionV2):
            raise FamilyAContractError("preview decision must be FamilyAEntryDecisionV2")
        canonical_family_a_entry_decision_v2(self.decision)
        if type(self.already_committed) is not bool:
            raise FamilyAContractError("already_committed must be boolean")


@dataclass(frozen=True, slots=True)
class FamilyAEntryCommitReceiptV2:
    """Ephemeral capability proving which transaction created an entry."""

    input_sha256: str
    event_id: str
    decision: FamilyAEntryDecisionV2
    preview_already_committed: bool
    pre_root_sha256: str
    pre_event_count: int
    post_root_sha256: str
    post_event_count: int
    disposition: FamilyAEntryCommitDispositionV2
    _owner_token: object = field(repr=False, compare=False)
    _rollback_capability: object = field(repr=False, compare=False)
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ENTRY_COMMIT_RECEIPT_FACTORY_TOKEN:
            raise FamilyAContractError(
                "Family A entry commit receipts must be created by the ledger"
            )
        _validate_sha256(self.input_sha256, "input_sha256")
        _validate_sha256(self.event_id, "event_id")
        _validate_sha256(self.pre_root_sha256, "pre_root_sha256")
        _validate_sha256(self.post_root_sha256, "post_root_sha256")
        _validate_nonnegative_int(self.pre_event_count, "pre_event_count")
        _validate_nonnegative_int(self.post_event_count, "post_event_count")
        if not isinstance(self.decision, FamilyAEntryDecisionV2):
            raise FamilyAContractError("receipt decision must be FamilyAEntryDecisionV2")
        canonical_family_a_entry_decision_v2(self.decision)
        if self.event_id != self.decision.event_id:
            raise FamilyAContractError("receipt event differs from its decision")
        if type(self.preview_already_committed) is not bool:
            raise FamilyAContractError("preview_already_committed must be boolean")
        if not isinstance(self.disposition, FamilyAEntryCommitDispositionV2):
            raise FamilyAContractError("disposition must be FamilyAEntryCommitDispositionV2")
        if self.disposition is FamilyAEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION:
            if self.preview_already_committed:
                raise FamilyAContractError("pre-existing preview cannot claim a new commit")
            if (
                self.post_event_count != self.pre_event_count + 1
                or self.post_root_sha256 == self.pre_root_sha256
            ):
                raise FamilyAContractError("new commit receipt has invalid post-state")
            return
        if self.preview_already_committed:
            if (
                self.post_event_count != self.pre_event_count
                or self.post_root_sha256 != self.pre_root_sha256
            ):
                raise FamilyAContractError("pre-existing replay receipt must preserve its state")
            return
        if (
            self.post_event_count != self.pre_event_count + 1
            or self.post_root_sha256 == self.pre_root_sha256
        ):
            raise FamilyAContractError("concurrent pre-existing receipt has invalid post-state")


@dataclass(frozen=True, slots=True)
class FamilyAPositionV2:
    """Frozen rule state created only after an external full-fill admission."""

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
    paper_executable_vwap: Decimal
    side: FamilyASideV2
    crowd_sign: int
    signal_bar_open_ms: int
    crowded_long_high: Decimal
    crowded_short_low: Decimal
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _POSITION_FACTORY_TOKEN:
            raise FamilyAContractError(
                "Family A position requires a ledgered external PAPER admission"
            )
        _validate_sha256(self.entry_event_id, "entry_event_id")
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_symbol(self.symbol)
        if self.venue is not VenueV2.USDM_FUTURES:
            raise FamilyAContractError("Family A position must remain USD-M Futures")
        for value, name in (
            (self.promoting_plan_sha256, "promoting_plan_sha256"),
            (self.feature_evidence_sha256, "feature_evidence_sha256"),
            (self.feature_source_root_sha256, "feature_source_root_sha256"),
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
            _validate_sha256(value, name)
        _validate_nonnegative_int(
            self.paper_registry_event_count,
            "paper_registry_event_count",
        )
        if self.paper_registry_event_count < 1:
            raise FamilyAContractError("paper registry checkpoint cannot be empty")
        if not all(
            _is_positive_finite(value)
            for value in (
                self.paper_requested_quantity,
                self.paper_filled_quantity,
                self.paper_executable_vwap,
            )
        ):
            raise FamilyAContractError("PAPER fill quantities and VWAP must be positive")
        if self.paper_requested_quantity != self.paper_filled_quantity:
            raise FamilyAContractError("position requires requested equals full fill")
        if self.crowd_sign not in (-1, 1):
            raise FamilyAContractError("crowd_sign must be -1 or 1")
        expected_side = FamilyASideV2.SHORT if self.crowd_sign == 1 else FamilyASideV2.LONG
        if self.side is not expected_side:
            raise FamilyAContractError("position side differs from crowd sign")
        _validate_nonnegative_int(self.signal_bar_open_ms, "signal_bar_open_ms")
        if self.signal_bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
            raise FamilyAContractError("signal bar must align to a 5m UTC slot")
        if not _is_positive_finite(self.crowded_long_high) or not _is_positive_finite(
            self.crowded_short_low
        ):
            raise FamilyAContractError("crowded references must be positive finite")
        if self.crowded_short_low > self.crowded_long_high:
            raise FamilyAContractError("crowded reference order is invalid")


@dataclass(frozen=True, slots=True)
class FamilyAAdmissionReceiptV2:
    """Ephemeral proof of one exact PAPER-backed position admission."""

    item: FamilyAEntryInputV2
    entry_decision: FamilyAEntryDecisionV2
    position: FamilyAPositionV2
    paper_decision: PaperFokEntryDecisionV2
    certificate: PaperFokFullFillCertificateV2
    input_sha256: str
    paper_registry_root_sha256: str
    paper_registry_event_count: int
    paper_registry_maximum_events: int
    paper_registry_checkpoint_sha256: str
    pre_root_sha256: str
    pre_event_count: int
    post_root_sha256: str
    post_event_count: int
    disposition: FamilyAAdmissionDispositionV2
    _owner_token: object = field(repr=False, compare=False)
    _rollback_capability: object | None = field(repr=False, compare=False)
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ADMISSION_RECEIPT_FACTORY_TOKEN:
            raise FamilyAContractError(
                "Family A admission receipts must be created by the episode ledger"
            )
        _validate_admission_receipt_contract(self)

    @property
    def entry_event_id(self) -> str:
        return self.entry_decision.event_id

    @property
    def paper_decision_event_id(self) -> str:
        return self.paper_decision.event_id

    @property
    def certificate_sha256(self) -> str:
        return self.certificate.certificate_sha256


@dataclass(frozen=True, slots=True)
class FamilyAExitInputV2:
    position: FamilyAPositionV2
    feature_evidence: FamilyAExitFeatureEvidenceV2

    def __post_init__(self) -> None:
        if not isinstance(self.position, FamilyAPositionV2):
            raise FamilyAContractError("position must be a ledger-created FamilyAPositionV2")
        if not isinstance(self.feature_evidence, FamilyAExitFeatureEvidenceV2):
            raise FamilyAContractError(
                "feature_evidence must come from the causal Family A exit factory"
            )
        evidence_identity = (
            self.feature_evidence.attempt_id,
            self.feature_evidence.symbol,
            self.feature_evidence.venue,
            self.feature_evidence.promoting_plan_sha256,
        )
        position_identity = (
            self.position.attempt_id,
            self.position.symbol,
            self.position.venue,
            self.position.promoting_plan_sha256,
        )
        if evidence_identity != position_identity:
            raise FamilyAContractError("exit evidence differs from position identity")
        if not 1 <= self.horizon_bars <= FAMILY_A_HARD_HORIZON_BARS_V2:
            raise FamilyAContractError("Family A exit horizon must be h=1..12")

    @property
    def bar_open_ms(self) -> int:
        return self.feature_evidence.bar_open_ms

    @property
    def bar_close_ms(self) -> int:
        return self.feature_evidence.bar_close_ms

    @property
    def decision_cutoff_ms(self) -> int:
        return self.feature_evidence.decision_cutoff_ms

    @property
    def horizon_bars(self) -> int:
        delta = self.bar_open_ms - self.position.signal_bar_open_ms
        if delta % FIVE_MINUTE_MS_V2 != 0:
            raise FamilyAContractError("exit bar is not aligned after signal bar")
        return delta // FIVE_MINUTE_MS_V2


@dataclass(frozen=True, slots=True)
class FamilyAExitDecisionV2:
    entry_event_id: str
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    feature_evidence_sha256: str
    feature_source_root_sha256: str
    side: FamilyASideV2
    action: FamilyAExitActionV2
    reason: FamilyAExitReasonV2
    reasons: tuple[str, ...]
    invalidation: str
    interval_status: FamilyAIntervalStatusV2
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FAMILY_A_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _DECISION_FACTORY_TOKEN:
            raise FamilyAContractError("Family A exit decisions must be created by the evaluator")
        _validate_sha256(self.entry_event_id, "entry_event_id")
        _validate_decision_identity(self)
        for value, name in (
            (self.feature_evidence_sha256, "feature_evidence_sha256"),
            (self.feature_source_root_sha256, "feature_source_root_sha256"),
        ):
            _validate_sha256(value, name)
        _validate_exit_decision_state(self)
        object.__setattr__(
            self,
            "event_id",
            _hash_document(_EXIT_ID_DOMAIN, _exit_identity_document(self)),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(
                _EXIT_PAYLOAD_DOMAIN,
                _exit_decision_document(self, include_payload_hash=False),
            ),
        )

    @property
    def exits_position(self) -> bool:
        return self.action is not FamilyAExitActionV2.HOLD


@dataclass(frozen=True, slots=True)
class FamilyAExitMutationReceiptV2:
    """Ephemeral proof of one ordered Family A exit-state mutation."""

    item: FamilyAExitInputV2
    decision: FamilyAExitDecisionV2
    input_sha256: str
    pre_root_sha256: str
    pre_event_count: int
    post_root_sha256: str
    post_event_count: int
    pre_next_horizon: int
    pre_sticky_inconclusive: bool
    pre_terminal: bool
    pre_active: bool
    post_next_horizon: int
    post_sticky_inconclusive: bool
    post_terminal: bool
    post_active: bool
    disposition: FamilyAExitDispositionV2
    _owner_token: object = field(repr=False, compare=False)
    _rollback_capability: object | None = field(repr=False, compare=False)
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _EXIT_MUTATION_RECEIPT_FACTORY_TOKEN:
            raise FamilyAContractError(
                "Family A exit mutation receipts must be created by the episode ledger"
            )
        _validate_exit_mutation_receipt_contract(self)

    @property
    def entry_event_id(self) -> str:
        return self.item.position.entry_event_id

    @property
    def exit_event_id(self) -> str:
        return self.decision.event_id


class FamilyADecisionRegistryV2:
    """Bounded append-once decision registry with canonical replay restore."""

    def __init__(self, *, maximum_events: int) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise FamilyAContractError("maximum_events must be a positive integer")
        self._maximum_events = maximum_events
        self._payload_by_event_id: dict[str, bytes] = {}

    @property
    def event_count(self) -> int:
        return len(self._payload_by_event_id)

    @property
    def root_sha256(self) -> str:
        return _hash_document(
            _REGISTRY_ROOT_DOMAIN,
            {
                "records": [
                    {
                        "event_id": event_id,
                        "payload_sha256": hashlib.sha256(payload).hexdigest(),
                    }
                    for event_id, payload in sorted(self._payload_by_event_id.items())
                ],
                "schema_version": "r4b_family_a_decision_registry_v2",
            },
        )

    def register(
        self,
        decision: FamilyAEntryDecisionV2 | FamilyAExitDecisionV2,
    ) -> FamilyARegistryDispositionV2:
        payload = _canonical_decision(decision)
        prior = self._payload_by_event_id.get(decision.event_id)
        if prior is not None:
            if prior != payload:
                raise FamilyAContractError(
                    "deterministic Family A event ID has conflicting payload"
                )
            return FamilyARegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if self.event_count >= self._maximum_events:
            raise FamilyAContractError("bounded Family A registry capacity exhausted")
        self._payload_by_event_id[decision.event_id] = payload
        return FamilyARegistryDispositionV2.NEW

    def export_replay_v2(self) -> bytes:
        return canonical_json_line(
            {
                "event_count": self.event_count,
                "records": [
                    {
                        "canonical_payload": payload.decode("utf-8"),
                        "event_id": event_id,
                    }
                    for event_id, payload in sorted(self._payload_by_event_id.items())
                ],
                "root_sha256": self.root_sha256,
                "schema_version": "r4b_family_a_registry_replay_v2",
            }
        )

    @classmethod
    def restore_replay_v2(
        cls,
        payload: bytes,
        *,
        maximum_events: int,
        expected_event_count: int,
        expected_root_sha256: str,
    ) -> FamilyADecisionRegistryV2:
        _validate_nonnegative_int(expected_event_count, "expected_event_count")
        _validate_sha256(expected_root_sha256, "expected_root_sha256")
        if not isinstance(payload, bytes):
            raise FamilyAContractError("registry replay payload must be bytes")
        try:
            document = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise FamilyAContractError("registry replay is not valid JSON") from error
        if not isinstance(document, dict) or canonical_json_line(document) != payload:
            raise FamilyAContractError("registry replay must be canonical JSON")
        if (
            set(document)
            != {
                "event_count",
                "records",
                "root_sha256",
                "schema_version",
            }
            or document.get("schema_version") != "r4b_family_a_registry_replay_v2"
        ):
            raise FamilyAContractError("registry replay schema is unsupported")
        if document.get("event_count") != expected_event_count:
            raise FamilyAContractError("registry replay count differs from checkpoint")
        if document.get("root_sha256") != expected_root_sha256:
            raise FamilyAContractError("registry replay root differs from checkpoint")
        records = document.get("records")
        if not isinstance(records, list):
            raise FamilyAContractError("registry replay records must be a list")
        registry = cls(maximum_events=maximum_events)
        prior_event_id = ""
        for row in records:
            if not isinstance(row, dict) or set(row) != {
                "canonical_payload",
                "event_id",
            }:
                raise FamilyAContractError("registry replay row has invalid shape")
            event_id = row["event_id"]
            canonical_payload = row["canonical_payload"]
            _validate_sha256(event_id, "event_id")
            if not isinstance(canonical_payload, str):
                raise FamilyAContractError("canonical payload must be a UTF-8 string")
            if event_id <= prior_event_id:
                raise FamilyAContractError("registry replay rows must be strictly sorted")
            encoded = canonical_payload.encode("utf-8")
            try:
                inner = json.loads(encoded)
            except json.JSONDecodeError as error:
                raise FamilyAContractError("decision replay payload is invalid") from error
            if not isinstance(inner, dict) or canonical_json_line(inner) != encoded:
                raise FamilyAContractError("decision replay payload is noncanonical")
            restored_decision = _decision_from_replay_document(inner)
            if (
                restored_decision.event_id != event_id
                or _canonical_decision(restored_decision) != encoded
            ):
                raise FamilyAContractError("decision replay payload is mismatched or invalid")
            if registry.event_count >= maximum_events:
                raise FamilyAContractError("registry replay exceeds bounded capacity")
            registry._payload_by_event_id[event_id] = encoded
            prior_event_id = event_id
        if registry.event_count != expected_event_count:
            raise FamilyAContractError("restored registry count differs")
        if registry.root_sha256 != expected_root_sha256:
            raise FamilyAContractError("restored registry root differs")
        return registry


@dataclass(slots=True)
class _FamilyAEpisodeStateV2:
    position: FamilyAPositionV2
    next_horizon: int = 1
    sticky_inconclusive: bool = False
    terminal: bool = False


class FamilyAEpisodeLedgerV2:
    """Bounded ledger for intents, admitted positions, and ordered h=1..12 exits."""

    def __init__(self, *, maximum_events: int) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise FamilyAContractError("maximum_events must be a positive integer")
        self._maximum_events = maximum_events
        self._entries: dict[str, tuple[str, FamilyAEntryDecisionV2]] = {}
        self._admissions: dict[str, tuple[str, FamilyAPositionV2]] = {}
        self._exits: dict[str, tuple[str, FamilyAExitDecisionV2]] = {}
        self._episodes: dict[str, _FamilyAEpisodeStateV2] = {}
        self._active_by_key: dict[tuple[str, VenueV2, str], str] = {}
        self._entry_commit_lock = RLock()
        self._entry_commit_owner_token = object()
        self._entry_rollback_capabilities: dict[str, object] = {}
        self._admission_owner_token = object()
        self._admission_rollback_capabilities: dict[str, object] = {}
        self._exit_mutation_owner_token = object()
        self._exit_rollback_capabilities: dict[str, object] = {}
        self._prospective_authority_token: object | None = None

    @property
    def event_count(self) -> int:
        return len(self._entries) + len(self._admissions) + len(self._exits)

    @property
    def maximum_events(self) -> int:
        return self._maximum_events

    @property
    def root_sha256(self) -> str:
        return self._root_sha256_with_entries(self._entries)

    def _claim_prospective_decision_authority_v2(self) -> object:
        """Exclusively gate mutations for one fresh prospective attempt."""

        with self._entry_commit_lock:
            if self._prospective_authority_token is not None:
                raise FamilyAContractError(
                    "Family A prospective decision authority is already held"
                )
            genesis = FamilyAEpisodeLedgerV2(maximum_events=self._maximum_events)
            if (
                self.event_count != 0
                or self.root_sha256 != genesis.root_sha256
                or self._entries
                or self._admissions
                or self._exits
                or self._episodes
                or self._active_by_key
                or self._entry_rollback_capabilities
                or self._admission_rollback_capabilities
                or self._exit_rollback_capabilities
            ):
                raise FamilyAContractError(
                    "Family A prospective authority requires exact genesis state"
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
            genesis = FamilyAEpisodeLedgerV2(maximum_events=self._maximum_events)
            if (
                self.event_count != 0
                or self.root_sha256 != genesis.root_sha256
                or self._entries
                or self._admissions
                or self._exits
                or self._episodes
                or self._active_by_key
                or self._entry_rollback_capabilities
                or self._admission_rollback_capabilities
                or self._exit_rollback_capabilities
            ):
                raise FamilyAContractError(
                    "cannot release a non-genesis Family A prospective authority"
                )
            self._prospective_authority_token = None

    def _assert_prospective_mutation_authority_v2(
        self,
        authority: object | None,
    ) -> None:
        held = self._prospective_authority_token
        if held is None:
            if authority is not None:
                raise FamilyAContractError("Family A prospective authority was not claimed")
            return
        if authority is not held:
            raise FamilyAContractError(
                "Family A mutation requires the held prospective decision authority"
            )

    def _root_sha256_with_entries(
        self,
        entries: dict[str, tuple[str, FamilyAEntryDecisionV2]],
    ) -> str:
        return _hash_document(
            _LEDGER_ROOT_DOMAIN,
            {
                "entries": [
                    {
                        "event_id": event_id,
                        "input_sha256": input_hash,
                        "payload_sha256": decision.payload_sha256,
                    }
                    for event_id, (input_hash, decision) in sorted(entries.items())
                ],
                "admissions": [
                    {
                        "entry_event_id": entry_event_id,
                        "input_sha256": input_hash,
                        "position": _position_document(position),
                    }
                    for entry_event_id, (input_hash, position) in sorted(self._admissions.items())
                ],
                "episodes": [
                    {
                        "admission_evidence_sha256": state.position.admission_evidence_sha256,
                        "entry_event_id": event_id,
                        "next_horizon": state.next_horizon,
                        "sticky_inconclusive": state.sticky_inconclusive,
                        "terminal": state.terminal,
                    }
                    for event_id, state in sorted(self._episodes.items())
                ],
                "exits": [
                    {
                        "event_id": event_id,
                        "input_sha256": input_hash,
                        "payload_sha256": decision.payload_sha256,
                    }
                    for event_id, (input_hash, decision) in sorted(self._exits.items())
                ],
                "schema_version": "r4b_family_a_episode_ledger_v2",
            },
        )

    def is_active(
        self,
        *,
        promoting_plan_sha256: str,
        venue: VenueV2,
        symbol: str,
    ) -> bool:
        return (promoting_plan_sha256, venue, symbol) in self._active_by_key

    def position_for_entry(self, entry_event_id: str) -> FamilyAPositionV2:
        """Return the immutable admitted position for restart-safe orchestration."""

        _validate_sha256(entry_event_id, "entry_event_id")
        state = self._episodes.get(entry_event_id)
        if state is None:
            raise FamilyAContractError("Family A episode position is absent")
        return state.position

    def evaluate_entry(
        self,
        item: FamilyAEntryInputV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyAEntryDecisionV2:
        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            preview = self.preview_entry(item)
            return self.commit_entry_preview(
                item,
                preview,
                _prospective_authority=_prospective_authority,
            )

    def preview_entry(self, item: FamilyAEntryInputV2) -> FamilyAEntryPreviewV2:
        """Evaluate against current owner state without mutating that state."""

        with self._entry_commit_lock:
            if not isinstance(item, FamilyAEntryInputV2):
                raise FamilyAContractError("item must be FamilyAEntryInputV2")
            logical_id = _entry_logical_event_id(item)
            input_hash = _entry_input_sha256(item)
            prior = self._entries.get(logical_id)
            if prior is not None:
                if prior[0] != input_hash:
                    raise FamilyAContractError(
                        "same Family A entry slot received conflicting causal input"
                    )
                decision = prior[1]
                already_committed = True
            else:
                self._require_capacity()
                active_key = (
                    item.promoting_plan_sha256,
                    item.venue,
                    item.symbol,
                )
                decision = _evaluate_entry(
                    item,
                    active_position=active_key in self._active_by_key,
                )
                if decision.event_id != logical_id:
                    raise FamilyAContractError("entry evaluator changed logical event ID")
                already_committed = False
            return FamilyAEntryPreviewV2(
                input_sha256=input_hash,
                pre_root_sha256=self.root_sha256,
                pre_event_count=self.event_count,
                decision=decision,
                already_committed=already_committed,
                _factory_token=_ENTRY_PREVIEW_FACTORY_TOKEN,
            )

    def commit_entry_preview(
        self,
        item: FamilyAEntryInputV2,
        preview: FamilyAEntryPreviewV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyAEntryDecisionV2:
        """Compatibility API returning the decision from a receipt-backed commit."""

        return self.commit_entry_preview_with_receipt(
            item,
            preview,
            _prospective_authority=_prospective_authority,
        ).decision

    def commit_entry_preview_with_receipt(
        self,
        item: FamilyAEntryInputV2,
        preview: FamilyAEntryPreviewV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyAEntryCommitReceiptV2:
        """Commit exactly one preview and identify which call created it."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            logical_id, input_hash = self._validate_entry_preview(item, preview)
            prior = self._entries.get(logical_id)
            if preview.already_committed:
                if (
                    self.event_count != preview.pre_event_count
                    or self.root_sha256 != preview.pre_root_sha256
                    or prior != (input_hash, preview.decision)
                ):
                    raise FamilyAContractError("Family A entry preview state drifted before commit")
                return self._entry_commit_receipt(
                    preview,
                    FamilyAEntryCommitDispositionV2.PREEXISTING,
                    object(),
                )
            if prior is not None:
                if prior != (input_hash, preview.decision):
                    raise FamilyAContractError(
                        "Family A entry preview conflicts with committed input"
                    )
                entries_without_target = dict(self._entries)
                del entries_without_target[logical_id]
                if (
                    self.event_count != preview.pre_event_count + 1
                    or self._root_sha256_with_entries(entries_without_target)
                    != preview.pre_root_sha256
                ):
                    raise FamilyAContractError("Family A entry preview state drifted before commit")
                return self._entry_commit_receipt(
                    preview,
                    FamilyAEntryCommitDispositionV2.PREEXISTING,
                    object(),
                )
            if (
                self.event_count != preview.pre_event_count
                or self.root_sha256 != preview.pre_root_sha256
            ):
                raise FamilyAContractError("Family A entry preview state drifted before commit")
            self._require_capacity()
            active_key = (
                item.promoting_plan_sha256,
                item.venue,
                item.symbol,
            )
            expected = _evaluate_entry(
                item,
                active_position=active_key in self._active_by_key,
            )
            if expected != preview.decision or expected.event_id != logical_id:
                raise FamilyAContractError("Family A entry preview decision drifted before commit")
            self._entries[logical_id] = (input_hash, preview.decision)
            rollback_capability = object()
            self._entry_rollback_capabilities[logical_id] = rollback_capability
            return self._entry_commit_receipt(
                preview,
                FamilyAEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION,
                rollback_capability,
            )

    def rollback_entry_preview(
        self,
        item: FamilyAEntryInputV2,
        preview: FamilyAEntryPreviewV2,
        receipt: FamilyAEntryCommitReceiptV2,
        *,
        _prospective_authority: object | None = None,
    ) -> bool:
        """Consume an exact NEW receipt and restore its untouched pre-state."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            logical_id, input_hash = self._validate_entry_preview(item, preview)
            self._validate_entry_commit_receipt(preview, receipt)
            if receipt.disposition is not FamilyAEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION:
                raise FamilyAContractError("cannot roll back a pre-existing Family A entry")
            if (
                self._entry_rollback_capabilities.get(logical_id)
                is not receipt._rollback_capability
            ):
                raise FamilyAContractError("Family A entry receipt does not own the current commit")
            if (
                self.event_count != receipt.post_event_count
                or self.root_sha256 != receipt.post_root_sha256
            ):
                raise FamilyAContractError("Family A entry preview state drifted before rollback")
            prior = self._entries.get(logical_id)
            if prior is None or prior != (input_hash, preview.decision):
                raise FamilyAContractError("Family A entry preview conflicts with rollback target")
            entries_without_target = dict(self._entries)
            del entries_without_target[logical_id]
            if self._root_sha256_with_entries(entries_without_target) != receipt.pre_root_sha256:
                raise FamilyAContractError("Family A entry preview state drifted before rollback")
            del self._entries[logical_id]
            if (
                self.event_count != receipt.pre_event_count
                or self.root_sha256 != receipt.pre_root_sha256
            ):
                self._entries[logical_id] = prior
                raise FamilyAContractError(
                    "Family A entry rollback failed to restore its checkpoint"
                )
            del self._entry_rollback_capabilities[logical_id]
            return True

    def _entry_commit_receipt(
        self,
        preview: FamilyAEntryPreviewV2,
        disposition: FamilyAEntryCommitDispositionV2,
        rollback_capability: object,
    ) -> FamilyAEntryCommitReceiptV2:
        return FamilyAEntryCommitReceiptV2(
            input_sha256=preview.input_sha256,
            event_id=preview.decision.event_id,
            decision=preview.decision,
            preview_already_committed=preview.already_committed,
            pre_root_sha256=preview.pre_root_sha256,
            pre_event_count=preview.pre_event_count,
            post_root_sha256=self.root_sha256,
            post_event_count=self.event_count,
            disposition=disposition,
            _owner_token=self._entry_commit_owner_token,
            _rollback_capability=rollback_capability,
            _factory_token=_ENTRY_COMMIT_RECEIPT_FACTORY_TOKEN,
        )

    def _validate_entry_commit_receipt(
        self,
        preview: FamilyAEntryPreviewV2,
        receipt: FamilyAEntryCommitReceiptV2,
    ) -> None:
        if not isinstance(receipt, FamilyAEntryCommitReceiptV2):
            raise FamilyAContractError("receipt must be FamilyAEntryCommitReceiptV2")
        if receipt._owner_token is not self._entry_commit_owner_token:
            raise FamilyAContractError("Family A entry receipt belongs to another ledger")
        if (
            receipt.input_sha256 != preview.input_sha256
            or receipt.event_id != preview.decision.event_id
            or receipt.decision != preview.decision
            or receipt.preview_already_committed != preview.already_committed
            or receipt.pre_root_sha256 != preview.pre_root_sha256
            or receipt.pre_event_count != preview.pre_event_count
        ):
            raise FamilyAContractError("Family A entry receipt differs from exact preview")

    def _validate_entry_preview(
        self,
        item: FamilyAEntryInputV2,
        preview: FamilyAEntryPreviewV2,
    ) -> tuple[str, str]:
        if not isinstance(item, FamilyAEntryInputV2):
            raise FamilyAContractError("item must be FamilyAEntryInputV2")
        if not isinstance(preview, FamilyAEntryPreviewV2):
            raise FamilyAContractError("preview must be FamilyAEntryPreviewV2")
        canonical_family_a_entry_decision_v2(preview.decision)
        logical_id = _entry_logical_event_id(item)
        input_hash = _entry_input_sha256(item)
        if preview.input_sha256 != input_hash or preview.decision.event_id != logical_id:
            raise FamilyAContractError("Family A entry preview differs from exact input")
        return logical_id, input_hash

    def admit_external_full_fill(
        self,
        item: FamilyAEntryInputV2,
        decision: FamilyAEntryDecisionV2,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        paper_registry: PaperFokDecisionRegistryV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyAPositionV2:
        """Compatibility API for a receipt-backed full PAPER FOK admission."""

        return self.admit_external_full_fill_with_receipt(
            item,
            decision,
            paper_decision,
            certificate,
            paper_registry,
            _prospective_authority=_prospective_authority,
        ).position

    def admit_external_full_fill_with_receipt(
        self,
        item: FamilyAEntryInputV2,
        decision: FamilyAEntryDecisionV2,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        paper_registry: PaperFokDecisionRegistryV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyAAdmissionReceiptV2:
        """Admit one exact PAPER fill and report mutation ownership."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            return self._admit_external_full_fill_with_receipt_guarded(
                item,
                decision,
                paper_decision,
                certificate,
                paper_registry,
            )

    def _admit_external_full_fill_with_receipt_guarded(
        self,
        item: FamilyAEntryInputV2,
        decision: FamilyAEntryDecisionV2,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        paper_registry: PaperFokDecisionRegistryV2,
    ) -> FamilyAAdmissionReceiptV2:
        """Apply a validated admission while the mutation lock is held."""

        self._assert_lifecycle_indices_consistent()
        pre_root_sha256 = self.root_sha256
        pre_event_count = self.event_count
        if not isinstance(paper_decision, PaperFokEntryDecisionV2):
            raise FamilyAContractError("paper_decision must be concrete PAPER FOK evidence")
        if not isinstance(certificate, PaperFokFullFillCertificateV2):
            raise FamilyAContractError("certificate must be concrete PAPER FOK evidence")
        if not isinstance(paper_registry, PaperFokDecisionRegistryV2):
            raise FamilyAContractError("paper_registry must be the concrete PAPER registry")
        stored = self._entries.get(decision.event_id)
        if stored is None or stored[1] != decision:
            raise FamilyAContractError("entry decision is absent from this episode ledger")
        if stored[0] != _entry_input_sha256(item):
            raise FamilyAContractError("entry item differs from its ledgered decision")
        if decision.status is not FamilyAEntryStatusV2.SIGNAL or decision.side is None:
            raise FamilyAContractError("only a ledgered Family A signal may be admitted")
        if paper_decision.status is not PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY:
            raise FamilyAContractError(
                "zero, partial, rejected, or pending PAPER entry is not a fill"
            )
        if not paper_decision.executed_full_quantity:
            raise FamilyAContractError("PAPER decision is not a full-quantity execution")
        expected_paper_side = (
            PaperFokSideV2.BUY if decision.side is FamilyASideV2.LONG else PaperFokSideV2.SELL
        )
        expected_identity = (
            item.attempt_id,
            decision.event_id,
            item.symbol,
            item.venue,
            item.promoting_plan_sha256,
            item.decision_cutoff_ms,
            item.decision_cutoff_ms + FAMILY_A_PAPER_TARGET_DELAY_MS_V2,
            expected_paper_side,
        )
        paper_identity = (
            paper_decision.attempt_id,
            paper_decision.signal_event_id,
            paper_decision.symbol,
            paper_decision.venue,
            paper_decision.promoting_plan_sha256,
            paper_decision.decision_cutoff_ms,
            paper_decision.target_venue_ms,
            paper_decision.side,
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
        if paper_identity != expected_identity or certificate_identity != expected_identity:
            raise FamilyAContractError("PAPER evidence identity differs from Family A signal")
        if (
            paper_decision.filled_quantity is None
            or paper_decision.executable_vwap is None
            or paper_decision.requested_quantity != paper_decision.filled_quantity
            or certificate.filled_quantity != paper_decision.requested_quantity
            or certificate.executable_vwap != paper_decision.executable_vwap
        ):
            raise FamilyAContractError("PAPER requested, filled, or VWAP evidence differs")
        paper_payload = canonical_paper_fok_entry_decision_v2(paper_decision)
        registry_root, registry_count, registry_checkpoint = _paper_registry_checkpoint(
            paper_registry,
            paper_decision,
            paper_payload,
        )
        expected_certificate = issue_paper_fok_full_fill_certificate_v2(
            paper_decision,
            registry=paper_registry,
            externally_pinned_checkpoint_sha256=registry_checkpoint,
        )
        if certificate != expected_certificate:
            raise FamilyAContractError("PAPER certificate differs from its sealed decision")
        admission_input_hash = _admission_input_sha256(
            entry_event_id=decision.event_id,
            paper_decision_payload_sha256=paper_decision.payload_sha256,
            certificate_sha256=certificate.certificate_sha256,
            paper_registry_root_sha256=registry_root,
            paper_registry_event_count=registry_count,
            paper_registry_checkpoint_sha256=registry_checkpoint,
        )
        prior_admission = self._admissions.get(decision.event_id)
        if prior_admission is not None:
            if prior_admission[0] != admission_input_hash:
                raise FamilyAContractError("same Family A admission has conflicting evidence")
            return self._admission_receipt(
                item=item,
                entry_decision=decision,
                position=prior_admission[1],
                paper_decision=paper_decision,
                certificate=certificate,
                input_sha256=admission_input_hash,
                paper_registry_root_sha256=registry_root,
                paper_registry_event_count=registry_count,
                paper_registry_maximum_events=paper_registry.maximum_events,
                paper_registry_checkpoint_sha256=registry_checkpoint,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                disposition=FamilyAAdmissionDispositionV2.PREEXISTING,
                rollback_capability=None,
            )
        self._require_capacity()
        active_key = (item.promoting_plan_sha256, item.venue, item.symbol)
        if active_key in self._active_by_key:
            raise FamilyAContractError("Family A symbol already has an active position")
        assert decision.crowded_long_high is not None
        assert decision.crowded_short_low is not None
        position = FamilyAPositionV2(
            entry_event_id=decision.event_id,
            attempt_id=item.attempt_id,
            symbol=item.symbol,
            venue=item.venue,
            promoting_plan_sha256=item.promoting_plan_sha256,
            feature_evidence_sha256=decision.feature_evidence_sha256,
            feature_source_root_sha256=decision.feature_source_root_sha256,
            admission_evidence_sha256=certificate.certificate_sha256,
            paper_decision_event_id=paper_decision.event_id,
            paper_decision_payload_sha256=paper_decision.payload_sha256,
            paper_registry_root_sha256=registry_root,
            paper_registry_event_count=registry_count,
            paper_registry_checkpoint_sha256=registry_checkpoint,
            paper_requested_quantity=paper_decision.requested_quantity,
            paper_filled_quantity=paper_decision.filled_quantity,
            paper_executable_vwap=paper_decision.executable_vwap,
            side=decision.side,
            crowd_sign=decision.crowd_sign,
            signal_bar_open_ms=decision.bar_open_ms,
            crowded_long_high=decision.crowded_long_high,
            crowded_short_low=decision.crowded_short_low,
            _factory_token=_POSITION_FACTORY_TOKEN,
        )
        if decision.event_id in self._admission_rollback_capabilities:
            raise FamilyAContractError("Family A admission has stale rollback ownership")
        self._admissions[decision.event_id] = (admission_input_hash, position)
        self._episodes[decision.event_id] = _FamilyAEpisodeStateV2(position=position)
        self._active_by_key[active_key] = decision.event_id
        rollback_capability = object()
        self._admission_rollback_capabilities[decision.event_id] = rollback_capability
        try:
            self._assert_lifecycle_indices_consistent()
            return self._admission_receipt(
                item=item,
                entry_decision=decision,
                position=position,
                paper_decision=paper_decision,
                certificate=certificate,
                input_sha256=admission_input_hash,
                paper_registry_root_sha256=registry_root,
                paper_registry_event_count=registry_count,
                paper_registry_maximum_events=paper_registry.maximum_events,
                paper_registry_checkpoint_sha256=registry_checkpoint,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                disposition=(FamilyAAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION),
                rollback_capability=rollback_capability,
            )
        except Exception:
            self._admission_rollback_capabilities.pop(decision.event_id, None)
            self._active_by_key.pop(active_key, None)
            self._episodes.pop(decision.event_id, None)
            self._admissions.pop(decision.event_id, None)
            raise

    def rollback_external_full_fill_admission(
        self,
        item: FamilyAEntryInputV2,
        decision: FamilyAEntryDecisionV2,
        receipt: FamilyAAdmissionReceiptV2,
        *,
        _prospective_authority: object | None = None,
    ) -> bool:
        """Consume an exact NEW receipt and remove only its untouched admission."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            self._validate_admission_receipt(item, decision, receipt)
            if receipt.disposition is not FamilyAAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION:
                raise FamilyAContractError("cannot roll back a pre-existing Family A admission")
            entry_event_id = decision.event_id
            if (
                self._admission_rollback_capabilities.get(entry_event_id)
                is not receipt._rollback_capability
            ):
                raise FamilyAContractError(
                    "Family A admission receipt does not own the current admission"
                )
            self._assert_lifecycle_indices_consistent()
            if (
                self.event_count != receipt.post_event_count
                or self.root_sha256 != receipt.post_root_sha256
            ):
                raise FamilyAContractError("Family A admission state drifted before rollback")
            stored = self._admissions.get(entry_event_id)
            state = self._episodes.get(entry_event_id)
            active_key = (
                receipt.position.promoting_plan_sha256,
                receipt.position.venue,
                receipt.position.symbol,
            )
            if stored is None or stored != (receipt.input_sha256, receipt.position):
                raise FamilyAContractError(
                    "Family A admission receipt differs from the rollback target"
                )
            if (
                state is None
                or state.position != receipt.position
                or state.next_horizon != 1
                or state.sticky_inconclusive
                or state.terminal
                or self._active_by_key.get(active_key) != entry_event_id
            ):
                raise FamilyAContractError("Family A admission episode drifted before rollback")
            assert stored is not None

            del self._active_by_key[active_key]
            del self._episodes[entry_event_id]
            del self._admissions[entry_event_id]
            try:
                self._assert_lifecycle_indices_consistent()
                if (
                    self.event_count != receipt.pre_event_count
                    or self.root_sha256 != receipt.pre_root_sha256
                ):
                    raise FamilyAContractError(
                        "Family A admission rollback failed to restore its checkpoint"
                    )
            except Exception:
                self._admissions[entry_event_id] = stored
                self._episodes[entry_event_id] = state
                self._active_by_key[active_key] = entry_event_id
                raise
            del self._admission_rollback_capabilities[entry_event_id]
            return True

    def _admission_receipt(
        self,
        *,
        item: FamilyAEntryInputV2,
        entry_decision: FamilyAEntryDecisionV2,
        position: FamilyAPositionV2,
        paper_decision: PaperFokEntryDecisionV2,
        certificate: PaperFokFullFillCertificateV2,
        input_sha256: str,
        paper_registry_root_sha256: str,
        paper_registry_event_count: int,
        paper_registry_maximum_events: int,
        paper_registry_checkpoint_sha256: str,
        pre_root_sha256: str,
        pre_event_count: int,
        disposition: FamilyAAdmissionDispositionV2,
        rollback_capability: object | None,
    ) -> FamilyAAdmissionReceiptV2:
        return FamilyAAdmissionReceiptV2(
            item=item,
            entry_decision=entry_decision,
            position=position,
            paper_decision=paper_decision,
            certificate=certificate,
            input_sha256=input_sha256,
            paper_registry_root_sha256=paper_registry_root_sha256,
            paper_registry_event_count=paper_registry_event_count,
            paper_registry_maximum_events=paper_registry_maximum_events,
            paper_registry_checkpoint_sha256=paper_registry_checkpoint_sha256,
            pre_root_sha256=pre_root_sha256,
            pre_event_count=pre_event_count,
            post_root_sha256=self.root_sha256,
            post_event_count=self.event_count,
            disposition=disposition,
            _owner_token=self._admission_owner_token,
            _rollback_capability=rollback_capability,
            _factory_token=_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )

    def _validate_admission_receipt(
        self,
        item: FamilyAEntryInputV2,
        decision: FamilyAEntryDecisionV2,
        receipt: FamilyAAdmissionReceiptV2,
    ) -> None:
        if type(receipt) is not FamilyAAdmissionReceiptV2:
            raise FamilyAContractError("receipt must be an exact FamilyAAdmissionReceiptV2")
        if receipt._owner_token is not self._admission_owner_token:
            raise FamilyAContractError("Family A admission receipt belongs to another ledger")
        _validate_admission_receipt_contract(receipt)
        if receipt.item != item or receipt.entry_decision != decision:
            raise FamilyAContractError("Family A admission receipt differs from the exact input")

    def evaluate_exit(
        self,
        item: FamilyAExitInputV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyAExitDecisionV2:
        """Compatibility API returning the decision from a receipt-backed mutation."""

        return self.evaluate_exit_with_receipt(
            item,
            _prospective_authority=_prospective_authority,
        ).decision

    def evaluate_exit_with_receipt(
        self,
        item: FamilyAExitInputV2,
        *,
        _prospective_authority: object | None = None,
    ) -> FamilyAExitMutationReceiptV2:
        """Evaluate one ordered exit slot and report exact mutation ownership."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            return self._evaluate_exit_with_receipt_guarded(item)

    def _evaluate_exit_with_receipt_guarded(
        self,
        item: FamilyAExitInputV2,
    ) -> FamilyAExitMutationReceiptV2:
        if not isinstance(item, FamilyAExitInputV2):
            raise FamilyAContractError("item must be FamilyAExitInputV2")
        self._assert_lifecycle_indices_consistent()
        logical_id = _exit_logical_event_id(item)
        input_hash = _exit_input_sha256(item)
        state = self._episodes.get(item.position.entry_event_id)
        if state is None or state.position != item.position:
            raise FamilyAContractError("exit position is absent from episode ledger")
        active_key = (
            item.position.promoting_plan_sha256,
            item.position.venue,
            item.position.symbol,
        )
        pre_root_sha256 = self.root_sha256
        pre_event_count = self.event_count
        pre_next_horizon = state.next_horizon
        pre_sticky_inconclusive = state.sticky_inconclusive
        pre_terminal = state.terminal
        pre_active = self._active_by_key.get(active_key) == item.position.entry_event_id
        prior = self._exits.get(logical_id)
        if prior is not None:
            if prior[0] != input_hash:
                raise FamilyAContractError(
                    "same Family A exit slot received conflicting causal input"
                )
            return self._exit_mutation_receipt(
                item=item,
                decision=prior[1],
                input_sha256=input_hash,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                pre_next_horizon=pre_next_horizon,
                pre_sticky_inconclusive=pre_sticky_inconclusive,
                pre_terminal=pre_terminal,
                pre_active=pre_active,
                disposition=FamilyAExitDispositionV2.PREEXISTING,
                rollback_capability=None,
            )
        self._require_capacity()
        if state.terminal:
            raise FamilyAContractError("Family A episode is already terminal")
        if item.horizon_bars != state.next_horizon:
            raise FamilyAContractError(f"expected Family A exit horizon h={state.next_horizon}")
        decision = _evaluate_exit(item)
        current_inconclusive = (
            item.feature_evidence.readiness is FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW
        )
        sticky = state.sticky_inconclusive or current_inconclusive
        if sticky and decision.interval_status is FamilyAIntervalStatusV2.COMPLETE:
            decision = _copy_exit_with_inconclusive_interval(decision)
        if logical_id in self._exit_rollback_capabilities:
            raise FamilyAContractError("Family A exit has stale rollback ownership")
        self._exits[logical_id] = (input_hash, decision)
        state.sticky_inconclusive = sticky
        rollback_capability = object()
        try:
            if decision.exits_position:
                state.terminal = True
                if self._active_by_key.get(active_key) != item.position.entry_event_id:
                    raise FamilyAContractError("active episode index differs from exit position")
                del self._active_by_key[active_key]
            else:
                state.next_horizon += 1
            self._exit_rollback_capabilities[logical_id] = rollback_capability
            self._assert_lifecycle_indices_consistent()
            return self._exit_mutation_receipt(
                item=item,
                decision=decision,
                input_sha256=input_hash,
                pre_root_sha256=pre_root_sha256,
                pre_event_count=pre_event_count,
                pre_next_horizon=pre_next_horizon,
                pre_sticky_inconclusive=pre_sticky_inconclusive,
                pre_terminal=pre_terminal,
                pre_active=pre_active,
                disposition=FamilyAExitDispositionV2.NEW_BY_THIS_TRANSACTION,
                rollback_capability=rollback_capability,
            )
        except Exception:
            self._exit_rollback_capabilities.pop(logical_id, None)
            self._exits.pop(logical_id, None)
            state.next_horizon = pre_next_horizon
            state.sticky_inconclusive = pre_sticky_inconclusive
            state.terminal = pre_terminal
            if pre_active:
                self._active_by_key[active_key] = item.position.entry_event_id
            else:
                self._active_by_key.pop(active_key, None)
            raise

    def rollback_exit(
        self,
        item: FamilyAExitInputV2,
        receipt: FamilyAExitMutationReceiptV2,
        *,
        _prospective_authority: object | None = None,
    ) -> bool:
        """Consume an exact NEW receipt and restore its untouched exit pre-state."""

        with self._entry_commit_lock:
            self._assert_prospective_mutation_authority_v2(_prospective_authority)
            self._validate_exit_mutation_receipt(item, receipt)
            if receipt.disposition is not FamilyAExitDispositionV2.NEW_BY_THIS_TRANSACTION:
                raise FamilyAContractError("cannot roll back a pre-existing Family A exit")
            logical_id = receipt.decision.event_id
            if self._exit_rollback_capabilities.get(logical_id) is not receipt._rollback_capability:
                raise FamilyAContractError(
                    "Family A exit receipt does not own the current mutation"
                )
            self._assert_lifecycle_indices_consistent()
            if (
                self.event_count != receipt.post_event_count
                or self.root_sha256 != receipt.post_root_sha256
            ):
                raise FamilyAContractError("Family A exit state drifted before rollback")
            stored_exit = self._exits.get(logical_id)
            state = self._episodes.get(receipt.entry_event_id)
            active_key = (
                receipt.item.position.promoting_plan_sha256,
                receipt.item.position.venue,
                receipt.item.position.symbol,
            )
            current_active = self._active_by_key.get(active_key) == receipt.entry_event_id
            if stored_exit is None or stored_exit != (
                receipt.input_sha256,
                receipt.decision,
            ):
                raise FamilyAContractError("Family A exit receipt differs from the rollback target")
            if (
                state is None
                or state.position != receipt.item.position
                or state.next_horizon != receipt.post_next_horizon
                or state.sticky_inconclusive != receipt.post_sticky_inconclusive
                or state.terminal != receipt.post_terminal
                or current_active != receipt.post_active
            ):
                raise FamilyAContractError("Family A exit episode drifted before rollback")
            assert stored_exit is not None

            del self._exits[logical_id]
            state.next_horizon = receipt.pre_next_horizon
            state.sticky_inconclusive = receipt.pre_sticky_inconclusive
            state.terminal = receipt.pre_terminal
            if receipt.pre_active:
                self._active_by_key[active_key] = receipt.entry_event_id
            else:
                self._active_by_key.pop(active_key, None)
            try:
                self._assert_lifecycle_indices_consistent()
                if (
                    self.event_count != receipt.pre_event_count
                    or self.root_sha256 != receipt.pre_root_sha256
                ):
                    raise FamilyAContractError(
                        "Family A exit rollback failed to restore its checkpoint"
                    )
            except Exception:
                self._exits[logical_id] = stored_exit
                state.next_horizon = receipt.post_next_horizon
                state.sticky_inconclusive = receipt.post_sticky_inconclusive
                state.terminal = receipt.post_terminal
                if receipt.post_active:
                    self._active_by_key[active_key] = receipt.entry_event_id
                else:
                    self._active_by_key.pop(active_key, None)
                raise
            del self._exit_rollback_capabilities[logical_id]
            return True

    def _exit_mutation_receipt(
        self,
        *,
        item: FamilyAExitInputV2,
        decision: FamilyAExitDecisionV2,
        input_sha256: str,
        pre_root_sha256: str,
        pre_event_count: int,
        pre_next_horizon: int,
        pre_sticky_inconclusive: bool,
        pre_terminal: bool,
        pre_active: bool,
        disposition: FamilyAExitDispositionV2,
        rollback_capability: object | None,
    ) -> FamilyAExitMutationReceiptV2:
        state = self._episodes.get(item.position.entry_event_id)
        if state is None or state.position != item.position:
            raise FamilyAContractError("exit position disappeared while issuing receipt")
        active_key = (
            item.position.promoting_plan_sha256,
            item.position.venue,
            item.position.symbol,
        )
        return FamilyAExitMutationReceiptV2(
            item=item,
            decision=decision,
            input_sha256=input_sha256,
            pre_root_sha256=pre_root_sha256,
            pre_event_count=pre_event_count,
            post_root_sha256=self.root_sha256,
            post_event_count=self.event_count,
            pre_next_horizon=pre_next_horizon,
            pre_sticky_inconclusive=pre_sticky_inconclusive,
            pre_terminal=pre_terminal,
            pre_active=pre_active,
            post_next_horizon=state.next_horizon,
            post_sticky_inconclusive=state.sticky_inconclusive,
            post_terminal=state.terminal,
            post_active=(self._active_by_key.get(active_key) == item.position.entry_event_id),
            disposition=disposition,
            _owner_token=self._exit_mutation_owner_token,
            _rollback_capability=rollback_capability,
            _factory_token=_EXIT_MUTATION_RECEIPT_FACTORY_TOKEN,
        )

    def _validate_exit_mutation_receipt(
        self,
        item: FamilyAExitInputV2,
        receipt: FamilyAExitMutationReceiptV2,
    ) -> None:
        if type(receipt) is not FamilyAExitMutationReceiptV2:
            raise FamilyAContractError("receipt must be an exact FamilyAExitMutationReceiptV2")
        if receipt._owner_token is not self._exit_mutation_owner_token:
            raise FamilyAContractError("Family A exit receipt belongs to another ledger")
        _validate_exit_mutation_receipt_contract(receipt)
        if receipt.item != item:
            raise FamilyAContractError("Family A exit receipt differs from the exact input")

    def _assert_lifecycle_indices_consistent(self) -> None:
        """Fail closed when rooted episode state and its active index diverge."""

        if set(self._admissions) != set(self._episodes):
            raise FamilyAContractError("Family A admission and episode indices are inconsistent")
        expected_active: dict[tuple[str, VenueV2, str], str] = {}
        for entry_event_id, state in self._episodes.items():
            admission = self._admissions.get(entry_event_id)
            if admission is None or admission[1] != state.position:
                raise FamilyAContractError("Family A episode position differs from its admission")
            if state.terminal:
                continue
            active_key = (
                state.position.promoting_plan_sha256,
                state.position.venue,
                state.position.symbol,
            )
            if active_key in expected_active:
                raise FamilyAContractError("Family A has multiple active episodes for one symbol")
            expected_active[active_key] = entry_event_id
        if self._active_by_key != expected_active:
            raise FamilyAContractError(
                "Family A active episode index differs from rooted lifecycle state"
            )
        if any(
            decision.entry_event_id not in self._episodes for _, decision in self._exits.values()
        ):
            raise FamilyAContractError("Family A exit refers to an absent episode")

    def export_state_v2(self) -> bytes:
        """Export a bounded canonical restart checkpoint for the full episode state."""

        return canonical_json_line(
            {
                "admissions": [
                    {
                        "entry_event_id": entry_event_id,
                        "input_sha256": input_hash,
                        "position": _position_document(position),
                    }
                    for entry_event_id, (input_hash, position) in sorted(self._admissions.items())
                ],
                "entries": [
                    {
                        "canonical_decision": canonical_family_a_entry_decision_v2(decision).decode(
                            "utf-8"
                        ),
                        "event_id": event_id,
                        "input_sha256": input_hash,
                    }
                    for event_id, (input_hash, decision) in sorted(self._entries.items())
                ],
                "episodes": [
                    {
                        "entry_event_id": entry_event_id,
                        "next_horizon": state.next_horizon,
                        "sticky_inconclusive": state.sticky_inconclusive,
                        "terminal": state.terminal,
                    }
                    for entry_event_id, state in sorted(self._episodes.items())
                ],
                "event_count": self.event_count,
                "exits": [
                    {
                        "canonical_decision": canonical_family_a_exit_decision_v2(decision).decode(
                            "utf-8"
                        ),
                        "event_id": event_id,
                        "input_sha256": input_hash,
                    }
                    for event_id, (input_hash, decision) in sorted(self._exits.items())
                ],
                "maximum_events": self._maximum_events,
                "root_sha256": self.root_sha256,
                "schema_version": "r4b_family_a_episode_state_v2",
            }
        )

    @classmethod
    def restore_state_v2(
        cls,
        payload: bytes,
        *,
        maximum_events: int,
        expected_event_count: int,
        expected_root_sha256: str,
    ) -> FamilyAEpisodeLedgerV2:
        """Restore only against an external count/root checkpoint."""

        _validate_sha256(expected_root_sha256, "expected_root_sha256")
        _validate_nonnegative_int(expected_event_count, "expected_event_count")
        if not isinstance(payload, bytes):
            raise FamilyAContractError("episode state payload must be bytes")
        try:
            document = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise FamilyAContractError("episode state is invalid JSON") from error
        if not isinstance(document, dict) or canonical_json_line(document) != payload:
            raise FamilyAContractError("episode state must be canonical JSON")
        required = {
            "admissions",
            "entries",
            "episodes",
            "event_count",
            "exits",
            "maximum_events",
            "root_sha256",
            "schema_version",
        }
        if set(document) != required or document.get("schema_version") != (
            "r4b_family_a_episode_state_v2"
        ):
            raise FamilyAContractError("episode state schema is unsupported")
        if document.get("maximum_events") != maximum_events:
            raise FamilyAContractError("episode state capacity differs")
        if document.get("event_count") != expected_event_count:
            raise FamilyAContractError("episode event count differs from checkpoint")
        if document.get("root_sha256") != expected_root_sha256:
            raise FamilyAContractError("episode root differs from checkpoint")
        ledger = cls(maximum_events=maximum_events)
        _restore_decision_rows(document.get("entries"), ledger, entry=True)
        _restore_admission_rows(document.get("admissions"), ledger)
        _restore_decision_rows(document.get("exits"), ledger, entry=False)
        _restore_episode_rows(document.get("episodes"), ledger)
        if ledger.event_count != expected_event_count:
            raise FamilyAContractError("restored episode event count differs")
        if ledger.root_sha256 != expected_root_sha256:
            raise FamilyAContractError("restored episode root differs")
        if ledger.export_state_v2() != payload:
            raise FamilyAContractError("episode state does not replay byte-for-byte")
        return ledger

    def _require_capacity(self) -> None:
        if self.event_count >= self._maximum_events:
            raise FamilyAContractError("bounded Family A episode ledger exhausted")


def evaluate_family_a_entry_v2(item: FamilyAEntryInputV2) -> FamilyAEntryDecisionV2:
    """Evaluate a source-sealed intent without claiming admission or a fill."""

    return _evaluate_entry(item, active_position=False)


def evaluate_family_a_exit_v2(item: FamilyAExitInputV2) -> FamilyAExitDecisionV2:
    """Evaluate one source-sealed exit without mutating episode sequence state."""

    return _evaluate_exit(item)


def canonical_family_a_entry_decision_v2(decision: FamilyAEntryDecisionV2) -> bytes:
    if not isinstance(decision, FamilyAEntryDecisionV2):
        raise FamilyAContractError("decision must be FamilyAEntryDecisionV2")
    expected = _hash_document(
        _ENTRY_PAYLOAD_DOMAIN,
        _entry_decision_document(decision, include_payload_hash=False),
    )
    if decision.payload_sha256 != expected:
        raise FamilyAContractError("entry payload hash differs from canonical decision")
    return canonical_json_line(_entry_decision_document(decision, include_payload_hash=True))


def parse_canonical_family_a_entry_decision_v2(
    payload: bytes,
) -> FamilyAEntryDecisionV2:
    """Restore one entry decision only from its exact canonical JSONL bytes."""

    if not isinstance(payload, bytes):
        raise FamilyAContractError("entry decision payload must be bytes")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FamilyAContractError("entry decision payload is invalid JSON") from error
    if not isinstance(document, dict) or canonical_json_line(document) != payload:
        raise FamilyAContractError("entry decision payload must be canonical JSONL")
    decision = _decision_from_replay_document(document)
    if not isinstance(decision, FamilyAEntryDecisionV2):
        raise FamilyAContractError("entry decision payload has a non-entry role")
    if canonical_family_a_entry_decision_v2(decision) != payload:
        raise FamilyAContractError("entry decision differs from canonical payload")
    return decision


def canonical_family_a_exit_decision_v2(decision: FamilyAExitDecisionV2) -> bytes:
    if not isinstance(decision, FamilyAExitDecisionV2):
        raise FamilyAContractError("decision must be FamilyAExitDecisionV2")
    expected = _hash_document(
        _EXIT_PAYLOAD_DOMAIN,
        _exit_decision_document(decision, include_payload_hash=False),
    )
    if decision.payload_sha256 != expected:
        raise FamilyAContractError("exit payload hash differs from canonical decision")
    return canonical_json_line(_exit_decision_document(decision, include_payload_hash=True))


def _validate_admission_receipt_contract(receipt: FamilyAAdmissionReceiptV2) -> None:
    if not isinstance(receipt.item, FamilyAEntryInputV2):
        raise FamilyAContractError("admission receipt item must be FamilyAEntryInputV2")
    if not isinstance(receipt.entry_decision, FamilyAEntryDecisionV2):
        raise FamilyAContractError("admission receipt decision must be FamilyAEntryDecisionV2")
    if not isinstance(receipt.position, FamilyAPositionV2):
        raise FamilyAContractError("admission receipt position must be FamilyAPositionV2")
    if not isinstance(receipt.paper_decision, PaperFokEntryDecisionV2):
        raise FamilyAContractError("admission receipt PAPER decision has the wrong type")
    if not isinstance(receipt.certificate, PaperFokFullFillCertificateV2):
        raise FamilyAContractError("admission receipt certificate has the wrong type")
    try:
        canonical_family_a_entry_decision_v2(receipt.entry_decision)
        canonical_paper_fok_entry_decision_v2(receipt.paper_decision)
        canonical_paper_fok_full_fill_certificate_v2(receipt.certificate)
    except ValueError as error:
        raise FamilyAContractError(
            "admission receipt contains noncanonical decision evidence"
        ) from error
    for value, name in (
        (receipt.input_sha256, "input_sha256"),
        (receipt.paper_registry_root_sha256, "paper_registry_root_sha256"),
        (
            receipt.paper_registry_checkpoint_sha256,
            "paper_registry_checkpoint_sha256",
        ),
        (receipt.pre_root_sha256, "pre_root_sha256"),
        (receipt.post_root_sha256, "post_root_sha256"),
    ):
        _validate_sha256(value, name)
    _validate_nonnegative_int(
        receipt.paper_registry_event_count,
        "paper_registry_event_count",
    )
    if (
        type(receipt.paper_registry_maximum_events) is not int
        or receipt.paper_registry_maximum_events < 1
        or receipt.paper_registry_event_count > receipt.paper_registry_maximum_events
    ):
        raise FamilyAContractError("admission receipt PAPER registry bounds are invalid")
    _validate_nonnegative_int(receipt.pre_event_count, "pre_event_count")
    _validate_nonnegative_int(receipt.post_event_count, "post_event_count")
    if not isinstance(receipt.disposition, FamilyAAdmissionDispositionV2):
        raise FamilyAContractError("admission disposition must be FamilyAAdmissionDispositionV2")

    item = receipt.item
    entry_decision = receipt.entry_decision
    position = receipt.position
    paper_decision = receipt.paper_decision
    certificate = receipt.certificate
    if (
        entry_decision.event_id != _entry_logical_event_id(item)
        or entry_decision.attempt_id != item.attempt_id
        or entry_decision.symbol != item.symbol
        or entry_decision.venue is not item.venue
        or entry_decision.promoting_plan_sha256 != item.promoting_plan_sha256
        or entry_decision.feature_evidence_sha256 != item.feature_evidence.evidence_sha256
        or entry_decision.status is not FamilyAEntryStatusV2.SIGNAL
        or entry_decision.side is None
    ):
        raise FamilyAContractError("admission receipt entry decision differs from its exact input")
    expected_paper_side = (
        PaperFokSideV2.BUY if entry_decision.side is FamilyASideV2.LONG else PaperFokSideV2.SELL
    )
    expected_identity = (
        item.attempt_id,
        entry_decision.event_id,
        item.symbol,
        item.venue,
        item.promoting_plan_sha256,
        item.decision_cutoff_ms,
        item.decision_cutoff_ms + FAMILY_A_PAPER_TARGET_DELAY_MS_V2,
        expected_paper_side,
    )
    if (
        paper_decision.attempt_id,
        paper_decision.signal_event_id,
        paper_decision.symbol,
        paper_decision.venue,
        paper_decision.promoting_plan_sha256,
        paper_decision.decision_cutoff_ms,
        paper_decision.target_venue_ms,
        paper_decision.side,
    ) != expected_identity or (
        certificate.attempt_id,
        certificate.signal_event_id,
        certificate.symbol,
        certificate.venue,
        certificate.promoting_plan_sha256,
        certificate.decision_cutoff_ms,
        certificate.target_venue_ms,
        certificate.side,
    ) != expected_identity:
        raise FamilyAContractError("admission receipt PAPER identity differs from its signal")
    if (
        not paper_decision.executed_full_quantity
        or paper_decision.filled_quantity is None
        or paper_decision.executable_vwap is None
        or certificate.decision_event_id != paper_decision.event_id
        or certificate.decision_payload_sha256 != paper_decision.payload_sha256
        or certificate.requested_quantity != paper_decision.requested_quantity
        or certificate.filled_quantity != paper_decision.filled_quantity
        or certificate.executable_vwap != paper_decision.executable_vwap
    ):
        raise FamilyAContractError("admission receipt PAPER decision and certificate differ")
    if (
        receipt.paper_registry_root_sha256 != certificate.terminal_registry_replay_root_sha256
        or receipt.paper_registry_event_count != certificate.terminal_registry_event_count
        or receipt.paper_registry_maximum_events != certificate.terminal_registry_maximum_events
        or receipt.paper_registry_checkpoint_sha256
        != certificate.terminal_registry_checkpoint_sha256
    ):
        raise FamilyAContractError(
            "admission receipt differs from its exact PAPER registry checkpoint"
        )
    if (
        position.entry_event_id != entry_decision.event_id
        or position.attempt_id != item.attempt_id
        or position.symbol != item.symbol
        or position.venue is not item.venue
        or position.promoting_plan_sha256 != item.promoting_plan_sha256
        or position.admission_evidence_sha256 != certificate.certificate_sha256
        or position.paper_decision_event_id != paper_decision.event_id
        or position.paper_decision_payload_sha256 != paper_decision.payload_sha256
        or position.paper_registry_root_sha256 != receipt.paper_registry_root_sha256
        or position.paper_registry_event_count != receipt.paper_registry_event_count
        or position.paper_registry_checkpoint_sha256 != receipt.paper_registry_checkpoint_sha256
        or position.side is not entry_decision.side
    ):
        raise FamilyAContractError("admission receipt position differs from its exact evidence")
    expected_input_sha256 = _admission_input_sha256(
        entry_event_id=entry_decision.event_id,
        paper_decision_payload_sha256=paper_decision.payload_sha256,
        certificate_sha256=certificate.certificate_sha256,
        paper_registry_root_sha256=receipt.paper_registry_root_sha256,
        paper_registry_event_count=receipt.paper_registry_event_count,
        paper_registry_checkpoint_sha256=(receipt.paper_registry_checkpoint_sha256),
    )
    if receipt.input_sha256 != expected_input_sha256:
        raise FamilyAContractError("admission receipt input hash is noncanonical")
    if receipt.disposition is FamilyAAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION:
        if receipt._rollback_capability is None:
            raise FamilyAContractError("new admission receipt lacks rollback ownership")
        if (
            receipt.post_event_count != receipt.pre_event_count + 1
            or receipt.post_root_sha256 == receipt.pre_root_sha256
        ):
            raise FamilyAContractError("new admission receipt has invalid post-state")
        return
    if receipt._rollback_capability is not None:
        raise FamilyAContractError("pre-existing admission receipt cannot own rollback authority")
    if (
        receipt.post_event_count != receipt.pre_event_count
        or receipt.post_root_sha256 != receipt.pre_root_sha256
    ):
        raise FamilyAContractError("pre-existing admission receipt must preserve state")


def _validate_exit_mutation_receipt_contract(
    receipt: FamilyAExitMutationReceiptV2,
) -> None:
    if not isinstance(receipt.item, FamilyAExitInputV2):
        raise FamilyAContractError("exit receipt item must be FamilyAExitInputV2")
    if not isinstance(receipt.decision, FamilyAExitDecisionV2):
        raise FamilyAContractError("exit receipt decision must be FamilyAExitDecisionV2")
    canonical_family_a_exit_decision_v2(receipt.decision)
    for value, name in (
        (receipt.input_sha256, "input_sha256"),
        (receipt.pre_root_sha256, "pre_root_sha256"),
        (receipt.post_root_sha256, "post_root_sha256"),
    ):
        _validate_sha256(value, name)
    for value, name in (
        (receipt.pre_event_count, "pre_event_count"),
        (receipt.post_event_count, "post_event_count"),
        (receipt.pre_next_horizon, "pre_next_horizon"),
        (receipt.post_next_horizon, "post_next_horizon"),
    ):
        _validate_nonnegative_int(value, name)
    if not (
        1 <= receipt.pre_next_horizon <= FAMILY_A_HARD_HORIZON_BARS_V2
        and 1 <= receipt.post_next_horizon <= FAMILY_A_HARD_HORIZON_BARS_V2
    ):
        raise FamilyAContractError("exit receipt horizon state is outside h=1..12")
    for value, name in (
        (receipt.pre_sticky_inconclusive, "pre_sticky_inconclusive"),
        (receipt.pre_terminal, "pre_terminal"),
        (receipt.pre_active, "pre_active"),
        (receipt.post_sticky_inconclusive, "post_sticky_inconclusive"),
        (receipt.post_terminal, "post_terminal"),
        (receipt.post_active, "post_active"),
    ):
        if type(value) is not bool:
            raise FamilyAContractError(f"{name} must be boolean")
    if receipt.pre_active == receipt.pre_terminal:
        raise FamilyAContractError("exit receipt pre-state active/terminal is inconsistent")
    if receipt.post_active == receipt.post_terminal:
        raise FamilyAContractError("exit receipt post-state active/terminal is inconsistent")
    if not isinstance(receipt.disposition, FamilyAExitDispositionV2):
        raise FamilyAContractError("exit disposition must be FamilyAExitDispositionV2")

    item = receipt.item
    decision = receipt.decision
    if (
        receipt.input_sha256 != _exit_input_sha256(item)
        or decision.event_id != _exit_logical_event_id(item)
        or decision.entry_event_id != item.position.entry_event_id
        or decision.attempt_id != item.position.attempt_id
        or decision.symbol != item.position.symbol
        or decision.venue is not item.position.venue
        or decision.promoting_plan_sha256 != item.position.promoting_plan_sha256
        or decision.bar_open_ms != item.bar_open_ms
        or decision.bar_close_ms != item.bar_close_ms
        or decision.decision_cutoff_ms != item.decision_cutoff_ms
        or decision.feature_evidence_sha256 != item.feature_evidence.evidence_sha256
        or decision.feature_source_root_sha256 != item.feature_evidence.source_root_sha256
        or decision.side is not item.position.side
    ):
        raise FamilyAContractError("exit receipt differs from its canonical input")
    if receipt.disposition is FamilyAExitDispositionV2.PREEXISTING:
        if receipt._rollback_capability is not None:
            raise FamilyAContractError("pre-existing exit receipt cannot own rollback authority")
        if (
            receipt.post_event_count != receipt.pre_event_count
            or receipt.post_root_sha256 != receipt.pre_root_sha256
            or receipt.post_next_horizon != receipt.pre_next_horizon
            or receipt.post_sticky_inconclusive != receipt.pre_sticky_inconclusive
            or receipt.post_terminal != receipt.pre_terminal
            or receipt.post_active != receipt.pre_active
        ):
            raise FamilyAContractError("pre-existing exit receipt must preserve current state")
        return
    if receipt._rollback_capability is None:
        raise FamilyAContractError("new exit receipt lacks rollback ownership")
    if (
        receipt.post_event_count != receipt.pre_event_count + 1
        or receipt.post_root_sha256 == receipt.pre_root_sha256
        or receipt.pre_terminal
        or not receipt.pre_active
        or item.horizon_bars != receipt.pre_next_horizon
        or (receipt.pre_sticky_inconclusive and not receipt.post_sticky_inconclusive)
    ):
        raise FamilyAContractError("new exit receipt has invalid pre/post state")
    if decision.exits_position:
        if (
            not receipt.post_terminal
            or receipt.post_active
            or receipt.post_next_horizon != receipt.pre_next_horizon
        ):
            raise FamilyAContractError("terminal exit receipt has an invalid state transition")
        return
    if (
        receipt.post_terminal
        or not receipt.post_active
        or receipt.post_next_horizon != receipt.pre_next_horizon + 1
    ):
        raise FamilyAContractError("holding exit receipt has an invalid state transition")


def _paper_registry_checkpoint(
    registry: PaperFokDecisionRegistryV2,
    decision: PaperFokEntryDecisionV2,
    canonical_payload: bytes,
) -> tuple[str, int, str]:
    state = registry.export_state_v2()
    checkpoint = registry.terminal_checkpoint_v2()
    if canonical_paper_fok_entry_decision_v2(decision) != canonical_payload:
        raise FamilyAContractError("PAPER decision payload is not canonical")
    if not registry.contains_exact_v2(decision):
        raise FamilyAContractError("PAPER decision is absent from registry checkpoint")
    restored = PaperFokDecisionRegistryV2.from_state_v2(
        state,
        expected_replay_root_sha256=checkpoint.replay_root_sha256,
        expected_event_count=checkpoint.event_count,
        expected_maximum_events=checkpoint.maximum_events,
        expected_attempt_id=checkpoint.attempt_id,
        expected_promoting_plan_sha256=checkpoint.promoting_plan_sha256,
        expected_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    if (
        restored.replay_root_sha256 != checkpoint.replay_root_sha256
        or restored.event_count != checkpoint.event_count
        or restored.terminal_checkpoint_v2() != checkpoint
        or not restored.contains_exact_v2(decision)
    ):
        raise FamilyAContractError("PAPER registry checkpoint does not replay exactly")
    return (
        checkpoint.replay_root_sha256,
        checkpoint.event_count,
        checkpoint.checkpoint_sha256,
    )


def _restore_decision_rows(
    raw_rows: object,
    ledger: FamilyAEpisodeLedgerV2,
    *,
    entry: bool,
) -> None:
    if not isinstance(raw_rows, list):
        raise FamilyAContractError("episode decision rows must be a list")
    prior_event_id = ""
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict) or set(raw_row) != {
            "canonical_decision",
            "event_id",
            "input_sha256",
        }:
            raise FamilyAContractError("episode decision row has invalid shape")
        event_id = raw_row["event_id"]
        input_hash = raw_row["input_sha256"]
        canonical_decision = raw_row["canonical_decision"]
        _validate_sha256(event_id, "event_id")
        _validate_sha256(input_hash, "input_sha256")
        if event_id <= prior_event_id:
            raise FamilyAContractError("episode decision rows are not strictly sorted")
        if not isinstance(canonical_decision, str):
            raise FamilyAContractError("canonical decision must be a string")
        encoded = canonical_decision.encode("utf-8")
        try:
            inner = json.loads(encoded)
        except json.JSONDecodeError as error:
            raise FamilyAContractError("episode decision payload is invalid") from error
        if not isinstance(inner, dict) or canonical_json_line(inner) != encoded:
            raise FamilyAContractError("episode decision payload is noncanonical")
        decision = _decision_from_replay_document(inner)
        if decision.event_id != event_id or _canonical_decision(decision) != encoded:
            raise FamilyAContractError("episode decision payload does not rederive")
        if entry and not isinstance(decision, FamilyAEntryDecisionV2):
            raise FamilyAContractError("entry row contains an exit decision")
        if not entry and not isinstance(decision, FamilyAExitDecisionV2):
            raise FamilyAContractError("exit row contains an entry decision")
        target = ledger._entries if entry else ledger._exits
        if event_id in target:
            raise FamilyAContractError("episode state repeats a decision event")
        target[event_id] = (input_hash, decision)  # type: ignore[assignment]
        prior_event_id = event_id
    if ledger.event_count > ledger._maximum_events:
        raise FamilyAContractError("episode decision rows exceed capacity")


def _restore_admission_rows(
    raw_rows: object,
    ledger: FamilyAEpisodeLedgerV2,
) -> None:
    if not isinstance(raw_rows, list):
        raise FamilyAContractError("episode admission rows must be a list")
    prior_entry_event_id = ""
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict) or set(raw_row) != {
            "entry_event_id",
            "input_sha256",
            "position",
        }:
            raise FamilyAContractError("episode admission row has invalid shape")
        entry_event_id = raw_row["entry_event_id"]
        input_hash = raw_row["input_sha256"]
        _validate_sha256(entry_event_id, "entry_event_id")
        _validate_sha256(input_hash, "input_sha256")
        if entry_event_id <= prior_entry_event_id:
            raise FamilyAContractError("episode admission rows are not strictly sorted")
        position = _position_from_document(raw_row["position"])
        if position.entry_event_id != entry_event_id:
            raise FamilyAContractError("admission position names a different entry")
        entry_record = ledger._entries.get(entry_event_id)
        if (
            entry_record is None
            or entry_record[1].status is not FamilyAEntryStatusV2.SIGNAL
            or entry_record[1].side is not position.side
        ):
            raise FamilyAContractError("admission has no matching signal decision")
        entry_decision = entry_record[1]
        if (
            position.attempt_id != entry_decision.attempt_id
            or position.symbol != entry_decision.symbol
            or position.venue is not entry_decision.venue
            or position.promoting_plan_sha256 != entry_decision.promoting_plan_sha256
            or position.feature_evidence_sha256 != entry_decision.feature_evidence_sha256
            or position.feature_source_root_sha256 != entry_decision.feature_source_root_sha256
            or position.side is not entry_decision.side
            or position.crowd_sign != entry_decision.crowd_sign
            or position.signal_bar_open_ms != entry_decision.bar_open_ms
            or position.crowded_long_high != entry_decision.crowded_long_high
            or position.crowded_short_low != entry_decision.crowded_short_low
        ):
            raise FamilyAContractError("admitted position differs from signal decision")
        ledger._admissions[entry_event_id] = (input_hash, position)
        prior_entry_event_id = entry_event_id
    if ledger.event_count > ledger._maximum_events:
        raise FamilyAContractError("episode admission rows exceed capacity")


def _restore_episode_rows(
    raw_rows: object,
    ledger: FamilyAEpisodeLedgerV2,
) -> None:
    if not isinstance(raw_rows, list):
        raise FamilyAContractError("episode state rows must be a list")
    prior_entry_event_id = ""
    for raw_row in raw_rows:
        if not isinstance(raw_row, dict) or set(raw_row) != {
            "entry_event_id",
            "next_horizon",
            "sticky_inconclusive",
            "terminal",
        }:
            raise FamilyAContractError("episode row has invalid shape")
        entry_event_id = raw_row["entry_event_id"]
        _validate_sha256(entry_event_id, "entry_event_id")
        if entry_event_id <= prior_entry_event_id:
            raise FamilyAContractError("episode rows are not strictly sorted")
        admission = ledger._admissions.get(entry_event_id)
        if admission is None:
            raise FamilyAContractError("episode row has no admitted position")
        next_horizon = raw_row["next_horizon"]
        sticky = raw_row["sticky_inconclusive"]
        terminal = raw_row["terminal"]
        if type(next_horizon) is not int or not 1 <= next_horizon <= 12:
            raise FamilyAContractError("episode next horizon is invalid")
        if type(sticky) is not bool or type(terminal) is not bool:
            raise FamilyAContractError("episode flags must be boolean")
        position = admission[1]
        exits = sorted(
            (
                decision
                for _, decision in ledger._exits.values()
                if decision.entry_event_id == entry_event_id
            ),
            key=lambda decision: decision.bar_open_ms,
        )
        horizons = tuple(
            (decision.bar_open_ms - position.signal_bar_open_ms) // FIVE_MINUTE_MS_V2
            for decision in exits
        )
        if horizons != tuple(range(1, len(exits) + 1)):
            raise FamilyAContractError("episode exits are not contiguous h1..h12")
        if any(
            (
                decision.attempt_id,
                decision.symbol,
                decision.venue,
                decision.promoting_plan_sha256,
                decision.side,
            )
            != (
                position.attempt_id,
                position.symbol,
                position.venue,
                position.promoting_plan_sha256,
                position.side,
            )
            for decision in exits
        ):
            raise FamilyAContractError("episode exit identity differs from position")
        observed_terminal = bool(exits and exits[-1].exits_position)
        if any(decision.exits_position for decision in exits[:-1]):
            raise FamilyAContractError("episode has rows after a terminal exit")
        expected_next = len(exits) if observed_terminal else len(exits) + 1
        if terminal != observed_terminal or next_horizon != expected_next:
            raise FamilyAContractError("episode horizon or terminal state differs")
        observed_flow_inconclusive = any(
            "INCOMPLETE_FLOW_CONDITION_NOT_EVALUATED" in decision.reasons for decision in exits
        )
        if observed_flow_inconclusive and not sticky:
            raise FamilyAContractError("episode lost sticky flow-inconclusive state")
        state = _FamilyAEpisodeStateV2(
            position=position,
            next_horizon=next_horizon,
            sticky_inconclusive=sticky,
            terminal=terminal,
        )
        ledger._episodes[entry_event_id] = state
        if not terminal:
            active_key = (
                position.promoting_plan_sha256,
                position.venue,
                position.symbol,
            )
            if active_key in ledger._active_by_key:
                raise FamilyAContractError("episode state repeats an active symbol")
            ledger._active_by_key[active_key] = entry_event_id
        prior_entry_event_id = entry_event_id
    if set(ledger._episodes) != set(ledger._admissions):
        raise FamilyAContractError("admissions and episode state differ")
    if any(
        decision.entry_event_id not in ledger._episodes for _, decision in ledger._exits.values()
    ):
        raise FamilyAContractError("episode state contains an orphan exit")


def _position_from_document(raw: object) -> FamilyAPositionV2:
    required = {
        "admission_evidence_sha256",
        "attempt_id",
        "crowd_sign",
        "crowded_long_high",
        "crowded_short_low",
        "entry_event_id",
        "feature_evidence_sha256",
        "feature_source_root_sha256",
        "paper_decision_event_id",
        "paper_decision_payload_sha256",
        "paper_executable_vwap",
        "paper_filled_quantity",
        "paper_registry_checkpoint_sha256",
        "paper_registry_event_count",
        "paper_registry_root_sha256",
        "paper_requested_quantity",
        "promoting_plan_sha256",
        "side",
        "signal_bar_open_ms",
        "symbol",
        "venue",
    }
    if not isinstance(raw, dict) or set(raw) != required:
        raise FamilyAContractError("position replay document has invalid shape")
    try:
        return FamilyAPositionV2(
            entry_event_id=_json_str(raw, "entry_event_id"),
            attempt_id=_json_str(raw, "attempt_id"),
            symbol=_json_str(raw, "symbol"),
            venue=VenueV2(_json_str(raw, "venue")),
            promoting_plan_sha256=_json_str(raw, "promoting_plan_sha256"),
            feature_evidence_sha256=_json_str(raw, "feature_evidence_sha256"),
            feature_source_root_sha256=_json_str(
                raw,
                "feature_source_root_sha256",
            ),
            admission_evidence_sha256=_json_str(
                raw,
                "admission_evidence_sha256",
            ),
            paper_decision_event_id=_json_str(raw, "paper_decision_event_id"),
            paper_decision_payload_sha256=_json_str(
                raw,
                "paper_decision_payload_sha256",
            ),
            paper_registry_root_sha256=_json_str(
                raw,
                "paper_registry_root_sha256",
            ),
            paper_registry_event_count=_json_int(raw, "paper_registry_event_count"),
            paper_registry_checkpoint_sha256=_json_str(
                raw,
                "paper_registry_checkpoint_sha256",
            ),
            paper_requested_quantity=Decimal(_json_str(raw, "paper_requested_quantity")),
            paper_filled_quantity=Decimal(_json_str(raw, "paper_filled_quantity")),
            paper_executable_vwap=Decimal(_json_str(raw, "paper_executable_vwap")),
            side=FamilyASideV2(_json_str(raw, "side")),
            crowd_sign=_json_int(raw, "crowd_sign"),
            signal_bar_open_ms=_json_int(raw, "signal_bar_open_ms"),
            crowded_long_high=Decimal(_json_str(raw, "crowded_long_high")),
            crowded_short_low=Decimal(_json_str(raw, "crowded_short_low")),
            _factory_token=_POSITION_FACTORY_TOKEN,
        )
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, FamilyAContractError):
            raise
        raise FamilyAContractError("position replay field is invalid") from error


def _evaluate_entry(
    item: FamilyAEntryInputV2,
    *,
    active_position: bool,
) -> FamilyAEntryDecisionV2:
    evidence = item.feature_evidence
    if evidence.readiness is not FamilyAFeatureReadinessV2.READY:
        status = _entry_status_from_readiness(evidence.readiness)
        return _entry_decision(
            item,
            status=status,
            side=None,
            reasons=evidence.reasons,
            invalidation=f"{status.value}_DO_NOT_ACT",
            crowd_sign=0,
            crowded_long_high=None,
            crowded_short_low=None,
        )
    assert evidence.r12_previous is not None
    crowd_sign = _sign(evidence.r12_previous)
    if crowd_sign == 0:
        return _entry_decision(
            item,
            status=FamilyAEntryStatusV2.NO_SIGNAL,
            side=None,
            reasons=("CROWD_SIGN_ZERO",),
            invalidation="NO_POSITION_NO_INVALIDATION",
            crowd_sign=0,
            crowded_long_high=None,
            crowded_short_low=None,
        )
    failed = _failed_entry_conditions(evidence, crowd_sign)
    if failed:
        return _entry_decision(
            item,
            status=FamilyAEntryStatusV2.NO_SIGNAL,
            side=None,
            reasons=failed,
            invalidation="NO_POSITION_NO_INVALIDATION",
            crowd_sign=crowd_sign,
            crowded_long_high=None,
            crowded_short_low=None,
        )
    if active_position:
        return _entry_decision(
            item,
            status=FamilyAEntryStatusV2.NOT_ADMITTED_ACTIVE_POSITION,
            side=None,
            reasons=("FAMILY_SYMBOL_POSITION_ALREADY_OPEN",),
            invalidation="ACTIVE_POSITION_UNCHANGED",
            crowd_sign=crowd_sign,
            crowded_long_high=None,
            crowded_short_low=None,
        )
    side = FamilyASideV2.SHORT if crowd_sign == 1 else FamilyASideV2.LONG
    assert evidence.crowded_long_high is not None
    assert evidence.crowded_short_low is not None
    invalidation = (
        "close_j > crowded_long_high"
        if side is FamilyASideV2.SHORT
        else "close_j < crowded_short_low"
    )
    return _entry_decision(
        item,
        status=FamilyAEntryStatusV2.SIGNAL,
        side=side,
        reasons=(
            "CROWDING_PRECONDITIONS_MET",
            "DELEVERAGING_TRIGGER_MET",
            f"ACTION_{side.value}",
            "INTENT_ONLY_PAPER_ADMISSION_REQUIRED",
        ),
        invalidation=invalidation,
        crowd_sign=crowd_sign,
        crowded_long_high=evidence.crowded_long_high,
        crowded_short_low=evidence.crowded_short_low,
    )


def _evaluate_exit(
    item: FamilyAExitInputV2,
) -> FamilyAExitDecisionV2:
    evidence = item.feature_evidence
    action = (
        FamilyAExitActionV2.EXIT_LONG
        if item.position.side is FamilyASideV2.LONG
        else FamilyAExitActionV2.EXIT_SHORT
    )
    if evidence.readiness not in (
        FamilyAFeatureReadinessV2.READY,
        FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW,
    ):
        return _exit_decision(
            item,
            action=action,
            reason=FamilyAExitReasonV2.MANDATORY_DATA_EMERGENCY,
            reasons=(*evidence.reasons, "REQUIRED_EXIT_DATA_UNAVAILABLE"),
            invalidation="POSITION_EXIT_REQUIRED",
            interval_status=FamilyAIntervalStatusV2.INCONCLUSIVE_DATA,
        )
    assert evidence.close_price is not None
    assert evidence.rz_basis_current is not None
    if (
        item.position.side is FamilyASideV2.SHORT
        and evidence.close_price > item.position.crowded_long_high
    ) or (
        item.position.side is FamilyASideV2.LONG
        and evidence.close_price < item.position.crowded_short_low
    ):
        return _exit_decision(
            item,
            action=action,
            reason=FamilyAExitReasonV2.ADVERSE_INVALIDATION,
            reasons=("STRICT_CROWDED_REFERENCE_BREACH",),
            invalidation="POSITION_EXIT_REQUIRED",
            interval_status=_interval_status(evidence),
        )
    crowd = Decimal(item.position.crowd_sign)
    with localcontext(protocol_decimal_context_v2()):
        normalized_basis = crowd * evidence.rz_basis_current
    if normalized_basis <= 0:
        return _exit_decision(
            item,
            action=action,
            reason=FamilyAExitReasonV2.BASIS_NORMALIZATION,
            reasons=("CROWD_SIGN_TIMES_BASIS_RZ_LE_ZERO",),
            invalidation="POSITION_EXIT_REQUIRED",
            interval_status=_interval_status(evidence),
        )
    if evidence.readiness is FamilyAFeatureReadinessV2.READY:
        assert evidence.flow_previous is not None
        assert evidence.flow_current is not None
        with localcontext(protocol_decimal_context_v2()):
            previous_aligned_flow = crowd * evidence.flow_previous
            current_aligned_flow = crowd * evidence.flow_current
        if previous_aligned_flow >= _EXIT_FLOW and current_aligned_flow >= _EXIT_FLOW:
            return _exit_decision(
                item,
                action=action,
                reason=FamilyAExitReasonV2.TWO_BAR_FLOW_REVERSAL,
                reasons=("TWO_COMPLETE_BARS_AT_FLOW_REVERSAL_BOUNDARY",),
                invalidation="POSITION_EXIT_REQUIRED",
                interval_status=FamilyAIntervalStatusV2.COMPLETE,
            )
    if item.horizon_bars == FAMILY_A_HARD_HORIZON_BARS_V2:
        return _exit_decision(
            item,
            action=action,
            reason=FamilyAExitReasonV2.HARD_HORIZON,
            reasons=("HARD_HORIZON_EXACT",),
            invalidation="POSITION_EXIT_REQUIRED",
            interval_status=_interval_status(evidence),
        )
    reasons = (
        ("INCOMPLETE_FLOW_CONDITION_NOT_EVALUATED",)
        if evidence.readiness is FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW
        else ("NO_EXIT_CONDITION_MET",)
    )
    return _exit_decision(
        item,
        action=FamilyAExitActionV2.HOLD,
        reason=FamilyAExitReasonV2.HOLD,
        reasons=reasons,
        invalidation=(
            "close_j > crowded_long_high"
            if item.position.side is FamilyASideV2.SHORT
            else "close_j < crowded_short_low"
        ),
        interval_status=_interval_status(evidence),
    )


def _failed_entry_conditions(
    evidence: FamilyAEntryFeatureEvidenceV2,
    crowd_sign: int,
) -> tuple[str, ...]:
    values = (
        evidence.rz_r12_previous,
        evidence.rz_doi12_previous,
        evidence.rz_basis_previous,
        evidence.rz_funding_previous,
        evidence.rz_r1_current,
        evidence.rz_doi1_current,
        evidence.flow_current,
    )
    assert all(isinstance(value, Decimal) for value in values)
    rz_r12, rz_doi12, rz_basis, rz_funding, rz_r1, rz_doi1, flow = values
    assert isinstance(rz_r12, Decimal)
    assert isinstance(rz_doi12, Decimal)
    assert isinstance(rz_basis, Decimal)
    assert isinstance(rz_funding, Decimal)
    assert isinstance(rz_r1, Decimal)
    assert isinstance(rz_doi1, Decimal)
    assert isinstance(flow, Decimal)
    crowd = Decimal(crowd_sign)
    with localcontext(protocol_decimal_context_v2()):
        aligned_basis = crowd * rz_basis
        aligned_funding = crowd * rz_funding
        aligned_r1 = crowd * rz_r1
        aligned_flow = crowd * flow
        absolute_r12 = abs(rz_r12)
    checks = (
        (absolute_r12 >= _PRE_R12, "PRE_ABS_RZ_R12_LT_1_5"),
        (rz_doi12 >= _PRE_DOI12, "PRE_RZ_DOI12_LT_1_5"),
        (aligned_basis >= _PRE_BASIS, "PRE_ALIGNED_BASIS_LT_1_5"),
        (aligned_funding >= _PRE_FUNDING, "PRE_ALIGNED_FUNDING_LT_1_0"),
        (aligned_r1 <= _TRIGGER_R1, "TRIGGER_REVERSAL_GT_NEG_0_5"),
        (rz_doi1 <= _TRIGGER_DOI1, "TRIGGER_DOI1_GT_NEG_1_0"),
        (aligned_flow <= _TRIGGER_FLOW, "TRIGGER_FLOW_GT_NEG_0_35"),
    )
    return tuple(reason for passed, reason in checks if not passed)


def _entry_decision(
    item: FamilyAEntryInputV2,
    *,
    status: FamilyAEntryStatusV2,
    side: FamilyASideV2 | None,
    reasons: tuple[str, ...],
    invalidation: str,
    crowd_sign: int,
    crowded_long_high: Decimal | None,
    crowded_short_low: Decimal | None,
) -> FamilyAEntryDecisionV2:
    return FamilyAEntryDecisionV2(
        attempt_id=item.attempt_id,
        symbol=item.symbol,
        venue=item.venue,
        promoting_plan_sha256=item.promoting_plan_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        feature_evidence_sha256=item.feature_evidence.evidence_sha256,
        feature_source_root_sha256=item.feature_evidence.source_root_sha256,
        status=status,
        side=side,
        reasons=reasons,
        invalidation=invalidation,
        crowd_sign=crowd_sign,
        crowded_long_high=crowded_long_high,
        crowded_short_low=crowded_short_low,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _exit_decision(
    item: FamilyAExitInputV2,
    *,
    action: FamilyAExitActionV2,
    reason: FamilyAExitReasonV2,
    reasons: tuple[str, ...],
    invalidation: str,
    interval_status: FamilyAIntervalStatusV2,
) -> FamilyAExitDecisionV2:
    evidence = item.feature_evidence
    position = item.position
    return FamilyAExitDecisionV2(
        entry_event_id=position.entry_event_id,
        attempt_id=position.attempt_id,
        symbol=position.symbol,
        venue=position.venue,
        promoting_plan_sha256=position.promoting_plan_sha256,
        bar_open_ms=evidence.bar_open_ms,
        bar_close_ms=evidence.bar_close_ms,
        decision_cutoff_ms=evidence.decision_cutoff_ms,
        feature_evidence_sha256=evidence.evidence_sha256,
        feature_source_root_sha256=evidence.source_root_sha256,
        side=position.side,
        action=action,
        reason=reason,
        reasons=reasons,
        invalidation=invalidation,
        interval_status=interval_status,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _copy_exit_with_inconclusive_interval(
    decision: FamilyAExitDecisionV2,
) -> FamilyAExitDecisionV2:
    return FamilyAExitDecisionV2(
        entry_event_id=decision.entry_event_id,
        attempt_id=decision.attempt_id,
        symbol=decision.symbol,
        venue=decision.venue,
        promoting_plan_sha256=decision.promoting_plan_sha256,
        bar_open_ms=decision.bar_open_ms,
        bar_close_ms=decision.bar_close_ms,
        decision_cutoff_ms=decision.decision_cutoff_ms,
        feature_evidence_sha256=decision.feature_evidence_sha256,
        feature_source_root_sha256=decision.feature_source_root_sha256,
        side=decision.side,
        action=decision.action,
        reason=decision.reason,
        reasons=decision.reasons,
        invalidation=decision.invalidation,
        interval_status=FamilyAIntervalStatusV2.INCONCLUSIVE_DATA,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _entry_status_from_readiness(
    value: FamilyAFeatureReadinessV2,
) -> FamilyAEntryStatusV2:
    if value in (
        FamilyAFeatureReadinessV2.FEATURE_NOT_READY_WARMUP,
        FamilyAFeatureReadinessV2.FEATURE_NOT_READY_ZERO_SCALE,
    ):
        return FamilyAEntryStatusV2.FEATURE_NOT_READY
    if value in (
        FamilyAFeatureReadinessV2.FEATURE_NOT_READY_SOURCE,
        FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW,
        FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
    ):
        return FamilyAEntryStatusV2.INCONCLUSIVE_DATA
    return FamilyAEntryStatusV2.DATA_INVALID


def _interval_status(
    evidence: FamilyAExitFeatureEvidenceV2,
) -> FamilyAIntervalStatusV2:
    if evidence.readiness is FamilyAFeatureReadinessV2.READY:
        return FamilyAIntervalStatusV2.COMPLETE
    return FamilyAIntervalStatusV2.INCONCLUSIVE_DATA


def _decision_from_replay_document(
    value: dict[str, object],
) -> FamilyAEntryDecisionV2 | FamilyAExitDecisionV2:
    role = value.get("role")
    try:
        if role == "ENTRY_DECISION":
            required = {
                "attempt_id",
                "bar_close_ms",
                "bar_open_ms",
                "crowd_sign",
                "crowded_long_high",
                "crowded_short_low",
                "decision_cutoff_ms",
                "event_id",
                "family",
                "feature_evidence_sha256",
                "feature_source_root_sha256",
                "invalidation",
                "payload_sha256",
                "promoting_plan_sha256",
                "reasons",
                "role",
                "rule_version",
                "side",
                "status",
                "symbol",
                "venue",
            }
            if set(value) != required or value.get("family") != "A":
                raise FamilyAContractError("entry replay fields are not exact")
            side_raw = value["side"]
            decision = FamilyAEntryDecisionV2(
                attempt_id=_json_str(value, "attempt_id"),
                symbol=_json_str(value, "symbol"),
                venue=VenueV2(_json_str(value, "venue")),
                promoting_plan_sha256=_json_str(
                    value,
                    "promoting_plan_sha256",
                ),
                bar_open_ms=_json_int(value, "bar_open_ms"),
                bar_close_ms=_json_int(value, "bar_close_ms"),
                decision_cutoff_ms=_json_int(value, "decision_cutoff_ms"),
                feature_evidence_sha256=_json_str(
                    value,
                    "feature_evidence_sha256",
                ),
                feature_source_root_sha256=_json_str(
                    value,
                    "feature_source_root_sha256",
                ),
                status=FamilyAEntryStatusV2(_json_str(value, "status")),
                side=(None if side_raw is None else FamilyASideV2(_json_str(value, "side"))),
                reasons=_json_reasons(value),
                invalidation=_json_str(value, "invalidation"),
                crowd_sign=_json_int(value, "crowd_sign"),
                crowded_long_high=_json_optional_decimal(
                    value,
                    "crowded_long_high",
                ),
                crowded_short_low=_json_optional_decimal(
                    value,
                    "crowded_short_low",
                ),
                _factory_token=_DECISION_FACTORY_TOKEN,
            )
        elif role == "EXIT_DECISION":
            required = {
                "action",
                "attempt_id",
                "bar_close_ms",
                "bar_open_ms",
                "decision_cutoff_ms",
                "entry_event_id",
                "event_id",
                "family",
                "feature_evidence_sha256",
                "feature_source_root_sha256",
                "interval_status",
                "invalidation",
                "payload_sha256",
                "promoting_plan_sha256",
                "reason",
                "reasons",
                "role",
                "rule_version",
                "side",
                "symbol",
                "venue",
            }
            if set(value) != required or value.get("family") != "A":
                raise FamilyAContractError("exit replay fields are not exact")
            decision = FamilyAExitDecisionV2(
                entry_event_id=_json_str(value, "entry_event_id"),
                attempt_id=_json_str(value, "attempt_id"),
                symbol=_json_str(value, "symbol"),
                venue=VenueV2(_json_str(value, "venue")),
                promoting_plan_sha256=_json_str(
                    value,
                    "promoting_plan_sha256",
                ),
                bar_open_ms=_json_int(value, "bar_open_ms"),
                bar_close_ms=_json_int(value, "bar_close_ms"),
                decision_cutoff_ms=_json_int(value, "decision_cutoff_ms"),
                feature_evidence_sha256=_json_str(
                    value,
                    "feature_evidence_sha256",
                ),
                feature_source_root_sha256=_json_str(
                    value,
                    "feature_source_root_sha256",
                ),
                side=FamilyASideV2(_json_str(value, "side")),
                action=FamilyAExitActionV2(_json_str(value, "action")),
                reason=FamilyAExitReasonV2(_json_str(value, "reason")),
                reasons=_json_reasons(value),
                invalidation=_json_str(value, "invalidation"),
                interval_status=FamilyAIntervalStatusV2(_json_str(value, "interval_status")),
                _factory_token=_DECISION_FACTORY_TOKEN,
            )
        else:
            raise FamilyAContractError("decision replay role is unsupported")
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, FamilyAContractError):
            raise
        raise FamilyAContractError("decision replay field value is invalid") from error
    if value.get("event_id") != decision.event_id:
        raise FamilyAContractError("decision replay event ID does not rederive")
    if value.get("payload_sha256") != decision.payload_sha256:
        raise FamilyAContractError("decision replay payload hash does not rederive")
    if value.get("rule_version") != FAMILY_A_RULE_VERSION_V2:
        raise FamilyAContractError("decision replay rule version differs")
    return decision


def _json_str(value: dict[str, object], field_name: str) -> str:
    item = value[field_name]
    if not isinstance(item, str):
        raise FamilyAContractError(f"{field_name} must be a string")
    return item


def _json_int(value: dict[str, object], field_name: str) -> int:
    item = value[field_name]
    if type(item) is not int:
        raise FamilyAContractError(f"{field_name} must be an integer")
    return item


def _json_reasons(value: dict[str, object]) -> tuple[str, ...]:
    item = value["reasons"]
    if not isinstance(item, list) or not all(isinstance(reason, str) for reason in item):
        raise FamilyAContractError("reasons must be a string list")
    return tuple(item)


def _json_optional_decimal(
    value: dict[str, object],
    field_name: str,
) -> Decimal | None:
    item = value[field_name]
    if item is None:
        return None
    if not isinstance(item, str):
        raise FamilyAContractError(f"{field_name} must be a decimal string")
    return Decimal(item)


def _canonical_decision(
    decision: FamilyAEntryDecisionV2 | FamilyAExitDecisionV2,
) -> bytes:
    if isinstance(decision, FamilyAEntryDecisionV2):
        return canonical_family_a_entry_decision_v2(decision)
    if isinstance(decision, FamilyAExitDecisionV2):
        return canonical_family_a_exit_decision_v2(decision)
    raise FamilyAContractError("registry accepts Family A decisions only")


def _entry_logical_event_id(item: FamilyAEntryInputV2) -> str:
    return _hash_document(
        _DECISION_ID_DOMAIN,
        {
            "attempt_id": item.attempt_id,
            "bar_open_ms": item.bar_open_ms,
            "family": "A",
            "promoting_plan_sha256": item.promoting_plan_sha256,
            "role": "ENTRY_DECISION",
            "rule_version": FAMILY_A_RULE_VERSION_V2,
            "symbol": item.symbol,
            "venue": item.venue.value,
        },
    )


def _exit_logical_event_id(item: FamilyAExitInputV2) -> str:
    return _hash_document(
        _EXIT_ID_DOMAIN,
        {
            "attempt_id": item.position.attempt_id,
            "bar_open_ms": item.bar_open_ms,
            "entry_event_id": item.position.entry_event_id,
            "family": "A",
            "promoting_plan_sha256": item.position.promoting_plan_sha256,
            "role": "EXIT_DECISION",
            "rule_version": FAMILY_A_RULE_VERSION_V2,
            "symbol": item.position.symbol,
            "venue": item.position.venue.value,
        },
    )


def _entry_input_sha256(item: FamilyAEntryInputV2) -> str:
    return _hash_document(
        _INPUT_DOMAIN,
        {
            "attempt_id": item.attempt_id,
            "bar_open_ms": item.bar_open_ms,
            "feature_evidence_sha256": item.feature_evidence.evidence_sha256,
            "promoting_plan_sha256": item.promoting_plan_sha256,
            "role": "ENTRY_INPUT",
            "symbol": item.symbol,
            "venue": item.venue.value,
        },
    )


def _admission_input_sha256(
    *,
    entry_event_id: str,
    paper_decision_payload_sha256: str,
    certificate_sha256: str,
    paper_registry_root_sha256: str,
    paper_registry_event_count: int,
    paper_registry_checkpoint_sha256: str,
) -> str:
    return _hash_document(
        _INPUT_DOMAIN,
        {
            "certificate_sha256": certificate_sha256,
            "entry_event_id": entry_event_id,
            "paper_decision_payload_sha256": paper_decision_payload_sha256,
            "paper_registry_checkpoint_sha256": (paper_registry_checkpoint_sha256),
            "paper_registry_event_count": paper_registry_event_count,
            "paper_registry_root_sha256": paper_registry_root_sha256,
            "role": "PAPER_FULL_FILL_ADMISSION",
        },
    )


def _exit_input_sha256(item: FamilyAExitInputV2) -> str:
    return _hash_document(
        _INPUT_DOMAIN,
        {
            "entry_event_id": item.position.entry_event_id,
            "feature_evidence_sha256": item.feature_evidence.evidence_sha256,
            "position_admission_sha256": item.position.admission_evidence_sha256,
            "role": "EXIT_INPUT",
        },
    )


def _entry_identity_document(
    decision: FamilyAEntryDecisionV2,
) -> dict[str, object]:
    return {
        "attempt_id": decision.attempt_id,
        "bar_open_ms": decision.bar_open_ms,
        "family": "A",
        "promoting_plan_sha256": decision.promoting_plan_sha256,
        "role": "ENTRY_DECISION",
        "rule_version": decision.rule_version,
        "symbol": decision.symbol,
        "venue": decision.venue.value,
    }


def _exit_identity_document(decision: FamilyAExitDecisionV2) -> dict[str, object]:
    return {
        "attempt_id": decision.attempt_id,
        "bar_open_ms": decision.bar_open_ms,
        "entry_event_id": decision.entry_event_id,
        "family": "A",
        "promoting_plan_sha256": decision.promoting_plan_sha256,
        "role": "EXIT_DECISION",
        "rule_version": decision.rule_version,
        "symbol": decision.symbol,
        "venue": decision.venue.value,
    }


def _entry_decision_document(
    decision: FamilyAEntryDecisionV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_entry_identity_document(decision),
        "bar_close_ms": decision.bar_close_ms,
        "crowd_sign": decision.crowd_sign,
        "crowded_long_high": _decimal_or_none(decision.crowded_long_high),
        "crowded_short_low": _decimal_or_none(decision.crowded_short_low),
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "event_id": decision.event_id,
        "feature_evidence_sha256": decision.feature_evidence_sha256,
        "feature_source_root_sha256": decision.feature_source_root_sha256,
        "invalidation": decision.invalidation,
        "reasons": list(decision.reasons),
        "side": None if decision.side is None else decision.side.value,
        "status": decision.status.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = decision.payload_sha256
    return document


def _exit_decision_document(
    decision: FamilyAExitDecisionV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_exit_identity_document(decision),
        "action": decision.action.value,
        "bar_close_ms": decision.bar_close_ms,
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "event_id": decision.event_id,
        "feature_evidence_sha256": decision.feature_evidence_sha256,
        "feature_source_root_sha256": decision.feature_source_root_sha256,
        "interval_status": decision.interval_status.value,
        "invalidation": decision.invalidation,
        "reason": decision.reason.value,
        "reasons": list(decision.reasons),
        "side": decision.side.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = decision.payload_sha256
    return document


def _position_document(position: FamilyAPositionV2) -> dict[str, object]:
    return {
        "admission_evidence_sha256": position.admission_evidence_sha256,
        "attempt_id": position.attempt_id,
        "crowd_sign": position.crowd_sign,
        "crowded_long_high": str(position.crowded_long_high),
        "crowded_short_low": str(position.crowded_short_low),
        "entry_event_id": position.entry_event_id,
        "feature_evidence_sha256": position.feature_evidence_sha256,
        "feature_source_root_sha256": position.feature_source_root_sha256,
        "paper_decision_event_id": position.paper_decision_event_id,
        "paper_decision_payload_sha256": position.paper_decision_payload_sha256,
        "paper_executable_vwap": str(position.paper_executable_vwap),
        "paper_filled_quantity": str(position.paper_filled_quantity),
        "paper_registry_checkpoint_sha256": (position.paper_registry_checkpoint_sha256),
        "paper_registry_event_count": position.paper_registry_event_count,
        "paper_registry_root_sha256": position.paper_registry_root_sha256,
        "paper_requested_quantity": str(position.paper_requested_quantity),
        "promoting_plan_sha256": position.promoting_plan_sha256,
        "side": position.side.value,
        "signal_bar_open_ms": position.signal_bar_open_ms,
        "symbol": position.symbol,
        "venue": position.venue.value,
    }


def _validate_entry_decision_state(decision: FamilyAEntryDecisionV2) -> None:
    if not isinstance(decision.status, FamilyAEntryStatusV2):
        raise FamilyAContractError("entry status is unsupported")
    _validate_reasons(decision.reasons)
    _validate_identity(decision.invalidation, "invalidation")
    if decision.crowd_sign not in (-1, 0, 1):
        raise FamilyAContractError("crowd_sign must be -1, 0, or 1")
    if decision.status is FamilyAEntryStatusV2.SIGNAL:
        if decision.crowd_sign not in (-1, 1) or not isinstance(decision.side, FamilyASideV2):
            raise FamilyAContractError("SIGNAL requires a side and nonzero crowd sign")
        expected_side = FamilyASideV2.SHORT if decision.crowd_sign == 1 else FamilyASideV2.LONG
        if decision.side is not expected_side:
            raise FamilyAContractError("signal side differs from crowd sign")
        if not _is_positive_finite(decision.crowded_long_high) or not _is_positive_finite(
            decision.crowded_short_low
        ):
            raise FamilyAContractError("SIGNAL requires frozen crowded references")
        assert decision.crowded_long_high is not None
        assert decision.crowded_short_low is not None
        if decision.crowded_short_low > decision.crowded_long_high:
            raise FamilyAContractError("crowded reference order is invalid")
        return
    if (
        decision.side is not None
        or decision.crowded_long_high is not None
        or decision.crowded_short_low is not None
    ):
        raise FamilyAContractError("non-signal decision cannot expose position state")
    if (
        decision.status
        in (
            FamilyAEntryStatusV2.FEATURE_NOT_READY,
            FamilyAEntryStatusV2.INCONCLUSIVE_DATA,
            FamilyAEntryStatusV2.DATA_INVALID,
        )
        and decision.crowd_sign != 0
    ):
        raise FamilyAContractError("non-ready decision crowd sign must be zero")


def _validate_exit_decision_state(decision: FamilyAExitDecisionV2) -> None:
    if not isinstance(decision.side, FamilyASideV2):
        raise FamilyAContractError("exit side is unsupported")
    if not isinstance(decision.action, FamilyAExitActionV2) or not isinstance(
        decision.reason, FamilyAExitReasonV2
    ):
        raise FamilyAContractError("exit action or reason is unsupported")
    if not isinstance(decision.interval_status, FamilyAIntervalStatusV2):
        raise FamilyAContractError("exit interval status is unsupported")
    _validate_reasons(decision.reasons)
    _validate_identity(decision.invalidation, "invalidation")
    expected_exit = (
        FamilyAExitActionV2.EXIT_LONG
        if decision.side is FamilyASideV2.LONG
        else FamilyAExitActionV2.EXIT_SHORT
    )
    if decision.reason is FamilyAExitReasonV2.HOLD:
        if decision.action is not FamilyAExitActionV2.HOLD:
            raise FamilyAContractError("HOLD reason requires HOLD action")
    elif decision.action is not expected_exit:
        raise FamilyAContractError("exit action differs from the frozen position side")


def _validate_decision_identity(
    decision: FamilyAEntryDecisionV2 | FamilyAExitDecisionV2,
) -> None:
    _validate_identity(decision.attempt_id, "attempt_id")
    _validate_symbol(decision.symbol)
    if decision.venue is not VenueV2.USDM_FUTURES:
        raise FamilyAContractError("Family A decision must retain USD-M Futures")
    _validate_sha256(decision.promoting_plan_sha256, "promoting_plan_sha256")
    _validate_bar_times(
        decision.bar_open_ms,
        decision.bar_close_ms,
        decision.decision_cutoff_ms,
    )


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _validate_reasons(value: tuple[str, ...]) -> None:
    if type(value) is not tuple or not value:
        raise FamilyAContractError("reasons must be a non-empty tuple")
    for item in value:
        _validate_identity(item, "reason")


def _validate_identity(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value or value.strip() != value or len(value) > 256:
        raise FamilyAContractError(f"{field_name} must be a bounded normalized identity")


def _validate_symbol(value: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise FamilyAContractError("symbol must be a normalized USDT symbol")


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise FamilyAContractError(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise FamilyAContractError(f"{field_name} must be a nonnegative integer")


def _validate_bar_times(
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> None:
    try:
        validate_decision_bar_v2(bar_open_ms, bar_close_ms, decision_cutoff_ms)
    except ValueError as error:
        raise FamilyAContractError(str(error)) from error


def _is_positive_finite(value: Decimal | None) -> bool:
    return type(value) is Decimal and value.is_finite() and value > 0
