"""Typed decision PREPARE/DISPOSITION payloads for the prospective WAL.

These factories accept only evaluator-sealed Family A/B/C entry decisions via
their transactional preview owners.  They do not write a WAL, evaluate PAPER
FOK, issue a causal target cursor, seal a segment, or authorize an order.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import InitVar, dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Final, cast

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveExpectedCellV2,
    canonical_prospective_census_plan_v2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    CELL_DISPOSITION_PAYLOAD_SCHEMA_V2,
    DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
    ProspectiveWalRecordKindV2,
)
from signalbot.r4b_v2.strategy.family_a import (
    FamilyAEntryCommitDispositionV2,
    FamilyAEntryCommitReceiptV2,
    FamilyAEntryDecisionV2,
    FamilyAEntryPreviewV2,
    FamilyAEntryStatusV2,
    canonical_family_a_entry_decision_v2,
    parse_canonical_family_a_entry_decision_v2,
)
from signalbot.r4b_v2.strategy.family_b import (
    FamilyBEntryCommitDispositionV2,
    FamilyBEntryCommitReceiptV2,
    FamilyBEntryDecisionV2,
    FamilyBEntryPreviewV2,
    FamilyBEntryStatusV2,
    canonical_family_b_entry_decision_v2,
    parse_canonical_family_b_entry_decision_v2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FamilyCEntryCommitDispositionV2,
    FamilyCEntryCommitReceiptV2,
    FamilyCEntryDecisionV2,
    FamilyCEntryPreviewV2,
    FamilyCEntryStatusV2,
    canonical_family_c_entry_decision_v2,
    parse_canonical_family_c_entry_decision_v2,
)

if TYPE_CHECKING:
    from signalbot.r4b_v2.execution.prospective_daily_wal_store import (
        ProspectiveDailyWalDurableBatchReceiptV2,
    )

PROSPECTIVE_DECISION_PAYLOAD_RULE_VERSION_V2: Final = "R4B_CAUSAL_V2.4.0_PROSPECTIVE_DECISION_WAL"

_PREPARE_HASH_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_DECISION_PREPARE_PAYLOAD_V2\0"
_DISPOSITION_HASH_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_CELL_DISPOSITION_PAYLOAD_V2\0"
_PREPARE_FACTORY_TOKEN: Final = object()
_DISPOSITION_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_JCS_MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991
_MAX_MONOTONIC_NS: Final = 9_223_372_036_854_775_807


class ProspectiveDecisionPayloadContractErrorV2(ValueError):
    """Raised when a typed prospective decision payload is not exact."""


class ProspectiveDispositionClassV2(StrEnum):
    """The complete high-level cell disposition vocabulary."""

    SIGNAL = "SIGNAL"
    NO_SIGNAL = "NO_SIGNAL"
    SUPPRESSED = "SUPPRESSED"
    INCONCLUSIVE = "INCONCLUSIVE"


type FamilyEntryDecisionV2 = (
    FamilyAEntryDecisionV2 | FamilyBEntryDecisionV2 | FamilyCEntryDecisionV2
)
type FamilyEntryPreviewV2 = FamilyAEntryPreviewV2 | FamilyBEntryPreviewV2 | FamilyCEntryPreviewV2
type FamilyEntryCommitReceiptV2 = (
    FamilyAEntryCommitReceiptV2 | FamilyBEntryCommitReceiptV2 | FamilyCEntryCommitReceiptV2
)


@dataclass(frozen=True, slots=True)
class ProspectiveDecisionPreparePayloadV2:
    """Exact pre-commit decision bytes durably written before state mutation."""

    attempt_id: str
    attempt_plan_sha256: str
    promoting_plan_sha256: str
    segment_id: str
    cell_id: str
    family: PromotingFamilyV2
    rule_version: str
    symbol: str
    venue: VenueV2
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    family_input_sha256: str
    family_state_root_before_sha256: str
    family_state_event_count_before: int
    decision_event_id: str
    decision_payload_sha256: str
    family_status: str
    disposition_class: ProspectiveDispositionClassV2
    signal_side: str | None
    canonical_decision_jsonl: bytes = field(repr=False)
    _factory_token: InitVar[object | None] = None
    payload_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
    )
    protocol_rule_version: str = field(
        init=False,
        default=PROSPECTIVE_DECISION_PAYLOAD_RULE_VERSION_V2,
    )
    paper_fok_evaluated: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PREPARE_FACTORY_TOKEN:
            raise ProspectiveDecisionPayloadContractErrorV2(
                "decision PREPARE payloads are factory-sealed"
            )
        _validate_prepare(self)
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(_PREPARE_HASH_DOMAIN, _prepare_document(self)),
        )

    @property
    def decision(self) -> FamilyEntryDecisionV2:
        """Return a fresh, constructor-validated family decision."""

        return _parse_family_decision(self.family, self.canonical_decision_jsonl)


@dataclass(frozen=True, slots=True)
class ProspectiveCellDispositionPayloadV2:
    """Post-commit receipt binding one durable PREPARE to exact owner state."""

    attempt_plan_sha256: str
    segment_id: str
    cell_id: str
    family: PromotingFamilyV2
    rule_version: str
    decision_cutoff_ms: int
    decision_event_id: str
    decision_payload_sha256: str
    family_status: str
    disposition_class: ProspectiveDispositionClassV2
    signal_side: str | None
    family_state_root_before_sha256: str
    family_state_root_after_sha256: str
    family_state_event_count_before: int
    family_state_event_count_after: int
    prepare_payload_sha256: str
    prepare_record_sha256: str
    decision_receipt_wall_ms: int
    decision_receipt_monotonic_ns: int
    _factory_token: InitVar[object | None] = None
    payload_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=CELL_DISPOSITION_PAYLOAD_SCHEMA_V2,
    )
    protocol_rule_version: str = field(
        init=False,
        default=PROSPECTIVE_DECISION_PAYLOAD_RULE_VERSION_V2,
    )
    decision_receipt_at_or_after_cutoff: bool = field(init=False, default=True)
    paper_fok_evaluated: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _DISPOSITION_FACTORY_TOKEN:
            raise ProspectiveDecisionPayloadContractErrorV2(
                "cell DISPOSITION payloads are factory-sealed"
            )
        _validate_disposition(self)
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(
                _DISPOSITION_HASH_DOMAIN,
                _disposition_document(self),
            ),
        )


def build_prospective_decision_prepare_payload_v2(
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    preview: FamilyEntryPreviewV2,
) -> ProspectiveDecisionPreparePayloadV2:
    """Bind one non-mutating family preview to its exact frozen census cell."""

    exact_cell = _exact_plan_cell(plan, cell)
    family, decision, input_sha256, pre_root, pre_count, committed = _preview_projection(preview)
    if committed:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "a new durable PREPARE requires an uncommitted family preview"
        )
    canonical_decision = _canonical_family_decision(family, decision)
    disposition_class, side = _classify_decision(family, decision)
    payload = ProspectiveDecisionPreparePayloadV2(
        attempt_id=exact_cell.attempt_id,
        attempt_plan_sha256=exact_cell.attempt_plan_sha256,
        promoting_plan_sha256=plan.promoting_plan_sha256,
        segment_id=exact_cell.segment_id,
        cell_id=exact_cell.cell_id,
        family=family,
        rule_version=exact_cell.rule_version,
        symbol=exact_cell.symbol,
        venue=VenueV2.USDM_FUTURES,
        bar_open_ms=exact_cell.bar_open_ms,
        bar_close_ms=exact_cell.bar_close_ms,
        decision_cutoff_ms=exact_cell.decision_cutoff_ms,
        family_input_sha256=input_sha256,
        family_state_root_before_sha256=pre_root,
        family_state_event_count_before=pre_count,
        decision_event_id=decision.event_id,
        decision_payload_sha256=decision.payload_sha256,
        family_status=decision.status.value,
        disposition_class=disposition_class,
        signal_side=side,
        canonical_decision_jsonl=canonical_decision,
        _factory_token=_PREPARE_FACTORY_TOKEN,
    )
    _validate_prepare_against_plan_cell(payload, plan, exact_cell)
    return payload


def canonical_prospective_decision_prepare_payload_v2(
    payload: ProspectiveDecisionPreparePayloadV2,
) -> bytes:
    """Serialize one self-hash-checked typed PREPARE payload."""

    if type(payload) is not ProspectiveDecisionPreparePayloadV2:
        raise TypeError("payload must be exact ProspectiveDecisionPreparePayloadV2")
    _validate_prepare(payload)
    expected = _hash_document(_PREPARE_HASH_DOMAIN, _prepare_document(payload))
    if payload.payload_sha256 != expected:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision PREPARE payload hash differs from canonical content"
        )
    return canonical_json_line(
        {**_prepare_document(payload), "payload_sha256": payload.payload_sha256}
    )


def parse_prospective_decision_prepare_payload_v2(
    encoded: bytes,
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
) -> ProspectiveDecisionPreparePayloadV2:
    """Strictly parse and rebind one canonical PREPARE to its frozen cell."""

    document = _decode_exact_object(encoded, _PREPARE_KEYS, "decision PREPARE")
    family = _parse_family(document.get("family"))
    decision_document = document.get("canonical_decision")
    if not isinstance(decision_document, dict):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision PREPARE canonical_decision must be an object"
        )
    decision_jsonl = canonical_json_line(cast(dict[str, object], decision_document))
    payload = ProspectiveDecisionPreparePayloadV2(
        attempt_id=_text(document, "attempt_id"),
        attempt_plan_sha256=_text(document, "attempt_plan_sha256"),
        promoting_plan_sha256=_text(document, "promoting_plan_sha256"),
        segment_id=_text(document, "segment_id"),
        cell_id=_text(document, "cell_id"),
        family=family,
        rule_version=_text(document, "rule_version"),
        symbol=_text(document, "symbol"),
        venue=_parse_venue(document.get("venue")),
        bar_open_ms=_integer(document, "bar_open_ms"),
        bar_close_ms=_integer(document, "bar_close_ms"),
        decision_cutoff_ms=_integer(document, "decision_cutoff_ms"),
        family_input_sha256=_text(document, "family_input_sha256"),
        family_state_root_before_sha256=_text(document, "family_state_root_before_sha256"),
        family_state_event_count_before=_integer(document, "family_state_event_count_before"),
        decision_event_id=_text(document, "decision_event_id"),
        decision_payload_sha256=_text(document, "decision_payload_sha256"),
        family_status=_text(document, "family_status"),
        disposition_class=_parse_disposition_class(document.get("disposition_class")),
        signal_side=_optional_text(document, "signal_side"),
        canonical_decision_jsonl=decision_jsonl,
        _factory_token=_PREPARE_FACTORY_TOKEN,
    )
    if document.get("payload_sha256") != payload.payload_sha256:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "stored decision PREPARE payload hash does not rederive"
        )
    _validate_prepare_against_plan_cell(payload, plan, _exact_plan_cell(plan, cell))
    if canonical_prospective_decision_prepare_payload_v2(payload) != encoded:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision PREPARE payload does not replay byte-for-byte"
        )
    return payload


def build_prospective_cell_disposition_payload_v2(
    *,
    prepare: ProspectiveDecisionPreparePayloadV2,
    prepare_record_sha256: str,
    decision_receipt: ReceiptTimestamp,
    family_state_root_after_sha256: str,
    family_state_event_count_after: int,
) -> ProspectiveCellDispositionPayloadV2:
    """Bind an exact committed state transition to the durable PREPARE record."""

    canonical_prospective_decision_prepare_payload_v2(prepare)
    if type(decision_receipt) is not ReceiptTimestamp:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision_receipt must be an exact ReceiptTimestamp"
        )
    return ProspectiveCellDispositionPayloadV2(
        attempt_plan_sha256=prepare.attempt_plan_sha256,
        segment_id=prepare.segment_id,
        cell_id=prepare.cell_id,
        family=prepare.family,
        rule_version=prepare.rule_version,
        decision_cutoff_ms=prepare.decision_cutoff_ms,
        decision_event_id=prepare.decision_event_id,
        decision_payload_sha256=prepare.decision_payload_sha256,
        family_status=prepare.family_status,
        disposition_class=prepare.disposition_class,
        signal_side=prepare.signal_side,
        family_state_root_before_sha256=(prepare.family_state_root_before_sha256),
        family_state_root_after_sha256=family_state_root_after_sha256,
        family_state_event_count_before=(prepare.family_state_event_count_before),
        family_state_event_count_after=family_state_event_count_after,
        prepare_payload_sha256=prepare.payload_sha256,
        prepare_record_sha256=prepare_record_sha256,
        decision_receipt_wall_ms=decision_receipt.received_at_ms,
        decision_receipt_monotonic_ns=(decision_receipt.received_monotonic_ns),
        _factory_token=_DISPOSITION_FACTORY_TOKEN,
    )


def build_prospective_cell_disposition_payload_from_receipts_v2(
    *,
    prepare: ProspectiveDecisionPreparePayloadV2,
    prepare_durable_receipt: ProspectiveDailyWalDurableBatchReceiptV2,
    commit_receipt: FamilyEntryCommitReceiptV2,
    decision_receipt: ReceiptTimestamp,
) -> ProspectiveCellDispositionPayloadV2:
    """Derive every post-commit scalar from exact factory-sealed receipts.

    This is the coordinator-facing factory.  It rejects a generic durable batch,
    a PREEXISTING family receipt, a receipt from another family/cell, and any
    prepare payload whose canonical bytes are not the single durably acknowledged
    record.  The lower-level scalar factory remains only for strict replay/parser
    construction until all restart reconciliation is owned by one authority.
    """

    # Local import keeps the storage layer's payload-parser dependency acyclic at
    # module import time while still enforcing the exact factory-sealed type.
    from signalbot.r4b_v2.execution.prospective_daily_wal_store import (
        ProspectiveDailyWalDurableBatchReceiptV2 as DurableReceipt,
    )

    canonical_prepare = canonical_prospective_decision_prepare_payload_v2(prepare)
    if type(prepare_durable_receipt) is not DurableReceipt:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "prepare_durable_receipt must be an exact daily-WAL durable receipt"
        )
    if (
        prepare_durable_receipt.attempt_plan_sha256 != prepare.attempt_plan_sha256
        or prepare_durable_receipt.segment_id != prepare.segment_id
        or len(prepare_durable_receipt.records) != 1
    ):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "durable receipt does not cover one exact PREPARE record"
        )
    [durable_record] = prepare_durable_receipt.records
    if (
        durable_record.kind is not ProspectiveWalRecordKindV2.DECISION_PREPARE
        or durable_record.cell_id != prepare.cell_id
        or durable_record.sizing_cell is not None
        or durable_record.payload_sha256 != hashlib.sha256(canonical_prepare).hexdigest()
    ):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "durable receipt identity differs from the canonical PREPARE"
        )

    (
        family,
        decision,
        input_sha256,
        pre_root_sha256,
        pre_event_count,
        post_root_sha256,
        post_event_count,
        preview_already_committed,
        newly_committed,
    ) = _commit_receipt_projection(commit_receipt)
    if (
        family is not prepare.family
        or decision != prepare.decision
        or input_sha256 != prepare.family_input_sha256
        or decision.event_id != prepare.decision_event_id
        or decision.payload_sha256 != prepare.decision_payload_sha256
        or pre_root_sha256 != prepare.family_state_root_before_sha256
        or pre_event_count != prepare.family_state_event_count_before
    ):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "family commit receipt differs from the durable PREPARE"
        )
    if preview_already_committed or not newly_committed:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "DISPOSITION requires a NEW commit owned by this transaction"
        )
    return build_prospective_cell_disposition_payload_v2(
        prepare=prepare,
        prepare_record_sha256=durable_record.record_sha256,
        decision_receipt=decision_receipt,
        family_state_root_after_sha256=post_root_sha256,
        family_state_event_count_after=post_event_count,
    )


def canonical_prospective_cell_disposition_payload_v2(
    payload: ProspectiveCellDispositionPayloadV2,
) -> bytes:
    """Serialize one self-hash-checked typed DISPOSITION payload."""

    if type(payload) is not ProspectiveCellDispositionPayloadV2:
        raise TypeError("payload must be exact ProspectiveCellDispositionPayloadV2")
    _validate_disposition(payload)
    expected = _hash_document(
        _DISPOSITION_HASH_DOMAIN,
        _disposition_document(payload),
    )
    if payload.payload_sha256 != expected:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "cell DISPOSITION payload hash differs from canonical content"
        )
    return canonical_json_line(
        {
            **_disposition_document(payload),
            "payload_sha256": payload.payload_sha256,
        }
    )


def parse_prospective_cell_disposition_payload_v2(
    encoded: bytes,
    *,
    prepare: ProspectiveDecisionPreparePayloadV2,
    prepare_record_sha256: str,
) -> ProspectiveCellDispositionPayloadV2:
    """Strictly parse a DISPOSITION and bind it to its exact PREPARE record."""

    canonical_prospective_decision_prepare_payload_v2(prepare)
    document = _decode_exact_object(
        encoded,
        _DISPOSITION_KEYS,
        "cell DISPOSITION",
    )
    payload = ProspectiveCellDispositionPayloadV2(
        attempt_plan_sha256=_text(document, "attempt_plan_sha256"),
        segment_id=_text(document, "segment_id"),
        cell_id=_text(document, "cell_id"),
        family=_parse_family(document.get("family")),
        rule_version=_text(document, "rule_version"),
        decision_cutoff_ms=_integer(document, "decision_cutoff_ms"),
        decision_event_id=_text(document, "decision_event_id"),
        decision_payload_sha256=_text(document, "decision_payload_sha256"),
        family_status=_text(document, "family_status"),
        disposition_class=_parse_disposition_class(document.get("disposition_class")),
        signal_side=_optional_text(document, "signal_side"),
        family_state_root_before_sha256=_text(document, "family_state_root_before_sha256"),
        family_state_root_after_sha256=_text(document, "family_state_root_after_sha256"),
        family_state_event_count_before=_integer(document, "family_state_event_count_before"),
        family_state_event_count_after=_integer(document, "family_state_event_count_after"),
        prepare_payload_sha256=_text(document, "prepare_payload_sha256"),
        prepare_record_sha256=_text(document, "prepare_record_sha256"),
        decision_receipt_wall_ms=_integer(document, "decision_receipt_wall_ms"),
        decision_receipt_monotonic_ns=_decimal_integer_string(
            document,
            "decision_receipt_monotonic_ns",
        ),
        _factory_token=_DISPOSITION_FACTORY_TOKEN,
    )
    _validate_disposition_against_prepare(
        payload,
        prepare,
        prepare_record_sha256,
    )
    if document.get("payload_sha256") != payload.payload_sha256:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "stored cell DISPOSITION payload hash does not rederive"
        )
    if canonical_prospective_cell_disposition_payload_v2(payload) != encoded:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "cell DISPOSITION payload does not replay byte-for-byte"
        )
    return payload


def _preview_projection(
    preview: FamilyEntryPreviewV2,
) -> tuple[PromotingFamilyV2, FamilyEntryDecisionV2, str, str, int, bool]:
    if type(preview) is FamilyAEntryPreviewV2:
        return (
            PromotingFamilyV2.A,
            preview.decision,
            preview.input_sha256,
            preview.pre_root_sha256,
            preview.pre_event_count,
            preview.already_committed,
        )
    if type(preview) is FamilyBEntryPreviewV2:
        return (
            PromotingFamilyV2.B,
            preview.decision,
            preview.input_sha256,
            preview.pre_replay_root_sha256,
            preview.pre_event_count,
            preview.already_committed,
        )
    if type(preview) is FamilyCEntryPreviewV2:
        return (
            PromotingFamilyV2.C,
            preview.decision,
            preview.input_sha256,
            preview.pre_root_sha256,
            preview.pre_event_count,
            preview.already_committed,
        )
    raise ProspectiveDecisionPayloadContractErrorV2(
        "preview must be an exact Family A/B/C entry preview"
    )


def _commit_receipt_projection(
    receipt: FamilyEntryCommitReceiptV2,
) -> tuple[
    PromotingFamilyV2,
    FamilyEntryDecisionV2,
    str,
    str,
    int,
    str,
    int,
    bool,
    bool,
]:
    if type(receipt) is FamilyAEntryCommitReceiptV2:
        return (
            PromotingFamilyV2.A,
            receipt.decision,
            receipt.input_sha256,
            receipt.pre_root_sha256,
            receipt.pre_event_count,
            receipt.post_root_sha256,
            receipt.post_event_count,
            receipt.preview_already_committed,
            receipt.disposition is FamilyAEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION,
        )
    if type(receipt) is FamilyBEntryCommitReceiptV2:
        return (
            PromotingFamilyV2.B,
            receipt.decision,
            receipt.input_sha256,
            receipt.pre_root_sha256,
            receipt.pre_event_count,
            receipt.post_root_sha256,
            receipt.post_event_count,
            receipt.preview_already_committed,
            receipt.disposition is FamilyBEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION,
        )
    if type(receipt) is FamilyCEntryCommitReceiptV2:
        return (
            PromotingFamilyV2.C,
            receipt.decision,
            receipt.input_sha256,
            receipt.pre_root_sha256,
            receipt.pre_event_count,
            receipt.post_root_sha256,
            receipt.post_event_count,
            receipt.preview_already_committed,
            receipt.disposition is FamilyCEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION,
        )
    raise ProspectiveDecisionPayloadContractErrorV2(
        "commit_receipt must be an exact Family A/B/C entry commit receipt"
    )


def _canonical_family_decision(
    family: PromotingFamilyV2,
    decision: FamilyEntryDecisionV2,
) -> bytes:
    if family is PromotingFamilyV2.A and type(decision) is FamilyAEntryDecisionV2:
        return canonical_family_a_entry_decision_v2(decision)
    if family is PromotingFamilyV2.B and type(decision) is FamilyBEntryDecisionV2:
        return canonical_family_b_entry_decision_v2(decision)
    if family is PromotingFamilyV2.C and type(decision) is FamilyCEntryDecisionV2:
        return canonical_family_c_entry_decision_v2(decision)
    raise ProspectiveDecisionPayloadContractErrorV2(
        "decision type differs from its promoting family"
    )


def _parse_family_decision(
    family: PromotingFamilyV2,
    encoded: bytes,
) -> FamilyEntryDecisionV2:
    try:
        if family is PromotingFamilyV2.A:
            return parse_canonical_family_a_entry_decision_v2(encoded)
        if family is PromotingFamilyV2.B:
            return parse_canonical_family_b_entry_decision_v2(encoded)
        return parse_canonical_family_c_entry_decision_v2(encoded)
    except (TypeError, ValueError) as error:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "canonical family decision fails constructor-equivalent replay"
        ) from error


def _classify_decision(
    family: PromotingFamilyV2,
    decision: FamilyEntryDecisionV2,
) -> tuple[ProspectiveDispositionClassV2, str | None]:
    _canonical_family_decision(family, decision)
    if family is PromotingFamilyV2.A and type(decision) is FamilyAEntryDecisionV2:
        status = decision.status
        if status is FamilyAEntryStatusV2.SIGNAL:
            return _signal_disposition(decision.side)
        if status is FamilyAEntryStatusV2.NO_SIGNAL:
            return ProspectiveDispositionClassV2.NO_SIGNAL, None
        if status is FamilyAEntryStatusV2.NOT_ADMITTED_ACTIVE_POSITION:
            return ProspectiveDispositionClassV2.SUPPRESSED, None
        if status in (
            FamilyAEntryStatusV2.FEATURE_NOT_READY,
            FamilyAEntryStatusV2.INCONCLUSIVE_DATA,
            FamilyAEntryStatusV2.DATA_INVALID,
        ):
            return ProspectiveDispositionClassV2.INCONCLUSIVE, None
    elif family is PromotingFamilyV2.B and type(decision) is FamilyBEntryDecisionV2:
        status = decision.status
        if status is FamilyBEntryStatusV2.SIGNAL:
            return _signal_disposition(decision.side)
        if status is FamilyBEntryStatusV2.NO_SIGNAL:
            return ProspectiveDispositionClassV2.NO_SIGNAL, None
        if status is FamilyBEntryStatusV2.NOT_ADMITTED_ACTIVE_POSITION:
            return ProspectiveDispositionClassV2.SUPPRESSED, None
        if status in (
            FamilyBEntryStatusV2.FEATURE_NOT_READY,
            FamilyBEntryStatusV2.INCONCLUSIVE_DATA,
            FamilyBEntryStatusV2.DATA_INVALID,
            FamilyBEntryStatusV2.DATA_INVALID_RULE_INVARIANT,
        ):
            return ProspectiveDispositionClassV2.INCONCLUSIVE, None
    elif family is PromotingFamilyV2.C and type(decision) is FamilyCEntryDecisionV2:
        status = decision.status
        if status is FamilyCEntryStatusV2.SIGNAL:
            return _signal_disposition(decision.side)
        if status is FamilyCEntryStatusV2.NO_SIGNAL:
            return ProspectiveDispositionClassV2.NO_SIGNAL, None
        if status is FamilyCEntryStatusV2.NOT_ADMITTED_ACTIVE_POSITION:
            return ProspectiveDispositionClassV2.SUPPRESSED, None
        if status in (
            FamilyCEntryStatusV2.FEATURE_NOT_READY_HISTORY,
            FamilyCEntryStatusV2.FEATURE_NOT_READY_ZERO_MARKET_VARIANCE,
            FamilyCEntryStatusV2.FEATURE_NOT_READY_ZERO_SCALE,
            FamilyCEntryStatusV2.INCONCLUSIVE_CROSS_SECTION,
            FamilyCEntryStatusV2.INCONCLUSIVE_DATA,
            FamilyCEntryStatusV2.DATA_INVALID,
        ):
            return ProspectiveDispositionClassV2.INCONCLUSIVE, None
    raise ProspectiveDecisionPayloadContractErrorV2(
        "family entry status has no frozen disposition mapping"
    )


def _signal_disposition(side: object) -> tuple[ProspectiveDispositionClassV2, str]:
    value = getattr(side, "value", None)
    if value not in {"LONG", "SHORT"}:
        raise ProspectiveDecisionPayloadContractErrorV2("SIGNAL decision requires a concrete side")
    return ProspectiveDispositionClassV2.SIGNAL, value


def _validate_prepare(payload: ProspectiveDecisionPreparePayloadV2) -> None:
    for value, name in (
        (payload.attempt_plan_sha256, "attempt_plan_sha256"),
        (payload.promoting_plan_sha256, "promoting_plan_sha256"),
        (payload.segment_id, "segment_id"),
        (payload.cell_id, "cell_id"),
        (payload.family_input_sha256, "family_input_sha256"),
        (
            payload.family_state_root_before_sha256,
            "family_state_root_before_sha256",
        ),
        (payload.decision_event_id, "decision_event_id"),
        (payload.decision_payload_sha256, "decision_payload_sha256"),
    ):
        _require_sha256(value, name)
    _require_identity(payload.attempt_id, "attempt_id")
    _require_identity(payload.rule_version, "rule_version")
    _require_symbol(payload.symbol)
    if not isinstance(payload.family, PromotingFamilyV2):
        raise ProspectiveDecisionPayloadContractErrorV2("family must be PromotingFamilyV2")
    if payload.venue is not VenueV2.USDM_FUTURES:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "prospective decisions require USD-M Futures"
        )
    for value, name in (
        (payload.bar_open_ms, "bar_open_ms"),
        (payload.bar_close_ms, "bar_close_ms"),
        (payload.decision_cutoff_ms, "decision_cutoff_ms"),
        (
            payload.family_state_event_count_before,
            "family_state_event_count_before",
        ),
    ):
        _require_nonnegative_safe_integer(value, name)
    if type(payload.canonical_decision_jsonl) is not bytes:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "canonical_decision_jsonl must be immutable bytes"
        )
    decision = _parse_family_decision(payload.family, payload.canonical_decision_jsonl)
    disposition, side = _classify_decision(payload.family, decision)
    identity = (
        decision.attempt_id,
        decision.promoting_plan_sha256,
        decision.symbol,
        decision.venue,
        decision.bar_open_ms,
        decision.bar_close_ms,
        decision.decision_cutoff_ms,
        decision.rule_version,
        decision.event_id,
        decision.payload_sha256,
        decision.status.value,
    )
    expected = (
        payload.attempt_id,
        payload.promoting_plan_sha256,
        payload.symbol,
        payload.venue,
        payload.bar_open_ms,
        payload.bar_close_ms,
        payload.decision_cutoff_ms,
        payload.rule_version,
        payload.decision_event_id,
        payload.decision_payload_sha256,
        payload.family_status,
    )
    if identity != expected:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "family decision identity differs from PREPARE"
        )
    if (payload.disposition_class, payload.signal_side) != (disposition, side):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "PREPARE disposition differs from the exact family status"
        )
    if (
        payload.family is PromotingFamilyV2.C
        and cast(FamilyCEntryDecisionV2, decision).episode_ledger_root_sha256
        != payload.family_state_root_before_sha256
    ):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "Family C decision does not bind the PREPARE pre-state root"
        )
    if payload.schema_version != DECISION_PREPARE_PAYLOAD_SCHEMA_V2:
        raise ProspectiveDecisionPayloadContractErrorV2("unsupported decision PREPARE schema")
    if payload.protocol_rule_version != PROSPECTIVE_DECISION_PAYLOAD_RULE_VERSION_V2:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision PREPARE protocol rule version differs"
        )
    if payload.paper_fok_evaluated or payload.production_order_placement:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision PREPARE cannot claim execution or order placement"
        )


def _validate_prepare_against_plan_cell(
    payload: ProspectiveDecisionPreparePayloadV2,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
) -> None:
    canonical_prospective_census_plan_v2(plan)
    exact = _exact_plan_cell(plan, cell)
    if (
        payload.attempt_id,
        payload.attempt_plan_sha256,
        payload.promoting_plan_sha256,
        payload.segment_id,
        payload.cell_id,
        payload.family,
        payload.rule_version,
        payload.symbol,
        payload.bar_open_ms,
        payload.bar_close_ms,
        payload.decision_cutoff_ms,
    ) != (
        exact.attempt_id,
        exact.attempt_plan_sha256,
        plan.promoting_plan_sha256,
        exact.segment_id,
        exact.cell_id,
        exact.family,
        exact.rule_version,
        exact.symbol,
        exact.bar_open_ms,
        exact.bar_close_ms,
        exact.decision_cutoff_ms,
    ):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision PREPARE differs from its exact frozen census cell"
        )


def _validate_disposition(payload: ProspectiveCellDispositionPayloadV2) -> None:
    for value, name in (
        (payload.attempt_plan_sha256, "attempt_plan_sha256"),
        (payload.segment_id, "segment_id"),
        (payload.cell_id, "cell_id"),
        (payload.decision_event_id, "decision_event_id"),
        (payload.decision_payload_sha256, "decision_payload_sha256"),
        (
            payload.family_state_root_before_sha256,
            "family_state_root_before_sha256",
        ),
        (
            payload.family_state_root_after_sha256,
            "family_state_root_after_sha256",
        ),
        (payload.prepare_payload_sha256, "prepare_payload_sha256"),
        (payload.prepare_record_sha256, "prepare_record_sha256"),
    ):
        _require_sha256(value, name)
    if not isinstance(payload.family, PromotingFamilyV2):
        raise ProspectiveDecisionPayloadContractErrorV2("family must be PromotingFamilyV2")
    _require_identity(payload.rule_version, "rule_version")
    _require_identity(payload.family_status, "family_status")
    if not isinstance(payload.disposition_class, ProspectiveDispositionClassV2):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "disposition_class must be ProspectiveDispositionClassV2"
        )
    if payload.disposition_class is ProspectiveDispositionClassV2.SIGNAL:
        if payload.signal_side not in {"LONG", "SHORT"}:
            raise ProspectiveDecisionPayloadContractErrorV2(
                "SIGNAL disposition requires LONG or SHORT"
            )
    elif payload.signal_side is not None:
        raise ProspectiveDecisionPayloadContractErrorV2("non-signal disposition forbids a side")
    for value, name in (
        (payload.decision_cutoff_ms, "decision_cutoff_ms"),
        (
            payload.family_state_event_count_before,
            "family_state_event_count_before",
        ),
        (
            payload.family_state_event_count_after,
            "family_state_event_count_after",
        ),
        (payload.decision_receipt_wall_ms, "decision_receipt_wall_ms"),
    ):
        _require_nonnegative_safe_integer(value, name)
    if (
        type(payload.decision_receipt_monotonic_ns) is not int
        or payload.decision_receipt_monotonic_ns < 0
        or payload.decision_receipt_monotonic_ns > _MAX_MONOTONIC_NS
    ):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision_receipt_monotonic_ns must fit signed 64-bit decimal text"
        )
    if payload.family_state_event_count_after != (payload.family_state_event_count_before + 1):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "family entry commit must advance the event count by exactly one"
        )
    if payload.family_state_root_after_sha256 == payload.family_state_root_before_sha256:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "family entry commit must change the owner root"
        )
    if payload.decision_receipt_wall_ms < payload.decision_cutoff_ms:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision receipt cannot precede the closed-bar cutoff"
        )
    if not payload.decision_receipt_at_or_after_cutoff:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "decision receipt cutoff assertion must remain true"
        )
    if payload.schema_version != CELL_DISPOSITION_PAYLOAD_SCHEMA_V2:
        raise ProspectiveDecisionPayloadContractErrorV2("unsupported cell DISPOSITION schema")
    if payload.protocol_rule_version != PROSPECTIVE_DECISION_PAYLOAD_RULE_VERSION_V2:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "cell DISPOSITION protocol rule version differs"
        )
    if payload.paper_fok_evaluated or payload.production_order_placement:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "cell DISPOSITION cannot claim PAPER evaluation or order placement"
        )


def _validate_disposition_against_prepare(
    payload: ProspectiveCellDispositionPayloadV2,
    prepare: ProspectiveDecisionPreparePayloadV2,
    prepare_record_sha256: str,
) -> None:
    _require_sha256(prepare_record_sha256, "prepare_record_sha256")
    if (
        payload.attempt_plan_sha256,
        payload.segment_id,
        payload.cell_id,
        payload.family,
        payload.rule_version,
        payload.decision_cutoff_ms,
        payload.decision_event_id,
        payload.decision_payload_sha256,
        payload.family_status,
        payload.disposition_class,
        payload.signal_side,
        payload.family_state_root_before_sha256,
        payload.family_state_event_count_before,
        payload.prepare_payload_sha256,
        payload.prepare_record_sha256,
    ) != (
        prepare.attempt_plan_sha256,
        prepare.segment_id,
        prepare.cell_id,
        prepare.family,
        prepare.rule_version,
        prepare.decision_cutoff_ms,
        prepare.decision_event_id,
        prepare.decision_payload_sha256,
        prepare.family_status,
        prepare.disposition_class,
        prepare.signal_side,
        prepare.family_state_root_before_sha256,
        prepare.family_state_event_count_before,
        prepare.payload_sha256,
        prepare_record_sha256,
    ):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "cell DISPOSITION differs from its exact durable PREPARE"
        )


def _exact_plan_cell(
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
) -> ProspectiveExpectedCellV2:
    canonical_prospective_census_plan_v2(plan)
    if type(cell) is not ProspectiveExpectedCellV2:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "cell must be exact ProspectiveExpectedCellV2"
        )
    if cell.attempt_plan_sha256 != plan.plan_sha256:
        raise ProspectiveDecisionPayloadContractErrorV2("cell targets a foreign prospective plan")
    try:
        exact = plan.expected_cell(
            family=cell.family,
            symbol=cell.symbol,
            bar_open_ms=cell.bar_open_ms,
        )
    except ValueError as error:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "cell is outside the frozen prospective census"
        ) from error
    if exact != cell:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "cell differs from its exact frozen census identity"
        )
    return exact


def _prepare_document(
    payload: ProspectiveDecisionPreparePayloadV2,
) -> dict[str, object]:
    decision = json.loads(payload.canonical_decision_jsonl)
    if not isinstance(decision, dict):
        raise ProspectiveDecisionPayloadContractErrorV2(
            "canonical family decision must be an object"
        )
    return {
        "attempt_id": payload.attempt_id,
        "attempt_plan_sha256": payload.attempt_plan_sha256,
        "bar_close_ms": payload.bar_close_ms,
        "bar_open_ms": payload.bar_open_ms,
        "canonical_decision": decision,
        "cell_id": payload.cell_id,
        "decision_cutoff_ms": payload.decision_cutoff_ms,
        "decision_event_id": payload.decision_event_id,
        "decision_payload_sha256": payload.decision_payload_sha256,
        "disposition_class": payload.disposition_class.value,
        "family": payload.family.value,
        "family_input_sha256": payload.family_input_sha256,
        "family_state_event_count_before": (payload.family_state_event_count_before),
        "family_state_root_before_sha256": (payload.family_state_root_before_sha256),
        "family_status": payload.family_status,
        "paper_fok_evaluated": payload.paper_fok_evaluated,
        "production_order_placement": payload.production_order_placement,
        "promoting_plan_sha256": payload.promoting_plan_sha256,
        "protocol_rule_version": payload.protocol_rule_version,
        "rule_version": payload.rule_version,
        "schema_version": payload.schema_version,
        "segment_id": payload.segment_id,
        "signal_side": payload.signal_side,
        "symbol": payload.symbol,
        "venue": payload.venue.value,
    }


def _disposition_document(
    payload: ProspectiveCellDispositionPayloadV2,
) -> dict[str, object]:
    return {
        "attempt_plan_sha256": payload.attempt_plan_sha256,
        "cell_id": payload.cell_id,
        "decision_cutoff_ms": payload.decision_cutoff_ms,
        "decision_event_id": payload.decision_event_id,
        "decision_payload_sha256": payload.decision_payload_sha256,
        "decision_receipt_at_or_after_cutoff": (payload.decision_receipt_at_or_after_cutoff),
        "decision_receipt_monotonic_ns": str(payload.decision_receipt_monotonic_ns),
        "decision_receipt_wall_ms": payload.decision_receipt_wall_ms,
        "disposition_class": payload.disposition_class.value,
        "family": payload.family.value,
        "family_state_event_count_after": (payload.family_state_event_count_after),
        "family_state_event_count_before": (payload.family_state_event_count_before),
        "family_state_root_after_sha256": (payload.family_state_root_after_sha256),
        "family_state_root_before_sha256": (payload.family_state_root_before_sha256),
        "family_status": payload.family_status,
        "paper_fok_evaluated": payload.paper_fok_evaluated,
        "prepare_payload_sha256": payload.prepare_payload_sha256,
        "prepare_record_sha256": payload.prepare_record_sha256,
        "production_order_placement": payload.production_order_placement,
        "protocol_rule_version": payload.protocol_rule_version,
        "rule_version": payload.rule_version,
        "schema_version": payload.schema_version,
        "segment_id": payload.segment_id,
        "signal_side": payload.signal_side,
    }


_PREPARE_KEYS: Final = frozenset(
    {
        "attempt_id",
        "attempt_plan_sha256",
        "bar_close_ms",
        "bar_open_ms",
        "canonical_decision",
        "cell_id",
        "decision_cutoff_ms",
        "decision_event_id",
        "decision_payload_sha256",
        "disposition_class",
        "family",
        "family_input_sha256",
        "family_state_event_count_before",
        "family_state_root_before_sha256",
        "family_status",
        "paper_fok_evaluated",
        "payload_sha256",
        "production_order_placement",
        "promoting_plan_sha256",
        "protocol_rule_version",
        "rule_version",
        "schema_version",
        "segment_id",
        "signal_side",
        "symbol",
        "venue",
    }
)
_DISPOSITION_KEYS: Final = frozenset(
    {
        "attempt_plan_sha256",
        "cell_id",
        "decision_cutoff_ms",
        "decision_event_id",
        "decision_payload_sha256",
        "decision_receipt_at_or_after_cutoff",
        "decision_receipt_monotonic_ns",
        "decision_receipt_wall_ms",
        "disposition_class",
        "family",
        "family_state_event_count_after",
        "family_state_event_count_before",
        "family_state_root_after_sha256",
        "family_state_root_before_sha256",
        "family_status",
        "paper_fok_evaluated",
        "payload_sha256",
        "prepare_payload_sha256",
        "prepare_record_sha256",
        "production_order_placement",
        "protocol_rule_version",
        "rule_version",
        "schema_version",
        "segment_id",
        "signal_side",
    }
)


def _decode_exact_object(
    encoded: bytes,
    expected_keys: frozenset[str],
    label: str,
) -> dict[str, object]:
    if type(encoded) is not bytes or not encoded:
        raise ProspectiveDecisionPayloadContractErrorV2(
            f"{label} must be non-empty immutable bytes"
        )
    try:
        decoded: object = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProspectiveDecisionPayloadContractErrorV2(f"{label} is invalid UTF-8 JSON") from error
    if (
        not isinstance(decoded, dict)
        or frozenset(decoded) != expected_keys
        or canonical_json_line(decoded) != encoded
    ):
        raise ProspectiveDecisionPayloadContractErrorV2(
            f"{label} schema or canonical JSONL differs"
        )
    return cast(dict[str, object], decoded)


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _parse_family(value: object) -> PromotingFamilyV2:
    if not isinstance(value, str):
        raise ProspectiveDecisionPayloadContractErrorV2("family must be text")
    try:
        return PromotingFamilyV2(value)
    except ValueError as error:
        raise ProspectiveDecisionPayloadContractErrorV2("family is unsupported") from error


def _parse_disposition_class(value: object) -> ProspectiveDispositionClassV2:
    if not isinstance(value, str):
        raise ProspectiveDecisionPayloadContractErrorV2("disposition_class must be text")
    try:
        return ProspectiveDispositionClassV2(value)
    except ValueError as error:
        raise ProspectiveDecisionPayloadContractErrorV2(
            "disposition_class is unsupported"
        ) from error


def _parse_venue(value: object) -> VenueV2:
    if not isinstance(value, str):
        raise ProspectiveDecisionPayloadContractErrorV2("venue must be text")
    try:
        return VenueV2(value)
    except ValueError as error:
        raise ProspectiveDecisionPayloadContractErrorV2("venue is unsupported") from error


def _text(document: dict[str, object], field_name: str) -> str:
    value = document.get(field_name)
    if not isinstance(value, str):
        raise ProspectiveDecisionPayloadContractErrorV2(f"{field_name} must be text")
    return value


def _optional_text(
    document: dict[str, object],
    field_name: str,
) -> str | None:
    value = document.get(field_name)
    if value is not None and not isinstance(value, str):
        raise ProspectiveDecisionPayloadContractErrorV2(f"{field_name} must be null or text")
    return value


def _integer(document: dict[str, object], field_name: str) -> int:
    value = document.get(field_name)
    if type(value) is not int:
        raise ProspectiveDecisionPayloadContractErrorV2(f"{field_name} must be an integer")
    return value


def _decimal_integer_string(
    document: dict[str, object],
    field_name: str,
) -> int:
    value = document.get(field_name)
    if (
        not isinstance(value, str)
        or not value
        or (value != "0" and value.startswith("0"))
        or not value.isascii()
        or not value.isdecimal()
    ):
        raise ProspectiveDecisionPayloadContractErrorV2(
            f"{field_name} must be canonical nonnegative decimal text"
        )
    return int(value)


def _require_sha256(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ProspectiveDecisionPayloadContractErrorV2(
            f"{field_name} must be lowercase SHA-256 hex"
        )


def _require_identity(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ProspectiveDecisionPayloadContractErrorV2(
            f"{field_name} must be non-empty bounded text"
        )


def _require_symbol(value: object) -> None:
    if (
        not isinstance(value, str)
        or not value.endswith("USDT")
        or not value.isascii()
        or not value.isalnum()
    ):
        raise ProspectiveDecisionPayloadContractErrorV2("symbol must be normalized USDT text")


def _require_nonnegative_safe_integer(value: object, field_name: str) -> None:
    if type(value) is not int or value < 0 or value > _JCS_MAX_SAFE_INTEGER:
        raise ProspectiveDecisionPayloadContractErrorV2(
            f"{field_name} must be a nonnegative RFC8785-safe integer"
        )
