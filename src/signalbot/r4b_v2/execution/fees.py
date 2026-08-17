from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import zlib
from collections.abc import Sequence
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2

if TYPE_CHECKING:
    from signalbot.r4b_v2.execution.mandatory_exit import (
        MandatoryExitFeeCertificateV2,
    )

FEE_RULE_VERSION_V2: Final = "R4B_CAUSAL_V2.2.0_PUBLIC_FEE_VERSION_V2"
FEE_POLL_CADENCE_MS_V2: Final = 900_000
SPOT_PUBLIC_TAKER_RATE_V2: Final = Decimal("0.001000")
USDM_PUBLIC_TAKER_RATE_V2: Final = Decimal("0.000500")
SPOT_OFFICIAL_FEE_URL_V2: Final = "https://www.binance.com/en/fee/trading"
USDM_OFFICIAL_FEE_URL_V2: Final = "https://www.binance.com/en/fee/futureFee"
PUBLIC_FEE_SCENARIO_V2: Final = "PUBLIC_REGULAR_USER_VIP0_TAKER_NO_BNB_BASELINE"

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]{2,30}$")
_PERCENT_PAIR_RE: Final = re.compile(
    r"(?<![\d.])(\d{1,3}\.\d{4})\s*%\s*/\s*(\d{1,3}\.\d{4})\s*%(?![\d.])"
)
_MAX_IDENTITY_LENGTH: Final = 256
_MAX_PAGE_BYTES: Final = 16 * 1024 * 1024
_MAX_PNG_BYTES: Final = 32 * 1024 * 1024
_PNG_SIGNATURE: Final = b"\x89PNG\r\n\x1a\n"

_CAPTURE_ID_DOMAIN: Final = b"R4B_PUBLIC_FEE_CAPTURE_ID_V2\0"
_CAPTURE_PAYLOAD_DOMAIN: Final = b"R4B_PUBLIC_FEE_CAPTURE_PAYLOAD_V2\0"
_CAPTURE_LEDGER_LEAF_DOMAIN: Final = b"R4B_PUBLIC_FEE_CAPTURE_LEDGER_LEAF_V2\0"
_CAPTURE_LEDGER_NODE_DOMAIN: Final = b"R4B_PUBLIC_FEE_CAPTURE_LEDGER_NODE_V2\0"
_CAPTURE_LEDGER_CHECKPOINT_DOMAIN: Final = (
    b"R4B_PUBLIC_FEE_CAPTURE_LEDGER_CHECKPOINT_V2\0"
)
_ARCHIVE_ROOT_DOMAIN: Final = b"R4B_PUBLIC_FEE_ARCHIVE_ROOT_V2\0"
_PARSED_VERSION_DOMAIN: Final = b"R4B_PUBLIC_FEE_PARSED_VERSION_V2\0"
_MANIFEST_DOMAIN: Final = b"R4B_PUBLIC_FEE_MANIFEST_V2\0"
_POLL_AUDIT_DOMAIN: Final = b"R4B_PUBLIC_FEE_POLL_AUDIT_V2\0"
_REGISTRY_REPLAY_DOMAIN: Final = b"R4B_PUBLIC_FEE_REGISTRY_REPLAY_V2\0"
_REGISTRY_CHECKPOINT_DOMAIN: Final = b"R4B_PUBLIC_FEE_REGISTRY_CHECKPOINT_V2\0"
_TIMELINE_ROOT_DOMAIN: Final = b"R4B_PUBLIC_FEE_TIMELINE_ROOT_V2\0"
_TIMELINE_CHECKPOINT_DOMAIN: Final = b"R4B_PUBLIC_FEE_TIMELINE_CHECKPOINT_V2\0"
_RESOLUTION_ID_DOMAIN: Final = b"R4B_PUBLIC_FEE_RESOLUTION_ID_V2\0"
_RESOLUTION_PAYLOAD_DOMAIN: Final = b"R4B_PUBLIC_FEE_RESOLUTION_PAYLOAD_V2\0"
_COST_PAYLOAD_DOMAIN: Final = b"R4B_PUBLIC_FEE_COST_PAYLOAD_V2\0"
_SLICE_COST_PAYLOAD_DOMAIN: Final = b"R4B_PUBLIC_FEE_SLICE_COST_PAYLOAD_V2\0"
_POSITION_COST_PAYLOAD_DOMAIN: Final = b"R4B_PUBLIC_FEE_POSITION_COST_PAYLOAD_V2\0"

_CAPTURE_SCHEMA: Final = "r4b_public_fee_capture_v2"
_TRANSPORT_SCHEMA: Final = "r4b_public_fee_transport_v2"
_CAPTURE_LEDGER_CHECKPOINT_SCHEMA: Final = (
    "r4b_public_fee_capture_ledger_checkpoint_v2"
)
_PARSED_SCHEMA: Final = "r4b_public_fee_parsed_page_v2"
_ARCHIVE_SCHEMA: Final = "r4b_public_fee_capture_archive_v2"
_ARCHIVE_ROOT_SCHEMA: Final = "r4b_public_fee_archive_root_v2"
_MANIFEST_SCHEMA: Final = "r4b_public_fee_manifest_v2"
_POLL_AUDIT_SCHEMA: Final = "r4b_public_fee_poll_audit_v2"
_REGISTRY_STATE_SCHEMA: Final = "r4b_public_fee_registry_state_v2"
_TIMELINE_SCHEMA: Final = "r4b_public_fee_timeline_checkpoint_v2"
_RESOLUTION_SCHEMA: Final = "r4b_public_fee_resolution_v2"
_COST_SCHEMA: Final = "r4b_public_fee_cost_v2"
_SLICE_COST_SCHEMA: Final = "r4b_public_fee_slice_cost_v2"
_POSITION_COST_SCHEMA: Final = "r4b_public_fee_position_cost_v2"

_CAPTURE_FACTORY_TOKEN: Final = object()
_RESOLUTION_FACTORY_TOKEN: Final = object()
_FEE_COST_FACTORY_TOKEN: Final = object()
_FEE_SLICE_COST_FACTORY_TOKEN: Final = object()
_POSITION_FEE_COST_FACTORY_TOKEN: Final = object()


class FeeContractErrorV2(ValueError):
    """Raised when public-fee evidence or arithmetic violates the V2 contract."""


class FeeCaptureArtifactKindV2(StrEnum):
    RAW_RESPONSE = "RAW_RESPONSE"
    RENDERED_DOM = "RENDERED_DOM"


class FeeCaptureRoleV2(StrEnum):
    PRE_T0 = "PRE_T0"
    POST_T0_POLL = "POST_T0_POLL"


class FeeUseV2(StrEnum):
    DIAGNOSTIC_SPOT = "DIAGNOSTIC_SPOT"
    PROMOTING_USDM_FUTURES = "PROMOTING_USDM_FUTURES"


class FeeMultiplierV2(StrEnum):
    PRIMARY_1_0X = "1.0"
    PRIMARY_1_5X = "1.5"
    MANDATORY_ADVERSE_2_0X = "2.0"

    @property
    def decimal(self) -> Decimal:
        return Decimal(self.value)

    @property
    def primary(self) -> bool:
        return self is not FeeMultiplierV2.MANDATORY_ADVERSE_2_0X


class FeeSliceKindV2(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


class FilledPositionFeeStatusV2(StrEnum):
    ENTRY_ONLY = "ENTRY_ONLY"
    PARTIAL_EXIT = "PARTIAL_EXIT"
    BOTH_LEGS_COMPLETE = "BOTH_LEGS_COMPLETE"
    INCOMPLETE_FEE_VERSION = "INCOMPLETE_FEE_VERSION"


class FeeResolutionStatusV2(StrEnum):
    RESOLVED = "RESOLVED"
    TARGET_AFTER_OBSERVED_EVIDENCE = "TARGET_AFTER_OBSERVED_EVIDENCE"
    PENDING_OPEN_TAIL = "PENDING_OPEN_TAIL"
    INCONCLUSIVE_FEE_VERSION = "INCONCLUSIVE_FEE_VERSION"
    INCONCLUSIVE_FEE_POLL_GAP = "INCONCLUSIVE_FEE_POLL_GAP"


class FeeRegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


@dataclass(frozen=True, slots=True)
class FeeProtocolScopeV2:
    attempt_id: str
    plan_id: str
    protocol_hash: str
    universe_sha256: str

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_identity(self.plan_id, "plan_id")
        _validate_sha256(self.protocol_hash, "protocol_hash")
        _validate_sha256(self.universe_sha256, "universe_sha256")


@dataclass(frozen=True, slots=True)
class FeeHttpCaptureEnvelopeV2:
    """Anonymous official-page receipt retained by an external capture ledger."""

    scope: FeeProtocolScopeV2
    venue: VenueV2
    request_id: str
    request_url: str
    final_url: str
    request_started_ms: int
    response_completion_ms: int
    receipt_monotonic_ns: int
    ingest_seq: int
    http_status: int
    content_type: str
    tls_verified: bool
    account_authenticated: bool
    authorization_header_present: bool
    raw_or_dom_kind: FeeCaptureArtifactKindV2
    raw_or_dom_bytes: bytes = field(repr=False)
    raw_or_dom_sha256: str = field(init=False)
    raw_or_dom_len: int = field(init=False)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, FeeProtocolScopeV2):
            raise FeeContractErrorV2("scope must be FeeProtocolScopeV2")
        _validate_venue(self.venue)
        _validate_identity(self.request_id, "request_id")
        official_url = _official_url(self.venue)
        if self.request_url != official_url or self.final_url != official_url:
            raise FeeContractErrorV2(
                "request and final URL must equal the exact official Binance fee URL"
            )
        _validate_nonnegative_int(self.request_started_ms, "request_started_ms")
        _validate_nonnegative_int(
            self.response_completion_ms,
            "response_completion_ms",
        )
        if self.response_completion_ms < self.request_started_ms:
            raise FeeContractErrorV2("response completed before its request started")
        _validate_nonnegative_int(self.receipt_monotonic_ns, "receipt_monotonic_ns")
        _validate_positive_int(self.ingest_seq, "ingest_seq")
        if type(self.http_status) is not int or self.http_status != 200:
            raise FeeContractErrorV2("official fee capture requires HTTP status 200")
        if (
            not isinstance(self.content_type, str)
            or not self.content_type.lower().startswith("text/html")
        ):
            raise FeeContractErrorV2("official fee capture requires text/html")
        if self.tls_verified is not True:
            raise FeeContractErrorV2("official fee capture requires verified TLS")
        if self.account_authenticated is not False:
            raise FeeContractErrorV2("public fee evidence must be unauthenticated")
        if self.authorization_header_present is not False:
            raise FeeContractErrorV2("public fee request cannot carry authorization")
        if not isinstance(self.raw_or_dom_kind, FeeCaptureArtifactKindV2):
            raise FeeContractErrorV2(
                "raw_or_dom_kind must be FeeCaptureArtifactKindV2"
            )
        raw_hash = _sha256_bytes(
            self.raw_or_dom_bytes,
            "raw_or_dom_bytes",
            maximum_size=_MAX_PAGE_BYTES,
        )
        object.__setattr__(self, "raw_or_dom_sha256", raw_hash)
        object.__setattr__(self, "raw_or_dom_len", len(self.raw_or_dom_bytes))


@dataclass(frozen=True, slots=True)
class FeeCaptureLedgerCheckpointV2:
    """External append-only capture-ledger checkpoint, checked by expected hash."""

    scope: FeeProtocolScopeV2
    ledger_id: str
    ledger_root_sha256: str
    event_count: int
    observed_through_ms: int
    sealed_at_ms: int
    checkpoint_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, FeeProtocolScopeV2):
            raise FeeContractErrorV2("checkpoint scope must be FeeProtocolScopeV2")
        _validate_identity(self.ledger_id, "ledger_id")
        _validate_sha256(self.ledger_root_sha256, "ledger_root_sha256")
        _validate_positive_int(self.event_count, "event_count")
        _validate_nonnegative_int(self.observed_through_ms, "observed_through_ms")
        _validate_nonnegative_int(self.sealed_at_ms, "sealed_at_ms")
        if self.sealed_at_ms < self.observed_through_ms:
            raise FeeContractErrorV2("ledger seal predates its observed-through clock")
        checkpoint_sha256 = hashlib.sha256(
            _CAPTURE_LEDGER_CHECKPOINT_DOMAIN
            + canonical_json_line(_capture_ledger_checkpoint_document(self))
        ).hexdigest()
        object.__setattr__(self, "checkpoint_sha256", checkpoint_sha256)


@dataclass(frozen=True, slots=True)
class FeePageCaptureEvidenceV2:
    """Parser-derived fee evidence with raw artifacts and external membership."""

    transport: FeeHttpCaptureEnvelopeV2
    capture_role: FeeCaptureRoleV2
    png_bytes: bytes = field(repr=False)
    png_sha256: str
    parsed_json_bytes: bytes = field(repr=False)
    parsed_json_sha256: str
    parsed_taker_rate: Decimal
    ledger_checkpoint: FeeCaptureLedgerCheckpointV2
    ledger_leaf_sha256: str
    ledger_leaf_index: int
    ledger_merkle_siblings: tuple[str, ...]
    archive_root_manifest_bytes: bytes = field(repr=False)
    archive_root_sha256: str
    poll_sequence: int | None
    poll_scheduled_ms: int | None
    _factory_token: InitVar[object | None] = None
    parsed_version_sha256: str = field(init=False)
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    account_authenticated: bool = field(init=False, default=False)
    actual_private_account_fee_claim: bool = field(init=False, default=False)
    scenario: str = field(init=False, default=PUBLIC_FEE_SCENARIO_V2)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CAPTURE_FACTORY_TOKEN:
            raise FeeContractErrorV2(
                "fee captures are factory-sealed; use build_fee_page_capture_v2"
            )
        if not isinstance(self.transport, FeeHttpCaptureEnvelopeV2):
            raise FeeContractErrorV2("transport must be FeeHttpCaptureEnvelopeV2")
        if not isinstance(self.capture_role, FeeCaptureRoleV2):
            raise FeeContractErrorV2("capture_role must be FeeCaptureRoleV2")
        _validate_capture_slot_values(
            self.capture_role,
            poll_sequence=self.poll_sequence,
            poll_scheduled_ms=self.poll_scheduled_ms,
            request_started_ms=self.transport.request_started_ms,
            response_completion_ms=self.response_completion_ms,
        )
        _validate_png(self.png_bytes)
        _verify_artifact(self.png_bytes, self.png_sha256, "png")
        parsed_rate = parse_public_fee_taker_rate_v2(self.transport)
        if self.parsed_taker_rate != parsed_rate:
            raise FeeContractErrorV2("stored fee rate differs from strict raw-page parse")
        expected_parsed = canonical_fee_parsed_json_v2(
            self.venue,
            parsed_rate,
        )
        if self.parsed_json_bytes != expected_parsed:
            raise FeeContractErrorV2("parsed JSON was not produced by the strict parser")
        _verify_artifact(
            self.parsed_json_bytes,
            self.parsed_json_sha256,
            "parsed_json",
        )
        if not isinstance(self.ledger_checkpoint, FeeCaptureLedgerCheckpointV2):
            raise FeeContractErrorV2(
                "ledger_checkpoint must be FeeCaptureLedgerCheckpointV2"
            )
        if self.ledger_checkpoint.scope != self.scope:
            raise FeeContractErrorV2("capture and external ledger scopes differ")
        if self.response_completion_ms > self.ledger_checkpoint.observed_through_ms:
            raise FeeContractErrorV2(
                "ledger checkpoint does not observe capture completion"
            )
        _validate_sha256(self.ledger_leaf_sha256, "ledger_leaf_sha256")
        expected_leaf = fee_capture_ledger_leaf_sha256_v2(
            self.transport,
            self.png_bytes,
            capture_role=self.capture_role,
            poll_sequence=self.poll_sequence,
            poll_scheduled_ms=self.poll_scheduled_ms,
        )
        if self.ledger_leaf_sha256 != expected_leaf:
            raise FeeContractErrorV2("capture ledger leaf differs from retained artifacts")
        _verify_merkle_membership(
            leaf_sha256=self.ledger_leaf_sha256,
            leaf_index=self.ledger_leaf_index,
            event_count=self.ledger_checkpoint.event_count,
            siblings=self.ledger_merkle_siblings,
            expected_root_sha256=self.ledger_checkpoint.ledger_root_sha256,
        )
        expected_archive = canonical_fee_archive_root_manifest_v2(
            transport=self.transport,
            capture_role=self.capture_role,
            png_sha256=self.png_sha256,
            parsed_json_sha256=self.parsed_json_sha256,
            parsed_taker_rate=self.parsed_taker_rate,
            ledger_checkpoint=self.ledger_checkpoint,
            ledger_leaf_sha256=self.ledger_leaf_sha256,
            ledger_leaf_index=self.ledger_leaf_index,
            ledger_merkle_siblings=self.ledger_merkle_siblings,
            poll_sequence=self.poll_sequence,
            poll_scheduled_ms=self.poll_scheduled_ms,
        )
        if self.archive_root_manifest_bytes != expected_archive:
            raise FeeContractErrorV2(
                "archive root manifest does not bind the exact capture artifacts"
            )
        expected_archive_hash = hashlib.sha256(
            _ARCHIVE_ROOT_DOMAIN + expected_archive
        ).hexdigest()
        if self.archive_root_sha256 != expected_archive_hash:
            raise FeeContractErrorV2("archive root hash mismatch")
        parsed_version = _parsed_version_sha256(
            transport=self.transport,
            parsed_json_sha256=self.parsed_json_sha256,
            parsed_taker_rate=self.parsed_taker_rate,
        )
        object.__setattr__(self, "parsed_version_sha256", parsed_version)
        event_id = hashlib.sha256(
            _CAPTURE_ID_DOMAIN
            + canonical_json_line(_capture_identity_document(self))
        ).hexdigest()
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = hashlib.sha256(
            _CAPTURE_PAYLOAD_DOMAIN
            + canonical_json_line(_capture_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def scope(self) -> FeeProtocolScopeV2:
        return self.transport.scope

    @property
    def venue(self) -> VenueV2:
        return self.transport.venue

    @property
    def response_completion_ms(self) -> int:
        return self.transport.response_completion_ms

    @property
    def official_source_url(self) -> str:
        return self.transport.final_url


@dataclass(frozen=True, slots=True)
class PublicFeeManifestV2:
    scope: FeeProtocolScopeV2
    t0_ms: int
    horizon_end_ms: int
    spot_capture: FeePageCaptureEvidenceV2
    usdm_capture: FeePageCaptureEvidenceV2
    manifest_sha256: str = field(init=False)
    scenario: str = field(init=False, default=PUBLIC_FEE_SCENARIO_V2)
    fixed_horizon_continues_after_fee_change: bool = field(init=False, default=True)
    outcome_based_stop_authorized: bool = field(init=False, default=False)
    restart_authorized: bool = field(init=False, default=False)
    actual_private_account_fee_claim: bool = field(init=False, default=False)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, FeeProtocolScopeV2):
            raise FeeContractErrorV2("manifest scope must be FeeProtocolScopeV2")
        _validate_nonnegative_int(self.t0_ms, "t0_ms")
        _validate_nonnegative_int(self.horizon_end_ms, "horizon_end_ms")
        if self.horizon_end_ms <= self.t0_ms:
            raise FeeContractErrorV2("horizon_end_ms must be strictly after T0")
        captures = (self.spot_capture, self.usdm_capture)
        if not all(isinstance(item, FeePageCaptureEvidenceV2) for item in captures):
            raise FeeContractErrorV2("manifest requires typed Spot and USD-M captures")
        if (
            self.spot_capture.venue is not VenueV2.SPOT
            or self.usdm_capture.venue is not VenueV2.USDM_FUTURES
        ):
            raise FeeContractErrorV2("T0_BLOCKED: fee manifest mixes required venues")
        for capture in captures:
            canonical_fee_page_capture_v2(capture)
            if capture.scope != self.scope:
                raise FeeContractErrorV2("T0_BLOCKED: capture scope differs from manifest")
            if capture.capture_role is not FeeCaptureRoleV2.PRE_T0:
                raise FeeContractErrorV2("T0_BLOCKED: manifest captures must be PRE_T0")
            if capture.response_completion_ms > self.t0_ms:
                raise FeeContractErrorV2("T0_BLOCKED: pre-T0 capture completed after T0")
            if capture.ledger_checkpoint.sealed_at_ms > self.t0_ms:
                raise FeeContractErrorV2("T0_BLOCKED: capture membership sealed after T0")
        if self.spot_capture.parsed_taker_rate != SPOT_PUBLIC_TAKER_RATE_V2:
            raise FeeContractErrorV2("T0_BLOCKED: Spot fee differs from frozen baseline")
        if self.usdm_capture.parsed_taker_rate != USDM_PUBLIC_TAKER_RATE_V2:
            raise FeeContractErrorV2("T0_BLOCKED: USD-M fee differs from frozen baseline")
        manifest_sha256 = hashlib.sha256(
            _MANIFEST_DOMAIN
            + canonical_json_line(_manifest_document(self, include_manifest_hash=False))
        ).hexdigest()
        object.__setattr__(self, "manifest_sha256", manifest_sha256)

    @property
    def attempt_id(self) -> str:
        return self.scope.attempt_id


@dataclass(frozen=True, slots=True)
class FeePollCadenceAuditV2:
    scope: FeeProtocolScopeV2
    venue: VenueV2
    t0_ms: int
    observed_through_ms: int
    expected_due_poll_count: int
    completed_poll_sequences: tuple[int, ...]
    missing_due_poll_count: int
    first_missing_due_sequence: int | None
    first_missing_scheduled_ms: int | None
    payload_sha256: str = field(init=False)
    poll_cadence_ms: int = field(init=False, default=FEE_POLL_CADENCE_MS_V2)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, FeeProtocolScopeV2):
            raise FeeContractErrorV2("poll audit scope must be FeeProtocolScopeV2")
        _validate_venue(self.venue)
        _validate_nonnegative_int(self.t0_ms, "t0_ms")
        _validate_nonnegative_int(self.observed_through_ms, "observed_through_ms")
        if self.observed_through_ms < self.t0_ms:
            raise FeeContractErrorV2("poll audit cannot end before T0")
        expected_count = (
            self.observed_through_ms - self.t0_ms
        ) // FEE_POLL_CADENCE_MS_V2
        if self.expected_due_poll_count != expected_count:
            raise FeeContractErrorV2("poll due count differs from exact cadence")
        if type(self.completed_poll_sequences) is not tuple:
            raise FeeContractErrorV2("completed polls must be an immutable tuple")
        expected_completed = tuple(sorted(set(self.completed_poll_sequences)))
        if self.completed_poll_sequences != expected_completed or any(
            type(value) is not int or value < 1 or value > expected_count
            for value in self.completed_poll_sequences
        ):
            raise FeeContractErrorV2("completed poll sequences are invalid")
        missing = expected_count - len(self.completed_poll_sequences)
        if self.missing_due_poll_count != missing:
            raise FeeContractErrorV2("poll missing count is contradictory")
        first = _first_missing_sequence(self.completed_poll_sequences, expected_count)
        scheduled = (
            None if first is None else self.t0_ms + first * FEE_POLL_CADENCE_MS_V2
        )
        if (
            self.first_missing_due_sequence != first
            or self.first_missing_scheduled_ms != scheduled
        ):
            raise FeeContractErrorV2("poll first-missing evidence is contradictory")
        payload_sha256 = hashlib.sha256(
            _POLL_AUDIT_DOMAIN
            + canonical_json_line(_poll_audit_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def cadence_complete(self) -> bool:
        return self.missing_due_poll_count == 0


@dataclass(frozen=True, slots=True)
class FeeTimelineCheckpointV2:
    scope: FeeProtocolScopeV2
    manifest_sha256: str
    venue: VenueV2
    registry_replay_root_sha256: str
    registry_event_count: int
    timeline_root_sha256: str
    timeline_capture_count: int
    observed_through_ms: int
    sealed_at_ms: int
    qualification_final: bool
    checkpoint_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, FeeProtocolScopeV2):
            raise FeeContractErrorV2("timeline scope must be FeeProtocolScopeV2")
        _validate_sha256(self.manifest_sha256, "manifest_sha256")
        _validate_venue(self.venue)
        _validate_sha256(
            self.registry_replay_root_sha256,
            "registry_replay_root_sha256",
        )
        _validate_nonnegative_int(self.registry_event_count, "registry_event_count")
        _validate_sha256(self.timeline_root_sha256, "timeline_root_sha256")
        _validate_positive_int(self.timeline_capture_count, "timeline_capture_count")
        _validate_nonnegative_int(self.observed_through_ms, "observed_through_ms")
        _validate_nonnegative_int(self.sealed_at_ms, "sealed_at_ms")
        if self.sealed_at_ms < self.observed_through_ms:
            raise FeeContractErrorV2("timeline seal predates observed-through clock")
        if type(self.qualification_final) is not bool:
            raise FeeContractErrorV2("qualification_final must be bool")
        checkpoint_sha256 = hashlib.sha256(
            _TIMELINE_CHECKPOINT_DOMAIN
            + canonical_json_line(_timeline_checkpoint_document(self))
        ).hexdigest()
        object.__setattr__(self, "checkpoint_sha256", checkpoint_sha256)


@dataclass(frozen=True, slots=True)
class FeeVersionResolutionV2:
    scope: FeeProtocolScopeV2
    manifest_sha256: str
    venue: VenueV2
    symbol: str
    position_event_id: str
    target_ms: int
    horizon_end_ms: int
    timeline_checkpoint_sha256: str
    timeline_root_sha256: str
    timeline_capture_count: int
    timeline_observed_through_ms: int
    timeline_qualification_final: bool
    cadence_audit_sha256: str
    status: FeeResolutionStatusV2
    taker_rate: Decimal | None
    source_capture_event_id: str | None
    parsed_version_sha256: str | None
    reasons: tuple[str, ...]
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    use: FeeUseV2 = field(init=False)
    scenario: str = field(init=False, default=PUBLIC_FEE_SCENARIO_V2)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RESOLUTION_FACTORY_TOKEN:
            raise FeeContractErrorV2(
                "fee resolutions are factory-sealed; use resolve_fee_version_v2"
            )
        if not isinstance(self.scope, FeeProtocolScopeV2):
            raise FeeContractErrorV2("resolution scope must be FeeProtocolScopeV2")
        _validate_sha256(self.manifest_sha256, "manifest_sha256")
        _validate_venue(self.venue)
        _validate_symbol(self.symbol)
        _validate_sha256(self.position_event_id, "position_event_id")
        _validate_nonnegative_int(self.target_ms, "target_ms")
        _validate_nonnegative_int(self.horizon_end_ms, "horizon_end_ms")
        _validate_sha256(
            self.timeline_checkpoint_sha256,
            "timeline_checkpoint_sha256",
        )
        _validate_sha256(self.timeline_root_sha256, "timeline_root_sha256")
        _validate_positive_int(self.timeline_capture_count, "timeline_capture_count")
        _validate_nonnegative_int(
            self.timeline_observed_through_ms,
            "timeline_observed_through_ms",
        )
        if type(self.timeline_qualification_final) is not bool:
            raise FeeContractErrorV2("timeline_qualification_final must be bool")
        _validate_sha256(self.cadence_audit_sha256, "cadence_audit_sha256")
        if not isinstance(self.status, FeeResolutionStatusV2):
            raise FeeContractErrorV2("status must be FeeResolutionStatusV2")
        if type(self.reasons) is not tuple or not self.reasons:
            raise FeeContractErrorV2("resolution requires immutable reasons")
        for reason in self.reasons:
            _validate_identity(reason, "reason")
        if self.status is FeeResolutionStatusV2.RESOLVED:
            _validate_nonnegative_decimal(self.taker_rate, "taker_rate")
            if self.source_capture_event_id is None or self.parsed_version_sha256 is None:
                raise FeeContractErrorV2("resolved fee requires capture and version hashes")
            _validate_sha256(self.source_capture_event_id, "source_capture_event_id")
            _validate_sha256(self.parsed_version_sha256, "parsed_version_sha256")
        elif any(
            value is not None
            for value in (
                self.taker_rate,
                self.source_capture_event_id,
                self.parsed_version_sha256,
            )
        ):
            raise FeeContractErrorV2("unresolved fee cannot invent a rate or source")
        object.__setattr__(self, "use", _fee_use(self.venue))
        event_id = hashlib.sha256(
            _RESOLUTION_ID_DOMAIN
            + canonical_json_line(_resolution_identity_document(self))
        ).hexdigest()
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = hashlib.sha256(
            _RESOLUTION_PAYLOAD_DOMAIN
            + canonical_json_line(_resolution_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def resolved(self) -> bool:
        return self.status is FeeResolutionStatusV2.RESOLVED

    @property
    def promoting(self) -> bool:
        return self.use is FeeUseV2.PROMOTING_USDM_FUTURES


@dataclass(frozen=True, slots=True)
class FilledBothLegFeeV2:
    scope: FeeProtocolScopeV2
    symbol: str
    position_event_id: str
    entry_resolution_event_id: str
    exit_resolution_event_id: str
    final_timeline_checkpoint_sha256: str
    final_timeline_root_sha256: str
    final_timeline_capture_count: int
    final_timeline_observed_through_ms: int
    venue: VenueV2
    use: FeeUseV2
    multiplier: FeeMultiplierV2
    entry_taker_rate: Decimal
    exit_taker_rate: Decimal
    entry_filled_notional: Decimal
    exit_filled_notional: Decimal
    entry_fee: Decimal
    exit_fee: Decimal
    total_fee: Decimal
    _factory_token: InitVar[object | None] = None
    payload_sha256: str = field(init=False)
    scenario: str = field(init=False, default=PUBLIC_FEE_SCENARIO_V2)
    both_entry_and_exit_charged: bool = field(init=False, default=True)
    actual_private_account_fee_claim: bool = field(init=False, default=False)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FEE_COST_FACTORY_TOKEN:
            raise FeeContractErrorV2(
                "fee costs are factory-sealed; use calculate_filled_both_leg_fee_v2"
            )
        if not isinstance(self.scope, FeeProtocolScopeV2):
            raise FeeContractErrorV2("cost scope must be FeeProtocolScopeV2")
        _validate_symbol(self.symbol)
        for value, label in (
            (self.position_event_id, "position_event_id"),
            (self.entry_resolution_event_id, "entry_resolution_event_id"),
            (self.exit_resolution_event_id, "exit_resolution_event_id"),
            (self.final_timeline_checkpoint_sha256, "final_timeline_checkpoint_sha256"),
            (self.final_timeline_root_sha256, "final_timeline_root_sha256"),
        ):
            _validate_sha256(value, label)
        _validate_positive_int(
            self.final_timeline_capture_count,
            "final_timeline_capture_count",
        )
        _validate_nonnegative_int(
            self.final_timeline_observed_through_ms,
            "final_timeline_observed_through_ms",
        )
        _validate_venue(self.venue)
        if self.use is not _fee_use(self.venue):
            raise FeeContractErrorV2("cost use contradicts venue")
        if not isinstance(self.multiplier, FeeMultiplierV2):
            raise FeeContractErrorV2("multiplier must be FeeMultiplierV2")
        for name in (
            "entry_taker_rate",
            "exit_taker_rate",
            "entry_filled_notional",
            "exit_filled_notional",
            "entry_fee",
            "exit_fee",
            "total_fee",
        ):
            _validate_nonnegative_decimal(getattr(self, name), name)
        if self.entry_filled_notional <= 0 or self.exit_filled_notional <= 0:
            raise FeeContractErrorV2("both filled legs require positive notional")
        with localcontext(protocol_decimal_context_v2()):
            expected_entry = (
                self.entry_filled_notional
                * self.entry_taker_rate
                * self.multiplier.decimal
            )
            expected_exit = (
                self.exit_filled_notional
                * self.exit_taker_rate
                * self.multiplier.decimal
            )
            expected_total = expected_entry + expected_exit
        if (
            self.entry_fee != expected_entry
            or self.exit_fee != expected_exit
            or self.total_fee != expected_total
        ):
            raise FeeContractErrorV2("both-leg fee arithmetic is contradictory")
        payload_sha256 = hashlib.sha256(
            _COST_PAYLOAD_DOMAIN
            + canonical_json_line(_fee_cost_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def promoting(self) -> bool:
        return self.use is FeeUseV2.PROMOTING_USDM_FUTURES

    @property
    def primary_cell(self) -> bool:
        return self.multiplier.primary

    @property
    def mandatory_adverse_report(self) -> bool:
        return self.multiplier is FeeMultiplierV2.MANDATORY_ADVERSE_2_0X


@dataclass(frozen=True, slots=True)
class FilledFeeSliceV2:
    """One exact simulated fill and its final-timeline fee resolution."""

    kind: FeeSliceKindV2
    position_event_id: str
    execution_event_id: str
    execution_payload_sha256: str
    generation_event_id: str
    generation_evidence_sha256: str
    fill_event_ms: int
    filled_quantity: Decimal
    gross_notional: Decimal
    resolution_event_id: str
    resolution_payload_sha256: str
    resolution_status: FeeResolutionStatusV2
    taker_rate: Decimal | None
    multiplier: FeeMultiplierV2
    realized_fee: Decimal | None
    _factory_token: InitVar[object | None] = None
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FEE_SLICE_COST_FACTORY_TOKEN:
            raise FeeContractErrorV2(
                "fee slices are factory-sealed; use calculate_filled_position_fee_v2"
            )
        if not isinstance(self.kind, FeeSliceKindV2):
            raise FeeContractErrorV2("fee slice kind has the wrong type")
        for value, label in (
            (self.position_event_id, "position_event_id"),
            (self.execution_event_id, "execution_event_id"),
            (self.execution_payload_sha256, "execution_payload_sha256"),
            (self.generation_event_id, "generation_event_id"),
            (self.generation_evidence_sha256, "generation_evidence_sha256"),
            (self.resolution_event_id, "resolution_event_id"),
            (self.resolution_payload_sha256, "resolution_payload_sha256"),
        ):
            _validate_sha256(value, label)
        _validate_nonnegative_int(self.fill_event_ms, "fill_event_ms")
        _validate_nonnegative_decimal(self.filled_quantity, "filled_quantity")
        _validate_nonnegative_decimal(self.gross_notional, "gross_notional")
        if self.filled_quantity <= 0 or self.gross_notional <= 0:
            raise FeeContractErrorV2(
                "fee slice requires positive quantity and gross notional"
            )
        if not isinstance(self.resolution_status, FeeResolutionStatusV2):
            raise FeeContractErrorV2("fee slice resolution status has the wrong type")
        if not isinstance(self.multiplier, FeeMultiplierV2):
            raise FeeContractErrorV2("fee slice multiplier has the wrong type")
        if self.resolution_status is FeeResolutionStatusV2.RESOLVED:
            _validate_nonnegative_decimal(self.taker_rate, "taker_rate")
            _validate_nonnegative_decimal(self.realized_fee, "realized_fee")
            assert self.taker_rate is not None
            assert self.realized_fee is not None
            with localcontext(protocol_decimal_context_v2()):
                expected = self.gross_notional * self.taker_rate * self.multiplier.decimal
            if self.realized_fee != expected:
                raise FeeContractErrorV2("fee slice arithmetic is contradictory")
        elif self.taker_rate is not None or self.realized_fee is not None:
            raise FeeContractErrorV2(
                "unresolved fee slice cannot invent a rate or numeric cost"
            )
        object.__setattr__(
            self,
            "payload_sha256",
            hashlib.sha256(
                _SLICE_COST_PAYLOAD_DOMAIN
                + canonical_json_line(
                    _filled_fee_slice_document(self, include_payload_hash=False)
                )
            ).hexdigest(),
        )

    @property
    def resolved(self) -> bool:
        return self.resolution_status is FeeResolutionStatusV2.RESOLVED


@dataclass(frozen=True, slots=True)
class FilledPositionFeeV2:
    """Known realized fee subtotal for a PAPER entry and 0..N exit fills."""

    scope: FeeProtocolScopeV2
    symbol: str
    position_event_id: str
    mandatory_exit_fee_certificate_sha256: str
    final_timeline_checkpoint_sha256: str
    final_timeline_root_sha256: str
    final_timeline_capture_count: int
    final_timeline_observed_through_ms: int
    venue: VenueV2
    use: FeeUseV2
    multiplier: FeeMultiplierV2
    entry_slice: FilledFeeSliceV2
    exit_slices: tuple[FilledFeeSliceV2, ...]
    terminal_status: str | None
    residual_quantity: Decimal
    status: FilledPositionFeeStatusV2
    known_realized_entry_fee: Decimal
    known_realized_exit_fee: Decimal
    known_realized_total_fee: Decimal
    unresolved_slice_count: int
    legacy_single_exit_fee: FilledBothLegFeeV2 | None
    _factory_token: InitVar[object | None] = None
    payload_sha256: str = field(init=False)
    scenario: str = field(init=False, default=PUBLIC_FEE_SCENARIO_V2)
    actual_private_account_fee_claim: bool = field(init=False, default=False)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _POSITION_FEE_COST_FACTORY_TOKEN:
            raise FeeContractErrorV2(
                "position fee costs are factory-sealed; "
                "use calculate_filled_position_fee_v2"
            )
        if not isinstance(self.scope, FeeProtocolScopeV2):
            raise FeeContractErrorV2("position fee scope has the wrong type")
        _validate_symbol(self.symbol)
        for value, label in (
            (self.position_event_id, "position_event_id"),
            (
                self.mandatory_exit_fee_certificate_sha256,
                "mandatory_exit_fee_certificate_sha256",
            ),
            (
                self.final_timeline_checkpoint_sha256,
                "final_timeline_checkpoint_sha256",
            ),
            (self.final_timeline_root_sha256, "final_timeline_root_sha256"),
        ):
            _validate_sha256(value, label)
        _validate_positive_int(
            self.final_timeline_capture_count,
            "final_timeline_capture_count",
        )
        _validate_nonnegative_int(
            self.final_timeline_observed_through_ms,
            "final_timeline_observed_through_ms",
        )
        _validate_venue(self.venue)
        if self.use is not _fee_use(self.venue):
            raise FeeContractErrorV2("position fee use contradicts venue")
        if not isinstance(self.multiplier, FeeMultiplierV2):
            raise FeeContractErrorV2("position fee multiplier has the wrong type")
        if not isinstance(self.entry_slice, FilledFeeSliceV2) or (
            self.entry_slice.kind is not FeeSliceKindV2.ENTRY
        ):
            raise FeeContractErrorV2("position fee requires one typed entry slice")
        if type(self.exit_slices) is not tuple or any(
            not isinstance(value, FilledFeeSliceV2)
            or value.kind is not FeeSliceKindV2.EXIT
            for value in self.exit_slices
        ):
            raise FeeContractErrorV2(
                "position fee exit slices must be an immutable typed tuple"
            )
        ordered = tuple(
            sorted(
                self.exit_slices,
                key=lambda value: (value.fill_event_ms, value.execution_event_id),
            )
        )
        if ordered != self.exit_slices:
            raise FeeContractErrorV2(
                "position fee exit slices must use chronological order"
            )
        if len({value.execution_event_id for value in self.exit_slices}) != len(
            self.exit_slices
        ):
            raise FeeContractErrorV2("position fee exit slice IDs must be unique")
        for value in (self.entry_slice, *self.exit_slices):
            if (
                value.position_event_id != self.position_event_id
                or value.multiplier is not self.multiplier
            ):
                raise FeeContractErrorV2(
                    "fee slice differs from the position or multiplier"
                )
        if self.terminal_status not in (
            None,
            "EXITED_FULL",
            "DUST_RESIDUAL_RETAINED",
            "POST_ENTRY_UNRESOLVED_EXIT",
        ):
            raise FeeContractErrorV2("position fee terminal status is unsupported")
        _validate_nonnegative_decimal(self.residual_quantity, "residual_quantity")
        for name in (
            "known_realized_entry_fee",
            "known_realized_exit_fee",
            "known_realized_total_fee",
        ):
            _validate_nonnegative_decimal(getattr(self, name), name)
        _validate_nonnegative_int(self.unresolved_slice_count, "unresolved_slice_count")
        known_entry = (
            Decimal(0)
            if self.entry_slice.realized_fee is None
            else self.entry_slice.realized_fee
        )
        with localcontext(protocol_decimal_context_v2()):
            known_exit = sum(
                (
                    Decimal(0) if value.realized_fee is None else value.realized_fee
                    for value in self.exit_slices
                ),
                Decimal(0),
            )
            known_total = known_entry + known_exit
            filled_exit_quantity = sum(
                (value.filled_quantity for value in self.exit_slices),
                Decimal(0),
            )
        unresolved = sum(
            1 for value in (self.entry_slice, *self.exit_slices) if not value.resolved
        )
        if (
            self.known_realized_entry_fee != known_entry
            or self.known_realized_exit_fee != known_exit
            or self.known_realized_total_fee != known_total
            or self.unresolved_slice_count != unresolved
        ):
            raise FeeContractErrorV2("position fee known subtotal is contradictory")
        full_inventory_exit = bool(
            self.terminal_status == "EXITED_FULL"
            and self.residual_quantity == 0
            and filled_exit_quantity == self.entry_slice.filled_quantity
        )
        if self.terminal_status == "EXITED_FULL" and not full_inventory_exit:
            raise FeeContractErrorV2(
                "EXITED_FULL fee state does not conserve entry inventory"
            )
        expected_status = (
            FilledPositionFeeStatusV2.INCOMPLETE_FEE_VERSION
            if unresolved
            else (
                FilledPositionFeeStatusV2.BOTH_LEGS_COMPLETE
                if full_inventory_exit
                else (
                    FilledPositionFeeStatusV2.ENTRY_ONLY
                    if not self.exit_slices
                    else FilledPositionFeeStatusV2.PARTIAL_EXIT
                )
            )
        )
        if self.status is not expected_status:
            raise FeeContractErrorV2("position fee completion status is contradictory")
        if self.status is FilledPositionFeeStatusV2.BOTH_LEGS_COMPLETE:
            if len(self.exit_slices) == 1:
                if not isinstance(self.legacy_single_exit_fee, FilledBothLegFeeV2):
                    raise FeeContractErrorV2(
                        "single-exit completion must retain legacy both-leg fee"
                    )
                legacy = self.legacy_single_exit_fee
                canonical_filled_both_leg_fee_v2(legacy)
                if (
                    legacy.position_event_id != self.position_event_id
                    or legacy.multiplier is not self.multiplier
                    or legacy.entry_resolution_event_id
                    != self.entry_slice.resolution_event_id
                    or legacy.exit_resolution_event_id
                    != self.exit_slices[0].resolution_event_id
                    or legacy.entry_fee != self.known_realized_entry_fee
                    or legacy.exit_fee != self.known_realized_exit_fee
                    or legacy.total_fee != self.known_realized_total_fee
                ):
                    raise FeeContractErrorV2(
                        "legacy single-exit fee differs from multi-slice aggregation"
                    )
            elif self.legacy_single_exit_fee is not None:
                raise FeeContractErrorV2(
                    "multi-exit completion cannot expose a singular legacy fee"
                )
        elif self.legacy_single_exit_fee is not None:
            raise FeeContractErrorV2(
                "incomplete position cannot expose a legacy both-leg fee"
            )
        object.__setattr__(
            self,
            "payload_sha256",
            hashlib.sha256(
                _POSITION_COST_PAYLOAD_DOMAIN
                + canonical_json_line(
                    _filled_position_fee_document(self, include_payload_hash=False)
                )
            ).hexdigest(),
        )

    @property
    def both_legs_complete(self) -> bool:
        return self.status is FilledPositionFeeStatusV2.BOTH_LEGS_COMPLETE

    @property
    def final_fee_cost_complete(self) -> bool:
        return self.both_legs_complete

    @property
    def final_total_fee(self) -> Decimal | None:
        return self.known_realized_total_fee if self.both_legs_complete else None


class FeeCaptureRegistryV2:
    """Bounded full-artifact registry with externally pinned restart state."""

    def __init__(self, *, maximum_events: int, scope: FeeProtocolScopeV2) -> None:
        _validate_positive_int(maximum_events, "maximum_events")
        if not isinstance(scope, FeeProtocolScopeV2):
            raise FeeContractErrorV2("registry scope must be FeeProtocolScopeV2")
        self._maximum_events = maximum_events
        self._scope = scope
        self._captures: dict[str, FeePageCaptureEvidenceV2] = {}

    @property
    def scope(self) -> FeeProtocolScopeV2:
        return self._scope

    @property
    def event_count(self) -> int:
        return len(self._captures)

    @property
    def maximum_events(self) -> int:
        return self._maximum_events

    @property
    def replay_root_sha256(self) -> str:
        return _registry_replay_root(self._ordered_state_rows())

    def register(self, capture: FeePageCaptureEvidenceV2) -> FeeRegistryDispositionV2:
        if not isinstance(capture, FeePageCaptureEvidenceV2):
            raise FeeContractErrorV2("registry accepts FeePageCaptureEvidenceV2 only")
        canonical_fee_page_capture_v2(capture)
        canonical_fee_capture_archive_v2(capture)
        if capture.scope != self._scope:
            raise FeeContractErrorV2("registry rejects a different protocol scope")
        prior = self._captures.get(capture.event_id)
        if prior is not None:
            if canonical_fee_capture_archive_v2(prior) != canonical_fee_capture_archive_v2(
                capture
            ):
                raise FeeContractErrorV2(
                    "deterministic fee event ID collides with different artifacts"
                )
            return FeeRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if len(self._captures) >= self._maximum_events:
            raise FeeContractErrorV2("bounded fee registry capacity exhausted")
        self._captures[capture.event_id] = capture
        return FeeRegistryDispositionV2.NEW

    def captures_for_venue(self, venue: VenueV2) -> tuple[FeePageCaptureEvidenceV2, ...]:
        _validate_venue(venue)
        return tuple(
            sorted(
                (item for item in self._captures.values() if item.venue is venue),
                key=lambda item: (
                    item.response_completion_ms,
                    item.capture_role.value,
                    item.event_id,
                ),
            )
        )

    def export_state_v2(self) -> bytes:
        rows = self._ordered_state_rows()
        return canonical_json_line(
            {
                "events": rows,
                "maximum_events": self._maximum_events,
                "replay_root_sha256": _registry_replay_root(rows),
                "schema_version": _REGISTRY_STATE_SCHEMA,
                "scope": _scope_document(self._scope),
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
        expected_attempt_id: str,
        expected_plan_id: str,
        expected_protocol_hash: str,
        expected_universe_sha256: str,
        expected_checkpoint_sha256: str,
    ) -> FeeCaptureRegistryV2:
        """Restore only against an out-of-band root/count/scope checkpoint."""

        _validate_sha256(expected_replay_root_sha256, "expected_replay_root_sha256")
        _validate_nonnegative_int(expected_event_count, "expected_event_count")
        _validate_positive_int(expected_maximum_events, "expected_maximum_events")
        expected_scope = FeeProtocolScopeV2(
            attempt_id=expected_attempt_id,
            plan_id=expected_plan_id,
            protocol_hash=expected_protocol_hash,
            universe_sha256=expected_universe_sha256,
        )
        checkpoint = fee_registry_checkpoint_sha256_v2(
            scope=expected_scope,
            replay_root_sha256=expected_replay_root_sha256,
            event_count=expected_event_count,
            maximum_events=expected_maximum_events,
        )
        _validate_sha256(expected_checkpoint_sha256, "expected_checkpoint_sha256")
        if checkpoint != expected_checkpoint_sha256:
            raise FeeContractErrorV2("external registry checkpoint hash mismatch")
        document = _parse_canonical_json(payload, "registry state")
        if set(document) != {
            "events",
            "maximum_events",
            "replay_root_sha256",
            "schema_version",
            "scope",
        } or document.get("schema_version") != _REGISTRY_STATE_SCHEMA:
            raise FeeContractErrorV2("registry state schema is unsupported")
        if document.get("scope") != _scope_document(expected_scope):
            raise FeeContractErrorV2("registry state differs from expected scope")
        if document.get("maximum_events") != expected_maximum_events:
            raise FeeContractErrorV2("registry capacity differs from checkpoint")
        raw_rows = document.get("events")
        if not isinstance(raw_rows, list):
            raise FeeContractErrorV2("registry events must be a list")
        if len(raw_rows) != expected_event_count:
            raise FeeContractErrorV2("registry event census differs from checkpoint")
        registry = cls(maximum_events=expected_maximum_events, scope=expected_scope)
        prior_key: tuple[int, str, int, str] | None = None
        canonical_rows: list[dict[str, object]] = []
        for raw_row in raw_rows:
            row, capture, order_key = _parse_registry_state_row(raw_row)
            if prior_key is not None and order_key <= prior_key:
                raise FeeContractErrorV2("registry rows are not in strict replay order")
            prior_key = order_key
            registry.register(capture)
            canonical_rows.append(row)
        replay_root = _registry_replay_root(canonical_rows)
        if document.get("replay_root_sha256") != replay_root:
            raise FeeContractErrorV2("registry state replay root is internally invalid")
        if replay_root != expected_replay_root_sha256:
            raise FeeContractErrorV2("registry replay root differs from checkpoint")
        return registry

    def _ordered_state_rows(self) -> list[dict[str, object]]:
        return [
            _registry_state_row(capture)
            for capture in sorted(self._captures.values(), key=_registry_order_key)
        ]


def parse_public_fee_taker_rate_v2(
    transport: FeeHttpCaptureEnvelopeV2,
) -> Decimal:
    """Strictly parse the unique anonymous Regular User maker/taker table row."""

    if not isinstance(transport, FeeHttpCaptureEnvelopeV2):
        raise FeeContractErrorV2("transport must be FeeHttpCaptureEnvelopeV2")
    try:
        page = transport.raw_or_dom_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise FeeContractErrorV2("official fee page must be valid UTF-8 HTML") from exc
    parser = _FeeHtmlParser()
    try:
        parser.feed(page)
        parser.close()
    except (ValueError, AssertionError) as exc:
        raise FeeContractErrorV2("official fee page HTML is malformed") from exc
    title = _normalize_space(" ".join(parser.title_parts)).lower()
    if "binance" not in title or "fee" not in title:
        raise FeeContractErrorV2("official fee page title is missing Binance fee identity")
    canonical_urls = tuple(dict.fromkeys(parser.canonical_urls))
    if canonical_urls != (transport.final_url,):
        raise FeeContractErrorV2("official fee page has no unique exact canonical URL")
    candidates: list[tuple[str, str]] = []
    for row in parser.rows:
        text = _normalize_space(" ".join(row))
        folded = text.casefold()
        if "regular user" not in folded:
            continue
        if transport.venue is VenueV2.SPOT:
            if "spot" not in folded or "maker" not in folded or "taker" not in folded:
                continue
        elif "usdt" not in folded or "maker" not in folded or "taker" not in folded:
            continue
        pairs = _PERCENT_PAIR_RE.findall(text)
        if len(pairs) != 1:
            raise FeeContractErrorV2(
                "Regular User row must contain exactly one maker/taker percent pair"
            )
        candidates.append(pairs[0])
    if len(candidates) != 1:
        raise FeeContractErrorV2(
            "official fee page must contain one unambiguous Regular User fee row"
        )
    _, taker_percent_text = candidates[0]
    with localcontext(protocol_decimal_context_v2()):
        taker_rate = (Decimal(taker_percent_text) / Decimal("100")).quantize(
            Decimal("0.000001")
        )
    if taker_rate <= 0 or taker_rate > Decimal("0.05"):
        raise FeeContractErrorV2("parsed public taker rate is outside allowed bounds")
    return taker_rate


def canonical_fee_parsed_json_v2(venue: VenueV2, taker_rate: Decimal) -> bytes:
    _validate_venue(venue)
    _validate_nonnegative_decimal(taker_rate, "taker_rate")
    return canonical_json_line(
        {
            "account_authenticated": False,
            "bnb_fee_discount": False,
            "official_source_url": _official_url(venue),
            "order_liquidity_role": "TAKER",
            "parsed_schema_version": _PARSED_SCHEMA,
            "scenario": PUBLIC_FEE_SCENARIO_V2,
            "taker_rate_decimal": _decimal_text(taker_rate),
            "venue": venue.value,
            "vip_tier": "REGULAR_USER_VIP0",
        }
    )


def fee_capture_ledger_leaf_sha256_v2(
    transport: FeeHttpCaptureEnvelopeV2,
    png_bytes: bytes,
    *,
    capture_role: FeeCaptureRoleV2,
    poll_sequence: int | None = None,
    poll_scheduled_ms: int | None = None,
) -> str:
    """Compute the exact external-ledger leaf from parser-derived artifacts."""

    if not isinstance(transport, FeeHttpCaptureEnvelopeV2):
        raise FeeContractErrorV2("transport must be FeeHttpCaptureEnvelopeV2")
    if not isinstance(capture_role, FeeCaptureRoleV2):
        raise FeeContractErrorV2("capture_role must be FeeCaptureRoleV2")
    _validate_capture_slot_values(
        capture_role,
        poll_sequence=poll_sequence,
        poll_scheduled_ms=poll_scheduled_ms,
        request_started_ms=transport.request_started_ms,
        response_completion_ms=transport.response_completion_ms,
    )
    _validate_png(png_bytes)
    png_hash = _sha256_bytes(png_bytes, "png_bytes", maximum_size=_MAX_PNG_BYTES)
    rate = parse_public_fee_taker_rate_v2(transport)
    parsed = canonical_fee_parsed_json_v2(transport.venue, rate)
    document = {
        "capture_role": capture_role.value,
        "parsed_json_sha256": hashlib.sha256(parsed).hexdigest(),
        "parsed_taker_rate_decimal": _decimal_text(rate),
        "png_sha256": png_hash,
        "poll_scheduled_ms": poll_scheduled_ms,
        "poll_sequence": poll_sequence,
        "transport": _transport_document(transport),
    }
    return hashlib.sha256(
        _CAPTURE_LEDGER_LEAF_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def fee_capture_ledger_root_v2(leaves: Sequence[str]) -> str:
    """Build the deterministic duplicate-last Merkle root used by membership proofs."""

    if isinstance(leaves, (str, bytes)) or not isinstance(leaves, Sequence):
        raise FeeContractErrorV2("leaves must be a finite digest sequence")
    level = list(leaves)
    if not level:
        raise FeeContractErrorV2("capture ledger requires at least one leaf")
    for leaf in level:
        _validate_sha256(leaf, "ledger leaf")
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        level = [
            _merkle_parent(level[index], level[index + 1])
            for index in range(0, len(level), 2)
        ]
    return level[0]


def build_fee_page_capture_v2(
    *,
    transport: FeeHttpCaptureEnvelopeV2,
    capture_role: FeeCaptureRoleV2,
    png_bytes: bytes,
    ledger_checkpoint: FeeCaptureLedgerCheckpointV2,
    ledger_leaf_index: int,
    ledger_merkle_siblings: tuple[str, ...],
    expected_ledger_checkpoint_sha256: str,
    poll_sequence: int | None = None,
    poll_scheduled_ms: int | None = None,
) -> FeePageCaptureEvidenceV2:
    """Build evidence only after strict parse and external checkpoint verification."""

    if not isinstance(transport, FeeHttpCaptureEnvelopeV2):
        raise FeeContractErrorV2("transport must be FeeHttpCaptureEnvelopeV2")
    if not isinstance(ledger_checkpoint, FeeCaptureLedgerCheckpointV2):
        raise FeeContractErrorV2(
            "ledger_checkpoint must be FeeCaptureLedgerCheckpointV2"
        )
    _validate_sha256(
        expected_ledger_checkpoint_sha256,
        "expected_ledger_checkpoint_sha256",
    )
    if ledger_checkpoint.checkpoint_sha256 != expected_ledger_checkpoint_sha256:
        raise FeeContractErrorV2("external capture-ledger checkpoint hash mismatch")
    _validate_png(png_bytes)
    png_hash = hashlib.sha256(png_bytes).hexdigest()
    rate = parse_public_fee_taker_rate_v2(transport)
    parsed = canonical_fee_parsed_json_v2(transport.venue, rate)
    parsed_hash = hashlib.sha256(parsed).hexdigest()
    leaf_hash = fee_capture_ledger_leaf_sha256_v2(
        transport,
        png_bytes,
        capture_role=capture_role,
        poll_sequence=poll_sequence,
        poll_scheduled_ms=poll_scheduled_ms,
    )
    _verify_merkle_membership(
        leaf_sha256=leaf_hash,
        leaf_index=ledger_leaf_index,
        event_count=ledger_checkpoint.event_count,
        siblings=ledger_merkle_siblings,
        expected_root_sha256=ledger_checkpoint.ledger_root_sha256,
    )
    archive_manifest = canonical_fee_archive_root_manifest_v2(
        transport=transport,
        capture_role=capture_role,
        png_sha256=png_hash,
        parsed_json_sha256=parsed_hash,
        parsed_taker_rate=rate,
        ledger_checkpoint=ledger_checkpoint,
        ledger_leaf_sha256=leaf_hash,
        ledger_leaf_index=ledger_leaf_index,
        ledger_merkle_siblings=ledger_merkle_siblings,
        poll_sequence=poll_sequence,
        poll_scheduled_ms=poll_scheduled_ms,
    )
    archive_root = hashlib.sha256(_ARCHIVE_ROOT_DOMAIN + archive_manifest).hexdigest()
    return FeePageCaptureEvidenceV2(
        transport=transport,
        capture_role=capture_role,
        png_bytes=png_bytes,
        png_sha256=png_hash,
        parsed_json_bytes=parsed,
        parsed_json_sha256=parsed_hash,
        parsed_taker_rate=rate,
        ledger_checkpoint=ledger_checkpoint,
        ledger_leaf_sha256=leaf_hash,
        ledger_leaf_index=ledger_leaf_index,
        ledger_merkle_siblings=ledger_merkle_siblings,
        archive_root_manifest_bytes=archive_manifest,
        archive_root_sha256=archive_root,
        poll_sequence=poll_sequence,
        poll_scheduled_ms=poll_scheduled_ms,
        _factory_token=_CAPTURE_FACTORY_TOKEN,
    )


def canonical_fee_archive_root_manifest_v2(
    *,
    transport: FeeHttpCaptureEnvelopeV2,
    capture_role: FeeCaptureRoleV2,
    png_sha256: str,
    parsed_json_sha256: str,
    parsed_taker_rate: Decimal,
    ledger_checkpoint: FeeCaptureLedgerCheckpointV2,
    ledger_leaf_sha256: str,
    ledger_leaf_index: int,
    ledger_merkle_siblings: tuple[str, ...],
    poll_sequence: int | None,
    poll_scheduled_ms: int | None,
) -> bytes:
    if not isinstance(transport, FeeHttpCaptureEnvelopeV2):
        raise FeeContractErrorV2("transport must be FeeHttpCaptureEnvelopeV2")
    if not isinstance(capture_role, FeeCaptureRoleV2):
        raise FeeContractErrorV2("capture_role must be FeeCaptureRoleV2")
    for value, label in (
        (png_sha256, "png_sha256"),
        (parsed_json_sha256, "parsed_json_sha256"),
        (ledger_leaf_sha256, "ledger_leaf_sha256"),
    ):
        _validate_sha256(value, label)
    _validate_nonnegative_decimal(parsed_taker_rate, "parsed_taker_rate")
    if not isinstance(ledger_checkpoint, FeeCaptureLedgerCheckpointV2):
        raise FeeContractErrorV2("invalid capture ledger checkpoint")
    _validate_nonnegative_int(ledger_leaf_index, "ledger_leaf_index")
    if type(ledger_merkle_siblings) is not tuple:
        raise FeeContractErrorV2("ledger_merkle_siblings must be tuple")
    for sibling in ledger_merkle_siblings:
        _validate_sha256(sibling, "ledger Merkle sibling")
    _validate_capture_slot_values(
        capture_role,
        poll_sequence=poll_sequence,
        poll_scheduled_ms=poll_scheduled_ms,
        request_started_ms=transport.request_started_ms,
        response_completion_ms=transport.response_completion_ms,
    )
    return canonical_json_line(
        {
            "capture_role": capture_role.value,
            "ledger_checkpoint": _capture_ledger_checkpoint_document(
                ledger_checkpoint,
                include_checkpoint_hash=True,
            ),
            "ledger_leaf_index": ledger_leaf_index,
            "ledger_leaf_sha256": ledger_leaf_sha256,
            "ledger_merkle_siblings": list(ledger_merkle_siblings),
            "parsed_json_sha256": parsed_json_sha256,
            "parsed_taker_rate_decimal": _decimal_text(parsed_taker_rate),
            "png_sha256": png_sha256,
            "poll_scheduled_ms": poll_scheduled_ms,
            "poll_sequence": poll_sequence,
            "schema_version": _ARCHIVE_ROOT_SCHEMA,
            "transport": _transport_document(transport),
        }
    )


def canonical_fee_page_capture_v2(capture: FeePageCaptureEvidenceV2) -> bytes:
    if not isinstance(capture, FeePageCaptureEvidenceV2):
        raise FeeContractErrorV2("capture must be FeePageCaptureEvidenceV2")
    expected = hashlib.sha256(
        _CAPTURE_PAYLOAD_DOMAIN
        + canonical_json_line(_capture_document(capture, include_payload_hash=False))
    ).hexdigest()
    if capture.payload_sha256 != expected:
        raise FeeContractErrorV2("fee capture payload hash mismatch")
    return canonical_json_line(_capture_document(capture, include_payload_hash=True))


def canonical_fee_capture_archive_v2(capture: FeePageCaptureEvidenceV2) -> bytes:
    """Serialize all bytes needed to independently repeat parse/hash validation."""

    canonical_fee_page_capture_v2(capture)
    return canonical_json_line(
        {
            "archive_root_manifest_base64": _b64(capture.archive_root_manifest_bytes),
            "capture_payload": _capture_document(capture, include_payload_hash=True),
            "ledger_merkle_siblings": list(capture.ledger_merkle_siblings),
            "parsed_json_base64": _b64(capture.parsed_json_bytes),
            "png_base64": _b64(capture.png_bytes),
            "raw_or_dom_base64": _b64(capture.transport.raw_or_dom_bytes),
            "schema_version": _ARCHIVE_SCHEMA,
            "transport": _transport_document(capture.transport),
        }
    )


def canonical_public_fee_manifest_v2(manifest: PublicFeeManifestV2) -> bytes:
    if not isinstance(manifest, PublicFeeManifestV2):
        raise FeeContractErrorV2("manifest must be PublicFeeManifestV2")
    expected = hashlib.sha256(
        _MANIFEST_DOMAIN
        + canonical_json_line(_manifest_document(manifest, include_manifest_hash=False))
    ).hexdigest()
    if manifest.manifest_sha256 != expected:
        raise FeeContractErrorV2("public fee manifest hash mismatch")
    return canonical_json_line(_manifest_document(manifest, include_manifest_hash=True))


def fee_registry_checkpoint_sha256_v2(
    *,
    scope: FeeProtocolScopeV2,
    replay_root_sha256: str,
    event_count: int,
    maximum_events: int,
) -> str:
    if not isinstance(scope, FeeProtocolScopeV2):
        raise FeeContractErrorV2("scope must be FeeProtocolScopeV2")
    _validate_sha256(replay_root_sha256, "replay_root_sha256")
    _validate_nonnegative_int(event_count, "event_count")
    _validate_positive_int(maximum_events, "maximum_events")
    return hashlib.sha256(
        _REGISTRY_CHECKPOINT_DOMAIN
        + canonical_json_line(
            {
                "event_count": event_count,
                "maximum_events": maximum_events,
                "replay_root_sha256": replay_root_sha256,
                "scope": _scope_document(scope),
            }
        )
    ).hexdigest()


def build_fee_timeline_checkpoint_v2(
    manifest: PublicFeeManifestV2,
    registry: FeeCaptureRegistryV2,
    venue: VenueV2,
    *,
    observed_through_ms: int,
    sealed_at_ms: int,
) -> FeeTimelineCheckpointV2:
    """Build a checkpoint; consumers must separately pin its expected hash."""

    _validate_manifest_registry(manifest, registry)
    _validate_venue(venue)
    _validate_nonnegative_int(observed_through_ms, "observed_through_ms")
    _validate_nonnegative_int(sealed_at_ms, "sealed_at_ms")
    if observed_through_ms < manifest.t0_ms:
        raise FeeContractErrorV2("timeline cannot end before T0")
    registry_captures = tuple(registry._captures.values())
    if any(
        item.response_completion_ms > observed_through_ms
        for item in registry_captures
    ):
        raise FeeContractErrorV2(
            "global registry includes capture unavailable at timeline observed-through"
        )
    if any(
        item.ledger_checkpoint.sealed_at_ms > sealed_at_ms
        for item in registry_captures
    ):
        raise FeeContractErrorV2(
            "global registry includes capture membership sealed after timeline seal"
        )
    captures = registry.captures_for_venue(venue)
    if not captures:
        raise FeeContractErrorV2("timeline has no capture for venue")
    _validate_venue_timeline(manifest, venue, captures)
    root = _timeline_root(captures)
    latest_completion = captures[-1].response_completion_ms
    expected_due_count, completed_due_sequences = _due_poll_sequences(
        captures,
        t0_ms=manifest.t0_ms,
        observed_through_ms=observed_through_ms,
    )
    cadence_complete = completed_due_sequences == tuple(
        range(1, expected_due_count + 1)
    )
    qualification_final = (
        observed_through_ms >= manifest.horizon_end_ms
        and latest_completion >= manifest.horizon_end_ms
        and cadence_complete
    )
    return FeeTimelineCheckpointV2(
        scope=manifest.scope,
        manifest_sha256=manifest.manifest_sha256,
        venue=venue,
        registry_replay_root_sha256=registry.replay_root_sha256,
        registry_event_count=registry.event_count,
        timeline_root_sha256=root,
        timeline_capture_count=len(captures),
        observed_through_ms=observed_through_ms,
        sealed_at_ms=sealed_at_ms,
        qualification_final=qualification_final,
    )


def audit_fee_poll_cadence_v2(
    manifest: PublicFeeManifestV2,
    registry: FeeCaptureRegistryV2,
    timeline_checkpoint: FeeTimelineCheckpointV2,
    *,
    expected_timeline_checkpoint_sha256: str,
) -> FeePollCadenceAuditV2:
    captures = _validate_timeline_checkpoint(
        manifest,
        registry,
        timeline_checkpoint,
        expected_timeline_checkpoint_sha256=expected_timeline_checkpoint_sha256,
    )
    expected_count, due_completed = _due_poll_sequences(
        captures,
        t0_ms=manifest.t0_ms,
        observed_through_ms=timeline_checkpoint.observed_through_ms,
    )
    first_missing = _first_missing_sequence(due_completed, expected_count)
    return FeePollCadenceAuditV2(
        scope=manifest.scope,
        venue=timeline_checkpoint.venue,
        t0_ms=manifest.t0_ms,
        observed_through_ms=timeline_checkpoint.observed_through_ms,
        expected_due_poll_count=expected_count,
        completed_poll_sequences=due_completed,
        missing_due_poll_count=expected_count - len(due_completed),
        first_missing_due_sequence=first_missing,
        first_missing_scheduled_ms=(
            None
            if first_missing is None
            else manifest.t0_ms + first_missing * FEE_POLL_CADENCE_MS_V2
        ),
    )


def canonical_fee_poll_audit_v2(audit: FeePollCadenceAuditV2) -> bytes:
    if not isinstance(audit, FeePollCadenceAuditV2):
        raise FeeContractErrorV2("audit must be FeePollCadenceAuditV2")
    expected = hashlib.sha256(
        _POLL_AUDIT_DOMAIN
        + canonical_json_line(_poll_audit_document(audit, include_payload_hash=False))
    ).hexdigest()
    if audit.payload_sha256 != expected:
        raise FeeContractErrorV2("fee poll audit payload hash mismatch")
    return canonical_json_line(_poll_audit_document(audit, include_payload_hash=True))


def resolve_fee_version_v2(
    manifest: PublicFeeManifestV2,
    registry: FeeCaptureRegistryV2,
    timeline_checkpoint: FeeTimelineCheckpointV2,
    *,
    target_ms: int,
    symbol: str,
    position_event_id: str,
    expected_timeline_checkpoint_sha256: str,
) -> FeeVersionResolutionV2:
    """Resolve only an exact or closed completion bracket; never an open tail."""

    _validate_nonnegative_int(target_ms, "target_ms")
    _validate_symbol(symbol)
    _validate_sha256(position_event_id, "position_event_id")
    if target_ms < manifest.t0_ms:
        raise FeeContractErrorV2("fee target before T0 is outside the sealed attempt")
    if target_ms > manifest.horizon_end_ms:
        raise FeeContractErrorV2("fee target exceeds the fixed attempt horizon")
    captures = _validate_timeline_checkpoint(
        manifest,
        registry,
        timeline_checkpoint,
        expected_timeline_checkpoint_sha256=expected_timeline_checkpoint_sha256,
    )
    audit = audit_fee_poll_cadence_v2(
        manifest,
        registry,
        timeline_checkpoint,
        expected_timeline_checkpoint_sha256=expected_timeline_checkpoint_sha256,
    )
    if target_ms > timeline_checkpoint.observed_through_ms:
        return _unresolved_fee_version(
            manifest,
            timeline_checkpoint,
            audit,
            target_ms=target_ms,
            symbol=symbol,
            position_event_id=position_event_id,
            status=FeeResolutionStatusV2.TARGET_AFTER_OBSERVED_EVIDENCE,
            reasons=("TARGET_AFTER_OBSERVED_EVIDENCE",),
        )
    if (
        audit.first_missing_scheduled_ms is not None
        and target_ms >= audit.first_missing_scheduled_ms
    ):
        return _unresolved_fee_version(
            manifest,
            timeline_checkpoint,
            audit,
            target_ms=target_ms,
            symbol=symbol,
            position_event_id=position_event_id,
            status=FeeResolutionStatusV2.INCONCLUSIVE_FEE_POLL_GAP,
            reasons=(
                "EXPECTED_900000MS_POLL_MISSING",
                f"FIRST_MISSING_SEQUENCE_{audit.first_missing_due_sequence}",
            ),
        )
    exact = next(
        (item for item in captures if item.response_completion_ms == target_ms),
        None,
    )
    if exact is not None:
        return _resolved_fee_version(
            manifest,
            timeline_checkpoint,
            audit,
            exact,
            target_ms=target_ms,
            symbol=symbol,
            position_event_id=position_event_id,
            reasons=("TARGET_EQUALS_CAPTURE_COMPLETION",),
        )
    left: FeePageCaptureEvidenceV2 | None = None
    right: FeePageCaptureEvidenceV2 | None = None
    for capture in captures:
        if capture.response_completion_ms < target_ms:
            left = capture
            continue
        right = capture
        break
    if left is None:
        raise FeeContractErrorV2("sealed pre-T0 anchor is not before target")
    if right is None:
        return _unresolved_fee_version(
            manifest,
            timeline_checkpoint,
            audit,
            target_ms=target_ms,
            symbol=symbol,
            position_event_id=position_event_id,
            status=FeeResolutionStatusV2.PENDING_OPEN_TAIL,
            reasons=("OPEN_TAIL_HAS_NO_RIGHT_COMPLETION",),
        )
    if _same_parsed_version(left, right):
        return _resolved_fee_version(
            manifest,
            timeline_checkpoint,
            audit,
            left,
            target_ms=target_ms,
            symbol=symbol,
            position_event_id=position_event_id,
            reasons=("ADJACENT_PARSED_VERSIONS_EQUAL",),
        )
    return _unresolved_fee_version(
        manifest,
        timeline_checkpoint,
        audit,
        target_ms=target_ms,
        symbol=symbol,
        position_event_id=position_event_id,
        status=FeeResolutionStatusV2.INCONCLUSIVE_FEE_VERSION,
        reasons=(
            "STRICTLY_INSIDE_CHANGED_COMPLETION_BRACKET",
            f"LEFT_COMPLETION_{left.response_completion_ms}",
            f"RIGHT_COMPLETION_{right.response_completion_ms}",
        ),
    )


def canonical_fee_version_resolution_v2(
    resolution: FeeVersionResolutionV2,
) -> bytes:
    if not isinstance(resolution, FeeVersionResolutionV2):
        raise FeeContractErrorV2("resolution must be FeeVersionResolutionV2")
    expected = hashlib.sha256(
        _RESOLUTION_PAYLOAD_DOMAIN
        + canonical_json_line(_resolution_document(resolution, include_payload_hash=False))
    ).hexdigest()
    if resolution.payload_sha256 != expected:
        raise FeeContractErrorV2("fee resolution payload hash mismatch")
    return canonical_json_line(_resolution_document(resolution, include_payload_hash=True))


def calculate_filled_both_leg_fee_v2(
    entry_resolution: FeeVersionResolutionV2,
    exit_resolution: FeeVersionResolutionV2,
    *,
    manifest: PublicFeeManifestV2,
    registry: FeeCaptureRegistryV2,
    final_timeline_checkpoint: FeeTimelineCheckpointV2,
    expected_final_timeline_checkpoint_sha256: str,
    entry_filled_notional: Decimal,
    exit_filled_notional: Decimal,
    multiplier: FeeMultiplierV2,
) -> FilledBothLegFeeV2:
    """Re-resolve both legs against the externally pinned final timeline."""

    if not isinstance(entry_resolution, FeeVersionResolutionV2) or not isinstance(
        exit_resolution,
        FeeVersionResolutionV2,
    ):
        raise FeeContractErrorV2("both legs require typed fee resolutions")
    if not isinstance(final_timeline_checkpoint, FeeTimelineCheckpointV2):
        raise FeeContractErrorV2("final timeline checkpoint is required")
    _validate_timeline_checkpoint(
        manifest,
        registry,
        final_timeline_checkpoint,
        expected_timeline_checkpoint_sha256=(
            expected_final_timeline_checkpoint_sha256
        ),
    )
    if not final_timeline_checkpoint.qualification_final:
        raise FeeContractErrorV2(
            "fee cost requires a final timeline that closes the horizon tail"
        )
    if (
        entry_resolution.scope != exit_resolution.scope
        or entry_resolution.scope != manifest.scope
        or entry_resolution.manifest_sha256 != exit_resolution.manifest_sha256
        or entry_resolution.manifest_sha256 != manifest.manifest_sha256
        or entry_resolution.venue is not exit_resolution.venue
        or entry_resolution.venue is not final_timeline_checkpoint.venue
        or entry_resolution.symbol != exit_resolution.symbol
        or entry_resolution.position_event_id != exit_resolution.position_event_id
    ):
        raise FeeContractErrorV2(
            "entry and exit resolutions differ in sealed scope, venue, or position"
        )
    if entry_resolution.target_ms > exit_resolution.target_ms:
        raise FeeContractErrorV2("entry fee target must not be after exit target")
    expected_entry = resolve_fee_version_v2(
        manifest,
        registry,
        final_timeline_checkpoint,
        target_ms=entry_resolution.target_ms,
        symbol=entry_resolution.symbol,
        position_event_id=entry_resolution.position_event_id,
        expected_timeline_checkpoint_sha256=(
            expected_final_timeline_checkpoint_sha256
        ),
    )
    expected_exit = resolve_fee_version_v2(
        manifest,
        registry,
        final_timeline_checkpoint,
        target_ms=exit_resolution.target_ms,
        symbol=exit_resolution.symbol,
        position_event_id=exit_resolution.position_event_id,
        expected_timeline_checkpoint_sha256=(
            expected_final_timeline_checkpoint_sha256
        ),
    )
    if canonical_fee_version_resolution_v2(entry_resolution) != (
        canonical_fee_version_resolution_v2(expected_entry)
    ) or canonical_fee_version_resolution_v2(exit_resolution) != (
        canonical_fee_version_resolution_v2(expected_exit)
    ):
        raise FeeContractErrorV2(
            "stale or forged resolution differs from final timeline re-resolution"
        )
    if not expected_entry.resolved or not expected_exit.resolved:
        raise FeeContractErrorV2(
            "both-leg fees require independently final resolved fee versions"
        )
    if not isinstance(multiplier, FeeMultiplierV2):
        raise FeeContractErrorV2("multiplier must be FeeMultiplierV2")
    _validate_nonnegative_decimal(entry_filled_notional, "entry_filled_notional")
    _validate_nonnegative_decimal(exit_filled_notional, "exit_filled_notional")
    if entry_filled_notional <= 0 or exit_filled_notional <= 0:
        raise FeeContractErrorV2("both filled legs require positive notional")
    assert expected_entry.taker_rate is not None
    assert expected_exit.taker_rate is not None
    with localcontext(protocol_decimal_context_v2()):
        entry_fee = (
            entry_filled_notional * expected_entry.taker_rate * multiplier.decimal
        )
        exit_fee = exit_filled_notional * expected_exit.taker_rate * multiplier.decimal
        total_fee = entry_fee + exit_fee
    return FilledBothLegFeeV2(
        scope=manifest.scope,
        symbol=expected_entry.symbol,
        position_event_id=expected_entry.position_event_id,
        entry_resolution_event_id=expected_entry.event_id,
        exit_resolution_event_id=expected_exit.event_id,
        final_timeline_checkpoint_sha256=final_timeline_checkpoint.checkpoint_sha256,
        final_timeline_root_sha256=final_timeline_checkpoint.timeline_root_sha256,
        final_timeline_capture_count=final_timeline_checkpoint.timeline_capture_count,
        final_timeline_observed_through_ms=(
            final_timeline_checkpoint.observed_through_ms
        ),
        venue=expected_entry.venue,
        use=expected_entry.use,
        multiplier=multiplier,
        entry_taker_rate=expected_entry.taker_rate,
        exit_taker_rate=expected_exit.taker_rate,
        entry_filled_notional=entry_filled_notional,
        exit_filled_notional=exit_filled_notional,
        entry_fee=entry_fee,
        exit_fee=exit_fee,
        total_fee=total_fee,
        _factory_token=_FEE_COST_FACTORY_TOKEN,
    )


def canonical_filled_both_leg_fee_v2(fee: FilledBothLegFeeV2) -> bytes:
    if not isinstance(fee, FilledBothLegFeeV2):
        raise FeeContractErrorV2("fee must be FilledBothLegFeeV2")
    expected = hashlib.sha256(
        _COST_PAYLOAD_DOMAIN
        + canonical_json_line(_fee_cost_document(fee, include_payload_hash=False))
    ).hexdigest()
    if fee.payload_sha256 != expected:
        raise FeeContractErrorV2("both-leg fee payload hash mismatch")
    return canonical_json_line(_fee_cost_document(fee, include_payload_hash=True))


def calculate_filled_position_fee_v2(
    certificate: MandatoryExitFeeCertificateV2,
    *,
    manifest: PublicFeeManifestV2,
    registry: FeeCaptureRegistryV2,
    final_timeline_checkpoint: FeeTimelineCheckpointV2,
    expected_final_timeline_checkpoint_sha256: str,
    multiplier: FeeMultiplierV2,
) -> FilledPositionFeeV2:
    """Re-resolve entry and every exact exit fill against one final timeline."""

    from signalbot.r4b_v2.execution.mandatory_exit import (
        MandatoryExitFeeCertificateV2,
        canonical_mandatory_exit_fee_certificate_v2,
    )

    if not isinstance(certificate, MandatoryExitFeeCertificateV2):
        raise FeeContractErrorV2(
            "certificate must be MandatoryExitFeeCertificateV2"
        )
    canonical_mandatory_exit_fee_certificate_v2(certificate)
    if not isinstance(manifest, PublicFeeManifestV2):
        raise FeeContractErrorV2("public fee manifest is required")
    if not isinstance(registry, FeeCaptureRegistryV2):
        raise FeeContractErrorV2("fee capture registry is required")
    if not isinstance(final_timeline_checkpoint, FeeTimelineCheckpointV2):
        raise FeeContractErrorV2("final timeline checkpoint is required")
    _validate_timeline_checkpoint(
        manifest,
        registry,
        final_timeline_checkpoint,
        expected_timeline_checkpoint_sha256=(
            expected_final_timeline_checkpoint_sha256
        ),
    )
    if not final_timeline_checkpoint.qualification_final:
        raise FeeContractErrorV2(
            "position fee cost requires a final timeline that closes the horizon tail"
        )
    if not isinstance(multiplier, FeeMultiplierV2):
        raise FeeContractErrorV2("multiplier must be FeeMultiplierV2")
    position = certificate.position
    if (
        manifest.scope.attempt_id != position.attempt_id
        or final_timeline_checkpoint.scope != manifest.scope
        or final_timeline_checkpoint.venue is not position.venue
        or position.venue is not VenueV2.USDM_FUTURES
    ):
        raise FeeContractErrorV2(
            "mandatory exit fee certificate differs from fee scope or venue"
        )
    entry_resolution = resolve_fee_version_v2(
        manifest,
        registry,
        final_timeline_checkpoint,
        target_ms=position.entry_target_venue_ms,
        symbol=position.symbol,
        position_event_id=position.event_id,
        expected_timeline_checkpoint_sha256=(
            expected_final_timeline_checkpoint_sha256
        ),
    )
    entry_slice = _build_filled_fee_slice_v2(
        kind=FeeSliceKindV2.ENTRY,
        position_event_id=position.event_id,
        execution_event_id=position.entry_execution_event_id,
        execution_payload_sha256=position.entry_execution_payload_sha256,
        generation_event_id=position.entry_execution_event_id,
        generation_evidence_sha256=position.entry_execution_evidence_sha256,
        fill_event_ms=position.entry_target_venue_ms,
        filled_quantity=position.initial_quantity,
        gross_notional=position.entry_notional,
        resolution=entry_resolution,
        multiplier=multiplier,
    )
    exit_resolutions: list[FeeVersionResolutionV2] = []
    exit_slices: list[FilledFeeSliceV2] = []
    for attempt in certificate.filled_exit_attempts:
        resolution = resolve_fee_version_v2(
            manifest,
            registry,
            final_timeline_checkpoint,
            target_ms=attempt.generation_venue_ms,
            symbol=position.symbol,
            position_event_id=position.event_id,
            expected_timeline_checkpoint_sha256=(
                expected_final_timeline_checkpoint_sha256
            ),
        )
        exit_resolutions.append(resolution)
        exit_slices.append(
            _build_filled_fee_slice_v2(
                kind=FeeSliceKindV2.EXIT,
                position_event_id=position.event_id,
                execution_event_id=attempt.event_id,
                execution_payload_sha256=attempt.payload_sha256,
                generation_event_id=attempt.generation_event_id,
                generation_evidence_sha256=attempt.generation_evidence_sha256,
                fill_event_ms=attempt.generation_venue_ms,
                filled_quantity=attempt.filled_quantity,
                gross_notional=attempt.gross_notional,
                resolution=resolution,
                multiplier=multiplier,
            )
        )
    all_slices = (entry_slice, *exit_slices)
    unresolved_slice_count = sum(1 for value in all_slices if not value.resolved)
    known_entry = (
        Decimal(0) if entry_slice.realized_fee is None else entry_slice.realized_fee
    )
    with localcontext(protocol_decimal_context_v2()):
        known_exit = sum(
            (
                Decimal(0) if value.realized_fee is None else value.realized_fee
                for value in exit_slices
            ),
            Decimal(0),
        )
        known_total = known_entry + known_exit
    status = (
        FilledPositionFeeStatusV2.INCOMPLETE_FEE_VERSION
        if unresolved_slice_count
        else (
            FilledPositionFeeStatusV2.BOTH_LEGS_COMPLETE
            if certificate.full_exit
            else (
                FilledPositionFeeStatusV2.ENTRY_ONLY
                if not exit_slices
                else FilledPositionFeeStatusV2.PARTIAL_EXIT
            )
        )
    )
    legacy_single_exit_fee: FilledBothLegFeeV2 | None = None
    if status is FilledPositionFeeStatusV2.BOTH_LEGS_COMPLETE and len(exit_slices) == 1:
        legacy_single_exit_fee = calculate_filled_both_leg_fee_v2(
            entry_resolution,
            exit_resolutions[0],
            manifest=manifest,
            registry=registry,
            final_timeline_checkpoint=final_timeline_checkpoint,
            expected_final_timeline_checkpoint_sha256=(
                expected_final_timeline_checkpoint_sha256
            ),
            entry_filled_notional=position.entry_notional,
            exit_filled_notional=exit_slices[0].gross_notional,
            multiplier=multiplier,
        )
    return FilledPositionFeeV2(
        scope=manifest.scope,
        symbol=position.symbol,
        position_event_id=position.event_id,
        mandatory_exit_fee_certificate_sha256=certificate.certificate_sha256,
        final_timeline_checkpoint_sha256=final_timeline_checkpoint.checkpoint_sha256,
        final_timeline_root_sha256=final_timeline_checkpoint.timeline_root_sha256,
        final_timeline_capture_count=final_timeline_checkpoint.timeline_capture_count,
        final_timeline_observed_through_ms=(
            final_timeline_checkpoint.observed_through_ms
        ),
        venue=position.venue,
        use=_fee_use(position.venue),
        multiplier=multiplier,
        entry_slice=entry_slice,
        exit_slices=tuple(exit_slices),
        terminal_status=(
            None
            if certificate.terminal is None
            else certificate.terminal.terminal_status.value
        ),
        residual_quantity=certificate.residual_quantity,
        status=status,
        known_realized_entry_fee=known_entry,
        known_realized_exit_fee=known_exit,
        known_realized_total_fee=known_total,
        unresolved_slice_count=unresolved_slice_count,
        legacy_single_exit_fee=legacy_single_exit_fee,
        _factory_token=_POSITION_FEE_COST_FACTORY_TOKEN,
    )


def canonical_filled_fee_slice_v2(fee_slice: FilledFeeSliceV2) -> bytes:
    if not isinstance(fee_slice, FilledFeeSliceV2):
        raise FeeContractErrorV2("fee_slice must be FilledFeeSliceV2")
    expected = hashlib.sha256(
        _SLICE_COST_PAYLOAD_DOMAIN
        + canonical_json_line(
            _filled_fee_slice_document(fee_slice, include_payload_hash=False)
        )
    ).hexdigest()
    if fee_slice.payload_sha256 != expected:
        raise FeeContractErrorV2("fee slice payload hash mismatch")
    return canonical_json_line(
        _filled_fee_slice_document(fee_slice, include_payload_hash=True)
    )


def canonical_filled_position_fee_v2(fee: FilledPositionFeeV2) -> bytes:
    if not isinstance(fee, FilledPositionFeeV2):
        raise FeeContractErrorV2("fee must be FilledPositionFeeV2")
    expected = hashlib.sha256(
        _POSITION_COST_PAYLOAD_DOMAIN
        + canonical_json_line(
            _filled_position_fee_document(fee, include_payload_hash=False)
        )
    ).hexdigest()
    if fee.payload_sha256 != expected:
        raise FeeContractErrorV2("position fee payload hash mismatch")
    return canonical_json_line(
        _filled_position_fee_document(fee, include_payload_hash=True)
    )


def _build_filled_fee_slice_v2(
    *,
    kind: FeeSliceKindV2,
    position_event_id: str,
    execution_event_id: str,
    execution_payload_sha256: str,
    generation_event_id: str,
    generation_evidence_sha256: str,
    fill_event_ms: int,
    filled_quantity: Decimal,
    gross_notional: Decimal,
    resolution: FeeVersionResolutionV2,
    multiplier: FeeMultiplierV2,
) -> FilledFeeSliceV2:
    canonical_fee_version_resolution_v2(resolution)
    if (
        resolution.position_event_id != position_event_id
        or resolution.target_ms != fill_event_ms
    ):
        raise FeeContractErrorV2(
            "fee resolution differs from exact simulated fill identity or timestamp"
        )
    realized_fee: Decimal | None = None
    if resolution.resolved:
        assert resolution.taker_rate is not None
        with localcontext(protocol_decimal_context_v2()):
            realized_fee = gross_notional * resolution.taker_rate * multiplier.decimal
    return FilledFeeSliceV2(
        kind=kind,
        position_event_id=position_event_id,
        execution_event_id=execution_event_id,
        execution_payload_sha256=execution_payload_sha256,
        generation_event_id=generation_event_id,
        generation_evidence_sha256=generation_evidence_sha256,
        fill_event_ms=fill_event_ms,
        filled_quantity=filled_quantity,
        gross_notional=gross_notional,
        resolution_event_id=resolution.event_id,
        resolution_payload_sha256=resolution.payload_sha256,
        resolution_status=resolution.status,
        taker_rate=resolution.taker_rate,
        multiplier=multiplier,
        realized_fee=realized_fee,
        _factory_token=_FEE_SLICE_COST_FACTORY_TOKEN,
    )


class _FeeHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.canonical_urls: list[str] = []
        self.rows: list[tuple[str, ...]] = []
        self._in_title = False
        self._row_cells: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        attr_map = {key.casefold(): value for key, value in attrs}
        lowered = tag.casefold()
        if lowered == "title":
            self._in_title = True
        elif lowered == "link" and (
            (attr_map.get("rel") or "").casefold() == "canonical"
        ):
            href = attr_map.get("href")
            if href is not None:
                self.canonical_urls.append(href)
        elif lowered == "tr":
            if self._row_cells is not None:
                raise ValueError("nested table row")
            self._row_cells = []
        elif lowered in ("td", "th") and self._row_cells is not None:
            if self._cell_parts is not None:
                raise ValueError("nested table cell")
            self._cell_parts = []

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.casefold()
        if lowered == "title":
            self._in_title = False
        elif lowered in ("td", "th") and self._cell_parts is not None:
            assert self._row_cells is not None
            self._row_cells.append(_normalize_space(" ".join(self._cell_parts)))
            self._cell_parts = None
        elif lowered == "tr" and self._row_cells is not None:
            if self._cell_parts is not None:
                raise ValueError("unclosed table cell")
            self.rows.append(tuple(self._row_cells))
            self._row_cells = None

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def close(self) -> None:
        super().close()
        if self._row_cells is not None or self._cell_parts is not None:
            raise ValueError("unclosed table structure")


def _validate_png(payload: bytes) -> None:
    """Verify PNG framing, CRCs, IHDR/IEND, and inflate every image scanline."""

    _sha256_bytes(payload, "png_bytes", maximum_size=_MAX_PNG_BYTES)
    if not payload.startswith(_PNG_SIGNATURE):
        raise FeeContractErrorV2("PNG signature is invalid")
    offset = len(_PNG_SIGNATURE)
    chunks: list[tuple[bytes, bytes]] = []
    saw_iend = False
    while offset < len(payload):
        if len(payload) - offset < 12:
            raise FeeContractErrorV2("PNG chunk is truncated")
        length = int.from_bytes(payload[offset : offset + 4], "big")
        chunk_type = payload[offset + 4 : offset + 8]
        end = offset + 12 + length
        if end > len(payload):
            raise FeeContractErrorV2("PNG chunk payload is truncated")
        data = payload[offset + 8 : offset + 8 + length]
        expected_crc = int.from_bytes(payload[offset + 8 + length : end], "big")
        observed_crc = zlib.crc32(chunk_type + data) & 0xFFFFFFFF
        if expected_crc != observed_crc:
            raise FeeContractErrorV2("PNG chunk CRC is invalid")
        chunks.append((chunk_type, data))
        offset = end
        if chunk_type == b"IEND":
            saw_iend = True
            break
    if not saw_iend or offset != len(payload):
        raise FeeContractErrorV2("PNG must end exactly at IEND")
    if not chunks or chunks[0][0] != b"IHDR" or chunks[-1][0] != b"IEND":
        raise FeeContractErrorV2("PNG IHDR/IEND ordering is invalid")
    ihdr = chunks[0][1]
    if len(ihdr) != 13:
        raise FeeContractErrorV2("PNG IHDR length is invalid")
    width = int.from_bytes(ihdr[0:4], "big")
    height = int.from_bytes(ihdr[4:8], "big")
    bit_depth, color_type, compression, filtering, interlace = ihdr[8:13]
    if width < 1 or height < 1 or width * height > 25_000_000:
        raise FeeContractErrorV2("PNG dimensions are invalid")
    channels_by_type = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}
    allowed_depths = {
        0: {1, 2, 4, 8, 16},
        2: {8, 16},
        3: {1, 2, 4, 8},
        4: {8, 16},
        6: {8, 16},
    }
    if color_type not in channels_by_type or bit_depth not in allowed_depths[color_type]:
        raise FeeContractErrorV2("PNG color type or bit depth is unsupported")
    if compression != 0 or filtering != 0 or interlace != 0:
        raise FeeContractErrorV2("PNG must use standard non-interlaced encoding")
    if sum(kind == b"IHDR" for kind, _ in chunks) != 1:
        raise FeeContractErrorV2("PNG must contain exactly one IHDR")
    if len(chunks[-1][1]) != 0 or sum(kind == b"IEND" for kind, _ in chunks) != 1:
        raise FeeContractErrorV2("PNG must contain one empty IEND")
    known_critical = {b"IHDR", b"PLTE", b"IDAT", b"IEND"}
    if any(
        kind[:1].isalpha() and kind[:1].isupper() and kind not in known_critical
        for kind, _ in chunks
    ):
        raise FeeContractErrorV2("PNG contains an unknown critical chunk")
    plte_chunks = tuple(data for kind, data in chunks if kind == b"PLTE")
    if len(plte_chunks) > 1:
        raise FeeContractErrorV2("PNG contains duplicate PLTE chunks")
    if color_type == 3:
        if not plte_chunks:
            raise FeeContractErrorV2("indexed PNG requires a PLTE chunk")
        palette_len = len(plte_chunks[0])
        if (
            palette_len < 3
            or palette_len % 3 != 0
            or palette_len // 3 > 2**bit_depth
        ):
            raise FeeContractErrorV2("indexed PNG palette is invalid")
    elif color_type in (0, 4) and plte_chunks:
        raise FeeContractErrorV2("grayscale PNG cannot contain PLTE")
    idat_positions = [index for index, (kind, _) in enumerate(chunks) if kind == b"IDAT"]
    if not idat_positions:
        raise FeeContractErrorV2("PNG contains no IDAT data")
    if idat_positions != list(range(idat_positions[0], idat_positions[-1] + 1)):
        raise FeeContractErrorV2("PNG IDAT chunks must be consecutive")
    idat = b"".join(data for kind, data in chunks if kind == b"IDAT")
    if not idat:
        raise FeeContractErrorV2("PNG contains no IDAT data")
    bits_per_row = width * channels_by_type[color_type] * bit_depth
    bytes_per_row = (bits_per_row + 7) // 8
    expected_length = height * (bytes_per_row + 1)
    if expected_length > 128 * 1024 * 1024:
        raise FeeContractErrorV2("PNG decoded payload exceeds bounded size")
    inflater = zlib.decompressobj()
    try:
        decoded = inflater.decompress(idat, expected_length + 1)
        if inflater.unconsumed_tail or len(decoded) > expected_length:
            raise FeeContractErrorV2("PNG decoded payload exceeds IHDR dimensions")
        decoded += inflater.flush(expected_length + 1 - len(decoded))
    except zlib.error as exc:
        raise FeeContractErrorV2("PNG image data cannot be decoded") from exc
    if not inflater.eof or inflater.unused_data or inflater.unconsumed_tail:
        raise FeeContractErrorV2("PNG compressed stream has trailing or incomplete data")
    if len(decoded) != expected_length:
        raise FeeContractErrorV2("PNG decoded scanline length is invalid")
    stride = bytes_per_row + 1
    if any(decoded[offset] > 4 for offset in range(0, len(decoded), stride)):
        raise FeeContractErrorV2("PNG scanline filter is invalid")


def _validate_timeline_checkpoint(
    manifest: PublicFeeManifestV2,
    registry: FeeCaptureRegistryV2,
    checkpoint: FeeTimelineCheckpointV2,
    *,
    expected_timeline_checkpoint_sha256: str,
) -> tuple[FeePageCaptureEvidenceV2, ...]:
    _validate_manifest_registry(manifest, registry)
    if not isinstance(checkpoint, FeeTimelineCheckpointV2):
        raise FeeContractErrorV2("timeline checkpoint is required")
    _validate_sha256(
        expected_timeline_checkpoint_sha256,
        "expected_timeline_checkpoint_sha256",
    )
    if checkpoint.checkpoint_sha256 != expected_timeline_checkpoint_sha256:
        raise FeeContractErrorV2("external timeline checkpoint hash mismatch")
    if checkpoint.scope != manifest.scope or checkpoint.manifest_sha256 != (
        manifest.manifest_sha256
    ):
        raise FeeContractErrorV2("timeline checkpoint differs from manifest scope")
    if (
        checkpoint.registry_replay_root_sha256 != registry.replay_root_sha256
        or checkpoint.registry_event_count != registry.event_count
    ):
        raise FeeContractErrorV2("stale timeline registry root or census")
    captures = registry.captures_for_venue(checkpoint.venue)
    if any(item.response_completion_ms > checkpoint.observed_through_ms for item in captures):
        raise FeeContractErrorV2("timeline as-of excludes a registered capture")
    _validate_venue_timeline(manifest, checkpoint.venue, captures)
    recomputed = build_fee_timeline_checkpoint_v2(
        manifest,
        registry,
        checkpoint.venue,
        observed_through_ms=checkpoint.observed_through_ms,
        sealed_at_ms=checkpoint.sealed_at_ms,
    )
    if recomputed != checkpoint:
        raise FeeContractErrorV2("timeline root, census, as-of, or finality is invalid")
    return captures


def _validate_manifest_registry(
    manifest: PublicFeeManifestV2,
    registry: FeeCaptureRegistryV2,
) -> None:
    if not isinstance(manifest, PublicFeeManifestV2):
        raise FeeContractErrorV2("manifest must be PublicFeeManifestV2")
    canonical_public_fee_manifest_v2(manifest)
    if not isinstance(registry, FeeCaptureRegistryV2):
        raise FeeContractErrorV2("registry must be FeeCaptureRegistryV2")
    if registry.scope != manifest.scope:
        raise FeeContractErrorV2("registry and manifest scopes differ")
    for anchor in (manifest.spot_capture, manifest.usdm_capture):
        registered = registry._captures.get(anchor.event_id)
        if registered is None or canonical_fee_capture_archive_v2(registered) != (
            canonical_fee_capture_archive_v2(anchor)
        ):
            raise FeeContractErrorV2("registry does not contain exact manifest anchor")


def _validate_venue_timeline(
    manifest: PublicFeeManifestV2,
    venue: VenueV2,
    captures: tuple[FeePageCaptureEvidenceV2, ...],
) -> None:
    anchor = manifest.spot_capture if venue is VenueV2.SPOT else manifest.usdm_capture
    pre = tuple(item for item in captures if item.capture_role is FeeCaptureRoleV2.PRE_T0)
    if pre != (anchor,):
        raise FeeContractErrorV2("timeline must contain exactly its sealed pre-T0 anchor")
    seen_sequences: set[int] = set()
    seen_completions: set[int] = set()
    for capture in captures:
        canonical_fee_page_capture_v2(capture)
        if capture.scope != manifest.scope or capture.venue is not venue:
            raise FeeContractErrorV2("timeline capture scope or venue differs")
        if capture.response_completion_ms in seen_completions:
            raise FeeContractErrorV2("timeline has ambiguous equal completion clocks")
        seen_completions.add(capture.response_completion_ms)
        if capture.capture_role is FeeCaptureRoleV2.POST_T0_POLL:
            assert capture.poll_sequence is not None
            assert capture.poll_scheduled_ms is not None
            expected_schedule = (
                manifest.t0_ms + capture.poll_sequence * FEE_POLL_CADENCE_MS_V2
            )
            if capture.poll_scheduled_ms != expected_schedule:
                raise FeeContractErrorV2("post-T0 poll differs from exact cadence")
            if capture.poll_sequence in seen_sequences:
                raise FeeContractErrorV2("timeline has duplicate post-T0 poll slot")
            seen_sequences.add(capture.poll_sequence)


def _resolved_fee_version(
    manifest: PublicFeeManifestV2,
    timeline: FeeTimelineCheckpointV2,
    audit: FeePollCadenceAuditV2,
    capture: FeePageCaptureEvidenceV2,
    *,
    target_ms: int,
    symbol: str,
    position_event_id: str,
    reasons: tuple[str, ...],
) -> FeeVersionResolutionV2:
    return FeeVersionResolutionV2(
        scope=manifest.scope,
        manifest_sha256=manifest.manifest_sha256,
        venue=capture.venue,
        symbol=symbol,
        position_event_id=position_event_id,
        target_ms=target_ms,
        horizon_end_ms=manifest.horizon_end_ms,
        timeline_checkpoint_sha256=timeline.checkpoint_sha256,
        timeline_root_sha256=timeline.timeline_root_sha256,
        timeline_capture_count=timeline.timeline_capture_count,
        timeline_observed_through_ms=timeline.observed_through_ms,
        timeline_qualification_final=timeline.qualification_final,
        cadence_audit_sha256=audit.payload_sha256,
        status=FeeResolutionStatusV2.RESOLVED,
        taker_rate=capture.parsed_taker_rate,
        source_capture_event_id=capture.event_id,
        parsed_version_sha256=capture.parsed_version_sha256,
        reasons=reasons,
        _factory_token=_RESOLUTION_FACTORY_TOKEN,
    )


def _unresolved_fee_version(
    manifest: PublicFeeManifestV2,
    timeline: FeeTimelineCheckpointV2,
    audit: FeePollCadenceAuditV2,
    *,
    target_ms: int,
    symbol: str,
    position_event_id: str,
    status: FeeResolutionStatusV2,
    reasons: tuple[str, ...],
) -> FeeVersionResolutionV2:
    return FeeVersionResolutionV2(
        scope=manifest.scope,
        manifest_sha256=manifest.manifest_sha256,
        venue=timeline.venue,
        symbol=symbol,
        position_event_id=position_event_id,
        target_ms=target_ms,
        horizon_end_ms=manifest.horizon_end_ms,
        timeline_checkpoint_sha256=timeline.checkpoint_sha256,
        timeline_root_sha256=timeline.timeline_root_sha256,
        timeline_capture_count=timeline.timeline_capture_count,
        timeline_observed_through_ms=timeline.observed_through_ms,
        timeline_qualification_final=timeline.qualification_final,
        cadence_audit_sha256=audit.payload_sha256,
        status=status,
        taker_rate=None,
        source_capture_event_id=None,
        parsed_version_sha256=None,
        reasons=reasons,
        _factory_token=_RESOLUTION_FACTORY_TOKEN,
    )


def _capture_identity_document(capture: FeePageCaptureEvidenceV2) -> dict[str, object]:
    return {
        "capture_role": capture.capture_role.value,
        "poll_scheduled_ms": capture.poll_scheduled_ms,
        "poll_sequence": capture.poll_sequence,
        "rule_version": capture.rule_version,
        "scope": _scope_document(capture.scope),
        "venue": capture.venue.value,
    }


def _capture_document(
    capture: FeePageCaptureEvidenceV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_capture_identity_document(capture),
        "account_authenticated": capture.account_authenticated,
        "actual_private_account_fee_claim": capture.actual_private_account_fee_claim,
        "archive_root_sha256": capture.archive_root_sha256,
        "event_id": capture.event_id,
        "ledger_checkpoint_sha256": capture.ledger_checkpoint.checkpoint_sha256,
        "ledger_leaf_index": capture.ledger_leaf_index,
        "ledger_leaf_sha256": capture.ledger_leaf_sha256,
        "official_source_url": capture.official_source_url,
        "parsed_json_sha256": capture.parsed_json_sha256,
        "parsed_taker_rate_decimal": _decimal_text(capture.parsed_taker_rate),
        "parsed_version_sha256": capture.parsed_version_sha256,
        "png_sha256": capture.png_sha256,
        "response_completion_ms": capture.response_completion_ms,
        "scenario": capture.scenario,
        "schema_version": _CAPTURE_SCHEMA,
        "transport_sha256": _transport_sha256(capture.transport),
    }
    if include_payload_hash:
        document["payload_sha256"] = capture.payload_sha256
    return document


def _manifest_document(
    manifest: PublicFeeManifestV2,
    *,
    include_manifest_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "actual_private_account_fee_claim": manifest.actual_private_account_fee_claim,
        "fixed_horizon_continues_after_fee_change": (
            manifest.fixed_horizon_continues_after_fee_change
        ),
        "horizon_end_ms": manifest.horizon_end_ms,
        "outcome_based_stop_authorized": manifest.outcome_based_stop_authorized,
        "restart_authorized": manifest.restart_authorized,
        "rule_version": manifest.rule_version,
        "scenario": manifest.scenario,
        "schema_version": _MANIFEST_SCHEMA,
        "scope": _scope_document(manifest.scope),
        "spot_capture": _manifest_capture_reference(manifest.spot_capture),
        "t0_ms": manifest.t0_ms,
        "usdm_capture": _manifest_capture_reference(manifest.usdm_capture),
    }
    if include_manifest_hash:
        document["manifest_sha256"] = manifest.manifest_sha256
    return document


def _manifest_capture_reference(
    capture: FeePageCaptureEvidenceV2,
) -> dict[str, object]:
    return {
        "archive_root_sha256": capture.archive_root_sha256,
        "capture_event_id": capture.event_id,
        "capture_payload_sha256": capture.payload_sha256,
        "ledger_checkpoint_sha256": capture.ledger_checkpoint.checkpoint_sha256,
        "parsed_taker_rate_decimal": _decimal_text(capture.parsed_taker_rate),
        "parsed_version_sha256": capture.parsed_version_sha256,
        "response_completion_ms": capture.response_completion_ms,
        "venue": capture.venue.value,
    }


def _poll_audit_document(
    audit: FeePollCadenceAuditV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "completed_poll_sequences": list(audit.completed_poll_sequences),
        "expected_due_poll_count": audit.expected_due_poll_count,
        "first_missing_due_sequence": audit.first_missing_due_sequence,
        "first_missing_scheduled_ms": audit.first_missing_scheduled_ms,
        "missing_due_poll_count": audit.missing_due_poll_count,
        "observed_through_ms": audit.observed_through_ms,
        "poll_cadence_ms": audit.poll_cadence_ms,
        "rule_version": audit.rule_version,
        "schema_version": _POLL_AUDIT_SCHEMA,
        "scope": _scope_document(audit.scope),
        "t0_ms": audit.t0_ms,
        "venue": audit.venue.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = audit.payload_sha256
    return document


def _timeline_checkpoint_document(
    checkpoint: FeeTimelineCheckpointV2,
) -> dict[str, object]:
    return {
        "manifest_sha256": checkpoint.manifest_sha256,
        "observed_through_ms": checkpoint.observed_through_ms,
        "qualification_final": checkpoint.qualification_final,
        "registry_event_count": checkpoint.registry_event_count,
        "registry_replay_root_sha256": checkpoint.registry_replay_root_sha256,
        "rule_version": checkpoint.rule_version,
        "schema_version": _TIMELINE_SCHEMA,
        "scope": _scope_document(checkpoint.scope),
        "sealed_at_ms": checkpoint.sealed_at_ms,
        "timeline_capture_count": checkpoint.timeline_capture_count,
        "timeline_root_sha256": checkpoint.timeline_root_sha256,
        "venue": checkpoint.venue.value,
    }


def _resolution_identity_document(
    resolution: FeeVersionResolutionV2,
) -> dict[str, object]:
    return {
        "manifest_sha256": resolution.manifest_sha256,
        "position_event_id": resolution.position_event_id,
        "rule_version": resolution.rule_version,
        "scope": _scope_document(resolution.scope),
        "symbol": resolution.symbol,
        "target_ms": resolution.target_ms,
        "venue": resolution.venue.value,
    }


def _resolution_document(
    resolution: FeeVersionResolutionV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        **_resolution_identity_document(resolution),
        "cadence_audit_sha256": resolution.cadence_audit_sha256,
        "event_id": resolution.event_id,
        "horizon_end_ms": resolution.horizon_end_ms,
        "parsed_version_sha256": resolution.parsed_version_sha256,
        "reasons": list(resolution.reasons),
        "scenario": resolution.scenario,
        "schema_version": _RESOLUTION_SCHEMA,
        "source_capture_event_id": resolution.source_capture_event_id,
        "status": resolution.status.value,
        "taker_rate_decimal": (
            None
            if resolution.taker_rate is None
            else _decimal_text(resolution.taker_rate)
        ),
        "timeline_capture_count": resolution.timeline_capture_count,
        "timeline_checkpoint_sha256": resolution.timeline_checkpoint_sha256,
        "timeline_observed_through_ms": resolution.timeline_observed_through_ms,
        "timeline_qualification_final": resolution.timeline_qualification_final,
        "timeline_root_sha256": resolution.timeline_root_sha256,
        "use": resolution.use.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = resolution.payload_sha256
    return document


def _fee_cost_document(
    fee: FilledBothLegFeeV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "actual_private_account_fee_claim": fee.actual_private_account_fee_claim,
        "both_entry_and_exit_charged": fee.both_entry_and_exit_charged,
        "entry_fee_decimal": _decimal_text(fee.entry_fee),
        "entry_filled_notional_decimal": _decimal_text(fee.entry_filled_notional),
        "entry_resolution_event_id": fee.entry_resolution_event_id,
        "entry_taker_rate_decimal": _decimal_text(fee.entry_taker_rate),
        "exit_fee_decimal": _decimal_text(fee.exit_fee),
        "exit_filled_notional_decimal": _decimal_text(fee.exit_filled_notional),
        "exit_resolution_event_id": fee.exit_resolution_event_id,
        "exit_taker_rate_decimal": _decimal_text(fee.exit_taker_rate),
        "final_timeline_capture_count": fee.final_timeline_capture_count,
        "final_timeline_checkpoint_sha256": fee.final_timeline_checkpoint_sha256,
        "final_timeline_observed_through_ms": (
            fee.final_timeline_observed_through_ms
        ),
        "final_timeline_root_sha256": fee.final_timeline_root_sha256,
        "mandatory_adverse_report": fee.mandatory_adverse_report,
        "multiplier": fee.multiplier.value,
        "position_event_id": fee.position_event_id,
        "primary_cell": fee.primary_cell,
        "rule_version": fee.rule_version,
        "scenario": fee.scenario,
        "schema_version": _COST_SCHEMA,
        "scope": _scope_document(fee.scope),
        "symbol": fee.symbol,
        "total_fee_decimal": _decimal_text(fee.total_fee),
        "use": fee.use.value,
        "venue": fee.venue.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = fee.payload_sha256
    return document


def _filled_fee_slice_document(
    fee_slice: FilledFeeSliceV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "execution_event_id": fee_slice.execution_event_id,
        "execution_payload_sha256": fee_slice.execution_payload_sha256,
        "fill_event_ms": fee_slice.fill_event_ms,
        "filled_quantity_decimal": _decimal_text(fee_slice.filled_quantity),
        "generation_event_id": fee_slice.generation_event_id,
        "generation_evidence_sha256": fee_slice.generation_evidence_sha256,
        "gross_notional_decimal": _decimal_text(fee_slice.gross_notional),
        "kind": fee_slice.kind.value,
        "multiplier": fee_slice.multiplier.value,
        "position_event_id": fee_slice.position_event_id,
        "realized_fee_decimal": (
            None
            if fee_slice.realized_fee is None
            else _decimal_text(fee_slice.realized_fee)
        ),
        "resolution_event_id": fee_slice.resolution_event_id,
        "resolution_payload_sha256": fee_slice.resolution_payload_sha256,
        "resolution_status": fee_slice.resolution_status.value,
        "rule_version": fee_slice.rule_version,
        "schema_version": _SLICE_COST_SCHEMA,
        "taker_rate_decimal": (
            None
            if fee_slice.taker_rate is None
            else _decimal_text(fee_slice.taker_rate)
        ),
    }
    if include_payload_hash:
        document["payload_sha256"] = fee_slice.payload_sha256
    return document


def _filled_position_fee_document(
    fee: FilledPositionFeeV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "actual_private_account_fee_claim": fee.actual_private_account_fee_claim,
        "entry_slice": _filled_fee_slice_document(
            fee.entry_slice,
            include_payload_hash=True,
        ),
        "exit_slices": [
            _filled_fee_slice_document(item, include_payload_hash=True)
            for item in fee.exit_slices
        ],
        "final_timeline_capture_count": fee.final_timeline_capture_count,
        "final_timeline_checkpoint_sha256": fee.final_timeline_checkpoint_sha256,
        "final_timeline_observed_through_ms": (
            fee.final_timeline_observed_through_ms
        ),
        "final_timeline_root_sha256": fee.final_timeline_root_sha256,
        "known_realized_entry_fee_decimal": _decimal_text(
            fee.known_realized_entry_fee
        ),
        "known_realized_exit_fee_decimal": _decimal_text(
            fee.known_realized_exit_fee
        ),
        "known_realized_total_fee_decimal": _decimal_text(
            fee.known_realized_total_fee
        ),
        "legacy_single_exit_fee": (
            None
            if fee.legacy_single_exit_fee is None
            else json.loads(
                canonical_filled_both_leg_fee_v2(fee.legacy_single_exit_fee)
            )
        ),
        "mandatory_exit_fee_certificate_sha256": (
            fee.mandatory_exit_fee_certificate_sha256
        ),
        "multiplier": fee.multiplier.value,
        "position_event_id": fee.position_event_id,
        "residual_quantity_decimal": _decimal_text(fee.residual_quantity),
        "rule_version": fee.rule_version,
        "scenario": fee.scenario,
        "schema_version": _POSITION_COST_SCHEMA,
        "scope": _scope_document(fee.scope),
        "status": fee.status.value,
        "symbol": fee.symbol,
        "terminal_status": fee.terminal_status,
        "unresolved_slice_count": fee.unresolved_slice_count,
        "use": fee.use.value,
        "venue": fee.venue.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = fee.payload_sha256
    return document


def _transport_document(transport: FeeHttpCaptureEnvelopeV2) -> dict[str, object]:
    return {
        "account_authenticated": transport.account_authenticated,
        "authorization_header_present": transport.authorization_header_present,
        "content_type": transport.content_type,
        "final_url": transport.final_url,
        "http_status": transport.http_status,
        "ingest_seq": transport.ingest_seq,
        "raw_or_dom_kind": transport.raw_or_dom_kind.value,
        "raw_or_dom_len": transport.raw_or_dom_len,
        "raw_or_dom_sha256": transport.raw_or_dom_sha256,
        "receipt_monotonic_ns": transport.receipt_monotonic_ns,
        "request_id": transport.request_id,
        "request_started_ms": transport.request_started_ms,
        "request_url": transport.request_url,
        "response_completion_ms": transport.response_completion_ms,
        "rule_version": transport.rule_version,
        "schema_version": _TRANSPORT_SCHEMA,
        "scope": _scope_document(transport.scope),
        "tls_verified": transport.tls_verified,
        "venue": transport.venue.value,
    }


def _transport_sha256(transport: FeeHttpCaptureEnvelopeV2) -> str:
    return hashlib.sha256(canonical_json_line(_transport_document(transport))).hexdigest()


def _capture_ledger_checkpoint_document(
    checkpoint: FeeCaptureLedgerCheckpointV2,
    *,
    include_checkpoint_hash: bool = False,
) -> dict[str, object]:
    document: dict[str, object] = {
        "event_count": checkpoint.event_count,
        "ledger_id": checkpoint.ledger_id,
        "ledger_root_sha256": checkpoint.ledger_root_sha256,
        "observed_through_ms": checkpoint.observed_through_ms,
        "rule_version": checkpoint.rule_version,
        "schema_version": _CAPTURE_LEDGER_CHECKPOINT_SCHEMA,
        "scope": _scope_document(checkpoint.scope),
        "sealed_at_ms": checkpoint.sealed_at_ms,
    }
    if include_checkpoint_hash:
        document["checkpoint_sha256"] = checkpoint.checkpoint_sha256
    return document


def _scope_document(scope: FeeProtocolScopeV2) -> dict[str, object]:
    return {
        "attempt_id": scope.attempt_id,
        "plan_id": scope.plan_id,
        "protocol_hash": scope.protocol_hash,
        "universe_sha256": scope.universe_sha256,
    }


def _timeline_root(captures: Sequence[FeePageCaptureEvidenceV2]) -> str:
    return hashlib.sha256(
        _TIMELINE_ROOT_DOMAIN
        + canonical_json_line(
            {
                "captures": [
                    {
                        "archive_root_sha256": item.archive_root_sha256,
                        "event_id": item.event_id,
                        "payload_sha256": item.payload_sha256,
                        "response_completion_ms": item.response_completion_ms,
                    }
                    for item in captures
                ]
            }
        )
    ).hexdigest()


def _registry_order_key(capture: FeePageCaptureEvidenceV2) -> tuple[int, str, int, str]:
    role_rank = 0 if capture.capture_role is FeeCaptureRoleV2.PRE_T0 else 1
    scheduled = -1 if capture.poll_scheduled_ms is None else capture.poll_scheduled_ms
    return role_rank, capture.venue.value, scheduled, capture.event_id


def _registry_state_row(capture: FeePageCaptureEvidenceV2) -> dict[str, object]:
    archive = canonical_fee_capture_archive_v2(capture)
    return {
        "archive_base64": _b64(archive),
        "archive_sha256": hashlib.sha256(archive).hexdigest(),
        "event_id": capture.event_id,
        "order_key": list(_registry_order_key(capture)),
        "payload_sha256": capture.payload_sha256,
    }


def _registry_replay_root(rows: list[dict[str, object]]) -> str:
    return hashlib.sha256(
        _REGISTRY_REPLAY_DOMAIN + canonical_json_line({"events": rows})
    ).hexdigest()


def _parse_registry_state_row(
    raw_row: object,
) -> tuple[dict[str, object], FeePageCaptureEvidenceV2, tuple[int, str, int, str]]:
    if not isinstance(raw_row, dict) or set(raw_row) != {
        "archive_base64",
        "archive_sha256",
        "event_id",
        "order_key",
        "payload_sha256",
    }:
        raise FeeContractErrorV2("registry row schema is unsupported")
    archive = _decode_b64(raw_row.get("archive_base64"), "archive_base64")
    archive_sha = raw_row.get("archive_sha256")
    _validate_sha256(archive_sha, "archive_sha256")
    if archive_sha != hashlib.sha256(archive).hexdigest():
        raise FeeContractErrorV2("registry archive hash mismatch")
    capture = _capture_from_archive(archive)
    row = _registry_state_row(capture)
    if row != raw_row:
        raise FeeContractErrorV2("registry row is noncanonical or contradictory")
    order_key = _registry_order_key(capture)
    return row, capture, order_key


def _capture_from_archive(payload: bytes) -> FeePageCaptureEvidenceV2:
    document = _parse_canonical_json(payload, "fee capture archive")
    if set(document) != {
        "archive_root_manifest_base64",
        "capture_payload",
        "ledger_merkle_siblings",
        "parsed_json_base64",
        "png_base64",
        "raw_or_dom_base64",
        "schema_version",
        "transport",
    } or document.get("schema_version") != _ARCHIVE_SCHEMA:
        raise FeeContractErrorV2("fee capture archive schema is unsupported")
    transport_doc = document.get("transport")
    if not isinstance(transport_doc, dict):
        raise FeeContractErrorV2("archive transport descriptor is invalid")
    scope = _scope_from_document(transport_doc.get("scope"))
    try:
        venue = VenueV2(transport_doc.get("venue"))
        artifact_kind = FeeCaptureArtifactKindV2(transport_doc.get("raw_or_dom_kind"))
    except (TypeError, ValueError) as exc:
        raise FeeContractErrorV2("archive transport enum is invalid") from exc
    raw = _decode_b64(document.get("raw_or_dom_base64"), "raw_or_dom_base64")
    transport = FeeHttpCaptureEnvelopeV2(
        scope=scope,
        venue=venue,
        request_id=_require_str(transport_doc.get("request_id"), "request_id"),
        request_url=_require_str(transport_doc.get("request_url"), "request_url"),
        final_url=_require_str(transport_doc.get("final_url"), "final_url"),
        request_started_ms=_require_int(
            transport_doc.get("request_started_ms"),
            "request_started_ms",
        ),
        response_completion_ms=_require_int(
            transport_doc.get("response_completion_ms"),
            "response_completion_ms",
        ),
        receipt_monotonic_ns=_require_int(
            transport_doc.get("receipt_monotonic_ns"),
            "receipt_monotonic_ns",
        ),
        ingest_seq=_require_int(transport_doc.get("ingest_seq"), "ingest_seq"),
        http_status=_require_int(transport_doc.get("http_status"), "http_status"),
        content_type=_require_str(transport_doc.get("content_type"), "content_type"),
        tls_verified=_require_bool(transport_doc.get("tls_verified"), "tls_verified"),
        account_authenticated=_require_bool(
            transport_doc.get("account_authenticated"),
            "account_authenticated",
        ),
        authorization_header_present=_require_bool(
            transport_doc.get("authorization_header_present"),
            "authorization_header_present",
        ),
        raw_or_dom_kind=artifact_kind,
        raw_or_dom_bytes=raw,
    )
    if _transport_document(transport) != transport_doc:
        raise FeeContractErrorV2("archive transport descriptor is contradictory")
    capture_doc = document.get("capture_payload")
    if not isinstance(capture_doc, dict):
        raise FeeContractErrorV2("archive capture payload is invalid")
    checkpoint_doc = _archive_checkpoint_document(document)
    checkpoint_scope = _scope_from_document(checkpoint_doc.get("scope"))
    checkpoint = FeeCaptureLedgerCheckpointV2(
        scope=checkpoint_scope,
        ledger_id=_require_str(checkpoint_doc.get("ledger_id"), "ledger_id"),
        ledger_root_sha256=_require_str(
            checkpoint_doc.get("ledger_root_sha256"),
            "ledger_root_sha256",
        ),
        event_count=_require_int(checkpoint_doc.get("event_count"), "event_count"),
        observed_through_ms=_require_int(
            checkpoint_doc.get("observed_through_ms"),
            "observed_through_ms",
        ),
        sealed_at_ms=_require_int(checkpoint_doc.get("sealed_at_ms"), "sealed_at_ms"),
    )
    if checkpoint_doc != _capture_ledger_checkpoint_document(
        checkpoint,
        include_checkpoint_hash=True,
    ):
        raise FeeContractErrorV2("archive ledger checkpoint is contradictory")
    try:
        role = FeeCaptureRoleV2(capture_doc.get("capture_role"))
    except (TypeError, ValueError) as exc:
        raise FeeContractErrorV2("archive capture role is invalid") from exc
    siblings_raw = document.get("ledger_merkle_siblings")
    if not isinstance(siblings_raw, list) or not all(
        isinstance(item, str) for item in siblings_raw
    ):
        raise FeeContractErrorV2("archive Merkle siblings are invalid")
    capture = build_fee_page_capture_v2(
        transport=transport,
        capture_role=role,
        png_bytes=_decode_b64(document.get("png_base64"), "png_base64"),
        ledger_checkpoint=checkpoint,
        ledger_leaf_index=_require_int(
            capture_doc.get("ledger_leaf_index"),
            "ledger_leaf_index",
        ),
        ledger_merkle_siblings=tuple(siblings_raw),
        expected_ledger_checkpoint_sha256=checkpoint.checkpoint_sha256,
        poll_sequence=_optional_int(capture_doc.get("poll_sequence"), "poll_sequence"),
        poll_scheduled_ms=_optional_int(
            capture_doc.get("poll_scheduled_ms"),
            "poll_scheduled_ms",
        ),
    )
    if capture_doc != _capture_document(capture, include_payload_hash=True):
        raise FeeContractErrorV2("archive capture payload is contradictory")
    parsed = _decode_b64(document.get("parsed_json_base64"), "parsed_json_base64")
    if parsed != capture.parsed_json_bytes:
        raise FeeContractErrorV2("archive parsed JSON bytes differ from strict parse")
    archive_manifest = _decode_b64(
        document.get("archive_root_manifest_base64"),
        "archive_root_manifest_base64",
    )
    if archive_manifest != capture.archive_root_manifest_bytes:
        raise FeeContractErrorV2("archive root manifest bytes are contradictory")
    if canonical_fee_capture_archive_v2(capture) != payload:
        raise FeeContractErrorV2("fee capture archive is noncanonical")
    return capture


def _archive_checkpoint_document(archive_document: dict[str, object]) -> dict[str, object]:
    encoded = archive_document.get("archive_root_manifest_base64")
    manifest = _parse_canonical_json(
        _decode_b64(encoded, "archive_root_manifest_base64"),
        "archive root manifest",
    )
    checkpoint = manifest.get("ledger_checkpoint")
    if not isinstance(checkpoint, dict):
        raise FeeContractErrorV2("archive root has no ledger checkpoint")
    return checkpoint


def _scope_from_document(raw: object) -> FeeProtocolScopeV2:
    if not isinstance(raw, dict) or set(raw) != {
        "attempt_id",
        "plan_id",
        "protocol_hash",
        "universe_sha256",
    }:
        raise FeeContractErrorV2("scope document is invalid")
    return FeeProtocolScopeV2(
        attempt_id=_require_str(raw.get("attempt_id"), "attempt_id"),
        plan_id=_require_str(raw.get("plan_id"), "plan_id"),
        protocol_hash=_require_str(raw.get("protocol_hash"), "protocol_hash"),
        universe_sha256=_require_str(raw.get("universe_sha256"), "universe_sha256"),
    )


def _parsed_version_sha256(
    *,
    transport: FeeHttpCaptureEnvelopeV2,
    parsed_json_sha256: str,
    parsed_taker_rate: Decimal,
) -> str:
    if not isinstance(transport, FeeHttpCaptureEnvelopeV2):
        raise FeeContractErrorV2("transport must be FeeHttpCaptureEnvelopeV2")
    _validate_sha256(parsed_json_sha256, "parsed_json_sha256")
    _validate_nonnegative_decimal(parsed_taker_rate, "parsed_taker_rate")
    return hashlib.sha256(
        _PARSED_VERSION_DOMAIN
        + canonical_json_line(
            {
                "official_source_url": _official_url(transport.venue),
                "parsed_json_sha256": parsed_json_sha256,
                "parsed_taker_rate_decimal": _decimal_text(parsed_taker_rate),
                "raw_or_dom_kind": transport.raw_or_dom_kind.value,
                "raw_or_dom_sha256": transport.raw_or_dom_sha256,
                "schema_version": _PARSED_SCHEMA,
                "venue": transport.venue.value,
            }
        )
    ).hexdigest()


def _verify_merkle_membership(
    *,
    leaf_sha256: str,
    leaf_index: int,
    event_count: int,
    siblings: tuple[str, ...],
    expected_root_sha256: str,
) -> None:
    _validate_sha256(leaf_sha256, "ledger leaf")
    _validate_nonnegative_int(leaf_index, "ledger_leaf_index")
    _validate_positive_int(event_count, "ledger event_count")
    _validate_sha256(expected_root_sha256, "ledger root")
    if leaf_index >= event_count:
        raise FeeContractErrorV2("ledger leaf index exceeds event census")
    if type(siblings) is not tuple:
        raise FeeContractErrorV2("ledger Merkle siblings must be tuple")
    expected_depth = 0
    width = event_count
    while width > 1:
        expected_depth += 1
        width = (width + 1) // 2
    if len(siblings) != expected_depth:
        raise FeeContractErrorV2("ledger Merkle proof depth differs from census")
    current = leaf_sha256
    index = leaf_index
    width = event_count
    for sibling in siblings:
        _validate_sha256(sibling, "ledger Merkle sibling")
        if width % 2 and index == width - 1 and sibling != current:
            raise FeeContractErrorV2("odd Merkle leaf must duplicate itself")
        current = (
            _merkle_parent(current, sibling)
            if index % 2 == 0
            else _merkle_parent(sibling, current)
        )
        index //= 2
        width = (width + 1) // 2
    if current != expected_root_sha256:
        raise FeeContractErrorV2("capture is not a member of external ledger root")


def _merkle_parent(left: str, right: str) -> str:
    return hashlib.sha256(
        _CAPTURE_LEDGER_NODE_DOMAIN + bytes.fromhex(left) + bytes.fromhex(right)
    ).hexdigest()


def _same_parsed_version(
    left: FeePageCaptureEvidenceV2,
    right: FeePageCaptureEvidenceV2,
) -> bool:
    return (
        left.parsed_version_sha256 == right.parsed_version_sha256
        and left.parsed_taker_rate == right.parsed_taker_rate
    )


def _first_missing_sequence(
    completed: tuple[int, ...],
    expected_due_count: int,
) -> int | None:
    expected = 1
    for sequence in completed:
        if sequence > expected:
            break
        if sequence == expected:
            expected += 1
    return expected if expected <= expected_due_count else None


def _due_poll_sequences(
    captures: Sequence[FeePageCaptureEvidenceV2],
    *,
    t0_ms: int,
    observed_through_ms: int,
) -> tuple[int, tuple[int, ...]]:
    expected_count = (
        observed_through_ms - t0_ms
    ) // FEE_POLL_CADENCE_MS_V2
    completed = tuple(
        sorted(
            capture.poll_sequence
            for capture in captures
            if capture.capture_role is FeeCaptureRoleV2.POST_T0_POLL
            and capture.poll_sequence is not None
            and capture.poll_sequence <= expected_count
        )
    )
    return expected_count, completed


def _official_url(venue: VenueV2) -> str:
    _validate_venue(venue)
    return (
        SPOT_OFFICIAL_FEE_URL_V2
        if venue is VenueV2.SPOT
        else USDM_OFFICIAL_FEE_URL_V2
    )


def _fee_use(venue: VenueV2) -> FeeUseV2:
    _validate_venue(venue)
    return (
        FeeUseV2.DIAGNOSTIC_SPOT
        if venue is VenueV2.SPOT
        else FeeUseV2.PROMOTING_USDM_FUTURES
    )


def _validate_capture_slot_values(
    capture_role: FeeCaptureRoleV2,
    *,
    poll_sequence: int | None,
    poll_scheduled_ms: int | None,
    request_started_ms: int,
    response_completion_ms: int,
) -> None:
    if capture_role is FeeCaptureRoleV2.PRE_T0:
        if poll_sequence is not None or poll_scheduled_ms is not None:
            raise FeeContractErrorV2("PRE_T0 capture cannot claim a poll slot")
        return
    if type(poll_sequence) is not int or poll_sequence < 1:
        raise FeeContractErrorV2("POST_T0_POLL requires positive poll_sequence")
    if type(poll_scheduled_ms) is not int or poll_scheduled_ms < 0:
        raise FeeContractErrorV2("POST_T0_POLL requires poll_scheduled_ms")
    if request_started_ms != poll_scheduled_ms:
        raise FeeContractErrorV2(
            "post-T0 request must start at the exact sealed poll schedule"
        )
    if response_completion_ms < poll_scheduled_ms:
        raise FeeContractErrorV2("post-T0 response completed before scheduled poll")


def _verify_artifact(payload: bytes, digest: str, label: str) -> None:
    _validate_sha256(digest, f"{label}_sha256")
    if hashlib.sha256(payload).hexdigest() != digest:
        raise FeeContractErrorV2(f"{label} artifact hash mismatch")


def _sha256_bytes(payload: bytes, label: str, *, maximum_size: int) -> str:
    if type(payload) is not bytes or not payload:
        raise FeeContractErrorV2(f"{label} must be non-empty immutable bytes")
    if len(payload) > maximum_size:
        raise FeeContractErrorV2(f"{label} exceeds bounded artifact size")
    return hashlib.sha256(payload).hexdigest()


def _decimal_text(value: Decimal) -> str:
    _validate_nonnegative_decimal(value, "Decimal value")
    return format(value, "f")


def _validate_nonnegative_decimal(value: Decimal | None, field_name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value < 0:
        raise FeeContractErrorV2(
            f"{field_name} must be a nonnegative finite Decimal"
        )


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise FeeContractErrorV2(f"{field_name} must be a nonnegative integer")


def _validate_positive_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise FeeContractErrorV2(f"{field_name} must be a positive integer")


def _validate_venue(venue: VenueV2) -> None:
    if venue not in (VenueV2.SPOT, VenueV2.USDM_FUTURES):
        raise FeeContractErrorV2("fee venue must be Spot or USD-M Futures")


def _validate_symbol(value: str) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise FeeContractErrorV2("symbol must be an uppercase normalized market symbol")


def _validate_identity(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise FeeContractErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )


def _validate_sha256(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise FeeContractErrorV2(
            f"{field_name} must be a lowercase SHA-256 digest"
        )


def _normalize_space(value: str) -> str:
    return " ".join(value.split())


def _b64(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def _decode_b64(value: object, field_name: str) -> bytes:
    if not isinstance(value, str):
        raise FeeContractErrorV2(f"{field_name} must be base64 text")
    try:
        payload = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise FeeContractErrorV2(f"{field_name} is invalid base64") from exc
    if _b64(payload) != value:
        raise FeeContractErrorV2(f"{field_name} is noncanonical base64")
    return payload


def _parse_canonical_json(payload: bytes, label: str) -> dict[str, object]:
    if type(payload) is not bytes or not payload:
        raise FeeContractErrorV2(f"{label} must be non-empty bytes")
    try:
        document = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise FeeContractErrorV2(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(document, dict) or canonical_json_line(document) != payload:
        raise FeeContractErrorV2(f"{label} must be canonical JSONL")
    return document


def _require_str(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise FeeContractErrorV2(f"{field_name} must be text")
    return value


def _require_int(value: object, field_name: str) -> int:
    if type(value) is not int:
        raise FeeContractErrorV2(f"{field_name} must be integer")
    return value


def _optional_int(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    return _require_int(value, field_name)


def _require_bool(value: object, field_name: str) -> bool:
    if type(value) is not bool:
        raise FeeContractErrorV2(f"{field_name} must be bool")
    return value
