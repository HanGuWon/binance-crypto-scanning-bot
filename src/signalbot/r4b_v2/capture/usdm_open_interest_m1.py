from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import InitVar, asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final, Literal, cast

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import StorageRootBindingV2
from signalbot.r4b_v2.capture.block_container import BlockSigningAuthorityV2
from signalbot.r4b_v2.capture.blocks import BlockPolicyV2
from signalbot.r4b_v2.capture.integrity_ledger import CaptureIntegrityLedgerV2
from signalbot.r4b_v2.capture.membership import (
    VerifiedRawMembershipLeafV2,
    reverify_verified_raw_membership_leaf_v2,
)
from signalbot.r4b_v2.capture.models import TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingRestCapturePlanV2,
    provisional_promoting_plan_sha256_v2,
    validate_provisional_promoting_capture_plans_v2,
)
from signalbot.r4b_v2.capture.rest import (
    PUBLIC_OI_REST_POLL_INTERVAL_MS_V2,
    PublicOiRestAttemptPayloadV2,
    public_oi_rest_source_logical_key_v2,
)
from signalbot.r4b_v2.capture.rest_census import (
    public_oi_rest_plan_sha256_v2,
    public_oi_rest_symbol_census_sha256_v2,
)
from signalbot.r4b_v2.capture.rest_open_interest_semantics import (
    PUBLIC_OI_REST_BODY_EXTRA_FIELDS_POLICY_V2,
    PUBLIC_OI_REST_BODY_SEMANTIC_SCOPE_V2,
    verify_public_oi_rest_attempt_body_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

_SCHEMA_VERSION = "r4b_v2_usdm_open_interest_m1_v1"
_ROW_HASH_DOMAIN = b"R4B_V2_USDM_OPEN_INTEREST_M1_ROW\0"
_FACTORY_TOKEN = object()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_MAX_IDENTITY_LENGTH = 256
_MAX_SIGNED_INT64 = (1 << 63) - 1
_MAX_CANONICAL_INTEGER = (1 << 53) - 1

USDM_OPEN_INTEREST_M1_ONLY_REASON_V2: Final = (
    "STRICT_M0_M1_OI_BODY_REQUIRES_SCHEDULE_CELL_FRESHNESS_AND_M2_CAUSAL_CURSOR"
)

_PARSER_CONTRACT = {
    "attempt_schema": "r4b_v2_public_oi_rest_attempt_v2",
    "body_extra_fields_policy": PUBLIC_OI_REST_BODY_EXTRA_FIELDS_POLICY_V2,
    "body_semantic_scope": PUBLIC_OI_REST_BODY_SEMANTIC_SCOPE_V2,
    "completion_binding": "PAYLOAD_COMPLETION_EQUALS_OUTER_RAW_RECEIPT",
    "freshness_policy": "UNVERIFIED_NO_AUTHENTICATED_BINANCE_CLOCK_OFFSET",
    "method": "GET",
    "m2_policy": "NO_SOURCE_CENSUS_OR_CAUSAL_CURSOR_CLAIM",
    "query": "EXACT_SYMBOL_ONLY",
    "route_id": "usdm_public_rest",
    "schedule_binding": "DEFER_TO_FINALIZED_CENSUS_CERTIFICATE",
    "schema_version": _SCHEMA_VERSION,
    "transport": "https",
    "venue": "usdm_futures",
}
USDM_OPEN_INTEREST_M1_PARSER_CONTRACT_SHA256_V2: Final = hashlib.sha256(
    canonical_json_line(_PARSER_CONTRACT)
).hexdigest()


class UsdmOpenInterestM1ContractErrorV2(RuntimeError):
    """One retained public OI member fails its exact M0/M1 contract."""


@dataclass(frozen=True, slots=True)
class UsdmOpenInterestM1V2:
    """Factory-only OI body interpretation with live-reverified M0 lineage.

    This row proves exact interpretation of one retained successful response.
    It deliberately does not prove that the response belongs to a complete
    schedule cell, is economically fresh, or is covered by an M2 cursor.
    """

    symbol: str
    venue: VenueV2
    route_id: Literal["usdm_public_rest"]
    promoting_plan_sha256: str
    rest_plan_sha256: str
    symbol_census_sha256: str
    capture_authority_sha256: str
    protocol_sha256: str
    parser_contract_sha256: str
    m0_leaf_sha256: str
    raw_payload_hash_v2: str
    attempt_payload_sha256: str
    session_id: str
    plan_id: str
    connection_id: str
    generation: int
    ingest_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    poll_cycle_seq: int
    symbol_ordinal: int
    scheduled_slot_wall_ms: int
    attempt: Literal[1]
    request_started_wall_ms: int
    request_started_monotonic_ns: int
    response_first_header_wall_ms: int
    response_first_header_monotonic_ns: int
    attempt_ended_wall_ms: int
    attempt_ended_monotonic_ns: int
    completion_admission_wall_ms: int
    completion_admission_monotonic_ns: int
    open_interest_text: str
    open_interest: Decimal
    transaction_time_ms: int
    body_sha256: str
    _factory_token: InitVar[object | None] = None
    m1_payload_sha256: str = field(init=False, default="")
    schema_version: Literal["r4b_v2_usdm_open_interest_m1_v1"] = field(
        init=False,
        default=_SCHEMA_VERSION,
    )
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise UsdmOpenInterestM1ContractErrorV2(
                "USD-M open-interest M1 rows are factory-sealed"
            )
        _validate_row(self)
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        object.__setattr__(self, "m1_payload_sha256", _row_hash(self))

    @property
    def parser_bound(self) -> Literal[True]:
        return True

    @property
    def body_semantics_verified(self) -> Literal[True]:
        return True

    @property
    def completion_receipt_bound(self) -> Literal[True]:
        return True

    @property
    def live_reverification_required(self) -> Literal[True]:
        return True

    @property
    def current_authority_claimed(self) -> Literal[False]:
        return False

    @property
    def schedule_cell_bound(self) -> Literal[False]:
        return False

    @property
    def freshness_verified(self) -> Literal[False]:
        return False

    @property
    def transaction_time_causally_bounded(self) -> Literal[False]:
        return False

    @property
    def cursor_complete(self) -> Literal[False]:
        return False

    @property
    def causal_inputs_complete(self) -> Literal[False]:
        return False

    @property
    def authority_reason(self) -> str:
        return USDM_OPEN_INTEREST_M1_ONLY_REASON_V2

    @property
    def source_evidence_sha256(self) -> str:
        return self.m1_payload_sha256


def parse_verified_usdm_open_interest_m1_v2(
    leaf: VerifiedRawMembershipLeafV2,
    *,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
    block_directory: str | Path,
    block_root_binding: StorageRootBindingV2,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    integrity_ledger: CaptureIntegrityLedgerV2,
) -> UsdmOpenInterestM1V2:
    """Live-reverify one M0 OI attempt and parse its exact successful body."""

    if not isinstance(leaf, VerifiedRawMembershipLeafV2):
        raise TypeError("leaf must be a VerifiedRawMembershipLeafV2")
    # Take one immutable ownership snapshot before validation, hashing, and
    # owner selection.  Reiterating a caller-owned mutable Sequence could bind
    # the authority hash to one plan set and parse under another.
    frozen_plans = tuple(promoting_plans)
    validate_provisional_promoting_capture_plans_v2(frozen_plans)
    plan_bundle_sha256 = provisional_promoting_plan_sha256_v2(frozen_plans)
    if authority.plan_sha256 != plan_bundle_sha256:
        raise UsdmOpenInterestM1ContractErrorV2(
            "trusted WAL authority differs from the frozen promoting plan"
        )
    rest_plans = tuple(
        cast(ProvisionalPromotingRestCapturePlanV2, item)
        for item in frozen_plans
        if type(item) is ProvisionalPromotingRestCapturePlanV2
    )
    if len(rest_plans) != 1:
        raise UsdmOpenInterestM1ContractErrorV2(
            "promoting plan has no unique USD-M public OI owner"
        )
    rest_plan = rest_plans[0]
    record = leaf.record
    if record.symbol is None:
        raise UsdmOpenInterestM1ContractErrorV2("OI attempt M1 requires an outer symbol")

    reverify_verified_raw_membership_leaf_v2(
        leaf,
        block_directory=block_directory,
        block_root_binding=block_root_binding,
        authority=authority,
        policy=policy,
        signing_authority=signing_authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        integrity_ledger=integrity_ledger,
        expected_transport=TransportV2.HTTPS,
        expected_venue=VenueV2.USDM_FUTURES,
        expected_route_id="usdm_public_rest",
        expected_symbol=record.symbol,
    )
    if record.plan_id != rest_plan.name:
        raise UsdmOpenInterestM1ContractErrorV2(
            "raw record plan_id differs from the public OI plan"
        )
    if record.frame_seq is not None:
        raise UsdmOpenInterestM1ContractErrorV2(
            "HTTPS OI attempt must not carry a WebSocket frame sequence"
        )
    if record.source_logical_key != public_oi_rest_source_logical_key_v2(record.symbol):
        raise UsdmOpenInterestM1ContractErrorV2(
            "OI source logical key differs from its exact symbol"
        )
    try:
        payload = PublicOiRestAttemptPayloadV2.from_canonical_bytes(
            record.payload_bytes(),
            plan=rest_plan,
        )
        verified_body = verify_public_oi_rest_attempt_body_v2(payload)
    except (TypeError, ValueError) as exc:
        raise UsdmOpenInterestM1ContractErrorV2(
            "retained public OI attempt or body fails its exact semantic contract"
        ) from exc
    if payload.symbol != record.symbol or verified_body.symbol != record.symbol:
        raise UsdmOpenInterestM1ContractErrorV2(
            "OI payload or body symbol differs from its raw envelope"
        )
    if (
        payload.completion_admission_wall_ms != record.receipt_wall_ms
        or payload.completion_admission_monotonic_ns != record.receipt_monotonic_ns
    ):
        raise UsdmOpenInterestM1ContractErrorV2(
            "OI completion admission differs from the outer raw receipt"
        )
    first_header_wall_ms = payload.response_first_header_wall_ms
    first_header_monotonic_ns = payload.response_first_header_monotonic_ns
    if first_header_wall_ms is None or first_header_monotonic_ns is None:
        raise UsdmOpenInterestM1ContractErrorV2(
            "successful OI response requires first-header clocks"
        )

    return UsdmOpenInterestM1V2(
        symbol=record.symbol,
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public_rest",
        promoting_plan_sha256=plan_bundle_sha256,
        rest_plan_sha256=public_oi_rest_plan_sha256_v2(rest_plan),
        symbol_census_sha256=public_oi_rest_symbol_census_sha256_v2(rest_plan),
        capture_authority_sha256=leaf.authority_sha256,
        protocol_sha256=record.protocol_hash,
        parser_contract_sha256=USDM_OPEN_INTEREST_M1_PARSER_CONTRACT_SHA256_V2,
        m0_leaf_sha256=leaf.leaf_sha256,
        raw_payload_hash_v2=leaf.raw_payload_hash_v2,
        attempt_payload_sha256=hashlib.sha256(record.payload_bytes()).hexdigest(),
        session_id=record.session_id,
        plan_id=record.plan_id,
        connection_id=record.connection_id,
        generation=record.generation,
        ingest_seq=record.ingest_seq,
        receipt_wall_ms=record.receipt_wall_ms,
        receipt_monotonic_ns=record.receipt_monotonic_ns,
        poll_cycle_seq=payload.poll_cycle_seq,
        symbol_ordinal=payload.symbol_ordinal,
        scheduled_slot_wall_ms=payload.scheduled_slot_wall_ms,
        attempt=1,
        request_started_wall_ms=payload.request_started_wall_ms,
        request_started_monotonic_ns=payload.request_started_monotonic_ns,
        response_first_header_wall_ms=first_header_wall_ms,
        response_first_header_monotonic_ns=first_header_monotonic_ns,
        attempt_ended_wall_ms=payload.attempt_ended_wall_ms,
        attempt_ended_monotonic_ns=payload.attempt_ended_monotonic_ns,
        completion_admission_wall_ms=payload.completion_admission_wall_ms,
        completion_admission_monotonic_ns=(payload.completion_admission_monotonic_ns),
        open_interest_text=verified_body.open_interest_text,
        open_interest=verified_body.open_interest,
        transaction_time_ms=verified_body.transaction_time_ms,
        body_sha256=verified_body.body_sha256,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_usdm_open_interest_m1_v2(row: UsdmOpenInterestM1V2) -> bytes:
    """Serialize one self-consistent public OI M1 issuance snapshot."""

    if type(row) is not UsdmOpenInterestM1V2:
        raise TypeError("row must be an exact UsdmOpenInterestM1V2")
    if getattr(row, "_factory_seal", None) is not _FACTORY_TOKEN:
        raise UsdmOpenInterestM1ContractErrorV2("open-interest M1 row factory seal differs")
    _validate_row(row)
    if row.m1_payload_sha256 != _row_hash(row):
        raise UsdmOpenInterestM1ContractErrorV2(
            "open-interest M1 row differs from canonical evidence"
        )
    return canonical_json_line(_row_document(row, include_hash=True))


def _validate_row(row: UsdmOpenInterestM1V2) -> None:
    if row.schema_version != _SCHEMA_VERSION:
        raise UsdmOpenInterestM1ContractErrorV2("unsupported USD-M open-interest M1 schema")
    if _SYMBOL_RE.fullmatch(row.symbol) is None:
        raise UsdmOpenInterestM1ContractErrorV2("OI M1 symbol is not normalized USD-M USDT")
    if row.venue is not VenueV2.USDM_FUTURES or row.route_id != "usdm_public_rest":
        raise UsdmOpenInterestM1ContractErrorV2("OI M1 row is outside the USD-M public REST route")
    for name in (
        "promoting_plan_sha256",
        "rest_plan_sha256",
        "symbol_census_sha256",
        "capture_authority_sha256",
        "protocol_sha256",
        "parser_contract_sha256",
        "m0_leaf_sha256",
        "raw_payload_hash_v2",
        "attempt_payload_sha256",
        "body_sha256",
    ):
        _require_sha256(getattr(row, name), name)
    if row.parser_contract_sha256 != USDM_OPEN_INTEREST_M1_PARSER_CONTRACT_SHA256_V2:
        raise UsdmOpenInterestM1ContractErrorV2("OI M1 parser contract hash differs")
    for name in ("session_id", "plan_id", "connection_id"):
        _require_identity(getattr(row, name), name)
    for name in ("generation", "ingest_seq", "poll_cycle_seq"):
        _require_positive_canonical_integer(getattr(row, name), name)
    _require_nonnegative_canonical_integer(row.symbol_ordinal, "symbol_ordinal")
    _require_nonnegative_canonical_integer(
        row.scheduled_slot_wall_ms,
        "scheduled_slot_wall_ms",
    )
    if row.scheduled_slot_wall_ms % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2 != 0:
        raise UsdmOpenInterestM1ContractErrorV2(
            "OI scheduled slot is not aligned to the frozen cadence"
        )
    if row.attempt != 1:
        raise UsdmOpenInterestM1ContractErrorV2("OI M1 row requires the frozen single attempt")
    for name in (
        "receipt_wall_ms",
        "receipt_monotonic_ns",
        "request_started_wall_ms",
        "request_started_monotonic_ns",
        "response_first_header_wall_ms",
        "response_first_header_monotonic_ns",
        "attempt_ended_wall_ms",
        "attempt_ended_monotonic_ns",
        "completion_admission_wall_ms",
        "completion_admission_monotonic_ns",
    ):
        _require_nonnegative_canonical_integer(getattr(row, name), name)
    # Binance's source T uses a signed-int64 domain and is serialized below as
    # decimal text.  All other integers are RFC 8785 lineage that has already
    # crossed the M0/REST canonical boundary.
    _require_nonnegative_int64(row.transaction_time_ms, "transaction_time_ms")
    if (
        row.completion_admission_wall_ms != row.receipt_wall_ms
        or row.completion_admission_monotonic_ns != row.receipt_monotonic_ns
    ):
        raise UsdmOpenInterestM1ContractErrorV2(
            "OI completion clocks differ from the outer receipt"
        )
    if not (
        row.request_started_wall_ms
        <= row.response_first_header_wall_ms
        <= row.attempt_ended_wall_ms
        <= row.completion_admission_wall_ms
    ) or not (
        row.request_started_monotonic_ns
        <= row.response_first_header_monotonic_ns
        <= row.attempt_ended_monotonic_ns
        <= row.completion_admission_monotonic_ns
    ):
        raise UsdmOpenInterestM1ContractErrorV2("OI response clocks are not causally ordered")
    if type(row.open_interest_text) is not str:
        raise UsdmOpenInterestM1ContractErrorV2("OI exact decimal text must remain text")
    if type(row.open_interest) is not Decimal:
        raise UsdmOpenInterestM1ContractErrorV2("OI value must remain an exact Decimal")
    if not row.open_interest.is_finite() or row.open_interest < 0:
        raise UsdmOpenInterestM1ContractErrorV2("OI value must be finite and nonnegative")
    try:
        text_value = Decimal(row.open_interest_text)
    except InvalidOperation as exc:
        raise UsdmOpenInterestM1ContractErrorV2(
            "OI exact text cannot be represented as Decimal"
        ) from exc
    if text_value != row.open_interest:
        raise UsdmOpenInterestM1ContractErrorV2("OI Decimal differs from its exact source text")


def _row_hash(row: UsdmOpenInterestM1V2) -> str:
    return hashlib.sha256(
        _ROW_HASH_DOMAIN + canonical_json_line(_row_document(row, include_hash=False))
    ).hexdigest()


def _row_document(
    row: UsdmOpenInterestM1V2,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document = asdict(row)
    document.pop("_factory_seal", None)
    document.pop("m1_payload_sha256", None)
    document["venue"] = row.venue.value
    document["open_interest"] = str(row.open_interest)
    # Binance specifies this source value as signed int64, which is wider than
    # RFC 8785's interoperable integer domain.  Preserve it losslessly as
    # decimal text, as the schedule/body verifier does.
    document.pop("transaction_time_ms", None)
    document["transaction_time_ms_text"] = str(row.transaction_time_ms)
    document.update(
        {
            "authority_reason": row.authority_reason,
            "body_semantics_verified": row.body_semantics_verified,
            "causal_inputs_complete": row.causal_inputs_complete,
            "completion_receipt_bound": row.completion_receipt_bound,
            "current_authority_claimed": row.current_authority_claimed,
            "cursor_complete": row.cursor_complete,
            "freshness_verified": row.freshness_verified,
            "live_reverification_required": row.live_reverification_required,
            "parser_bound": row.parser_bound,
            "schedule_cell_bound": row.schedule_cell_bound,
            "transaction_time_causally_bounded": (row.transaction_time_causally_bounded),
        }
    )
    if include_hash:
        document["m1_payload_sha256"] = row.m1_payload_sha256
    return document


def _require_sha256(value: object, field_name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise UsdmOpenInterestM1ContractErrorV2(f"{field_name} must be lowercase SHA-256 text")


def _require_identity(value: object, field_name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise UsdmOpenInterestM1ContractErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )


def _require_nonnegative_int64(value: object, field_name: str) -> int:
    if type(value) is not int or not 0 <= value <= _MAX_SIGNED_INT64:
        raise UsdmOpenInterestM1ContractErrorV2(f"{field_name} must be a nonnegative int64")
    return value


def _require_nonnegative_canonical_integer(value: object, field_name: str) -> int:
    if type(value) is not int or not 0 <= value <= _MAX_CANONICAL_INTEGER:
        raise UsdmOpenInterestM1ContractErrorV2(
            f"{field_name} must be a nonnegative RFC 8785 safe integer"
        )
    return value


def _require_positive_canonical_integer(value: object, field_name: str) -> int:
    parsed = _require_nonnegative_canonical_integer(value, field_name)
    if parsed == 0:
        raise UsdmOpenInterestM1ContractErrorV2(f"{field_name} must be positive")
    return parsed
