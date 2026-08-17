from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import InitVar, asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final, Literal, TypedDict, cast

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import StorageRootBindingV2
from signalbot.r4b_v2.capture.block_container import BlockSigningAuthorityV2
from signalbot.r4b_v2.capture.blocks import BlockPolicyV2
from signalbot.r4b_v2.capture.integrity_ledger import CaptureIntegrityLedgerV2
from signalbot.r4b_v2.capture.membership import (
    CurrentVerifiedRawMembershipLeafUseV2,
    VerifiedRawMembershipLeafV2,
    canonical_verified_raw_membership_leaf_v2,
    consume_current_verified_raw_membership_leaf_v2,
    reverify_verified_raw_membership_leaf_v2,
)
from signalbot.r4b_v2.capture.models import TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    provisional_promoting_plan_sha256_v2,
    validate_provisional_promoting_capture_plans_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_DECIMAL_TEXT_RE = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")
_DEPTH_DATA_KEYS = frozenset({"e", "E", "T", "s", "U", "u", "pu", "b", "a", "ps", "st"})
_COMBINED_WRAPPER_KEYS = frozenset({"stream", "data"})
_MAX_IDENTITY_LENGTH = 256
_MAX_DECIMAL_TEXT_LENGTH = 128
USDM_PUBLIC_DEPTH_M1_MAX_FRAME_BYTES_V2: Final = 1_048_576
USDM_PUBLIC_DEPTH_M1_MAX_LEVELS_PER_SIDE_V2: Final = 10_000
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_FACTORY_TOKEN = object()
_ROW_HASH_DOMAIN = b"R4B_V2_USDM_PUBLIC_DEPTH_M1_ROW\0"
_M1_SCHEMA_VERSION = "r4b_v2_usdm_public_depth_m1_v1"

USDM_PUBLIC_DEPTH_M1_ONLY_REASON_V2: Final = (
    "M1_DEPTH_ROW_DOES_NOT_PROVE_SEQUENCE_CONTINUITY_SNAPSHOT_OR_LOCAL_BOOK_M2"
)

_PARSER_CONTRACT = {
    "access_mode": "COMBINED_QUERY",
    "combined_wrapper_keys": tuple(sorted(_COMBINED_WRAPPER_KEYS)),
    "decimal_text_max_length": _MAX_DECIMAL_TEXT_LENGTH,
    "decimal_text_regex": _DECIMAL_TEXT_RE.pattern,
    "depth_data_keys": tuple(sorted(_DEPTH_DATA_KEYS)),
    "depth_field_types": {
        "E": "nonnegative_int64_exchange_event_time",
        "T": "nonnegative_int64_transaction_time",
        "U": "positive_int64_first_update_id",
        "a": "bounded_array_of_exact_two_decimal_strings",
        "b": "bounded_array_of_exact_two_decimal_strings",
        "e": "literal_depthUpdate",
        "ps": "stream_bound_normalized_symbol",
        "pu": "nonnegative_int64_previous_final_update_id",
        "s": "stream_bound_normalized_symbol",
        "st": "integer_literal_1",
        "u": "positive_int64_final_update_id",
    },
    "duplicate_key_policy": "REJECT_AT_EVERY_OBJECT_DEPTH",
    "exact_inner_key_policy": "PROJECT_FROZEN_CURRENT_USDM_PUBLIC_DEPTH",
    "frame_max_bytes": USDM_PUBLIC_DEPTH_M1_MAX_FRAME_BYTES_V2,
    "integer_policy": "JSON_INTEGER_SIGNED_INT64_WITH_FIELD_BOUNDS",
    "level_invariants": (
        "price_positive_finite_decimal_text",
        "quantity_nonnegative_finite_decimal_text_ZERO_REMOVES_LEVEL",
    ),
    "levels_per_side_maximum": USDM_PUBLIC_DEPTH_M1_MAX_LEVELS_PER_SIDE_V2,
    "local_invariants": (
        "event_time_not_before_transaction_time",
        "first_update_id_not_after_final_update_id",
    ),
    "m1_nonclaims": (
        "NO_PREVIOUS_EVENT_CONTINUITY",
        "NO_REST_SNAPSHOT_BINDING",
        "NO_LOCAL_BOOK_RECONSTRUCTION",
        "NO_M2_COMPLETENESS",
    ),
    "numeric_literal_policy": "DECIMALS_MUST_REMAIN_JSON_STRINGS",
    "route_id": "usdm_public",
    "schema_reference": (
        "https://developers.binance.com/en/docs/products/derivatives-trading-usds-"
        "futures/websocket-market-streams/Diff-Book-Depth-Streams"
    ),
    "schema_version": "r4b_v2_usdm_public_depth_strict_parser_contract_v1",
    "sequence_continuity_policy": "DEFER_PU_EQUALS_PREVIOUS_U_TO_STATEFUL_M2",
    "stream_suffix": "@depth@100ms",
    "transport": "websocket",
    "unknown_field_policy": "REJECT_SCHEMA_DRIFT_BUT_RETAIN_RAW_M0",
    "whitespace_policy": "NO_LEADING_OR_TRAILING_FRAME_WHITESPACE",
    "venue": "usdm_futures",
}
USDM_PUBLIC_DEPTH_M1_PARSER_CONTRACT_SHA256_V2: Final = hashlib.sha256(
    canonical_json_line(_PARSER_CONTRACT)
).hexdigest()


class UsdmPublicDepthM1ContractErrorV2(RuntimeError):
    """Raised when a signed USD-M public depth member fails its exact M1 contract."""


type UsdmDepthLevelM1V2 = tuple[Decimal, Decimal]


class _CommonFields(TypedDict):
    symbol: str
    venue: VenueV2
    route_id: Literal["usdm_public"]
    stream: str
    promoting_plan_sha256: str
    capture_authority_sha256: str
    protocol_sha256: str
    parser_contract_sha256: str
    m0_leaf_sha256: str
    raw_payload_hash_v2: str
    session_id: str
    plan_id: str
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int


@dataclass(frozen=True, slots=True)
class UsdmDepthDiff100msM1V2:
    """Factory-only exact USD-M 100 ms diff-depth row parsed from one M0 leaf.

    This is a calculation-only representation of one message.  It deliberately
    does not join messages through ``pu``, bind a REST snapshot, or materialize
    a local book; those are stateful M2 responsibilities.
    """

    symbol: str
    venue: VenueV2
    route_id: Literal["usdm_public"]
    stream: str
    promoting_plan_sha256: str
    capture_authority_sha256: str
    protocol_sha256: str
    parser_contract_sha256: str
    m0_leaf_sha256: str
    raw_payload_hash_v2: str
    session_id: str
    plan_id: str
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    event_ms: int
    transaction_time_ms: int
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int
    bids: tuple[UsdmDepthLevelM1V2, ...]
    asks: tuple[UsdmDepthLevelM1V2, ...]
    stream_type: Literal[1]
    _factory_token: InitVar[object] = None
    m1_payload_sha256: str = field(init=False, default="")
    schema_version: str = field(init=False, default=_M1_SCHEMA_VERSION)
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise UsdmPublicDepthM1ContractErrorV2(
                "USD-M public depth requires the verified M1 parser factory"
            )
        _validate_row(self)
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        object.__setattr__(self, "m1_payload_sha256", _row_hash(self))

    @property
    def parser_bound(self) -> Literal[True]:
        return True

    @property
    def live_reverification_required(self) -> Literal[True]:
        return True

    @property
    def current_authority_claimed(self) -> Literal[False]:
        return False

    @property
    def cursor_complete(self) -> Literal[False]:
        return False

    @property
    def causal_inputs_complete(self) -> Literal[False]:
        return False

    @property
    def sequence_continuity_claimed(self) -> Literal[False]:
        return False

    @property
    def snapshot_bound(self) -> Literal[False]:
        return False

    @property
    def local_book_reconstructed(self) -> Literal[False]:
        return False

    @property
    def m2_complete(self) -> Literal[False]:
        return False

    @property
    def authority_reason(self) -> str:
        return USDM_PUBLIC_DEPTH_M1_ONLY_REASON_V2

    @property
    def source_evidence_sha256(self) -> str:
        return self.m1_payload_sha256


def parse_verified_usdm_public_depth_m1_v2(
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
) -> UsdmDepthDiff100msM1V2:
    """Live-reverify one M0 member and parse its exact depth combined frame."""

    public_plan = _validated_public_plan(leaf, promoting_plans)
    promoting_plan_sha256 = provisional_promoting_plan_sha256_v2(promoting_plans)
    if authority.plan_sha256 != promoting_plan_sha256:
        raise UsdmPublicDepthM1ContractErrorV2(
            "trusted WAL authority differs from the frozen promoting plan"
        )

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
        expected_transport=TransportV2.WEBSOCKET,
        expected_venue=VenueV2.USDM_FUTURES,
        expected_route_id="usdm_public",
        expected_symbol=None,
    )
    return _parse_current_leaf(leaf, public_plan)


def parse_current_verified_usdm_public_depth_m1_v2(
    current_use: CurrentVerifiedRawMembershipLeafUseV2,
    *,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
) -> UsdmDepthDiff100msM1V2:
    """Consume and parse one callback-scoped leaf without another prefix scan.

    The use is factory-minted by the signed-prefix streaming verifier and is
    valid exactly once inside that verifier's callback.  The resulting durable
    M1 row retains its existing nonclaims and cannot carry current authority.
    """

    leaf = consume_current_verified_raw_membership_leaf_v2(current_use)
    canonical_verified_raw_membership_leaf_v2(leaf)
    public_plan = _validated_public_plan(leaf, promoting_plans)
    return _parse_current_leaf(leaf, public_plan)


def _validated_public_plan(
    leaf: VerifiedRawMembershipLeafV2,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
) -> ProvisionalPromotingCapturePlanV2:
    if not isinstance(leaf, VerifiedRawMembershipLeafV2):
        raise TypeError("leaf must be a VerifiedRawMembershipLeafV2")
    validate_provisional_promoting_capture_plans_v2(promoting_plans)
    promoting_plan_sha256 = provisional_promoting_plan_sha256_v2(promoting_plans)
    if leaf.authority.plan_sha256 != promoting_plan_sha256:
        raise UsdmPublicDepthM1ContractErrorV2(
            "trusted WAL authority differs from the frozen promoting plan"
        )
    public_plans = tuple(
        item
        for item in promoting_plans
        if isinstance(item, ProvisionalPromotingCapturePlanV2)
        and item.route_id == "usdm_public"
    )
    if len(public_plans) != 1:
        raise UsdmPublicDepthM1ContractErrorV2(
            "promoting plan has no unique USD-M public stream owner"
        )
    return public_plans[0]


def _parse_current_leaf(
    leaf: VerifiedRawMembershipLeafV2,
    public_plan: ProvisionalPromotingCapturePlanV2,
) -> UsdmDepthDiff100msM1V2:
    """Strict parser core shared by durable and callback-scoped M0 paths."""

    record = leaf.record
    if record.symbol is not None:
        raise UsdmPublicDepthM1ContractErrorV2(
            "combined-query M1 raw record symbol must remain unresolved"
        )
    if record.plan_id != public_plan.name:
        raise UsdmPublicDepthM1ContractErrorV2(
            "raw record plan_id differs from the USD-M public plan"
        )
    if record.frame_seq is None:
        raise UsdmPublicDepthM1ContractErrorV2(
            "USD-M WebSocket public record requires frame_seq"
        )
    wrapper = _parse_strict_json_object(record.payload_bytes())
    _require_exact_keys(wrapper, _COMBINED_WRAPPER_KEYS, "combined wrapper")
    stream = _require_text(wrapper.get("stream"), "stream")
    if stream not in public_plan.streams:
        raise UsdmPublicDepthM1ContractErrorV2(
            "combined stream is outside the frozen USD-M public plan"
        )
    symbol = _stream_symbol(stream)
    data = wrapper.get("data")
    if not isinstance(data, dict):
        raise UsdmPublicDepthM1ContractErrorV2(
            "combined depth data must be a JSON object"
        )
    typed_data = cast(dict[str, object], data)
    _require_exact_keys(typed_data, _DEPTH_DATA_KEYS, "depth@100ms data")
    if typed_data.get("e") != "depthUpdate":
        raise UsdmPublicDepthM1ContractErrorV2("depth event type is not exact")
    if typed_data.get("s") != symbol or typed_data.get("ps") != symbol:
        raise UsdmPublicDepthM1ContractErrorV2(
            "depth symbol or pair differs from its combined stream"
        )

    return UsdmDepthDiff100msM1V2(
        **_common_fields(leaf, stream, symbol),
        event_ms=_require_nonnegative_int64(typed_data.get("E"), "E"),
        transaction_time_ms=_require_nonnegative_int64(typed_data.get("T"), "T"),
        first_update_id=_require_positive_int64(typed_data.get("U"), "U"),
        final_update_id=_require_positive_int64(typed_data.get("u"), "u"),
        previous_final_update_id=_require_nonnegative_int64(
            typed_data.get("pu"), "pu"
        ),
        bids=_parse_levels(typed_data.get("b"), "b"),
        asks=_parse_levels(typed_data.get("a"), "a"),
        stream_type=_require_usdm_stream_type(typed_data.get("st")),
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_usdm_public_depth_m1_v2(row: UsdmDepthDiff100msM1V2) -> bytes:
    """Serialize one self-consistent depth M1 issuance snapshot canonically."""

    if not isinstance(row, UsdmDepthDiff100msM1V2):
        raise TypeError("row must be a USD-M public depth M1 row")
    if row._factory_seal is not _FACTORY_TOKEN:
        raise UsdmPublicDepthM1ContractErrorV2("depth M1 row factory seal differs")
    _validate_row(row)
    if row.m1_payload_sha256 != _row_hash(row):
        raise UsdmPublicDepthM1ContractErrorV2(
            "depth M1 row differs from canonical evidence"
        )
    return canonical_json_line(_row_document(row, include_hash=True))


def _common_fields(
    leaf: VerifiedRawMembershipLeafV2,
    stream: str,
    symbol: str,
) -> _CommonFields:
    record = leaf.record
    if record.frame_seq is None:
        raise UsdmPublicDepthM1ContractErrorV2(
            "WebSocket depth M1 row requires frame_seq"
        )
    return {
        "symbol": symbol,
        "venue": VenueV2.USDM_FUTURES,
        "route_id": "usdm_public",
        "stream": stream,
        "promoting_plan_sha256": leaf.authority.plan_sha256,
        "capture_authority_sha256": leaf.authority_sha256,
        "protocol_sha256": record.protocol_hash,
        "parser_contract_sha256": USDM_PUBLIC_DEPTH_M1_PARSER_CONTRACT_SHA256_V2,
        "m0_leaf_sha256": leaf.leaf_sha256,
        "raw_payload_hash_v2": leaf.raw_payload_hash_v2,
        "session_id": record.session_id,
        "plan_id": record.plan_id,
        "connection_id": record.connection_id,
        "generation": record.generation,
        "frame_seq": record.frame_seq,
        "ingest_seq": record.ingest_seq,
        "receipt_wall_ms": record.receipt_wall_ms,
        "receipt_monotonic_ns": record.receipt_monotonic_ns,
    }


def _validate_row(row: UsdmDepthDiff100msM1V2) -> None:
    if row.schema_version != _M1_SCHEMA_VERSION:
        raise UsdmPublicDepthM1ContractErrorV2(
            "unsupported USD-M public depth M1 schema"
        )
    if _SYMBOL_RE.fullmatch(row.symbol) is None:
        raise UsdmPublicDepthM1ContractErrorV2(
            "depth M1 symbol is not normalized USD-M USDT"
        )
    if row.venue is not VenueV2.USDM_FUTURES or row.route_id != "usdm_public":
        raise UsdmPublicDepthM1ContractErrorV2(
            "depth M1 row is outside the USD-M public route"
        )
    if row.stream != f"{row.symbol.lower()}@depth@100ms":
        raise UsdmPublicDepthM1ContractErrorV2(
            "depth M1 stream differs from row identity"
        )
    for value, name in (
        (row.promoting_plan_sha256, "promoting_plan_sha256"),
        (row.capture_authority_sha256, "capture_authority_sha256"),
        (row.protocol_sha256, "protocol_sha256"),
        (row.parser_contract_sha256, "parser_contract_sha256"),
        (row.m0_leaf_sha256, "m0_leaf_sha256"),
        (row.raw_payload_hash_v2, "raw_payload_hash_v2"),
    ):
        _require_sha256(value, name)
    if row.parser_contract_sha256 != USDM_PUBLIC_DEPTH_M1_PARSER_CONTRACT_SHA256_V2:
        raise UsdmPublicDepthM1ContractErrorV2(
            "depth M1 parser contract hash differs"
        )
    for value, name in (
        (row.session_id, "session_id"),
        (row.plan_id, "plan_id"),
        (row.connection_id, "connection_id"),
    ):
        _require_text(value, name)
    _require_positive_int64(row.generation, "generation")
    _require_positive_int64(row.frame_seq, "frame_seq")
    _require_positive_int64(row.ingest_seq, "ingest_seq")
    _require_nonnegative_int64(row.receipt_wall_ms, "receipt_wall_ms")
    _require_nonnegative_int64(row.receipt_monotonic_ns, "receipt_monotonic_ns")
    _require_nonnegative_int64(row.event_ms, "event_ms")
    _require_nonnegative_int64(row.transaction_time_ms, "transaction_time_ms")
    if row.event_ms < row.transaction_time_ms:
        raise UsdmPublicDepthM1ContractErrorV2(
            "depth event time precedes transaction time"
        )
    _require_positive_int64(row.first_update_id, "first_update_id")
    _require_positive_int64(row.final_update_id, "final_update_id")
    if row.first_update_id > row.final_update_id:
        raise UsdmPublicDepthM1ContractErrorV2(
            "depth update ID range is reversed"
        )
    _require_nonnegative_int64(
        row.previous_final_update_id, "previous_final_update_id"
    )
    _validate_level_tuple(row.bids, "bids")
    _validate_level_tuple(row.asks, "asks")
    if type(row.stream_type) is not int or row.stream_type != 1:
        raise UsdmPublicDepthM1ContractErrorV2("USD-M depth st must equal 1")


def _parse_levels(value: object, field_name: str) -> tuple[UsdmDepthLevelM1V2, ...]:
    if not isinstance(value, list):
        raise UsdmPublicDepthM1ContractErrorV2(
            f"depth field {field_name} must be a JSON array"
        )
    if len(value) > USDM_PUBLIC_DEPTH_M1_MAX_LEVELS_PER_SIDE_V2:
        raise UsdmPublicDepthM1ContractErrorV2(
            f"depth field {field_name} exceeds its fixed level bound"
        )
    levels: list[UsdmDepthLevelM1V2] = []
    for index, level in enumerate(value):
        if not isinstance(level, list) or len(level) != 2:
            raise UsdmPublicDepthM1ContractErrorV2(
                f"depth field {field_name}[{index}] must be an exact JSON pair"
            )
        levels.append(
            (
                _parse_decimal_text(
                    level[0], f"{field_name}[{index}].price", positive=True
                ),
                _parse_decimal_text(
                    level[1], f"{field_name}[{index}].quantity", positive=False
                ),
            )
        )
    return tuple(levels)


def _validate_level_tuple(
    value: tuple[UsdmDepthLevelM1V2, ...],
    field_name: str,
) -> None:
    if not isinstance(value, tuple):
        raise UsdmPublicDepthM1ContractErrorV2(
            f"depth row {field_name} must be an immutable tuple"
        )
    if len(value) > USDM_PUBLIC_DEPTH_M1_MAX_LEVELS_PER_SIDE_V2:
        raise UsdmPublicDepthM1ContractErrorV2(
            f"depth row {field_name} exceeds its fixed level bound"
        )
    for index, level in enumerate(value):
        if not isinstance(level, tuple) or len(level) != 2:
            raise UsdmPublicDepthM1ContractErrorV2(
                f"depth row {field_name}[{index}] must be an immutable pair"
            )
        price, quantity = level
        _require_positive_decimal(price, f"{field_name}[{index}].price")
        _require_nonnegative_decimal(quantity, f"{field_name}[{index}].quantity")


def _parse_strict_json_object(payload: bytes) -> dict[str, object]:
    if not isinstance(payload, bytes):
        raise TypeError("payload must be immutable bytes")
    if len(payload) > USDM_PUBLIC_DEPTH_M1_MAX_FRAME_BYTES_V2:
        raise UsdmPublicDepthM1ContractErrorV2(
            "USD-M public depth frame exceeds the frozen M1 byte limit"
        )
    try:
        text = payload.decode("utf-8")
        if not text or text.strip() != text:
            raise UsdmPublicDepthM1ContractErrorV2(
                "USD-M public depth frame has leading or trailing whitespace"
            )
        document = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_json_constant,
            parse_float=_reject_json_float,
            parse_int=_parse_json_integer,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise UsdmPublicDepthM1ContractErrorV2(
            "USD-M public depth frame is not strict UTF-8 JSON"
        ) from exc
    if not isinstance(document, dict):
        raise UsdmPublicDepthM1ContractErrorV2(
            "USD-M public depth frame must be a JSON object"
        )
    return cast(dict[str, object], document)


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise UsdmPublicDepthM1ContractErrorV2(
                "USD-M public depth JSON repeats an object key"
            )
        document[key] = value
    return document


def _reject_json_constant(value: str) -> object:
    raise UsdmPublicDepthM1ContractErrorV2(
        f"USD-M public depth JSON contains forbidden constant {value}"
    )


def _reject_json_float(value: str) -> object:
    raise UsdmPublicDepthM1ContractErrorV2(
        f"USD-M public depth JSON contains non-schema numeric literal {value}"
    )


def _parse_json_integer(value: str) -> int:
    if len(value) > 20:
        raise UsdmPublicDepthM1ContractErrorV2(
            "USD-M public depth JSON integer exceeds signed int64 text length"
        )
    parsed = int(value)
    if not _INT64_MIN <= parsed <= _INT64_MAX:
        raise UsdmPublicDepthM1ContractErrorV2(
            "USD-M public depth JSON integer is outside signed int64"
        )
    return parsed


def _require_exact_keys(
    value: dict[str, object],
    expected: frozenset[str],
    label: str,
) -> None:
    if set(value) != expected:
        raise UsdmPublicDepthM1ContractErrorV2(f"{label} schema is not exact")


def _stream_symbol(stream: str) -> str:
    suffix = "@depth@100ms"
    if not stream.endswith(suffix):
        raise UsdmPublicDepthM1ContractErrorV2(
            "combined depth stream suffix differs"
        )
    symbol = stream[: -len(suffix)].upper()
    if _SYMBOL_RE.fullmatch(symbol) is None:
        raise UsdmPublicDepthM1ContractErrorV2(
            "combined depth stream symbol is not normalized"
        )
    if stream != f"{symbol.lower()}{suffix}":
        raise UsdmPublicDepthM1ContractErrorV2(
            "combined depth stream case is not canonical"
        )
    return symbol


def _require_text(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > _MAX_IDENTITY_LENGTH
        or value.strip() != value
        or any(character in value for character in "\r\n\x00")
    ):
        raise UsdmPublicDepthM1ContractErrorV2(
            f"{field_name} must be bounded normalized text"
        )
    return value


def _parse_decimal_text(
    value: object,
    field_name: str,
    *,
    positive: bool,
) -> Decimal:
    if (
        not isinstance(value, str)
        or len(value) > _MAX_DECIMAL_TEXT_LENGTH
        or _DECIMAL_TEXT_RE.fullmatch(value) is None
    ):
        raise UsdmPublicDepthM1ContractErrorV2(
            f"{field_name} must be a strict nonnegative decimal string"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise UsdmPublicDepthM1ContractErrorV2(
            f"{field_name} is not a finite decimal"
        ) from exc
    if not parsed.is_finite() or parsed < 0 or (positive and parsed <= 0):
        qualifier = "positive" if positive else "nonnegative"
        raise UsdmPublicDepthM1ContractErrorV2(
            f"{field_name} must be {qualifier} finite Decimal"
        )
    return parsed


def _require_usdm_stream_type(value: object) -> Literal[1]:
    if type(value) is not int or value != 1:
        raise UsdmPublicDepthM1ContractErrorV2(
            "depth st must be integer 1 for USD-M"
        )
    return 1


def _require_nonnegative_int64(value: object, field_name: str) -> int:
    if type(value) is not int or not 0 <= value <= _INT64_MAX:
        raise UsdmPublicDepthM1ContractErrorV2(
            f"{field_name} must be a nonnegative int64"
        )
    return value


def _require_positive_int64(value: object, field_name: str) -> int:
    parsed = _require_nonnegative_int64(value, field_name)
    if parsed == 0:
        raise UsdmPublicDepthM1ContractErrorV2(f"{field_name} must be positive")
    return parsed


def _require_positive_decimal(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise UsdmPublicDepthM1ContractErrorV2(
            f"{field_name} must be positive finite Decimal"
        )


def _require_nonnegative_decimal(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise UsdmPublicDepthM1ContractErrorV2(
            f"{field_name} must be nonnegative finite Decimal"
        )


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise UsdmPublicDepthM1ContractErrorV2(
            f"{field_name} must be a lowercase SHA-256 digest"
        )


def _row_hash(row: UsdmDepthDiff100msM1V2) -> str:
    return hashlib.sha256(
        _ROW_HASH_DOMAIN + canonical_json_line(_row_document(row, include_hash=False))
    ).hexdigest()


def _row_document(
    row: UsdmDepthDiff100msM1V2,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document = asdict(row)
    document.pop("_factory_seal", None)
    document.pop("m1_payload_sha256", None)
    document["venue"] = row.venue.value
    document["bids"] = [[str(price), str(quantity)] for price, quantity in row.bids]
    document["asks"] = [[str(price), str(quantity)] for price, quantity in row.asks]
    document.update(
        {
            "authority_reason": row.authority_reason,
            "causal_inputs_complete": row.causal_inputs_complete,
            "current_authority_claimed": row.current_authority_claimed,
            "cursor_complete": row.cursor_complete,
            "live_reverification_required": row.live_reverification_required,
            "local_book_reconstructed": row.local_book_reconstructed,
            "m2_complete": row.m2_complete,
            "parser_bound": row.parser_bound,
            "sequence_continuity_claimed": row.sequence_continuity_claimed,
            "snapshot_bound": row.snapshot_bound,
        }
    )
    if include_hash:
        document["m1_payload_sha256"] = row.m1_payload_sha256
    return document
