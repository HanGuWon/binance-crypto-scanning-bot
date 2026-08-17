from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, DecimalException
from itertools import pairwise
from typing import Literal, cast

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.plans import ProvisionalDepthRestQualificationPlanV8
from signalbot.r4b_v2.capture.rest_depth import (
    PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8,
    PUBLIC_DEPTH_REST_MAXIMUM_LEVELS_PER_SIDE_V8,
    PublicDepthRestAttemptPayloadV8,
    public_depth_rest_attempt_payload_sha256_v8,
    public_depth_rest_plan_sha256_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicDepthRestAdmissionReceiptV8,
    validate_public_depth_rest_admission_receipt_v8,
)

PUBLIC_DEPTH_REST_BODY_EXTRA_FIELDS_POLICY_V8 = "REJECT_UNDOCUMENTED_FIELDS_FAIL_CLOSED"
PUBLIC_DEPTH_REST_BODY_SEMANTIC_SCOPE_V8 = (
    "ONE_QUEUE_ADMITTED_COMPLETE_HTTP_200_SNAPSHOT_ONLY"
)

_EXPECTED_FIELDS = frozenset({"lastUpdateId", "E", "T", "bids", "asks"})
_DECIMAL_TEXT_RE = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_SIGNED_INT64 = (1 << 63) - 1
_MAX_DECIMAL_TEXT_LENGTH = 64
_RESULT_FACTORY_TOKEN = object()
_SEMANTIC_HASH_DOMAIN = b"R4B_V2_PUBLIC_DEPTH_REST_SEMANTIC_ADMISSION_V8\0"


class PublicDepthRestSnapshotSemanticErrorV8(ValueError):
    """A queue-admitted attempt is not one exact documented depth snapshot."""


@dataclass(frozen=True, slots=True)
class PublicDepthLevelV8:
    """One exact positive price/quantity text pair from the retained body."""

    price_text: str
    quantity_text: str

    def __post_init__(self) -> None:
        _parse_positive_decimal(self.price_text, "price")
        _parse_positive_decimal(self.quantity_text, "quantity")

    @property
    def price(self) -> Decimal:
        return Decimal(self.price_text)

    @property
    def quantity(self) -> Decimal:
        return Decimal(self.quantity_text)


@dataclass(frozen=True, slots=True)
class VerifiedPublicDepthRestSnapshotV8:
    """Factory-sealed single-attempt semantics, never book or M2 authority."""

    symbol: str
    last_update_id: int
    event_time_ms: int
    transaction_time_ms: int
    bids: tuple[PublicDepthLevelV8, ...]
    asks: tuple[PublicDepthLevelV8, ...]
    plan_sha256: str
    attempt_payload_sha256: str
    raw_record_sha256: str
    body_sha256: str
    queue_admission_verified: Literal[True] = True
    body_semantics_valid: Literal[True] = True
    verification_scope: Literal[
        "ONE_QUEUE_ADMITTED_COMPLETE_HTTP_200_SNAPSHOT_ONLY"
    ] = "ONE_QUEUE_ADMITTED_COMPLETE_HTTP_200_SNAPSHOT_ONLY"
    qualification_only: Literal[True] = True
    promoting: Literal[False] = False
    promotion_ready: Literal[False] = False
    wal_durability_verified: Literal[False] = False
    finality_fence_verified: Literal[False] = False
    freshness_verified: Literal[False] = False
    coverage_complete: Literal[False] = False
    m2_certified: Literal[False] = False
    book_bridge_certified: Literal[False] = False
    liquidity_signal_emitted: Literal[False] = False
    order_execution_enabled: Literal[False] = False
    semantic_admission_sha256: str = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RESULT_FACTORY_TOKEN:
            raise TypeError("verified public depth snapshots are factory-sealed")
        object.__setattr__(self, "_factory_seal", _RESULT_FACTORY_TOKEN)
        _validate_verified_snapshot_material_v8(self)
        object.__setattr__(
            self,
            "semantic_admission_sha256",
            _semantic_admission_sha256_v8(self),
        )


def verify_admitted_public_depth_rest_snapshot_v8(
    receipt: PublicDepthRestAdmissionReceiptV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8 | None = None,
) -> VerifiedPublicDepthRestSnapshotV8:
    """Verify one actual queue admission and its strict complete HTTP 200 body."""

    record = validate_public_depth_rest_admission_receipt_v8(receipt, plan=plan)
    authority = receipt.plan
    payload = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
        record.payload_bytes(),
        plan=authority,
    )
    if (
        payload.response_status != 200
        or not payload.payload_complete
        or payload.error_category is not None
        or payload.admission_cancellation_requested
    ):
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "snapshot semantics require a complete error-free uncancelled HTTP 200 attempt"
        )
    body = payload.body_bytes()
    if not body or len(body) > PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot body must be nonempty and within the exact byte cap"
        )
    document = _parse_strict_json_object(body)
    if set(document) != _EXPECTED_FIELDS:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot fields differ from the documented exact schema"
        )
    last_update_id = _parse_nonnegative_int64(document["lastUpdateId"], "lastUpdateId")
    event_time_ms = _parse_nonnegative_int64(document["E"], "E")
    transaction_time_ms = _parse_nonnegative_int64(document["T"], "T")
    if event_time_ms < transaction_time_ms:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot event time precedes transaction time"
        )
    bids = _parse_levels(document["bids"], side="bids")
    asks = _parse_levels(document["asks"], side="asks")
    _validate_book_geometry(bids=bids, asks=asks)
    return VerifiedPublicDepthRestSnapshotV8(
        symbol=payload.symbol,
        last_update_id=last_update_id,
        event_time_ms=event_time_ms,
        transaction_time_ms=transaction_time_ms,
        bids=bids,
        asks=asks,
        plan_sha256=public_depth_rest_plan_sha256_v8(authority),
        attempt_payload_sha256=public_depth_rest_attempt_payload_sha256_v8(payload),
        raw_record_sha256=receipt.queued_record.encoded_sha256,
        body_sha256=payload.body_sha256,
        _factory_token=_RESULT_FACTORY_TOKEN,
    )


def validate_verified_public_depth_rest_snapshot_v8(
    value: VerifiedPublicDepthRestSnapshotV8,
) -> str:
    """Revalidate factory provenance and return its deterministic semantic hash."""

    if type(value) is not VerifiedPublicDepthRestSnapshotV8:
        raise TypeError("verified depth snapshot must be an exact factory result")
    if getattr(value, "_factory_seal", None) is not _RESULT_FACTORY_TOKEN:
        raise ValueError("verified depth snapshot lacks factory provenance")
    _validate_verified_snapshot_material_v8(value)
    _require_sha256(value.semantic_admission_sha256, "semantic_admission_sha256")
    expected = _semantic_admission_sha256_v8(value)
    if not hmac.compare_digest(value.semantic_admission_sha256, expected):
        raise ValueError("verified depth snapshot semantic hash differs")
    return value.semantic_admission_sha256


def _validate_verified_snapshot_material_v8(
    value: VerifiedPublicDepthRestSnapshotV8,
) -> None:
    if type(value.symbol) is not str or not value.symbol:
        raise TypeError("verified depth symbol must be nonempty exact text")
    _parse_nonnegative_int64(value.last_update_id, "last_update_id")
    _parse_nonnegative_int64(value.event_time_ms, "event_time_ms")
    _parse_nonnegative_int64(value.transaction_time_ms, "transaction_time_ms")
    if value.event_time_ms < value.transaction_time_ms:
        raise ValueError("verified depth event time precedes transaction time")
    if type(value.bids) is not tuple or type(value.asks) is not tuple:
        raise TypeError("verified depth sides must be exact tuples")
    if any(type(level) is not PublicDepthLevelV8 for level in (*value.bids, *value.asks)):
        raise TypeError("verified depth levels must be exact values")
    _validate_book_geometry(bids=value.bids, asks=value.asks)
    for field_name in (
        "plan_sha256",
        "attempt_payload_sha256",
        "raw_record_sha256",
        "body_sha256",
    ):
        _require_sha256(getattr(value, field_name), field_name)
    if (
        value.queue_admission_verified is not True
        or value.body_semantics_valid is not True
        or value.verification_scope != PUBLIC_DEPTH_REST_BODY_SEMANTIC_SCOPE_V8
        or value.qualification_only is not True
        or value.promoting is not False
        or value.promotion_ready is not False
        or value.wal_durability_verified is not False
        or value.finality_fence_verified is not False
        or value.freshness_verified is not False
        or value.coverage_complete is not False
        or value.m2_certified is not False
        or value.book_bridge_certified is not False
        or value.liquidity_signal_emitted is not False
        or value.order_execution_enabled is not False
    ):
        raise ValueError("verified depth snapshot contains a forbidden authority claim")


def _parse_strict_json_object(body: bytes) -> dict[str, object]:
    try:
        decoded = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot body must be strict UTF-8"
        ) from exc

    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise PublicDepthRestSnapshotSemanticErrorV8(
                    "depth snapshot body contains a duplicate JSON key"
                )
            result[key] = value
        return result

    def reject_float(value: str) -> object:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            f"depth snapshot contains an unsupported JSON float: {value}"
        )

    def reject_constant(value: str) -> object:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            f"depth snapshot contains a non-finite JSON constant: {value}"
        )

    try:
        parsed = json.loads(
            decoded,
            object_pairs_hook=reject_duplicate_keys,
            parse_float=reject_float,
            parse_constant=reject_constant,
        )
    except PublicDepthRestSnapshotSemanticErrorV8:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot body is invalid JSON"
        ) from exc
    if type(parsed) is not dict:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot body must be one JSON object"
        )
    return cast(dict[str, object], parsed)


def _parse_levels(value: object, *, side: str) -> tuple[PublicDepthLevelV8, ...]:
    if type(value) is not list or not value:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            f"depth snapshot {side} must be one nonempty JSON array"
        )
    if len(value) > PUBLIC_DEPTH_REST_MAXIMUM_LEVELS_PER_SIDE_V8:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            f"depth snapshot {side} exceeds the limit=1000 level bound"
        )
    levels: list[PublicDepthLevelV8] = []
    for index, item in enumerate(value):
        if type(item) is not list or len(item) != 2:
            raise PublicDepthRestSnapshotSemanticErrorV8(
                f"depth snapshot {side}[{index}] must be one exact pair"
            )
        price_text, quantity_text = item
        if type(price_text) is not str or type(quantity_text) is not str:
            raise PublicDepthRestSnapshotSemanticErrorV8(
                f"depth snapshot {side}[{index}] values must be exact text"
            )
        try:
            level = PublicDepthLevelV8(price_text, quantity_text)
        except (TypeError, ValueError) as exc:
            raise PublicDepthRestSnapshotSemanticErrorV8(
                f"depth snapshot {side}[{index}] contains an invalid level"
            ) from exc
        levels.append(level)
    return tuple(levels)


def _validate_book_geometry(
    *,
    bids: tuple[PublicDepthLevelV8, ...],
    asks: tuple[PublicDepthLevelV8, ...],
) -> None:
    if not bids or not asks:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot requires both nonempty sides"
        )
    if (
        len(bids) > PUBLIC_DEPTH_REST_MAXIMUM_LEVELS_PER_SIDE_V8
        or len(asks) > PUBLIC_DEPTH_REST_MAXIMUM_LEVELS_PER_SIDE_V8
    ):
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot side exceeds the limit=1000 bound"
        )
    bid_prices = tuple(level.price for level in bids)
    ask_prices = tuple(level.price for level in asks)
    if any(left <= right for left, right in pairwise(bid_prices)):
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot bids must be strictly descending"
        )
    if any(left >= right for left, right in pairwise(ask_prices)):
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot asks must be strictly ascending"
        )
    if bid_prices[0] >= ask_prices[0]:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            "depth snapshot best bid must be below best ask"
        )


def _parse_positive_decimal(value: str, field_name: str) -> Decimal:
    if (
        type(value) is not str
        or not 1 <= len(value) <= _MAX_DECIMAL_TEXT_LENGTH
        or _DECIMAL_TEXT_RE.fullmatch(value) is None
    ):
        raise ValueError(f"depth {field_name} must be bounded unsigned decimal text")
    try:
        parsed = Decimal(value)
    except (DecimalException, ValueError) as exc:
        raise ValueError(f"depth {field_name} cannot be represented exactly") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"depth {field_name} must be finite and positive")
    return parsed


def _parse_nonnegative_int64(value: object, field_name: str) -> int:
    if type(value) is not int or not 0 <= value <= _MAX_SIGNED_INT64:
        raise PublicDepthRestSnapshotSemanticErrorV8(
            f"depth snapshot {field_name} must be one nonnegative int64"
        )
    return value


def _semantic_admission_sha256_v8(value: VerifiedPublicDepthRestSnapshotV8) -> str:
    document = {
        "symbol": value.symbol,
        # RFC 8785 JSON numbers are restricted to the IEEE-754 exact integer
        # domain. The source contract is signed-int64, so hash the validated
        # decimal text losslessly instead of narrowing its allowed values.
        "last_update_id_text": str(value.last_update_id),
        "event_time_ms_text": str(value.event_time_ms),
        "transaction_time_ms_text": str(value.transaction_time_ms),
        "bids": tuple((level.price_text, level.quantity_text) for level in value.bids),
        "asks": tuple((level.price_text, level.quantity_text) for level in value.asks),
        "plan_sha256": value.plan_sha256,
        "attempt_payload_sha256": value.attempt_payload_sha256,
        "raw_record_sha256": value.raw_record_sha256,
        "body_sha256": value.body_sha256,
        "queue_admission_verified": value.queue_admission_verified,
        "body_semantics_valid": value.body_semantics_valid,
        "verification_scope": value.verification_scope,
        "qualification_only": value.qualification_only,
        "promoting": value.promoting,
        "promotion_ready": value.promotion_ready,
        "wal_durability_verified": value.wal_durability_verified,
        "finality_fence_verified": value.finality_fence_verified,
        "freshness_verified": value.freshness_verified,
        "coverage_complete": value.coverage_complete,
        "m2_certified": value.m2_certified,
        "book_bridge_certified": value.book_bridge_certified,
        "liquidity_signal_emitted": value.liquidity_signal_emitted,
        "order_execution_enabled": value.order_execution_enabled,
    }
    return hashlib.sha256(
        _SEMANTIC_HASH_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _require_sha256(value: object, field_name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be lowercase SHA-256 text")
