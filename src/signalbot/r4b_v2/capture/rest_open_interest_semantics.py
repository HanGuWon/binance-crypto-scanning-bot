from __future__ import annotations

import json
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, DecimalException
from typing import Final, Literal, cast

from signalbot.r4b_v2.capture.rest import (
    PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2,
    PublicOiRestAttemptPayloadV2,
)

PUBLIC_OI_REST_BODY_CONTRACT_URL_V2: Final = (
    "https://developers.binance.com/en/docs/catalog/"
    "core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/"
    "market-data#open-interest"
)
PUBLIC_OI_REST_BODY_EXTRA_FIELDS_POLICY_V2: Final = (
    "REJECT_UNDOCUMENTED_FIELDS_FAIL_CLOSED"
)
PUBLIC_OI_REST_BODY_SEMANTIC_SCOPE_V2: Final = "SINGLE_COMPLETE_HTTP_200_BODY_ONLY"

_EXPECTED_FIELDS = frozenset({"openInterest", "symbol", "time"})
_NONNEGATIVE_DECIMAL_TEXT_RE = re.compile(
    r"^[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$"
)
_MAX_SIGNED_INT64 = (1 << 63) - 1
_RESULT_FACTORY_TOKEN = object()


class PublicOiRestBodySemanticErrorV2(ValueError):
    """One retained REST attempt does not match the documented OI body contract."""


@dataclass(frozen=True, slots=True)
class VerifiedPublicOiRestBodyV2:
    """Factory-sealed semantic value for one response body, never a coverage proof."""

    symbol: str
    open_interest_text: str
    open_interest: Decimal
    transaction_time_ms: int
    body_sha256: str
    body_semantics_valid: Literal[True] = True
    verification_scope: Literal["SINGLE_COMPLETE_HTTP_200_BODY_ONLY"] = (
        "SINGLE_COMPLETE_HTTP_200_BODY_ONLY"
    )
    freshness_verified: Literal[False] = False
    coverage_complete: Literal[False] = False
    m2_certified: Literal[False] = False
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RESULT_FACTORY_TOKEN:
            raise TypeError("verified public OI bodies are factory-sealed")
        object.__setattr__(self, "_factory_seal", _RESULT_FACTORY_TOKEN)
        if type(self.symbol) is not str or not self.symbol:
            raise TypeError("verified public OI symbol must be nonempty exact text")
        if type(self.open_interest_text) is not str:
            raise TypeError("verified open-interest text must be exact text")
        if type(self.open_interest) is not Decimal:
            raise TypeError("verified open interest must be an exact Decimal")
        if not self.open_interest.is_finite() or self.open_interest < 0:
            raise ValueError("verified open interest must be finite and nonnegative")
        if type(self.transaction_time_ms) is not int:
            raise TypeError("verified transaction time must be an exact integer")
        if not 0 <= self.transaction_time_ms <= _MAX_SIGNED_INT64:
            raise ValueError("verified transaction time must be a nonnegative int64")
        if type(self.body_sha256) is not str or re.fullmatch(
            r"[0-9a-f]{64}", self.body_sha256
        ) is None:
            raise ValueError("verified body_sha256 must be lowercase SHA-256 text")
        if self.body_semantics_valid is not True:
            raise ValueError("verified body semantics must be true")
        if self.verification_scope != PUBLIC_OI_REST_BODY_SEMANTIC_SCOPE_V2:
            raise ValueError("verified body has an unsupported verification scope")
        if (
            self.freshness_verified is not False
            or self.coverage_complete is not False
            or self.m2_certified is not False
        ):
            raise ValueError("one OI body cannot certify freshness, coverage, or M2")


def verify_public_oi_rest_attempt_body_v2(
    payload: PublicOiRestAttemptPayloadV2,
) -> VerifiedPublicOiRestBodyV2:
    """Verify the documented semantics of one exact, complete HTTP 200 body.

    Unknown fields are rejected deliberately.  This is a promotion boundary, so an
    additive upstream schema change must be reviewed instead of silently becoming
    verified evidence.  Successful return does not establish response freshness,
    schedule/census coverage, source completeness, or M2.
    """

    if type(payload) is not PublicOiRestAttemptPayloadV2:
        raise TypeError("payload must be an exact PublicOiRestAttemptPayloadV2")
    if (
        payload.response_status != 200
        or not payload.payload_complete
        or payload.error_category is not None
        or payload.admission_cancellation_requested
    ):
        raise PublicOiRestBodySemanticErrorV2(
            "body semantics require one complete, error-free, uncancelled HTTP 200 attempt"
        )
    body = payload.body_bytes()
    if not body or len(body) > PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI body must be nonempty and within the frozen byte cap"
        )
    document = _parse_strict_json_object(body)
    if set(document) != _EXPECTED_FIELDS:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI body fields differ from the documented exact schema"
        )

    symbol = document["symbol"]
    if type(symbol) is not str or symbol != payload.symbol:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI response symbol differs from the exact requested symbol"
        )
    open_interest_text = document["openInterest"]
    if type(open_interest_text) is not str:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI openInterest must be documented string text"
        )
    open_interest = _parse_nonnegative_open_interest(open_interest_text)
    transaction_time_ms = document["time"]
    if (
        type(transaction_time_ms) is not int
        or not 0 <= transaction_time_ms <= _MAX_SIGNED_INT64
    ):
        raise PublicOiRestBodySemanticErrorV2(
            "public OI transaction time must be one nonnegative int64 millisecond value"
        )

    return VerifiedPublicOiRestBodyV2(
        symbol=symbol,
        open_interest_text=open_interest_text,
        open_interest=open_interest,
        transaction_time_ms=transaction_time_ms,
        body_sha256=payload.body_sha256,
        _factory_token=_RESULT_FACTORY_TOKEN,
    )


def _parse_strict_json_object(body: bytes) -> dict[str, object]:
    try:
        decoded = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI body must be strict UTF-8"
        ) from exc

    def reject_duplicate_keys(
        pairs: list[tuple[str, object]],
    ) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise PublicOiRestBodySemanticErrorV2(
                    "public OI body contains a duplicate JSON key"
                )
            result[key] = value
        return result

    def reject_float(value: str) -> object:
        raise PublicOiRestBodySemanticErrorV2(
            f"public OI body contains an unsupported JSON float: {value}"
        )

    def reject_constant(value: str) -> object:
        raise PublicOiRestBodySemanticErrorV2(
            f"public OI body contains a non-finite JSON constant: {value}"
        )

    try:
        parsed = json.loads(
            decoded,
            object_pairs_hook=reject_duplicate_keys,
            parse_float=reject_float,
            parse_constant=reject_constant,
        )
    except (json.JSONDecodeError, RecursionError) as exc:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI body is invalid JSON"
        ) from exc
    if type(parsed) is not dict:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI body must be one JSON object"
        )
    return cast(dict[str, object], parsed)


def _parse_nonnegative_open_interest(value: str) -> Decimal:
    if _NONNEGATIVE_DECIMAL_TEXT_RE.fullmatch(value) is None:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI openInterest is not finite nonnegative decimal text"
        )
    try:
        parsed = Decimal(value)
    except (DecimalException, ValueError) as exc:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI openInterest cannot be represented exactly"
        ) from exc
    if not parsed.is_finite() or parsed < 0:
        raise PublicOiRestBodySemanticErrorV2(
            "public OI openInterest must be finite and nonnegative"
        )
    return parsed
