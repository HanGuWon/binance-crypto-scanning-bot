from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import (
    PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2,
    PublicOiRestAttemptPayloadV2,
    PublicOiRestTerminalObservationV2,
)
from signalbot.r4b_v2.capture.rest_open_interest_semantics import (
    PUBLIC_OI_REST_BODY_CONTRACT_URL_V2,
    PUBLIC_OI_REST_BODY_EXTRA_FIELDS_POLICY_V2,
    PublicOiRestBodySemanticErrorV2,
    VerifiedPublicOiRestBodyV2,
    verify_public_oi_rest_attempt_body_v2,
)

_SLOT = 1_700_000_000_000


def _plan() -> ProvisionalPromotingRestCapturePlanV2:
    return ProvisionalPromotingRestCapturePlanV2(
        name="v2-usdm-public-rest-oi-promoting-semantics",
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public_rest",
        method="GET",
        endpoint="/fapi/v1/openInterest",
        symbols=("BTCUSDT",),
    )


def _payload(
    body: bytes,
    *,
    response_status: int = 200,
) -> PublicOiRestAttemptPayloadV2:
    observation = PublicOiRestTerminalObservationV2.for_plan(
        _plan(),
        symbol="BTCUSDT",
        poll_cycle_seq=1,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=_SLOT,
        attempt=1,
        request_started_wall_ms=_SLOT + 1,
        request_started_monotonic_ns=10_000,
        response_first_header_wall_ms=_SLOT + 2,
        response_first_header_monotonic_ns=10_001,
        attempt_ended_wall_ms=_SLOT + 3,
        attempt_ended_monotonic_ns=10_002,
        response_status=response_status,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        body=body,
    )
    return observation.build_payload(ReceiptTimestamp(_SLOT + 4, 10_003))


def test_exact_documented_body_returns_single_attempt_only_semantics() -> None:
    payload = _payload(
        b'{"time":1589437530011,"symbol":"BTCUSDT","openInterest":"10659.509"}'
    )

    verified = verify_public_oi_rest_attempt_body_v2(payload)

    assert verified.symbol == "BTCUSDT"
    assert verified.open_interest_text == "10659.509"
    assert verified.open_interest == Decimal("10659.509")
    assert verified.transaction_time_ms == 1_589_437_530_011
    assert verified.body_sha256 == payload.body_sha256
    assert verified.body_semantics_valid is True
    assert verified.verification_scope == "SINGLE_COMPLETE_HTTP_200_BODY_ONLY"
    assert verified.freshness_verified is False
    assert verified.coverage_complete is False
    assert verified.m2_certified is False
    assert PUBLIC_OI_REST_BODY_CONTRACT_URL_V2.endswith("#open-interest")
    assert (
        PUBLIC_OI_REST_BODY_EXTRA_FIELDS_POLICY_V2
        == "REJECT_UNDOCUMENTED_FIELDS_FAIL_CLOSED"
    )


@pytest.mark.parametrize(
    ("open_interest", "expected"),
    [
        ("0", Decimal("0")),
        ("0.00000000", Decimal("0.00000000")),
        ("0001.2500", Decimal("1.2500")),
        ("1e-8", Decimal("1e-8")),
        ("1E+8", Decimal("1E+8")),
    ],
)
def test_finite_nonnegative_decimal_text_boundaries_are_exact(
    open_interest: str,
    expected: Decimal,
) -> None:
    body = (
        f'{{"openInterest":"{open_interest}","symbol":"BTCUSDT","time":0}}'
    ).encode()

    verified = verify_public_oi_rest_attempt_body_v2(_payload(body))

    assert verified.open_interest == expected
    assert verified.transaction_time_ms == 0


def test_body_at_exact_byte_cap_remains_bounded_and_parseable() -> None:
    core = b'{"openInterest":"0","symbol":"BTCUSDT","time":0}'
    body = core + b" " * (PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2 - len(core))

    verified = verify_public_oi_rest_attempt_body_v2(_payload(body))

    assert verified.open_interest == 0


@pytest.mark.parametrize(
    "body",
    [
        b"[]",
        b"null",
        b"not-json",
        b'\xff{"openInterest":"1","symbol":"BTCUSDT","time":1}',
        b'{"openInterest":"1","symbol":"BTCUSDT"}',
        b'{"openInterest":"1","symbol":"BTCUSDT","time":1,"newField":1}',
        b'{"openInterest":"1","openInterest":"2","symbol":"BTCUSDT","time":1}',
        b'{"openInterest":"1","symbol":"ETHUSDT","time":1}',
        b'{"openInterest":1,"symbol":"BTCUSDT","time":1}',
        (b"[" * 1_500) + (b"]" * 1_500),
    ],
)
def test_nonobject_malformed_drift_duplicate_mismatch_and_wrong_types_fail_closed(
    body: bytes,
) -> None:
    with pytest.raises(PublicOiRestBodySemanticErrorV2):
        verify_public_oi_rest_attempt_body_v2(_payload(body))


@pytest.mark.parametrize(
    "open_interest",
    ["", "-1", "+1", ".1", "1.", "1_0", "NaN", "Infinity", " 1", "1 "],
)
def test_nonfinite_negative_or_ambiguous_open_interest_text_is_rejected(
    open_interest: str,
) -> None:
    body = (
        f'{{"openInterest":"{open_interest}","symbol":"BTCUSDT","time":1}}'
    ).encode()

    with pytest.raises(PublicOiRestBodySemanticErrorV2, match="openInterest"):
        verify_public_oi_rest_attempt_body_v2(_payload(body))


@pytest.mark.parametrize(
    "time_json",
    ["true", '"1"', "1.0", "-1", str(1 << 63), "NaN", "Infinity"],
)
def test_transaction_time_requires_nonnegative_signed_int64(time_json: str) -> None:
    body = (
        f'{{"openInterest":"1","symbol":"BTCUSDT","time":{time_json}}}'
    ).encode()

    with pytest.raises(PublicOiRestBodySemanticErrorV2):
        verify_public_oi_rest_attempt_body_v2(_payload(body))


def test_only_exact_error_free_uncancelled_http_200_is_semantically_eligible() -> None:
    body = b'{"openInterest":"1","symbol":"BTCUSDT","time":1}'

    with pytest.raises(PublicOiRestBodySemanticErrorV2, match="HTTP 200"):
        verify_public_oi_rest_attempt_body_v2(_payload(body, response_status=201))

    cancelled = replace(_payload(body), admission_cancellation_requested=True)
    with pytest.raises(PublicOiRestBodySemanticErrorV2, match="uncancelled"):
        verify_public_oi_rest_attempt_body_v2(cancelled)


def test_verified_result_cannot_be_forged_through_public_constructor() -> None:
    with pytest.raises(TypeError, match="factory-sealed"):
        VerifiedPublicOiRestBodyV2(
            symbol="BTCUSDT",
            open_interest_text="1",
            open_interest=Decimal("1"),
            transaction_time_ms=1,
            body_sha256="0" * 64,
        )
