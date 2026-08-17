from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import InitVar, asdict, dataclass, field
from typing import Final, Literal, cast

from signalbot.capture.clock_health_report import (
    CLOCK_HEADER_RTT_MAX_MS_V1,
    CLOCK_RATE_ERROR_PPM_V1,
    CLOCK_SAMPLE_MAX_AGE_MS_V1,
    WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_MS_V1,
    VenueClockSampleV1,
    clock_samples_rate_continuous,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.membership import (
    CurrentVerifiedRawMembershipLeafUseV2,
    VerifiedRawMembershipLeafV2,
    canonical_verified_raw_membership_leaf_v2,
    consume_current_verified_raw_membership_leaf_v2,
)
from signalbot.r4b_v2.capture.models import TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingPlanV9,
    ProvisionalUsdmVenueClockRestCapturePlanV9,
    provisional_promoting_plan_sha256_v9,
    validate_provisional_promoting_capture_plans_v9,
)
from signalbot.r4b_v2.capture.rest_clock import (
    PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
    PublicUsdmVenueClockRestAttemptPayloadV9,
    public_usdm_venue_clock_rest_plan_sha256_v9,
)

_SCHEMA_VERSION = "r4b_v2_usdm_venue_clock_sample_m1_v1"
_FACTORY_TOKEN = object()
_ROW_HASH_DOMAIN = b"R4B_V2_USDM_VENUE_CLOCK_SAMPLE_M1\0"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_SIGNED_INT64 = (1 << 63) - 1
_TIMESTAMP_QUANTIZATION_MARGIN_MS = 1
_TEXT_INTEGER_FIELDS = frozenset(
    {
        "available_at_monotonic_ns",
        "completion_wall_monotonic_residual_ns",
        "header_rtt_ns",
        "header_wall_monotonic_residual_ns",
        "offset_lower_ms",
        "offset_upper_ms",
        "receipt_monotonic_ns",
        "request_started_monotonic_ns",
        "response_completed_monotonic_ns",
        "response_first_header_monotonic_ns",
        "server_time_ms",
    }
)

USDM_VENUE_CLOCK_M1_ONLY_REASON_V2: Final = (
    "CURRENT_SIGNED_RAW_MEMBER_AND_STRICT_CLOCK_SAMPLE_ONLY_"
    "NO_PREFIX_CONTINUITY_OR_CAUSAL_CURSOR_COMPLETENESS"
)

_PARSER_CONTRACT = {
    "age_ms_inclusive": CLOCK_SAMPLE_MAX_AGE_MS_V1,
    "body_schema": {"serverTime": "NONNEGATIVE_INT64"},
    "endpoint": "/fapi/v1/time",
    "header_rtt_ms_inclusive": CLOCK_HEADER_RTT_MAX_MS_V1,
    "maximum_rate_error_ppm_inclusive": CLOCK_RATE_ERROR_PPM_V1,
    "membership": "CONSUME_CURRENT_VERIFIED_RAW_MEMBERSHIP_ONCE",
    "residual_ms_inclusive": WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_MS_V1,
    "route_id": "usdm_venue_clock_rest",
    "schema_version": _SCHEMA_VERSION,
    "scope": "SINGLE_SAMPLE_ONLY_NO_CAUSAL_CURSOR",
    "status": 200,
    "transport": "https",
    "venue": "usdm_futures",
}
USDM_VENUE_CLOCK_M1_PARSER_CONTRACT_SHA256_V2: Final = hashlib.sha256(
    canonical_json_line(_PARSER_CONTRACT)
).hexdigest()


class UsdmVenueClockSampleM1ContractErrorV2(RuntimeError):
    """One current public USD-M time member fails the strict M1 contract."""


@dataclass(frozen=True, slots=True)
class UsdmVenueClockSampleM1V2:
    """Factory-only single clock sample; never a complete causal cursor."""

    venue: VenueV2
    route_id: Literal["usdm_venue_clock_rest"]
    promoting_plan_sha256: str
    rest_plan_sha256: str
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
    scheduled_slot_wall_ms: int
    request_started_wall_ms: int
    request_started_monotonic_ns: int
    response_first_header_wall_ms: int
    response_first_header_monotonic_ns: int
    response_completed_wall_ms: int
    response_completed_monotonic_ns: int
    server_time_ms: int
    header_rtt_ns: int
    wall_header_elapsed_ms: int
    header_wall_monotonic_residual_ns: int
    completion_wall_elapsed_ms: int
    completion_wall_monotonic_residual_ns: int
    offset_lower_ms: int
    offset_upper_ms: int
    available_at_monotonic_ns: int
    _factory_token: InitVar[object | None] = None
    m1_payload_sha256: str = field(init=False, default="")
    schema_version: Literal["r4b_v2_usdm_venue_clock_sample_m1_v1"] = field(
        init=False,
        default=_SCHEMA_VERSION,
    )
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise UsdmVenueClockSampleM1ContractErrorV2(
                "USD-M venue-clock M1 samples are factory-sealed"
            )
        _validate_sample(self)
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        object.__setattr__(self, "m1_payload_sha256", _sample_hash(self))

    @property
    def parser_bound(self) -> Literal[True]:
        return True

    @property
    def body_semantics_verified(self) -> Literal[True]:
        return True

    @property
    def current_membership_consumed(self) -> Literal[True]:
        return True

    @property
    def current_authority_claimed(self) -> Literal[False]:
        return False

    @property
    def durable_membership_reverified_after_factory(self) -> Literal[False]:
        return False

    @property
    def freshness_at_factory_verified(self) -> Literal[False]:
        return False

    @property
    def prefix_rate_continuity_verified(self) -> Literal[False]:
        return False

    @property
    def causal_cursor_complete(self) -> Literal[False]:
        return False

    @property
    def authority_reason(self) -> str:
        return USDM_VENUE_CLOCK_M1_ONLY_REASON_V2

    def as_clock_health_sample_v1(self) -> VenueClockSampleV1:
        """Project exact shared clock mathematics without adding authority."""

        return VenueClockSampleV1(
            schema_version="venue_clock_sample_v1",
            market="futures",
            request_role="futures_venue_time",
            source_ingest_seq=self.ingest_seq,
            request_started_at_ms=self.request_started_wall_ms,
            request_started_monotonic_ns=self.request_started_monotonic_ns,
            response_first_byte_at_ms=self.response_first_header_wall_ms,
            response_first_byte_monotonic_ns=(
                self.response_first_header_monotonic_ns
            ),
            response_completed_at_ms=self.response_completed_wall_ms,
            response_completed_monotonic_ns=self.response_completed_monotonic_ns,
            server_time_ms=self.server_time_ms,
            header_rtt_ns=self.header_rtt_ns,
            wall_header_elapsed_ms=self.wall_header_elapsed_ms,
            header_wall_monotonic_residual_ns=(
                self.header_wall_monotonic_residual_ns
            ),
            completion_wall_elapsed_ms=self.completion_wall_elapsed_ms,
            completion_wall_monotonic_residual_ns=(
                self.completion_wall_monotonic_residual_ns
            ),
            offset_lower_ms=self.offset_lower_ms,
            offset_upper_ms=self.offset_upper_ms,
            available_at_monotonic_ns=self.available_at_monotonic_ns,
        )


def parse_current_verified_usdm_venue_clock_sample_m1_v2(
    current_use: CurrentVerifiedRawMembershipLeafUseV2,
    *,
    promoting_plans: Sequence[ProvisionalPromotingPlanV9],
) -> UsdmVenueClockSampleM1V2:
    """Consume one current signed member and derive one strict clock sample."""

    leaf = consume_current_verified_raw_membership_leaf_v2(current_use)
    canonical_verified_raw_membership_leaf_v2(leaf)
    frozen_plans = tuple(promoting_plans)
    validate_provisional_promoting_capture_plans_v9(frozen_plans)
    bundle_sha256 = provisional_promoting_plan_sha256_v9(frozen_plans)
    if leaf.authority.plan_sha256 != bundle_sha256:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "trusted WAL authority differs from the frozen v9 capture plan"
        )
    clock_plans = tuple(
        cast(ProvisionalUsdmVenueClockRestCapturePlanV9, item)
        for item in frozen_plans
        if type(item) is ProvisionalUsdmVenueClockRestCapturePlanV9
    )
    if len(clock_plans) != 1:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "v9 capture plan has no unique USD-M venue-clock owner"
        )
    return _parse_current_leaf(leaf, clock_plans[0], bundle_sha256)


def usdm_venue_clock_sample_fresh_at_m1_v2(
    sample: UsdmVenueClockSampleM1V2,
    *,
    observed_monotonic_ns: int,
) -> bool:
    """Return true only from availability through the inclusive 60-second age."""

    _require_factory_sample(sample)
    if type(observed_monotonic_ns) is not int or observed_monotonic_ns < 0:
        raise ValueError("clock sample age observation must be nonnegative monotonic ns")
    if observed_monotonic_ns < sample.available_at_monotonic_ns:
        return False
    return (
        observed_monotonic_ns - sample.available_at_monotonic_ns
        <= CLOCK_SAMPLE_MAX_AGE_MS_V1 * 1_000_000
    )


def usdm_venue_clock_samples_rate_continuous_m1_v2(
    previous: UsdmVenueClockSampleM1V2,
    current: UsdmVenueClockSampleM1V2,
) -> bool:
    """Apply the existing inclusive +/-1,000 ppm continuity mathematics."""

    _require_factory_sample(previous)
    _require_factory_sample(current)
    if (
        current.promoting_plan_sha256 != previous.promoting_plan_sha256
        or current.rest_plan_sha256 != previous.rest_plan_sha256
        or current.protocol_sha256 != previous.protocol_sha256
        or current.session_id != previous.session_id
        or current.available_at_monotonic_ns < previous.available_at_monotonic_ns
    ):
        return False
    return clock_samples_rate_continuous(
        previous.as_clock_health_sample_v1(),
        current.as_clock_health_sample_v1(),
    )


def canonical_usdm_venue_clock_sample_m1_v2(
    sample: UsdmVenueClockSampleM1V2,
) -> bytes:
    """Serialize one factory sample with its deterministic source hash."""

    _require_factory_sample(sample)
    return _canonical_sample_document(sample)


def _parse_current_leaf(
    leaf: VerifiedRawMembershipLeafV2,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    bundle_sha256: str,
) -> UsdmVenueClockSampleM1V2:
    record = leaf.record
    if (
        record.transport is not TransportV2.HTTPS
        or record.venue is not VenueV2.USDM_FUTURES
        or record.route_id != plan.route_id
        or record.symbol is not None
        or record.plan_id != plan.name
        or record.frame_seq is not None
        or record.source_logical_key
        != PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9
    ):
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock raw member differs from its exact public outer identity"
        )
    try:
        payload = PublicUsdmVenueClockRestAttemptPayloadV9.from_canonical_bytes(
            record.payload_bytes(),
            plan=plan,
        )
        server_time_ms = _parse_server_time_body(payload)
    except (TypeError, ValueError) as exc:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock attempt or body fails its exact semantic contract"
        ) from exc
    if (
        payload.session_id != record.session_id
        or payload.protocol_hash != record.protocol_hash
        or payload.connection_id != record.connection_id
        or payload.connection_generation != record.generation
        or payload.completion_admission_wall_ms != record.receipt_wall_ms
        or payload.completion_admission_monotonic_ns != record.receipt_monotonic_ns
    ):
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock attempt lineage or completion differs from its raw envelope"
        )
    first_wall = payload.response_first_header_wall_ms
    first_monotonic = payload.response_first_header_monotonic_ns
    if first_wall is None or first_monotonic is None:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "successful venue-clock sample requires first-header clocks"
        )
    completion_wall = payload.completion_admission_wall_ms
    completion_monotonic = payload.completion_admission_monotonic_ns
    if not (
        payload.request_started_wall_ms
        <= first_wall
        <= payload.attempt_ended_wall_ms
        <= completion_wall
    ):
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock wall timestamps are reversed"
        )
    if not (
        payload.request_started_monotonic_ns
        <= first_monotonic
        <= payload.attempt_ended_monotonic_ns
        <= completion_monotonic
    ):
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock monotonic timestamps are reversed"
        )
    header_rtt_ns = first_monotonic - payload.request_started_monotonic_ns
    wall_header_elapsed_ms = first_wall - payload.request_started_wall_ms
    header_residual_ns = abs(wall_header_elapsed_ms * 1_000_000 - header_rtt_ns)
    completion_wall_elapsed_ms = completion_wall - first_wall
    completion_elapsed_ns = completion_monotonic - first_monotonic
    completion_residual_ns = abs(
        completion_wall_elapsed_ms * 1_000_000 - completion_elapsed_ns
    )
    if header_rtt_ns > CLOCK_HEADER_RTT_MAX_MS_V1 * 1_000_000:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock header RTT exceeds the inclusive 2000ms bound"
        )
    residual_bound_ns = WALL_MONOTONIC_HEALTHY_MAX_RESIDUAL_MS_V1 * 1_000_000
    if header_residual_ns > residual_bound_ns or completion_residual_ns > residual_bound_ns:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock wall/monotonic residual exceeds the inclusive 2ms bound"
        )

    return UsdmVenueClockSampleM1V2(
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_venue_clock_rest",
        promoting_plan_sha256=bundle_sha256,
        rest_plan_sha256=public_usdm_venue_clock_rest_plan_sha256_v9(plan),
        capture_authority_sha256=leaf.authority_sha256,
        protocol_sha256=record.protocol_hash,
        parser_contract_sha256=USDM_VENUE_CLOCK_M1_PARSER_CONTRACT_SHA256_V2,
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
        scheduled_slot_wall_ms=payload.scheduled_slot_wall_ms,
        request_started_wall_ms=payload.request_started_wall_ms,
        request_started_monotonic_ns=payload.request_started_monotonic_ns,
        response_first_header_wall_ms=first_wall,
        response_first_header_monotonic_ns=first_monotonic,
        response_completed_wall_ms=completion_wall,
        response_completed_monotonic_ns=completion_monotonic,
        server_time_ms=server_time_ms,
        header_rtt_ns=header_rtt_ns,
        wall_header_elapsed_ms=wall_header_elapsed_ms,
        header_wall_monotonic_residual_ns=header_residual_ns,
        completion_wall_elapsed_ms=completion_wall_elapsed_ms,
        completion_wall_monotonic_residual_ns=completion_residual_ns,
        offset_lower_ms=(
            server_time_ms - first_wall - _TIMESTAMP_QUANTIZATION_MARGIN_MS
        ),
        offset_upper_ms=(
            server_time_ms
            - payload.request_started_wall_ms
            + _TIMESTAMP_QUANTIZATION_MARGIN_MS
        ),
        available_at_monotonic_ns=completion_monotonic,
        _factory_token=_FACTORY_TOKEN,
    )


def _parse_server_time_body(payload: PublicUsdmVenueClockRestAttemptPayloadV9) -> int:
    if (
        payload.response_status != 200
        or not payload.payload_complete
        or payload.error_category is not None
        or payload.admission_cancellation_requested
    ):
        raise ValueError("venue-clock sample requires one successful HTTP 200 attempt")
    body = payload.body_bytes()
    try:
        decoded = body.decode("utf-8")
        parsed = json.loads(decoded, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("venue-clock body must be strict UTF-8 JSON") from exc
    if type(parsed) is not dict or tuple(parsed) != ("serverTime",):
        raise ValueError("venue-clock body must contain only serverTime")
    value = parsed["serverTime"]
    if type(value) is not int or not 0 <= value <= _MAX_SIGNED_INT64:
        raise ValueError("venue-clock serverTime must be one nonnegative int64")
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("venue-clock body contains a duplicate JSON key")
        result[key] = value
    return result


def _validate_sample(sample: UsdmVenueClockSampleM1V2) -> None:
    if sample.schema_version != _SCHEMA_VERSION:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "unsupported venue-clock M1 schema"
        )
    if (
        sample.venue is not VenueV2.USDM_FUTURES
        or sample.route_id != "usdm_venue_clock_rest"
    ):
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock M1 sample has the wrong route"
        )
    for value, field_name in (
        (sample.promoting_plan_sha256, "promoting_plan_sha256"),
        (sample.rest_plan_sha256, "rest_plan_sha256"),
        (sample.capture_authority_sha256, "capture_authority_sha256"),
        (sample.protocol_sha256, "protocol_sha256"),
        (sample.parser_contract_sha256, "parser_contract_sha256"),
        (sample.m0_leaf_sha256, "m0_leaf_sha256"),
        (sample.raw_payload_hash_v2, "raw_payload_hash_v2"),
        (sample.attempt_payload_sha256, "attempt_payload_sha256"),
    ):
        if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
            raise UsdmVenueClockSampleM1ContractErrorV2(
                f"{field_name} must be lowercase SHA-256 text"
            )
    if sample.parser_contract_sha256 != USDM_VENUE_CLOCK_M1_PARSER_CONTRACT_SHA256_V2:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock M1 parser contract hash differs"
        )
    if sample.available_at_monotonic_ns != sample.response_completed_monotonic_ns:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock availability differs from completion admission"
        )
    if sample.offset_lower_ms > sample.offset_upper_ms:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock offset interval is reversed"
        )
    try:
        sample.as_clock_health_sample_v1()
    except ValueError as exc:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock sample differs from frozen clock mathematics"
        ) from exc


def _require_factory_sample(sample: UsdmVenueClockSampleM1V2) -> None:
    if type(sample) is not UsdmVenueClockSampleM1V2:
        raise TypeError("sample must be an exact UsdmVenueClockSampleM1V2")
    if getattr(sample, "_factory_seal", None) is not _FACTORY_TOKEN:
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock M1 sample factory seal differs"
        )
    _validate_sample(sample)
    if sample.m1_payload_sha256 != _sample_hash(sample):
        raise UsdmVenueClockSampleM1ContractErrorV2(
            "venue-clock M1 sample hash differs"
        )


def _sample_hash(sample: UsdmVenueClockSampleM1V2) -> str:
    document = _sample_document(sample)
    document["m1_payload_sha256"] = ""
    return hashlib.sha256(
        _ROW_HASH_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _canonical_sample_document(sample: UsdmVenueClockSampleM1V2) -> bytes:
    return canonical_json_line(_sample_document(sample))


def _sample_document(sample: UsdmVenueClockSampleM1V2) -> dict[str, object]:
    document: dict[str, object] = asdict(sample)
    document.pop("_factory_seal")
    for field_name in _TEXT_INTEGER_FIELDS:
        value = document.pop(field_name)
        if type(value) is not int:
            raise UsdmVenueClockSampleM1ContractErrorV2(
                f"{field_name} must remain an exact integer"
            )
        document[f"{field_name}_text"] = str(value)
    return document
