"""Narrow WebSocket owner-stop and local cursor-finality evidence.

These types bind the last frame retained by each local WebSocket owner to one
capture finality receipt.  They deliberately make no claim that Binance
emitted no messages outside the retained prefix, that retained payloads parse,
or that M2 causal inputs are complete.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Sequence
from dataclasses import InitVar, asdict, dataclass, field, fields
from typing import Literal, cast

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.pipeline import CaptureFinalityFenceReceiptV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingPlanV8,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_plan_sha256_v8,
    provisional_promoting_stream_census_sha256_v2,
    validate_provisional_promoting_capture_plans_v2,
    validate_provisional_promoting_capture_plans_v8,
)

_STOP_SCHEMA = "r4b_v2_websocket_route_stop_receipt_v1"
_STOP_SCHEMA_V8 = "r4b_v2_websocket_route_stop_receipt_v8"
_FINALIZED_CURSOR_SCHEMA = "r4b_v2_finalized_websocket_route_cursor_v1"
_FINALIZED_CURSOR_SCHEMA_V8 = "r4b_v2_finalized_websocket_route_cursor_v8"
_CLOSURE_ENTRY_SCHEMA = "r4b_v2_websocket_route_cursor_closure_entry_v1"
_CLOSURE_ENTRY_SCHEMA_V8 = "r4b_v2_websocket_route_cursor_closure_entry_v8"
_CLOSURE_PAIR_SCHEMA = "r4b_v2_websocket_route_cursor_closure_pair_v1"
_CLOSURE_PAIR_SCHEMA_V8 = "r4b_v2_websocket_route_cursor_closure_pair_v8"
_STOP_DOMAIN = b"R4B_V2_WEBSOCKET_ROUTE_STOP_RECEIPT\0"
_STOP_DOMAIN_V8 = b"R4B_V2_WEBSOCKET_ROUTE_STOP_RECEIPT_V8\0"
_FINALIZED_CURSOR_DOMAIN = b"R4B_V2_FINALIZED_WEBSOCKET_ROUTE_CURSOR\0"
_FINALIZED_CURSOR_DOMAIN_V8 = b"R4B_V2_FINALIZED_WEBSOCKET_ROUTE_CURSOR_V8\0"
_CLOSURE_ENTRY_DOMAIN = b"R4B_V2_WEBSOCKET_ROUTE_CURSOR_CLOSURE_ENTRY\0"
_CLOSURE_ENTRY_DOMAIN_V8 = b"R4B_V2_WEBSOCKET_ROUTE_CURSOR_CLOSURE_ENTRY_V8\0"
_CLOSURE_PAIR_DOMAIN = b"R4B_V2_WEBSOCKET_ROUTE_CURSOR_CLOSURE_PAIR\0"
_CLOSURE_PAIR_DOMAIN_V8 = b"R4B_V2_WEBSOCKET_ROUTE_CURSOR_CLOSURE_PAIR_V8\0"
_STOP_FACTORY_TOKEN = object()
_STOP_FACTORY_TOKEN_V8 = object()
_FINALIZED_CURSOR_FACTORY_TOKEN = object()
_FINALIZED_CURSOR_FACTORY_TOKEN_V8 = object()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_ROUTES = ("usdm_market", "usdm_public")
_MAX_IDENTITY_LENGTH = 256


class WebSocketRouteFinalityErrorV2(ValueError):
    """Raised when local WebSocket stop/finality evidence is inconsistent."""


@dataclass(frozen=True, slots=True)
class WebSocketRouteStopReceiptV2:
    """Factory-only receipt for one exact retained cursor at ``OWNER_STOP``."""

    session_id: str
    process_boot_id: str
    plan_bundle_sha256: str
    plan_id: str
    venue: Literal["usdm_futures"]
    route_id: Literal["usdm_market", "usdm_public"]
    stream_census_sha256: str
    stream_count: int
    connection_id: str
    generation: int
    last_frame_seq: int
    last_ingest_seq: int
    last_receipt_wall_ms: int
    last_receipt_monotonic_ns: int
    stop_observed_wall_ms: int
    stop_observed_monotonic_ns: int
    close_reason: Literal["OWNER_STOP"]
    pending_source_gap: Literal[False]
    retained_frame_parser_health_claimed: Literal[False]
    upstream_message_completeness_claimed: Literal[False]
    m2_certified: Literal[False]
    schema_version: Literal["r4b_v2_websocket_route_stop_receipt_v1"] = _STOP_SCHEMA
    receipt_sha256: str = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _STOP_FACTORY_TOKEN:
            raise TypeError("WebSocket route stop receipts are factory-sealed")
        object.__setattr__(self, "_factory_seal", _STOP_FACTORY_TOKEN)
        _validate_stop_material(self, verify_digest=False)
        object.__setattr__(self, "receipt_sha256", _stop_receipt_sha256(self))


@dataclass(frozen=True, slots=True)
class WebSocketRouteStopReceiptV8:
    """Factory-only OWNER_STOP cursor bound to the full four-plan V8 authority."""

    session_id: str
    process_boot_id: str
    plan_bundle_sha256: str
    plan_id: str
    venue: Literal["usdm_futures"]
    route_id: Literal["usdm_market", "usdm_public"]
    stream_census_sha256: str
    stream_count: int
    connection_id: str
    generation: int
    last_frame_seq: int
    last_ingest_seq: int
    last_receipt_wall_ms: int
    last_receipt_monotonic_ns: int
    stop_observed_wall_ms: int
    stop_observed_monotonic_ns: int
    close_reason: Literal["OWNER_STOP"]
    pending_source_gap: Literal[False]
    retained_frame_parser_health_claimed: Literal[False]
    upstream_message_completeness_claimed: Literal[False]
    m2_certified: Literal[False]
    depth_bridge_complete_claimed: Literal[False]
    schema_version: Literal["r4b_v2_websocket_route_stop_receipt_v8"] = (
        _STOP_SCHEMA_V8
    )
    receipt_sha256: str = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _STOP_FACTORY_TOKEN_V8:
            raise TypeError("V8 WebSocket route stop receipts are factory-sealed")
        object.__setattr__(self, "_factory_seal", _STOP_FACTORY_TOKEN_V8)
        _validate_stop_material_v8(self, verify_digest=False)
        object.__setattr__(self, "receipt_sha256", _stop_receipt_sha256_v8(self))


@dataclass(frozen=True, slots=True)
class FinalizedWebSocketRouteCursorV2:
    """Factory-only join of one local owner-stop cursor and capture finality."""

    stop_receipt: WebSocketRouteStopReceiptV2 = field(repr=False)
    stop_receipt_sha256: str
    finality_receipt_sha256: str
    finality_authority_sha256: str
    finality_exact_prefix_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    local_route_cursor_finalized: Literal[True]
    retained_frame_parser_health_claimed: Literal[False]
    upstream_message_completeness_claimed: Literal[False]
    m2_certified: Literal[False]
    schema_version: Literal["r4b_v2_finalized_websocket_route_cursor_v1"] = (
        _FINALIZED_CURSOR_SCHEMA
    )
    cursor_sha256: str = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FINALIZED_CURSOR_FACTORY_TOKEN:
            raise TypeError("finalized WebSocket route cursors are factory-sealed")
        object.__setattr__(self, "_factory_seal", _FINALIZED_CURSOR_FACTORY_TOKEN)
        _validate_finalized_cursor_material(self, finality_receipt=None, verify_digest=False)
        object.__setattr__(self, "cursor_sha256", _finalized_cursor_sha256(self))


@dataclass(frozen=True, slots=True)
class FinalizedWebSocketRouteCursorV8:
    """Factory-only V8 join of one OWNER_STOP cursor and local finality."""

    stop_receipt: WebSocketRouteStopReceiptV8 = field(repr=False)
    stop_receipt_sha256: str
    finality_receipt_sha256: str
    finality_authority_sha256: str
    finality_exact_prefix_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    local_route_cursor_finalized: Literal[True]
    retained_frame_parser_health_claimed: Literal[False]
    upstream_message_completeness_claimed: Literal[False]
    m2_certified: Literal[False]
    depth_bridge_complete_claimed: Literal[False]
    schema_version: Literal["r4b_v2_finalized_websocket_route_cursor_v8"] = (
        _FINALIZED_CURSOR_SCHEMA_V8
    )
    cursor_sha256: str = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FINALIZED_CURSOR_FACTORY_TOKEN_V8:
            raise TypeError("V8 finalized WebSocket route cursors are factory-sealed")
        object.__setattr__(
            self,
            "_factory_seal",
            _FINALIZED_CURSOR_FACTORY_TOKEN_V8,
        )
        _validate_finalized_cursor_material_v8(
            self,
            finality_receipt=None,
            verify_digest=False,
        )
        object.__setattr__(
            self,
            "cursor_sha256",
            _finalized_cursor_sha256_v8(self),
        )


@dataclass(frozen=True, slots=True)
class WebSocketRouteCursorClosureEntryV2:
    """Serializable projection persisted only by the CLEAN session closure."""

    session_id: str
    process_boot_id: str
    plan_bundle_sha256: str
    plan_id: str
    venue: Literal["usdm_futures"]
    route_id: Literal["usdm_market", "usdm_public"]
    stream_census_sha256: str
    stream_count: int
    connection_id: str
    generation: int
    last_frame_seq: int
    last_ingest_seq: int
    last_receipt_wall_ms: int
    last_receipt_monotonic_ns: int
    stop_observed_wall_ms: int
    stop_observed_monotonic_ns: int
    stop_receipt_sha256: str
    finalized_route_cursor_sha256: str
    finality_receipt_sha256: str
    finality_authority_sha256: str
    finality_exact_prefix_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    close_reason: Literal["OWNER_STOP"]
    pending_source_gap: Literal[False]
    local_route_cursor_finalized: Literal[True]
    retained_frame_parser_health_claimed: Literal[False]
    upstream_message_completeness_claimed: Literal[False]
    m2_certified: Literal[False]
    schema_version: Literal["r4b_v2_websocket_route_cursor_closure_entry_v1"] = (
        _CLOSURE_ENTRY_SCHEMA
    )

    def __post_init__(self) -> None:
        _validate_closure_entry_material(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(
            _CLOSURE_ENTRY_DOMAIN + canonical_json_line(self)
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class WebSocketRouteCursorClosureEntryV8:
    """V8 closure projection that preserves only local cursor finality claims."""

    session_id: str
    process_boot_id: str
    plan_bundle_sha256: str
    plan_id: str
    venue: Literal["usdm_futures"]
    route_id: Literal["usdm_market", "usdm_public"]
    stream_census_sha256: str
    stream_count: int
    connection_id: str
    generation: int
    last_frame_seq: int
    last_ingest_seq: int
    last_receipt_wall_ms: int
    last_receipt_monotonic_ns: int
    stop_observed_wall_ms: int
    stop_observed_monotonic_ns: int
    stop_receipt_sha256: str
    finalized_route_cursor_sha256: str
    finality_receipt_sha256: str
    finality_authority_sha256: str
    finality_exact_prefix_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    close_reason: Literal["OWNER_STOP"]
    pending_source_gap: Literal[False]
    local_route_cursor_finalized: Literal[True]
    retained_frame_parser_health_claimed: Literal[False]
    upstream_message_completeness_claimed: Literal[False]
    m2_certified: Literal[False]
    depth_bridge_complete_claimed: Literal[False]
    schema_version: Literal[
        "r4b_v2_websocket_route_cursor_closure_entry_v8"
    ] = _CLOSURE_ENTRY_SCHEMA_V8

    def __post_init__(self) -> None:
        _validate_closure_entry_material_v8(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(
            _CLOSURE_ENTRY_DOMAIN_V8 + canonical_json_line(self)
        ).hexdigest()


type FinalizedWebSocketRouteCursorPairV2 = tuple[
    FinalizedWebSocketRouteCursorV2,
    FinalizedWebSocketRouteCursorV2,
]
type WebSocketRouteCursorClosurePairV2 = tuple[
    WebSocketRouteCursorClosureEntryV2,
    WebSocketRouteCursorClosureEntryV2,
]
type FinalizedWebSocketRouteCursorPairV8 = tuple[
    FinalizedWebSocketRouteCursorV8,
    FinalizedWebSocketRouteCursorV8,
]
type WebSocketRouteCursorClosurePairV8 = tuple[
    WebSocketRouteCursorClosureEntryV8,
    WebSocketRouteCursorClosureEntryV8,
]


def _issue_websocket_route_stop_receipt_v2(
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
    plan: ProvisionalPromotingCapturePlanV2,
    *,
    session_id: str,
    process_boot_id: str,
    connection_id: str,
    generation: int,
    last_frame_seq: int,
    last_ingest_seq: int,
    last_receipt_wall_ms: int,
    last_receipt_monotonic_ns: int,
    stop_observed: ReceiptTimestamp,
) -> WebSocketRouteStopReceiptV2:
    """Issue at the lifecycle owner-stop seam; callers must not re-export it."""

    _validate_selected_plan(promoting_plans, plan)
    if type(stop_observed) is not ReceiptTimestamp:
        raise TypeError("owner-stop observation must be an exact ReceiptTimestamp")
    return WebSocketRouteStopReceiptV2(
        session_id=session_id,
        process_boot_id=process_boot_id,
        plan_bundle_sha256=provisional_promoting_plan_sha256_v2(promoting_plans),
        plan_id=plan.name,
        venue=cast(Literal["usdm_futures"], plan.venue.value),
        route_id=plan.route_id,
        stream_census_sha256=provisional_promoting_stream_census_sha256_v2(plan),
        stream_count=len(plan.streams),
        connection_id=connection_id,
        generation=generation,
        last_frame_seq=last_frame_seq,
        last_ingest_seq=last_ingest_seq,
        last_receipt_wall_ms=last_receipt_wall_ms,
        last_receipt_monotonic_ns=last_receipt_monotonic_ns,
        stop_observed_wall_ms=stop_observed.received_at_ms,
        stop_observed_monotonic_ns=stop_observed.received_monotonic_ns,
        close_reason="OWNER_STOP",
        pending_source_gap=False,
        retained_frame_parser_health_claimed=False,
        upstream_message_completeness_claimed=False,
        m2_certified=False,
        _factory_token=_STOP_FACTORY_TOKEN,
    )


def validate_websocket_route_stop_receipt_v2(
    receipt: WebSocketRouteStopReceiptV2,
    *,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2] | None = None,
    plan: ProvisionalPromotingCapturePlanV2 | None = None,
) -> None:
    """Revalidate factory provenance, canonical digest, and optional plan scope."""

    if type(receipt) is not WebSocketRouteStopReceiptV2:
        raise TypeError("route stop receipt must be an exact WebSocketRouteStopReceiptV2")
    if getattr(receipt, "_factory_seal", None) is not _STOP_FACTORY_TOKEN:
        raise WebSocketRouteFinalityErrorV2("route stop receipt lacks lifecycle provenance")
    _validate_stop_material(receipt, verify_digest=True)
    if (promoting_plans is None) != (plan is None):
        raise TypeError("promoting_plans and plan must be supplied together")
    if promoting_plans is None or plan is None:
        return
    _validate_selected_plan(promoting_plans, plan)
    expected = (
        provisional_promoting_plan_sha256_v2(promoting_plans),
        plan.name,
        plan.venue.value,
        plan.route_id,
        provisional_promoting_stream_census_sha256_v2(plan),
        len(plan.streams),
    )
    observed = (
        receipt.plan_bundle_sha256,
        receipt.plan_id,
        receipt.venue,
        receipt.route_id,
        receipt.stream_census_sha256,
        receipt.stream_count,
    )
    if observed != expected:
        raise WebSocketRouteFinalityErrorV2(
            "route stop receipt differs from its exact promoting plan"
        )


def _issue_websocket_route_stop_receipt_v8(
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    plan: ProvisionalPromotingCapturePlanV2,
    *,
    session_id: str,
    process_boot_id: str,
    connection_id: str,
    generation: int,
    last_frame_seq: int,
    last_ingest_seq: int,
    last_receipt_wall_ms: int,
    last_receipt_monotonic_ns: int,
    stop_observed: ReceiptTimestamp,
) -> WebSocketRouteStopReceiptV8:
    """Issue one exact V8 OWNER_STOP receipt at the lifecycle seam."""

    _validate_selected_plan_v8(promoting_plans, plan)
    if type(stop_observed) is not ReceiptTimestamp:
        raise TypeError("V8 owner-stop observation must be an exact ReceiptTimestamp")
    return WebSocketRouteStopReceiptV8(
        session_id=session_id,
        process_boot_id=process_boot_id,
        plan_bundle_sha256=provisional_promoting_plan_sha256_v8(promoting_plans),
        plan_id=plan.name,
        venue=cast(Literal["usdm_futures"], plan.venue.value),
        route_id=plan.route_id,
        stream_census_sha256=provisional_promoting_stream_census_sha256_v2(plan),
        stream_count=len(plan.streams),
        connection_id=connection_id,
        generation=generation,
        last_frame_seq=last_frame_seq,
        last_ingest_seq=last_ingest_seq,
        last_receipt_wall_ms=last_receipt_wall_ms,
        last_receipt_monotonic_ns=last_receipt_monotonic_ns,
        stop_observed_wall_ms=stop_observed.received_at_ms,
        stop_observed_monotonic_ns=stop_observed.received_monotonic_ns,
        close_reason="OWNER_STOP",
        pending_source_gap=False,
        retained_frame_parser_health_claimed=False,
        upstream_message_completeness_claimed=False,
        m2_certified=False,
        depth_bridge_complete_claimed=False,
        _factory_token=_STOP_FACTORY_TOKEN_V8,
    )


def validate_websocket_route_stop_receipt_v8(
    receipt: WebSocketRouteStopReceiptV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...] | None = None,
    plan: ProvisionalPromotingCapturePlanV2 | None = None,
) -> None:
    """Revalidate exact V8 provenance, digest, and optional four-plan scope."""

    if type(receipt) is not WebSocketRouteStopReceiptV8:
        raise TypeError("route stop receipt must be an exact WebSocketRouteStopReceiptV8")
    if getattr(receipt, "_factory_seal", None) is not _STOP_FACTORY_TOKEN_V8:
        raise WebSocketRouteFinalityErrorV2(
            "V8 route stop receipt lacks lifecycle provenance"
        )
    _validate_stop_material_v8(receipt, verify_digest=True)
    if (promoting_plans is None) != (plan is None):
        raise TypeError("V8 promoting_plans and plan must be supplied together")
    if promoting_plans is None or plan is None:
        return
    _validate_selected_plan_v8(promoting_plans, plan)
    expected = (
        provisional_promoting_plan_sha256_v8(promoting_plans),
        plan.name,
        plan.venue.value,
        plan.route_id,
        provisional_promoting_stream_census_sha256_v2(plan),
        len(plan.streams),
    )
    observed = (
        receipt.plan_bundle_sha256,
        receipt.plan_id,
        receipt.venue,
        receipt.route_id,
        receipt.stream_census_sha256,
        receipt.stream_count,
    )
    if observed != expected:
        raise WebSocketRouteFinalityErrorV2(
            "V8 route stop receipt differs from its exact four-plan authority"
        )


def finalize_websocket_route_cursor_v2(
    receipt: WebSocketRouteStopReceiptV2,
    finality_receipt: CaptureFinalityFenceReceiptV2,
) -> FinalizedWebSocketRouteCursorV2:
    """Join one lifecycle-issued terminal cursor to one local finality receipt."""

    validate_websocket_route_stop_receipt_v2(receipt)
    _validate_finality_receipt(finality_receipt)
    if receipt.last_ingest_seq > finality_receipt.fence_ingest_seq:
        raise WebSocketRouteFinalityErrorV2(
            "finality tail precedes the WebSocket route stop cursor"
        )
    if receipt.stop_observed_monotonic_ns > finality_receipt.fence_monotonic_ns:
        raise WebSocketRouteFinalityErrorV2(
            "finality fence precedes the WebSocket owner-stop observation"
        )
    return FinalizedWebSocketRouteCursorV2(
        stop_receipt=receipt,
        stop_receipt_sha256=receipt.receipt_sha256,
        finality_receipt_sha256=finality_receipt.sha256,
        finality_authority_sha256=finality_receipt.authority_sha256,
        finality_exact_prefix_sha256=finality_receipt.exact_prefix_sha256,
        finality_prefix_proof_sha256=finality_receipt.prefix_proof_sha256,
        finality_tail_ingest_seq=finality_receipt.fence_ingest_seq,
        local_route_cursor_finalized=True,
        retained_frame_parser_health_claimed=False,
        upstream_message_completeness_claimed=False,
        m2_certified=False,
        _factory_token=_FINALIZED_CURSOR_FACTORY_TOKEN,
    )


def validate_finalized_websocket_route_cursor_v2(
    cursor: FinalizedWebSocketRouteCursorV2,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2 | None = None,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2] | None = None,
    plan: ProvisionalPromotingCapturePlanV2 | None = None,
) -> None:
    """Revalidate one sealed join and, when supplied, its live input objects."""

    if type(cursor) is not FinalizedWebSocketRouteCursorV2:
        raise TypeError(
            "finalized route cursor must be an exact FinalizedWebSocketRouteCursorV2"
        )
    if getattr(cursor, "_factory_seal", None) is not _FINALIZED_CURSOR_FACTORY_TOKEN:
        raise WebSocketRouteFinalityErrorV2(
            "finalized WebSocket route cursor lacks factory provenance"
        )
    _validate_finalized_cursor_material(
        cursor,
        finality_receipt=finality_receipt,
        verify_digest=True,
    )
    validate_websocket_route_stop_receipt_v2(
        cursor.stop_receipt,
        promoting_plans=promoting_plans,
        plan=plan,
    )


def finalize_websocket_route_cursor_pair_v2(
    receipts: tuple[WebSocketRouteStopReceiptV2, WebSocketRouteStopReceiptV2],
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
) -> FinalizedWebSocketRouteCursorPairV2:
    """Mint the canonical market/public pair for one finalized runtime tail."""

    if type(receipts) is not tuple or len(receipts) != 2:
        raise TypeError("WebSocket stop receipts must be an exact two-item tuple")
    validate_provisional_promoting_capture_plans_v2(promoting_plans)
    by_route: dict[str, WebSocketRouteStopReceiptV2] = {}
    for receipt in receipts:
        validate_websocket_route_stop_receipt_v2(receipt)
        if receipt.route_id in by_route:
            raise WebSocketRouteFinalityErrorV2("duplicate WebSocket route stop receipt")
        by_route[receipt.route_id] = receipt
    if tuple(sorted(by_route)) != tuple(sorted(_EXPECTED_ROUTES)):
        raise WebSocketRouteFinalityErrorV2(
            "WebSocket route stop receipts do not cover the canonical route pair"
        )
    cursors: list[FinalizedWebSocketRouteCursorV2] = []
    for route_id in _EXPECTED_ROUTES:
        plan = _selected_route_plan(promoting_plans, route_id)
        receipt = by_route[route_id]
        validate_websocket_route_stop_receipt_v2(
            receipt,
            promoting_plans=promoting_plans,
            plan=plan,
        )
        cursors.append(finalize_websocket_route_cursor_v2(receipt, finality_receipt))
    pair = (cursors[0], cursors[1])
    validate_finalized_websocket_route_cursor_pair_v2(
        pair,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
    )
    return pair


def validate_finalized_websocket_route_cursor_pair_v2(
    pair: FinalizedWebSocketRouteCursorPairV2,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2] | None = None,
) -> None:
    """Require an exact, ordered market/public pair bound to one finality tail."""

    if type(pair) is not tuple or len(pair) != 2:
        raise TypeError("finalized WebSocket route cursors must be an exact pair")
    _validate_finality_receipt(finality_receipt)
    if any(type(cursor) is not FinalizedWebSocketRouteCursorV2 for cursor in pair):
        raise TypeError("finalized WebSocket route cursor pair contains a foreign type")
    if tuple(cursor.stop_receipt.route_id for cursor in pair) != _EXPECTED_ROUTES:
        raise WebSocketRouteFinalityErrorV2(
            "finalized WebSocket route cursors are not in canonical route order"
        )
    identities = {
        (
            cursor.stop_receipt.session_id,
            cursor.stop_receipt.process_boot_id,
            cursor.stop_receipt.plan_bundle_sha256,
        )
        for cursor in pair
    }
    if len(identities) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "finalized WebSocket route cursors cross session or plan authority"
        )
    for cursor in pair:
        plan = (
            None
            if promoting_plans is None
            else _selected_route_plan(promoting_plans, cursor.stop_receipt.route_id)
        )
        validate_finalized_websocket_route_cursor_v2(
            cursor,
            finality_receipt=finality_receipt,
            promoting_plans=promoting_plans,
            plan=plan,
        )


def websocket_route_cursor_closure_pair_v2(
    pair: FinalizedWebSocketRouteCursorPairV2,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
) -> WebSocketRouteCursorClosurePairV2:
    """Project a verified factory pair into canonical closure-safe values."""

    validate_finalized_websocket_route_cursor_pair_v2(
        pair,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
    )
    entries = tuple(_closure_entry(cursor) for cursor in pair)
    return cast(WebSocketRouteCursorClosurePairV2, entries)


def websocket_route_cursor_closure_pair_sha256_v2(
    pair: WebSocketRouteCursorClosurePairV2,
) -> str:
    """Hash the exact canonical persisted market/public cursor pair."""

    validate_websocket_route_cursor_closure_pair_v2(pair)
    document = {
        "schema_version": _CLOSURE_PAIR_SCHEMA,
        "entries": tuple(asdict(entry) for entry in pair),
    }
    return hashlib.sha256(
        _CLOSURE_PAIR_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def validate_websocket_route_cursor_closure_pair_v2(
    pair: WebSocketRouteCursorClosurePairV2,
) -> None:
    if type(pair) is not tuple or len(pair) != 2:
        raise TypeError("persisted WebSocket cursor entries must be an exact pair")
    if any(type(entry) is not WebSocketRouteCursorClosureEntryV2 for entry in pair):
        raise TypeError("persisted WebSocket cursor pair contains a foreign type")
    for entry in pair:
        entry.__post_init__()
    if tuple(entry.route_id for entry in pair) != _EXPECTED_ROUTES:
        raise WebSocketRouteFinalityErrorV2(
            "persisted WebSocket cursor entries are not in canonical route order"
        )
    identities = {
        (entry.session_id, entry.process_boot_id, entry.plan_bundle_sha256)
        for entry in pair
    }
    if len(identities) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "persisted WebSocket cursor entries cross session or plan authority"
        )
    finality = {
        (
            entry.finality_receipt_sha256,
            entry.finality_authority_sha256,
            entry.finality_exact_prefix_sha256,
            entry.finality_prefix_proof_sha256,
            entry.finality_tail_ingest_seq,
        )
        for entry in pair
    }
    if len(finality) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "persisted WebSocket cursor entries bind different finality tails"
        )


def finalize_websocket_route_cursor_v8(
    receipt: WebSocketRouteStopReceiptV8,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    plan: ProvisionalPromotingCapturePlanV2,
) -> FinalizedWebSocketRouteCursorV8:
    """Join one exact V8 OWNER_STOP receipt to one exact local finality tail."""

    validate_websocket_route_stop_receipt_v8(
        receipt,
        promoting_plans=promoting_plans,
        plan=plan,
    )
    _validate_finality_receipt(finality_receipt)
    if receipt.last_ingest_seq > finality_receipt.fence_ingest_seq:
        raise WebSocketRouteFinalityErrorV2(
            "V8 finality tail precedes the WebSocket route stop cursor"
        )
    if receipt.stop_observed_monotonic_ns > finality_receipt.fence_monotonic_ns:
        raise WebSocketRouteFinalityErrorV2(
            "V8 finality fence precedes the WebSocket owner-stop observation"
        )
    cursor = FinalizedWebSocketRouteCursorV8(
        stop_receipt=receipt,
        stop_receipt_sha256=receipt.receipt_sha256,
        finality_receipt_sha256=finality_receipt.sha256,
        finality_authority_sha256=finality_receipt.authority_sha256,
        finality_exact_prefix_sha256=finality_receipt.exact_prefix_sha256,
        finality_prefix_proof_sha256=finality_receipt.prefix_proof_sha256,
        finality_tail_ingest_seq=finality_receipt.fence_ingest_seq,
        local_route_cursor_finalized=True,
        retained_frame_parser_health_claimed=False,
        upstream_message_completeness_claimed=False,
        m2_certified=False,
        depth_bridge_complete_claimed=False,
        _factory_token=_FINALIZED_CURSOR_FACTORY_TOKEN_V8,
    )
    validate_finalized_websocket_route_cursor_v8(
        cursor,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
        plan=plan,
    )
    return cursor


def validate_finalized_websocket_route_cursor_v8(
    cursor: FinalizedWebSocketRouteCursorV8,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    plan: ProvisionalPromotingCapturePlanV2,
) -> None:
    """Revalidate one V8 cursor against every exact live authority input."""

    if type(cursor) is not FinalizedWebSocketRouteCursorV8:
        raise TypeError(
            "V8 finalized route cursor must be an exact "
            "FinalizedWebSocketRouteCursorV8"
        )
    if (
        getattr(cursor, "_factory_seal", None)
        is not _FINALIZED_CURSOR_FACTORY_TOKEN_V8
    ):
        raise WebSocketRouteFinalityErrorV2(
            "V8 finalized WebSocket route cursor lacks factory provenance"
        )
    _validate_finalized_cursor_material_v8(
        cursor,
        finality_receipt=finality_receipt,
        verify_digest=True,
    )
    validate_websocket_route_stop_receipt_v8(
        cursor.stop_receipt,
        promoting_plans=promoting_plans,
        plan=plan,
    )


def finalize_websocket_route_cursor_pair_v8(
    receipts: tuple[WebSocketRouteStopReceiptV8, WebSocketRouteStopReceiptV8],
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
) -> FinalizedWebSocketRouteCursorPairV8:
    """Mint the canonical V8 market/public pair for one local finality tail."""

    if type(receipts) is not tuple or len(receipts) != 2:
        raise TypeError("V8 WebSocket stop receipts must be an exact two-item tuple")
    _validate_promoting_plans_tuple_v8(promoting_plans)
    by_route: dict[str, WebSocketRouteStopReceiptV8] = {}
    for receipt in receipts:
        validate_websocket_route_stop_receipt_v8(receipt)
        if receipt.route_id in by_route:
            raise WebSocketRouteFinalityErrorV2(
                "duplicate V8 WebSocket route stop receipt"
            )
        by_route[receipt.route_id] = receipt
    if tuple(sorted(by_route)) != tuple(sorted(_EXPECTED_ROUTES)):
        raise WebSocketRouteFinalityErrorV2(
            "V8 WebSocket route stop receipts do not cover the canonical route pair"
        )
    cursors: list[FinalizedWebSocketRouteCursorV8] = []
    for route_id in _EXPECTED_ROUTES:
        plan = _selected_route_plan_v8(promoting_plans, route_id)
        cursors.append(
            finalize_websocket_route_cursor_v8(
                by_route[route_id],
                finality_receipt,
                promoting_plans=promoting_plans,
                plan=plan,
            )
        )
    pair = (cursors[0], cursors[1])
    validate_finalized_websocket_route_cursor_pair_v8(
        pair,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
    )
    return pair


def validate_finalized_websocket_route_cursor_pair_v8(
    pair: FinalizedWebSocketRouteCursorPairV8,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
) -> None:
    """Require one exact ordered V8 route pair under one session and tail."""

    if type(pair) is not tuple or len(pair) != 2:
        raise TypeError("V8 finalized WebSocket route cursors must be an exact pair")
    if any(type(cursor) is not FinalizedWebSocketRouteCursorV8 for cursor in pair):
        raise TypeError(
            "V8 finalized WebSocket route cursor pair contains a foreign type"
        )
    _validate_finality_receipt(finality_receipt)
    _validate_promoting_plans_tuple_v8(promoting_plans)
    if tuple(cursor.stop_receipt.route_id for cursor in pair) != _EXPECTED_ROUTES:
        raise WebSocketRouteFinalityErrorV2(
            "V8 finalized WebSocket route cursors are not in canonical route order"
        )
    identities = {
        (
            cursor.stop_receipt.session_id,
            cursor.stop_receipt.process_boot_id,
            cursor.stop_receipt.plan_bundle_sha256,
        )
        for cursor in pair
    }
    if len(identities) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "V8 finalized WebSocket route cursors cross session or plan authority"
        )
    for cursor in pair:
        validate_finalized_websocket_route_cursor_v8(
            cursor,
            finality_receipt=finality_receipt,
            promoting_plans=promoting_plans,
            plan=_selected_route_plan_v8(
                promoting_plans,
                cursor.stop_receipt.route_id,
            ),
        )


def websocket_route_cursor_closure_pair_v8(
    pair: FinalizedWebSocketRouteCursorPairV8,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
) -> WebSocketRouteCursorClosurePairV8:
    """Project a verified V8 cursor pair into canonical closure-safe values."""

    validate_finalized_websocket_route_cursor_pair_v8(
        pair,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
    )
    entries = tuple(_closure_entry_v8(cursor) for cursor in pair)
    closure_pair = cast(WebSocketRouteCursorClosurePairV8, entries)
    validate_websocket_route_cursor_closure_pair_v8(
        closure_pair,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
    )
    return closure_pair


def websocket_route_cursor_closure_pair_sha256_v8(
    pair: WebSocketRouteCursorClosurePairV8,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2 | None = None,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...] | None = None,
) -> str:
    """Hash the exact canonical persisted V8 market/public cursor pair."""

    validate_websocket_route_cursor_closure_pair_v8(
        pair,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
    )
    document = {
        "schema_version": _CLOSURE_PAIR_SCHEMA_V8,
        "entries": tuple(asdict(entry) for entry in pair),
    }
    return hashlib.sha256(
        _CLOSURE_PAIR_DOMAIN_V8 + canonical_json_line(document)
    ).hexdigest()


def validate_websocket_route_cursor_closure_pair_v8(
    pair: WebSocketRouteCursorClosurePairV8,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2 | None = None,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...] | None = None,
) -> None:
    """Validate one V8 closure projection and optional exact live bindings."""

    if type(pair) is not tuple or len(pair) != 2:
        raise TypeError("persisted V8 WebSocket cursor entries must be an exact pair")
    if any(type(entry) is not WebSocketRouteCursorClosureEntryV8 for entry in pair):
        raise TypeError("persisted V8 WebSocket cursor pair contains a foreign type")
    for entry in pair:
        entry.__post_init__()
    if tuple(entry.route_id for entry in pair) != _EXPECTED_ROUTES:
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 WebSocket cursor entries are not in canonical route order"
        )
    identities = {
        (entry.session_id, entry.process_boot_id, entry.plan_bundle_sha256)
        for entry in pair
    }
    if len(identities) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 WebSocket cursor entries cross session or plan authority"
        )
    finality = {
        (
            entry.finality_receipt_sha256,
            entry.finality_authority_sha256,
            entry.finality_exact_prefix_sha256,
            entry.finality_prefix_proof_sha256,
            entry.finality_tail_ingest_seq,
        )
        for entry in pair
    }
    if len(finality) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 WebSocket cursor entries bind different finality tails"
        )
    if finality_receipt is not None:
        _validate_finality_receipt(finality_receipt)
        expected_finality = (
            finality_receipt.sha256,
            finality_receipt.authority_sha256,
            finality_receipt.exact_prefix_sha256,
            finality_receipt.prefix_proof_sha256,
            finality_receipt.fence_ingest_seq,
        )
        if next(iter(finality)) != expected_finality:
            raise WebSocketRouteFinalityErrorV2(
                "persisted V8 WebSocket cursor pair differs from finality receipt"
            )
    if promoting_plans is not None:
        _validate_promoting_plans_tuple_v8(promoting_plans)
        for entry in pair:
            _validate_closure_entry_plan_scope_v8(
                entry,
                promoting_plans=promoting_plans,
                plan=_selected_route_plan_v8(promoting_plans, entry.route_id),
            )


def _closure_entry(
    cursor: FinalizedWebSocketRouteCursorV2,
) -> WebSocketRouteCursorClosureEntryV2:
    receipt = cursor.stop_receipt
    return WebSocketRouteCursorClosureEntryV2(
        session_id=receipt.session_id,
        process_boot_id=receipt.process_boot_id,
        plan_bundle_sha256=receipt.plan_bundle_sha256,
        plan_id=receipt.plan_id,
        venue=receipt.venue,
        route_id=receipt.route_id,
        stream_census_sha256=receipt.stream_census_sha256,
        stream_count=receipt.stream_count,
        connection_id=receipt.connection_id,
        generation=receipt.generation,
        last_frame_seq=receipt.last_frame_seq,
        last_ingest_seq=receipt.last_ingest_seq,
        last_receipt_wall_ms=receipt.last_receipt_wall_ms,
        last_receipt_monotonic_ns=receipt.last_receipt_monotonic_ns,
        stop_observed_wall_ms=receipt.stop_observed_wall_ms,
        stop_observed_monotonic_ns=receipt.stop_observed_monotonic_ns,
        stop_receipt_sha256=receipt.receipt_sha256,
        finalized_route_cursor_sha256=cursor.cursor_sha256,
        finality_receipt_sha256=cursor.finality_receipt_sha256,
        finality_authority_sha256=cursor.finality_authority_sha256,
        finality_exact_prefix_sha256=cursor.finality_exact_prefix_sha256,
        finality_prefix_proof_sha256=cursor.finality_prefix_proof_sha256,
        finality_tail_ingest_seq=cursor.finality_tail_ingest_seq,
        close_reason=receipt.close_reason,
        pending_source_gap=False,
        local_route_cursor_finalized=True,
        retained_frame_parser_health_claimed=False,
        upstream_message_completeness_claimed=False,
        m2_certified=False,
    )


def _closure_entry_v8(
    cursor: FinalizedWebSocketRouteCursorV8,
) -> WebSocketRouteCursorClosureEntryV8:
    receipt = cursor.stop_receipt
    return WebSocketRouteCursorClosureEntryV8(
        session_id=receipt.session_id,
        process_boot_id=receipt.process_boot_id,
        plan_bundle_sha256=receipt.plan_bundle_sha256,
        plan_id=receipt.plan_id,
        venue=receipt.venue,
        route_id=receipt.route_id,
        stream_census_sha256=receipt.stream_census_sha256,
        stream_count=receipt.stream_count,
        connection_id=receipt.connection_id,
        generation=receipt.generation,
        last_frame_seq=receipt.last_frame_seq,
        last_ingest_seq=receipt.last_ingest_seq,
        last_receipt_wall_ms=receipt.last_receipt_wall_ms,
        last_receipt_monotonic_ns=receipt.last_receipt_monotonic_ns,
        stop_observed_wall_ms=receipt.stop_observed_wall_ms,
        stop_observed_monotonic_ns=receipt.stop_observed_monotonic_ns,
        stop_receipt_sha256=receipt.receipt_sha256,
        finalized_route_cursor_sha256=cursor.cursor_sha256,
        finality_receipt_sha256=cursor.finality_receipt_sha256,
        finality_authority_sha256=cursor.finality_authority_sha256,
        finality_exact_prefix_sha256=cursor.finality_exact_prefix_sha256,
        finality_prefix_proof_sha256=cursor.finality_prefix_proof_sha256,
        finality_tail_ingest_seq=cursor.finality_tail_ingest_seq,
        close_reason=receipt.close_reason,
        pending_source_gap=False,
        local_route_cursor_finalized=True,
        retained_frame_parser_health_claimed=False,
        upstream_message_completeness_claimed=False,
        m2_certified=False,
        depth_bridge_complete_claimed=False,
    )


def _validate_stop_material(
    receipt: WebSocketRouteStopReceiptV2,
    *,
    verify_digest: bool,
) -> None:
    for value, name in (
        (receipt.session_id, "session_id"),
        (receipt.process_boot_id, "process_boot_id"),
        (receipt.plan_id, "plan_id"),
        (receipt.connection_id, "connection_id"),
    ):
        _require_identity(value, name)
    for value, name in (
        (receipt.plan_bundle_sha256, "plan_bundle_sha256"),
        (receipt.stream_census_sha256, "stream_census_sha256"),
    ):
        _require_sha256(value, name)
    if receipt.venue != "usdm_futures" or receipt.route_id not in _EXPECTED_ROUTES:
        raise WebSocketRouteFinalityErrorV2("route stop receipt scope is unsupported")
    for value, name in (
        (receipt.stream_count, "stream_count"),
        (receipt.generation, "generation"),
        (receipt.last_frame_seq, "last_frame_seq"),
        (receipt.last_ingest_seq, "last_ingest_seq"),
    ):
        _require_positive_int(value, name)
    for value, name in (
        (receipt.last_receipt_wall_ms, "last_receipt_wall_ms"),
        (receipt.last_receipt_monotonic_ns, "last_receipt_monotonic_ns"),
        (receipt.stop_observed_wall_ms, "stop_observed_wall_ms"),
        (receipt.stop_observed_monotonic_ns, "stop_observed_monotonic_ns"),
    ):
        _require_nonnegative_int(value, name)
    if receipt.stop_observed_monotonic_ns < receipt.last_receipt_monotonic_ns:
        raise WebSocketRouteFinalityErrorV2(
            "owner-stop observation precedes the retained cursor"
        )
    if receipt.close_reason != "OWNER_STOP" or receipt.pending_source_gap is not False:
        raise WebSocketRouteFinalityErrorV2(
            "route stop receipt must represent a gap-free local OWNER_STOP tail"
        )
    _require_nonclaims(
        receipt.retained_frame_parser_health_claimed,
        receipt.upstream_message_completeness_claimed,
        receipt.m2_certified,
    )
    if receipt.schema_version != _STOP_SCHEMA:
        raise WebSocketRouteFinalityErrorV2("unsupported route stop receipt schema")
    if verify_digest:
        _require_sha256(receipt.receipt_sha256, "receipt_sha256")
        if not hmac.compare_digest(receipt.receipt_sha256, _stop_receipt_sha256(receipt)):
            raise WebSocketRouteFinalityErrorV2("route stop receipt digest differs")


def _validate_stop_material_v8(
    receipt: WebSocketRouteStopReceiptV8,
    *,
    verify_digest: bool,
) -> None:
    for value, name in (
        (receipt.session_id, "session_id"),
        (receipt.process_boot_id, "process_boot_id"),
        (receipt.plan_id, "plan_id"),
        (receipt.connection_id, "connection_id"),
    ):
        _require_identity(value, name)
    for value, name in (
        (receipt.plan_bundle_sha256, "plan_bundle_sha256"),
        (receipt.stream_census_sha256, "stream_census_sha256"),
    ):
        _require_sha256(value, name)
    if receipt.venue != "usdm_futures" or receipt.route_id not in _EXPECTED_ROUTES:
        raise WebSocketRouteFinalityErrorV2("V8 route stop receipt scope is unsupported")
    for value, name in (
        (receipt.stream_count, "stream_count"),
        (receipt.generation, "generation"),
        (receipt.last_frame_seq, "last_frame_seq"),
        (receipt.last_ingest_seq, "last_ingest_seq"),
    ):
        _require_positive_int(value, name)
    for value, name in (
        (receipt.last_receipt_wall_ms, "last_receipt_wall_ms"),
        (receipt.last_receipt_monotonic_ns, "last_receipt_monotonic_ns"),
        (receipt.stop_observed_wall_ms, "stop_observed_wall_ms"),
        (receipt.stop_observed_monotonic_ns, "stop_observed_monotonic_ns"),
    ):
        _require_nonnegative_int(value, name)
    if receipt.stop_observed_monotonic_ns < receipt.last_receipt_monotonic_ns:
        raise WebSocketRouteFinalityErrorV2(
            "V8 owner-stop observation precedes the retained cursor"
        )
    if receipt.close_reason != "OWNER_STOP" or receipt.pending_source_gap is not False:
        raise WebSocketRouteFinalityErrorV2(
            "V8 route stop receipt must represent a gap-free local OWNER_STOP tail"
        )
    _require_nonclaims(
        receipt.retained_frame_parser_health_claimed,
        receipt.upstream_message_completeness_claimed,
        receipt.m2_certified,
        receipt.depth_bridge_complete_claimed,
    )
    if receipt.schema_version != _STOP_SCHEMA_V8:
        raise WebSocketRouteFinalityErrorV2("unsupported V8 route stop receipt schema")
    if verify_digest:
        _require_sha256(receipt.receipt_sha256, "receipt_sha256")
        if not hmac.compare_digest(
            receipt.receipt_sha256,
            _stop_receipt_sha256_v8(receipt),
        ):
            raise WebSocketRouteFinalityErrorV2("V8 route stop receipt digest differs")


def _validate_finalized_cursor_material(
    cursor: FinalizedWebSocketRouteCursorV2,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2 | None,
    verify_digest: bool,
) -> None:
    if type(cursor.stop_receipt) is not WebSocketRouteStopReceiptV2:
        raise TypeError("finalized route cursor requires an exact stop receipt")
    validate_websocket_route_stop_receipt_v2(cursor.stop_receipt)
    for value, name in (
        (cursor.stop_receipt_sha256, "stop_receipt_sha256"),
        (cursor.finality_receipt_sha256, "finality_receipt_sha256"),
        (cursor.finality_authority_sha256, "finality_authority_sha256"),
        (cursor.finality_exact_prefix_sha256, "finality_exact_prefix_sha256"),
        (cursor.finality_prefix_proof_sha256, "finality_prefix_proof_sha256"),
    ):
        _require_sha256(value, name)
    if cursor.stop_receipt_sha256 != cursor.stop_receipt.receipt_sha256:
        raise WebSocketRouteFinalityErrorV2(
            "finalized route cursor differs from its stop receipt"
        )
    _require_positive_int(cursor.finality_tail_ingest_seq, "finality_tail_ingest_seq")
    if cursor.stop_receipt.last_ingest_seq > cursor.finality_tail_ingest_seq:
        raise WebSocketRouteFinalityErrorV2(
            "finality tail precedes the WebSocket route stop cursor"
        )
    if cursor.local_route_cursor_finalized is not True:
        raise WebSocketRouteFinalityErrorV2(
            "finalized route cursor must affirm only local cursor finality"
        )
    _require_nonclaims(
        cursor.retained_frame_parser_health_claimed,
        cursor.upstream_message_completeness_claimed,
        cursor.m2_certified,
    )
    if cursor.schema_version != _FINALIZED_CURSOR_SCHEMA:
        raise WebSocketRouteFinalityErrorV2(
            "unsupported finalized WebSocket route cursor schema"
        )
    if finality_receipt is not None:
        _validate_finality_receipt(finality_receipt)
        expected = (
            finality_receipt.sha256,
            finality_receipt.authority_sha256,
            finality_receipt.exact_prefix_sha256,
            finality_receipt.prefix_proof_sha256,
            finality_receipt.fence_ingest_seq,
        )
        observed = (
            cursor.finality_receipt_sha256,
            cursor.finality_authority_sha256,
            cursor.finality_exact_prefix_sha256,
            cursor.finality_prefix_proof_sha256,
            cursor.finality_tail_ingest_seq,
        )
        if observed != expected:
            raise WebSocketRouteFinalityErrorV2(
                "finalized route cursor differs from the supplied finality receipt"
            )
        if cursor.stop_receipt.stop_observed_monotonic_ns > (
            finality_receipt.fence_monotonic_ns
        ):
            raise WebSocketRouteFinalityErrorV2(
                "finality fence precedes the WebSocket owner-stop observation"
            )
    if verify_digest:
        _require_sha256(cursor.cursor_sha256, "cursor_sha256")
        if not hmac.compare_digest(cursor.cursor_sha256, _finalized_cursor_sha256(cursor)):
            raise WebSocketRouteFinalityErrorV2(
                "finalized WebSocket route cursor digest differs"
            )


def _validate_finalized_cursor_material_v8(
    cursor: FinalizedWebSocketRouteCursorV8,
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2 | None,
    verify_digest: bool,
) -> None:
    if type(cursor.stop_receipt) is not WebSocketRouteStopReceiptV8:
        raise TypeError("V8 finalized route cursor requires an exact V8 stop receipt")
    validate_websocket_route_stop_receipt_v8(cursor.stop_receipt)
    for value, name in (
        (cursor.stop_receipt_sha256, "stop_receipt_sha256"),
        (cursor.finality_receipt_sha256, "finality_receipt_sha256"),
        (cursor.finality_authority_sha256, "finality_authority_sha256"),
        (cursor.finality_exact_prefix_sha256, "finality_exact_prefix_sha256"),
        (cursor.finality_prefix_proof_sha256, "finality_prefix_proof_sha256"),
    ):
        _require_sha256(value, name)
    if cursor.stop_receipt_sha256 != cursor.stop_receipt.receipt_sha256:
        raise WebSocketRouteFinalityErrorV2(
            "V8 finalized route cursor differs from its stop receipt"
        )
    _require_positive_int(cursor.finality_tail_ingest_seq, "finality_tail_ingest_seq")
    if cursor.stop_receipt.last_ingest_seq > cursor.finality_tail_ingest_seq:
        raise WebSocketRouteFinalityErrorV2(
            "V8 finality tail precedes the WebSocket route stop cursor"
        )
    if cursor.local_route_cursor_finalized is not True:
        raise WebSocketRouteFinalityErrorV2(
            "V8 finalized route cursor must affirm only local cursor finality"
        )
    _require_nonclaims(
        cursor.retained_frame_parser_health_claimed,
        cursor.upstream_message_completeness_claimed,
        cursor.m2_certified,
        cursor.depth_bridge_complete_claimed,
    )
    if cursor.schema_version != _FINALIZED_CURSOR_SCHEMA_V8:
        raise WebSocketRouteFinalityErrorV2(
            "unsupported V8 finalized WebSocket route cursor schema"
        )
    if finality_receipt is not None:
        _validate_finality_receipt(finality_receipt)
        expected = (
            finality_receipt.sha256,
            finality_receipt.authority_sha256,
            finality_receipt.exact_prefix_sha256,
            finality_receipt.prefix_proof_sha256,
            finality_receipt.fence_ingest_seq,
        )
        observed = (
            cursor.finality_receipt_sha256,
            cursor.finality_authority_sha256,
            cursor.finality_exact_prefix_sha256,
            cursor.finality_prefix_proof_sha256,
            cursor.finality_tail_ingest_seq,
        )
        if observed != expected:
            raise WebSocketRouteFinalityErrorV2(
                "V8 finalized route cursor differs from the supplied finality receipt"
            )
        if cursor.stop_receipt.stop_observed_monotonic_ns > (
            finality_receipt.fence_monotonic_ns
        ):
            raise WebSocketRouteFinalityErrorV2(
                "V8 finality fence precedes the WebSocket owner-stop observation"
            )
    if verify_digest:
        _require_sha256(cursor.cursor_sha256, "cursor_sha256")
        if not hmac.compare_digest(
            cursor.cursor_sha256,
            _finalized_cursor_sha256_v8(cursor),
        ):
            raise WebSocketRouteFinalityErrorV2(
                "V8 finalized WebSocket route cursor digest differs"
            )


def _validate_closure_entry_material(entry: WebSocketRouteCursorClosureEntryV2) -> None:
    for value, name in (
        (entry.session_id, "session_id"),
        (entry.process_boot_id, "process_boot_id"),
        (entry.plan_id, "plan_id"),
        (entry.connection_id, "connection_id"),
    ):
        _require_identity(value, name)
    for value, name in (
        (entry.plan_bundle_sha256, "plan_bundle_sha256"),
        (entry.stream_census_sha256, "stream_census_sha256"),
        (entry.stop_receipt_sha256, "stop_receipt_sha256"),
        (entry.finalized_route_cursor_sha256, "finalized_route_cursor_sha256"),
        (entry.finality_receipt_sha256, "finality_receipt_sha256"),
        (entry.finality_authority_sha256, "finality_authority_sha256"),
        (entry.finality_exact_prefix_sha256, "finality_exact_prefix_sha256"),
        (entry.finality_prefix_proof_sha256, "finality_prefix_proof_sha256"),
    ):
        _require_sha256(value, name)
    if entry.venue != "usdm_futures" or entry.route_id not in _EXPECTED_ROUTES:
        raise WebSocketRouteFinalityErrorV2("persisted route cursor scope is unsupported")
    for value, name in (
        (entry.stream_count, "stream_count"),
        (entry.generation, "generation"),
        (entry.last_frame_seq, "last_frame_seq"),
        (entry.last_ingest_seq, "last_ingest_seq"),
        (entry.finality_tail_ingest_seq, "finality_tail_ingest_seq"),
    ):
        _require_positive_int(value, name)
    for value, name in (
        (entry.last_receipt_wall_ms, "last_receipt_wall_ms"),
        (entry.last_receipt_monotonic_ns, "last_receipt_monotonic_ns"),
        (entry.stop_observed_wall_ms, "stop_observed_wall_ms"),
        (entry.stop_observed_monotonic_ns, "stop_observed_monotonic_ns"),
    ):
        _require_nonnegative_int(value, name)
    if entry.stop_observed_monotonic_ns < entry.last_receipt_monotonic_ns:
        raise WebSocketRouteFinalityErrorV2(
            "persisted owner-stop observation precedes its retained cursor"
        )
    if entry.last_ingest_seq > entry.finality_tail_ingest_seq:
        raise WebSocketRouteFinalityErrorV2(
            "persisted finality tail precedes its route stop cursor"
        )
    if (
        entry.close_reason != "OWNER_STOP"
        or entry.pending_source_gap is not False
        or entry.local_route_cursor_finalized is not True
    ):
        raise WebSocketRouteFinalityErrorV2(
            "persisted route cursor is not a local gap-free OWNER_STOP finality join"
        )
    _require_nonclaims(
        entry.retained_frame_parser_health_claimed,
        entry.upstream_message_completeness_claimed,
        entry.m2_certified,
    )
    if entry.schema_version != _CLOSURE_ENTRY_SCHEMA:
        raise WebSocketRouteFinalityErrorV2(
            "unsupported persisted WebSocket route cursor schema"
        )
    if not hmac.compare_digest(
        entry.stop_receipt_sha256,
        _closure_entry_stop_receipt_sha256(entry),
    ):
        raise WebSocketRouteFinalityErrorV2(
            "persisted WebSocket stop receipt digest differs"
        )
    if not hmac.compare_digest(
        entry.finalized_route_cursor_sha256,
        _closure_entry_finalized_cursor_sha256(entry),
    ):
        raise WebSocketRouteFinalityErrorV2(
            "persisted finalized WebSocket route cursor digest differs"
        )


def _validate_closure_entry_material_v8(
    entry: WebSocketRouteCursorClosureEntryV8,
) -> None:
    for value, name in (
        (entry.session_id, "session_id"),
        (entry.process_boot_id, "process_boot_id"),
        (entry.plan_id, "plan_id"),
        (entry.connection_id, "connection_id"),
    ):
        _require_identity(value, name)
    for value, name in (
        (entry.plan_bundle_sha256, "plan_bundle_sha256"),
        (entry.stream_census_sha256, "stream_census_sha256"),
        (entry.stop_receipt_sha256, "stop_receipt_sha256"),
        (entry.finalized_route_cursor_sha256, "finalized_route_cursor_sha256"),
        (entry.finality_receipt_sha256, "finality_receipt_sha256"),
        (entry.finality_authority_sha256, "finality_authority_sha256"),
        (entry.finality_exact_prefix_sha256, "finality_exact_prefix_sha256"),
        (entry.finality_prefix_proof_sha256, "finality_prefix_proof_sha256"),
    ):
        _require_sha256(value, name)
    if entry.venue != "usdm_futures" or entry.route_id not in _EXPECTED_ROUTES:
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 route cursor scope is unsupported"
        )
    for value, name in (
        (entry.stream_count, "stream_count"),
        (entry.generation, "generation"),
        (entry.last_frame_seq, "last_frame_seq"),
        (entry.last_ingest_seq, "last_ingest_seq"),
        (entry.finality_tail_ingest_seq, "finality_tail_ingest_seq"),
    ):
        _require_positive_int(value, name)
    for value, name in (
        (entry.last_receipt_wall_ms, "last_receipt_wall_ms"),
        (entry.last_receipt_monotonic_ns, "last_receipt_monotonic_ns"),
        (entry.stop_observed_wall_ms, "stop_observed_wall_ms"),
        (entry.stop_observed_monotonic_ns, "stop_observed_monotonic_ns"),
    ):
        _require_nonnegative_int(value, name)
    if entry.stop_observed_monotonic_ns < entry.last_receipt_monotonic_ns:
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 owner-stop observation precedes its retained cursor"
        )
    if entry.last_ingest_seq > entry.finality_tail_ingest_seq:
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 finality tail precedes its route stop cursor"
        )
    if (
        entry.close_reason != "OWNER_STOP"
        or entry.pending_source_gap is not False
        or entry.local_route_cursor_finalized is not True
    ):
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 route cursor is not a local gap-free OWNER_STOP join"
        )
    _require_nonclaims(
        entry.retained_frame_parser_health_claimed,
        entry.upstream_message_completeness_claimed,
        entry.m2_certified,
        entry.depth_bridge_complete_claimed,
    )
    if entry.schema_version != _CLOSURE_ENTRY_SCHEMA_V8:
        raise WebSocketRouteFinalityErrorV2(
            "unsupported persisted V8 WebSocket route cursor schema"
        )
    if not hmac.compare_digest(
        entry.stop_receipt_sha256,
        _closure_entry_stop_receipt_sha256_v8(entry),
    ):
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 WebSocket stop receipt digest differs"
        )
    if not hmac.compare_digest(
        entry.finalized_route_cursor_sha256,
        _closure_entry_finalized_cursor_sha256_v8(entry),
    ):
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 finalized WebSocket route cursor digest differs"
        )


def _validate_finality_receipt(receipt: CaptureFinalityFenceReceiptV2) -> None:
    if type(receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError("finality receipt must be an exact CaptureFinalityFenceReceiptV2")
    receipt.__post_init__()


def _validate_selected_plan(
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
    plan: ProvisionalPromotingCapturePlanV2,
) -> None:
    validate_provisional_promoting_capture_plans_v2(promoting_plans)
    if type(plan) is not ProvisionalPromotingCapturePlanV2:
        raise TypeError("selected WebSocket plan must be exact")
    if sum(candidate == plan for candidate in promoting_plans) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "selected WebSocket plan is not unique in the promoting bundle"
        )


def _validate_selected_plan_v8(
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    plan: ProvisionalPromotingCapturePlanV2,
) -> None:
    _validate_promoting_plans_tuple_v8(promoting_plans)
    if type(plan) is not ProvisionalPromotingCapturePlanV2:
        raise TypeError("selected V8 WebSocket plan must be exact")
    if sum(candidate is plan for candidate in promoting_plans) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "selected WebSocket plan is not the exact V8 authority object"
        )


def _validate_promoting_plans_tuple_v8(
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
) -> None:
    if type(promoting_plans) is not tuple:
        raise TypeError("V8 promoting plans must be an exact tuple")
    validate_provisional_promoting_capture_plans_v8(promoting_plans)


def _selected_route_plan(
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
    route_id: str,
) -> ProvisionalPromotingCapturePlanV2:
    matches = tuple(
        plan
        for plan in promoting_plans
        if type(plan) is ProvisionalPromotingCapturePlanV2
        and plan.route_id == route_id
    )
    if len(matches) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "promoting bundle lacks one exact WebSocket route plan"
        )
    return matches[0]


def _selected_route_plan_v8(
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    route_id: str,
) -> ProvisionalPromotingCapturePlanV2:
    _validate_promoting_plans_tuple_v8(promoting_plans)
    matches = tuple(
        plan
        for plan in promoting_plans
        if type(plan) is ProvisionalPromotingCapturePlanV2
        and plan.route_id == route_id
    )
    if len(matches) != 1:
        raise WebSocketRouteFinalityErrorV2(
            "V8 authority lacks one exact WebSocket route plan"
        )
    return matches[0]


def _validate_closure_entry_plan_scope_v8(
    entry: WebSocketRouteCursorClosureEntryV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    plan: ProvisionalPromotingCapturePlanV2,
) -> None:
    _validate_selected_plan_v8(promoting_plans, plan)
    expected = (
        provisional_promoting_plan_sha256_v8(promoting_plans),
        plan.name,
        plan.venue.value,
        plan.route_id,
        provisional_promoting_stream_census_sha256_v2(plan),
        len(plan.streams),
    )
    observed = (
        entry.plan_bundle_sha256,
        entry.plan_id,
        entry.venue,
        entry.route_id,
        entry.stream_census_sha256,
        entry.stream_count,
    )
    if observed != expected:
        raise WebSocketRouteFinalityErrorV2(
            "persisted V8 WebSocket cursor differs from four-plan authority"
        )


def _stop_receipt_sha256(receipt: WebSocketRouteStopReceiptV2) -> str:
    document = {
        model_field.name: getattr(receipt, model_field.name)
        for model_field in fields(receipt)
        if model_field.name not in {"receipt_sha256", "_factory_seal"}
    }
    return hashlib.sha256(_STOP_DOMAIN + canonical_json_line(document)).hexdigest()


def _stop_receipt_sha256_v8(receipt: WebSocketRouteStopReceiptV8) -> str:
    document = {
        model_field.name: getattr(receipt, model_field.name)
        for model_field in fields(receipt)
        if model_field.name not in {"receipt_sha256", "_factory_seal"}
    }
    return hashlib.sha256(
        _STOP_DOMAIN_V8 + canonical_json_line(document)
    ).hexdigest()


def _finalized_cursor_sha256(cursor: FinalizedWebSocketRouteCursorV2) -> str:
    document = {
        model_field.name: getattr(cursor, model_field.name)
        for model_field in fields(cursor)
        if model_field.name
        not in {"stop_receipt", "cursor_sha256", "_factory_seal"}
    }
    return hashlib.sha256(
        _FINALIZED_CURSOR_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _finalized_cursor_sha256_v8(cursor: FinalizedWebSocketRouteCursorV8) -> str:
    document = {
        model_field.name: getattr(cursor, model_field.name)
        for model_field in fields(cursor)
        if model_field.name
        not in {"stop_receipt", "cursor_sha256", "_factory_seal"}
    }
    return hashlib.sha256(
        _FINALIZED_CURSOR_DOMAIN_V8 + canonical_json_line(document)
    ).hexdigest()


def _closure_entry_stop_receipt_sha256(
    entry: WebSocketRouteCursorClosureEntryV2,
) -> str:
    document = {
        "session_id": entry.session_id,
        "process_boot_id": entry.process_boot_id,
        "plan_bundle_sha256": entry.plan_bundle_sha256,
        "plan_id": entry.plan_id,
        "venue": entry.venue,
        "route_id": entry.route_id,
        "stream_census_sha256": entry.stream_census_sha256,
        "stream_count": entry.stream_count,
        "connection_id": entry.connection_id,
        "generation": entry.generation,
        "last_frame_seq": entry.last_frame_seq,
        "last_ingest_seq": entry.last_ingest_seq,
        "last_receipt_wall_ms": entry.last_receipt_wall_ms,
        "last_receipt_monotonic_ns": entry.last_receipt_monotonic_ns,
        "stop_observed_wall_ms": entry.stop_observed_wall_ms,
        "stop_observed_monotonic_ns": entry.stop_observed_monotonic_ns,
        "close_reason": entry.close_reason,
        "pending_source_gap": entry.pending_source_gap,
        "retained_frame_parser_health_claimed": (
            entry.retained_frame_parser_health_claimed
        ),
        "upstream_message_completeness_claimed": (
            entry.upstream_message_completeness_claimed
        ),
        "m2_certified": entry.m2_certified,
        "schema_version": _STOP_SCHEMA,
    }
    return hashlib.sha256(_STOP_DOMAIN + canonical_json_line(document)).hexdigest()


def _closure_entry_finalized_cursor_sha256(
    entry: WebSocketRouteCursorClosureEntryV2,
) -> str:
    document = {
        "stop_receipt_sha256": entry.stop_receipt_sha256,
        "finality_receipt_sha256": entry.finality_receipt_sha256,
        "finality_authority_sha256": entry.finality_authority_sha256,
        "finality_exact_prefix_sha256": entry.finality_exact_prefix_sha256,
        "finality_prefix_proof_sha256": entry.finality_prefix_proof_sha256,
        "finality_tail_ingest_seq": entry.finality_tail_ingest_seq,
        "local_route_cursor_finalized": entry.local_route_cursor_finalized,
        "retained_frame_parser_health_claimed": (
            entry.retained_frame_parser_health_claimed
        ),
        "upstream_message_completeness_claimed": (
            entry.upstream_message_completeness_claimed
        ),
        "m2_certified": entry.m2_certified,
        "schema_version": _FINALIZED_CURSOR_SCHEMA,
    }
    return hashlib.sha256(
        _FINALIZED_CURSOR_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _closure_entry_stop_receipt_sha256_v8(
    entry: WebSocketRouteCursorClosureEntryV8,
) -> str:
    document = {
        "session_id": entry.session_id,
        "process_boot_id": entry.process_boot_id,
        "plan_bundle_sha256": entry.plan_bundle_sha256,
        "plan_id": entry.plan_id,
        "venue": entry.venue,
        "route_id": entry.route_id,
        "stream_census_sha256": entry.stream_census_sha256,
        "stream_count": entry.stream_count,
        "connection_id": entry.connection_id,
        "generation": entry.generation,
        "last_frame_seq": entry.last_frame_seq,
        "last_ingest_seq": entry.last_ingest_seq,
        "last_receipt_wall_ms": entry.last_receipt_wall_ms,
        "last_receipt_monotonic_ns": entry.last_receipt_monotonic_ns,
        "stop_observed_wall_ms": entry.stop_observed_wall_ms,
        "stop_observed_monotonic_ns": entry.stop_observed_monotonic_ns,
        "close_reason": entry.close_reason,
        "pending_source_gap": entry.pending_source_gap,
        "retained_frame_parser_health_claimed": (
            entry.retained_frame_parser_health_claimed
        ),
        "upstream_message_completeness_claimed": (
            entry.upstream_message_completeness_claimed
        ),
        "m2_certified": entry.m2_certified,
        "depth_bridge_complete_claimed": entry.depth_bridge_complete_claimed,
        "schema_version": _STOP_SCHEMA_V8,
    }
    return hashlib.sha256(
        _STOP_DOMAIN_V8 + canonical_json_line(document)
    ).hexdigest()


def _closure_entry_finalized_cursor_sha256_v8(
    entry: WebSocketRouteCursorClosureEntryV8,
) -> str:
    document = {
        "stop_receipt_sha256": entry.stop_receipt_sha256,
        "finality_receipt_sha256": entry.finality_receipt_sha256,
        "finality_authority_sha256": entry.finality_authority_sha256,
        "finality_exact_prefix_sha256": entry.finality_exact_prefix_sha256,
        "finality_prefix_proof_sha256": entry.finality_prefix_proof_sha256,
        "finality_tail_ingest_seq": entry.finality_tail_ingest_seq,
        "local_route_cursor_finalized": entry.local_route_cursor_finalized,
        "retained_frame_parser_health_claimed": (
            entry.retained_frame_parser_health_claimed
        ),
        "upstream_message_completeness_claimed": (
            entry.upstream_message_completeness_claimed
        ),
        "m2_certified": entry.m2_certified,
        "depth_bridge_complete_claimed": entry.depth_bridge_complete_claimed,
        "schema_version": _FINALIZED_CURSOR_SCHEMA_V8,
    }
    return hashlib.sha256(
        _FINALIZED_CURSOR_DOMAIN_V8 + canonical_json_line(document)
    ).hexdigest()


def _require_nonclaims(*values: object) -> None:
    if any(type(value) is not bool or value is not False for value in values):
        raise WebSocketRouteFinalityErrorV2(
            "WebSocket cursor evidence cannot claim parser health, upstream completeness, or M2"
        )


def _require_sha256(value: object, name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise WebSocketRouteFinalityErrorV2(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _require_identity(value: object, name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise WebSocketRouteFinalityErrorV2(
            f"{name} must be a bounded normalized identity"
        )


def _require_positive_int(value: object, name: str) -> None:
    if type(value) is not int or value < 1:
        raise WebSocketRouteFinalityErrorV2(f"{name} must be a positive integer")


def _require_nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int or value < 0:
        raise WebSocketRouteFinalityErrorV2(f"{name} must be a nonnegative integer")
