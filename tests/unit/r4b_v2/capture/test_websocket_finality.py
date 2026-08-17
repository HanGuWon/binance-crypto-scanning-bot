from __future__ import annotations

from dataclasses import replace
from typing import cast

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.capture.authority import StorageRootBindingV2
from signalbot.r4b_v2.capture.pipeline import CaptureFinalityFenceReceiptV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingPlanV8,
    build_provisional_promoting_capture_plans_v2,
    build_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.wal import WalDurabilityBindingV2
from signalbot.r4b_v2.capture.websocket_finality import (
    FinalizedWebSocketRouteCursorPairV8,
    WebSocketRouteCursorClosurePairV8,
    WebSocketRouteFinalityErrorV2,
    WebSocketRouteStopReceiptV2,
    WebSocketRouteStopReceiptV8,
    _issue_websocket_route_stop_receipt_v2,
    _issue_websocket_route_stop_receipt_v8,
    finalize_websocket_route_cursor_pair_v2,
    finalize_websocket_route_cursor_pair_v8,
    finalize_websocket_route_cursor_v8,
    validate_finalized_websocket_route_cursor_pair_v8,
    validate_finalized_websocket_route_cursor_v8,
    validate_websocket_route_cursor_closure_pair_v8,
    validate_websocket_route_stop_receipt_v8,
    websocket_route_cursor_closure_pair_sha256_v8,
    websocket_route_cursor_closure_pair_v2,
    websocket_route_cursor_closure_pair_v8,
)

_AUTHORITY_SHA256 = "a" * 64


def _plans_v8(symbol: str = "BTCUSDT") -> tuple[ProvisionalPromotingPlanV8, ...]:
    return build_provisional_promoting_capture_plans_v8((symbol,))


def _websocket_plan_v8(
    plans: tuple[ProvisionalPromotingPlanV8, ...],
    route_id: str,
) -> ProvisionalPromotingCapturePlanV2:
    return next(
        plan
        for plan in plans
        if type(plan) is ProvisionalPromotingCapturePlanV2
        and plan.route_id == route_id
    )


def _stop_v8(
    plans: tuple[ProvisionalPromotingPlanV8, ...],
    route_id: str,
    *,
    session_id: str = "session-1",
    process_boot_id: str = "boot-1",
    last_ingest_seq: int = 10,
    stop_observed_monotonic_ns: int = 2_000,
) -> WebSocketRouteStopReceiptV8:
    plan = _websocket_plan_v8(plans, route_id)
    return _issue_websocket_route_stop_receipt_v8(
        plans,
        plan,
        session_id=session_id,
        process_boot_id=process_boot_id,
        connection_id=f"connection-{route_id}",
        generation=1,
        last_frame_seq=5,
        last_ingest_seq=last_ingest_seq,
        last_receipt_wall_ms=1_700_000_000_000,
        last_receipt_monotonic_ns=1_000,
        stop_observed=ReceiptTimestamp(
            received_at_ms=1_700_000_000_010,
            received_monotonic_ns=stop_observed_monotonic_ns,
        ),
    )


def _finality(
    *,
    fence_ingest_seq: int = 20,
    authority_sha256: str = _AUTHORITY_SHA256,
    exact_prefix_sha256: str = "c" * 64,
) -> CaptureFinalityFenceReceiptV2:
    wal_root = StorageRootBindingV2(
        storage_kind="WAL",
        root_role="PROVISIONAL_SINGLE",
        failure_domain_id="unit-test-wal",
        authority_sha256=authority_sha256,
        contract_sha256="b" * 64,
    )
    return CaptureFinalityFenceReceiptV2(
        authority_sha256=authority_sha256,
        attempt_id="unit-test-attempt",
        qualification_id="unit-test-policy",
        requested_ingest_seq=fence_ingest_seq,
        fence_ingest_seq=fence_ingest_seq,
        fence_monotonic_ns=3_000,
        writer_observed_monotonic_ns=3_000,
        wal_durable_ack_seq=fence_ingest_seq,
        finalized_block_tail_ingest_seq=fence_ingest_seq,
        durable_record_count=fence_ingest_seq,
        exact_prefix_sha256=exact_prefix_sha256,
        wal_durability_binding=WalDurabilityBindingV2(
            mode="SINGLE_ROOT",
            root_bindings=(wal_root,),
            qualification_selection_receipt_sha256=None,
            physical_failure_domain_independence_verified=False,
        ),
        grouped_block_root_binding=StorageRootBindingV2(
            storage_kind="GROUPED_BLOCK",
            root_role="PROVISIONAL_SINGLE",
            failure_domain_id="unit-test-block",
            authority_sha256=authority_sha256,
            contract_sha256="d" * 64,
        ),
        block_signing_authority_sha256="e" * 64,
        final_block_sequence=1,
        final_block_hash="f" * 64,
        final_block_manifest_sha256="1" * 64,
        final_block_container_sha256="2" * 64,
        target_last_receipt_wall_ms=1_700_000_000_000,
        target_last_receipt_monotonic_ns=1_000,
        stream_group_id="unit-test-stream-group",
        segment_id="unit-test-segment",
    )


def _finalized_pair_v8(
    *,
    plans: tuple[ProvisionalPromotingPlanV8, ...] | None = None,
    finality: CaptureFinalityFenceReceiptV2 | None = None,
    session_id: str = "session-1",
) -> tuple[
    tuple[ProvisionalPromotingPlanV8, ...],
    CaptureFinalityFenceReceiptV2,
    FinalizedWebSocketRouteCursorPairV8,
]:
    actual_plans = _plans_v8() if plans is None else plans
    actual_finality = _finality() if finality is None else finality
    market = _stop_v8(actual_plans, "usdm_market", session_id=session_id)
    public = _stop_v8(actual_plans, "usdm_public", session_id=session_id)
    pair = finalize_websocket_route_cursor_pair_v8(
        (public, market),
        finality_receipt=actual_finality,
        promoting_plans=actual_plans,
    )
    return actual_plans, actual_finality, pair


def test_v8_finality_projects_canonical_pair_without_upstream_claims() -> None:
    plans, finality, pair = _finalized_pair_v8()

    assert tuple(cursor.stop_receipt.route_id for cursor in pair) == (
        "usdm_market",
        "usdm_public",
    )
    assert all(
        cursor.schema_version == "r4b_v2_finalized_websocket_route_cursor_v8"
        and cursor.local_route_cursor_finalized is True
        and cursor.retained_frame_parser_health_claimed is False
        and cursor.upstream_message_completeness_claimed is False
        and cursor.m2_certified is False
        and cursor.depth_bridge_complete_claimed is False
        for cursor in pair
    )
    closure = websocket_route_cursor_closure_pair_v8(
        pair,
        finality_receipt=finality,
        promoting_plans=plans,
    )
    assert tuple(entry.route_id for entry in closure) == (
        "usdm_market",
        "usdm_public",
    )
    assert all(
        entry.schema_version
        == "r4b_v2_websocket_route_cursor_closure_entry_v8"
        and entry.depth_bridge_complete_claimed is False
        for entry in closure
    )
    first_hash = websocket_route_cursor_closure_pair_sha256_v8(
        closure,
        finality_receipt=finality,
        promoting_plans=plans,
    )
    assert first_hash == websocket_route_cursor_closure_pair_sha256_v8(closure)
    assert len(first_hash) == 64


def test_v8_single_cursor_requires_exact_four_plan_authority_object() -> None:
    plans = _plans_v8()
    finality = _finality()
    market_plan = _websocket_plan_v8(plans, "usdm_market")
    market = _stop_v8(plans, "usdm_market")
    cursor = finalize_websocket_route_cursor_v8(
        market,
        finality,
        promoting_plans=plans,
        plan=market_plan,
    )

    cloned_plans: tuple[ProvisionalPromotingPlanV8, ...] = (
        replace(market_plan),
        *plans[1:],
    )
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="exact V8 authority"):
        validate_finalized_websocket_route_cursor_v8(
            cursor,
            finality_receipt=finality,
            promoting_plans=cloned_plans,
            plan=market_plan,
        )
    with pytest.raises(TypeError, match="exact tuple"):
        validate_finalized_websocket_route_cursor_v8(
            cursor,
            finality_receipt=finality,
            promoting_plans=cast(
                tuple[ProvisionalPromotingPlanV8, ...],
                list(plans),
            ),
            plan=market_plan,
        )


def test_v8_factory_seals_stop_receipts_and_finalized_cursors() -> None:
    plans, finality, pair = _finalized_pair_v8()
    market_plan = _websocket_plan_v8(plans, "usdm_market")

    with pytest.raises(TypeError, match="factory-sealed"):
        replace(pair[0])
    with pytest.raises(TypeError, match="factory-sealed"):
        replace(pair[0].stop_receipt)

    validate_finalized_websocket_route_cursor_v8(
        pair[0],
        finality_receipt=finality,
        promoting_plans=plans,
        plan=market_plan,
    )


def test_v8_pair_rejects_session_route_and_plan_splices() -> None:
    plans = _plans_v8()
    finality = _finality()
    market = _stop_v8(plans, "usdm_market", session_id="session-a")
    public_other_session = _stop_v8(
        plans,
        "usdm_public",
        session_id="session-b",
    )

    with pytest.raises(WebSocketRouteFinalityErrorV2, match="cross session"):
        finalize_websocket_route_cursor_pair_v8(
            (market, public_other_session),
            finality_receipt=finality,
            promoting_plans=plans,
        )
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="duplicate"):
        finalize_websocket_route_cursor_pair_v8(
            (market, _stop_v8(plans, "usdm_market")),
            finality_receipt=finality,
            promoting_plans=plans,
        )

    other_plans = _plans_v8("ETHUSDT")
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="four-plan authority"):
        finalize_websocket_route_cursor_pair_v8(
            (market, _stop_v8(plans, "usdm_public")),
            finality_receipt=finality,
            promoting_plans=other_plans,
        )


def test_v8_pair_rejects_finality_and_order_splices() -> None:
    plans, finality, pair = _finalized_pair_v8()
    other_finality = _finality(exact_prefix_sha256="9" * 64)

    with pytest.raises(WebSocketRouteFinalityErrorV2, match="supplied finality"):
        validate_finalized_websocket_route_cursor_pair_v8(
            pair,
            finality_receipt=other_finality,
            promoting_plans=plans,
        )
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="canonical route order"):
        validate_finalized_websocket_route_cursor_pair_v8(
            (pair[1], pair[0]),
            finality_receipt=finality,
            promoting_plans=plans,
        )


def test_v8_cursor_and_stop_digest_tampering_fail_closed() -> None:
    plans, finality, pair = _finalized_pair_v8()
    market_plan = _websocket_plan_v8(plans, "usdm_market")
    object.__setattr__(pair[0], "cursor_sha256", "0" * 64)
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="cursor digest differs"):
        validate_finalized_websocket_route_cursor_v8(
            pair[0],
            finality_receipt=finality,
            promoting_plans=plans,
            plan=market_plan,
        )

    stop = _stop_v8(plans, "usdm_market")
    object.__setattr__(stop, "receipt_sha256", "0" * 64)
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="receipt digest differs"):
        validate_websocket_route_stop_receipt_v8(
            stop,
            promoting_plans=plans,
            plan=market_plan,
        )


def test_v8_closure_rejects_session_finality_plan_and_nonclaim_splices() -> None:
    plans, finality, pair = _finalized_pair_v8()
    closure = websocket_route_cursor_closure_pair_v8(
        pair,
        finality_receipt=finality,
        promoting_plans=plans,
    )
    _, other_finality, other_pair = _finalized_pair_v8(
        finality=_finality(exact_prefix_sha256="8" * 64),
        session_id="session-2",
    )
    other_closure = websocket_route_cursor_closure_pair_v8(
        other_pair,
        finality_receipt=other_finality,
        promoting_plans=plans,
    )

    with pytest.raises(WebSocketRouteFinalityErrorV2, match="cross session"):
        validate_websocket_route_cursor_closure_pair_v8(
            (closure[0], other_closure[1])
        )
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="finality receipt"):
        validate_websocket_route_cursor_closure_pair_v8(
            closure,
            finality_receipt=other_finality,
            promoting_plans=plans,
        )
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="four-plan authority"):
        validate_websocket_route_cursor_closure_pair_v8(
            closure,
            promoting_plans=_plans_v8("ETHUSDT"),
        )
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="cannot claim"):
        replace(closure[0], depth_bridge_complete_claimed=True)  # type: ignore[arg-type]


def test_v8_validators_reject_v2_cursor_and_closure_types() -> None:
    plans_v2: tuple[ProvisionalPromotingPlanV2, ...] = (
        build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
    )
    finality = _finality()
    websocket_plans_v2 = tuple(
        plan
        for plan in plans_v2
        if type(plan) is ProvisionalPromotingCapturePlanV2
    )
    receipts_v2 = tuple(
        _issue_websocket_route_stop_receipt_v2(
            plans_v2,
            plan,
            session_id="session-v2",
            process_boot_id="boot-v2",
            connection_id=f"connection-{plan.route_id}",
            generation=1,
            last_frame_seq=5,
            last_ingest_seq=10,
            last_receipt_wall_ms=1_700_000_000_000,
            last_receipt_monotonic_ns=1_000,
            stop_observed=ReceiptTimestamp(
                received_at_ms=1_700_000_000_010,
                received_monotonic_ns=2_000,
            ),
        )
        for plan in websocket_plans_v2
    )
    pair_v2 = finalize_websocket_route_cursor_pair_v2(
        cast(tuple[WebSocketRouteStopReceiptV2, WebSocketRouteStopReceiptV2], receipts_v2),
        finality_receipt=finality,
        promoting_plans=plans_v2,
    )
    closure_v2 = websocket_route_cursor_closure_pair_v2(
        pair_v2,
        finality_receipt=finality,
        promoting_plans=plans_v2,
    )
    plans_v8 = _plans_v8()

    with pytest.raises(TypeError, match="foreign type"):
        validate_finalized_websocket_route_cursor_pair_v8(
            cast(FinalizedWebSocketRouteCursorPairV8, pair_v2),
            finality_receipt=finality,
            promoting_plans=plans_v8,
        )
    with pytest.raises(TypeError, match="foreign type"):
        validate_websocket_route_cursor_closure_pair_v8(
            cast(WebSocketRouteCursorClosurePairV8, closure_v2)
        )


def test_v8_finalizer_rejects_early_finality_tail_and_clock() -> None:
    plans = _plans_v8()
    market_plan = _websocket_plan_v8(plans, "usdm_market")
    stop = _stop_v8(plans, "usdm_market", last_ingest_seq=10)

    with pytest.raises(WebSocketRouteFinalityErrorV2, match="tail precedes"):
        finalize_websocket_route_cursor_v8(
            stop,
            _finality(fence_ingest_seq=9),
            promoting_plans=plans,
            plan=market_plan,
        )

    late_stop = _stop_v8(
        plans,
        "usdm_market",
        stop_observed_monotonic_ns=4_000,
    )
    with pytest.raises(WebSocketRouteFinalityErrorV2, match="fence precedes"):
        finalize_websocket_route_cursor_v8(
            late_stop,
            _finality(),
            promoting_plans=plans,
            plan=market_plan,
        )
