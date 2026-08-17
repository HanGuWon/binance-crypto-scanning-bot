from __future__ import annotations

import base64
import hashlib
import inspect
import json
from dataclasses import replace
from decimal import ROUND_DOWN, Decimal, Underflow, getcontext, localcontext
from pathlib import Path

import pytest

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.fees import (
    FEE_POLL_CADENCE_MS_V2,
    SPOT_OFFICIAL_FEE_URL_V2,
    SPOT_PUBLIC_TAKER_RATE_V2,
    USDM_OFFICIAL_FEE_URL_V2,
    USDM_PUBLIC_TAKER_RATE_V2,
    FeeCaptureArtifactKindV2,
    FeeCaptureLedgerCheckpointV2,
    FeeCaptureRegistryV2,
    FeeCaptureRoleV2,
    FeeContractErrorV2,
    FeeHttpCaptureEnvelopeV2,
    FeeMultiplierV2,
    FeeProtocolScopeV2,
    FeeRegistryDispositionV2,
    FeeResolutionStatusV2,
    PublicFeeManifestV2,
    audit_fee_poll_cadence_v2,
    build_fee_page_capture_v2,
    build_fee_timeline_checkpoint_v2,
    calculate_filled_both_leg_fee_v2,
    canonical_fee_capture_archive_v2,
    canonical_fee_page_capture_v2,
    canonical_fee_poll_audit_v2,
    canonical_fee_version_resolution_v2,
    canonical_filled_both_leg_fee_v2,
    canonical_public_fee_manifest_v2,
    fee_capture_ledger_leaf_sha256_v2,
    fee_capture_ledger_root_v2,
    fee_registry_checkpoint_sha256_v2,
    parse_public_fee_taker_rate_v2,
    resolve_fee_version_v2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2

_FIXTURES = Path(__file__).parents[3] / "fixtures" / "r4b_v2" / "fees"
_ATTEMPT = "prospective-attempt-v2"
_T0 = 10_000_000
_HORIZON_END = _T0 + 2 * FEE_POLL_CADENCE_MS_V2
_PROTOCOL_HASH = "1" * 64
_UNIVERSE_HASH = "2" * 64
_POSITION_EVENT_ID = "3" * 64
_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYGBgAAAABQABpfZFQAAAAABJRU5ErkJggg=="
)


def _scope(*, attempt_id: str = _ATTEMPT, plan_id: str = "frozen-plan-v2") -> FeeProtocolScopeV2:
    return FeeProtocolScopeV2(
        attempt_id=attempt_id,
        plan_id=plan_id,
        protocol_hash=_PROTOCOL_HASH,
        universe_sha256=_UNIVERSE_HASH,
    )


def _url(venue: VenueV2) -> str:
    return (
        SPOT_OFFICIAL_FEE_URL_V2
        if venue is VenueV2.SPOT
        else USDM_OFFICIAL_FEE_URL_V2
    )


def _page(venue: VenueV2, *, taker_percent: str | None = None) -> bytes:
    name = (
        "binance_spot_fee_page.html"
        if venue is VenueV2.SPOT
        else "binance_usdm_fee_page.html"
    )
    page = (_FIXTURES / name).read_text(encoding="utf-8")
    if taker_percent is not None:
        old = "0.1000% / 0.1000%" if venue is VenueV2.SPOT else "0.0200% / 0.0500%"
        maker = "0.1000" if venue is VenueV2.SPOT else "0.0200"
        page = page.replace(old, f"{maker}% / {taker_percent}%")
    return page.encode("utf-8")


def _transport(
    venue: VenueV2,
    *,
    completion_ms: int,
    sequence: int,
    scope: FeeProtocolScopeV2 | None = None,
    page: bytes | None = None,
    request_started_ms: int | None = None,
) -> FeeHttpCaptureEnvelopeV2:
    return FeeHttpCaptureEnvelopeV2(
        scope=_scope() if scope is None else scope,
        venue=venue,
        request_id=f"fee-request-{venue.value}-{sequence}",
        request_url=_url(venue),
        final_url=_url(venue),
        request_started_ms=(
            completion_ms - 5
            if request_started_ms is None
            else request_started_ms
        ),
        response_completion_ms=completion_ms,
        receipt_monotonic_ns=completion_ms * 1_000_000,
        ingest_seq=sequence,
        http_status=200,
        content_type="text/html; charset=utf-8",
        tls_verified=True,
        account_authenticated=False,
        authorization_header_present=False,
        raw_or_dom_kind=FeeCaptureArtifactKindV2.RENDERED_DOM,
        raw_or_dom_bytes=_page(venue) if page is None else page,
    )


def _capture(
    venue: VenueV2,
    *,
    completion_ms: int,
    role: FeeCaptureRoleV2,
    sequence: int,
    scope: FeeProtocolScopeV2 | None = None,
    taker_percent: str | None = None,
    png: bytes = _PNG,
    page: bytes | None = None,
) -> object:
    poll_sequence = sequence if role is FeeCaptureRoleV2.POST_T0_POLL else None
    poll_scheduled_ms = (
        _T0 + sequence * FEE_POLL_CADENCE_MS_V2
        if role is FeeCaptureRoleV2.POST_T0_POLL
        else None
    )
    transport = _transport(
        venue,
        completion_ms=completion_ms,
        sequence=sequence,
        scope=scope,
        page=(
            _page(venue, taker_percent=taker_percent)
            if page is None
            else page
        ),
        request_started_ms=(
            poll_scheduled_ms
            if poll_scheduled_ms is not None
            else completion_ms - 5
        ),
    )
    leaf = fee_capture_ledger_leaf_sha256_v2(
        transport,
        png,
        capture_role=role,
        poll_sequence=poll_sequence,
        poll_scheduled_ms=poll_scheduled_ms,
    )
    checkpoint = FeeCaptureLedgerCheckpointV2(
        scope=transport.scope,
        ledger_id=f"external-capture-ledger-{venue.value}-{sequence}",
        ledger_root_sha256=fee_capture_ledger_root_v2((leaf,)),
        event_count=1,
        observed_through_ms=completion_ms,
        sealed_at_ms=completion_ms,
    )
    return build_fee_page_capture_v2(
        transport=transport,
        capture_role=role,
        png_bytes=png,
        ledger_checkpoint=checkpoint,
        ledger_leaf_index=0,
        ledger_merkle_siblings=(),
        expected_ledger_checkpoint_sha256=checkpoint.checkpoint_sha256,
        poll_sequence=poll_sequence,
        poll_scheduled_ms=poll_scheduled_ms,
    )


def _manifest(*, scope: FeeProtocolScopeV2 | None = None) -> PublicFeeManifestV2:
    selected_scope = _scope() if scope is None else scope
    spot = _capture(
        VenueV2.SPOT,
        completion_ms=_T0 - 2,
        role=FeeCaptureRoleV2.PRE_T0,
        sequence=1,
        scope=selected_scope,
    )
    usdm = _capture(
        VenueV2.USDM_FUTURES,
        completion_ms=_T0 - 1,
        role=FeeCaptureRoleV2.PRE_T0,
        sequence=2,
        scope=selected_scope,
    )
    return PublicFeeManifestV2(
        scope=selected_scope,
        t0_ms=_T0,
        horizon_end_ms=_HORIZON_END,
        spot_capture=spot,  # type: ignore[arg-type]
        usdm_capture=usdm,  # type: ignore[arg-type]
    )


def _registry(
    manifest: PublicFeeManifestV2,
    *captures: object,
    maximum_events: int = 16,
) -> FeeCaptureRegistryV2:
    registry = FeeCaptureRegistryV2(maximum_events=maximum_events, scope=manifest.scope)
    registry.register(manifest.spot_capture)
    registry.register(manifest.usdm_capture)
    for capture in captures:
        registry.register(capture)  # type: ignore[arg-type]
    return registry


def _post(
    venue: VenueV2,
    sequence: int,
    *,
    taker_percent: str | None = None,
    scope: FeeProtocolScopeV2 | None = None,
    page: bytes | None = None,
) -> object:
    scheduled = _T0 + sequence * FEE_POLL_CADENCE_MS_V2
    return _capture(
        venue,
        completion_ms=scheduled,
        role=FeeCaptureRoleV2.POST_T0_POLL,
        sequence=sequence,
        scope=scope,
        taker_percent=taker_percent,
        page=page,
    )


def _timeline(
    manifest: PublicFeeManifestV2,
    registry: FeeCaptureRegistryV2,
    *,
    observed_through_ms: int,
) -> object:
    return build_fee_timeline_checkpoint_v2(
        manifest,
        registry,
        VenueV2.USDM_FUTURES,
        observed_through_ms=observed_through_ms,
        sealed_at_ms=observed_through_ms,
    )


def _resolve(
    manifest: PublicFeeManifestV2,
    registry: FeeCaptureRegistryV2,
    timeline: object,
    target_ms: int,
):
    return resolve_fee_version_v2(
        manifest,
        registry,
        timeline,  # type: ignore[arg-type]
        target_ms=target_ms,
        symbol="BTCUSDT",
        position_event_id=_POSITION_EVENT_ID,
        expected_timeline_checkpoint_sha256=timeline.checkpoint_sha256,  # type: ignore[attr-defined]
    )


def test_strict_parser_derives_rates_from_recorded_official_page_shapes() -> None:
    spot = _transport(VenueV2.SPOT, completion_ms=_T0 - 2, sequence=1)
    usdm = _transport(VenueV2.USDM_FUTURES, completion_ms=_T0 - 1, sequence=2)

    assert parse_public_fee_taker_rate_v2(spot) == SPOT_PUBLIC_TAKER_RATE_V2
    assert parse_public_fee_taker_rate_v2(usdm) == USDM_PUBLIC_TAKER_RATE_V2
    assert format(parse_public_fee_taker_rate_v2(spot), "f") == "0.001000"
    assert format(parse_public_fee_taker_rate_v2(usdm), "f") == "0.000500"
    assert "parsed_taker_rate" not in inspect.signature(
        build_fee_page_capture_v2
    ).parameters


@pytest.mark.parametrize(
    "page",
    (
        b"NOT A BINANCE RESPONSE",
        _page(VenueV2.USDM_FUTURES).replace(b"Binance", b"Example", 1),
        _page(VenueV2.USDM_FUTURES).replace(
            b"</tbody>",
            b"<tr><td>Regular User</td><td>USDT Maker / Taker 0.0200% / 0.0500%</td></tr></tbody>",
        ),
    ),
)
def test_parser_rejects_arbitrary_wrong_identity_and_ambiguous_rows(page: bytes) -> None:
    transport = _transport(
        VenueV2.USDM_FUTURES,
        completion_ms=_T0 - 1,
        sequence=1,
        page=page,
    )
    with pytest.raises(FeeContractErrorV2):
        parse_public_fee_taker_rate_v2(transport)


def test_transport_rejects_wrong_final_url_tls_auth_and_status() -> None:
    valid = _transport(VenueV2.SPOT, completion_ms=_T0 - 1, sequence=1)
    with pytest.raises(FeeContractErrorV2, match="exact official"):
        replace(valid, final_url="https://example.com/fee")
    with pytest.raises(FeeContractErrorV2, match="verified TLS"):
        replace(valid, tls_verified=False)
    with pytest.raises(FeeContractErrorV2, match="unauthenticated"):
        replace(valid, account_authenticated=True)
    with pytest.raises(FeeContractErrorV2, match="status 200"):
        replace(valid, http_status=500)


def test_post_t0_poll_start_is_exact_and_cannot_be_late_or_relabelled() -> None:
    first_schedule = _T0 + FEE_POLL_CADENCE_MS_V2
    on_time = _transport(
        VenueV2.USDM_FUTURES,
        completion_ms=first_schedule + 25,
        sequence=1,
        request_started_ms=first_schedule,
    )
    assert fee_capture_ledger_leaf_sha256_v2(
        on_time,
        _PNG,
        capture_role=FeeCaptureRoleV2.POST_T0_POLL,
        poll_sequence=1,
        poll_scheduled_ms=first_schedule,
    )

    late_start = replace(on_time, request_started_ms=first_schedule + 1)
    with pytest.raises(FeeContractErrorV2, match="exact sealed poll schedule"):
        fee_capture_ledger_leaf_sha256_v2(
            late_start,
            _PNG,
            capture_role=FeeCaptureRoleV2.POST_T0_POLL,
            poll_sequence=1,
            poll_scheduled_ms=first_schedule,
        )

    second_schedule = _T0 + 2 * FEE_POLL_CADENCE_MS_V2
    second_request = _transport(
        VenueV2.USDM_FUTURES,
        completion_ms=second_schedule,
        sequence=2,
        request_started_ms=second_schedule,
    )
    with pytest.raises(FeeContractErrorV2, match="exact sealed poll schedule"):
        fee_capture_ledger_leaf_sha256_v2(
            second_request,
            _PNG,
            capture_role=FeeCaptureRoleV2.POST_T0_POLL,
            poll_sequence=1,
            poll_scheduled_ms=first_schedule,
        )


def test_png_requires_real_signature_crc_and_decodable_scanlines() -> None:
    _capture(
        VenueV2.SPOT,
        completion_ms=_T0 - 1,
        role=FeeCaptureRoleV2.PRE_T0,
        sequence=1,
    )
    with pytest.raises(FeeContractErrorV2, match="PNG signature"):
        _capture(
            VenueV2.SPOT,
            completion_ms=_T0 - 1,
            role=FeeCaptureRoleV2.PRE_T0,
            sequence=1,
            png=b"NOT A PNG",
        )
    corrupted = bytearray(_PNG)
    corrupted[-8] ^= 1
    with pytest.raises(FeeContractErrorV2, match="CRC"):
        _capture(
            VenueV2.SPOT,
            completion_ms=_T0 - 1,
            role=FeeCaptureRoleV2.PRE_T0,
            sequence=1,
            png=bytes(corrupted),
        )


def test_capture_requires_external_checkpoint_hash_and_merkle_membership() -> None:
    transport = _transport(VenueV2.SPOT, completion_ms=_T0 - 1, sequence=1)
    leaf = fee_capture_ledger_leaf_sha256_v2(
        transport,
        _PNG,
        capture_role=FeeCaptureRoleV2.PRE_T0,
    )
    checkpoint = FeeCaptureLedgerCheckpointV2(
        scope=transport.scope,
        ledger_id="external-fee-ledger",
        ledger_root_sha256=leaf,
        event_count=1,
        observed_through_ms=_T0 - 1,
        sealed_at_ms=_T0 - 1,
    )
    with pytest.raises(FeeContractErrorV2, match="checkpoint hash mismatch"):
        build_fee_page_capture_v2(
            transport=transport,
            capture_role=FeeCaptureRoleV2.PRE_T0,
            png_bytes=_PNG,
            ledger_checkpoint=checkpoint,
            ledger_leaf_index=0,
            ledger_merkle_siblings=(),
            expected_ledger_checkpoint_sha256="f" * 64,
        )
    wrong_root = replace(checkpoint, ledger_root_sha256="f" * 64)
    with pytest.raises(FeeContractErrorV2, match="not a member"):
        build_fee_page_capture_v2(
            transport=transport,
            capture_role=FeeCaptureRoleV2.PRE_T0,
            png_bytes=_PNG,
            ledger_checkpoint=wrong_root,
            ledger_leaf_index=0,
            ledger_merkle_siblings=(),
            expected_ledger_checkpoint_sha256=wrong_root.checkpoint_sha256,
        )
    changed_completion = replace(
        transport,
        response_completion_ms=transport.response_completion_ms + 1,
    )
    with pytest.raises(FeeContractErrorV2, match="not a member"):
        build_fee_page_capture_v2(
            transport=changed_completion,
            capture_role=FeeCaptureRoleV2.PRE_T0,
            png_bytes=_PNG,
            ledger_checkpoint=checkpoint,
            ledger_leaf_index=0,
            ledger_merkle_siblings=(),
            expected_ledger_checkpoint_sha256=checkpoint.checkpoint_sha256,
        )


def test_manifest_binds_scope_baselines_and_pre_t0_membership_seal() -> None:
    manifest = _manifest()
    assert json.loads(canonical_public_fee_manifest_v2(manifest))["scope"][
        "plan_id"
    ] == "frozen-plan-v2"
    changed_usdm = _capture(
        VenueV2.USDM_FUTURES,
        completion_ms=_T0 - 1,
        role=FeeCaptureRoleV2.PRE_T0,
        sequence=2,
        taker_percent="0.0600",
    )
    with pytest.raises(FeeContractErrorV2, match="frozen baseline"):
        PublicFeeManifestV2(
            scope=manifest.scope,
            t0_ms=_T0,
            horizon_end_ms=_HORIZON_END,
            spot_capture=manifest.spot_capture,
            usdm_capture=changed_usdm,  # type: ignore[arg-type]
        )
    other_scope = _scope(plan_id="other-plan")
    with pytest.raises(FeeContractErrorV2, match="scope differs"):
        PublicFeeManifestV2(
            scope=other_scope,
            t0_ms=_T0,
            horizon_end_ms=_HORIZON_END,
            spot_capture=manifest.spot_capture,
            usdm_capture=manifest.usdm_capture,
        )


def test_registry_restore_requires_external_root_count_scope_checkpoint() -> None:
    manifest = _manifest()
    registry = _registry(manifest, _post(VenueV2.USDM_FUTURES, 1))
    checkpoint_sha = fee_registry_checkpoint_sha256_v2(
        scope=registry.scope,
        replay_root_sha256=registry.replay_root_sha256,
        event_count=registry.event_count,
        maximum_events=registry.maximum_events,
    )
    restored = FeeCaptureRegistryV2.from_state_v2(
        registry.export_state_v2(),
        expected_replay_root_sha256=registry.replay_root_sha256,
        expected_event_count=registry.event_count,
        expected_maximum_events=registry.maximum_events,
        expected_attempt_id=registry.scope.attempt_id,
        expected_plan_id=registry.scope.plan_id,
        expected_protocol_hash=registry.scope.protocol_hash,
        expected_universe_sha256=registry.scope.universe_sha256,
        expected_checkpoint_sha256=checkpoint_sha,
    )
    assert restored.replay_root_sha256 == registry.replay_root_sha256
    assert restored.event_count == registry.event_count


def test_registry_restore_rejects_prefix_truncation_even_if_prefix_is_self_consistent() -> None:
    manifest = _manifest()
    full = _registry(manifest, _post(VenueV2.USDM_FUTURES, 1))
    prefix = _registry(manifest)
    expected_checkpoint = fee_registry_checkpoint_sha256_v2(
        scope=full.scope,
        replay_root_sha256=full.replay_root_sha256,
        event_count=full.event_count,
        maximum_events=full.maximum_events,
    )
    with pytest.raises(FeeContractErrorV2, match="event census"):
        FeeCaptureRegistryV2.from_state_v2(
            prefix.export_state_v2(),
            expected_replay_root_sha256=full.replay_root_sha256,
            expected_event_count=full.event_count,
            expected_maximum_events=full.maximum_events,
            expected_attempt_id=full.scope.attempt_id,
            expected_plan_id=full.scope.plan_id,
            expected_protocol_hash=full.scope.protocol_hash,
            expected_universe_sha256=full.scope.universe_sha256,
            expected_checkpoint_sha256=expected_checkpoint,
        )


def test_registry_restore_revalidates_actual_parsed_artifact_bytes() -> None:
    manifest = _manifest()
    registry = _registry(manifest)
    state = json.loads(registry.export_state_v2())
    row = state["events"][0]
    archive = json.loads(base64.b64decode(row["archive_base64"]))
    parsed = bytearray(base64.b64decode(archive["parsed_json_base64"]))
    parsed[-2] ^= 1
    archive["parsed_json_base64"] = base64.b64encode(parsed).decode("ascii")
    archive_bytes = canonical_json_line(archive)
    row["archive_base64"] = base64.b64encode(archive_bytes).decode("ascii")
    row["archive_sha256"] = hashlib.sha256(archive_bytes).hexdigest()
    tampered = canonical_json_line(state)
    checkpoint = fee_registry_checkpoint_sha256_v2(
        scope=registry.scope,
        replay_root_sha256=registry.replay_root_sha256,
        event_count=registry.event_count,
        maximum_events=registry.maximum_events,
    )
    with pytest.raises(FeeContractErrorV2, match="parsed JSON bytes"):
        FeeCaptureRegistryV2.from_state_v2(
            tampered,
            expected_replay_root_sha256=registry.replay_root_sha256,
            expected_event_count=registry.event_count,
            expected_maximum_events=registry.maximum_events,
            expected_attempt_id=registry.scope.attempt_id,
            expected_plan_id=registry.scope.plan_id,
            expected_protocol_hash=registry.scope.protocol_hash,
            expected_universe_sha256=registry.scope.universe_sha256,
            expected_checkpoint_sha256=checkpoint,
        )


def test_registry_restore_recomputes_archive_root_manifest_from_artifacts() -> None:
    manifest = _manifest()
    registry = _registry(manifest)
    state = json.loads(registry.export_state_v2())
    row = state["events"][0]
    archive = json.loads(base64.b64decode(row["archive_base64"]))
    root_manifest = json.loads(
        base64.b64decode(archive["archive_root_manifest_base64"])
    )
    root_manifest["png_sha256"] = "f" * 64
    archive["archive_root_manifest_base64"] = base64.b64encode(
        canonical_json_line(root_manifest)
    ).decode("ascii")
    archive_bytes = canonical_json_line(archive)
    row["archive_base64"] = base64.b64encode(archive_bytes).decode("ascii")
    row["archive_sha256"] = hashlib.sha256(archive_bytes).hexdigest()
    tampered = canonical_json_line(state)
    checkpoint = fee_registry_checkpoint_sha256_v2(
        scope=registry.scope,
        replay_root_sha256=registry.replay_root_sha256,
        event_count=registry.event_count,
        maximum_events=registry.maximum_events,
    )
    with pytest.raises(FeeContractErrorV2, match="root manifest bytes"):
        FeeCaptureRegistryV2.from_state_v2(
            tampered,
            expected_replay_root_sha256=registry.replay_root_sha256,
            expected_event_count=registry.event_count,
            expected_maximum_events=registry.maximum_events,
            expected_attempt_id=registry.scope.attempt_id,
            expected_plan_id=registry.scope.plan_id,
            expected_protocol_hash=registry.scope.protocol_hash,
            expected_universe_sha256=registry.scope.universe_sha256,
            expected_checkpoint_sha256=checkpoint,
        )


def test_registry_conflict_idempotency_capacity_and_scope() -> None:
    manifest = _manifest()
    registry = FeeCaptureRegistryV2(maximum_events=2, scope=manifest.scope)
    assert registry.register(manifest.spot_capture) is FeeRegistryDispositionV2.NEW
    assert (
        registry.register(manifest.spot_capture)
        is FeeRegistryDispositionV2.IDEMPOTENT_DUPLICATE
    )
    registry.register(manifest.usdm_capture)
    with pytest.raises(FeeContractErrorV2, match="capacity"):
        registry.register(_post(VenueV2.USDM_FUTURES, 1))  # type: ignore[arg-type]
    other = _manifest(scope=_scope(plan_id="other-plan"))
    with pytest.raises(FeeContractErrorV2, match="different protocol scope"):
        registry.register(other.spot_capture)


def test_open_tail_never_resolves_or_becomes_qualification_final() -> None:
    manifest = _manifest()
    registry = _registry(manifest)
    timeline = _timeline(manifest, registry, observed_through_ms=_T0)
    resolution = _resolve(manifest, registry, timeline, _T0)

    assert timeline.qualification_final is False  # type: ignore[attr-defined]
    assert resolution.status is FeeResolutionStatusV2.PENDING_OPEN_TAIL
    assert resolution.resolved is False
    assert resolution.taker_rate is None


def test_same_version_closed_bracket_resolves_and_cadence_is_audited() -> None:
    manifest = _manifest()
    first = _post(VenueV2.USDM_FUTURES, 1)
    second = _post(VenueV2.USDM_FUTURES, 2)
    registry = _registry(manifest, first, second)
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    audit = audit_fee_poll_cadence_v2(
        manifest,
        registry,
        timeline,  # type: ignore[arg-type]
        expected_timeline_checkpoint_sha256=timeline.checkpoint_sha256,  # type: ignore[attr-defined]
    )
    resolution = _resolve(manifest, registry, timeline, _T0)

    assert timeline.qualification_final is True  # type: ignore[attr-defined]
    assert audit.cadence_complete
    assert audit.completed_poll_sequences == (1, 2)
    assert resolution.status is FeeResolutionStatusV2.RESOLVED
    assert resolution.taker_rate == USDM_PUBLIC_TAKER_RATE_V2
    assert resolution.timeline_qualification_final is True
    assert json.loads(canonical_fee_poll_audit_v2(audit))["payload_sha256"] == (
        audit.payload_sha256
    )


def test_changed_version_is_inconclusive_strictly_inside_bracket() -> None:
    manifest = _manifest()
    changed = _post(VenueV2.USDM_FUTURES, 1, taker_percent="0.0600")
    second = _post(VenueV2.USDM_FUTURES, 2, taker_percent="0.0600")
    registry = _registry(manifest, changed, second)
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    resolution = _resolve(manifest, registry, timeline, _T0)

    assert resolution.status is FeeResolutionStatusV2.INCONCLUSIVE_FEE_VERSION
    assert resolution.taker_rate is None


def test_changed_version_uses_each_endpoint_but_not_strict_interior() -> None:
    manifest = _manifest()
    old = _post(VenueV2.USDM_FUTURES, 1)
    changed = _post(VenueV2.USDM_FUTURES, 2, taker_percent="0.0600")
    registry = _registry(manifest, old, changed)
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    old_target = _T0 + FEE_POLL_CADENCE_MS_V2
    new_target = _HORIZON_END

    old_resolution = _resolve(manifest, registry, timeline, old_target)
    interior = _resolve(manifest, registry, timeline, old_target + 1)
    new_resolution = _resolve(manifest, registry, timeline, new_target)

    assert old_resolution.status is FeeResolutionStatusV2.RESOLVED
    assert old_resolution.taker_rate == Decimal("0.000500")
    assert interior.status is FeeResolutionStatusV2.INCONCLUSIVE_FEE_VERSION
    assert new_resolution.status is FeeResolutionStatusV2.RESOLVED
    assert new_resolution.taker_rate == Decimal("0.000600")


def test_same_rate_with_changed_source_artifact_opens_uncertain_interval() -> None:
    manifest = _manifest()
    first = _post(VenueV2.USDM_FUTURES, 1)
    changed_page = _page(VenueV2.USDM_FUTURES).replace(
        b"<main>",
        b"<main><!-- changed archived source bytes -->",
    )
    second = _post(VenueV2.USDM_FUTURES, 2, page=changed_page)
    registry = _registry(manifest, first, second)
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)

    assert first.parsed_taker_rate == second.parsed_taker_rate  # type: ignore[attr-defined]
    assert first.transport.raw_or_dom_sha256 != second.transport.raw_or_dom_sha256  # type: ignore[attr-defined]
    assert first.parsed_version_sha256 != second.parsed_version_sha256  # type: ignore[attr-defined]
    interior = _resolve(
        manifest,
        registry,
        timeline,
        _T0 + FEE_POLL_CADENCE_MS_V2 + 1,
    )
    assert interior.status is FeeResolutionStatusV2.INCONCLUSIVE_FEE_VERSION


def test_missing_poll_fails_closed_at_and_after_missing_schedule() -> None:
    manifest = _manifest()
    second = _post(VenueV2.USDM_FUTURES, 2)
    registry = _registry(manifest, second)
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    resolution = _resolve(
        manifest,
        registry,
        timeline,
        _T0 + FEE_POLL_CADENCE_MS_V2,
    )
    assert timeline.qualification_final is False  # type: ignore[attr-defined]
    assert resolution.status is FeeResolutionStatusV2.INCONCLUSIVE_FEE_POLL_GAP


def test_target_before_t0_is_rejected_not_sourced_from_pre_t0_capture() -> None:
    manifest = _manifest()
    registry = _registry(manifest)
    timeline = _timeline(manifest, registry, observed_through_ms=_T0)
    with pytest.raises(FeeContractErrorV2, match="before T0"):
        _resolve(manifest, registry, timeline, _T0 - 1)


def test_timeline_expected_hash_and_registry_root_census_are_revalidated() -> None:
    manifest = _manifest()
    first = _post(VenueV2.USDM_FUTURES, 1)
    registry = _registry(manifest, first)
    timeline = _timeline(
        manifest,
        registry,
        observed_through_ms=_T0 + FEE_POLL_CADENCE_MS_V2,
    )
    with pytest.raises(FeeContractErrorV2, match="checkpoint hash mismatch"):
        resolve_fee_version_v2(
            manifest,
            registry,
            timeline,  # type: ignore[arg-type]
            target_ms=_T0,
            symbol="BTCUSDT",
            position_event_id=_POSITION_EVENT_ID,
            expected_timeline_checkpoint_sha256="f" * 64,
        )
    registry.register(_post(VenueV2.USDM_FUTURES, 2))  # type: ignore[arg-type]
    with pytest.raises(FeeContractErrorV2, match="stale timeline registry"):
        _resolve(manifest, registry, timeline, _T0)


def test_future_spot_capture_cannot_enter_earlier_usdm_global_checkpoint() -> None:
    manifest = _manifest()
    registry = _registry(
        manifest,
        _post(VenueV2.USDM_FUTURES, 1),
        _post(VenueV2.USDM_FUTURES, 2),
        _post(VenueV2.SPOT, 3),
    )
    with pytest.raises(FeeContractErrorV2, match="global registry includes capture"):
        _timeline(manifest, registry, observed_through_ms=_HORIZON_END)


@pytest.mark.parametrize(
    ("multiplier", "expected_total"),
    (
        (FeeMultiplierV2.PRIMARY_1_0X, Decimal("0.1050000")),
        (FeeMultiplierV2.PRIMARY_1_5X, Decimal("0.1575000")),
        (FeeMultiplierV2.MANDATORY_ADVERSE_2_0X, Decimal("0.2100000")),
    ),
)
def test_final_both_leg_fee_charges_entry_and_exit(
    multiplier: FeeMultiplierV2,
    expected_total: Decimal,
) -> None:
    manifest = _manifest()
    registry = _registry(
        manifest,
        _post(VenueV2.USDM_FUTURES, 1),
        _post(VenueV2.USDM_FUTURES, 2),
    )
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    entry = _resolve(manifest, registry, timeline, _T0)
    exit_ = _resolve(manifest, registry, timeline, _HORIZON_END)
    fee = calculate_filled_both_leg_fee_v2(
        entry,
        exit_,
        manifest=manifest,
        registry=registry,
        final_timeline_checkpoint=timeline,  # type: ignore[arg-type]
        expected_final_timeline_checkpoint_sha256=timeline.checkpoint_sha256,  # type: ignore[attr-defined]
        entry_filled_notional=Decimal("100"),
        exit_filled_notional=Decimal("110"),
        multiplier=multiplier,
    )

    assert fee.total_fee == expected_total
    assert fee.symbol == "BTCUSDT"
    assert fee.position_event_id == _POSITION_EVENT_ID
    assert fee.final_timeline_root_sha256 == timeline.timeline_root_sha256  # type: ignore[attr-defined]
    assert json.loads(canonical_filled_both_leg_fee_v2(fee))["payload_sha256"] == (
        fee.payload_sha256
    )


def test_cost_rejects_nonfinal_and_stale_resolution_instead_of_trusting_token() -> None:
    manifest = _manifest()
    first = _post(VenueV2.USDM_FUTURES, 1)
    registry = _registry(manifest, first)
    early_timeline = _timeline(
        manifest,
        registry,
        observed_through_ms=_T0 + FEE_POLL_CADENCE_MS_V2,
    )
    early = _resolve(manifest, registry, early_timeline, _T0)
    with pytest.raises(FeeContractErrorV2, match="closes the horizon tail"):
        calculate_filled_both_leg_fee_v2(
            early,
            early,
            manifest=manifest,
            registry=registry,
            final_timeline_checkpoint=early_timeline,  # type: ignore[arg-type]
            expected_final_timeline_checkpoint_sha256=early_timeline.checkpoint_sha256,  # type: ignore[attr-defined]
            entry_filled_notional=Decimal("100"),
            exit_filled_notional=Decimal("100"),
            multiplier=FeeMultiplierV2.PRIMARY_1_0X,
        )
    registry.register(_post(VenueV2.USDM_FUTURES, 2))  # type: ignore[arg-type]
    final_timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    final_exit = _resolve(manifest, registry, final_timeline, _HORIZON_END)
    with pytest.raises(FeeContractErrorV2, match="stale or forged"):
        calculate_filled_both_leg_fee_v2(
            early,
            final_exit,
            manifest=manifest,
            registry=registry,
            final_timeline_checkpoint=final_timeline,  # type: ignore[arg-type]
            expected_final_timeline_checkpoint_sha256=final_timeline.checkpoint_sha256,  # type: ignore[attr-defined]
            entry_filled_notional=Decimal("100"),
            exit_filled_notional=Decimal("100"),
            multiplier=FeeMultiplierV2.PRIMARY_1_0X,
        )


def test_fee_math_uses_frozen_decimal34_independent_of_ambient_context() -> None:
    manifest = _manifest()
    registry = _registry(
        manifest,
        _post(VenueV2.USDM_FUTURES, 1),
        _post(VenueV2.USDM_FUTURES, 2),
    )
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    entry = _resolve(manifest, registry, timeline, _T0)
    exit_ = _resolve(manifest, registry, timeline, _HORIZON_END)
    notional = Decimal("100.123456789012345678901234567890123456789")
    with localcontext(protocol_decimal_context_v2()):
        expected_leg = notional * Decimal("0.000500") * Decimal("1.5")
        expected_total = expected_leg + expected_leg
    ambient = getcontext().copy()
    try:
        getcontext().prec = 7
        getcontext().rounding = ROUND_DOWN
        fee = calculate_filled_both_leg_fee_v2(
            entry,
            exit_,
            manifest=manifest,
            registry=registry,
            final_timeline_checkpoint=timeline,  # type: ignore[arg-type]
            expected_final_timeline_checkpoint_sha256=timeline.checkpoint_sha256,  # type: ignore[attr-defined]
            entry_filled_notional=notional,
            exit_filled_notional=notional,
            multiplier=FeeMultiplierV2.PRIMARY_1_5X,
        )
    finally:
        getcontext().prec = ambient.prec
        getcontext().rounding = ambient.rounding
    assert fee.entry_fee == expected_leg
    assert fee.total_fee == expected_total
    assert fee.entry_fee == Decimal("0.07509259259175925925917592592591759")


def test_fee_math_preserves_frozen_underflow_trap() -> None:
    manifest = _manifest()
    registry = _registry(
        manifest,
        _post(VenueV2.USDM_FUTURES, 1),
        _post(VenueV2.USDM_FUTURES, 2),
    )
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    entry = _resolve(manifest, registry, timeline, _T0)
    exit_ = _resolve(manifest, registry, timeline, _HORIZON_END)
    subnormal_inexact = Decimal(
        "1.2345678901234567890123456789012345e-999999"
    )
    with pytest.raises(Underflow):
        calculate_filled_both_leg_fee_v2(
            entry,
            exit_,
            manifest=manifest,
            registry=registry,
            final_timeline_checkpoint=timeline,  # type: ignore[arg-type]
            expected_final_timeline_checkpoint_sha256=timeline.checkpoint_sha256,  # type: ignore[attr-defined]
            entry_filled_notional=subnormal_inexact,
            exit_filled_notional=subnormal_inexact,
            multiplier=FeeMultiplierV2.PRIMARY_1_5X,
        )


def test_canonical_capture_and_archive_bind_bytes_parser_and_archive_root() -> None:
    capture = _capture(
        VenueV2.SPOT,
        completion_ms=_T0 - 1,
        role=FeeCaptureRoleV2.PRE_T0,
        sequence=1,
    )
    public = json.loads(canonical_fee_page_capture_v2(capture))  # type: ignore[arg-type]
    archive = json.loads(canonical_fee_capture_archive_v2(capture))  # type: ignore[arg-type]
    assert public["archive_root_sha256"] == capture.archive_root_sha256  # type: ignore[attr-defined]
    assert hashlib.sha256(base64.b64decode(archive["raw_or_dom_base64"])).hexdigest() == (
        capture.transport.raw_or_dom_sha256  # type: ignore[attr-defined]
    )
    assert base64.b64decode(archive["parsed_json_base64"]) == capture.parsed_json_bytes  # type: ignore[attr-defined]


def test_resolution_canonical_payload_binds_plan_universe_symbol_position_and_timeline() -> None:
    manifest = _manifest()
    registry = _registry(
        manifest,
        _post(VenueV2.USDM_FUTURES, 1),
        _post(VenueV2.USDM_FUTURES, 2),
    )
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    resolution = _resolve(manifest, registry, timeline, _T0)
    document = json.loads(canonical_fee_version_resolution_v2(resolution))
    assert document["scope"]["plan_id"] == "frozen-plan-v2"
    assert document["scope"]["universe_sha256"] == _UNIVERSE_HASH
    assert document["symbol"] == "BTCUSDT"
    assert document["position_event_id"] == _POSITION_EVENT_ID
    assert document["timeline_checkpoint_sha256"] == timeline.checkpoint_sha256  # type: ignore[attr-defined]
