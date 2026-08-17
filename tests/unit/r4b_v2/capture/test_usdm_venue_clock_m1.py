from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
)
from signalbot.r4b_v2.capture.integrity_ledger import CaptureIntegrityLedgerV2
from signalbot.r4b_v2.capture.membership import (
    CurrentVerifiedRawMembershipLeafUseV2,
    consume_current_verified_raw_membership_prefix_v2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingPlanV9,
    ProvisionalUsdmVenueClockRestCapturePlanV9,
    build_provisional_promoting_capture_plans_v9,
    provisional_promoting_plan_sha256_v9,
)
from signalbot.r4b_v2.capture.rest_clock import (
    PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
    PublicUsdmVenueClockRestErrorCategoryV9,
    PublicUsdmVenueClockRestTerminalObservationV9,
)
from signalbot.r4b_v2.capture.usdm_venue_clock_m1 import (
    USDM_VENUE_CLOCK_M1_ONLY_REASON_V2,
    UsdmVenueClockSampleM1ContractErrorV2,
    UsdmVenueClockSampleM1V2,
    canonical_usdm_venue_clock_sample_m1_v2,
    parse_current_verified_usdm_venue_clock_sample_m1_v2,
    usdm_venue_clock_sample_fresh_at_m1_v2,
    usdm_venue_clock_samples_rate_continuous_m1_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

_PROTOCOL_SHA256 = "a" * 64
_SLOT_MS = 1_710_000_000_000
_MAXIMUM_BYTES = 8 * 1024 * 1024
_RESERVE_BYTES = 1_024


def _plans() -> tuple[ProvisionalPromotingPlanV9, ...]:
    return build_provisional_promoting_capture_plans_v9(("BTCUSDT",))


def _clock_plan(
    plans: tuple[ProvisionalPromotingPlanV9, ...],
) -> ProvisionalUsdmVenueClockRestCapturePlanV9:
    [plan] = [
        item
        for item in plans
        if type(item) is ProvisionalUsdmVenueClockRestCapturePlanV9
    ]
    return plan


def _signer() -> Ed25519BlockSignerV2:
    return Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="usdm-venue-clock-m1-test-key",
        private_key_bytes=b"\x39" * 32,
    )


def _signing_authority() -> BlockSigningAuthorityV2:
    signer = _signer()
    return BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )


def _policy() -> BlockPolicyV2:
    return BlockPolicyV2(
        qualification_id="usdm-venue-clock-m1-zstd",
        codec_candidate_id="usdm-venue-clock-m1-zstd-candidate",
        compression_level=9,
        max_uncompressed_bytes=4_194_304,
        max_linger_ms=1_000,
    )


@dataclass(frozen=True, slots=True)
class _Times:
    request_wall_ms: int
    request_monotonic_ns: int
    first_wall_ms: int
    first_monotonic_ns: int
    ended_wall_ms: int
    ended_monotonic_ns: int
    completion_wall_ms: int
    completion_monotonic_ns: int


def _times(
    *,
    base_wall_ms: int = _SLOT_MS,
    base_monotonic_ns: int = 0,
    header_wall_ms: int = 10,
    header_monotonic_ns: int = 10_000_000,
    completion_wall_ms: int = 12,
    completion_monotonic_ns: int = 12_000_000,
) -> _Times:
    return _Times(
        request_wall_ms=base_wall_ms,
        request_monotonic_ns=base_monotonic_ns,
        first_wall_ms=base_wall_ms + header_wall_ms,
        first_monotonic_ns=base_monotonic_ns + header_monotonic_ns,
        ended_wall_ms=base_wall_ms + completion_wall_ms,
        ended_monotonic_ns=base_monotonic_ns + completion_monotonic_ns,
        completion_wall_ms=base_wall_ms + completion_wall_ms,
        completion_monotonic_ns=base_monotonic_ns + completion_monotonic_ns,
    )


def _sample(
    root: Path,
    *,
    times: _Times | None = None,
    body: bytes = b'{"serverTime":1710000000011}',
    response_status: int = 200,
) -> UsdmVenueClockSampleM1V2:
    selected = _times() if times is None else times
    plans = _plans()
    plan = _clock_plan(plans)
    error_category = (
        None
        if response_status == 200
        else PublicUsdmVenueClockRestErrorCategoryV9.HTTP_STATUS
    )
    observation = PublicUsdmVenueClockRestTerminalObservationV9.for_plan(
        plan,
        session_id="session-usdm-venue-clock-m1",
        protocol_hash=_PROTOCOL_SHA256,
        connection_id="connection-usdm-venue-clock-m1",
        connection_generation=1,
        poll_cycle_seq=1,
        scheduled_slot_wall_ms=_SLOT_MS,
        request_started_wall_ms=selected.request_wall_ms,
        request_started_monotonic_ns=selected.request_monotonic_ns,
        response_first_header_wall_ms=selected.first_wall_ms,
        response_first_header_monotonic_ns=selected.first_monotonic_ns,
        attempt_ended_wall_ms=selected.ended_wall_ms,
        attempt_ended_monotonic_ns=selected.ended_monotonic_ns,
        response_status=response_status,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        body=body,
        error_category=error_category,
        error_detail=(None if error_category is None else f"HTTP status {response_status}"),
    )
    payload = observation(
        ReceiptTimestamp(
            selected.completion_wall_ms,
            selected.completion_monotonic_ns,
        )
    )
    authority = WalAuthorityV2(
        attempt_id="attempt-usdm-venue-clock-m1",
        protocol_sha256=_PROTOCOL_SHA256,
        plan_sha256=provisional_promoting_plan_sha256_v9(plans),
        source_manifest_sha256="b" * 64,
        schema_sha256="c" * 64,
        runtime_manifest_sha256="d" * 64,
    )
    record = RawRecordV2.from_payload(
        session_id="session-usdm-venue-clock-m1",
        plan_id=plan.name,
        protocol_hash=_PROTOCOL_SHA256,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id=plan.route_id,
        symbol=None,
        connection_id="connection-usdm-venue-clock-m1",
        generation=1,
        frame_seq=None,
        ingest_seq=1,
        receipt_wall_ms=selected.completion_wall_ms,
        receipt_monotonic_ns=selected.completion_monotonic_ns,
        raw_payload=payload,
        source_logical_key=PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
    )
    queued = QueuedRawRecordV2.encode(
        record,
        enqueued_monotonic_ns=selected.completion_monotonic_ns + 1,
    )
    writer = GroupedBlockWriterV2(
        root / "blocks",
        authority=authority,
        policy=_policy(),
        signer=_signer(),
        signing_authority=_signing_authority(),
        stream_group_id="usdm-venue-clock-rest-group",
        segment_id="segment-000001",
        maximum_total_bytes=_MAXIMUM_BYTES,
        emergency_reserve_bytes=_RESERVE_BYTES,
    )
    builder = GroupedBlockBuilderV2(writer.policy)
    assert not builder.offer(queued, now_ns=selected.completion_monotonic_ns + 2)
    block = builder.flush_tail(now_ns=selected.completion_monotonic_ns + 3)
    assert block is not None
    writer.commit(block)
    ledger = CaptureIntegrityLedgerV2(
        root / "ledger",
        authority=authority,
        block_directory=writer.directory,
        block_root_binding=writer.root_binding,
        block_signing_authority=writer.signing_authority,
        block_policy=writer.policy,
        block_stream_group_id=writer.stream_group_id,
        block_segment_id=writer.segment_id,
        maximum_total_bytes=_MAXIMUM_BYTES,
        emergency_reserve_bytes=_RESERVE_BYTES,
        max_events=32,
        failure_domain_id="usdm-venue-clock-m1-ledger-device",
    )
    rows: list[UsdmVenueClockSampleM1V2] = []

    def consume(
        ingest_seq: int,
        encoded_line: bytes,
        current_use: CurrentVerifiedRawMembershipLeafUseV2 | None,
    ) -> None:
        del ingest_seq, encoded_line
        assert current_use is not None
        rows.append(
            parse_current_verified_usdm_venue_clock_sample_m1_v2(
                current_use,
                promoting_plans=plans,
            )
        )

    consume_current_verified_raw_membership_prefix_v2(
        writer,
        integrity_ledger=ledger,
        expected_transport=TransportV2.HTTPS,
        expected_venue=VenueV2.USDM_FUTURES,
        expected_route_id=plan.route_id,
        expected_symbol=None,
        consume=consume,
    )
    assert len(rows) == 1
    return rows[0]


def test_current_membership_sample_parses_and_preserves_explicit_nonclaims(
    tmp_path: Path,
) -> None:
    sample = _sample(tmp_path)

    assert sample.server_time_ms == 1_710_000_000_011
    assert sample.header_rtt_ns == 10_000_000
    assert sample.header_wall_monotonic_residual_ns == 0
    assert sample.completion_wall_monotonic_residual_ns == 0
    assert sample.current_membership_consumed and sample.body_semantics_verified
    assert not sample.current_authority_claimed
    assert not sample.durable_membership_reverified_after_factory
    assert not sample.freshness_at_factory_verified
    assert not sample.prefix_rate_continuity_verified
    assert not sample.causal_cursor_complete
    assert sample.authority_reason == USDM_VENUE_CLOCK_M1_ONLY_REASON_V2
    assert canonical_usdm_venue_clock_sample_m1_v2(sample).endswith(b"\n")


@pytest.mark.parametrize(
    "body",
    (
        b'{"serverTime":true}',
        b'{"serverTime":1.5}',
        b'{"serverTime":1,"extra":2}',
        b'{"serverTime":1,"serverTime":2}',
        b'{"serverTime":-1}',
    ),
)
def test_status_and_exact_server_time_body_fail_closed(
    tmp_path: Path,
    body: bytes,
) -> None:
    with pytest.raises(UsdmVenueClockSampleM1ContractErrorV2, match="semantic"):
        _sample(tmp_path, body=body)


def test_non_200_status_fails_sample_semantics(tmp_path: Path) -> None:
    with pytest.raises(UsdmVenueClockSampleM1ContractErrorV2, match="semantic"):
        _sample(tmp_path, response_status=500)


def test_server_time_zero_and_int64_max_boundaries_parse(tmp_path: Path) -> None:
    assert _sample(tmp_path / "zero", body=b'{"serverTime":0}').server_time_ms == 0
    maximum = (1 << 63) - 1
    sample = _sample(
        tmp_path / "maximum",
        body=f'{{"serverTime":{maximum}}}'.encode(),
    )
    assert sample.server_time_ms == maximum
    assert b'"server_time_ms_text":"9223372036854775807"' in (
        canonical_usdm_venue_clock_sample_m1_v2(sample)
    )

    with pytest.raises(UsdmVenueClockSampleM1ContractErrorV2, match="semantic"):
        _sample(
            tmp_path / "over",
            body=f'{{"serverTime":{maximum + 1}}}'.encode(),
        )


def test_reversed_wall_and_monotonic_timestamps_fail_closed(tmp_path: Path) -> None:
    reversed_wall = _Times(
        request_wall_ms=_SLOT_MS,
        request_monotonic_ns=0,
        first_wall_ms=_SLOT_MS - 1,
        first_monotonic_ns=1,
        ended_wall_ms=_SLOT_MS + 1,
        ended_monotonic_ns=2,
        completion_wall_ms=_SLOT_MS + 2,
        completion_monotonic_ns=3,
    )
    with pytest.raises(UsdmVenueClockSampleM1ContractErrorV2, match="wall timestamps"):
        _sample(tmp_path / "wall", times=reversed_wall)

    reversed_monotonic = _Times(
        request_wall_ms=_SLOT_MS,
        request_monotonic_ns=2,
        first_wall_ms=_SLOT_MS + 1,
        first_monotonic_ns=1,
        ended_wall_ms=_SLOT_MS + 2,
        ended_monotonic_ns=3,
        completion_wall_ms=_SLOT_MS + 3,
        completion_monotonic_ns=4,
    )
    with pytest.raises(ValueError, match="monotonic attempt clocks"):
        _sample(tmp_path / "monotonic", times=reversed_monotonic)


def test_rtt_exact_2000ms_passes_and_one_nanosecond_over_fails(
    tmp_path: Path,
) -> None:
    exact = _times(
        header_wall_ms=2_000,
        header_monotonic_ns=2_000_000_000,
        completion_wall_ms=2_000,
        completion_monotonic_ns=2_000_000_000,
    )
    assert _sample(tmp_path / "exact", times=exact).header_rtt_ns == 2_000_000_000

    over = _times(
        header_wall_ms=2_000,
        header_monotonic_ns=2_000_000_001,
        completion_wall_ms=2_000,
        completion_monotonic_ns=2_000_000_001,
    )
    with pytest.raises(UsdmVenueClockSampleM1ContractErrorV2, match="RTT exceeds"):
        _sample(tmp_path / "over", times=over)


def test_residual_exact_2ms_passes_and_one_nanosecond_over_fails(
    tmp_path: Path,
) -> None:
    exact = _times(
        header_wall_ms=12,
        header_monotonic_ns=10_000_000,
        completion_wall_ms=12,
        completion_monotonic_ns=10_000_000,
    )
    assert (
        _sample(tmp_path / "exact", times=exact).header_wall_monotonic_residual_ns
        == 2_000_000
    )

    over = _times(
        header_wall_ms=12,
        header_monotonic_ns=9_999_999,
        completion_wall_ms=12,
        completion_monotonic_ns=9_999_999,
    )
    with pytest.raises(UsdmVenueClockSampleM1ContractErrorV2, match="residual exceeds"):
        _sample(tmp_path / "over", times=over)


def test_age_exact_60000ms_is_fresh_and_one_nanosecond_over_is_not(
    tmp_path: Path,
) -> None:
    sample = _sample(tmp_path)
    available = sample.available_at_monotonic_ns
    assert usdm_venue_clock_sample_fresh_at_m1_v2(
        sample,
        observed_monotonic_ns=available + 60_000_000_000,
    )
    assert not usdm_venue_clock_sample_fresh_at_m1_v2(
        sample,
        observed_monotonic_ns=available + 60_000_000_001,
    )
    assert not usdm_venue_clock_sample_fresh_at_m1_v2(
        sample,
        observed_monotonic_ns=available - 1,
    )


@pytest.mark.parametrize(
    ("server_elapsed_ms", "expected"),
    ((996, False), (997, True), (1_003, True), (1_004, False)),
)
def test_rate_continuity_inclusive_1000ppm_plus_quantization_boundary(
    tmp_path: Path,
    server_elapsed_ms: int,
    expected: bool,
) -> None:
    zero = _times(
        header_wall_ms=0,
        header_monotonic_ns=0,
        completion_wall_ms=0,
        completion_monotonic_ns=0,
    )
    later = _times(
        base_wall_ms=_SLOT_MS + 1_000,
        base_monotonic_ns=1_000_000_000,
        header_wall_ms=0,
        header_monotonic_ns=0,
        completion_wall_ms=0,
        completion_monotonic_ns=0,
    )
    previous = _sample(
        tmp_path / "previous",
        times=zero,
        body=b'{"serverTime":1000000}',
    )
    current = _sample(
        tmp_path / "current",
        times=later,
        body=(f'{{"serverTime":{1_000_000 + server_elapsed_ms}}}'.encode()),
    )
    assert (
        usdm_venue_clock_samples_rate_continuous_m1_v2(previous, current)
        is expected
    )
