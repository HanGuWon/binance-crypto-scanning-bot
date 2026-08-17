from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import cast, overload

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockManifestV2,
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerV2,
    attest_finalized_block_v2,
)
from signalbot.r4b_v2.capture.membership import (
    RawRecordMembershipErrorV2,
    VerifiedRawMembershipLeafV2,
    attest_raw_record_membership_v2,
    verify_raw_record_membership_leaf_v2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingRestCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
    provisional_promoting_plan_sha256_v2,
)
from signalbot.r4b_v2.capture.rest import (
    PublicOiRestErrorCategoryV2,
    PublicOiRestTerminalObservationV2,
)
from signalbot.r4b_v2.capture.usdm_open_interest_m1 import (
    USDM_OPEN_INTEREST_M1_ONLY_REASON_V2,
    USDM_OPEN_INTEREST_M1_PARSER_CONTRACT_SHA256_V2,
    UsdmOpenInterestM1ContractErrorV2,
    UsdmOpenInterestM1V2,
    canonical_usdm_open_interest_m1_v2,
    parse_verified_usdm_open_interest_m1_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

PROTOCOL_SHA256 = "a" * 64
SLOT_MS = 1_710_000_000_000
MAXIMUM_BYTES = 8 * 1024 * 1024
RESERVE_BYTES = 1_024


def _plans(
    symbols: tuple[str, ...] = ("BTCUSDT",),
) -> tuple[ProvisionalPromotingPlanV2, ...]:
    return build_provisional_promoting_capture_plans_v2(symbols)


def _rest_plan(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> ProvisionalPromotingRestCapturePlanV2:
    [plan] = [
        cast(ProvisionalPromotingRestCapturePlanV2, item)
        for item in plans
        if type(item) is ProvisionalPromotingRestCapturePlanV2
    ]
    return plan


def _authority(plans: tuple[ProvisionalPromotingPlanV2, ...]) -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-usdm-oi-m1",
        protocol_sha256=PROTOCOL_SHA256,
        plan_sha256=provisional_promoting_plan_sha256_v2(plans),
        source_manifest_sha256="b" * 64,
        schema_sha256="c" * 64,
        runtime_manifest_sha256="d" * 64,
    )


def _signer() -> Ed25519BlockSignerV2:
    return Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="usdm-oi-m1-test-key",
        private_key_bytes=b"\x37" * 32,
    )


def _signing_authority() -> BlockSigningAuthorityV2:
    signer = _signer()
    return BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )


def _policy() -> BlockPolicyV2:
    return BlockPolicyV2(
        qualification_id="usdm-oi-m1-zstd",
        codec_candidate_id="usdm-oi-m1-zstd-candidate",
        compression_level=9,
        max_uncompressed_bytes=4_194_304,
        max_linger_ms=1_000,
    )


def _attempt_payload(
    plan: ProvisionalPromotingRestCapturePlanV2,
    *,
    body: bytes | None = None,
    response_status: int = 200,
    completion_wall_ms: int = SLOT_MS + 40,
    completion_monotonic_ns: int = 140,
) -> bytes:
    symbol = plan.symbols[0]
    observation = PublicOiRestTerminalObservationV2.for_plan(
        plan,
        symbol=symbol,
        poll_cycle_seq=7,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=SLOT_MS,
        attempt=1,
        request_started_wall_ms=SLOT_MS + 10,
        request_started_monotonic_ns=110,
        response_first_header_wall_ms=SLOT_MS + 20,
        response_first_header_monotonic_ns=120,
        attempt_ended_wall_ms=SLOT_MS + 30,
        attempt_ended_monotonic_ns=130,
        response_status=response_status,
        response_headers=(),
        payload_complete=True,
        body=(
            b'{"openInterest":"12345.6700","symbol":"BTCUSDT","time":1710000000025}'
            if body is None
            else body
        ),
        error_category=(
            None if 200 <= response_status < 300 else PublicOiRestErrorCategoryV2.HTTP_STATUS
        ),
        error_detail=(None if 200 <= response_status < 300 else f"HTTP status {response_status}"),
    )
    return observation(
        ReceiptTimestamp(
            received_at_ms=completion_wall_ms,
            received_monotonic_ns=completion_monotonic_ns,
        )
    )


@dataclass(frozen=True, slots=True)
class _Minted:
    plans: tuple[ProvisionalPromotingPlanV2, ...]
    writer: GroupedBlockWriterV2
    ledger: CaptureIntegrityLedgerV2
    manifest: BlockManifestV2
    leaf: VerifiedRawMembershipLeafV2

    def parse(
        self,
        *,
        plans: Sequence[ProvisionalPromotingPlanV2] | None = None,
    ) -> UsdmOpenInterestM1V2:
        return parse_verified_usdm_open_interest_m1_v2(
            self.leaf,
            promoting_plans=self.plans if plans is None else plans,
            block_directory=self.writer.directory,
            block_root_binding=self.writer.root_binding,
            authority=self.writer.authority,
            policy=self.writer.policy,
            signing_authority=self.writer.signing_authority,
            stream_group_id=self.writer.stream_group_id,
            segment_id=self.writer.segment_id,
            integrity_ledger=self.ledger,
        )


class _PlanSwappingSequence(Sequence[ProvisionalPromotingPlanV2]):
    """Expose one plan snapshot first and a conflicting one if reiterated."""

    def __init__(
        self,
        first: tuple[ProvisionalPromotingPlanV2, ...],
        later: tuple[ProvisionalPromotingPlanV2, ...],
    ) -> None:
        self._first = first
        self._later = later
        self.iteration_count = 0

    def __len__(self) -> int:
        return len(self._first)

    @overload
    def __getitem__(self, index: int) -> ProvisionalPromotingPlanV2: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[ProvisionalPromotingPlanV2]: ...

    def __getitem__(
        self, index: int | slice
    ) -> ProvisionalPromotingPlanV2 | Sequence[ProvisionalPromotingPlanV2]:
        return self._first[index]

    def __iter__(self) -> Iterator[ProvisionalPromotingPlanV2]:
        self.iteration_count += 1
        selected = self._first if self.iteration_count == 1 else self._later
        return iter(selected)


def _mint(
    root: Path,
    *,
    plans: tuple[ProvisionalPromotingPlanV2, ...] | None = None,
    body: bytes | None = None,
    response_status: int = 200,
    route_id: str = "usdm_public_rest",
    symbol: str = "BTCUSDT",
    plan_id: str | None = None,
    source_logical_key: str | None = None,
    frame_seq: int | None = None,
    payload_completion_wall_ms: int = SLOT_MS + 40,
    outer_receipt_wall_ms: int = SLOT_MS + 40,
) -> _Minted:
    frozen_plans = _plans() if plans is None else plans
    rest_plan = _rest_plan(frozen_plans)
    authority = _authority(frozen_plans)
    payload = _attempt_payload(
        rest_plan,
        body=body,
        response_status=response_status,
        completion_wall_ms=payload_completion_wall_ms,
    )
    record = RawRecordV2.from_payload(
        session_id="session-usdm-oi-m1",
        plan_id=rest_plan.name if plan_id is None else plan_id,
        protocol_hash=PROTOCOL_SHA256,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id=route_id,
        symbol=symbol,
        connection_id="connection-usdm-oi-m1",
        generation=1,
        frame_seq=frame_seq,
        ingest_seq=1,
        receipt_wall_ms=outer_receipt_wall_ms,
        receipt_monotonic_ns=140,
        raw_payload=payload,
        source_logical_key=(
            f"openInterest:{symbol}" if source_logical_key is None else source_logical_key
        ),
    )
    queued = QueuedRawRecordV2.encode(record, enqueued_monotonic_ns=141)
    signer = _signer()
    writer = GroupedBlockWriterV2(
        root / "blocks",
        authority=authority,
        policy=_policy(),
        signer=signer,
        signing_authority=_signing_authority(),
        stream_group_id="usdm-oi-rest-group",
        segment_id="segment-000001",
        maximum_total_bytes=MAXIMUM_BYTES,
        emergency_reserve_bytes=RESERVE_BYTES,
    )
    builder = GroupedBlockBuilderV2(writer.policy)
    assert not builder.offer(queued, now_ns=142)
    block = builder.flush_tail(now_ns=143)
    assert block is not None
    manifest = writer.commit(block)
    ledger = CaptureIntegrityLedgerV2(
        root / "ledger",
        authority=authority,
        block_directory=writer.directory,
        block_root_binding=writer.root_binding,
        block_signing_authority=writer.signing_authority,
        block_policy=writer.policy,
        block_stream_group_id=writer.stream_group_id,
        block_segment_id=writer.segment_id,
        maximum_total_bytes=MAXIMUM_BYTES,
        emergency_reserve_bytes=RESERVE_BYTES,
        max_events=32,
        failure_domain_id="usdm-oi-m1-ledger-device",
    )
    certificate = attest_raw_record_membership_v2(
        writer,
        manifest,
        expected_record_jsonl=queued.encoded_line,
        integrity_ledger=ledger,
    )
    leaf = verify_raw_record_membership_leaf_v2(
        certificate,
        block_directory=writer.directory,
        block_root_binding=writer.root_binding,
        authority=authority,
        policy=writer.policy,
        signing_authority=writer.signing_authority,
        stream_group_id=writer.stream_group_id,
        segment_id=writer.segment_id,
        integrity_ledger=ledger,
        expected_transport=TransportV2.HTTPS,
        expected_venue=VenueV2.USDM_FUTURES,
        expected_route_id=route_id,
        expected_symbol=symbol,
    )
    return _Minted(frozen_plans, writer, ledger, manifest, leaf)


def test_exact_current_oi_attempt_parses_body_and_binds_completion(
    tmp_path: Path,
) -> None:
    row = _mint(tmp_path).parse()

    assert row.symbol == "BTCUSDT"
    assert row.open_interest_text == "12345.6700"
    assert row.open_interest == Decimal("12345.6700")
    assert row.transaction_time_ms == 1_710_000_000_025
    assert row.poll_cycle_seq == 7
    assert row.scheduled_slot_wall_ms == SLOT_MS
    assert row.completion_admission_wall_ms == row.receipt_wall_ms
    assert row.completion_admission_monotonic_ns == row.receipt_monotonic_ns
    assert row.parser_contract_sha256 == USDM_OPEN_INTEREST_M1_PARSER_CONTRACT_SHA256_V2
    assert row.parser_bound and row.body_semantics_verified
    assert row.completion_receipt_bound and row.live_reverification_required
    assert not row.current_authority_claimed
    assert not row.schedule_cell_bound
    assert not row.freshness_verified
    assert not row.transaction_time_causally_bounded
    assert not row.cursor_complete
    assert not row.causal_inputs_complete
    assert row.authority_reason == USDM_OPEN_INTEREST_M1_ONLY_REASON_V2

    document = cast(dict[str, object], json.loads(canonical_usdm_open_interest_m1_v2(row)))
    assert document["open_interest"] == "12345.6700"
    assert document["m1_payload_sha256"] == row.m1_payload_sha256


def test_zero_oi_and_max_transaction_time_boundaries_parse(tmp_path: Path) -> None:
    body = (
        b'{"openInterest":"0","symbol":"BTCUSDT","time":'
        + str((1 << 63) - 1).encode("ascii")
        + b"}"
    )
    row = _mint(tmp_path, body=body).parse()

    assert row.open_interest == Decimal(0)
    assert row.transaction_time_ms == (1 << 63) - 1
    document = cast(dict[str, object], json.loads(canonical_usdm_open_interest_m1_v2(row)))
    assert document["transaction_time_ms_text"] == str((1 << 63) - 1)
    assert "transaction_time_ms" not in document


@pytest.mark.parametrize(
    ("body", "match"),
    [
        (b'{"openInterest":"1","symbol":"ETHUSDT","time":1}', "semantic"),
        (
            b'{"extra":1,"openInterest":"1","symbol":"BTCUSDT","time":1}',
            "semantic",
        ),
        (b'{"openInterest":1,"symbol":"BTCUSDT","time":1}', "semantic"),
        (b'{"openInterest":"-1","symbol":"BTCUSDT","time":1}', "semantic"),
    ],
)
def test_body_schema_and_decimal_fail_closed(
    tmp_path: Path,
    body: bytes,
    match: str,
) -> None:
    with pytest.raises(UsdmOpenInterestM1ContractErrorV2, match=match):
        _mint(tmp_path, body=body).parse()


def test_failed_http_and_completion_receipt_mismatch_fail_closed(
    tmp_path: Path,
) -> None:
    with pytest.raises(UsdmOpenInterestM1ContractErrorV2, match="semantic"):
        _mint(tmp_path / "status", response_status=500).parse()

    with pytest.raises(UsdmOpenInterestM1ContractErrorV2, match="outer raw receipt"):
        _mint(
            tmp_path / "receipt",
            payload_completion_wall_ms=SLOT_MS + 40,
            outer_receipt_wall_ms=SLOT_MS + 41,
        ).parse()


def test_outer_scope_plan_and_source_key_drift_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(RawRecordMembershipErrorV2, match="trusted scope"):
        _mint(tmp_path / "route", route_id="other-rest").parse()
    with pytest.raises(UsdmOpenInterestM1ContractErrorV2, match="plan_id differs"):
        _mint(tmp_path / "plan", plan_id="other-plan").parse()
    with pytest.raises(UsdmOpenInterestM1ContractErrorV2, match="logical key"):
        _mint(tmp_path / "key", source_logical_key="other-key").parse()
    with pytest.raises(UsdmOpenInterestM1ContractErrorV2, match="frame sequence"):
        _mint(tmp_path / "frame", frame_seq=1).parse()

    minted = _mint(tmp_path / "plan-hash")
    with pytest.raises(UsdmOpenInterestM1ContractErrorV2, match="frozen promoting plan"):
        minted.parse(plans=_plans(("BTCUSDT", "ETHUSDT")))


def test_promoting_plan_sequence_is_snapshotted_exactly_once(tmp_path: Path) -> None:
    minted = _mint(tmp_path)
    swapping = _PlanSwappingSequence(
        minted.plans,
        _plans(("BTCUSDT", "ETHUSDT")),
    )

    row = minted.parse(plans=swapping)

    assert swapping.iteration_count == 1
    assert row.promoting_plan_sha256 == provisional_promoting_plan_sha256_v2(minted.plans)


def test_factory_canonical_tamper_and_post_mint_void_fail_closed(
    tmp_path: Path,
) -> None:
    minted = _mint(tmp_path)
    row = minted.parse()
    with pytest.raises(UsdmOpenInterestM1ContractErrorV2, match="factory-sealed"):
        replace(row)

    object.__setattr__(row, "transaction_time_ms", row.transaction_time_ms + 1)
    with pytest.raises(UsdmOpenInterestM1ContractErrorV2, match="canonical evidence"):
        canonical_usdm_open_interest_m1_v2(row)

    reference = attest_finalized_block_v2(minted.writer, minted.manifest)
    data_path = minted.writer.directory / minted.manifest.data_file
    original = data_path.read_bytes()
    data_path.write_bytes(original + b"post-mint-corruption")
    try:
        minted.ledger.append_void_for_finalized_block(
            reference,
            detector_component="usdm-oi-m1-post-mint-auditor",
            detection_evidence_sha256="e" * 64,
        )
    finally:
        data_path.write_bytes(original)
    with pytest.raises(RawRecordMembershipErrorV2, match="VOID"):
        minted.parse()


def test_lineage_integer_outside_rfc8785_domain_fails_before_hashing(
    tmp_path: Path,
) -> None:
    row = _mint(tmp_path).parse()
    object.__setattr__(row, "receipt_monotonic_ns", 1 << 53)

    with pytest.raises(
        UsdmOpenInterestM1ContractErrorV2,
        match="RFC 8785 safe integer",
    ):
        canonical_usdm_open_interest_m1_v2(row)
