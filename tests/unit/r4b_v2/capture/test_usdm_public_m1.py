from __future__ import annotations

import copy
import json
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

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
    RawRecordMembershipErrorV2,
    VerifiedRawMembershipLeafV2,
    attest_raw_record_membership_v2,
    verify_raw_record_membership_leaf_v2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    build_provisional_promoting_capture_plans_v2,
    provisional_promoting_plan_sha256_v2,
)
from signalbot.r4b_v2.capture.usdm_public_m1 import (
    USDM_PUBLIC_DEPTH_M1_MAX_FRAME_BYTES_V2,
    USDM_PUBLIC_DEPTH_M1_MAX_LEVELS_PER_SIDE_V2,
    USDM_PUBLIC_DEPTH_M1_ONLY_REASON_V2,
    USDM_PUBLIC_DEPTH_M1_PARSER_CONTRACT_SHA256_V2,
    UsdmDepthDiff100msM1V2,
    UsdmPublicDepthM1ContractErrorV2,
    canonical_usdm_public_depth_m1_v2,
    parse_verified_usdm_public_depth_m1_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

PROTOCOL_SHA256 = "a" * 64
MAXIMUM_BYTES = 8 * 1024 * 1024
RESERVE_BYTES = 1024
RECEIPT_WALL_MS = 1_784_455_200_500
DEPTH_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "fixtures"
    / "r4b_v2"
    / "capture"
    / "usdm_depth_update.json"
)


def _plans(
    symbols: tuple[str, ...] = ("BTCUSDT",),
) -> tuple[ProvisionalPromotingPlanV2, ...]:
    return build_provisional_promoting_capture_plans_v2(symbols)


def _public_plan(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> ProvisionalPromotingCapturePlanV2:
    [plan] = [
        item
        for item in plans
        if isinstance(item, ProvisionalPromotingCapturePlanV2)
        and item.route_id == "usdm_public"
    ]
    return plan


def _authority(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-usdm-public-m1",
        protocol_sha256=PROTOCOL_SHA256,
        plan_sha256=provisional_promoting_plan_sha256_v2(plans),
        source_manifest_sha256="b" * 64,
        schema_sha256="c" * 64,
        runtime_manifest_sha256="d" * 64,
    )


def _signer() -> Ed25519BlockSignerV2:
    return Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="usdm-public-m1-test-key",
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
        qualification_id="sealed-zstd-1.5.7-l9",
        codec_candidate_id="zstd-1.5.7-l9-w0-checksum-content-size",
        compression_level=9,
        max_uncompressed_bytes=4_194_304,
        max_linger_ms=1_000,
    )


def _fixture_bytes() -> bytes:
    return DEPTH_FIXTURE.read_bytes().rstrip(b"\r\n")


def _fixture_document() -> dict[str, object]:
    return cast(dict[str, object], json.loads(_fixture_bytes()))


def _encoded(document: dict[str, object]) -> bytes:
    return json.dumps(
        document,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class _Minted:
    plans: tuple[ProvisionalPromotingPlanV2, ...]
    writer: GroupedBlockWriterV2
    ledger: CaptureIntegrityLedgerV2
    leaf: VerifiedRawMembershipLeafV2

    def parse(
        self,
        *,
        plans: tuple[ProvisionalPromotingPlanV2, ...] | None = None,
    ) -> UsdmDepthDiff100msM1V2:
        return parse_verified_usdm_public_depth_m1_v2(
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


def _mint(
    root: Path,
    payload: bytes,
    *,
    plans: tuple[ProvisionalPromotingPlanV2, ...] | None = None,
    route_id: str = "usdm_public",
    record_symbol: str | None = None,
    plan_id: str | None = None,
) -> _Minted:
    frozen_plans = _plans() if plans is None else plans
    authority = _authority(frozen_plans)
    public_plan = _public_plan(frozen_plans)
    record = RawRecordV2.from_payload(
        session_id="session-usdm-public-m1",
        plan_id=public_plan.name if plan_id is None else plan_id,
        protocol_hash=PROTOCOL_SHA256,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id=route_id,
        symbol=record_symbol,
        connection_id="connection-usdm-public-m1",
        generation=1,
        frame_seq=1,
        ingest_seq=1,
        receipt_wall_ms=RECEIPT_WALL_MS,
        receipt_monotonic_ns=10_000_000,
        raw_payload=payload,
        source_logical_key=None,
    )
    queued = QueuedRawRecordV2.encode(record, enqueued_monotonic_ns=10_000_001)
    signer = _signer()
    signing_authority = _signing_authority()
    writer = GroupedBlockWriterV2(
        root / "blocks",
        authority=authority,
        policy=_policy(),
        signer=signer,
        signing_authority=signing_authority,
        stream_group_id="usdm-public-combined-group",
        segment_id="segment-000001",
        maximum_total_bytes=MAXIMUM_BYTES,
        emergency_reserve_bytes=RESERVE_BYTES,
    )
    builder = GroupedBlockBuilderV2(writer.policy)
    assert not builder.offer(queued, now_ns=10_000_002)
    block = builder.flush_tail(now_ns=10_000_003)
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
        failure_domain_id="usdm-public-m1-ledger-device",
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
        expected_transport=TransportV2.WEBSOCKET,
        expected_venue=VenueV2.USDM_FUTURES,
        expected_route_id=route_id,
        expected_symbol=record_symbol,
    )
    return _Minted(frozen_plans, writer, ledger, leaf)


def test_recorded_current_depth_frame_parses_without_m2_overclaims(
    tmp_path: Path,
) -> None:
    row = _mint(tmp_path, _fixture_bytes()).parse()

    assert row.symbol == "BTCUSDT"
    assert row.stream == "btcusdt@depth@100ms"
    assert row.event_ms == 1_784_455_200_123
    assert row.transaction_time_ms == 1_784_455_200_119
    assert row.first_update_id == 660_000_001
    assert row.final_update_id == 660_000_004
    assert row.previous_final_update_id == 660_000_000
    assert row.bids == (
        (Decimal("117842.30"), Decimal("1.250")),
        (Decimal("117841.90"), Decimal("0")),
    )
    assert row.asks[-1] == (Decimal("117843.00"), Decimal("0"))
    assert row.stream_type == 1
    assert row.parser_contract_sha256 == (
        USDM_PUBLIC_DEPTH_M1_PARSER_CONTRACT_SHA256_V2
    )
    assert row.parser_bound
    assert row.live_reverification_required
    assert not row.current_authority_claimed
    assert not row.cursor_complete
    assert not row.causal_inputs_complete
    assert not row.sequence_continuity_claimed
    assert not row.snapshot_bound
    assert not row.local_book_reconstructed
    assert not row.m2_complete
    assert row.authority_reason == USDM_PUBLIC_DEPTH_M1_ONLY_REASON_V2
    canonical = json.loads(canonical_usdm_public_depth_m1_v2(row))
    assert canonical["m1_payload_sha256"] == row.m1_payload_sha256
    assert canonical["bids"][1] == ["117841.90", "0"]


def test_integer_and_empty_side_boundaries_are_accepted(tmp_path: Path) -> None:
    document = _fixture_document()
    data = cast(dict[str, object], document["data"])
    data.update({"E": 0, "T": 0, "U": 1, "u": 1, "pu": 0, "b": []})
    data["a"] = [["0.00000001", "0"]]

    row = _mint(tmp_path, _encoded(document)).parse()

    assert row.event_ms == 0
    assert row.transaction_time_ms == 0
    assert row.first_update_id == row.final_update_id == 1
    assert row.previous_final_update_id == 0
    assert row.bids == ()
    assert row.asks == ((Decimal("0.00000001"), Decimal("0")),)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("E", -1, "nonnegative int64"),
        ("T", True, "nonnegative int64"),
        ("U", 0, "positive"),
        ("u", 0, "positive"),
        ("pu", -1, "nonnegative int64"),
        ("st", True, "integer 1"),
        ("st", 2, "integer 1"),
    ],
)
def test_scalar_type_and_boundary_drift_fails_closed(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    document = _fixture_document()
    data = cast(dict[str, object], document["data"])
    data[field] = value

    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match=message):
        _mint(tmp_path, _encoded(document)).parse()


def test_reversed_update_range_and_schema_identity_drift_fail_closed(
    tmp_path: Path,
) -> None:
    reversed_document = _fixture_document()
    reversed_data = cast(dict[str, object], reversed_document["data"])
    reversed_data["U"] = cast(int, reversed_data["u"]) + 1
    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match="range is reversed"):
        _mint(tmp_path / "reversed", _encoded(reversed_document)).parse()

    reversed_clock_document = _fixture_document()
    reversed_clock_data = cast(dict[str, object], reversed_clock_document["data"])
    reversed_clock_data["E"] = cast(int, reversed_clock_data["T"]) - 1
    with pytest.raises(
        UsdmPublicDepthM1ContractErrorV2,
        match="event time precedes transaction time",
    ):
        _mint(
            tmp_path / "reversed-clock",
            _encoded(reversed_clock_document),
        ).parse()

    for index, mutation in enumerate(
        (
            lambda document, data: data.__setitem__("s", "ETHUSDT"),
            lambda document, data: data.__setitem__("ps", "ETHUSDT"),
            lambda document, data: data.__setitem__("extra", 1),
            lambda document, data: data.__delitem__("T"),
            lambda document, data: document.__setitem__("extra", 1),
            lambda document, data: document.__setitem__(
                "stream", "btcusdt@depth@250ms"
            ),
        )
    ):
        document = _fixture_document()
        data = cast(dict[str, object], document["data"])
        mutation(document, data)
        with pytest.raises(UsdmPublicDepthM1ContractErrorV2):
            _mint(tmp_path / f"identity-{index}", _encoded(document)).parse()


@pytest.mark.parametrize(
    ("level", "message"),
    [
        (["117842.30"], "exact JSON pair"),
        (["117842.30", "1", "extra"], "exact JSON pair"),
        ([117842, "1"], "decimal string"),
        (["0", "1"], "positive"),
        (["-1", "1"], "decimal string"),
        (["1e3", "1"], "decimal string"),
        (["117842.30", -1], "decimal string"),
        (["117842.30", "-1"], "decimal string"),
    ],
)
def test_level_shape_and_decimal_drift_fails_closed(
    tmp_path: Path,
    level: list[object],
    message: str,
) -> None:
    document = _fixture_document()
    data = cast(dict[str, object], document["data"])
    data["b"] = [level]

    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match=message):
        _mint(tmp_path, _encoded(document)).parse()


def test_level_count_bound_accepts_exact_limit_and_rejects_successor(
    tmp_path: Path,
) -> None:
    document = _fixture_document()
    data = cast(dict[str, object], document["data"])
    data["b"] = [
        [str(index + 1), "0"]
        for index in range(USDM_PUBLIC_DEPTH_M1_MAX_LEVELS_PER_SIDE_V2)
    ]
    data["a"] = []
    row = _mint(tmp_path / "exact", _encoded(document)).parse()
    assert len(row.bids) == USDM_PUBLIC_DEPTH_M1_MAX_LEVELS_PER_SIDE_V2

    oversized = copy.deepcopy(document)
    oversized_data = cast(dict[str, object], oversized["data"])
    bids = cast(list[list[str]], oversized_data["b"])
    bids.append(["10001", "0"])
    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match="fixed level bound"):
        _mint(tmp_path / "successor", _encoded(oversized)).parse()


def test_encoded_frame_bound_duplicate_keys_and_floats_fail_closed(
    tmp_path: Path,
) -> None:
    above_limit = b"{" + b" " * USDM_PUBLIC_DEPTH_M1_MAX_FRAME_BYTES_V2
    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match="byte limit"):
        _mint(tmp_path / "size", above_limit).parse()

    duplicate = _fixture_bytes().replace(
        b'"E":1784455200123',
        b'"E":1784455200123,"E":1784455200124',
    )
    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match="repeats"):
        _mint(tmp_path / "duplicate", duplicate).parse()

    numeric_literal = _fixture_bytes().replace(b'"117842.30"', b"117842.30")
    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match="numeric literal"):
        _mint(tmp_path / "float", numeric_literal).parse()


def test_factory_canonical_and_lineage_tamper_fail_closed(tmp_path: Path) -> None:
    minted = _mint(tmp_path / "valid", _fixture_bytes())
    row = minted.parse()

    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match="factory"):
        replace(row)

    object.__setattr__(row, "previous_final_update_id", row.previous_final_update_id + 1)
    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match="canonical evidence"):
        canonical_usdm_public_depth_m1_v2(row)

    wrong_plans = _plans(("ETHUSDT",))
    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match="trusted WAL authority"):
        minted.parse(plans=wrong_plans)


def test_raw_route_symbol_and_plan_identity_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(RawRecordMembershipErrorV2, match="route"):
        _mint(tmp_path / "route", _fixture_bytes(), route_id="usdm_market").parse()
    with pytest.raises(RawRecordMembershipErrorV2, match="symbol"):
        _mint(tmp_path / "symbol", _fixture_bytes(), record_symbol="BTCUSDT").parse()
    with pytest.raises(UsdmPublicDepthM1ContractErrorV2, match="plan_id"):
        _mint(tmp_path / "plan", _fixture_bytes(), plan_id="wrong-plan").parse()
