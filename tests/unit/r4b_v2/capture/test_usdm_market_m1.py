from __future__ import annotations

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
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    build_provisional_promoting_capture_plans_v2,
    provisional_promoting_plan_sha256_v2,
)
from signalbot.r4b_v2.capture.usdm_market_m1 import (
    USDM_MARKET_M1_ONLY_REASON_V2,
    USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
    UsdmAggTradeM1V2,
    UsdmKline5mM1V2,
    UsdmMarketM1ContractErrorV2,
    UsdmMarketM1V2,
    UsdmMarkPrice1sM1V2,
    canonical_usdm_market_m1_v2,
    parse_verified_usdm_market_m1_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

PROTOCOL_SHA256 = "a" * 64
MAXIMUM_BYTES = 8 * 1024 * 1024
RESERVE_BYTES = 1024
BAR_OPEN_MS = 1_710_000_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + 299_999
MARK_PRICE_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "fixtures"
    / "r4b_v2"
    / "capture"
    / "usdm_mark_price_update.json"
)


def _plans(
    symbols: tuple[str, ...] = ("BTCUSDT",),
) -> tuple[ProvisionalPromotingPlanV2, ...]:
    return build_provisional_promoting_capture_plans_v2(symbols)


def _market_plan(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> ProvisionalPromotingCapturePlanV2:
    [plan] = [
        item
        for item in plans
        if isinstance(item, ProvisionalPromotingCapturePlanV2)
        and item.route_id == "usdm_market"
    ]
    return plan


def _authority(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> WalAuthorityV2:
    return WalAuthorityV2(
        attempt_id="attempt-usdm-m1",
        protocol_sha256=PROTOCOL_SHA256,
        plan_sha256=provisional_promoting_plan_sha256_v2(plans),
        source_manifest_sha256="b" * 64,
        schema_sha256="c" * 64,
        runtime_manifest_sha256="d" * 64,
    )


def _signer() -> Ed25519BlockSignerV2:
    return Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="usdm-m1-test-key",
        private_key_bytes=b"\x29" * 32,
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


def _agg_data() -> dict[str, object]:
    return {
        "e": "aggTrade",
        "E": BAR_CLOSE_MS + 1_000,
        "a": 7_001,
        "s": "BTCUSDT",
        "p": "65000.25",
        "q": "2.500",
        "nq": "2.125",
        "f": 90_001,
        "l": 90_003,
        "T": BAR_CLOSE_MS + 900,
        "m": False,
        "st": 1,
    }


def _kline_data(*, closed: bool = True) -> dict[str, object]:
    return {
        "e": "kline",
        "E": BAR_CLOSE_MS + 2,
        "s": "BTCUSDT",
        "k": {
            "t": BAR_OPEN_MS,
            "T": BAR_CLOSE_MS,
            "s": "BTCUSDT",
            "i": "5m",
            "f": 80_001,
            "L": 80_010,
            "o": "64000.00",
            "c": "65000.00",
            "h": "65100.00",
            "l": "63900.00",
            "v": "20.0",
            "n": 10,
            "x": closed,
            "q": "1290000.0",
            "V": "12.0",
            "Q": "775000.0",
            "B": "0",
        },
    }


def _frame(stream: str, data: dict[str, object]) -> bytes:
    return json.dumps(
        {"stream": stream, "data": data},
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _mark_price_fixture() -> bytes:
    return MARK_PRICE_FIXTURE.read_bytes().rstrip(b"\r\n")


def _mark_price_document() -> dict[str, object]:
    return cast(dict[str, object], json.loads(_mark_price_fixture()))


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
        plans: tuple[ProvisionalPromotingPlanV2, ...] | None = None,
    ) -> UsdmMarketM1V2:
        return parse_verified_usdm_market_m1_v2(
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
    route_id: str = "usdm_market",
    record_symbol: str | None = None,
    plan_id: str | None = None,
) -> _Minted:
    frozen_plans = _plans() if plans is None else plans
    authority = _authority(frozen_plans)
    market_plan = _market_plan(frozen_plans)
    record = RawRecordV2.from_payload(
        session_id="session-usdm-m1",
        plan_id=market_plan.name if plan_id is None else plan_id,
        protocol_hash=PROTOCOL_SHA256,
        transport=TransportV2.WEBSOCKET,
        venue=VenueV2.USDM_FUTURES,
        route_id=route_id,
        symbol=record_symbol,
        connection_id="connection-usdm-m1",
        generation=1,
        frame_seq=1,
        ingest_seq=1,
        receipt_wall_ms=BAR_CLOSE_MS + 2_000,
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
        stream_group_id="usdm-market-combined-group",
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
        failure_domain_id="usdm-m1-ledger-device",
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
    return _Minted(frozen_plans, writer, ledger, manifest, leaf)


def test_exact_current_agg_trade_parses_nq_and_usdm_stream_type(
    tmp_path: Path,
) -> None:
    minted = _mint(tmp_path, _frame("btcusdt@aggTrade", _agg_data()))
    row = minted.parse()

    assert isinstance(row, UsdmAggTradeM1V2)
    assert row.symbol == "BTCUSDT"
    assert row.quantity == Decimal("2.500")
    assert row.normal_quantity == Decimal("2.125")
    assert row.buyer_maker is False
    assert row.stream_type == 1
    assert row.parser_contract_sha256 == USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2
    assert row.parser_bound
    assert row.live_reverification_required
    assert not row.current_authority_claimed
    assert not row.cursor_complete
    assert not row.causal_inputs_complete
    assert row.authority_reason == USDM_MARKET_M1_ONLY_REASON_V2
    encoded = canonical_usdm_market_m1_v2(row)
    assert json.loads(encoded)["m1_payload_sha256"] == row.m1_payload_sha256


def test_closed_kline_retains_exchange_event_after_bar_close(
    tmp_path: Path,
) -> None:
    row = _mint(
        tmp_path,
        _frame("btcusdt@kline_5m", _kline_data()),
    ).parse()

    assert isinstance(row, UsdmKline5mM1V2)
    assert row.closed
    assert row.event_ms == row.bar_close_ms + 2
    assert row.bar_close_ms == row.bar_open_ms + 299_999
    assert row.high == Decimal("65100.00")
    assert row.taker_buy_base_volume == Decimal("12.0")


def test_unclosed_kline_is_typed_but_never_claims_causal_completeness(
    tmp_path: Path,
) -> None:
    row = _mint(
        tmp_path,
        _frame("btcusdt@kline_5m", _kline_data(closed=False)),
    ).parse()

    assert isinstance(row, UsdmKline5mM1V2)
    assert not row.closed
    assert row.parser_bound
    assert not row.current_authority_claimed
    assert not row.cursor_complete
    assert not row.causal_inputs_complete


def test_recorded_current_mark_price_1s_preserves_event_and_receipt_clocks(
    tmp_path: Path,
) -> None:
    minted = _mint(tmp_path, _mark_price_fixture())
    row = minted.parse()

    assert isinstance(row, UsdmMarkPrice1sM1V2)
    assert row.symbol == "BTCUSDT"
    assert row.stream == "btcusdt@markPrice@1s"
    assert row.event_ms == 1_784_455_200_123
    assert row.receipt_wall_ms == BAR_CLOSE_MS + 2_000
    assert row.receipt_monotonic_ns == 10_000_000
    assert row.mark_price == Decimal("117842.37000000")
    assert row.mark_moving_average_price == Decimal("117836.51428571")
    assert row.estimated_settlement_price == Decimal("0.00000000")
    assert row.index_price == Decimal("117881.62956522")
    assert row.funding_rate == Decimal("0.00009367")
    assert row.next_funding_time_ms == 1_784_476_800_000
    assert not row.next_funding_time_is_observation_time
    assert row.stream_type == 1
    assert row.parser_bound
    assert not row.cursor_complete
    assert not row.causal_inputs_complete

    encoded = json.loads(canonical_usdm_market_m1_v2(row))
    assert encoded["event_ms"] == row.event_ms
    assert encoded["receipt_wall_ms"] == row.receipt_wall_ms
    assert encoded["next_funding_time_ms"] == row.next_funding_time_ms
    assert encoded["next_funding_time_is_observation_time"] is False


def test_mark_price_signed_rate_and_nonnegative_time_boundaries_parse(
    tmp_path: Path,
) -> None:
    document = _mark_price_document()
    data = cast(dict[str, object], document["data"])
    data["E"] = 0
    data["P"] = "0"
    data["r"] = "-0.00025000"
    data["T"] = 0
    payload = json.dumps(document, separators=(",", ":")).encode("utf-8")

    row = _mint(tmp_path, payload).parse()

    assert isinstance(row, UsdmMarkPrice1sM1V2)
    assert row.event_ms == 0
    assert row.estimated_settlement_price == Decimal("0")
    assert row.funding_rate == Decimal("-0.00025000")
    assert row.next_funding_time_ms == 0


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ("missing_ap", "schema is not exact"),
        ("extra_key", "schema is not exact"),
        ("wrong_event", "event type is not exact"),
        ("wrong_symbol", "payload symbol differs"),
        ("wrong_st", "st must be integer 1"),
        ("boolean_st", "st must be integer 1"),
        ("zero_mark", "p must be positive"),
        ("zero_average", "ap must be positive"),
        ("zero_index", "i must be positive"),
        ("negative_settlement", "nonnegative decimal string"),
        ("numeric_rate", "numeric literal"),
        ("exponent_rate", "signed decimal string"),
        ("negative_event_time", "nonnegative int64"),
        ("negative_funding_time", "nonnegative int64"),
        ("oversized_funding_time", "signed int64"),
    ],
)
def test_mark_price_current_schema_omission_and_boundaries_fail_closed(
    tmp_path: Path,
    mutation: str,
    match: str,
) -> None:
    document = _mark_price_document()
    data = cast(dict[str, object], document["data"])
    if mutation == "missing_ap":
        data.pop("ap")
    elif mutation == "extra_key":
        data["unexpected"] = "retained-only-in-M0"
    elif mutation == "wrong_event":
        data["e"] = "markPrice"
    elif mutation == "wrong_symbol":
        data["s"] = "ETHUSDT"
    elif mutation == "wrong_st":
        data["st"] = 2
    elif mutation == "boolean_st":
        data["st"] = True
    elif mutation == "zero_mark":
        data["p"] = "0"
    elif mutation == "zero_average":
        data["ap"] = "0"
    elif mutation == "zero_index":
        data["i"] = "0"
    elif mutation == "negative_settlement":
        data["P"] = "-0.01"
    elif mutation == "numeric_rate":
        data["r"] = -0.00025
    elif mutation == "exponent_rate":
        data["r"] = "1e-4"
    elif mutation == "negative_event_time":
        data["E"] = -1
    elif mutation == "negative_funding_time":
        data["T"] = -1
    elif mutation == "oversized_funding_time":
        data["T"] = 10**30
    else:  # pragma: no cover - parameter list owns the cases
        raise AssertionError(mutation)
    payload = json.dumps(document, separators=(",", ":")).encode("utf-8")

    with pytest.raises(UsdmMarketM1ContractErrorV2, match=match):
        _mint(tmp_path, payload).parse()


def test_mark_price_stream_suffix_and_factory_tamper_fail_closed(
    tmp_path: Path,
) -> None:
    document = _mark_price_document()
    document["stream"] = "btcusdt@markPrice"
    outside_plan = json.dumps(document, separators=(",", ":")).encode("utf-8")
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="outside the frozen"):
        _mint(tmp_path / "suffix", outside_plan).parse()

    minted = _mint(tmp_path / "factory", _mark_price_fixture())
    row = minted.parse()
    assert isinstance(row, UsdmMarkPrice1sM1V2)
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="parser factory"):
        replace(row)

    object.__setattr__(row, "next_funding_time_ms", row.next_funding_time_ms + 1)
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="canonical evidence"):
        canonical_usdm_market_m1_v2(row)

    invalid = minted.parse()
    assert isinstance(invalid, UsdmMarkPrice1sM1V2)
    object.__setattr__(invalid, "index_price", Decimal("0"))
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="index_price"):
        canonical_usdm_market_m1_v2(invalid)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ("missing_nq", "schema is not exact"),
        ("extra_data_key", "schema is not exact"),
        ("wrong_st", "st must be integer 1"),
        ("normal_above_total", "cannot exceed"),
        ("symbol_conflict", "payload symbol differs"),
        ("numeric_price", "numeric literal"),
        ("oversized_integer", "signed int64"),
    ],
)
def test_agg_trade_schema_and_local_invariants_fail_closed(
    tmp_path: Path,
    mutation: str,
    match: str,
) -> None:
    data = _agg_data()
    if mutation == "missing_nq":
        data.pop("nq")
    elif mutation == "extra_data_key":
        data["unexpected"] = "retained-only-in-M0"
    elif mutation == "wrong_st":
        data["st"] = 2
    elif mutation == "normal_above_total":
        data["nq"] = "2.501"
    elif mutation == "symbol_conflict":
        data["s"] = "ETHUSDT"
    elif mutation == "numeric_price":
        data["p"] = 65000.25
    elif mutation == "oversized_integer":
        data["a"] = 10**30
    else:  # pragma: no cover - parameter list owns the cases
        raise AssertionError(mutation)

    minted = _mint(tmp_path, _frame("btcusdt@aggTrade", data))
    with pytest.raises(UsdmMarketM1ContractErrorV2, match=match):
        minted.parse()


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ("missing_inner_key", "schema is not exact"),
        ("extra_inner_key", "schema is not exact"),
        ("reversed_ids", "ID bounds are reversed"),
        ("bad_alignment", "not interval-aligned"),
        ("bad_ohlc", "OHLC geometry"),
        ("taker_base_above_total", "exceeds total base"),
        ("taker_quote_above_total", "exceeds total quote"),
    ],
)
def test_kline_schema_geometry_and_volume_invariants_fail_closed(
    tmp_path: Path,
    mutation: str,
    match: str,
) -> None:
    data = _kline_data()
    inner = cast(dict[str, object], data["k"])
    if mutation == "missing_inner_key":
        inner.pop("B")
    elif mutation == "extra_inner_key":
        inner["unexpected"] = "retained-only-in-M0"
    elif mutation == "reversed_ids":
        inner["f"] = 80_011
    elif mutation == "bad_alignment":
        inner["t"] = BAR_OPEN_MS + 1
    elif mutation == "bad_ohlc":
        inner["h"] = "64999.99"
    elif mutation == "taker_base_above_total":
        inner["V"] = "20.001"
    elif mutation == "taker_quote_above_total":
        inner["Q"] = "1290000.1"
    else:  # pragma: no cover - parameter list owns the cases
        raise AssertionError(mutation)

    minted = _mint(tmp_path, _frame("btcusdt@kline_5m", data))
    with pytest.raises(UsdmMarketM1ContractErrorV2, match=match):
        minted.parse()


def test_nested_duplicate_kline_key_and_frame_size_boundary_fail_closed(
    tmp_path: Path,
) -> None:
    encoded = _frame("btcusdt@kline_5m", _kline_data())
    duplicate = encoded.replace(b'"B":"0"', b'"B":"0","B":"0"')
    assert duplicate != encoded
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="repeats"):
        _mint(tmp_path / "nested-duplicate", duplicate).parse()

    at_limit = b"{" + b" " * 16_383
    above_limit = b"{" + b" " * 16_384
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="whitespace"):
        _mint(tmp_path / "at-limit", at_limit).parse()
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="byte limit"):
        _mint(tmp_path / "above-limit", above_limit).parse()


def test_duplicate_and_extra_combined_wrapper_keys_fail_closed(tmp_path: Path) -> None:
    data = json.dumps(_agg_data(), separators=(",", ":")).encode("utf-8")
    duplicate = (
        b'{"stream":"btcusdt@aggTrade","stream":"btcusdt@aggTrade","data":'
        + data
        + b"}"
    )
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="repeats"):
        _mint(tmp_path / "duplicate", duplicate).parse()

    extra = json.dumps(
        {
            "stream": "btcusdt@aggTrade",
            "data": _agg_data(),
            "unexpected": True,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="schema is not exact"):
        _mint(tmp_path / "extra", extra).parse()


def test_combined_m1_rejects_predecoded_symbol_route_and_plan_drift(
    tmp_path: Path,
) -> None:
    payload = _frame("btcusdt@aggTrade", _agg_data())
    with pytest.raises(RawRecordMembershipErrorV2, match="trusted scope"):
        _mint(tmp_path / "symbol", payload, record_symbol="BTCUSDT").parse()
    with pytest.raises(RawRecordMembershipErrorV2, match="trusted scope"):
        _mint(tmp_path / "route", payload, route_id="other-market-route").parse()
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="plan_id differs"):
        _mint(tmp_path / "plan-id", payload, plan_id="other-plan").parse()

    minted = _mint(tmp_path / "plan-hash", payload)
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="frozen promoting plan"):
        minted.parse(plans=_plans(("BTCUSDT", "ETHUSDT")))


def test_factory_canonical_tamper_and_post_mint_void_fail_closed(
    tmp_path: Path,
) -> None:
    minted = _mint(tmp_path, _frame("btcusdt@aggTrade", _agg_data()))
    row = minted.parse()
    assert isinstance(row, UsdmAggTradeM1V2)
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="parser factory"):
        replace(row)

    object.__setattr__(row, "event_ms", row.event_ms + 1)
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="canonical evidence"):
        canonical_usdm_market_m1_v2(row)

    semantically_invalid = minted.parse()
    assert isinstance(semantically_invalid, UsdmAggTradeM1V2)
    object.__setattr__(
        semantically_invalid,
        "normal_quantity",
        semantically_invalid.quantity + Decimal("1"),
    )
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="cannot exceed"):
        canonical_usdm_market_m1_v2(semantically_invalid)

    wrong_stream_type = minted.parse()
    assert isinstance(wrong_stream_type, UsdmAggTradeM1V2)
    object.__setattr__(wrong_stream_type, "stream_type", True)
    with pytest.raises(UsdmMarketM1ContractErrorV2, match="st must equal 1"):
        canonical_usdm_market_m1_v2(wrong_stream_type)

    reference = attest_finalized_block_v2(minted.writer, minted.manifest)
    data_path = minted.writer.directory / minted.manifest.data_file
    original = data_path.read_bytes()
    data_path.write_bytes(original + b"post-mint-corruption")
    try:
        minted.ledger.append_void_for_finalized_block(
            reference,
            detector_component="usdm-m1-post-mint-auditor",
            detection_evidence_sha256="e" * 64,
        )
    finally:
        data_path.write_bytes(original)
    with pytest.raises(RawRecordMembershipErrorV2, match="VOID"):
        minted.parse()
