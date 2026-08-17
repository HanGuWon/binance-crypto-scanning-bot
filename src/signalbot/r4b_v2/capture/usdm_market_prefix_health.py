"""Current-storage parser-health proof for one retained USD-M market prefix.

This module deliberately proves less than upstream completeness.  It replays the
exact signed local prefix of a durably CLEAN session, live-reverifies membership
for every retained ``usdm_market`` row, and binds every successful strict-M1
parse into bounded rolling roots.  A bounded SOURCE_GAP remains evidence of an
unknown upstream interval; it is counted and never upgraded to losslessness.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import InitVar, asdict, dataclass, field, fields
from pathlib import Path
from typing import Final, Literal, cast

from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.blocks import (
    BlockManifestV2,
    GroupedBlockWriterV2,
    verify_grouped_blocks,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerIntegrityError,
    CaptureIntegrityLedgerV2,
    PersistedCaptureCleanClosureSealReceiptV2,
)
from signalbot.r4b_v2.capture.membership import (
    attest_raw_record_membership_v2,
    verify_raw_record_membership_leaf_v2,
)
from signalbot.r4b_v2.capture.models import TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import CaptureBatchPipelineV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_stream_census_sha256_v2,
    validate_provisional_promoting_capture_plans_v2,
)
from signalbot.r4b_v2.capture.session import (
    PersistedSessionClosureAuthorityV2,
    PersistedSessionStartAuthorityV2,
    assert_persisted_session_closure_authority_current_v2,
)
from signalbot.r4b_v2.capture.usdm_market_m1 import (
    USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
    UsdmAggTradeM1V2,
    UsdmKline5mM1V2,
    UsdmMarketM1ContractErrorV2,
    UsdmMarkPrice1sM1V2,
    parse_verified_usdm_market_m1_v2,
)
from signalbot.r4b_v2.capture.websocket_finality import (
    WebSocketRouteCursorClosureEntryV2,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FACTORY_TOKEN: Final = object()
_CERTIFICATE_SCHEMA: Final = "r4b_v2_retained_usdm_market_parser_health_certificate_v1"
_NONCERTIFYING_SCHEMA: Final = "r4b_v2_retained_usdm_market_parser_health_noncertifying_v1"
_STREAM_SCAN_SCHEMA: Final = "r4b_v2_retained_usdm_market_stream_scan_v1"
_CERTIFICATE_DOMAIN: Final = b"R4B_V2_RETAINED_USDM_MARKET_PARSER_HEALTH_CERTIFICATE\0"
_NONCERTIFYING_DOMAIN: Final = b"R4B_V2_RETAINED_USDM_MARKET_PARSER_HEALTH_NONCERTIFYING\0"
_PREFIX_DOMAIN: Final = b"R4B_V2_RETAINED_SIGNED_PREFIX_SEQUENCE\0"
_SUCCESS_DOMAIN: Final = b"R4B_V2_USDM_MARKET_M1_SUCCESS_SEQUENCE\0"
_FAILURE_DOMAIN: Final = b"R4B_V2_USDM_MARKET_M1_FAILURE_SEQUENCE\0"
_STREAM_DOMAIN: Final = b"R4B_V2_USDM_MARKET_M1_STREAM_SEQUENCE\0"
_MANIFEST_DOMAIN: Final = b"R4B_V2_GROUPED_BLOCK_MANIFEST_SEQUENCE\0"
_GAP_DOMAIN: Final = b"R4B_V2_USDM_MARKET_BOUNDED_GAP_SEQUENCE\0"

_ISSUE_ORDER: Final = (
    "STRICT_M1_PARSE_FAILURE",
    "UNKNOWN_OR_UNPLANNED_STREAM",
    "LOCAL_CURSOR_CONFLICT",
    "MARKET_RECORD_AFTER_FINALIZED_CURSOR",
    "TERMINAL_CURSOR_MISMATCH",
    "MISSING_PLANNED_STREAM",
)

type MarketStreamTypeV2 = Literal["AGG_TRADE", "KLINE_5M", "MARK_PRICE_1S"]


class RetainedUsdmMarketParserHealthErrorV2(RuntimeError):
    """Raised when exact certification inputs or current storage differ."""


@dataclass(frozen=True, slots=True)
class RetainedUsdmMarketStreamScanV2:
    """Bounded per-logical-stream census; row hashes remain in a rolling root."""

    stream: str
    stream_type: MarketStreamTypeV2
    successful_parse_count: int
    first_ingest_seq: int | None
    last_ingest_seq: int | None
    successful_parse_sequence_sha256: str
    schema_version: str = _STREAM_SCAN_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != _STREAM_SCAN_SCHEMA:
            raise ValueError("unsupported retained market stream-scan schema")
        if _stream_type(self.stream) != self.stream_type:
            raise ValueError("retained market stream type differs from its logical stream")
        _require_nonnegative_int(self.successful_parse_count, "successful_parse_count")
        _require_sha256(
            self.successful_parse_sequence_sha256,
            "successful_parse_sequence_sha256",
        )
        if self.successful_parse_count == 0:
            if self.first_ingest_seq is not None or self.last_ingest_seq is not None:
                raise ValueError("empty stream scan cannot expose ingest bounds")
            return
        _require_positive_int(self.first_ingest_seq, "first_ingest_seq")
        _require_positive_int(self.last_ingest_seq, "last_ingest_seq")
        assert self.first_ingest_seq is not None
        assert self.last_ingest_seq is not None
        if self.first_ingest_seq > self.last_ingest_seq:
            raise ValueError("stream scan ingest bounds are reversed")


@dataclass(frozen=True, slots=True)
class RetainedUsdmMarketParserHealthCertificateV2:
    """Factory-sealed proof that every retained market row strictly parsed."""

    session_id: str
    process_boot_id: str
    plan_bundle_sha256: str
    plan_id: str
    route_id: Literal["usdm_market"]
    stream_census_sha256: str
    stream_count: int
    parser_contract_sha256: str
    session_start_manifest_sha256: str
    session_closure_manifest_sha256: str
    websocket_route_cursors_sha256: str
    route_cursor_closure_entry_sha256: str
    stop_receipt_sha256: str
    finalized_route_cursor_sha256: str
    finality_receipt_sha256: str
    finality_authority_sha256: str
    finality_exact_prefix_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    grouped_block_root_binding_sha256: str
    grouped_block_manifest_count: int
    grouped_block_manifest_sequence_sha256: str
    scanned_prefix_first_ingest_seq: Literal[1]
    scanned_prefix_last_ingest_seq: int
    scanned_prefix_record_count: int
    scanned_prefix_sequence_sha256: str
    market_first_ingest_seq: int
    market_last_ingest_seq: int
    market_record_count: int
    terminal_connection_id: str
    terminal_generation: int
    terminal_frame_seq: int
    agg_trade_success_count: int
    kline_5m_success_count: int
    mark_price_1s_success_count: int
    successful_parse_count: int
    successful_parse_sequence_sha256: str
    stream_scans: tuple[RetainedUsdmMarketStreamScanV2, ...]
    bounded_market_source_gap_count: int
    bounded_market_source_gap_sequence_sha256: str
    ledger_clean_closure_receipt_sha256: str
    ledger_clean_closure_seal_sha256: str
    _factory_token: InitVar[object | None] = None
    certificate_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_CERTIFICATE_SCHEMA)
    current_storage_reverified: Literal[True] = field(init=False, default=True)
    retained_market_prefix_parser_health_certified: Literal[True] = field(
        init=False,
        default=True,
    )
    parse_failure_count: Literal[0] = field(init=False, default=0)
    unknown_stream_count: Literal[0] = field(init=False, default=0)
    local_cursor_conflict_count: Literal[0] = field(init=False, default=0)
    missing_planned_stream_count: Literal[0] = field(init=False, default=0)
    void_count: Literal[0] = field(init=False, default=0)
    unbounded_market_source_gap_count: Literal[0] = field(init=False, default=0)
    upstream_message_losslessness_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    required_source_completeness_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    oi_schedule_completeness_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    oi_freshness_claimed: Literal[False] = field(init=False, default=False)
    m2_certified: Literal[False] = field(init=False, default=False)
    strategy_ready: Literal[False] = field(init=False, default=False)
    pnl_or_order_authority: Literal[False] = field(init=False, default=False)
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise TypeError("retained market parser-health certificates are factory-sealed")
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        _validate_common_result(self)
        if self.schema_version != _CERTIFICATE_SCHEMA:
            raise ValueError("unsupported retained market parser-health certificate schema")
        if self.successful_parse_count != self.market_record_count:
            raise ValueError("certified market prefix has an unparsed retained row")
        if any(scan.successful_parse_count < 1 for scan in self.stream_scans):
            raise ValueError("certified market prefix must observe every planned stream")
        _require_literal_nonclaims(self)
        object.__setattr__(
            self,
            "certificate_sha256",
            _result_hash(_CERTIFICATE_DOMAIN, self, "certificate_sha256"),
        )


@dataclass(frozen=True, slots=True)
class RetainedUsdmMarketParserHealthNoncertifyingV2:
    """Factory-sealed, current-storage result that makes no health claim."""

    session_id: str
    process_boot_id: str
    plan_bundle_sha256: str
    plan_id: str
    route_id: Literal["usdm_market"]
    stream_census_sha256: str
    stream_count: int
    parser_contract_sha256: str
    session_start_manifest_sha256: str
    session_closure_manifest_sha256: str
    websocket_route_cursors_sha256: str
    route_cursor_closure_entry_sha256: str
    stop_receipt_sha256: str
    finalized_route_cursor_sha256: str
    finality_receipt_sha256: str
    finality_authority_sha256: str
    finality_exact_prefix_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    grouped_block_root_binding_sha256: str
    grouped_block_manifest_count: int
    grouped_block_manifest_sequence_sha256: str
    scanned_prefix_first_ingest_seq: Literal[1]
    scanned_prefix_last_ingest_seq: int
    scanned_prefix_record_count: int
    scanned_prefix_sequence_sha256: str
    market_first_ingest_seq: int
    market_last_ingest_seq: int
    market_record_count: int
    terminal_connection_id: str
    terminal_generation: int
    terminal_frame_seq: int
    agg_trade_success_count: int
    kline_5m_success_count: int
    mark_price_1s_success_count: int
    successful_parse_count: int
    successful_parse_sequence_sha256: str
    stream_scans: tuple[RetainedUsdmMarketStreamScanV2, ...]
    bounded_market_source_gap_count: int
    bounded_market_source_gap_sequence_sha256: str
    ledger_clean_closure_receipt_sha256: str
    ledger_clean_closure_seal_sha256: str
    parse_failure_count: int
    unknown_stream_count: int
    local_cursor_conflict_count: int
    missing_planned_stream_count: int
    issue_codes: tuple[str, ...]
    first_issue_ingest_seq: int | None
    failure_sequence_sha256: str
    _factory_token: InitVar[object | None] = None
    result_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_NONCERTIFYING_SCHEMA)
    current_storage_reverified: Literal[True] = field(init=False, default=True)
    retained_market_prefix_parser_health_certified: Literal[False] = field(
        init=False,
        default=False,
    )
    void_count: Literal[0] = field(init=False, default=0)
    unbounded_market_source_gap_count: Literal[0] = field(init=False, default=0)
    upstream_message_losslessness_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    required_source_completeness_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    oi_schedule_completeness_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    oi_freshness_claimed: Literal[False] = field(init=False, default=False)
    m2_certified: Literal[False] = field(init=False, default=False)
    strategy_ready: Literal[False] = field(init=False, default=False)
    pnl_or_order_authority: Literal[False] = field(init=False, default=False)
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise TypeError("noncertifying retained market results are factory-sealed")
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        _validate_common_result(self)
        if self.schema_version != _NONCERTIFYING_SCHEMA:
            raise ValueError("unsupported noncertifying retained market result schema")
        for value, name in (
            (self.parse_failure_count, "parse_failure_count"),
            (self.unknown_stream_count, "unknown_stream_count"),
            (self.local_cursor_conflict_count, "local_cursor_conflict_count"),
            (self.missing_planned_stream_count, "missing_planned_stream_count"),
        ):
            _require_nonnegative_int(value, name)
        if self.unknown_stream_count > self.parse_failure_count:
            raise ValueError("unknown stream count exceeds strict parse failures")
        if self.successful_parse_count + self.parse_failure_count != self.market_record_count:
            raise ValueError("noncertifying market parse census differs from retained rows")
        if not self.issue_codes:
            raise ValueError("noncertifying market result requires at least one issue")
        if self.issue_codes != tuple(code for code in _ISSUE_ORDER if code in self.issue_codes):
            raise ValueError("noncertifying issue codes are unknown, duplicate, or out of order")
        if self.first_issue_ingest_seq is not None:
            _require_positive_int(self.first_issue_ingest_seq, "first_issue_ingest_seq")
        _require_sha256(self.failure_sequence_sha256, "failure_sequence_sha256")
        _require_literal_nonclaims(self)
        object.__setattr__(
            self,
            "result_sha256",
            _result_hash(_NONCERTIFYING_DOMAIN, self, "result_sha256"),
        )


type RetainedUsdmMarketParserHealthResultV2 = (
    RetainedUsdmMarketParserHealthCertificateV2
    | RetainedUsdmMarketParserHealthNoncertifyingV2
)


@dataclass(slots=True)
class _MutableStreamScan:
    stream: str
    stream_type: MarketStreamTypeV2
    count: int = 0
    first_ingest_seq: int | None = None
    last_ingest_seq: int | None = None
    sequence: object = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.sequence = hashlib.sha256(_STREAM_DOMAIN + self.stream.encode("utf-8") + b"\0")

    def add(self, ingest_seq: int, m0_leaf_sha256: str, m1_payload_sha256: str) -> None:
        hasher = cast("hashlib._Hash", self.sequence)
        hasher.update(
            canonical_json_line(
                {
                    "ingest_seq": ingest_seq,
                    "m0_leaf_sha256": m0_leaf_sha256,
                    "m1_payload_sha256": m1_payload_sha256,
                    "stream": self.stream,
                }
            )
        )
        self.count += 1
        if self.first_ingest_seq is None:
            self.first_ingest_seq = ingest_seq
        self.last_ingest_seq = ingest_seq

    def freeze(self) -> RetainedUsdmMarketStreamScanV2:
        hasher = cast("hashlib._Hash", self.sequence)
        return RetainedUsdmMarketStreamScanV2(
            stream=self.stream,
            stream_type=self.stream_type,
            successful_parse_count=self.count,
            first_ingest_seq=self.first_ingest_seq,
            last_ingest_seq=self.last_ingest_seq,
            successful_parse_sequence_sha256=hasher.hexdigest(),
        )


def certify_retained_usdm_market_parser_health_v2(
    closure_authority: PersistedSessionClosureAuthorityV2,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    integrity_ledger: CaptureIntegrityLedgerV2,
    block_writer: GroupedBlockWriterV2,
) -> RetainedUsdmMarketParserHealthResultV2:
    """Reverify and strictly parse the exact retained local market prefix.

    Storage/authority drift raises and produces no result.  Retained content that
    remains signed but fails the strict M1 or local cursor contract returns a
    typed noncertifying result.  State is bounded by the frozen plan stream
    census, ledger event cap, and a constant number of rolling SHA-256 states.
    """

    _validate_inputs(
        closure_authority,
        lease=lease,
        session_start_authority=session_start_authority,
        promoting_plans=promoting_plans,
        pipeline=pipeline,
        ledger_seal_receipt=ledger_seal_receipt,
        integrity_ledger=integrity_ledger,
        block_writer=block_writer,
    )
    manifest = closure_authority.manifest
    finality = manifest.finality_receipt
    market_plan = _market_plan(promoting_plans)
    market_entry = _market_cursor_entry(manifest.websocket_route_cursors)

    _assert_current_closure(
        closure_authority,
        lease=lease,
        session_start_authority=session_start_authority,
        promoting_plans=promoting_plans,
        pipeline=pipeline,
        ledger_seal_receipt=ledger_seal_receipt,
        integrity_ledger=integrity_ledger,
    )
    bounded_gap_count, bounded_gap_root = _reverify_market_gaps(
        ledger_seal_receipt,
        integrity_ledger=integrity_ledger,
        market_plan=market_plan,
    )

    manifests = verify_grouped_blocks(
        block_writer.directory,
        authority=block_writer.authority,
        policy=block_writer.policy,
        signing_authority=block_writer.signing_authority,
        stream_group_id=block_writer.stream_group_id,
        segment_id=block_writer.segment_id,
    )
    if not manifests or manifests[-1].last_ingest_seq != finality.fence_ingest_seq:
        raise RetainedUsdmMarketParserHealthErrorV2(
            "verified grouped-block tail differs from the persisted finality fence"
        )
    manifest_root = _manifest_sequence_sha256(manifests)
    scans = {
        stream: _MutableStreamScan(stream, _stream_type(stream))
        for stream in sorted(market_plan.streams)
    }
    prefix_root = hashlib.sha256(_PREFIX_DOMAIN)
    success_root = hashlib.sha256(_SUCCESS_DOMAIN)
    failure_root = hashlib.sha256(_FAILURE_DOMAIN)
    manifest_index = 0
    market_count = 0
    market_first = 0
    market_last = 0
    successful = 0
    parse_failures = 0
    unknown_streams = 0
    cursor_conflicts = 0
    after_cursor = 0
    first_issue_ingest: int | None = None
    suffix_counts: dict[MarketStreamTypeV2, int] = {
        "AGG_TRADE": 0,
        "KLINE_5M": 0,
        "MARK_PRICE_1S": 0,
    }
    previous_cursor: tuple[str, int, int, int, int, int] | None = None
    last_cursor: tuple[str, int, int, int, int, int] | None = None

    def note_issue(ingest_seq: int) -> None:
        nonlocal first_issue_ingest
        if first_issue_ingest is None:
            first_issue_ingest = ingest_seq

    def consume(ingest_seq: int, encoded_line: bytes) -> None:
        nonlocal manifest_index
        nonlocal market_count, market_first, market_last, successful
        nonlocal parse_failures, unknown_streams, cursor_conflicts, after_cursor
        nonlocal previous_cursor, last_cursor
        prefix_root.update(
            canonical_json_line(
                {
                    "ingest_seq": ingest_seq,
                    "record_jsonl_sha256": hashlib.sha256(encoded_line).hexdigest(),
                }
            )
        )
        document = json.loads(encoded_line)
        if not isinstance(document, dict) or document.get("route_id") != "usdm_market":
            return
        while ingest_seq > manifests[manifest_index].last_ingest_seq:
            manifest_index += 1
        block_manifest = manifests[manifest_index]
        certificate = attest_raw_record_membership_v2(
            block_writer,
            block_manifest,
            expected_record_jsonl=encoded_line,
            integrity_ledger=integrity_ledger,
        )
        leaf = verify_raw_record_membership_leaf_v2(
            certificate,
            block_directory=block_writer.directory,
            block_root_binding=block_writer.root_binding,
            authority=block_writer.authority,
            policy=block_writer.policy,
            signing_authority=block_writer.signing_authority,
            stream_group_id=block_writer.stream_group_id,
            segment_id=block_writer.segment_id,
            integrity_ledger=integrity_ledger,
            expected_transport=TransportV2.WEBSOCKET,
            expected_venue=VenueV2.USDM_FUTURES,
            expected_route_id="usdm_market",
            expected_symbol=None,
        )
        record = leaf.record
        if (
            record.session_id != manifest.session_id
            or record.plan_id != market_plan.name
            or record.protocol_hash
            != session_start_authority.manifest.wal_authority.protocol_sha256
            or record.frame_seq is None
        ):
            raise RetainedUsdmMarketParserHealthErrorV2(
                "signed market record differs from the persisted session authority"
            )
        market_count += 1
        market_first = ingest_seq if market_first == 0 else market_first
        market_last = ingest_seq
        assert record.frame_seq is not None
        cursor = (
            record.connection_id,
            record.generation,
            record.frame_seq,
            record.ingest_seq,
            record.receipt_wall_ms,
            record.receipt_monotonic_ns,
        )
        if not _cursor_transition_is_exact(previous_cursor, cursor, market_plan.name):
            cursor_conflicts += 1
            note_issue(ingest_seq)
        previous_cursor = cursor
        last_cursor = cursor
        if ingest_seq > market_entry.last_ingest_seq:
            after_cursor += 1
            note_issue(ingest_seq)
        try:
            row = parse_verified_usdm_market_m1_v2(
                leaf,
                promoting_plans=promoting_plans,
                block_directory=block_writer.directory,
                block_root_binding=block_writer.root_binding,
                authority=block_writer.authority,
                policy=block_writer.policy,
                signing_authority=block_writer.signing_authority,
                stream_group_id=block_writer.stream_group_id,
                segment_id=block_writer.segment_id,
                integrity_ledger=integrity_ledger,
            )
        except UsdmMarketM1ContractErrorV2:
            parse_failures += 1
            diagnostic_stream = _diagnostic_stream(record.payload_bytes())
            unknown = diagnostic_stream is not None and (
                diagnostic_stream not in scans or _stream_type_or_none(diagnostic_stream) is None
            )
            if unknown:
                unknown_streams += 1
            failure_root.update(
                canonical_json_line(
                    {
                        "category": (
                            "UNKNOWN_OR_UNPLANNED_STREAM"
                            if unknown
                            else "STRICT_M1_PARSE_FAILURE"
                        ),
                        "ingest_seq": ingest_seq,
                        "raw_payload_sha256": hashlib.sha256(
                            record.payload_bytes()
                        ).hexdigest(),
                    }
                )
            )
            note_issue(ingest_seq)
            return
        stream_type = _row_stream_type(row)
        scans[row.stream].add(ingest_seq, row.m0_leaf_sha256, row.m1_payload_sha256)
        suffix_counts[stream_type] += 1
        successful += 1
        success_root.update(
            canonical_json_line(
                {
                    "ingest_seq": ingest_seq,
                    "m0_leaf_sha256": row.m0_leaf_sha256,
                    "m1_payload_sha256": row.m1_payload_sha256,
                    "stream": row.stream,
                    "stream_type": stream_type,
                }
            )
        )

    delivered = block_writer.consume_committed_records(consume)
    if delivered != finality.fence_ingest_seq:
        raise RetainedUsdmMarketParserHealthErrorV2(
            "scanned grouped-block record count differs from persisted finality"
        )
    if last_cursor is None:
        raise RetainedUsdmMarketParserHealthErrorV2(
            "persisted market route cursor has no retained market record"
        )
    terminal_matches = last_cursor == (
        market_entry.connection_id,
        market_entry.generation,
        market_entry.last_frame_seq,
        market_entry.last_ingest_seq,
        market_entry.last_receipt_wall_ms,
        market_entry.last_receipt_monotonic_ns,
    )
    if not terminal_matches:
        note_issue(market_last)
    frozen_scans = tuple(scans[stream].freeze() for stream in sorted(scans))
    missing_streams = sum(scan.successful_parse_count == 0 for scan in frozen_scans)

    _assert_current_closure(
        closure_authority,
        lease=lease,
        session_start_authority=session_start_authority,
        promoting_plans=promoting_plans,
        pipeline=pipeline,
        ledger_seal_receipt=ledger_seal_receipt,
        integrity_ledger=integrity_ledger,
    )
    common = dict(
        session_id=manifest.session_id,
        process_boot_id=manifest.process_boot_id,
        plan_bundle_sha256=manifest.plan_bundle_sha256,
        plan_id=market_plan.name,
        route_id="usdm_market",
        stream_census_sha256=provisional_promoting_stream_census_sha256_v2(market_plan),
        stream_count=len(market_plan.streams),
        parser_contract_sha256=USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
        session_start_manifest_sha256=session_start_authority.manifest_sha256,
        session_closure_manifest_sha256=closure_authority.manifest_sha256,
        websocket_route_cursors_sha256=cast(str, manifest.websocket_route_cursors_sha256),
        route_cursor_closure_entry_sha256=market_entry.sha256,
        stop_receipt_sha256=market_entry.stop_receipt_sha256,
        finalized_route_cursor_sha256=market_entry.finalized_route_cursor_sha256,
        finality_receipt_sha256=manifest.finality_receipt_sha256,
        finality_authority_sha256=finality.authority_sha256,
        finality_exact_prefix_sha256=finality.exact_prefix_sha256,
        finality_prefix_proof_sha256=finality.prefix_proof_sha256,
        finality_tail_ingest_seq=finality.fence_ingest_seq,
        grouped_block_root_binding_sha256=(
            session_start_authority.manifest.storage_roots[2].root_binding_sha256
        ),
        grouped_block_manifest_count=len(manifests),
        grouped_block_manifest_sequence_sha256=manifest_root,
        scanned_prefix_first_ingest_seq=1,
        scanned_prefix_last_ingest_seq=delivered,
        scanned_prefix_record_count=delivered,
        scanned_prefix_sequence_sha256=prefix_root.hexdigest(),
        market_first_ingest_seq=market_first,
        market_last_ingest_seq=market_last,
        market_record_count=market_count,
        terminal_connection_id=last_cursor[0],
        terminal_generation=last_cursor[1],
        terminal_frame_seq=last_cursor[2],
        agg_trade_success_count=suffix_counts["AGG_TRADE"],
        kline_5m_success_count=suffix_counts["KLINE_5M"],
        mark_price_1s_success_count=suffix_counts["MARK_PRICE_1S"],
        successful_parse_count=successful,
        successful_parse_sequence_sha256=success_root.hexdigest(),
        stream_scans=frozen_scans,
        bounded_market_source_gap_count=bounded_gap_count,
        bounded_market_source_gap_sequence_sha256=bounded_gap_root,
        ledger_clean_closure_receipt_sha256=ledger_seal_receipt.sha256,
        ledger_clean_closure_seal_sha256=ledger_seal_receipt.seal_sha256,
    )
    issue_flags = {
        "STRICT_M1_PARSE_FAILURE": parse_failures > unknown_streams,
        "UNKNOWN_OR_UNPLANNED_STREAM": unknown_streams > 0,
        "LOCAL_CURSOR_CONFLICT": cursor_conflicts > 0,
        "MARKET_RECORD_AFTER_FINALIZED_CURSOR": after_cursor > 0,
        "TERMINAL_CURSOR_MISMATCH": not terminal_matches,
        "MISSING_PLANNED_STREAM": missing_streams > 0,
    }
    issue_codes = tuple(code for code in _ISSUE_ORDER if issue_flags[code])
    if issue_codes:
        return RetainedUsdmMarketParserHealthNoncertifyingV2(
            **common,
            parse_failure_count=parse_failures,
            unknown_stream_count=unknown_streams,
            local_cursor_conflict_count=cursor_conflicts,
            missing_planned_stream_count=missing_streams,
            issue_codes=issue_codes,
            first_issue_ingest_seq=first_issue_ingest,
            failure_sequence_sha256=failure_root.hexdigest(),
            _factory_token=_FACTORY_TOKEN,
        )
    return RetainedUsdmMarketParserHealthCertificateV2(
        **common,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_retained_usdm_market_parser_health_result_v2(
    result: RetainedUsdmMarketParserHealthResultV2,
) -> bytes:
    """Serialize one live-valid factory result canonically."""

    if type(result) is RetainedUsdmMarketParserHealthCertificateV2:
        if result._factory_seal is not _FACTORY_TOKEN:
            raise RetainedUsdmMarketParserHealthErrorV2("certificate factory seal differs")
        _validate_common_result(result)
        _require_literal_nonclaims(result)
        if (
            result.successful_parse_count != result.market_record_count
            or any(scan.successful_parse_count < 1 for scan in result.stream_scans)
        ):
            raise RetainedUsdmMarketParserHealthErrorV2(
                "certificate no longer proves a fully parsed planned stream census"
            )
        expected = _result_hash(_CERTIFICATE_DOMAIN, result, "certificate_sha256")
        if result.certificate_sha256 != expected:
            raise RetainedUsdmMarketParserHealthErrorV2("certificate hash differs")
    elif type(result) is RetainedUsdmMarketParserHealthNoncertifyingV2:
        if result._factory_seal is not _FACTORY_TOKEN:
            raise RetainedUsdmMarketParserHealthErrorV2("result factory seal differs")
        _validate_common_result(result)
        _require_literal_nonclaims(result)
        expected = _result_hash(_NONCERTIFYING_DOMAIN, result, "result_sha256")
        if result.result_sha256 != expected:
            raise RetainedUsdmMarketParserHealthErrorV2("result hash differs")
    else:
        raise TypeError("result must be a retained market parser-health result")
    return canonical_json_line(_result_document(result))


def _validate_inputs(
    closure_authority: PersistedSessionClosureAuthorityV2,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    integrity_ledger: CaptureIntegrityLedgerV2,
    block_writer: GroupedBlockWriterV2,
) -> None:
    if type(closure_authority) is not PersistedSessionClosureAuthorityV2:
        raise TypeError("closure_authority must be exact")
    if type(lease) is not WriterLease:
        raise TypeError("lease must be exact")
    if type(session_start_authority) is not PersistedSessionStartAuthorityV2:
        raise TypeError("session_start_authority must be exact")
    if type(promoting_plans) is not tuple:
        raise TypeError("promoting_plans must be an exact tuple")
    validate_provisional_promoting_capture_plans_v2(promoting_plans)
    if type(pipeline) is not CaptureBatchPipelineV2:
        raise TypeError("pipeline must be exact")
    if type(ledger_seal_receipt) is not PersistedCaptureCleanClosureSealReceiptV2:
        raise TypeError("ledger_seal_receipt must be exact")
    if type(integrity_ledger) is not CaptureIntegrityLedgerV2:
        raise TypeError("integrity_ledger must be exact")
    if type(block_writer) is not GroupedBlockWriterV2:
        raise TypeError("block_writer must be exact")
    start = session_start_authority.manifest
    block_root = start.storage_roots[2]
    if (
        block_writer.authority != start.wal_authority
        or block_writer.policy != start.block_policy
        or block_writer.signing_authority != start.block_signing_authority
        or block_writer.stream_group_id != start.stream_group_id
        or block_writer.segment_id != start.segment_id
        or block_writer.root_binding != block_root.root_binding
        or Path(block_writer.directory).resolve(strict=True)
        != Path(block_root.canonical_path).resolve(strict=True)
        or provisional_promoting_plan_sha256_v2(promoting_plans)
        != start.wal_authority.plan_sha256
    ):
        raise RetainedUsdmMarketParserHealthErrorV2(
            "grouped-block reader or plan differs from session-start authority"
        )


def _assert_current_closure(
    authority: PersistedSessionClosureAuthorityV2,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    integrity_ledger: CaptureIntegrityLedgerV2,
) -> None:
    assert_persisted_session_closure_authority_current_v2(
        authority,
        lease=lease,
        session_start_authority=session_start_authority,
        promoting_plans=promoting_plans,
        finality_receipt=authority.manifest.finality_receipt,
        pipeline=pipeline,
        ledger_seal_receipt=ledger_seal_receipt,
        ledger=integrity_ledger,
    )


def _market_plan(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> ProvisionalPromotingCapturePlanV2:
    matches = tuple(
        plan
        for plan in plans
        if type(plan) is ProvisionalPromotingCapturePlanV2
        and plan.route_id == "usdm_market"
    )
    if len(matches) != 1:
        raise RetainedUsdmMarketParserHealthErrorV2(
            "promoting plan lacks one exact USD-M market owner"
        )
    return matches[0]


def _market_cursor_entry(
    entries: tuple[WebSocketRouteCursorClosureEntryV2, ...],
) -> WebSocketRouteCursorClosureEntryV2:
    if len(entries) != 2 or entries[0].route_id != "usdm_market":
        raise RetainedUsdmMarketParserHealthErrorV2(
            "session closure lacks the canonical persisted market/public cursor pair"
        )
    return entries[0]


def _reverify_market_gaps(
    receipt: PersistedCaptureCleanClosureSealReceiptV2,
    *,
    integrity_ledger: CaptureIntegrityLedgerV2,
    market_plan: ProvisionalPromotingCapturePlanV2,
) -> tuple[int, str]:
    seal = receipt.seal
    events = integrity_ledger.events
    if len(events) != seal.event_count or (
        (events[-1].sha256 if events else None) != seal.event_tip_sha256
    ):
        raise CaptureIntegrityLedgerIntegrityError(
            "current ledger events differ from the CLEAN closure seal"
        )
    open_count = 0
    bounded_count = 0
    gap_root = hashlib.sha256(_GAP_DOMAIN)
    for event in events:
        if event.event_type == "VOID":
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN retained market proof encountered VOID evidence"
            )
        if event.event_type != "SOURCE_GAP":
            continue
        if (
            event.payload.get("route_id") != "usdm_market"
            or event.payload.get("plan_id") != market_plan.name
        ):
            continue
        phase = event.payload.get("phase")
        if phase == "OPEN":
            open_count += 1
            continue
        if phase != "BOUNDED":
            raise CaptureIntegrityLedgerIntegrityError(
                "market SOURCE_GAP phase is not OPEN or BOUNDED"
            )
        integrity_ledger.assert_source_gap_bounded_current_v2(event)
        bounded_count += 1
        gap_root.update(
            canonical_json_line(
                {"event_sequence": event.event_sequence, "event_sha256": event.sha256}
            )
        )
    if open_count != bounded_count:
        raise CaptureIntegrityLedgerIntegrityError(
            "retained market prefix contains an unbounded SOURCE_GAP"
        )
    return bounded_count, gap_root.hexdigest()


def _manifest_sequence_sha256(manifests: list[BlockManifestV2]) -> str:
    root = hashlib.sha256(_MANIFEST_DOMAIN)
    for manifest in manifests:
        root.update(
            canonical_json_line(
                {
                    "block_sequence": manifest.block_sequence,
                    "manifest_sha256": hashlib.sha256(
                        canonical_json_line(asdict(manifest))
                    ).hexdigest(),
                }
            )
        )
    return root.hexdigest()


def _cursor_transition_is_exact(
    previous: tuple[str, int, int, int, int, int] | None,
    current: tuple[str, int, int, int, int, int],
    plan_id: str,
) -> bool:
    connection_id, generation, frame_seq, ingest_seq, wall_ms, monotonic_ns = current
    if connection_id != f"{plan_id}-g{generation:06d}":
        return False
    if previous is None:
        return frame_seq == 1
    (
        previous_connection,
        previous_generation,
        previous_frame,
        previous_ingest,
        previous_wall,
        previous_monotonic,
    ) = previous
    if (
        ingest_seq <= previous_ingest
        or wall_ms < previous_wall
        or monotonic_ns < previous_monotonic
    ):
        return False
    if generation == previous_generation:
        return connection_id == previous_connection and frame_seq == previous_frame + 1
    return (
        generation > previous_generation
        and connection_id != previous_connection
        and frame_seq == 1
    )


def _diagnostic_stream(payload: bytes) -> str | None:
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return None
    if not isinstance(document, dict):
        return None
    value = document.get("stream")
    return value if isinstance(value, str) else None


def _stream_type(stream: str) -> MarketStreamTypeV2:
    result = _stream_type_or_none(stream)
    if result is None:
        raise ValueError("logical stream is outside the strict USD-M market set")
    return result


def _stream_type_or_none(stream: str) -> MarketStreamTypeV2 | None:
    if stream.endswith("@aggTrade"):
        return "AGG_TRADE"
    if stream.endswith("@kline_5m"):
        return "KLINE_5M"
    if stream.endswith("@markPrice@1s"):
        return "MARK_PRICE_1S"
    return None


def _row_stream_type(
    row: UsdmAggTradeM1V2 | UsdmKline5mM1V2 | UsdmMarkPrice1sM1V2,
) -> MarketStreamTypeV2:
    if isinstance(row, UsdmAggTradeM1V2):
        return "AGG_TRADE"
    if isinstance(row, UsdmKline5mM1V2):
        return "KLINE_5M"
    if isinstance(row, UsdmMarkPrice1sM1V2):
        return "MARK_PRICE_1S"
    raise TypeError("strict market parser returned an unsupported row type")


def _validate_common_result(
    result: RetainedUsdmMarketParserHealthResultV2,
) -> None:
    if result.route_id != "usdm_market":
        raise ValueError("retained parser-health result requires usdm_market")
    for value, name in (
        (result.plan_bundle_sha256, "plan_bundle_sha256"),
        (result.stream_census_sha256, "stream_census_sha256"),
        (result.parser_contract_sha256, "parser_contract_sha256"),
        (result.session_start_manifest_sha256, "session_start_manifest_sha256"),
        (result.session_closure_manifest_sha256, "session_closure_manifest_sha256"),
        (result.websocket_route_cursors_sha256, "websocket_route_cursors_sha256"),
        (result.route_cursor_closure_entry_sha256, "route_cursor_closure_entry_sha256"),
        (result.stop_receipt_sha256, "stop_receipt_sha256"),
        (result.finalized_route_cursor_sha256, "finalized_route_cursor_sha256"),
        (result.finality_receipt_sha256, "finality_receipt_sha256"),
        (result.finality_authority_sha256, "finality_authority_sha256"),
        (result.finality_exact_prefix_sha256, "finality_exact_prefix_sha256"),
        (result.finality_prefix_proof_sha256, "finality_prefix_proof_sha256"),
        (result.grouped_block_root_binding_sha256, "grouped_block_root_binding_sha256"),
        (result.grouped_block_manifest_sequence_sha256, "grouped_block_manifest_sequence_sha256"),
        (result.scanned_prefix_sequence_sha256, "scanned_prefix_sequence_sha256"),
        (result.successful_parse_sequence_sha256, "successful_parse_sequence_sha256"),
        (
            result.bounded_market_source_gap_sequence_sha256,
            "bounded_market_source_gap_sequence_sha256",
        ),
        (result.ledger_clean_closure_receipt_sha256, "ledger_clean_closure_receipt_sha256"),
        (result.ledger_clean_closure_seal_sha256, "ledger_clean_closure_seal_sha256"),
    ):
        _require_sha256(value, name)
    if result.parser_contract_sha256 != USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2:
        raise ValueError("retained parser-health result binds a foreign parser contract")
    for value, name in (
        (result.stream_count, "stream_count"),
        (result.finality_tail_ingest_seq, "finality_tail_ingest_seq"),
        (result.grouped_block_manifest_count, "grouped_block_manifest_count"),
        (result.scanned_prefix_last_ingest_seq, "scanned_prefix_last_ingest_seq"),
        (result.scanned_prefix_record_count, "scanned_prefix_record_count"),
        (result.market_first_ingest_seq, "market_first_ingest_seq"),
        (result.market_last_ingest_seq, "market_last_ingest_seq"),
        (result.market_record_count, "market_record_count"),
        (result.terminal_generation, "terminal_generation"),
        (result.terminal_frame_seq, "terminal_frame_seq"),
    ):
        _require_positive_int(value, name)
    for value, name in (
        (result.agg_trade_success_count, "agg_trade_success_count"),
        (result.kline_5m_success_count, "kline_5m_success_count"),
        (result.mark_price_1s_success_count, "mark_price_1s_success_count"),
        (result.successful_parse_count, "successful_parse_count"),
        (result.bounded_market_source_gap_count, "bounded_market_source_gap_count"),
    ):
        _require_nonnegative_int(value, name)
    if (
        result.scanned_prefix_first_ingest_seq != 1
        or result.scanned_prefix_last_ingest_seq != result.finality_tail_ingest_seq
        or result.scanned_prefix_record_count != result.finality_tail_ingest_seq
        or result.market_first_ingest_seq > result.market_last_ingest_seq
        or result.market_last_ingest_seq > result.finality_tail_ingest_seq
    ):
        raise ValueError("retained parser-health scan bounds differ from finality")
    if result.successful_parse_count != (
        result.agg_trade_success_count
        + result.kline_5m_success_count
        + result.mark_price_1s_success_count
    ):
        raise ValueError("retained market suffix census is inconsistent")
    if type(result.stream_scans) is not tuple or len(result.stream_scans) != result.stream_count:
        raise ValueError("retained market stream scan differs from the frozen census")
    if tuple(scan.stream for scan in result.stream_scans) != tuple(
        sorted(scan.stream for scan in result.stream_scans)
    ):
        raise ValueError("retained market stream scans are not canonical")
    if (
        sum(scan.successful_parse_count for scan in result.stream_scans)
        != result.successful_parse_count
    ):
        raise ValueError("per-stream parse census differs from the total")


def _require_literal_nonclaims(result: object) -> None:
    if getattr(result, "current_storage_reverified", None) is not True:
        raise ValueError("retained parser-health result requires current storage re-verification")
    for name in (
        "upstream_message_losslessness_claimed",
        "required_source_completeness_claimed",
        "oi_schedule_completeness_claimed",
        "oi_freshness_claimed",
        "m2_certified",
        "strategy_ready",
        "pnl_or_order_authority",
    ):
        if getattr(result, name, None) is not False:
            raise ValueError(f"retained parser-health result cannot claim {name}")
    if getattr(result, "void_count", None) != 0 or getattr(
        result,
        "unbounded_market_source_gap_count",
        None,
    ) != 0:
        raise ValueError("retained parser-health result cannot contain VOID or unbounded gaps")


def _result_hash(
    domain: bytes,
    result: RetainedUsdmMarketParserHealthResultV2,
    digest_field: str,
) -> str:
    document = _result_document(result, excluded_field=digest_field)
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _result_document(
    result: RetainedUsdmMarketParserHealthResultV2,
    *,
    excluded_field: str | None = None,
) -> dict[str, object]:
    document: dict[str, object] = {}
    for model_field in fields(result):
        if model_field.name == excluded_field or model_field.name.startswith("_"):
            continue
        value: object = getattr(result, model_field.name)
        if model_field.name == "stream_scans":
            value = tuple(asdict(scan) for scan in result.stream_scans)
        document[model_field.name] = value
    return document


def _require_sha256(value: object, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _require_positive_int(value: object, name: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _require_nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
