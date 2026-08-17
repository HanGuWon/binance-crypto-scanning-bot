"""Current-storage parser health for one retained USD-M public-depth prefix.

The certificate in this module is deliberately narrower than depth-book M2.  It
replays the exact signed local prefix of a durably CLEAN session, live-rechecks
membership for every retained ``usdm_public`` row, strictly parses each row with
the frozen depth M1 contract, and binds the results into bounded rolling roots.

Consecutive successfully parsed rows for the same planned stream and connection
generation are also checked for the observed ``pu == previous u`` relation.  A
successful check is only a local contradiction check: it does not bridge a REST
snapshot, reconstruct a book, prove upstream losslessness, or authorize M2.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import InitVar, asdict, dataclass, field, fields
from pathlib import Path
from typing import Final, Literal, cast

from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.blocks import (
    BlockManifestV2,
    GroupedBlockWriterV2,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerIntegrityError,
    CaptureIntegrityLedgerV2,
    PersistedCaptureCleanClosureSealReceiptV2,
)
from signalbot.r4b_v2.capture.membership import (
    CurrentVerifiedRawMembershipLeafUseV2,
    consume_current_verified_raw_membership_prefix_v2,
    inspect_current_verified_raw_membership_leaf_v2,
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
from signalbot.r4b_v2.capture.usdm_public_m1 import (
    USDM_PUBLIC_DEPTH_M1_PARSER_CONTRACT_SHA256_V2,
    UsdmDepthDiff100msM1V2,
    UsdmPublicDepthM1ContractErrorV2,
    parse_current_verified_usdm_public_depth_m1_v2,
)
from signalbot.r4b_v2.capture.websocket_finality import (
    WebSocketRouteCursorClosureEntryV2,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FACTORY_TOKEN: Final = object()
_CERTIFICATE_SCHEMA: Final = "r4b_v2_retained_usdm_public_parser_health_certificate_v1"
_NONCERTIFYING_SCHEMA: Final = (
    "r4b_v2_retained_usdm_public_parser_health_noncertifying_v1"
)
_STREAM_SCAN_SCHEMA: Final = "r4b_v2_retained_usdm_public_stream_scan_v1"
_CERTIFICATE_DOMAIN: Final = b"R4B_V2_RETAINED_USDM_PUBLIC_PARSER_HEALTH_CERTIFICATE\0"
_NONCERTIFYING_DOMAIN: Final = (
    b"R4B_V2_RETAINED_USDM_PUBLIC_PARSER_HEALTH_NONCERTIFYING\0"
)
_PREFIX_DOMAIN: Final = b"R4B_V2_RETAINED_PUBLIC_SIGNED_PREFIX_SEQUENCE\0"
_SUCCESS_DOMAIN: Final = b"R4B_V2_USDM_PUBLIC_DEPTH_M1_SUCCESS_SEQUENCE\0"
_FAILURE_DOMAIN: Final = b"R4B_V2_USDM_PUBLIC_DEPTH_M1_FAILURE_SEQUENCE\0"
_STREAM_DOMAIN: Final = b"R4B_V2_USDM_PUBLIC_DEPTH_M1_STREAM_SEQUENCE\0"
_MANIFEST_DOMAIN: Final = b"R4B_V2_GROUPED_BLOCK_MANIFEST_SEQUENCE\0"
_GAP_DOMAIN: Final = b"R4B_V2_USDM_PUBLIC_BOUNDED_GAP_SEQUENCE\0"
_PU_CHECK_DOMAIN: Final = b"R4B_V2_USDM_PUBLIC_OBSERVED_PU_CHECK_SEQUENCE\0"

_ISSUE_ORDER: Final = (
    "STRICT_M1_PARSE_FAILURE",
    "UNKNOWN_OR_UNPLANNED_STREAM",
    "LOCAL_CURSOR_CONFLICT",
    "PUBLIC_RECORD_AFTER_FINALIZED_CURSOR",
    "TERMINAL_CURSOR_MISMATCH",
    "DEPTH_PU_DISCONTINUITY",
    "MISSING_PLANNED_STREAM",
)


class RetainedUsdmPublicParserHealthErrorV2(RuntimeError):
    """Raised when certification inputs or current storage no longer agree."""


@dataclass(frozen=True, slots=True)
class RetainedUsdmPublicStreamScanV2:
    """Bounded per-depth-stream census; row hashes remain in a rolling root."""

    stream: str
    stream_type: Literal["DEPTH_100MS"]
    successful_parse_count: int
    first_ingest_seq: int | None
    last_ingest_seq: int | None
    successful_parse_sequence_sha256: str
    schema_version: str = _STREAM_SCAN_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != _STREAM_SCAN_SCHEMA:
            raise ValueError("unsupported retained public stream-scan schema")
        if _stream_type_or_none(self.stream) != self.stream_type:
            raise ValueError("retained public stream type differs from its logical stream")
        _require_nonnegative_int(self.successful_parse_count, "successful_parse_count")
        _require_sha256(
            self.successful_parse_sequence_sha256,
            "successful_parse_sequence_sha256",
        )
        if self.successful_parse_count == 0:
            if self.first_ingest_seq is not None or self.last_ingest_seq is not None:
                raise ValueError("empty public stream scan cannot expose ingest bounds")
            return
        _require_positive_int(self.first_ingest_seq, "first_ingest_seq")
        _require_positive_int(self.last_ingest_seq, "last_ingest_seq")
        assert self.first_ingest_seq is not None
        assert self.last_ingest_seq is not None
        if self.first_ingest_seq > self.last_ingest_seq:
            raise ValueError("public stream scan ingest bounds are reversed")


@dataclass(frozen=True, slots=True)
class RetainedUsdmPublicParserHealthCertificateV2:
    """Factory-sealed proof that every retained public-depth row strictly parsed."""

    session_id: str
    process_boot_id: str
    plan_bundle_sha256: str
    plan_id: str
    route_id: Literal["usdm_public"]
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
    public_first_ingest_seq: int
    public_last_ingest_seq: int
    public_record_count: int
    terminal_connection_id: str
    terminal_generation: int
    terminal_frame_seq: int
    depth_100ms_success_count: int
    successful_parse_count: int
    successful_parse_sequence_sha256: str
    stream_scans: tuple[RetainedUsdmPublicStreamScanV2, ...]
    observed_pu_transition_check_count: int
    observed_pu_transition_sequence_sha256: str
    bounded_public_source_gap_count: int
    bounded_public_source_gap_sequence_sha256: str
    ledger_clean_closure_receipt_sha256: str
    ledger_clean_closure_seal_sha256: str
    _factory_token: InitVar[object | None] = None
    certificate_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_CERTIFICATE_SCHEMA)
    current_storage_reverified: Literal[True] = field(init=False, default=True)
    retained_public_prefix_parser_health_certified: Literal[True] = field(
        init=False,
        default=True,
    )
    parse_failure_count: Literal[0] = field(init=False, default=0)
    unknown_stream_count: Literal[0] = field(init=False, default=0)
    local_cursor_conflict_count: Literal[0] = field(init=False, default=0)
    public_record_after_finalized_cursor_count: Literal[0] = field(
        init=False,
        default=0,
    )
    terminal_cursor_mismatch_count: Literal[0] = field(init=False, default=0)
    depth_pu_discontinuity_count: Literal[0] = field(init=False, default=0)
    missing_planned_stream_count: Literal[0] = field(init=False, default=0)
    void_count: Literal[0] = field(init=False, default=0)
    unbounded_public_source_gap_count: Literal[0] = field(init=False, default=0)
    depth_sequence_continuity_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    snapshot_bridge_claimed: Literal[False] = field(init=False, default=False)
    local_book_reconstructed: Literal[False] = field(init=False, default=False)
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
    paper_ready: Literal[False] = field(init=False, default=False)
    pnl_authority: Literal[False] = field(init=False, default=False)
    production_order_authority: Literal[False] = field(init=False, default=False)
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise TypeError("retained public parser-health certificates are factory-sealed")
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        _validate_common_result(self)
        if self.schema_version != _CERTIFICATE_SCHEMA:
            raise ValueError("unsupported retained public parser-health certificate schema")
        if self.successful_parse_count != self.public_record_count:
            raise ValueError("certified public prefix has an unparsed retained row")
        if any(scan.successful_parse_count < 1 for scan in self.stream_scans):
            raise ValueError("certified public prefix must observe every planned stream")
        _require_literal_nonclaims(self)
        object.__setattr__(
            self,
            "certificate_sha256",
            _result_hash(_CERTIFICATE_DOMAIN, self, "certificate_sha256"),
        )


@dataclass(frozen=True, slots=True)
class RetainedUsdmPublicParserHealthNoncertifyingV2:
    """Factory-sealed current-storage result that makes no health claim."""

    session_id: str
    process_boot_id: str
    plan_bundle_sha256: str
    plan_id: str
    route_id: Literal["usdm_public"]
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
    public_first_ingest_seq: int
    public_last_ingest_seq: int
    public_record_count: int
    terminal_connection_id: str
    terminal_generation: int
    terminal_frame_seq: int
    depth_100ms_success_count: int
    successful_parse_count: int
    successful_parse_sequence_sha256: str
    stream_scans: tuple[RetainedUsdmPublicStreamScanV2, ...]
    observed_pu_transition_check_count: int
    observed_pu_transition_sequence_sha256: str
    bounded_public_source_gap_count: int
    bounded_public_source_gap_sequence_sha256: str
    ledger_clean_closure_receipt_sha256: str
    ledger_clean_closure_seal_sha256: str
    parse_failure_count: int
    unknown_stream_count: int
    local_cursor_conflict_count: int
    public_record_after_finalized_cursor_count: int
    terminal_cursor_mismatch_count: int
    depth_pu_discontinuity_count: int
    missing_planned_stream_count: int
    issue_codes: tuple[str, ...]
    first_issue_ingest_seq: int | None
    failure_sequence_sha256: str
    _factory_token: InitVar[object | None] = None
    result_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_NONCERTIFYING_SCHEMA)
    current_storage_reverified: Literal[True] = field(init=False, default=True)
    retained_public_prefix_parser_health_certified: Literal[False] = field(
        init=False,
        default=False,
    )
    void_count: Literal[0] = field(init=False, default=0)
    unbounded_public_source_gap_count: Literal[0] = field(init=False, default=0)
    depth_sequence_continuity_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    snapshot_bridge_claimed: Literal[False] = field(init=False, default=False)
    local_book_reconstructed: Literal[False] = field(init=False, default=False)
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
    paper_ready: Literal[False] = field(init=False, default=False)
    pnl_authority: Literal[False] = field(init=False, default=False)
    production_order_authority: Literal[False] = field(init=False, default=False)
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise TypeError("noncertifying retained public results are factory-sealed")
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        _validate_common_result(self)
        if self.schema_version != _NONCERTIFYING_SCHEMA:
            raise ValueError("unsupported noncertifying retained public result schema")
        for value, name in (
            (self.parse_failure_count, "parse_failure_count"),
            (self.unknown_stream_count, "unknown_stream_count"),
            (self.local_cursor_conflict_count, "local_cursor_conflict_count"),
            (
                self.public_record_after_finalized_cursor_count,
                "public_record_after_finalized_cursor_count",
            ),
            (self.terminal_cursor_mismatch_count, "terminal_cursor_mismatch_count"),
            (self.depth_pu_discontinuity_count, "depth_pu_discontinuity_count"),
            (self.missing_planned_stream_count, "missing_planned_stream_count"),
        ):
            _require_nonnegative_int(value, name)
        if self.unknown_stream_count > self.parse_failure_count:
            raise ValueError("unknown stream count exceeds strict parse failures")
        if self.successful_parse_count + self.parse_failure_count != self.public_record_count:
            raise ValueError("noncertifying public parse census differs from retained rows")
        if not self.issue_codes:
            raise ValueError("noncertifying public result requires at least one issue")
        if self.issue_codes != tuple(
            code for code in _ISSUE_ORDER if code in self.issue_codes
        ):
            raise ValueError(
                "noncertifying public issue codes are unknown, duplicate, or out of order"
            )
        if self.first_issue_ingest_seq is not None:
            _require_positive_int(self.first_issue_ingest_seq, "first_issue_ingest_seq")
        _require_sha256(self.failure_sequence_sha256, "failure_sequence_sha256")
        _require_literal_nonclaims(self)
        object.__setattr__(
            self,
            "result_sha256",
            _result_hash(_NONCERTIFYING_DOMAIN, self, "result_sha256"),
        )


type RetainedUsdmPublicParserHealthResultV2 = (
    RetainedUsdmPublicParserHealthCertificateV2
    | RetainedUsdmPublicParserHealthNoncertifyingV2
)


@dataclass(slots=True)
class _MutableStreamScan:
    stream: str
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

    def freeze(self) -> RetainedUsdmPublicStreamScanV2:
        hasher = cast("hashlib._Hash", self.sequence)
        return RetainedUsdmPublicStreamScanV2(
            stream=self.stream,
            stream_type="DEPTH_100MS",
            successful_parse_count=self.count,
            first_ingest_seq=self.first_ingest_seq,
            last_ingest_seq=self.last_ingest_seq,
            successful_parse_sequence_sha256=hasher.hexdigest(),
        )


def certify_retained_usdm_public_parser_health_v2(
    closure_authority: PersistedSessionClosureAuthorityV2,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    integrity_ledger: CaptureIntegrityLedgerV2,
    block_writer: GroupedBlockWriterV2,
) -> RetainedUsdmPublicParserHealthResultV2:
    """Certify one closed public prefix under one held lease operation guard."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be exact")
    with lease.operation_guard():
        return _certify_retained_usdm_public_parser_health_guarded_v2(
            closure_authority,
            lease=lease,
            session_start_authority=session_start_authority,
            promoting_plans=promoting_plans,
            pipeline=pipeline,
            ledger_seal_receipt=ledger_seal_receipt,
            integrity_ledger=integrity_ledger,
            block_writer=block_writer,
        )


def _certify_retained_usdm_public_parser_health_guarded_v2(
    closure_authority: PersistedSessionClosureAuthorityV2,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    integrity_ledger: CaptureIntegrityLedgerV2,
    block_writer: GroupedBlockWriterV2,
) -> RetainedUsdmPublicParserHealthResultV2:
    """Reverify and strictly parse the exact retained local public prefix.

    Storage or authority drift raises and emits no result.  Signed retained
    content that fails M1, local cursor, planned-stream, or observed same-
    generation ``pu`` checks returns a typed noncertifying result.
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
    public_plan = _public_plan(promoting_plans)
    public_entry = _public_cursor_entry(manifest.websocket_route_cursors)

    _assert_current_closure(
        closure_authority,
        lease=lease,
        session_start_authority=session_start_authority,
        promoting_plans=promoting_plans,
        pipeline=pipeline,
        ledger_seal_receipt=ledger_seal_receipt,
        integrity_ledger=integrity_ledger,
    )
    bounded_gap_count, bounded_gap_root = _reverify_public_gaps(
        ledger_seal_receipt,
        integrity_ledger=integrity_ledger,
        public_plan=public_plan,
    )

    scans = {
        stream: _MutableStreamScan(stream) for stream in sorted(public_plan.streams)
    }
    prefix_root = hashlib.sha256(_PREFIX_DOMAIN)
    success_root = hashlib.sha256(_SUCCESS_DOMAIN)
    failure_root = hashlib.sha256(_FAILURE_DOMAIN)
    pu_root = hashlib.sha256(_PU_CHECK_DOMAIN)
    public_count = 0
    public_first = 0
    public_last = 0
    successful = 0
    parse_failures = 0
    unknown_streams = 0
    cursor_conflicts = 0
    after_cursor = 0
    terminal_mismatch = 0
    pu_checks = 0
    pu_discontinuities = 0
    first_issue_ingest: int | None = None
    previous_cursor: tuple[str, int, int, int, int, int] | None = None
    last_cursor: tuple[str, int, int, int, int, int] | None = None
    previous_depth_by_stream: dict[str, tuple[int, int, int]] = {}

    def note_issue(
        category: str,
        ingest_seq: int,
        details: dict[str, object] | None = None,
    ) -> None:
        nonlocal first_issue_ingest
        if first_issue_ingest is None:
            first_issue_ingest = ingest_seq
        document: dict[str, object] = {
            "category": category,
            "ingest_seq": ingest_seq,
        }
        if details is not None:
            document.update(details)
        failure_root.update(canonical_json_line(document))

    def consume(
        ingest_seq: int,
        encoded_line: bytes,
        current_use: CurrentVerifiedRawMembershipLeafUseV2 | None,
    ) -> None:
        nonlocal public_count, public_first, public_last, successful
        nonlocal parse_failures, unknown_streams, cursor_conflicts, after_cursor
        nonlocal pu_checks, pu_discontinuities
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
        if not isinstance(document, dict) or document.get("route_id") != "usdm_public":
            if current_use is not None:
                raise RetainedUsdmPublicParserHealthErrorV2(
                    "non-public prefix row unexpectedly received a current M0 use"
                )
            return
        if current_use is None:
            raise RetainedUsdmPublicParserHealthErrorV2(
                "public prefix row lacks its callback-scoped current M0 use"
            )
        leaf = inspect_current_verified_raw_membership_leaf_v2(current_use)
        record = leaf.record
        if (
            record.session_id != manifest.session_id
            or record.plan_id != public_plan.name
            or record.protocol_hash
            != session_start_authority.manifest.wal_authority.protocol_sha256
            or record.frame_seq is None
        ):
            raise RetainedUsdmPublicParserHealthErrorV2(
                "signed public record differs from the persisted session authority"
            )
        public_count += 1
        public_first = ingest_seq if public_first == 0 else public_first
        public_last = ingest_seq
        assert record.frame_seq is not None
        cursor = (
            record.connection_id,
            record.generation,
            record.frame_seq,
            record.ingest_seq,
            record.receipt_wall_ms,
            record.receipt_monotonic_ns,
        )
        if not _cursor_transition_is_exact(previous_cursor, cursor, public_plan.name):
            cursor_conflicts += 1
            note_issue("LOCAL_CURSOR_CONFLICT", ingest_seq)
        previous_cursor = cursor
        last_cursor = cursor
        if ingest_seq > public_entry.last_ingest_seq:
            after_cursor += 1
            note_issue("PUBLIC_RECORD_AFTER_FINALIZED_CURSOR", ingest_seq)
        try:
            row = parse_current_verified_usdm_public_depth_m1_v2(
                current_use,
                promoting_plans=promoting_plans,
            )
        except UsdmPublicDepthM1ContractErrorV2:
            parse_failures += 1
            diagnostic_stream = _diagnostic_stream(record.payload_bytes())
            unknown = diagnostic_stream is not None and (
                diagnostic_stream not in scans
                or _stream_type_or_none(diagnostic_stream) is None
            )
            if unknown:
                unknown_streams += 1
            elif diagnostic_stream in scans:
                previous_depth_by_stream.pop(diagnostic_stream, None)
            note_issue(
                (
                    "UNKNOWN_OR_UNPLANNED_STREAM"
                    if unknown
                    else "STRICT_M1_PARSE_FAILURE"
                ),
                ingest_seq,
                {
                    "raw_payload_sha256": hashlib.sha256(
                        record.payload_bytes()
                    ).hexdigest()
                },
            )
            return
        _consume_depth_row(
            row,
            ingest_seq=ingest_seq,
            scans=scans,
            success_root=success_root,
        )
        successful += 1
        previous_depth = previous_depth_by_stream.get(row.stream)
        pu_consistent = _observed_pu_transition_is_consistent(
            previous_depth,
            generation=row.generation,
            previous_final_update_id=row.previous_final_update_id,
        )
        if pu_consistent is not None:
            assert previous_depth is not None
            previous_ingest_seq = previous_depth[1]
            previous_final_update_id = previous_depth[2]
            pu_checks += 1
            pu_document = {
                "generation": row.generation,
                "ingest_seq": ingest_seq,
                "observed_previous_final_update_id": row.previous_final_update_id,
                "previous_final_update_id": previous_final_update_id,
                "previous_ingest_seq": previous_ingest_seq,
                "stream": row.stream,
            }
            pu_root.update(canonical_json_line(pu_document))
            if not pu_consistent:
                pu_discontinuities += 1
                note_issue("DEPTH_PU_DISCONTINUITY", ingest_seq, pu_document)
        previous_depth_by_stream[row.stream] = (
            row.generation,
            ingest_seq,
            row.final_update_id,
        )

    delivered, manifests = consume_current_verified_raw_membership_prefix_v2(
        block_writer,
        integrity_ledger=integrity_ledger,
        expected_transport=TransportV2.WEBSOCKET,
        expected_venue=VenueV2.USDM_FUTURES,
        expected_route_id="usdm_public",
        expected_symbol=None,
        consume=consume,
    )
    if not manifests or manifests[-1].last_ingest_seq != finality.fence_ingest_seq:
        raise RetainedUsdmPublicParserHealthErrorV2(
            "verified grouped-block tail differs from the persisted finality fence"
        )
    manifest_root = _manifest_sequence_sha256(manifests)
    if delivered != finality.fence_ingest_seq:
        raise RetainedUsdmPublicParserHealthErrorV2(
            "scanned grouped-block record count differs from persisted finality"
        )
    if last_cursor is None:
        raise RetainedUsdmPublicParserHealthErrorV2(
            "persisted public route cursor has no retained public record"
        )
    terminal_matches = last_cursor == (
        public_entry.connection_id,
        public_entry.generation,
        public_entry.last_frame_seq,
        public_entry.last_ingest_seq,
        public_entry.last_receipt_wall_ms,
        public_entry.last_receipt_monotonic_ns,
    )
    if not terminal_matches:
        terminal_mismatch = 1
        note_issue("TERMINAL_CURSOR_MISMATCH", public_last)
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
        plan_id=public_plan.name,
        route_id="usdm_public",
        stream_census_sha256=provisional_promoting_stream_census_sha256_v2(public_plan),
        stream_count=len(public_plan.streams),
        parser_contract_sha256=USDM_PUBLIC_DEPTH_M1_PARSER_CONTRACT_SHA256_V2,
        session_start_manifest_sha256=session_start_authority.manifest_sha256,
        session_closure_manifest_sha256=closure_authority.manifest_sha256,
        websocket_route_cursors_sha256=cast(
            str,
            manifest.websocket_route_cursors_sha256,
        ),
        route_cursor_closure_entry_sha256=public_entry.sha256,
        stop_receipt_sha256=public_entry.stop_receipt_sha256,
        finalized_route_cursor_sha256=public_entry.finalized_route_cursor_sha256,
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
        public_first_ingest_seq=public_first,
        public_last_ingest_seq=public_last,
        public_record_count=public_count,
        terminal_connection_id=last_cursor[0],
        terminal_generation=last_cursor[1],
        terminal_frame_seq=last_cursor[2],
        depth_100ms_success_count=successful,
        successful_parse_count=successful,
        successful_parse_sequence_sha256=success_root.hexdigest(),
        stream_scans=frozen_scans,
        observed_pu_transition_check_count=pu_checks,
        observed_pu_transition_sequence_sha256=pu_root.hexdigest(),
        bounded_public_source_gap_count=bounded_gap_count,
        bounded_public_source_gap_sequence_sha256=bounded_gap_root,
        ledger_clean_closure_receipt_sha256=ledger_seal_receipt.sha256,
        ledger_clean_closure_seal_sha256=ledger_seal_receipt.seal_sha256,
    )
    issue_flags = {
        "STRICT_M1_PARSE_FAILURE": parse_failures > unknown_streams,
        "UNKNOWN_OR_UNPLANNED_STREAM": unknown_streams > 0,
        "LOCAL_CURSOR_CONFLICT": cursor_conflicts > 0,
        "PUBLIC_RECORD_AFTER_FINALIZED_CURSOR": after_cursor > 0,
        "TERMINAL_CURSOR_MISMATCH": terminal_mismatch > 0,
        "DEPTH_PU_DISCONTINUITY": pu_discontinuities > 0,
        "MISSING_PLANNED_STREAM": missing_streams > 0,
    }
    issue_codes = tuple(code for code in _ISSUE_ORDER if issue_flags[code])
    if issue_codes:
        return RetainedUsdmPublicParserHealthNoncertifyingV2(
            **common,
            parse_failure_count=parse_failures,
            unknown_stream_count=unknown_streams,
            local_cursor_conflict_count=cursor_conflicts,
            public_record_after_finalized_cursor_count=after_cursor,
            terminal_cursor_mismatch_count=terminal_mismatch,
            depth_pu_discontinuity_count=pu_discontinuities,
            missing_planned_stream_count=missing_streams,
            issue_codes=issue_codes,
            first_issue_ingest_seq=first_issue_ingest,
            failure_sequence_sha256=failure_root.hexdigest(),
            _factory_token=_FACTORY_TOKEN,
        )
    return RetainedUsdmPublicParserHealthCertificateV2(
        **common,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_retained_usdm_public_parser_health_result_v2(
    result: RetainedUsdmPublicParserHealthResultV2,
) -> bytes:
    """Serialize one live-valid factory result canonically."""

    if type(result) is RetainedUsdmPublicParserHealthCertificateV2:
        if result._factory_seal is not _FACTORY_TOKEN:
            raise RetainedUsdmPublicParserHealthErrorV2(
                "public certificate factory seal differs"
            )
        _validate_common_result(result)
        _require_literal_nonclaims(result)
        if (
            result.successful_parse_count != result.public_record_count
            or any(scan.successful_parse_count < 1 for scan in result.stream_scans)
        ):
            raise RetainedUsdmPublicParserHealthErrorV2(
                "certificate no longer proves a fully parsed planned stream census"
            )
        expected = _result_hash(_CERTIFICATE_DOMAIN, result, "certificate_sha256")
        if result.certificate_sha256 != expected:
            raise RetainedUsdmPublicParserHealthErrorV2("public certificate hash differs")
    elif type(result) is RetainedUsdmPublicParserHealthNoncertifyingV2:
        if result._factory_seal is not _FACTORY_TOKEN:
            raise RetainedUsdmPublicParserHealthErrorV2(
                "public result factory seal differs"
            )
        _validate_common_result(result)
        _require_literal_nonclaims(result)
        expected = _result_hash(_NONCERTIFYING_DOMAIN, result, "result_sha256")
        if result.result_sha256 != expected:
            raise RetainedUsdmPublicParserHealthErrorV2("public result hash differs")
    else:
        raise TypeError("result must be a retained public parser-health result")
    return canonical_json_line(_result_document(result))


def _consume_depth_row(
    row: UsdmDepthDiff100msM1V2,
    *,
    ingest_seq: int,
    scans: dict[str, _MutableStreamScan],
    success_root: object,
) -> None:
    scan = scans.get(row.stream)
    if scan is None:
        raise RetainedUsdmPublicParserHealthErrorV2(
            "strict public parser returned an unplanned stream"
        )
    scan.add(ingest_seq, row.m0_leaf_sha256, row.m1_payload_sha256)
    hasher = cast("hashlib._Hash", success_root)
    hasher.update(
        canonical_json_line(
            {
                "ingest_seq": ingest_seq,
                "m0_leaf_sha256": row.m0_leaf_sha256,
                "m1_payload_sha256": row.m1_payload_sha256,
                "stream": row.stream,
                "stream_type": "DEPTH_100MS",
            }
        )
    )


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
        raise RetainedUsdmPublicParserHealthErrorV2(
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


def _public_plan(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> ProvisionalPromotingCapturePlanV2:
    matches = tuple(
        plan
        for plan in plans
        if type(plan) is ProvisionalPromotingCapturePlanV2
        and plan.route_id == "usdm_public"
    )
    if len(matches) != 1:
        raise RetainedUsdmPublicParserHealthErrorV2(
            "promoting plan lacks one exact USD-M public owner"
        )
    return matches[0]


def _public_cursor_entry(
    entries: tuple[WebSocketRouteCursorClosureEntryV2, ...],
) -> WebSocketRouteCursorClosureEntryV2:
    if (
        len(entries) != 2
        or entries[0].route_id != "usdm_market"
        or entries[1].route_id != "usdm_public"
    ):
        raise RetainedUsdmPublicParserHealthErrorV2(
            "session closure lacks the canonical persisted market/public cursor pair"
        )
    return entries[1]


def _reverify_public_gaps(
    receipt: PersistedCaptureCleanClosureSealReceiptV2,
    *,
    integrity_ledger: CaptureIntegrityLedgerV2,
    public_plan: ProvisionalPromotingCapturePlanV2,
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
                "CLEAN retained public proof encountered VOID evidence"
            )
        if event.event_type != "SOURCE_GAP":
            continue
        if (
            event.payload.get("route_id") != "usdm_public"
            or event.payload.get("plan_id") != public_plan.name
        ):
            continue
        phase = event.payload.get("phase")
        if phase == "OPEN":
            open_count += 1
            continue
        if phase != "BOUNDED":
            raise CaptureIntegrityLedgerIntegrityError(
                "public SOURCE_GAP phase is not OPEN or BOUNDED"
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
            "retained public prefix contains an unbounded SOURCE_GAP"
        )
    return bounded_count, gap_root.hexdigest()


def _manifest_sequence_sha256(manifests: Sequence[BlockManifestV2]) -> str:
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


def _observed_pu_transition_is_consistent(
    previous: tuple[int, int, int] | None,
    *,
    generation: int,
    previous_final_update_id: int,
) -> bool | None:
    """Check only adjacent observed rows in one connection generation.

    ``None`` is the deliberate boundary result for a stream's first row or its
    first row after a generation change.  Such a row needs a future snapshot
    bridge and cannot be judged by a predecessor from another connection.
    """

    if previous is None or previous[0] != generation:
        return None
    return previous_final_update_id == previous[2]


def _diagnostic_stream(payload: bytes) -> str | None:
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return None
    if not isinstance(document, dict):
        return None
    value = document.get("stream")
    return value if isinstance(value, str) else None


def _stream_type_or_none(stream: str) -> Literal["DEPTH_100MS"] | None:
    if stream.endswith("@depth@100ms"):
        return "DEPTH_100MS"
    return None


def _validate_common_result(result: RetainedUsdmPublicParserHealthResultV2) -> None:
    if result.route_id != "usdm_public":
        raise ValueError("retained public parser-health result requires usdm_public")
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
        (
            result.grouped_block_manifest_sequence_sha256,
            "grouped_block_manifest_sequence_sha256",
        ),
        (result.scanned_prefix_sequence_sha256, "scanned_prefix_sequence_sha256"),
        (result.successful_parse_sequence_sha256, "successful_parse_sequence_sha256"),
        (
            result.observed_pu_transition_sequence_sha256,
            "observed_pu_transition_sequence_sha256",
        ),
        (
            result.bounded_public_source_gap_sequence_sha256,
            "bounded_public_source_gap_sequence_sha256",
        ),
        (
            result.ledger_clean_closure_receipt_sha256,
            "ledger_clean_closure_receipt_sha256",
        ),
        (
            result.ledger_clean_closure_seal_sha256,
            "ledger_clean_closure_seal_sha256",
        ),
    ):
        _require_sha256(value, name)
    if result.parser_contract_sha256 != USDM_PUBLIC_DEPTH_M1_PARSER_CONTRACT_SHA256_V2:
        raise ValueError("retained public result binds a foreign parser contract")
    for value, name in (
        (result.stream_count, "stream_count"),
        (result.finality_tail_ingest_seq, "finality_tail_ingest_seq"),
        (result.grouped_block_manifest_count, "grouped_block_manifest_count"),
        (result.scanned_prefix_last_ingest_seq, "scanned_prefix_last_ingest_seq"),
        (result.scanned_prefix_record_count, "scanned_prefix_record_count"),
        (result.public_first_ingest_seq, "public_first_ingest_seq"),
        (result.public_last_ingest_seq, "public_last_ingest_seq"),
        (result.public_record_count, "public_record_count"),
        (result.terminal_generation, "terminal_generation"),
        (result.terminal_frame_seq, "terminal_frame_seq"),
    ):
        _require_positive_int(value, name)
    for value, name in (
        (result.depth_100ms_success_count, "depth_100ms_success_count"),
        (result.successful_parse_count, "successful_parse_count"),
        (result.observed_pu_transition_check_count, "observed_pu_transition_check_count"),
        (result.bounded_public_source_gap_count, "bounded_public_source_gap_count"),
    ):
        _require_nonnegative_int(value, name)
    if (
        result.scanned_prefix_first_ingest_seq != 1
        or result.scanned_prefix_last_ingest_seq != result.finality_tail_ingest_seq
        or result.scanned_prefix_record_count != result.finality_tail_ingest_seq
        or result.public_first_ingest_seq > result.public_last_ingest_seq
        or result.public_last_ingest_seq > result.finality_tail_ingest_seq
    ):
        raise ValueError("retained public parser-health scan bounds differ from finality")
    if result.successful_parse_count != result.depth_100ms_success_count:
        raise ValueError("retained public depth census is inconsistent")
    if type(result.stream_scans) is not tuple or len(result.stream_scans) != result.stream_count:
        raise ValueError("retained public stream scan differs from the frozen census")
    if tuple(scan.stream for scan in result.stream_scans) != tuple(
        sorted(scan.stream for scan in result.stream_scans)
    ):
        raise ValueError("retained public stream scans are not canonical")
    if (
        sum(scan.successful_parse_count for scan in result.stream_scans)
        != result.successful_parse_count
    ):
        raise ValueError("per-stream public parse census differs from the total")


def _require_literal_nonclaims(result: object) -> None:
    if getattr(result, "current_storage_reverified", None) is not True:
        raise ValueError("retained public result requires current storage re-verification")
    for name in (
        "depth_sequence_continuity_claimed",
        "snapshot_bridge_claimed",
        "local_book_reconstructed",
        "upstream_message_losslessness_claimed",
        "required_source_completeness_claimed",
        "oi_schedule_completeness_claimed",
        "oi_freshness_claimed",
        "m2_certified",
        "strategy_ready",
        "paper_ready",
        "pnl_authority",
        "production_order_authority",
    ):
        if getattr(result, name, None) is not False:
            raise ValueError(f"retained public parser-health result cannot claim {name}")
    if getattr(result, "void_count", None) != 0 or getattr(
        result,
        "unbounded_public_source_gap_count",
        None,
    ) != 0:
        raise ValueError("retained public result cannot contain VOID or unbounded gaps")


def _result_hash(
    domain: bytes,
    result: RetainedUsdmPublicParserHealthResultV2,
    digest_field: str,
) -> str:
    document = _result_document(result, excluded_field=digest_field)
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _result_document(
    result: RetainedUsdmPublicParserHealthResultV2,
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
