from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from typing import Final

from signalbot.capture.clock_health_report import assess_causal_clock_cutoff
from signalbot.r4b_v2.alerts.actionability import (
    PRIMARY_PAPER_TARGET_DELAY_MS_V2,
    CausalTargetCursorV2,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.blocks import (
    BlockManifestV2,
    GroupedBlockWriterV2,
    parse_raw_record_line_v2,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityEventV2,
    CaptureIntegrityLedgerV2,
    attest_finalized_block_chain_v2,
)
from signalbot.r4b_v2.capture.membership import (
    CurrentVerifiedRawMembershipLeafUseV2,
    consume_current_verified_raw_membership_leaf_v2,
    consume_current_verified_raw_membership_prefix_v2,
)
from signalbot.r4b_v2.capture.models import TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingPlanV9,
    ProvisionalUsdmVenueClockRestCapturePlanV9,
    provisional_promoting_plan_sha256_v9,
    validate_provisional_promoting_capture_plans_v9,
)
from signalbot.r4b_v2.capture.usdm_venue_clock_m1 import (
    UsdmVenueClockSampleM1V2,
    parse_current_verified_usdm_venue_clock_sample_m1_v2,
    usdm_venue_clock_sample_fresh_at_m1_v2,
    usdm_venue_clock_samples_rate_continuous_m1_v2,
)

CAUSAL_TARGET_CURSOR_SNAPSHOT_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.0_CAUSAL_TARGET_CURSOR_SNAPSHOT"
)
CAUSAL_TARGET_CURSOR_SNAPSHOT_ONLY_REASON_V2: Final = (
    "FACTORY_DERIVED_FROM_CURRENT_SIGNED_PREFIX_AT_ISSUANCE_BUT_"
    "CALLBACK_AUTHORITY_IS_REVOKED_AND_NO_LIVE_REVERIFICATION_TOKEN_EXISTS"
)

_SCHEMA_VERSION = "r4b_v2_causal_target_cursor_snapshot_v1"
_FACTORY_TOKEN = object()
_PREFIX_CHAIN_DOMAIN = b"R4B_V2_CAUSAL_TARGET_PREFIX_CHAIN\0"
_CLOCK_SEGMENT_ROOT_DOMAIN = b"R4B_V2_CAUSAL_TARGET_CLOCK_SEGMENT\0"
_SNAPSHOT_HASH_DOMAIN = b"R4B_V2_CAUSAL_TARGET_CURSOR_SNAPSHOT\0"
_ZERO_HASH = "0" * 64
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GAP_EVENT_TYPES = frozenset({"DATA_GAP", "SOURCE_GAP", "VOID"})


class CausalTargetCursorDerivationErrorV2(RuntimeError):
    """The retained evidence cannot establish the target infimum safely."""


@dataclass(frozen=True, slots=True)
class CausalTargetCursorSnapshotV2:
    """Factory-only issuance snapshot of one derived causal target.

    The scalar cursor witness is derived from the signed raw prefix and strict
    ``/fapi/v1/time`` samples.  This object deliberately does not export current
    storage authority: the callback-scoped membership capabilities have already
    been revoked when the factory returns.  A later PAPER owner must live-reverify
    the bound prefix rather than accepting this durable snapshot by itself.
    """

    decision_cutoff_ms: int
    target_venue_ms: int
    prior_ingest_seq: int
    target_ingest_seq: int
    prior_local_cursor_ms: int
    target_local_cursor_ms: int
    prior_receipt_monotonic_ns: int
    target_receipt_monotonic_ns: int
    prior_venue_lower_bound_ms: int
    target_venue_lower_bound_ms: int
    prior_record_jsonl_sha256: str
    target_record_jsonl_sha256: str
    prior_clock_sample_m1_sha256: str
    target_clock_sample_m1_sha256: str
    clock_segment_root_sha256: str
    promoting_plan_sha256: str
    capture_authority_sha256: str
    integrity_ledger_root_binding_sha256: str
    block_root_binding_sha256: str
    signed_prefix_tip_ingest_seq: int
    signed_prefix_tip_block_hash: str
    signed_prefix_manifest_count: int
    integrity_event_count: int
    integrity_event_tip_sha256: str | None
    _factory_token: InitVar[object | None] = None
    legacy_cursor_evidence_sha256: str = field(init=False)
    snapshot_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_SCHEMA_VERSION)
    rule_version: str = field(
        init=False,
        default=CAUSAL_TARGET_CURSOR_SNAPSHOT_RULE_VERSION_V2,
    )
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise CausalTargetCursorDerivationErrorV2(
                "causal target cursor snapshots are factory-sealed"
            )
        _validate_snapshot_fields(self)
        cursor = _legacy_cursor(self)
        object.__setattr__(
            self,
            "legacy_cursor_evidence_sha256",
            cursor.cursor_evidence_sha256,
        )
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        object.__setattr__(
            self,
            "snapshot_sha256",
            hashlib.sha256(
                _SNAPSHOT_HASH_DOMAIN
                + canonical_json_line(_snapshot_document(self, include_snapshot_sha256=False))
            ).hexdigest(),
        )

    @property
    def cursor_math_complete_at_issuance(self) -> bool:
        return True

    @property
    def signed_prefix_verified_at_issuance(self) -> bool:
        return True

    @property
    def caller_cursor_scalars_accepted(self) -> bool:
        return False

    @property
    def current_authority_claimed(self) -> bool:
        return False

    @property
    def paper_input_authorized(self) -> bool:
        return False

    @property
    def production_order_placement(self) -> bool:
        return False

    @property
    def authority_reason(self) -> str:
        return CAUSAL_TARGET_CURSOR_SNAPSHOT_ONLY_REASON_V2


@dataclass(frozen=True, slots=True)
class _CursorPoint:
    ingest_seq: int
    local_cursor_ms: int
    receipt_monotonic_ns: int
    venue_lower_bound_ms: int
    record_jsonl_sha256: str
    clock_sample_m1_sha256: str


@dataclass(frozen=True, slots=True)
class _CursorPair:
    prior: _CursorPoint
    target: _CursorPoint
    target_prefix_chain_sha256: str


class _SignedPrefixCursorScanner:
    def __init__(
        self,
        *,
        promoting_plans: tuple[ProvisionalPromotingPlanV9, ...],
        clock_plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
        target_venue_ms: int,
    ) -> None:
        self._promoting_plans = promoting_plans
        self._clock_plan = clock_plan
        self._target_venue_ms = target_venue_ms
        self._next_ingest_seq = 1
        self._latest_sample: UsdmVenueClockSampleM1V2 | None = None
        self._prior_point: _CursorPoint | None = None
        self._candidate: _CursorPair | None = None
        self._prefix_chain_sha256 = _ZERO_HASH

    @property
    def delivered_count(self) -> int:
        return self._next_ingest_seq - 1

    @property
    def candidate(self) -> _CursorPair | None:
        return self._candidate

    def consume(
        self,
        ingest_seq: int,
        encoded_line: bytes,
        current_use: CurrentVerifiedRawMembershipLeafUseV2 | None,
    ) -> None:
        if ingest_seq != self._next_ingest_seq:
            raise CausalTargetCursorDerivationErrorV2("signed raw prefix has a local ingest gap")
        record = parse_raw_record_line_v2(encoded_line)
        if record.ingest_seq != ingest_seq:
            raise CausalTargetCursorDerivationErrorV2(
                "signed raw prefix callback differs from its raw envelope"
            )
        line_sha256 = hashlib.sha256(encoded_line).hexdigest()
        self._prefix_chain_sha256 = hashlib.sha256(
            _PREFIX_CHAIN_DOMAIN
            + bytes.fromhex(self._prefix_chain_sha256)
            + ingest_seq.to_bytes(8, "big")
            + bytes.fromhex(line_sha256)
        ).hexdigest()
        self._next_ingest_seq += 1

        if self._candidate is not None:
            if record.route_id == self._clock_plan.route_id:
                if current_use is None:
                    raise CausalTargetCursorDerivationErrorV2(
                        "venue-clock member lacks current verified raw membership"
                    )
                consume_current_verified_raw_membership_leaf_v2(current_use)
            elif current_use is not None:
                raise CausalTargetCursorDerivationErrorV2(
                    "current verified membership was issued for a non-clock route"
                )
            return

        if record.route_id == self._clock_plan.route_id:
            if current_use is None:
                raise CausalTargetCursorDerivationErrorV2(
                    "venue-clock member lacks current verified raw membership"
                )
            current_sample = parse_current_verified_usdm_venue_clock_sample_m1_v2(
                current_use,
                promoting_plans=self._promoting_plans,
            )
            if self._latest_sample is not None and not (
                usdm_venue_clock_samples_rate_continuous_m1_v2(
                    self._latest_sample,
                    current_sample,
                )
            ):
                raise CausalTargetCursorDerivationErrorV2(
                    "venue-clock samples violate the frozen rate-continuity envelope"
                )
            self._latest_sample = current_sample
        elif current_use is not None:
            raise CausalTargetCursorDerivationErrorV2(
                "current verified membership was issued for a non-clock route"
            )

        sample = self._latest_sample
        if sample is None:
            self._prior_point = None
            return
        if not usdm_venue_clock_sample_fresh_at_m1_v2(
            sample,
            observed_monotonic_ns=record.receipt_monotonic_ns,
        ):
            raise CausalTargetCursorDerivationErrorV2(
                "latest causally available venue-clock sample is stale"
            )
        assessment = assess_causal_clock_cutoff(
            sample.as_clock_health_sample_v1(),
            receipt_monotonic_ns=record.receipt_monotonic_ns,
            cutoff_ms=self._target_venue_ms,
        )
        lower_bound = assessment.venue_time_lower_ms
        if lower_bound is None or assessment.source_ingest_seq != sample.ingest_seq:
            raise CausalTargetCursorDerivationErrorV2(
                "venue-clock lower bound is not causally available"
            )
        if lower_bound < 0:
            raise CausalTargetCursorDerivationErrorV2(
                "venue-clock lower bound cannot be represented as Unix milliseconds"
            )
        point = _CursorPoint(
            ingest_seq=ingest_seq,
            local_cursor_ms=record.receipt_wall_ms,
            receipt_monotonic_ns=record.receipt_monotonic_ns,
            venue_lower_bound_ms=lower_bound,
            record_jsonl_sha256=line_sha256,
            clock_sample_m1_sha256=sample.m1_payload_sha256,
        )
        if lower_bound < self._target_venue_ms:
            self._prior_point = point
            return

        prior = self._prior_point
        if prior is None or prior.ingest_seq + 1 != point.ingest_seq:
            raise CausalTargetCursorDerivationErrorV2(
                "first target crossing lacks an exact contiguous left-bound witness"
            )
        if prior.local_cursor_ms >= point.local_cursor_ms:
            raise CausalTargetCursorDerivationErrorV2(
                "target crossing is not representable by strictly ordered local milliseconds"
            )
        self._candidate = _CursorPair(
            prior=prior,
            target=point,
            target_prefix_chain_sha256=self._prefix_chain_sha256,
        )


def derive_causal_target_cursor_snapshot_v2(
    block_writer: GroupedBlockWriterV2,
    *,
    integrity_ledger: CaptureIntegrityLedgerV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV9, ...],
    decision_cutoff_ms: int,
) -> CausalTargetCursorSnapshotV2:
    """Derive one target witness without accepting caller-provided cursor scalars.

    Only finalized signed block bytes participate.  The result is a historical
    issuance snapshot, not a reusable authority capability and not an order or
    PAPER-execution authorization.
    """

    if type(block_writer) is not GroupedBlockWriterV2:
        raise TypeError("block_writer must be an exact GroupedBlockWriterV2")
    if type(integrity_ledger) is not CaptureIntegrityLedgerV2:
        raise TypeError("integrity_ledger must be an exact CaptureIntegrityLedgerV2")
    if type(promoting_plans) is not tuple:
        raise TypeError("promoting_plans must be the exact frozen tuple")
    if type(decision_cutoff_ms) is not int or decision_cutoff_ms < 0:
        raise ValueError("decision_cutoff_ms must be nonnegative Unix milliseconds")
    frozen_plans = promoting_plans
    validate_provisional_promoting_capture_plans_v9(frozen_plans)
    plan_sha256 = provisional_promoting_plan_sha256_v9(frozen_plans)
    if block_writer.authority.plan_sha256 != plan_sha256:
        raise CausalTargetCursorDerivationErrorV2(
            "signed block authority differs from the frozen v9 capture plan"
        )
    clock_plans = tuple(
        item for item in frozen_plans if type(item) is ProvisionalUsdmVenueClockRestCapturePlanV9
    )
    if len(clock_plans) != 1:
        raise CausalTargetCursorDerivationErrorV2(
            "frozen v9 capture plan lacks one unique USD-M venue-clock owner"
        )
    clock_plan = clock_plans[0]

    try:
        initial_chain = attest_finalized_block_chain_v2(block_writer)
        if initial_chain:
            integrity_ledger.assert_finalized_prefix_not_void_v2(initial_chain[-1][1])
    except (RuntimeError, TypeError, ValueError) as exc:
        raise CausalTargetCursorDerivationErrorV2(
            f"signed-prefix preflight failed closed: {exc}"
        ) from exc
    if not initial_chain:
        raise CausalTargetCursorDerivationErrorV2(
            "causal target derivation requires a finalized signed block"
        )
    events_before = integrity_ledger.events
    _require_gap_free_integrity_events(events_before)

    scanner = _SignedPrefixCursorScanner(
        promoting_plans=frozen_plans,
        clock_plan=clock_plan,
        target_venue_ms=(decision_cutoff_ms + PRIMARY_PAPER_TARGET_DELAY_MS_V2),
    )
    try:
        delivered, manifests = consume_current_verified_raw_membership_prefix_v2(
            block_writer,
            integrity_ledger=integrity_ledger,
            expected_transport=TransportV2.HTTPS,
            expected_venue=VenueV2.USDM_FUTURES,
            expected_route_id=clock_plan.route_id,
            expected_symbol=None,
            consume=scanner.consume,
        )
    except CausalTargetCursorDerivationErrorV2:
        raise
    except (RuntimeError, TypeError, ValueError) as exc:
        raise CausalTargetCursorDerivationErrorV2(
            "signed-prefix causal target derivation failed closed"
        ) from exc

    initial_manifests = tuple(manifest for manifest, _reference in initial_chain)
    if manifests != initial_manifests:
        raise CausalTargetCursorDerivationErrorV2(
            "signed block prefix changed across the integrity-event snapshot"
        )
    events_after = integrity_ledger.events
    if events_after != events_before:
        raise CausalTargetCursorDerivationErrorV2(
            "integrity-event ledger changed during causal target derivation"
        )
    _require_gap_free_integrity_events(events_after)
    if scanner.delivered_count != delivered:
        raise CausalTargetCursorDerivationErrorV2(
            "cursor scanner did not consume the complete signed prefix"
        )
    candidate = scanner.candidate
    if candidate is None:
        raise CausalTargetCursorDerivationErrorV2(
            "signed prefix does not contain a provable causal target crossing"
        )
    if delivered < candidate.target.ingest_seq:
        raise CausalTargetCursorDerivationErrorV2(
            "derived target lies beyond the verified signed prefix"
        )
    target_manifests = tuple(
        manifest
        for manifest in manifests
        if manifest.first_ingest_seq <= candidate.target.ingest_seq
    )
    if not target_manifests or not (
        target_manifests[-1].first_ingest_seq
        <= candidate.target.ingest_seq
        <= target_manifests[-1].last_ingest_seq
    ):
        raise CausalTargetCursorDerivationErrorV2(
            "derived target has no exact signed-manifest owner"
        )
    event_tip = events_after[-1].sha256 if events_after else None
    clock_segment_root = _clock_segment_root(
        block_writer=block_writer,
        plan_sha256=plan_sha256,
        candidate=candidate,
        target_manifests=target_manifests,
        integrity_event_hashes=tuple(event.sha256 for event in events_after),
    )
    return CausalTargetCursorSnapshotV2(
        decision_cutoff_ms=decision_cutoff_ms,
        target_venue_ms=(decision_cutoff_ms + PRIMARY_PAPER_TARGET_DELAY_MS_V2),
        prior_ingest_seq=candidate.prior.ingest_seq,
        target_ingest_seq=candidate.target.ingest_seq,
        prior_local_cursor_ms=candidate.prior.local_cursor_ms,
        target_local_cursor_ms=candidate.target.local_cursor_ms,
        prior_receipt_monotonic_ns=candidate.prior.receipt_monotonic_ns,
        target_receipt_monotonic_ns=candidate.target.receipt_monotonic_ns,
        prior_venue_lower_bound_ms=candidate.prior.venue_lower_bound_ms,
        target_venue_lower_bound_ms=candidate.target.venue_lower_bound_ms,
        prior_record_jsonl_sha256=candidate.prior.record_jsonl_sha256,
        target_record_jsonl_sha256=candidate.target.record_jsonl_sha256,
        prior_clock_sample_m1_sha256=(candidate.prior.clock_sample_m1_sha256),
        target_clock_sample_m1_sha256=(candidate.target.clock_sample_m1_sha256),
        clock_segment_root_sha256=clock_segment_root,
        promoting_plan_sha256=plan_sha256,
        capture_authority_sha256=block_writer.authority.sha256,
        integrity_ledger_root_binding_sha256=(integrity_ledger.ledger_root_binding_sha256),
        block_root_binding_sha256=integrity_ledger.block_root_binding_sha256,
        signed_prefix_tip_ingest_seq=delivered,
        signed_prefix_tip_block_hash=manifests[-1].block_hash,
        signed_prefix_manifest_count=len(manifests),
        integrity_event_count=len(events_after),
        integrity_event_tip_sha256=event_tip,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_causal_target_cursor_snapshot_v2(
    snapshot: CausalTargetCursorSnapshotV2,
) -> bytes:
    """Serialize only an untampered factory snapshot and its explicit nonclaims."""

    _require_factory_snapshot(snapshot)
    return canonical_json_line(_snapshot_document(snapshot, include_snapshot_sha256=True))


def require_factory_causal_target_cursor_snapshot_v2(
    value: object,
) -> CausalTargetCursorSnapshotV2:
    """Reject legacy/direct cursors and return only this factory's snapshot."""

    if type(value) is not CausalTargetCursorSnapshotV2:
        raise TypeError(
            "value must be a factory-derived CausalTargetCursorSnapshotV2; "
            "direct CausalTargetCursorV2 values are not accepted"
        )
    _require_factory_snapshot(value)
    return value


def _require_gap_free_integrity_events(
    events: tuple[CaptureIntegrityEventV2, ...],
) -> None:
    for event in events:
        if event.event_type in _GAP_EVENT_TYPES:
            raise CausalTargetCursorDerivationErrorV2(
                f"causal target derivation rejects {event.event_type} evidence"
            )


def _clock_segment_root(
    *,
    block_writer: GroupedBlockWriterV2,
    plan_sha256: str,
    candidate: _CursorPair,
    target_manifests: tuple[BlockManifestV2, ...],
    integrity_event_hashes: tuple[str, ...],
) -> str:
    document = {
        "capture_authority_sha256": block_writer.authority.sha256,
        "clock_sample_m1_sha256": candidate.target.clock_sample_m1_sha256,
        "integrity_event_hashes": integrity_event_hashes,
        "manifest_block_hashes": tuple(manifest.block_hash for manifest in target_manifests),
        "plan_sha256": plan_sha256,
        "prior_ingest_seq": candidate.prior.ingest_seq,
        "prior_record_jsonl_sha256": candidate.prior.record_jsonl_sha256,
        "stream_group_id": block_writer.stream_group_id,
        "target_ingest_seq": candidate.target.ingest_seq,
        "target_prefix_chain_sha256": candidate.target_prefix_chain_sha256,
        "target_record_jsonl_sha256": candidate.target.record_jsonl_sha256,
    }
    return hashlib.sha256(_CLOCK_SEGMENT_ROOT_DOMAIN + canonical_json_line(document)).hexdigest()


def _legacy_cursor(snapshot: CausalTargetCursorSnapshotV2) -> CausalTargetCursorV2:
    return CausalTargetCursorV2(
        decision_cutoff_ms=snapshot.decision_cutoff_ms,
        target_venue_ms=snapshot.target_venue_ms,
        prior_local_cursor_ms=snapshot.prior_local_cursor_ms,
        prior_venue_lower_bound_ms=snapshot.prior_venue_lower_bound_ms,
        target_local_cursor_ms=snapshot.target_local_cursor_ms,
        target_venue_lower_bound_ms=snapshot.target_venue_lower_bound_ms,
        clock_segment_root_sha256=snapshot.clock_segment_root_sha256,
        contiguous_cursor_evidence=True,
    )


def _validate_snapshot_fields(snapshot: CausalTargetCursorSnapshotV2) -> None:
    if snapshot.schema_version != _SCHEMA_VERSION:
        raise CausalTargetCursorDerivationErrorV2(
            "unsupported causal target cursor snapshot schema"
        )
    if snapshot.rule_version != CAUSAL_TARGET_CURSOR_SNAPSHOT_RULE_VERSION_V2:
        raise CausalTargetCursorDerivationErrorV2(
            "causal target cursor snapshot rule version differs"
        )
    for value, name in (
        (snapshot.decision_cutoff_ms, "decision_cutoff_ms"),
        (snapshot.target_venue_ms, "target_venue_ms"),
        (snapshot.prior_local_cursor_ms, "prior_local_cursor_ms"),
        (snapshot.target_local_cursor_ms, "target_local_cursor_ms"),
        (snapshot.prior_receipt_monotonic_ns, "prior_receipt_monotonic_ns"),
        (snapshot.target_receipt_monotonic_ns, "target_receipt_monotonic_ns"),
        (
            snapshot.prior_venue_lower_bound_ms,
            "prior_venue_lower_bound_ms",
        ),
        (
            snapshot.target_venue_lower_bound_ms,
            "target_venue_lower_bound_ms",
        ),
        (snapshot.integrity_event_count, "integrity_event_count"),
    ):
        if type(value) is not int or value < 0:
            raise CausalTargetCursorDerivationErrorV2(f"{name} must be a nonnegative integer")
    for value, name in (
        (snapshot.prior_ingest_seq, "prior_ingest_seq"),
        (snapshot.target_ingest_seq, "target_ingest_seq"),
        (snapshot.signed_prefix_tip_ingest_seq, "signed_prefix_tip_ingest_seq"),
        (snapshot.signed_prefix_manifest_count, "signed_prefix_manifest_count"),
    ):
        if type(value) is not int or value < 1:
            raise CausalTargetCursorDerivationErrorV2(f"{name} must be a positive integer")
    for value, name in (
        (snapshot.prior_record_jsonl_sha256, "prior_record_jsonl_sha256"),
        (snapshot.target_record_jsonl_sha256, "target_record_jsonl_sha256"),
        (
            snapshot.prior_clock_sample_m1_sha256,
            "prior_clock_sample_m1_sha256",
        ),
        (
            snapshot.target_clock_sample_m1_sha256,
            "target_clock_sample_m1_sha256",
        ),
        (snapshot.clock_segment_root_sha256, "clock_segment_root_sha256"),
        (snapshot.promoting_plan_sha256, "promoting_plan_sha256"),
        (snapshot.capture_authority_sha256, "capture_authority_sha256"),
        (
            snapshot.integrity_ledger_root_binding_sha256,
            "integrity_ledger_root_binding_sha256",
        ),
        (snapshot.block_root_binding_sha256, "block_root_binding_sha256"),
        (
            snapshot.signed_prefix_tip_block_hash,
            "signed_prefix_tip_block_hash",
        ),
    ):
        _validate_sha256(value, name)
    if snapshot.integrity_event_tip_sha256 is not None:
        _validate_sha256(
            snapshot.integrity_event_tip_sha256,
            "integrity_event_tip_sha256",
        )
    if (snapshot.integrity_event_count == 0) != (snapshot.integrity_event_tip_sha256 is None):
        raise CausalTargetCursorDerivationErrorV2("integrity-event count and tip are inconsistent")
    if snapshot.target_venue_ms != (snapshot.decision_cutoff_ms + PRIMARY_PAPER_TARGET_DELAY_MS_V2):
        raise CausalTargetCursorDerivationErrorV2(
            "target venue time must be derived as decision cutoff plus 10000 ms"
        )
    if snapshot.target_ingest_seq != snapshot.prior_ingest_seq + 1:
        raise CausalTargetCursorDerivationErrorV2(
            "target ingest cursor lacks its exact immediate left boundary"
        )
    if snapshot.signed_prefix_tip_ingest_seq < snapshot.target_ingest_seq:
        raise CausalTargetCursorDerivationErrorV2("signed prefix tip precedes the derived target")
    if not (snapshot.prior_receipt_monotonic_ns <= snapshot.target_receipt_monotonic_ns):
        raise CausalTargetCursorDerivationErrorV2(
            "target monotonic cursor precedes its left boundary"
        )
    try:
        _legacy_cursor(snapshot)
    except ValueError as exc:
        raise CausalTargetCursorDerivationErrorV2(
            "derived cursor differs from the frozen actionability mathematics"
        ) from exc


def _validate_sha256(value: str, name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise CausalTargetCursorDerivationErrorV2(f"{name} must be lowercase SHA-256 text")


def _require_factory_snapshot(snapshot: CausalTargetCursorSnapshotV2) -> None:
    if type(snapshot) is not CausalTargetCursorSnapshotV2:
        raise TypeError("snapshot must be an exact CausalTargetCursorSnapshotV2")
    if getattr(snapshot, "_factory_seal", None) is not _FACTORY_TOKEN:
        raise CausalTargetCursorDerivationErrorV2(
            "causal target cursor snapshot factory seal differs"
        )
    _validate_snapshot_fields(snapshot)
    if snapshot.legacy_cursor_evidence_sha256 != (_legacy_cursor(snapshot).cursor_evidence_sha256):
        raise CausalTargetCursorDerivationErrorV2(
            "legacy cursor evidence hash differs from derived scalars"
        )
    expected_hash = hashlib.sha256(
        _SNAPSHOT_HASH_DOMAIN
        + canonical_json_line(_snapshot_document(snapshot, include_snapshot_sha256=False))
    ).hexdigest()
    if snapshot.snapshot_sha256 != expected_hash:
        raise CausalTargetCursorDerivationErrorV2("causal target cursor snapshot hash differs")


def _snapshot_document(
    snapshot: CausalTargetCursorSnapshotV2,
    *,
    include_snapshot_sha256: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "authority_reason": snapshot.authority_reason,
        "block_root_binding_sha256": snapshot.block_root_binding_sha256,
        "caller_cursor_scalars_accepted": snapshot.caller_cursor_scalars_accepted,
        "capture_authority_sha256": snapshot.capture_authority_sha256,
        "clock_segment_root_sha256": snapshot.clock_segment_root_sha256,
        "current_authority_claimed": snapshot.current_authority_claimed,
        "cursor_math_complete_at_issuance": (snapshot.cursor_math_complete_at_issuance),
        "decision_cutoff_ms": snapshot.decision_cutoff_ms,
        "integrity_event_count": snapshot.integrity_event_count,
        "integrity_event_tip_sha256": snapshot.integrity_event_tip_sha256,
        "integrity_ledger_root_binding_sha256": (snapshot.integrity_ledger_root_binding_sha256),
        "legacy_cursor_evidence_sha256": (snapshot.legacy_cursor_evidence_sha256),
        "paper_input_authorized": snapshot.paper_input_authorized,
        "prior_clock_sample_m1_sha256": (snapshot.prior_clock_sample_m1_sha256),
        "prior_ingest_seq": snapshot.prior_ingest_seq,
        "prior_local_cursor_ms": snapshot.prior_local_cursor_ms,
        "prior_receipt_monotonic_ns_text": str(snapshot.prior_receipt_monotonic_ns),
        "prior_record_jsonl_sha256": snapshot.prior_record_jsonl_sha256,
        "prior_venue_lower_bound_ms": snapshot.prior_venue_lower_bound_ms,
        "production_order_placement": snapshot.production_order_placement,
        "promoting_plan_sha256": snapshot.promoting_plan_sha256,
        "rule_version": snapshot.rule_version,
        "schema_version": snapshot.schema_version,
        "signed_prefix_manifest_count": snapshot.signed_prefix_manifest_count,
        "signed_prefix_tip_block_hash": snapshot.signed_prefix_tip_block_hash,
        "signed_prefix_tip_ingest_seq": snapshot.signed_prefix_tip_ingest_seq,
        "signed_prefix_verified_at_issuance": (snapshot.signed_prefix_verified_at_issuance),
        "target_clock_sample_m1_sha256": (snapshot.target_clock_sample_m1_sha256),
        "target_ingest_seq": snapshot.target_ingest_seq,
        "target_local_cursor_ms": snapshot.target_local_cursor_ms,
        "target_receipt_monotonic_ns_text": str(snapshot.target_receipt_monotonic_ns),
        "target_record_jsonl_sha256": snapshot.target_record_jsonl_sha256,
        "target_venue_lower_bound_ms": snapshot.target_venue_lower_bound_ms,
        "target_venue_ms": snapshot.target_venue_ms,
    }
    if include_snapshot_sha256:
        document["snapshot_sha256"] = snapshot.snapshot_sha256
    return document
