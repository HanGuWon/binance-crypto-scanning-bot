from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path

import pytest

from signalbot.capture.writer_lease import WriterLease, WriterLeaseNotHeldError
from signalbot.r4b_v2.capture.causal_target_authority import (
    CausalTargetAuthorityOwnerV2,
)
from signalbot.r4b_v2.capture.causal_target_cursor import (
    CausalTargetCursorSnapshotV2,
    derive_causal_target_cursor_snapshot_v2,
)
from signalbot.r4b_v2.capture.integrity_ledger import CaptureIntegrityLedgerV2
from signalbot.r4b_v2.execution.authorized_paper_entry import (
    AuthorizedPaperEntryContractErrorV2,
    AuthorizedSizedPaperEntryV2,
    CausalPaperSizingReferenceV2,
    canonical_authorized_sized_paper_entry_v2,
    canonical_causal_paper_sizing_reference_v2,
    evaluate_authorized_sized_paper_entry_v2,
)
from signalbot.r4b_v2.execution.paper_fok import (
    MARK_PRICE_MAX_STALENESS_MS_V2,
    PaperFokEntryInputV2,
    PaperFokEntryStatusV2,
)
from signalbot.r4b_v2.execution.paper_sizing import (
    PaperSizingCellV2,
    PaperSizingStatusV2,
)

from ..capture import test_causal_target_authority as authority_testkit
from ..capture import test_causal_target_cursor as cursor_testkit

_DECISION_CUTOFF_MS = cursor_testkit._BASE_WALL_MS + 2_000  # pyright: ignore[reportPrivateUsage]


@dataclass(slots=True)
class _Fixture:
    lease: WriterLease
    snapshot: CausalTargetCursorSnapshotV2
    owner: CausalTargetAuthorityOwnerV2

    def close(self) -> None:
        try:
            self.lease.release()
        except WriterLeaseNotHeldError:
            pass


@pytest.fixture
def authority_fixture(tmp_path: Path):  # type: ignore[no-untyped-def]
    plans = cursor_testkit._plans()  # pyright: ignore[reportPrivateUsage]
    source = cursor_testkit._fixture(  # pyright: ignore[reportPrivateUsage]
        tmp_path,
        (
            cursor_testkit._clock_record(  # pyright: ignore[reportPrivateUsage]
                plans,
                ingest_seq=1,
                elapsed_ms=0,
                server_time_ms=cursor_testkit._BASE_WALL_MS,  # pyright: ignore[reportPrivateUsage]
            ),
            cursor_testkit._plain_record(  # pyright: ignore[reportPrivateUsage]
                ingest_seq=2,
                elapsed_ms=12_013,
            ),
            cursor_testkit._plain_record(  # pyright: ignore[reportPrivateUsage]
                ingest_seq=3,
                elapsed_ms=12_014,
            ),
        ),
    )
    lease = WriterLease.acquire(tmp_path)
    ledger = CaptureIntegrityLedgerV2(
        tmp_path / "ledger",
        authority=source.writer.authority,
        block_directory=source.writer.directory,
        block_root_binding=source.writer.root_binding,
        block_signing_authority=source.writer.signing_authority,
        block_policy=source.writer.policy,
        block_stream_group_id=source.writer.stream_group_id,
        block_segment_id=source.writer.segment_id,
        maximum_total_bytes=cursor_testkit._MAXIMUM_BYTES,  # pyright: ignore[reportPrivateUsage]
        emergency_reserve_bytes=cursor_testkit._RESERVE_BYTES,  # pyright: ignore[reportPrivateUsage]
        max_events=32,
        failure_domain_id="causal-target-cursor-ledger-device",
        writer_lease=lease,
    )
    with lease.operation_guard():
        snapshot = derive_causal_target_cursor_snapshot_v2(
            source.writer,
            integrity_ledger=ledger,
            promoting_plans=source.plans,
            decision_cutoff_ms=_DECISION_CUTOFF_MS,
        )
    owner = CausalTargetAuthorityOwnerV2(
        block_writer=source.writer,
        integrity_ledger=ledger,
        promoting_plans=source.plans,
        writer_lease=lease,
        maximum_authorizations=16,
    )
    fixture = _Fixture(lease=lease, snapshot=snapshot, owner=owner)
    try:
        yield fixture
    finally:
        fixture.close()


def _paper_input(
    snapshot: CausalTargetCursorSnapshotV2,
    *,
    requested_quantity: Decimal = Decimal("1.00"),
    mark_age_ms: int = MARK_PRICE_MAX_STALENESS_MS_V2,
) -> PaperFokEntryInputV2:
    base = authority_testkit._paper_input(snapshot)  # pyright: ignore[reportPrivateUsage]
    target_venue_ms = snapshot.target_venue_ms
    target_local_ms = snapshot.target_local_cursor_ms
    events = tuple(
        replace(
            event,
            event_time_ms=target_venue_ms - 1_000,
            transaction_time_ms=target_venue_ms - 1_000,
            receipt_completion_ms=target_local_ms - 500,
        )
        for event in base.pre_target_depth_events
    )
    successors = tuple(
        replace(
            event,
            promoting_plan_sha256=snapshot.promoting_plan_sha256,
            event_time_ms=target_venue_ms + 1,
            transaction_time_ms=target_venue_ms + 1,
            receipt_completion_ms=target_local_ms + 1,
        )
        for event in base.closure.successor_candidates
    )
    return replace(
        base,
        requested_quantity=requested_quantity,
        snapshot=replace(
            base.snapshot,
            response_completion_ms=target_local_ms - 1_000,
        ),
        pre_target_depth_events=events,
        closure=replace(
            base.closure,
            closure_grace_end_local_ms=target_local_ms + 30_000,
            finalized_through_local_ms=target_local_ms,
            successor_candidates=successors,
        ),
        mark=replace(
            base.mark,
            event_time_ms=target_venue_ms - mark_age_ms,
            receipt_completion_ms=target_local_ms,
        ),
        exchange_info=replace(
            base.exchange_info,
            response_completion_ms=target_local_ms - 1_000,
            version_valid_from_local_ms=target_local_ms - 10_000,
            version_valid_through_local_ms=target_local_ms,
        ),
    )


def _evaluate(
    fixture: _Fixture,
    item: PaperFokEntryInputV2,
) -> AuthorizedSizedPaperEntryV2:
    return fixture.owner.with_current_authority(
        fixture.snapshot,
        consume=lambda current: evaluate_authorized_sized_paper_entry_v2(
            item,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            current_target_authority=current,
        ),
    )


def test_current_capability_binds_mark_sizing_and_paper_fok(
    authority_fixture: _Fixture,
) -> None:
    result = _evaluate(
        authority_fixture,
        _paper_input(authority_fixture.snapshot),
    )

    assert result.sizing.status is PaperSizingStatusV2.READY
    assert result.sizing.requested_quantity == Decimal("1.00")
    assert result.paper_decision.status is (
        PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY
    ), result.paper_decision.reasons
    assert result.reference.mark_price == Decimal("100.00")
    assert (
        result.sizing.reference_evidence_sha256
        == result.reference.evidence_sha256
    )
    assert result.current_signed_prefix_authoritative
    assert result.sizing_reference_membership_authoritative
    assert result.causal_target_membership_authoritative
    assert not result.durable_capability_persisted
    assert not result.typed_wal_replay_authoritative
    assert not result.efficacy_eligible
    assert not result.production_order_placement
    assert canonical_causal_paper_sizing_reference_v2(result.reference).endswith(b"\n")
    assert canonical_authorized_sized_paper_entry_v2(result).endswith(b"\n")
    assert authority_fixture.owner.authorization_count == 1


def test_requested_quantity_is_recomputed_not_trusted(
    authority_fixture: _Fixture,
) -> None:
    item = _paper_input(
        authority_fixture.snapshot,
        requested_quantity=Decimal("2.00"),
    )
    with pytest.raises(
        AuthorizedPaperEntryContractErrorV2,
        match="requested quantity differs",
    ):
        _evaluate(authority_fixture, item)
    assert authority_fixture.owner.authorization_count == 1


def test_mark_staleness_boundary_is_inclusive_then_fails_closed(
    authority_fixture: _Fixture,
) -> None:
    accepted = _paper_input(
        authority_fixture.snapshot,
        mark_age_ms=MARK_PRICE_MAX_STALENESS_MS_V2,
    )
    assert _evaluate(authority_fixture, accepted).reference.mark_event_time_ms == (
        authority_fixture.snapshot.target_venue_ms
        - MARK_PRICE_MAX_STALENESS_MS_V2
    )

    stale = _paper_input(
        authority_fixture.snapshot,
        mark_age_ms=MARK_PRICE_MAX_STALENESS_MS_V2 + 1,
    )
    with pytest.raises(
        AuthorizedPaperEntryContractErrorV2,
        match="staleness boundary",
    ):
        _evaluate(authority_fixture, stale)
    assert authority_fixture.owner.authorization_count == 2


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda item: replace(
                item,
                mark=replace(item.mark, source_root_sha256="f" * 64),
            ),
            "outside the exact PAPER lineage",
        ),
        (
            lambda item: replace(
                item,
                exchange_info=replace(
                    item.exchange_info,
                    response_completion_ms=item.target_local_cursor_ms + 1,
                    version_valid_through_local_ms=item.target_local_cursor_ms + 1,
                ),
            ),
            "arrived after the target cursor",
        ),
    ),
)
def test_foreign_mark_or_noncausal_filter_is_rejected(
    authority_fixture: _Fixture,
    mutation,  # type: ignore[no-untyped-def]
    message: str,
) -> None:
    item = mutation(_paper_input(authority_fixture.snapshot))
    with pytest.raises(AuthorizedPaperEntryContractErrorV2, match=message):
        _evaluate(authority_fixture, item)


def test_direct_cursor_and_direct_receipt_construction_are_rejected(
    authority_fixture: _Fixture,
) -> None:
    item = _paper_input(authority_fixture.snapshot)
    with pytest.raises(
        AuthorizedPaperEntryContractErrorV2,
        match="current causal-target authority",
    ):
        evaluate_authorized_sized_paper_entry_v2(
            item,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            current_target_authority=item.target_cursor,
        )

    result = _evaluate(authority_fixture, item)
    with pytest.raises(AuthorizedPaperEntryContractErrorV2, match="factory-sealed"):
        replace(result)
    with pytest.raises(AuthorizedPaperEntryContractErrorV2, match="factory-sealed"):
        replace(result.reference)
    with pytest.raises(AuthorizedPaperEntryContractErrorV2, match="factory-sealed"):
        CausalPaperSizingReferenceV2(
            attempt_id=result.reference.attempt_id,
            signal_event_id=result.reference.signal_event_id,
            symbol=result.reference.symbol,
            venue=result.reference.venue,
            promoting_plan_sha256=result.reference.promoting_plan_sha256,
            source_root_sha256=result.reference.source_root_sha256,
            mark_schema_sha256=result.reference.mark_schema_sha256,
            decision_cutoff_ms=result.reference.decision_cutoff_ms,
            target_venue_ms=result.reference.target_venue_ms,
            target_local_cursor_ms=result.reference.target_local_cursor_ms,
            target_state_last_ingest_seq=(
                result.reference.target_state_last_ingest_seq
            ),
            capability_id=result.reference.capability_id,
            cursor_snapshot_sha256=result.reference.cursor_snapshot_sha256,
            pair=result.reference.pair,
            routing_status=result.reference.routing_status,
            mark_price=result.reference.mark_price,
            mark_event_time_ms=result.reference.mark_event_time_ms,
            mark_receipt_completion_ms=(
                result.reference.mark_receipt_completion_ms
            ),
            _factory_token=object(),
        )
