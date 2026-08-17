from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

import signalbot.r4b_v2.capture.observed_session as observed_session_module
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease, WriterLeaseError
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.observed_session import (
    OBSERVED_SESSION_CERTIFICATE_PERSISTENCE_SUPPORTED_V2,
    ObservedSessionCertificateErrorV2,
    ObservedSessionCertificateV2,
    create_observed_session_certificate_v2,
    validate_observed_session_certificate_v2,
    verify_observed_session_certificate_current_v2,
)
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import PublicOiRestTerminalObservationV2
from signalbot.r4b_v2.capture.rest_census import (
    PublicOiRestCellOutcomeV2,
    PublicOiRestCoverageCloseV2,
    PublicOiRestSlotCensusEntryV2,
    PublicOiRestSlotCensusV2,
    public_oi_rest_attempt_record_sha256_v2,
)
from signalbot.r4b_v2.capture.rest_census_verifier import (
    PublicOiRestCensusVerificationCertificateV2,
    verify_public_oi_rest_census_prefix_v2,
)
from signalbot.r4b_v2.capture.rest_schedule_body_verifier import (
    PublicOiScheduleBodyVerificationCertificateV2,
    verify_public_oi_schedule_bodies_v2,
)
from signalbot.r4b_v2.capture.session import SessionAuthorityIntegrityError

from .test_session import _ClosureFixture


class _ObservedFixture(_ClosureFixture):
    oi_certificate: PublicOiRestCensusVerificationCertificateV2
    oi_body_certificate: PublicOiScheduleBodyVerificationCertificateV2
    close_record: RawRecordV2
    observed_records: tuple[RawRecordV2, ...]

    async def finalize_observed(self) -> None:
        self.pipeline.start()
        rest_plan = cast(
            ProvisionalPromotingRestCapturePlanV2,
            next(
                plan for plan in self.plans if type(plan) is ProvisionalPromotingRestCapturePlanV2
            ),
        )
        coverage_start = ((self.started_wall_ms + 4_999) // 5_000) * 5_000
        base_monotonic_ns = max(
            self.started_monotonic_ns + 1,
            time.monotonic_ns() - 1_000_000,
        )
        symbol = rest_plan.symbols[0]
        observation = PublicOiRestTerminalObservationV2.for_plan(
            rest_plan,
            symbol=symbol,
            poll_cycle_seq=1,
            symbol_ordinal=0,
            scheduled_slot_wall_ms=coverage_start,
            attempt=1,
            request_started_wall_ms=coverage_start + 10,
            request_started_monotonic_ns=base_monotonic_ns + 10,
            response_first_header_wall_ms=coverage_start + 20,
            response_first_header_monotonic_ns=base_monotonic_ns + 20,
            attempt_ended_wall_ms=coverage_start + 30,
            attempt_ended_monotonic_ns=base_monotonic_ns + 30,
            response_status=200,
            response_headers=(),
            payload_complete=True,
            body=(b'{"openInterest":"1.0","symbol":"BTCUSDT","time":1700000000000}'),
        )
        attempt_record = RawRecordV2.from_payload(
            session_id=self.session_id,
            plan_id=rest_plan.name,
            protocol_hash=self.authority.protocol_sha256,
            transport=TransportV2.HTTPS,
            venue=VenueV2.USDM_FUTURES,
            route_id=rest_plan.route_id,
            symbol=symbol,
            connection_id="oi-rest-observed",
            generation=1,
            frame_seq=None,
            ingest_seq=1,
            receipt_wall_ms=coverage_start + 40,
            receipt_monotonic_ns=base_monotonic_ns + 40,
            raw_payload=observation(
                ReceiptTimestamp(
                    received_at_ms=coverage_start + 40,
                    received_monotonic_ns=base_monotonic_ns + 40,
                )
            ),
            source_logical_key=f"openInterest:{symbol}",
        )
        entry = PublicOiRestSlotCensusEntryV2.for_plan(
            rest_plan,
            session_start_manifest_sha256=self.start.manifest_sha256,
            plan_bundle_sha256=self.authority.plan_sha256,
            symbol_ordinal=0,
            scheduled_slot_wall_ms=coverage_start,
            outcome=PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
            attempt_ingest_seq=attempt_record.ingest_seq,
            attempt_record_sha256=public_oi_rest_attempt_record_sha256_v2(attempt_record),
        )
        slot = PublicOiRestSlotCensusV2.for_plan(
            rest_plan,
            session_id=self.session_id,
            session_start_manifest_sha256=self.start.manifest_sha256,
            plan_bundle_sha256=self.authority.plan_sha256,
            scheduled_slot_wall_ms=coverage_start,
            entries=(entry,),
            closed_wall_ms=coverage_start + 50,
            closed_monotonic_ns=base_monotonic_ns + 50,
        )
        slot_record = RawRecordV2.from_payload(
            session_id=self.session_id,
            plan_id=rest_plan.name,
            protocol_hash=self.authority.protocol_sha256,
            transport=TransportV2.HTTPS,
            venue=VenueV2.USDM_FUTURES,
            route_id=rest_plan.route_id,
            symbol=None,
            connection_id="oi-rest-census",
            generation=1,
            frame_seq=None,
            ingest_seq=2,
            receipt_wall_ms=coverage_start + 60,
            receipt_monotonic_ns=base_monotonic_ns + 60,
            raw_payload=slot.canonical_bytes(),
            source_logical_key="openInterest:census",
        )
        stop_monotonic_ns = base_monotonic_ns + 70
        close = PublicOiRestCoverageCloseV2.for_plan(
            rest_plan,
            session_id=self.session_id,
            session_start_manifest_sha256=self.start.manifest_sha256,
            plan_bundle_sha256=self.authority.plan_sha256,
            coverage_start_slot_wall_ms=coverage_start,
            stop_requested_wall_ms=coverage_start + 5_000,
            stop_requested_monotonic_ns=stop_monotonic_ns,
            last_census_ingest_seq=slot_record.ingest_seq,
        )
        self.close_record = RawRecordV2.from_payload(
            session_id=self.session_id,
            plan_id=rest_plan.name,
            protocol_hash=self.authority.protocol_sha256,
            transport=TransportV2.HTTPS,
            venue=VenueV2.USDM_FUTURES,
            route_id=rest_plan.route_id,
            symbol=None,
            connection_id="oi-rest-census",
            generation=1,
            frame_seq=None,
            ingest_seq=3,
            receipt_wall_ms=coverage_start + 5_001,
            receipt_monotonic_ns=stop_monotonic_ns + 10,
            raw_payload=close.canonical_bytes(),
            source_logical_key="openInterest:census",
        )
        self.observed_records = (attempt_record, slot_record, self.close_record)
        for record in self.observed_records:
            self.pipeline.offer(record)
        self.finality = await self.pipeline.finalize_current_tail_and_stop(
            timeout_seconds=5,
        )
        self.ledger_seal = self.ledger.seal_clean_closure_v2(
            promoting_plans=self.plans,
            finality_receipt=self.finality,
            wal_writer=self.wal_writer,
            block_writer=self.block_writer,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            seal_wall_ms=self.finality.target_last_receipt_wall_ms + 1,
            seal_monotonic_ns=self.finality.writer_observed_monotonic_ns + 1,
        )
        self.oi_certificate = verify_public_oi_rest_census_prefix_v2(
            self.observed_records,
            plan=rest_plan,
            session_id=self.session_id,
            protocol_hash=self.authority.protocol_sha256,
            session_start_manifest_sha256=self.start.manifest_sha256,
            plan_bundle_sha256=self.authority.plan_sha256,
            finality_receipt=self.finality,
        )
        self.oi_body_certificate = verify_public_oi_schedule_bodies_v2(
            self.observed_records,
            plan=rest_plan,
            session_id=self.session_id,
            protocol_hash=self.authority.protocol_sha256,
            session_start_manifest_sha256=self.start.manifest_sha256,
            plan_bundle_sha256=self.authority.plan_sha256,
            finality_receipt=self.finality,
            observed_schedule_certificate=self.oi_certificate,
        )
        self.write()

    def observed_certificate(
        self,
        *,
        oi_certificate: PublicOiRestCensusVerificationCertificateV2 | None = None,
        oi_body_certificate: PublicOiScheduleBodyVerificationCertificateV2 | None = None,
        lease: WriterLease | None = None,
    ) -> ObservedSessionCertificateV2:
        assert self.persisted_closure is not None
        return create_observed_session_certificate_v2(
            session_closure_authority=self.persisted_closure,
            finality_receipt=self.finality,
            oi_schedule_certificate=oi_certificate or self.oi_certificate,
            oi_schedule_body_certificate=(oi_body_certificate or self.oi_body_certificate),
            lease=lease or self.lease,
            session_start_authority=self.start,
            promoting_plans=self.plans,
            pipeline=self.pipeline,
            ledger_seal_receipt=self.ledger_seal,
            ledger=self.ledger,
        )

    def verify_current(
        self,
        certificate: ObservedSessionCertificateV2,
        *,
        lease: WriterLease | None = None,
    ) -> str:
        return verify_observed_session_certificate_current_v2(
            certificate,
            lease=lease or self.lease,
            session_start_authority=self.start,
            promoting_plans=self.plans,
            pipeline=self.pipeline,
            ledger_seal_receipt=self.ledger_seal,
            ledger=self.ledger,
        )


@pytest.fixture
async def observed_fixture(tmp_path: Path) -> AsyncIterator[_ObservedFixture]:
    fixture = _ObservedFixture(tmp_path)
    try:
        await fixture.finalize_observed()
        yield fixture
    finally:
        await fixture.close()


def test_certificate_binds_exact_current_authorities_but_never_claims_m2(
    observed_fixture: _ObservedFixture,
) -> None:
    certificate = observed_fixture.observed_certificate()
    persisted_closure = observed_fixture.persisted_closure
    assert persisted_closure is not None

    assert certificate.session_closure_authority is persisted_closure
    assert certificate.session_closure_manifest is persisted_closure.manifest
    assert certificate.finality_receipt is observed_fixture.finality
    assert certificate.oi_schedule_certificate is observed_fixture.oi_certificate
    assert certificate.oi_schedule_body_certificate is observed_fixture.oi_body_certificate
    assert certificate.oi_schedule_body_verified is True
    assert certificate.oi_freshness_verified is False
    assert certificate.writer_lease is certificate.session_closure_manifest.writer_lease
    assert (
        certificate.storage_roots
        is certificate.session_closure_manifest.session_start_manifest.storage_roots
    )
    assert certificate.current_authority_reproved_at_issue is True
    assert certificate.certificate_persisted is False
    assert OBSERVED_SESSION_CERTIFICATE_PERSISTENCE_SUPPORTED_V2 is False
    assert certificate.observed_source_completeness_claimed is False
    assert certificate.data_complete is False
    assert certificate.m2_certified is False
    assert certificate.session_close_authorized is False
    assert certificate.production_order_execution_enabled is False
    assert validate_observed_session_certificate_v2(certificate) == certificate.certificate_sha256
    assert observed_fixture.verify_current(certificate) == certificate.certificate_sha256
    repeated = observed_fixture.observed_certificate()
    assert repeated.bound_material_line == certificate.bound_material_line
    assert repeated.certificate_sha256 == certificate.certificate_sha256


def test_certificate_is_factory_sealed_and_canonical_tamper_is_detected(
    observed_fixture: _ObservedFixture,
) -> None:
    certificate = observed_fixture.observed_certificate()

    with pytest.raises(TypeError, match="factory-sealed"):
        replace(certificate, data_complete=True)

    object.__setattr__(certificate, "plan_bundle_sha256", "0" * 64)
    with pytest.raises(
        ObservedSessionCertificateErrorV2,
        match=r"canonical binding|certificate hash|OI certificate",
    ):
        validate_observed_session_certificate_v2(certificate)


def test_cross_session_oi_schedule_certificate_is_rejected(tmp_path: Path) -> None:
    first = _ObservedFixture(tmp_path / "first")
    second = _ObservedFixture(tmp_path / "second")

    async def exercise() -> None:
        try:
            await first.finalize_observed()
            await second.finalize_observed()
            with pytest.raises(
                ObservedSessionCertificateErrorV2,
                match="session, protocol, or plan",
            ):
                first.observed_certificate(
                    oi_certificate=second.oi_certificate,
                    oi_body_certificate=second.oi_body_certificate,
                )
        finally:
            await first.close()
            await second.close()

    asyncio.run(exercise())


def test_cross_schedule_body_certificate_is_rejected(tmp_path: Path) -> None:
    first = _ObservedFixture(tmp_path / "first")
    second = _ObservedFixture(tmp_path / "second")

    async def exercise() -> None:
        try:
            await first.finalize_observed()
            await second.finalize_observed()
            with pytest.raises(
                ObservedSessionCertificateErrorV2,
                match="schedule/body evidence",
            ):
                first.observed_certificate(
                    oi_body_certificate=second.oi_body_certificate,
                )
        finally:
            await first.close()
            await second.close()

    asyncio.run(exercise())


def test_foreign_live_lease_cannot_issue_certificate(
    observed_fixture: _ObservedFixture,
    tmp_path: Path,
) -> None:
    foreign_scope = tmp_path / "foreign-scope"
    foreign_scope.mkdir()
    foreign_lease = WriterLease.acquire(foreign_scope)
    try:
        with pytest.raises(SessionAuthorityIntegrityError):
            observed_fixture.observed_certificate(lease=foreign_lease)
    finally:
        foreign_lease.release()


def test_persisted_closure_tamper_breaks_current_certificate_verification(
    observed_fixture: _ObservedFixture,
) -> None:
    certificate = observed_fixture.observed_certificate()
    Path(certificate.session_closure_canonical_path).write_bytes(b"{}\n")

    with pytest.raises(SessionAuthorityIntegrityError):
        observed_fixture.verify_current(certificate)


def test_certificate_cannot_replay_under_a_new_lease_acquisition(tmp_path: Path) -> None:
    fixture = _ObservedFixture(tmp_path)

    async def exercise() -> None:
        await fixture.finalize_observed()
        certificate = fixture.observed_certificate()
        await fixture.close()
        replacement_lease = WriterLease.acquire(fixture.scope)
        try:
            with pytest.raises(SessionAuthorityIntegrityError):
                fixture.verify_current(certificate, lease=replacement_lease)
        finally:
            replacement_lease.release()

    asyncio.run(exercise())


def test_issuance_holds_lease_guard_through_final_certificate_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _ObservedFixture(tmp_path)
    real_validate = observed_session_module.validate_observed_session_certificate_v2
    validation_observed = False

    def validate_while_release_is_attempted(
        certificate: ObservedSessionCertificateV2,
    ) -> str:
        nonlocal validation_observed
        validation_observed = True
        with pytest.raises(WriterLeaseError, match="active storage operation"):
            fixture.lease.release()
        return real_validate(certificate)

    async def exercise() -> None:
        try:
            await fixture.finalize_observed()
            monkeypatch.setattr(
                observed_session_module,
                "validate_observed_session_certificate_v2",
                validate_while_release_is_attempted,
            )
            certificate = fixture.observed_certificate()
            assert certificate.current_authority_reproved_at_issue is True
            assert validation_observed is True
        finally:
            await fixture.close()

    asyncio.run(exercise())


def test_current_verification_holds_lease_guard_through_final_digest_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _ObservedFixture(tmp_path)
    real_validate = observed_session_module.validate_observed_session_certificate_v2
    validation_count = 0

    def validate_while_release_is_attempted(
        certificate: ObservedSessionCertificateV2,
    ) -> str:
        nonlocal validation_count
        validation_count += 1
        certificate_sha256 = real_validate(certificate)
        if validation_count == 2:
            with pytest.raises(WriterLeaseError, match="active storage operation"):
                fixture.lease.release()
        return certificate_sha256

    async def exercise() -> None:
        try:
            await fixture.finalize_observed()
            certificate = fixture.observed_certificate()
            monkeypatch.setattr(
                observed_session_module,
                "validate_observed_session_certificate_v2",
                validate_while_release_is_attempted,
            )
            assert fixture.verify_current(certificate) == certificate.certificate_sha256
            assert validation_count == 2
        finally:
            await fixture.close()

    asyncio.run(exercise())
