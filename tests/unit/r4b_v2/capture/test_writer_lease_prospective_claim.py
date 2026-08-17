from __future__ import annotations

from pathlib import Path

import pytest

from signalbot.capture.writer_lease import (
    WriterLease,
    WriterLeaseProspectiveAttemptClaimError,
)

PLAN_SHA256 = "a" * 64


def test_prospective_claim_requires_guard_and_is_irreversible_per_acquisition(
    tmp_path: Path,
) -> None:
    lease = WriterLease.acquire(tmp_path)
    try:
        with pytest.raises(
            WriterLeaseProspectiveAttemptClaimError,
            match="operation guard",
        ):
            lease.claim_prospective_attempt_authority(
                attempt_plan_sha256=PLAN_SHA256
            )

        with lease.operation_guard():
            lease.claim_prospective_attempt_authority(
                attempt_plan_sha256=PLAN_SHA256
            )
            lease.assert_prospective_attempt_authority_claim(
                attempt_plan_sha256=PLAN_SHA256
            )
            with pytest.raises(
                WriterLeaseProspectiveAttemptClaimError,
                match="already consumed",
            ):
                lease.claim_prospective_attempt_authority(
                    attempt_plan_sha256=PLAN_SHA256
                )
    finally:
        lease.release()


def test_prospective_claim_rejects_foreign_plan_and_invalid_digest(
    tmp_path: Path,
) -> None:
    lease = WriterLease.acquire(tmp_path)
    try:
        with lease.operation_guard():
            with pytest.raises(ValueError, match="lowercase SHA-256"):
                lease.claim_prospective_attempt_authority(
                    attempt_plan_sha256="not-a-digest"
                )
            lease.claim_prospective_attempt_authority(
                attempt_plan_sha256=PLAN_SHA256
            )
            with pytest.raises(
                WriterLeaseProspectiveAttemptClaimError,
                match="differs",
            ):
                lease.assert_prospective_attempt_authority_claim(
                    attempt_plan_sha256="b" * 64
                )
    finally:
        lease.release()


def test_new_lease_acquisition_may_resume_same_attempt(tmp_path: Path) -> None:
    first = WriterLease.acquire(tmp_path)
    with first.operation_guard():
        first.claim_prospective_attempt_authority(
            attempt_plan_sha256=PLAN_SHA256
        )
    first.release()

    second = WriterLease.acquire(tmp_path)
    try:
        with second.operation_guard():
            second.claim_prospective_attempt_authority(
                attempt_plan_sha256=PLAN_SHA256
            )
            second.assert_prospective_attempt_authority_claim(
                attempt_plan_sha256=PLAN_SHA256
            )
    finally:
        second.release()
