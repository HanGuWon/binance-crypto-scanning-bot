from pathlib import Path

import pytest

from signalbot.domain.enums import Direction
from signalbot.prospective.directional_candidates import (
    DirectionalReceipt,
    HistoricalScreenStatus,
    PromotionEvidence,
    PromotionVerdict,
    ProspectiveStatus,
    build_research_manifest,
    evaluate_promotion,
    load_preregistration,
)

CONFIG_PATH = Path("config/research.futures-bidirectional.v1.yaml")


def test_successor_preregistration_freezes_both_futures_directions() -> None:
    preregistration = load_preregistration(CONFIG_PATH)

    assert preregistration.market == "futures"
    assert preregistration.long_family == "breakout_long"
    assert preregistration.short_family == "breakdown_short"
    assert preregistration.authority_status == "WAITING_FOR_AUTHORITY"
    assert preregistration.config_sha256 == load_preregistration(CONFIG_PATH).config_sha256


def test_manifest_is_bound_to_config_identity() -> None:
    preregistration = load_preregistration(CONFIG_PATH)
    manifest = build_research_manifest(
        preregistration,
        source_identity="worktree-source-v1:source",
        data_authority_sha256="data",
        universe_manifest_sha256="universe",
        trial_registry_sha256="trial",
    )

    assert manifest.config_sha256 == preregistration.config_sha256
    assert manifest.manifest_sha256() == build_research_manifest(
        preregistration,
        source_identity="worktree-source-v1:source",
        data_authority_sha256="data",
        universe_manifest_sha256="universe",
        trial_registry_sha256="trial",
    ).manifest_sha256()


def test_promotion_stays_waiting_until_authority_is_released() -> None:
    preregistration = load_preregistration(CONFIG_PATH)
    evidence = PromotionEvidence(
        candidate_version=preregistration.candidate_version,
        config_sha256=preregistration.config_sha256,
        data_authority_sha256="data",
        trial_registry_sha256="trial",
        historical_screen=HistoricalScreenStatus.PASS,
        prospective_status=ProspectiveStatus.COMPLETE,
    )

    assert evaluate_promotion(preregistration, evidence) == PromotionVerdict.WAITING_FOR_AUTHORITY


def test_promotion_requires_independent_receipts_for_long_and_short() -> None:
    preregistration = load_preregistration(CONFIG_PATH)
    manifest = build_research_manifest(
        preregistration,
        source_identity="source",
        data_authority_sha256="data",
        universe_manifest_sha256="universe",
        trial_registry_sha256="trial",
    )
    common = {
        "candidate_version": preregistration.candidate_version,
        "config_sha256": preregistration.config_sha256,
        "sample_count": 100,
        "symbol_count": 4,
        "regime_count": 3,
        "censor_fraction": 0.1,
        "data_quality_passed": True,
        "operational_health_passed": True,
        "independent_review_passed": True,
    }
    long_receipt = DirectionalReceipt(direction=Direction.LONG, **common)
    evidence = PromotionEvidence(
        candidate_version=preregistration.candidate_version,
        config_sha256=preregistration.config_sha256,
        data_authority_sha256="data",
        trial_registry_sha256="trial",
        authority_released=True,
        historical_screen=HistoricalScreenStatus.PASS,
        prospective_status=ProspectiveStatus.COMPLETE,
        independent_review_passed=True,
        directions=(long_receipt,),
    )

    assert (
        evaluate_promotion(preregistration, evidence, manifest=manifest)
        == PromotionVerdict.CONTINUE_OBSERVING
    )


def test_promotion_requires_both_directional_receipts() -> None:
    preregistration = load_preregistration(CONFIG_PATH)
    manifest = build_research_manifest(
        preregistration,
        source_identity="source",
        data_authority_sha256="data",
        universe_manifest_sha256="universe",
        trial_registry_sha256="trial",
    )
    common = {
        "candidate_version": preregistration.candidate_version,
        "config_sha256": preregistration.config_sha256,
        "sample_count": 100,
        "symbol_count": 4,
        "regime_count": 3,
        "censor_fraction": 0.1,
        "data_quality_passed": True,
        "operational_health_passed": True,
        "independent_review_passed": True,
    }
    evidence = PromotionEvidence(
        candidate_version=preregistration.candidate_version,
        config_sha256=preregistration.config_sha256,
        data_authority_sha256="data",
        trial_registry_sha256="trial",
        authority_released=True,
        historical_screen=HistoricalScreenStatus.PASS,
        prospective_status=ProspectiveStatus.COMPLETE,
        independent_review_passed=True,
        directions=(
            DirectionalReceipt(direction=Direction.LONG, **common),
            DirectionalReceipt(direction=Direction.SHORT, **common),
        ),
    )

    assert (
        evaluate_promotion(preregistration, evidence, manifest=manifest)
        == PromotionVerdict.PROMOTE
    )


def test_promotion_rejects_receipts_from_another_data_authority() -> None:
    preregistration = load_preregistration(CONFIG_PATH)
    manifest = build_research_manifest(
        preregistration,
        source_identity="source",
        data_authority_sha256="data",
        universe_manifest_sha256="universe",
        trial_registry_sha256="trial",
    )
    evidence = PromotionEvidence(
        candidate_version=preregistration.candidate_version,
        config_sha256=preregistration.config_sha256,
        data_authority_sha256="other-data",
        trial_registry_sha256="trial",
        authority_released=True,
    )

    assert (
        evaluate_promotion(preregistration, evidence, manifest=manifest)
        == PromotionVerdict.REJECT
    )


def test_mismatched_config_cannot_promote() -> None:
    preregistration = load_preregistration(CONFIG_PATH)
    evidence = PromotionEvidence(
        candidate_version=preregistration.candidate_version,
        config_sha256="different-config",
        data_authority_sha256="data",
        trial_registry_sha256="trial",
        authority_released=True,
    )

    assert evaluate_promotion(preregistration, evidence) == PromotionVerdict.REJECT


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("entry_timing", "SAME_BAR_CLOSE", ""),
    ],
)
def test_successor_contract_rejects_unfrozen_execution_assumptions(
    field: str, value: str, message: str
) -> None:
    del message
    preregistration = load_preregistration(CONFIG_PATH)
    payload = preregistration.canonical_payload()
    payload[field] = value

    with pytest.raises(ValueError):
        type(preregistration).model_validate(payload)
