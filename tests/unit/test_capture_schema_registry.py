from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path
from typing import cast

import pytest
import yaml

from signalbot.capture import storage
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    ConnectionTransitionV1,
    CoverageTransitionV1,
    RestEnvelopeV1,
    RestEnvelopeV2,
)
from signalbot.capture.schema_registry import (
    canonical_registry_bytes,
    capture_schema_registry,
    capture_schema_registry_sha256,
    dataclass_field_contract,
    validate_runtime_schema_contracts,
)
from signalbot.capture.storage import SegmentManifestV1

_ROOT = Path(__file__).parents[2]
_ERRATA = _ROOT / "artifacts" / "research" / "2026-07-17" / "r4b_protocol_errata_spot_depth_v1.yaml"
_DEPTH_REVALIDATION = (
    _ROOT / "artifacts" / "research" / "2026-07-17" / "binance_depth_revalidation_20260717.md"
)
_EXPECTED_RECORD_MODELS = {
    "capture_envelope_v1": CaptureEnvelopeV1,
    "rest_envelope_v1": RestEnvelopeV1,
    "rest_envelope_v2": RestEnvelopeV2,
    "connection_transition_v1": ConnectionTransitionV1,
    "coverage_transition_v1": CoverageTransitionV1,
}


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _list_of_mappings(value: object) -> list[dict[str, str]]:
    assert isinstance(value, list)
    assert all(isinstance(item, dict) for item in value)
    return cast(list[dict[str, str]], value)


@pytest.mark.parametrize(("schema_version", "model"), _EXPECTED_RECORD_MODELS.items())
def test_persisted_record_registry_detects_dataclass_field_and_type_drift(
    schema_version: str,
    model: type[object],
) -> None:
    registry = capture_schema_registry()
    schemas = _mapping(registry["persisted_record_schemas"])
    schema = _mapping(schemas[schema_version])

    assert _list_of_mappings(schema["fields"]) == dataclass_field_contract(model)


def test_segment_manifest_registry_detects_dataclass_field_and_type_drift() -> None:
    manifest = _mapping(capture_schema_registry()["segment_manifest"])

    assert _list_of_mappings(manifest["fields"]) == dataclass_field_contract(SegmentManifestV1)
    validate_runtime_schema_contracts()


def test_registry_contains_every_segment_record_schema_with_lifecycle() -> None:
    schemas = _mapping(capture_schema_registry()["persisted_record_schemas"])

    assert set(schemas) == set(_EXPECTED_RECORD_MODELS)
    assert _mapping(schemas["rest_envelope_v1"])["status"] == ("legacy_read_compatibility_only")
    assert _mapping(schemas["rest_envelope_v2"])["status"] == "active"


def test_receipt_fields_are_declared_and_coverage_receipt_is_inherited() -> None:
    schemas = _mapping(capture_schema_registry()["persisted_record_schemas"])
    for schema_value in schemas.values():
        schema = _mapping(schema_value)
        names = {field["name"] for field in _list_of_mappings(schema["fields"])}
        receipt = _mapping(schema["authoritative_receipt"])
        assert receipt["wall_time_field"] in names
        assert receipt["monotonic_time_field"] in names
        assert receipt["ingest_order_field"] in names

    coverage = _mapping(schemas["coverage_transition_v1"])
    seam = _mapping(coverage["authoritative_receipt"])["seam"]
    assert isinstance(seam, str)
    assert "not_the_failure_detection_time" in seam

    rest_v2 = _mapping(schemas["rest_envelope_v2"])
    rest_seam = _mapping(rest_v2["authoritative_receipt"])["seam"]
    assert isinstance(rest_seam, str)
    assert "after_bounded_response_close" in rest_seam
    assert "shared_ingest_sequence_allocation" in rest_seam


def test_stable_type_adapter_covers_nested_arrays_enums_and_nullable_fields() -> None:
    fields_by_name = {
        field["name"]: field["type"] for field in dataclass_field_contract(RestEnvelopeV2)
    }

    assert fields_by_name["canonical_query"] == "array<array<string,string>>"
    assert fields_by_name["market"] == "string-enum<signalbot.domain.enums.Market>"
    assert fields_by_name["response_status"] == "integer|null"
    assert fields_by_name["error_category"] == (
        "string-enum<signalbot.capture.models.RestErrorCategory>|null"
    )


def test_registry_is_json_compatible_mutation_isolated_and_hash_deterministic() -> None:
    first = capture_schema_registry()
    encoded = json.dumps(
        first,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    first["schema_version"] = "mutated_in_test"

    assert capture_schema_registry()["schema_version"] == "capture_schema_registry_v1"
    assert canonical_registry_bytes() == encoded
    assert capture_schema_registry_sha256() == hashlib.sha256(encoded).hexdigest()
    assert capture_schema_registry_sha256() == (
        "0095c40fc3e14f13763de0a5e9454b0d640e263ad6c79be85c46b0183519d099"
    )


def test_outer_frame_registry_matches_storage_contract() -> None:
    outer = _mapping(capture_schema_registry()["outer_frame_v1"])

    assert outer["format_version"] == storage._OUTER_FRAME_FORMAT_VERSION
    assert outer["magic_ascii"] == storage._OUTER_FRAME_MAGIC.decode("ascii")
    assert outer["header_core_struct"] == storage._OUTER_FRAME_HEADER_CORE.format
    assert outer["header_core_bytes"] == struct.calcsize(">8sBQQ32s")
    assert outer["header_total_bytes"] == storage._OUTER_FRAME_HEADER_SIZE
    assert _mapping(outer["payload_contract"])["zstd_content_checksum_required"] is True


def test_depth_contract_never_synthesizes_spot_pu() -> None:
    registry = capture_schema_registry()
    contracts = _mapping(registry["depth_sequence_contracts"])
    spot = _mapping(contracts["spot"])
    futures = _mapping(contracts["usd_m_futures"])

    assert spot["stream_sequence_fields"] == ["U", "u"]
    assert spot["previous_event_final_update_field"] is None
    assert spot["synthesized_fields_forbidden"] == ["pu"]
    assert _mapping(spot["snapshot_bridge"])["authority"] == "DESIGN"
    assert futures["stream_sequence_fields"] == ["U", "u", "pu"]
    assert futures["authority"] == "OFFICIAL"
    assert _mapping(spot["configured_snapshot_request"]) == {
        "authority": "OFFICIAL_CURRENT_REVALIDATED",
        "path": "/api/v3/depth",
        "fixed_limit": 5_000,
        "maximum_query_limit": 5_000,
        "request_weight": 250,
    }
    assert _mapping(futures["configured_snapshot_request"]) == {
        "authority": "OFFICIAL_CURRENT_REVALIDATED",
        "path": "/fapi/v1/depth",
        "fixed_limit": 1_000,
        "maximum_query_limit": 1_000,
        "request_weight": 20,
    }
    official_spot_bootstrap = _mapping(spot["official_revalidated_snapshot_bootstrap"])
    assert official_spot_bootstrap["resnapshot_if"] == "lastUpdateId < first_buffered_U"
    assert official_spot_bootstrap["first_event_bridge"] == "U <= lastUpdateId <= u"
    assert official_spot_bootstrap["operational_snapshot_cycle_cap"] == 3
    assert official_spot_bootstrap["operational_bridge_wait_seconds"] == 2
    futures_bridge = _mapping(futures["snapshot_bridge"])
    assert futures_bridge["operational_snapshot_cycle_cap"] == 3
    assert futures_bridge["operational_bridge_wait_seconds"] == 2
    assert futures_bridge["cap_exhaustion_action"] == "quarantine_not_http_retry"

    issue = _mapping(registry["downloaded_protocol_depth_issue"])
    interim = _mapping(issue["safe_interim_decision"])
    assert issue["generic_sequence_fields"] == ["U", "u", "pu"]
    assert interim == {
        "spot_family_B_status": "INCONCLUSIVE_DATA",
        "spot_family_B_enabled": False,
        "spot_pu_synthesis": "forbidden",
    }
    assert issue["prospective_holdout_started"] is False
    revalidation = _mapping(issue["current_depth_revalidation_artifact"])
    assert revalidation["sha256"] == hashlib.sha256(_DEPTH_REVALIDATION.read_bytes()).hexdigest()
    conflict = _mapping(issue["original_research_conflict"])
    assert conflict == {
        "preserved": True,
        "old_common_snapshot_limit": 1_000,
        "current_spot_snapshot_limit": 5_000,
        "current_futures_snapshot_limit": 1_000,
        "resolution_scope": "capture_configuration_only_no_family_B_enablement",
    }


def test_errata_binds_unchanged_inputs_and_disables_spot_family_b() -> None:
    raw = yaml.safe_load(_ERRATA.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    errata = cast(dict[str, object], raw)
    protocol = _mapping(errata["downloaded_protocol"])
    research = _mapping(errata["official_research_artifact"])
    interim = _mapping(errata["safe_interim_decision"])

    protocol_path = _ROOT / str(protocol["path"])
    research_path = _ROOT / str(research["path"])
    assert hashlib.sha256(protocol_path.read_bytes()).hexdigest() == protocol["sha256"]
    assert hashlib.sha256(research_path.read_bytes()).hexdigest() == research["sha256"]
    assert protocol["mutated_by_this_errata"] is False
    assert interim["spot_family_B_enabled"] is False
    assert interim["spot_family_B_status"] == "INCONCLUSIVE_DATA"
    assert interim["synthesize_spot_pu"] == "forbidden"
    assert errata["prospective_holdout_started"] is False
    assert "pnl" not in _ERRATA.read_text(encoding="utf-8").casefold()
    assert "efficacy" not in _ERRATA.read_text(encoding="utf-8").casefold()
