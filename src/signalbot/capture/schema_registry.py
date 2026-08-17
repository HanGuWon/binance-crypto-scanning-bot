from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import fields, is_dataclass
from enum import Enum
from types import UnionType
from typing import Union, get_args, get_origin, get_type_hints

from signalbot.capture.config import (
    FUTURES_DEPTH_SNAPSHOT_LIMIT,
    FUTURES_DEPTH_SNAPSHOT_REQUEST_WEIGHT,
    SPOT_DEPTH_SNAPSHOT_LIMIT,
    SPOT_DEPTH_SNAPSHOT_REQUEST_WEIGHT,
)
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    ConnectionTransitionV1,
    CoverageTransitionV1,
    RestEnvelopeV1,
    RestEnvelopeV2,
)
from signalbot.capture.storage import SegmentManifestV1

_DOWNLOADED_PROTOCOL_SHA256 = "9c925f5988e65a1371e8859dd00ea6a61db0c3b9ea34622432e9a28a3bab297b"
_OFFICIAL_RESEARCH_ARTIFACT_SHA256 = (
    "c2bac0a16aa13ea449c5cead80b5733b0745d69943f332f86562d9c8325b07c6"
)
_DEPTH_REVALIDATION_ARTIFACT_SHA256 = (
    "9e513ece7e1f26f29afc1d26b41731c5b8a64a0c56771dd2c9ccc8efd27b2994"
)

_MODEL_FIELD_CONTRACTS: dict[str, tuple[tuple[str, str], ...]] = {
    "capture_envelope_v1": (
        ("received_at_ms", "integer"),
        ("received_monotonic_ns", "integer"),
        ("plan_sha256", "string"),
        ("process_boot_id", "string"),
        ("connection_id", "string"),
        ("frame_seq", "integer"),
        ("ingest_seq", "integer"),
        ("market", "string-enum<signalbot.domain.enums.Market>"),
        ("route", "string"),
        ("stream", "string"),
        ("subscription_streams", "array<string>"),
        ("raw_payload", "string"),
        (
            "raw_payload_encoding",
            "string-enum<signalbot.capture.models.RawPayloadEncoding>",
        ),
        ("schema_version", "string"),
        ("source", "string"),
        ("transport", "string"),
    ),
    "rest_envelope_v1": (
        ("request_started_at_ms", "integer"),
        ("request_started_monotonic_ns", "integer"),
        ("response_received_at_ms", "integer"),
        ("response_received_monotonic_ns", "integer"),
        ("plan_sha256", "string"),
        ("process_boot_id", "string"),
        ("request_id", "string"),
        ("attempt", "integer"),
        ("ingest_seq", "integer"),
        ("market", "string-enum<signalbot.domain.enums.Market>"),
        ("endpoint_path", "string"),
        ("canonical_query", "array<array<string,string>>"),
        ("response_status", "integer|null"),
        ("raw_payload", "string"),
        (
            "raw_payload_encoding",
            "string-enum<signalbot.capture.models.RawPayloadEncoding>",
        ),
        ("error", "string|null"),
        ("schema_version", "string"),
        ("source", "string"),
        ("transport", "string"),
    ),
    "rest_envelope_v2": (
        ("request_started_at_ms", "integer"),
        ("request_started_monotonic_ns", "integer"),
        ("response_first_byte_at_ms", "integer|null"),
        ("response_first_byte_monotonic_ns", "integer|null"),
        ("response_completed_at_ms", "integer"),
        ("response_completed_monotonic_ns", "integer"),
        ("plan_sha256", "string"),
        ("process_boot_id", "string"),
        ("request_role", "string"),
        ("correlation_id", "string"),
        ("attempt", "integer"),
        ("ingest_seq", "integer"),
        ("market", "string-enum<signalbot.domain.enums.Market>"),
        ("endpoint_path", "string"),
        ("canonical_query", "array<array<string,string>>"),
        ("response_status", "integer|null"),
        ("response_headers", "array<array<string,string>>"),
        ("payload_complete", "boolean"),
        ("raw_payload", "string"),
        (
            "raw_payload_encoding",
            "string-enum<signalbot.capture.models.RawPayloadEncoding>",
        ),
        (
            "error_category",
            "string-enum<signalbot.capture.models.RestErrorCategory>|null",
        ),
        ("error_detail", "string|null"),
        ("schema_version", "string"),
        ("source", "string"),
        ("transport", "string"),
    ),
    "connection_transition_v1": (
        ("received_at_ms", "integer"),
        ("received_monotonic_ns", "integer"),
        ("plan_sha256", "string"),
        ("process_boot_id", "string"),
        ("connection_id", "string"),
        ("ingest_seq", "integer"),
        ("last_frame_seq", "integer"),
        ("market", "string-enum<signalbot.domain.enums.Market>"),
        ("route", "string"),
        ("streams", "array<string>"),
        (
            "state",
            "string-enum<signalbot.capture.models.ConnectionState>",
        ),
        ("reason", "string"),
        ("close_code", "integer|null"),
        ("schema_version", "string"),
        ("source", "string"),
    ),
    "coverage_transition_v1": (
        ("received_at_ms", "integer"),
        ("received_monotonic_ns", "integer"),
        ("plan_sha256", "string"),
        ("process_boot_id", "string"),
        ("connection_id", "string"),
        ("frame_seq", "integer"),
        ("ingest_seq", "integer"),
        ("market", "string-enum<signalbot.domain.enums.Market>"),
        ("route", "string"),
        ("stream", "string"),
        ("state", "string-enum<signalbot.capture.models.CoverageState>"),
        ("reason", "string-enum<signalbot.capture.models.CoverageReason>"),
        ("detail", "string"),
        ("schema_version", "string"),
        ("source", "string"),
    ),
    "capture_segment_manifest_v1": (
        ("data_file", "string"),
        ("sequence", "integer"),
        ("bucket_start_ms", "integer"),
        ("rotation_interval_ms", "integer"),
        ("plan_sha256", "string"),
        ("process_boot_id", "string"),
        ("first_received_at_ms", "integer"),
        ("last_received_at_ms", "integer"),
        ("first_ingest_seq", "integer"),
        ("last_ingest_seq", "integer"),
        ("record_count", "integer"),
        ("frame_count", "integer"),
        ("uncompressed_bytes", "integer"),
        ("compressed_bytes", "integer"),
        ("sha256", "string"),
        ("previous_segment_sha256", "string|null"),
        ("recovered_from_partial", "boolean"),
        ("frame_format_version", "integer"),
        ("schema_version", "string"),
    ),
}

_SCHEMA_MODELS: dict[str, type[object]] = {
    "capture_envelope_v1": CaptureEnvelopeV1,
    "rest_envelope_v1": RestEnvelopeV1,
    "rest_envelope_v2": RestEnvelopeV2,
    "connection_transition_v1": ConnectionTransitionV1,
    "coverage_transition_v1": CoverageTransitionV1,
    "capture_segment_manifest_v1": SegmentManifestV1,
}


def _field_documents(schema_version: str) -> list[dict[str, str]]:
    return [
        {"name": name, "type": type_description}
        for name, type_description in _MODEL_FIELD_CONTRACTS[schema_version]
    ]


_CAPTURE_SCHEMA_REGISTRY: dict[str, object] = {
    "schema_version": "capture_schema_registry_v1",
    "scope": "public_market_data_capture_contract",
    "persisted_record_schemas": {
        "capture_envelope_v1": {
            "status": "active",
            "python_dataclass": "signalbot.capture.models.CaptureEnvelopeV1",
            "fields": _field_documents("capture_envelope_v1"),
            "authoritative_receipt": {
                "wall_time_field": "received_at_ms",
                "monotonic_time_field": "received_monotonic_ns",
                "ingest_order_field": "ingest_seq",
                "seam": (
                    "first_statement_after_the_websocket_iterator_yields_the_raw_frame;"
                    "before_payload_decode_or_pipeline_offer"
                ),
            },
        },
        "rest_envelope_v1": {
            "status": "legacy_read_compatibility_only",
            "python_dataclass": "signalbot.capture.models.RestEnvelopeV1",
            "fields": _field_documents("rest_envelope_v1"),
            "authoritative_receipt": {
                "wall_time_field": "response_received_at_ms",
                "monotonic_time_field": "response_received_monotonic_ns",
                "ingest_order_field": "ingest_seq",
                "seam": (
                    "legacy_persisted_response_receipt;no_first_byte_or_completion_split;"
                    "must_not_be_reinterpreted_as_first_byte_time"
                ),
            },
        },
        "rest_envelope_v2": {
            "status": "active",
            "python_dataclass": "signalbot.capture.models.RestEnvelopeV2",
            "fields": _field_documents("rest_envelope_v2"),
            "authoritative_receipt": {
                "wall_time_field": "response_completed_at_ms",
                "monotonic_time_field": "response_completed_monotonic_ns",
                "ingest_order_field": "ingest_seq",
                "seam": (
                    "normal_path_after_bounded_response_close_and_immediately_before_"
                    "shared_ingest_sequence_allocation_and_pipeline_offer;"
                    "error_paths_at_their_terminal_no_await_admission_seam"
                ),
                "supporting_clock_samples": {
                    "request_start": [
                        "request_started_at_ms",
                        "request_started_monotonic_ns",
                    ],
                    "response_headers_available_proxy": [
                        "response_first_byte_at_ms",
                        "response_first_byte_monotonic_ns",
                    ],
                },
            },
        },
        "connection_transition_v1": {
            "status": "active",
            "python_dataclass": "signalbot.capture.models.ConnectionTransitionV1",
            "fields": _field_documents("connection_transition_v1"),
            "authoritative_receipt": {
                "wall_time_field": "received_at_ms",
                "monotonic_time_field": "received_monotonic_ns",
                "ingest_order_field": "ingest_seq",
                "seam": (
                    "sampled_when_the_owner_emits_the_observed_connection_state;"
                    "before_ingest_sequence_allocation_and_pipeline_offer"
                ),
            },
        },
        "coverage_transition_v1": {
            "status": "active",
            "python_dataclass": "signalbot.capture.models.CoverageTransitionV1",
            "fields": _field_documents("coverage_transition_v1"),
            "authoritative_receipt": {
                "wall_time_field": "received_at_ms",
                "monotonic_time_field": "received_monotonic_ns",
                "ingest_order_field": "ingest_seq",
                "seam": (
                    "copied_from_the_affected_source_record_by_invalidation_for_record;"
                    "not_the_failure_detection_time"
                ),
            },
        },
    },
    "segment_manifest": {
        "schema_version": "capture_segment_manifest_v1",
        "python_dataclass": "signalbot.capture.storage.SegmentManifestV1",
        "fields": _field_documents("capture_segment_manifest_v1"),
        "receipt_semantics": {
            "first_received_at_ms": "minimum_ordered_record_receipt_in_segment",
            "last_received_at_ms": "maximum_ordered_record_receipt_in_segment",
            "own_receipt_timestamp": False,
        },
        "chain": {
            "data_digest_field": "sha256",
            "previous_data_digest_field": "previous_segment_sha256",
            "frame_format_binding_field": "frame_format_version",
        },
    },
    "outer_frame_v1": {
        "format_version": 1,
        "magic_ascii": "SBCAPFRM",
        "byte_order": "big_endian",
        "header_core_struct": ">8sBQQ32s",
        "header_core_bytes": 57,
        "header_digest_algorithm": "sha256",
        "header_digest_bytes": 32,
        "header_total_bytes": 89,
        "header_core_fields": [
            {"name": "magic", "encoding": "fixed_ascii", "bytes": 8},
            {"name": "format_version", "encoding": "unsigned_integer", "bytes": 1},
            {
                "name": "compressed_length",
                "encoding": "unsigned_integer",
                "bytes": 8,
            },
            {
                "name": "uncompressed_length",
                "encoding": "unsigned_integer",
                "bytes": 8,
            },
            {
                "name": "compressed_sha256",
                "encoding": "raw_sha256_digest",
                "bytes": 32,
            },
        ],
        "header_digest_covers": "header_core_exact_bytes",
        "payload_contract": {
            "compression": "zstd",
            "zstd_content_checksum_required": True,
            "zstd_content_size_must_equal_outer_uncompressed_length": True,
            "compressed_sha256_must_match_header": True,
            "records_per_frame": 1,
            "decoded_record": "one_utf8_json_object_followed_by_one_newline",
        },
        "recovery_contract": {
            "recoverable_tail": (
                "complete_integrity_checked_outer_header_followed_by_short_payload"
            ),
            "fatal": [
                "incomplete_outer_header",
                "outer_header_digest_mismatch",
                "compressed_payload_digest_mismatch",
                "complete_outer_frame_with_torn_inner_zstd_frame",
                "decoded_length_mismatch",
                "multiple_or_unterminated_jsonl_records",
            ],
        },
    },
    "depth_sequence_contracts": {
        "spot": {
            "authority": "OFFICIAL_fields_with_DESIGN_snapshot_bridge",
            "stream_sequence_fields": ["U", "u"],
            "previous_event_final_update_field": None,
            "synthesized_fields_forbidden": ["pu"],
            "snapshot_field": "lastUpdateId",
            "configured_snapshot_request": {
                "authority": "OFFICIAL_CURRENT_REVALIDATED",
                "path": "/api/v3/depth",
                "fixed_limit": SPOT_DEPTH_SNAPSHOT_LIMIT,
                "maximum_query_limit": SPOT_DEPTH_SNAPSHOT_LIMIT,
                "request_weight": SPOT_DEPTH_SNAPSHOT_REQUEST_WEIGHT,
            },
            "official_revalidated_snapshot_bootstrap": {
                "authority": "OFFICIAL_CURRENT_REVALIDATED",
                "resnapshot_if": "lastUpdateId < first_buffered_U",
                "discard_buffered_if": "u <= lastUpdateId",
                "first_event_bridge": "U <= lastUpdateId <= u",
                "operational_snapshot_cycle_cap": 3,
                "operational_bridge_wait_seconds": 2,
                "cap_exhaustion_action": "quarantine_not_http_retry",
            },
            "snapshot_bridge": {
                "authority": "DESIGN",
                "discard_buffered_if": "u <= lastUpdateId",
                "local_id_initial_value": "lastUpdateId",
                "accept_first_event_only_if_all": [
                    "u > local_id",
                    "U <= local_id + 1",
                    "local_id + 1 <= u",
                ],
                "failure_action": "invalidate_local_book_and_resnapshot",
            },
            "subsequent_gap_rule": {
                "authority": "OFFICIAL",
                "gap_if": "U > local_id + 1",
                "old_event_if": "u < local_id",
                "after_apply": "local_id = u",
            },
        },
        "usd_m_futures": {
            "authority": "OFFICIAL",
            "stream_sequence_fields": ["U", "u", "pu"],
            "previous_event_final_update_field": "pu",
            "snapshot_field": "lastUpdateId",
            "configured_snapshot_request": {
                "authority": "OFFICIAL_CURRENT_REVALIDATED",
                "path": "/fapi/v1/depth",
                "fixed_limit": FUTURES_DEPTH_SNAPSHOT_LIMIT,
                "maximum_query_limit": FUTURES_DEPTH_SNAPSHOT_LIMIT,
                "request_weight": FUTURES_DEPTH_SNAPSHOT_REQUEST_WEIGHT,
            },
            "snapshot_bridge": {
                "authority": "OFFICIAL",
                "discard_buffered_if": "u < lastUpdateId",
                "accept_first_event_only_if_all": [
                    "U <= lastUpdateId",
                    "lastUpdateId <= u",
                ],
                "operational_snapshot_cycle_cap": 3,
                "operational_bridge_wait_seconds": 2,
                "cap_exhaustion_action": "quarantine_not_http_retry",
            },
            "subsequent_gap_rule": {
                "authority": "OFFICIAL",
                "continuity_required": "pu == previous.u",
                "failure_action": "invalidate_local_book_and_resnapshot",
            },
        },
    },
    "downloaded_protocol_depth_issue": {
        "downloaded_protocol_sha256": _DOWNLOADED_PROTOCOL_SHA256,
        "generic_requirement_path": ("families.B.local_book_contract.sequence_fields_required"),
        "generic_sequence_fields": ["U", "u", "pu"],
        "official_research_artifact": {
            "path": ("artifacts/research/2026-07-17/binance_public_capture_contract_20260717.md"),
            "sha256": _OFFICIAL_RESEARCH_ARTIFACT_SHA256,
        },
        "current_depth_revalidation_artifact": {
            "path": ("artifacts/research/2026-07-17/binance_depth_revalidation_20260717.md"),
            "sha256": _DEPTH_REVALIDATION_ARTIFACT_SHA256,
        },
        "original_research_conflict": {
            "preserved": True,
            "old_common_snapshot_limit": 1_000,
            "current_spot_snapshot_limit": SPOT_DEPTH_SNAPSHOT_LIMIT,
            "current_futures_snapshot_limit": FUTURES_DEPTH_SNAPSHOT_LIMIT,
            "resolution_scope": "capture_configuration_only_no_family_B_enablement",
        },
        "issue": (
            "the_generic_U_u_pu_requirement_cannot_be_satisfied_by_official_spot_depth;"
            "spot_has_U_u_only"
        ),
        "safe_interim_decision": {
            "spot_family_B_status": "INCONCLUSIVE_DATA",
            "spot_family_B_enabled": False,
            "spot_pu_synthesis": "forbidden",
        },
        "enablement_condition": (
            "independently_adjudicated_pre_holdout_protocol_revision_with_"
            "venue_specific_depth_sequence_contracts_and_regenerated_hashes"
        ),
        "downloaded_source_mutated": False,
        "prospective_holdout_started": False,
    },
}


def capture_schema_registry() -> dict[str, object]:
    """Return a mutation-isolated, JSON-compatible canonical registry document."""

    return copy.deepcopy(_CAPTURE_SCHEMA_REGISTRY)


def canonical_registry_bytes() -> bytes:
    """Encode the registry deterministically for later provenance binding."""

    return json.dumps(
        capture_schema_registry(),
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def capture_schema_registry_sha256() -> str:
    """Return the SHA-256 of the deterministic canonical registry bytes."""

    return hashlib.sha256(canonical_registry_bytes()).hexdigest()


def dataclass_field_contract(model: type[object]) -> list[dict[str, str]]:
    """Describe a supported dataclass using stable JSON-oriented type names."""

    if not is_dataclass(model):
        raise TypeError("model must be a dataclass type")
    hints = get_type_hints(model)
    return [
        {"name": item.name, "type": _stable_type_description(hints[item.name])}
        for item in fields(model)
    ]


def validate_runtime_schema_contracts() -> None:
    """Fail if a persisted dataclass drifts from the canonical literal registry."""

    for schema_version, model in _SCHEMA_MODELS.items():
        expected = _field_documents(schema_version)
        actual = dataclass_field_contract(model)
        if actual != expected:
            raise ValueError(f"persisted dataclass drift: {schema_version}")


def _stable_type_description(annotation: object) -> str:
    primitives = {
        int: "integer",
        str: "string",
        bool: "boolean",
        type(None): "null",
    }
    primitive = primitives.get(annotation)
    if primitive is not None:
        return primitive
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return f"string-enum<{annotation.__module__}.{annotation.__qualname__}>"

    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin is tuple:
        if len(arguments) == 2 and arguments[1] is Ellipsis:
            return f"array<{_stable_type_description(arguments[0])}>"
        return "array<" + ",".join(map(_stable_type_description, arguments)) + ">"
    if origin in {Union, UnionType}:
        descriptions = [_stable_type_description(argument) for argument in arguments]
        descriptions.sort(key=lambda item: (item == "null", item))
        return "|".join(descriptions)
    raise TypeError(f"unsupported persisted field annotation: {annotation!r}")
