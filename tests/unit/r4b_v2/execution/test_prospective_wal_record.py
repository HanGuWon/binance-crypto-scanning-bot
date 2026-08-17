from __future__ import annotations

import hashlib
import json
from dataclasses import fields, replace

import pytest

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.wal import WalQueuedRecordV2
from signalbot.r4b_v2.execution.prospective_wal_record import (
    CELL_DISPOSITION_PAYLOAD_SCHEMA_V2,
    DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
    MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2,
    PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
    PROSPECTIVE_WAL_RECORD_SCHEMA_V2,
    ProspectiveWalRecordContractErrorV2,
    ProspectiveWalRecordKindV2,
    ProspectiveWalRecordV2,
    build_prospective_wal_record_v2,
    parse_prospective_wal_record_v2,
    verify_prospective_wal_successor_v2,
)

PLAN_SHA = "a" * 64
OTHER_PLAN_SHA = "b" * 64
SEGMENT_SHA = "c" * 64
OTHER_SEGMENT_SHA = "d" * 64
CELL_SHA = "e" * 64
OTHER_CELL_SHA = "f" * 64

SCHEMA_BY_KIND = {
    ProspectiveWalRecordKindV2.DECISION_PREPARE: (
        DECISION_PREPARE_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveWalRecordKindV2.CELL_DISPOSITION: (
        CELL_DISPOSITION_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveWalRecordKindV2.PAPER_TERMINAL: PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
}


def _payload(
    schema: str = DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
    **extra: object,
) -> bytes:
    return canonical_json_line({"schema_version": schema, **extra})


def _record(
    *,
    ingest_seq: int = 1,
    kind: ProspectiveWalRecordKindV2 = (
        ProspectiveWalRecordKindV2.DECISION_PREPARE
    ),
    attempt_plan_sha256: str = PLAN_SHA,
    segment_id: str = SEGMENT_SHA,
    cell_id: str = CELL_SHA,
    payload: bytes | None = None,
    previous_record_sha256: str | None = None,
) -> ProspectiveWalRecordV2:
    schema = SCHEMA_BY_KIND[kind]
    return build_prospective_wal_record_v2(
        ingest_seq=ingest_seq,
        kind=kind,
        attempt_plan_sha256=attempt_plan_sha256,
        segment_id=segment_id,
        cell_id=cell_id,
        payload_schema=schema,
        canonical_payload_jsonl=(
            _payload(schema, disposition="SIGNAL_LONG")
            if payload is None
            else payload
        ),
        previous_record_sha256=previous_record_sha256,
    )


@pytest.mark.parametrize("kind", tuple(ProspectiveWalRecordKindV2))
def test_factory_builds_exact_canonical_wal_protocol_record(
    kind: ProspectiveWalRecordKindV2,
) -> None:
    schema = SCHEMA_BY_KIND[kind]
    record = _record(kind=kind, payload=_payload(schema, reason="closed-candle"))
    queued: WalQueuedRecordV2 = record

    queued.verify_integrity()
    document = json.loads(record.encoded_line)
    assert document == {
        "attempt_plan_sha256": PLAN_SHA,
        "cell_id": CELL_SHA,
        "ingest_seq": 1,
        "kind": kind.value,
        "payload": {"reason": "closed-candle", "schema_version": schema},
        "payload_schema": schema,
        "payload_sha256": hashlib.sha256(
            _payload(schema, reason="closed-candle")
        ).hexdigest(),
        "previous_record_sha256": None,
        "record_sha256": record.record_sha256,
        "schema_version": PROSPECTIVE_WAL_RECORD_SCHEMA_V2,
        "segment_id": SEGMENT_SHA,
    }
    assert record.encoded_line == canonical_json_line(document)
    assert record.encoded_len == len(record.encoded_line)
    assert record.encoded_sha256 == hashlib.sha256(record.encoded_line).hexdigest()
    assert record.payload == document["payload"]


def test_payload_property_returns_a_copy_not_mutable_record_state() -> None:
    record = _record()
    exposed = record.payload
    exposed["disposition"] = "TAMPERED"

    assert record.payload["disposition"] == "SIGNAL_LONG"
    record.verify_integrity()


def test_direct_construction_and_dataclass_replace_cannot_bypass_factory() -> None:
    record = _record()
    constructor_values = {
        item.name: getattr(record, item.name)
        for item in fields(record)
        if item.init
    }

    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="factory-sealed"):
        ProspectiveWalRecordV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="factory-sealed"):
        replace(record, cell_id=OTHER_CELL_SHA)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"schema_version":"r4b_v2_prospective_decision_prepare_payload_v2"}',
        b'{"schema_version": "r4b_v2_prospective_decision_prepare_payload_v2"}\n',
        b'{"schema_version":"r4b_v2_prospective_decision_prepare_payload_v2"}\n\n',
        b'["r4b_v2_prospective_decision_prepare_payload_v2"]\n',
        b'\xff\n',
    ],
)
def test_factory_rejects_noncanonical_nonobject_or_non_utf8_payloads(
    payload: bytes,
) -> None:
    with pytest.raises(ProspectiveWalRecordContractErrorV2):
        _record(payload=payload)


def test_factory_rejects_mutable_payload_bytes() -> None:
    with pytest.raises(
        ProspectiveWalRecordContractErrorV2,
        match="immutable bytes",
    ):
        build_prospective_wal_record_v2(
            ingest_seq=1,
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            attempt_plan_sha256=PLAN_SHA,
            segment_id=SEGMENT_SHA,
            cell_id=CELL_SHA,
            payload_schema=DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
            canonical_payload_jsonl=bytearray(_payload()),  # type: ignore[arg-type]
            previous_record_sha256=None,
        )


def test_factory_requires_exact_kind_schema_and_embedded_schema() -> None:
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="record-kind"):
        build_prospective_wal_record_v2(
            ingest_seq=1,
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            attempt_plan_sha256=PLAN_SHA,
            segment_id=SEGMENT_SHA,
            cell_id=CELL_SHA,
            payload_schema=CELL_DISPOSITION_PAYLOAD_SCHEMA_V2,
            canonical_payload_jsonl=_payload(CELL_DISPOSITION_PAYLOAD_SCHEMA_V2),
            previous_record_sha256=None,
        )
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="schema_version"):
        build_prospective_wal_record_v2(
            ingest_seq=1,
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            attempt_plan_sha256=PLAN_SHA,
            segment_id=SEGMENT_SHA,
            cell_id=CELL_SHA,
            payload_schema=DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
            canonical_payload_jsonl=_payload(CELL_DISPOSITION_PAYLOAD_SCHEMA_V2),
            previous_record_sha256=None,
        )


def test_payload_size_boundary_accepts_64kib_and_rejects_one_byte_more() -> None:
    empty = _payload(blob="")
    exact = _payload(blob="x" * (MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2 - len(empty)))
    oversized = _payload(
        blob="x" * (MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2 - len(empty) + 1)
    )
    assert len(exact) == MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2
    assert len(oversized) == MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2 + 1

    assert _record(payload=exact).payload["blob"]
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="65536"):
        _record(payload=oversized)


@pytest.mark.parametrize("ingest_seq", [False, 0, -1, 9_007_199_254_740_992])
def test_ingest_sequence_must_be_positive_and_rfc8785_safe(
    ingest_seq: int,
) -> None:
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="ingest_seq"):
        _record(ingest_seq=ingest_seq)


def test_genesis_and_non_genesis_predecessor_contract_is_exact() -> None:
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="genesis"):
        _record(previous_record_sha256="1" * 64)
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="requires"):
        _record(ingest_seq=2)
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="lowercase"):
        _record(ingest_seq=2, previous_record_sha256="A" * 64)


def test_factory_rejects_invalid_plan_segment_and_cell_hashes() -> None:
    for field_name in ("attempt_plan_sha256", "segment_id", "cell_id"):
        arguments: dict[str, object] = {field_name: "A" * 64}
        with pytest.raises(
            ProspectiveWalRecordContractErrorV2,
            match=field_name,
        ):
            _record(**arguments)  # type: ignore[arg-type]


def test_adjacent_segment_chain_accepts_only_actual_contiguous_predecessor() -> None:
    first = _record()
    second = _record(
        ingest_seq=2,
        kind=ProspectiveWalRecordKindV2.CELL_DISPOSITION,
        cell_id=OTHER_CELL_SHA,
        previous_record_sha256=first.record_sha256,
    )
    verify_prospective_wal_successor_v2(first, second)

    wrong_hash = _record(
        ingest_seq=2,
        previous_record_sha256="1" * 64,
    )
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="actual predecessor"):
        verify_prospective_wal_successor_v2(first, wrong_hash)
    gap = _record(
        ingest_seq=3,
        previous_record_sha256=first.record_sha256,
    )
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="contiguous"):
        verify_prospective_wal_successor_v2(first, gap)
    other_segment = _record(
        ingest_seq=2,
        segment_id=OTHER_SEGMENT_SHA,
        previous_record_sha256=first.record_sha256,
    )
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="segment_id"):
        verify_prospective_wal_successor_v2(first, other_segment)
    other_plan = _record(
        ingest_seq=2,
        attempt_plan_sha256=OTHER_PLAN_SHA,
        previous_record_sha256=first.record_sha256,
    )
    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="attempt_plan"):
        verify_prospective_wal_successor_v2(first, other_plan)


def test_payload_change_changes_payload_record_and_envelope_hashes() -> None:
    first = _record(payload=_payload(disposition="SIGNAL_LONG"))
    changed = _record(payload=_payload(disposition="NO_SIGNAL"))

    assert changed.payload_sha256 != first.payload_sha256
    assert changed.record_sha256 != first.record_sha256
    assert changed.encoded_sha256 != first.encoded_sha256


@pytest.mark.parametrize(
    ("field_name", "replacement", "match"),
    [
        ("payload_jsonl", _payload(disposition="NO_SIGNAL"), "payload_sha256"),
        ("record_sha256", "0" * 64, "record_sha256"),
        ("_encoded_len", 1, "encoded_len"),
        ("_encoded_sha256", "0" * 64, "encoded_sha256"),
        ("_encoded_line", b"{}\n", "encoded_len|encoded_line"),
    ],
)
def test_integrity_verification_detects_tampering(
    field_name: str,
    replacement: object,
    match: str,
) -> None:
    record = _record()
    object.__setattr__(record, field_name, replacement)

    with pytest.raises(ProspectiveWalRecordContractErrorV2, match=match):
        record.verify_integrity()


def test_strict_parser_round_trips_only_factory_equivalent_canonical_bytes() -> None:
    record = _record()
    parsed = parse_prospective_wal_record_v2(record.encoded_line)

    assert parsed == record
    assert parsed.encoded_line == record.encoded_line
    parsed.verify_integrity()


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ({"unknown": True}, "missing or unknown"),
        ({"payload_sha256": "0" * 64}, "stored payload_sha256"),
        ({"record_sha256": "0" * 64}, "stored record_sha256"),
        ({"kind": "UNKNOWN"}, "unsupported.*kind"),
    ],
)
def test_strict_parser_rejects_unknown_fields_and_tampered_hashes(
    mutation: dict[str, object],
    match: str,
) -> None:
    document = json.loads(_record().encoded_line)
    document.update(mutation)
    encoded = canonical_json_line(document)

    with pytest.raises(ProspectiveWalRecordContractErrorV2, match=match):
        parse_prospective_wal_record_v2(encoded)


def test_strict_parser_rejects_noncanonical_envelope() -> None:
    document = json.loads(_record().encoded_line)
    noncanonical = json.dumps(document, sort_keys=False).encode("utf-8") + b"\n"
    assert noncanonical != _record().encoded_line

    with pytest.raises(ProspectiveWalRecordContractErrorV2, match="canonical"):
        parse_prospective_wal_record_v2(noncanonical)
