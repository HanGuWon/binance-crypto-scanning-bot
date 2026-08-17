from __future__ import annotations

import json
from dataclasses import replace

import pytest

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.paper_sizing import PaperSizingCellV2
from signalbot.r4b_v2.execution.prospective_outcome_wal_record import (
    FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2,
    FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2,
    POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2,
    POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2,
    POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2,
    POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
    ProspectiveOutcomeWalRecordContractErrorV2,
    ProspectiveOutcomeWalRecordKindV2,
    build_prospective_outcome_wal_record_v2,
    parse_prospective_outcome_wal_record_v2,
    prospective_outcome_id_v2,
    verify_prospective_outcome_wal_successor_v2,
)

PLAN = "a" * 64
SEGMENT = "b" * 64
CELL = "c" * 64

SCHEMA_BY_KIND = {
    ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE: (
        POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION: (
        POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_PREPARE: (FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2),
    ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_DISPOSITION: (
        FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW: (POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2),
    ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL: (POSITION_TERMINAL_PAYLOAD_SCHEMA_V2),
}


def _payload(schema: str, *, marker: str = "x") -> bytes:
    return canonical_json_line(
        {
            "marker": marker,
            "production_order_placement": False,
            "schema_version": schema,
        }
    )


def _record(
    kind: ProspectiveOutcomeWalRecordKindV2,
    *,
    ingest_seq: int = 1,
    previous: str | None = None,
    cell: str = CELL,
    sizing_cell: PaperSizingCellV2 = PaperSizingCellV2.NOTIONAL_100_USDT,
):
    schema = SCHEMA_BY_KIND[kind]
    return build_prospective_outcome_wal_record_v2(
        ingest_seq=ingest_seq,
        kind=kind,
        attempt_plan_sha256=PLAN,
        origin_segment_id=SEGMENT,
        origin_cell_id=cell,
        sizing_cell=sizing_cell,
        payload_schema=schema,
        canonical_payload_jsonl=_payload(schema, marker=kind.value),
        previous_record_sha256=previous,
    )


@pytest.mark.parametrize("kind", tuple(ProspectiveOutcomeWalRecordKindV2))
def test_all_outcome_kinds_round_trip_exactly(
    kind: ProspectiveOutcomeWalRecordKindV2,
) -> None:
    record = _record(kind)

    assert parse_prospective_outcome_wal_record_v2(record.encoded_line) == record
    assert record.outcome_id == prospective_outcome_id_v2(
        attempt_plan_sha256=PLAN,
        origin_segment_id=SEGMENT,
        origin_cell_id=CELL,
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
    )
    assert record.production_order_placement is False


def test_outcome_id_changes_with_cell_or_sizing_but_not_record_kind() -> None:
    first = _record(ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE)
    later_kind = _record(ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL)
    other_cell = _record(
        ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE,
        cell="d" * 64,
    )
    other_size = _record(
        ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE,
        sizing_cell=PaperSizingCellV2.NOTIONAL_1000_USDT,
    )

    assert first.outcome_id == later_kind.outcome_id
    assert first.outcome_id != other_cell.outcome_id
    assert first.outcome_id != other_size.outcome_id


def test_successor_requires_same_attempt_adjacent_sequence_and_hash() -> None:
    first = _record(ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE)
    second = _record(
        ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION,
        ingest_seq=2,
        previous=first.record_sha256,
    )
    verify_prospective_outcome_wal_successor_v2(first, second)

    wrong = json.loads(second.encoded_line)
    wrong["previous_record_sha256"] = "f" * 64
    with pytest.raises(ProspectiveOutcomeWalRecordContractErrorV2):
        parse_prospective_outcome_wal_record_v2(canonical_json_line(wrong))


def test_unknown_field_payload_tamper_and_noncanonical_bytes_fail_closed() -> None:
    record = _record(ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW)
    unknown = json.loads(record.encoded_line)
    unknown["unknown"] = True
    with pytest.raises(
        ProspectiveOutcomeWalRecordContractErrorV2,
        match="missing or unknown",
    ):
        parse_prospective_outcome_wal_record_v2(canonical_json_line(unknown))

    payload_tamper = json.loads(record.encoded_line)
    payload_tamper["payload"]["marker"] = "tampered"
    with pytest.raises(ProspectiveOutcomeWalRecordContractErrorV2):
        parse_prospective_outcome_wal_record_v2(canonical_json_line(payload_tamper))

    with pytest.raises(
        ProspectiveOutcomeWalRecordContractErrorV2,
        match="canonical",
    ):
        parse_prospective_outcome_wal_record_v2(
            json.dumps(json.loads(record.encoded_line)).encode() + b"\n"
        )


def test_factory_seal_and_payload_byte_bound_reject_forgery() -> None:
    record = _record(ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL)
    with pytest.raises(
        ProspectiveOutcomeWalRecordContractErrorV2,
        match="factory-sealed",
    ):
        replace(record, record_sha256="f" * 64)

    with pytest.raises(
        ProspectiveOutcomeWalRecordContractErrorV2,
        match="payload exceeds",
    ):
        build_prospective_outcome_wal_record_v2(
            ingest_seq=1,
            kind=ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL,
            attempt_plan_sha256=PLAN,
            origin_segment_id=SEGMENT,
            origin_cell_id=CELL,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            payload_schema=POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
            canonical_payload_jsonl=_payload(
                POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
                marker="x" * 65_536,
            ),
            previous_record_sha256=None,
        )

    binary_float = (
        b'{"marker":1.5,"production_order_placement":false,'
        b'"schema_version":"r4b_v2_prospective_position_terminal_payload_v2"}\n'
    )
    with pytest.raises(
        ProspectiveOutcomeWalRecordContractErrorV2,
        match="unsupported canonical JSON",
    ):
        build_prospective_outcome_wal_record_v2(
            ingest_seq=1,
            kind=ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL,
            attempt_plan_sha256=PLAN,
            origin_segment_id=SEGMENT,
            origin_cell_id=CELL,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            payload_schema=POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
            canonical_payload_jsonl=binary_float,
            previous_record_sha256=None,
        )


def test_genesis_schema_and_jcs_integer_boundaries_are_strict() -> None:
    with pytest.raises(
        ProspectiveOutcomeWalRecordContractErrorV2,
        match="genesis",
    ):
        _record(
            ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE,
            previous="e" * 64,
        )
    with pytest.raises(
        ProspectiveOutcomeWalRecordContractErrorV2,
        match="JCS-safe",
    ):
        build_prospective_outcome_wal_record_v2(
            ingest_seq=9_007_199_254_740_992,
            kind=ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE,
            attempt_plan_sha256=PLAN,
            origin_segment_id=SEGMENT,
            origin_cell_id=CELL,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            payload_schema=POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2,
            canonical_payload_jsonl=_payload(POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2),
            previous_record_sha256=None,
        )
    with pytest.raises(
        ProspectiveOutcomeWalRecordContractErrorV2,
        match="payload_schema",
    ):
        build_prospective_outcome_wal_record_v2(
            ingest_seq=1,
            kind=ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL,
            attempt_plan_sha256=PLAN,
            origin_segment_id=SEGMENT,
            origin_cell_id=CELL,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            payload_schema=POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2,
            canonical_payload_jsonl=_payload(POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2),
            previous_record_sha256=None,
        )
