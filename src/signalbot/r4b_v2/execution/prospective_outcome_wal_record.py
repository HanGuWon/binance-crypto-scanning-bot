"""Attempt-wide hash-chained WAL records for prospective position outcomes.

The decision WAL is sharded by the UTC day of the originating census cell.
Position evidence can become final much later, so it must not be appended back
to an old daily shard.  This module defines only the canonical attempt-wide
record envelope and transition vocabulary.  Typed lifecycle owners remain
responsible for validating payload semantics before a writer admits bytes.

No function here places an order, evaluates a strategy, or claims efficacy.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import InitVar, dataclass, field
from enum import StrEnum
from typing import Final, cast

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.paper_sizing import PaperSizingCellV2

PROSPECTIVE_OUTCOME_WAL_RECORD_SCHEMA_V2: Final = "r4b_v2_prospective_outcome_wal_record_v2"
PROSPECTIVE_OUTCOME_WAL_RULE_VERSION_V2: Final = "R4B_CAUSAL_V2.4.0_PROSPECTIVE_ATTEMPT_OUTCOME_WAL"
POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2: Final = (
    "r4b_v2_prospective_position_open_prepare_payload_v2"
)
POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2: Final = (
    "r4b_v2_prospective_position_open_disposition_payload_v2"
)
FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2: Final = "r4b_v2_prospective_family_exit_prepare_payload_v2"
FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2: Final = (
    "r4b_v2_prospective_family_exit_disposition_payload_v2"
)
POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2: Final = "r4b_v2_prospective_position_cashflow_payload_v2"
POSITION_TERMINAL_PAYLOAD_SCHEMA_V2: Final = "r4b_v2_prospective_position_terminal_payload_v2"
MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2: Final = 64 * 1024
MAX_PROSPECTIVE_OUTCOME_WAL_RECORD_BYTES_V2: Final = (
    MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2 + 4 * 1024
)

_OUTCOME_ID_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_OUTCOME_ID_V2\0"
_RECORD_HASH_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_OUTCOME_WAL_RECORD_V2\0"
_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_JCS_MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991
_RECORD_KEYS: Final = frozenset(
    {
        "attempt_plan_sha256",
        "ingest_seq",
        "kind",
        "origin_cell_id",
        "origin_segment_id",
        "outcome_id",
        "payload",
        "payload_schema",
        "payload_sha256",
        "previous_record_sha256",
        "record_sha256",
        "schema_version",
        "sizing_cell",
    }
)


class ProspectiveOutcomeWalRecordKindV2(StrEnum):
    """Complete position-lifecycle vocabulary for the attempt-wide WAL."""

    POSITION_OPEN_PREPARE = "POSITION_OPEN_PREPARE"
    POSITION_OPEN_DISPOSITION = "POSITION_OPEN_DISPOSITION"
    FAMILY_EXIT_PREPARE = "FAMILY_EXIT_PREPARE"
    FAMILY_EXIT_DISPOSITION = "FAMILY_EXIT_DISPOSITION"
    POSITION_CASHFLOW = "POSITION_CASHFLOW"
    POSITION_TERMINAL = "POSITION_TERMINAL"


_PAYLOAD_SCHEMA_BY_KIND: Final = {
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


class ProspectiveOutcomeWalRecordContractErrorV2(ValueError):
    """Raised when an attempt-wide outcome record is not exact."""


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeWalRecordV2:
    """Factory-sealed canonical record for one origin cell and sizing cell."""

    ingest_seq: int
    kind: ProspectiveOutcomeWalRecordKindV2
    attempt_plan_sha256: str
    origin_segment_id: str
    origin_cell_id: str
    sizing_cell: PaperSizingCellV2
    outcome_id: str
    payload_schema: str
    payload_jsonl: bytes = field(repr=False)
    payload_sha256: str
    previous_record_sha256: str | None
    record_sha256: str
    _encoded_line: bytes = field(repr=False)
    _encoded_sha256: str = field(repr=False)
    _factory_token: InitVar[object | None] = None
    schema_version: str = field(
        init=False,
        default=PROSPECTIVE_OUTCOME_WAL_RECORD_SCHEMA_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "outcome WAL records are factory-sealed"
            )
        self.verify_integrity()

    @property
    def payload(self) -> dict[str, object]:
        return _decode_canonical_payload(self.payload_jsonl)

    @property
    def encoded_line(self) -> bytes:
        return self._encoded_line

    @property
    def encoded_len(self) -> int:
        return len(self._encoded_line)

    @property
    def encoded_sha256(self) -> str:
        return self._encoded_sha256

    @property
    def production_order_placement(self) -> bool:
        return False

    def verify_integrity(self) -> None:
        _validate_positive_safe_integer(self.ingest_seq, "ingest_seq")
        if not isinstance(self.kind, ProspectiveOutcomeWalRecordKindV2):
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "kind must be ProspectiveOutcomeWalRecordKindV2"
            )
        for value, label in (
            (self.attempt_plan_sha256, "attempt_plan_sha256"),
            (self.origin_segment_id, "origin_segment_id"),
            (self.origin_cell_id, "origin_cell_id"),
            (self.outcome_id, "outcome_id"),
            (self.payload_sha256, "payload_sha256"),
            (self.record_sha256, "record_sha256"),
            (self._encoded_sha256, "encoded_sha256"),
        ):
            _validate_sha256(value, label)
        if not isinstance(self.sizing_cell, PaperSizingCellV2):
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "sizing_cell must be PaperSizingCellV2"
            )
        expected_outcome_id = prospective_outcome_id_v2(
            attempt_plan_sha256=self.attempt_plan_sha256,
            origin_segment_id=self.origin_segment_id,
            origin_cell_id=self.origin_cell_id,
            sizing_cell=self.sizing_cell,
        )
        if not hmac.compare_digest(self.outcome_id, expected_outcome_id):
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "outcome_id differs from its origin cell and sizing identity"
            )
        expected_payload_schema = _PAYLOAD_SCHEMA_BY_KIND[self.kind]
        if self.payload_schema != expected_payload_schema:
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "payload_schema does not match record kind"
            )
        if self.schema_version != PROSPECTIVE_OUTCOME_WAL_RECORD_SCHEMA_V2:
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "unsupported outcome WAL record schema"
            )
        payload = _decode_canonical_payload(self.payload_jsonl)
        if payload.get("schema_version") != self.payload_schema:
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "payload schema_version differs from payload_schema"
            )
        actual_payload_sha256 = hashlib.sha256(self.payload_jsonl).hexdigest()
        if not hmac.compare_digest(self.payload_sha256, actual_payload_sha256):
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "payload_sha256 differs from canonical payload bytes"
            )
        _validate_predecessor(self.ingest_seq, self.previous_record_sha256)
        document = _record_document(
            ingest_seq=self.ingest_seq,
            kind=self.kind,
            attempt_plan_sha256=self.attempt_plan_sha256,
            origin_segment_id=self.origin_segment_id,
            origin_cell_id=self.origin_cell_id,
            sizing_cell=self.sizing_cell,
            outcome_id=self.outcome_id,
            payload_schema=self.payload_schema,
            payload=payload,
            payload_sha256=self.payload_sha256,
            previous_record_sha256=self.previous_record_sha256,
        )
        expected_record_sha256 = _record_sha256(document)
        if not hmac.compare_digest(self.record_sha256, expected_record_sha256):
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "record_sha256 differs from canonical record content"
            )
        expected_line = canonical_json_line({**document, "record_sha256": self.record_sha256})
        if len(expected_line) > MAX_PROSPECTIVE_OUTCOME_WAL_RECORD_BYTES_V2:
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "encoded outcome record exceeds the fixed byte bound"
            )
        if self._encoded_line != expected_line:
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "cached encoded outcome record differs from canonical content"
            )
        expected_encoded_sha256 = hashlib.sha256(expected_line).hexdigest()
        if not hmac.compare_digest(self._encoded_sha256, expected_encoded_sha256):
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "cached encoded hash differs from canonical record bytes"
            )


def prospective_outcome_id_v2(
    *,
    attempt_plan_sha256: str,
    origin_segment_id: str,
    origin_cell_id: str,
    sizing_cell: PaperSizingCellV2,
) -> str:
    """Derive the global per-attempt lifecycle identity."""

    for value, label in (
        (attempt_plan_sha256, "attempt_plan_sha256"),
        (origin_segment_id, "origin_segment_id"),
        (origin_cell_id, "origin_cell_id"),
    ):
        _validate_sha256(value, label)
    if not isinstance(sizing_cell, PaperSizingCellV2):
        raise ProspectiveOutcomeWalRecordContractErrorV2("sizing_cell must be PaperSizingCellV2")
    return hashlib.sha256(
        _OUTCOME_ID_DOMAIN
        + canonical_json_line(
            {
                "attempt_plan_sha256": attempt_plan_sha256,
                "origin_cell_id": origin_cell_id,
                "origin_segment_id": origin_segment_id,
                "sizing_cell": sizing_cell.value,
            }
        )
    ).hexdigest()


def build_prospective_outcome_wal_record_v2(
    *,
    ingest_seq: int,
    kind: ProspectiveOutcomeWalRecordKindV2,
    attempt_plan_sha256: str,
    origin_segment_id: str,
    origin_cell_id: str,
    sizing_cell: PaperSizingCellV2,
    payload_schema: str,
    canonical_payload_jsonl: bytes,
    previous_record_sha256: str | None,
) -> ProspectiveOutcomeWalRecordV2:
    """Build one immutable record from already-typed canonical payload bytes."""

    _validate_positive_safe_integer(ingest_seq, "ingest_seq")
    if not isinstance(kind, ProspectiveOutcomeWalRecordKindV2):
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "kind must be ProspectiveOutcomeWalRecordKindV2"
        )
    expected_payload_schema = _PAYLOAD_SCHEMA_BY_KIND[kind]
    if payload_schema != expected_payload_schema:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "payload_schema does not match record kind"
        )
    _validate_predecessor(ingest_seq, previous_record_sha256)
    payload = _decode_canonical_payload(canonical_payload_jsonl)
    if payload.get("schema_version") != payload_schema:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "payload schema_version differs from payload_schema"
        )
    payload_sha256 = hashlib.sha256(canonical_payload_jsonl).hexdigest()
    outcome_id = prospective_outcome_id_v2(
        attempt_plan_sha256=attempt_plan_sha256,
        origin_segment_id=origin_segment_id,
        origin_cell_id=origin_cell_id,
        sizing_cell=sizing_cell,
    )
    document = _record_document(
        ingest_seq=ingest_seq,
        kind=kind,
        attempt_plan_sha256=attempt_plan_sha256,
        origin_segment_id=origin_segment_id,
        origin_cell_id=origin_cell_id,
        sizing_cell=sizing_cell,
        outcome_id=outcome_id,
        payload_schema=payload_schema,
        payload=payload,
        payload_sha256=payload_sha256,
        previous_record_sha256=previous_record_sha256,
    )
    record_sha256 = _record_sha256(document)
    encoded_line = canonical_json_line({**document, "record_sha256": record_sha256})
    if len(encoded_line) > MAX_PROSPECTIVE_OUTCOME_WAL_RECORD_BYTES_V2:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "encoded outcome record exceeds the fixed byte bound"
        )
    return ProspectiveOutcomeWalRecordV2(
        ingest_seq=ingest_seq,
        kind=kind,
        attempt_plan_sha256=attempt_plan_sha256,
        origin_segment_id=origin_segment_id,
        origin_cell_id=origin_cell_id,
        sizing_cell=sizing_cell,
        outcome_id=outcome_id,
        payload_schema=payload_schema,
        payload_jsonl=canonical_payload_jsonl,
        payload_sha256=payload_sha256,
        previous_record_sha256=previous_record_sha256,
        record_sha256=record_sha256,
        _encoded_line=encoded_line,
        _encoded_sha256=hashlib.sha256(encoded_line).hexdigest(),
        _factory_token=_FACTORY_TOKEN,
    )


def parse_prospective_outcome_wal_record_v2(
    encoded_line: bytes,
) -> ProspectiveOutcomeWalRecordV2:
    """Parse one exact canonical line and re-run all constructor checks."""

    if type(encoded_line) is not bytes:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "encoded outcome record must be immutable bytes"
        )
    if not encoded_line.endswith(b"\n") or b"\n" in encoded_line[:-1]:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "encoded outcome record must be one newline-terminated line"
        )
    if len(encoded_line) > MAX_PROSPECTIVE_OUTCOME_WAL_RECORD_BYTES_V2:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "encoded outcome record exceeds the fixed byte bound"
        )
    document = _decode_object(encoded_line, "outcome record")
    if frozenset(document) != _RECORD_KEYS:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "outcome record has missing or unknown fields"
        )
    try:
        canonical_record = canonical_json_line(document)
    except (TypeError, ValueError) as exc:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "outcome record contains unsupported canonical JSON"
        ) from exc
    if canonical_record != encoded_line:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "outcome record is not exact canonical JSONL"
        )
    try:
        kind = ProspectiveOutcomeWalRecordKindV2(_text(document, "kind"))
        sizing_cell = PaperSizingCellV2(_text(document, "sizing_cell"))
    except ValueError as exc:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "outcome record enum value is unsupported"
        ) from exc
    payload_object = document.get("payload")
    if not isinstance(payload_object, dict):
        raise ProspectiveOutcomeWalRecordContractErrorV2("outcome record payload must be an object")
    payload_jsonl = canonical_json_line(cast(dict[str, object], payload_object))
    record = ProspectiveOutcomeWalRecordV2(
        ingest_seq=_integer(document, "ingest_seq"),
        kind=kind,
        attempt_plan_sha256=_text(document, "attempt_plan_sha256"),
        origin_segment_id=_text(document, "origin_segment_id"),
        origin_cell_id=_text(document, "origin_cell_id"),
        sizing_cell=sizing_cell,
        outcome_id=_text(document, "outcome_id"),
        payload_schema=_text(document, "payload_schema"),
        payload_jsonl=payload_jsonl,
        payload_sha256=_text(document, "payload_sha256"),
        previous_record_sha256=_optional_text(
            document,
            "previous_record_sha256",
        ),
        record_sha256=_text(document, "record_sha256"),
        _encoded_line=encoded_line,
        _encoded_sha256=hashlib.sha256(encoded_line).hexdigest(),
        _factory_token=_FACTORY_TOKEN,
    )
    return record


def verify_prospective_outcome_wal_successor_v2(
    previous: ProspectiveOutcomeWalRecordV2,
    current: ProspectiveOutcomeWalRecordV2,
) -> None:
    """Require one adjacent record in the same attempt-wide chain."""

    if (
        type(previous) is not ProspectiveOutcomeWalRecordV2
        or type(current) is not ProspectiveOutcomeWalRecordV2
    ):
        raise TypeError("successor verification requires exact outcome records")
    previous.verify_integrity()
    current.verify_integrity()
    if current.attempt_plan_sha256 != previous.attempt_plan_sha256:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "outcome successor crosses prospective attempts"
        )
    if current.ingest_seq != previous.ingest_seq + 1:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "outcome successor ingest sequence is not adjacent"
        )
    if current.previous_record_sha256 != previous.record_sha256:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "outcome successor predecessor hash differs"
        )


def _record_document(
    *,
    ingest_seq: int,
    kind: ProspectiveOutcomeWalRecordKindV2,
    attempt_plan_sha256: str,
    origin_segment_id: str,
    origin_cell_id: str,
    sizing_cell: PaperSizingCellV2,
    outcome_id: str,
    payload_schema: str,
    payload: dict[str, object],
    payload_sha256: str,
    previous_record_sha256: str | None,
) -> dict[str, object]:
    return {
        "attempt_plan_sha256": attempt_plan_sha256,
        "ingest_seq": ingest_seq,
        "kind": kind.value,
        "origin_cell_id": origin_cell_id,
        "origin_segment_id": origin_segment_id,
        "outcome_id": outcome_id,
        "payload": payload,
        "payload_schema": payload_schema,
        "payload_sha256": payload_sha256,
        "previous_record_sha256": previous_record_sha256,
        "schema_version": PROSPECTIVE_OUTCOME_WAL_RECORD_SCHEMA_V2,
        "sizing_cell": sizing_cell.value,
    }


def _record_sha256(document: dict[str, object]) -> str:
    return hashlib.sha256(_RECORD_HASH_DOMAIN + canonical_json_line(document)).hexdigest()


def _decode_canonical_payload(encoded: bytes) -> dict[str, object]:
    if type(encoded) is not bytes:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "canonical payload must be immutable bytes"
        )
    if not encoded.endswith(b"\n") or b"\n" in encoded[:-1]:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "canonical payload must be one newline-terminated line"
        )
    if len(encoded) > MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "canonical payload exceeds the fixed byte bound"
        )
    document = _decode_object(encoded, "outcome payload")
    try:
        canonical_payload = canonical_json_line(document)
    except (TypeError, ValueError) as exc:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "outcome payload contains unsupported canonical JSON"
        ) from exc
    if canonical_payload != encoded:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "outcome payload is not exact canonical JSONL"
        )
    return document


def _decode_object(encoded: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            f"{label} is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(value, dict):
        raise ProspectiveOutcomeWalRecordContractErrorV2(f"{label} must be a JSON object")
    return cast(dict[str, object], value)


def _validate_predecessor(ingest_seq: int, value: str | None) -> None:
    if ingest_seq == 1:
        if value is not None:
            raise ProspectiveOutcomeWalRecordContractErrorV2(
                "outcome WAL genesis cannot have a predecessor"
            )
        return
    if value is None:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            "non-genesis outcome record requires a predecessor"
        )
    _validate_sha256(value, "previous_record_sha256")


def _validate_positive_safe_integer(value: object, label: str) -> None:
    if type(value) is not int or not 1 <= value <= _JCS_MAX_SAFE_INTEGER:
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            f"{label} must be a positive JCS-safe integer"
        )


def _validate_sha256(value: object, label: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ProspectiveOutcomeWalRecordContractErrorV2(f"{label} must be lowercase SHA-256")


def _text(document: dict[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str):
        raise ProspectiveOutcomeWalRecordContractErrorV2(f"outcome record {key} must be text")
    return value


def _optional_text(document: dict[str, object], key: str) -> str | None:
    value = document.get(key)
    if value is not None and not isinstance(value, str):
        raise ProspectiveOutcomeWalRecordContractErrorV2(
            f"outcome record {key} must be text or null"
        )
    return value


def _integer(document: dict[str, object], key: str) -> int:
    value = document.get(key)
    if type(value) is not int:
        raise ProspectiveOutcomeWalRecordContractErrorV2(f"outcome record {key} must be an integer")
    return value
