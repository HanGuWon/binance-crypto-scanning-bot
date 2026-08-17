"""Canonical segment-local hash-chained WAL records for prospective evidence.

This module owns only record serialization and adjacent-record verification.  It
does not evaluate a strategy, establish census membership, write a WAL, or make
an efficacy claim.  Callers must supply payload bytes already emitted by the
typed decision/execution owner; the factory rejects any non-canonical rewrite.
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

PROSPECTIVE_WAL_RECORD_SCHEMA_V2: Final = "r4b_v2_prospective_wal_record_v2"
DECISION_PREPARE_PAYLOAD_SCHEMA_V2: Final = (
    "r4b_v2_prospective_decision_prepare_payload_v2"
)
CELL_DISPOSITION_PAYLOAD_SCHEMA_V2: Final = (
    "r4b_v2_prospective_cell_disposition_payload_v2"
)
PAPER_TERMINAL_PAYLOAD_SCHEMA_V2: Final = (
    "r4b_v2_prospective_paper_terminal_payload_v2"
)
MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2: Final = 64 * 1024
MAX_PROSPECTIVE_WAL_RECORD_BYTES_V2: Final = (
    MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2 + 4 * 1024
)

_RECORD_HASH_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_WAL_RECORD_V2\0"
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_JCS_MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991
_FACTORY_TOKEN: Final = object()
_RECORD_KEYS: Final = frozenset(
    {
        "attempt_plan_sha256",
        "cell_id",
        "ingest_seq",
        "kind",
        "payload",
        "payload_schema",
        "payload_sha256",
        "previous_record_sha256",
        "record_sha256",
        "schema_version",
        "segment_id",
    }
)


class ProspectiveWalRecordKindV2(StrEnum):
    """The complete record-kind vocabulary of the prospective WAL."""

    DECISION_PREPARE = "DECISION_PREPARE"
    CELL_DISPOSITION = "CELL_DISPOSITION"
    PAPER_TERMINAL = "PAPER_TERMINAL"


_PAYLOAD_SCHEMA_BY_KIND: Final = {
    ProspectiveWalRecordKindV2.DECISION_PREPARE: (
        DECISION_PREPARE_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveWalRecordKindV2.CELL_DISPOSITION: (
        CELL_DISPOSITION_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveWalRecordKindV2.PAPER_TERMINAL: PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
}


class ProspectiveWalRecordContractErrorV2(ValueError):
    """Raised when prospective WAL record bytes or identities are invalid."""


@dataclass(frozen=True, slots=True)
class ProspectiveWalRecordV2:
    """Factory-sealed, encode-once item accepted by ``WalQueuedRecordV2``.

    ``payload_jsonl`` is retained as immutable input evidence.  ``encoded_line``
    is a separate canonical envelope; WAL writers must append those cached bytes
    verbatim.  ``verify_integrity`` re-derives both hashes and the complete
    envelope, so post-construction mutation is fail-loud.
    """

    ingest_seq: int
    kind: ProspectiveWalRecordKindV2
    attempt_plan_sha256: str
    segment_id: str
    cell_id: str
    payload_schema: str
    payload_jsonl: bytes = field(repr=False)
    payload_sha256: str
    previous_record_sha256: str | None
    record_sha256: str
    _encoded_line: bytes = field(repr=False)
    _encoded_len: int = field(repr=False)
    _encoded_sha256: str = field(repr=False)
    _factory_token: InitVar[object | None] = None
    schema_version: str = field(
        init=False,
        default=PROSPECTIVE_WAL_RECORD_SCHEMA_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ProspectiveWalRecordContractErrorV2(
                "prospective WAL records are factory-sealed; "
                "use build_prospective_wal_record_v2"
            )
        self.verify_integrity()

    @property
    def payload(self) -> dict[str, object]:
        """Return a fresh JSON object decoded from the retained canonical bytes."""

        return _decode_canonical_payload(self.payload_jsonl)

    @property
    def encoded_line(self) -> bytes:
        return self._encoded_line

    @property
    def encoded_len(self) -> int:
        return self._encoded_len

    @property
    def encoded_sha256(self) -> str:
        return self._encoded_sha256

    def verify_integrity(self) -> None:
        """Re-derive payload, chain, record, and cached-envelope identities."""

        _validate_positive_ingest_seq(self.ingest_seq)
        if not isinstance(self.kind, ProspectiveWalRecordKindV2):
            raise ProspectiveWalRecordContractErrorV2(
                "kind must be ProspectiveWalRecordKindV2"
            )
        _validate_sha256(self.attempt_plan_sha256, "attempt_plan_sha256")
        _validate_sha256(self.segment_id, "segment_id")
        _validate_sha256(self.cell_id, "cell_id")
        expected_payload_schema = _PAYLOAD_SCHEMA_BY_KIND[self.kind]
        if self.payload_schema != expected_payload_schema:
            raise ProspectiveWalRecordContractErrorV2(
                "payload_schema does not match the exact record-kind schema"
            )
        if self.schema_version != PROSPECTIVE_WAL_RECORD_SCHEMA_V2:
            raise ProspectiveWalRecordContractErrorV2(
                "unsupported prospective WAL record schema"
            )

        payload = _decode_canonical_payload(self.payload_jsonl)
        if payload.get("schema_version") != self.payload_schema:
            raise ProspectiveWalRecordContractErrorV2(
                "payload schema_version differs from payload_schema"
            )
        _validate_sha256(self.payload_sha256, "payload_sha256")
        actual_payload_sha256 = hashlib.sha256(self.payload_jsonl).hexdigest()
        if not hmac.compare_digest(actual_payload_sha256, self.payload_sha256):
            raise ProspectiveWalRecordContractErrorV2(
                "payload_sha256 differs from canonical payload_jsonl"
            )

        _validate_predecessor(self.ingest_seq, self.previous_record_sha256)
        _validate_sha256(self.record_sha256, "record_sha256")
        expected_record_sha256 = _record_sha256(
            ingest_seq=self.ingest_seq,
            kind=self.kind,
            attempt_plan_sha256=self.attempt_plan_sha256,
            segment_id=self.segment_id,
            cell_id=self.cell_id,
            payload_schema=self.payload_schema,
            payload=payload,
            payload_sha256=self.payload_sha256,
            previous_record_sha256=self.previous_record_sha256,
        )
        if not hmac.compare_digest(expected_record_sha256, self.record_sha256):
            raise ProspectiveWalRecordContractErrorV2(
                "record_sha256 differs from the exact prospective WAL record"
            )

        if type(self._encoded_line) is not bytes:
            raise ProspectiveWalRecordContractErrorV2(
                "encoded_line must be immutable bytes"
            )
        if type(self._encoded_len) is not int or self._encoded_len < 1:
            raise ProspectiveWalRecordContractErrorV2(
                "encoded_len must be a positive integer"
            )
        if self._encoded_len != len(self._encoded_line):
            raise ProspectiveWalRecordContractErrorV2(
                "encoded_len differs from encoded_line"
            )
        _validate_sha256(self._encoded_sha256, "encoded_sha256")
        actual_encoded_sha256 = hashlib.sha256(self._encoded_line).hexdigest()
        if not hmac.compare_digest(actual_encoded_sha256, self._encoded_sha256):
            raise ProspectiveWalRecordContractErrorV2(
                "encoded_sha256 differs from encoded_line"
            )
        expected_encoded_line = canonical_json_line(
            _record_document(
                ingest_seq=self.ingest_seq,
                kind=self.kind,
                attempt_plan_sha256=self.attempt_plan_sha256,
                segment_id=self.segment_id,
                cell_id=self.cell_id,
                payload_schema=self.payload_schema,
                payload=payload,
                payload_sha256=self.payload_sha256,
                previous_record_sha256=self.previous_record_sha256,
                record_sha256=self.record_sha256,
            )
        )
        if not hmac.compare_digest(expected_encoded_line, self._encoded_line):
            raise ProspectiveWalRecordContractErrorV2(
                "encoded_line differs from the exact canonical record envelope"
            )


def build_prospective_wal_record_v2(
    *,
    ingest_seq: int,
    kind: ProspectiveWalRecordKindV2,
    attempt_plan_sha256: str,
    segment_id: str,
    cell_id: str,
    payload_schema: str,
    canonical_payload_jsonl: bytes,
    previous_record_sha256: str | None,
) -> ProspectiveWalRecordV2:
    """Seal one already-canonical typed payload into a WAL queue record."""

    _validate_positive_ingest_seq(ingest_seq)
    if not isinstance(kind, ProspectiveWalRecordKindV2):
        raise ProspectiveWalRecordContractErrorV2(
            "kind must be ProspectiveWalRecordKindV2"
        )
    _validate_sha256(attempt_plan_sha256, "attempt_plan_sha256")
    _validate_sha256(segment_id, "segment_id")
    _validate_sha256(cell_id, "cell_id")
    expected_payload_schema = _PAYLOAD_SCHEMA_BY_KIND[kind]
    if payload_schema != expected_payload_schema:
        raise ProspectiveWalRecordContractErrorV2(
            "payload_schema does not match the exact record-kind schema"
        )
    payload = _decode_canonical_payload(canonical_payload_jsonl)
    if payload.get("schema_version") != payload_schema:
        raise ProspectiveWalRecordContractErrorV2(
            "payload schema_version differs from payload_schema"
        )
    _validate_predecessor(ingest_seq, previous_record_sha256)

    payload_sha256 = hashlib.sha256(canonical_payload_jsonl).hexdigest()
    record_sha256 = _record_sha256(
        ingest_seq=ingest_seq,
        kind=kind,
        attempt_plan_sha256=attempt_plan_sha256,
        segment_id=segment_id,
        cell_id=cell_id,
        payload_schema=payload_schema,
        payload=payload,
        payload_sha256=payload_sha256,
        previous_record_sha256=previous_record_sha256,
    )
    encoded_line = canonical_json_line(
        _record_document(
            ingest_seq=ingest_seq,
            kind=kind,
            attempt_plan_sha256=attempt_plan_sha256,
            segment_id=segment_id,
            cell_id=cell_id,
            payload_schema=payload_schema,
            payload=payload,
            payload_sha256=payload_sha256,
            previous_record_sha256=previous_record_sha256,
            record_sha256=record_sha256,
        )
    )
    return ProspectiveWalRecordV2(
        ingest_seq=ingest_seq,
        kind=kind,
        attempt_plan_sha256=attempt_plan_sha256,
        segment_id=segment_id,
        cell_id=cell_id,
        payload_schema=payload_schema,
        payload_jsonl=canonical_payload_jsonl,
        payload_sha256=payload_sha256,
        previous_record_sha256=previous_record_sha256,
        record_sha256=record_sha256,
        _encoded_line=encoded_line,
        _encoded_len=len(encoded_line),
        _encoded_sha256=hashlib.sha256(encoded_line).hexdigest(),
        _factory_token=_FACTORY_TOKEN,
    )


def verify_prospective_wal_successor_v2(
    previous: ProspectiveWalRecordV2,
    successor: ProspectiveWalRecordV2,
) -> None:
    """Verify one exact adjacent link in a segment-local record chain."""

    if not isinstance(previous, ProspectiveWalRecordV2) or not isinstance(
        successor, ProspectiveWalRecordV2
    ):
        raise ProspectiveWalRecordContractErrorV2(
            "chain members must be ProspectiveWalRecordV2"
        )
    previous.verify_integrity()
    successor.verify_integrity()
    if successor.attempt_plan_sha256 != previous.attempt_plan_sha256:
        raise ProspectiveWalRecordContractErrorV2(
            "successor attempt_plan_sha256 differs from its predecessor"
        )
    if successor.segment_id != previous.segment_id:
        raise ProspectiveWalRecordContractErrorV2(
            "successor segment_id differs from its predecessor"
        )
    if successor.ingest_seq != previous.ingest_seq + 1:
        raise ProspectiveWalRecordContractErrorV2(
            "successor ingest_seq is not contiguous"
        )
    if successor.previous_record_sha256 is None or not hmac.compare_digest(
        successor.previous_record_sha256,
        previous.record_sha256,
    ):
        raise ProspectiveWalRecordContractErrorV2(
            "successor does not bind the actual predecessor record_sha256"
        )


def parse_prospective_wal_record_v2(encoded_line: bytes) -> ProspectiveWalRecordV2:
    """Strictly parse canonical WAL bytes and mint a verified sealed record.

    Parsing always routes through ``build_prospective_wal_record_v2``.  Stored
    payload, record, and envelope hashes are compared with the rebuilt values;
    unknown fields and non-canonical encodings are rejected.
    """

    if type(encoded_line) is not bytes:
        raise ProspectiveWalRecordContractErrorV2(
            "encoded_line must be immutable bytes"
        )
    if not 1 <= len(encoded_line) <= MAX_PROSPECTIVE_WAL_RECORD_BYTES_V2:
        raise ProspectiveWalRecordContractErrorV2(
            "encoded_line exceeds the bounded prospective WAL record size"
        )
    if not encoded_line.endswith(b"\n") or encoded_line.count(b"\n") != 1:
        raise ProspectiveWalRecordContractErrorV2(
            "encoded_line must be exactly one JSONL record"
        )
    try:
        decoded: object = json.loads(encoded_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectiveWalRecordContractErrorV2(
            "encoded_line is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(decoded, dict):
        raise ProspectiveWalRecordContractErrorV2(
            "encoded_line must contain a JSON object"
        )
    document = cast(dict[str, object], decoded)
    if frozenset(document) != _RECORD_KEYS:
        raise ProspectiveWalRecordContractErrorV2(
            "encoded_line has missing or unknown prospective WAL fields"
        )
    if document["schema_version"] != PROSPECTIVE_WAL_RECORD_SCHEMA_V2:
        raise ProspectiveWalRecordContractErrorV2(
            "unsupported prospective WAL record schema"
        )
    payload = document["payload"]
    if not isinstance(payload, dict):
        raise ProspectiveWalRecordContractErrorV2(
            "encoded_line payload must be a JSON object"
        )
    try:
        canonical_payload_jsonl = canonical_json_line(
            cast(dict[str, object], payload)
        )
    except (TypeError, ValueError) as exc:
        raise ProspectiveWalRecordContractErrorV2(
            "encoded_line payload is outside the RFC8785 protocol domain"
        ) from exc

    kind_value = document["kind"]
    if not isinstance(kind_value, str):
        raise ProspectiveWalRecordContractErrorV2("kind must be text")
    try:
        kind = ProspectiveWalRecordKindV2(kind_value)
    except ValueError as exc:
        raise ProspectiveWalRecordContractErrorV2(
            "unsupported prospective WAL record kind"
        ) from exc
    ingest_seq = document["ingest_seq"]
    if type(ingest_seq) is not int:
        raise ProspectiveWalRecordContractErrorV2("ingest_seq must be an integer")
    attempt_plan_sha256 = _require_text(document, "attempt_plan_sha256")
    segment_id = _require_text(document, "segment_id")
    cell_id = _require_text(document, "cell_id")
    payload_schema = _require_text(document, "payload_schema")
    stored_payload_sha256 = _require_text(document, "payload_sha256")
    stored_record_sha256 = _require_text(document, "record_sha256")
    previous_value = document["previous_record_sha256"]
    if previous_value is not None and not isinstance(previous_value, str):
        raise ProspectiveWalRecordContractErrorV2(
            "previous_record_sha256 must be null or text"
        )

    record = build_prospective_wal_record_v2(
        ingest_seq=ingest_seq,
        kind=kind,
        attempt_plan_sha256=attempt_plan_sha256,
        segment_id=segment_id,
        cell_id=cell_id,
        payload_schema=payload_schema,
        canonical_payload_jsonl=canonical_payload_jsonl,
        previous_record_sha256=previous_value,
    )
    if not hmac.compare_digest(record.payload_sha256, stored_payload_sha256):
        raise ProspectiveWalRecordContractErrorV2(
            "stored payload_sha256 differs from the rebuilt payload"
        )
    if not hmac.compare_digest(record.record_sha256, stored_record_sha256):
        raise ProspectiveWalRecordContractErrorV2(
            "stored record_sha256 differs from the rebuilt record"
        )
    if not hmac.compare_digest(record.encoded_line, encoded_line):
        raise ProspectiveWalRecordContractErrorV2(
            "encoded_line is not the exact canonical prospective WAL envelope"
        )
    return record


def _decode_canonical_payload(payload_jsonl: bytes) -> dict[str, object]:
    if type(payload_jsonl) is not bytes:
        raise ProspectiveWalRecordContractErrorV2(
            "canonical_payload_jsonl must be immutable bytes"
        )
    if not 1 <= len(payload_jsonl) <= MAX_PROSPECTIVE_WAL_PAYLOAD_BYTES_V2:
        raise ProspectiveWalRecordContractErrorV2(
            "canonical_payload_jsonl must be between 1 and 65536 bytes"
        )
    if not payload_jsonl.endswith(b"\n") or payload_jsonl.count(b"\n") != 1:
        raise ProspectiveWalRecordContractErrorV2(
            "canonical_payload_jsonl must be exactly one JSONL record"
        )
    try:
        decoded: object = json.loads(payload_jsonl)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectiveWalRecordContractErrorV2(
            "canonical_payload_jsonl is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(decoded, dict):
        raise ProspectiveWalRecordContractErrorV2(
            "canonical_payload_jsonl must contain a JSON object"
        )
    payload = cast(dict[str, object], decoded)
    try:
        canonical = canonical_json_line(payload)
    except (TypeError, ValueError) as exc:
        raise ProspectiveWalRecordContractErrorV2(
            "canonical_payload_jsonl is outside the RFC8785 protocol domain"
        ) from exc
    if not hmac.compare_digest(canonical, payload_jsonl):
        raise ProspectiveWalRecordContractErrorV2(
            "canonical_payload_jsonl is not exact RFC8785 JSONL"
        )
    return payload


def _validate_positive_ingest_seq(value: int) -> None:
    if type(value) is not int or not 1 <= value <= _JCS_MAX_SAFE_INTEGER:
        raise ProspectiveWalRecordContractErrorV2(
            "ingest_seq must be a positive RFC8785-safe integer"
        )


def _validate_sha256(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ProspectiveWalRecordContractErrorV2(
            f"{name} must be a lowercase SHA-256 digest"
        )


def _require_text(document: dict[str, object], name: str) -> str:
    value = document[name]
    if not isinstance(value, str):
        raise ProspectiveWalRecordContractErrorV2(f"{name} must be text")
    return value


def _validate_predecessor(
    ingest_seq: int,
    previous_record_sha256: str | None,
) -> None:
    if ingest_seq == 1:
        if previous_record_sha256 is not None:
            raise ProspectiveWalRecordContractErrorV2(
                "genesis record must not have previous_record_sha256"
            )
        return
    if previous_record_sha256 is None:
        raise ProspectiveWalRecordContractErrorV2(
            "non-genesis record requires previous_record_sha256"
        )
    _validate_sha256(previous_record_sha256, "previous_record_sha256")


def _record_sha256(
    *,
    ingest_seq: int,
    kind: ProspectiveWalRecordKindV2,
    attempt_plan_sha256: str,
    segment_id: str,
    cell_id: str,
    payload_schema: str,
    payload: dict[str, object],
    payload_sha256: str,
    previous_record_sha256: str | None,
) -> str:
    document = _record_document(
        ingest_seq=ingest_seq,
        kind=kind,
        attempt_plan_sha256=attempt_plan_sha256,
        segment_id=segment_id,
        cell_id=cell_id,
        payload_schema=payload_schema,
        payload=payload,
        payload_sha256=payload_sha256,
        previous_record_sha256=previous_record_sha256,
        record_sha256=None,
    )
    return hashlib.sha256(
        _RECORD_HASH_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _record_document(
    *,
    ingest_seq: int,
    kind: ProspectiveWalRecordKindV2,
    attempt_plan_sha256: str,
    segment_id: str,
    cell_id: str,
    payload_schema: str,
    payload: dict[str, object],
    payload_sha256: str,
    previous_record_sha256: str | None,
    record_sha256: str | None,
) -> dict[str, object]:
    document: dict[str, object] = {
        "attempt_plan_sha256": attempt_plan_sha256,
        "cell_id": cell_id,
        "ingest_seq": ingest_seq,
        "kind": kind.value,
        "payload": payload,
        "payload_schema": payload_schema,
        "payload_sha256": payload_sha256,
        "previous_record_sha256": previous_record_sha256,
        "schema_version": PROSPECTIVE_WAL_RECORD_SCHEMA_V2,
        "segment_id": segment_id,
    }
    if record_sha256 is not None:
        document["record_sha256"] = record_sha256
    return document
