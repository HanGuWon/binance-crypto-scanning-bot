from __future__ import annotations

import base64
import binascii
import hashlib
import re
import struct
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,30}$")
_MAX_IDENTITY_LENGTH = 256


class RawEncodingV2(StrEnum):
    BASE64 = "base64"


class VenueV2(StrEnum):
    SPOT = "spot"
    USDM_FUTURES = "usdm_futures"


class TransportV2(StrEnum):
    WEBSOCKET = "websocket"
    HTTPS = "https"


@dataclass(frozen=True, slots=True)
class RawRecordV2:
    """Minimal immutable V2 raw-evidence envelope.

    This model carries receipt/lineage facts only. It has no strategy, alert,
    execution, position, or outcome fields and is not connected to live capture.
    """

    session_id: str
    plan_id: str
    protocol_hash: str
    source: Literal["binance"]
    transport: TransportV2
    venue: VenueV2
    route_id: str
    symbol: str | None
    connection_id: str
    generation: int
    frame_seq: int | None
    ingest_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    raw_encoding: RawEncodingV2
    raw_len: int
    raw_payload: str
    source_logical_key: str | None = None
    schema_version: Literal["r4b_v2_raw_record_v2"] = "r4b_v2_raw_record_v2"

    def __post_init__(self) -> None:
        if self.schema_version != "r4b_v2_raw_record_v2":
            raise ValueError("unsupported RawRecordV2 schema_version")
        _require_identity(self.session_id, "session_id")
        _require_identity(self.plan_id, "plan_id")
        _require_sha256(self.protocol_hash, "protocol_hash")
        if self.source != "binance":
            raise ValueError("RawRecordV2 source must be binance")
        if not isinstance(self.transport, TransportV2):
            raise ValueError("transport must be a TransportV2 value")
        if not isinstance(self.venue, VenueV2):
            raise ValueError("venue must be a VenueV2 value")
        _require_identity(self.route_id, "route_id")
        if self.symbol is not None and _SYMBOL_RE.fullmatch(self.symbol) is None:
            raise ValueError("symbol must be an uppercase normalized market symbol")
        _require_identity(self.connection_id, "connection_id")
        _require_positive_int(self.generation, "generation")
        if self.frame_seq is not None:
            _require_positive_int(self.frame_seq, "frame_seq")
        _require_positive_int(self.ingest_seq, "ingest_seq")
        _require_nonnegative_int(self.receipt_wall_ms, "receipt_wall_ms")
        _require_nonnegative_int(self.receipt_monotonic_ns, "receipt_monotonic_ns")
        if self.raw_encoding is not RawEncodingV2.BASE64:
            raise ValueError("raw_encoding must be base64 for every V2 capture record")
        if not isinstance(self.raw_payload, str):
            raise ValueError("raw_payload must be a retained string representation")
        _require_nonnegative_int(self.raw_len, "raw_len")
        if self.source_logical_key is not None:
            _require_identity(self.source_logical_key, "source_logical_key")
        raw = self.payload_bytes()
        if len(raw) != self.raw_len:
            raise ValueError("raw_len differs from the retained raw payload")

    @classmethod
    def from_payload(
        cls,
        *,
        session_id: str,
        plan_id: str,
        protocol_hash: str,
        transport: TransportV2,
        venue: VenueV2,
        route_id: str,
        symbol: str | None,
        connection_id: str,
        generation: int,
        frame_seq: int | None,
        ingest_seq: int,
        receipt_wall_ms: int,
        receipt_monotonic_ns: int,
        raw_payload: str | bytes,
        source_logical_key: str | None = None,
    ) -> RawRecordV2:
        """Retain source bytes losslessly as base64 with their exact byte length."""

        if isinstance(raw_payload, str):
            try:
                raw = raw_payload.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValueError("text raw_payload must be valid UTF-8") from exc
        elif isinstance(raw_payload, bytes):
            raw = raw_payload
        else:
            raise TypeError("raw_payload must be str or bytes")
        retained = base64.b64encode(raw).decode("ascii")
        return cls(
            session_id=session_id,
            plan_id=plan_id,
            protocol_hash=protocol_hash,
            source="binance",
            transport=transport,
            venue=venue,
            route_id=route_id,
            symbol=symbol,
            connection_id=connection_id,
            generation=generation,
            frame_seq=frame_seq,
            ingest_seq=ingest_seq,
            receipt_wall_ms=receipt_wall_ms,
            receipt_monotonic_ns=receipt_monotonic_ns,
            raw_encoding=RawEncodingV2.BASE64,
            raw_len=len(raw),
            raw_payload=retained,
            source_logical_key=source_logical_key,
        )

    def payload_bytes(self) -> bytes:
        try:
            return base64.b64decode(self.raw_payload, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("base64 raw_payload is invalid") from exc

    def derive_raw_payload_hash(self, stream_id: str | bytes) -> str:
        """Derive the downstream-only domain-separated raw evidence hash."""

        return derive_raw_payload_hash(stream_id, self.payload_bytes())

    @property
    def source_kind(self) -> str:
        """Return a bounded-cardinality health label, never a strategy label."""

        return f"{self.venue.value}:{self.route_id}"


def derive_raw_payload_hash(stream_id: str | bytes, raw_bytes: bytes) -> str:
    """Derive the selected-spec raw hash without persisting it in capture JSONL."""

    if isinstance(stream_id, str):
        try:
            encoded_stream_id = stream_id.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("stream_id must be valid UTF-8") from exc
    elif isinstance(stream_id, bytes):
        encoded_stream_id = stream_id
    else:
        raise TypeError("stream_id must be str or bytes")
    if not encoded_stream_id:
        raise ValueError("stream_id must be non-empty")
    if not isinstance(raw_bytes, bytes):
        raise TypeError("raw_bytes must be immutable bytes")
    return hashlib.sha256(
        b"R4B_RAW_V2\0"
        + struct.pack(">Q", len(encoded_stream_id))
        + encoded_stream_id
        + struct.pack(">Q", len(raw_bytes))
        + raw_bytes
    ).hexdigest()


def _require_identity(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")


def _require_sha256(value: str, field: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")


def _require_positive_int(value: int, field: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be a positive integer")


def _require_nonnegative_int(value: int, field: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
