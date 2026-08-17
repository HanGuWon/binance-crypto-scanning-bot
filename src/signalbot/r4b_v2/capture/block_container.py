from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import struct
from dataclasses import asdict, dataclass
from typing import Protocol, cast

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.wal import crc32c

BLOCK_MAGIC_V2 = b"R4BBLK21"
BLOCK_COMMIT_MARKER_V2 = b"R4BCOMMIT21"
BLOCK_FORMAT_VERSION_V2 = 2

_BLOCK_HASH_DOMAIN = b"R4B_BLOCK_HASH_V2\0"
_BLOCK_SIGNATURE_DOMAIN = b"R4B_BLOCK_ED25519_SIGNATURE_V2\0"
_U32 = struct.Struct(">I")
_U64 = struct.Struct(">Q")
_MAX_METADATA_BYTES = 65_536
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_IDENTITY_LENGTH = 256


class SignedBlockContainerError(RuntimeError):
    """Raised when a signed V2 block container cannot be proven authentic."""


class BlockSignerV2(Protocol):
    """Minimal interface implemented by an Ed25519 software key or HSM adapter."""

    @property
    def key_id(self) -> str: ...

    @property
    def public_key_bytes(self) -> bytes: ...

    def sign(self, message: bytes) -> bytes: ...


@dataclass(frozen=True, slots=True)
class BlockSigningAuthorityV2:
    """Explicitly trusted Ed25519 public authority; never read from a block."""

    key_id: str
    public_key_base64: str
    algorithm: str = "Ed25519"
    schema_version: str = "r4b_v2_block_signing_authority_v1"

    def __post_init__(self) -> None:
        _validate_identity(self.key_id, "key_id")
        if self.algorithm != "Ed25519":
            raise ValueError("block signing algorithm must be Ed25519")
        if self.schema_version != "r4b_v2_block_signing_authority_v1":
            raise ValueError("unsupported block signing authority schema")
        if len(self.public_key_bytes) != 32:
            raise ValueError("Ed25519 public key must contain exactly 32 bytes")

    @classmethod
    def from_public_key_bytes(
        cls,
        *,
        key_id: str,
        public_key_bytes: bytes,
    ) -> BlockSigningAuthorityV2:
        if len(public_key_bytes) != 32:
            raise ValueError("Ed25519 public key must contain exactly 32 bytes")
        return cls(
            key_id=key_id,
            public_key_base64=base64.b64encode(public_key_bytes).decode("ascii"),
        )

    @property
    def public_key_bytes(self) -> bytes:
        return _strict_base64(self.public_key_base64, "Ed25519 public key")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical_json_line(asdict(self))).hexdigest()

    def verify(self, signature: bytes, message: bytes) -> None:
        try:
            Ed25519PublicKey.from_public_bytes(self.public_key_bytes).verify(
                signature,
                message,
            )
        except InvalidSignature as exc:
            raise SignedBlockContainerError("block Ed25519 signature is invalid") from exc


class Ed25519BlockSignerV2:
    """Explicit software-key signer; callers must supply key material."""

    def __init__(self, *, key_id: str, private_key: Ed25519PrivateKey) -> None:
        _validate_identity(key_id, "key_id")
        self._key_id = key_id
        self._private_key = private_key

    @classmethod
    def from_private_key_bytes(
        cls,
        *,
        key_id: str,
        private_key_bytes: bytes,
    ) -> Ed25519BlockSignerV2:
        if len(private_key_bytes) != 32:
            raise ValueError("Ed25519 private key seed must contain exactly 32 bytes")
        return cls(
            key_id=key_id,
            private_key=Ed25519PrivateKey.from_private_bytes(private_key_bytes),
        )

    @property
    def key_id(self) -> str:
        return self._key_id

    @property
    def public_key_bytes(self) -> bytes:
        return self._private_key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

    def sign(self, message: bytes) -> bytes:
        return self._private_key.sign(message)


@dataclass(frozen=True, slots=True)
class BlockCodecParametersV2:
    codec: str
    library_version: str
    level: int
    workers: int
    checksum: bool
    content_size: bool
    dictionary_sha256: str | None
    codec_candidate_id: str
    qualification_id: str

    def __post_init__(self) -> None:
        if (
            self.codec != "zstd"
            or self.library_version != "1.5.7"
            or type(self.level) is not int
            or self.level != 9
            or type(self.workers) is not int
            or self.workers != 0
            or type(self.checksum) is not bool
            or self.checksum is not True
            or type(self.content_size) is not bool
            or self.content_size is not True
            or self.dictionary_sha256 is not None
        ):
            raise ValueError("block codec parameters differ from sealed zstd 1.5.7 L9")
        _validate_identity(self.codec_candidate_id, "codec_candidate_id")
        _validate_identity(self.qualification_id, "qualification_id")


@dataclass(frozen=True, slots=True)
class BlockContainerHeaderV2:
    magic: str
    format_version: int
    codec_and_parameters: BlockCodecParametersV2
    schema_hash: str
    protocol_hash: str
    authority_hash: str
    plan_hash: str
    source_manifest_hash: str
    runtime_manifest_hash: str
    attempt_id: str
    stream_group_id: str
    segment_id: str
    block_index: int
    previous_block_hash: str | None
    record_count: int
    first_ingest_seq: int
    last_ingest_seq: int
    first_receipt_monotonic_ns: int
    last_receipt_monotonic_ns: int
    uncompressed_length: int

    def __post_init__(self) -> None:
        if self.magic != BLOCK_MAGIC_V2.decode("ascii"):
            raise ValueError("block header magic must be R4BBLK21")
        if (
            type(self.format_version) is not int
            or self.format_version != BLOCK_FORMAT_VERSION_V2
        ):
            raise ValueError("block format_version must be 2")
        for field_name in (
            "schema_hash",
            "protocol_hash",
            "authority_hash",
            "plan_hash",
            "source_manifest_hash",
            "runtime_manifest_hash",
        ):
            _validate_sha256(cast(str, getattr(self, field_name)), field_name)
        for field_name in ("attempt_id", "stream_group_id", "segment_id"):
            _validate_identity(cast(str, getattr(self, field_name)), field_name)
        if self.previous_block_hash is not None:
            _validate_sha256(self.previous_block_hash, "previous_block_hash")
        for field_name in (
            "block_index",
            "record_count",
            "first_ingest_seq",
            "last_ingest_seq",
            "uncompressed_length",
        ):
            _validate_positive_int(cast(int, getattr(self, field_name)), field_name)
        for field_name in (
            "first_receipt_monotonic_ns",
            "last_receipt_monotonic_ns",
        ):
            _validate_nonnegative_int(cast(int, getattr(self, field_name)), field_name)
        if self.last_ingest_seq - self.first_ingest_seq + 1 != self.record_count:
            raise ValueError("block header ingest range differs from record_count")
        if self.last_receipt_monotonic_ns < self.first_receipt_monotonic_ns:
            raise ValueError("block header receipt monotonic range is reversed")


@dataclass(frozen=True, slots=True)
class BlockContainerTrailerV2:
    compressed_length: int
    crc32c: int
    uncompressed_sha256: str
    compressed_sha256: str
    record_merkle_root_sha256: str
    block_hash_sha256: str
    writer_key_id: str
    writer_ed25519_signature: str
    commit_marker: str

    def __post_init__(self) -> None:
        _validate_positive_int(self.compressed_length, "compressed_length")
        _validate_nonnegative_int(self.crc32c, "CRC32C")
        if self.crc32c > 0xFFFFFFFF:
            raise ValueError("CRC32C is outside the uint32 range")
        for field_name in (
            "uncompressed_sha256",
            "compressed_sha256",
            "record_merkle_root_sha256",
            "block_hash_sha256",
        ):
            _validate_sha256(cast(str, getattr(self, field_name)), field_name)
        _validate_identity(self.writer_key_id, "writer_key_id")
        signature = _strict_base64(self.writer_ed25519_signature, "writer signature")
        if len(signature) != 64:
            raise ValueError("Ed25519 writer signature must contain exactly 64 bytes")
        if self.commit_marker != BLOCK_COMMIT_MARKER_V2.decode("ascii"):
            raise ValueError("block commit marker must be R4BCOMMIT21")

    @property
    def signature_bytes(self) -> bytes:
        return _strict_base64(self.writer_ed25519_signature, "writer signature")


@dataclass(frozen=True, slots=True)
class SignedBlockContainerV2:
    header: BlockContainerHeaderV2
    trailer: BlockContainerTrailerV2
    compressed: bytes
    encoded: bytes

    @property
    def container_sha256(self) -> str:
        return hashlib.sha256(self.encoded).hexdigest()


def encode_signed_block_container_v2(
    *,
    header: BlockContainerHeaderV2,
    compressed: bytes,
    uncompressed_sha256: str,
    record_merkle_root_sha256: str,
    signer: BlockSignerV2,
    signing_authority: BlockSigningAuthorityV2,
) -> SignedBlockContainerV2:
    """Encode the deterministic, length-delimited R4BBLK21 binary framing.

    Layout (all integers unsigned big-endian):
      magic[8] || u32(header JCS bytes) || header JCS ||
      u64(compressed bytes) || one zstd frame ||
      u32(trailer JCS bytes) || trailer JCS || commit_marker[11].

    CRC-32C covers the compressed zstd frame. The domain-separated block hash
    covers the physical magic, length-delimited header, length-delimited zstd
    frame, and length-delimited unsigned trailer core. The Ed25519 signature is
    over a second domain plus the 32 raw block-hash bytes.
    """

    if not compressed:
        raise ValueError("signed block compressed payload must be non-empty")
    _validate_sha256(uncompressed_sha256, "uncompressed_sha256")
    _validate_sha256(record_merkle_root_sha256, "record_merkle_root_sha256")
    _validate_signer(signer, signing_authority)
    header_bytes = _canonical_document(_header_document(header))
    compressed_sha256 = hashlib.sha256(compressed).hexdigest()
    unsigned_trailer = _unsigned_trailer_document(
        compressed_length=len(compressed),
        crc=crc32c(compressed),
        uncompressed_sha256=uncompressed_sha256,
        compressed_sha256=compressed_sha256,
        record_merkle_root_sha256=record_merkle_root_sha256,
        writer_key_id=signing_authority.key_id,
    )
    unsigned_trailer_bytes = _canonical_document(unsigned_trailer)
    digest = _calculate_block_hash(header_bytes, compressed, unsigned_trailer_bytes)
    signature = signer.sign(_signature_message(digest))
    if len(signature) != 64:
        raise SignedBlockContainerError("Ed25519 signer returned a non-64-byte signature")
    signing_authority.verify(signature, _signature_message(digest))
    trailer = BlockContainerTrailerV2(
        compressed_length=len(compressed),
        crc32c=crc32c(compressed),
        uncompressed_sha256=uncompressed_sha256,
        compressed_sha256=compressed_sha256,
        record_merkle_root_sha256=record_merkle_root_sha256,
        block_hash_sha256=digest,
        writer_key_id=signing_authority.key_id,
        writer_ed25519_signature=base64.b64encode(signature).decode("ascii"),
        commit_marker=BLOCK_COMMIT_MARKER_V2.decode("ascii"),
    )
    trailer_bytes = _canonical_document(_trailer_document(trailer))
    encoded = (
        BLOCK_MAGIC_V2
        + _pack_metadata(header_bytes)
        + _U64.pack(len(compressed))
        + compressed
        + _pack_metadata(trailer_bytes)
        + BLOCK_COMMIT_MARKER_V2
    )
    return SignedBlockContainerV2(
        header=header,
        trailer=trailer,
        compressed=compressed,
        encoded=encoded,
    )


def parse_and_verify_signed_block_container_v2(
    encoded: bytes,
    *,
    signing_authority: BlockSigningAuthorityV2,
) -> SignedBlockContainerV2:
    """Parse canonical framing and verify it against an out-of-band authority."""

    minimum = len(BLOCK_MAGIC_V2) + _U32.size + _U64.size + _U32.size + len(
        BLOCK_COMMIT_MARKER_V2
    )
    if len(encoded) <= minimum:
        raise SignedBlockContainerError("signed block container is truncated")
    if not encoded.startswith(BLOCK_MAGIC_V2):
        raise SignedBlockContainerError("signed block outer magic is invalid")
    cursor = len(BLOCK_MAGIC_V2)
    header_bytes, cursor = _read_metadata(encoded, cursor, "header")
    header_document = _decode_canonical_document(header_bytes, "header")
    header = _parse_header(header_document)
    if cursor + _U64.size > len(encoded):
        raise SignedBlockContainerError("signed block compressed length is truncated")
    compressed_length = _U64.unpack_from(encoded, cursor)[0]
    cursor += _U64.size
    if compressed_length < 1:
        raise SignedBlockContainerError("signed block compressed length is zero")
    compressed_end = cursor + compressed_length
    if compressed_end > len(encoded):
        raise SignedBlockContainerError("signed block compressed payload is truncated")
    compressed = encoded[cursor:compressed_end]
    cursor = compressed_end
    trailer_bytes, cursor = _read_metadata(encoded, cursor, "trailer")
    if encoded[cursor:] != BLOCK_COMMIT_MARKER_V2:
        raise SignedBlockContainerError("signed block physical commit marker is invalid")
    trailer_document = _decode_canonical_document(trailer_bytes, "trailer")
    trailer = _parse_trailer(trailer_document)
    if trailer.compressed_length != compressed_length:
        raise SignedBlockContainerError("signed block compressed lengths differ")
    if trailer.crc32c != crc32c(compressed):
        raise SignedBlockContainerError("signed block compressed CRC32C differs")
    if trailer.compressed_sha256 != hashlib.sha256(compressed).hexdigest():
        raise SignedBlockContainerError("signed block compressed SHA-256 differs")
    if trailer.writer_key_id != signing_authority.key_id:
        raise SignedBlockContainerError("signed block writer key ID is not trusted")
    unsigned_trailer_bytes = _canonical_document(
        _unsigned_trailer_document(
            compressed_length=trailer.compressed_length,
            crc=trailer.crc32c,
            uncompressed_sha256=trailer.uncompressed_sha256,
            compressed_sha256=trailer.compressed_sha256,
            record_merkle_root_sha256=trailer.record_merkle_root_sha256,
            writer_key_id=trailer.writer_key_id,
        )
    )
    digest = _calculate_block_hash(header_bytes, compressed, unsigned_trailer_bytes)
    if not hmac.compare_digest(digest, trailer.block_hash_sha256):
        raise SignedBlockContainerError("signed block domain hash differs")
    signing_authority.verify(trailer.signature_bytes, _signature_message(digest))
    return SignedBlockContainerV2(
        header=header,
        trailer=trailer,
        compressed=compressed,
        encoded=encoded,
    )


def _header_document(header: BlockContainerHeaderV2) -> dict[str, object]:
    document = asdict(header)
    return cast(dict[str, object], document)


def _trailer_document(trailer: BlockContainerTrailerV2) -> dict[str, object]:
    return {
        "compressed_length": trailer.compressed_length,
        "CRC32C": trailer.crc32c,
        "uncompressed_SHA256": trailer.uncompressed_sha256,
        "compressed_SHA256": trailer.compressed_sha256,
        "record_Merkle_root_SHA256": trailer.record_merkle_root_sha256,
        "block_hash_SHA256": trailer.block_hash_sha256,
        "writer_key_id": trailer.writer_key_id,
        "writer_Ed25519_signature": trailer.writer_ed25519_signature,
        "commit_marker": trailer.commit_marker,
    }


def _unsigned_trailer_document(
    *,
    compressed_length: int,
    crc: int,
    uncompressed_sha256: str,
    compressed_sha256: str,
    record_merkle_root_sha256: str,
    writer_key_id: str,
) -> dict[str, object]:
    return {
        "compressed_length": compressed_length,
        "CRC32C": crc,
        "uncompressed_SHA256": uncompressed_sha256,
        "compressed_SHA256": compressed_sha256,
        "record_Merkle_root_SHA256": record_merkle_root_sha256,
        "writer_key_id": writer_key_id,
    }


def _parse_header(document: dict[str, object]) -> BlockContainerHeaderV2:
    expected = {
        "magic",
        "format_version",
        "codec_and_parameters",
        "schema_hash",
        "protocol_hash",
        "authority_hash",
        "plan_hash",
        "source_manifest_hash",
        "runtime_manifest_hash",
        "attempt_id",
        "stream_group_id",
        "segment_id",
        "block_index",
        "previous_block_hash",
        "record_count",
        "first_ingest_seq",
        "last_ingest_seq",
        "first_receipt_monotonic_ns",
        "last_receipt_monotonic_ns",
        "uncompressed_length",
    }
    _require_exact_keys(document, expected, "header")
    codec_document = document["codec_and_parameters"]
    if not isinstance(codec_document, dict):
        raise SignedBlockContainerError("signed block codec parameters are not an object")
    codec_expected = {
        "codec",
        "library_version",
        "level",
        "workers",
        "checksum",
        "content_size",
        "dictionary_sha256",
        "codec_candidate_id",
        "qualification_id",
    }
    codec = cast(dict[str, object], codec_document)
    _require_exact_keys(codec, codec_expected, "codec parameters")
    try:
        parameters = BlockCodecParametersV2(
            codec=cast(str, codec["codec"]),
            library_version=cast(str, codec["library_version"]),
            level=cast(int, codec["level"]),
            workers=cast(int, codec["workers"]),
            checksum=cast(bool, codec["checksum"]),
            content_size=cast(bool, codec["content_size"]),
            dictionary_sha256=cast(str | None, codec["dictionary_sha256"]),
            codec_candidate_id=cast(str, codec["codec_candidate_id"]),
            qualification_id=cast(str, codec["qualification_id"]),
        )
        return BlockContainerHeaderV2(
            magic=cast(str, document["magic"]),
            format_version=cast(int, document["format_version"]),
            codec_and_parameters=parameters,
            schema_hash=cast(str, document["schema_hash"]),
            protocol_hash=cast(str, document["protocol_hash"]),
            authority_hash=cast(str, document["authority_hash"]),
            plan_hash=cast(str, document["plan_hash"]),
            source_manifest_hash=cast(str, document["source_manifest_hash"]),
            runtime_manifest_hash=cast(str, document["runtime_manifest_hash"]),
            attempt_id=cast(str, document["attempt_id"]),
            stream_group_id=cast(str, document["stream_group_id"]),
            segment_id=cast(str, document["segment_id"]),
            block_index=cast(int, document["block_index"]),
            previous_block_hash=cast(str | None, document["previous_block_hash"]),
            record_count=cast(int, document["record_count"]),
            first_ingest_seq=cast(int, document["first_ingest_seq"]),
            last_ingest_seq=cast(int, document["last_ingest_seq"]),
            first_receipt_monotonic_ns=cast(
                int,
                document["first_receipt_monotonic_ns"],
            ),
            last_receipt_monotonic_ns=cast(
                int,
                document["last_receipt_monotonic_ns"],
            ),
            uncompressed_length=cast(int, document["uncompressed_length"]),
        )
    except (TypeError, ValueError) as exc:
        raise SignedBlockContainerError("signed block header values are invalid") from exc


def _parse_trailer(document: dict[str, object]) -> BlockContainerTrailerV2:
    expected = {
        "compressed_length",
        "CRC32C",
        "uncompressed_SHA256",
        "compressed_SHA256",
        "record_Merkle_root_SHA256",
        "block_hash_SHA256",
        "writer_key_id",
        "writer_Ed25519_signature",
        "commit_marker",
    }
    _require_exact_keys(document, expected, "trailer")
    try:
        return BlockContainerTrailerV2(
            compressed_length=cast(int, document["compressed_length"]),
            crc32c=cast(int, document["CRC32C"]),
            uncompressed_sha256=cast(str, document["uncompressed_SHA256"]),
            compressed_sha256=cast(str, document["compressed_SHA256"]),
            record_merkle_root_sha256=cast(
                str,
                document["record_Merkle_root_SHA256"],
            ),
            block_hash_sha256=cast(str, document["block_hash_SHA256"]),
            writer_key_id=cast(str, document["writer_key_id"]),
            writer_ed25519_signature=cast(
                str,
                document["writer_Ed25519_signature"],
            ),
            commit_marker=cast(str, document["commit_marker"]),
        )
    except (TypeError, ValueError) as exc:
        raise SignedBlockContainerError("signed block trailer values are invalid") from exc


def _calculate_block_hash(
    header_bytes: bytes,
    compressed: bytes,
    unsigned_trailer_bytes: bytes,
) -> str:
    hash_input = (
        _BLOCK_HASH_DOMAIN
        + BLOCK_MAGIC_V2
        + _U32.pack(len(header_bytes))
        + header_bytes
        + _U64.pack(len(compressed))
        + compressed
        + _U32.pack(len(unsigned_trailer_bytes))
        + unsigned_trailer_bytes
    )
    return hashlib.sha256(hash_input).hexdigest()


def _signature_message(block_hash: str) -> bytes:
    return _BLOCK_SIGNATURE_DOMAIN + bytes.fromhex(block_hash)


def _canonical_document(document: dict[str, object]) -> bytes:
    line = canonical_json_line(document)
    return line[:-1]


def _decode_canonical_document(payload: bytes, label: str) -> dict[str, object]:
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SignedBlockContainerError(f"signed block {label} is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise SignedBlockContainerError(f"signed block {label} is not a JSON object")
    document = cast(dict[str, object], decoded)
    try:
        canonical = _canonical_document(document)
    except (TypeError, ValueError) as exc:
        raise SignedBlockContainerError(
            f"signed block {label} is outside canonical JSON"
        ) from exc
    if not hmac.compare_digest(payload, canonical):
        raise SignedBlockContainerError(f"signed block {label} is not canonical JCS")
    return document


def _pack_metadata(payload: bytes) -> bytes:
    if not payload or len(payload) > _MAX_METADATA_BYTES:
        raise SignedBlockContainerError("signed block metadata length is outside its bound")
    return _U32.pack(len(payload)) + payload


def _read_metadata(encoded: bytes, cursor: int, label: str) -> tuple[bytes, int]:
    if cursor + _U32.size > len(encoded):
        raise SignedBlockContainerError(f"signed block {label} length is truncated")
    length = _U32.unpack_from(encoded, cursor)[0]
    cursor += _U32.size
    if length < 1 or length > _MAX_METADATA_BYTES:
        raise SignedBlockContainerError(
            f"signed block {label} length is outside its bound"
        )
    end = cursor + length
    if end > len(encoded):
        raise SignedBlockContainerError(f"signed block {label} is truncated")
    return encoded[cursor:end], end


def _validate_signer(
    signer: BlockSignerV2,
    signing_authority: BlockSigningAuthorityV2,
) -> None:
    if signer.key_id != signing_authority.key_id:
        raise ValueError("block signer key ID differs from trusted signing authority")
    if not hmac.compare_digest(signer.public_key_bytes, signing_authority.public_key_bytes):
        raise ValueError("block signer public key differs from trusted signing authority")


def _strict_base64(value: str, label: str) -> bytes:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be base64 text")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError(f"{label} is not strict base64") from exc
    if base64.b64encode(decoded).decode("ascii") != value:
        raise ValueError(f"{label} is not canonical base64")
    return decoded


def _require_exact_keys(
    document: dict[str, object],
    expected: set[str],
    label: str,
) -> None:
    if set(document) != expected:
        raise SignedBlockContainerError(f"signed block {label} fields differ")


def _validate_identity(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")


def _validate_sha256(value: str, field: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")


def _validate_positive_int(value: int, field: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be a positive integer")


def _validate_nonnegative_int(value: int, field: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")
