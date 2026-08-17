from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

import rfc8785


def canonical_json_line(value: object) -> bytes:
    """Encode one RFC 8785/JCS UTF-8 JSONL value.

    The V2 queue calls this exactly once per accepted raw record. Downstream
    writers consume the returned immutable bytes and verify their recorded hash;
    they must not reserialize the domain object.

    Protocol documents deliberately exclude binary floats. RFC 8785 permits
    them, but the sealed V2 arithmetic contract does not. Rejecting them at the
    canonical boundary prevents platform-dependent values from entering an
    identity, WAL, block, or manifest hash.
    """

    if is_dataclass(value) and not isinstance(value, type):
        document: Any = asdict(value)
    elif isinstance(value, dict):
        document = value
    else:
        raise TypeError("canonical JSON input must be a dataclass instance or dict")
    _validate_protocol_json(document, path="$")
    return rfc8785.dumps(document) + b"\n"


def _validate_protocol_json(value: object, *, path: str) -> None:
    if value is None or isinstance(value, (str, bool)):
        return
    if type(value) is int:
        return
    if isinstance(value, float):
        raise TypeError(f"binary float is forbidden in canonical protocol JSON at {path}")
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_protocol_json(item, path=f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"canonical protocol JSON key must be text at {path}")
            _validate_protocol_json(item, path=f"{path}.{key}")
        return
    raise TypeError(
        f"unsupported canonical protocol JSON value {type(value).__name__} at {path}"
    )
