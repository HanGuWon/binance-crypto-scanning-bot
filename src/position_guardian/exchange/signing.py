from __future__ import annotations

import hashlib
import hmac
from collections.abc import Sequence
from urllib.parse import urlencode

type QueryValue = str | int | bool
type QueryPair = tuple[str, QueryValue]


def _query_value(value: QueryValue) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def canonical_query(params: Sequence[QueryPair]) -> str:
    """Encode Binance query pairs in their supplied order for signing."""

    if any(key == "signature" for key, _ in params):
        raise ValueError("signature must not be included in the canonical query")
    return urlencode([(key, _query_value(value)) for key, value in params])


def sign_query(params: Sequence[QueryPair], api_secret: str) -> str:
    query = canonical_query(params)
    digest = hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256)
    return f"{query}&signature={digest.hexdigest()}"
