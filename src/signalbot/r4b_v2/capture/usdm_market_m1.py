from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import InitVar, asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final, Literal, TypedDict, cast

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import StorageRootBindingV2
from signalbot.r4b_v2.capture.block_container import BlockSigningAuthorityV2
from signalbot.r4b_v2.capture.blocks import BlockPolicyV2
from signalbot.r4b_v2.capture.integrity_ledger import CaptureIntegrityLedgerV2
from signalbot.r4b_v2.capture.membership import (
    VerifiedRawMembershipLeafV2,
    reverify_verified_raw_membership_leaf_v2,
)
from signalbot.r4b_v2.capture.models import TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    provisional_promoting_plan_sha256_v2,
    validate_provisional_promoting_capture_plans_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_DECIMAL_TEXT_RE = re.compile(r"^[0-9]+(?:\.[0-9]+)?$")
_SIGNED_DECIMAL_TEXT_RE = re.compile(r"^-?[0-9]+(?:\.[0-9]+)?$")
_MAX_IDENTITY_LENGTH = 256
_MAX_DECIMAL_TEXT_LENGTH = 128
_MAX_FRAME_BYTES = 16_384
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_KLINE_INTERVAL_MS = 300_000
_AGG_TRADE_KEYS = frozenset(
    {"e", "E", "a", "s", "p", "q", "nq", "f", "l", "T", "m", "st"}
)
_KLINE_OUTER_KEYS = frozenset({"e", "E", "s", "k"})
_KLINE_INNER_KEYS = frozenset(
    {"t", "T", "s", "i", "f", "L", "o", "c", "h", "l", "v", "n", "x", "q", "V", "Q", "B"}
)
_MARK_PRICE_KEYS = frozenset({"e", "E", "s", "p", "ap", "P", "i", "r", "T", "st"})
_COMBINED_WRAPPER_KEYS = frozenset({"stream", "data"})
_FACTORY_TOKEN = object()
_ROW_HASH_DOMAIN = b"R4B_V2_USDM_MARKET_M1_ROW\0"
_M1_SCHEMA_VERSION = "r4b_v2_usdm_market_m1_v2"

USDM_MARKET_M1_ONLY_REASON_V2: Final = (
    "STRICT_M0_M1_SNAPSHOT_REQUIRES_LIVE_REVERIFICATION_AND_M2_CAUSAL_CURSOR"
)

_PARSER_CONTRACT = {
    "access_mode": "COMBINED_QUERY",
    "agg_trade_data_keys": tuple(sorted(_AGG_TRADE_KEYS)),
    "agg_trade_field_types": {
        "E": "nonnegative_int64",
        "T": "nonnegative_int64",
        "a": "nonnegative_int64",
        "e": "literal_aggTrade",
        "f": "nonnegative_int64",
        "l": "nonnegative_int64",
        "m": "boolean",
        "nq": "strict_nonnegative_decimal_text",
        "p": "strict_positive_decimal_text",
        "q": "strict_positive_decimal_text",
        "s": "stream_bound_normalized_symbol",
        "st": "integer_literal_1",
    },
    "combined_wrapper_keys": tuple(sorted(_COMBINED_WRAPPER_KEYS)),
    "decimal_text_max_length": _MAX_DECIMAL_TEXT_LENGTH,
    "decimal_text_regex": _DECIMAL_TEXT_RE.pattern,
    "duplicate_key_policy": "REJECT_AT_EVERY_OBJECT_DEPTH",
    "exact_inner_key_policy": (
        "PROJECT_FROZEN_STRICTER_THAN_OFFICIAL_SCHEMA_V1_0_0"
    ),
    "frame_max_bytes": _MAX_FRAME_BYTES,
    "integer_policy": "JSON_INTEGER_NONNEGATIVE_SIGNED_INT64",
    "kline_field_types": {
        "E": "nonnegative_int64",
        "e": "literal_kline",
        "k.B": "strict_nonnegative_decimal_text",
        "k.L": "nonnegative_int64",
        "k.Q": "strict_nonnegative_decimal_text",
        "k.T": "nonnegative_int64",
        "k.V": "strict_nonnegative_decimal_text",
        "k.c": "strict_positive_decimal_text",
        "k.f": "nonnegative_int64",
        "k.h": "strict_positive_decimal_text",
        "k.i": "literal_5m",
        "k.l": "strict_positive_decimal_text",
        "k.n": "nonnegative_int64",
        "k.o": "strict_positive_decimal_text",
        "k.q": "strict_nonnegative_decimal_text",
        "k.s": "stream_bound_normalized_symbol",
        "k.t": "nonnegative_int64",
        "k.v": "strict_nonnegative_decimal_text",
        "k.x": "boolean",
        "s": "stream_bound_normalized_symbol",
    },
    "kline_inner_keys": tuple(sorted(_KLINE_INNER_KEYS)),
    "kline_interval": "5m",
    "kline_interval_ms": _KLINE_INTERVAL_MS,
    "kline_outer_keys": tuple(sorted(_KLINE_OUTER_KEYS)),
    "mark_price_1s_data_keys": tuple(sorted(_MARK_PRICE_KEYS)),
    "mark_price_1s_field_types": {
        "E": "nonnegative_int64_exchange_event_time",
        "P": "strict_nonnegative_decimal_text",
        "T": "nonnegative_int64_next_funding_time_NOT_observation_time",
        "ap": "strict_positive_decimal_text_mark_moving_average",
        "e": "literal_markPriceUpdate",
        "i": "strict_positive_decimal_text_index_price",
        "p": "strict_positive_decimal_text_mark_price",
        "r": "strict_finite_signed_decimal_text_funding_rate",
        "s": "stream_bound_normalized_symbol",
        "st": "integer_literal_1",
    },
    "mark_price_interval": "1s",
    "local_invariants": (
        "event_time_not_before_trade_time",
        "first_trade_id_not_after_last_trade_id",
        "normal_quantity_not_above_quantity",
        "exact_5m_close_boundary",
        "ohlc_geometry",
        "taker_volume_not_above_total_volume",
    ),
    "numeric_literal_policy": "DECIMALS_MUST_REMAIN_JSON_STRINGS",
    "route_id": "usdm_market",
    "schema_reference": (
        "https://developers.binance.com/en/docs/catalog/"
        "core-trading-derivatives-trading-usd-s-m-futures/api/"
        "ws-streams/1.0.0/schema.yaml"
    ),
    "schema_version": "r4b_v2_usdm_market_strict_parser_contract_v2",
    "signed_decimal_text_regex": _SIGNED_DECIMAL_TEXT_RE.pattern,
    "transport": "websocket",
    "unknown_field_policy": "REJECT_SCHEMA_DRIFT_BUT_RETAIN_RAW_M0",
    "whitespace_policy": "NO_LEADING_OR_TRAILING_FRAME_WHITESPACE",
    "venue": "usdm_futures",
}
USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2: Final = hashlib.sha256(
    canonical_json_line(_PARSER_CONTRACT)
).hexdigest()


class UsdmMarketM1ContractErrorV2(RuntimeError):
    """Raised when a signed USD-M market member fails the exact M1 contract."""


class _CommonFields(TypedDict):
    symbol: str
    venue: VenueV2
    route_id: Literal["usdm_market"]
    stream: str
    promoting_plan_sha256: str
    capture_authority_sha256: str
    protocol_sha256: str
    parser_contract_sha256: str
    m0_leaf_sha256: str
    raw_payload_hash_v2: str
    session_id: str
    plan_id: str
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int


@dataclass(frozen=True, slots=True)
class UsdmAggTradeM1V2:
    """Factory-only exact USD-M aggregate-trade row parsed from one M0 leaf."""

    symbol: str
    venue: VenueV2
    route_id: Literal["usdm_market"]
    stream: str
    promoting_plan_sha256: str
    capture_authority_sha256: str
    protocol_sha256: str
    parser_contract_sha256: str
    m0_leaf_sha256: str
    raw_payload_hash_v2: str
    session_id: str
    plan_id: str
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    event_ms: int
    aggregate_trade_id: int
    price: Decimal
    quantity: Decimal
    normal_quantity: Decimal
    first_trade_id: int
    last_trade_id: int
    trade_time_ms: int
    buyer_maker: bool
    stream_type: Literal[1]
    _factory_token: InitVar[object] = None
    m1_payload_sha256: str = field(init=False, default="")
    schema_version: str = field(init=False, default=_M1_SCHEMA_VERSION)
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise UsdmMarketM1ContractErrorV2(
                "USD-M aggregate trade requires the verified M1 parser factory"
            )
        _validate_common_row(self)
        _validate_agg_trade_row(self)
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        object.__setattr__(self, "m1_payload_sha256", _row_hash(self))

    @property
    def parser_bound(self) -> bool:
        return True

    @property
    def live_reverification_required(self) -> bool:
        return True

    @property
    def current_authority_claimed(self) -> bool:
        return False

    @property
    def cursor_complete(self) -> bool:
        return False

    @property
    def causal_inputs_complete(self) -> bool:
        return False

    @property
    def authority_reason(self) -> str:
        return USDM_MARKET_M1_ONLY_REASON_V2

    @property
    def source_evidence_sha256(self) -> str:
        return self.m1_payload_sha256


@dataclass(frozen=True, slots=True)
class UsdmKline5mM1V2:
    """Factory-only exact USD-M 5-minute kline update parsed from one M0 leaf."""

    symbol: str
    venue: VenueV2
    route_id: Literal["usdm_market"]
    stream: str
    promoting_plan_sha256: str
    capture_authority_sha256: str
    protocol_sha256: str
    parser_contract_sha256: str
    m0_leaf_sha256: str
    raw_payload_hash_v2: str
    session_id: str
    plan_id: str
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    event_ms: int
    interval: Literal["5m"]
    bar_open_ms: int
    bar_close_ms: int
    first_trade_id: int
    last_trade_id: int
    open: Decimal
    close: Decimal
    high: Decimal
    low: Decimal
    base_volume: Decimal
    trade_count: int
    closed: bool
    quote_volume: Decimal
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    ignored_volume: Decimal
    _factory_token: InitVar[object] = None
    m1_payload_sha256: str = field(init=False, default="")
    schema_version: str = field(init=False, default=_M1_SCHEMA_VERSION)
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise UsdmMarketM1ContractErrorV2(
                "USD-M kline requires the verified M1 parser factory"
            )
        _validate_common_row(self)
        _validate_kline_row(self)
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        object.__setattr__(self, "m1_payload_sha256", _row_hash(self))

    @property
    def parser_bound(self) -> bool:
        return True

    @property
    def live_reverification_required(self) -> bool:
        return True

    @property
    def current_authority_claimed(self) -> bool:
        return False

    @property
    def cursor_complete(self) -> bool:
        return False

    @property
    def causal_inputs_complete(self) -> bool:
        return False

    @property
    def authority_reason(self) -> str:
        return USDM_MARKET_M1_ONLY_REASON_V2

    @property
    def source_evidence_sha256(self) -> str:
        return self.m1_payload_sha256


@dataclass(frozen=True, slots=True)
class UsdmMarkPrice1sM1V2:
    """Factory-only exact USD-M 1-second mark-price update from one M0 leaf.

    ``event_ms`` retains Binance ``E``.  The local receipt wall and monotonic
    clocks remain inherited from M0.  Binance ``T`` is stored only as
    ``next_funding_time_ms`` and is never treated as an observation timestamp.
    """

    symbol: str
    venue: VenueV2
    route_id: Literal["usdm_market"]
    stream: str
    promoting_plan_sha256: str
    capture_authority_sha256: str
    protocol_sha256: str
    parser_contract_sha256: str
    m0_leaf_sha256: str
    raw_payload_hash_v2: str
    session_id: str
    plan_id: str
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    event_ms: int
    mark_price: Decimal
    mark_moving_average_price: Decimal
    estimated_settlement_price: Decimal
    index_price: Decimal
    funding_rate: Decimal
    next_funding_time_ms: int
    stream_type: Literal[1]
    _factory_token: InitVar[object] = None
    m1_payload_sha256: str = field(init=False, default="")
    schema_version: str = field(init=False, default=_M1_SCHEMA_VERSION)
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise UsdmMarketM1ContractErrorV2(
                "USD-M mark price requires the verified M1 parser factory"
            )
        _validate_common_row(self)
        _validate_mark_price_row(self)
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        object.__setattr__(self, "m1_payload_sha256", _row_hash(self))

    @property
    def parser_bound(self) -> bool:
        return True

    @property
    def live_reverification_required(self) -> bool:
        return True

    @property
    def current_authority_claimed(self) -> bool:
        return False

    @property
    def cursor_complete(self) -> bool:
        return False

    @property
    def causal_inputs_complete(self) -> bool:
        return False

    @property
    def next_funding_time_is_observation_time(self) -> Literal[False]:
        return False

    @property
    def authority_reason(self) -> str:
        return USDM_MARKET_M1_ONLY_REASON_V2

    @property
    def source_evidence_sha256(self) -> str:
        return self.m1_payload_sha256


type UsdmMarketM1V2 = UsdmAggTradeM1V2 | UsdmKline5mM1V2 | UsdmMarkPrice1sM1V2


def parse_verified_usdm_market_m1_v2(
    leaf: VerifiedRawMembershipLeafV2,
    *,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
    block_directory: str | Path,
    block_root_binding: StorageRootBindingV2,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    integrity_ledger: CaptureIntegrityLedgerV2,
) -> UsdmMarketM1V2:
    """Live-reverify one M0 member and immediately parse its exact combined frame.

    The returned row is parser-bound but remains only an issuance snapshot.  It
    does not prove a source census, gap-free cursor, candle finality set, or the
    completeness of any decision's causal inputs.
    """

    if not isinstance(leaf, VerifiedRawMembershipLeafV2):
        raise TypeError("leaf must be a VerifiedRawMembershipLeafV2")
    validate_provisional_promoting_capture_plans_v2(promoting_plans)
    promoting_plan_sha256 = provisional_promoting_plan_sha256_v2(promoting_plans)
    if authority.plan_sha256 != promoting_plan_sha256:
        raise UsdmMarketM1ContractErrorV2(
            "trusted WAL authority differs from the frozen promoting plan"
        )
    market_plans = tuple(
        item
        for item in promoting_plans
        if isinstance(item, ProvisionalPromotingCapturePlanV2)
        and item.route_id == "usdm_market"
    )
    if len(market_plans) != 1:
        raise UsdmMarketM1ContractErrorV2(
            "promoting plan has no unique USD-M market stream owner"
        )
    market_plan = market_plans[0]

    reverify_verified_raw_membership_leaf_v2(
        leaf,
        block_directory=block_directory,
        block_root_binding=block_root_binding,
        authority=authority,
        policy=policy,
        signing_authority=signing_authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        integrity_ledger=integrity_ledger,
        expected_transport=TransportV2.WEBSOCKET,
        expected_venue=VenueV2.USDM_FUTURES,
        expected_route_id="usdm_market",
        expected_symbol=None,
    )

    record = leaf.record
    if record.symbol is not None:
        raise UsdmMarketM1ContractErrorV2(
            "combined-query M1 raw record symbol must remain unresolved"
        )
    if record.plan_id != market_plan.name:
        raise UsdmMarketM1ContractErrorV2(
            "raw record plan_id differs from the USD-M market plan"
        )
    if record.frame_seq is None:
        raise UsdmMarketM1ContractErrorV2(
            "USD-M WebSocket market record requires frame_seq"
        )
    wrapper = _parse_strict_json_object(record.payload_bytes())
    _require_exact_keys(wrapper, _COMBINED_WRAPPER_KEYS, "combined wrapper")
    stream = _require_text(wrapper.get("stream"), "stream")
    if stream not in market_plan.streams:
        raise UsdmMarketM1ContractErrorV2(
            "combined stream is outside the frozen USD-M market plan"
        )
    data = wrapper.get("data")
    if not isinstance(data, dict):
        raise UsdmMarketM1ContractErrorV2("combined data must be a JSON object")
    typed_data = cast(dict[str, object], data)
    if stream.endswith("@aggTrade"):
        return _parse_agg_trade(leaf, stream, typed_data)
    if stream.endswith("@kline_5m"):
        return _parse_kline_5m(leaf, stream, typed_data)
    if stream.endswith("@markPrice@1s"):
        return _parse_mark_price_1s(leaf, stream, typed_data)
    raise UsdmMarketM1ContractErrorV2(
        "combined stream is not an M1 aggTrade, kline_5m, or markPrice@1s source"
    )


def canonical_usdm_market_m1_v2(row: UsdmMarketM1V2) -> bytes:
    """Serialize one self-consistent M1 issuance snapshot canonically."""

    if not isinstance(
        row,
        (UsdmAggTradeM1V2, UsdmKline5mM1V2, UsdmMarkPrice1sM1V2),
    ):
        raise TypeError("row must be a USD-M market M1 row")
    if row._factory_seal is not _FACTORY_TOKEN:
        raise UsdmMarketM1ContractErrorV2("M1 row factory seal differs")
    _validate_common_row(row)
    if isinstance(row, UsdmAggTradeM1V2):
        _validate_agg_trade_row(row)
    elif isinstance(row, UsdmKline5mM1V2):
        _validate_kline_row(row)
    else:
        _validate_mark_price_row(row)
    expected = _row_hash(row)
    if row.m1_payload_sha256 != expected:
        raise UsdmMarketM1ContractErrorV2("M1 row differs from canonical evidence")
    document = _row_document(row, include_hash=True)
    return canonical_json_line(document)


def _parse_agg_trade(
    leaf: VerifiedRawMembershipLeafV2,
    stream: str,
    data: dict[str, object],
) -> UsdmAggTradeM1V2:
    _require_exact_keys(data, _AGG_TRADE_KEYS, "aggTrade data")
    if data.get("e") != "aggTrade":
        raise UsdmMarketM1ContractErrorV2("aggTrade event type is not exact")
    symbol = _stream_symbol(stream, "@aggTrade")
    if data.get("s") != symbol:
        raise UsdmMarketM1ContractErrorV2(
            "aggTrade payload symbol differs from its combined stream"
        )
    return UsdmAggTradeM1V2(
        **_common_fields(leaf, stream, symbol),
        event_ms=_require_nonnegative_int64(data.get("E"), "E"),
        aggregate_trade_id=_require_nonnegative_int64(data.get("a"), "a"),
        price=_parse_decimal_text(data.get("p"), "p", positive=True),
        quantity=_parse_decimal_text(data.get("q"), "q", positive=True),
        normal_quantity=_parse_decimal_text(data.get("nq"), "nq", positive=False),
        first_trade_id=_require_nonnegative_int64(data.get("f"), "f"),
        last_trade_id=_require_nonnegative_int64(data.get("l"), "l"),
        trade_time_ms=_require_nonnegative_int64(data.get("T"), "T"),
        buyer_maker=_require_bool(data.get("m"), "m"),
        stream_type=_require_usdm_stream_type(data.get("st")),
        _factory_token=_FACTORY_TOKEN,
    )


def _parse_kline_5m(
    leaf: VerifiedRawMembershipLeafV2,
    stream: str,
    data: dict[str, object],
) -> UsdmKline5mM1V2:
    _require_exact_keys(data, _KLINE_OUTER_KEYS, "kline data")
    if data.get("e") != "kline":
        raise UsdmMarketM1ContractErrorV2("kline event type is not exact")
    symbol = _stream_symbol(stream, "@kline_5m")
    if data.get("s") != symbol:
        raise UsdmMarketM1ContractErrorV2(
            "kline outer symbol differs from its combined stream"
        )
    inner = data.get("k")
    if not isinstance(inner, dict):
        raise UsdmMarketM1ContractErrorV2("kline field k must be a JSON object")
    typed_inner = cast(dict[str, object], inner)
    _require_exact_keys(typed_inner, _KLINE_INNER_KEYS, "kline field k")
    if typed_inner.get("s") != symbol or typed_inner.get("i") != "5m":
        raise UsdmMarketM1ContractErrorV2(
            "kline inner symbol or interval differs from its stream"
        )
    return UsdmKline5mM1V2(
        **_common_fields(leaf, stream, symbol),
        event_ms=_require_nonnegative_int64(data.get("E"), "E"),
        interval="5m",
        bar_open_ms=_require_nonnegative_int64(typed_inner.get("t"), "k.t"),
        bar_close_ms=_require_nonnegative_int64(typed_inner.get("T"), "k.T"),
        first_trade_id=_require_nonnegative_int64(typed_inner.get("f"), "k.f"),
        last_trade_id=_require_nonnegative_int64(typed_inner.get("L"), "k.L"),
        open=_parse_decimal_text(typed_inner.get("o"), "k.o", positive=True),
        close=_parse_decimal_text(typed_inner.get("c"), "k.c", positive=True),
        high=_parse_decimal_text(typed_inner.get("h"), "k.h", positive=True),
        low=_parse_decimal_text(typed_inner.get("l"), "k.l", positive=True),
        base_volume=_parse_decimal_text(typed_inner.get("v"), "k.v", positive=False),
        trade_count=_require_nonnegative_int64(typed_inner.get("n"), "k.n"),
        closed=_require_bool(typed_inner.get("x"), "k.x"),
        quote_volume=_parse_decimal_text(typed_inner.get("q"), "k.q", positive=False),
        taker_buy_base_volume=_parse_decimal_text(
            typed_inner.get("V"), "k.V", positive=False
        ),
        taker_buy_quote_volume=_parse_decimal_text(
            typed_inner.get("Q"), "k.Q", positive=False
        ),
        ignored_volume=_parse_decimal_text(
            typed_inner.get("B"), "k.B", positive=False
        ),
        _factory_token=_FACTORY_TOKEN,
    )


def _parse_mark_price_1s(
    leaf: VerifiedRawMembershipLeafV2,
    stream: str,
    data: dict[str, object],
) -> UsdmMarkPrice1sM1V2:
    _require_exact_keys(data, _MARK_PRICE_KEYS, "markPrice@1s data")
    if data.get("e") != "markPriceUpdate":
        raise UsdmMarketM1ContractErrorV2("markPrice event type is not exact")
    symbol = _stream_symbol(stream, "@markPrice@1s")
    if data.get("s") != symbol:
        raise UsdmMarketM1ContractErrorV2(
            "markPrice payload symbol differs from its combined stream"
        )
    return UsdmMarkPrice1sM1V2(
        **_common_fields(leaf, stream, symbol),
        event_ms=_require_nonnegative_int64(data.get("E"), "E"),
        mark_price=_parse_decimal_text(data.get("p"), "p", positive=True),
        mark_moving_average_price=_parse_decimal_text(
            data.get("ap"), "ap", positive=True
        ),
        estimated_settlement_price=_parse_decimal_text(
            data.get("P"), "P", positive=False
        ),
        index_price=_parse_decimal_text(data.get("i"), "i", positive=True),
        funding_rate=_parse_signed_decimal_text(data.get("r"), "r"),
        next_funding_time_ms=_require_nonnegative_int64(data.get("T"), "T"),
        stream_type=_require_usdm_stream_type(data.get("st"), event="markPrice"),
        _factory_token=_FACTORY_TOKEN,
    )


def _common_fields(
    leaf: VerifiedRawMembershipLeafV2,
    stream: str,
    symbol: str,
) -> _CommonFields:
    record = leaf.record
    if record.frame_seq is None:
        raise UsdmMarketM1ContractErrorV2("WebSocket M1 row requires frame_seq")
    return {
        "symbol": symbol,
        "venue": VenueV2.USDM_FUTURES,
        "route_id": "usdm_market",
        "stream": stream,
        "promoting_plan_sha256": leaf.authority.plan_sha256,
        "capture_authority_sha256": leaf.authority_sha256,
        "protocol_sha256": record.protocol_hash,
        "parser_contract_sha256": USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2,
        "m0_leaf_sha256": leaf.leaf_sha256,
        "raw_payload_hash_v2": leaf.raw_payload_hash_v2,
        "session_id": record.session_id,
        "plan_id": record.plan_id,
        "connection_id": record.connection_id,
        "generation": record.generation,
        "frame_seq": record.frame_seq,
        "ingest_seq": record.ingest_seq,
        "receipt_wall_ms": record.receipt_wall_ms,
        "receipt_monotonic_ns": record.receipt_monotonic_ns,
    }


def _validate_common_row(row: UsdmMarketM1V2) -> None:
    if row.schema_version != _M1_SCHEMA_VERSION:
        raise UsdmMarketM1ContractErrorV2("unsupported USD-M market M1 schema")
    if _SYMBOL_RE.fullmatch(row.symbol) is None:
        raise UsdmMarketM1ContractErrorV2("M1 symbol is not normalized USD-M USDT")
    if row.venue is not VenueV2.USDM_FUTURES or row.route_id != "usdm_market":
        raise UsdmMarketM1ContractErrorV2("M1 row is outside the USD-M market route")
    if isinstance(row, UsdmAggTradeM1V2):
        suffix = "@aggTrade"
    elif isinstance(row, UsdmKline5mM1V2):
        suffix = "@kline_5m"
    else:
        suffix = "@markPrice@1s"
    if row.stream != f"{row.symbol.lower()}{suffix}":
        raise UsdmMarketM1ContractErrorV2("M1 stream differs from row identity")
    for value, name in (
        (row.promoting_plan_sha256, "promoting_plan_sha256"),
        (row.capture_authority_sha256, "capture_authority_sha256"),
        (row.protocol_sha256, "protocol_sha256"),
        (row.parser_contract_sha256, "parser_contract_sha256"),
        (row.m0_leaf_sha256, "m0_leaf_sha256"),
        (row.raw_payload_hash_v2, "raw_payload_hash_v2"),
    ):
        _require_sha256(value, name)
    if row.parser_contract_sha256 != USDM_MARKET_M1_PARSER_CONTRACT_SHA256_V2:
        raise UsdmMarketM1ContractErrorV2("M1 parser contract hash differs")
    for value, name in (
        (row.session_id, "session_id"),
        (row.plan_id, "plan_id"),
        (row.connection_id, "connection_id"),
    ):
        _require_identity(value, name)
    _require_positive_int64(row.generation, "generation")
    _require_positive_int64(row.frame_seq, "frame_seq")
    _require_positive_int64(row.ingest_seq, "ingest_seq")
    _require_nonnegative_int64(row.receipt_wall_ms, "receipt_wall_ms")
    _require_nonnegative_int64(row.receipt_monotonic_ns, "receipt_monotonic_ns")


def _validate_agg_trade_row(row: UsdmAggTradeM1V2) -> None:
    _require_nonnegative_int64(row.event_ms, "event_ms")
    _require_nonnegative_int64(row.aggregate_trade_id, "aggregate_trade_id")
    _require_positive_decimal(row.price, "price")
    _require_positive_decimal(row.quantity, "quantity")
    _require_nonnegative_decimal(row.normal_quantity, "normal_quantity")
    if row.normal_quantity > row.quantity:
        raise UsdmMarketM1ContractErrorV2(
            "normal_quantity cannot exceed aggregate quantity"
        )
    _require_nonnegative_int64(row.first_trade_id, "first_trade_id")
    _require_nonnegative_int64(row.last_trade_id, "last_trade_id")
    if row.first_trade_id > row.last_trade_id:
        raise UsdmMarketM1ContractErrorV2(
            "aggregate trade ID bounds are reversed"
        )
    _require_nonnegative_int64(row.trade_time_ms, "trade_time_ms")
    if row.event_ms < row.trade_time_ms:
        raise UsdmMarketM1ContractErrorV2(
            "aggregate-trade event time precedes transaction time"
        )
    if type(row.buyer_maker) is not bool:
        raise UsdmMarketM1ContractErrorV2("buyer_maker must be boolean")
    if type(row.stream_type) is not int or row.stream_type != 1:
        raise UsdmMarketM1ContractErrorV2("USD-M aggregate trade st must equal 1")


def _validate_kline_row(row: UsdmKline5mM1V2) -> None:
    _require_nonnegative_int64(row.event_ms, "event_ms")
    if row.interval != "5m":
        raise UsdmMarketM1ContractErrorV2("kline interval must be exactly 5m")
    _require_nonnegative_int64(row.bar_open_ms, "bar_open_ms")
    _require_nonnegative_int64(row.bar_close_ms, "bar_close_ms")
    if row.bar_open_ms % _KLINE_INTERVAL_MS != 0:
        raise UsdmMarketM1ContractErrorV2("5m kline open is not interval-aligned")
    if row.bar_close_ms != row.bar_open_ms + _KLINE_INTERVAL_MS - 1:
        raise UsdmMarketM1ContractErrorV2("5m kline close boundary is not exact")
    _require_nonnegative_int64(row.first_trade_id, "first_trade_id")
    _require_nonnegative_int64(row.last_trade_id, "last_trade_id")
    if row.first_trade_id > row.last_trade_id:
        raise UsdmMarketM1ContractErrorV2("kline trade ID bounds are reversed")
    for value, name in (
        (row.open, "open"),
        (row.close, "close"),
        (row.high, "high"),
        (row.low, "low"),
    ):
        _require_positive_decimal(value, name)
    if row.low > min(row.open, row.close) or row.high < max(row.open, row.close):
        raise UsdmMarketM1ContractErrorV2(
            "kline OHLC geometry is internally inconsistent"
        )
    for value, name in (
        (row.base_volume, "base_volume"),
        (row.quote_volume, "quote_volume"),
        (row.taker_buy_base_volume, "taker_buy_base_volume"),
        (row.taker_buy_quote_volume, "taker_buy_quote_volume"),
        (row.ignored_volume, "ignored_volume"),
    ):
        _require_nonnegative_decimal(value, name)
    if row.taker_buy_base_volume > row.base_volume:
        raise UsdmMarketM1ContractErrorV2(
            "taker-buy base volume exceeds total base volume"
        )
    if row.taker_buy_quote_volume > row.quote_volume:
        raise UsdmMarketM1ContractErrorV2(
            "taker-buy quote volume exceeds total quote volume"
        )
    _require_nonnegative_int64(row.trade_count, "trade_count")
    if type(row.closed) is not bool:
        raise UsdmMarketM1ContractErrorV2("kline closed flag must be boolean")


def _validate_mark_price_row(row: UsdmMarkPrice1sM1V2) -> None:
    _require_nonnegative_int64(row.event_ms, "event_ms")
    _require_positive_decimal(row.mark_price, "mark_price")
    _require_positive_decimal(
        row.mark_moving_average_price, "mark_moving_average_price"
    )
    _require_nonnegative_decimal(
        row.estimated_settlement_price, "estimated_settlement_price"
    )
    _require_positive_decimal(row.index_price, "index_price")
    _require_finite_decimal(row.funding_rate, "funding_rate")
    _require_nonnegative_int64(row.next_funding_time_ms, "next_funding_time_ms")
    if type(row.stream_type) is not int or row.stream_type != 1:
        raise UsdmMarketM1ContractErrorV2("USD-M markPrice st must equal 1")


def _parse_strict_json_object(payload: bytes) -> dict[str, object]:
    if not isinstance(payload, bytes):
        raise TypeError("payload must be immutable bytes")
    if len(payload) > _MAX_FRAME_BYTES:
        raise UsdmMarketM1ContractErrorV2(
            "USD-M market frame exceeds the frozen M1 byte limit"
        )
    try:
        text = payload.decode("utf-8")
        if not text or text.strip() != text:
            raise UsdmMarketM1ContractErrorV2(
                "USD-M market frame has leading or trailing whitespace"
            )
        document = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_json_constant,
            parse_float=_reject_json_float,
            parse_int=_parse_json_integer,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise UsdmMarketM1ContractErrorV2(
            "USD-M market frame is not strict UTF-8 JSON"
        ) from exc
    if not isinstance(document, dict):
        raise UsdmMarketM1ContractErrorV2(
            "USD-M market frame must be a JSON object"
        )
    return cast(dict[str, object], document)


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise UsdmMarketM1ContractErrorV2(
                "USD-M market JSON repeats an object key"
            )
        document[key] = value
    return document


def _reject_json_constant(value: str) -> object:
    raise UsdmMarketM1ContractErrorV2(
        f"USD-M market JSON contains forbidden constant {value}"
    )


def _reject_json_float(value: str) -> object:
    raise UsdmMarketM1ContractErrorV2(
        f"USD-M market JSON contains non-schema numeric literal {value}"
    )


def _parse_json_integer(value: str) -> int:
    if len(value) > 20:
        raise UsdmMarketM1ContractErrorV2(
            "USD-M market JSON integer exceeds signed int64 text length"
        )
    parsed = int(value)
    if not _INT64_MIN <= parsed <= _INT64_MAX:
        raise UsdmMarketM1ContractErrorV2(
            "USD-M market JSON integer is outside signed int64"
        )
    return parsed


def _require_exact_keys(
    value: dict[str, object],
    expected: frozenset[str],
    label: str,
) -> None:
    if set(value) != expected:
        raise UsdmMarketM1ContractErrorV2(f"{label} schema is not exact")


def _stream_symbol(stream: str, suffix: str) -> str:
    if not stream.endswith(suffix):
        raise UsdmMarketM1ContractErrorV2("combined stream suffix differs")
    symbol = stream[: -len(suffix)].upper()
    if _SYMBOL_RE.fullmatch(symbol) is None:
        raise UsdmMarketM1ContractErrorV2("combined stream symbol is not normalized")
    if stream != f"{symbol.lower()}{suffix}":
        raise UsdmMarketM1ContractErrorV2("combined stream case is not canonical")
    return symbol


def _require_text(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > _MAX_IDENTITY_LENGTH
        or value.strip() != value
        or any(character in value for character in "\r\n\x00")
    ):
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} must be bounded normalized text"
        )
    return value


def _parse_decimal_text(
    value: object,
    field_name: str,
    *,
    positive: bool,
) -> Decimal:
    if (
        not isinstance(value, str)
        or len(value) > _MAX_DECIMAL_TEXT_LENGTH
        or _DECIMAL_TEXT_RE.fullmatch(value) is None
    ):
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} must be a strict nonnegative decimal string"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} is not a finite decimal"
        ) from exc
    if not parsed.is_finite() or parsed < 0 or (positive and parsed <= 0):
        qualifier = "positive" if positive else "nonnegative"
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} must be {qualifier} finite Decimal"
        )
    return parsed


def _parse_signed_decimal_text(value: object, field_name: str) -> Decimal:
    if (
        not isinstance(value, str)
        or len(value) > _MAX_DECIMAL_TEXT_LENGTH
        or _SIGNED_DECIMAL_TEXT_RE.fullmatch(value) is None
    ):
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} must be a strict signed decimal string"
        )
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} is not a finite decimal"
        ) from exc
    _require_finite_decimal(parsed, field_name)
    return parsed


def _require_bool(value: object, field_name: str) -> bool:
    if type(value) is not bool:
        raise UsdmMarketM1ContractErrorV2(f"{field_name} must be boolean")
    return value


def _require_usdm_stream_type(
    value: object,
    *,
    event: str = "aggTrade",
) -> Literal[1]:
    if type(value) is not int or value != 1:
        raise UsdmMarketM1ContractErrorV2(
            f"{event} st must be integer 1 for USD-M"
        )
    return 1


def _require_nonnegative_int64(value: object, field_name: str) -> int:
    if type(value) is not int or not 0 <= value <= _INT64_MAX:
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} must be a nonnegative int64"
        )
    return value


def _require_positive_int64(value: object, field_name: str) -> int:
    parsed = _require_nonnegative_int64(value, field_name)
    if parsed == 0:
        raise UsdmMarketM1ContractErrorV2(f"{field_name} must be positive")
    return parsed


def _require_positive_decimal(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite() or value <= 0:
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} must be positive finite Decimal"
        )


def _require_nonnegative_decimal(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} must be nonnegative finite Decimal"
        )


def _require_finite_decimal(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} must be finite Decimal"
        )


def _require_identity(value: str, field_name: str) -> None:
    _require_text(value, field_name)


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise UsdmMarketM1ContractErrorV2(
            f"{field_name} must be a lowercase SHA-256 digest"
        )


def _row_hash(row: UsdmMarketM1V2) -> str:
    return hashlib.sha256(
        _ROW_HASH_DOMAIN + canonical_json_line(_row_document(row, include_hash=False))
    ).hexdigest()


def _row_document(
    row: UsdmMarketM1V2,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document = asdict(row)
    document.pop("_factory_seal", None)
    document.pop("m1_payload_sha256", None)
    for name, value in tuple(document.items()):
        if isinstance(value, Decimal):
            document[name] = str(value)
        elif isinstance(value, VenueV2):
            document[name] = value.value
    document.update(
        {
            "authority_reason": row.authority_reason,
            "causal_inputs_complete": row.causal_inputs_complete,
            "current_authority_claimed": row.current_authority_claimed,
            "cursor_complete": row.cursor_complete,
            "live_reverification_required": row.live_reverification_required,
            "parser_bound": row.parser_bound,
        }
    )
    if isinstance(row, UsdmMarkPrice1sM1V2):
        document["next_funding_time_is_observation_time"] = (
            row.next_funding_time_is_observation_time
        )
    if include_hash:
        document["m1_payload_sha256"] = row.m1_payload_sha256
    return document
