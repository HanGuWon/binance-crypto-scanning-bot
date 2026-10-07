"""Raw-first Pump-fade v2 capture materialization into isolated shadow decisions.

The materializer consumes only already captured public Binance records. It owns
no sockets, HTTP client, Discord transport, account data, or order path. A full
capture directory can be replayed deterministically after segment verification;
the same typed boundary may also be used by a future disabled shadow process.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter, ValidationError

from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    ConnectionState,
    ConnectionTransitionV1,
    CoverageState,
    CoverageTransitionV1,
    RestEnvelopeV1,
    RestEnvelopeV2,
    payload_bytes,
    record_to_json_line,
)
from signalbot.capture.storage import consume_segment_lines, verify_capture_segments
from signalbot.pump_fade_v2.public_capture import (
    ListingObservation,
    evidence_to_predicted_funding,
    normalize_closed_kline_frame,
    normalize_exchange_info,
    normalize_funding_metadata,
    normalize_liquidation,
    normalize_oi_history,
    normalize_open_interest,
    normalize_premium_index,
    normalize_public_frame,
    normalize_rest_klines,
)
from signalbot.pump_fade_v2.shadow import AdvanceReceipt, ShadowEpisodeEngine
from signalbot.pump_fade_v2.state import (
    BookQuality,
    ClosedBar,
    Event,
    FundingCap,
    InputPanel,
    LiquidationSample,
    Observation,
    State,
    admit_pump,
)

_RECORD_ADAPTERS: dict[str, TypeAdapter[Any]] = {
    "capture_envelope_v1": TypeAdapter(CaptureEnvelopeV1),
    "rest_envelope_v1": TypeAdapter(RestEnvelopeV1),
    "rest_envelope_v2": TypeAdapter(RestEnvelopeV2),
    "connection_transition_v1": TypeAdapter(ConnectionTransitionV1),
    "coverage_transition_v1": TypeAdapter(CoverageTransitionV1),
}


@dataclass(frozen=True, slots=True)
class _Bbo:
    received_ms: int
    bid: float
    ask: float


@dataclass(slots=True)
class _SymbolInputs:
    bars: dict[int, ClosedBar] = field(default_factory=dict)
    oi: dict[int, Observation] = field(default_factory=dict)
    funding_by_minute: dict[int, Observation] = field(default_factory=dict)
    cap_changes: deque[FundingCap] = field(default_factory=lambda: deque(maxlen=127))
    cap_latest: FundingCap | None = None
    liquidations: deque[LiquidationSample] = field(default_factory=lambda: deque(maxlen=4096))
    listing: ListingObservation | None = None
    bbo: _Bbo | None = None
    book: BookQuality | None = None
    latest_admitted_event_ms: int | None = None
    active_events: dict[str, Event] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MaterializeReceipt:
    admitted_event_ids: tuple[str, ...]
    advances: tuple[AdvanceReceipt, ...]


def _json_payload(record: CaptureEnvelopeV1 | RestEnvelopeV2) -> Any:
    raw = payload_bytes(record.raw_payload, record.raw_payload_encoding)
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("captured Binance payload is not valid JSON") from exc


def _rest_success(record: RestEnvelopeV2) -> bool:
    return (
        record.payload_complete
        and record.response_status is not None
        and 200 <= record.response_status < 300
        and record.error_category is None
    )


def _same_bar(left: ClosedBar, right: ClosedBar) -> bool:
    return (
        left.close_ms == right.close_ms
        and left.open == right.open
        and left.high == right.high
        and left.low == right.low
        and left.close == right.close
        and left.base_volume == right.base_volume
        and left.quote_volume == right.quote_volume
    )


def _depth_notional(
    payload: dict[str, Any], *, bbo: _Bbo, received_ms: int,
) -> BookQuality:
    bids = payload.get("bids")
    asks = payload.get("asks")
    if not isinstance(bids, list) or not isinstance(asks, list):
        raise ValueError("depth snapshot must contain bid and ask arrays")
    mid = (bbo.bid + bbo.ask) / 2
    bid_floor = mid * 0.995
    ask_ceiling = mid * 1.005

    def side_notional(rows: list[Any], *, bid: bool) -> float:
        total = 0.0
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                raise ValueError("depth level must be [price, quantity]")
            price = float(row[0])
            quantity = float(row[1])
            if (
                not math.isfinite(price)
                or not math.isfinite(quantity)
                or price <= 0
                or quantity < 0
            ):
                raise ValueError("depth level contains invalid numbers")
            if (bid and price >= bid_floor) or (not bid and price <= ask_ceiling):
                total += price * quantity
        return total

    return BookQuality(
        received_ms=bbo.received_ms,
        bid=bbo.bid,
        ask=bbo.ask,
        bid_depth_50bps_usdt=side_notional(bids, bid=True),
        ask_depth_50bps_usdt=side_notional(asks, bid=False),
        depth_received_ms=received_ms,
    )


class PumpInputMaterializer:
    """Bounded causal materializer for one immutable Pump capture generation."""

    def __init__(
        self,
        symbols: tuple[str, ...],
        shadow: ShadowEpisodeEngine,
    ) -> None:
        if not symbols or len(symbols) != len(set(symbols)) or len(symbols) > 24:
            raise ValueError("materializer requires 1..24 distinct admitted symbols")
        self.symbols = symbols
        self.admitted_symbols = frozenset(symbols)
        self.shadow = shadow
        self._states = {symbol: _SymbolInputs() for symbol in symbols}
        self._process_boot_id: str | None = None
        self._last_ingest_seq = 0

    def panel(self, symbol: str) -> InputPanel:
        state = self._states[symbol]
        bars = tuple(sorted(state.bars.values(), key=lambda item: item.close_ms))
        oi = tuple(sorted(state.oi.values(), key=lambda item: (item.received_ms, item.event_ms)))
        funding = tuple(sorted(
            state.funding_by_minute.values(), key=lambda item: (item.received_ms, item.event_ms)
        ))
        caps = tuple(state.cap_changes)
        if state.cap_latest is not None and (
            not caps or state.cap_latest.observed_ms != caps[-1].observed_ms
        ):
            caps = (*caps, state.cap_latest)
        return InputPanel(
            five_minute=bars,
            fifteen_minute=(),
            oi=oi,
            predicted_funding=funding,
            caps=caps,
            book=state.book,
            liquidations=tuple(state.liquidations),
        )

    def consume(self, record: CaptureRecord) -> MaterializeReceipt:
        self._check_sequence(record)
        admitted: list[str] = []
        evaluation_symbols: set[str] = set()
        now_ms = self._record_receipt_ms(record)
        if isinstance(record, CaptureEnvelopeV1):
            new_event, evaluate = self._consume_websocket(record)
            if new_event is not None:
                admitted.append(new_event.event_id)
            if evaluate is not None:
                evaluation_symbols.add(evaluate)
        elif isinstance(record, RestEnvelopeV2):
            evaluate = self._consume_rest(record)
            if evaluate is not None:
                evaluation_symbols.add(evaluate)
        elif isinstance(record, ConnectionTransitionV1):
            self._consume_connection_transition(record)
        elif isinstance(record, CoverageTransitionV1):
            if record.state is CoverageState.INVALID:
                for state in self._states.values():
                    state.book = None
                    state.bbo = None
        advances: list[AdvanceReceipt] = []
        for symbol in sorted(evaluation_symbols):
            advances.extend(self._evaluate_symbol(symbol, now_ms))
        return MaterializeReceipt(tuple(admitted), tuple(advances))

    def _check_sequence(self, record: CaptureRecord) -> None:
        if self._process_boot_id is None:
            if record.ingest_seq != 1:
                raise CaptureIntegrityError("Pump materializer requires the initial ingest prefix")
            self._process_boot_id = record.process_boot_id
        elif record.process_boot_id != self._process_boot_id:
            raise CaptureIntegrityError("Pump materializer cannot mix process boot identities")
        if record.ingest_seq != self._last_ingest_seq + 1:
            raise CaptureIntegrityError("Pump materializer ingest sequence is not contiguous")
        self._last_ingest_seq = record.ingest_seq

    @staticmethod
    def _record_receipt_ms(record: CaptureRecord) -> int:
        if isinstance(record, RestEnvelopeV1):
            return record.response_received_at_ms
        if isinstance(record, RestEnvelopeV2):
            return record.response_completed_at_ms
        return record.received_at_ms

    def _consume_websocket(self, record: CaptureEnvelopeV1) -> tuple[Event | None, str | None]:
        payload = _json_payload(record)
        if not isinstance(payload, dict):
            raise ValueError("WebSocket capture payload must be a JSON object")
        normalized_bar = normalize_closed_kline_frame(
            payload,
            received_ms=record.received_at_ms,
            admitted_symbols=self.admitted_symbols,
        )
        if normalized_bar is not None:
            symbol, bar = normalized_bar
            self._merge_bar(symbol, bar)
            event = self._admit(symbol, record.received_at_ms)
            return event, symbol
        evidence = normalize_public_frame(
            payload,
            received_ms=record.received_at_ms,
            admitted_symbols=self.admitted_symbols,
        )
        if evidence is None:
            return None, None
        state = self._states[evidence.symbol]
        if evidence.stream.endswith("@bookTicker"):
            state.bbo = _Bbo(
                evidence.received_ms,
                float(str(evidence.fields["bid"])),
                float(str(evidence.fields["ask"])),
            )
            return None, None
        if evidence.stream.endswith("@markPrice@1s"):
            sample = evidence_to_predicted_funding(evidence)
            minute = sample.received_ms // 60_000
            previous = state.funding_by_minute.get(minute)
            if previous is None or sample.value > previous.value:
                state.funding_by_minute[minute] = sample
            while len(state.funding_by_minute) > 1_500:
                del state.funding_by_minute[min(state.funding_by_minute)]
            return None, None
        if evidence.censored_sample:
            sample = normalize_liquidation(evidence)
            if not state.liquidations or state.liquidations[-1] != sample:
                state.liquidations.append(sample)
        return None, None

    def _consume_rest(self, record: RestEnvelopeV2) -> str | None:
        if not _rest_success(record):
            return None
        payload = _json_payload(record)
        role = record.request_role
        received_ms = record.response_completed_at_ms
        if role == "exchange_info_all":
            if not isinstance(payload, dict):
                raise ValueError("exchangeInfo response must be an object")
            for listing in normalize_exchange_info(
                payload, received_ms=received_ms, admitted_symbols=self.admitted_symbols
            ):
                state = self._states[listing.symbol]
                if state.listing is not None and state.listing.onboard_ms != listing.onboard_ms:
                    raise ValueError("listing onboardDate changed across observations")
                state.listing = listing
            return None
        if role == "funding_info_all":
            if not isinstance(payload, list):
                raise ValueError("fundingInfo response must be an array")
            for symbol, state in self._states.items():
                cap = normalize_funding_metadata(payload, symbol=symbol, received_ms=received_ms)
                if cap is not None:
                    self._merge_cap(state, cap)
            return None
        if role.startswith("bootstrap_5m_"):
            symbol = role.removeprefix("bootstrap_5m_")
            if symbol not in self._states or not isinstance(payload, list):
                raise ValueError("invalid Pump bootstrap response")
            for bar in normalize_rest_klines(payload, received_ms=received_ms):
                self._merge_bar(symbol, bar)
            return None
        if role.startswith("oi_history_"):
            symbol = role.removeprefix("oi_history_")
            if symbol not in self._states or not isinstance(payload, list):
                raise ValueError("invalid Pump OI-history response")
            for sample in normalize_oi_history(payload, received_ms=received_ms):
                self._merge_oi(self._states[symbol], sample)
            return None
        if role.startswith("oi_"):
            symbol = role.removeprefix("oi_")
            if symbol not in self._states or not isinstance(payload, dict):
                raise ValueError("invalid Pump open-interest response")
            self._merge_oi(
                self._states[symbol], normalize_open_interest(payload, received_ms=received_ms)
            )
            return None
        if role.startswith("premium_"):
            symbol = role.removeprefix("premium_")
            if symbol not in self._states or not isinstance(payload, dict):
                raise ValueError("invalid Pump premium response")
            sample = normalize_premium_index(payload, received_ms=received_ms)
            minute = sample.received_ms // 60_000
            previous = self._states[symbol].funding_by_minute.get(minute)
            if previous is None or sample.value > previous.value:
                self._states[symbol].funding_by_minute[minute] = sample
            return None
        if role.startswith("quality_depth_") or role.startswith("depth_"):
            prefix = "quality_depth_" if role.startswith("quality_depth_") else "depth_"
            symbol = role.removeprefix(prefix)
            if symbol not in self._states or not isinstance(payload, dict):
                raise ValueError("invalid Pump depth response")
            state = self._states[symbol]
            if state.bbo is not None:
                state.book = _depth_notional(payload, bbo=state.bbo, received_ms=received_ms)
            return symbol if prefix == "quality_depth_" else None
        return None

    def _merge_bar(self, symbol: str, bar: ClosedBar) -> None:
        state = self._states[symbol]
        previous = state.bars.get(bar.close_ms)
        if previous is not None:
            if not _same_bar(previous, bar):
                raise ValueError("same closed kline changed across public observations")
            if bar.received_ms < previous.received_ms:
                state.bars[bar.close_ms] = bar
            return
        state.bars[bar.close_ms] = bar
        if len(state.bars) > 640:
            del state.bars[min(state.bars)]

    @staticmethod
    def _merge_oi(state: _SymbolInputs, sample: Observation) -> None:
        previous = state.oi.get(sample.event_ms)
        if previous is not None:
            if previous.value != sample.value:
                raise ValueError("same OI event timestamp changed across observations")
            if sample.received_ms < previous.received_ms:
                state.oi[sample.event_ms] = sample
            return
        state.oi[sample.event_ms] = sample
        if len(state.oi) > 2_048:
            oldest = min(state.oi.values(), key=lambda item: (item.received_ms, item.event_ms))
            del state.oi[oldest.event_ms]

    @staticmethod
    def _merge_cap(state: _SymbolInputs, cap: FundingCap) -> None:
        previous = state.cap_latest
        by_time = {item.observed_ms: item for item in state.cap_changes}
        same_time = by_time.get(cap.observed_ms)
        if same_time is not None and same_time != cap:
            raise ValueError("funding metadata changed at the same observed timestamp")
        by_time[cap.observed_ms] = cap
        if previous is not None and previous.observed_ms == cap.observed_ms and previous != cap:
            raise ValueError("funding metadata changed at the same observed timestamp")
        ordered = sorted(by_time.values(), key=lambda item: item.observed_ms)[-127:]
        state.cap_changes.clear()
        state.cap_changes.extend(ordered)
        if previous is None or cap.observed_ms > previous.observed_ms:
            state.cap_latest = cap
        elif cap.observed_ms == previous.observed_ms:
            state.cap_latest = cap

    def _admit(self, symbol: str, now_ms: int) -> Event | None:
        state = self._states[symbol]
        listing = state.listing
        if listing is None:
            return None
        bars = tuple(sorted(state.bars.values(), key=lambda item: item.close_ms))
        event = admit_pump(
            symbol,
            bars,
            now_ms=now_ms,
            latest_admitted_event_ms=state.latest_admitted_event_ms,
            listing_open_ms=listing.onboard_ms,
            listing_observed_ms=listing.observed_ms,
        )
        if event is None:
            return None
        state.latest_admitted_event_ms = event.event_ms
        checkpoint = self.shadow.checkpoint(event.event_id)
        if checkpoint is None or checkpoint.state not in (State.SKIP, State.EXPIRED):
            state.active_events[event.event_id] = event
        return event

    def _evaluate_symbol(self, symbol: str, now_ms: int) -> list[AdvanceReceipt]:
        state = self._states[symbol]
        panel = self.panel(symbol)
        output: list[AdvanceReceipt] = []
        for event_id, event in tuple(sorted(state.active_events.items())):
            checkpoint = self.shadow.checkpoint(event_id)
            if checkpoint is not None:
                if checkpoint.state in (State.SKIP, State.EXPIRED):
                    del state.active_events[event_id]
                    continue
                if now_ms <= checkpoint.evaluated_at_ms:
                    continue
            receipt = self.shadow.advance(event, panel, now_ms=now_ms)
            output.append(receipt)
            if receipt.payload["state"] in (State.SKIP.value, State.EXPIRED.value):
                del state.active_events[event_id]
        return output

    def _consume_connection_transition(self, record: ConnectionTransitionV1) -> None:
        if record.state is ConnectionState.CONNECTED:
            return
        if record.route != "public":
            return
        for stream in record.streams:
            if "@bookTicker" not in stream and "@depth" not in stream:
                continue
            symbol = stream.split("@", 1)[0].upper()
            state = self._states.get(symbol)
            if state is not None:
                state.bbo = None
                state.book = None


def replay_capture_directory(
    capture_directory: str | Path,
    materializer: PumpInputMaterializer,
) -> dict[str, object]:
    """Verify one completed Pump capture directory and replay its canonical records."""

    root = Path(capture_directory).absolute()
    if not root.is_dir():
        raise FileNotFoundError("verified Pump capture directory does not exist")
    receipt_path = root / "pump-v2-capture-receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    plan_sha = str(receipt["plan_sha256"])
    process_boot_id = str(receipt["process_boot_id"])
    if tuple(receipt["symbols"]) != materializer.symbols:
        raise CaptureIntegrityError("capture receipt symbols differ from materializer authority")
    manifests = verify_capture_segments(
        root,
        expected_plan_sha256=plan_sha,
        expected_process_boot_id=process_boot_id,
    )
    counts = {"records": 0, "admitted_events": 0, "posted_previews": 0}

    def consume_line(line: bytes) -> None:
        try:
            raw = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CaptureIntegrityError("Pump capture record is invalid JSON") from exc
        if not isinstance(raw, dict) or not isinstance(raw.get("schema_version"), str):
            raise CaptureIntegrityError("Pump capture record lacks a schema version")
        adapter = _RECORD_ADAPTERS.get(raw["schema_version"])
        if adapter is None:
            raise CaptureIntegrityError("Pump capture record schema is unsupported")
        try:
            record = adapter.validate_json(line, strict=True)
        except ValidationError as exc:
            raise CaptureIntegrityError("Pump capture record violates its schema") from exc
        if record_to_json_line(record) != line:
            raise CaptureIntegrityError("Pump capture record is not canonical")
        result = materializer.consume(record)
        counts["records"] += 1
        counts["admitted_events"] += len(result.admitted_event_ids)
        counts["posted_previews"] += sum(item.posted for item in result.advances)

    for manifest in manifests:
        consume_segment_lines(root / manifest.data_file, consume_line)
    return {
        "schema_version": "pump_fade_v2_materialization_receipt_v1",
        "evidence_class": "LOCAL_PUBLIC_CAPTURE_REPLAY_NOT_STRATEGY_CONFIRMATION",
        "segment_count": len(manifests),
        **counts,
    }


def main(argv: Sequence[str] | None = None) -> None:
    """Replay a completed local Pump capture into a separate preview-only SQLite DB."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-dir", required=True, type=Path)
    parser.add_argument("--shadow-db", required=True, type=Path)
    parser.add_argument("--receipt-out", required=True, type=Path)
    args = parser.parse_args(argv)
    capture_dir = args.capture_dir.absolute()
    if not capture_dir.is_dir():
        raise FileNotFoundError("Pump capture directory does not exist")
    capture_receipt = json.loads(
        (capture_dir / "pump-v2-capture-receipt.json").read_text(encoding="utf-8")
    )
    raw_symbols = capture_receipt.get("symbols")
    if not isinstance(raw_symbols, list) or not raw_symbols or not all(
        isinstance(item, str) for item in raw_symbols
    ):
        raise ValueError("Pump capture receipt does not contain a valid symbol authority")
    engine = ShadowEpisodeEngine(args.shadow_db)
    materializer = PumpInputMaterializer(tuple(raw_symbols), engine)
    receipt = replay_capture_directory(capture_dir, materializer)
    args.receipt_out.parent.mkdir(parents=True, exist_ok=True)
    args.receipt_out.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(receipt, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
