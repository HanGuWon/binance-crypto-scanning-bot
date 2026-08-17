from __future__ import annotations

import re
from collections.abc import AsyncIterable
from urllib.parse import quote

from signalbot.capture.models import CaptureEnvelopeV1, payload_text, validate_public_route
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.receipts import (
    IngestSequencer,
    ReceiptClock,
    ReceiptTimestamp,
    SystemReceiptClock,
)
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import (
    FUTURES_WS_MARKET,
    FUTURES_WS_PUBLIC,
    SPOT_WS_MARKET_DATA_ONLY,
    WebSocketPlan,
)

__all__ = [
    "IngestSequencer",
    "PublicWebSocketCaptureAdapter",
    "ReceiptClock",
    "ReceiptTimestamp",
    "SystemReceiptClock",
    "validate_public_websocket_plan",
]

_SYMBOL = r"[a-z0-9]+"
_SPOT_STREAM = re.compile(rf"^{_SYMBOL}@(aggTrade|bookTicker|depth@100ms|kline_5m)$")
_FUTURES_MARKET_STREAM = re.compile(
    rf"^{_SYMBOL}@(aggTrade|kline_5m|markPrice@1s)$"
)
_FUTURES_PUBLIC_STREAM = re.compile(rf"^{_SYMBOL}@(bookTicker|depth@100ms)$")

class PublicWebSocketCaptureAdapter:
    """Receipt-at-iterator seam for public Binance WebSocket frames.

    ``consume`` deliberately owns no socket connection.  The caller supplies the
    real socket async iterator, so timestamping is the first statement after the
    iterator yields.  Raw preservation and bounded ``put_nowait`` happen before
    any downstream decode or await; downstream dispatch is owned by
    ``CapturePipeline`` and occurs only after successful storage.
    """

    def __init__(
        self,
        plan: WebSocketPlan,
        *,
        plan_sha256: str,
        process_boot_id: str,
        connection_id: str,
        pipeline: CapturePipeline,
        clock: ReceiptClock,
        sequencer: IngestSequencer,
    ) -> None:
        validate_public_websocket_plan(plan)
        if not process_boot_id or not connection_id:
            raise ValueError("process_boot_id and connection_id must be non-empty")
        self.plan = plan
        self.plan_sha256 = plan_sha256
        self.process_boot_id = process_boot_id
        self.connection_id = connection_id
        self.pipeline = pipeline
        self.clock = clock
        self.sequencer = sequencer
        self.frame_seq = 0

    async def consume(self, frames: AsyncIterable[str | bytes]) -> None:
        async for raw in frames:
            receipt = self.clock.capture()
            self.frame_seq += 1
            ingest_seq = self.sequencer.next()
            payload, encoding = payload_text(raw)
            # Do not parse on the producer hot path.  Combined-wrapper stream
            # resolution belongs to an offline materializer after raw evidence
            # is durably captured.
            stream = (
                self.plan.streams[0]
                if len(self.plan.streams) == 1
                else f"combined:{self.plan.name}"
            )
            envelope = CaptureEnvelopeV1(
                received_at_ms=receipt.received_at_ms,
                received_monotonic_ns=receipt.received_monotonic_ns,
                plan_sha256=self.plan_sha256,
                process_boot_id=self.process_boot_id,
                connection_id=self.connection_id,
                frame_seq=self.frame_seq,
                ingest_seq=ingest_seq,
                market=self.plan.market,
                route=self.plan.route,
                stream=stream,
                subscription_streams=self.plan.streams,
                raw_payload=payload,
                raw_payload_encoding=encoding,
            )
            self.pipeline.offer(envelope)


def validate_public_websocket_plan(plan: WebSocketPlan) -> None:
    """Reject private, authenticated, or unknown WebSocket paths and streams."""

    validate_public_route(plan.market, plan.route)
    if not plan.streams:
        raise ValueError("capture WebSocket plan must contain streams")
    if plan.market is Market.SPOT:
        expected_base = SPOT_WS_MARKET_DATA_ONLY
        matcher = _SPOT_STREAM
    elif plan.route == "public":
        expected_base = FUTURES_WS_PUBLIC
        matcher = _FUTURES_PUBLIC_STREAM
    else:
        expected_base = FUTURES_WS_MARKET
        matcher = _FUTURES_MARKET_STREAM
    expected_url = expected_base + "/".join(
        quote(stream, safe="@!_-") for stream in plan.streams
    )
    if plan.url != expected_url:
        raise ValueError("capture WebSocket URL is not the routed public plan URL")
    invalid = [stream for stream in plan.streams if matcher.fullmatch(stream) is None]
    if invalid:
        raise ValueError(f"capture WebSocket plan contains non-allowlisted streams: {invalid}")
