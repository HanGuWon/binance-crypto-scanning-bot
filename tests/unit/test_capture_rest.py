from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from signalbot.capture.config import capture_rest_request_plan
from signalbot.capture.handoff import BoundedCaptureHandoff, CaptureFatalState
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    CoverageTransitionV1,
    RawPayloadEncoding,
    RestEnvelopeV2,
    RestErrorCategory,
    payload_bytes,
    record_to_json_line,
)
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.receipts import IngestSequencer, ReceiptTimestamp
from signalbot.capture.rest import PublicRestCaptureAdapter
from signalbot.capture.storage import SegmentedCaptureWriter, verify_capture_segments
from signalbot.capture.websocket import PublicWebSocketCaptureAdapter
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import (
    SPOT_WS_MARKET_DATA_ONLY,
    WebSocketPlan,
)

PLAN_SHA256 = hashlib.sha256(b"prospective-rest-capture-test").hexdigest()
SPOT_TIME_URL = "https://data-api.binance.vision/api/v3/time"
FUTURES_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest"


class SequenceClock:
    def __init__(self, *timestamps: ReceiptTimestamp) -> None:
        self._timestamps = iter(timestamps)
        self.calls = 0

    def capture(self) -> ReceiptTimestamp:
        self.calls += 1
        return next(self._timestamps)


class RecordingWriter:
    def __init__(self) -> None:
        self.records: list[CaptureRecord] = []

    def append(self, record: CaptureRecord, encoded_line: bytes) -> None:
        assert encoded_line == record_to_json_line(record)
        self.records.append(record)

    def close(self) -> None:
        return

    def abort(self) -> None:
        return

    def write_emergency_transition(self, transition: CoverageTransitionV1) -> None:
        self.records.append(transition)


class FailingResponseStream(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"partial"
        raise httpx.ReadError("secret transport detail must not be retained")

    async def aclose(self) -> None:
        return


class BlockingResponseStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"partial"
        self.entered.set()
        await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed = True


class CancellationResistantCloseStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.close_started = asyncio.Event()
        self.release_close = asyncio.Event()
        self.close_finished = asyncio.Event()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"{}"

    async def aclose(self) -> None:
        self.close_started.set()
        try:
            while not self.release_close.is_set():
                try:
                    await self.release_close.wait()
                except asyncio.CancelledError:
                    continue
        finally:
            self.close_finished.set()


def _pipeline() -> tuple[CapturePipeline, RecordingWriter]:
    writer = RecordingWriter()
    pipeline = CapturePipeline(
        BoundedCaptureHandoff(
            max_events=32,
            max_bytes=32 * 1024 * 1024,
            fatal_state=CaptureFatalState(),
        ),
        writer,
    )
    pipeline.start()
    return pipeline, writer


def _clock_for_success() -> SequenceClock:
    return SequenceClock(
        ReceiptTimestamp(1_000, 10_000),
        ReceiptTimestamp(1_001, 10_001),
        ReceiptTimestamp(1_002, 10_002),
    )


async def _one_frame(raw: str | bytes) -> AsyncIterator[str | bytes]:
    yield raw


@pytest.mark.asyncio
async def test_binary_response_is_losslessly_preserved_and_offered() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"\xff\x00")

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method="GET",
            market=Market.SPOT,
            url=SPOT_TIME_URL,
            request_role="clock_sample",
            correlation_id="clock-1",
            attempt=1,
        )
    await pipeline.stop()

    assert envelope.raw_payload_encoding is RawPayloadEncoding.BASE64
    assert payload_bytes(envelope.raw_payload, envelope.raw_payload_encoding) == b"\xff\x00"
    assert envelope.payload_complete is True
    assert writer.records == [envelope]


@pytest.mark.asyncio
async def test_non_json_response_and_sorted_query_are_preserved_without_parsing() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["accept-encoding"] == "identity"
        assert request.headers["user-agent"] == "binance-signalbot-capture/0.1"
        assert request.url.params.multi_items() == [
            ("limit", "5"),
            ("symbol", "BTCUSDT"),
        ]
        return httpx.Response(200, text="not-json")

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method="GET",
            market=Market.FUTURES,
            url=FUTURES_OI_URL,
            request_role="open_interest",
            correlation_id="oi-1",
            attempt=1,
            query=(('symbol', 'BTCUSDT'), ('limit', '5')),
        )
    await pipeline.stop()

    assert envelope.canonical_query == (("limit", "5"), ("symbol", "BTCUSDT"))
    assert envelope.raw_payload == "not-json"
    assert envelope.raw_payload_encoding is RawPayloadEncoding.TEXT
    assert envelope.error_category is None
    assert writer.records == [envelope]


@pytest.mark.asyncio
async def test_fixed_spot_exchange_info_symbols_query_is_encoded_and_preserved() -> None:
    decoded_symbols = '["BTCUSDT","ETHUSDT","SOLUSDT"]'
    encoded_query = (
        "symbols=%5B%22BTCUSDT%22%2C%22ETHUSDT%22%2C%22SOLUSDT%22%5D"
    )
    entry = next(
        item for item in capture_rest_request_plan() if item.role == "spot_exchange_info"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.query == encoded_query.encode("ascii")
        assert request.url.params.multi_items() == [("symbols", decoded_symbols)]
        return httpx.Response(200, json={"symbols": []})

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method=entry.method,
            market=entry.market,
            url=f"{entry.rest_base}{entry.path}",
            request_role=entry.role,
            correlation_id="spot-exchange-info-1",
            attempt=1,
            query=entry.fixed_query,
        )
    await pipeline.stop()

    assert envelope.endpoint_path == "/api/v3/exchangeInfo"
    assert envelope.canonical_query == (("symbols", decoded_symbols),)
    assert envelope.payload_complete is True
    assert envelope.error_category is None
    assert writer.records == [envelope]


@pytest.mark.asyncio
async def test_429_preserves_body_and_only_normalized_allowlisted_headers() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={
                "X-MBX-USED-WEIGHT-1M": "12",
                "Retry-After": "2",
                "Date": "Thu, 17 Jul 2026 00:00:00 GMT",
                "Content-Type": "application/json",
                "Set-Cookie": "private=value",
                "Location": "https://example.invalid/secret",
            },
            content=b'{"code":-1003}',
        )

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method="GET",
            market=Market.SPOT,
            url=SPOT_TIME_URL,
            request_role="clock_sample",
            correlation_id="rate-limit-1",
            attempt=2,
        )
    await pipeline.stop()

    assert envelope.response_status == 429
    assert envelope.raw_payload == '{"code":-1003}'
    assert envelope.payload_complete is True
    assert envelope.error_category is RestErrorCategory.HTTP_STATUS
    assert envelope.error_detail == "HTTP status 429"
    assert envelope.response_headers == (
        ("content-type", "application/json"),
        ("date", "Thu, 17 Jul 2026 00:00:00 GMT"),
        ("retry-after", "2"),
        ("x-mbx-used-weight-1m", "12"),
    )
    assert writer.records == [envelope]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected_category", "expected_detail"),
    [
        pytest.param(
            200,
            RestErrorCategory.RESPONSE_CLOSE,
            "response close failed after response headers",
            id="successful-body-close-failure",
        ),
        pytest.param(
            429,
            RestErrorCategory.HTTP_STATUS,
            "HTTP status 429",
            id="http-status-remains-primary",
        ),
    ],
)
async def test_response_close_failure_is_recorded_without_losing_http_status(
    status: int,
    expected_category: RestErrorCategory,
    expected_detail: str,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        response = httpx.Response(status, content=b'{"code":-1003}')

        async def fail_close() -> None:
            raise httpx.ReadError("close detail must not be retained")

        response.aclose = fail_close  # pyright: ignore[reportAttributeAccessIssue]
        return response

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method="GET",
            market=Market.SPOT,
            url=SPOT_TIME_URL,
            request_role="clock_sample",
            correlation_id=f"close-{status}",
            attempt=1,
        )
    await pipeline.stop()

    assert envelope.payload_complete is True
    assert envelope.error_category is expected_category
    assert envelope.error_detail == expected_detail
    assert "close detail" not in repr(envelope)
    assert writer.records == [envelope]


@pytest.mark.asyncio
async def test_pre_response_network_error_is_sanitized_timestamped_and_offered() -> None:
    transport_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal transport_calls
        transport_calls += 1
        raise httpx.ConnectError(
            "secret-token=do-not-store",
            request=request,
        )

    clock = SequenceClock(
        ReceiptTimestamp(2_000, 20_000),
        ReceiptTimestamp(2_001, 20_001),
    )
    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=clock,
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method="GET",
            market=Market.SPOT,
            url=SPOT_TIME_URL,
            request_role="clock_sample",
            correlation_id="network-1",
            attempt=1,
        )
    await pipeline.stop()

    assert clock.calls == 2
    assert envelope.response_first_byte_at_ms is None
    assert envelope.response_first_byte_monotonic_ns is None
    assert envelope.response_completed_monotonic_ns == 20_001
    assert envelope.response_status is None
    assert envelope.payload_complete is False
    assert envelope.error_category is RestErrorCategory.NETWORK
    assert envelope.error_detail == "network request failed before response headers"
    assert "secret" not in envelope.error_detail
    assert transport_calls == 1
    assert writer.records == [envelope]


@pytest.mark.asyncio
async def test_response_body_read_error_preserves_bounded_partial_attempt() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=FailingResponseStream())

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method="GET",
            market=Market.SPOT,
            url=SPOT_TIME_URL,
            request_role="clock_sample",
            correlation_id="read-error-1",
            attempt=1,
        )
    await pipeline.stop()

    assert envelope.raw_payload == "partial"
    assert envelope.payload_complete is False
    assert envelope.error_category is RestErrorCategory.RESPONSE_READ
    assert envelope.error_detail == "response body read failed after response headers"
    assert "secret" not in envelope.error_detail
    assert writer.records == [envelope]


@pytest.mark.asyncio
async def test_cancellation_after_headers_persists_partial_attempt_then_reraises() -> None:
    stream = BlockingResponseStream()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-cancel-body",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        task = asyncio.create_task(
            adapter.capture_attempt(
                method="GET",
                market=Market.SPOT,
                url=SPOT_TIME_URL,
                request_role="clock_sample",
                correlation_id="cancel-body-1",
                attempt=1,
            )
        )
        await asyncio.wait_for(stream.entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await pipeline.stop()

    [envelope] = writer.records
    assert isinstance(envelope, RestEnvelopeV2)
    assert envelope.raw_payload == "partial"
    assert envelope.payload_complete is False
    assert envelope.error_category is RestErrorCategory.CANCELLED
    assert envelope.error_detail == "request cancelled while reading response body"
    assert stream.closed is True


@pytest.mark.asyncio
async def test_cancellation_before_headers_persists_empty_attempt_then_reraises() -> None:
    entered = asyncio.Event()

    async def handler(_request: httpx.Request) -> httpx.Response:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    clock = SequenceClock(
        ReceiptTimestamp(5_000, 50_000),
        ReceiptTimestamp(5_001, 50_001),
    )
    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-cancel-send",
        pipeline=pipeline,
        clock=clock,
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        task = asyncio.create_task(
            adapter.capture_attempt(
                method="GET",
                market=Market.SPOT,
                url=SPOT_TIME_URL,
                request_role="clock_sample",
                correlation_id="cancel-send-1",
                attempt=1,
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    await pipeline.stop()

    [envelope] = writer.records
    assert isinstance(envelope, RestEnvelopeV2)
    assert envelope.response_status is None
    assert envelope.response_first_byte_at_ms is None
    assert envelope.raw_payload == ""
    assert envelope.payload_complete is False
    assert envelope.error_category is RestErrorCategory.CANCELLED
    assert envelope.error_detail == "request cancelled before response headers"


@pytest.mark.asyncio
async def test_repeated_cancel_during_blocked_close_cannot_erase_completed_attempt() -> None:
    stream = CancellationResistantCloseStream()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-cancel-close",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        task = asyncio.create_task(
            adapter.capture_attempt(
                method="GET",
                market=Market.SPOT,
                url=SPOT_TIME_URL,
                request_role="clock_sample",
                correlation_id="cancel-close-1",
                attempt=1,
            )
        )
        await asyncio.wait_for(stream.close_started.wait(), timeout=1)
        task.cancel()
        await asyncio.sleep(0)
        assert task.done()
        with pytest.raises(asyncio.CancelledError):
            await task
        task.cancel()
        stream.release_close.set()
        await asyncio.wait_for(stream.close_finished.wait(), timeout=1)
    await pipeline.stop()

    [envelope] = writer.records
    assert isinstance(envelope, RestEnvelopeV2)
    assert envelope.raw_payload == "{}"
    assert envelope.payload_complete is True
    assert envelope.error_category is RestErrorCategory.CANCELLED
    assert envelope.error_detail == "request cancelled during response close"


@pytest.mark.asyncio
async def test_rest_admission_receipt_cannot_backdate_websocket_during_close() -> None:
    stream = CancellationResistantCloseStream()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=stream)

    clock = SequenceClock(
        ReceiptTimestamp(1_000, 10_000),
        ReceiptTimestamp(1_001, 10_001),
        ReceiptTimestamp(1_002, 10_002),
        ReceiptTimestamp(1_003, 10_003),
    )
    sequencer = IngestSequencer()
    pipeline, writer = _pipeline()
    websocket = PublicWebSocketCaptureAdapter(
        WebSocketPlan(
            name="spot-one",
            market=Market.SPOT,
            route="spot",
            streams=("btcusdt@aggTrade",),
            url=f"{SPOT_WS_MARKET_DATA_ONLY}btcusdt@aggTrade",
        ),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-interleave",
        connection_id="connection-1",
        pipeline=pipeline,
        clock=clock,
        sequencer=sequencer,
    )
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-interleave",
        pipeline=pipeline,
        clock=clock,
        sequencer=sequencer,
        transport=httpx.MockTransport(handler),
    ) as adapter:
        rest_task = asyncio.create_task(
            adapter.capture_attempt(
                method="GET",
                market=Market.SPOT,
                url=SPOT_TIME_URL,
                request_role="clock_sample",
                correlation_id="interleave-close-1",
                attempt=1,
            )
        )
        await asyncio.wait_for(stream.close_started.wait(), timeout=1)
        await websocket.consume(_one_frame("{}"))
        stream.release_close.set()
        rest_envelope = await asyncio.wait_for(rest_task, timeout=1)
    await pipeline.stop()

    assert [record.ingest_seq for record in writer.records] == [1, 2]
    websocket_envelope, persisted_rest = writer.records
    assert isinstance(websocket_envelope, CaptureEnvelopeV1)
    assert isinstance(persisted_rest, RestEnvelopeV2)
    assert persisted_rest == rest_envelope
    assert (
        websocket_envelope.received_monotonic_ns
        < persisted_rest.response_completed_monotonic_ns
    )
    assert clock.calls == 4


@pytest.mark.asyncio
async def test_first_byte_and_completion_monotonic_order_is_preserved_and_enforced() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{}")

    pipeline, _writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method="GET",
            market=Market.SPOT,
            url=SPOT_TIME_URL,
            request_role="clock_sample",
            correlation_id="ordering-1",
            attempt=1,
        )
    await pipeline.stop()

    assert (
        envelope.response_first_byte_monotonic_ns is not None
    )
    assert (
        envelope.request_started_monotonic_ns
        < envelope.response_first_byte_monotonic_ns
        < envelope.response_completed_monotonic_ns
    )
    with pytest.raises(ValueError, match="completion time precedes first byte"):
        replace(envelope, response_completed_monotonic_ns=10_000)
    with pytest.raises(ValueError, match="timestamps must be paired"):
        replace(envelope, response_first_byte_at_ms=None)


@pytest.mark.asyncio
async def test_body_cap_preserves_bounded_prefix_and_marks_attempt_incomplete() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=httpx.ByteStream(b"abcdef"))

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        maximum_body_bytes=4,
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method="GET",
            market=Market.SPOT,
            url=SPOT_TIME_URL,
            request_role="clock_sample",
            correlation_id="large-body-1",
            attempt=1,
        )
    await pipeline.stop()

    assert envelope.raw_payload == "abcd"
    assert envelope.payload_complete is False
    assert envelope.error_category is RestErrorCategory.BODY_LIMIT
    assert envelope.error_detail == "response body exceeded the configured byte cap"
    assert writer.records == [envelope]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "market", "url", "headers", "query", "expected"),
    [
        pytest.param(
            "POST",
            Market.SPOT,
            SPOT_TIME_URL,
            None,
            (),
            "GET only",
            id="method",
        ),
        pytest.param(
            "GET",
            Market.SPOT,
            "https://api.binance.com/api/v3/time",
            None,
            (),
            "exact allowlisted",
            id="host",
        ),
        pytest.param(
            "GET",
            Market.SPOT,
            "https://data-api.binance.vision/api/v3/order",
            None,
            (),
            "public market-data allowlist",
            id="path",
        ),
        pytest.param(
            "GET",
            Market.SPOT,
            SPOT_TIME_URL,
            {"X-MBX-APIKEY": "must-not-leave-process"},
            (),
            "non-allowlisted header",
            id="api-key-header",
        ),
        pytest.param(
            "GET",
            Market.SPOT,
            SPOT_TIME_URL,
            None,
            (("signature", "must-not-be-stored"),),
            "credential parameter",
            id="signature-query",
        ),
    ],
)
async def test_forbidden_requests_fail_before_transport_or_capture(
    method: str,
    market: Market,
    url: str,
    headers: dict[str, str] | None,
    query: tuple[tuple[str, str], ...],
    expected: str,
) -> None:
    transport_calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal transport_calls
        transport_calls += 1
        return httpx.Response(200, content=b"{}")

    pipeline, writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        with pytest.raises(ValueError, match=expected):
            await adapter.capture_attempt(
                method=method,
                market=market,
                url=url,
                request_role="forbidden",
                correlation_id="forbidden-1",
                attempt=1,
                query=query,
                request_headers=headers,
            )
    await pipeline.stop()

    assert transport_calls == 0
    assert writer.records == []


@pytest.mark.asyncio
async def test_redirect_is_recorded_once_and_never_followed() -> None:
    transport_calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal transport_calls
        transport_calls += 1
        return httpx.Response(302, headers={"Location": "https://example.invalid"})

    pipeline, _writer = _pipeline()
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        transport=httpx.MockTransport(handler),
    ) as adapter:
        envelope = await adapter.capture_attempt(
            method="GET",
            market=Market.SPOT,
            url=SPOT_TIME_URL,
            request_role="clock_sample",
            correlation_id="redirect-1",
            attempt=1,
        )
    await pipeline.stop()

    assert transport_calls == 1
    assert envelope.response_status == 302
    assert envelope.error_category is RestErrorCategory.HTTP_STATUS
    assert envelope.response_headers == ()


@pytest.mark.asyncio
async def test_shared_sequencer_is_contiguous_across_websocket_and_rest() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{}")

    sequencer = IngestSequencer()
    clock = SequenceClock(
        ReceiptTimestamp(3_000, 30_000),
        ReceiptTimestamp(3_001, 30_001),
        ReceiptTimestamp(3_002, 30_002),
        ReceiptTimestamp(3_003, 30_003),
    )
    pipeline, writer = _pipeline()
    websocket = PublicWebSocketCaptureAdapter(
        WebSocketPlan(
            name="spot-one",
            market=Market.SPOT,
            route="spot",
            streams=("btcusdt@aggTrade",),
            url=f"{SPOT_WS_MARKET_DATA_ONLY}btcusdt@aggTrade",
        ),
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        connection_id="connection-1",
        pipeline=pipeline,
        clock=clock,
        sequencer=sequencer,
    )
    await websocket.consume(_one_frame("{}"))
    async with PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=clock,
        sequencer=sequencer,
        transport=httpx.MockTransport(handler),
    ) as rest:
        envelope = await rest.capture_attempt(
            method="GET",
            market=Market.SPOT,
            url=SPOT_TIME_URL,
            request_role="clock_sample",
            correlation_id="shared-sequence-1",
            attempt=1,
        )
    await pipeline.stop()

    assert envelope.ingest_seq == 2
    assert [record.ingest_seq for record in writer.records] == [1, 2]
    assert isinstance(writer.records[0], CaptureEnvelopeV1)
    assert isinstance(writer.records[1], RestEnvelopeV2)


def test_rest_v2_round_trips_storage_metadata(tmp_path: Path) -> None:
    envelope = RestEnvelopeV2(
        request_started_at_ms=4_000,
        request_started_monotonic_ns=40_000,
        response_first_byte_at_ms=4_001,
        response_first_byte_monotonic_ns=40_001,
        response_completed_at_ms=4_002,
        response_completed_monotonic_ns=40_002,
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        request_role="clock_sample",
        correlation_id="storage-1",
        attempt=1,
        ingest_seq=1,
        market=Market.SPOT,
        endpoint_path="/api/v3/time",
        canonical_query=(),
        response_status=200,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        raw_payload="{}",
    )
    writer = SegmentedCaptureWriter(
        tmp_path,
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        maximum_total_bytes=4 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    writer.append(envelope, record_to_json_line(envelope))
    writer.close()

    [manifest] = verify_capture_segments(tmp_path)
    assert manifest.first_received_at_ms == envelope.response_completed_at_ms
    assert manifest.last_received_at_ms == envelope.response_completed_at_ms
    assert manifest.record_count == 1


@pytest.mark.asyncio
async def test_maximum_body_cap_cannot_exceed_16_mib() -> None:
    pipeline, _writer = _pipeline()
    with pytest.raises(ValueError, match="16 MiB"):
        PublicRestCaptureAdapter(
            plan_sha256=PLAN_SHA256,
            process_boot_id="boot-1",
            pipeline=pipeline,
            clock=_clock_for_success(),
            sequencer=IngestSequencer(),
            maximum_body_bytes=16 * 1024 * 1024 + 1,
            transport=httpx.MockTransport(lambda _request: httpx.Response(200)),
        )
    await pipeline.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param({"timeout_seconds": 0}, "timeout_seconds", id="zero-timeout"),
        pytest.param({"timeout_seconds": True}, "timeout_seconds", id="bool-timeout"),
        pytest.param(
            {"timeout_seconds": 60.1},
            "timeout_seconds",
            id="excessive-timeout",
        ),
        pytest.param(
            {"maximum_connections": 0},
            "maximum_connections",
            id="zero-connections",
        ),
        pytest.param(
            {"maximum_connections": 5},
            "maximum_connections",
            id="excessive-connections",
        ),
    ],
)
async def test_rest_timeout_and_connection_pool_are_strictly_bounded(
    overrides: dict[str, object],
    expected: str,
) -> None:
    pipeline, _writer = _pipeline()
    arguments: dict[str, object] = {
        "plan_sha256": PLAN_SHA256,
        "process_boot_id": "boot-1",
        "pipeline": pipeline,
        "clock": _clock_for_success(),
        "sequencer": IngestSequencer(),
        "transport": httpx.MockTransport(lambda _request: httpx.Response(200)),
    }
    arguments.update(overrides)
    with pytest.raises(ValueError, match=expected):
        PublicRestCaptureAdapter(**arguments)  # pyright: ignore[reportArgumentType]
    await pipeline.stop()


@pytest.mark.asyncio
async def test_rest_adapter_owns_the_frozen_timeout_and_pool_bounds() -> None:
    pipeline, _writer = _pipeline()
    adapter = PublicRestCaptureAdapter(
        plan_sha256=PLAN_SHA256,
        process_boot_id="boot-1",
        pipeline=pipeline,
        clock=_clock_for_success(),
        sequencer=IngestSequencer(),
        timeout_seconds=15,
        maximum_connections=4,
        transport=httpx.MockTransport(lambda _request: httpx.Response(200)),
    )

    assert adapter.timeout_seconds == 15.0
    assert adapter.maximum_connections == 4
    await adapter.aclose()
    await pipeline.stop()
