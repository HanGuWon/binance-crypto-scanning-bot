from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from collections import Counter, defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any

from signalbot.capture.errors import CaptureQueueOverflow
from signalbot.capture.handoff import BoundedCaptureHandoff, CaptureFatalState
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    ConnectionState,
    ConnectionTransitionV1,
    CoverageReason,
    CoverageState,
    CoverageTransitionV1,
    RawPayloadEncoding,
    RestEnvelopeV1,
    RestEnvelopeV2,
    RestErrorCategory,
    record_to_json_line,
)
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.storage import (
    SegmentedCaptureWriter,
    consume_segment_lines,
    verify_capture_segments,
)
from signalbot.domain.enums import Market
from signalbot.pump_fade_v3.disk_budget import DiskBudget

LOGGER = logging.getLogger("pump_fade_v3.replay")
MAX_EVENTS = 2048
MAX_BYTES = 32 * 1024 * 1024
MAX_TOTAL_BYTES = 16 * 1024**3
MIN_FREE_BYTES = 12 * 1024**3
EMERGENCY_RESERVE_BYTES = 512 * 1024**2
_STREAM_FIELDS = {"subscription_streams", "streams"}
_TUPLE_PAIR_FIELDS = {"canonical_query", "response_headers"}


@dataclass(frozen=True, slots=True)
class ReplayRecord:
    record: CaptureRecord
    encoded_line: bytes
    stream: str
    timestamp_ms: int


@dataclass(slots=True)
class ReplayMetrics:
    offered_events_high_water: int = 0
    offered_bytes_high_water: int = 0
    offer_latency_ms: list[float] | None = None
    consumer_latency_ms: list[float] | None = None
    loop_lag_ms: list[float] | None = None
    long_calls: list[dict[str, Any]] | None = None
    overflow: dict[str, Any] | None = None
    compressed_bytes_by_stream: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.offer_latency_ms = []
        self.consumer_latency_ms = []
        self.loop_lag_ms = []
        self.long_calls = []


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * p + 0.999999)))
    return ordered[index]


def _record_from_line(line: bytes) -> CaptureRecord:
    value = json.loads(line)
    schema = value.pop("schema_version")
    value.pop("source", None)
    value.pop("transport", None)
    value["market"] = Market(value["market"])
    record_type: type[Any]
    enum_fields: dict[str, type[Any]] = {}
    if schema == "capture_envelope_v1":
        record_type = CaptureEnvelopeV1
        enum_fields = {"raw_payload_encoding": RawPayloadEncoding}
    elif schema == "rest_envelope_v1":
        record_type = RestEnvelopeV1
        enum_fields = {"raw_payload_encoding": RawPayloadEncoding}
    elif schema == "rest_envelope_v2":
        record_type = RestEnvelopeV2
        enum_fields = {
            "raw_payload_encoding": RawPayloadEncoding,
            "error_category": RestErrorCategory,
        }
    elif schema == "connection_transition_v1":
        record_type = ConnectionTransitionV1
        enum_fields = {"state": ConnectionState}
    elif schema == "coverage_transition_v1":
        record_type = CoverageTransitionV1
        enum_fields = {"state": CoverageState, "reason": CoverageReason}
    else:
        raise ValueError(f"unsupported capture schema: {schema}")
    for name in _STREAM_FIELDS & value.keys():
        value[name] = tuple(value[name])
    for name in _TUPLE_PAIR_FIELDS & value.keys():
        value[name] = tuple(tuple(pair) for pair in value[name])
    for name, enum_type in enum_fields.items():
        if value.get(name) is not None:
            value[name] = enum_type(value[name])
    init_fields = {field.name for field in fields(record_type) if field.init}
    record = record_type(**{name: item for name, item in value.items() if name in init_fields})
    if record_to_json_line(record) != line:
        raise ValueError("decoded record does not round-trip to its source JSONL")
    return record


def _timestamp_ms(record: CaptureRecord) -> int:
    if isinstance(record, RestEnvelopeV1):
        return record.response_received_at_ms
    if isinstance(record, RestEnvelopeV2):
        return record.response_completed_at_ms
    return record.received_at_ms


def _timestamp_monotonic_ns(record: CaptureRecord) -> int:
    if isinstance(record, RestEnvelopeV1):
        return record.response_received_monotonic_ns
    if isinstance(record, RestEnvelopeV2):
        return record.response_completed_monotonic_ns
    return record.received_monotonic_ns


def _stream_name(record: CaptureRecord) -> str:
    if isinstance(record, CaptureEnvelopeV1):
        try:
            wrapper = json.loads(record.raw_payload)
        except (ValueError, TypeError):
            return record.stream
        stream = wrapper.get("stream") if isinstance(wrapper, dict) else None
        return stream if isinstance(stream, str) else record.stream
    if isinstance(record, (RestEnvelopeV1, RestEnvelopeV2)):
        role = record.request_role if isinstance(record, RestEnvelopeV2) else record.endpoint_path
        return f"REST:{role}"
    return f"META:{record.schema_version}"


def read_generations(input_root: Path) -> list[tuple[dict[str, Any], list[ReplayRecord]]]:
    generations: list[tuple[dict[str, Any], list[ReplayRecord]]] = []
    directories = sorted(input_root.glob("capture-*-g009[6-9]"))
    if not directories:
        raise ValueError(f"no g0096-g0099 generation directories under {input_root}")
    for directory in directories:
        receipt = json.loads((directory / "generation-receipt.json").read_text(encoding="utf-8"))
        if receipt.get("generation_directory", directory.name) != directory.name:
            raise ValueError(f"generation receipt name mismatch: {directory}")
        manifests = verify_capture_segments(
            directory,
            expected_plan_sha256=receipt["plan_sha256"],
            expected_process_boot_id=receipt["process_boot_id"],
        )
        records: list[ReplayRecord] = []
        for manifest in manifests:
            segment = directory / manifest.data_file

            def consume(
                line: bytes,
                *,
                expected_boot_id: str = receipt["process_boot_id"],
                source_segment: Path = segment,
                record_list: list[ReplayRecord] = records,
            ) -> None:
                record = _record_from_line(line)
                if record.process_boot_id != expected_boot_id:
                    raise ValueError(
                        f"record boot identity mismatch: {source_segment.name}"
                    )
                record_list.append(
                    ReplayRecord(
                        record=record,
                        encoded_line=line,
                        stream=_stream_name(record),
                        timestamp_ms=_timestamp_ms(record),
                    )
                )

            consume_segment_lines(segment, consume)
        records.sort(key=lambda item: item.record.ingest_seq)
        generations.append((receipt, records))
    return generations


def profile_streams(generations: list[tuple[dict[str, Any], list[ReplayRecord]]]) -> dict[str, Any]:
    grouped: dict[str, list[tuple[int, int]]] = defaultdict(list)
    minute_counts: Counter[int] = Counter()
    minute_bytes: Counter[int] = Counter()
    total_records = 0
    all_timestamps: list[int] = []
    for _, records in generations:
        for item in records:
            encoded_size = len(record_to_json_line(item.record))
            grouped[item.stream].append((item.timestamp_ms, encoded_size))
            minute = item.timestamp_ms // 60_000
            minute_counts[minute] += 1
            minute_bytes[minute] += encoded_size
            all_timestamps.append(item.timestamp_ms)
            total_records += 1
    streams: dict[str, Any] = {}
    for name, values in grouped.items():
        values.sort()
        counts_by_second: Counter[int] = Counter()
        bytes_by_second: Counter[int] = Counter()
        for timestamp, size in values:
            counts_by_second[timestamp // 1000] += 1
            bytes_by_second[timestamp // 1000] += size
        queue: deque[tuple[int, int]] = deque()
        rolling_bytes = peak_ten_count = peak_ten_bytes = 0
        for timestamp, size in values:
            queue.append((timestamp, size))
            rolling_bytes += size
            while queue and queue[0][0] < timestamp - 9_999:
                _, removed = queue.popleft()
                rolling_bytes -= removed
            peak_ten_count = max(peak_ten_count, len(queue))
            peak_ten_bytes = max(peak_ten_bytes, rolling_bytes)
        duration_seconds = max(1.0, (values[-1][0] - values[0][0]) / 1000)
        total_bytes = sum(size for _, size in values)
        streams[name] = {
            "messages": len(values),
            "average_messages_per_second": len(values) / duration_seconds,
            "average_encoded_bytes_per_second": total_bytes / duration_seconds,
            "peak_1s_messages": max(counts_by_second.values()),
            "peak_1s_encoded_bytes": max(bytes_by_second.values()),
            "peak_10s_messages": peak_ten_count,
            "peak_10s_encoded_bytes": peak_ten_bytes,
            "encoded_bytes": total_bytes,
        }
    return {
        "record_count": total_records,
        "first_received_at_ms": min(all_timestamps),
        "last_received_at_ms": max(all_timestamps),
        "streams": streams,
        "minute_buckets": [
            {"minute_start_ms": minute * 60_000, "messages": minute_counts[minute],
             "encoded_bytes": minute_bytes[minute]}
            for minute in sorted(minute_counts)
        ],
    }


class _TimedWriter:
    def __init__(self, writer: SegmentedCaptureWriter, metrics: ReplayMetrics,
                 offer_times: dict[int, int]) -> None:
        self._writer = writer
        self._metrics = metrics
        self._offer_times = offer_times

    def append(self, record: CaptureRecord, encoded_line: bytes) -> None:
        try:
            self._writer.append(record, encoded_line)
        finally:
            offered = self._offer_times.pop(record.ingest_seq, time.perf_counter_ns())
            assert self._metrics.consumer_latency_ms is not None
            self._metrics.consumer_latency_ms.append((time.perf_counter_ns() - offered) / 1e6)

    def close(self) -> None:
        self._writer.close()

    def abort(self) -> None:
        self._writer.abort()

    def write_emergency_transition(self, transition: CoverageTransitionV1) -> None:
        self._writer.write_emergency_transition(transition)


def _install_timers(metrics: ReplayMetrics) -> list[tuple[Any, str, Any]]:
    import signalbot.capture.storage as storage

    targets: list[tuple[Any, str]] = [
        (SegmentedCaptureWriter, "append"),
        (SegmentedCaptureWriter, "close"),
        (SegmentedCaptureWriter, "_open_segment"),
        (SegmentedCaptureWriter, "_finalize_current"),
        (SegmentedCaptureWriter, "_ensure_data_capacity"),
        (DiskBudget, "_safe_tree_bytes"),
        (DiskBudget, "ensure_capacity"),
        (DiskBudget, "reconcile"),
        (storage, "_encode_outer_frame"),
        (storage, "_write_all"),
        (storage, "_sha256_file"),
        (storage, "_fsync_path"),
        (storage, "_fsync_parent"),
    ]
    restored: list[tuple[Any, str, Any]] = []
    for owner, name in targets:
        original = getattr(owner, name)

        def timed(*args: Any, _original: Callable[..., Any] = original,
                  _name: str = name, **kwargs: Any) -> Any:
            started = time.perf_counter_ns()
            try:
                return _original(*args, **kwargs)
            finally:
                duration_ms = (time.perf_counter_ns() - started) / 1e6
                if duration_ms > 100:
                    assert metrics.long_calls is not None
                    metrics.long_calls.append({"call": _name, "duration_ms": duration_ms})

        setattr(owner, name, timed)
        restored.append((owner, name, original))
    return restored


def _restore_timers(restored: list[tuple[Any, str, Any]]) -> None:
    for owner, name, original in reversed(restored):
        setattr(owner, name, original)


async def _monitor_event_loop(metrics: ReplayMetrics, stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    interval = 0.01
    target = loop.time() + interval
    while not stop.is_set():
        await asyncio.sleep(max(0, target - loop.time()))
        now = loop.time()
        assert metrics.loop_lag_ms is not None
        metrics.loop_lag_ms.append(max(0.0, (now - target) * 1000))
        target += interval


async def replay_generation(
    receipt: dict[str, Any], records: list[ReplayRecord], *, speed: float,
    output_root: Path, metrics: ReplayMetrics,
) -> None:
    if not records:
        return
    generation_dir = output_root / f"g{int(receipt['generation']):04d}"
    generation_dir.mkdir(parents=True, exist_ok=False)
    budget = DiskBudget(
        output_root,
        maximum_total_bytes=MAX_TOTAL_BYTES,
        minimum_free_bytes=MIN_FREE_BYTES,
        emergency_reserve_bytes=EMERGENCY_RESERVE_BYTES,
        expected_mount=Path(output_root.anchor),
        event_depth_shed=lambda: None,
    )
    fatal = CaptureFatalState()
    handoff = BoundedCaptureHandoff(
        max_events=MAX_EVENTS, max_bytes=MAX_BYTES, fatal_state=fatal
    )
    writer = SegmentedCaptureWriter(
        generation_dir,
        plan_sha256=receipt["plan_sha256"],
        process_boot_id=receipt["process_boot_id"],
        maximum_total_bytes=MAX_TOTAL_BYTES,
        emergency_reserve_bytes=EMERGENCY_RESERVE_BYTES,
        capacity_guard=budget.ensure_capacity,
    )
    offer_times: dict[int, int] = {}
    pipeline = CapturePipeline(handoff, _TimedWriter(writer, metrics, offer_times))
    pipeline.start()
    loop = asyncio.get_running_loop()
    playback_start = loop.time()
    recorded_start = _timestamp_monotonic_ns(records[0].record)
    try:
        for item in records:
            relative_seconds = max(
                0.0,
                (_timestamp_monotonic_ns(item.record) - recorded_start) / 1e9 / speed,
            )
            await asyncio.sleep(max(0.0, playback_start + relative_seconds - loop.time()))
            offered_at = time.perf_counter_ns()
            assert metrics.offer_latency_ms is not None
            try:
                handoff.offer(item.record)
            except CaptureQueueOverflow as exc:
                metrics.overflow = {
                    "generation": receipt["generation"],
                    "ingest_seq": item.record.ingest_seq,
                    "stream": item.stream,
                    "received_at_ms": item.timestamp_ms,
                    "message": str(exc),
                }
                break
            offer_times[item.record.ingest_seq] = offered_at
            metrics.offer_latency_ms.append((time.perf_counter_ns() - offered_at) / 1e6)
            metrics.offered_events_high_water = max(
                metrics.offered_events_high_water, handoff.queued_events
            )
            metrics.offered_bytes_high_water = max(
                metrics.offered_bytes_high_water, handoff.queued_bytes
            )
    finally:
        try:
            await pipeline.stop()
        except Exception as exc:
            if metrics.overflow is None:
                metrics.overflow = {
                    "generation": receipt["generation"],
                    "consumer_error": repr(exc),
                }
        for stream, byte_count in writer.compressed_bytes_by_stream.items():
            metrics.compressed_bytes_by_stream[stream] = (
                metrics.compressed_bytes_by_stream.get(stream, 0) + byte_count
            )


def _summarize_metrics(metrics: ReplayMetrics) -> dict[str, Any]:
    assert metrics.offer_latency_ms is not None
    assert metrics.consumer_latency_ms is not None
    assert metrics.loop_lag_ms is not None
    return {
        "queue_bounds": {"events": MAX_EVENTS, "bytes": MAX_BYTES},
        "queue_high_water": {
            "events": metrics.offered_events_high_water,
            "bytes": metrics.offered_bytes_high_water,
            "events_percent": metrics.offered_events_high_water / MAX_EVENTS * 100,
            "bytes_percent": metrics.offered_bytes_high_water / MAX_BYTES * 100,
        },
        "producer_offer_latency_ms": {
            "p50": percentile(metrics.offer_latency_ms, 0.50),
            "p99": percentile(metrics.offer_latency_ms, 0.99),
            "max": max(metrics.offer_latency_ms, default=0.0),
        },
        "consumer_record_latency_ms": {
            "p50": percentile(metrics.consumer_latency_ms, 0.50),
            "p99": percentile(metrics.consumer_latency_ms, 0.99),
            "max": max(metrics.consumer_latency_ms, default=0.0),
        },
        "event_loop_lag_ms": {
            "p50": percentile(metrics.loop_lag_ms, 0.50),
            "p99": percentile(metrics.loop_lag_ms, 0.99),
            "max": max(metrics.loop_lag_ms, default=0.0),
        },
        "blocking_calls_over_100ms": metrics.long_calls,
        "compressed_bytes_by_stream": metrics.compressed_bytes_by_stream,
        "overflow": metrics.overflow,
    }


async def run_replay(
    generations: list[tuple[dict[str, Any], list[ReplayRecord]]],
    *, speed: float, output_root: Path,
) -> dict[str, Any]:
    await asyncio.to_thread(output_root.mkdir, parents=True, exist_ok=False)
    metrics = ReplayMetrics()
    stop = asyncio.Event()
    monitor = asyncio.create_task(_monitor_event_loop(metrics, stop))
    restored = _install_timers(metrics)
    try:
        for receipt, records in generations:
            await replay_generation(receipt, records, speed=speed,
                                    output_root=output_root, metrics=metrics)
            if metrics.overflow is not None:
                break
    finally:
        stop.set()
        await monitor
        _restore_timers(restored)
    return {"speed": speed, **_summarize_metrics(metrics)}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline capture queue replay")
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--speed", type=float, action="append", default=[])
    parser.add_argument(
        "--exclude-stream-suffix", action="append", default=[],
        help="omit matching stream records from replay (for offline counterfactuals)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    speeds = args.speed or [1.0, 3.0]
    if any(speed <= 0 for speed in speeds):
        raise SystemExit("--speed values must be positive")
    generations = read_generations(args.input_root)
    profile = profile_streams(generations)
    if args.exclude_stream_suffix:
        filtered: list[tuple[dict[str, Any], list[ReplayRecord]]] = []
        for receipt, records in generations:
            retained = [
                item for item in records
                if not any(
                    item.stream.endswith(suffix)
                    for suffix in args.exclude_stream_suffix
                )
            ]
            replay_records = [
                replace(
                    item,
                    record=replace(item.record, ingest_seq=index),
                )
                for index, item in enumerate(retained, start=1)
            ]
            filtered.append((receipt, replay_records))
        generations = filtered
    results = []
    for speed in speeds:
        result = asyncio.run(
            run_replay(
                generations,
                speed=speed,
                output_root=args.output_root / f"speed-{speed:g}x",
            )
        )
        results.append(result)
        if result["overflow"] is not None:
            break
    report = {
        "stream_profile": profile,
        "excluded_stream_suffixes": args.exclude_stream_suffix,
        "replays": results,
    }
    args.output_root.mkdir(parents=True, exist_ok=True)
    report_path = args.output_root / "replay-report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 1 if any(result["overflow"] is not None for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
