"""Binance WebSocket transport preflight (Phase-K, charter s7/s17).

Generates the ACTUAL runtime endpoints from
signalbot.exchange.binance.endpoints.build_websocket_plans() and probes every
unique (scheme, host, port, route-kind) with bounded timeouts:

    DNS -> TCP -> TLS -> WebSocket handshake -> first frame -> clean close

Usage:
    uv run python tools/binance_ws_transport_preflight.py [--symbols BTCUSDT]
        [--first-frame-timeout 15] [--output results.json]

Exit codes: 0 = all routes healthy; 1 = at least one route failed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import ssl
import sys
import time
from dataclasses import asdict, dataclass, field
from urllib.parse import urlsplit

from websockets.asyncio.client import connect

from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import build_websocket_plans


@dataclass
class StageResult:
    ok: bool
    detail: str = ""
    elapsed_ms: float | None = None


@dataclass
class EndpointReport:
    plan_name: str
    market: str
    route: str
    scheme: str
    hostname: str
    port: int
    dns: StageResult | None = None
    tcp: StageResult | None = None
    tls: StageResult | None = None
    ws_handshake: StageResult | None = None
    first_frame: StageResult | None = None
    clean_close: StageResult | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return all(
            getattr(self, stage) is not None and getattr(self, stage).ok
            for stage in ("dns", "tcp", "tls", "ws_handshake", "first_frame")
        )


def _unique_endpoints(symbols: list[str]) -> list[dict]:
    seen: dict[tuple, dict] = {}
    for market in Market:
        plans = build_websocket_plans(
            market,
            symbols,
            ["1m", "5m"],
            batch_size=180,
        )
        for plan in plans:
            parts = urlsplit(plan.url)
            key = (parts.scheme, parts.hostname, parts.port or 443, plan.route)
            if key not in seen:
                seen[key] = {
                    "plan_name": plan.name,
                    "market": market.value,
                    "route": plan.route,
                    "url": plan.url,
                    "scheme": parts.scheme,
                    "hostname": parts.hostname or "",
                    "port": parts.port or 443,
                }
    return list(seen.values())


def _timed(fn, *args):
    start = time.monotonic()
    result = fn(*args)
    return result, (time.monotonic() - start) * 1000.0


async def probe_endpoint(endpoint: dict, first_frame_timeout: float) -> EndpointReport:
    report = EndpointReport(
        plan_name=endpoint["plan_name"],
        market=endpoint["market"],
        route=endpoint["route"],
        scheme=endpoint["scheme"],
        hostname=endpoint["hostname"],
        port=endpoint["port"],
    )

    # DNS
    try:
        start = time.monotonic()
        infos = await asyncio.get_running_loop().getaddrinfo(
            endpoint["hostname"], endpoint["port"], type=socket.SOCK_STREAM
        )
        addresses = sorted({info[4][0] for info in infos})
        report.dns = StageResult(
            ok=bool(addresses),
            detail=",".join(addresses[:4]),
            elapsed_ms=(time.monotonic() - start) * 1000.0,
        )
    except OSError as exc:
        report.dns = StageResult(ok=False, detail=str(exc))
        report.errors.append(f"dns: {exc}")
        return report

    # TCP (raw socket so we can layer TLS manually)
    try:
        start = time.monotonic()
        raw_tcp = socket.create_connection(
            (endpoint["hostname"], endpoint["port"]), timeout=10
        )
        report.tcp = StageResult(ok=True, elapsed_ms=(time.monotonic() - start) * 1000.0)
    except (OSError, TimeoutError) as exc:
        report.tcp = StageResult(ok=False, detail=str(exc))
        report.errors.append(f"tcp: {exc}")
        return report

    # TLS handshake over the real socket
    try:
        start = time.monotonic()
        tls_context = ssl.create_default_context()
        tls_sock = tls_context.wrap_socket(
            raw_tcp, server_hostname=endpoint["hostname"], do_handshake_on_connect=False
        )
        loop = asyncio.get_running_loop()
        await asyncio.wait_for(loop.run_in_executor(None, tls_sock.do_handshake), timeout=10)
        report.tls = StageResult(
            ok=True, elapsed_ms=(time.monotonic() - start) * 1000.0
        )
        tls_sock.close()
    except (OSError, TimeoutError, ssl.SSLError) as exc:
        report.tls = StageResult(ok=False, detail=str(exc))
        report.errors.append(f"tls: {exc}")
        try:
            raw_tcp.close()
        except OSError:
            pass
        return report

    # WebSocket handshake + first frame + clean close via websockets library
    ws_connection = None
    try:
        start = time.monotonic()
        ws_connection = await asyncio.wait_for(
            connect(endpoint["url"], open_timeout=15, close_timeout=5),
            timeout=20,
        )
        report.ws_handshake = StageResult(
            ok=True, elapsed_ms=(time.monotonic() - start) * 1000.0
        )
        start = time.monotonic()
        frame = await asyncio.wait_for(ws_connection.recv(), timeout=first_frame_timeout)
        report.first_frame = StageResult(
            ok=len(frame) > 0,
            detail=f"{len(frame)} bytes",
            elapsed_ms=(time.monotonic() - start) * 1000.0,
        )
        await ws_connection.close()
        report.clean_close = StageResult(ok=True)
    except Exception as exc:
        stage = (
            "ws_handshake"
            if report.ws_handshake is None
            else "first_frame"
        )
        setattr(report, stage, StageResult(ok=False, detail=str(exc)))
        report.errors.append(f"{stage}: {exc}")
        if ws_connection is not None:
            try:
                await ws_connection.close()
                report.clean_close = StageResult(ok=True)
            except Exception:
                report.clean_close = StageResult(ok=False, detail="close failed")
    return report


async def main_async(args: argparse.Namespace) -> int:
    symbols = args.symbols.split(",")
    endpoints = _unique_endpoints(symbols)
    reports: list[EndpointReport] = []
    for endpoint in endpoints:
        print(f"probing {endpoint['market']}/{endpoint['route']} "
              f"{endpoint['hostname']}:{endpoint['port']} ...", flush=True)
        report = await probe_endpoint(endpoint, args.first_frame_timeout)
        reports.append(report)
        print(json.dumps(asdict(report), default=str)[:400], flush=True)

    payload = {
        "schema_version": "binance_ws_transport_preflight_v1",
        "generated_from": "build_websocket_plans()",
        "symbols": symbols,
        "reports": [asdict(r) for r in reports],
        "all_healthy": all(r.healthy for r in reports),
    }
    text = json.dumps(payload, indent=2, default=str)
    print(text)
    if args.output:
        # Blocking write is fine for a CLI preflight tool at exit.
        with open(args.output, "w", encoding="utf-8") as handle:  # noqa: ASYNC230
            handle.write(text)
    return 0 if payload["all_healthy"] else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default="BTCUSDT")
    parser.add_argument("--first-frame-timeout", type=float, default=15.0)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
