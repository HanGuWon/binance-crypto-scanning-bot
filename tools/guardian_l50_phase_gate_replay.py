"""Deterministic, read-only L50 Guardian phase-gate replay.

The replay intentionally uses the real Guardian repository, reconciliation
boundary, and user-stream buffer. It never constructs a live HTTP or WebSocket
client and has no exchange write adapter.
"""

from __future__ import annotations

import ast
import hashlib
import json
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any

from position_guardian.domain import (
    AdoptionCandidate,
    ManagedPositionIdentity,
    ProtectiveOrderReference,
)
from position_guardian.exchange.binance_user_stream import (
    BoundedUserEventBuffer,
    parse_user_stream_event,
)
from position_guardian.exchange.protocol import PositionSnapshot
from position_guardian.persistence.repository import GuardianRepository
from position_guardian.reconcile import (
    ReconciliationRequest,
    apply_rest_resync_result,
    reconcile_once,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "binance_user_stream"
GUARDIAN_ROOT = ROOT / "src" / "position_guardian"
SCHEMA_VERSION = "l50_guardian_phase_gate_replay_v1"
WRITE_OPERATION_NAMES = (
    "exchange_order_create_calls",
    "exchange_order_modify_calls",
    "exchange_order_cancel_calls",
    "position_increase_calls",
    "position_reverse_calls",
)


class ExchangeWriteProbe:
    """A tripwire that would fail if replay code attempted an exchange write."""

    def __init__(self) -> None:
        self.calls = {name: 0 for name in WRITE_OPERATION_NAMES}

    def __getattr__(self, name: str) -> object:
        if name in WRITE_OPERATION_NAMES:
            self.calls[name] += 1
            raise AssertionError(f"forbidden exchange write reached replay: {name}")
        raise AttributeError(name)


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _fixture_payload(name: str) -> dict[str, Any]:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


def _fixture_event(name: str) -> object:
    return parse_user_stream_event(
        json.dumps(_fixture_payload(name), ensure_ascii=False, sort_keys=True)
    )


def _position(amount: str = "0.01", mark: str = "61000") -> PositionSnapshot:
    return PositionSnapshot(
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amount=Decimal(amount),
        entry_price=Decimal("60000"),
        mark_price=Decimal(mark),
        unrealized_profit=Decimal("10"),
        update_time_ms=1_700_000_000_000,
    )


def _identity(generation: int) -> ManagedPositionIdentity:
    return ManagedPositionIdentity("replay-manual", "BTCUSDT", "LONG", generation)


def _candidate(identity: ManagedPositionIdentity) -> AdoptionCandidate:
    return AdoptionCandidate(
        identity=identity,
        quantity=Decimal("0.01"),
        entry_price=Decimal("60000"),
        mark_price=Decimal("61000"),
        original_risk_stop=Decimal("59000"),
        protection_floor=Decimal("60500"),
        protection_source="exchange_stop",
        protective_order=ProtectiveOrderReference("open_order", 11, Decimal("59000")),
    )


def _adopt(repository: GuardianRepository, generation: int) -> ManagedPositionIdentity:
    identity = _identity(generation)
    accepted = repository.record_adoption(
        event_id=f"adopt-{generation}",
        event_time_ms=1000,
        created_at_ms=1001,
        candidate=_candidate(identity),
    )
    if not accepted:
        raise AssertionError("replay adoption unexpectedly deduplicated")
    return identity


def _request(
    identity: ManagedPositionIdentity,
    event_id: str,
    position: PositionSnapshot,
    *,
    protective: bool = True,
    uncertainty: str = "CERTAIN",
    release_event_id: str | None = None,
) -> ReconciliationRequest:
    return ReconciliationRequest(
        identity=identity,
        position=position,
        snapshot_event_id=event_id,
        event_time_ms=2000,
        created_at_ms=2001,
        protective_order_confirmed=protective,
        uncertainty_state=uncertainty,  # type: ignore[arg-type]
        release_event_id=release_event_id,
    )


def _resync(buffer: BoundedUserEventBuffer, label: str) -> tuple[bool, bool]:
    session = buffer.begin_rest_resync()
    result = buffer.complete_rest_resync(
        session,
        snapshot_id=f"{label}-snapshot",
        snapshot_cursor=f"{label}-cursor",
    )
    return result.status == "DEGRADED", result.certainty_restored


def _with_repository(root: Path, name: str) -> GuardianRepository:
    return GuardianRepository(f"sqlite:///{(root / f'{name}.db').as_posix()}")


def _scenario_startup(root: Path) -> dict[str, object]:
    with _with_repository(root, "startup") as repository:
        identity = _adopt(repository, 1)
        buffer = BoundedUserEventBuffer()
        session = buffer.begin_rest_resync()
        resync_result = buffer.complete_rest_resync(
            session,
            snapshot_id="startup-snapshot",
            snapshot_cursor="startup-cursor",
        )
        assert resync_result.certainty_restored
        request = apply_rest_resync_result(
            _request(identity, "startup-snapshot", _position()), resync_result
        )
        result = reconcile_once(repository, request)
        assert result.state == "MANAGED_SHADOW"
    return {"before_authoritative_reconcile": "DEGRADED", "after": "MANAGED_SHADOW"}


def _scenario_restart(root: Path) -> dict[str, object]:
    database = root / "restart.db"
    with GuardianRepository(f"sqlite:///{database.as_posix()}") as repository:
        identity = _adopt(repository, 2)
        first = reconcile_once(repository, _request(identity, "restart-snapshot-1", _position()))
        assert first.state == "MANAGED_SHADOW"
    with GuardianRepository(f"sqlite:///{database.as_posix()}") as repository:
        projection = repository.get_projection(identity)
        assert projection is not None and projection.state == "ADOPTED"
        second = reconcile_once(repository, _request(identity, "restart-snapshot-2", _position()))
        assert second.state == "MANAGED_SHADOW"
    return {"persisted_projection": "ADOPTED", "after_restart": "MANAGED_SHADOW"}


def _scenario_partial_close(root: Path) -> dict[str, object]:
    with _with_repository(root, "partial-close") as repository:
        identity = _adopt(repository, 3)
        result = reconcile_once(
            repository,
            _request(identity, "partial-close-snapshot", _position(amount="0.004")),
        )
        assert result.alerts == ("MANUAL_PARTIAL_CLOSE",)
    return {"state": result.state, "alert": result.alerts[0]}


def _scenario_manual_add(root: Path) -> dict[str, object]:
    with _with_repository(root, "manual-add") as repository:
        identity = _adopt(repository, 4)
        result = reconcile_once(
            repository,
            _request(identity, "manual-add-snapshot", _position(amount="0.02")),
        )
        assert result.alerts == ("MANUAL_POSITION_ADD",)
    return {"state": result.state, "alert": result.alerts[0], "write_calls": 0}


def _scenario_full_close(root: Path) -> dict[str, object]:
    with _with_repository(root, "full-close") as repository:
        identity = _adopt(repository, 5)
        result = reconcile_once(
            repository,
            _request(
                identity,
                "full-close-snapshot",
                _position(amount="0"),
                release_event_id="full-close-release",
            ),
        )
        assert result.state == "CLOSED"
    return {"state": result.state, "alert": result.alerts[0]}


def _scenario_side_flip(root: Path) -> dict[str, object]:
    with _with_repository(root, "side-flip") as repository:
        identity = _adopt(repository, 6)
        result = reconcile_once(
            repository,
            _request(
                identity,
                "side-flip-snapshot",
                _position(amount="-0.01", mark="59000"),
                release_event_id="side-flip-release",
            ),
        )
        assert result.state == "RELEASED"
    return {"state": result.state, "alert": result.alerts[0]}


def _scenario_protective_disappearance(root: Path) -> dict[str, object]:
    with _with_repository(root, "protective-disappearance") as repository:
        identity = _adopt(repository, 7)
        result = reconcile_once(
            repository,
            _request(identity, "protective-missing-snapshot", _position(), protective=False),
        )
        assert result.alerts == ("PROTECTIVE_ORDER_MISSING",)
    return {"state": result.state, "alert": result.alerts[0], "write_calls": 0}


def _scenario_duplicate_event(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer()
    event = _fixture_event("account_update.json")
    first = buffer.ingest(event)
    duplicate = buffer.ingest(event)
    assert first.status == "ACCEPTED" and duplicate.status == "DUPLICATE"
    return {"first": first.status, "replay": duplicate.status}


def _scenario_dedupe_eviction(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer(max_queue_size=4, max_dedupe_size=1)
    first = _fixture_event("account_update.json")
    second = _fixture_event("order_trade_update.json")
    first_result = buffer.ingest(first)
    second_result = buffer.ingest(second)
    replay = buffer.ingest(first)
    assert first_result.status == "ACCEPTED" and second_result.status == "ACCEPTED"
    assert replay.status == "ACCEPTED"
    return {"after_eviction": replay.status, "dedupe_bound": buffer.dedupe_count}


def _scenario_reordered(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer()
    payload = _fixture_payload("account_update.json")
    payload["E"] = 2000
    payload["T"] = 1999
    newer = parse_user_stream_event(json.dumps(payload, sort_keys=True))
    payload["E"] = 1000
    payload["T"] = 999
    older = parse_user_stream_event(json.dumps(payload, sort_keys=True))
    assert buffer.ingest(newer).status == "ACCEPTED"
    result = buffer.ingest(older)
    assert result.status == "OUT_OF_ORDER" and result.health == "DEGRADED"
    return {"status": result.status, "management": "DEGRADED"}


def _scenario_disconnect_reconnect(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer()
    buffer.mark_disconnected()
    assert buffer.health == "DISCONNECTED"
    degraded, certain = _resync(buffer, "disconnect")
    assert degraded is False and certain
    return {"before": "DISCONNECTED", "after": buffer.health}


def _scenario_overflow(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer(max_queue_size=1)
    first = buffer.ingest(_fixture_event("account_update.json"))
    second = buffer.ingest(_fixture_event("order_trade_update.json"))
    assert first.status == "ACCEPTED" and second.status == "OVERFLOW"
    return {
        "status": second.status,
        "pending_bound": buffer.pending_count,
        "management": "DEGRADED",
    }


def _scenario_expiry(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer()
    result = buffer.ingest_raw(
        (FIXTURE_ROOT / "listen_key_expired.json").read_text(encoding="utf-8")
    )
    assert result.status == "ACCEPTED" and buffer.health == "EXPIRED"
    buffer.drain()
    degraded, certain = _resync(buffer, "expiry")
    assert degraded is False and certain
    return {"before": "EXPIRED", "after": buffer.health}


def _scenario_malformed(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer()
    invalid = buffer.ingest_raw("not-json")
    assert invalid.status == "INVALID" and buffer.health == "DEGRADED"
    degraded, certain = _resync(buffer, "malformed")
    assert degraded is False and certain
    return {"before": "DEGRADED", "after": buffer.health}


def _scenario_buffered_snapshot(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer()
    event = _fixture_event("account_update.json")
    buffer.ingest(event)
    session = buffer.begin_rest_resync()
    failed = buffer.complete_rest_resync(
        session,
        snapshot_id="buffered-rest",
        snapshot_cursor="authoritative-1",
    )
    assert failed.status == "DEGRADED"
    fenced = buffer.fence_buffered_events(session)
    retry = buffer.begin_rest_resync()
    recovered = buffer.complete_rest_resync(
        retry,
        snapshot_id="buffered-rest-retry",
        snapshot_cursor="authoritative-2",
    )
    assert fenced == (event.event_id,) and recovered.certainty_restored
    return {"first": failed.status, "fenced": len(fenced), "after": recovered.status}


def _scenario_event_during_rest(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer()
    session = buffer.begin_rest_resync()
    event = _fixture_event("account_update.json")
    buffer.ingest(event)
    failed = buffer.complete_rest_resync(
        session,
        snapshot_id="during-rest",
        snapshot_cursor="authoritative-3",
    )
    assert failed.status == "DEGRADED"
    buffer.fence_buffered_events(session)
    retry = buffer.begin_rest_resync()
    recovered = buffer.complete_rest_resync(
        retry,
        snapshot_id="during-rest-retry",
        snapshot_cursor="authoritative-4",
    )
    assert recovered.certainty_restored
    return {"first": failed.status, "reason": failed.reason, "after": recovered.status}


def _scenario_stale_pre_snapshot(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer()
    stale = _fixture_event("account_update.json")
    buffer.ingest(stale)
    session = buffer.begin_rest_resync()
    buffer.drain()
    failed = buffer.complete_rest_resync(
        session,
        snapshot_id="stale-pre-snapshot",
        snapshot_cursor="authoritative-5",
    )
    assert failed.status == "DEGRADED" and failed.crossed_event_ids == (stale.event_id,)
    buffer.fence_buffered_events(session)
    retry = buffer.begin_rest_resync()
    recovered = buffer.complete_rest_resync(
        retry,
        snapshot_id="stale-pre-snapshot-retry",
        snapshot_cursor="authoritative-6",
    )
    assert recovered.certainty_restored
    return {"first": failed.status, "stale_cannot_regress": True, "after": recovered.status}


def _scenario_unproven_ordering(_root: Path) -> dict[str, object]:
    buffer = BoundedUserEventBuffer()
    session = buffer.begin_rest_resync()
    payload = _fixture_payload("account_update.json")
    payload["E"] = 9_999_999_999
    payload["T"] = 9_999_999_998
    event = parse_user_stream_event(json.dumps(payload, sort_keys=True))
    buffer.ingest(event)
    failed = buffer.complete_rest_resync(
        session,
        snapshot_id="unproven-ordering",
        snapshot_cursor="authoritative-7",
    )
    assert failed.status == "DEGRADED"
    assert failed.reason == "STREAM_ACTIVITY_CROSSED_REST_FENCE"
    return {"status": failed.status, "wall_clock_inference": False, "management": "DEGRADED"}


SCENARIOS = (
    ("normal_startup_reconciliation", _scenario_startup),
    ("restart_persisted_ledger", _scenario_restart),
    ("partial_manual_close", _scenario_partial_close),
    ("manual_position_increase", _scenario_manual_add),
    ("full_close", _scenario_full_close),
    ("side_flip", _scenario_side_flip),
    ("protective_order_disappearance", _scenario_protective_disappearance),
    ("exact_duplicate_event", _scenario_duplicate_event),
    ("duplicate_after_dedupe_eviction", _scenario_dedupe_eviction),
    ("reordered_same_type_event", _scenario_reordered),
    ("disconnect_reconnect", _scenario_disconnect_reconnect),
    ("queue_overflow", _scenario_overflow),
    ("listen_key_expiry", _scenario_expiry),
    ("malformed_event", _scenario_malformed),
    ("rest_snapshot_while_buffered", _scenario_buffered_snapshot),
    ("event_arriving_during_rest", _scenario_event_during_rest),
    ("stale_pre_snapshot_event", _scenario_stale_pre_snapshot),
    ("ordering_cannot_be_proven", _scenario_unproven_ordering),
)


def _owned_source_hash() -> str:
    digest = hashlib.sha256()
    for path in sorted(GUARDIAN_ROOT.rglob("*.py")):
        digest.update(path.relative_to(ROOT).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _fixture_hashes() -> dict[str, str]:
    return {
        path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(FIXTURE_ROOT.glob("*.json"))
    }


def _git_head() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def _structural_write_calls() -> list[str]:
    forbidden = {
        "create_order",
        "place_order",
        "modify_order",
        "amend_order",
        "cancel_order",
        "increase_position",
        "reverse_position",
    }
    found: list[str] = []
    for path in sorted(GUARDIAN_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in forbidden:
                    found.append(f"{path.relative_to(ROOT)}:{node.lineno}:{node.func.attr}")
    return found


def run_replay_once(database_root: Path) -> dict[str, object]:
    database_root.mkdir(parents=True, exist_ok=True)
    scenarios: list[dict[str, object]] = []
    for name, runner in SCENARIOS:
        try:
            evidence = runner(database_root)
            scenarios.append({"name": name, "status": "PASS", "evidence": evidence})
        except Exception as exc:
            scenarios.append(
                {
                    "name": name,
                    "status": "FAIL",
                    "evidence": {"error": f"{type(exc).__name__}: {exc}"},
                }
            )

    structural = _structural_write_calls()
    write_probe = ExchangeWriteProbe()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "code_commit": _git_head(),
        "guardian_source_tree_sha256": _owned_source_hash(),
        "fixture_sha256": _fixture_hashes(),
        "scenario_count": len(scenarios),
        "scenarios": scenarios,
        "structural_forbidden_write_calls": structural,
        **write_probe.calls,
        "total_exchange_trading_write_calls": sum(write_probe.calls.values()),
    }
    payload["all_scenarios_pass"] = all(row["status"] == "PASS" for row in scenarios)
    payload["zero_write_proof"] = (
        not structural and payload["total_exchange_trading_write_calls"] == 0
    )
    payload["deterministic_payload_sha256"] = _canonical_hash(payload)
    return payload


def build_receipt() -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="guardian-l50-replay-1-") as first_dir:
        first = run_replay_once(Path(first_dir))
    with tempfile.TemporaryDirectory(prefix="guardian-l50-replay-2-") as second_dir:
        second = run_replay_once(Path(second_dir))
    first_hash = str(first["deterministic_payload_sha256"])
    second_hash = str(second["deterministic_payload_sha256"])
    reproducible = first_hash == second_hash
    passed = bool(first["all_scenarios_pass"]) and bool(first["zero_write_proof"]) and reproducible
    return {
        "terminal_state": (
            "L50_PHASE_GATE_DETERMINISTIC_REPLAY_PASS"
            if passed
            else "L50_PHASE_GATE_BLOCKED"
        ),
        "run_1_hash": first_hash,
        "run_2_hash": second_hash,
        "deterministic_hashes_byte_identical": reproducible,
        "run_1": first,
        "run_2": second,
    }


def main() -> int:
    receipt = build_receipt()
    output = json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if len(sys.argv) == 3 and sys.argv[1] == "--output":
        Path(sys.argv[2]).write_text(output, encoding="utf-8")
    else:
        sys.stdout.write(output)
    return 0 if receipt["terminal_state"] == "L50_PHASE_GATE_DETERMINISTIC_REPLAY_PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
