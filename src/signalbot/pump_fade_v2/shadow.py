"""English preview payload and isolated, durable, never-sending shadow outbox."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from signalbot.pump_fade_v2.state import (
    Decision,
    Episode,
    Event,
    InputPanel,
    State,
    start_episode,
    step_episode,
)


def _times(ms: int) -> dict[str, str]:
    utc = datetime.fromtimestamp(ms / 1_000, tz=UTC)
    return {"utc": utc.isoformat(timespec="seconds"),
            "asia_seoul": utc.astimezone(ZoneInfo("Asia/Seoul")).isoformat(
                timespec="seconds")}


def preview_payload(decision: Decision, *, spread_bps: float | None = None,
                    cost_scenario_bps: float | None = None) -> dict[str, object]:
    """Deterministic research payload; contains no individually calibrated probability."""

    normalized = asdict(decision)
    identity = hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()
    metrics = dict(decision.metrics)
    freshness = {
        key: value for key, value in metrics.items()
        if key.endswith("_age_ms")
    }
    return {
        "schema_version": "pump_v2_shadow_preview_v1",
        "message_id": identity,
        "event_id": decision.event_id,
        "policy_version": decision.policy_version,
        "state": decision.state.value,
        "validation_tier": decision.validation_tier,
        "decision_time": _times(decision.decision_at_ms),
        "expires": _times(decision.expires_at_ms),
        "reasons": list(decision.reasons),
        "metrics": metrics,
        "freshness_ms": freshness,
        "release_candidates": list(decision.releases),
        "continuation_risk": decision.continuation_risk,
        "continuation_risk_evidence": "observed_positive_only_liquidation_sampling_censored",
        "invalidation_price": decision.invalidation_price,
        "spread_bps": spread_bps,
        "cost_scenario_bps": cost_scenario_bps,
        "historical_probability": "UNAVAILABLE_UNTIL_COHORT_N_AND_CONFIDENCE_INTERVAL",
        "delivery_enabled": False,
    }


class ShadowPreviewStore:
    """Separate local outbox, with no transport or send method by construction."""

    def __init__(self, path: str | Path, *, max_records: int = 50_000) -> None:
        if not 1 <= max_records <= 1_000_000:
            raise ValueError("shadow preview retention capacity out of bounds")
        self.max_records = max_records
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as connection:
            connection.execute("""CREATE TABLE IF NOT EXISTS pump_v2_preview (
                message_id TEXT PRIMARY KEY, payload_sha256 TEXT NOT NULL,
                payload_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'preview_only'
            )""")

    def enqueue(self, payload: dict[str, object]) -> bool:
        """Persist exactly once across restarts; never contact a webhook."""

        if payload.get("delivery_enabled") is not False:
            raise ValueError("this store accepts preview-only payloads")
        identity = str(payload["message_id"])
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        with sqlite3.connect(self.path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            previous = conn.execute("SELECT payload_sha256 FROM pump_v2_preview WHERE message_id=?",
                                    (identity,)).fetchone()
            if previous is not None:
                if previous[0] != digest:
                    raise ValueError("shadow payload identity conflict")
                return False
            if self._at_capacity(conn):
                raise OverflowError("preview store reached capacity; export and archive first")
            conn.execute("INSERT INTO pump_v2_preview(message_id,payload_sha256,payload_json) "
                         "VALUES (?,?,?)", (identity, digest, serialized))
            return True

    def _at_capacity(self, conn: sqlite3.Connection) -> bool:
        count = conn.execute("SELECT COUNT(*) FROM pump_v2_preview").fetchone()
        assert count is not None
        return int(count[0]) >= self.max_records


@dataclass(frozen=True, slots=True)
class AdvanceReceipt:
    """Purely local state transition; posted=False means exact replay duplicate."""

    posted: bool
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class EpisodeCheckpoint:
    state: State
    evaluated_at_ms: int


@dataclass(frozen=True, slots=True)
class ArchiveReceipt:
    path: str
    sha256: str
    event_count: int
    decision_count: int
    preview_count: int


class ShadowEpisodeEngine(ShadowPreviewStore):
    """Atomic durable episode/checkpoint plus never-delivered decision preview.

    Every new logical input clock has an append-only audit identity. Snapshot and
    preview commit together. Fail closed at bounded storage instead of deleting
    unarchived or uncertain records; no webhook or production code is imported.
    """

    def __init__(
        self, path: str | Path, *, max_episodes: int = 128,
        max_records: int = 50_000, maximum_archive_bytes: int = 8 * 1024**3,
    ) -> None:
        super().__init__(path, max_records=max_records)
        if not 1 <= max_episodes <= 1024:
            raise ValueError("episode capacity out of bounds")
        if not 1 <= maximum_archive_bytes <= 64 * 1024**3:
            raise ValueError("terminal archive capacity out of bounds")
        self.max_episodes = max_episodes
        self.maximum_archive_bytes = maximum_archive_bytes
        with sqlite3.connect(self.path) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS pump_v2_episode (
                event_id TEXT PRIMARY KEY,
                serialized TEXT NOT NULL,
                last_input_hash TEXT NOT NULL,
                last_payload_json TEXT NOT NULL
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS pump_v2_decision_receipt (
                event_id TEXT NOT NULL, decision_at_ms INTEGER NOT NULL,
                input_sha256 TEXT NOT NULL,
                message_id TEXT NOT NULL,
                PRIMARY KEY(event_id, decision_at_ms)
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS pump_v2_outcome_receipt (
                event_id TEXT NOT NULL, origin TEXT NOT NULL, origin_ms INTEGER NOT NULL,
                horizon_hours INTEGER NOT NULL, status TEXT NOT NULL,
                outcome_identity TEXT, payload_sha256 TEXT,
                PRIMARY KEY(event_id, origin, origin_ms, horizon_hours)
            )""")

    def register_outcome_requirement(
        self, event_id: str, *, origin: str, origin_ms: int, horizon_hours: int,
    ) -> None:
        """Durably require one event/alert horizon before terminal archival."""

        if origin not in ("event", "alert") or horizon_hours not in (4, 24):
            raise ValueError("unknown outcome origin or horizon")
        if not event_id or origin_ms < 0:
            raise ValueError("outcome requirement identity is invalid")
        with sqlite3.connect(self.path) as conn:
            conn.execute("INSERT OR IGNORE INTO pump_v2_outcome_receipt "
                         "(event_id,origin,origin_ms,horizon_hours,status) "
                         "VALUES (?,?,?,?, 'PENDING')",
                         (event_id, origin, origin_ms, horizon_hours))

    def record_outcome_receipt(
        self, event_id: str, *, origin: str, origin_ms: int, horizon_hours: int,
        outcome_identity: str, payload_sha256: str, status: str,
    ) -> None:
        """Acknowledge an immutable outcome snapshot; terminal status gates prune."""

        if status not in ("PENDING", "CENSORED", "COMPLETE"):
            raise ValueError("unknown durable outcome status")
        if len(payload_sha256) != 64 or not outcome_identity:
            raise ValueError("outcome receipt requires an identity and SHA-256")
        self.register_outcome_requirement(
            event_id, origin=origin, origin_ms=origin_ms, horizon_hours=horizon_hours,
        )
        with sqlite3.connect(self.path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            prior = conn.execute(
                "SELECT status,outcome_identity,payload_sha256 FROM pump_v2_outcome_receipt "
                "WHERE event_id=? AND origin=? AND origin_ms=? AND horizon_hours=?",
                (event_id, origin, origin_ms, horizon_hours),
            ).fetchone()
            if prior is None:
                raise ValueError("outcome requirement disappeared")
            if prior[0] in ("CENSORED", "COMPLETE"):
                if prior[1:] != (outcome_identity, payload_sha256) or prior[0] != status:
                    raise ValueError("terminal outcome receipt is immutable")
                return
            conn.execute(
                "UPDATE pump_v2_outcome_receipt SET status=?,outcome_identity=?,payload_sha256=? "
                "WHERE event_id=? AND origin=? AND origin_ms=? AND horizon_hours=?",
                (status, outcome_identity, payload_sha256, event_id, origin, origin_ms,
                 horizon_hours),
            )

    def advance(self, event: Event, panel: InputPanel, *, now_ms: int) -> AdvanceReceipt:
        """Advance one event at strictly monotonic receipt time, idempotently."""

        if len(panel.five_minute) > 640 or len(panel.fifteen_minute) > 128:
            raise ValueError("unbounded candle input panel")
        if (len(panel.oi) > 2048 or len(panel.predicted_funding) > 2048
                or len(panel.caps) > 128 or len(panel.liquidations) > 4096):
            raise ValueError("unbounded auxiliary input panel")
        input_sha = hashlib.sha256(json.dumps(asdict(panel), sort_keys=True,
                                               separators=(",", ":")).encode()).hexdigest()
        with sqlite3.connect(self.path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT serialized,last_input_hash,last_payload_json "
                               "FROM pump_v2_episode WHERE event_id=?",
                               (event.event_id,)).fetchone()
            if row is None:
                count = conn.execute("SELECT COUNT(*) FROM pump_v2_episode").fetchone()
                assert count is not None
                if int(count[0]) >= self.max_episodes:
                    raise OverflowError("episode capacity reached; archive before admitting")
                episode = start_episode(event)
                for horizon in (4, 24):
                    conn.execute(
                        "INSERT OR IGNORE INTO pump_v2_outcome_receipt "
                        "(event_id,origin,origin_ms,horizon_hours,status) "
                        "VALUES (?,'event',?,?,'PENDING')",
                        (event.event_id, event.event_ms, horizon),
                    )
            else:
                saved = json.loads(str(row[0]))
                episode = Episode(
                    event=Event(**saved["event"]), state=State(saved["state"]),
                    peak=float(saved["peak"]), peak_at_ms=int(saved["peak_at_ms"]),
                    trough=float(saved["trough"]),
                    evaluated_at_ms=int(saved["evaluated_at_ms"]),
                )
                if episode.event != event:
                    raise ValueError("event identity payload changed across restart")
                if episode.evaluated_at_ms == now_ms:
                    if row[1] != input_sha:
                        raise ValueError("different evidence for same decision clock")
                    return AdvanceReceipt(False, json.loads(str(row[2])))
            next_episode, decision = step_episode(episode, panel, now_ms=now_ms)
            payload = preview_payload(decision)
            serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            digest = hashlib.sha256(serialized.encode()).hexdigest()
            message_id = str(payload["message_id"])
            if decision.state is State.FADE_CANDIDATE:
                for horizon in (4, 24):
                    conn.execute(
                        "INSERT OR IGNORE INTO pump_v2_outcome_receipt "
                        "(event_id,origin,origin_ms,horizon_hours,status) "
                        "VALUES (?,'alert',?,?, 'PENDING')",
                        (event.event_id, now_ms, horizon),
                    )
            if self._at_capacity(conn):
                raise OverflowError("preview capacity reached; archive before admitting")
            conn.execute("INSERT INTO pump_v2_preview (message_id,payload_sha256,payload_json) "
                         "VALUES (?,?,?)", (message_id, digest, serialized))
            conn.execute("INSERT INTO pump_v2_decision_receipt "
                         "(event_id,decision_at_ms,input_sha256,message_id) "
                         "VALUES (?,?,?,?)", (event.event_id, now_ms, input_sha, message_id))
            conn.execute("INSERT INTO pump_v2_episode "
                         "(event_id,serialized,last_input_hash,last_payload_json) "
                         "VALUES (?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET "
                         "serialized=excluded.serialized,"
                         "last_input_hash=excluded.last_input_hash,"
                         "last_payload_json=excluded.last_payload_json",
                         (event.event_id,
                          json.dumps(asdict(next_episode), sort_keys=True),
                          input_sha, serialized))
            return AdvanceReceipt(True, payload)

    def checkpoint(self, event_id: str) -> EpisodeCheckpoint | None:
        """Return the durable latest clock/state used for replay catch-up."""

        with sqlite3.connect(self.path) as conn:
            row = conn.execute(
                "SELECT serialized FROM pump_v2_episode WHERE event_id=?", (event_id,)
            ).fetchone()
        if row is None:
            return None
        saved = json.loads(str(row[0]))
        return EpisodeCheckpoint(State(saved["state"]), int(saved["evaluated_at_ms"]))

    def archive_terminal(self, path: str | Path) -> ArchiveReceipt:
        """Create/verify a canonical archive before pruning only terminal events."""

        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            episode_rows = conn.execute(
                "SELECT event_id,serialized,last_input_hash,last_payload_json "
                "FROM pump_v2_episode ORDER BY event_id"
            ).fetchall()
            terminal = []
            for row in episode_rows:
                serialized = json.loads(str(row[1]))
                if State(serialized["state"]) not in (State.SKIP, State.EXPIRED):
                    continue
                pending = conn.execute(
                    "SELECT 1 FROM pump_v2_outcome_receipt WHERE event_id=? "
                    "AND status='PENDING' LIMIT 1", (str(row[0]),),
                ).fetchone()
                if pending is None:
                    terminal.append(row)
            event_ids = [str(row[0]) for row in terminal]
            decision_rows: list[tuple[object, ...]] = []
            preview_rows: list[tuple[object, ...]] = []
            outcome_rows: list[tuple[object, ...]] = []
            for event_id in event_ids:
                outcome_rows.extend(conn.execute(
                    "SELECT event_id,origin,origin_ms,horizon_hours,status,"
                    "outcome_identity,payload_sha256 FROM pump_v2_outcome_receipt "
                    "WHERE event_id=? ORDER BY origin,origin_ms,horizon_hours",
                    (event_id,),
                ).fetchall())
                decisions = conn.execute(
                    "SELECT event_id,decision_at_ms,input_sha256,message_id "
                    "FROM pump_v2_decision_receipt WHERE event_id=? "
                    "ORDER BY decision_at_ms,message_id",
                    (event_id,),
                ).fetchall()
                decision_rows.extend(decisions)
                for decision in decisions:
                    preview = conn.execute(
                        "SELECT message_id,payload_sha256,payload_json,status "
                        "FROM pump_v2_preview WHERE message_id=?",
                        (str(decision[3]),),
                    ).fetchone()
                    if preview is None:
                        raise ValueError("terminal archive is missing a linked preview")
                    preview_rows.append(preview)
        if not terminal:
            return ArchiveReceipt(str(target), "", 0, 0, 0)
        document = {
            "schema_version": "pump_fade_v2_terminal_archive_v1",
            "episodes": [
                {
                    "event_id": row[0], "serialized": row[1],
                    "last_input_hash": row[2], "last_payload_json": row[3],
                }
                for row in terminal
            ],
            "decisions": [
                {
                    "event_id": row[0], "decision_at_ms": row[1],
                    "input_sha256": row[2], "message_id": row[3],
                }
                for row in decision_rows
            ],
            "previews": [
                {
                    "message_id": row[0], "payload_sha256": row[1],
                    "payload_json": row[2], "status": row[3],
                }
                for row in sorted(preview_rows, key=lambda item: str(item[0]))
            ],
            "outcomes": [
                {
                    "event_id": row[0], "origin": row[1], "origin_ms": row[2],
                    "horizon_hours": row[3], "status": row[4],
                    "outcome_identity": row[5], "payload_sha256": row[6],
                }
                for row in sorted(
                    outcome_rows,
                    key=lambda item: (str(item[1]), cast(int, item[2]),
                                      cast(int, item[3])),
                )
            ],
        }
        payload = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
        digest = hashlib.sha256(payload).hexdigest()
        if target.exists():
            if target.read_bytes() != payload:
                raise FileExistsError("terminal archive path exists with different bytes")
        else:
            archive_bytes = sum(
                existing.stat().st_size for existing in target.parent.glob("*.json")
                if existing.is_file()
            )
            if archive_bytes + len(payload) > self.maximum_archive_bytes:
                raise OverflowError(
                    "terminal archive capacity reached; prune retained archives first"
                )
            temporary = target.with_name(target.name + ".partial")
            if temporary.exists():
                raise FileExistsError("terminal archive partial requires operator review")
            with temporary.open("xb", buffering=0) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            raise OSError("terminal archive hash acknowledgement failed")
        with sqlite3.connect(self.path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            for row in terminal:
                current = conn.execute(
                    "SELECT serialized,last_input_hash,last_payload_json "
                    "FROM pump_v2_episode WHERE event_id=?", (row[0],)
                ).fetchone()
                if current != (row[1], row[2], row[3]):
                    raise ValueError("terminal episode changed after archive snapshot")
            for decision in decision_rows:
                current_outcomes = conn.execute(
                    "SELECT event_id,origin,origin_ms,horizon_hours,status,"
                    "outcome_identity,payload_sha256 FROM pump_v2_outcome_receipt "
                    "WHERE event_id=? ORDER BY origin,origin_ms,horizon_hours",
                    (str(decision[0]),),
                ).fetchall()
                expected_outcomes = [row for row in outcome_rows if row[0] == decision[0]]
                if current_outcomes != expected_outcomes:
                    raise ValueError("outcome receipts changed after archive snapshot")
                conn.execute("DELETE FROM pump_v2_preview WHERE message_id=?", (str(decision[3]),))
            for event_id in event_ids:
                conn.execute("DELETE FROM pump_v2_decision_receipt WHERE event_id=?", (event_id,))
                conn.execute("DELETE FROM pump_v2_outcome_receipt WHERE event_id=?", (event_id,))
                conn.execute("DELETE FROM pump_v2_episode WHERE event_id=?", (event_id,))
        return ArchiveReceipt(str(target), digest, len(event_ids),
                              len(decision_rows), len(preview_rows))

