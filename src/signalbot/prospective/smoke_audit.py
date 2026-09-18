"""Read-only, deterministic audit for a completed causal-retest smoke DB."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from signalbot.config import Settings
from signalbot.domain.models import ObservedBboSnapshot
from signalbot.persistence.repository import (
    SqlRepository,
    _coverage_content_sha256,
    _coverage_shape_errors,
)
from signalbot.prospective.observer import shadow_config_sha256, shadow_policy_identity
from signalbot.prospective.retest import (
    restore_lifecycle,
    retest_policy_for_horizon,
)
from signalbot.prospective.retest_outcomes import (
    RetestOutcomePolicy,
    evaluate_reference,
    reference_from_raw_observation,
    reference_from_ready_snapshot,
)
from signalbot.prospective.source_freeze import default_source_root, freeze_source

AUDIT_SCHEMA_VERSION = "causal_retest_smoke_audit_v1"
AUDIT_SCHEMA_VERSION_V2 = "causal_retest_smoke_audit_v2"
PRIMARY_INTERVAL_MS = 5 * 60 * 1000
INFRA_SMOKE_MIN_CONSECUTIVE_CELLS = 18


def _sha256_canonical(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_object(value: str, label: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} is not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{label} must be a JSON object")
    return parsed


def _summary(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "median": None, "max": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "median": ordered[(len(ordered) - 1) // 2],
        "max": ordered[-1],
    }


def _identity_report(settings: Settings, campaign: dict[str, Any]) -> dict[str, Any]:
    freeze = freeze_source(default_source_root())
    retest_sha = retest_policy_for_horizon(settings.shadow.retest_horizon_bars).sha256
    outcome_sha = RetestOutcomePolicy().sha256
    computed = {
        "source_root_sha256": freeze.source_root_sha256,
        "source_identity": freeze.source_identity,
        "config_sha256": shadow_config_sha256(settings),
        "shadow_policy_sha256": shadow_policy_identity(settings.shadow, settings.signals),
        "retest_policy_sha256": retest_sha,
        "outcome_policy_sha256": outcome_sha,
    }
    # Semantically honest provenance: the campaign manifest durably persists
    # source/config/shadow identities; retest and outcome policies are not
    # persisted as campaign columns, so they are reported as derived from the
    # frozen source rather than masquerading as configured values.
    configured = {
        "source_identity": {
            "value": settings.shadow.source_identity,
            "provenance": "persisted",
        },
        "config_sha256": {"value": campaign["config_sha256"], "provenance": "persisted"},
        "shadow_policy_sha256": {
            "value": campaign["policy_sha256"],
            "provenance": "persisted",
        },
        "retest_policy_sha256": {
            "value": retest_sha,
            "provenance": "derived_from_frozen_source",
        },
        "outcome_policy_sha256": {
            "value": outcome_sha,
            "provenance": "derived_from_frozen_source",
        },
    }
    mismatches = {
        key: {"computed": value, "configured": configured.get(key)}
        for key, value in computed.items()
        if key in configured and configured[key]["value"] != value
    }
    if campaign["source_identity"] != freeze.source_identity:
        mismatches["campaign_source_identity"] = {
            "computed": freeze.source_identity,
            "configured": campaign["source_identity"],
        }
    if campaign["campaign_id"] != settings.shadow.campaign_id:
        mismatches["campaign_id"] = {
            "computed": settings.shadow.campaign_id,
            "configured": campaign["campaign_id"],
        }
    return {"computed": computed, "mismatches": mismatches}


def _coverage_report(
    repository: SqlRepository,
    *,
    campaign_id: str,
    manifest_sha256: str,
) -> tuple[dict[str, Any], list[str]]:
    rows = repository.list_shadow_coverage(
        campaign_id=campaign_id,
        campaign_manifest_sha256=manifest_sha256,
    )
    counts: Counter[str] = Counter()
    market_counts: dict[str, Counter[str]] = defaultdict(Counter)
    evidence_failures = 0
    errors: list[str] = []
    for row in rows:
        status = str(row["status"])
        counts[status] += 1
        market_counts[str(row["market"])][status] += 1
        evidence_failures += int(row["evidence_failures"])
        seen = sorted(set(json.loads(str(row["seen_symbols_json"]))))
        shape_errors = _coverage_shape_errors(
            mature_count=int(row["mature_count"]),
            htf_ready_count=int(row["htf_ready_count"]),
            fresh_bbo_count=int(row["fresh_bbo_count"]),
            raw_c0_count=int(row["raw_c0_count"]),
            comparator_rows=int(row["comparator_rows"]),
            evidence_failures=int(row["evidence_failures"]),
            seen_symbols=seen,
        )
        expected = int(row["expected_tradable_count"])
        if not (
            int(row["comparator_rows"])
            <= int(row["raw_c0_count"])
            <= int(row["mature_count"])
            <= expected
        ):
            shape_errors.append("counter ordering violates comparator<=raw<=mature<=expected")
        if int(row["htf_ready_count"]) > int(row["mature_count"]):
            shape_errors.append("htf_ready_count exceeds mature_count")
        if int(row["fresh_bbo_count"]) > int(row["mature_count"]):
            shape_errors.append("fresh_bbo_count exceeds mature_count")
        if len(seen) != int(row["mature_count"]):
            shape_errors.append("seen symbol count differs from mature_count")
        if status == "OPEN":
            errors.append(f"OPEN coverage cell remains: {row['market']}/{row['decision_close_ms']}")
        if status not in {"SEALED", "INCOMPLETE", "LEGACY_UNVERIFIED", "OPEN"}:
            errors.append(f"unknown coverage status: {status}")
        if status in {"SEALED", "INCOMPLETE"}:
            expected_sha = _coverage_content_sha256(
                campaign_id=campaign_id,
                market=str(row["market"]),
                decision_close_ms=int(row["decision_close_ms"]),
                primary_interval=str(row["primary_interval"]),
                expected_tradable_count=expected,
                tradable_universe_hash=str(row["tradable_universe_hash"]),
                mature_count=int(row["mature_count"]),
                htf_ready_count=int(row["htf_ready_count"]),
                fresh_bbo_count=int(row["fresh_bbo_count"]),
                raw_c0_count=int(row["raw_c0_count"]),
                comparator_rows=int(row["comparator_rows"]),
                evidence_failures=int(row["evidence_failures"]),
                seen_symbols=seen,
                complete=bool(row["complete"]),
                failures=json.loads(str(row["failures_json"])),
            )
            if expected_sha != row["content_sha256"]:
                errors.append(
                    "coverage content hash mismatch: "
                    f"{row['market']}/{row['decision_close_ms']}"
                )
        if bool(row["complete"]):
            if int(row["raw_c0_count"]) != int(row["comparator_rows"]):
                shape_errors.append("COMPLETE raw_c0_count differs from comparator_rows")
            if int(row["evidence_failures"]) != 0:
                shape_errors.append("COMPLETE cell has evidence failures")
        errors.extend(
            f"coverage shape {row['market']}/{row['decision_close_ms']}: {detail}"
            for detail in shape_errors
        )
    return (
        {
            "total_cells": len(rows),
            "by_status": dict(sorted(counts.items())),
            "per_market": {
                market: dict(sorted(values.items()))
                for market, values in sorted(market_counts.items())
            },
            "evidence_failures": evidence_failures,
        },
        errors,
    )


def _decimal_equal(left: Any, right: Any) -> bool:
    """Canonical numeric equality without binary-float information loss."""

    try:
        return _to_decimal(left) == _to_decimal(right)
    except (InvalidOperation, TypeError):
        return False


def _to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _bbo_report(observations: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    exact = receipt = 0
    absent = 0
    absent_receipt = 0
    eligible = ineligible = 0
    ages: list[float] = []
    errors: list[str] = []
    capacity_available = 0
    for row in observations:
        payload = _json_object(str(row["payload_json"]), "observation payload")
        execution = payload.get("execution_evidence")
        if not isinstance(execution, dict):
            errors.append(f"missing execution evidence: {row['observation_id']}")
            continue
        raw = execution.get("observed_bbo")
        if raw is None:
            absent += 1
            continue
        exact += 1
        try:
            snapshot = ObservedBboSnapshot.model_validate(raw)
        except Exception as exc:
            errors.append(f"invalid exact BBO {row['observation_id']}: {type(exc).__name__}")
            continue
        if snapshot.receipt_time_ms is not None:
            receipt += 1
        else:
            absent_receipt += 1
        ages.append(float(snapshot.age_ms))
        if snapshot.bid_quantity >= 0 and snapshot.ask_quantity >= 0:
            capacity_available += 1
        direction = str(row["direction"]).lower()
        executable = execution.get("executable_bbo_reference_price")
        expected = snapshot.ask_price if direction == "long" else snapshot.bid_price
        if executable is not None and not _decimal_equal(executable, expected):
            errors.append(f"directional BBO reference mismatch: {row['observation_id']}")
        if bool(execution.get("execution_available")):
            eligible += 1
        else:
            ineligible += 1
    return (
        {
            "raw_c0_rows": len(observations),
            "exact_bbo_present": exact,
            "exact_bbo_absent": absent,
            "receipt_clock_present": receipt,
            "receipt_clock_absent": absent_receipt,
            "spread_eligible": eligible,
            "spread_ineligible": ineligible,
            "directional_capacity_available": capacity_available,
            "age_ms": _summary(ages),
        },
        errors,
    )


def _raw_tape_report(raw_event_directory: Path) -> dict[str, Any]:
    """Read-only raw-event tape statistics for smoke qualification evidence.

    The tape is never rewritten or normalized; malformed records are counted
    without aborting so the audit can describe exactly what was recorded.
    """

    files = sorted(raw_event_directory.rglob("*.jsonl")) if raw_event_directory.is_dir() else []
    by_market: Counter[str] = Counter()
    bytes_by_market: Counter[str] = Counter()
    first_received_ms: int | None = None
    last_received_ms: int | None = None
    malformed = backwards_clock = market_mismatch = 0
    total_records = 0
    for path in files:
        market = path.parent.name
        size = path.stat().st_size
        bytes_by_market[market] += size
        previous_receipt: int | None = None
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                total_records += 1
                try:
                    record = json.loads(stripped)
                except json.JSONDecodeError:
                    malformed += 1
                    continue
                if not isinstance(record, dict):
                    malformed += 1
                    continue
                record_market = record.get("market")
                received_at_ms = record.get("received_at_ms")
                if (
                    record_market != market
                    or isinstance(received_at_ms, bool)
                    or not isinstance(received_at_ms, int)
                ):
                    market_mismatch += 1
                    continue
                by_market[market] += 1
                if previous_receipt is not None and received_at_ms < previous_receipt:
                    backwards_clock += 1
                previous_receipt = received_at_ms
                if first_received_ms is None or received_at_ms < first_received_ms:
                    first_received_ms = received_at_ms
                if last_received_ms is None or received_at_ms > last_received_ms:
                    last_received_ms = received_at_ms
    return {
        "files": len(files),
        "total_records": total_records,
        "records_by_market": dict(sorted(by_market.items())),
        "bytes_by_market": dict(sorted(bytes_by_market.items())),
        "first_received_at_ms": first_received_ms,
        "last_received_at_ms": last_received_ms,
        "malformed_records": malformed,
        "backwards_receipt_clocks": backwards_clock,
        "market_mismatches": market_mismatch,
    }


def _qualification_report(
    repository: SqlRepository,
    *,
    campaign_id: str,
    manifest_sha256: str,
    markets: list[str],
    integrity_errors: list[str],
) -> dict[str, Any]:
    """Frozen infra-smoke qualification contract.

    Requires at least 18 consecutive primary 5m decision-close coverage cells
    per configured market with zero post-shutdown OPEN cells and no integrity
    P0. Bootstrap REST candles are not coverage cells and can never satisfy
    this rule. Empty campaigns are NOT_RUN, not PASS.
    """

    rows = repository.list_shadow_coverage(
        campaign_id=campaign_id,
        campaign_manifest_sha256=manifest_sha256,
    )
    consecutive: dict[str, int] = {market: 0 for market in markets}
    maximum_consecutive: dict[str, int] = {market: 0 for market in markets}
    sealed_by_market: dict[str, list[int]] = {market: [] for market in markets}
    open_cells = sum(1 for row in rows if row["status"] == "OPEN")
    for row in rows:
        market = str(row["market"])
        if market in sealed_by_market and row["status"] in {"SEALED", "INCOMPLETE"}:
            sealed_by_market[market].append(int(row["decision_close_ms"]))
    for market, closes in sealed_by_market.items():
        ordered = sorted(set(closes))
        run = 0
        prior: int | None = None
        for close in ordered:
            if prior is not None and close - prior == PRIMARY_INTERVAL_MS:
                run += 1
            else:
                run = 1
            maximum_consecutive[market] = max(maximum_consecutive[market], run)
            prior = close
        consecutive[market] = maximum_consecutive[market]
    qualified_markets = [
        market
        for market in markets
        if consecutive.get(market, 0) >= INFRA_SMOKE_MIN_CONSECUTIVE_CELLS
    ]
    all_markets_qualified = bool(markets) and len(qualified_markets) == len(markets)
    has_integrity_p0 = bool(integrity_errors)
    if not any(row["status"] in {"SEALED", "INCOMPLETE"} for row in rows):
        status = "NOT_RUN"
    elif all_markets_qualified and open_cells == 0 and not has_integrity_p0:
        status = "PASS"
    elif has_integrity_p0 or open_cells > 0:
        status = "FAIL"
    else:
        status = "INCONCLUSIVE_INSUFFICIENT_RUNTIME"
    return {
        "infra_smoke": status,
        "required_consecutive_primary_cells_per_market": INFRA_SMOKE_MIN_CONSECUTIVE_CELLS,
        "qualifying_consecutive_primary_cells_by_market": {
            market: consecutive.get(market, 0) for market in markets
        },
        "maximum_consecutive_primary_cells_by_market": maximum_consecutive,
        "open_coverage_cells_after_shutdown": open_cells,
        "integrity_p0_present": has_integrity_p0,
    }


def _outcome_report(
    repository: SqlRepository,
    observations: list[dict[str, Any]],
    lifecycles: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    symbols = {str(row["symbol"]) for row in observations}
    markets = {str(row["market"]) for row in observations}
    candles = repository.list_candles(symbols=symbols, interval="5m")
    grouped: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for candle in candles:
        grouped[(candle.market.value, candle.symbol)].append(candle)
    lifecycles_by_opportunity = {
        str(row["opportunity_id"]): row for row in lifecycles
    }
    counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    errors: list[str] = []
    evaluated_references = 0
    for observation in observations:
        payload = _json_object(str(observation["payload_json"]), "observation payload")
        try:
            reference = reference_from_raw_observation(
                payload,
                campaign_id=str(observation["campaign_id"]),
                campaign_manifest_sha256=str(observation["campaign_manifest_sha256"]),
                retest_policy_sha256=str(
                    lifecycles_by_opportunity.get(str(observation["opportunity_id"]), {})
                    .get("retest_policy_sha256", "")
                ),
            )
        except Exception as exc:
            errors.append(
                f"raw reference invalid {observation['opportunity_id']}: "
                f"{type(exc).__name__}"
            )
            continue
        for outcome in evaluate_reference(
            reference,
            grouped[(reference.market.value, reference.symbol)],
        ):
            evaluated_references += 1
            counts[f"RAW_C0/{outcome.horizon_bars}/{outcome.status.value}"] += 1
            if outcome.reason:
                reason_counts[outcome.reason] += 1
        lifecycle_row = lifecycles_by_opportunity.get(str(observation["opportunity_id"]))
        if lifecycle_row is None:
            continue
        lifecycle = restore_lifecycle(lifecycle_row["lifecycle"])
        if lifecycle.ready_snapshot is None:
            continue
        ready_reference = reference_from_ready_snapshot(
            lifecycle.ready_snapshot,
            retest_policy_sha256=str(lifecycle_row["retest_policy_sha256"]),
        )
        for outcome in evaluate_reference(
            ready_reference,
            grouped[(ready_reference.market.value, ready_reference.symbol)],
        ):
            evaluated_references += 1
            counts[f"READY/{outcome.horizon_bars}/{outcome.status.value}"] += 1
            if outcome.reason:
                reason_counts[outcome.reason] += 1
    return (
        {
            "markets_with_candles": sorted(markets),
            "evaluated_reference_count": evaluated_references,
            "by_reference_horizon_status": dict(sorted(counts.items())),
            "reasons": dict(sorted(reason_counts.items())),
        },
        errors,
    )


def build_smoke_audit(
    settings: Settings,
    repository: SqlRepository,
    *,
    campaign_id: str,
    raw_event_directory: str | Path | None = None,
    generated_at_ms: int | None = None,
) -> dict[str, Any]:
    """Build a report without mutating any evidence table."""

    campaign = repository.get_shadow_campaign(campaign_id)
    errors: list[str] = []
    if campaign is None:
        raise ValueError(f"smoke campaign not found: {campaign_id}")
    if campaign["campaign_mode"] != "smoke":
        errors.append("campaign_mode is not smoke")
    if not campaign_id.lower().startswith("smoke-"):
        errors.append("campaign_id does not use smoke namespace")
    if campaign["observation_schema_version"] != "shadow_observation_v2":
        errors.append("campaign observation schema is not shadow_observation_v2")
    identity = _identity_report(settings, campaign)
    errors.extend(
        f"identity mismatch {key}: computed={value['computed']} configured={value['configured']}"
        for key, value in identity["mismatches"].items()
    )
    manifest_sha = str(campaign["manifest_sha256"])
    observations = repository.list_shadow_observations(
        campaign_id=campaign_id,
        campaign_manifest_sha256=manifest_sha,
    )
    coverage, coverage_errors = _coverage_report(
        repository,
        campaign_id=campaign_id,
        manifest_sha256=manifest_sha,
    )
    all_coverage = repository.list_shadow_coverage(
        campaign_id=campaign_id,
        campaign_manifest_sha256=None,
    )
    coverage["legacy_unverified"] = sum(
        1
        for row in all_coverage
        if row["status"] == "LEGACY_UNVERIFIED"
        or row["campaign_manifest_sha256"] == ""
    )
    errors.extend(coverage_errors)
    denominator = repository.audit_retest_denominator(
        campaign_id=campaign_id,
        campaign_manifest_sha256=manifest_sha,
        retest_policy_sha256=identity["computed"]["retest_policy_sha256"],
    )
    denominator_errors = [
        name
        for name in (
            "missing_opportunity_ids",
            "unexpected_opportunity_ids",
            "duplicate_expected_ids",
            "duplicate_lifecycle_ids",
        )
        if denominator[name]
    ]
    errors.extend(f"denominator {name} is non-empty" for name in denominator_errors)
    lifecycles = repository.list_retest_lifecycles(
        campaign_id=campaign_id,
        campaign_manifest_sha256=manifest_sha,
        retest_policy_sha256=identity["computed"]["retest_policy_sha256"],
        active_only=False,
    )
    bbo, bbo_errors = _bbo_report(observations)
    errors.extend(bbo_errors)
    outcomes, outcome_errors = _outcome_report(repository, observations, lifecycles)
    errors.extend(outcome_errors)
    transitions: list[dict[str, Any]] = []
    transition_errors: list[str] = []
    for row in lifecycles:
        history = repository.list_retest_transitions(
            campaign_id=campaign_id,
            opportunity_id=str(row["opportunity_id"]),
        )
        transitions.extend(history)
        for transition in history:
            payload = _json_object(
                str(transition["payload_json"]), "transition payload"
            )
            if _sha256_canonical(payload) != transition["payload_sha256"]:
                transition_errors.append(
                    f"transition payload hash mismatch: {transition['transition_id']}"
                )
    errors.extend(transition_errors)
    report = {
        "audit_schema_version": AUDIT_SCHEMA_VERSION,
        "generated_at_ms": generated_at_ms
        if generated_at_ms is not None
        else int(datetime.now(UTC).timestamp() * 1000),
        "campaign": {
            "campaign_id": campaign_id,
            "manifest_sha256": manifest_sha,
            "campaign_mode": campaign["campaign_mode"],
            "observation_schema_version": campaign["observation_schema_version"],
            "status": campaign["status"],
        },
        "identities": identity,
        "coverage": coverage,
        "retest": {
            "raw_c0_opportunities": len(observations),
            **{
                key: denominator[key]
                for key in (
                    "lifecycle_rows",
                    "active",
                    "touched",
                    "READY",
                    "INVALID",
                    "TIMEOUT",
                    "CENSORED",
                    "missing_opportunity_ids", "unexpected_opportunity_ids",
                    "duplicate_expected_ids", "duplicate_lifecycle_ids",
                )
            },
            "transition_count": len(transitions),
        },
        "bbo": bbo,
        "outcomes": outcomes,
        "qualification": _qualification_report(
            repository,
            campaign_id=campaign_id,
            manifest_sha256=manifest_sha,
            markets=[market.value for market in settings.binance.markets],
            integrity_errors=list(errors),
        ),
        "raw_tape": _raw_tape_report(
            Path(
                raw_event_directory
                if raw_event_directory is not None
                else settings.runtime.raw_event_directory
            )
        ),
        "integrity": {
            "errors": sorted(set(errors)),
            "pass": not errors,
        },
        "parity": {
            "LIFECYCLE_PRICE_PARITY": "INCONCLUSIVE",
            "BBO_RECEIPT_PARITY": "INCONCLUSIVE",
            "FULL_READY_SNAPSHOT_PARITY": "INCONCLUSIVE_MISSING_INDEPENDENT_FUNDING_AUTHORITY",
            "HISTORICAL_EXECUTION_PARITY": "INCONCLUSIVE_NO_HISTORICAL_BBO",
        },
    }
    content = {key: value for key, value in report.items() if key != "generated_at_ms"}
    report["report_sha256"] = _sha256_canonical(content)
    return report


def write_smoke_audit(
    settings: Settings,
    repository: SqlRepository,
    *,
    campaign_id: str,
    output: str | Path,
    raw_event_directory: str | Path | None = None,
    generated_at_ms: int | None = None,
) -> dict[str, Any]:
    report = build_smoke_audit(
        settings,
        repository,
        campaign_id=campaign_id,
        raw_event_directory=raw_event_directory,
        generated_at_ms=generated_at_ms,
    )
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n",
        encoding="utf-8",
    )
    return report


def build_smoke_audit_v2(
    settings: Settings,
    repository: SqlRepository,
    *,
    campaign_id: str,
    raw_event_directory: str | Path | None = None,
    generated_at_ms: int | None = None,
) -> dict[str, Any]:
    """WP5 audit v2: v1 semantics untouched; adds HARD raw-tape gates.

    Raw-tape integrity is evaluated through the unified streaming reader
    (iter_raw_tape): broken chain, missing manifest, extra segment, partial
    tail, hash mismatch, count mismatch, receipt rollback, market mismatch,
    and malformed envelopes are all HARD qualification failures. Any raw-tape
    integrity error forces infra_smoke FAIL regardless of coverage counts.
    """
    report = build_smoke_audit(
        settings,
        repository,
        campaign_id=campaign_id,
        raw_event_directory=raw_event_directory,
        generated_at_ms=generated_at_ms,
    )
    tape_dir = Path(
        raw_event_directory
        if raw_event_directory is not None
        else settings.runtime.raw_event_directory
    )
    from signalbot.domain.enums import Market
    from signalbot.prospective.segmented_replay import (
        RawTapeIntegrityError,
        iter_legacy_jsonl,
        iter_segmented_zstd,
    )

    hard_gate_errors: list[str] = []
    markets_streamed: dict[str, int] = {}
    if tape_dir.is_dir():
        for market in Market:
            market_dir = tape_dir / market.value
            if not market_dir.is_dir():
                continue
            has_manifests = any(market_dir.glob("*.manifest.json"))
            try:
                count = 0
                if has_manifests:
                    for _record in iter_segmented_zstd(market_dir):
                        count += 1
                else:
                    for _record in iter_legacy_jsonl(market_dir):
                        count += 1
                markets_streamed[market.value] = count
            except RawTapeIntegrityError as exc:
                hard_gate_errors.append(f"{market.value}: {exc}")
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                hard_gate_errors.append(f"{market.value}: malformed envelope: {exc}")
            except OSError as exc:
                hard_gate_errors.append(f"{market.value}: unreadable: {exc}")
    report["audit_schema_version"] = AUDIT_SCHEMA_VERSION_V2
    report["raw_tape_hard_gates"] = {
        "markets_streamed": markets_streamed,
        "errors": sorted(set(hard_gate_errors)),
        "pass": not hard_gate_errors,
    }
    qualification = report.get("qualification", {})
    if hard_gate_errors:
        qualification["infra_smoke"] = "FAIL"
        qualification["integrity_p0_present"] = True
    # Recompute the report hash over the amended content.
    content = {
        key: value
        for key, value in report.items()
        if key not in ("generated_at_ms", "report_sha256")
    }
    report["report_sha256"] = _sha256_canonical(content)
    return report
