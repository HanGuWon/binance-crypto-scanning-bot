from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

_DAY_MS = 86_400_000
_SIDES = (("spot", "long"), ("futures", "short"))
_BLOCK_DAYS = (7, 14, 28)
_PRIMARY_BLOCK_DAYS = 7
_PROTOCOL_VERSION = "r2_retrospective_screen_v1"
_FULL_R2_STATUS = "INCONCLUSIVE_NO_HISTORICAL_BBO"
_FROZEN_BOOTSTRAP_SAMPLES = 50_000
_FROZEN_BOOTSTRAP_SEED = 20_260_716
_FROZEN_PLAN_SHA256 = (
    "42fc382837747150f02cd14dacdb6b3573c77c1e017c9eb997529659c616d2de"
)
_FROZEN_SPEC_SHA256 = {
    "c0": "2dce99a243c4f94c446cf48a0edcb103093db9cecf0e6da261c35b45fa235c7f",
    "h1": "e1585d449098115074e3cb735763a84b3b9982f8c5943ecf3dfe8db5b354732b",
}
_FROZEN_ASSETS = ("BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "SUI", "WIF")
_FROZEN_INPUT_PATHS = frozenset(
    f"{market}/{asset}__{asset}USDT__5m.csv.gz"
    for market in ("spot", "futures", "funding")
    for asset in _FROZEN_ASSETS
)

R2Status = Literal[
    "INVALID",
    "INCONCLUSIVE",
    "RETROSPECTIVE_SCREEN_PASS",
    "FAIL",
]

_RUN_OUTPUTS = ("opportunities.csv", "trades.csv", "results.json", "report.md")
_R2_RUNNER_CONTRACT_METADATA: dict[str, object] = {
    "opportunity_panel_horizon_bars": 72,
    "outcome_edge_margin_bps": 0.0,
    "prediction_horizons_bars": [3, 6, 12],
    "prediction_entry": "next_contiguous_5m_open",
    "prediction_exit": "decision_index_plus_h_close",
    "outcome_labels": [
        "KLINE_PROXY_LONG",
        "KLINE_PROXY_FLAT",
        "KLINE_PROXY_SHORT",
    ],
}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _require_sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value.lower()
    ):
        raise ValueError(f"{field} must be a 64-character SHA-256 digest")
    return value.lower()


def _read_verified_run_manifest(run_dir: Path) -> dict[str, Any]:
    manifest_path = run_dir / "run_manifest.json"
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"run manifest root is not an object: {manifest_path}")
    outputs = raw.get("outputs")
    if not isinstance(outputs, dict):
        raise ValueError(f"run manifest outputs are missing: {manifest_path}")
    for name in _RUN_OUTPUTS:
        declared = outputs.get(name)
        if not isinstance(declared, str) or len(declared) != 64:
            raise ValueError(f"run manifest lacks {name} hash: {manifest_path}")
        actual = _sha256_file(run_dir / name)
        if actual != declared:
            raise ValueError(f"run output hash mismatch for {run_dir / name}")
    return raw


def _validate_r2_backtest_contract(
    value: object,
    *,
    candidate_policy: str,
    role_name: str,
) -> None:
    """Validate either the legacy R2 contract or its recognized runner extension."""

    if not isinstance(value, dict):
        raise ValueError(f"R2 frozen backtest contract mismatch for {role_name}")
    legacy: dict[str, object] = {
        "candidate_policy": candidate_policy,
        "confirmation_mode": "explicit_trigger",
        "interval": "5m",
        "max_holding_bars": 72,
    }
    allowed_keys = set(legacy) | set(_R2_RUNNER_CONTRACT_METADATA)
    unknown_keys = set(value) - allowed_keys
    if unknown_keys:
        raise ValueError(
            f"R2 frozen backtest contract has unknown keys for {role_name}: "
            f"{sorted(unknown_keys)}"
        )
    missing_legacy = set(legacy) - set(value)
    if missing_legacy:
        raise ValueError(
            f"R2 frozen backtest contract is missing keys for {role_name}: "
            f"{sorted(missing_legacy)}"
        )
    for key, expected in legacy.items():
        actual = value[key]
        if type(actual) is not type(expected) or actual != expected:
            raise ValueError(
                f"R2 frozen backtest contract mismatch for {role_name} field {key}"
            )

    additive_keys = set(value) & set(_R2_RUNNER_CONTRACT_METADATA)
    if additive_keys and additive_keys != set(_R2_RUNNER_CONTRACT_METADATA):
        missing_additive = set(_R2_RUNNER_CONTRACT_METADATA) - additive_keys
        raise ValueError(
            f"R2 runner contract metadata is incomplete for {role_name}: "
            f"{sorted(missing_additive)}"
        )
    for key in additive_keys:
        actual = value[key]
        expected = _R2_RUNNER_CONTRACT_METADATA[key]
        if type(actual) is not type(expected) or actual != expected:
            raise ValueError(
                f"R2 runner contract metadata mismatch for {role_name} field {key}"
            )


def validate_r2_run_provenance(
    c0_a_dir: str | Path,
    c0_b_dir: str | Path,
    h1_a_dir: str | Path,
    h1_b_dir: str | Path,
) -> dict[str, Any]:
    """Verify paired A/B determinism and the shared frozen input contract."""

    directories = {
        "c0_a": Path(c0_a_dir),
        "c0_b": Path(c0_b_dir),
        "h1_a": Path(h1_a_dir),
        "h1_b": Path(h1_b_dir),
    }
    resolved_directories = {directory.resolve() for directory in directories.values()}
    if len(resolved_directories) != len(directories):
        raise ValueError("R2 A/B role directories must be four distinct paths")
    manifests = {
        name: _read_verified_run_manifest(directory)
        for name, directory in directories.items()
    }
    for left, right in (("c0_a", "c0_b"), ("h1_a", "h1_b")):
        for field in (
            "protocol_version",
            "rule_version",
            "code_sha256",
            "spec_sha256",
            "backtest_contract",
            "config_input_sha256",
            "effective_settings_sha256",
            "experiment_plan_sha256",
            "inputs",
            "outputs",
        ):
            if manifests[left].get(field) != manifests[right].get(field):
                raise ValueError(
                    f"A/B provenance mismatch for {left}/{right} field {field}"
                )

    common_fields = (
        "code_sha256",
        "config_input_sha256",
        "effective_settings_sha256",
        "experiment_plan_sha256",
        "inputs",
    )
    reference = manifests["c0_a"]
    for name, manifest in manifests.items():
        for field in common_fields:
            if manifest.get(field) != reference.get(field):
                raise ValueError(
                    f"shared R2 provenance mismatch for {name} field {field}"
                )
        environment = manifest.get("environment")
        reference_environment = reference.get("environment")
        if not isinstance(environment, dict) or not isinstance(
            reference_environment, dict
        ):
            raise ValueError(f"environment provenance is missing for {name}")
        if environment.get("uv_lock_sha256") != reference_environment.get(
            "uv_lock_sha256"
        ):
            raise ValueError(f"uv.lock provenance mismatch for {name}")

    expected_roles = {
        "c0_a": (
            "r2_retrospective_screen_v1_c0_corrected",
            "c0_frozen",
        ),
        "c0_b": (
            "r2_retrospective_screen_v1_c0_corrected",
            "c0_frozen",
        ),
        "h1_a": (
            "r2_retrospective_screen_v1_h1_strict_pit_htf",
            "strict_pit_htf_diagnostic",
        ),
        "h1_b": (
            "r2_retrospective_screen_v1_h1_strict_pit_htf",
            "strict_pit_htf_diagnostic",
        ),
    }
    for name, (protocol_version, candidate_policy) in expected_roles.items():
        manifest = manifests[name]
        if manifest.get("protocol_version") != protocol_version:
            raise ValueError(f"R2 role/protocol mismatch for {name}")
        _validate_r2_backtest_contract(
            manifest.get("backtest_contract"),
            candidate_policy=candidate_policy,
            role_name=name,
        )
        for field in (
            "code_sha256",
            "spec_sha256",
            "config_input_sha256",
            "effective_settings_sha256",
            "experiment_plan_sha256",
        ):
            _require_sha256(manifest.get(field), f"{name}.{field}")
        role = "c0" if name.startswith("c0_") else "h1"
        if _require_sha256(
            manifest.get("experiment_plan_sha256"),
            f"{name}.experiment_plan_sha256",
        ) != _FROZEN_PLAN_SHA256:
            raise ValueError(f"R2 frozen experiment plan hash mismatch for {name}")
        if _require_sha256(
            manifest.get("spec_sha256"), f"{name}.spec_sha256"
        ) != _FROZEN_SPEC_SHA256[role]:
            raise ValueError(f"R2 frozen specification hash mismatch for {name}")
        inputs = manifest.get("inputs")
        if not isinstance(inputs, dict) or not inputs:
            raise ValueError(f"R2 input provenance is empty for {name}")
        if frozenset(inputs) != _FROZEN_INPUT_PATHS:
            raise ValueError(f"R2 frozen 24-file input panel mismatch for {name}")
        for input_path, input_sha in inputs.items():
            if not isinstance(input_path, str) or not input_path:
                raise ValueError(f"R2 input path is invalid for {name}")
            _require_sha256(input_sha, f"{name}.inputs[{input_path}]")
        environment = manifest.get("environment")
        if not isinstance(environment, dict):
            raise ValueError(f"environment provenance is missing for {name}")
        _require_sha256(
            environment.get("uv_lock_sha256"), f"{name}.environment.uv_lock_sha256"
        )

    if manifests["c0_a"].get("spec_sha256") == manifests["h1_a"].get(
        "spec_sha256"
    ):
        raise ValueError("C0 and H1 must use distinct frozen specifications")

    plan_sha = reference.get("experiment_plan_sha256")
    code_sha = reference.get("code_sha256")
    if not isinstance(plan_sha, str) or len(plan_sha) != 64:
        raise ValueError("frozen experiment plan hash is missing")
    if not isinstance(code_sha, str) or len(code_sha) != 64:
        raise ValueError("source code hash is missing")
    return {
        "valid": True,
        "ab_output_identity": {"c0": True, "h1": True},
        "shared_input_identity": True,
        "code_sha256": code_sha,
        "experiment_plan_sha256": plan_sha,
        "uv_lock_sha256": reference["environment"]["uv_lock_sha256"],
        "run_manifest_sha256": {
            name: _sha256_file(directory / "run_manifest.json")
            for name, directory in directories.items()
        },
    }


def validate_r2_analysis_parameters(samples: int, seed: int) -> None:
    """Reject CLI parameters that deviate from the frozen R2 protocol."""

    if samples != _FROZEN_BOOTSTRAP_SAMPLES or seed != _FROZEN_BOOTSTRAP_SEED:
        raise ValueError(
            "frozen R2 analysis requires 50000 bootstrap samples and seed 20260716"
        )


@dataclass(frozen=True, slots=True)
class R2Opportunity:
    """One frozen C0-trigger episode as observed by one strategy variant.

    ``episode_net_return_60m`` is a fixed-horizon, already costed return from
    the next 5-minute open to the twelfth 5-minute close. Fee and slippage
    components are positive costs; funding is signed P&L. The H1 panel uses
    ``h1_accepted`` as its policy indicator. The C0 value of that field is not
    used, but retaining a common shape makes CSV ingestion and integrity checks
    straightforward.
    """

    opportunity_id: str
    asset: str
    market: str
    direction: str
    decision_time_ms: int
    h1_accepted: bool
    episode_net_return_60m: float | None
    analysis_eligible: bool = True
    h1_feature_available: bool = True
    gross_return_60m: float | None = None
    fee_return_60m: float | None = None
    slippage_return_60m: float | None = None
    funding_return_60m: float | None = None
    historical_bbo_observed: bool = False
    next_open_time_ms: int | None = None

    @property
    def utc_day(self) -> int:
        """Return the zero-based Unix UTC calendar day containing the decision."""

        return self.decision_time_ms // _DAY_MS


@dataclass(frozen=True, slots=True)
class AlignedR2Opportunity:
    """A C0 episode and its H1 decision after strict identity validation."""

    c0: R2Opportunity
    h1: R2Opportunity


@dataclass(frozen=True, slots=True)
class R2TechnicalTrade:
    """One independently openable T72 technical-exit episode.

    ``opportunity_id`` should be populated by new R2 artifacts. The CSV reader
    also supports legacy trade files without that column; those rows are
    matched by the strict, unique asset/side/decision-time tuple instead.
    """

    opportunity_id: str
    asset: str
    market: str
    direction: str
    decision_time_ms: int
    entry_time_ms: int
    exit_time_ms: int
    technical_net_return: float
    split_contained: bool = True
    gross_return: float | None = None
    fee_return: float | None = None
    slippage_return: float | None = None
    funding_return: float | None = None
    bars_held: int | None = None


@dataclass(frozen=True, slots=True)
class _MappedTechnicalTrade:
    opportunity: AlignedR2Opportunity
    trade: R2TechnicalTrade


@dataclass(frozen=True, slots=True)
class _SideDay:
    common_count: int = 0
    c0_return_sum: float = 0.0
    accepted_count: int = 0
    accepted_return_sum: float = 0.0
    technical_count: int = 0
    technical_return_sum: float = 0.0
    technical_uplift_sum: float = 0.0


@dataclass(frozen=True, slots=True)
class _BootstrapSide:
    conditional_mean: tuple[float, ...]
    policy_uplift: tuple[float, ...]
    technical_mean: tuple[float, ...]
    technical_uplift: tuple[float, ...]
    entry_invalid_replicates: int
    exit_invalid_replicates: int


def _strict_bool(value: object, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    raise ValueError(f"{field} must be true or false")


def _optional_bool(value: str | None, field: str, *, default: bool) -> bool:
    if value is None or not value.strip():
        return default
    return _strict_bool(value, field)


def _optional_finite_float(value: str | None, field: str) -> float | None:
    if value is None or not value.strip():
        return None
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be finite when present")
    return parsed


def _first_present(row: Mapping[str, str | None], *fields: str) -> str | None:
    for field in fields:
        value = row.get(field)
        if value is not None and value.strip():
            return value
    return None


def _derived_htf_availability(row: Mapping[str, str | None]) -> bool:
    explicit = _first_present(row, "h1_feature_available", "htf_filter_available")
    if explicit is not None:
        return _strict_bool(explicit, "h1_feature_available")
    failures = (row.get("htf_filter_failures") or "").lower()
    unavailable_markers = (
        "missing mature strictly-prior",
        "context interval mismatch",
        "context is not strictly prior",
    )
    return not any(marker in failures for marker in unavailable_markers)


def read_r2_opportunities(path: str | Path) -> tuple[R2Opportunity, ...]:
    """Read the strict R2 subset of an ``opportunities.csv`` artifact.

    The reader accepts the engine's ``f60_*`` names and the descriptive
    ``*_return_60m`` aliases used by this analysis module. Extra CSV fields are
    ignored. Malformed rows fail with their physical CSV line number.
    """

    output: list[R2Opportunity] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                opportunity_id = str(row["opportunity_id"]).strip()
                asset = str(row["asset"]).strip().upper()
                if not opportunity_id or not asset:
                    raise ValueError("opportunity_id and asset must not be empty")
                decision_time_ms = int(row["decision_time_ms"])
                net_return = _optional_finite_float(
                    _first_present(
                        row,
                        "episode_net_return_60m",
                        "f60_net_return",
                    ),
                    "episode_net_return_60m",
                )
                failures = row.get("htf_filter_failures") or ""
                historical_bbo = _optional_bool(
                    _first_present(
                        row,
                        "historical_bbo_observed",
                        "execution_observed",
                    ),
                    "historical_bbo_observed",
                    default=False,
                )
                output.append(
                    R2Opportunity(
                        opportunity_id=opportunity_id,
                        asset=asset,
                        market=str(row["market"]).strip().lower(),
                        direction=str(row["direction"]).strip().lower(),
                        decision_time_ms=decision_time_ms,
                        h1_accepted=_strict_bool(
                            _first_present(row, "h1_accepted", "htf_filter_accepted"),
                            "h1_accepted",
                        ),
                        episode_net_return_60m=net_return,
                        analysis_eligible=_optional_bool(
                            _first_present(row, "analysis_eligible_72"),
                            "analysis_eligible_72",
                            default=net_return is not None,
                        ),
                        h1_feature_available=_derived_htf_availability(row),
                        gross_return_60m=_optional_finite_float(
                            _first_present(row, "gross_return_60m", "f60_gross_return"),
                            "gross_return_60m",
                        ),
                        fee_return_60m=_optional_finite_float(
                            _first_present(row, "fee_return_60m", "f60_fee_return"),
                            "fee_return_60m",
                        ),
                        slippage_return_60m=_optional_finite_float(
                            _first_present(
                                row,
                                "slippage_return_60m",
                                "f60_slippage_return",
                            ),
                            "slippage_return_60m",
                        ),
                        funding_return_60m=_optional_finite_float(
                            _first_present(
                                row,
                                "funding_return_60m",
                                "f60_funding_return",
                            ),
                            "funding_return_60m",
                        ),
                        historical_bbo_observed=historical_bbo,
                        next_open_time_ms=(
                            None
                            if (next_open := _first_present(row, "next_open_time_ms"))
                            is None
                            else int(next_open)
                        ),
                    )
                )
                if failures and not output[-1].h1_feature_available and output[-1].h1_accepted:
                    raise ValueError("an unavailable H1 feature cannot be accepted")
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"invalid R2 opportunity on CSV line {line_number}") from exc
    return tuple(output)


def read_r2_technical_trades(path: str | Path) -> tuple[R2TechnicalTrade, ...]:
    """Read the R2 T72 subset of a technical ``trades.csv`` artifact."""

    output: list[R2TechnicalTrade] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        for line_number, row in enumerate(csv.DictReader(handle), start=2):
            try:
                asset = str(row["asset"]).strip().upper()
                if not asset:
                    raise ValueError("asset must not be empty")
                decision_time_raw = _first_present(
                    row, "decision_time_ms", "entry_signal_time_ms"
                )
                net_return_raw = _first_present(
                    row, "technical_net_return", "t72_net_return", "net_return"
                )
                if decision_time_raw is None or net_return_raw is None:
                    raise ValueError("decision time and technical net return are required")
                net_return = float(net_return_raw)
                if not math.isfinite(net_return):
                    raise ValueError("technical_net_return must be finite")
                entry_time_raw = _first_present(row, "entry_time_ms")
                exit_time_raw = _first_present(row, "exit_time_ms")
                if entry_time_raw is None or exit_time_raw is None:
                    raise ValueError("entry and exit times are required")
                output.append(
                    R2TechnicalTrade(
                        opportunity_id=(row.get("opportunity_id") or "").strip(),
                        asset=asset,
                        market=str(row["market"]).strip().lower(),
                        direction=str(row["direction"]).strip().lower(),
                        decision_time_ms=int(decision_time_raw),
                        entry_time_ms=int(entry_time_raw),
                        exit_time_ms=int(exit_time_raw),
                        technical_net_return=net_return,
                        split_contained=_optional_bool(
                            _first_present(row, "split_contained"),
                            "split_contained",
                            default=True,
                        ),
                        gross_return=_optional_finite_float(
                            _first_present(row, "technical_gross_return", "gross_return"),
                            "technical_gross_return",
                        ),
                        fee_return=_optional_finite_float(
                            _first_present(row, "technical_fee_return", "fee_return"),
                            "technical_fee_return",
                        ),
                        slippage_return=_optional_finite_float(
                            _first_present(
                                row,
                                "technical_slippage_return",
                                "slippage_return",
                            ),
                            "technical_slippage_return",
                        ),
                        funding_return=_optional_finite_float(
                            _first_present(
                                row,
                                "technical_funding_return",
                                "funding_return",
                            ),
                            "technical_funding_return",
                        ),
                        bars_held=(
                            None
                            if (bars_held := _first_present(row, "bars_held")) is None
                            else int(bars_held)
                        ),
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"invalid R2 technical trade on CSV line {line_number}"
                ) from exc
    return tuple(output)


def _validate_record(variant: str, item: R2Opportunity) -> None:
    identity = f"{variant}:{item.opportunity_id}"
    if not item.opportunity_id or not item.asset:
        raise ValueError(f"{identity} has an empty ID or asset")
    if item.decision_time_ms < 0:
        raise ValueError(f"{identity} has a negative decision time")
    if (item.market, item.direction) not in _SIDES:
        raise ValueError(f"{identity} has unsupported side {item.market}-{item.direction}")
    if not isinstance(item.h1_accepted, bool):
        raise ValueError(f"{identity} has non-boolean H1 acceptance")
    if not isinstance(item.analysis_eligible, bool) or not isinstance(
        item.h1_feature_available, bool
    ):
        raise ValueError(f"{identity} has non-boolean availability")
    if item.h1_accepted and not item.h1_feature_available and variant == "H1":
        raise ValueError(f"{identity} accepts an unavailable H1 feature")
    for field, value in (
        ("episode_net_return_60m", item.episode_net_return_60m),
        ("gross_return_60m", item.gross_return_60m),
        ("fee_return_60m", item.fee_return_60m),
        ("slippage_return_60m", item.slippage_return_60m),
        ("funding_return_60m", item.funding_return_60m),
    ):
        if value is not None and not math.isfinite(value):
            raise ValueError(f"{identity} has non-finite {field}")
    if item.analysis_eligible and item.episode_net_return_60m is None:
        raise ValueError(f"{identity} is analysis-eligible without a 60m net return")
    if item.analysis_eligible and item.next_open_time_ms is None:
        raise ValueError(f"{identity} is analysis-eligible without a next-open time")
    if (
        item.next_open_time_ms is not None
        and item.next_open_time_ms <= item.decision_time_ms
    ):
        raise ValueError(f"{identity} has a non-causal next-open time")
    if item.fee_return_60m is not None and item.fee_return_60m < 0:
        raise ValueError(f"{identity} has a negative fee cost")
    if item.slippage_return_60m is not None and item.slippage_return_60m < 0:
        raise ValueError(f"{identity} has a negative slippage cost")
    components = (
        item.gross_return_60m,
        item.fee_return_60m,
        item.slippage_return_60m,
        item.funding_return_60m,
    )
    if all(value is not None for value in components) and item.episode_net_return_60m is not None:
        gross, fee, slippage, funding = components
        assert gross is not None
        assert fee is not None
        assert slippage is not None
        assert funding is not None
        reconstructed = gross - fee - slippage + funding
        if not math.isclose(
            reconstructed,
            item.episode_net_return_60m,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError(f"{identity} has an inconsistent F60 cost decomposition")


def _record_index(
    variant: str, records: Sequence[R2Opportunity]
) -> dict[str, R2Opportunity]:
    indexed: dict[str, R2Opportunity] = {}
    for item in records:
        if not isinstance(item, R2Opportunity):
            raise ValueError(f"{variant} contains a non-R2Opportunity record")
        _validate_record(variant, item)
        if item.opportunity_id in indexed:
            raise ValueError(
                f"{variant} contains duplicate opportunity_id {item.opportunity_id}"
            )
        indexed[item.opportunity_id] = item
    return indexed


def align_r2_opportunities(
    c0_records: Sequence[R2Opportunity],
    h1_records: Sequence[R2Opportunity],
) -> tuple[AlignedR2Opportunity, ...]:
    """Strictly align C0 and H1 on one immutable raw-trigger panel."""

    c0 = _record_index("C0", c0_records)
    h1 = _record_index("H1", h1_records)
    if not c0:
        raise ValueError("C0 contains no opportunity IDs")
    if set(c0) != set(h1):
        raise ValueError(
            "H1 opportunity_id set differs from C0; every frozen C0 trigger "
            "must appear exactly once"
        )
    output: list[AlignedR2Opportunity] = []
    for opportunity_id in sorted(c0):
        base = c0[opportunity_id]
        candidate = h1[opportunity_id]
        base_identity = (
            base.asset.upper(),
            base.market,
            base.direction,
            base.decision_time_ms,
            base.analysis_eligible,
            base.next_open_time_ms,
        )
        candidate_identity = (
            candidate.asset.upper(),
            candidate.market,
            candidate.direction,
            candidate.decision_time_ms,
            candidate.analysis_eligible,
            candidate.next_open_time_ms,
        )
        if candidate_identity != base_identity:
            raise ValueError(f"opportunity identity mismatch for {opportunity_id}")
        base_outcome = (
            base.episode_net_return_60m,
            base.gross_return_60m,
            base.fee_return_60m,
            base.slippage_return_60m,
            base.funding_return_60m,
        )
        candidate_outcome = (
            candidate.episode_net_return_60m,
            candidate.gross_return_60m,
            candidate.fee_return_60m,
            candidate.slippage_return_60m,
            candidate.funding_return_60m,
        )
        if candidate_outcome != base_outcome:
            raise ValueError(f"F60 outcome mismatch for {opportunity_id}")
        output.append(AlignedR2Opportunity(base, candidate))
    return tuple(
        sorted(
            output,
            key=lambda item: (item.c0.decision_time_ms, item.c0.opportunity_id),
        )
    )


def _validate_trade(variant: str, item: R2TechnicalTrade) -> None:
    identity = item.opportunity_id or (
        f"{item.asset}:{item.market}:{item.direction}:{item.decision_time_ms}"
    )
    if not item.asset:
        raise ValueError(f"{variant}:{identity} has an empty asset")
    if (item.market, item.direction) not in _SIDES:
        raise ValueError(
            f"{variant}:{identity} has unsupported side {item.market}-{item.direction}"
        )
    if item.decision_time_ms < 0 or item.entry_time_ms < 0 or item.exit_time_ms < 0:
        raise ValueError(f"{variant}:{identity} has a negative timestamp")
    if not item.entry_time_ms < item.exit_time_ms:
        raise ValueError(f"{variant}:{identity} must exit after entry")
    if not isinstance(item.split_contained, bool):
        raise ValueError(f"{variant}:{identity} has non-boolean split containment")
    if item.bars_held is None or not 0 <= item.bars_held <= 72:
        raise ValueError(f"{variant}:{identity} must hold between 0 and 72 bars")
    for field, value in (
        ("technical_net_return", item.technical_net_return),
        ("gross_return", item.gross_return),
        ("fee_return", item.fee_return),
        ("slippage_return", item.slippage_return),
        ("funding_return", item.funding_return),
    ):
        if value is not None and not math.isfinite(value):
            raise ValueError(f"{variant}:{identity} has non-finite {field}")
    if item.fee_return is not None and item.fee_return < 0:
        raise ValueError(f"{variant}:{identity} has a negative fee cost")
    if item.slippage_return is not None and item.slippage_return < 0:
        raise ValueError(f"{variant}:{identity} has a negative slippage cost")
    components = (
        item.gross_return,
        item.fee_return,
        item.slippage_return,
        item.funding_return,
    )
    if all(value is not None for value in components):
        gross, fee, slippage, funding = components
        assert gross is not None
        assert fee is not None
        assert slippage is not None
        assert funding is not None
        expected = gross - fee - slippage + funding
        if not math.isclose(
            expected,
            item.technical_net_return,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            raise ValueError(
                f"{variant}:{identity} has an inconsistent technical cost decomposition"
            )


def _map_technical_trades(
    variant: str,
    trades: Sequence[R2TechnicalTrade],
    aligned: Sequence[AlignedR2Opportunity],
) -> tuple[_MappedTechnicalTrade, ...]:
    by_id = {item.c0.opportunity_id: item for item in aligned}
    by_tuple: dict[tuple[str, str, str, int], list[AlignedR2Opportunity]] = {}
    for item in aligned:
        key = (
            item.c0.asset.upper(),
            item.c0.market,
            item.c0.direction,
            item.c0.decision_time_ms,
        )
        by_tuple.setdefault(key, []).append(item)

    mapped: list[_MappedTechnicalTrade] = []
    seen_opportunities: set[str] = set()
    for trade in trades:
        if not isinstance(trade, R2TechnicalTrade):
            raise ValueError(f"{variant} contains a non-R2TechnicalTrade record")
        _validate_trade(variant, trade)
        if not trade.split_contained:
            raise ValueError(f"{variant} contains a split-crossing technical trade")
        if trade.opportunity_id:
            opportunity = by_id.get(trade.opportunity_id)
            if opportunity is None:
                raise ValueError(
                    f"{variant} trade maps to unknown opportunity_id {trade.opportunity_id}"
                )
        else:
            key = (
                trade.asset.upper(),
                trade.market,
                trade.direction,
                trade.decision_time_ms,
            )
            matches = by_tuple.get(key, [])
            if len(matches) != 1:
                raise ValueError(
                    f"{variant} trade tuple must map to exactly one opportunity: {key}"
                )
            opportunity = matches[0]
        expected = (
            opportunity.c0.asset.upper(),
            opportunity.c0.market,
            opportunity.c0.direction,
            opportunity.c0.decision_time_ms,
        )
        actual = (
            trade.asset.upper(),
            trade.market,
            trade.direction,
            trade.decision_time_ms,
        )
        if actual != expected:
            raise ValueError(
                f"{variant} trade identity mismatch for {opportunity.c0.opportunity_id}"
            )
        if not opportunity.c0.analysis_eligible:
            raise ValueError(
                f"{variant} trade maps to an analysis-ineligible opportunity "
                f"{opportunity.c0.opportunity_id}"
            )
        if trade.entry_time_ms != opportunity.c0.next_open_time_ms:
            raise ValueError(
                f"{variant} trade does not enter at the frozen next open for "
                f"{opportunity.c0.opportunity_id}"
            )
        if variant == "H1" and not opportunity.h1.h1_accepted:
            raise ValueError(
                f"H1 trade maps to a rejected opportunity {opportunity.c0.opportunity_id}"
            )
        opportunity_id = opportunity.c0.opportunity_id
        if opportunity_id in seen_opportunities:
            raise ValueError(
                f"{variant} contains multiple technical trades for {opportunity_id}"
            )
        seen_opportunities.add(opportunity_id)
        mapped.append(_MappedTechnicalTrade(opportunity, trade))
    return tuple(
        sorted(
            mapped,
            key=lambda item: (
                item.opportunity.c0.decision_time_ms,
                item.opportunity.c0.opportunity_id,
            ),
        )
    )


def _quantile(sorted_values: Sequence[float], probability: float) -> float:
    if not sorted_values:
        raise ValueError("cannot take a quantile of an empty sample")
    bounded = min(1.0, max(0.0, probability))
    index = bounded * (len(sorted_values) - 1)
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return sorted_values[lower]
    weight = index - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def one_sided_basic_lower_bound(
    point_estimate: float,
    bootstrap_estimates: Sequence[float],
    *,
    alpha: float,
) -> float:
    """Return ``2*point - q_(1-alpha)(bootstrap)`` for a one-sided bound."""

    if not 0 < alpha < 1:
        raise ValueError("alpha must lie strictly between zero and one")
    values = sorted(bootstrap_estimates)
    if not values:
        raise ValueError("bootstrap estimates must not be empty")
    if any(not math.isfinite(value) for value in values):
        raise ValueError("bootstrap estimates must be finite")
    return 2 * point_estimate - _quantile(values, 1 - alpha)


def pro_one_sided_p_value(
    point_estimate: float, bootstrap_estimates: Sequence[float]
) -> float:
    """Apply the pre-registered Pro centered-error one-sided p-value formula."""

    values = tuple(bootstrap_estimates)
    if not values:
        raise ValueError("bootstrap estimates must not be empty")
    if not math.isfinite(point_estimate) or any(
        not math.isfinite(value) for value in values
    ):
        raise ValueError("point and bootstrap estimates must be finite")
    if point_estimate <= 0:
        return 1.0
    exceedances = sum(
        value - point_estimate >= point_estimate for value in values
    )
    return (1 + exceedances) / (len(values) + 1)


def circular_moving_block_indices(
    day_count: int,
    block_days: int,
    starts: Sequence[int],
) -> tuple[int, ...]:
    """Expand explicit circular block starts into exactly ``day_count`` indices."""

    if day_count <= 0 or block_days <= 0:
        raise ValueError("day_count and block_days must be positive")
    full_blocks, remainder = divmod(day_count, block_days)
    lengths = [block_days] * full_blocks
    if remainder:
        lengths.append(remainder)
    if len(starts) != len(lengths):
        raise ValueError(f"starts must contain exactly {len(lengths)} block starts")
    if any(start < 0 or start >= day_count for start in starts):
        raise ValueError("every block start must be a valid day index")
    return tuple(
        (start + offset) % day_count
        for start, length in zip(starts, lengths, strict=True)
        for offset in range(length)
    )


def holm_step_down(
    p_values: Mapping[str, float], *, alpha: float = 0.05
) -> dict[str, dict[str, float | int | bool]]:
    """Return deterministic Holm adjusted p-values and sequential decisions."""

    if not p_values:
        raise ValueError("p_values must not be empty")
    if not 0 < alpha < 1:
        raise ValueError("alpha must lie strictly between zero and one")
    for name, value in p_values.items():
        if not name or not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError("Holm hypothesis names and p-values must be valid")

    ordered = sorted(p_values.items(), key=lambda item: (item[1], item[0]))
    count = len(ordered)
    running_adjusted = 0.0
    rejection_chain_open = True
    ranked: dict[str, dict[str, float | int | bool]] = {}
    for zero_rank, (name, p_value) in enumerate(ordered):
        rank = zero_rank + 1
        remaining = count - zero_rank
        local_alpha = alpha / remaining
        rejected = rejection_chain_open and p_value <= local_alpha
        if not rejected:
            rejection_chain_open = False
        running_adjusted = max(running_adjusted, min(1.0, remaining * p_value))
        ranked[name] = {
            "rank": rank,
            "raw_p_value": p_value,
            "adjusted_p_value": running_adjusted,
            "local_alpha": local_alpha,
            "rejected": rejected,
        }
    return {name: ranked[name] for name in sorted(ranked)}


def _block_lengths(day_count: int, block_days: int) -> tuple[int, ...]:
    full_blocks, remainder = divmod(day_count, block_days)
    values = [block_days] * full_blocks
    if remainder:
        values.append(remainder)
    return tuple(values)


def _circular_window_sums(values: Sequence[float], length: int) -> tuple[float, ...]:
    if not values or length <= 0 or length > len(values):
        raise ValueError("circular window length must fit the values")
    doubled = tuple(values) + tuple(values[: length - 1])
    running = sum(doubled[:length])
    output = [running]
    for start in range(1, len(values)):
        running += doubled[start + length - 1] - doubled[start - 1]
        output.append(running)
    return tuple(output)


def _bootstrap_sides(
    daily: Mapping[tuple[str, str], Sequence[_SideDay]],
    *,
    samples: int,
    block_days: int,
    seed: int,
) -> dict[tuple[str, str], _BootstrapSide]:
    day_count = len(next(iter(daily.values())))
    lengths = _block_lengths(day_count, block_days)
    unique_lengths = set(lengths)
    metrics: dict[tuple[str, str, str, int], tuple[float, ...]] = {}
    for side, values in daily.items():
        series = {
            "common_count": [float(item.common_count) for item in values],
            "c0_return_sum": [item.c0_return_sum for item in values],
            "accepted_count": [float(item.accepted_count) for item in values],
            "accepted_return_sum": [item.accepted_return_sum for item in values],
            "technical_count": [float(item.technical_count) for item in values],
            "technical_return_sum": [item.technical_return_sum for item in values],
            "technical_uplift_sum": [item.technical_uplift_sum for item in values],
        }
        for name, metric_values in series.items():
            for length in unique_lengths:
                metrics[(*side, name, length)] = _circular_window_sums(
                    metric_values, length
                )

    conditional: dict[tuple[str, str], list[float]] = {side: [] for side in _SIDES}
    uplift: dict[tuple[str, str], list[float]] = {side: [] for side in _SIDES}
    technical: dict[tuple[str, str], list[float]] = {side: [] for side in _SIDES}
    technical_uplift: dict[tuple[str, str], list[float]] = {
        side: [] for side in _SIDES
    }
    entry_invalid = {side: 0 for side in _SIDES}
    exit_invalid = {side: 0 for side in _SIDES}
    rng = random.Random(seed)
    for _ in range(samples):
        starts = [rng.randrange(day_count) for _ in lengths]
        for side in _SIDES:
            totals: dict[str, float] = {}
            for name in (
                "common_count",
                "c0_return_sum",
                "accepted_count",
                "accepted_return_sum",
                "technical_count",
                "technical_return_sum",
                "technical_uplift_sum",
            ):
                totals[name] = sum(
                    metrics[(*side, name, length)][start]
                    for start, length in zip(starts, lengths, strict=True)
                )
            if totals["common_count"] <= 0 or totals["accepted_count"] <= 0:
                entry_invalid[side] += 1
            else:
                c0_mean = totals["c0_return_sum"] / totals["common_count"]
                h1_policy = totals["accepted_return_sum"] / totals["common_count"]
                conditional[side].append(
                    totals["accepted_return_sum"] / totals["accepted_count"]
                )
                uplift[side].append(h1_policy - c0_mean)
            if totals["technical_count"] <= 0:
                exit_invalid[side] += 1
            else:
                technical[side].append(
                    totals["technical_return_sum"] / totals["technical_count"]
                )
                technical_uplift[side].append(
                    totals["technical_uplift_sum"] / totals["technical_count"]
                )
    return {
        side: _BootstrapSide(
            conditional_mean=tuple(conditional[side]),
            policy_uplift=tuple(uplift[side]),
            technical_mean=tuple(technical[side]),
            technical_uplift=tuple(technical_uplift[side]),
            entry_invalid_replicates=entry_invalid[side],
            exit_invalid_replicates=exit_invalid[side],
        )
        for side in _SIDES
    }


def _profit_factor(values: Sequence[float]) -> tuple[float | None, str]:
    wins = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses > 0:
        return wins / losses, "FINITE"
    if wins > 0:
        return None, "POSITIVE_INFINITY"
    return None, "UNDEFINED"


def _sensitivity(
    rows: Sequence[AlignedR2Opportunity], *, notional_usdt: float
) -> dict[str, Any]:
    if not rows:
        return {"available": False, "missing_component_rows": 0, "scenarios": []}
    missing = sum(item.c0.slippage_return_60m is None for item in rows)
    if missing:
        return {
            "available": False,
            "missing_component_rows": missing,
            "scenarios": [],
        }
    scenarios: list[dict[str, Any]] = []
    for multiplier, label in ((0.0, "0x_slippage"), (2.0, "2x_slippage")):
        c0_returns: list[float] = []
        accepted_returns: list[float] = []
        contribution_sum = 0.0
        for item in rows:
            net = item.c0.episode_net_return_60m
            slippage = item.c0.slippage_return_60m
            assert net is not None and slippage is not None
            adjusted = net + (1 - multiplier) * slippage
            c0_returns.append(adjusted)
            if item.h1.h1_accepted:
                accepted_returns.append(adjusted)
                contribution_sum += adjusted
        common_count = len(rows)
        scenarios.append(
            {
                "scenario": label,
                "c0_policy_mean": statistics.fmean(c0_returns),
                "h1_policy_contribution": contribution_sum / common_count,
                "h1_conditional_mean": (
                    statistics.fmean(accepted_returns) if accepted_returns else None
                ),
                "h1_policy_uplift": contribution_sum / common_count
                - statistics.fmean(c0_returns),
                "h1_fixed_notional_pnl_usdt": sum(accepted_returns) * notional_usdt,
            }
        )
    return {
        "available": True,
        "missing_component_rows": 0,
        "scenarios": scenarios,
    }


def _technical_summary(
    rows: Sequence[_MappedTechnicalTrade],
    *,
    common_count: int,
    notional_usdt: float,
) -> dict[str, Any]:
    technical_values = [item.trade.technical_net_return for item in rows]
    deltas = [
        item.trade.technical_net_return
        - float(item.opportunity.c0.episode_net_return_60m)
        for item in rows
        if item.opportunity.c0.episode_net_return_60m is not None
    ]
    profit_factor, profit_factor_state = _profit_factor(technical_values)
    assets: list[dict[str, Any]] = []
    for asset in sorted({item.opportunity.c0.asset.upper() for item in rows}):
        values = [
            item.trade.technical_net_return
            for item in rows
            if item.opportunity.c0.asset.upper() == asset
        ]
        return_sum = sum(values)
        assets.append(
            {
                "asset": asset,
                "technical_trades": len(values),
                "technical_return_sum": return_sum,
                "technical_mean_contribution": (
                    return_sum / len(rows) if rows else None
                ),
                "fixed_notional_pnl_usdt": return_sum * notional_usdt,
            }
        )
    positive_assets = [item for item in assets if item["technical_return_sum"] > 0]
    total_positive = sum(item["technical_return_sum"] for item in positive_assets)
    concentration = (
        max(item["technical_return_sum"] for item in positive_assets) / total_positive
        if total_positive > 0
        else None
    )

    missing_slippage = sum(
        item.trade.slippage_return is None
        or item.opportunity.c0.slippage_return_60m is None
        for item in rows
    )
    scenarios: list[dict[str, Any]] = []
    if rows and not missing_slippage:
        for multiplier, label in ((0.0, "0x_slippage"), (2.0, "2x_slippage")):
            adjusted_technical: list[float] = []
            adjusted_deltas: list[float] = []
            for item in rows:
                technical_slippage = item.trade.slippage_return
                f60_slippage = item.opportunity.c0.slippage_return_60m
                f60_net = item.opportunity.c0.episode_net_return_60m
                assert technical_slippage is not None
                assert f60_slippage is not None
                assert f60_net is not None
                technical_net = item.trade.technical_net_return + (
                    1 - multiplier
                ) * technical_slippage
                episode_net = f60_net + (1 - multiplier) * f60_slippage
                adjusted_technical.append(technical_net)
                adjusted_deltas.append(technical_net - episode_net)
            scenarios.append(
                {
                    "scenario": label,
                    "technical_mean": statistics.fmean(adjusted_technical),
                    "technical_uplift_vs_f60": statistics.fmean(adjusted_deltas),
                    "fixed_notional_pnl_usdt": sum(adjusted_technical)
                    * notional_usdt,
                }
            )

    return {
        "common_stop_openable_trades": len(rows),
        "coverage_of_common_opportunities": (
            len(rows) / common_count if common_count else 0.0
        ),
        "valid_utc_days": len(
            {item.opportunity.c0.utc_day for item in rows}
        ),
        "technical_mean": (
            statistics.fmean(technical_values) if technical_values else None
        ),
        "technical_uplift_vs_f60": statistics.fmean(deltas) if deltas else None,
        "fixed_notional_pnl_usdt": sum(technical_values) * notional_usdt,
        "profit_factor": profit_factor,
        "profit_factor_state": profit_factor_state,
        "positive_contribution_assets": len(positive_assets),
        "largest_positive_asset_concentration": concentration,
        "asset_contributions": assets,
        "slippage_sensitivity": {
            "available": bool(rows) and missing_slippage == 0,
            "missing_component_rows": missing_slippage,
            "scenarios": scenarios,
        },
    }


def _endpoint_report(
    point: float,
    values: Sequence[float],
    *,
    alpha: float,
) -> dict[str, float | int | None]:
    if not values:
        return {
            "point_estimate": point,
            "valid_replicates": 0,
            "one_sided_p_value": None,
            "one_sided_basic_lower": None,
        }
    return {
        "point_estimate": point,
        "valid_replicates": len(values),
        "one_sided_p_value": pro_one_sided_p_value(point, values),
        "one_sided_basic_lower": one_sided_basic_lower_bound(
            point, values, alpha=alpha
        ),
    }


def _invalid_result(
    reason: str,
    *,
    samples: int,
    seed: int,
    block_days: Sequence[int],
    notional_usdt: float,
) -> dict[str, Any]:
    return {
        "protocol_version": _PROTOCOL_VERSION,
        "status": "INVALID",
        "status_reasons": [reason],
        "full_r2_status": _FULL_R2_STATUS,
        "historical_execution_boundary": {
            "historical_bbo_available": False,
            "status": "NOT_TESTABLE",
            "reason": "historical opportunity data contain no decision-time BBO/depth/receipt-time",
        },
        "configuration": {
            "bootstrap_samples": samples,
            "seed": seed,
            "block_days": list(block_days),
            "notional_usdt": notional_usdt,
        },
        "integrity": {"valid": False, "common_opportunity_ids": 0},
        "entry_hypotheses": [],
        "exit_hypotheses": [],
        "sides": [],
    }


def analyze_r2_retrospective(
    c0_records: Sequence[R2Opportunity] | str | Path,
    h1_records: Sequence[R2Opportunity] | str | Path,
    *,
    c0_trades: Sequence[R2TechnicalTrade] | str | Path | None = None,
    h1_trades: Sequence[R2TechnicalTrade] | str | Path | None = None,
    bootstrap_samples: int = 50_000,
    block_days: Sequence[int] = _BLOCK_DAYS,
    seed: int = 20_260_716,
    notional_usdt: float = 100.0,
    alpha: float = 0.05,
    min_accepted: int = 500,
    min_valid_days: int = 120,
    min_coverage: float = 0.10,
    min_feature_availability: float = 0.99,
    max_invalid_replicate_rate: float = 0.001,
    min_conditional_mean: float = 0.0005,
    min_profit_factor: float = 1.05,
    min_positive_assets: int = 6,
    max_positive_asset_concentration: float = 0.35,
) -> dict[str, Any]:
    """Evaluate the pre-registered R2 H1 entry screen on a common C0 panel.

    Integrity failures return ``INVALID`` without emitting partial statistics.
    Insufficient information returns ``INCONCLUSIVE`` rather than a negative
    efficacy claim. Even a retrospective pass leaves ``full_r2_status`` at
    ``INCONCLUSIVE_NO_HISTORICAL_BBO`` because kline-only history cannot test
    the prospective execution candidate.
    """

    block_days_tuple = tuple(block_days)
    try:
        if bootstrap_samples < 100:
            raise ValueError("bootstrap_samples must be at least 100")
        if block_days_tuple != _BLOCK_DAYS:
            raise ValueError(f"block_days must remain frozen at {_BLOCK_DAYS}")
        if not isinstance(seed, int):
            raise ValueError("seed must be an integer")
        if not math.isfinite(notional_usdt) or notional_usdt <= 0:
            raise ValueError("notional_usdt must be positive and finite")
        if not 0 < alpha < 1:
            raise ValueError("alpha must lie strictly between zero and one")
        if min_accepted < 1 or min_valid_days < 1 or min_positive_assets < 1:
            raise ValueError("minimum information thresholds must be positive")
        for name, value in (
            ("min_coverage", min_coverage),
            ("min_feature_availability", min_feature_availability),
            ("max_invalid_replicate_rate", max_invalid_replicate_rate),
            ("max_positive_asset_concentration", max_positive_asset_concentration),
        ):
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f"{name} must lie between zero and one")
        if not math.isfinite(min_conditional_mean) or not math.isfinite(
            min_profit_factor
        ):
            raise ValueError("return and profit-factor thresholds must be finite")
        loaded_c0 = (
            read_r2_opportunities(c0_records)
            if isinstance(c0_records, (str, Path))
            else tuple(c0_records)
        )
        loaded_h1 = (
            read_r2_opportunities(h1_records)
            if isinstance(h1_records, (str, Path))
            else tuple(h1_records)
        )
        aligned = align_r2_opportunities(loaded_c0, loaded_h1)
        loaded_c0_trades = (
            ()
            if c0_trades is None
            else read_r2_technical_trades(c0_trades)
            if isinstance(c0_trades, (str, Path))
            else tuple(c0_trades)
        )
        loaded_h1_trades = (
            ()
            if h1_trades is None
            else read_r2_technical_trades(h1_trades)
            if isinstance(h1_trades, (str, Path))
            else tuple(h1_trades)
        )
        mapped_c0_trades = _map_technical_trades(
            "C0", loaded_c0_trades, aligned
        )
        mapped_h1_trades = _map_technical_trades(
            "H1", loaded_h1_trades, aligned
        )
    except (OSError, TypeError, ValueError) as exc:
        return _invalid_result(
            str(exc),
            samples=bootstrap_samples,
            seed=seed,
            block_days=block_days_tuple,
            notional_usdt=notional_usdt,
        )

    analyzable = tuple(item for item in aligned if item.c0.analysis_eligible)
    if analyzable:
        first_day = min(item.c0.utc_day for item in analyzable)
        last_day = max(item.c0.utc_day for item in analyzable)
        calendar_days = last_day - first_day + 1
    else:
        first_day = 0
        last_day = -1
        calendar_days = 0

    side_rows = {
        side: tuple(
            item
            for item in analyzable
            if (item.c0.market, item.c0.direction) == side
        )
        for side in _SIDES
    }
    daily: dict[tuple[str, str], list[_SideDay]] = {
        side: [_SideDay() for _ in range(calendar_days)] for side in _SIDES
    }
    for side, rows in side_rows.items():
        mutable = [
            [
                item.common_count,
                item.c0_return_sum,
                item.accepted_count,
                item.accepted_return_sum,
                item.technical_count,
                item.technical_return_sum,
                item.technical_uplift_sum,
            ]
            for item in daily[side]
        ]
        for item in rows:
            net = item.c0.episode_net_return_60m
            assert net is not None
            index = item.c0.utc_day - first_day
            mutable[index][0] += 1
            mutable[index][1] += net
            if item.h1.h1_accepted:
                mutable[index][2] += 1
                mutable[index][3] += net
        for mapped_trade in mapped_c0_trades:
            opportunity = mapped_trade.opportunity.c0
            if (opportunity.market, opportunity.direction) != side:
                continue
            episode_return = opportunity.episode_net_return_60m
            assert episode_return is not None
            index = opportunity.utc_day - first_day
            mutable[index][4] += 1
            mutable[index][5] += mapped_trade.trade.technical_net_return
            mutable[index][6] += (
                mapped_trade.trade.technical_net_return - episode_return
            )
        daily[side] = [
            _SideDay(
                int(common),
                c0_sum,
                int(accepted),
                accepted_sum,
                int(technical_count),
                technical_sum,
                technical_uplift_sum,
            )
            for (
                common,
                c0_sum,
                accepted,
                accepted_sum,
                technical_count,
                technical_sum,
                technical_uplift_sum,
            ) in mutable
        ]

    bootstrap_by_block: dict[
        int, dict[tuple[str, str], _BootstrapSide]
    ] = {}
    if calendar_days:
        for days in block_days_tuple:
            bootstrap_by_block[days] = _bootstrap_sides(
                daily,
                samples=bootstrap_samples,
                block_days=days,
                seed=seed,
            )

    side_output: list[dict[str, Any]] = []
    primary_distributions: dict[tuple[str, str], _BootstrapSide | None] = {}
    preliminary: dict[tuple[str, str], dict[str, Any]] = {}
    for side in _SIDES:
        market, direction = side
        rows = side_rows[side]
        common_count = len(rows)
        accepted_rows = [item for item in rows if item.h1.h1_accepted]
        accepted_values = [
            float(item.c0.episode_net_return_60m)
            for item in accepted_rows
            if item.c0.episode_net_return_60m is not None
        ]
        c0_values = [
            float(item.c0.episode_net_return_60m)
            for item in rows
            if item.c0.episode_net_return_60m is not None
        ]
        accepted_count = len(accepted_values)
        c0_mean = statistics.fmean(c0_values) if c0_values else None
        h1_contribution = sum(accepted_values) / common_count if common_count else None
        conditional_mean = (
            statistics.fmean(accepted_values) if accepted_values else None
        )
        policy_uplift = (
            h1_contribution - c0_mean
            if h1_contribution is not None and c0_mean is not None
            else None
        )
        conditional_uplift = (
            conditional_mean - c0_mean
            if conditional_mean is not None and c0_mean is not None
            else None
        )
        coverage = accepted_count / common_count if common_count else 0.0
        feature_availability = (
            sum(item.h1.h1_feature_available for item in rows) / common_count
            if common_count
            else 0.0
        )
        valid_days = len({item.c0.utc_day for item in rows})
        profit_factor, profit_factor_state = _profit_factor(accepted_values)

        assets: list[dict[str, Any]] = []
        for asset in sorted({item.c0.asset.upper() for item in rows}):
            values = [
                float(item.c0.episode_net_return_60m)
                for item in accepted_rows
                if item.c0.asset.upper() == asset
                and item.c0.episode_net_return_60m is not None
            ]
            return_sum = sum(values)
            assets.append(
                {
                    "asset": asset,
                    "accepted_opportunities": len(values),
                    "accepted_return_sum": return_sum,
                    "policy_contribution": (
                        return_sum / common_count if common_count else None
                    ),
                    "fixed_notional_pnl_usdt": return_sum * notional_usdt,
                }
            )
        positive_assets = [item for item in assets if item["accepted_return_sum"] > 0]
        total_positive = sum(item["accepted_return_sum"] for item in positive_assets)
        concentration = (
            max(item["accepted_return_sum"] for item in positive_assets) / total_positive
            if total_positive > 0
            else None
        )
        sensitivity = _sensitivity(rows, notional_usdt=notional_usdt)
        c0_technical_rows = tuple(
            item
            for item in mapped_c0_trades
            if (item.opportunity.c0.market, item.opportunity.c0.direction) == side
        )
        h1_technical_rows = tuple(
            item
            for item in mapped_h1_trades
            if (item.opportunity.c0.market, item.opportunity.c0.direction) == side
        )
        technical_summary = _technical_summary(
            c0_technical_rows,
            common_count=common_count,
            notional_usdt=notional_usdt,
        )
        technical_summary["inferential_role"] = "PRIMARY_C0_COMMON_STOP_OPENABLE_X"
        h1_technical_secondary = _technical_summary(
            h1_technical_rows,
            common_count=accepted_count,
            notional_usdt=notional_usdt,
        )
        h1_technical_secondary["inferential_role"] = (
            "SECONDARY_H1_OWN_OPENABLE_SET_NOT_IN_PRIMARY_HOLM"
        )

        bootstrap_rows: list[dict[str, Any]] = []
        for days in block_days_tuple:
            distribution = (
                bootstrap_by_block.get(days, {}).get(side)
                if calendar_days
                else None
            )
            entry_invalid_count = (
                bootstrap_samples
                if distribution is None
                else distribution.entry_invalid_replicates
            )
            exit_invalid_count = (
                bootstrap_samples
                if distribution is None
                else distribution.exit_invalid_replicates
            )
            conditional_report = (
                _endpoint_report(
                    conditional_mean or 0.0,
                    distribution.conditional_mean,
                    alpha=alpha,
                )
                if distribution is not None
                else _endpoint_report(conditional_mean or 0.0, (), alpha=alpha)
            )
            uplift_report = (
                _endpoint_report(
                    policy_uplift or 0.0,
                    distribution.policy_uplift,
                    alpha=alpha,
                )
                if distribution is not None
                else _endpoint_report(policy_uplift or 0.0, (), alpha=alpha)
            )
            conditional_p = conditional_report["one_sided_p_value"]
            uplift_p = uplift_report["one_sided_p_value"]
            composite_p = (
                max(float(conditional_p), float(uplift_p))
                if conditional_p is not None and uplift_p is not None
                else None
            )
            technical_mean = technical_summary["technical_mean"]
            technical_uplift = technical_summary["technical_uplift_vs_f60"]
            technical_report = (
                _endpoint_report(
                    technical_mean or 0.0,
                    distribution.technical_mean,
                    alpha=alpha,
                )
                if distribution is not None
                else _endpoint_report(technical_mean or 0.0, (), alpha=alpha)
            )
            technical_uplift_report = (
                _endpoint_report(
                    technical_uplift or 0.0,
                    distribution.technical_uplift,
                    alpha=alpha,
                )
                if distribution is not None
                else _endpoint_report(technical_uplift or 0.0, (), alpha=alpha)
            )
            technical_p = technical_report["one_sided_p_value"]
            technical_uplift_p = technical_uplift_report["one_sided_p_value"]
            exit_composite_p = (
                max(float(technical_p), float(technical_uplift_p))
                if technical_p is not None and technical_uplift_p is not None
                else None
            )
            bootstrap_rows.append(
                {
                    "block_days": days,
                    "calendar_days": calendar_days,
                    "blocks_per_replicate": (
                        len(_block_lengths(calendar_days, days))
                        if calendar_days
                        else 0
                    ),
                    "samples": bootstrap_samples,
                    "seed": seed,
                    "entry_invalid_replicates": entry_invalid_count,
                    "entry_invalid_replicate_rate": entry_invalid_count
                    / bootstrap_samples,
                    "exit_invalid_replicates": exit_invalid_count,
                    "exit_invalid_replicate_rate": exit_invalid_count
                    / bootstrap_samples,
                    "conditional_mean_endpoint": conditional_report,
                    "policy_uplift_endpoint": uplift_report,
                    "entry_intersection_union_p_value": composite_p,
                    "technical_mean_endpoint": technical_report,
                    "technical_uplift_endpoint": technical_uplift_report,
                    "exit_intersection_union_p_value": exit_composite_p,
                }
            )
            if days == _PRIMARY_BLOCK_DAYS:
                primary_distributions[side] = distribution

        preliminary[side] = {
            "market": market,
            "direction": direction,
            "common_opportunities": common_count,
            "accepted_opportunities": accepted_count,
            "coverage": coverage,
            "valid_utc_days": valid_days,
            "h1_feature_availability": feature_availability,
            "c0_policy_mean": c0_mean,
            "h1_policy_contribution": h1_contribution,
            "h1_conditional_mean": conditional_mean,
            "h1_conditional_uplift_vs_c0": conditional_uplift,
            "h1_policy_uplift_vs_c0": policy_uplift,
            "h1_fixed_notional_pnl_usdt": sum(accepted_values) * notional_usdt,
            "profit_factor": profit_factor,
            "profit_factor_state": profit_factor_state,
            "positive_contribution_assets": len(positive_assets),
            "largest_positive_asset_concentration": concentration,
            "asset_contributions": assets,
            "slippage_sensitivity": sensitivity,
            "c0_technical_exit": technical_summary,
            "h1_technical_exit_secondary": h1_technical_secondary,
            "bootstrap": bootstrap_rows,
        }

    primary_p_values: dict[str, float] = {}
    for side in _SIDES:
        row = preliminary[side]
        primary = next(
            item
            for item in row["bootstrap"]
            if item["block_days"] == _PRIMARY_BLOCK_DAYS
        )
        for family, field in (
            ("entry", "entry_intersection_union_p_value"),
            ("exit", "exit_intersection_union_p_value"),
        ):
            composite = primary[field]
            key = f"{family}:{side[0]}:{side[1]}"
            primary_p_values[key] = 1.0 if composite is None else float(composite)
    holm = holm_step_down(primary_p_values, alpha=alpha)

    entry_hypotheses: list[dict[str, Any]] = []
    exit_hypotheses: list[dict[str, Any]] = []
    overall_reasons: list[str] = []
    statuses: list[R2Status] = []
    for side in _SIDES:
        row = preliminary[side]
        distribution = primary_distributions.get(side)
        primary_bootstrap = next(
            item
            for item in row["bootstrap"]
            if item["block_days"] == _PRIMARY_BLOCK_DAYS
        )

        entry_key = f"entry:{side[0]}:{side[1]}"
        entry_holm = holm[entry_key]
        conditional_mean = row["h1_conditional_mean"]
        policy_uplift = row["h1_policy_uplift_vs_c0"]
        if (
            distribution is not None
            and conditional_mean is not None
            and policy_uplift is not None
            and distribution.conditional_mean
            and distribution.policy_uplift
        ):
            entry_alpha = float(entry_holm["local_alpha"])
            conditional_holm_lower = one_sided_basic_lower_bound(
                conditional_mean,
                distribution.conditional_mean,
                alpha=entry_alpha,
            )
            policy_holm_lower = one_sided_basic_lower_bound(
                policy_uplift,
                distribution.policy_uplift,
                alpha=entry_alpha,
            )
        else:
            conditional_holm_lower = None
            policy_holm_lower = None

        entry_information_failures: list[str] = []
        if row["accepted_opportunities"] < min_accepted:
            entry_information_failures.append(
                f"accepted opportunities {row['accepted_opportunities']} < {min_accepted}"
            )
        if row["valid_utc_days"] < min_valid_days:
            entry_information_failures.append(
                f"valid UTC days {row['valid_utc_days']} < {min_valid_days}"
            )
        if row["coverage"] < min_coverage:
            entry_information_failures.append(
                f"coverage {row['coverage']:.12g} < {min_coverage}"
            )
        if row["h1_feature_availability"] < min_feature_availability:
            entry_information_failures.append(
                "H1 feature availability "
                f"{row['h1_feature_availability']:.12g} < {min_feature_availability}"
            )
        if (
            primary_bootstrap["entry_invalid_replicate_rate"]
            > max_invalid_replicate_rate
        ):
            entry_information_failures.append(
                "entry invalid bootstrap replicate rate "
                f"{primary_bootstrap['entry_invalid_replicate_rate']:.12g} > "
                f"{max_invalid_replicate_rate}"
            )
        if not row["slippage_sensitivity"]["available"]:
            entry_information_failures.append("F60 slippage components are incomplete")

        entry_two_x = next(
            (
                item
                for item in row["slippage_sensitivity"]["scenarios"]
                if item["scenario"] == "2x_slippage"
            ),
            None,
        )
        entry_pf_passed = (
            row["profit_factor_state"] == "POSITIVE_INFINITY"
            or (
                row["profit_factor"] is not None
                and row["profit_factor"] > min_profit_factor
            )
        )
        entry_checks = {
            "holm_intersection_union": bool(entry_holm["rejected"])
            and conditional_holm_lower is not None
            and conditional_holm_lower > 0
            and policy_holm_lower is not None
            and policy_holm_lower > 0,
            "conditional_mean_at_least_5bp": conditional_mean is not None
            and conditional_mean >= min_conditional_mean,
            "fixed_notional_pnl_positive": row["h1_fixed_notional_pnl_usdt"] > 0,
            "profit_factor_above_threshold": entry_pf_passed,
            "two_x_slippage_nonnegative": entry_two_x is not None
            and entry_two_x["h1_conditional_mean"] is not None
            and entry_two_x["h1_conditional_mean"] >= 0,
            "positive_asset_count": row["positive_contribution_assets"]
            >= min_positive_assets,
            "positive_asset_concentration": row[
                "largest_positive_asset_concentration"
            ]
            is not None
            and row["largest_positive_asset_concentration"]
            <= max_positive_asset_concentration,
        }
        if entry_information_failures:
            entry_status: R2Status = "INCONCLUSIVE"
            entry_reasons = entry_information_failures
        else:
            failed = [name for name, passed in entry_checks.items() if not passed]
            if failed:
                entry_status = "FAIL"
                entry_reasons = [
                    f"failed frozen entry efficacy check: {name}" for name in failed
                ]
            else:
                entry_status = "RETROSPECTIVE_SCREEN_PASS"
                entry_reasons = []
        entry_hypothesis = {
            "hypothesis": entry_key,
            "market": side[0],
            "direction": side[1],
            "endpoint_p_values": {
                "h1_conditional_mean": primary_bootstrap[
                    "conditional_mean_endpoint"
                ]["one_sided_p_value"],
                "h1_policy_uplift": primary_bootstrap["policy_uplift_endpoint"][
                    "one_sided_p_value"
                ],
            },
            "intersection_union_p_value": primary_p_values[entry_key],
            "holm": entry_holm,
            "holm_one_sided_basic_lower": {
                "h1_conditional_mean": conditional_holm_lower,
                "h1_policy_uplift": policy_holm_lower,
            },
            "efficacy_checks": entry_checks,
            "information_failures": entry_information_failures,
            "status": entry_status,
            "status_reasons": entry_reasons,
        }
        entry_hypotheses.append(entry_hypothesis)

        exit_key = f"exit:{side[0]}:{side[1]}"
        exit_holm = holm[exit_key]
        technical_summary = row["c0_technical_exit"]
        technical_mean = technical_summary["technical_mean"]
        technical_uplift = technical_summary["technical_uplift_vs_f60"]
        if (
            distribution is not None
            and technical_mean is not None
            and technical_uplift is not None
            and distribution.technical_mean
            and distribution.technical_uplift
        ):
            exit_alpha = float(exit_holm["local_alpha"])
            technical_holm_lower = one_sided_basic_lower_bound(
                technical_mean,
                distribution.technical_mean,
                alpha=exit_alpha,
            )
            technical_uplift_holm_lower = one_sided_basic_lower_bound(
                technical_uplift,
                distribution.technical_uplift,
                alpha=exit_alpha,
            )
        else:
            technical_holm_lower = None
            technical_uplift_holm_lower = None

        exit_information_failures: list[str] = []
        if technical_summary["common_stop_openable_trades"] < min_accepted:
            exit_information_failures.append(
                "common stop-openable trades "
                f"{technical_summary['common_stop_openable_trades']} < {min_accepted}"
            )
        if technical_summary["valid_utc_days"] < min_valid_days:
            exit_information_failures.append(
                "technical valid UTC days "
                f"{technical_summary['valid_utc_days']} < {min_valid_days}"
            )
        if (
            primary_bootstrap["exit_invalid_replicate_rate"]
            > max_invalid_replicate_rate
        ):
            exit_information_failures.append(
                "exit invalid bootstrap replicate rate "
                f"{primary_bootstrap['exit_invalid_replicate_rate']:.12g} > "
                f"{max_invalid_replicate_rate}"
            )
        if not technical_summary["slippage_sensitivity"]["available"]:
            exit_information_failures.append(
                "technical/F60 slippage components are incomplete"
            )

        exit_two_x = next(
            (
                item
                for item in technical_summary["slippage_sensitivity"]["scenarios"]
                if item["scenario"] == "2x_slippage"
            ),
            None,
        )
        exit_pf_passed = (
            technical_summary["profit_factor_state"] == "POSITIVE_INFINITY"
            or (
                technical_summary["profit_factor"] is not None
                and technical_summary["profit_factor"] > min_profit_factor
            )
        )
        exit_checks = {
            "holm_intersection_union": bool(exit_holm["rejected"])
            and technical_holm_lower is not None
            and technical_holm_lower > 0
            and technical_uplift_holm_lower is not None
            and technical_uplift_holm_lower > 0,
            "technical_mean_at_least_5bp": technical_mean is not None
            and technical_mean >= min_conditional_mean,
            "fixed_notional_pnl_positive": technical_summary[
                "fixed_notional_pnl_usdt"
            ]
            > 0,
            "profit_factor_above_threshold": exit_pf_passed,
            "two_x_slippage_nonnegative": exit_two_x is not None
            and exit_two_x["technical_mean"] >= 0,
            "positive_asset_count": technical_summary[
                "positive_contribution_assets"
            ]
            >= min_positive_assets,
            "positive_asset_concentration": technical_summary[
                "largest_positive_asset_concentration"
            ]
            is not None
            and technical_summary["largest_positive_asset_concentration"]
            <= max_positive_asset_concentration,
        }
        if exit_information_failures:
            exit_status: R2Status = "INCONCLUSIVE"
            exit_reasons = exit_information_failures
        else:
            failed = [name for name, passed in exit_checks.items() if not passed]
            if failed:
                exit_status = "FAIL"
                exit_reasons = [
                    f"failed frozen exit efficacy check: {name}" for name in failed
                ]
            else:
                exit_status = "RETROSPECTIVE_SCREEN_PASS"
                exit_reasons = []
        exit_hypothesis = {
            "hypothesis": exit_key,
            "market": side[0],
            "direction": side[1],
            "endpoint_p_values": {
                "technical_mean": primary_bootstrap["technical_mean_endpoint"][
                    "one_sided_p_value"
                ],
                "technical_uplift_vs_f60": primary_bootstrap[
                    "technical_uplift_endpoint"
                ]["one_sided_p_value"],
            },
            "intersection_union_p_value": primary_p_values[exit_key],
            "holm": exit_holm,
            "holm_one_sided_basic_lower": {
                "technical_mean": technical_holm_lower,
                "technical_uplift_vs_f60": technical_uplift_holm_lower,
            },
            "efficacy_checks": exit_checks,
            "information_failures": exit_information_failures,
            "status": exit_status,
            "status_reasons": exit_reasons,
        }
        exit_hypotheses.append(exit_hypothesis)

        statuses.extend((entry_status, exit_status))
        overall_reasons.extend(
            f"{entry_key}: {reason}" for reason in entry_reasons
        )
        overall_reasons.extend(f"{exit_key}: {reason}" for reason in exit_reasons)
        if "FAIL" in {entry_status, exit_status}:
            side_status: R2Status = "FAIL"
        elif "INCONCLUSIVE" in {entry_status, exit_status}:
            side_status = "INCONCLUSIVE"
        else:
            side_status = "RETROSPECTIVE_SCREEN_PASS"
        row["status"] = side_status
        row["entry_hypothesis"] = entry_hypothesis
        row["exit_hypothesis"] = exit_hypothesis
        side_output.append(row)

    if any(status == "FAIL" for status in statuses):
        overall_status: R2Status = "FAIL"
    elif any(status == "INCONCLUSIVE" for status in statuses):
        overall_status = "INCONCLUSIVE"
    else:
        overall_status = "RETROSPECTIVE_SCREEN_PASS"

    return {
        "protocol_version": _PROTOCOL_VERSION,
        "status": overall_status,
        "status_reasons": overall_reasons,
        "full_r2_status": _FULL_R2_STATUS,
        "historical_execution_boundary": {
            "historical_bbo_available": False,
            "status": "NOT_TESTABLE",
            "reason": "historical opportunity data contain no decision-time BBO/depth/receipt-time",
        },
        "configuration": {
            "bootstrap_method": "circular moving UTC-day block bootstrap",
            "bootstrap_samples": bootstrap_samples,
            "seed": seed,
            "block_days": list(block_days_tuple),
            "primary_block_days": _PRIMARY_BLOCK_DAYS,
            "one_sided_alpha": alpha,
            "notional_usdt": notional_usdt,
            "p_value_formula": (
                "point<=0 => 1; else (1 + count[(theta_star-theta_hat)>=theta_hat])/(B+1)"
            ),
            "lower_bound_method": "one-sided basic centered-bootstrap bound",
            "multiple_testing": (
                "intersection-union within hypothesis; Holm step-down across "
                "entry/exit x spot/futures (four composites)"
            ),
            "thresholds": {
                "min_accepted": min_accepted,
                "min_valid_days": min_valid_days,
                "min_coverage": min_coverage,
                "min_feature_availability": min_feature_availability,
                "max_invalid_replicate_rate": max_invalid_replicate_rate,
                "min_conditional_mean": min_conditional_mean,
                "min_profit_factor": min_profit_factor,
                "min_positive_assets": min_positive_assets,
                "max_positive_asset_concentration": max_positive_asset_concentration,
            },
        },
        "integrity": {
            "valid": True,
            "c0_input_ids": len(loaded_c0),
            "h1_input_ids": len(loaded_h1),
            "c0_technical_trade_rows": len(loaded_c0_trades),
            "h1_technical_trade_rows": len(loaded_h1_trades),
            "common_opportunity_ids": len(aligned),
            "analysis_eligible_ids": len(analyzable),
            "excluded_ineligible_ids": len(aligned) - len(analyzable),
            "calendar_start_day": first_day if calendar_days else None,
            "calendar_end_day": last_day if calendar_days else None,
            "calendar_days": calendar_days,
        },
        "entry_hypotheses": entry_hypotheses,
        "exit_hypotheses": exit_hypotheses,
        "sides": side_output,
    }
