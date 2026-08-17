from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

import signalbot.capture.config as capture_config_module
from signalbot.capture.config import (
    CANARY_SYMBOLS,
    FROZEN_PROTOCOL_SHA256,
    FUTURES_DEPTH_SNAPSHOT_LIMIT,
    SPOT_DEPTH_SNAPSHOT_LIMIT,
    SPOT_DEPTH_SNAPSHOT_MINIMUM_ADMISSION_INTERVAL_SECONDS,
    SPOT_DEPTH_SNAPSHOT_REQUEST_WEIGHT,
    SPOT_EXCHANGE_INFO_SYMBOLS_QUERY,
    SPOT_REQUEST_WEIGHT_LIMIT_PER_MINUTE,
    SPOT_USED_WEIGHT_QUARANTINE_THRESHOLD,
    CanaryRestRequestPlanEntry,
    CaptureCanaryConfig,
    capture_rest_request_plan,
    capture_route_registry,
    load_capture_canary_config,
    validate_capture_route_registry,
)
from signalbot.capture.models import (
    PUBLIC_REST_PATHS_BY_MARKET,
    is_allowed_rest_response_header,
)
from signalbot.capture.provenance import (
    ExternalAuditRecordV1,
    build_capture_source_manifest,
    canonical_json_bytes,
    canonical_sha256,
    detect_repository_metadata,
    validate_external_audit_roots,
    write_external_audit_record,
)
from signalbot.domain.enums import Market

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config/capture.r4b-canary-v1.yaml"
PROTOCOL = ROOT / "artifacts/oracle/2026-07-17/R4b_frozen_experiment_spec_v1.yaml"


def test_frozen_canary_configuration_and_public_routes_load() -> None:
    settings = load_capture_canary_config(CONFIG, protocol_file=PROTOCOL)

    assert settings.purpose == "infrastructure_only"
    assert settings.efficacy_outputs_enabled is False
    assert settings.order_execution_enabled is False
    assert settings.api_credentials_enabled is False
    assert settings.symbols == CANARY_SYMBOLS
    assert settings.duration_seconds == 86_400
    assert settings.websocket.maximum_connection_age_seconds == 86_100
    assert settings.websocket.reconnect_delays_seconds == (1, 2, 4, 8, 16, 30, 30, 30)
    assert settings.handoff.maximum_events == 100_000
    assert settings.handoff.maximum_encoded_bytes == 256 * 1024 * 1024
    assert settings.storage.maximum_total_bytes == 100 * 1024**3
    assert settings.storage.emergency_reserve_bytes == 512 * 1024**2
    assert settings.rest.maximum_concurrency == 4
    assert settings.rest.maximum_body_bytes == 16 * 1024**2
    assert settings.rest.maximum_attempts == 2
    assert settings.rest.retry_delays_seconds == (1.0,)
    assert settings.rest.follow_redirects is False
    assert settings.rest.trust_environment is False
    assert settings.polling.venue_time_seconds == 30
    assert settings.polling.exchange_info_seconds == 60
    assert settings.polling.exchange_info_hash_on_change is True
    assert SPOT_REQUEST_WEIGHT_LIMIT_PER_MINUTE == 6_000
    assert SPOT_USED_WEIGHT_QUARANTINE_THRESHOLD == (
        SPOT_REQUEST_WEIGHT_LIMIT_PER_MINUTE
        - settings.rest.maximum_concurrency * SPOT_DEPTH_SNAPSHOT_REQUEST_WEIGHT
    )
    assert SPOT_DEPTH_SNAPSHOT_MINIMUM_ADMISSION_INTERVAL_SECONDS == 3.2
    assert settings.polling.spot_depth_snapshot_limit == SPOT_DEPTH_SNAPSHOT_LIMIT
    assert settings.polling.futures_depth_snapshot_limit == FUTURES_DEPTH_SNAPSHOT_LIMIT
    assert settings.polling.depth_snapshot_bridge_maximum_attempts == 3
    assert settings.polling.depth_snapshot_bridge_wait_seconds == 2
    assert settings.polling.depth_snapshot_triggers == (
        "startup",
        "reconnect",
        "sequence_gap",
    )
    assert settings.polling.futures_open_interest_seconds == 5
    assert settings.polling.futures_open_interest_history_period == "5m"
    assert settings.polling.futures_open_interest_history_delay_seconds == 15
    assert settings.polling.futures_open_interest_history_data_role == "cross_check_non_primary"
    assert settings.polling.futures_premium_index_seconds == 30
    assert settings.polling.futures_premium_index_data_role == "cross_check_non_primary"
    assert settings.polling.futures_funding_rate_delay_seconds == 15
    assert settings.polling.futures_funding_rate_maximum_attempts == 2
    assert settings.polling.futures_funding_info_seconds == 300
    assert settings.polling.futures_funding_info_on_exchange_info_change is True

    routes = capture_route_registry()
    validate_capture_route_registry()
    transport = routes["transport_public_allowlist"]
    assert isinstance(transport, dict)
    spot = transport["spot"]
    futures = transport["futures"]
    assert isinstance(spot, dict)
    assert isinstance(futures, dict)
    assert spot["rest_base"] == "https://data-api.binance.vision"
    assert "api.binance.com" not in str(spot["rest_base"])
    assert str(spot["websocket_base"]).startswith("wss://data-stream.binance.vision")
    assert "/market/" in str(futures["websocket_market_base"])
    assert "/public/" in str(futures["websocket_public_base"])
    assert spot["rest_paths"] == list(PUBLIC_REST_PATHS_BY_MARKET[Market.SPOT])
    assert futures["rest_paths"] == list(PUBLIC_REST_PATHS_BY_MARKET[Market.FUTURES])
    assert "/fapi/v1/fundingInfo" in futures["rest_paths"]
    assert is_allowed_rest_response_header("content-encoding")
    assert "private" not in canonical_json_bytes(routes).decode("utf-8").casefold()


def test_frozen_rest_request_plan_is_exact_bounded_and_separate_from_transport() -> None:
    plan = capture_rest_request_plan()
    by_role = {entry.role: entry for entry in plan}

    assert len(plan) == 11
    assert len(by_role) == len(plan)
    assert all(entry.fixed_request_headers == (("accept-encoding", "identity"),) for entry in plan)
    assert by_role["spot_venue_time"].interval_seconds == 30
    assert by_role["futures_venue_time"].interval_seconds == 30
    assert by_role["spot_exchange_info"].interval_seconds == 60
    assert by_role["spot_exchange_info"].hash_on_change is True
    assert SPOT_EXCHANGE_INFO_SYMBOLS_QUERY == json.dumps(
        CANARY_SYMBOLS,
        separators=(",", ":"),
    )
    assert by_role["spot_exchange_info"].fixed_query == (
        ("symbols", SPOT_EXCHANGE_INFO_SYMBOLS_QUERY),
    )
    assert by_role["spot_exchange_info"].allowed_query_keys == ("symbols",)
    assert by_role["spot_exchange_info"].allowed_query_values[0].values == (
        '["BTCUSDT","ETHUSDT","SOLUSDT"]',
    )
    assert by_role["futures_exchange_info"].interval_seconds == 60
    assert by_role["futures_exchange_info"].hash_on_change is True

    expected_depth_limits = {
        "spot_depth_snapshot": SPOT_DEPTH_SNAPSHOT_LIMIT,
        "futures_depth_snapshot": FUTURES_DEPTH_SNAPSHOT_LIMIT,
    }
    for role, limit in expected_depth_limits.items():
        entry = by_role[role]
        assert entry.fixed_query == (("limit", str(limit)),)
        assert entry.maximum_query_limit == limit
        assert entry.allowed_query_values[0].values == (str(limit),)
        assert entry.trigger == "depth_resync_event_only"
        assert entry.trigger_events == ("startup", "reconnect", "sequence_gap")
        assert entry.interval_seconds is None

    assert by_role["futures_open_interest"].interval_seconds == 5
    history = by_role["futures_open_interest_history"]
    assert history.fixed_query == (("limit", "1"), ("period", "5m"))
    assert history.trigger == "utc_bar_close"
    assert history.interval_seconds == 300
    assert history.delay_seconds == 15
    assert history.data_role == "cross_check_non_primary"
    premium = by_role["futures_premium_index"]
    assert premium.interval_seconds == 30
    assert premium.data_role == "cross_check_non_primary"
    funding = by_role["futures_funding_rate_confirmation"]
    assert funding.trigger == "next_funding_time"
    assert funding.delay_seconds == 15
    assert funding.maximum_attempts == 2
    funding_info = by_role["futures_funding_info"]
    assert funding_info.path == "/fapi/v1/fundingInfo"
    assert funding_info.interval_seconds == 300
    assert funding_info.trigger_events == ("exchange_info_hash_change",)

    registry = capture_route_registry()
    serialized = registry["frozen_canary_rest_request_plan"]
    assert serialized == [entry.model_dump(mode="json") for entry in plan]
    planned_paths = {entry.path for entry in plan}
    transport_paths = {path for paths in PUBLIC_REST_PATHS_BY_MARKET.values() for path in paths}
    assert planned_paths < transport_paths
    assert all(entry.path in PUBLIC_REST_PATHS_BY_MARKET[entry.market] for entry in plan)


def test_configuration_rejects_extra_fields_and_nonfrozen_boundaries(tmp_path: Path) -> None:
    raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    raw["unexpected"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        CaptureCanaryConfig.model_validate(raw)

    raw.pop("unexpected")
    raw["duration_seconds"] = 86_399
    with pytest.raises(ValidationError, match="Input should be 86400"):
        CaptureCanaryConfig.model_validate(raw)

    raw["duration_seconds"] = 86_400
    raw["symbols"] = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    with pytest.raises(ValidationError, match="symbols must be exactly"):
        CaptureCanaryConfig.model_validate(raw)

    raw["symbols"] = list(CANARY_SYMBOLS)
    assert isinstance(raw["websocket"], dict)
    raw["websocket"]["reconnect_delays_seconds"] = [1, 2, 4, 8, 16, 30, 30, 29]
    with pytest.raises(ValidationError, match="frozen canary"):
        CaptureCanaryConfig.model_validate(raw)

    invalid_config = tmp_path / "invalid.yaml"
    invalid_config.write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    with pytest.raises(ValueError, match="root must be a mapping"):
        load_capture_canary_config(invalid_config, protocol_file=PROTOCOL)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        pytest.param("spot_depth_snapshot_limit", 4_999, id="spot-below-maximum"),
        pytest.param("spot_depth_snapshot_limit", 5_001, id="spot-above-maximum"),
        pytest.param("futures_depth_snapshot_limit", 999, id="futures-below-maximum"),
        pytest.param("futures_depth_snapshot_limit", 1_001, id="futures-above-maximum"),
        pytest.param(
            "depth_snapshot_bridge_maximum_attempts",
            2,
            id="bootstrap-cap-below-frozen",
        ),
        pytest.param(
            "depth_snapshot_bridge_maximum_attempts",
            4,
            id="bootstrap-cap-above-frozen",
        ),
        pytest.param(
            "depth_snapshot_bridge_wait_seconds",
            1,
            id="bootstrap-wait-below-frozen",
        ),
        pytest.param(
            "depth_snapshot_bridge_wait_seconds",
            3,
            id="bootstrap-wait-above-frozen",
        ),
    ],
)
def test_configuration_rejects_nonfrozen_venue_depth_boundaries(
    field: str,
    invalid_value: int,
) -> None:
    raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    polling = raw["polling"]
    assert isinstance(polling, dict)
    polling[field] = invalid_value

    with pytest.raises(ValidationError):
        CaptureCanaryConfig.model_validate(raw)


def test_configuration_rejects_legacy_common_depth_limit() -> None:
    raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    polling = raw["polling"]
    assert isinstance(polling, dict)
    polling["depth_snapshot_limit"] = 1_000

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        CaptureCanaryConfig.model_validate(raw)


@pytest.mark.parametrize(
    ("role", "wrong_limit"),
    [
        pytest.param("spot_depth_snapshot", 1_000, id="spot-common-1000"),
        pytest.param("futures_depth_snapshot", 5_000, id="futures-spot-5000"),
    ],
)
def test_request_plan_rejects_internally_consistent_wrong_venue_depth_limit(
    role: str,
    wrong_limit: int,
) -> None:
    entry = next(item for item in capture_rest_request_plan() if item.role == role)
    payload = entry.model_dump(mode="python")
    payload["fixed_query"] = (("limit", str(wrong_limit)),)
    values = list(payload["allowed_query_values"])
    values[0] = {"key": "limit", "values": (str(wrong_limit),)}
    payload["allowed_query_values"] = tuple(values)
    payload["maximum_query_limit"] = wrong_limit

    with pytest.raises(ValidationError, match="venue-specific contract"):
        CanaryRestRequestPlanEntry.model_validate(payload)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        pytest.param("symbols", '["BTCUSDT","ETHUSDT"]', id="missing-symbol"),
        pytest.param(
            "symbols",
            '["ETHUSDT","BTCUSDT","SOLUSDT"]',
            id="reordered-symbols",
        ),
        pytest.param("symbol", "BTCUSDT", id="singular-symbol-query"),
    ],
)
def test_spot_exchange_info_rejects_consistent_query_scope_drift(
    key: str,
    value: str,
) -> None:
    entry = next(
        item for item in capture_rest_request_plan() if item.role == "spot_exchange_info"
    )
    payload = entry.model_dump(mode="python")
    payload["fixed_query"] = ((key, value),)
    payload["allowed_query_keys"] = (key,)
    payload["allowed_query_values"] = ({"key": key, "values": (value,)},)

    with pytest.raises(ValidationError, match="exact canary-symbol contract"):
        CanaryRestRequestPlanEntry.model_validate(payload)


@pytest.mark.parametrize(
    ("path", "invalid_value"),
    [
        pytest.param(("duration_seconds",), 86_400.0, id="float-as-integer"),
        pytest.param(("efficacy_outputs_enabled",), 0, id="integer-as-boolean"),
        pytest.param(
            ("polling", "futures_funding_rate_maximum_attempts"),
            2.0,
            id="nested-float-as-integer",
        ),
        pytest.param(
            ("polling", "exchange_info_hash_on_change"),
            1,
            id="nested-integer-as-boolean",
        ),
    ],
)
def test_configuration_rejects_cross_type_scalars(
    path: tuple[str, ...],
    invalid_value: object,
) -> None:
    raw = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    target = raw
    for key in path[:-1]:
        child = target[key]
        assert isinstance(child, dict)
        target = child
    target[path[-1]] = invalid_value

    with pytest.raises(ValidationError):
        CaptureCanaryConfig.model_validate(raw)


def test_configuration_rejects_protocol_byte_change(tmp_path: Path) -> None:
    changed_protocol = tmp_path / "protocol.yaml"
    changed_protocol.write_bytes(PROTOCOL.read_bytes() + b"\n# changed\n")

    with pytest.raises(ValueError, match="protocol_file SHA-256"):
        load_capture_canary_config(CONFIG, protocol_file=changed_protocol)


def test_route_registry_drift_is_rejected_and_changes_canonical_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = capture_route_registry()
    changed = copy.deepcopy(routes)
    transport = changed["transport_public_allowlist"]
    assert isinstance(transport, dict)
    spot = transport["spot"]
    assert isinstance(spot, dict)
    spot["rest_base"] = "https://data-api.binance.vision.invalid"
    assert canonical_sha256(routes) != canonical_sha256(changed)
    manifest = build_capture_source_manifest(
        ROOT,
        protocol_file=PROTOCOL,
        config_file=CONFIG,
    )
    changed_manifest = copy.deepcopy(manifest.document)
    registries = changed_manifest["registries"]
    assert isinstance(registries, dict)
    registries["routes"] = changed
    assert canonical_sha256(changed_manifest) != manifest.sha256

    monkeypatch.setattr(
        capture_config_module,
        "SPOT_MARKET_DATA_REST_BASE",
        "https://api.binance.com",
    )
    with pytest.raises(ValueError, match="public-only routes"):
        validate_capture_route_registry()


def test_source_manifest_is_deterministic_and_covers_required_authority() -> None:
    first = build_capture_source_manifest(
        ROOT,
        protocol_file=PROTOCOL,
        config_file=CONFIG,
    )
    second = build_capture_source_manifest(
        ROOT,
        protocol_file=PROTOCOL,
        config_file=CONFIG,
    )

    assert first.canonical_bytes == second.canonical_bytes
    assert first.sha256 == second.sha256
    assert canonical_sha256(first.document) == first.sha256
    assert first.document["purpose"] == "infrastructure_only"
    entries = first.document["files"]
    assert isinstance(entries, list)
    paths = {str(entry["path"]) for entry in entries if isinstance(entry, dict)}
    assert "pyproject.toml" in paths
    assert "uv.lock" in paths
    assert "src/signalbot/capture/config.py" in paths
    assert "src/signalbot/capture/canary_report.py" in paths
    assert "src/signalbot/capture/clock_health_report.py" in paths
    assert "src/signalbot/capture/depth_sequence.py" in paths
    assert "src/signalbot/capture/live.py" in paths
    assert "src/signalbot/capture/provenance.py" in paths
    assert "config/capture.r4b-canary-v1.yaml" in paths
    assert "artifacts/oracle/2026-07-17/R4b_frozen_experiment_spec_v1.yaml" in paths
    assert isinstance(first.document["runtime"], dict)
    assert isinstance(first.document["dependencies"], list)
    repository = first.document["repository"]
    assert isinstance(repository, dict)
    assert "git_head" in repository
    assert "state" in repository


def test_source_and_configuration_byte_changes_change_manifest_hash(tmp_path: Path) -> None:
    workspace = _minimal_workspace(tmp_path)
    protocol = workspace / "protocol.yaml"
    config = workspace / "config.yaml"
    source = workspace / "src/example.py"
    baseline = build_capture_source_manifest(
        workspace,
        protocol_file=protocol,
        config_file=config,
    )

    source.write_text("VALUE = 2\n", encoding="utf-8")
    source_changed = build_capture_source_manifest(
        workspace,
        protocol_file=protocol,
        config_file=config,
    )
    assert source_changed.sha256 != baseline.sha256

    source.write_text("VALUE = 1\n", encoding="utf-8")
    config.write_bytes(config.read_bytes() + b"\n# byte-only audit change\n")
    config_changed = build_capture_source_manifest(
        workspace,
        protocol_file=protocol,
        config_file=config,
    )
    assert config_changed.sha256 != baseline.sha256


def test_source_manifest_does_not_dump_environment_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = _minimal_workspace(tmp_path)
    secret = "DO_NOT_PERSIST_THIS_CAPTURE_SECRET"
    monkeypatch.setenv("BINANCE_API_SECRET", secret)
    artifact = build_capture_source_manifest(
        workspace,
        protocol_file=workspace / "protocol.yaml",
        config_file=workspace / "config.yaml",
    )

    assert secret.encode() not in artifact.canonical_bytes
    assert b"BINANCE_API_SECRET" not in artifact.canonical_bytes
    assert "environment" not in artifact.document


def test_null_git_head_is_a_valid_repository_manifest_state(tmp_path: Path) -> None:
    metadata = detect_repository_metadata(tmp_path)
    assert metadata["git_head"] is None
    assert metadata["state"] == "NOT_A_GIT_REPOSITORY"


def test_external_start_and_closure_are_write_once_and_fsynced_files(tmp_path: Path) -> None:
    output = tmp_path / "capture-output"
    external = tmp_path / "audit-heads"
    output.mkdir()
    external.mkdir()
    start = _audit_record("start")
    start_write = write_external_audit_record(
        start,
        external_root=external,
        output_root=output,
    )
    assert start_write.path.is_file()
    stored = json.loads(start_write.path.read_text(encoding="utf-8"))
    assert stored["trust_classification"] == "SEPARATE_PATH_AUDIT_ONLY"
    assert stored["previous_record_sha256"] is None

    with pytest.raises(FileExistsError):
        write_external_audit_record(
            start,
            external_root=external,
            output_root=output,
        )

    closure = _audit_record("closure", previous_record_sha256=start_write.sha256)
    closure_write = write_external_audit_record(
        closure,
        external_root=external,
        output_root=output,
    )
    assert closure_write.path.is_file()
    assert closure_write.path != start_write.path


@pytest.mark.parametrize("relation", ["same", "external_inside_output", "output_inside_external"])
def test_external_and_output_roots_must_be_distinct_and_non_nested(
    tmp_path: Path,
    relation: str,
) -> None:
    if relation == "same":
        output = tmp_path / "same"
        output.mkdir()
        external = output
    elif relation == "external_inside_output":
        output = tmp_path / "output"
        external = output / "heads"
        external.mkdir(parents=True)
    else:
        external = tmp_path / "heads"
        external.mkdir()
        output = external / "output"

    with pytest.raises(ValueError, match="distinct, non-nested"):
        validate_external_audit_roots(external, output)


def test_symlinked_source_and_external_root_are_rejected_when_supported(
    tmp_path: Path,
) -> None:
    workspace = _minimal_workspace(tmp_path)
    real_config = workspace / "config.yaml"
    linked_config = workspace / "linked-config.yaml"
    external = tmp_path / "real-heads"
    external.mkdir()
    linked_external = tmp_path / "linked-heads"
    try:
        linked_config.symlink_to(real_config)
        linked_external.symlink_to(external, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symbolic links are unavailable: {exc}")

    with pytest.raises(ValueError, match="symbolic-link"):
        build_capture_source_manifest(
            workspace,
            protocol_file=workspace / "protocol.yaml",
            config_file=linked_config,
        )
    with pytest.raises(ValueError, match="symbolic-link"):
        validate_external_audit_roots(linked_external, tmp_path / "output")


def test_audit_record_phase_and_hash_boundaries() -> None:
    with pytest.raises(ValidationError, match="cannot have a previous"):
        _audit_record("start", previous_record_sha256="d" * 64)
    with pytest.raises(ValidationError, match="must bind the start"):
        _audit_record("closure")
    with pytest.raises(ValidationError, match="safe for a write-once filename"):
        _audit_record("start", session_id="../escape")


def test_config_manifest_and_audit_record_expose_no_financial_result_keys() -> None:
    settings = load_capture_canary_config(CONFIG, protocol_file=PROTOCOL)
    manifest = build_capture_source_manifest(
        ROOT,
        protocol_file=PROTOCOL,
        config_file=CONFIG,
    )
    record = _audit_record("start")
    keys = {
        *_all_keys(settings.model_dump(mode="json")),
        *_all_keys(manifest.document),
        *_all_keys(record.model_dump(mode="json")),
    }
    assert keys.isdisjoint({"pnl", "outcome", "return", "label", "threshold", "signal"})


def test_reparse_attribute_is_rejected_independently_of_platform() -> None:
    import os
    import stat
    from types import SimpleNamespace
    from typing import cast

    from signalbot.capture.path_safety import (
        FILE_ATTRIBUTE_REPARSE_POINT,
        is_link_or_reparse_status,
    )

    reparse_status = cast(
        os.stat_result,
        SimpleNamespace(
            st_mode=stat.S_IFDIR | 0o755,
            st_file_attributes=FILE_ATTRIBUTE_REPARSE_POINT,
        ),
    )
    ordinary_status = cast(
        os.stat_result,
        SimpleNamespace(st_mode=stat.S_IFDIR | 0o755, st_file_attributes=0),
    )

    assert is_link_or_reparse_status(reparse_status) is True
    assert is_link_or_reparse_status(ordinary_status) is False


def test_path_inspection_error_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import signalbot.capture.path_safety as path_safety

    def deny_inspection(_path: object) -> object:
        raise PermissionError("inspection denied")

    monkeypatch.setattr(path_safety.os, "lstat", deny_inspection)
    with pytest.raises(ValueError, match="path inspection failed"):
        path_safety.inspect_link_free_path(tmp_path, "test_path")


def test_broken_symlink_component_is_rejected_when_supported(tmp_path: Path) -> None:
    broken = tmp_path / "broken-audit-root"
    try:
        broken.symlink_to(tmp_path / "missing-target", target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symbolic links are unavailable: {exc}")

    with pytest.raises(ValueError, match="symbolic-link or reparse-point"):
        validate_external_audit_roots(broken, tmp_path / "output")


def test_windows_junction_component_is_rejected_when_available(tmp_path: Path) -> None:
    import os
    import subprocess

    if os.name != "nt":
        pytest.skip("Windows junctions are Windows-only")
    target = tmp_path / "junction-target"
    junction = tmp_path / "junction-audit-root"
    target.mkdir()
    result = subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(junction), str(target)],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0 or not junction.exists():
        pytest.skip(f"junction creation is unavailable: {result.stderr.strip()}")
    try:
        with pytest.raises(ValueError, match="reparse-point"):
            validate_external_audit_roots(junction, tmp_path / "output")
    finally:
        junction.rmdir()


def test_source_manifest_rejects_empty_python_source(tmp_path: Path) -> None:
    workspace = _minimal_workspace(tmp_path)
    for source in (workspace / "src").rglob("*.py"):
        source.unlink()

    with pytest.raises(ValueError, match="at least one Python file"):
        build_capture_source_manifest(
            workspace,
            protocol_file=workspace / "protocol.yaml",
            config_file=workspace / "config.yaml",
        )


def test_source_manifest_requires_exact_capture_entrypoints(tmp_path: Path) -> None:
    workspace = _minimal_workspace(tmp_path)
    required = workspace / "src/signalbot/capture/__init__.py"
    required.unlink()

    with pytest.raises(ValueError, match=r"src/signalbot/capture/__init__\.py"):
        build_capture_source_manifest(
            workspace,
            protocol_file=workspace / "protocol.yaml",
            config_file=workspace / "config.yaml",
        )


def test_source_manifest_requires_clock_report_entrypoint(tmp_path: Path) -> None:
    workspace = _minimal_workspace(tmp_path)
    required = workspace / "src/signalbot/capture/clock_health_report.py"
    required.unlink()

    with pytest.raises(
        ValueError,
        match=r"src/signalbot/capture/clock_health_report\.py",
    ):
        build_capture_source_manifest(
            workspace,
            protocol_file=workspace / "protocol.yaml",
            config_file=workspace / "config.yaml",
        )


def test_clock_report_byte_change_changes_source_manifest_hash(tmp_path: Path) -> None:
    workspace = _minimal_workspace(tmp_path)
    protocol = workspace / "protocol.yaml"
    config = workspace / "config.yaml"
    source = workspace / "src/signalbot/capture/clock_health_report.py"
    baseline = build_capture_source_manifest(
        workspace,
        protocol_file=protocol,
        config_file=config,
    )

    source.write_bytes(source.read_bytes() + b"# clock contract changed\n")
    changed = build_capture_source_manifest(
        workspace,
        protocol_file=protocol,
        config_file=config,
    )

    assert changed.sha256 != baseline.sha256


def test_source_snapshot_rejects_in_flight_stat_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os

    import signalbot.capture.provenance as provenance_module

    workspace = _minimal_workspace(tmp_path)
    real_fstat = provenance_module.os.fstat
    call_count = 0

    def changing_fstat(descriptor: int) -> os.stat_result:
        nonlocal call_count
        status = real_fstat(descriptor)
        call_count += 1
        if call_count == 2:
            values = list(status)
            values[6] = status.st_size + 1
            return os.stat_result(values)
        return status

    monkeypatch.setattr(provenance_module.os, "fstat", changing_fstat)
    with pytest.raises(ValueError, match="changed identity, size, or mtime"):
        build_capture_source_manifest(
            workspace,
            protocol_file=workspace / "protocol.yaml",
            config_file=workspace / "config.yaml",
        )


def test_manifest_opens_config_and_protocol_once_from_the_hashed_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os

    import signalbot.capture.provenance as provenance_module

    workspace = _minimal_workspace(tmp_path)
    targets = {
        (workspace / "config.yaml").resolve(),
        (workspace / "protocol.yaml").resolve(),
    }
    counts = {target: 0 for target in targets}
    real_open = provenance_module.os.open

    def tracking_open(
        path: str | os.PathLike[str],
        flags: int,
        mode: int = 0o777,
    ) -> int:
        candidate = Path(path).resolve()
        if candidate in counts:
            counts[candidate] += 1
        return real_open(path, flags, mode)

    monkeypatch.setattr(provenance_module.os, "open", tracking_open)
    build_capture_source_manifest(
        workspace,
        protocol_file=workspace / "protocol.yaml",
        config_file=workspace / "config.yaml",
    )

    assert counts == {target: 1 for target in targets}


def test_closure_audit_rejects_orphan_start(tmp_path: Path) -> None:
    output, external = _audit_roots(tmp_path)
    closure = _audit_record("closure", previous_record_sha256="d" * 64)

    with pytest.raises(ValueError, match="external start audit record"):
        write_external_audit_record(
            closure,
            external_root=external,
            output_root=output,
        )
    assert not (external / "session-0001.closure.audit-head.json").exists()


def test_closure_audit_rejects_actual_start_hash_mismatch(tmp_path: Path) -> None:
    output, external = _audit_roots(tmp_path)
    write_external_audit_record(
        _audit_record("start"),
        external_root=external,
        output_root=output,
    )
    closure = _audit_record("closure", previous_record_sha256="c" * 64)

    with pytest.raises(ValueError, match="actual start"):
        write_external_audit_record(
            closure,
            external_root=external,
            output_root=output,
        )


def test_closure_audit_rejects_authority_mismatch_and_time_reversal(
    tmp_path: Path,
) -> None:
    output, external = _audit_roots(tmp_path)
    start_write = write_external_audit_record(
        _audit_record("start"),
        external_root=external,
        output_root=output,
    )
    authority_mismatch = _audit_record(
        "closure",
        previous_record_sha256=start_write.sha256,
        source_manifest_sha256="c" * 64,
    )
    with pytest.raises(ValueError, match="authority differs"):
        write_external_audit_record(
            authority_mismatch,
            external_root=external,
            output_root=output,
        )

    time_reversal = _audit_record(
        "closure",
        previous_record_sha256=start_write.sha256,
        recorded_at_ms=1_720_999_999_999,
    )
    with pytest.raises(ValueError, match="time precedes"):
        write_external_audit_record(
            time_reversal,
            external_root=external,
            output_root=output,
        )


def test_closure_audit_rejects_noncanonical_start(tmp_path: Path) -> None:
    import hashlib

    output, external = _audit_roots(tmp_path)
    start = _audit_record("start")
    noncanonical = (
        json.dumps(
            start.model_dump(mode="json"),
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )
    start_path = external / "session-0001.start.audit-head.json"
    start_path.write_bytes(noncanonical)
    closure = _audit_record(
        "closure",
        previous_record_sha256=hashlib.sha256(noncanonical).hexdigest(),
    )

    with pytest.raises(ValueError, match="not canonical"):
        write_external_audit_record(
            closure,
            external_root=external,
            output_root=output,
        )


def test_closure_audit_rejects_linked_start_when_supported(tmp_path: Path) -> None:
    import hashlib

    output, external = _audit_roots(tmp_path)
    canonical = canonical_json_bytes(_audit_record("start").model_dump(mode="json")) + b"\n"
    target = tmp_path / "start-target.json"
    target.write_bytes(canonical)
    linked_start = external / "session-0001.start.audit-head.json"
    try:
        linked_start.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symbolic links are unavailable: {exc}")
    closure = _audit_record(
        "closure",
        previous_record_sha256=hashlib.sha256(canonical).hexdigest(),
    )

    with pytest.raises(ValueError, match="symbolic-link or reparse-point"):
        write_external_audit_record(
            closure,
            external_root=external,
            output_root=output,
        )


def _minimal_workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    source = workspace / "src/example.py"
    source.parent.mkdir(parents=True)
    source.write_text("VALUE = 1\n", encoding="utf-8")
    capture_root = workspace / "src/signalbot/capture"
    capture_root.mkdir(parents=True)
    for filename in (
        "__init__.py",
        "canary_report.py",
        "cli.py",
        "clock_health_report.py",
        "closed_evidence.py",
        "config.py",
        "depth_coverage_report.py",
        "depth_sequence.py",
        "errors.py",
        "handoff.py",
        "live.py",
        "local_book.py",
        "models.py",
        "pipeline.py",
        "plans.py",
        "path_safety.py",
        "provenance.py",
        "receipts.py",
        "rest.py",
        "rest_scheduler.py",
        "schema_registry.py",
        "session.py",
        "storage.py",
        "websocket.py",
        "ws_owner.py",
    ):
        (capture_root / filename).write_text(
            f"# test entrypoint: {filename}\n",
            encoding="utf-8",
        )
    shutil.copyfile(PROTOCOL, workspace / "protocol.yaml")
    shutil.copyfile(CONFIG, workspace / "config.yaml")
    (workspace / "pyproject.toml").write_text("[project]\nname='test'\n", encoding="utf-8")
    (workspace / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    return workspace


def _audit_record(
    phase: str,
    *,
    previous_record_sha256: str | None = None,
    session_id: str = "session-0001",
    source_manifest_sha256: str = "a" * 64,
    recorded_at_ms: int = 1_721_000_000_000,
) -> ExternalAuditRecordV1:
    return ExternalAuditRecordV1.model_validate(
        {
            "schema_version": "capture_external_audit_record_v1",
            "purpose": "infrastructure_only",
            "trust_classification": "SEPARATE_PATH_AUDIT_ONLY",
            "phase": phase,
            "session_id": session_id,
            "recorded_at_ms": recorded_at_ms,
            "protocol_sha256": FROZEN_PROTOCOL_SHA256,
            "source_manifest_sha256": source_manifest_sha256,
            "subject_sha256": "b" * 64,
            "previous_record_sha256": previous_record_sha256,
        }
    )


def _audit_roots(tmp_path: Path) -> tuple[Path, Path]:
    output = tmp_path / "capture-output"
    external = tmp_path / "audit-heads"
    output.mkdir()
    external.mkdir()
    return output, external


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return {
            *(str(key).casefold() for key in value),
            *(key for item in value.values() for key in _all_keys(item)),
        }
    if isinstance(value, list | tuple):
        return {key for item in value for key in _all_keys(item)}
    return set()
