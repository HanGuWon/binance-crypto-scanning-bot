from __future__ import annotations

import hashlib
import os
import shutil
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from signalbot.backtest import d1_scefb_historical_attempt_wal as attempt_wal
from signalbot.backtest import d1_scefb_historical_development as development
from signalbot.backtest import d1_scefb_historical_operator as subject

_RECORDED_D1_INPUT_SHA256_V0: dict[str, tuple[str, str, str]] = {
    "BTCUSDT": (
        "51495fd4ffc163cd3b801b6981eeb07719216950d1c69687b0ed190c9bae5e46",
        "065b1485f1c651955f10ed3fc772e5fe2373273317cc73e9c4121a10436225ff",
        "0ef6996baf929ddcc5462f95950ca4f12221ea2f54b5a80ac4d21028a41194cf",
    ),
    "ETHUSDT": (
        "18655396921527037f48e6c1bb38d14e75d08d4f755e4aa6d16e6c38db45c20b",
        "adf38583f799767e0529eeaa1cfc8607e583fe27711f536360251b3c2bb9d3f3",
        "e928706d2ddc7e897fe766f051fec57f31aa2ee270573822221e295647e47f54",
    ),
    "BNBUSDT": (
        "bb0181f03a4f47d3d6f35837c483fdbe343cd69b331edbef064ee802d79117f7",
        "e02e0ac41e0e7320cbe828857f3b5c9b147a99fee233edb6fb3f66e25532b575",
        "66adad63de0f903ecf22b4d596132e501550b090152bf54aeb1b44f673ac052c",
    ),
    "SOLUSDT": (
        "7b430d47903156cabdacf3dc78d11604604df62326d1f729bd8834aed5e589ae",
        "956eaefb596283998b5765604be633026f1a08dfe72d5242bbf9dd7cdaf0f720",
        "1857be550bbd809c5bb45d337ef3b9d9c6654de72c327ed39c9e259f55fb290a",
    ),
    "XRPUSDT": (
        "a7215a22243ea942fcc19e57355ad2dd62e56f823e4c55fd6517afc1558396d7",
        "fc22aefefe4f0e7d98038631d16187d7bd7193b8cafb0132977f0004d1cf02d1",
        "9330164595f20151628c41aa0942eeb4604882654cc778b317faec758ca4db20",
    ),
    "DOGEUSDT": (
        "177a8f253632160097dbf7dd6cfe4f833f94ef402eb7e2410d3476311d133c8a",
        "efc7f8d4bd1f45f8eafbf6418cbe96d5ac9b5fdef4e47f6d85f8b3d477eb364a",
        "a6999e961b58b169fb66167905355f704e0ccff8f6c6469ee209df8d48ceaf1b",
    ),
    "ARBUSDT": (
        "7e1bd0d34a224f1ca212b8840b83bbfa1e6b1bb18f6366b3d73acf9d1c581022",
        "b6832f296b93303b626d0f218c821800f4b3d8668b33f10ffb34a91d469049a8",
        "90311d0b5439e79461aa30cd38e273e75db5d095939905439af612802231f265",
    ),
    "OPUSDT": (
        "e3d92525490a781fc52463caabcfd897dff63b5b8d1a6a27258f26f6fe941c5e",
        "9d3e9a1891089dfd13fa1b3446f61c170eb01a23457bba3539194d20732abe8e",
        "484d602e2891e7c01abbd652f9fd80694c42798e98921ae8209a0390fe750696",
    ),
    "SUIUSDT": (
        "c15e2b54f17b7e9cdd0e4e6dbd7ab4852838c51ef2943d38080a55eadc39b8bd",
        "9ecd16409502b3582b9ea605969cd55b0a916aa85c2ff15a2234b37c0defbca1",
        "09afc36b26274eda74c2560ecd405482e2d4960531809e86695dc10b59a881e4",
    ),
    "WIFUSDT": (
        "4536fe5bd694ab93283ee59270f5d5712cb3c67d2a7e351197d2776f2d7bbbd8",
        "885e80311daa9841fbde5d8935c469d4268f6c84030d7d21bcbe40ae9c23f303",
        "b7786b32a3a1cabd868c38472c9d713f7d0006c8fe2fb7feaeef06b376522f69",
    ),
}


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def test_recorded_outcome_blind_metadata_matches_literal_operator_pins() -> None:
    assert tuple(_RECORDED_D1_INPUT_SHA256_V0) == development.D1_HISTORICAL_UNIVERSE_V0

    funding_bindings: list[development.D1HistoricalFundingFileBindingV0] = []
    kline_bindings: list[development.D1HistoricalKlineManifestBindingV0] = []
    for symbol in development.D1_HISTORICAL_UNIVERSE_V0:
        alias = development.D1_HISTORICAL_ALIAS_BY_SYMBOL_V0[symbol]
        funding_sha256, five_minute_manifest_sha256, hourly_manifest_sha256 = (
            _RECORDED_D1_INPUT_SHA256_V0[symbol]
        )
        funding_path = subject._funding_relative_path(symbol)
        assert funding_path == (
            f"data/backtest/funding/{alias}__{symbol}__5m.csv.gz"
        )
        funding_bindings.append(
            development.D1HistoricalFundingFileBindingV0(
                symbol=symbol,
                relative_path=funding_path,
                sha256=funding_sha256,
            )
        )
        kline_bindings.extend(
            (
                development.D1HistoricalKlineManifestBindingV0(
                    symbol=symbol,
                    interval="5m",
                    relative_manifest_path=subject._kline_manifest_relative_path(
                        symbol, "5m"
                    ),
                    manifest_sha256=five_minute_manifest_sha256,
                ),
                development.D1HistoricalKlineManifestBindingV0(
                    symbol=symbol,
                    interval="1h",
                    relative_manifest_path=subject._kline_manifest_relative_path(
                        symbol, "1h"
                    ),
                    manifest_sha256=hourly_manifest_sha256,
                ),
            )
        )

    funding_raw = development.canonical_d1_historical_funding_authority_manifest_v0(
        tuple(funding_bindings)
    )
    assert len(funding_raw) == 1_729
    assert _sha(funding_raw) == subject.D1_OPERATOR_EXPECTED_FUNDING_AUTHORITY_FILE_SHA256_V0

    authority = development.build_d1_historical_input_authority_v0(
        kline_manifests=tuple(kline_bindings),
        funding_manifest_relative_path=(
            f"{subject.D1_OPERATOR_INPUT_AUTHORITY_DIR_V0}/"
            f"{subject.D1_OPERATOR_FUNDING_AUTHORITY_FILE_V0}"
        ),
        funding_manifest_sha256=_sha(funding_raw),
    )
    authority_raw = development.canonical_d1_historical_input_authority_v0(authority)
    assert authority.authority_sha256 == subject.D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SHA256_V0
    assert _sha(authority_raw) == subject.D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_FILE_SHA256_V0
    assert len(authority_raw) == subject.D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SIZE_BYTES_V0


def test_successor_freeze_policy_is_canonical_and_run_001_stays_retired() -> None:
    include_files = development.D1_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0
    assert include_files == tuple(sorted(set(include_files)))
    assert (
        development.D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_RELATIVE_PATH_V0
        in include_files
    )
    assert subject.D1_OPERATOR_FREEZE_MANIFEST_V0.endswith(
        "d1-scefb-v0-development-freeze-002/freeze_manifest.json"
    )
    assert subject.D1_OPERATOR_ATTEMPT_DIR_V0.endswith(
        "d1-scefb-v0-development-run-002-attempt"
    )
    assert subject.D1_OPERATOR_OUTPUT_DIR_V0.endswith(
        "d1-scefb-v0-development-run-002"
    )
    assert subject.D1_OPERATOR_RUN_ID_V0 == "d1-scefb-v0-development-run-002"


def _configure_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> development.D1HistoricalInputAuthorityV0:
    monkeypatch.setattr(subject, "D1_OPERATOR_INPUT_AUTHORITY_DIR_V0", "authority")
    monkeypatch.setattr(
        subject,
        "D1_OPERATOR_FREEZE_MANIFEST_V0",
        "freeze/freeze_manifest.json",
    )
    monkeypatch.setattr(subject, "D1_OPERATOR_ATTEMPT_DIR_V0", "attempt")
    monkeypatch.setattr(subject, "D1_OPERATOR_OUTPUT_DIR_V0", "output")
    monkeypatch.setattr(subject, "D1_OPERATOR_PREREGISTRATION_FILE_V0", "preregistered.md")

    preregistration = b"fixed preregistration\n"
    (tmp_path / "preregistered.md").write_bytes(preregistration)
    monkeypatch.setattr(
        subject,
        "D1_OPERATOR_EXPECTED_PREREGISTRATION_SHA256_V0",
        _sha(preregistration),
    )

    funding_bindings: list[development.D1HistoricalFundingFileBindingV0] = []
    for symbol in development.D1_HISTORICAL_UNIVERSE_V0:
        relative = subject._funding_relative_path(symbol)
        path = tmp_path.joinpath(*relative.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = f"opaque compressed fixture {symbol}\n".encode()
        path.write_bytes(raw)
        funding_bindings.append(
            development.D1HistoricalFundingFileBindingV0(
                symbol=symbol,
                relative_path=relative,
                sha256=_sha(raw),
            )
        )
    funding_raw = development.canonical_d1_historical_funding_authority_manifest_v0(
        tuple(funding_bindings)
    )
    monkeypatch.setattr(
        subject,
        "D1_OPERATOR_EXPECTED_FUNDING_AUTHORITY_FILE_SHA256_V0",
        _sha(funding_raw),
    )

    kline_bindings: list[development.D1HistoricalKlineManifestBindingV0] = []
    for symbol in development.D1_HISTORICAL_UNIVERSE_V0:
        for interval in ("5m", "1h"):
            relative = subject._kline_manifest_relative_path(symbol, interval)
            path = tmp_path.joinpath(*relative.split("/"))
            path.parent.mkdir(parents=True, exist_ok=True)
            raw = f"opaque manifest fixture {symbol} {interval}\n".encode()
            path.write_bytes(raw)
            kline_bindings.append(
                development.D1HistoricalKlineManifestBindingV0(
                    symbol=symbol,
                    interval=interval,
                    relative_manifest_path=relative,
                    manifest_sha256=_sha(raw),
                )
            )
    authority = development.build_d1_historical_input_authority_v0(
        kline_manifests=tuple(kline_bindings),
        funding_manifest_relative_path="authority/funding_authority.jsonl",
        funding_manifest_sha256=_sha(funding_raw),
    )
    authority_raw = development.canonical_d1_historical_input_authority_v0(authority)
    monkeypatch.setattr(
        subject,
        "D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SHA256_V0",
        authority.authority_sha256,
    )
    monkeypatch.setattr(
        subject,
        "D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_FILE_SHA256_V0",
        _sha(authority_raw),
    )
    monkeypatch.setattr(
        subject,
        "D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SIZE_BYTES_V0",
        len(authority_raw),
    )
    return authority


def _freeze(
    authority: development.D1HistoricalInputAuthorityV0,
) -> development.D1HistoricalDevelopmentFreezeV0:
    return development.D1HistoricalDevelopmentFreezeV0(
        manifest_sha256="a" * 64,
        manifest_created_at_ms=1_000,
        input_authority_sha256=authority.authority_sha256,
        preregistration_sha256=subject.D1_OPERATOR_EXPECTED_PREREGISTRATION_SHA256_V0,
        frozen_file_count=3,
        _factory_token=development._FREEZE_FACTORY_TOKEN,
    )


def _prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    development.D1HistoricalInputAuthorityV0,
    subject.D1HistoricalInputAuthorityArtifactsV0,
    development.D1HistoricalDevelopmentFreezeV0,
]:
    authority = _configure_inputs(tmp_path, monkeypatch)
    bundle = subject.create_d1_historical_input_authority_artifacts_v0(
        workspace_root=tmp_path
    )
    freeze = _freeze(authority)
    return authority, bundle, freeze


def test_prepare_is_opaque_outcome_blind_canonical_and_no_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = _configure_inputs(tmp_path, monkeypatch)
    runner_called = False

    def forbidden_runner(**_kwargs):
        nonlocal runner_called
        runner_called = True
        raise AssertionError("preparation must not call the outcome runner")

    monkeypatch.setattr(subject, "run_d1_historical_development_v0", forbidden_runner)

    bundle = subject.create_d1_historical_input_authority_artifacts_v0(
        workspace_root=tmp_path
    )

    assert not runner_called
    assert bundle.authority.authority_sha256 == expected.authority_sha256
    assert {value.name for value in bundle.output_dir.iterdir()} == {
        "funding_authority.jsonl",
        "input_authority.jsonl",
    }
    before = {
        value.name: value.read_bytes() for value in bundle.output_dir.iterdir()
    }
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="absent"):
        subject.create_d1_historical_input_authority_artifacts_v0(
            workspace_root=tmp_path
        )
    assert {value.name: value.read_bytes() for value in bundle.output_dir.iterdir()} == before


def test_publication_parent_is_qualified_before_mkdir_and_missing_chain_flushes_deepest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "new" / "deep" / "authority"
    qualified: list[Path] = []

    def reject_volume(path: Path) -> None:
        qualified.append(path)
        raise subject.D1HistoricalOperatorErrorV0("injected unsupported volume")

    monkeypatch.setattr(subject, "_require_publication_volume_supported", reject_volume)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="unsupported volume"):
        subject._publish_fresh_directory(target=target, files={"a": b"a"})
    assert qualified == [tmp_path]
    assert not (tmp_path / "new").exists()

    monkeypatch.setattr(
        subject,
        "_require_publication_volume_supported",
        lambda _path: None,
    )
    flushed: list[Path] = []
    monkeypatch.setattr(
        subject,
        "_flush_publication_directory",
        lambda path: flushed.append(path),
    )
    subject._prepare_real_parent(target)
    assert flushed == [tmp_path / "new" / "deep", tmp_path / "new", tmp_path]


def test_fresh_publication_flushes_staging_target_and_parent_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "authority"
    real_flush = subject._flush_publication_directory
    flushed: list[Path] = []

    def tracking_flush(path: Path) -> None:
        flushed.append(path)
        real_flush(path)

    monkeypatch.setattr(subject, "_flush_publication_directory", tracking_flush)
    subject._publish_fresh_directory(
        target=target,
        files={"a.jsonl": b"a\n", "b.jsonl": b"b\n"},
    )

    assert target.is_dir()
    assert len(flushed) == 6
    assert flushed[0] == tmp_path
    assert flushed[1].parent == tmp_path
    assert flushed[1].name.startswith(".authority.tmp-")
    assert flushed[2:] == [tmp_path, target, tmp_path, tmp_path]


def test_post_rename_flush_failure_preserves_target_and_forbids_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "authority"
    files = {"a.jsonl": b"a\n"}
    real_flush = subject._flush_publication_directory

    def fail_after_target_flush(path: Path) -> None:
        real_flush(path)
        if path == target:
            raise subject.D1HistoricalOperatorErrorV0(
                "injected post-rename target flush uncertainty"
            )

    monkeypatch.setattr(subject, "_flush_publication_directory", fail_after_target_flush)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="durability-ambiguous"):
        subject._publish_fresh_directory(target=target, files=files)
    assert target.is_dir()
    assert (target / "a.jsonl").read_bytes() == b"a\n"

    monkeypatch.setattr(subject, "_flush_publication_directory", real_flush)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="must be absent"):
        subject._publish_fresh_directory(target=target, files=files)
    assert (target / "a.jsonl").read_bytes() == b"a\n"


def test_rename_success_then_wrapper_error_is_ambiguous_and_target_is_not_deleted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "authority"
    files = {"a.jsonl": b"a\n"}
    real_rename = subject._rename_directory_no_replace

    def rename_then_fail(*, staging: Path, target: Path) -> None:
        real_rename(staging=staging, target=target)
        raise OSError("injected error after successful rename")

    monkeypatch.setattr(subject, "_rename_directory_no_replace", rename_then_fail)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="durability-ambiguous"):
        subject._publish_fresh_directory(target=target, files=files)
    assert target.is_dir()
    assert (target / "a.jsonl").read_bytes() == b"a\n"


def test_same_content_directory_swap_after_rename_is_ambiguous_and_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "authority"
    replacement = tmp_path / "replacement"
    displaced = tmp_path / "displaced"
    files = {"a.jsonl": b"same\n", "b.jsonl": b"bytes\n"}
    replacement.mkdir()
    for name, raw in files.items():
        (replacement / name).write_bytes(raw)
    replacement_identity = subject._directory_object_identity(
        replacement.stat(follow_symlinks=False)
    )
    real_flush = subject._flush_publication_directory
    swapped = False

    def swap_after_target_flush(path: Path) -> None:
        nonlocal swapped
        real_flush(path)
        if path == target and not swapped:
            swapped = True
            try:
                target.rename(displaced)
                replacement.rename(target)
            except PermissionError:
                pytest.skip("host cannot replace the flushed directory pathname")

    monkeypatch.setattr(subject, "_flush_publication_directory", swap_after_target_flush)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="durability-ambiguous"):
        subject._publish_fresh_directory(target=target, files=files)
    assert swapped
    assert subject._directory_object_identity(
        target.stat(follow_symlinks=False)
    ) == replacement_identity
    assert {path.name: path.read_bytes() for path in target.iterdir()} == files


@pytest.mark.skipif(os.name != "nt", reason="Win32 publication durability adapter only")
def test_windows_publication_directory_handle_uses_exact_write_through_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: list[tuple[object, ...]] = []

    class _FakeCreateFile:
        argtypes: object = None
        restype: object = None

        def __call__(self, *args: object) -> int:
            observed.append(args)
            return 1234

    fake_create_file = _FakeCreateFile()

    def fake_api(name: str):
        assert name == "CreateFileW"
        return fake_create_file

    monkeypatch.setattr(subject, "_windows_publication_api", fake_api)
    assert subject._windows_open_publication_directory_handle(tmp_path) == 1234
    assert len(observed) == 1
    arguments = observed[0]
    assert arguments[0] == str(tmp_path)
    assert arguments[1] == 0x40000000
    assert arguments[2] == 0x00000007
    assert arguments[4] == 3
    assert arguments[5] == 0x82200000


@pytest.mark.skipif(os.name != "nt", reason="Win32 publication durability adapter only")
def test_windows_publication_flush_uses_same_handle_and_reopens_path_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_open = subject._windows_open_publication_directory_handle
    real_information = subject._windows_publication_file_information
    real_flush = subject._windows_flush_publication_directory_handle
    real_close = subject._windows_close_publication_handle
    events: list[tuple[str, int]] = []

    def tracking_open(path: Path) -> int:
        handle = real_open(path)
        events.append(("open", handle))
        return handle

    def tracking_information(handle: int):
        events.append(("information", handle))
        return real_information(handle)

    def tracking_flush(handle: int) -> None:
        events.append(("flush", handle))
        real_flush(handle)

    def tracking_close(handle: int) -> None:
        events.append(("close", handle))
        real_close(handle)

    monkeypatch.setattr(subject, "_windows_open_publication_directory_handle", tracking_open)
    monkeypatch.setattr(subject, "_windows_publication_file_information", tracking_information)
    monkeypatch.setattr(subject, "_windows_flush_publication_directory_handle", tracking_flush)
    monkeypatch.setattr(subject, "_windows_close_publication_handle", tracking_close)
    subject._flush_publication_directory(tmp_path)

    first_handle = events[0][1]
    second_handle = next(
        value
        for event, value in events
        if event == "open" and value != first_handle
    )
    assert events == [
        ("open", first_handle),
        ("information", first_handle),
        ("flush", first_handle),
        ("information", first_handle),
        ("open", second_handle),
        ("information", second_handle),
        ("close", second_handle),
        ("close", first_handle),
    ]


@pytest.mark.skipif(os.name != "nt", reason="Win32 publication durability adapter only")
def test_windows_publication_close_failure_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_close = subject._windows_close_publication_handle
    close_calls = 0

    def close_then_fail(handle: int) -> None:
        nonlocal close_calls
        close_calls += 1
        real_close(handle)
        if close_calls == 1:
            raise subject.D1HistoricalOperatorErrorV0(
                "injected CloseHandle publication failure"
            )

    monkeypatch.setattr(subject, "_windows_close_publication_handle", close_then_fail)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="CloseHandle"):
        subject._flush_publication_directory(tmp_path)
    assert close_calls == 2


@pytest.mark.skipif(os.name != "nt", reason="Win32 publication durability adapter only")
def test_windows_flushfilebuffers_and_closehandle_zero_are_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ZeroFunction:
        argtypes: object = None
        restype: object = None

        def __call__(self, *_args: object) -> int:
            return 0

    zero = _ZeroFunction()
    monkeypatch.setattr(
        subject,
        "_windows_publication_api",
        lambda _name: zero,
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="FlushFileBuffers"):
        subject._windows_flush_publication_directory_handle(123)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="CloseHandle"):
        subject._windows_close_publication_handle(123)


@pytest.mark.skipif(os.name != "nt", reason="Win32 publication durability adapter only")
@pytest.mark.parametrize("unsupported_filesystem", ("FAT32", "ReFS"))
def test_windows_nonfixed_and_non_ntfs_volumes_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsupported_filesystem: str,
) -> None:
    class _FakeFunction:
        argtypes: object = None
        restype: object = None

        def __init__(self, callback) -> None:
            self._callback = callback

        def __call__(self, *args: object):
            return self._callback(*args)

    def volume_path(_path, output, _size) -> int:
        output.value = "C:\\"
        return 1

    nonfixed_api = {
        "GetVolumePathNameW": _FakeFunction(volume_path),
        "GetDriveTypeW": _FakeFunction(lambda _root: 2),
    }
    monkeypatch.setattr(
        subject,
        "_windows_publication_api",
        lambda name: nonfixed_api[name],
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="local fixed"):
        subject._windows_local_publication_volume_identity(tmp_path)

    def unsupported_volume(
        _root,
        _label,
        _label_size,
        _serial,
        _maximum_component,
        _flags,
        filesystem,
        _filesystem_size,
    ) -> int:
        filesystem.value = unsupported_filesystem
        return 1

    unsupported_api = {
        "GetVolumePathNameW": _FakeFunction(volume_path),
        "GetDriveTypeW": _FakeFunction(lambda _root: 3),
        "GetVolumeInformationW": _FakeFunction(unsupported_volume),
    }
    monkeypatch.setattr(
        subject,
        "_windows_publication_api",
        lambda name: unsupported_api[name],
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="fixed NTFS"):
        subject._windows_local_publication_volume_identity(tmp_path)


@pytest.mark.skipif(os.name != "nt", reason="Win32 publication durability adapter only")
def test_windows_reopened_path_handle_identity_drift_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handles = iter((101, 202))

    def information(*, file_index: int) -> subject._WindowsByHandleFileInformationV0:
        value = subject._WindowsByHandleFileInformationV0()
        value.file_attributes = 0x00000010
        value.volume_serial_number = 7
        value.file_index_high = file_index >> 32
        value.file_index_low = file_index & 0xFFFFFFFF
        return value

    calls_by_handle: dict[int, int] = {}

    def fake_information(handle: int) -> subject._WindowsByHandleFileInformationV0:
        calls_by_handle[handle] = calls_by_handle.get(handle, 0) + 1
        return information(file_index=10 if handle == 101 else 11)

    monkeypatch.setattr(
        subject,
        "_windows_local_publication_volume_identity",
        lambda _path: ("fixed|NTFS|7", 7),
    )
    monkeypatch.setattr(
        subject,
        "_windows_open_publication_directory_handle",
        lambda _path: next(handles),
    )
    monkeypatch.setattr(
        subject,
        "_windows_publication_file_information",
        fake_information,
    )
    monkeypatch.setattr(
        subject,
        "_windows_flush_publication_directory_handle",
        lambda _handle: None,
    )
    monkeypatch.setattr(
        subject,
        "_windows_close_publication_handle",
        lambda _handle: None,
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="pathname identity"):
        subject._windows_flush_publication_directory(tmp_path)
    assert calls_by_handle == {101: 2, 202: 1}


def test_authority_loader_rejects_extra_and_symlink_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, bundle, _freeze_value = _prepare(tmp_path, monkeypatch)
    (bundle.output_dir / "extra.jsonl").write_bytes(b"{}\n")
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="membership"):
        subject.load_d1_historical_input_authority_artifacts_v0(workspace_root=tmp_path)
    (bundle.output_dir / "extra.jsonl").unlink()
    target = bundle.output_dir / "funding_authority.jsonl"
    original = target.read_bytes()
    target.unlink()
    try:
        target.symlink_to(bundle.output_dir / "input_authority.jsonl")
    except OSError:
        target.write_bytes(original)
        pytest.skip("symlink creation is unavailable on this Windows host")
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="non-symlink"):
        subject.load_d1_historical_input_authority_artifacts_v0(workspace_root=tmp_path)


def test_exact_directory_membership_scan_stops_at_first_extra_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory = tmp_path / "bounded"
    directory.mkdir()
    expected_file = directory / "expected"
    expected_file.write_bytes(b"x")
    next_calls = 0

    class _Entry:
        def __init__(self, name: str) -> None:
            self.name = name

        def stat(self, *, follow_symlinks: bool = True):
            assert not follow_symlinks
            return expected_file.stat(follow_symlinks=False)

    class _BoundedScan:
        def __enter__(self):
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def __iter__(self):
            return self

        def __next__(self):
            nonlocal next_calls
            next_calls += 1
            if next_calls == 1:
                return _Entry("expected")
            if next_calls == 2:
                return _Entry("extra")
            raise AssertionError("bounded membership scan read beyond first extra entry")

    monkeypatch.setattr(subject.os, "scandir", lambda _path: _BoundedScan())
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="membership differs"):
        subject._require_exact_directory(
            directory,
            frozenset({"expected"}),
            "bounded fixture",
        )
    assert next_calls == 2


def test_exact_freeze_creator_uses_the_broad_fixed_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    observed: dict[str, object] = {}

    def fake_create(**kwargs):
        observed.update(kwargs)
        return SimpleNamespace(manifest_sha256=freeze.manifest_sha256)

    def fake_load(
        manifest_path,
        *,
        workspace_root,
        expected_manifest_sha256,
        input_authority,
        preregistration_sha256,
    ):
        assert manifest_path == subject.D1_OPERATOR_FREEZE_MANIFEST_V0
        assert Path(workspace_root) == tmp_path
        assert expected_manifest_sha256 == freeze.manifest_sha256
        assert input_authority.authority_sha256 == authority.authority_sha256
        assert preregistration_sha256 == freeze.preregistration_sha256
        return freeze

    monkeypatch.setattr(subject, "create_downstream_code_freeze_v1", fake_create)
    monkeypatch.setattr(subject, "load_d1_historical_development_freeze_v0", fake_load)

    assert subject.create_d1_historical_development_freeze_v0(
        workspace_root=tmp_path
    ) == freeze
    assert observed["purpose"] == development.D1_DEVELOPMENT_FREEZE_PURPOSE_V0
    assert observed["include_trees"] == development.D1_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0
    assert observed["include_files"] == development.D1_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0
    assert "tests/unit/test_d1_scefb_historical_operator.py" in (
        development.D1_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0
    )
    assert observed["upstream_sha256"] == {
        "d1_input_authority": authority.authority_sha256,
        "d1_predecessor_freeze_001": (
            development.D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0
        ),
        "d1_preregistration": freeze.preregistration_sha256,
    }


def test_cli_exposes_a_separate_arm_boundary_and_receipts_reject_true_claims(
    tmp_path: Path,
) -> None:
    parsed = subject._parser().parse_args(
        [
            "arm-development-attempt",
            "--workspace-root",
            str(tmp_path),
            "--expected-freeze-manifest-sha256",
            "a" * 64,
        ]
    )
    assert parsed.command == "arm-development-attempt"

    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="outcome claims"):
        subject.D1HistoricalDevelopmentAttemptArmV0(
            attempt_dir=tmp_path.resolve(),
            armed_record_sha256="b" * 64,
            code_freeze_manifest_sha256="a" * 64,
            probability_claim=True,
        )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="outcome claims"):
        subject.D1HistoricalDevelopmentPublicationVerificationV0(
            status="INCOMPLETE",
            run_id=subject.D1_OPERATOR_RUN_ID_V0,
            attempt_dir=tmp_path.resolve(),
            output_dir=(tmp_path / "output").resolve(),
            start_receipt_sha256=None,
            terminal_receipt_sha256=None,
            result_sha256=None,
            artifact_manifest_sha256=None,
            prospective=True,
        )


def test_wrong_freeze_hash_fails_before_reservation_or_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, _freeze_value = _prepare(tmp_path, monkeypatch)
    runner_calls = 0

    def reject_freeze(*_args, **_kwargs):
        raise ValueError("literal freeze mismatch")

    def forbidden_runner(**_kwargs):
        nonlocal runner_calls
        runner_calls += 1
        raise AssertionError

    monkeypatch.setattr(subject, "load_d1_historical_development_freeze_v0", reject_freeze)
    monkeypatch.setattr(subject, "run_d1_historical_development_v0", forbidden_runner)

    with pytest.raises(ValueError, match="literal freeze mismatch"):
        subject.arm_d1_historical_development_attempt_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256="b" * 64,
        )
    assert runner_calls == 0
    assert not (tmp_path / "attempt").exists()


def _install_freeze(
    monkeypatch: pytest.MonkeyPatch,
    freeze: development.D1HistoricalDevelopmentFreezeV0,
) -> None:
    monkeypatch.setattr(
        subject,
        "load_d1_historical_development_freeze_v0",
        lambda *_args, **_kwargs: freeze,
    )
    monkeypatch.setattr(subject.time, "time_ns", lambda: 2_000_000_000)


def _zero_episode_result(
    *,
    bundle: subject.D1HistoricalInputAuthorityArtifactsV0,
    freeze: development.D1HistoricalDevelopmentFreezeV0,
    run_started_at_ms: int = 2_000,
) -> development.D1HistoricalDevelopmentResultV0:
    summary = development._summarize_development_v0(
        episodes=(),
        censors=(),
        counters=development._RunCountersV0(),
        funding_coverage_status_by_symbol=tuple(
            (
                symbol,
                development.D1HistoricalFundingCoverageStatusV0.EXACT_STANDARD_8H_DEVELOPMENT_COVERAGE.value,
            )
            for symbol in development.D1_HISTORICAL_UNIVERSE_V0
        ),
    )
    return development.D1HistoricalDevelopmentResultV0(
        run_id=subject.D1_OPERATOR_RUN_ID_V0,
        run_started_at_ms=run_started_at_ms,
        input_authority_sha256=bundle.authority.authority_sha256,
        code_freeze_receipt_sha256=freeze.receipt_sha256,
        code_freeze_manifest_sha256=freeze.manifest_sha256,
        preregistration_sha256=freeze.preregistration_sha256,
        episodes=(),
        censors=(),
        summary=summary,
        _factory_token=development._RESULT_FACTORY_TOKEN,
    )


def _arm(
    *,
    tmp_path: Path,
    freeze: development.D1HistoricalDevelopmentFreezeV0,
) -> subject.D1HistoricalDevelopmentAttemptArmV0:
    return subject.arm_d1_historical_development_attempt_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )


def test_arm_is_outcome_blind_and_run_requires_a_preexisting_exact_arm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    runner_calls = 0

    def forbidden_runner(**_kwargs):
        nonlocal runner_calls
        runner_calls += 1
        raise AssertionError("ARMED must not access outcomes")

    monkeypatch.setattr(subject, "run_d1_historical_development_v0", forbidden_runner)
    arm = _arm(tmp_path=tmp_path, freeze=freeze)
    snapshot = subject.load_attempt_wal_v0(tmp_path / "attempt")

    assert runner_calls == 0
    assert snapshot.last_state == "ARMED"
    assert snapshot.torn_tail is None
    assert snapshot.records[0].record_sha256 == arm.armed_record_sha256
    assert {value.name for value in (tmp_path / "attempt").iterdir()} == {"attempt.wal"}

    other_root = tmp_path / "other"
    other_root.mkdir()
    authority_reads = 0

    def forbidden_authority(**_kwargs):
        nonlocal authority_reads
        authority_reads += 1
        raise AssertionError("missing WAL gate must precede authority reads")

    monkeypatch.setattr(
        subject,
        "load_d1_historical_input_authority_artifacts_v0",
        forbidden_authority,
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="WAL is unavailable"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=other_root,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert authority_reads == 0


def test_start_is_durable_before_runner_and_second_attempt_never_reads_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, bundle, freeze = _prepare(tmp_path, monkeypatch)
    runner_calls = 0
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    wal_path = tmp_path / "attempt" / "attempt.wal"
    armed_inode = wal_path.stat().st_ino
    result = _zero_episode_result(bundle=bundle, freeze=freeze)

    def fake_runner(**_kwargs):
        nonlocal runner_calls
        runner_calls += 1
        snapshot = subject.load_attempt_wal_v0(tmp_path / "attempt")
        assert snapshot.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
        assert snapshot.torn_tail is None
        assert wal_path.stat().st_ino == armed_inode
        assert not (tmp_path / "output").exists()
        return result

    monkeypatch.setattr(subject, "run_d1_historical_development_v0", fake_runner)
    verification = subject.run_and_publish_d1_historical_development_once_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verification.status == "COMPLETED"
    assert verification.result_sha256 == result.result_sha256
    assert runner_calls == 1
    assert subject.load_attempt_wal_v0(tmp_path / "attempt").last_state == "COMPLETED"

    authority_reads = 0

    def forbidden_authority(**_kwargs):
        nonlocal authority_reads
        authority_reads += 1
        raise AssertionError("retry gate must precede authority reads")

    monkeypatch.setattr(
        subject,
        "load_d1_historical_input_authority_artifacts_v0",
        forbidden_authority,
    )

    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="retry is permanently"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert runner_calls == 1
    assert authority_reads == 0


def test_crash_immediately_before_grant_callback_reads_zero_outcomes_and_burns_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    runner_calls = 0

    def forbidden_runner(**_kwargs):
        nonlocal runner_calls
        runner_calls += 1
        raise AssertionError("pre-callback crash must not enter the outcome runner")

    def crash_before_callback(
        _self: attempt_wal.D1OutcomeAccessGrantV0,
        _callback: object,
    ) -> None:
        raise SystemExit("injected process death before callback entry")

    monkeypatch.setattr(subject, "run_d1_historical_development_v0", forbidden_runner)
    monkeypatch.setattr(
        attempt_wal.D1OutcomeAccessGrantV0,
        "consume_once_v0",
        crash_before_callback,
    )
    with pytest.raises(SystemExit, match="before callback entry"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    observed = subject.load_attempt_wal_v0(tmp_path / "attempt")
    assert observed.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert observed.start_seal_valid
    assert runner_calls == 0
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="retry is permanently"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert runner_calls == 0


def test_concurrent_restart_enters_one_grant_callback_and_one_runner_at_most(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    result = _zero_episode_result(bundle=bundle, freeze=freeze)
    runner_entered = threading.Event()
    runner_release = threading.Event()
    runner_calls = 0
    callback_boundaries = 0
    outcomes: list[subject.D1HistoricalDevelopmentPublicationVerificationV0] = []
    failures: list[Exception] = []
    real_consume = attempt_wal.D1OutcomeAccessGrantV0.consume_once_v0

    def counted_consume(self, callback):
        nonlocal callback_boundaries
        callback_boundaries += 1
        return real_consume(self, callback)

    def blocking_runner(**_kwargs):
        nonlocal runner_calls
        runner_calls += 1
        runner_entered.set()
        if not runner_release.wait(timeout=5):
            raise RuntimeError("test runner release timed out")
        return result

    def invoke_operator() -> None:
        try:
            outcomes.append(
                subject.run_and_publish_d1_historical_development_once_v0(
                    workspace_root=tmp_path,
                    expected_freeze_manifest_sha256=freeze.manifest_sha256,
                )
            )
        except Exception as error:
            failures.append(error)

    monkeypatch.setattr(subject, "run_d1_historical_development_v0", blocking_runner)
    monkeypatch.setattr(
        attempt_wal.D1OutcomeAccessGrantV0,
        "consume_once_v0",
        counted_consume,
    )
    first = threading.Thread(target=invoke_operator, daemon=True)
    first.start()
    assert runner_entered.wait(timeout=5)
    second = threading.Thread(target=invoke_operator, daemon=True)
    second.start()
    second.join(timeout=5)
    runner_release.set()
    first.join(timeout=5)

    assert not first.is_alive() and not second.is_alive()
    assert len(outcomes) == 1 and outcomes[0].status == "COMPLETED"
    assert len(failures) == 1
    assert isinstance(failures[0], subject.D1HistoricalOperatorErrorV0)
    assert runner_calls == 1
    assert callback_boundaries == 1


def test_failed_attempt_is_terminal_and_cannot_be_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    runner_calls = 0
    callback_boundaries = 0
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    real_consume = attempt_wal.D1OutcomeAccessGrantV0.consume_once_v0

    def counted_consume(self, callback):
        nonlocal callback_boundaries
        callback_boundaries += 1
        return real_consume(self, callback)

    def failing_runner(**_kwargs):
        nonlocal runner_calls
        runner_calls += 1
        assert subject.load_attempt_wal_v0(tmp_path / "attempt").last_state == (
            "STARTED_BEFORE_OUTCOME_ACCESS"
        )
        raise RuntimeError("fixture failure")

    monkeypatch.setattr(subject, "run_d1_historical_development_v0", failing_runner)
    monkeypatch.setattr(
        attempt_wal.D1OutcomeAccessGrantV0,
        "consume_once_v0",
        counted_consume,
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="ended as FAILED"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert subject.load_attempt_wal_v0(tmp_path / "attempt").last_state == "FAILED"
    verification = subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verification.status == "FAILED"
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="retry is permanently"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert runner_calls == 1
    assert callback_boundaries == 1


def test_read_only_verifier_reconciles_success_and_rejects_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    result = _zero_episode_result(bundle=bundle, freeze=freeze)
    monkeypatch.setattr(
        subject,
        "run_d1_historical_development_v0",
        lambda **_kwargs: result,
    )
    subject.run_and_publish_d1_historical_development_once_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )

    verification = subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verification.status == "COMPLETED"
    assert verification.result_sha256 == result.result_sha256

    (tmp_path / "output" / "report.md").write_bytes(b"tampered\n")
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="binding differs"):
        subject.verify_d1_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )


def test_output_verifier_revalidates_final_directory_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    result = _zero_episode_result(bundle=bundle, freeze=freeze)
    monkeypatch.setattr(
        subject,
        "run_d1_historical_development_v0",
        lambda **_kwargs: result,
    )
    subject.run_and_publish_d1_historical_development_once_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    real_serialized_verifier = subject.verify_d1_historical_serialized_artifacts_v0
    swapped = False

    def swap_after_serialized_verification(**kwargs):
        nonlocal swapped
        verified = real_serialized_verifier(**kwargs)
        if not swapped:
            original = tmp_path / "output-original"
            (tmp_path / "output").rename(original)
            shutil.copytree(original, tmp_path / "output")
            swapped = True
        return verified

    monkeypatch.setattr(
        subject,
        "verify_d1_historical_serialized_artifacts_v0",
        swap_after_serialized_verification,
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="directory changed"):
        subject.verify_d1_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )


def test_output_commit_followed_by_writer_error_is_ambiguous_never_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    result = _zero_episode_result(bundle=bundle, freeze=freeze)
    real_writer = subject.write_d1_historical_development_artifacts_v0
    monkeypatch.setattr(
        subject,
        "run_d1_historical_development_v0",
        lambda **_kwargs: result,
    )

    def commit_then_raise(**kwargs):
        real_writer(**kwargs)
        raise OSError("injected post-rename cleanup failure")

    monkeypatch.setattr(
        subject,
        "write_d1_historical_development_artifacts_v0",
        commit_then_raise,
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="AMBIGUOUS_OUTPUT"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    snapshot = subject.load_attempt_wal_v0(tmp_path / "attempt")
    assert snapshot.last_state == "AMBIGUOUS_OUTPUT"
    assert all(record.state != "FAILED" for record in snapshot.records)
    assert (tmp_path / "output" / "manifest.jsonl").is_file()
    verification = subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verification.status == "AMBIGUOUS_OUTPUT"


@pytest.mark.parametrize("orphan_name", (".output.tmp-crash", ".output.publish.lock"))
def test_staging_or_lock_orphan_after_start_is_ambiguous_never_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    orphan_name: str,
) -> None:
    _authority, bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    result = _zero_episode_result(bundle=bundle, freeze=freeze)
    monkeypatch.setattr(
        subject,
        "run_d1_historical_development_v0",
        lambda **_kwargs: result,
    )

    def leave_orphan_then_raise(**_kwargs):
        orphan = tmp_path / orphan_name
        if orphan_name.endswith("lock"):
            orphan.write_bytes(b"stale lock")
        else:
            orphan.mkdir()
        raise OSError("injected publication crash")

    monkeypatch.setattr(
        subject,
        "write_d1_historical_development_artifacts_v0",
        leave_orphan_then_raise,
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="AMBIGUOUS_OUTPUT"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    snapshot = subject.load_attempt_wal_v0(tmp_path / "attempt")
    assert snapshot.last_state == "AMBIGUOUS_OUTPUT"
    assert all(record.state != "FAILED" for record in snapshot.records)
    assert subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    ).status == "AMBIGUOUS_OUTPUT"


def test_completed_append_failure_leaves_output_typed_ambiguous_and_blocks_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    result = _zero_episode_result(bundle=bundle, freeze=freeze)
    monkeypatch.setattr(
        subject,
        "run_d1_historical_development_v0",
        lambda **_kwargs: result,
    )
    real_append_terminal = subject.append_terminal_v0

    def reject_completed_append(**kwargs):
        if kwargs["state"] == "COMPLETED":
            raise OSError("injected terminal append failure")
        return real_append_terminal(**kwargs)

    monkeypatch.setattr(subject, "append_terminal_v0", reject_completed_append)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="append is uncertain"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    snapshot = subject.load_attempt_wal_v0(tmp_path / "attempt")
    assert snapshot.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert (tmp_path / "output" / "manifest.jsonl").is_file()
    assert subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    ).status == "AMBIGUOUS_OUTPUT"

    authority_reads = 0

    def forbidden_authority(**_kwargs):
        nonlocal authority_reads
        authority_reads += 1
        raise AssertionError("retry gate must precede authority reads")

    monkeypatch.setattr(
        subject,
        "load_d1_historical_input_authority_artifacts_v0",
        forbidden_authority,
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="retry is permanently"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert authority_reads == 0


def test_terminal_wal_restore_to_start_with_existing_output_is_ambiguous_and_no_rerun(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    result = _zero_episode_result(bundle=bundle, freeze=freeze)
    wal_path = tmp_path / "attempt" / attempt_wal.D1_ATTEMPT_WAL_FILE_V0
    wal_inode = wal_path.stat().st_ino
    saved_start: bytes | None = None
    runner_calls = 0

    def capture_start_then_run(**_kwargs):
        nonlocal runner_calls, saved_start
        runner_calls += 1
        observed = subject.load_attempt_wal_v0(tmp_path / "attempt")
        assert observed.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
        assert observed.start_seal_valid
        saved_start = wal_path.read_bytes()
        return result

    monkeypatch.setattr(
        subject,
        "run_d1_historical_development_v0",
        capture_start_then_run,
    )
    completed = subject.run_and_publish_d1_historical_development_once_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert completed.status == "COMPLETED"
    assert saved_start is not None

    with wal_path.open("r+b", buffering=0) as handle:
        handle.seek(0)
        handle.write(saved_start)
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())
    assert wal_path.stat().st_ino == wal_inode
    restored = subject.load_attempt_wal_v0(tmp_path / "attempt")
    assert restored.last_state == "STARTED_BEFORE_OUTCOME_ACCESS"
    assert restored.start_seal_valid

    verification = subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verification.status == "AMBIGUOUS_OUTPUT"
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="retry is permanently"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert runner_calls == 1


def test_start_only_and_torn_tail_are_typed_and_permanently_block_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    armed = subject.load_attempt_wal_v0(tmp_path / "attempt")
    started = subject.append_started_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    verification = subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verification.status == "INCOMPLETE"

    with (tmp_path / "attempt" / "attempt.wal").open("ab") as handle:
        handle.write(b"D1")
        handle.flush()
    torn = subject.load_attempt_wal_v0(tmp_path / "attempt")
    assert torn.prefix == started.prefix
    assert torn.torn_tail is not None
    assert subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    ).status == "INCOMPLETE"

    authority_reads = 0

    def forbidden_authority(**_kwargs):
        nonlocal authority_reads
        authority_reads += 1
        raise AssertionError("torn retry gate must precede authority reads")

    monkeypatch.setattr(
        subject,
        "load_d1_historical_input_authority_artifacts_v0",
        forbidden_authority,
    )
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="retry is permanently"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert authority_reads == 0


def test_partial_start_wal_append_enters_zero_callbacks_and_never_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    original_write = attempt_wal._write_once
    write_calls = 0
    runner_calls = 0

    def partial_start_then_fail(descriptor: int, payload: bytes) -> int:
        nonlocal write_calls
        write_calls += 1
        if write_calls == 1:
            return original_write(descriptor, payload[:13])
        raise OSError("injected partial START append")

    def forbidden_runner(**_kwargs):
        nonlocal runner_calls
        runner_calls += 1
        raise AssertionError("partial START must not enter the outcome runner")

    monkeypatch.setattr(attempt_wal, "_write_once", partial_start_then_fail)
    monkeypatch.setattr(subject, "run_d1_historical_development_v0", forbidden_runner)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="START append is uncertain"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    monkeypatch.setattr(attempt_wal, "_write_once", original_write)

    observed = subject.load_attempt_wal_v0(tmp_path / "attempt")
    assert observed.last_state == "ARMED"
    assert observed.torn_tail is not None
    assert not observed.start_seal_valid
    assert runner_calls == 0
    assert subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    ).status == "INCOMPLETE"
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="retry is permanently"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert runner_calls == 0


def test_start_without_seal_is_incomplete_and_runner_cannot_be_entered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    armed = subject.load_attempt_wal_v0(tmp_path / "attempt")
    real_seal_creator = attempt_wal._create_and_sync_start_seal

    def fail_before_seal(*_args, **_kwargs):
        raise attempt_wal.D1HistoricalAttemptWalDurabilityErrorV0(
            "injected pre-seal crash"
        )

    monkeypatch.setattr(attempt_wal, "_create_and_sync_start_seal", fail_before_seal)
    with pytest.raises(attempt_wal.D1HistoricalAttemptWalDurabilityErrorV0):
        subject.append_started_v0(
            attempt_dir=tmp_path / "attempt",
            expected_prefix=armed.prefix,
            started_at_ms=2_000,
        )
    monkeypatch.setattr(
        attempt_wal,
        "_create_and_sync_start_seal",
        real_seal_creator,
    )

    observed = subject.load_attempt_wal_v0(tmp_path / "attempt")
    assert not observed.start_seal_valid
    assert subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    ).status == "INCOMPLETE"
    runner_calls = 0

    def forbidden_runner(**_kwargs):
        nonlocal runner_calls
        runner_calls += 1
        raise AssertionError("unsealed START must never enter runner")

    monkeypatch.setattr(subject, "run_d1_historical_development_v0", forbidden_runner)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="retry is permanently"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert runner_calls == 0


def test_completed_missing_output_and_completed_torn_tail_are_typed_ambiguous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    armed = subject.load_attempt_wal_v0(tmp_path / "attempt")
    started = subject.append_started_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    completed = subject.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.prefix,
        state="COMPLETED",
        terminal_at_ms=3_000,
        result_sha256="a" * 64,
        artifact_manifest_sha256="b" * 64,
    )

    assert subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    ).status == "AMBIGUOUS_OUTPUT"
    with completed.wal_path.open("ab", buffering=0) as handle:
        handle.write(b"D1")
        handle.flush()
    torn = subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert torn.status == "AMBIGUOUS_OUTPUT"
    assert torn.result_sha256 == "a" * 64


def test_terminal_missing_seal_is_typed_ambiguous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    armed = subject.load_attempt_wal_v0(tmp_path / "attempt")
    started = subject.append_started_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    ).snapshot
    subject.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.prefix,
        state="FAILED",
        terminal_at_ms=3_000,
        detail_code="RUN_FAILED_OUTPUT_ABSENT",
    )
    (tmp_path / "attempt" / attempt_wal.D1_ATTEMPT_START_SEAL_FILE_V0).unlink()

    verification = subject.verify_d1_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verification.status == "AMBIGUOUS_OUTPUT"


def test_existing_malformed_canonical_output_is_hard_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    armed = subject.load_attempt_wal_v0(tmp_path / "attempt")
    subject.append_started_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    )
    output = tmp_path / "output"
    output.mkdir()
    (output / "manifest.jsonl").write_bytes(b"{}\n")
    with pytest.raises(subject.D1HistoricalOperatorErrorV0):
        subject.verify_d1_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )


def test_existing_symlink_canonical_output_is_hard_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, _bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    armed = subject.load_attempt_wal_v0(tmp_path / "attempt")
    subject.append_started_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=armed.prefix,
        started_at_ms=2_000,
    )
    target = tmp_path / "symlink-target"
    target.mkdir()
    try:
        (tmp_path / "output").symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlink creation is unavailable on this Windows host")

    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="symlink"):
        subject.verify_d1_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )


def test_post_completed_verifier_failure_appends_ambiguity_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _authority, bundle, freeze = _prepare(tmp_path, monkeypatch)
    _install_freeze(monkeypatch, freeze)
    _arm(tmp_path=tmp_path, freeze=freeze)
    result = _zero_episode_result(bundle=bundle, freeze=freeze)
    monkeypatch.setattr(
        subject,
        "run_d1_historical_development_v0",
        lambda **_kwargs: result,
    )
    real_verify = subject.verify_d1_historical_development_publication_v0
    monkeypatch.setattr(
        subject,
        "verify_d1_historical_development_publication_v0",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("post-write verifier failure")),
    )

    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="AMBIGUOUS_OUTPUT"):
        subject.run_and_publish_d1_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    snapshot = subject.load_attempt_wal_v0(tmp_path / "attempt")
    assert tuple(record.state for record in snapshot.records) == (
        "ARMED",
        "STARTED_BEFORE_OUTCOME_ACCESS",
        "COMPLETED",
        "AMBIGUOUS_OUTPUT",
    )
    assert snapshot.records[-1].result_sha256 == snapshot.records[-2].result_sha256
    monkeypatch.setattr(
        subject,
        "verify_d1_historical_development_publication_v0",
        real_verify,
    )
    assert real_verify(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    ).status == "AMBIGUOUS_OUTPUT"


def test_aggregate_artifact_cap_accepts_exact_boundary_and_rejects_cap_plus_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(subject, "D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0", 10)

    assert subject._bounded_artifact_total(4, 6) == 10
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="aggregate byte cap"):
        subject._bounded_artifact_total(4, 7)


class _FragmentingReader:
    def __init__(self, handle, *, fragment_size: int, early_eof_after: int | None = None) -> None:
        self._handle = handle
        self._fragment_size = fragment_size
        self._early_eof_after = early_eof_after
        self._delivered = 0

    def read(self, size: int = -1) -> bytes:
        if self._early_eof_after is not None and self._delivered >= self._early_eof_after:
            return b""
        requested = self._fragment_size if size < 0 else min(size, self._fragment_size)
        if self._early_eof_after is not None:
            requested = min(requested, self._early_eof_after - self._delivered)
        chunk = self._handle.read(requested)
        self._delivered += len(chunk)
        return chunk

    def fileno(self) -> int:
        return self._handle.fileno()

    def close(self) -> None:
        self._handle.close()


def test_operator_reader_loops_over_short_reads_and_requires_true_eof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "bounded.bin"
    raw = b"0123456789"
    source.write_bytes(raw)
    real_open_stable = subject._open_stable_regular

    def fragmented_open(path: Path, label: str):
        handle, opened = real_open_stable(path, label)
        return _FragmentingReader(handle, fragment_size=2), opened

    monkeypatch.setattr(subject, "_open_stable_regular", fragmented_open)
    assert subject._read_stable_regular_file(
        source,
        "short-read fixture",
        maximum_bytes=len(raw),
    ) == raw

    monkeypatch.undo()
    real_open_stable = subject._open_stable_regular

    def early_eof_open(path: Path, label: str):
        handle, opened = real_open_stable(path, label)
        return _FragmentingReader(handle, fragment_size=2, early_eof_after=4), opened

    monkeypatch.setattr(subject, "_open_stable_regular", early_eof_open)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="opened size"):
        subject._read_stable_regular_file(
            source,
            "early-EOF fixture",
            maximum_bytes=len(raw),
        )


def test_operator_reader_accepts_exact_cap_and_rejects_cap_plus_one(
    tmp_path: Path,
) -> None:
    source = tmp_path / "bounded.bin"
    source.write_bytes(b"12345")

    assert subject._read_stable_regular_file(
        source,
        "exact-cap fixture",
        maximum_bytes=5,
    ) == b"12345"
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="byte cap"):
        subject._read_stable_regular_file(
            source,
            "cap-plus-one fixture",
            maximum_bytes=4,
        )


def test_operator_reader_rejects_pathname_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.bin"
    replacement = tmp_path / "replacement.bin"
    source.write_bytes(b"original")
    replacement.write_bytes(b"replacement")
    real_verify = subject._verify_stable_regular

    def swapping_verify(path, handle, opened, label) -> None:
        try:
            replacement.replace(source)
        except PermissionError:
            pytest.skip("host cannot replace an open pathname")
        real_verify(path, handle, opened, label)

    monkeypatch.setattr(subject, "_verify_stable_regular", swapping_verify)
    with pytest.raises(subject.D1HistoricalOperatorErrorV0, match="changed during read"):
        subject._read_stable_regular_file(
            source,
            "pathname-swap fixture",
            maximum_bytes=32,
        )
