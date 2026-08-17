from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Protocol, cast

import pytest

import signalbot.backtest.downstream_code_freeze as subject
from signalbot.backtest.downstream_code_freeze import (
    DOWNSTREAM_CODE_FREEZE_MAX_FILE_BYTES_V1,
    DOWNSTREAM_CODE_FREEZE_POSIX_DURABILITY_CONTRACT_V1,
    DOWNSTREAM_CODE_FREEZE_SCHEMA_V1,
    DOWNSTREAM_CODE_FREEZE_STATUS_V1,
    DOWNSTREAM_CODE_FREEZE_WINDOWS_DURABILITY_CONTRACT_V1,
    DownstreamCodeFreezeDurabilityErrorV1,
    DownstreamCodeFreezeErrorV1,
    create_downstream_code_freeze_v1,
    downstream_code_freeze_durability_contract_v1,
    load_downstream_code_freeze_v1,
    main,
)
from signalbot.r4b_v2.canonical import canonical_json_line

_CENSUS_FREEZE_SHA256 = (
    "b7868404318b3179274bde738e28c9574718e380a5f37c3a2b4b195ca5fafb60"
)
_FUNDING_AUTHORITY_SHA256 = (
    "d51ae9b1b3bf11bc50b97d461c25efd4e8c77e0f54405bf85422cc186bf0d7ef"
)


class _FakeWin32Function:
    def __init__(self, result: object) -> None:
        self.result = result
        self.calls: list[tuple[object, ...]] = []
        self.argtypes: object = None
        self.restype: object = None

    def __call__(self, *args: object) -> object:
        self.calls.append(args)
        if callable(self.result):
            return self.result(*args)
        return self.result


class _UnicodeBuffer(Protocol):
    value: str


class _SecondStatIdentityDriftPath:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.stat_calls = 0

    def __fspath__(self) -> str:
        return os.fspath(self.path)

    def stat(self, *, follow_symlinks: bool = True) -> os.stat_result:
        metadata = self.path.stat(follow_symlinks=follow_symlinks)
        self.stat_calls += 1
        if self.stat_calls != 2:
            return metadata
        return cast(
            os.stat_result,
            SimpleNamespace(
                st_dev=metadata.st_dev,
                st_file_attributes=getattr(metadata, "st_file_attributes", 0),
                st_ino=metadata.st_ino + 1,
                st_mode=metadata.st_mode,
                st_mtime_ns=metadata.st_mtime_ns,
                st_size=metadata.st_size,
            ),
        )


def _windows_directory_information(
    *,
    serial: int,
    file_index: int,
) -> subject._WindowsByHandleFileInformationV1:
    information = subject._WindowsByHandleFileInformationV1()
    information.file_attributes = 0x00000010
    information.volume_serial_number = serial
    information.file_index_high = file_index >> 32
    information.file_index_low = file_index & 0xFFFFFFFF
    return information


def _workspace(root: Path) -> Path:
    (root / "src/signalbot").mkdir(parents=True)
    (root / "src/signalbot/__init__.py").write_bytes(b"")
    (root / "src/signalbot/runner.py").write_bytes(b"VALUE = 1\n")
    (root / "src/signalbot/ignored.txt").write_bytes(b"not in suffix scope\n")
    (root / "pyproject.toml").write_bytes(b"[project]\nname='fixture'\n")
    return root


def _create(root: Path) -> tuple[Path, str]:
    manifest = root / "artifacts/downstream-freeze.json"
    authority = create_downstream_code_freeze_v1(
        workspace_root=root,
        manifest_path=manifest,
        purpose="PRE_OUTCOME_FIXED_HORIZON_TE0_ANALYSIS",
        include_trees=("src/signalbot",),
        include_files=("pyproject.toml",),
        upstream_sha256={
            "census_code_freeze": _CENSUS_FREEZE_SHA256,
            "funding_authority": _FUNDING_AUTHORITY_SHA256,
        },
        created_at_utc=datetime(2026, 7, 20, 10, 0, tzinfo=UTC),
    )
    return manifest, authority.manifest_sha256


def test_create_and_load_bind_exact_canonical_regular_file_scope(
    tmp_path: Path,
) -> None:
    root = _workspace(tmp_path)
    manifest, manifest_sha256 = _create(root)

    authority = load_downstream_code_freeze_v1(
        manifest,
        workspace_root=root,
        required_upstream_sha256={
            "census_code_freeze": _CENSUS_FREEZE_SHA256,
            "funding_authority": _FUNDING_AUTHORITY_SHA256,
        },
        forbidden_manifest_sha256=(_CENSUS_FREEZE_SHA256,),
    )
    document = json.loads(manifest.read_bytes())

    assert authority.manifest_sha256 == manifest_sha256
    assert authority.manifest_sha256 != _CENSUS_FREEZE_SHA256
    assert document["schema_version"] == DOWNSTREAM_CODE_FREEZE_SCHEMA_V1
    assert document["status"] == DOWNSTREAM_CODE_FREEZE_STATUS_V1
    assert document["created_at_utc"] == "2026-07-20T10:00:00+00:00"
    assert document["include_trees"] == ["src/signalbot"]
    assert document["include_files"] == ["pyproject.toml"]
    assert document["included_suffixes"] == [".py"]
    assert list(document["file_sha256"]) == [
        "pyproject.toml",
        "src/signalbot/__init__.py",
        "src/signalbot/runner.py",
    ]
    assert document["file_count"] == 3
    assert "src/signalbot/ignored.txt" not in document["file_sha256"]
    assert manifest.read_bytes().endswith(b"\n")
    assert b"\r" not in manifest.read_bytes()


def test_create_never_replaces_an_existing_freeze_manifest(tmp_path: Path) -> None:
    root = _workspace(tmp_path)
    manifest, _ = _create(root)
    original = manifest.read_bytes()

    with pytest.raises(DownstreamCodeFreezeErrorV1, match="already exists"):
        create_downstream_code_freeze_v1(
            workspace_root=root,
            manifest_path=manifest,
            purpose="SECOND_CREATION_MUST_FAIL",
            include_trees=("src/signalbot",),
            include_files=("pyproject.toml",),
            upstream_sha256={
                "census_code_freeze": _CENSUS_FREEZE_SHA256,
                "funding_authority": _FUNDING_AUTHORITY_SHA256,
            },
            created_at_utc=datetime(2026, 7, 20, 10, 1, tzinfo=UTC),
        )

    assert manifest.read_bytes() == original


def test_publication_fsyncs_parent_after_link_and_after_temp_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path
    target = root / "freeze.json"
    observations: list[tuple[bool, int]] = []

    def observe_directory_sync(path: Path) -> None:
        staging_names = tuple(root.glob(f".{target.name}.*"))
        observations.append((path.exists(), len(staging_names)))

    monkeypatch.setattr(
        subject,
        "_fsync_publication_directory",
        observe_directory_sync,
    )

    subject._write_new_atomic(root, target, b"immutable\n")

    assert observations == [(True, 1), (True, 0)]
    assert target.read_bytes() == b"immutable\n"


def test_posix_directory_fsync_uses_exact_real_directory_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened: list[tuple[Path, int]] = []
    synced: list[int] = []
    closed: list[int] = []
    fake_descriptor = 731
    metadata = tmp_path.stat()

    monkeypatch.setattr(
        subject.os,
        "open",
        lambda path, flags: opened.append((Path(path), flags)) or fake_descriptor,
    )
    monkeypatch.setattr(subject.os, "fstat", lambda _descriptor: metadata)
    monkeypatch.setattr(subject.os, "fsync", synced.append)
    monkeypatch.setattr(subject.os, "close", closed.append)

    subject._posix_flush_directory_entry_v1(tmp_path)

    assert opened == [
        (
            tmp_path,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
    ]
    assert synced == [fake_descriptor]
    assert closed == [fake_descriptor]


def test_created_manifest_parent_chain_is_flushed_deepest_first(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "new" / "nested" / "freeze.json"
    flushed: list[Path] = []

    monkeypatch.setattr(
        subject,
        "_windows_local_volume_identity_v1",
        lambda _path: ("fixture-volume", 1),
    )
    monkeypatch.setattr(subject, "_flush_directory_entry_v1", flushed.append)

    subject._write_new_atomic(tmp_path, target, b"immutable\n")

    assert flushed == [
        target.parent,
        target.parent.parent,
        tmp_path,
        target.parent,
        target.parent,
    ]
    assert target.read_bytes() == b"immutable\n"


@pytest.mark.skipif(os.name != "nt", reason="requires the Win32 durability path")
def test_windows_volume_is_qualified_before_manifest_parent_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "must-not-exist" / "nested" / "freeze.json"

    def reject_volume(_path: Path) -> tuple[str, int]:
        raise DownstreamCodeFreezeDurabilityErrorV1("unsupported test volume")

    monkeypatch.setattr(subject, "_windows_local_volume_identity_v1", reject_volume)

    with pytest.raises(
        DownstreamCodeFreezeDurabilityErrorV1,
        match="unsupported test volume",
    ):
        subject._write_new_atomic(tmp_path, target, b"never-published\n")

    assert not target.parent.parent.exists()


def test_link_then_directory_fsync_failure_is_ambiguous_and_preserves_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _workspace(tmp_path)
    target = root / "artifacts/freeze.json"
    real_link = os.link
    link_calls = 0

    def counted_link(source: str | Path, destination: str | Path) -> None:
        nonlocal link_calls
        link_calls += 1
        real_link(source, destination)

    def fail_directory_sync(_path: Path) -> None:
        raise OSError("injected directory fsync failure")

    monkeypatch.setattr(subject.os, "link", counted_link)
    monkeypatch.setattr(
        subject,
        "_fsync_publication_directory",
        fail_directory_sync,
    )

    with pytest.raises(
        DownstreamCodeFreezeErrorV1,
        match=r"durability-ambiguous.*do not retry, delete, or replace",
    ):
        create_downstream_code_freeze_v1(
            workspace_root=root,
            manifest_path=target,
            purpose="DIR_FSYNC_FAULT_TEST",
            include_trees=("src/signalbot",),
            include_files=("pyproject.toml",),
            upstream_sha256={"census_code_freeze": _CENSUS_FREEZE_SHA256},
            created_at_utc=datetime(2026, 7, 20, 10, 2, tzinfo=UTC),
        )

    assert link_calls == 1
    published = target.read_bytes()
    assert json.loads(published)["purpose"] == "DIR_FSYNC_FAULT_TEST"
    assert published == canonical_json_line(json.loads(published))
    assert tuple(target.parent.glob(f".{target.name}.*")) == ()


def test_link_success_then_reported_error_is_ambiguous_and_preserves_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "freeze.json"
    real_link = os.link

    def link_then_raise(source: str | Path, destination: str | Path) -> None:
        real_link(source, destination)
        raise OSError("injected error after committed hard link")

    monkeypatch.setattr(subject.os, "link", link_then_raise)

    with pytest.raises(
        DownstreamCodeFreezeErrorV1,
        match=r"durability-ambiguous.*do not retry, delete, or replace",
    ):
        subject._write_new_atomic(tmp_path, target, b"committed\n")

    assert target.read_bytes() == b"committed\n"
    assert tuple(tmp_path.glob(f".{target.name}.*")) == ()


def test_linked_file_flush_revalidates_the_open_descriptor_against_its_path(
    tmp_path: Path,
) -> None:
    staged = tmp_path / "staged.json"
    target = tmp_path / "freeze.json"
    staged.write_bytes(b"immutable\n")
    staged_metadata = staged.stat()
    os.link(staged, target)
    drifting_path = _SecondStatIdentityDriftPath(target)

    with pytest.raises(
        DownstreamCodeFreezeDurabilityErrorV1,
        match="changed while being flushed",
    ):
        subject._fsync_linked_regular_file(
            cast(Path, drifting_path),
            staged_metadata,
        )

    assert drifting_path.stat_calls == 2


def test_linked_file_flush_failure_is_ambiguous_and_preserves_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "freeze.json"

    def fail_linked_file_flush(
        _path: Path,
        _metadata: os.stat_result,
    ) -> None:
        raise OSError("injected linked-file flush failure")

    monkeypatch.setattr(
        subject,
        "_fsync_linked_regular_file",
        fail_linked_file_flush,
    )

    with pytest.raises(
        DownstreamCodeFreezeErrorV1,
        match=r"durability-ambiguous.*do not retry, delete, or replace",
    ):
        subject._write_new_atomic(tmp_path, target, b"linked-before-fault\n")

    assert target.read_bytes() == b"linked-before-fault\n"
    assert tuple(tmp_path.glob(f".{target.name}.*")) == ()


def test_temporary_name_unlink_failure_is_ambiguous_and_preserves_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "freeze.json"
    path_type = type(target)
    real_unlink = path_type.unlink
    injected = False

    def fail_first_temporary_unlink(
        path: Path,
        missing_ok: bool = False,
    ) -> None:
        nonlocal injected
        if path.name.startswith(f".{target.name}.") and not injected:
            injected = True
            raise OSError("injected temporary-name unlink failure")
        real_unlink(path, missing_ok=missing_ok)

    monkeypatch.setattr(path_type, "unlink", fail_first_temporary_unlink)

    with pytest.raises(
        DownstreamCodeFreezeErrorV1,
        match=r"durability-ambiguous.*do not retry, delete, or replace",
    ):
        subject._write_new_atomic(tmp_path, target, b"linked-before-unlink\n")

    assert injected
    assert target.read_bytes() == b"linked-before-unlink\n"
    assert tuple(tmp_path.glob(f".{target.name}.*")) == ()


def test_final_byte_revalidation_fails_closed_after_post_unlink_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "freeze.json"
    sync_calls = 0

    def mutate_after_second_directory_sync(_path: Path) -> None:
        nonlocal sync_calls
        sync_calls += 1
        if sync_calls == 2:
            target.write_bytes(b"tampered-after-unlink\n")

    monkeypatch.setattr(
        subject,
        "_fsync_publication_directory",
        mutate_after_second_directory_sync,
    )

    with pytest.raises(
        DownstreamCodeFreezeErrorV1,
        match=r"durability-ambiguous.*inspect it read-only",
    ):
        subject._write_new_atomic(tmp_path, target, b"original\n")

    assert sync_calls == 2
    assert target.read_bytes() == b"tampered-after-unlink\n"


def test_final_revalidation_rejects_same_bytes_on_a_different_file_object(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "freeze.json"
    raw = b"same-bytes-new-object\n"
    sync_calls = 0

    def replace_after_second_directory_sync(_path: Path) -> None:
        nonlocal sync_calls
        sync_calls += 1
        if sync_calls == 2:
            target.unlink()
            target.write_bytes(raw)

    monkeypatch.setattr(
        subject,
        "_fsync_publication_directory",
        replace_after_second_directory_sync,
    )

    with pytest.raises(
        DownstreamCodeFreezeErrorV1,
        match=r"durability-ambiguous.*inspect it read-only",
    ):
        subject._write_new_atomic(tmp_path, target, raw)

    assert sync_calls == 2
    assert target.read_bytes() == raw


def test_runtime_exposes_the_exact_platform_durability_contract() -> None:
    assert DOWNSTREAM_CODE_FREEZE_POSIX_DURABILITY_CONTRACT_V1 == (
        "POSIX_FILE_CREATED_PARENT_CHAIN_AND_PUBLICATION_DIRECTORY_FSYNC_"
        "HARDLINK_NOREPLACE_V1"
    )
    assert DOWNSTREAM_CODE_FREEZE_WINDOWS_DURABILITY_CONTRACT_V1 == (
        "WINDOWS_LOCAL_FIXED_NTFS_FILE_CREATED_PARENT_CHAIN_AND_PUBLICATION_"
        "DIRECTORY_FLUSH_HARDLINK_NOREPLACE_V1"
    )
    expected = (
        DOWNSTREAM_CODE_FREEZE_WINDOWS_DURABILITY_CONTRACT_V1
        if os.name == "nt"
        else DOWNSTREAM_CODE_FREEZE_POSIX_DURABILITY_CONTRACT_V1
    )
    assert downstream_code_freeze_durability_contract_v1() == expected
    assert "PORTABLE" not in expected


def test_windows_directory_open_uses_the_exact_durability_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_file = _FakeWin32Function(919)
    monkeypatch.setattr(
        subject,
        "_windows_api_v1",
        lambda name: create_file
        if name == "CreateFileW"
        else pytest.fail(f"unexpected Win32 API: {name}"),
    )

    assert subject._windows_open_directory_handle_v1(tmp_path) == 919

    assert len(create_file.calls) == 1
    arguments = create_file.calls[0]
    assert arguments[0] == os.fspath(tmp_path)
    assert arguments[1] == 0x40000000  # GENERIC_WRITE
    assert arguments[2] == 0x00000001 | 0x00000002 | 0x00000004
    assert arguments[3] is None
    assert arguments[4] == 3  # OPEN_EXISTING
    assert arguments[5] == 0x02000000 | 0x00200000 | 0x80000000
    assert arguments[6] is None


def test_windows_volume_qualification_rejects_nonfixed_media(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def set_volume_path(_path: object, buffer: object, _length: object) -> int:
        cast(_UnicodeBuffer, buffer).value = "Z:\\"
        return 1

    apis = {
        "GetVolumePathNameW": _FakeWin32Function(set_volume_path),
        "GetDriveTypeW": _FakeWin32Function(4),
    }
    monkeypatch.setattr(subject, "_windows_api_v1", lambda name: apis[name])

    with pytest.raises(
        DownstreamCodeFreezeDurabilityErrorV1,
        match="local fixed Windows volume",
    ):
        subject._windows_local_volume_identity_v1(tmp_path)

    assert len(apis["GetVolumePathNameW"].calls) == 1
    assert len(apis["GetDriveTypeW"].calls) == 1


def test_windows_volume_qualification_rejects_unsupported_filesystem(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def set_volume_path(_path: object, buffer: object, _length: object) -> int:
        cast(_UnicodeBuffer, buffer).value = "Z:\\"
        return 1

    def set_unsupported_filesystem(*arguments: object) -> int:
        cast(_UnicodeBuffer, arguments[6]).value = "FAT32"
        return 1

    apis = {
        "GetVolumePathNameW": _FakeWin32Function(set_volume_path),
        "GetDriveTypeW": _FakeWin32Function(3),
        "GetVolumeInformationW": _FakeWin32Function(set_unsupported_filesystem),
    }
    monkeypatch.setattr(subject, "_windows_api_v1", lambda name: apis[name])

    with pytest.raises(
        DownstreamCodeFreezeDurabilityErrorV1,
        match="local fixed NTFS",
    ):
        subject._windows_local_volume_identity_v1(tmp_path)

    assert len(apis["GetVolumeInformationW"].calls) == 1


def test_windows_volume_qualification_explicitly_rejects_refs_identity_truncation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def set_volume_path(_path: object, buffer: object, _length: object) -> int:
        cast(_UnicodeBuffer, buffer).value = "Z:\\"
        return 1

    def set_refs_filesystem(*arguments: object) -> int:
        cast(_UnicodeBuffer, arguments[6]).value = "ReFS"
        return 1

    apis = {
        "GetVolumePathNameW": _FakeWin32Function(set_volume_path),
        "GetDriveTypeW": _FakeWin32Function(3),
        "GetVolumeInformationW": _FakeWin32Function(set_refs_filesystem),
    }
    monkeypatch.setattr(subject, "_windows_api_v1", lambda name: apis[name])

    with pytest.raises(
        DownstreamCodeFreezeDurabilityErrorV1,
        match=r"local fixed NTFS; ReFS.*64-bit directory identity",
    ):
        subject._windows_local_volume_identity_v1(tmp_path)

    assert len(apis["GetVolumeInformationW"].calls) == 1


def test_windows_flushfilebuffers_failure_is_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    flush = _FakeWin32Function(0)
    monkeypatch.setattr(subject, "_windows_api_v1", lambda _name: flush)
    monkeypatch.setattr(subject, "_windows_last_error_v1", lambda: 995)

    with pytest.raises(
        DownstreamCodeFreezeDurabilityErrorV1,
        match=r"FlushFileBuffers.*995",
    ):
        subject._windows_flush_directory_handle_v1(83)

    assert len(flush.calls) == 1


def test_windows_closehandle_failure_is_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close = _FakeWin32Function(0)
    monkeypatch.setattr(subject, "_windows_api_v1", lambda _name: close)
    monkeypatch.setattr(subject, "_windows_last_error_v1", lambda: 6)

    with pytest.raises(
        DownstreamCodeFreezeDurabilityErrorV1,
        match=r"CloseHandle.*6",
    ):
        subject._windows_close_handle_v1(83)

    assert len(close.calls) == 1


def test_windows_directory_flush_revalidates_handle_and_path_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serial = 0xA1B2C3D4
    file_index = 0x0102030405060708
    handles = iter((101, 102))
    information = iter(
        (
            _windows_directory_information(serial=serial, file_index=file_index),
            _windows_directory_information(serial=serial, file_index=file_index),
            _windows_directory_information(serial=serial, file_index=file_index),
        )
    )
    flushed: list[int] = []
    closed: list[int] = []

    monkeypatch.setattr(
        subject,
        "_windows_local_volume_identity_v1",
        lambda _path: ("fixture-volume", serial),
    )
    monkeypatch.setattr(
        subject,
        "_windows_open_directory_handle_v1",
        lambda _path: next(handles),
    )
    monkeypatch.setattr(
        subject,
        "_windows_file_information_v1",
        lambda _handle: next(information),
    )
    monkeypatch.setattr(subject, "_windows_flush_directory_handle_v1", flushed.append)
    monkeypatch.setattr(subject, "_windows_close_handle_v1", closed.append)

    subject._windows_flush_directory_entry_v1(tmp_path)

    assert flushed == [101]
    assert closed == [102, 101]


def test_windows_directory_flush_rejects_path_handle_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serial = 27
    handles = iter((201, 202))
    information = iter(
        (
            _windows_directory_information(serial=serial, file_index=31),
            _windows_directory_information(serial=serial, file_index=31),
            _windows_directory_information(serial=serial, file_index=32),
        )
    )
    closed: list[int] = []

    monkeypatch.setattr(
        subject,
        "_windows_local_volume_identity_v1",
        lambda _path: ("fixture-volume", serial),
    )
    monkeypatch.setattr(
        subject,
        "_windows_open_directory_handle_v1",
        lambda _path: next(handles),
    )
    monkeypatch.setattr(
        subject,
        "_windows_file_information_v1",
        lambda _handle: next(information),
    )
    monkeypatch.setattr(subject, "_windows_flush_directory_handle_v1", lambda _handle: None)
    monkeypatch.setattr(subject, "_windows_close_handle_v1", closed.append)

    with pytest.raises(
        DownstreamCodeFreezeDurabilityErrorV1,
        match="pathname identity changed",
    ):
        subject._windows_flush_directory_entry_v1(tmp_path)

    assert closed == [202, 201]


@pytest.mark.skipif(os.name != "nt", reason="requires a real Win32 directory handle")
def test_windows_real_host_directory_flush_contract(tmp_path: Path) -> None:
    volume_identity, serial = subject._windows_local_volume_identity_v1(tmp_path)

    assert serial >= 0
    assert "|NTFS|" in volume_identity
    subject._windows_flush_directory_entry_v1(tmp_path)


@pytest.mark.parametrize("drift", ["hash", "missing", "extra"])
def test_validation_rejects_hash_missing_and_extra_membership_drift(
    tmp_path: Path, drift: str
) -> None:
    root = _workspace(tmp_path)
    manifest, _ = _create(root)
    if drift == "hash":
        (root / "src/signalbot/runner.py").write_bytes(b"VALUE = 2\n")
        message = "hash drift"
    elif drift == "missing":
        (root / "src/signalbot/runner.py").unlink()
        message = "membership drift"
    else:
        (root / "src/signalbot/new_owner.py").write_bytes(b"NEW = True\n")
        message = "membership drift"

    with pytest.raises(DownstreamCodeFreezeErrorV1, match=message):
        load_downstream_code_freeze_v1(manifest, workspace_root=root)


def test_noncanonical_manifest_unknown_fields_and_binding_drift_fail_closed(
    tmp_path: Path,
) -> None:
    root = _workspace(tmp_path)
    manifest, manifest_sha256 = _create(root)
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="required upstream"):
        load_downstream_code_freeze_v1(
            manifest,
            workspace_root=root,
            required_upstream_sha256={"census_code_freeze": "0" * 64},
        )
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="frozen authority"):
        load_downstream_code_freeze_v1(
            manifest,
            workspace_root=root,
            expected_manifest_sha256="0" * 64,
        )
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="distinct"):
        load_downstream_code_freeze_v1(
            manifest,
            workspace_root=root,
            forbidden_manifest_sha256=(manifest_sha256,),
        )

    original = manifest.read_bytes()
    manifest.write_bytes(b" " + original)
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="canonical RFC 8785"):
        load_downstream_code_freeze_v1(manifest, workspace_root=root)

    document = json.loads(original)
    document["unexpected"] = True
    manifest.write_bytes(canonical_json_line(document))
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="fields are not exact"):
        load_downstream_code_freeze_v1(manifest, workspace_root=root)


def test_manifest_self_reference_and_nonregular_explicit_path_are_rejected(
    tmp_path: Path,
) -> None:
    root = _workspace(tmp_path)
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="included tree"):
        create_downstream_code_freeze_v1(
            workspace_root=root,
            manifest_path="src/signalbot/freeze.json",
            purpose="SELF_REFERENCE_TEST",
            include_trees=("src/signalbot",),
            upstream_sha256={},
        )
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="regular non-symlink"):
        create_downstream_code_freeze_v1(
            workspace_root=root,
            manifest_path="artifacts/freeze.json",
            purpose="NONREGULAR_TEST",
            include_trees=("src/signalbot",),
            include_files=("src",),
            upstream_sha256={},
        )


def test_symlink_anywhere_in_included_tree_is_rejected(tmp_path: Path) -> None:
    root = _workspace(tmp_path)
    link = root / "src/signalbot/link.py"
    try:
        link.symlink_to(root / "src/signalbot/runner.py")
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows host")

    with pytest.raises(DownstreamCodeFreezeErrorV1, match="symlink is forbidden"):
        create_downstream_code_freeze_v1(
            workspace_root=root,
            manifest_path="artifacts/freeze.json",
            purpose="SYMLINK_TEST",
            include_trees=("src/signalbot",),
            upstream_sha256={},
        )


def test_regular_file_reader_accepts_exact_cap_and_rejects_cap_plus_one_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "bounded.py"
    with source.open("wb") as handle:
        handle.truncate(DOWNSTREAM_CODE_FREEZE_MAX_FILE_BYTES_V1)
    requested_sizes: list[int] = []
    real_read = os.read

    def tracked_read(descriptor: int, requested: int) -> bytes:
        requested_sizes.append(requested)
        return real_read(descriptor, requested)

    monkeypatch.setattr(subject.os, "read", tracked_read)
    raw = subject._read_regular_file(source, "bounded fixture")

    assert len(raw) == DOWNSTREAM_CODE_FREEZE_MAX_FILE_BYTES_V1
    assert raw[:1] == raw[-1:] == b"\0"
    assert requested_sizes
    assert max(requested_sizes) <= subject._REGULAR_FILE_READ_CHUNK_BYTES_V1

    requested_sizes.clear()
    with source.open("r+b") as handle:
        handle.truncate(DOWNSTREAM_CODE_FREEZE_MAX_FILE_BYTES_V1 + 1)
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="per-file byte cap"):
        subject._read_regular_file(source, "bounded fixture")
    assert requested_sizes == []


def test_regular_file_reader_fails_closed_when_file_mutates_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "mutating.py"
    source.write_bytes(b"VALUE = 1\n")
    original_metadata = source.stat()
    real_read = os.read
    mutated = False

    def mutating_read(descriptor: int, requested: int) -> bytes:
        nonlocal mutated
        chunk = real_read(descriptor, requested)
        if chunk and not mutated:
            source.write_bytes(b"VALUE = 2\n")
            os.utime(
                source,
                ns=(
                    original_metadata.st_atime_ns,
                    original_metadata.st_mtime_ns + 1_000_000_000,
                ),
            )
            mutated = True
        return chunk

    monkeypatch.setattr(subject.os, "read", mutating_read)

    with pytest.raises(
        DownstreamCodeFreezeErrorV1,
        match="identity or size changed during reading",
    ):
        subject._read_regular_file(source, "mutating fixture")
    assert mutated


def test_regular_file_reader_rejects_path_swap_while_descriptor_is_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "swapped.py"
    displaced = tmp_path / "original.py"
    source.write_bytes(b"ORIGINAL\n")
    real_read = os.read
    swapped = False

    def swapping_read(descriptor: int, requested: int) -> bytes:
        nonlocal swapped
        chunk = real_read(descriptor, requested)
        if chunk and not swapped:
            try:
                source.rename(displaced)
                source.write_bytes(b"REPLACED\n")
            except OSError as exc:
                pytest.skip(
                    f"host cannot replace a pathname while its descriptor is open: {exc}"
                )
            swapped = True
        return chunk

    monkeypatch.setattr(subject.os, "read", swapping_read)

    with pytest.raises(
        DownstreamCodeFreezeErrorV1,
        match="identity or size changed during reading",
    ):
        subject._read_regular_file(source, "path-swap fixture")
    assert swapped


def test_regular_file_reader_rejects_direct_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.py"
    link = tmp_path / "link.py"
    target.write_bytes(b"SAFE = True\n")
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows host")

    with pytest.raises(DownstreamCodeFreezeErrorV1, match="regular non-symlink"):
        subject._read_regular_file(link, "symlink fixture")


def test_duplicate_overlapping_scope_and_empty_scope_are_rejected(
    tmp_path: Path,
) -> None:
    root = _workspace(tmp_path)
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="more than one"):
        create_downstream_code_freeze_v1(
            workspace_root=root,
            manifest_path="artifacts/freeze.json",
            purpose="OVERLAP_TEST",
            include_trees=("src", "src/signalbot"),
            upstream_sha256={},
        )
    empty = tmp_path / "empty"
    (empty / "docs").mkdir(parents=True)
    with pytest.raises(DownstreamCodeFreezeErrorV1, match="contains no regular"):
        create_downstream_code_freeze_v1(
            workspace_root=empty,
            manifest_path="freeze.json",
            purpose="EMPTY_TEST",
            include_trees=("docs",),
            upstream_sha256={},
        )


def test_cli_create_and_validate_print_the_same_authority(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _workspace(tmp_path)
    manifest = root / "artifacts/freeze.json"
    assert (
        main(
            [
                "create",
                "--workspace-root",
                str(root),
                "--manifest",
                str(manifest),
                "--purpose",
                "CLI_TEST",
                "--include-tree",
                "src/signalbot",
                "--include-file",
                "pyproject.toml",
                "--binding",
                f"census_code_freeze={_CENSUS_FREEZE_SHA256}",
                "--created-at-utc",
                "2026-07-20T10:00:00+00:00",
            ]
        )
        == 0
    )
    created_sha = capsys.readouterr().out.strip()
    assert (
        main(
            [
                "validate",
                "--workspace-root",
                str(root),
                "--manifest",
                str(manifest),
                "--require-binding",
                f"census_code_freeze={_CENSUS_FREEZE_SHA256}",
                "--expected-manifest-sha256",
                created_sha,
                "--forbid-manifest-sha256",
                _CENSUS_FREEZE_SHA256,
            ]
        )
        == 0
    )
    assert capsys.readouterr().out.strip() == created_sha
