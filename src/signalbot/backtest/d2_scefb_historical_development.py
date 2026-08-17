"""One-shot D2 historical replay and deterministic artifact boundary.

D2 keeps the sealed D1 economic state machine but replaces the contradictory
native-1h input with UTC-aligned hours derived from the exact authenticated 5m
authority.  This module owns the post-START file-access adapter, the distinctly
labelled D2 result, and its no-replace artifact publication.  It never opens a
native 1h path and it grants no efficacy, probability, PAPER-fill, promotion,
or production-order claim.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import threading
from collections.abc import Callable, Iterable
from dataclasses import InitVar, dataclass, field
from datetime import UTC, datetime
from decimal import localcontext
from pathlib import Path
from typing import Final, Never, TypeVar, final

from signalbot.backtest.d1_scefb_historical_attempt_wal import (
    D1AttemptWalBindingsV0,
    D1AttemptWalSnapshotV0,
    D1HistoricalAttemptWalErrorV0,
    D1OutcomeAccessGrantV0,
    load_attempt_wal_v0,
)
from signalbot.backtest.d1_scefb_historical_development import (
    _CENSOR_SEQUENCE_ROOT_DOMAIN,
    _EPISODE_SEQUENCE_ROOT_DOMAIN,
    _RESULT_HASH_DOMAIN,
    D1_HISTORICAL_DATA_START_MS_V0,
    D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
    D1_HISTORICAL_DEVELOPMENT_RULE_V0,
    D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
    D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0,
    D1_HISTORICAL_HOURLY_ROW_COUNT_V0,
    D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0,
    D1_HISTORICAL_MAX_CENSORS_V0,
    D1_HISTORICAL_MAX_EPISODES_V0,
    D1_HISTORICAL_RECEIPT_CONVENTION_V0,
    D1_HISTORICAL_RESULT_STATUS_V0,
    D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0,
    D1_HISTORICAL_UNIVERSE_V0,
    D1HistoricalArtifactDurabilityErrorV0,
    D1HistoricalAuthenticatedFiveMinuteV0,
    D1HistoricalAuthenticatedFundingV0,
    D1HistoricalCensorV0,
    D1HistoricalDevelopmentContractErrorV0,
    D1HistoricalDevelopmentSummaryV0,
    D1HistoricalEpisodeV0,
    D1HistoricalReplayCoreResultV0,
    D1HistoricalReplaySymbolInputV0,
    _ArtifactBudgetV0,
    _file_identity_v0,
    _fresh_artifact_target,
    _fsync_directory_if_supported_v0,
    _is_link_or_reparse_v0,
    _publish_staging_no_replace,
    _read_exact_regular_file,
    _remove_staging_after_failure,
    _require_real_artifact_directory_v0,
    _revalidate_published_artifacts_v0,
    _write_bounded_artifact_file,
    canonical_d1_historical_censor_v0,
    canonical_d1_historical_episode_v0,
    canonical_d1_historical_summary_v0,
    d1_historical_artifact_durability_contract_v0,
    load_d1_historical_authenticated_five_minute_v0,
    load_d1_historical_authenticated_funding_bindings_v0,
    run_d1_historical_replay_core_v0,
    verify_d1_historical_serialized_artifacts_v0,
)
from signalbot.backtest.d2_scefb_derived_hourly_historical import (
    _DERIVED_MANIFEST_FACTORY_TOKEN,
    D1_PREDECESSOR_FAILURE_EVIDENCE_ARCHIVE_SHA256_V0,
    D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0,
    D1_PREDECESSOR_FREEZE_SHA256_V0,
    D1_PREDECESSOR_PREREGISTRATION_SHA256_V0,
    D2_HISTORICAL_DERIVATION_POLICY_V0,
    D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0,
    D2_HISTORICAL_FIXED_FUNDING_FILES_V0,
    D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
    D2_HISTORICAL_SOURCE_POLICY_SHA256_V0,
    D2_HISTORICAL_SOURCE_POLICY_V0,
    D2DerivedHourlyManifestV0,
    D2DerivedHourlyPanelV0,
    D2HistoricalInputAuthorityV0,
    canonical_d2_derived_hourly_manifest_v0,
    canonical_d2_historical_input_authority_v0,
    derive_d2_closed_hourly_v0,
    validate_d2_derived_hourly_panel_v0,
)
from signalbot.backtest.downstream_code_freeze import (
    DownstreamCodeFreezeAuthorityV1,
    load_downstream_code_freeze_v1,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.d1_scefb import D1_SCEFB_RULE_VERSION_V0

D2_HISTORICAL_DEVELOPMENT_RULE_V0: Final = "D2_SCEFB_DERIVED_1H_POST_D1_DEV_V0"
D2_HISTORICAL_RESULT_STATUS_V0: Final = D1_HISTORICAL_RESULT_STATUS_V0
D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0: Final = (
    "94c7ee24a5be0f36e48b0f62c2c9898601dc54cf73f8d2894f6a91304b4175a7"
)
D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0: Final = (
    "c29137ab1f307092137bdc30b678f2f78aa8964a88f6387c293eff92e88ec865"
)
D2_HISTORICAL_ADAPTATION_LABEL_V0: Final = "POST_D1_FAILURE_ADAPTATION"
D2_HISTORICAL_ROLE_V0: Final = "POST_D1_HISTORICAL_DEVELOPMENT_DIAGNOSTIC_ONLY"
D2_HISTORICAL_REPRODUCTION_RUN_ID_V0: Final = (
    "d2-scefb-derived-1h-v0-development-run-001"
)
D2_HISTORICAL_REPRODUCTION_ATTEMPT_RELATIVE_PATH_V0: Final = (
    "artifacts/backtest/2026-07-21-d2-scefb-derived-1h-v0-development-run-001-attempt"
)
D2_HISTORICAL_REPRODUCTION_OUTPUT_RELATIVE_PATH_V0: Final = (
    "artifacts/backtest/2026-07-21-d2-scefb-derived-1h-v0-development-run-001"
)

D2_DEVELOPMENT_FREEZE_PURPOSE_V0: Final = (
    "D2_SCEFB_DERIVED_1H_HISTORICAL_DEVELOPMENT_POST_D1_FAILURE_V0"
)
D2_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0: Final = ("src/signalbot",)
D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0: Final = (
    ".python-version",
    ("artifacts/backtest/2026-07-21-d1-scefb-v0-development-freeze-002/freeze_manifest.json"),
    (
        "artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-"
        "failure-evidence/evidence-manifest.jsonl"
    ),
    "docs/r4b-v2-d1-scefb-5m-preregistration-v0.md",
    "docs/r4b-v2-d1-scefb-run-002-terminal-failure-audit-v0.md",
    "docs/r4b-v2-d2-scefb-derived-hourly-development-preregistration-v0.md",
    "docs/r4b-v2-d2-scefb-operator-failure-receipt-amendment-a0.md",
    "docs/r4b-v2-d2-scefb-operator-failure-receipt-correction-a1.md",
    "pyproject.toml",
    "tests/unit/r4b_v2/strategy/test_d1_scefb.py",
    "tests/unit/test_backtest_context.py",
    "tests/unit/test_d1_scefb_historical_attempt_wal.py",
    "tests/unit/test_d1_scefb_historical_development.py",
    "tests/unit/test_d1_scefb_historical_math.py",
    "tests/unit/test_d2_scefb_derived_hourly_historical.py",
    "tests/unit/test_d2_scefb_historical_development.py",
    "tests/unit/test_d2_scefb_historical_operator.py",
    "uv.lock",
)
D2_DEVELOPMENT_FREEZE_SUFFIXES_V0: Final = (".py",)

_D2_PREREGISTRATION_RELATIVE_PATH: Final = (
    "docs/r4b-v2-d2-scefb-derived-hourly-development-preregistration-v0.md"
)
_D2_OPERATOR_AMENDMENT_RELATIVE_PATH: Final = (
    "docs/r4b-v2-d2-scefb-operator-failure-receipt-amendment-a0.md"
)
_D2_OPERATOR_CORRECTION_A1_RELATIVE_PATH: Final = (
    "docs/r4b-v2-d2-scefb-operator-failure-receipt-correction-a1.md"
)
_D1_ECONOMIC_PREREGISTRATION_RELATIVE_PATH: Final = "docs/r4b-v2-d1-scefb-5m-preregistration-v0.md"
_D1_PREDECESSOR_FREEZE_RELATIVE_PATH: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-development-freeze-002/freeze_manifest.json"
)
_D1_FAILURE_EVIDENCE_MANIFEST_RELATIVE_PATH: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-"
    "failure-evidence/evidence-manifest.jsonl"
)
_D2_RUNNER_RELATIVE_PATH: Final = "src/signalbot/backtest/d2_scefb_historical_development.py"
_D2_AUTHORITY_RELATIVE_PATH: Final = "src/signalbot/backtest/d2_scefb_derived_hourly_historical.py"
_D1_RULE_RELATIVE_PATH: Final = "src/signalbot/r4b_v2/strategy/d1_scefb.py"

_D2_FREEZE_SCHEMA_V0: Final = "d2_scefb_historical_development_freeze_v0"
_D2_RESULT_SCHEMA_V0: Final = "d2_scefb_historical_development_result_v0"
_D2_ARTIFACT_MANIFEST_SCHEMA_V0: Final = "d2_scefb_historical_artifact_manifest_v0"
_D2_REPRODUCTION_VERIFICATION_SCHEMA_V0: Final = (
    "d2_scefb_historical_reproduction_verification_v0"
)
_D2_FREEZE_RECEIPT_HASH_DOMAIN: Final = b"D2_SCEFB_DEVELOPMENT_FREEZE_RECEIPT_V0\0"
_D2_SOURCE_ROOT_HASH_DOMAIN: Final = b"D2_SCEFB_REPLAY_SOURCE_ROOT_V0\0"
_D2_RESULT_HASH_DOMAIN: Final = b"D2_SCEFB_HISTORICAL_DEVELOPMENT_RESULT_V0\0"
_D2_DERIVED_MANIFEST_SEQUENCE_ROOT_DOMAIN: Final = b"D2_SCEFB_DERIVED_MANIFEST_SEQUENCE_ROOT_V0\0"
_D2_EPISODE_SEQUENCE_ROOT_DOMAIN: Final = b"D2_SCEFB_EPISODE_SEQUENCE_ROOT_V0\0"
_D2_CENSOR_SEQUENCE_ROOT_DOMAIN: Final = b"D2_SCEFB_CENSOR_SEQUENCE_ROOT_V0\0"
_D2_REPRODUCTION_OUTPUT_PATH_HASH_DOMAIN: Final = (
    b"D2_HISTORICAL_OPERATOR_OUTPUT_PATH_V0\0"
)
_D2_ARTIFACT_AMBIGUOUS_MESSAGE_V0: Final = (
    "D2 artifact publication is durability-ambiguous after the no-replace directory "
    "commit; do not retry, delete, or replace the target; inspect it read-only"
)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_IDENTITY_RE: Final = re.compile(r"^[A-Za-z0-9._:-]{1,160}$")

_FREEZE_FACTORY_TOKEN: Final = object()
_RESULT_FACTORY_TOKEN: Final = object()
_PROOF_FACTORY_TOKEN: Final = object()
_ARTIFACT_FACTORY_TOKEN: Final = object()
_VERIFICATION_FACTORY_TOKEN: Final = object()
_REPRODUCTION_VERIFICATION_FACTORY_TOKEN: Final = object()

_ReproductionT = TypeVar("_ReproductionT")


class D2HistoricalDevelopmentContractErrorV0(D1HistoricalDevelopmentContractErrorV0):
    """Raised when a D2 replay, freeze, or serialized contract fails closed."""


class D2HistoricalArtifactDurabilityErrorV0(D2HistoricalDevelopmentContractErrorV0):
    """Raised when a D2 no-replace durability boundary cannot be established."""


@dataclass(frozen=True, slots=True)
class D2HistoricalDevelopmentFreezeV0:
    """Exact receipt for one loaded and policy-checked broad D2 code freeze."""

    manifest_sha256: str
    manifest_created_at_ms: int
    input_authority_sha256: str
    frozen_file_count: int
    _factory_token: InitVar[object | None] = None
    receipt_sha256: str = field(init=False)
    preregistration_sha256: str = field(
        init=False,
        default=D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
    )
    operator_amendment_sha256: str = field(
        init=False,
        default=D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0,
    )
    operator_correction_a1_sha256: str = field(
        init=False,
        default=D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0,
    )
    source_policy_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_D2_FREEZE_SCHEMA_V0)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FREEZE_FACTORY_TOKEN:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 development freeze must come from the pinned manifest loader"
            )
        for value, label in (
            (self.manifest_sha256, "freeze manifest_sha256"),
            (self.input_authority_sha256, "freeze input_authority_sha256"),
            (self.preregistration_sha256, "freeze preregistration_sha256"),
            (self.operator_amendment_sha256, "freeze operator amendment sha256"),
            (
                self.operator_correction_a1_sha256,
                "freeze operator correction A1 sha256",
            ),
        ):
            _require_sha256(value, label)
        _require_nonnegative_int(self.manifest_created_at_ms, "freeze created_at_ms")
        if type(self.frozen_file_count) is not int or self.frozen_file_count < 3:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 freeze must cover the rule, authority, and runner sources"
            )
        object.__setattr__(self, "source_policy_sha256", d2_source_policy_sha256_v0())
        object.__setattr__(
            self,
            "receipt_sha256",
            _hash_document(
                _D2_FREEZE_RECEIPT_HASH_DOMAIN,
                _freeze_document_v0(self),
            ),
        )


@dataclass(frozen=True, slots=True)
class _D2HistoricalReplaySymbolProofV0:
    """Factory-sealed authority-to-core proof for one sequential symbol input."""

    input_authority_sha256: str
    five_minute_authority_sha256: str
    funding_authority_sha256: str
    five_minute: D1HistoricalAuthenticatedFiveMinuteV0
    funding: D1HistoricalAuthenticatedFundingV0
    derived_hourly: D2DerivedHourlyPanelV0
    replay_input: D1HistoricalReplaySymbolInputV0
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PROOF_FACTORY_TOKEN:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 replay proofs must be created by the authenticated adapter"
            )
        _validate_replay_proof_v0(self)


@dataclass(frozen=True, slots=True)
class D2HistoricalDevelopmentResultV0:
    """Distinct D2 wrapper around immutable shared-core records and provenance."""

    run_id: str
    run_started_at_ms: int
    start_record_sha256: str
    attempt_directory_sha256: str
    attempt_bindings_sha256: str
    input_authority_sha256: str
    input_authority_file_sha256: str
    code_freeze_manifest_sha256: str
    code_freeze_receipt_sha256: str
    derived_hourly_manifests: tuple[D2DerivedHourlyManifestV0, ...]
    episodes: tuple[D1HistoricalEpisodeV0, ...]
    censors: tuple[D1HistoricalCensorV0, ...]
    summary: D1HistoricalDevelopmentSummaryV0
    _factory_token: InitVar[object | None] = None
    result_sha256: str = field(init=False)
    development_start_ms: int = field(
        init=False,
        default=D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
    )
    development_end_ms_exclusive: int = field(
        init=False,
        default=D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
    )
    universe: tuple[str, ...] = field(init=False, default=D1_HISTORICAL_UNIVERSE_V0)
    rule_version: str = field(init=False, default=D2_HISTORICAL_DEVELOPMENT_RULE_V0)
    economic_rule_version: str = field(init=False, default=D1_SCEFB_RULE_VERSION_V0)
    source_policy_version: str = field(init=False, default=D2_HISTORICAL_SOURCE_POLICY_V0)
    decision_source_root_policy: str = field(
        init=False,
        default=D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0,
    )
    derivation_policy_version: str = field(
        init=False,
        default=D2_HISTORICAL_DERIVATION_POLICY_V0,
    )
    preregistration_sha256: str = field(
        init=False,
        default=D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
    )
    operator_amendment_sha256: str = field(
        init=False,
        default=D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0,
    )
    operator_correction_a1_sha256: str = field(
        init=False,
        default=D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0,
    )
    adaptation_label: str = field(init=False, default=D2_HISTORICAL_ADAPTATION_LABEL_V0)
    historical_role: str = field(init=False, default=D2_HISTORICAL_ROLE_V0)
    historical_receipt_convention: str = field(
        init=False,
        default=D1_HISTORICAL_RECEIPT_CONVENTION_V0,
    )
    status: str = field(init=False, default=D2_HISTORICAL_RESULT_STATUS_V0)
    post_development_end_rows_used: bool = field(init=False, default=False)
    existing_result_artifact_used_as_input: bool = field(init=False, default=False)
    historical_bbo_available: bool = field(init=False, default=False)
    paper_fill_claim: bool = field(init=False, default=False)
    execution_conclusive: bool = field(init=False, default=False)
    probability_claim: bool = field(init=False, default=False)
    efficacy_claim: bool = field(init=False, default=False)
    promoting: bool = field(init=False, default=False)
    prospective: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    schema_version: str = field(init=False, default=_D2_RESULT_SCHEMA_V0)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RESULT_FACTORY_TOKEN:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 result must be created by the post-grant replay boundary"
            )
        _validate_result_members_v0(self)
        object.__setattr__(
            self,
            "result_sha256",
            _hash_document(
                _D2_RESULT_HASH_DOMAIN,
                _result_document_v0(self, include_hash=False),
            ),
        )


@dataclass(frozen=True, slots=True)
class D2HistoricalDevelopmentArtifactsV0:
    output_dir: Path
    manifest_sha256: str
    result_sha256: str
    output_file_sha256: tuple[tuple[str, str], ...]
    total_size_bytes: int
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _ARTIFACT_FACTORY_TOKEN:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 artifact receipts must be publisher-created"
            )
        if not isinstance(self.output_dir, Path) or not self.output_dir.is_absolute():
            raise D2HistoricalDevelopmentContractErrorV0("D2 artifact output_dir must be absolute")
        _require_sha256(self.manifest_sha256, "artifact manifest_sha256")
        _require_sha256(self.result_sha256, "artifact result_sha256")
        if type(self.output_file_sha256) is not tuple or any(
            type(value) is not tuple
            or len(value) != 2
            or not isinstance(value[0], str)
            or _SHA256_RE.fullmatch(value[1]) is None
            for value in self.output_file_sha256
        ):
            raise D2HistoricalDevelopmentContractErrorV0("D2 artifact output hashes are invalid")
        _require_nonnegative_int(self.total_size_bytes, "artifact total_size_bytes")
        if self.total_size_bytes > D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 artifact receipt exceeds the frozen byte cap"
            )


@dataclass(frozen=True, slots=True)
class D2HistoricalSerializedArtifactsVerificationV0:
    artifact_manifest_sha256: str
    result_sha256: str
    summary_sha256: str
    derived_manifest_sequence_root_sha256: str
    episode_sequence_root_sha256: str
    censor_sequence_root_sha256: str
    episode_count: int
    censor_count: int
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _VERIFICATION_FACTORY_TOKEN:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 serialized verification must be verifier-created"
            )
        for value, label in (
            (self.artifact_manifest_sha256, "verified artifact manifest sha256"),
            (self.result_sha256, "verified result sha256"),
            (self.summary_sha256, "verified summary sha256"),
            (
                self.derived_manifest_sequence_root_sha256,
                "verified derived manifest root",
            ),
            (self.episode_sequence_root_sha256, "verified episode root"),
            (self.censor_sequence_root_sha256, "verified censor root"),
        ):
            _require_sha256(value, label)
        _require_nonnegative_int(self.episode_count, "verified episode_count")
        _require_nonnegative_int(self.censor_count, "verified censor_count")


@final
class D2CompletedReproductionGrantV0:
    """Ephemeral one-use capability minted only from an exact COMPLETED WAL."""

    __slots__ = (
        "_attempt_directory_sha256",
        "_bindings",
        "_completed_record_sha256",
        "_consume_lock",
        "_consumed",
        "_mint_process_id",
        "_published_artifact_manifest_sha256",
        "_published_result_sha256",
        "_run_started_at_ms",
        "_start_record_sha256",
    )

    _attempt_directory_sha256: str
    _bindings: D1AttemptWalBindingsV0
    _completed_record_sha256: str
    _consume_lock: threading.Lock
    _consumed: bool
    _mint_process_id: int
    _published_artifact_manifest_sha256: str
    _published_result_sha256: str
    _run_started_at_ms: int
    _start_record_sha256: str

    def __new__(cls, *_args: object, **_kwargs: object) -> Never:
        raise TypeError(
            "D2CompletedReproductionGrantV0 is factory-sealed and cannot be constructed directly"
        )

    def __init_subclass__(cls, **_kwargs: object) -> Never:
        raise TypeError("D2CompletedReproductionGrantV0 cannot be subclassed")

    def __setattr__(self, _name: str, _value: object) -> Never:
        raise TypeError("D2CompletedReproductionGrantV0 fields are immutable")

    @property
    def bindings(self) -> D1AttemptWalBindingsV0:
        return self._bindings

    @property
    def run_started_at_ms(self) -> int:
        return self._run_started_at_ms

    @property
    def start_record_sha256(self) -> str:
        return self._start_record_sha256

    @property
    def completed_record_sha256(self) -> str:
        return self._completed_record_sha256

    @property
    def attempt_directory_sha256(self) -> str:
        return self._attempt_directory_sha256

    @property
    def published_result_sha256(self) -> str:
        return self._published_result_sha256

    @property
    def published_artifact_manifest_sha256(self) -> str:
        return self._published_artifact_manifest_sha256

    @property
    def consumed(self) -> bool:
        with self._consume_lock:
            return self._consumed

    def _consume_once_v0(
        self,
        callback: Callable[[], _ReproductionT],
    ) -> _ReproductionT:
        """Irreversibly consume before entering any serialized/raw replay callback."""

        if not callable(callback):
            raise TypeError("D2 reproduction callback must be callable")
        if os.getpid() != self._mint_process_id:
            object.__setattr__(self, "_consumed", True)
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 completed reproduction grant belongs to another process"
            )
        with self._consume_lock:
            if self._consumed:
                raise D2HistoricalDevelopmentContractErrorV0(
                    "D2 completed reproduction grant was already consumed"
                )
            object.__setattr__(self, "_consumed", True)
        return callback()

    def __copy__(self) -> Never:
        raise TypeError("D2CompletedReproductionGrantV0 cannot be copied")

    def __deepcopy__(self, _memo: dict[int, object]) -> Never:
        raise TypeError("D2CompletedReproductionGrantV0 cannot be deep-copied")

    def __reduce__(self) -> Never:
        raise TypeError("D2CompletedReproductionGrantV0 cannot be serialized")

    def __reduce_ex__(self, _protocol: object) -> Never:
        raise TypeError("D2CompletedReproductionGrantV0 cannot be serialized")

    def __getstate__(self) -> Never:
        raise TypeError("D2CompletedReproductionGrantV0 cannot be serialized")


@dataclass(frozen=True, slots=True)
class D2HistoricalReproductionVerificationV0:
    run_id: str
    run_started_at_ms: int
    start_record_sha256: str
    completed_record_sha256: str
    result_sha256: str
    artifact_manifest_sha256: str
    summary_sha256: str
    derived_manifest_sequence_root_sha256: str
    episode_sequence_root_sha256: str
    censor_sequence_root_sha256: str
    episode_count: int
    censor_count: int
    _factory_token: InitVar[object | None] = None
    raw_replay_performed: bool = field(init=False, default=True)
    published_artifacts_modified: bool = field(init=False, default=False)
    historical_bbo_available: bool = field(init=False, default=False)
    paper_fill_claim: bool = field(init=False, default=False)
    execution_conclusive: bool = field(init=False, default=False)
    probability_claim: bool = field(init=False, default=False)
    efficacy_claim: bool = field(init=False, default=False)
    promoting: bool = field(init=False, default=False)
    prospective: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    schema_version: str = field(
        init=False,
        default=_D2_REPRODUCTION_VERIFICATION_SCHEMA_V0,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _REPRODUCTION_VERIFICATION_FACTORY_TOKEN:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 reproduction verification must be replay-created"
            )
        _require_identity(self.run_id, "reproduced run_id")
        _require_nonnegative_int(self.run_started_at_ms, "reproduced run_started_at_ms")
        for value, label in (
            (self.start_record_sha256, "reproduced START record"),
            (self.completed_record_sha256, "reproduced COMPLETED record"),
            (self.result_sha256, "reproduced result"),
            (self.artifact_manifest_sha256, "reproduced artifact manifest"),
            (self.summary_sha256, "reproduced summary"),
            (self.derived_manifest_sequence_root_sha256, "reproduced derived root"),
            (self.episode_sequence_root_sha256, "reproduced episode root"),
            (self.censor_sequence_root_sha256, "reproduced censor root"),
        ):
            _require_sha256(value, label)
        _require_nonnegative_int(self.episode_count, "reproduced episode_count")
        _require_nonnegative_int(self.censor_count, "reproduced censor_count")
        if (
            self.raw_replay_performed is not True
            or self.published_artifacts_modified is not False
            or self.historical_bbo_available is not False
            or self.paper_fill_claim is not False
            or self.execution_conclusive is not False
            or self.probability_claim is not False
            or self.efficacy_claim is not False
            or self.promoting is not False
            or self.prospective is not False
            or self.production_order_placement is not False
            or self.schema_version != _D2_REPRODUCTION_VERIFICATION_SCHEMA_V0
        ):
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 reproduction receipt claims differ from its read-only contract"
            )


def d2_source_policy_sha256_v0() -> str:
    """Return the domain-separated identity of the exact D2 source policy."""

    return D2_HISTORICAL_SOURCE_POLICY_SHA256_V0


def d2_historical_development_freeze_upstream_v0(
    input_authority_sha256: str,
) -> dict[str, str]:
    """Return a fresh copy of the exact upstream map required to create a D2 freeze."""

    return _freeze_upstream_v0(input_authority_sha256)


def load_d2_historical_development_freeze_v0(
    manifest_path: str | Path,
    *,
    workspace_root: str | Path,
    expected_manifest_sha256: str,
    input_authority: D2HistoricalInputAuthorityV0,
) -> D2HistoricalDevelopmentFreezeV0:
    """Load a pinned broad freeze and enforce the exact D2 membership policy."""

    canonical_d2_historical_input_authority_v0(input_authority)
    _require_sha256(expected_manifest_sha256, "expected freeze manifest sha256")
    upstream = _freeze_upstream_v0(input_authority.authority_sha256)
    try:
        authority = load_downstream_code_freeze_v1(
            manifest_path,
            workspace_root=workspace_root,
            expected_manifest_sha256=expected_manifest_sha256,
            required_upstream_sha256=upstream,
        )
    except (ValueError, OSError) as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 code freeze could not be loaded through the validated owner"
        ) from error
    return _validate_freeze_authority_v0(
        authority,
        input_authority_sha256=input_authority.authority_sha256,
    )


def canonical_d2_historical_development_freeze_v0(
    value: D2HistoricalDevelopmentFreezeV0,
) -> bytes:
    """Revalidate and serialize one exact D2 freeze receipt."""

    if type(value) is not D2HistoricalDevelopmentFreezeV0:
        raise D2HistoricalDevelopmentContractErrorV0(
            "value must be exact D2HistoricalDevelopmentFreezeV0"
        )
    expected = _hash_document(_D2_FREEZE_RECEIPT_HASH_DOMAIN, _freeze_document_v0(value))
    if value.receipt_sha256 != expected:
        raise D2HistoricalDevelopmentContractErrorV0("D2 development freeze receipt hash differs")
    return canonical_json_line({**_freeze_document_v0(value), "receipt_sha256": expected})


def run_d2_historical_development_v0(
    *,
    data_root: str | Path,
    input_authority: D2HistoricalInputAuthorityV0,
    code_freeze: D2HistoricalDevelopmentFreezeV0,
    outcome_access_grant: D1OutcomeAccessGrantV0,
    run_id: str,
    run_started_at_ms: int,
) -> D2HistoricalDevelopmentResultV0:
    """Consume one durable START grant, then and only then open outcome files."""

    _require_identity(run_id, "run_id")
    _require_nonnegative_int(run_started_at_ms, "run_started_at_ms")
    authority_raw = canonical_d2_historical_input_authority_v0(input_authority)
    canonical_d2_historical_development_freeze_v0(code_freeze)
    if code_freeze.input_authority_sha256 != input_authority.authority_sha256:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 freeze is bound to a different input authority"
        )
    if code_freeze.manifest_created_at_ms > run_started_at_ms:
        raise D2HistoricalDevelopmentContractErrorV0("D2 run cannot precede its code freeze")
    _validate_outcome_grant_v0(
        outcome_access_grant,
        run_id=run_id,
        input_authority=input_authority,
        input_authority_raw=authority_raw,
        code_freeze=code_freeze,
    )
    return outcome_access_grant.consume_once_v0(
        lambda: _run_d2_after_outcome_access_v0(
            data_root=data_root,
            input_authority=input_authority,
            code_freeze=code_freeze,
            outcome_access_grant=outcome_access_grant,
            run_id=run_id,
            run_started_at_ms=run_started_at_ms,
            input_authority_file_sha256=hashlib.sha256(authority_raw).hexdigest(),
        )
    )


def canonical_d2_historical_development_result_v0(
    value: D2HistoricalDevelopmentResultV0,
) -> bytes:
    """Revalidate all shared-core members and serialize the distinct D2 wrapper."""

    if type(value) is not D2HistoricalDevelopmentResultV0:
        raise D2HistoricalDevelopmentContractErrorV0(
            "value must be exact D2HistoricalDevelopmentResultV0"
        )
    _validate_result_members_v0(value)
    expected = _hash_document(
        _D2_RESULT_HASH_DOMAIN,
        _result_document_v0(value, include_hash=False),
    )
    if value.result_sha256 != expected:
        raise D2HistoricalDevelopmentContractErrorV0("D2 result hash differs")
    return canonical_json_line(_result_document_v0(value, include_hash=True))


def write_d2_historical_development_artifacts_v0(
    *,
    result: D2HistoricalDevelopmentResultV0,
    input_authority: D2HistoricalInputAuthorityV0,
    code_freeze: D2HistoricalDevelopmentFreezeV0,
    output_dir: str | Path,
    maximum_total_bytes: int = D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0,
) -> D2HistoricalDevelopmentArtifactsV0:
    """Publish one bounded deterministic D2 directory with atomic no-replace."""

    if (
        type(maximum_total_bytes) is not int
        or maximum_total_bytes <= 0
        or maximum_total_bytes > D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "artifact byte cap must be positive and no larger than the frozen cap"
        )
    payloads = _artifact_payloads_v0(
        result=result,
        input_authority=input_authority,
        code_freeze=code_freeze,
    )
    try:
        target = _fresh_artifact_target(output_dir)
    except D1HistoricalDevelopmentContractErrorV0 as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 output requires a fresh safe absent target"
        ) from error
    budget = _ArtifactBudgetV0(maximum_bytes=maximum_total_bytes)
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=target.parent))
    output_metadata: dict[str, tuple[str, int]] = {}
    try:
        for name, raw in payloads:
            output_metadata[name] = _write_bounded_artifact_file(
                staging / name,
                (raw,),
                budget=budget,
            )
        manifest_raw = _artifact_manifest_raw_v0(result, output_metadata)
        output_metadata["manifest.jsonl"] = _write_bounded_artifact_file(
            staging / "manifest.jsonl",
            (manifest_raw,),
            budget=budget,
        )
        _fsync_directory_if_supported_v0(staging)
        _publish_staging_no_replace(staging=staging, target=target)
        _revalidate_published_artifacts_v0(
            target=target,
            output_metadata=output_metadata,
        )
    except (D1HistoricalArtifactDurabilityErrorV0, D1HistoricalDevelopmentContractErrorV0) as error:
        if target.exists() and not staging.exists():
            raise D2HistoricalArtifactDurabilityErrorV0(
                _D2_ARTIFACT_AMBIGUOUS_MESSAGE_V0
            ) from error
        try:
            _remove_staging_after_failure(staging)
        except D1HistoricalArtifactDurabilityErrorV0 as cleanup_error:
            raise D2HistoricalArtifactDurabilityErrorV0(
                "D2 artifact publication and staging cleanup both failed"
            ) from cleanup_error
        raise D2HistoricalDevelopmentContractErrorV0(
            "cannot publish D2 historical development artifacts"
        ) from error
    except OSError as error:
        if target.exists() and not staging.exists():
            raise D2HistoricalArtifactDurabilityErrorV0(
                _D2_ARTIFACT_AMBIGUOUS_MESSAGE_V0
            ) from error
        try:
            _remove_staging_after_failure(staging)
        except D1HistoricalArtifactDurabilityErrorV0 as cleanup_error:
            raise D2HistoricalArtifactDurabilityErrorV0(
                "D2 artifact publication and staging cleanup both failed"
            ) from cleanup_error
        raise D2HistoricalDevelopmentContractErrorV0(
            "cannot publish D2 historical development artifacts"
        ) from error
    return D2HistoricalDevelopmentArtifactsV0(
        output_dir=target,
        manifest_sha256=output_metadata["manifest.jsonl"][0],
        result_sha256=result.result_sha256,
        output_file_sha256=tuple(
            (name, digest) for name, (digest, _size) in sorted(output_metadata.items())
        ),
        total_size_bytes=budget.consumed_bytes,
        _factory_token=_ARTIFACT_FACTORY_TOKEN,
    )


def verify_d2_historical_serialized_artifacts_v0(
    *,
    output_dir: str | Path,
    expected_result: D2HistoricalDevelopmentResultV0,
    expected_input_authority: D2HistoricalInputAuthorityV0,
    expected_code_freeze: D2HistoricalDevelopmentFreezeV0,
) -> D2HistoricalSerializedArtifactsVerificationV0:
    """Same-process convenience wrapper around the independent verifier."""

    canonical_d2_historical_development_result_v0(expected_result)
    expected_manifest_raw = _expected_artifact_manifest_for_result_v0(
        result=expected_result,
        input_authority=expected_input_authority,
        code_freeze=expected_code_freeze,
    )
    return verify_d2_historical_published_artifact_bundle_v0(
        output_dir=output_dir,
        expected_result_sha256=expected_result.result_sha256,
        expected_manifest_sha256=hashlib.sha256(expected_manifest_raw).hexdigest(),
        expected_input_authority=expected_input_authority,
        expected_code_freeze=expected_code_freeze,
        expected_run_id=expected_result.run_id,
        expected_run_started_at_ms=expected_result.run_started_at_ms,
        expected_start_record_sha256=expected_result.start_record_sha256,
        expected_attempt_directory_sha256=expected_result.attempt_directory_sha256,
        expected_attempt_bindings_sha256=expected_result.attempt_bindings_sha256,
    )


def verify_d2_historical_published_artifact_bundle_v0(
    *,
    output_dir: str | Path,
    expected_result_sha256: str,
    expected_manifest_sha256: str,
    expected_input_authority: D2HistoricalInputAuthorityV0,
    expected_code_freeze: D2HistoricalDevelopmentFreezeV0,
    expected_run_id: str,
    expected_run_started_at_ms: int,
    expected_start_record_sha256: str,
    expected_attempt_directory_sha256: str,
    expected_attempt_bindings_sha256: str,
) -> D2HistoricalSerializedArtifactsVerificationV0:
    """Authenticate a published D2 bundle from serialized bytes in a fresh process."""

    _require_sha256(expected_result_sha256, "expected result sha256")
    _require_sha256(expected_manifest_sha256, "expected artifact manifest sha256")
    _require_identity(expected_run_id, "expected run_id")
    _require_nonnegative_int(expected_run_started_at_ms, "expected run_started_at_ms")
    for value, label in (
        (expected_start_record_sha256, "expected START record sha256"),
        (expected_attempt_directory_sha256, "expected attempt directory sha256"),
        (expected_attempt_bindings_sha256, "expected attempt bindings sha256"),
    ):
        _require_sha256(value, label)
    authority_raw = canonical_d2_historical_input_authority_v0(expected_input_authority)
    freeze_raw = canonical_d2_historical_development_freeze_v0(expected_code_freeze)
    if expected_code_freeze.input_authority_sha256 != expected_input_authority.authority_sha256:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized verifier authority and freeze bindings differ"
        )

    target = Path(os.path.abspath(Path(output_dir)))
    expected_names = frozenset(
        {
            "censors.jsonl",
            "code-freeze-receipt.jsonl",
            "derived-hourly-manifests.jsonl",
            "episodes.jsonl",
            "input-authority.jsonl",
            "manifest.jsonl",
            "report.md",
            "result-index.jsonl",
            "summary.jsonl",
        }
    )
    tree_before = _d2_artifact_tree_snapshot_v0(target)
    members_before, stated_total_bytes = _d2_artifact_member_snapshot_v0(
        target,
        expected_names=expected_names,
    )
    if stated_total_bytes > D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact bundle exceeds its aggregate byte cap"
        )
    raw_by_name = {name: _read_d2_artifact_member_v0(target, name) for name in expected_names}
    actual_total_bytes = sum(len(raw) for raw in raw_by_name.values())
    if actual_total_bytes != stated_total_bytes:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact aggregate bytes differ from the opening snapshot"
        )
    if actual_total_bytes > D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact bundle exceeds its aggregate byte cap"
        )
    if raw_by_name["input-authority.jsonl"] != authority_raw:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 input authority differs from the expected authority"
        )
    if raw_by_name["code-freeze-receipt.jsonl"] != freeze_raw:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 freeze receipt differs from the expected freeze"
        )

    manifest_raw = raw_by_name["manifest.jsonl"]
    if hashlib.sha256(manifest_raw).hexdigest() != expected_manifest_sha256:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact manifest byte hash differs"
        )
    manifest_document = _decode_canonical_object_v0(
        manifest_raw,
        "serialized D2 artifact manifest",
    )
    output_names = expected_names - {"manifest.jsonl"}
    output_metadata = {
        name: (hashlib.sha256(raw_by_name[name]).hexdigest(), len(raw_by_name[name]))
        for name in output_names
    }
    _validate_serialized_artifact_manifest_v0(
        manifest_document,
        expected_result_sha256=expected_result_sha256,
        expected_input_authority_sha256=expected_input_authority.authority_sha256,
        output_metadata=output_metadata,
    )

    derived_manifests = _parse_serialized_derived_manifests_v0(
        raw_by_name["derived-hourly-manifests.jsonl"]
    )
    episode_lines = _canonical_jsonl_lines_v0(
        raw_by_name["episodes.jsonl"],
        "serialized D2 episodes",
        maximum_count=D1_HISTORICAL_MAX_EPISODES_V0,
        allow_empty=True,
    )
    censor_lines = _canonical_jsonl_lines_v0(
        raw_by_name["censors.jsonl"],
        "serialized D2 censors",
        maximum_count=D1_HISTORICAL_MAX_CENSORS_V0,
        allow_empty=True,
    )
    summary_raw = raw_by_name["summary.jsonl"]
    summary_document = _decode_canonical_object_v0(
        summary_raw,
        "serialized D2 shared-core summary",
    )
    transient_d1_index = _transient_d1_verifier_index_v0(
        episode_lines=episode_lines,
        censor_lines=censor_lines,
        summary_document=summary_document,
        run_id=expected_run_id,
        run_started_at_ms=expected_run_started_at_ms,
        input_authority_sha256=expected_input_authority.authority_sha256,
        code_freeze_manifest_sha256=expected_code_freeze.manifest_sha256,
        code_freeze_receipt_sha256=expected_code_freeze.receipt_sha256,
    )
    try:
        d1_verified = verify_d1_historical_serialized_artifacts_v0(
            episode_lines=episode_lines,
            censor_lines=censor_lines,
            summary_raw=summary_raw,
            result_index_raw=transient_d1_index,
            expected_run_id=expected_run_id,
            expected_run_started_at_ms=expected_run_started_at_ms,
            expected_input_authority_sha256=expected_input_authority.authority_sha256,
            expected_code_freeze_manifest_sha256=expected_code_freeze.manifest_sha256,
            expected_code_freeze_receipt_sha256=expected_code_freeze.receipt_sha256,
            expected_preregistration_sha256=D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
        )
    except D1HistoricalDevelopmentContractErrorV0 as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 shared-core records failed exact D1 semantic verification"
        ) from error

    result_document = _decode_canonical_object_v0(
        raw_by_name["result-index.jsonl"],
        "serialized D2 result index",
    )
    roots = _validate_serialized_d2_result_v0(
        result_document=result_document,
        expected_result_sha256=expected_result_sha256,
        expected_input_authority=expected_input_authority,
        expected_code_freeze=expected_code_freeze,
        expected_run_id=expected_run_id,
        expected_run_started_at_ms=expected_run_started_at_ms,
        expected_start_record_sha256=expected_start_record_sha256,
        expected_attempt_directory_sha256=expected_attempt_directory_sha256,
        expected_attempt_bindings_sha256=expected_attempt_bindings_sha256,
        derived_manifests=derived_manifests,
        episode_lines=episode_lines,
        censor_lines=censor_lines,
        summary_sha256=d1_verified.summary_sha256,
    )
    expected_report = _development_report_from_documents_v0(
        result_document=result_document,
        summary_document=summary_document,
    )
    if raw_by_name["report.md"] != expected_report:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 report differs from deterministic result content"
        )
    all_output_metadata = {
        name: (hashlib.sha256(raw).hexdigest(), len(raw))
        for name, raw in raw_by_name.items()
    }
    try:
        _revalidate_published_artifacts_v0(
            target=target,
            output_metadata=all_output_metadata,
        )
    except (D1HistoricalArtifactDurabilityErrorV0, D1HistoricalDevelopmentContractErrorV0) as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact bundle changed during final byte revalidation"
        ) from error
    members_after, final_stated_total_bytes = _d2_artifact_member_snapshot_v0(
        target,
        expected_names=expected_names,
    )
    tree_after = _d2_artifact_tree_snapshot_v0(target)
    if (
        tree_after != tree_before
        or members_after != members_before
        or final_stated_total_bytes != actual_total_bytes
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact directory identity or membership changed during verification"
        )
    return D2HistoricalSerializedArtifactsVerificationV0(
        artifact_manifest_sha256=expected_manifest_sha256,
        result_sha256=expected_result_sha256,
        summary_sha256=d1_verified.summary_sha256,
        derived_manifest_sequence_root_sha256=roots[0],
        episode_sequence_root_sha256=roots[1],
        censor_sequence_root_sha256=roots[2],
        episode_count=len(episode_lines),
        censor_count=len(censor_lines),
        _factory_token=_VERIFICATION_FACTORY_TOKEN,
    )


def _load_d2_completed_reproduction_grant_v0(
    *,
    attempt_dir: str | Path,
    expected_attempt_bindings: D1AttemptWalBindingsV0,
    expected_input_authority: D2HistoricalInputAuthorityV0,
    expected_code_freeze: D2HistoricalDevelopmentFreezeV0,
) -> D2CompletedReproductionGrantV0:
    """Mint one non-serializable replay capability from exact COMPLETED evidence."""

    if type(expected_attempt_bindings) is not D1AttemptWalBindingsV0:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction expected bindings have the wrong exact type"
        )
    authority_raw = canonical_d2_historical_input_authority_v0(expected_input_authority)
    canonical_d2_historical_development_freeze_v0(expected_code_freeze)
    if not (
        expected_code_freeze.input_authority_sha256
        == expected_input_authority.authority_sha256
        == expected_attempt_bindings.input_authority_sha256
        and expected_attempt_bindings.run_id == D2_HISTORICAL_REPRODUCTION_RUN_ID_V0
        and expected_attempt_bindings.input_authority_file_sha256
        == hashlib.sha256(authority_raw).hexdigest()
        and expected_attempt_bindings.code_freeze_manifest_sha256
        == expected_code_freeze.manifest_sha256
        and expected_attempt_bindings.funding_authority_file_sha256
        == expected_input_authority.funding_manifest_sha256
        and expected_attempt_bindings.preregistration_sha256
        == D2_HISTORICAL_PREREGISTRATION_SHA256_V0
        and expected_attempt_bindings.output_path_sha256
        == hashlib.sha256(
            _D2_REPRODUCTION_OUTPUT_PATH_HASH_DOMAIN
            + D2_HISTORICAL_REPRODUCTION_OUTPUT_RELATIVE_PATH_V0.encode("utf-8")
        ).hexdigest()
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction bindings differ from the authority or freeze"
        )
    try:
        snapshot = load_attempt_wal_v0(
            attempt_dir,
            expected_bindings=expected_attempt_bindings,
        )
    except D1HistoricalAttemptWalErrorV0 as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction cannot authenticate the completed attempt WAL"
        ) from error
    return _mint_completed_reproduction_grant_v0(snapshot)


def reproduce_d2_historical_published_artifact_bundle_v0(
    *,
    data_root: str | Path,
    attempt_dir: str | Path,
    output_dir: str | Path,
    expected_attempt_bindings: D1AttemptWalBindingsV0,
    expected_input_authority: D2HistoricalInputAuthorityV0,
    expected_code_freeze: D2HistoricalDevelopmentFreezeV0,
) -> D2HistoricalReproductionVerificationV0:
    """Read-only raw-data reproduction of one exact COMPLETED publication."""

    root = Path(os.path.abspath(Path(data_root)))
    exact_attempt = root.joinpath(
        *D2_HISTORICAL_REPRODUCTION_ATTEMPT_RELATIVE_PATH_V0.split("/")
    )
    exact_output = root.joinpath(
        *D2_HISTORICAL_REPRODUCTION_OUTPUT_RELATIVE_PATH_V0.split("/")
    )
    supplied_attempt = Path(os.path.abspath(Path(attempt_dir)))
    supplied_output = Path(os.path.abspath(Path(output_dir)))
    if supplied_attempt != exact_attempt or supplied_output != exact_output:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction paths differ from the fixed attempt or publication target"
        )
    _d2_artifact_tree_snapshot_v0(root)
    _d2_artifact_tree_snapshot_v0(exact_attempt)
    _d2_artifact_tree_snapshot_v0(exact_output)
    grant = _load_d2_completed_reproduction_grant_v0(
        attempt_dir=exact_attempt,
        expected_attempt_bindings=expected_attempt_bindings,
        expected_input_authority=expected_input_authority,
        expected_code_freeze=expected_code_freeze,
    )
    return grant._consume_once_v0(
        lambda: _reproduce_d2_after_completed_grant_v0(
            data_root=root,
            attempt_dir=exact_attempt,
            output_dir=exact_output,
            expected_attempt_bindings=expected_attempt_bindings,
            input_authority=expected_input_authority,
            code_freeze=expected_code_freeze,
            grant=grant,
        )
    )


def _mint_completed_reproduction_grant_v0(
    snapshot: D1AttemptWalSnapshotV0,
) -> D2CompletedReproductionGrantV0:
    if type(snapshot) is not D1AttemptWalSnapshotV0:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction WAL snapshot has the wrong exact type"
        )
    if (
        snapshot.torn_tail is not None
        or snapshot.start_seal_torn
        or not snapshot.start_seal_valid
        or snapshot.total_file_bytes != snapshot.prefix.complete_bytes
        or len(snapshot.records) != 3
        or tuple(value.state for value in snapshot.records)
        != ("ARMED", "STARTED_BEFORE_OUTCOME_ACCESS", "COMPLETED")
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction requires one exact untorn ARMED-STARTED-COMPLETED WAL"
        )
    armed, started, completed = snapshot.records
    seal = snapshot.start_seal
    assert seal is not None
    if (
        started.sequence != 1
        or completed.sequence != 2
        or started.record_sha256 != seal.start_record_sha256
        or started.bindings_sha256 != seal.bindings_sha256
        or started.attempt_directory_sha256 != seal.attempt_directory_sha256
        or started.observed_at_ms != seal.started_at_ms
        or completed.detail_code is not None
        or completed.result_sha256 is None
        or completed.artifact_manifest_sha256 is None
        or armed.bindings != snapshot.bindings
        or started.bindings != snapshot.bindings
        or completed.bindings != snapshot.bindings
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction COMPLETED WAL or START seal bindings differ"
        )
    grant = object.__new__(D2CompletedReproductionGrantV0)
    object.__setattr__(grant, "_bindings", snapshot.bindings)
    object.__setattr__(grant, "_run_started_at_ms", started.observed_at_ms)
    object.__setattr__(grant, "_start_record_sha256", started.record_sha256)
    object.__setattr__(grant, "_completed_record_sha256", completed.record_sha256)
    object.__setattr__(grant, "_attempt_directory_sha256", started.attempt_directory_sha256)
    object.__setattr__(grant, "_published_result_sha256", completed.result_sha256)
    object.__setattr__(
        grant,
        "_published_artifact_manifest_sha256",
        completed.artifact_manifest_sha256,
    )
    object.__setattr__(grant, "_consume_lock", threading.Lock())
    object.__setattr__(grant, "_consumed", False)
    object.__setattr__(grant, "_mint_process_id", os.getpid())
    return grant


def _reproduce_d2_after_completed_grant_v0(
    *,
    data_root: str | Path,
    attempt_dir: str | Path,
    output_dir: str | Path,
    expected_attempt_bindings: D1AttemptWalBindingsV0,
    input_authority: D2HistoricalInputAuthorityV0,
    code_freeze: D2HistoricalDevelopmentFreezeV0,
    grant: D2CompletedReproductionGrantV0,
) -> D2HistoricalReproductionVerificationV0:
    if not grant.consumed:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction grant must be consumed before any replay read"
        )
    bindings = grant.bindings
    target = Path(os.path.abspath(Path(output_dir)))
    expected_names = frozenset(
        {
            "censors.jsonl",
            "code-freeze-receipt.jsonl",
            "derived-hourly-manifests.jsonl",
            "episodes.jsonl",
            "input-authority.jsonl",
            "manifest.jsonl",
            "report.md",
            "result-index.jsonl",
            "summary.jsonl",
        }
    )
    replay_tree_before = _d2_artifact_tree_snapshot_v0(target)
    replay_members_before, replay_stated_total = _d2_artifact_member_snapshot_v0(
        target,
        expected_names=expected_names,
    )
    attempt_target = Path(os.path.abspath(Path(attempt_dir)))
    attempt_names = _d2_directory_names_v0(
        attempt_target,
        label="D2 reproduction attempt directory",
    )
    attempt_tree_before = _d2_artifact_tree_snapshot_v0(attempt_target)
    attempt_members_before, attempt_stated_total = _d2_artifact_member_snapshot_v0(
        attempt_target,
        expected_names=attempt_names,
    )
    before = verify_d2_historical_published_artifact_bundle_v0(
        output_dir=output_dir,
        expected_result_sha256=grant.published_result_sha256,
        expected_manifest_sha256=grant.published_artifact_manifest_sha256,
        expected_input_authority=input_authority,
        expected_code_freeze=code_freeze,
        expected_run_id=bindings.run_id,
        expected_run_started_at_ms=grant.run_started_at_ms,
        expected_start_record_sha256=grant.start_record_sha256,
        expected_attempt_directory_sha256=grant.attempt_directory_sha256,
        expected_attempt_bindings_sha256=bindings.bindings_sha256,
    )
    with localcontext(protocol_decimal_context_v2()):
        manifests: list[D2DerivedHourlyManifestV0] = []
        core = run_d1_historical_replay_core_v0(
            symbol_inputs=_iter_authenticated_d2_replay_inputs_v0(
                data_root=data_root,
                input_authority=input_authority,
                derived_manifests=manifests,
            ),
            run_id=bindings.run_id,
            decision_start_ms=D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
            decision_end_ms=D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
        )
        reproduced = D2HistoricalDevelopmentResultV0(
            run_id=bindings.run_id,
            run_started_at_ms=grant.run_started_at_ms,
            start_record_sha256=grant.start_record_sha256,
            attempt_directory_sha256=grant.attempt_directory_sha256,
            attempt_bindings_sha256=bindings.bindings_sha256,
            input_authority_sha256=input_authority.authority_sha256,
            input_authority_file_sha256=bindings.input_authority_file_sha256,
            code_freeze_manifest_sha256=code_freeze.manifest_sha256,
            code_freeze_receipt_sha256=code_freeze.receipt_sha256,
            derived_hourly_manifests=tuple(manifests),
            episodes=core.episodes,
            censors=core.censors,
            summary=core.summary,
            _factory_token=_RESULT_FACTORY_TOKEN,
        )
    payloads = _artifact_payloads_v0(
        result=reproduced,
        input_authority=input_authority,
        code_freeze=code_freeze,
    )
    payload_metadata = {
        name: (hashlib.sha256(raw).hexdigest(), len(raw)) for name, raw in payloads
    }
    manifest_raw = _artifact_manifest_raw_v0(reproduced, payload_metadata)
    reproduced_raw = dict(payloads)
    reproduced_raw["manifest.jsonl"] = manifest_raw
    if (
        reproduced.result_sha256 != grant.published_result_sha256
        or hashlib.sha256(manifest_raw).hexdigest()
        != grant.published_artifact_manifest_sha256
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "raw D2 reproduction result or artifact manifest hash differs from COMPLETED"
        )
    after = verify_d2_historical_published_artifact_bundle_v0(
        output_dir=output_dir,
        expected_result_sha256=grant.published_result_sha256,
        expected_manifest_sha256=grant.published_artifact_manifest_sha256,
        expected_input_authority=input_authority,
        expected_code_freeze=code_freeze,
        expected_run_id=bindings.run_id,
        expected_run_started_at_ms=grant.run_started_at_ms,
        expected_start_record_sha256=grant.start_record_sha256,
        expected_attempt_directory_sha256=grant.attempt_directory_sha256,
        expected_attempt_bindings_sha256=bindings.bindings_sha256,
    )
    if after != before:
        raise D2HistoricalDevelopmentContractErrorV0(
            "published D2 verification receipt changed across raw reproduction"
        )
    if frozenset(reproduced_raw) != expected_names:
        raise D2HistoricalDevelopmentContractErrorV0(
            "raw D2 reproduction produced an unexpected artifact membership"
        )
    tree_before = _d2_artifact_tree_snapshot_v0(target)
    _members, stated_total = _d2_artifact_member_snapshot_v0(
        target,
        expected_names=expected_names,
    )
    published_raw = {
        name: _read_d2_artifact_member_v0(target, name) for name in expected_names
    }
    tree_after = _d2_artifact_tree_snapshot_v0(target)
    replay_members_after, replay_final_total = _d2_artifact_member_snapshot_v0(
        target,
        expected_names=expected_names,
    )
    if (
        tree_after != tree_before
        or sum(len(raw) for raw in published_raw.values()) != stated_total
        or published_raw != reproduced_raw
        or tree_after != replay_tree_before
        or replay_members_after != replay_members_before
        or replay_final_total != replay_stated_total
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "raw D2 reproduction bytes differ from the stable published bundle"
        )
    try:
        final_snapshot = load_attempt_wal_v0(
            attempt_target,
            expected_bindings=expected_attempt_bindings,
        )
    except D1HistoricalAttemptWalErrorV0 as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction attempt WAL changed during raw replay"
        ) from error
    final_grant = _mint_completed_reproduction_grant_v0(final_snapshot)
    attempt_tree_after = _d2_artifact_tree_snapshot_v0(attempt_target)
    attempt_members_after, attempt_final_total = _d2_artifact_member_snapshot_v0(
        attempt_target,
        expected_names=attempt_names,
    )
    if (
        final_grant.bindings != grant.bindings
        or final_grant.run_started_at_ms != grant.run_started_at_ms
        or final_grant.start_record_sha256 != grant.start_record_sha256
        or final_grant.completed_record_sha256 != grant.completed_record_sha256
        or final_grant.attempt_directory_sha256 != grant.attempt_directory_sha256
        or final_grant.published_result_sha256 != grant.published_result_sha256
        or final_grant.published_artifact_manifest_sha256
        != grant.published_artifact_manifest_sha256
        or attempt_tree_after != attempt_tree_before
        or attempt_members_after != attempt_members_before
        or attempt_final_total != attempt_stated_total
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 reproduction attempt identity changed during raw replay"
        )
    return D2HistoricalReproductionVerificationV0(
        run_id=bindings.run_id,
        run_started_at_ms=grant.run_started_at_ms,
        start_record_sha256=grant.start_record_sha256,
        completed_record_sha256=grant.completed_record_sha256,
        result_sha256=reproduced.result_sha256,
        artifact_manifest_sha256=hashlib.sha256(manifest_raw).hexdigest(),
        summary_sha256=reproduced.summary.summary_sha256,
        derived_manifest_sequence_root_sha256=after.derived_manifest_sequence_root_sha256,
        episode_sequence_root_sha256=after.episode_sequence_root_sha256,
        censor_sequence_root_sha256=after.censor_sequence_root_sha256,
        episode_count=len(reproduced.episodes),
        censor_count=len(reproduced.censors),
        _factory_token=_REPRODUCTION_VERIFICATION_FACTORY_TOKEN,
    )


def _run_d2_after_outcome_access_v0(
    *,
    data_root: str | Path,
    input_authority: D2HistoricalInputAuthorityV0,
    code_freeze: D2HistoricalDevelopmentFreezeV0,
    outcome_access_grant: D1OutcomeAccessGrantV0,
    run_id: str,
    run_started_at_ms: int,
    input_authority_file_sha256: str,
) -> D2HistoricalDevelopmentResultV0:
    with localcontext(protocol_decimal_context_v2()):
        manifests: list[D2DerivedHourlyManifestV0] = []
        core = run_d1_historical_replay_core_v0(
            symbol_inputs=_iter_authenticated_d2_replay_inputs_v0(
                data_root=data_root,
                input_authority=input_authority,
                derived_manifests=manifests,
            ),
            run_id=run_id,
            decision_start_ms=D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
            decision_end_ms=D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
        )
        if type(core) is not D1HistoricalReplayCoreResultV0:
            raise D2HistoricalDevelopmentContractErrorV0(
                "shared historical replay returned an unsupported result type"
            )
        if tuple(value.symbol for value in manifests) != D1_HISTORICAL_UNIVERSE_V0:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 replay did not produce the exact ordered derived-manifest census"
            )
        bindings = outcome_access_grant.bindings
        return D2HistoricalDevelopmentResultV0(
            run_id=run_id,
            run_started_at_ms=run_started_at_ms,
            start_record_sha256=outcome_access_grant.start_record_sha256,
            attempt_directory_sha256=outcome_access_grant.attempt_directory_sha256,
            attempt_bindings_sha256=bindings.bindings_sha256,
            input_authority_sha256=input_authority.authority_sha256,
            input_authority_file_sha256=input_authority_file_sha256,
            code_freeze_manifest_sha256=code_freeze.manifest_sha256,
            code_freeze_receipt_sha256=code_freeze.receipt_sha256,
            derived_hourly_manifests=tuple(manifests),
            episodes=core.episodes,
            censors=core.censors,
            summary=core.summary,
            _factory_token=_RESULT_FACTORY_TOKEN,
        )


def _iter_authenticated_d2_replay_inputs_v0(
    *,
    data_root: str | Path,
    input_authority: D2HistoricalInputAuthorityV0,
    derived_manifests: list[D2DerivedHourlyManifestV0],
) -> Iterable[D1HistoricalReplaySymbolInputV0]:
    """Open funding and one 5m symbol at a time; native 1h has no code path."""

    canonical_d2_historical_input_authority_v0(input_authority)
    try:
        funding_panel = load_d1_historical_authenticated_funding_bindings_v0(
            data_root=data_root,
            funding_manifest_relative_path=input_authority.funding_manifest_relative_path,
            funding_manifest_sha256=input_authority.funding_manifest_sha256,
            funding_files=input_authority.funding_files,
        )
    except D1HistoricalDevelopmentContractErrorV0 as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 funding authority or authenticated funding input failed"
        ) from error
    if (
        type(funding_panel) is not tuple
        or tuple(value.symbol for value in funding_panel) != D1_HISTORICAL_UNIVERSE_V0
        or any(type(value) is not D1HistoricalAuthenticatedFundingV0 for value in funding_panel)
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 funding loader returned a noncanonical symbol panel"
        )
    funding_by_symbol = {value.symbol: value for value in funding_panel}
    for symbol in D1_HISTORICAL_UNIVERSE_V0:
        binding = input_authority.five_minute_binding(symbol)
        try:
            five = load_d1_historical_authenticated_five_minute_v0(
                data_root=data_root,
                binding=binding,
            )
        except D1HistoricalDevelopmentContractErrorV0 as error:
            raise D2HistoricalDevelopmentContractErrorV0(
                f"D2 authenticated 5m input failed for {symbol}"
            ) from error
        if type(five) is not D1HistoricalAuthenticatedFiveMinuteV0:
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 5m loader returned a noncanonical authenticated value"
            )
        try:
            panel = derive_d2_closed_hourly_v0(
                symbol=symbol,
                five_minute_candles=five.candles,
                five_minute_manifest_sha256=five.manifest_sha256,
                five_minute_compressed_data_sha256=five.data_sha256,
            )
        except ValueError as error:
            raise D2HistoricalDevelopmentContractErrorV0(
                f"D2 closed-hour derivation failed for {symbol}"
            ) from error
        proof = _build_replay_proof_v0(
            input_authority=input_authority,
            five=five,
            funding=funding_by_symbol[symbol],
            derived_hourly=panel,
        )
        # Revalidate immediately before the caller's shared-core iteration.
        _validate_replay_proof_v0(proof)
        derived_manifests.append(proof.derived_hourly.manifest)
        yield proof.replay_input


def _build_replay_proof_v0(
    *,
    input_authority: D2HistoricalInputAuthorityV0,
    five: D1HistoricalAuthenticatedFiveMinuteV0,
    funding: D1HistoricalAuthenticatedFundingV0,
    derived_hourly: D2DerivedHourlyPanelV0,
) -> _D2HistoricalReplaySymbolProofV0:
    symbol = five.symbol
    source_root = _d2_source_root_v0(
        input_authority_sha256=input_authority.authority_sha256,
        five=five,
        funding=funding,
        derived_hourly=derived_hourly,
    )
    replay = D1HistoricalReplaySymbolInputV0(
        symbol=symbol,
        five_minute_manifest_sha256=five.manifest_sha256,
        higher_timeframe_source_sha256=derived_hourly.manifest.manifest_sha256,
        funding_file_sha256=funding.file_sha256,
        source_root_sha256=source_root,
        five_minute=five.candles,
        hourly=derived_hourly.candles,
        funding=funding.points,
        exact_standard_8h_development_funding_coverage=(
            funding.exact_standard_8h_development_coverage
        ),
        source_root_policy=D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0,
    )
    return _D2HistoricalReplaySymbolProofV0(
        input_authority_sha256=input_authority.authority_sha256,
        five_minute_authority_sha256=(input_authority.five_minute_binding(symbol).manifest_sha256),
        funding_authority_sha256=input_authority.funding_files[
            D1_HISTORICAL_UNIVERSE_V0.index(symbol)
        ].sha256,
        five_minute=five,
        funding=funding,
        derived_hourly=derived_hourly,
        replay_input=replay,
        _factory_token=_PROOF_FACTORY_TOKEN,
    )


def _validate_replay_proof_v0(proof: _D2HistoricalReplaySymbolProofV0) -> None:
    if type(proof.input_authority_sha256) is not str:
        raise D2HistoricalDevelopmentContractErrorV0("D2 replay proof authority is invalid")
    _require_sha256(proof.input_authority_sha256, "proof input authority sha256")
    _require_sha256(
        proof.five_minute_authority_sha256,
        "proof 5m authority sha256",
    )
    _require_sha256(
        proof.funding_authority_sha256,
        "proof funding authority sha256",
    )
    if type(proof.five_minute) is not D1HistoricalAuthenticatedFiveMinuteV0:
        raise D2HistoricalDevelopmentContractErrorV0("D2 replay proof 5m value is invalid")
    if type(proof.funding) is not D1HistoricalAuthenticatedFundingV0:
        raise D2HistoricalDevelopmentContractErrorV0("D2 replay proof funding value is invalid")
    if type(proof.derived_hourly) is not D2DerivedHourlyPanelV0:
        raise D2HistoricalDevelopmentContractErrorV0("D2 replay proof hourly value is invalid")
    if type(proof.replay_input) is not D1HistoricalReplaySymbolInputV0:
        raise D2HistoricalDevelopmentContractErrorV0("D2 replay proof core input is invalid")
    validate_d2_derived_hourly_panel_v0(proof.derived_hourly)
    replay = proof.replay_input
    if not (
        proof.five_minute.symbol
        == proof.funding.symbol
        == proof.derived_hourly.manifest.symbol
        == replay.symbol
        and replay.five_minute_manifest_sha256 == proof.five_minute.manifest_sha256
        and proof.five_minute.manifest_sha256 == proof.five_minute_authority_sha256
        and replay.higher_timeframe_source_sha256 == proof.derived_hourly.manifest.manifest_sha256
        and replay.funding_file_sha256 == proof.funding.file_sha256
        and proof.funding.file_sha256 == proof.funding_authority_sha256
        and replay.five_minute is proof.five_minute.candles
        and replay.hourly is proof.derived_hourly.candles
        and replay.funding is proof.funding.points
        and replay.exact_standard_8h_development_funding_coverage
        is proof.funding.exact_standard_8h_development_coverage
        and replay.source_root_policy
        == D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 replay proof does not preserve authenticated input identity"
        )
    expected_root = _d2_source_root_v0(
        input_authority_sha256=proof.input_authority_sha256,
        five=proof.five_minute,
        funding=proof.funding,
        derived_hourly=proof.derived_hourly,
    )
    if replay.source_root_sha256 != expected_root:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 replay source root differs from its complete proof"
        )


def _d2_source_root_v0(
    *,
    input_authority_sha256: str,
    five: D1HistoricalAuthenticatedFiveMinuteV0,
    funding: D1HistoricalAuthenticatedFundingV0,
    derived_hourly: D2DerivedHourlyPanelV0,
) -> str:
    validate_d2_derived_hourly_panel_v0(derived_hourly)
    return _hash_document(
        _D2_SOURCE_ROOT_HASH_DOMAIN,
        {
            "derived_hour_manifest_sha256": derived_hourly.manifest.manifest_sha256,
            "derived_hour_sequence_root_sha256": (
                derived_hourly.manifest.ordered_canonical_sequence_root_sha256
            ),
            "derivation_policy_version": D2_HISTORICAL_DERIVATION_POLICY_V0,
            "five_minute_compressed_data_sha256": five.data_sha256,
            "five_minute_manifest_sha256": five.manifest_sha256,
            "funding_file_sha256": funding.file_sha256,
            "input_authority_sha256": input_authority_sha256,
            "source_policy_version": D2_HISTORICAL_SOURCE_POLICY_V0,
            "symbol": five.symbol,
        },
    )


def _validate_outcome_grant_v0(
    grant: D1OutcomeAccessGrantV0,
    *,
    run_id: str,
    input_authority: D2HistoricalInputAuthorityV0,
    input_authority_raw: bytes,
    code_freeze: D2HistoricalDevelopmentFreezeV0,
) -> None:
    if type(grant) is not D1OutcomeAccessGrantV0:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 outcome access requires an exact fresh durable-START grant"
        )
    if grant.consumed:
        raise D2HistoricalDevelopmentContractErrorV0("D2 outcome access grant was already consumed")
    bindings = grant.bindings
    if type(bindings) is not D1AttemptWalBindingsV0:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 outcome grant bindings have the wrong validated type"
        )
    expected_authority_file_sha256 = hashlib.sha256(input_authority_raw).hexdigest()
    if not (
        bindings.run_id == run_id
        and bindings.code_freeze_manifest_sha256 == code_freeze.manifest_sha256
        and bindings.input_authority_sha256 == input_authority.authority_sha256
        and bindings.input_authority_file_sha256 == expected_authority_file_sha256
        and bindings.funding_authority_file_sha256 == input_authority.funding_manifest_sha256
        and bindings.preregistration_sha256 == D2_HISTORICAL_PREREGISTRATION_SHA256_V0
        and grant.start_prefix.record_count == 2
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 outcome grant differs from the run, freeze, or input authority"
        )
    _require_sha256(grant.start_record_sha256, "START record sha256")
    _require_sha256(grant.attempt_directory_sha256, "attempt directory sha256")


def _validate_result_members_v0(value: D2HistoricalDevelopmentResultV0) -> None:
    _require_identity(value.run_id, "result run_id")
    _require_nonnegative_int(value.run_started_at_ms, "result run_started_at_ms")
    for digest, label in (
        (value.start_record_sha256, "result START record"),
        (value.attempt_directory_sha256, "result attempt directory"),
        (value.attempt_bindings_sha256, "result attempt bindings"),
        (value.input_authority_sha256, "result input authority"),
        (value.input_authority_file_sha256, "result input authority file"),
        (value.code_freeze_manifest_sha256, "result freeze manifest"),
        (value.code_freeze_receipt_sha256, "result freeze receipt"),
        (value.preregistration_sha256, "result preregistration"),
        (value.operator_amendment_sha256, "result operator amendment"),
        (value.operator_correction_a1_sha256, "result operator correction A1"),
    ):
        _require_sha256(digest, label)
    if (
        type(value.derived_hourly_manifests) is not tuple
        or tuple(item.symbol for item in value.derived_hourly_manifests)
        != D1_HISTORICAL_UNIVERSE_V0
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "result requires one exact ordered derived-hour manifest per symbol"
        )
    _validate_production_derived_manifests_v0(value.derived_hourly_manifests)
    if (
        type(value.episodes) is not tuple
        or len(value.episodes) > D1_HISTORICAL_MAX_EPISODES_V0
        or any(type(item) is not D1HistoricalEpisodeV0 for item in value.episodes)
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 result episodes are not one bounded immutable shared-core tuple"
        )
    if (
        type(value.censors) is not tuple
        or len(value.censors) > D1_HISTORICAL_MAX_CENSORS_V0
        or any(type(item) is not D1HistoricalCensorV0 for item in value.censors)
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 result censors are not one bounded immutable shared-core tuple"
        )
    for episode in value.episodes:
        canonical_d1_historical_episode_v0(episode)
    for censor in value.censors:
        canonical_d1_historical_censor_v0(censor)
    canonical_d1_historical_summary_v0(value.summary)
    if (
        len(value.episodes) != value.summary.episode_count
        or len(value.censors) != value.summary.right_edge_censor_count
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 result shared-core records do not reconcile with summary"
        )
    manifest_by_symbol = {item.symbol: item for item in value.derived_hourly_manifests}
    five_minute_by_symbol = {
        symbol: manifest_sha256
        for symbol, _relative_path, manifest_sha256 in (
            D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0
        )
    }
    funding_by_symbol = {
        symbol: sha256 for symbol, _relative_path, sha256 in D2_HISTORICAL_FIXED_FUNDING_FILES_V0
    }
    for episode in value.episodes:
        if (
            episode.hourly_manifest_sha256 != manifest_by_symbol[episode.symbol].manifest_sha256
            or episode.five_minute_manifest_sha256 != five_minute_by_symbol[episode.symbol]
            or episode.funding_file_sha256 != funding_by_symbol[episode.symbol]
        ):
            raise D2HistoricalDevelopmentContractErrorV0(
                "D2 episode provenance differs from its fixed and derived authorities"
            )
    false_claims = (
        value.post_development_end_rows_used,
        value.existing_result_artifact_used_as_input,
        value.historical_bbo_available,
        value.paper_fill_claim,
        value.execution_conclusive,
        value.probability_claim,
        value.efficacy_claim,
        value.promoting,
        value.prospective,
        value.production_order_placement,
    )
    if any(item is not False for item in false_claims):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 historical result non-claims must remain exact false values"
        )
    if (
        value.rule_version != D2_HISTORICAL_DEVELOPMENT_RULE_V0
        or value.economic_rule_version != D1_SCEFB_RULE_VERSION_V0
        or value.source_policy_version != D2_HISTORICAL_SOURCE_POLICY_V0
        or value.decision_source_root_policy
        != D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0
        or value.derivation_policy_version != D2_HISTORICAL_DERIVATION_POLICY_V0
        or value.preregistration_sha256 != D2_HISTORICAL_PREREGISTRATION_SHA256_V0
        or value.operator_amendment_sha256 != D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0
        or value.operator_correction_a1_sha256 != D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0
        or value.status != D2_HISTORICAL_RESULT_STATUS_V0
        or value.schema_version != _D2_RESULT_SCHEMA_V0
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 result protocol fields differ from the preregistration"
        )


def _validate_production_derived_manifests_v0(
    manifests: tuple[D2DerivedHourlyManifestV0, ...],
) -> None:
    """Require the exact production-shaped derived census even with zero episodes."""

    if (
        type(manifests) is not tuple
        or tuple(value.symbol for value in manifests) != D1_HISTORICAL_UNIVERSE_V0
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 derived manifests must match the exact ordered production universe"
        )
    fixed_five_minute = {
        symbol: sha256
        for symbol, _relative_path, sha256 in D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0
    }
    for value in manifests:
        canonical_d2_derived_hourly_manifest_v0(value)
        if not (
            value.five_minute_manifest_sha256 == fixed_five_minute[value.symbol]
            and value.source_first_open_time_ms == D1_HISTORICAL_DATA_START_MS_V0
            and value.source_last_close_time_ms == D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1
            and value.source_row_count == D1_HISTORICAL_FIVE_MINUTE_ROW_COUNT_V0
            and value.derived_first_open_time_ms == D1_HISTORICAL_DATA_START_MS_V0
            and value.derived_last_close_time_ms == D1_HISTORICAL_DEVELOPMENT_END_MS_V0 - 1
            and value.derived_row_count == D1_HISTORICAL_HOURLY_ROW_COUNT_V0
            and value.derivation_policy_version == D2_HISTORICAL_DERIVATION_POLICY_V0
            and value.historical_receipt_convention == D1_HISTORICAL_RECEIPT_CONVENTION_V0
        ):
            raise D2HistoricalDevelopmentContractErrorV0(
                f"D2 derived manifest for {value.symbol} differs from the production authority"
            )


def _validate_freeze_authority_v0(
    authority: DownstreamCodeFreezeAuthorityV1,
    *,
    input_authority_sha256: str,
) -> D2HistoricalDevelopmentFreezeV0:
    if type(authority) is not DownstreamCodeFreezeAuthorityV1:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 freeze authority has the wrong validated type"
        )
    upstream = _freeze_upstream_v0(input_authority_sha256)
    expected_file_hashes = {
        _D2_PREREGISTRATION_RELATIVE_PATH: D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
        _D2_OPERATOR_AMENDMENT_RELATIVE_PATH: D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0,
        _D2_OPERATOR_CORRECTION_A1_RELATIVE_PATH: (D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0),
        _D1_ECONOMIC_PREREGISTRATION_RELATIVE_PATH: (D1_PREDECESSOR_PREREGISTRATION_SHA256_V0),
        _D1_PREDECESSOR_FREEZE_RELATIVE_PATH: D1_PREDECESSOR_FREEZE_SHA256_V0,
        _D1_FAILURE_EVIDENCE_MANIFEST_RELATIVE_PATH: (
            D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0
        ),
    }
    policy_exact = (
        authority.purpose == D2_DEVELOPMENT_FREEZE_PURPOSE_V0
        and authority.include_trees == D2_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0
        and authority.include_files == D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0
        and authority.included_suffixes == D2_DEVELOPMENT_FREEZE_SUFFIXES_V0
        and dict(authority.upstream_sha256) == upstream
        and all(
            authority.file_sha256.get(path) == digest
            for path, digest in expected_file_hashes.items()
        )
        and all(
            path in authority.file_sha256
            for path in (
                _D2_RUNNER_RELATIVE_PATH,
                _D2_AUTHORITY_RELATIVE_PATH,
                _D1_RULE_RELATIVE_PATH,
            )
        )
    )
    if not policy_exact:
        raise D2HistoricalDevelopmentContractErrorV0(
            "loaded code freeze differs from the exact D2 development policy"
        )
    try:
        created = datetime.fromisoformat(authority.created_at_utc)
    except ValueError as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 freeze created_at_utc is invalid"
        ) from error
    if created.tzinfo is None or created.utcoffset() != UTC.utcoffset(created):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 freeze created_at_utc must be timezone-aware UTC"
        )
    return D2HistoricalDevelopmentFreezeV0(
        manifest_sha256=authority.manifest_sha256,
        manifest_created_at_ms=int(created.timestamp() * 1_000),
        input_authority_sha256=input_authority_sha256,
        frozen_file_count=len(authority.file_sha256),
        _factory_token=_FREEZE_FACTORY_TOKEN,
    )


def _freeze_upstream_v0(input_authority_sha256: str) -> dict[str, str]:
    _require_sha256(input_authority_sha256, "D2 input authority sha256")
    return {
        "d1_economic_preregistration": D1_PREDECESSOR_PREREGISTRATION_SHA256_V0,
        "d1_failure_evidence_archive": D1_PREDECESSOR_FAILURE_EVIDENCE_ARCHIVE_SHA256_V0,
        "d1_failure_evidence_manifest": D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0,
        "d1_predecessor_freeze_002": D1_PREDECESSOR_FREEZE_SHA256_V0,
        "d2_input_authority": input_authority_sha256,
        "d2_operator_amendment": D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0,
        "d2_operator_correction_a1": D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0,
        "d2_preregistration": D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
        "d2_source_policy": d2_source_policy_sha256_v0(),
    }


def _freeze_document_v0(value: D2HistoricalDevelopmentFreezeV0) -> dict[str, object]:
    return {
        "frozen_file_count": value.frozen_file_count,
        "input_authority_sha256": value.input_authority_sha256,
        "manifest_created_at_ms": value.manifest_created_at_ms,
        "manifest_sha256": value.manifest_sha256,
        "operator_amendment_sha256": value.operator_amendment_sha256,
        "operator_correction_a1_sha256": value.operator_correction_a1_sha256,
        "preregistration_sha256": value.preregistration_sha256,
        "schema_version": value.schema_version,
        "source_policy_sha256": value.source_policy_sha256,
    }


def _result_document_v0(
    value: D2HistoricalDevelopmentResultV0,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "adaptation_label": value.adaptation_label,
        "attempt_bindings_sha256": value.attempt_bindings_sha256,
        "attempt_directory_sha256": value.attempt_directory_sha256,
        "censor_count": len(value.censors),
        "censor_sequence_root_sha256": _ordered_hash_root_v0(
            _D2_CENSOR_SEQUENCE_ROOT_DOMAIN,
            tuple(item.censor_sha256 for item in value.censors),
        ),
        "code_freeze_manifest_sha256": value.code_freeze_manifest_sha256,
        "code_freeze_receipt_sha256": value.code_freeze_receipt_sha256,
        "derivation_policy_version": value.derivation_policy_version,
        "decision_source_root_policy": value.decision_source_root_policy,
        "derived_hour_manifest_count": len(value.derived_hourly_manifests),
        "derived_hour_manifest_sequence_root_sha256": _derived_manifest_root_v0(
            value.derived_hourly_manifests
        ),
        "development_end_ms_exclusive": value.development_end_ms_exclusive,
        "development_start_ms": value.development_start_ms,
        "economic_rule_version": value.economic_rule_version,
        "efficacy_claim": value.efficacy_claim,
        "episode_count": len(value.episodes),
        "episode_sequence_root_sha256": _ordered_hash_root_v0(
            _D2_EPISODE_SEQUENCE_ROOT_DOMAIN,
            tuple(item.episode_sha256 for item in value.episodes),
        ),
        "execution_conclusive": value.execution_conclusive,
        "existing_result_artifact_used_as_input": (value.existing_result_artifact_used_as_input),
        "historical_bbo_available": value.historical_bbo_available,
        "historical_receipt_convention": value.historical_receipt_convention,
        "historical_role": value.historical_role,
        "input_authority_file_sha256": value.input_authority_file_sha256,
        "input_authority_sha256": value.input_authority_sha256,
        "operator_amendment_sha256": value.operator_amendment_sha256,
        "operator_correction_a1_sha256": value.operator_correction_a1_sha256,
        "paper_fill_claim": value.paper_fill_claim,
        "post_development_end_rows_used": value.post_development_end_rows_used,
        "probability_claim": value.probability_claim,
        "production_order_placement": value.production_order_placement,
        "promoting": value.promoting,
        "prospective": value.prospective,
        "preregistration_sha256": value.preregistration_sha256,
        "rule_version": value.rule_version,
        "run_id": value.run_id,
        "run_started_at_ms": value.run_started_at_ms,
        "schema_version": value.schema_version,
        "source_policy_version": value.source_policy_version,
        "start_record_sha256": value.start_record_sha256,
        "status": value.status,
        "summary_sha256": value.summary.summary_sha256,
        "universe": list(value.universe),
    }
    if include_hash:
        document["result_sha256"] = value.result_sha256
    return document


def _artifact_payloads_v0(
    *,
    result: D2HistoricalDevelopmentResultV0,
    input_authority: D2HistoricalInputAuthorityV0,
    code_freeze: D2HistoricalDevelopmentFreezeV0,
) -> tuple[tuple[str, bytes], ...]:
    result_raw = canonical_d2_historical_development_result_v0(result)
    authority_raw = canonical_d2_historical_input_authority_v0(input_authority)
    freeze_raw = canonical_d2_historical_development_freeze_v0(code_freeze)
    if not (
        result.input_authority_sha256 == input_authority.authority_sha256
        and result.input_authority_file_sha256 == hashlib.sha256(authority_raw).hexdigest()
        and result.code_freeze_manifest_sha256 == code_freeze.manifest_sha256
        and result.code_freeze_receipt_sha256 == code_freeze.receipt_sha256
        and code_freeze.input_authority_sha256 == input_authority.authority_sha256
    ):
        raise D2HistoricalDevelopmentContractErrorV0(
            "D2 result, input authority, and code freeze bindings differ"
        )
    return (
        ("input-authority.jsonl", authority_raw),
        ("code-freeze-receipt.jsonl", freeze_raw),
        (
            "derived-hourly-manifests.jsonl",
            b"".join(
                canonical_d2_derived_hourly_manifest_v0(value)
                for value in result.derived_hourly_manifests
            ),
        ),
        (
            "episodes.jsonl",
            b"".join(canonical_d1_historical_episode_v0(value) for value in result.episodes),
        ),
        (
            "censors.jsonl",
            b"".join(canonical_d1_historical_censor_v0(value) for value in result.censors),
        ),
        ("summary.jsonl", canonical_d1_historical_summary_v0(result.summary)),
        ("result-index.jsonl", result_raw),
        ("report.md", _development_report_markdown_v0(result)),
    )


def _artifact_manifest_raw_v0(
    result: D2HistoricalDevelopmentResultV0,
    output_metadata: dict[str, tuple[str, int]],
) -> bytes:
    return canonical_json_line(
        {
            "durability_contract": d1_historical_artifact_durability_contract_v0(),
            "efficacy_claim": False,
            "execution_conclusive": False,
            "historical_bbo_available": False,
            "input_authority_sha256": result.input_authority_sha256,
            "outputs": {
                name: {"sha256": digest, "size_bytes": size}
                for name, (digest, size) in sorted(output_metadata.items())
            },
            "paper_fill_claim": False,
            "probability_claim": False,
            "production_order_placement": False,
            "promoting": False,
            "prospective": False,
            "protocol": D2_HISTORICAL_DEVELOPMENT_RULE_V0,
            "result_sha256": result.result_sha256,
            "schema_version": _D2_ARTIFACT_MANIFEST_SCHEMA_V0,
            "source_policy_version": D2_HISTORICAL_SOURCE_POLICY_V0,
            "status": D2_HISTORICAL_RESULT_STATUS_V0,
        }
    )


def _expected_artifact_manifest_for_result_v0(
    *,
    result: D2HistoricalDevelopmentResultV0,
    input_authority: D2HistoricalInputAuthorityV0,
    code_freeze: D2HistoricalDevelopmentFreezeV0,
) -> bytes:
    payloads = _artifact_payloads_v0(
        result=result,
        input_authority=input_authority,
        code_freeze=code_freeze,
    )
    metadata = {name: (hashlib.sha256(raw).hexdigest(), len(raw)) for name, raw in payloads}
    return _artifact_manifest_raw_v0(result, metadata)


def _require_real_d2_artifact_directory_v0(target: Path) -> None:
    try:
        _require_real_artifact_directory_v0(
            target,
            "serialized D2 artifact directory",
        )
    except D1HistoricalArtifactDurabilityErrorV0 as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact directory must be real and non-reparse"
        ) from error


def _d2_artifact_tree_snapshot_v0(
    target: Path,
) -> tuple[tuple[str, int, int, int, int, int], ...]:
    """Snapshot every existing directory component through the final bundle."""

    if not target.is_absolute():
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact directory must be absolute"
        )
    current = Path(target.anchor)
    ordered = [current]
    for part in target.parts[1:]:
        current /= part
        ordered.append(current)
    snapshot: list[tuple[str, int, int, int, int, int]] = []
    for component in ordered:
        try:
            metadata = _require_real_artifact_directory_v0(
                component,
                "serialized D2 artifact path component",
            )
        except D1HistoricalArtifactDurabilityErrorV0 as error:
            raise D2HistoricalDevelopmentContractErrorV0(
                "serialized D2 artifact path must contain only real non-reparse directories"
            ) from error
        snapshot.append(
            (
                os.path.normcase(str(component)),
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_mode,
                metadata.st_mtime_ns if component == target else 0,
                getattr(metadata, "st_file_attributes", 0),
            )
        )
    return tuple(snapshot)


def _d2_artifact_member_snapshot_v0(
    target: Path,
    *,
    expected_names: frozenset[str],
) -> tuple[tuple[tuple[str, tuple[int, int, int, int, int]], ...], int]:
    """Capture exact regular-file membership and its aggregate stated size."""

    try:
        with os.scandir(target) as entries:
            snapshot_entries = tuple(entries)
    except OSError as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            "cannot inspect the serialized D2 artifact directory"
        ) from error
    actual_names = frozenset(entry.name for entry in snapshot_entries)
    if actual_names != expected_names:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact membership differs"
        )
    members: list[tuple[str, tuple[int, int, int, int, int]]] = []
    total_bytes = 0
    for entry in sorted(snapshot_entries, key=lambda value: value.name):
        try:
            metadata = entry.stat(follow_symlinks=False)
        except OSError as error:
            raise D2HistoricalDevelopmentContractErrorV0(
                f"serialized D2 artifact {entry.name} is unavailable"
            ) from error
        if _is_link_or_reparse_v0(metadata) or not stat.S_ISREG(metadata.st_mode):
            raise D2HistoricalDevelopmentContractErrorV0(
                f"serialized D2 artifact {entry.name} is not an exact regular file"
            )
        if metadata.st_size < 0:
            raise D2HistoricalDevelopmentContractErrorV0(
                f"serialized D2 artifact {entry.name} has an invalid size"
            )
        total_bytes += metadata.st_size
        if total_bytes > D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0:
            raise D2HistoricalDevelopmentContractErrorV0(
                "serialized D2 artifact bundle exceeds its aggregate byte cap"
            )
        members.append((entry.name, _file_identity_v0(metadata)))
    return tuple(members), total_bytes


def _d2_directory_names_v0(path: Path, *, label: str) -> frozenset[str]:
    try:
        with os.scandir(path) as entries:
            names = frozenset(entry.name for entry in entries)
    except OSError as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            f"cannot inspect {label}"
        ) from error
    if not names:
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} must not be empty")
    return names


def _read_d2_artifact_member_v0(target: Path, name: str) -> bytes:
    try:
        return _read_exact_regular_file(
            target / name,
            f"serialized D2 artifact {name}",
            maximum_bytes=D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0,
        )
    except D1HistoricalDevelopmentContractErrorV0 as error:
        raise D2HistoricalDevelopmentContractErrorV0(
            f"serialized D2 artifact {name} is not an exact regular file"
        ) from error


def _decode_canonical_object_v0(raw: bytes, label: str) -> dict[str, object]:
    if not raw or not raw.endswith(b"\n"):
        raise D2HistoricalDevelopmentContractErrorV0(
            f"{label} must be one newline-terminated canonical object"
        )
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} is invalid JSON") from error
    if not isinstance(value, dict) or canonical_json_line(value) != raw:
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} is not canonical JSONL")
    return value


def _canonical_jsonl_lines_v0(
    raw: bytes,
    label: str,
    *,
    maximum_count: int,
    allow_empty: bool,
) -> tuple[bytes, ...]:
    _require_nonnegative_int(maximum_count, "serialized maximum_count")
    if not raw:
        if allow_empty:
            return ()
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} must not be empty")
    lines = tuple(raw.splitlines(keepends=True))
    if len(lines) > maximum_count:
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} exceeds its count cap")
    for line in lines:
        _decode_canonical_object_v0(line, label)
    return lines


def _parse_serialized_derived_manifests_v0(
    raw: bytes,
) -> tuple[D2DerivedHourlyManifestV0, ...]:
    lines = _canonical_jsonl_lines_v0(
        raw,
        "serialized D2 derived-hour manifest",
        maximum_count=len(D1_HISTORICAL_UNIVERSE_V0),
        allow_empty=False,
    )
    expected_keys = {
        "derivation_policy_version",
        "derived_first_open_time_ms",
        "derived_last_close_time_ms",
        "derived_row_count",
        "five_minute_compressed_data_sha256",
        "five_minute_manifest_sha256",
        "historical_receipt_convention",
        "manifest_sha256",
        "ordered_canonical_sequence_root_sha256",
        "schema_version",
        "source_first_open_time_ms",
        "source_last_close_time_ms",
        "source_row_count",
        "symbol",
    }
    manifests: list[D2DerivedHourlyManifestV0] = []
    for line in lines:
        document = _decode_canonical_object_v0(
            line,
            "serialized D2 derived-hour manifest",
        )
        if set(document) != expected_keys:
            raise D2HistoricalDevelopmentContractErrorV0(
                "serialized D2 derived-hour manifest fields differ"
            )
        manifest = D2DerivedHourlyManifestV0(
            symbol=_serialized_text_v0(document["symbol"], "derived symbol"),
            five_minute_manifest_sha256=_serialized_sha256_v0(
                document["five_minute_manifest_sha256"],
                "derived 5m manifest",
            ),
            five_minute_compressed_data_sha256=_serialized_sha256_v0(
                document["five_minute_compressed_data_sha256"],
                "derived 5m data",
            ),
            source_first_open_time_ms=_serialized_nonnegative_int_v0(
                document["source_first_open_time_ms"],
                "derived source first open",
            ),
            source_last_close_time_ms=_serialized_nonnegative_int_v0(
                document["source_last_close_time_ms"],
                "derived source last close",
            ),
            source_row_count=_serialized_positive_int_v0(
                document["source_row_count"],
                "derived source row count",
            ),
            derived_first_open_time_ms=_serialized_nonnegative_int_v0(
                document["derived_first_open_time_ms"],
                "derived first open",
            ),
            derived_last_close_time_ms=_serialized_nonnegative_int_v0(
                document["derived_last_close_time_ms"],
                "derived last close",
            ),
            derived_row_count=_serialized_positive_int_v0(
                document["derived_row_count"],
                "derived row count",
            ),
            ordered_canonical_sequence_root_sha256=_serialized_sha256_v0(
                document["ordered_canonical_sequence_root_sha256"],
                "derived ordered sequence root",
            ),
            _factory_token=_DERIVED_MANIFEST_FACTORY_TOKEN,
        )
        if canonical_d2_derived_hourly_manifest_v0(manifest) != line:
            raise D2HistoricalDevelopmentContractErrorV0(
                "serialized D2 derived-hour manifest hash or protocol differs"
            )
        manifests.append(manifest)
    snapshot = tuple(manifests)
    if tuple(value.symbol for value in snapshot) != D1_HISTORICAL_UNIVERSE_V0:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 derived-hour manifests differ from the exact universe order"
        )
    _validate_production_derived_manifests_v0(snapshot)
    return snapshot


def _validate_serialized_artifact_manifest_v0(
    document: dict[str, object],
    *,
    expected_result_sha256: str,
    expected_input_authority_sha256: str,
    output_metadata: dict[str, tuple[str, int]],
) -> None:
    expected_keys = {
        "durability_contract",
        "efficacy_claim",
        "execution_conclusive",
        "historical_bbo_available",
        "input_authority_sha256",
        "outputs",
        "paper_fill_claim",
        "probability_claim",
        "production_order_placement",
        "promoting",
        "prospective",
        "protocol",
        "result_sha256",
        "schema_version",
        "source_policy_version",
        "status",
    }
    if set(document) != expected_keys:
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact manifest fields differ"
        )
    for name in (
        "efficacy_claim",
        "execution_conclusive",
        "historical_bbo_available",
        "paper_fill_claim",
        "probability_claim",
        "production_order_placement",
        "promoting",
        "prospective",
    ):
        if document[name] is not False:
            raise D2HistoricalDevelopmentContractErrorV0(
                f"serialized D2 artifact manifest false claim differs: {name}"
            )
    expected_fixed = {
        "durability_contract": d1_historical_artifact_durability_contract_v0(),
        "input_authority_sha256": expected_input_authority_sha256,
        "protocol": D2_HISTORICAL_DEVELOPMENT_RULE_V0,
        "result_sha256": expected_result_sha256,
        "schema_version": _D2_ARTIFACT_MANIFEST_SCHEMA_V0,
        "source_policy_version": D2_HISTORICAL_SOURCE_POLICY_V0,
        "status": D2_HISTORICAL_RESULT_STATUS_V0,
    }
    if any(document.get(name) != value for name, value in expected_fixed.items()):
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact manifest fixed bindings differ"
        )
    outputs = document.get("outputs")
    if not isinstance(outputs, dict) or set(outputs) != set(output_metadata):
        raise D2HistoricalDevelopmentContractErrorV0(
            "serialized D2 artifact output membership differs"
        )
    for name, (digest, size) in output_metadata.items():
        value = outputs.get(name)
        if (
            not isinstance(value, dict)
            or set(value) != {"sha256", "size_bytes"}
            or value.get("sha256") != digest
            or type(value.get("size_bytes")) is not int
            or value.get("size_bytes") != size
        ):
            raise D2HistoricalDevelopmentContractErrorV0(
                f"serialized D2 artifact output binding differs: {name}"
            )


def _transient_d1_verifier_index_v0(
    *,
    episode_lines: tuple[bytes, ...],
    censor_lines: tuple[bytes, ...],
    summary_document: dict[str, object],
    run_id: str,
    run_started_at_ms: int,
    input_authority_sha256: str,
    code_freeze_manifest_sha256: str,
    code_freeze_receipt_sha256: str,
) -> bytes:
    episode_hashes = tuple(
        _serialized_sha256_v0(
            _decode_canonical_object_v0(line, "transient D1 episode").get("episode_sha256"),
            "transient D1 episode hash",
        )
        for line in episode_lines
    )
    censor_hashes = tuple(
        _serialized_sha256_v0(
            _decode_canonical_object_v0(line, "transient D1 censor").get("censor_sha256"),
            "transient D1 censor hash",
        )
        for line in censor_lines
    )
    document: dict[str, object] = {
        "censor_count": len(censor_lines),
        "censor_sequence_root_sha256": _ordered_hash_root_v0(
            _CENSOR_SEQUENCE_ROOT_DOMAIN,
            censor_hashes,
        ),
        "code_freeze_manifest_sha256": code_freeze_manifest_sha256,
        "code_freeze_receipt_sha256": code_freeze_receipt_sha256,
        "development_end_ms_exclusive": D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
        "development_start_ms": D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
        "efficacy_claim": False,
        "episode_count": len(episode_lines),
        "episode_sequence_root_sha256": _ordered_hash_root_v0(
            _EPISODE_SEQUENCE_ROOT_DOMAIN,
            episode_hashes,
        ),
        "execution_conclusive": False,
        "existing_result_artifact_used_as_input": False,
        "historical_bbo_available": False,
        "historical_receipt_convention": D1_HISTORICAL_RECEIPT_CONVENTION_V0,
        "input_authority_sha256": input_authority_sha256,
        "paper_fill_claim": False,
        "post_development_end_rows_used": False,
        "preregistration_sha256": D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
        "probability_claim": False,
        "production_order_placement": False,
        "promoting": False,
        "prospective": False,
        "rule_version": D1_HISTORICAL_DEVELOPMENT_RULE_V0,
        "run_id": run_id,
        "run_started_at_ms": run_started_at_ms,
        "schema_version": "d1_historical_development_result_v0",
        "summary_sha256": _serialized_sha256_v0(
            summary_document.get("summary_sha256"),
            "transient D1 summary hash",
        ),
        "universe": list(D1_HISTORICAL_UNIVERSE_V0),
    }
    document["result_sha256"] = hashlib.sha256(
        _RESULT_HASH_DOMAIN + canonical_json_line(document)
    ).hexdigest()
    return canonical_json_line(document)


def _validate_serialized_d2_result_v0(
    *,
    result_document: dict[str, object],
    expected_result_sha256: str,
    expected_input_authority: D2HistoricalInputAuthorityV0,
    expected_code_freeze: D2HistoricalDevelopmentFreezeV0,
    expected_run_id: str,
    expected_run_started_at_ms: int,
    expected_start_record_sha256: str,
    expected_attempt_directory_sha256: str,
    expected_attempt_bindings_sha256: str,
    derived_manifests: tuple[D2DerivedHourlyManifestV0, ...],
    episode_lines: tuple[bytes, ...],
    censor_lines: tuple[bytes, ...],
    summary_sha256: str,
) -> tuple[str, str, str]:
    expected_keys = {
        "adaptation_label",
        "attempt_bindings_sha256",
        "attempt_directory_sha256",
        "censor_count",
        "censor_sequence_root_sha256",
        "code_freeze_manifest_sha256",
        "code_freeze_receipt_sha256",
        "decision_source_root_policy",
        "derivation_policy_version",
        "derived_hour_manifest_count",
        "derived_hour_manifest_sequence_root_sha256",
        "development_end_ms_exclusive",
        "development_start_ms",
        "economic_rule_version",
        "efficacy_claim",
        "episode_count",
        "episode_sequence_root_sha256",
        "execution_conclusive",
        "existing_result_artifact_used_as_input",
        "historical_bbo_available",
        "historical_receipt_convention",
        "historical_role",
        "input_authority_file_sha256",
        "input_authority_sha256",
        "operator_amendment_sha256",
        "operator_correction_a1_sha256",
        "paper_fill_claim",
        "post_development_end_rows_used",
        "preregistration_sha256",
        "probability_claim",
        "production_order_placement",
        "promoting",
        "prospective",
        "result_sha256",
        "rule_version",
        "run_id",
        "run_started_at_ms",
        "schema_version",
        "source_policy_version",
        "start_record_sha256",
        "status",
        "summary_sha256",
        "universe",
    }
    if set(result_document) != expected_keys:
        raise D2HistoricalDevelopmentContractErrorV0("serialized D2 result fields differ")
    claimed_result = _serialized_sha256_v0(
        result_document.get("result_sha256"),
        "serialized D2 result hash",
    )
    body = dict(result_document)
    del body["result_sha256"]
    computed_result = _hash_document(_D2_RESULT_HASH_DOMAIN, body)
    if claimed_result != computed_result or claimed_result != expected_result_sha256:
        raise D2HistoricalDevelopmentContractErrorV0("serialized D2 result hash differs")
    for name in (
        "efficacy_claim",
        "execution_conclusive",
        "existing_result_artifact_used_as_input",
        "historical_bbo_available",
        "paper_fill_claim",
        "post_development_end_rows_used",
        "probability_claim",
        "production_order_placement",
        "promoting",
        "prospective",
    ):
        if result_document[name] is not False:
            raise D2HistoricalDevelopmentContractErrorV0(
                f"serialized D2 result false claim differs: {name}"
            )
    authority_file_sha256 = hashlib.sha256(
        canonical_d2_historical_input_authority_v0(expected_input_authority)
    ).hexdigest()
    expected_fixed: dict[str, object] = {
        "adaptation_label": D2_HISTORICAL_ADAPTATION_LABEL_V0,
        "attempt_bindings_sha256": expected_attempt_bindings_sha256,
        "attempt_directory_sha256": expected_attempt_directory_sha256,
        "code_freeze_manifest_sha256": expected_code_freeze.manifest_sha256,
        "code_freeze_receipt_sha256": expected_code_freeze.receipt_sha256,
        "derivation_policy_version": D2_HISTORICAL_DERIVATION_POLICY_V0,
        "decision_source_root_policy": D1_HISTORICAL_SOURCE_ROOT_POLICY_USED_ROWS_V0,
        "development_end_ms_exclusive": D1_HISTORICAL_DEVELOPMENT_END_MS_V0,
        "development_start_ms": D1_HISTORICAL_DEVELOPMENT_START_MS_V0,
        "economic_rule_version": D1_SCEFB_RULE_VERSION_V0,
        "historical_receipt_convention": D1_HISTORICAL_RECEIPT_CONVENTION_V0,
        "historical_role": D2_HISTORICAL_ROLE_V0,
        "input_authority_file_sha256": authority_file_sha256,
        "input_authority_sha256": expected_input_authority.authority_sha256,
        "operator_amendment_sha256": D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0,
        "operator_correction_a1_sha256": (D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0),
        "preregistration_sha256": D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
        "rule_version": D2_HISTORICAL_DEVELOPMENT_RULE_V0,
        "run_id": expected_run_id,
        "run_started_at_ms": expected_run_started_at_ms,
        "schema_version": _D2_RESULT_SCHEMA_V0,
        "source_policy_version": D2_HISTORICAL_SOURCE_POLICY_V0,
        "start_record_sha256": expected_start_record_sha256,
        "status": D2_HISTORICAL_RESULT_STATUS_V0,
        "summary_sha256": summary_sha256,
        "universe": list(D1_HISTORICAL_UNIVERSE_V0),
    }
    if any(result_document.get(name) != value for name, value in expected_fixed.items()):
        raise D2HistoricalDevelopmentContractErrorV0("serialized D2 result fixed bindings differ")
    for name, expected in (
        ("derived_hour_manifest_count", len(derived_manifests)),
        ("episode_count", len(episode_lines)),
        ("censor_count", len(censor_lines)),
    ):
        if type(result_document.get(name)) is not int or result_document[name] != expected:
            raise D2HistoricalDevelopmentContractErrorV0(
                f"serialized D2 result count differs: {name}"
            )
    episode_documents = tuple(
        _decode_canonical_object_v0(line, "serialized D2 episode") for line in episode_lines
    )
    censor_documents = tuple(
        _decode_canonical_object_v0(line, "serialized D2 censor") for line in censor_lines
    )
    episode_hashes = tuple(
        _serialized_sha256_v0(value.get("episode_sha256"), "serialized D2 episode hash")
        for value in episode_documents
    )
    censor_hashes = tuple(
        _serialized_sha256_v0(value.get("censor_sha256"), "serialized D2 censor hash")
        for value in censor_documents
    )
    derived_root = _derived_manifest_root_v0(derived_manifests)
    episode_root = _ordered_hash_root_v0(_D2_EPISODE_SEQUENCE_ROOT_DOMAIN, episode_hashes)
    censor_root = _ordered_hash_root_v0(_D2_CENSOR_SEQUENCE_ROOT_DOMAIN, censor_hashes)
    expected_roots = {
        "derived_hour_manifest_sequence_root_sha256": derived_root,
        "episode_sequence_root_sha256": episode_root,
        "censor_sequence_root_sha256": censor_root,
    }
    if any(result_document.get(name) != value for name, value in expected_roots.items()):
        raise D2HistoricalDevelopmentContractErrorV0("serialized D2 result sequence root differs")
    derived_by_symbol = {value.symbol: value for value in derived_manifests}
    five_by_symbol = {
        symbol: digest for symbol, _path, digest in D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0
    }
    funding_by_symbol = {
        symbol: digest for symbol, _path, digest in D2_HISTORICAL_FIXED_FUNDING_FILES_V0
    }
    for episode in episode_documents:
        symbol = _serialized_text_v0(episode.get("symbol"), "serialized episode symbol")
        if (
            symbol not in derived_by_symbol
            or episode.get("hourly_manifest_sha256") != derived_by_symbol[symbol].manifest_sha256
            or episode.get("five_minute_manifest_sha256") != five_by_symbol[symbol]
            or episode.get("funding_file_sha256") != funding_by_symbol[symbol]
        ):
            raise D2HistoricalDevelopmentContractErrorV0(
                "serialized D2 episode provenance differs from fixed authorities"
            )
    return derived_root, episode_root, censor_root


def _serialized_text_v0(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} must be nonempty text")
    return value


def _serialized_sha256_v0(value: object, label: str) -> str:
    return _require_sha256(value, label)


def _serialized_nonnegative_int_v0(value: object, label: str) -> int:
    return _require_nonnegative_int(value, label)


def _serialized_positive_int_v0(value: object, label: str) -> int:
    parsed = _serialized_nonnegative_int_v0(value, label)
    if parsed == 0:
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} must be positive")
    return parsed


def _development_report_markdown_v0(result: D2HistoricalDevelopmentResultV0) -> bytes:
    return _development_report_from_documents_v0(
        result_document=_result_document_v0(result, include_hash=True),
        summary_document=_decode_canonical_object_v0(
            canonical_d1_historical_summary_v0(result.summary),
            "D2 report shared-core summary",
        ),
    )


def _development_report_from_documents_v0(
    *,
    result_document: dict[str, object],
    summary_document: dict[str, object],
) -> bytes:
    lines = [
        "# D2 SCEFB derived-hourly historical development diagnostic",
        "",
        "> Status: `INCONCLUSIVE_NO_HISTORICAL_BBO`. This post-D1 development ",
        "> diagnostic cannot establish probability, efficacy, PAPER fills, promotion, ",
        "> prospective performance, or production-order authority.",
        "",
        "## Identity",
        "",
        f"- Run ID: `{result_document['run_id']}`",
        f"- Result SHA-256: `{result_document['result_sha256']}`",
        f"- Rule: `{result_document['rule_version']}`",
        f"- Economic rule held fixed: `{result_document['economic_rule_version']}`",
        f"- Source policy: `{result_document['source_policy_version']}`",
        (
            "- Derived manifest sequence root: "
            f"`{result_document['derived_hour_manifest_sequence_root_sha256']}`"
        ),
        "",
        "## Descriptive census",
        "",
        f"- Full sealed signals: {summary_document['full_signal_count']}",
        f"- Completed episodes: {summary_document['episode_count']}",
        f"- Evaluable episodes: {summary_document['evaluable_episode_count']}",
        (
            "- Global non-overlapping evaluable episodes: "
            f"{summary_document['global_nonoverlap_evaluable_count']}"
        ),
        f"- Disposition: `{summary_document['disposition']}`",
        "",
        "## Preregistered interpretation-error scan",
        "",
        (
            "1. Outcome leakage/look-ahead rejected: only authenticated closed 5m rows "
            "and complete derived hours."
        ),
        "2. Correlated symbols/overlapping episodes are not treated as independent.",
        "3. Notional and fee display cells do not multiply statistical N.",
        "4. No post hoc subgroup or direction selection is authorized.",
        "5. No undeclared multiple-testing family is interpreted.",
        "6. No p-value or confidence-bound claim is made by this diagnostic.",
        "7. No practical-significance claim is inferred from a small effect.",
        "8. The fixed ten-symbol universe is not substituted after outcome access.",
        (
            "9. Frozen fees, adverse slippage, and funding remain included; "
            "capacity/no-fill is unresolved."
        ),
        "10. Historical open proxies are not extrapolated to executable PAPER fills.",
        "11. This development-contaminated interval is not extrapolated to prospective efficacy.",
        "",
        "## Non-claims",
        "",
        "- Historical BBO available: false",
        "- PAPER fill claim: false",
        "- Execution conclusive: false",
        "- Probability claim: false",
        "- Efficacy claim: false",
        "- Prospective: false",
        "- Promoting: false",
        "- Production order placement: false",
        "",
    ]
    return "\n".join(lines).encode("utf-8")


def _derived_manifest_root_v0(
    manifests: tuple[D2DerivedHourlyManifestV0, ...],
) -> str:
    return _ordered_hash_root_v0(
        _D2_DERIVED_MANIFEST_SEQUENCE_ROOT_DOMAIN,
        tuple(item.manifest_sha256 for item in manifests),
    )


def _ordered_hash_root_v0(domain: bytes, values: tuple[str, ...]) -> str:
    root = hashlib.sha256(domain + b"EMPTY").digest()
    for index, value in enumerate(values):
        _require_sha256(value, "ordered sequence member")
        root = hashlib.sha256(
            domain + root + index.to_bytes(8, byteorder="big", signed=False) + bytes.fromhex(value)
        ).digest()
    return root.hex()


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} must be a lowercase SHA-256 digest")
    return value


def _require_nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} must be a nonnegative integer")
    return value


def _require_identity(value: object, label: str) -> str:
    if not isinstance(value, str) or _IDENTITY_RE.fullmatch(value) is None:
        raise D2HistoricalDevelopmentContractErrorV0(f"{label} must be fixed normalized safe text")
    return value


if D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0 != tuple(
    sorted(D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0)
):  # pragma: no cover - import-time protocol assertion
    raise RuntimeError("D2 freeze include_files must remain lexically sorted")
