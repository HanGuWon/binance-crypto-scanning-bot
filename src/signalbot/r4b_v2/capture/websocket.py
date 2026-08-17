from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import re
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import InitVar, dataclass, field
from enum import StrEnum
from itertools import pairwise
from typing import Protocol, cast
from urllib.parse import quote

from signalbot.capture.depth_sequence import DepthRangeObservation, DepthResyncRequest
from signalbot.capture.receipts import IngestSequencer, ReceiptClock, ReceiptTimestamp
from signalbot.capture.websocket import validate_public_websocket_plan
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import WebSocketPlan
from signalbot.r4b_v2.capture.batching import (
    CaptureQueueAdmissionReceiptV2,
    QueuedRawRecordV2,
    validate_capture_queue_admission_receipt_v2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingRestCapturePlanV2,
    ProvisionalUsdmVenueClockRestCapturePlanV9,
)
from signalbot.r4b_v2.capture.rest import (
    PublicOiRestAttemptPayloadV2,
    PublicOiRestTerminalObservationV2,
)
from signalbot.r4b_v2.capture.rest_census import (
    PublicOiRestCensusPayloadV2,
    PublicOiRestCoverageCloseV2,
    PublicOiRestForwardGapRangeV2,
    PublicOiRestSlotCensusV2,
)
from signalbot.r4b_v2.capture.rest_clock import (
    PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
    PublicUsdmVenueClockRestAttemptPayloadV9,
    PublicUsdmVenueClockRestTerminalObservationV9,
)
from signalbot.r4b_v2.capture.rest_depth import (
    PublicDepthRestAttemptPayloadV8,
    PublicDepthRestTerminalObservationV8,
    public_depth_rest_source_logical_key_v8,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_IDENTITY_LENGTH = 256
_PUBLIC_OI_ADMISSION_RECEIPT_FACTORY_TOKEN = object()
_PUBLIC_DEPTH_REST_ADMISSION_RECEIPT_FACTORY_TOKEN = object()
_PUBLIC_USDM_VENUE_CLOCK_ADMISSION_RECEIPT_FACTORY_TOKEN = object()
_HTTPS_REST_WALL_REGRESSION_EVIDENCE_FACTORY_TOKEN = object()
_PUBLIC_OI_CENSUS_ADMISSION_RECEIPT_FACTORY_TOKEN = object()
_PUBLIC_RETAINED_DEPTH_RANGE_CALLBACK_RECEIPT_FACTORY_TOKEN = object()
_PUBLIC_RETAINED_DEPTH_RESYNC_CALLBACK_RECEIPT_FACTORY_TOKEN = object()
_PUBLIC_WEBSOCKET_CAPTURE_ADAPTER_FACTORY_TOKEN = object()
_RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2 = object()
_PUBLIC_OI_CENSUS_CONNECTION_ID = "oi-rest-census"
_PUBLIC_OI_CENSUS_SOURCE_LOGICAL_KEY = "openInterest:census"
_PUBLIC_OI_SLOT_CENSUS_SCHEMA = "r4b_v2_public_oi_rest_slot_census_v1"
_PUBLIC_OI_FORWARD_GAP_SCHEMA = "r4b_v2_public_oi_rest_forward_gap_range_v1"
_PUBLIC_OI_COVERAGE_CLOSE_SCHEMA = "r4b_v2_public_oi_rest_coverage_close_v1"
_MAX_PENDING_INGRESS_RESERVATIONS = 16


class _IngressWallOrderPolicyV2(StrEnum):
    STRICT = "strict"
    PRESERVE_HTTPS_TERMINAL = "preserve_https_terminal"


class _RawRecordOffererV2(Protocol):
    def offer(self, record: RawRecordV2) -> object: ...


class _HttpsQueueAdmitterV2(Protocol):
    def offer_with_admission_receipt(
        self,
        record: RawRecordV2,
    ) -> CaptureQueueAdmissionReceiptV2: ...

    def validate_queue_admission_receipt_v2(
        self,
        receipt: CaptureQueueAdmissionReceiptV2,
    ) -> QueuedRawRecordV2: ...


class SharedIngressOrderingErrorV2(RuntimeError):
    """Raised when receipt-order admission can no longer remain contiguous."""


@dataclass(slots=True)
class _IngressReservationV2:
    ingest_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    producer_kind: str
    prior_global_wall_ms: int | None = None
    admitted: bool = False
    released: bool = False


@dataclass(frozen=True, slots=True)
class PublicHttpsRestWallClockRegressionEvidenceV2:
    """Process-local proof that raw HTTPS wall clocks moved backwards.

    Monotonic timestamps remain the causal authority. This factory-sealed
    evidence is intentionally attached only to an in-memory admission receipt;
    the raw record continues to retain the unmodified UTC wall timestamps.
    """

    ingest_seq: int
    request_started_wall_ms: int
    response_first_header_wall_ms: int | None
    attempt_ended_wall_ms: int
    completion_admission_wall_ms: int
    prior_global_wall_ms: int | None
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _HTTPS_REST_WALL_REGRESSION_EVIDENCE_FACTORY_TOKEN:
            raise TypeError(
                "HTTPS REST wall-regression evidence can only be created by the shared ingress"
            )
        object.__setattr__(
            self,
            "_factory_seal",
            _HTTPS_REST_WALL_REGRESSION_EVIDENCE_FACTORY_TOKEN,
        )
        _validate_https_rest_wall_regression_evidence_material_v2(self)

    @property
    def intra_attempt_regression(self) -> bool:
        return _wall_sequence_regressed_v2(
            request_started_wall_ms=self.request_started_wall_ms,
            response_first_header_wall_ms=self.response_first_header_wall_ms,
            attempt_ended_wall_ms=self.attempt_ended_wall_ms,
            completion_admission_wall_ms=self.completion_admission_wall_ms,
        )

    @property
    def prior_global_regression(self) -> bool:
        prior = self.prior_global_wall_ms
        return prior is not None and self.completion_admission_wall_ms < prior


class HttpsRestWallClockRegressionErrorV2(RuntimeError):
    """Fatal surfaced only after one regressed-wall terminal row is admitted."""

    def __init__(
        self,
        *,
        route_id: str,
        symbol: str,
        evidence: PublicHttpsRestWallClockRegressionEvidenceV2,
    ) -> None:
        _validate_identity(route_id, "wall-regression route_id")
        _validate_identity(symbol, "wall-regression symbol")
        validate_public_https_rest_wall_clock_regression_evidence_v2(evidence)
        self.route_id = route_id
        self.symbol = symbol
        self.evidence = evidence
        dimensions = []
        if evidence.intra_attempt_regression:
            dimensions.append("intra_attempt")
        if evidence.prior_global_regression:
            dimensions.append("prior_global")
        super().__init__(
            "HTTPS REST UTC wall clock regressed after exact terminal admission: "
            f"route={route_id} symbol={symbol} ingest_seq={evidence.ingest_seq} "
            f"dimensions={','.join(dimensions)}"
        )


def validate_public_https_rest_wall_clock_regression_evidence_v2(
    evidence: PublicHttpsRestWallClockRegressionEvidenceV2,
) -> None:
    """Revalidate exact factory provenance and bounded regression material."""

    if type(evidence) is not PublicHttpsRestWallClockRegressionEvidenceV2:
        raise TypeError("wall-regression evidence must be the exact public type")
    if (
        getattr(evidence, "_factory_seal", None)
        is not _HTTPS_REST_WALL_REGRESSION_EVIDENCE_FACTORY_TOKEN
    ):
        raise ValueError("HTTPS REST wall-regression evidence lacks ingress provenance")
    _validate_https_rest_wall_regression_evidence_material_v2(evidence)


@dataclass(frozen=True, slots=True)
class PublicOiAdmissionReceiptV2:
    """Factory-sealed proof of one exact shared-ingress queue admission.

    This local receipt proves only that the exact raw record crossed the
    bounded handoff's synchronous acceptance seam.  WAL or grouped-block
    durability still requires the existing finality-fence receipt.
    """

    record: RawRecordV2 = field(repr=False)
    queue_admission_receipt: CaptureQueueAdmissionReceiptV2 = field(repr=False)
    wall_clock_regression: PublicHttpsRestWallClockRegressionEvidenceV2 | None = field(
        default=None,
        repr=False,
    )
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_OI_ADMISSION_RECEIPT_FACTORY_TOKEN:
            raise TypeError("PublicOiAdmissionReceiptV2 can only be created by the shared ingress")
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_OI_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )
        _validate_public_oi_admission_receipt_material_v2(self)

    @property
    def accepted_ingest_seq(self) -> int:
        """Return the exact in-memory handoff tail admitted by this offer."""

        return self.queue_admission_receipt.accepted_tail_ingest_seq

    @property
    def queued_record(self) -> QueuedRawRecordV2:
        """Return the immutable queue item authenticated by the handoff proof."""

        return self.queue_admission_receipt.queued_record


def validate_public_oi_admission_receipt_v2(
    receipt: PublicOiAdmissionReceiptV2,
) -> RawRecordV2:
    """Revalidate factory provenance and return the exact admitted record."""

    if type(receipt) is not PublicOiAdmissionReceiptV2:
        raise TypeError("OI adapter must return an exact PublicOiAdmissionReceiptV2")
    if getattr(receipt, "_factory_seal", None) is not _PUBLIC_OI_ADMISSION_RECEIPT_FACTORY_TOKEN:
        raise ValueError("public OI admission receipt lacks shared-ingress provenance")
    _validate_public_oi_admission_receipt_material_v2(receipt)
    return receipt.record


def _validate_public_oi_admission_receipt_material_v2(
    receipt: PublicOiAdmissionReceiptV2,
) -> None:
    if type(receipt.record) is not RawRecordV2:
        raise TypeError("public OI admission receipt requires an exact RawRecordV2")
    receipt.record.__post_init__()
    queued_record = validate_capture_queue_admission_receipt_v2(receipt.queue_admission_receipt)
    if queued_record.record is not receipt.record:
        raise ValueError("queued record is not the exact shared-ingress raw record")
    if queued_record.ingest_seq != receipt.record.ingest_seq:
        raise ValueError("queued admission sequence differs from its raw record")
    _validate_wall_regression_evidence_against_oi_record_v2(
        receipt.wall_clock_regression,
        receipt.record,
    )


@dataclass(frozen=True, slots=True)
class PublicDepthRestAdmissionReceiptV8:
    """Factory-sealed proof of one exact depth attempt queue admission only."""

    plan: ProvisionalDepthRestQualificationPlanV8 = field(repr=False)
    record: RawRecordV2 = field(repr=False)
    queue_admission_receipt: CaptureQueueAdmissionReceiptV2 = field(repr=False)
    wall_clock_regression: PublicHttpsRestWallClockRegressionEvidenceV2 | None = field(
        default=None,
        repr=False,
    )
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_DEPTH_REST_ADMISSION_RECEIPT_FACTORY_TOKEN:
            raise TypeError(
                "PublicDepthRestAdmissionReceiptV8 can only be created by shared ingress"
            )
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_DEPTH_REST_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )
        _validate_public_depth_rest_admission_receipt_material_v8(self)

    @property
    def accepted_ingest_seq(self) -> int:
        return self.queue_admission_receipt.accepted_tail_ingest_seq

    @property
    def queued_record(self) -> QueuedRawRecordV2:
        return self.queue_admission_receipt.queued_record


def validate_public_depth_rest_admission_receipt_v8(
    receipt: PublicDepthRestAdmissionReceiptV8,
    *,
    plan: ProvisionalDepthRestQualificationPlanV8 | None = None,
) -> RawRecordV2:
    """Revalidate exact shared-ingress and v8 depth-plan provenance."""

    if type(receipt) is not PublicDepthRestAdmissionReceiptV8:
        raise TypeError("depth ingress must return an exact depth REST admission receipt")
    if (
        getattr(receipt, "_factory_seal", None)
        is not _PUBLIC_DEPTH_REST_ADMISSION_RECEIPT_FACTORY_TOKEN
    ):
        raise ValueError("depth REST admission receipt lacks shared-ingress provenance")
    _validate_public_depth_rest_admission_receipt_material_v8(receipt)
    if plan is not None:
        if type(plan) is not ProvisionalDepthRestQualificationPlanV8:
            raise TypeError("expected depth REST plan must be exact")
        plan.__post_init__()
        if receipt.plan != plan:
            raise ValueError("depth REST admission receipt belongs to a different plan")
    return receipt.record


def _validate_public_depth_rest_admission_receipt_material_v8(
    receipt: PublicDepthRestAdmissionReceiptV8,
) -> None:
    plan = receipt.plan
    if type(plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("depth REST admission receipt requires the exact v8 plan")
    plan.__post_init__()
    record = receipt.record
    if type(record) is not RawRecordV2:
        raise TypeError("depth REST admission receipt requires an exact RawRecordV2")
    record.__post_init__()
    if (
        record.transport is not TransportV2.HTTPS
        or record.venue is not VenueV2.USDM_FUTURES
        or record.route_id != plan.route_id
        or record.plan_id != plan.name
        or record.symbol not in plan.symbols
        or record.frame_seq is not None
        or record.source_logical_key
        != public_depth_rest_source_logical_key_v8(cast(str, record.symbol))
    ):
        raise ValueError("depth REST admitted record differs from its exact outer identity")
    payload = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
        record.payload_bytes(),
        plan=plan,
    )
    if (
        payload.session_id != record.session_id
        or payload.protocol_hash != record.protocol_hash
        or payload.connection_id != record.connection_id
        or payload.symbol != record.symbol
        or payload.connection_generation != record.generation
        or payload.completion_admission_wall_ms != record.receipt_wall_ms
        or payload.completion_admission_monotonic_ns != record.receipt_monotonic_ns
    ):
        raise ValueError("depth REST payload identity differs from its outer record")
    queued_record = validate_capture_queue_admission_receipt_v2(receipt.queue_admission_receipt)
    if queued_record.record is not record:
        raise ValueError("queued depth record is not the exact shared-ingress record")
    if queued_record.ingest_seq != record.ingest_seq:
        raise ValueError("queued depth admission sequence differs from its raw record")
    _validate_wall_regression_evidence_against_depth_record_v8(
        receipt.wall_clock_regression,
        record,
        payload,
    )


@dataclass(frozen=True, slots=True)
class PublicUsdmVenueClockAdmissionReceiptV9:
    """Factory-sealed proof of one clock-attempt queue admission only."""

    plan: ProvisionalUsdmVenueClockRestCapturePlanV9 = field(repr=False)
    record: RawRecordV2 = field(repr=False)
    queue_admission_receipt: CaptureQueueAdmissionReceiptV2 = field(repr=False)
    wall_clock_regression: PublicHttpsRestWallClockRegressionEvidenceV2 | None = field(
        default=None,
        repr=False,
    )
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_USDM_VENUE_CLOCK_ADMISSION_RECEIPT_FACTORY_TOKEN:
            raise TypeError("venue-clock admission receipts require shared ingress")
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_USDM_VENUE_CLOCK_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )
        _validate_public_usdm_venue_clock_admission_receipt_material_v9(self)

    @property
    def accepted_ingest_seq(self) -> int:
        return self.queue_admission_receipt.accepted_tail_ingest_seq

    @property
    def queued_record(self) -> QueuedRawRecordV2:
        return self.queue_admission_receipt.queued_record


def validate_public_usdm_venue_clock_admission_receipt_v9(
    receipt: PublicUsdmVenueClockAdmissionReceiptV9,
    *,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9 | None = None,
) -> RawRecordV2:
    """Revalidate exact shared-ingress and venue-clock plan provenance."""

    if type(receipt) is not PublicUsdmVenueClockAdmissionReceiptV9:
        raise TypeError("clock ingress must return an exact venue-clock receipt")
    if (
        getattr(receipt, "_factory_seal", None)
        is not _PUBLIC_USDM_VENUE_CLOCK_ADMISSION_RECEIPT_FACTORY_TOKEN
    ):
        raise ValueError("venue-clock admission receipt lacks ingress provenance")
    _validate_public_usdm_venue_clock_admission_receipt_material_v9(receipt)
    if plan is not None:
        if type(plan) is not ProvisionalUsdmVenueClockRestCapturePlanV9:
            raise TypeError("expected venue-clock plan must be exact")
        plan.__post_init__()
        if receipt.plan != plan:
            raise ValueError("venue-clock admission receipt belongs to another plan")
    return receipt.record


def _validate_public_usdm_venue_clock_admission_receipt_material_v9(
    receipt: PublicUsdmVenueClockAdmissionReceiptV9,
) -> None:
    plan = receipt.plan
    if type(plan) is not ProvisionalUsdmVenueClockRestCapturePlanV9:
        raise TypeError("venue-clock admission receipt requires the exact v9 plan")
    plan.__post_init__()
    record = receipt.record
    if type(record) is not RawRecordV2:
        raise TypeError("venue-clock admission receipt requires an exact RawRecordV2")
    record.__post_init__()
    if (
        record.transport is not TransportV2.HTTPS
        or record.venue is not VenueV2.USDM_FUTURES
        or record.route_id != plan.route_id
        or record.plan_id != plan.name
        or record.symbol is not None
        or record.frame_seq is not None
        or record.source_logical_key
        != PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9
    ):
        raise ValueError("venue-clock admitted record has the wrong outer identity")
    payload = PublicUsdmVenueClockRestAttemptPayloadV9.from_canonical_bytes(
        record.payload_bytes(),
        plan=plan,
    )
    if (
        payload.session_id != record.session_id
        or payload.protocol_hash != record.protocol_hash
        or payload.connection_id != record.connection_id
        or payload.connection_generation != record.generation
        or payload.completion_admission_wall_ms != record.receipt_wall_ms
        or payload.completion_admission_monotonic_ns != record.receipt_monotonic_ns
    ):
        raise ValueError("venue-clock payload identity differs from its outer record")
    queued = validate_capture_queue_admission_receipt_v2(
        receipt.queue_admission_receipt
    )
    if queued.record is not record or queued.ingest_seq != record.ingest_seq:
        raise ValueError("queued venue-clock record differs from shared ingress")
    _validate_wall_regression_evidence_against_payload_v2(
        receipt.wall_clock_regression,
        record=record,
        request_started_wall_ms=payload.request_started_wall_ms,
        response_first_header_wall_ms=payload.response_first_header_wall_ms,
        attempt_ended_wall_ms=payload.attempt_ended_wall_ms,
        completion_admission_wall_ms=payload.completion_admission_wall_ms,
    )


@dataclass(frozen=True, slots=True)
class PublicOiCensusAdmissionReceiptV2:
    """Factory-sealed proof of one exact public OI census queue admission."""

    record: RawRecordV2 = field(repr=False)
    queue_admission_receipt: CaptureQueueAdmissionReceiptV2 = field(repr=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_OI_CENSUS_ADMISSION_RECEIPT_FACTORY_TOKEN:
            raise TypeError(
                "PublicOiCensusAdmissionReceiptV2 can only be created by the shared ingress"
            )
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_OI_CENSUS_ADMISSION_RECEIPT_FACTORY_TOKEN,
        )
        _validate_public_oi_census_admission_receipt_material_v2(self)

    @property
    def accepted_ingest_seq(self) -> int:
        """Return the exact in-memory handoff tail admitted by this offer."""

        return self.queue_admission_receipt.accepted_tail_ingest_seq

    @property
    def queued_record(self) -> QueuedRawRecordV2:
        """Return the immutable queue item authenticated by the handoff proof."""

        return self.queue_admission_receipt.queued_record


def validate_public_oi_census_admission_receipt_v2(
    receipt: PublicOiCensusAdmissionReceiptV2,
) -> RawRecordV2:
    """Revalidate census-ingress provenance and return its admitted record."""

    if type(receipt) is not PublicOiCensusAdmissionReceiptV2:
        raise TypeError("OI census ingress must return an exact PublicOiCensusAdmissionReceiptV2")
    if (
        getattr(receipt, "_factory_seal", None)
        is not _PUBLIC_OI_CENSUS_ADMISSION_RECEIPT_FACTORY_TOKEN
    ):
        raise ValueError("public OI census receipt lacks shared-ingress provenance")
    _validate_public_oi_census_admission_receipt_material_v2(receipt)
    return receipt.record


def _validate_public_oi_census_admission_receipt_material_v2(
    receipt: PublicOiCensusAdmissionReceiptV2,
) -> None:
    record = receipt.record
    if type(record) is not RawRecordV2:
        raise TypeError("public OI census receipt requires an exact RawRecordV2")
    record.__post_init__()
    if (
        record.transport is not TransportV2.HTTPS
        or record.venue is not VenueV2.USDM_FUTURES
        or record.route_id != "usdm_public_rest"
        or record.symbol is not None
        or record.connection_id != _PUBLIC_OI_CENSUS_CONNECTION_ID
        or record.generation != 1
        or record.frame_seq is not None
        or record.source_logical_key != _PUBLIC_OI_CENSUS_SOURCE_LOGICAL_KEY
    ):
        raise ValueError("public OI census record has the wrong fixed outer identity")
    payload = _parse_public_oi_census_payload_v2(record.payload_bytes())
    if (
        payload.session_id != record.session_id
        or payload.plan_id != record.plan_id
        or payload.route_id != record.route_id
    ):
        raise ValueError("public OI census payload identity differs from its outer record")
    queued_record = validate_capture_queue_admission_receipt_v2(receipt.queue_admission_receipt)
    if queued_record.record is not record:
        raise ValueError("queued census record is not the exact shared-ingress record")
    if queued_record.ingest_seq != record.ingest_seq:
        raise ValueError("queued census admission sequence differs from its raw record")


@dataclass(frozen=True, slots=True)
class PublicRetainedDepthRangeCallbackReceiptV2:
    """Factory-sealed source proof for one retained depth-range callback.

    This process-local receipt proves only that an exact V2 WebSocket adapter
    retained the callback's raw frame and that the owner observed the frame at
    its post-offer iterator seam. It grants no REST snapshot, local-book, M2,
    promotion, strategy, alert, or execution authority.
    """

    session_id: str
    protocol_hash: str
    market: Market
    route: str
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    raw_payload_sha256: str
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    observation: DepthRangeObservation
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    _material_seal: tuple[object, ...] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_RETAINED_DEPTH_RANGE_CALLBACK_RECEIPT_FACTORY_TOKEN:
            raise TypeError(
                "retained depth-range callback receipts can only be minted "
                "from the exact admitted-frame seam"
            )
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_RETAINED_DEPTH_RANGE_CALLBACK_RECEIPT_FACTORY_TOKEN,
        )
        _validate_public_retained_depth_range_callback_receipt_material_v2(self)
        object.__setattr__(
            self,
            "_material_seal",
            _retained_depth_range_callback_receipt_material_v2(self),
        )


@dataclass(frozen=True, slots=True)
class PublicRetainedDepthResyncCallbackReceiptV2:
    """Factory-sealed source proof for one retained depth-resync callback.

    This receipt is qualification/source evidence only. It does not assert that
    a REST snapshot was captured, bridged, or durable, that a local book is
    correct, or that any M2, promotion, trading, or execution criterion holds.
    """

    session_id: str
    protocol_hash: str
    market: Market
    route: str
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    raw_payload_sha256: str
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    request: DepthResyncRequest
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    _material_seal: tuple[object, ...] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_RETAINED_DEPTH_RESYNC_CALLBACK_RECEIPT_FACTORY_TOKEN:
            raise TypeError(
                "retained depth-resync callback receipts can only be minted "
                "from the exact admitted-frame seam"
            )
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_RETAINED_DEPTH_RESYNC_CALLBACK_RECEIPT_FACTORY_TOKEN,
        )
        _validate_public_retained_depth_resync_callback_receipt_material_v2(self)
        object.__setattr__(
            self,
            "_material_seal",
            _retained_depth_resync_callback_receipt_material_v2(self),
        )


def validate_public_retained_depth_range_callback_receipt_v2(
    receipt: PublicRetainedDepthRangeCallbackReceiptV2,
) -> None:
    """Revalidate exact factory provenance and immutable range material."""

    if type(receipt) is not PublicRetainedDepthRangeCallbackReceiptV2:
        raise TypeError("retained depth-range callback receipt must be the exact type")
    if (
        getattr(receipt, "_factory_seal", None)
        is not _PUBLIC_RETAINED_DEPTH_RANGE_CALLBACK_RECEIPT_FACTORY_TOKEN
    ):
        raise ValueError("retained depth-range callback receipt lacks factory provenance")
    _validate_public_retained_depth_range_callback_receipt_material_v2(receipt)
    if getattr(receipt, "_material_seal", None) != (
        _retained_depth_range_callback_receipt_material_v2(receipt)
    ):
        raise ValueError("retained depth-range callback receipt material was mutated")


def validate_public_retained_depth_resync_callback_receipt_v2(
    receipt: PublicRetainedDepthResyncCallbackReceiptV2,
) -> None:
    """Revalidate exact factory provenance and immutable resync material."""

    if type(receipt) is not PublicRetainedDepthResyncCallbackReceiptV2:
        raise TypeError("retained depth-resync callback receipt must be the exact type")
    if (
        getattr(receipt, "_factory_seal", None)
        is not _PUBLIC_RETAINED_DEPTH_RESYNC_CALLBACK_RECEIPT_FACTORY_TOKEN
    ):
        raise ValueError("retained depth-resync callback receipt lacks factory provenance")
    _validate_public_retained_depth_resync_callback_receipt_material_v2(receipt)
    if getattr(receipt, "_material_seal", None) != (
        _retained_depth_resync_callback_receipt_material_v2(receipt)
    ):
        raise ValueError("retained depth-resync callback receipt material was mutated")


def _validate_public_retained_depth_callback_scalars_v2(
    receipt: (
        PublicRetainedDepthRangeCallbackReceiptV2 | PublicRetainedDepthResyncCallbackReceiptV2
    ),
) -> None:
    _validate_lineage(
        session_id=receipt.session_id,
        protocol_hash=receipt.protocol_hash,
    )
    if type(receipt.market) is not Market or receipt.market is not Market.FUTURES:
        raise ValueError("retained depth callback requires the USD-M Futures market")
    if receipt.route != "public":
        raise ValueError("retained depth callback requires the routed public endpoint")
    _validate_identity(receipt.connection_id, "retained depth connection_id")
    for field_name, value in (
        ("generation", receipt.generation),
        ("frame_seq", receipt.frame_seq),
        ("ingest_seq", receipt.ingest_seq),
    ):
        if type(value) is not int or value < 1:
            raise ValueError(f"retained depth {field_name} must be a positive integer")
    for field_name, value in (
        ("receipt_wall_ms", receipt.receipt_wall_ms),
        ("receipt_monotonic_ns", receipt.receipt_monotonic_ns),
    ):
        if type(value) is not int or value < 0:
            raise ValueError(f"retained depth {field_name} must be nonnegative")
    if (
        type(receipt.raw_payload_sha256) is not str
        or _SHA256_RE.fullmatch(receipt.raw_payload_sha256) is None
    ):
        raise ValueError("retained depth raw hash must be a lowercase SHA-256 digest")


def _validate_public_retained_depth_range_callback_receipt_material_v2(
    receipt: PublicRetainedDepthRangeCallbackReceiptV2,
) -> None:
    _validate_public_retained_depth_callback_scalars_v2(receipt)
    observation = receipt.observation
    if type(observation) is not DepthRangeObservation:
        raise TypeError("retained depth callback requires an exact range observation")
    observation.__post_init__()
    if observation.market is not receipt.market or observation.generation != receipt.generation:
        raise ValueError("retained depth range differs from its callback lineage")


def _validate_public_retained_depth_resync_callback_receipt_material_v2(
    receipt: PublicRetainedDepthResyncCallbackReceiptV2,
) -> None:
    _validate_public_retained_depth_callback_scalars_v2(receipt)
    request = receipt.request
    if type(request) is not DepthResyncRequest:
        raise TypeError("retained depth callback requires an exact resync request")
    request.__post_init__()
    if request.market is not receipt.market or request.generation != receipt.generation:
        raise ValueError("retained depth resync differs from its callback lineage")
    if request.event == "startup" and receipt.generation != 1:
        raise ValueError("startup depth resync requires generation one")
    if request.event == "reconnect" and receipt.generation == 1:
        raise ValueError("reconnect depth resync requires a later generation")


def _retained_depth_callback_scalar_material_v2(
    receipt: (
        PublicRetainedDepthRangeCallbackReceiptV2 | PublicRetainedDepthResyncCallbackReceiptV2
    ),
) -> tuple[object, ...]:
    return (
        receipt.session_id,
        receipt.protocol_hash,
        receipt.market,
        receipt.route,
        receipt.connection_id,
        receipt.generation,
        receipt.frame_seq,
        receipt.ingest_seq,
        receipt.raw_payload_sha256,
        receipt.receipt_wall_ms,
        receipt.receipt_monotonic_ns,
    )


def _retained_depth_range_callback_receipt_material_v2(
    receipt: PublicRetainedDepthRangeCallbackReceiptV2,
) -> tuple[object, ...]:
    observation = receipt.observation
    return (
        _PUBLIC_RETAINED_DEPTH_RANGE_CALLBACK_RECEIPT_FACTORY_TOKEN,
        *_retained_depth_callback_scalar_material_v2(receipt),
        observation.market,
        observation.symbol,
        observation.generation,
        observation.U,
        observation.u,
        observation.reset,
    )


def _retained_depth_resync_callback_receipt_material_v2(
    receipt: PublicRetainedDepthResyncCallbackReceiptV2,
) -> tuple[object, ...]:
    request = receipt.request
    return (
        _PUBLIC_RETAINED_DEPTH_RESYNC_CALLBACK_RECEIPT_FACTORY_TOKEN,
        *_retained_depth_callback_scalar_material_v2(receipt),
        request.event,
        request.market,
        request.generation,
        request.watermarks,
    )


class WebSocketRecoveryLifecycleV2(Protocol):
    """Adapter-facing route lifecycle hooks at exact accepted-frame seams."""

    async def complete_recovery_successor(self, record: RawRecordV2) -> None: ...

    def record_retained_frame(self, record: RawRecordV2) -> None: ...

    def trip_fatal(self, cause: BaseException) -> None: ...


class WebSocketFrameRetentionCancelledV2(RuntimeError):
    """Exact evidence for a yielded frame cancelled before bounded capture."""

    def __init__(
        self,
        *,
        plan_id: str,
        route_id: str,
        connection_id: str,
        generation: int,
        candidate_frame_seq: int,
        receipt: ReceiptTimestamp,
        raw_payload_sha256: str,
    ) -> None:
        self.plan_id = plan_id
        self.route_id = route_id
        self.connection_id = connection_id
        self.generation = generation
        self.candidate_frame_seq = candidate_frame_seq
        self.receipt_wall_ms = receipt.received_at_ms
        self.receipt_monotonic_ns = receipt.received_monotonic_ns
        self.raw_payload_sha256 = raw_payload_sha256
        super().__init__(
            "yielded WebSocket frame was cancelled before its bounded capture offer: "
            f"{plan_id}/{route_id}/{connection_id}/g{generation}/f{candidate_frame_seq} "
            f"receipt={receipt.received_at_ms}:{receipt.received_monotonic_ns} "
            f"payload_sha256={raw_payload_sha256}"
        )


class SharedWebSocketIngressV2:
    """One process-local receipt-order authority shared by every public producer.

    ``recovered_wal_tail_ingest_seq`` is the exact durable WAL tail recovered
    before any producer starts.  A producer synchronously reserves its global
    sequence immediately after its receipt is sampled and before its first
    await.  The fair lock then admits reservations in that same order.  Recovery
    successor finality still holds the lock through SOURCE_GAP BOUNDED, so N+1
    may reserve but cannot cross the pipeline before N is causally complete.
    """

    def __init__(
        self,
        pipeline: _RawRecordOffererV2,
        *,
        recovered_wal_tail_ingest_seq: int,
    ) -> None:
        if type(recovered_wal_tail_ingest_seq) is not int or recovered_wal_tail_ingest_seq < 0:
            raise ValueError("recovered WAL tail ingest sequence must be nonnegative")
        self._pipeline = pipeline
        self._recovered_wal_tail_ingest_seq = recovered_wal_tail_ingest_seq
        self._sequencer = IngestSequencer(initial_value=recovered_wal_tail_ingest_seq)
        self._ingress_lock = asyncio.Lock()
        self._next_admit_ingest_seq = recovered_wal_tail_ingest_seq + 1
        self._pending_reservation_count = 0
        self._last_reserved_wall_ms: int | None = None
        self._last_reserved_monotonic_ns: int | None = None
        self._ordering_failure: BaseException | None = None

    @property
    def pipeline(self) -> _RawRecordOffererV2:
        return self._pipeline

    @property
    def recovered_wal_tail_ingest_seq(self) -> int:
        return self._recovered_wal_tail_ingest_seq

    @property
    def pending_reservation_count(self) -> int:
        return self._pending_reservation_count

    async def offer_frame(
        self,
        *,
        plan: ProvisionalPromotingCapturePlanV2,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        frame_seq: int,
        receipt: ReceiptTimestamp,
        raw_payload: bytes,
    ) -> RawRecordV2:
        """Reserve at receipt, then synchronously offer under the shared gate."""

        _validate_frame_offer_inputs(
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
            frame_seq=frame_seq,
            receipt=receipt,
            raw_payload=raw_payload,
        )
        reservation = self._reserve_after_receipt(
            receipt,
            producer_kind=f"WEBSOCKET:{plan.route_id}",
        )
        async with self._admission_turn(reservation):
            record = self._offer_frame_reserved(
                reservation=reservation,
                plan=plan,
                session_id=session_id,
                protocol_hash=protocol_hash,
                connection_id=connection_id,
                generation=generation,
                frame_seq=frame_seq,
                receipt=receipt,
                raw_payload=raw_payload,
            )
            self._mark_admitted(reservation)
            return record

    async def offer_recovery_successor(
        self,
        *,
        plan: ProvisionalPromotingCapturePlanV2,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        frame_seq: int,
        receipt: ReceiptTimestamp,
        raw_payload: bytes,
        complete: Callable[[RawRecordV2], Awaitable[None]],
    ) -> RawRecordV2:
        """Hold the global gate through successor offer and causal completion.

        The lifecycle coordinator supplies ``complete`` and owns finality,
        SOURCE_GAP bounding, and fatal-state transitions.  This ingress remains
        independent of those layers while preventing another route from
        assigning N+1 before successor N is finalized and bounded.
        """

        _validate_frame_offer_inputs(
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
            frame_seq=frame_seq,
            receipt=receipt,
            raw_payload=raw_payload,
        )
        if not callable(complete):
            raise TypeError("recovery successor completion must be callable")
        reservation = self._reserve_after_receipt(
            receipt,
            producer_kind=f"WEBSOCKET_RECOVERY:{plan.route_id}",
        )
        async with self._admission_turn(reservation):
            record = self._offer_frame_reserved(
                reservation=reservation,
                plan=plan,
                session_id=session_id,
                protocol_hash=protocol_hash,
                connection_id=connection_id,
                generation=generation,
                frame_seq=frame_seq,
                receipt=receipt,
                raw_payload=raw_payload,
            )
            self._mark_admitted(reservation)
            # The original callback exception is intentionally not wrapped.
            # The lifecycle callback must trip its shared fatal state before it
            # propagates any finality or ledger failure.
            await complete(record)
            return record

    def _reserve_after_receipt(
        self,
        receipt: ReceiptTimestamp,
        *,
        producer_kind: str,
        wall_order_policy: _IngressWallOrderPolicyV2 = (_IngressWallOrderPolicyV2.STRICT),
    ) -> _IngressReservationV2:
        """Synchronously establish global order before a producer can await."""

        _validate_receipt_timestamp(receipt, "shared-ingress producer receipt")
        _validate_identity(producer_kind, "producer_kind")
        if type(wall_order_policy) is not _IngressWallOrderPolicyV2:
            raise TypeError("shared-ingress wall-order policy must be exact")
        if (
            wall_order_policy is _IngressWallOrderPolicyV2.PRESERVE_HTTPS_TERMINAL
            and producer_kind
            not in {
                "HTTPS:usdm_public_rest",
                "HTTPS:usdm_public_depth_rest",
                "HTTPS:usdm_venue_clock_rest",
            }
        ):
            raise ValueError(
                "wall-regression preservation is restricted to terminal HTTPS attempts"
            )
        self._raise_if_ordering_failed()
        if self._pending_reservation_count >= _MAX_PENDING_INGRESS_RESERVATIONS:
            failure = SharedIngressOrderingErrorV2(
                "shared ingress exceeded its bounded pending-reservation capacity"
            )
            self._latch_ordering_failure(failure)
            raise failure
        prior_global_wall_ms = None
        if (
            self._last_reserved_wall_ms is not None
            and receipt.received_at_ms < self._last_reserved_wall_ms
        ):
            if wall_order_policy is _IngressWallOrderPolicyV2.STRICT:
                failure = SharedIngressOrderingErrorV2(
                    "producer wall receipt moved backwards before sequence reservation"
                )
                self._latch_ordering_failure(failure)
                raise failure
            prior_global_wall_ms = self._last_reserved_wall_ms
        if (
            self._last_reserved_monotonic_ns is not None
            and receipt.received_monotonic_ns < self._last_reserved_monotonic_ns
        ):
            failure = SharedIngressOrderingErrorV2(
                "producer monotonic receipt moved backwards before sequence reservation"
            )
            self._latch_ordering_failure(failure)
            raise failure
        reservation = _IngressReservationV2(
            ingest_seq=self._sequencer.next(),
            receipt_wall_ms=receipt.received_at_ms,
            receipt_monotonic_ns=receipt.received_monotonic_ns,
            producer_kind=producer_kind,
            prior_global_wall_ms=prior_global_wall_ms,
        )
        self._pending_reservation_count += 1
        if (
            self._last_reserved_wall_ms is None
            or receipt.received_at_ms > self._last_reserved_wall_ms
        ):
            self._last_reserved_wall_ms = receipt.received_at_ms
        self._last_reserved_monotonic_ns = receipt.received_monotonic_ns
        return reservation

    @asynccontextmanager
    async def _admission_turn(
        self,
        reservation: _IngressReservationV2,
    ) -> AsyncIterator[None]:
        try:
            async with self._ingress_lock:
                self._raise_if_ordering_failed()
                if reservation.ingest_seq != self._next_admit_ingest_seq:
                    raise SharedIngressOrderingErrorV2(
                        "shared-ingress reservation reached admission out of order"
                    )
                yield
                if not reservation.admitted:
                    raise SharedIngressOrderingErrorV2(
                        "shared-ingress reservation exited without bounded admission"
                    )
        except asyncio.CancelledError:
            if not reservation.admitted:
                self._latch_ordering_failure(
                    SharedIngressOrderingErrorV2(
                        "shared-ingress reservation was cancelled before admission"
                    )
                )
            raise
        except BaseException as exc:
            self._latch_ordering_failure(exc)
            raise
        finally:
            self._release_reservation(reservation)

    def _mark_admitted(self, reservation: _IngressReservationV2) -> None:
        if reservation.admitted:
            raise SharedIngressOrderingErrorV2(
                "shared-ingress reservation attempted duplicate admission"
            )
        if reservation.ingest_seq != self._next_admit_ingest_seq:
            raise SharedIngressOrderingErrorV2(
                "shared-ingress admitted sequence differs from its reservation turn"
            )
        reservation.admitted = True
        self._next_admit_ingest_seq += 1

    def _release_reservation(self, reservation: _IngressReservationV2) -> None:
        if reservation.released:
            return
        reservation.released = True
        self._pending_reservation_count -= 1
        if self._pending_reservation_count < 0:
            failure = SharedIngressOrderingErrorV2(
                "shared-ingress pending-reservation count underflowed"
            )
            self._latch_ordering_failure(failure)
            raise failure

    def _latch_ordering_failure(self, failure: BaseException) -> None:
        if self._ordering_failure is None:
            self._ordering_failure = failure

    def _raise_if_ordering_failed(self) -> None:
        if self._ordering_failure is None:
            return
        raise SharedIngressOrderingErrorV2(
            "shared ingress is fail-closed after an ordering failure"
        ) from self._ordering_failure

    async def offer_https_attempt(
        self,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        symbol: str,
        clock: ReceiptClock,
        observation: PublicOiRestTerminalObservationV2,
        source_logical_key: str,
        cancellation_requested: asyncio.Event | None = None,
    ) -> PublicOiAdmissionReceiptV2:
        """Admit one public HTTPS attempt through the shared global sequence gate.

        The completion timestamp and global sequence reservation are sampled
        before waiting for the same admission lock used by both WebSocket
        routes. Admission cancellation is selected again when that reservation
        reaches the bounded queue. Payload construction, envelope construction,
        and the bounded offer are synchronous inside that admission turn.
        """

        _validate_https_attempt_inputs(
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
            symbol=symbol,
            clock=clock,
            observation=observation,
            source_logical_key=source_logical_key,
            cancellation_requested=cancellation_requested,
        )
        _validate_https_queue_admitter(self._pipeline)
        queue_admitter = cast(_HttpsQueueAdmitterV2, self._pipeline)
        completion = clock.capture()
        _validate_receipt_timestamp(completion, "HTTPS completion receipt")
        preflight_observation = (
            observation.with_admission_cancellation_v2()
            if cancellation_requested is not None and cancellation_requested.is_set()
            else observation
        )
        _preflight_https_observation_payload_v2(
            observation=preflight_observation,
            plan=plan,
            symbol=symbol,
            completion=completion,
        )
        reservation = self._reserve_after_receipt(
            completion,
            producer_kind="HTTPS:usdm_public_rest",
            wall_order_policy=(_IngressWallOrderPolicyV2.PRESERVE_HTTPS_TERMINAL),
        )
        async with self._admission_turn(reservation):
            admitted_observation = (
                observation.with_admission_cancellation_v2()
                if cancellation_requested is not None and cancellation_requested.is_set()
                else observation
            )
            raw_payload = _preflight_https_observation_payload_v2(
                observation=admitted_observation,
                plan=plan,
                symbol=symbol,
                completion=completion,
            )
            wall_clock_regression = _mint_https_rest_wall_regression_evidence_v2(
                reservation=reservation,
                request_started_wall_ms=admitted_observation.request_started_wall_ms,
                response_first_header_wall_ms=(admitted_observation.response_first_header_wall_ms),
                attempt_ended_wall_ms=admitted_observation.attempt_ended_wall_ms,
                completion_admission_wall_ms=completion.received_at_ms,
            )
            record = RawRecordV2.from_payload(
                session_id=session_id,
                plan_id=plan.name,
                protocol_hash=protocol_hash,
                transport=TransportV2.HTTPS,
                venue=plan.venue,
                route_id=plan.route_id,
                symbol=symbol,
                connection_id=connection_id,
                generation=generation,
                frame_seq=None,
                ingest_seq=reservation.ingest_seq,
                receipt_wall_ms=completion.received_at_ms,
                receipt_monotonic_ns=completion.received_monotonic_ns,
                raw_payload=raw_payload,
                source_logical_key=source_logical_key,
            )
            queue_admission_receipt = _offer_with_queue_admission_receipt_v2(
                queue_admitter,
                record,
            )
            self._mark_admitted(reservation)
            return PublicOiAdmissionReceiptV2(
                record=record,
                queue_admission_receipt=queue_admission_receipt,
                wall_clock_regression=wall_clock_regression,
                _factory_token=_PUBLIC_OI_ADMISSION_RECEIPT_FACTORY_TOKEN,
            )

    async def offer_https_census(
        self,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2,
        session_id: str,
        protocol_hash: str,
        clock: ReceiptClock,
        payload: PublicOiRestCensusPayloadV2,
    ) -> PublicOiCensusAdmissionReceiptV2:
        """Admit one exact OI census carrier through the shared sequence gate."""

        raw_payload = _validate_and_encode_https_census_inputs(
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            clock=clock,
            payload=payload,
        )
        _validate_https_queue_admitter(self._pipeline)
        queue_admitter = cast(_HttpsQueueAdmitterV2, self._pipeline)
        receipt = clock.capture()
        _validate_receipt_timestamp(receipt, "HTTPS census receipt")
        _validate_https_census_receipt_order(payload=payload, receipt=receipt)
        reservation = self._reserve_after_receipt(
            receipt,
            producer_kind="HTTPS:usdm_public_rest:census",
        )
        async with self._admission_turn(reservation):
            record = RawRecordV2.from_payload(
                session_id=session_id,
                plan_id=plan.name,
                protocol_hash=protocol_hash,
                transport=TransportV2.HTTPS,
                venue=plan.venue,
                route_id=plan.route_id,
                symbol=None,
                connection_id=_PUBLIC_OI_CENSUS_CONNECTION_ID,
                generation=1,
                frame_seq=None,
                ingest_seq=reservation.ingest_seq,
                receipt_wall_ms=receipt.received_at_ms,
                receipt_monotonic_ns=receipt.received_monotonic_ns,
                raw_payload=raw_payload,
                source_logical_key=_PUBLIC_OI_CENSUS_SOURCE_LOGICAL_KEY,
            )
            queue_admission_receipt = _offer_with_queue_admission_receipt_v2(
                queue_admitter,
                record,
            )
            self._mark_admitted(reservation)
            return PublicOiCensusAdmissionReceiptV2(
                record=record,
                queue_admission_receipt=queue_admission_receipt,
                _factory_token=_PUBLIC_OI_CENSUS_ADMISSION_RECEIPT_FACTORY_TOKEN,
            )

    async def offer_depth_https_attempt_v8(
        self,
        *,
        plan: ProvisionalDepthRestQualificationPlanV8,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        symbol: str,
        clock: ReceiptClock,
        observation: PublicDepthRestTerminalObservationV8,
        source_logical_key: str,
        cancellation_requested: asyncio.Event | None = None,
    ) -> PublicDepthRestAdmissionReceiptV8:
        """Admit one qualification-only depth attempt under the global gate."""

        _validate_depth_https_attempt_inputs_v8(
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
            symbol=symbol,
            clock=clock,
            observation=observation,
            source_logical_key=source_logical_key,
            cancellation_requested=cancellation_requested,
        )
        _validate_https_queue_admitter(self._pipeline)
        queue_admitter = cast(_HttpsQueueAdmitterV2, self._pipeline)
        completion = clock.capture()
        _validate_receipt_timestamp(completion, "depth HTTPS completion receipt")
        preflight_observation = (
            observation.with_admission_cancellation_v8()
            if cancellation_requested is not None and cancellation_requested.is_set()
            else observation
        )
        _preflight_depth_https_observation_payload_v8(
            observation=preflight_observation,
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            symbol=symbol,
            generation=generation,
            completion=completion,
        )
        reservation = self._reserve_after_receipt(
            completion,
            producer_kind="HTTPS:usdm_public_depth_rest",
            wall_order_policy=(_IngressWallOrderPolicyV2.PRESERVE_HTTPS_TERMINAL),
        )
        async with self._admission_turn(reservation):
            admitted_observation = (
                observation.with_admission_cancellation_v8()
                if cancellation_requested is not None and cancellation_requested.is_set()
                else observation
            )
            raw_payload = _preflight_depth_https_observation_payload_v8(
                observation=admitted_observation,
                plan=plan,
                session_id=session_id,
                protocol_hash=protocol_hash,
                connection_id=connection_id,
                symbol=symbol,
                generation=generation,
                completion=completion,
            )
            wall_clock_regression = _mint_https_rest_wall_regression_evidence_v2(
                reservation=reservation,
                request_started_wall_ms=admitted_observation.request_started_wall_ms,
                response_first_header_wall_ms=(admitted_observation.response_first_header_wall_ms),
                attempt_ended_wall_ms=admitted_observation.attempt_ended_wall_ms,
                completion_admission_wall_ms=completion.received_at_ms,
            )
            record = RawRecordV2.from_payload(
                session_id=session_id,
                plan_id=plan.name,
                protocol_hash=protocol_hash,
                transport=TransportV2.HTTPS,
                venue=plan.venue,
                route_id=plan.route_id,
                symbol=symbol,
                connection_id=connection_id,
                generation=generation,
                frame_seq=None,
                ingest_seq=reservation.ingest_seq,
                receipt_wall_ms=completion.received_at_ms,
                receipt_monotonic_ns=completion.received_monotonic_ns,
                raw_payload=raw_payload,
                source_logical_key=source_logical_key,
            )
            queue_admission_receipt = _offer_with_queue_admission_receipt_v2(
                queue_admitter,
                record,
            )
            self._mark_admitted(reservation)
            return PublicDepthRestAdmissionReceiptV8(
                plan=plan,
                record=record,
                queue_admission_receipt=queue_admission_receipt,
                wall_clock_regression=wall_clock_regression,
                _factory_token=_PUBLIC_DEPTH_REST_ADMISSION_RECEIPT_FACTORY_TOKEN,
            )

    async def offer_usdm_venue_clock_https_attempt_v9(
        self,
        *,
        plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        clock: ReceiptClock,
        observation: PublicUsdmVenueClockRestTerminalObservationV9,
        source_logical_key: str,
        cancellation_requested: asyncio.Event | None = None,
    ) -> PublicUsdmVenueClockAdmissionReceiptV9:
        """Admit one public venue-time attempt through the global queue gate."""

        _validate_usdm_venue_clock_https_attempt_inputs_v9(
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
            clock=clock,
            observation=observation,
            source_logical_key=source_logical_key,
            cancellation_requested=cancellation_requested,
        )
        _validate_https_queue_admitter(self._pipeline)
        queue_admitter = cast(_HttpsQueueAdmitterV2, self._pipeline)
        completion = clock.capture()
        _validate_receipt_timestamp(completion, "venue-clock HTTPS completion receipt")
        preflight_observation = (
            observation.with_admission_cancellation_v9()
            if cancellation_requested is not None and cancellation_requested.is_set()
            else observation
        )
        _preflight_usdm_venue_clock_payload_v9(
            observation=preflight_observation,
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
            completion=completion,
        )
        reservation = self._reserve_after_receipt(
            completion,
            producer_kind="HTTPS:usdm_venue_clock_rest",
            wall_order_policy=_IngressWallOrderPolicyV2.PRESERVE_HTTPS_TERMINAL,
        )
        async with self._admission_turn(reservation):
            admitted_observation = (
                observation.with_admission_cancellation_v9()
                if cancellation_requested is not None
                and cancellation_requested.is_set()
                else observation
            )
            raw_payload = _preflight_usdm_venue_clock_payload_v9(
                observation=admitted_observation,
                plan=plan,
                session_id=session_id,
                protocol_hash=protocol_hash,
                connection_id=connection_id,
                generation=generation,
                completion=completion,
            )
            wall_clock_regression = _mint_https_rest_wall_regression_evidence_v2(
                reservation=reservation,
                request_started_wall_ms=admitted_observation.request_started_wall_ms,
                response_first_header_wall_ms=(
                    admitted_observation.response_first_header_wall_ms
                ),
                attempt_ended_wall_ms=admitted_observation.attempt_ended_wall_ms,
                completion_admission_wall_ms=completion.received_at_ms,
            )
            record = RawRecordV2.from_payload(
                session_id=session_id,
                plan_id=plan.name,
                protocol_hash=protocol_hash,
                transport=TransportV2.HTTPS,
                venue=plan.venue,
                route_id=plan.route_id,
                symbol=None,
                connection_id=connection_id,
                generation=generation,
                frame_seq=None,
                ingest_seq=reservation.ingest_seq,
                receipt_wall_ms=completion.received_at_ms,
                receipt_monotonic_ns=completion.received_monotonic_ns,
                raw_payload=raw_payload,
                source_logical_key=source_logical_key,
            )
            queue_receipt = _offer_with_queue_admission_receipt_v2(
                queue_admitter,
                record,
            )
            self._mark_admitted(reservation)
            return PublicUsdmVenueClockAdmissionReceiptV9(
                plan=plan,
                record=record,
                queue_admission_receipt=queue_receipt,
                wall_clock_regression=wall_clock_regression,
                _factory_token=(
                    _PUBLIC_USDM_VENUE_CLOCK_ADMISSION_RECEIPT_FACTORY_TOKEN
                ),
            )

    def _offer_frame_reserved(
        self,
        *,
        reservation: _IngressReservationV2,
        plan: ProvisionalPromotingCapturePlanV2,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        frame_seq: int,
        receipt: ReceiptTimestamp,
        raw_payload: bytes,
    ) -> RawRecordV2:
        record = RawRecordV2.from_payload(
            session_id=session_id,
            plan_id=plan.name,
            protocol_hash=protocol_hash,
            transport=TransportV2.WEBSOCKET,
            venue=plan.venue,
            route_id=plan.route_id,
            symbol=None,
            connection_id=connection_id,
            generation=generation,
            frame_seq=frame_seq,
            ingest_seq=reservation.ingest_seq,
            receipt_wall_ms=receipt.received_at_ms,
            receipt_monotonic_ns=receipt.received_monotonic_ns,
            raw_payload=raw_payload,
        )
        # Capture rejection is fatal at the bounded pipeline boundary.  Do not
        # swallow, retry, or request another socket frame.
        self._pipeline.offer(record)
        return record


class PublicWebSocketCaptureAdapterV2:
    """Retain V2 Binance frames at the existing owner's iterator seam."""

    def __init__(
        self,
        plan: ProvisionalPromotingCapturePlanV2,
        *,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        clock: ReceiptClock,
        ingress: SharedWebSocketIngressV2,
        recovery_lifecycle: WebSocketRecoveryLifecycleV2,
        _factory_token: object | None = None,
        _factory_capability: object | None = None,
    ) -> None:
        if _factory_token is not _PUBLIC_WEBSOCKET_CAPTURE_ADAPTER_FACTORY_TOKEN:
            raise TypeError(
                "PublicWebSocketCaptureAdapterV2 can only be created by its exact factory"
            )
        if type(_factory_capability) is not object:
            raise TypeError("V2 WebSocket adapter requires its factory capability")
        _validate_lineage(session_id=session_id, protocol_hash=protocol_hash)
        _validate_identity(connection_id, "connection_id")
        _validate_recovery_lifecycle(recovery_lifecycle)
        if type(generation) is not int or generation < 1:
            raise ValueError("generation must be a positive integer")
        # Prove compatibility with the sole existing socket owner without
        # weakening its routed-public URL and stream validation.
        build_public_websocket_owner_plan_v2(plan)
        self.plan = plan
        self.session_id = session_id
        self.protocol_hash = protocol_hash
        self.connection_id = connection_id
        self.generation = generation
        self.clock = clock
        self.ingress = ingress
        self.recovery_lifecycle = recovery_lifecycle
        self.frame_seq = 0
        self._factory_seal = _PUBLIC_WEBSOCKET_CAPTURE_ADAPTER_FACTORY_TOKEN
        self._factory_capability = _factory_capability
        self._last_admitted_raw_record_v2: RawRecordV2 | None = None
        self._last_admitted_raw_record_v2_seal: (
            tuple[object, RawRecordV2, tuple[object, ...]] | None
        ) = None
        self._last_retained_depth_range_receipt_frame_seq = 0
        self._last_retained_depth_resync_receipt_frame_seq = 0
        self._retained_depth_mint_cursor_seal: tuple[object, int, int] = (
            self._factory_capability,
            0,
            0,
        )

    @property
    def last_admitted_raw_record_v2(self) -> RawRecordV2 | None:
        """Return the exact last frame published after offer and lifecycle completion."""

        return self._last_admitted_raw_record_v2

    async def consume(self, frames: AsyncIterable[str | bytes]) -> None:
        async for raw in frames:
            receipt = self.clock.capture()
            # Receipt capture above is intentionally the first statement after
            # the socket iterator yields.  No JSON or combined-wrapper parsing
            # occurs on this producer path.
            retained_raw = _immutable_raw_bytes(raw)
            candidate_frame_seq = self.frame_seq + 1
            if candidate_frame_seq == 1:
                offer_crossed = False

                async def complete(record: RawRecordV2) -> None:
                    nonlocal offer_crossed
                    offer_crossed = True
                    await self.recovery_lifecycle.complete_recovery_successor(record)

                try:
                    record = await self.ingress.offer_recovery_successor(
                        plan=self.plan,
                        session_id=self.session_id,
                        protocol_hash=self.protocol_hash,
                        connection_id=self.connection_id,
                        generation=self.generation,
                        frame_seq=candidate_frame_seq,
                        receipt=receipt,
                        raw_payload=retained_raw,
                        complete=complete,
                    )
                except asyncio.CancelledError:
                    if not offer_crossed:
                        self._trip_pre_offer_cancellation(
                            candidate_frame_seq=candidate_frame_seq,
                            receipt=receipt,
                            raw_payload=retained_raw,
                        )
                    raise
            else:
                try:
                    record = await self.ingress.offer_frame(
                        plan=self.plan,
                        session_id=self.session_id,
                        protocol_hash=self.protocol_hash,
                        connection_id=self.connection_id,
                        generation=self.generation,
                        frame_seq=candidate_frame_seq,
                        receipt=receipt,
                        raw_payload=retained_raw,
                    )
                except asyncio.CancelledError:
                    self._trip_pre_offer_cancellation(
                        candidate_frame_seq=candidate_frame_seq,
                        receipt=receipt,
                        raw_payload=retained_raw,
                    )
                    raise
                self.recovery_lifecycle.record_retained_frame(record)
            # Publish only the exact tail whose offer and lifecycle callback
            # completed. Recovery frame 1 is not accepted until BOUNDED commits.
            self.frame_seq = candidate_frame_seq
            self._last_admitted_raw_record_v2 = record
            self._last_admitted_raw_record_v2_seal = (
                self._factory_capability,
                record,
                _raw_record_publication_material_v2(record),
            )

    def _trip_pre_offer_cancellation(
        self,
        *,
        candidate_frame_seq: int,
        receipt: ReceiptTimestamp,
        raw_payload: bytes,
    ) -> None:
        self.recovery_lifecycle.trip_fatal(
            WebSocketFrameRetentionCancelledV2(
                plan_id=self.plan.name,
                route_id=self.plan.route_id,
                connection_id=self.connection_id,
                generation=self.generation,
                candidate_frame_seq=candidate_frame_seq,
                receipt=receipt,
                raw_payload_sha256=hashlib.sha256(raw_payload).hexdigest(),
            )
        )


class PublicWebSocketFrameAdapterFactoryV2:
    """Plan-bound factory matching ``PublicWebSocketCaptureOwner``'s seam."""

    def __init__(
        self,
        plan: ProvisionalPromotingCapturePlanV2,
        *,
        session_id: str,
        protocol_hash: str,
        clock: ReceiptClock,
        ingress: SharedWebSocketIngressV2,
        recovery_lifecycle: WebSocketRecoveryLifecycleV2,
    ) -> None:
        _validate_lineage(session_id=session_id, protocol_hash=protocol_hash)
        _validate_recovery_lifecycle(recovery_lifecycle)
        if type(ingress) is not SharedWebSocketIngressV2:
            raise TypeError("V2 frame factory requires exact SharedWebSocketIngressV2")
        self.owner_plan = build_public_websocket_owner_plan_v2(plan)
        self.plan = plan
        self.session_id = session_id
        self.protocol_hash = protocol_hash
        self.clock = clock
        self.ingress = ingress
        self.recovery_lifecycle = recovery_lifecycle
        self._adapter_factory_capability = object()

    def __call__(
        self,
        *,
        connection_id: str,
        generation: int,
    ) -> PublicWebSocketCaptureAdapterV2:
        return PublicWebSocketCaptureAdapterV2(
            self.plan,
            session_id=self.session_id,
            protocol_hash=self.protocol_hash,
            connection_id=connection_id,
            generation=generation,
            clock=self.clock,
            ingress=self.ingress,
            recovery_lifecycle=self.recovery_lifecycle,
            _factory_token=_PUBLIC_WEBSOCKET_CAPTURE_ADAPTER_FACTORY_TOKEN,
            _factory_capability=self._adapter_factory_capability,
        )


def _mint_public_retained_depth_range_callback_receipt_v2(
    *,
    adapter: PublicWebSocketCaptureAdapterV2,
    owner_plan: WebSocketPlan,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    frame_seq: int,
    raw: str | bytes,
    observation: DepthRangeObservation,
    _owner_seam_token: object | None = None,
) -> PublicRetainedDepthRangeCallbackReceiptV2:
    """Mint one one-shot range receipt from the exact retained-frame seam."""

    _validate_retained_depth_owner_seam_token_v2(_owner_seam_token)
    record, raw_bytes = _validate_retained_depth_callback_source_v2(
        adapter=adapter,
        owner_plan=owner_plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        generation=generation,
        frame_seq=frame_seq,
        raw=raw,
    )
    if type(observation) is not DepthRangeObservation:
        raise TypeError("retained depth callback requires an exact range observation")
    observation.__post_init__()
    if observation.market is not owner_plan.market or observation.generation != generation:
        raise ValueError("depth range observation differs from the current owner generation")
    _validate_depth_range_observation_against_raw_v2(
        raw_bytes,
        plan=adapter.plan,
        observation=observation,
    )
    if adapter._last_retained_depth_range_receipt_frame_seq >= frame_seq:
        raise RuntimeError("retained depth range receipt was already minted for this frame")
    if adapter._last_retained_depth_resync_receipt_frame_seq >= frame_seq:
        raise RuntimeError("retained depth resync receipt cannot precede its range receipt")
    receipt = PublicRetainedDepthRangeCallbackReceiptV2(
        session_id=record.session_id,
        protocol_hash=record.protocol_hash,
        market=owner_plan.market,
        route=owner_plan.route,
        connection_id=record.connection_id,
        generation=record.generation,
        frame_seq=cast(int, record.frame_seq),
        ingest_seq=record.ingest_seq,
        raw_payload_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        receipt_wall_ms=record.receipt_wall_ms,
        receipt_monotonic_ns=record.receipt_monotonic_ns,
        observation=observation,
        _factory_token=_PUBLIC_RETAINED_DEPTH_RANGE_CALLBACK_RECEIPT_FACTORY_TOKEN,
    )
    adapter._last_retained_depth_range_receipt_frame_seq = frame_seq
    _seal_retained_depth_mint_cursors_v2(adapter)
    return receipt


def _mint_public_retained_depth_resync_callback_receipt_v2(
    *,
    adapter: PublicWebSocketCaptureAdapterV2,
    owner_plan: WebSocketPlan,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    frame_seq: int,
    raw: str | bytes,
    request: DepthResyncRequest,
    preceding_range_receipt: PublicRetainedDepthRangeCallbackReceiptV2,
    _owner_seam_token: object | None = None,
) -> PublicRetainedDepthResyncCallbackReceiptV2:
    """Mint one resync receipt after the same frame's range callback receipt."""

    _validate_retained_depth_owner_seam_token_v2(_owner_seam_token)
    record, raw_bytes = _validate_retained_depth_callback_source_v2(
        adapter=adapter,
        owner_plan=owner_plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        generation=generation,
        frame_seq=frame_seq,
        raw=raw,
    )
    if type(request) is not DepthResyncRequest:
        raise TypeError("retained depth callback requires an exact resync request")
    request.__post_init__()
    if request.market is not owner_plan.market or request.generation != generation:
        raise ValueError("depth resync request differs from the current owner generation")
    validate_public_retained_depth_range_callback_receipt_v2(preceding_range_receipt)
    range_observation = preceding_range_receipt.observation
    expected_range_scalars = (
        record.session_id,
        record.protocol_hash,
        owner_plan.market,
        owner_plan.route,
        record.connection_id,
        record.generation,
        record.frame_seq,
        record.ingest_seq,
        hashlib.sha256(raw_bytes).hexdigest(),
        record.receipt_wall_ms,
        record.receipt_monotonic_ns,
    )
    actual_range_scalars = (
        preceding_range_receipt.session_id,
        preceding_range_receipt.protocol_hash,
        preceding_range_receipt.market,
        preceding_range_receipt.route,
        preceding_range_receipt.connection_id,
        preceding_range_receipt.generation,
        preceding_range_receipt.frame_seq,
        preceding_range_receipt.ingest_seq,
        preceding_range_receipt.raw_payload_sha256,
        preceding_range_receipt.receipt_wall_ms,
        preceding_range_receipt.receipt_monotonic_ns,
    )
    if actual_range_scalars != expected_range_scalars:
        raise ValueError("resync callback range receipt belongs to a different frame")
    if not range_observation.reset:
        raise ValueError("a resync callback requires a resetting range observation")
    current_watermark = (range_observation.symbol, range_observation.U)
    if request.event == "sequence_gap":
        if request.watermarks != (current_watermark,):
            raise ValueError("sequence-gap resync differs from its retained gap frame")
    elif current_watermark not in request.watermarks:
        raise ValueError("generation resync omits its current retained baseline frame")
    if request.event in ("startup", "reconnect"):
        expected_symbols = tuple(
            sorted(
                stream.split("@", 1)[0].upper()
                for stream in adapter.plan.streams
                if stream.endswith("@depth@100ms")
            )
        )
        if tuple(symbol for symbol, _first_u in request.watermarks) != expected_symbols:
            raise ValueError(
                "generation resync watermarks differ from the exact depth-stream census"
            )
    if adapter._last_retained_depth_range_receipt_frame_seq != frame_seq:
        raise RuntimeError("retained depth resync requires this frame's range receipt")
    if adapter._last_retained_depth_resync_receipt_frame_seq >= frame_seq:
        raise RuntimeError("retained depth resync receipt was already minted for this frame")
    receipt = PublicRetainedDepthResyncCallbackReceiptV2(
        session_id=record.session_id,
        protocol_hash=record.protocol_hash,
        market=owner_plan.market,
        route=owner_plan.route,
        connection_id=record.connection_id,
        generation=record.generation,
        frame_seq=cast(int, record.frame_seq),
        ingest_seq=record.ingest_seq,
        raw_payload_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        receipt_wall_ms=record.receipt_wall_ms,
        receipt_monotonic_ns=record.receipt_monotonic_ns,
        request=request,
        _factory_token=_PUBLIC_RETAINED_DEPTH_RESYNC_CALLBACK_RECEIPT_FACTORY_TOKEN,
    )
    adapter._last_retained_depth_resync_receipt_frame_seq = frame_seq
    _seal_retained_depth_mint_cursors_v2(adapter)
    return receipt


def _validate_retained_depth_owner_seam_token_v2(value: object | None) -> None:
    if value is not _RETAINED_DEPTH_OWNER_SEAM_TOKEN_V2:
        raise TypeError("retained depth receipts can only be minted by the owner post-offer seam")


def _validate_retained_depth_callback_source_v2(
    *,
    adapter: PublicWebSocketCaptureAdapterV2,
    owner_plan: WebSocketPlan,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    frame_seq: int,
    raw: str | bytes,
) -> tuple[RawRecordV2, bytes]:
    if type(adapter) is not PublicWebSocketCaptureAdapterV2:
        raise TypeError("retained depth callback requires the exact V2 frame adapter")
    if (
        getattr(adapter, "_factory_seal", None)
        is not _PUBLIC_WEBSOCKET_CAPTURE_ADAPTER_FACTORY_TOKEN
        or type(getattr(adapter, "_factory_capability", None)) is not object
    ):
        raise ValueError("retained depth callback adapter lacks factory provenance")
    if type(owner_plan) is not WebSocketPlan:
        raise TypeError("retained depth callback requires the exact owner plan type")
    validate_public_websocket_plan(owner_plan)
    if type(adapter.plan) is not ProvisionalPromotingCapturePlanV2:
        raise TypeError("retained depth callback adapter has a foreign capture plan")
    adapter.plan.__post_init__()
    if build_public_websocket_owner_plan_v2(adapter.plan) != owner_plan:
        raise ValueError("retained depth callback adapter belongs to another owner plan")
    if adapter.plan.route_id != "usdm_public" or owner_plan.route != "public":
        raise ValueError("retained depth callbacks require the USD-M public depth route")
    _validate_lineage(session_id=session_id, protocol_hash=protocol_hash)
    _validate_identity(connection_id, "retained depth current connection_id")
    if type(generation) is not int or generation < 1:
        raise ValueError("retained depth current generation must be positive")
    if type(frame_seq) is not int or frame_seq < 1:
        raise ValueError("retained depth current frame_seq must be positive")
    if (
        adapter.session_id != session_id
        or adapter.protocol_hash != protocol_hash
        or adapter.connection_id != connection_id
        or adapter.generation != generation
        or adapter.frame_seq != frame_seq
    ):
        raise ValueError("retained depth adapter differs from the current owner generation")
    record = adapter.last_admitted_raw_record_v2
    if record is None:
        raise RuntimeError("retained depth adapter has no admitted raw record")
    if type(record) is not RawRecordV2:
        raise TypeError("retained depth adapter must expose an exact RawRecordV2")
    seal = getattr(adapter, "_last_admitted_raw_record_v2_seal", None)
    if (
        type(seal) is not tuple
        or len(seal) != 3
        or seal[0] is not adapter._factory_capability
        or seal[1] is not record
        or seal[2] != _raw_record_publication_material_v2(record)
    ):
        raise ValueError("retained depth raw record lacks adapter publication provenance")
    record.__post_init__()
    if (
        record.session_id != session_id
        or record.protocol_hash != protocol_hash
        or record.plan_id != adapter.plan.name
        or record.transport is not TransportV2.WEBSOCKET
        or record.venue is not adapter.plan.venue
        or record.route_id != adapter.plan.route_id
        or record.symbol is not None
        or record.connection_id != connection_id
        or record.generation != generation
        or record.frame_seq != frame_seq
        or record.source_logical_key is not None
    ):
        raise ValueError("retained depth raw record differs from its current outer identity")
    raw_bytes = _immutable_raw_bytes(raw)
    if record.payload_bytes() != raw_bytes:
        raise ValueError("retained depth raw record differs from the current socket frame")
    for field_name in (
        "_last_retained_depth_range_receipt_frame_seq",
        "_last_retained_depth_resync_receipt_frame_seq",
    ):
        cursor = getattr(adapter, field_name, None)
        if type(cursor) is not int or not 0 <= cursor <= frame_seq:
            raise ValueError("retained depth callback mint cursor is invalid")
    if getattr(adapter, "_retained_depth_mint_cursor_seal", None) != (
        adapter._factory_capability,
        adapter._last_retained_depth_range_receipt_frame_seq,
        adapter._last_retained_depth_resync_receipt_frame_seq,
    ):
        raise ValueError("retained depth callback mint cursor provenance was mutated")
    return record, raw_bytes


def _seal_retained_depth_mint_cursors_v2(
    adapter: PublicWebSocketCaptureAdapterV2,
) -> None:
    adapter._retained_depth_mint_cursor_seal = (
        adapter._factory_capability,
        adapter._last_retained_depth_range_receipt_frame_seq,
        adapter._last_retained_depth_resync_receipt_frame_seq,
    )


def _validate_depth_range_observation_against_raw_v2(
    raw_bytes: bytes,
    *,
    plan: ProvisionalPromotingCapturePlanV2,
    observation: DepthRangeObservation,
) -> None:
    try:
        payload = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise ValueError("retained depth callback raw frame is not valid JSON") from exc
    if type(payload) is not dict:
        raise ValueError("retained depth callback raw frame root must be an object")
    data = payload.get("data")
    stream = payload.get("stream")
    expected_stream = f"{observation.symbol.lower()}@depth@100ms"
    if stream != expected_stream or stream not in plan.streams or type(data) is not dict:
        raise ValueError("retained depth range differs from its subscribed raw frame")
    if (
        data.get("e") != "depthUpdate"
        or data.get("s") != observation.symbol
        or data.get("U") != observation.U
        or data.get("u") != observation.u
    ):
        raise ValueError("retained depth range differs from its raw event")
    for field_name in ("U", "u", "pu", "st"):
        value = data.get(field_name)
        if type(value) is not int or value < 0:
            raise ValueError(f"retained depth raw {field_name} must be nonnegative integer")
    if data.get("st") != 1 or data.get("ps") != observation.symbol:
        raise ValueError("retained depth raw public-stream identity differs")


def build_public_websocket_owner_plan_v2(
    plan: ProvisionalPromotingCapturePlanV2,
) -> WebSocketPlan:
    """Adapt one V2 USD-M plan to the existing validated socket-owner contract."""

    if not isinstance(plan, ProvisionalPromotingCapturePlanV2):
        raise TypeError("V2 WebSocket owner plan requires a promoting WebSocket plan")
    owner_route = "market" if plan.route_id == "usdm_market" else "public"
    url = plan.combined_base_url + "/".join(quote(stream, safe="@!_-") for stream in plan.streams)
    owner_plan = WebSocketPlan(
        name=plan.name,
        market=Market.FUTURES,
        route=owner_route,
        streams=plan.streams,
        url=url,
    )
    validate_public_websocket_plan(owner_plan)
    return owner_plan


def _immutable_raw_bytes(raw: str | bytes) -> bytes:
    if isinstance(raw, bytes):
        return raw
    if isinstance(raw, str):
        try:
            return raw.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("WebSocket text frame must be valid UTF-8") from exc
    raise TypeError("WebSocket frame must be str or bytes")


def _raw_record_publication_material_v2(record: RawRecordV2) -> tuple[object, ...]:
    """Snapshot every immutable raw-record field at adapter publication."""

    return (
        record.schema_version,
        record.session_id,
        record.plan_id,
        record.protocol_hash,
        record.source,
        record.transport,
        record.venue,
        record.route_id,
        record.symbol,
        record.connection_id,
        record.generation,
        record.frame_seq,
        record.ingest_seq,
        record.receipt_wall_ms,
        record.receipt_monotonic_ns,
        record.raw_encoding,
        record.raw_len,
        record.raw_payload,
        record.source_logical_key,
    )


def _mint_https_rest_wall_regression_evidence_v2(
    *,
    reservation: _IngressReservationV2,
    request_started_wall_ms: int,
    response_first_header_wall_ms: int | None,
    attempt_ended_wall_ms: int,
    completion_admission_wall_ms: int,
) -> PublicHttpsRestWallClockRegressionEvidenceV2 | None:
    intra_attempt_regression = _wall_sequence_regressed_v2(
        request_started_wall_ms=request_started_wall_ms,
        response_first_header_wall_ms=response_first_header_wall_ms,
        attempt_ended_wall_ms=attempt_ended_wall_ms,
        completion_admission_wall_ms=completion_admission_wall_ms,
    )
    if not intra_attempt_regression and reservation.prior_global_wall_ms is None:
        return None
    return PublicHttpsRestWallClockRegressionEvidenceV2(
        ingest_seq=reservation.ingest_seq,
        request_started_wall_ms=request_started_wall_ms,
        response_first_header_wall_ms=response_first_header_wall_ms,
        attempt_ended_wall_ms=attempt_ended_wall_ms,
        completion_admission_wall_ms=completion_admission_wall_ms,
        prior_global_wall_ms=reservation.prior_global_wall_ms,
        _factory_token=_HTTPS_REST_WALL_REGRESSION_EVIDENCE_FACTORY_TOKEN,
    )


def _validate_https_rest_wall_regression_evidence_material_v2(
    evidence: PublicHttpsRestWallClockRegressionEvidenceV2,
) -> None:
    for field_name, value in (
        ("ingest_seq", evidence.ingest_seq),
        ("request_started_wall_ms", evidence.request_started_wall_ms),
        ("attempt_ended_wall_ms", evidence.attempt_ended_wall_ms),
        (
            "completion_admission_wall_ms",
            evidence.completion_admission_wall_ms,
        ),
    ):
        if type(value) is not int or value < (1 if field_name == "ingest_seq" else 0):
            raise ValueError(f"{field_name} has an invalid wall-regression value")
    first_wall = evidence.response_first_header_wall_ms
    if first_wall is not None and (type(first_wall) is not int or first_wall < 0):
        raise ValueError("response_first_header_wall_ms has an invalid regression value")
    prior_wall = evidence.prior_global_wall_ms
    if prior_wall is not None and (type(prior_wall) is not int or prior_wall < 0):
        raise ValueError("prior_global_wall_ms has an invalid regression value")
    if prior_wall is not None and evidence.completion_admission_wall_ms >= prior_wall:
        raise ValueError("prior-global wall-regression evidence does not regress")
    if not evidence.intra_attempt_regression and not evidence.prior_global_regression:
        raise ValueError("wall-regression evidence contains no regression")


def _wall_sequence_regressed_v2(
    *,
    request_started_wall_ms: int,
    response_first_header_wall_ms: int | None,
    attempt_ended_wall_ms: int,
    completion_admission_wall_ms: int,
) -> bool:
    walls = [request_started_wall_ms]
    if response_first_header_wall_ms is not None:
        walls.append(response_first_header_wall_ms)
    walls.extend((attempt_ended_wall_ms, completion_admission_wall_ms))
    return any(current < previous for previous, current in pairwise(walls))


def _validate_wall_regression_evidence_against_oi_record_v2(
    evidence: PublicHttpsRestWallClockRegressionEvidenceV2 | None,
    record: RawRecordV2,
) -> None:
    payload = PublicOiRestAttemptPayloadV2.from_canonical_bytes(record.payload_bytes())
    _validate_wall_regression_evidence_against_payload_v2(
        evidence,
        record=record,
        request_started_wall_ms=payload.request_started_wall_ms,
        response_first_header_wall_ms=payload.response_first_header_wall_ms,
        attempt_ended_wall_ms=payload.attempt_ended_wall_ms,
        completion_admission_wall_ms=payload.completion_admission_wall_ms,
    )


def _validate_wall_regression_evidence_against_depth_record_v8(
    evidence: PublicHttpsRestWallClockRegressionEvidenceV2 | None,
    record: RawRecordV2,
    payload: PublicDepthRestAttemptPayloadV8,
) -> None:
    _validate_wall_regression_evidence_against_payload_v2(
        evidence,
        record=record,
        request_started_wall_ms=payload.request_started_wall_ms,
        response_first_header_wall_ms=payload.response_first_header_wall_ms,
        attempt_ended_wall_ms=payload.attempt_ended_wall_ms,
        completion_admission_wall_ms=payload.completion_admission_wall_ms,
    )


def _validate_wall_regression_evidence_against_payload_v2(
    evidence: PublicHttpsRestWallClockRegressionEvidenceV2 | None,
    *,
    record: RawRecordV2,
    request_started_wall_ms: int,
    response_first_header_wall_ms: int | None,
    attempt_ended_wall_ms: int,
    completion_admission_wall_ms: int,
) -> None:
    intra_attempt_regression = _wall_sequence_regressed_v2(
        request_started_wall_ms=request_started_wall_ms,
        response_first_header_wall_ms=response_first_header_wall_ms,
        attempt_ended_wall_ms=attempt_ended_wall_ms,
        completion_admission_wall_ms=completion_admission_wall_ms,
    )
    if evidence is None:
        if intra_attempt_regression:
            raise ValueError("regressed HTTPS REST payload lacks process-local evidence")
        return
    validate_public_https_rest_wall_clock_regression_evidence_v2(evidence)
    if (
        evidence.ingest_seq != record.ingest_seq
        or evidence.request_started_wall_ms != request_started_wall_ms
        or evidence.response_first_header_wall_ms != response_first_header_wall_ms
        or evidence.attempt_ended_wall_ms != attempt_ended_wall_ms
        or evidence.completion_admission_wall_ms != completion_admission_wall_ms
        or evidence.completion_admission_wall_ms != record.receipt_wall_ms
        or evidence.intra_attempt_regression != intra_attempt_regression
    ):
        raise ValueError("wall-regression evidence differs from its admitted record")


def _validate_lineage(*, session_id: str, protocol_hash: str) -> None:
    _validate_identity(session_id, "session_id")
    if not isinstance(protocol_hash, str) or _SHA256_RE.fullmatch(protocol_hash) is None:
        raise ValueError("protocol_hash must be a lowercase SHA-256 digest")


def _validate_frame_offer_inputs(
    *,
    plan: ProvisionalPromotingCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    frame_seq: int,
    receipt: ReceiptTimestamp,
    raw_payload: bytes,
) -> None:
    if type(plan) is not ProvisionalPromotingCapturePlanV2:
        raise TypeError("WebSocket ingress requires an exact promoting plan")
    build_public_websocket_owner_plan_v2(plan)
    _validate_lineage(session_id=session_id, protocol_hash=protocol_hash)
    _validate_identity(connection_id, "connection_id")
    if type(generation) is not int or generation < 1:
        raise ValueError("generation must be a positive integer")
    if type(frame_seq) is not int or frame_seq < 1:
        raise ValueError("frame_seq must be a positive integer")
    _validate_receipt_timestamp(receipt, "WebSocket frame receipt")
    if type(raw_payload) is not bytes:
        raise TypeError("retained WebSocket payload must be exact bytes")


def _validate_recovery_lifecycle(value: WebSocketRecoveryLifecycleV2) -> None:
    if any(
        not callable(getattr(value, method, None))
        for method in (
            "complete_recovery_successor",
            "record_retained_frame",
            "trip_fatal",
        )
    ):
        raise TypeError("V2 live WebSocket capture requires a recovery lifecycle")


def _validate_https_attempt_inputs(
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    symbol: str,
    clock: ReceiptClock,
    observation: PublicOiRestTerminalObservationV2,
    source_logical_key: str,
    cancellation_requested: asyncio.Event | None,
) -> None:
    if type(plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise TypeError("HTTPS ingress requires an exact promoting OI REST plan")
    plan.__post_init__()
    _validate_lineage(session_id=session_id, protocol_hash=protocol_hash)
    _validate_identity(connection_id, "connection_id")
    if type(generation) is not int or generation < 1:
        raise ValueError("generation must be a positive integer")
    if type(symbol) is not str or symbol not in plan.symbols:
        raise ValueError("HTTPS symbol must occur exactly in the OI REST plan census")
    _validate_identity(source_logical_key, "source_logical_key")
    if source_logical_key != f"openInterest:{symbol}":
        raise ValueError("source_logical_key must be the stable openInterest:symbol key")
    capture = getattr(clock, "capture", None)
    if not callable(capture) or inspect.iscoroutinefunction(capture):
        raise TypeError("HTTPS ingress requires a synchronous ReceiptClock")
    _validate_https_observation(observation=observation, plan=plan, symbol=symbol)
    if cancellation_requested is not None and type(cancellation_requested) is not asyncio.Event:
        raise TypeError("cancellation_requested must be an exact asyncio.Event or None")


def _validate_depth_https_attempt_inputs_v8(
    *,
    plan: ProvisionalDepthRestQualificationPlanV8,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    symbol: str,
    clock: ReceiptClock,
    observation: PublicDepthRestTerminalObservationV8,
    source_logical_key: str,
    cancellation_requested: asyncio.Event | None,
) -> None:
    if type(plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("depth HTTPS ingress requires the exact v8 qualification plan")
    plan.__post_init__()
    _validate_lineage(session_id=session_id, protocol_hash=protocol_hash)
    _validate_identity(connection_id, "connection_id")
    if type(generation) is not int or generation < 1:
        raise ValueError("depth HTTPS generation must be a positive integer")
    if type(symbol) is not str or symbol not in plan.symbols:
        raise ValueError("depth HTTPS symbol must occur in the exact plan census")
    _validate_identity(source_logical_key, "source_logical_key")
    if source_logical_key != public_depth_rest_source_logical_key_v8(symbol):
        raise ValueError("depth HTTPS source key must be the stable per-symbol key")
    capture = getattr(clock, "capture", None)
    if not callable(capture) or inspect.iscoroutinefunction(capture):
        raise TypeError("depth HTTPS ingress requires a synchronous ReceiptClock")
    _validate_depth_https_observation_v8(
        observation=observation,
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        symbol=symbol,
        generation=generation,
    )
    if cancellation_requested is not None and type(cancellation_requested) is not asyncio.Event:
        raise TypeError("cancellation_requested must be an exact asyncio.Event or None")


def _validate_usdm_venue_clock_https_attempt_inputs_v9(
    *,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    clock: ReceiptClock,
    observation: PublicUsdmVenueClockRestTerminalObservationV9,
    source_logical_key: str,
    cancellation_requested: asyncio.Event | None,
) -> None:
    if type(plan) is not ProvisionalUsdmVenueClockRestCapturePlanV9:
        raise TypeError("venue-clock HTTPS ingress requires the exact v9 plan")
    plan.__post_init__()
    _validate_lineage(session_id=session_id, protocol_hash=protocol_hash)
    _validate_identity(connection_id, "connection_id")
    if type(generation) is not int or generation < 1:
        raise ValueError("venue-clock HTTPS generation must be positive")
    if source_logical_key != PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9:
        raise ValueError("venue-clock HTTPS source key differs from its fixed role")
    capture = getattr(clock, "capture", None)
    if not callable(capture) or inspect.iscoroutinefunction(capture):
        raise TypeError("venue-clock HTTPS ingress requires a synchronous clock")
    _validate_usdm_venue_clock_observation_v9(
        observation=observation,
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        generation=generation,
    )
    if cancellation_requested is not None and type(cancellation_requested) is not asyncio.Event:
        raise TypeError("cancellation_requested must be an exact asyncio.Event or None")


def _validate_usdm_venue_clock_observation_v9(
    *,
    observation: PublicUsdmVenueClockRestTerminalObservationV9,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
) -> None:
    if type(observation) is not PublicUsdmVenueClockRestTerminalObservationV9:
        raise TypeError("venue-clock HTTPS ingress requires an exact observation")
    observation.__post_init__()
    observation.validate_against_plan(plan)
    if (
        observation.session_id != session_id
        or observation.protocol_hash != protocol_hash
        or observation.connection_id != connection_id
        or observation.connection_generation != generation
    ):
        raise ValueError("venue-clock observation lineage differs from its envelope")


def _preflight_usdm_venue_clock_payload_v9(
    *,
    observation: PublicUsdmVenueClockRestTerminalObservationV9,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    completion: ReceiptTimestamp,
) -> bytes:
    _validate_usdm_venue_clock_observation_v9(
        observation=observation,
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        generation=generation,
    )
    raw_payload = observation(completion)
    if type(raw_payload) is not bytes:
        raise TypeError("venue-clock observation must build exact canonical bytes")
    payload = PublicUsdmVenueClockRestAttemptPayloadV9.from_canonical_bytes(
        raw_payload,
        plan=plan,
    )
    if (
        payload.session_id != session_id
        or payload.protocol_hash != protocol_hash
        or payload.connection_id != connection_id
        or payload.connection_generation != generation
        or payload.completion_admission_wall_ms != completion.received_at_ms
        or payload.completion_admission_monotonic_ns
        != completion.received_monotonic_ns
    ):
        raise ValueError("venue-clock payload lineage differs from admission")
    return raw_payload


def _validate_depth_https_observation_v8(
    *,
    observation: PublicDepthRestTerminalObservationV8,
    plan: ProvisionalDepthRestQualificationPlanV8,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    symbol: str,
    generation: int,
) -> None:
    if type(observation) is not PublicDepthRestTerminalObservationV8:
        raise TypeError("depth HTTPS ingress requires an exact terminal observation")
    observation.__post_init__()
    observation.validate_against_plan(plan)
    expected_ordinal = plan.symbols.index(symbol)
    if (
        observation.session_id != session_id
        or observation.protocol_hash != protocol_hash
        or observation.connection_id != connection_id
    ):
        raise ValueError("depth HTTPS observation lineage differs from its envelope")
    if observation.symbol != symbol:
        raise ValueError("depth HTTPS observation symbol differs from its envelope")
    if observation.canonical_query != (("limit", "1000"), ("symbol", symbol)):
        raise ValueError("depth HTTPS observation query differs from its envelope")
    if observation.symbol_ordinal != expected_ordinal:
        raise ValueError("depth HTTPS observation ordinal differs from its plan")
    if observation.connection_generation != generation:
        raise ValueError("depth HTTPS observation generation differs from its envelope")


def _preflight_depth_https_observation_payload_v8(
    *,
    observation: PublicDepthRestTerminalObservationV8,
    plan: ProvisionalDepthRestQualificationPlanV8,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    symbol: str,
    generation: int,
    completion: ReceiptTimestamp,
) -> bytes:
    _validate_depth_https_observation_v8(
        observation=observation,
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        symbol=symbol,
        generation=generation,
    )
    raw_payload = observation(completion)
    if type(raw_payload) is not bytes:
        raise TypeError("depth HTTPS observation must build exact canonical bytes")
    payload = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
        raw_payload,
        plan=plan,
    )
    _validate_admitted_depth_https_payload_v8(
        payload=payload,
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        symbol=symbol,
        generation=generation,
        completion=completion,
    )
    return raw_payload


def _validate_admitted_depth_https_payload_v8(
    *,
    payload: PublicDepthRestAttemptPayloadV8,
    plan: ProvisionalDepthRestQualificationPlanV8,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    symbol: str,
    generation: int,
    completion: ReceiptTimestamp,
) -> None:
    if type(payload) is not PublicDepthRestAttemptPayloadV8:
        raise TypeError("depth HTTPS parser must return the exact attempt payload")
    payload.validate_against_plan(plan)
    expected_ordinal = plan.symbols.index(symbol)
    if (
        payload.session_id != session_id
        or payload.protocol_hash != protocol_hash
        or payload.connection_id != connection_id
    ):
        raise ValueError("depth HTTPS payload lineage differs")
    if payload.symbol != symbol or payload.symbol_ordinal != expected_ordinal:
        raise ValueError("depth HTTPS payload symbol identity differs")
    if payload.canonical_query != (("limit", "1000"), ("symbol", symbol)):
        raise ValueError("depth HTTPS payload query differs")
    if payload.connection_generation != generation:
        raise ValueError("depth HTTPS payload generation differs")
    if payload.completion_admission_wall_ms != completion.received_at_ms:
        raise ValueError("depth HTTPS payload wall completion differs")
    if payload.completion_admission_monotonic_ns != completion.received_monotonic_ns:
        raise ValueError("depth HTTPS payload monotonic completion differs")


def _validate_https_queue_admitter(pipeline: _RawRecordOffererV2) -> None:
    for method_name in (
        "offer_with_admission_receipt",
        "validate_queue_admission_receipt_v2",
    ):
        method = getattr(pipeline, method_name, None)
        if not callable(method) or inspect.iscoroutinefunction(method):
            raise TypeError(
                f"HTTPS shared ingress requires synchronous handoff-owned {method_name}"
            )


def _offer_with_queue_admission_receipt_v2(
    queue_admitter: _HttpsQueueAdmitterV2,
    record: RawRecordV2,
) -> CaptureQueueAdmissionReceiptV2:
    """Offer once and revalidate the actual handoff-owned proof without retrying."""

    queue_admission_receipt = queue_admitter.offer_with_admission_receipt(record)
    if type(queue_admission_receipt) is not CaptureQueueAdmissionReceiptV2:
        raise TypeError("HTTPS shared ingress requires an exact queue-admission receipt")
    queued_record = queue_admitter.validate_queue_admission_receipt_v2(queue_admission_receipt)
    if (
        type(queued_record) is not QueuedRawRecordV2
        or queued_record is not queue_admission_receipt.queued_record
        or queued_record.record is not record
    ):
        raise ValueError("HTTPS queue-admission proof differs from the offered raw record")
    return queue_admission_receipt


def _validate_and_encode_https_census_inputs(
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    clock: ReceiptClock,
    payload: PublicOiRestCensusPayloadV2,
) -> bytes:
    """Validate and canonical-round-trip one exact census union member."""

    if type(plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise TypeError("HTTPS census ingress requires an exact promoting OI REST plan")
    plan.__post_init__()
    _validate_lineage(session_id=session_id, protocol_hash=protocol_hash)
    capture = getattr(clock, "capture", None)
    if not callable(capture) or inspect.iscoroutinefunction(capture):
        raise TypeError("HTTPS census ingress requires a synchronous ReceiptClock")

    if type(payload) not in (
        PublicOiRestSlotCensusV2,
        PublicOiRestForwardGapRangeV2,
        PublicOiRestCoverageCloseV2,
    ):
        raise TypeError(
            "HTTPS census ingress requires an exact slot, forward-gap, or coverage-close payload"
        )
    payload.__post_init__()
    encoded = payload.canonical_bytes()
    restored = _parse_public_oi_census_payload_v2(encoded, plan=plan)
    if restored != payload or type(restored) is not type(payload):
        raise ValueError("HTTPS census canonical round trip differs from its payload")
    if restored.session_id != session_id:
        raise ValueError("HTTPS census payload session differs from its outer session")
    return encoded


def _parse_public_oi_census_payload_v2(
    encoded: bytes,
    *,
    plan: ProvisionalPromotingRestCapturePlanV2 | None = None,
) -> PublicOiRestCensusPayloadV2:
    """Select one exact public parser, which then enforces canonical JSONL."""

    try:
        document = json.loads(encoded)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("public OI census payload is not valid JSON") from exc
    if type(document) is not dict:
        raise ValueError("public OI census payload must be an object")
    schema_version = document.get("schema_version")
    if schema_version == _PUBLIC_OI_SLOT_CENSUS_SCHEMA:
        return PublicOiRestSlotCensusV2.from_canonical_bytes(encoded, plan=plan)
    if schema_version == _PUBLIC_OI_FORWARD_GAP_SCHEMA:
        return PublicOiRestForwardGapRangeV2.from_canonical_bytes(encoded, plan=plan)
    if schema_version == _PUBLIC_OI_COVERAGE_CLOSE_SCHEMA:
        return PublicOiRestCoverageCloseV2.from_canonical_bytes(encoded, plan=plan)
    raise ValueError("public OI census payload has an unsupported exact schema")


def _validate_https_census_receipt_order(
    *,
    payload: PublicOiRestCensusPayloadV2,
    receipt: ReceiptTimestamp,
) -> None:
    """Reject an outer admission receipt that precedes its terminal evidence."""

    if type(payload) is PublicOiRestSlotCensusV2:
        terminal_wall_ms = payload.closed_wall_ms
        terminal_monotonic_ns = payload.closed_monotonic_ns
    elif type(payload) is PublicOiRestForwardGapRangeV2:
        terminal_wall_ms = payload.observed_wall_ms
        terminal_monotonic_ns = payload.observed_monotonic_ns
    elif type(payload) is PublicOiRestCoverageCloseV2:
        terminal_wall_ms = payload.stop_requested_wall_ms
        terminal_monotonic_ns = payload.stop_requested_monotonic_ns
    else:
        raise TypeError("HTTPS census receipt ordering requires an exact census payload")
    if receipt.received_at_ms < terminal_wall_ms:
        raise ValueError("HTTPS census outer wall receipt precedes its terminal evidence")
    if receipt.received_monotonic_ns < terminal_monotonic_ns:
        raise ValueError("HTTPS census outer monotonic receipt precedes its terminal evidence")


def _validate_https_observation(
    *,
    observation: PublicOiRestTerminalObservationV2,
    plan: ProvisionalPromotingRestCapturePlanV2,
    symbol: str,
) -> None:
    if type(observation) is not PublicOiRestTerminalObservationV2:
        raise TypeError("HTTPS ingress requires an exact public OI terminal observation")
    observation.__post_init__()
    observation.validate_against_plan(plan)
    expected_ordinal = plan.symbols.index(symbol)
    if observation.symbol != symbol:
        raise ValueError("HTTPS observation symbol differs from its raw-record envelope")
    if observation.canonical_query != (("symbol", symbol),):
        raise ValueError("HTTPS observation query differs from its raw-record envelope")
    if observation.symbol_ordinal != expected_ordinal:
        raise ValueError("HTTPS observation ordinal differs from its exact plan position")


def _preflight_https_observation_payload_v2(
    *,
    observation: PublicOiRestTerminalObservationV2,
    plan: ProvisionalPromotingRestCapturePlanV2,
    symbol: str,
    completion: ReceiptTimestamp,
) -> bytes:
    _validate_https_observation(
        observation=observation,
        plan=plan,
        symbol=symbol,
    )
    raw_payload = observation(completion)
    if type(raw_payload) is not bytes:
        raise TypeError("HTTPS observation must build exact canonical bytes")
    payload = PublicOiRestAttemptPayloadV2.from_canonical_bytes(
        raw_payload,
        plan=plan,
    )
    _validate_admitted_https_payload(
        payload=payload,
        plan=plan,
        symbol=symbol,
        completion=completion,
    )
    return raw_payload


def _validate_admitted_https_payload(
    *,
    payload: PublicOiRestAttemptPayloadV2,
    plan: ProvisionalPromotingRestCapturePlanV2,
    symbol: str,
    completion: ReceiptTimestamp,
) -> None:
    if type(payload) is not PublicOiRestAttemptPayloadV2:
        raise TypeError("HTTPS admission parser must return the exact public OI payload")
    expected_ordinal = plan.symbols.index(symbol)
    if payload.symbol != symbol:
        raise ValueError("HTTPS payload symbol differs from its raw-record envelope")
    if payload.canonical_query != (("symbol", symbol),):
        raise ValueError("HTTPS payload query differs from its raw-record envelope")
    if payload.symbol_ordinal != expected_ordinal:
        raise ValueError("HTTPS payload ordinal differs from its exact plan position")
    if payload.completion_admission_wall_ms != completion.received_at_ms:
        raise ValueError("HTTPS payload wall completion differs from its envelope receipt")
    if payload.completion_admission_monotonic_ns != completion.received_monotonic_ns:
        raise ValueError("HTTPS payload monotonic completion differs from its envelope receipt")


def _validate_receipt_timestamp(value: ReceiptTimestamp, field: str) -> None:
    if (
        type(value) is not ReceiptTimestamp
        or type(value.received_at_ms) is not int
        or type(value.received_monotonic_ns) is not int
        or value.received_at_ms < 0
        or value.received_monotonic_ns < 0
    ):
        raise ValueError(f"{field} must be an exact nonnegative ReceiptTimestamp")


def _validate_identity(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")
