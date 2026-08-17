from __future__ import annotations


class CaptureError(RuntimeError):
    """Base error for the prospective evidence capture path."""


class CaptureQueueOverflow(CaptureError):
    """Raised synchronously when bounded handoff cannot accept a record."""


class CaptureHandoffClosed(CaptureError):
    """Raised when a producer submits after stop or fatal failure."""


class CaptureSerializationError(CaptureError):
    """Raised before enqueue when a source record cannot be encoded losslessly."""


class CaptureStorageError(CaptureError):
    """Base error for fail-closed storage failures."""


class CaptureStorageCapacityError(CaptureStorageError):
    """Raised before a write would exceed the configured disk quota."""


class CaptureShortWriteError(CaptureStorageError):
    """Raised when the operating system accepts fewer bytes than requested."""


class CaptureIntegrityError(CaptureStorageError):
    """Raised for a broken segment hash or hash chain."""
