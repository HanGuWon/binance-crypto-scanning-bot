"""Position Guardian service package.

The Guardian is intentionally separate from :mod:`signalbot`.  The scanner
publishes public-data recommendations and protection context; this package
owns private-account observation and, in later qualified phases, stop-only
protection for explicitly adopted USD-M Futures positions.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
