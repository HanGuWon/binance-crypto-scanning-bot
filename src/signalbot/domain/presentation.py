"""Presentation-version constants shared by alert rendering and persistence.

Kept dependency-free so the repository can read the footer marker without
importing the ``alerts`` package (which imports the repository).
"""

from __future__ import annotations

# Bump when embed wording or layout changes. It is rendered in the embed footer so a
# stored payload records which presentation produced it; the outbox conflict check
# tolerates a presentation-only difference between versions (see repository.py).
PRESENTATION_VERSION = 2
PRESENTATION_FOOTER_MARKER = "view v"
