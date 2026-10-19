"""Regenerate the stage-1 golden files from the *baseline* source (snapshot commit).

Run only against the unmodified deployed-release snapshot (commit "Snapshot deployed
release shadow-v2-3f4cabf6 source"); the goldens are the bytes that the stage-1
changes must keep reproducing when every new setting is off.

    PYTHONPATH=src:tests/unit python -m stage1_golden_gen tests/fixtures/stage1
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import stage1_scenario as scenario

CASES: dict[str, dict[str, object]] = {
    "plain": {},
    "gap_recovered": {"missing": [256]},
    "gap_unrecovered": {"missing": [256], "recover": "empty"},
    "gap_no_recoverer": {"missing": [256], "recover": "none"},
    "two_bar_gap": {"missing": [255, 256]},
    "long_family": {"family": "breakout_long"},
}


async def collect() -> dict[str, str]:
    from signalbot.domain.enums import SignalFamily

    outputs: dict[str, str] = {}
    for name, raw in CASES.items():
        kwargs = dict(raw)
        if "family" in kwargs:
            kwargs["family"] = SignalFamily(str(kwargs["family"]))
        result = await scenario.run_scenario(**kwargs)  # type: ignore[arg-type]
        outputs[name] = result.canonical() + "\n"
        print(name, len(result.signals), len(result.outbox))
        result.close()
    return outputs


def build(out_dir: Path) -> None:
    outputs = asyncio.run(collect())
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, text in outputs.items():
        (out_dir / f"{name}.json").write_text(text, encoding="utf-8")
    (out_dir / "MANIFEST.json").write_text(
        json.dumps({"cases": sorted(CASES)}, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    build(Path(sys.argv[1]))
