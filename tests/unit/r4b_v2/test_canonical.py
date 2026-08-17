from __future__ import annotations

from dataclasses import dataclass

import pytest
import rfc8785

from signalbot.r4b_v2.canonical import canonical_json_line


@dataclass(frozen=True)
class _Document:
    value: int
    label: str


def test_canonical_json_line_uses_rfc8785_utf16_property_order() -> None:
    document = {
        "\u20ac": "Euro Sign",
        "\r": "Carriage Return",
        "\ufb33": "Hebrew Letter Dalet With Dagesh",
        "1": "One",
        "\U0001f600": "Emoji: Grinning Face",
        "\u0080": "Control",
        "\u00f6": "Latin Small Letter O With Diaeresis",
    }

    assert canonical_json_line(document) == (
        '{"\\r":"Carriage Return","1":"One","\u0080":"Control",'
        '"ö":"Latin Small Letter O With Diaeresis","€":"Euro Sign",'
        '"😀":"Emoji: Grinning Face","דּ":"Hebrew Letter Dalet With Dagesh"}\n'
    ).encode()


def test_canonical_json_line_accepts_dataclass_and_appends_one_lf() -> None:
    assert canonical_json_line(_Document(value=2, label="x")) == b'{"label":"x","value":2}\n'


@pytest.mark.parametrize("value", [0.0, -0.0, 1.5, float("inf"), float("nan")])
def test_canonical_json_line_rejects_all_binary_floats(value: float) -> None:
    with pytest.raises(TypeError, match="binary float is forbidden"):
        canonical_json_line({"value": value})


def test_canonical_json_line_rejects_non_text_keys_before_hashing() -> None:
    with pytest.raises(TypeError, match="key must be text"):
        canonical_json_line({1: "value"})


def test_canonical_json_line_rejects_integer_outside_jcs_safe_domain() -> None:
    with pytest.raises(rfc8785.IntegerDomainError):
        canonical_json_line({"value": 9_007_199_254_740_992})
