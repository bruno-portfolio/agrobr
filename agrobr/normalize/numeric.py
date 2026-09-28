from __future__ import annotations

import math

_DEFAULT_NULL_MARKERS: frozenset[str] = frozenset(
    {"-", "\u2013", "\u2014", "...", "n.d.", "n/d", "nd", "n.d", "*"}
)


def safe_float(
    v: object,
    *,
    strip: str | tuple[str, ...] = (),
    null_markers: frozenset[str] | None = None,
    nan_as_none: bool = True,
    treat_zero_as_none: bool = False,
) -> float | None:
    if v is None:
        return None

    if isinstance(v, float):
        if nan_as_none and math.isnan(v):
            return None
        if treat_zero_as_none and v == 0.0:
            return None
        return v

    if isinstance(v, int):
        fv = float(v)
        if treat_zero_as_none and fv == 0.0:
            return None
        return fv

    s = str(v).strip()

    if isinstance(strip, str):
        strip = (strip,)
    for ch in strip:
        s = s.replace(ch, "")
    s = s.strip()

    markers = null_markers if null_markers is not None else _DEFAULT_NULL_MARKERS
    if s.lower() in markers or not s:
        return None

    s = s.replace(" ", "")

    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif s.count(".") > 1:
        s = s.replace(".", "")
    elif "." in s:
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 3 and parts[1].isdigit():
            s = s.replace(".", "")

    try:
        result = float(s)
    except (ValueError, TypeError):
        return None

    if nan_as_none and math.isnan(result):
        return None
    if treat_zero_as_none and result == 0.0:
        return None
    return result


def parse_numeric_br(v: object) -> float | None:
    if v is None or (isinstance(v, str) and v.strip() in ("", "-")):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        raw = str(v).strip().replace(" ", "")
        if "," in raw and "." in raw:
            raw = raw.replace(".", "").replace(",", ".")
        elif "," in raw:
            raw = raw.replace(",", ".")
        return float(raw)
    except (ValueError, TypeError):
        return None
