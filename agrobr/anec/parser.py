from __future__ import annotations

import hashlib
import io
import re
from typing import Any, NamedTuple

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.normalize.numeric import safe_float
from agrobr.normalize.regions import remover_acentos

logger = structlog.get_logger()

PARSER_VERSION = 1

X_TOLERANCE = 12
Y_TOLERANCE = 2
ROW_Y_TOL = 3.0

HEADER_WEEKLY = "Weekly shipments"
HEADER_MONTHLY = "Monthly shipments"
HEADER_YOY = "comparison of exports"
HEADER_DESTINATIONS = "Importers"

PRODUCT_HEADER_ORDER = ["soybean", "soybean_meal", "maize", "wheat", "ddgs", "sorghum"]

PERIODO_LAST_WEEK = "last_week"
PERIODO_CURRENT_WEEK = "current_week"

MONTH_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

PORTS_CANON = {
    "santos": "SANTOS",
    "paranaguá": "PARANAGUÁ",
    "são francisco do sul": "SÃO FRANCISCO DO SUL",
    "vitória": "VITÓRIA",
    "itacoatiara": "ITACOATIARA",
    "são luis/itaqui": "SÃO LUIS/ITAQUI",
    "rio grande": "RIO GRANDE",
    "santarém": "SANTARÉM",
    "barcarena": "BARCARENA",
    "aratu/cotegipe": "ARATU/COTEGIPE",
    "imbituba": "IMBITUBA",
    "ilhéus": "ILHÉUS",
    "tmib/sergipe": "TMIB/SERGIPE",
    "antonina": "ANTONINA",
    "santana": "SANTANA",
    "belém": "BELÉM",
    "rio de janeiro": "RIO DE JANEIRO",
    "salvador (enseada)": "SALVADOR (ENSEADA)",
    "barra dos coqueiros": "BARRA DOS COQUEIROS",
}


class ParsedReport(NamedTuple):
    weekly_shipments: pd.DataFrame
    monthly_shipments: pd.DataFrame
    yoy_comparison: pd.DataFrame
    destinations: pd.DataFrame
    fingerprint: str


def _check_pdfplumber() -> Any:
    try:
        import pdfplumber

        return pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber é necessário para parsear PDFs ANEC. Instale com: pip install agrobr[pdf]"
        ) from None


def _extract_pages_words(pdf_bytes: bytes) -> list[list[dict[str, Any]]]:
    pdfplumber = _check_pdfplumber()
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages_words = [
                page.extract_words(x_tolerance=X_TOLERANCE, y_tolerance=Y_TOLERANCE)
                for page in pdf.pages
            ]
    except Exception as exc:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason=f"Erro abrindo/lendo PDF: {exc}",
        ) from exc
    if not pages_words:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason="PDF sem páginas extraíveis",
        )
    return pages_words


def _extract_pages_rows(pdf_bytes: bytes) -> _RowsByPage:
    return [_group_by_row(words) for words in _extract_pages_words(pdf_bytes)]


def _group_by_row(
    words: list[dict[str, Any]], y_tol: float = ROW_Y_TOL
) -> list[tuple[float, list[dict[str, Any]]]]:
    if not words:
        return []
    sorted_words = sorted(words, key=lambda w: (w["top"], w["x0"]))
    rows: list[tuple[float, list[dict[str, Any]]]] = []
    cur_y = sorted_words[0]["top"]
    cur_row: list[dict[str, Any]] = []
    for w in sorted_words:
        if cur_row and abs(w["top"] - cur_y) > y_tol:
            rows.append((cur_y, sorted(cur_row, key=lambda x: x["x0"])))
            cur_y = w["top"]
            cur_row = [w]
        else:
            cur_row.append(w)
    if cur_row:
        rows.append((cur_y, sorted(cur_row, key=lambda x: x["x0"])))
    return rows


_NUM_FRAG_RE = re.compile(r"^[\d.]+$")
_DOT_FRAG_RE = re.compile(r"^\.\d{3}([.]\d{3})*$")


def _concat_fragmented_numbers(
    row: list[dict[str, Any]], x_join_threshold: float = 5.0
) -> list[dict[str, Any]]:
    if not row:
        return row
    out: list[dict[str, Any]] = []
    for w in row:
        if out:
            prev = out[-1]
            prev_text = prev["text"]
            cur_text = w["text"]
            if (
                _NUM_FRAG_RE.match(prev_text)
                and _DOT_FRAG_RE.match(cur_text)
                and (w["x0"] - prev.get("x1", prev["x0"])) < x_join_threshold
            ):
                merged = dict(prev)
                merged["text"] = prev_text + cur_text
                merged["x1"] = w.get("x1", w["x0"])
                out[-1] = merged
                continue
        out.append(w)
    return out


def _parse_value(s: str) -> float | None:
    s = s.strip()
    if s in ("-", ""):
        return None
    if s.endswith("%"):
        s = s[:-1].strip()
    return safe_float(s)


def _row_text(row: list[dict[str, Any]]) -> str:
    return " ".join(w["text"] for w in row)


_RowsByPage = list[list[tuple[float, list[dict[str, Any]]]]]


def _find_page_with_header(pages_rows: _RowsByPage, header: str) -> int:
    target = header.lower()
    for i, rows in enumerate(pages_rows):
        for _, row in rows:
            if target in _row_text(row).lower():
                return i
    return -1


def _find_pages_with_header(pages_rows: _RowsByPage, header: str) -> list[int]:
    target = header.lower()
    out: list[int] = []
    for i, rows in enumerate(pages_rows):
        for _, row in rows:
            if target in _row_text(row).lower():
                out.append(i)
                break
    return out


def _compute_fingerprint(pages_rows: _RowsByPage) -> str:
    parts: list[str] = [f"pages={len(pages_rows)}"]
    for i, rows in enumerate(pages_rows):
        parts.append(f"p{i + 1}:rows={len(rows)}")
        for _, row in rows[:6]:
            parts.append(_row_text(row).strip()[:80].lower())
    digest = hashlib.md5("\n".join(parts).encode("utf-8")).hexdigest()
    return digest[:16]


# === Header columns detection (weekly_shipments) ===


def _detect_weekly_columns(
    header_row: list[dict[str, Any]],
) -> list[tuple[str, str, float]]:
    """Detecta colunas (produto, periodo, x_center) na linha de header da p2.

    Layout: PORT Soybean Soybean meal Maize Wheat DDGS Sorghum [last_week] |
            Soybean Soybean meal Maize Wheat DDGS Sorghum [current_week]

    Retorna lista ordenada por x: [(produto, periodo, x_center), ...]
    """
    cols: list[tuple[str, str, float]] = []
    i = 0
    while i < len(header_row):
        w = header_row[i]
        txt = w["text"].lower()
        x_center = (w["x0"] + w.get("x1", w["x0"])) / 2

        if txt == "port":
            i += 1
            continue
        if txt in {"soybean", "soybeans"}:
            if i + 1 < len(header_row) and header_row[i + 1]["text"].lower() == "meal":
                next_w = header_row[i + 1]
                x_center = (w["x0"] + next_w.get("x1", next_w["x0"])) / 2
                cols.append(("soybean_meal", "", x_center))
                i += 2
                continue
            cols.append(("soybean", "", x_center))
        elif txt == "maize":
            cols.append(("maize", "", x_center))
        elif txt == "wheat":
            cols.append(("wheat", "", x_center))
        elif txt == "ddgs":
            cols.append(("ddgs", "", x_center))
        elif txt == "sorghum":
            cols.append(("sorghum", "", x_center))
        i += 1

    cols.sort(key=lambda c: c[2])
    occurrences: dict[str, int] = {}
    out: list[tuple[str, str, float]] = []
    for produto, _, x in cols:
        occurrence = occurrences.get(produto, 0)
        periodo = PERIODO_LAST_WEEK if occurrence == 0 else PERIODO_CURRENT_WEEK
        out.append((produto, periodo, x))
        occurrences[produto] = occurrence + 1
    return out


def _value_for_column(
    row: list[dict[str, Any]],
    col_x: float,
    x_tolerance: float = 25.0,
) -> float | None:
    candidates = [
        w
        for w in row
        if abs(((w["x0"] + w.get("x1", w["x0"])) / 2) - col_x) < x_tolerance
        and w["text"].strip().lower() not in PORTS_CANON
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda w: abs(((w["x0"] + w.get("x1", w["x0"])) / 2) - col_x))
    return _parse_value(candidates[0]["text"])


# === Sub-parsers ===


_PORTS_LOOKUP_COLLAPSED: dict[str, str] = {
    re.sub(r"[\s/]+", "", canon).lower(): key for key, canon in PORTS_CANON.items()
}

_PORTS_LOOKUP_NO_ACCENT: dict[str, str] = {
    re.sub(r"[\s/()]+", "", remover_acentos(canon)).lower(): key
    for key, canon in PORTS_CANON.items()
}


def _normalize_port_text(raw: str) -> str:
    s = re.sub(r"\s+", " ", raw).strip()
    s = re.sub(r"\s*\(\s*", " (", s)
    s = re.sub(r"\s*\)\s*", ")", s)
    return s.strip()


def resolve_port(text: str) -> str | None:
    norm = _normalize_port_text(text).lower()
    if norm in PORTS_CANON:
        return PORTS_CANON[norm]
    collapsed = re.sub(r"[\s/]+", "", norm)
    canon_key = _PORTS_LOOKUP_COLLAPSED.get(collapsed)
    if canon_key is not None:
        return PORTS_CANON[canon_key]
    no_accent = re.sub(r"[\s/()]+", "", remover_acentos(norm))
    canon_key = _PORTS_LOOKUP_NO_ACCENT.get(no_accent)
    if canon_key is not None:
        return PORTS_CANON[canon_key]
    return None


def _parse_weekly_shipments(words: list[dict[str, Any]]) -> pd.DataFrame:
    rows = _group_by_row(words)

    header_row: list[dict[str, Any]] | None = None
    header_y: float | None = None
    for y, row in rows:
        if not any(w["text"].upper() == "PORT" for w in row):
            continue
        if not any(w["text"].lower() == "soybean" for w in row):
            continue
        header_row = row
        header_y = y
        break

    if header_row is None or header_y is None:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason="Header row (PORT/Soybean) não encontrado em weekly_shipments",
        )

    columns = _detect_weekly_columns(header_row)
    if len(columns) < 6:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason=f"Esperado >=6 colunas em weekly_shipments, detectado {len(columns)}",
        )

    records: list[dict[str, Any]] = []
    for y, row in rows:
        if y <= header_y:
            continue
        if not row:
            continue
        consolidated = _concat_fragmented_numbers(row)

        port_words: list[dict[str, Any]] = []
        value_words: list[dict[str, Any]] = []
        for w in consolidated:
            txt = w["text"].strip()
            is_numeric = bool(re.fullmatch(r"[\d.,\-]+", txt))
            is_pct = txt.endswith("%")
            if (is_numeric or is_pct) and port_words:
                value_words.append(w)
            elif w["x0"] < 95 and not is_numeric and not is_pct:
                port_words.append(w)
            else:
                value_words.append(w)
        raw_port_text = " ".join(w["text"] for w in port_words)
        port_text = _normalize_port_text(raw_port_text)
        port_lower = port_text.lower()

        if not port_text:
            continue
        if port_lower.startswith("*"):
            continue
        if port_lower.startswith("total"):
            continue

        port_canon = resolve_port(port_text)
        if port_canon is None:
            logger.debug("anec_porto_desconhecido", text=port_text, y=y)
            continue

        for produto, periodo, col_x in columns:
            valor = _value_for_column(value_words, col_x)
            records.append(
                {
                    "porto": port_canon,
                    "produto": produto,
                    "periodo": periodo,
                    "valor_ton": valor,
                }
            )

    df = pd.DataFrame.from_records(records, columns=["porto", "produto", "periodo", "valor_ton"])
    if df.empty:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason="Nenhuma linha de porto extraída em weekly_shipments",
        )
    df["valor_ton"] = df["valor_ton"].astype("Float64")
    return df


_MONTH_RE = re.compile(
    r"^(january|february|march|april|may|june|july|august|september|october|november|december)\*?$",
    re.IGNORECASE,
)


def _parse_monthly_shipments(words: list[dict[str, Any]]) -> pd.DataFrame:
    rows = _group_by_row(words)

    sections: list[tuple[int, list[tuple[float, list[dict[str, Any]]]]]] = []
    cur_year: int | None = None
    cur_section: list[tuple[float, list[dict[str, Any]]]] = []
    section_year_re = re.compile(r"monthly\s+shipments\s+(\d{4})", re.IGNORECASE)
    for y, row in rows:
        text = _row_text(row)
        m = section_year_re.search(text)
        if m:
            if cur_year is not None and cur_section:
                sections.append((cur_year, cur_section))
            cur_year = int(m.group(1))
            cur_section = []
            continue
        if cur_year is not None:
            cur_section.append((y, row))
    if cur_year is not None and cur_section:
        sections.append((cur_year, cur_section))

    if not sections:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason="Nenhuma seção 'Monthly shipments YYYY' encontrada",
        )

    records: list[dict[str, Any]] = []

    for year, section_rows in sections:
        header_seen = False
        for _y, row in section_rows:
            text = _row_text(row).lower()
            if not header_seen:
                if "soybean" in text and "maize" in text and "wheat" in text:
                    header_seen = True
                continue
            consolidated = _concat_fragmented_numbers(row)
            if not consolidated:
                continue
            mes_word = consolidated[0]
            mes_text = mes_word["text"].strip().lower()
            if not _MONTH_RE.match(mes_text):
                continue
            eh_estimativa = mes_text.endswith("*")
            mes_clean = mes_text.rstrip("*").strip()
            mes_num = MONTH_NUM.get(mes_clean)
            if mes_num is None:
                continue

            value_words = consolidated[1:]
            for idx, produto in enumerate(PRODUCT_HEADER_ORDER):
                valor = _parse_value(value_words[idx]["text"]) if idx < len(value_words) else None
                records.append(
                    {
                        "ano": year,
                        "mes": mes_num,
                        "produto": produto,
                        "valor_ton": valor,
                        "eh_estimativa": eh_estimativa,
                    }
                )

    df = pd.DataFrame.from_records(
        records, columns=["ano", "mes", "produto", "valor_ton", "eh_estimativa"]
    )
    if df.empty:
        logger.warning(
            "anec_monthly_empty", note="seção sem rows de mês — possível semana de transição"
        )
        return df.astype(
            {"ano": "Int64", "mes": "Int64", "valor_ton": "Float64", "eh_estimativa": "bool"}
        )
    df["valor_ton"] = df["valor_ton"].astype("Float64")
    df["ano"] = df["ano"].astype("Int64")
    df["mes"] = df["mes"].astype("Int64")
    return df


_YOY_PRODUCT_TERMS = {
    "soybeans": "soybean",
    "soybean": "soybean",
    "maize": "maize",
    "wheat": "wheat",
    "ddgs": "ddgs",
    "sorghum": "sorghum",
}


def _detect_yoy_section_headers(
    rows: list[tuple[float, list[dict[str, Any]]]],
) -> list[tuple[float, list[tuple[str, float]]]]:
    yoy_anchor_y: float | None = None
    for y, row in rows:
        if "comparison of exports" in _row_text(row).lower():
            yoy_anchor_y = y
            break

    sections: list[tuple[float, list[tuple[str, float]]]] = []
    for y, row in rows:
        if yoy_anchor_y is not None and y <= yoy_anchor_y:
            continue
        if len(row) > 4:
            continue
        product_words: list[dict[str, Any]] = []
        i = 0
        while i < len(row):
            w = row[i]
            txt = w["text"].lower().rstrip(",.")
            if txt == "soybean" and i + 1 < len(row) and row[i + 1]["text"].lower() == "meal":
                nx = row[i + 1]
                product_words.append(
                    {
                        "text": "soybean_meal",
                        "x0": w["x0"],
                        "x1": nx.get("x1", nx["x0"]),
                    }
                )
                i += 2
                continue
            if txt == "total" and i + 1 < len(row) and row[i + 1]["text"].lower() == "products":
                nx = row[i + 1]
                product_words.append(
                    {
                        "text": "total_products",
                        "x0": w["x0"],
                        "x1": nx.get("x1", nx["x0"]),
                    }
                )
                i += 2
                continue
            if txt in _YOY_PRODUCT_TERMS:
                product_words.append(w)
            i += 1

        if len(product_words) >= 2:
            mapped: list[tuple[str, float]] = []
            for pw in product_words:
                key = pw["text"].lower().rstrip(",.")
                produto = _YOY_PRODUCT_TERMS.get(key, key)
                xc = (pw["x0"] + pw.get("x1", pw["x0"])) / 2
                mapped.append((produto, xc))
            mapped.sort(key=lambda c: c[1])
            sections.append((y, mapped))
    return sections


def _parse_yoy_section(
    rows: list[tuple[float, list[dict[str, Any]]]],
    header_y: float,
    next_header_y: float,
    produto_left: str,
    produto_right: str,
    midpoint: float,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for y, row in rows:
        if y <= header_y or y >= next_header_y:
            continue
        consolidated = _concat_fragmented_numbers(row)
        month_words = [w for w in consolidated if _MONTH_RE.match(w["text"].strip().lower())]
        if not month_words:
            continue

        for mw in month_words:
            mes_text = mw["text"].strip().lower().rstrip("*")
            mes_num = MONTH_NUM.get(mes_text)
            if mes_num is None:
                continue
            x_start = mw.get("x1", mw["x0"])
            next_month = next((w for w in month_words if w["x0"] > x_start), None)
            x_end = next_month["x0"] if next_month else float("inf")
            values = [
                w
                for w in consolidated
                if x_start < w["x0"] < x_end
                and (re.fullmatch(r"[\d.,\-]+", w["text"].strip()) or w["text"].strip() == "-")
            ]
            values.sort(key=lambda w: w["x0"])
            v_2025 = _parse_value(values[0]["text"]) if len(values) >= 1 else None
            v_2026 = _parse_value(values[1]["text"]) if len(values) >= 2 else None
            mw_xc = (mw["x0"] + mw.get("x1", mw["x0"])) / 2
            produto = produto_left if mw_xc < midpoint else produto_right
            records.append(
                {
                    "mes": mes_num,
                    "produto": produto,
                    "valor_2025": v_2025,
                    "valor_2026": v_2026,
                }
            )
    return records


def _parse_yoy_page(words: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = _group_by_row(words)
    sections = _detect_yoy_section_headers(rows)
    if not sections:
        return []
    records: list[dict[str, Any]] = []
    for idx, (header_y, product_cols) in enumerate(sections):
        next_header_y = sections[idx + 1][0] if idx + 1 < len(sections) else float("inf")
        if len(product_cols) < 2:
            continue
        midpoint = (product_cols[0][1] + product_cols[-1][1]) / 2
        produto_left = product_cols[0][0]
        produto_right = product_cols[-1][0]
        records.extend(
            _parse_yoy_section(rows, header_y, next_header_y, produto_left, produto_right, midpoint)
        )
    return records


def _parse_yoy_comparison(
    pages_words: list[list[dict[str, Any]]], page_indexes: list[int]
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for idx in page_indexes:
        records.extend(_parse_yoy_page(pages_words[idx]))
    df = pd.DataFrame.from_records(records, columns=["mes", "produto", "valor_2025", "valor_2026"])
    df["mes"] = df["mes"].astype("Int64")
    df["valor_2025"] = df["valor_2025"].astype("Float64")
    df["valor_2026"] = df["valor_2026"].astype("Float64")
    return df


_PRODUCT_FROM_DESTINATIONS_RE = re.compile(r"Brazilian\s+([A-Za-z\s]+?)\s+Importers", re.IGNORECASE)


def _parse_destinations_page(
    words: list[dict[str, Any]],
) -> tuple[str | None, list[dict[str, Any]]]:
    rows = _group_by_row(words)
    produto_canon: str | None = None
    for _, row in rows:
        text = _row_text(row)
        m = _PRODUCT_FROM_DESTINATIONS_RE.search(text)
        if m:
            label = m.group(1).strip().lower()
            if "soybean meal" in label or ("soybean" in label and "meal" in label):
                produto_canon = "soybean_meal"
            elif "soybeans" in label or label == "soybean":
                produto_canon = "soybean"
            elif "maize" in label or "corn" in label:
                produto_canon = "maize"
            elif "wheat" in label:
                produto_canon = "wheat"
            elif "ddgs" in label:
                produto_canon = "ddgs"
            elif "sorghum" in label:
                produto_canon = "sorghum"
            break

    out: list[dict[str, Any]] = []
    if produto_canon is None:
        return None, out

    for _y, row in rows:
        text = _row_text(row).strip()
        if not text or text.lower().startswith("brazilian"):
            continue
        if text.lower().startswith("destination"):
            continue
        if text.lower().startswith("source:") or "associação" in text.lower():
            continue
        if text.lower().startswith("week"):
            continue
        if not row:
            continue
        last = row[-1]["text"]
        if not last.endswith("%"):
            continue
        share = _parse_value(last)
        destino_words = [w["text"] for w in row[:-1]]
        destino = " ".join(destino_words).strip()
        if not destino:
            continue
        if destino.upper() == "TOTAL":
            continue
        out.append({"produto": produto_canon, "destino": destino.upper(), "share_pct": share})
    return produto_canon, out


def _parse_destinations(
    pages_words: list[list[dict[str, Any]]], page_indexes: list[int]
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for idx in page_indexes:
        _, recs = _parse_destinations_page(pages_words[idx])
        records.extend(recs)
    df = pd.DataFrame.from_records(records, columns=["produto", "destino", "share_pct"])
    df["share_pct"] = df["share_pct"].astype("Float64")
    return df


# === Orquestrador ===


def parse_anec_pdf(pdf_bytes: bytes) -> ParsedReport:
    pages_words = _extract_pages_words(pdf_bytes)
    pages_rows: _RowsByPage = [_group_by_row(words) for words in pages_words]
    fingerprint = _compute_fingerprint(pages_rows)

    weekly_idx = _find_page_with_header(pages_rows, HEADER_WEEKLY)
    if weekly_idx < 0:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason=f"Header '{HEADER_WEEKLY}' não encontrado",
        )
    weekly_df = _parse_weekly_shipments(pages_words[weekly_idx])

    monthly_idx = _find_page_with_header(pages_rows, HEADER_MONTHLY)
    if monthly_idx < 0:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason=f"Header '{HEADER_MONTHLY}' não encontrado",
        )
    monthly_df = _parse_monthly_shipments(pages_words[monthly_idx])

    yoy_anchors = _find_pages_with_header(pages_rows, HEADER_YOY)
    yoy_indexes: list[int] = []
    for idx in yoy_anchors:
        yoy_indexes.append(idx)
        if idx + 1 < len(pages_words):
            yoy_indexes.append(idx + 1)
    if not yoy_indexes:
        yoy_df = pd.DataFrame(columns=["mes", "produto", "valor_2025", "valor_2026"])
    else:
        yoy_df = _parse_yoy_comparison(pages_words, yoy_indexes)

    dest_indexes = _find_pages_with_header(pages_rows, HEADER_DESTINATIONS)
    destinations_df = _parse_destinations(pages_words, dest_indexes)

    logger.info(
        "anec_parse_done",
        fingerprint=fingerprint,
        weekly_rows=len(weekly_df),
        monthly_rows=len(monthly_df),
        yoy_rows=len(yoy_df),
        destinations_rows=len(destinations_df),
    )
    return ParsedReport(
        weekly_shipments=weekly_df,
        monthly_shipments=monthly_df,
        yoy_comparison=yoy_df,
        destinations=destinations_df,
        fingerprint=fingerprint,
    )
