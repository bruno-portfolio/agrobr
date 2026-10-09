from __future__ import annotations

import hashlib
import io
import re
from datetime import date, timedelta
from typing import Any, NamedTuple

import pandas as pd
import pydantic

from agrobr import _log
from agrobr.anec import models
from agrobr.exceptions import ParseError
from agrobr.normalize.numeric import safe_float
from agrobr.normalize.regions import remover_acentos

logger = _log.get_logger(__name__)

PARSER_VERSION = 6

X_TOLERANCE = 12
Y_TOLERANCE = 1
ROW_Y_TOL = 3.0

HEADER_WEEKLY = "Weekly shipments"
HEADER_MONTHLY = "Monthly shipments"
HEADER_YOY = "comparison of exports"
HEADER_DESTINATIONS = "Importers"

PRODUCT_HEADER_ORDER = ["soybean", "soybean_meal", "maize", "wheat", "ddgs", "sorghum"]
PRODUCT_HEADER_LAYOUTS = (PRODUCT_HEADER_ORDER, PRODUCT_HEADER_ORDER[:4])

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
MONTH_ABBR = {name[:3]: number for name, number in MONTH_NUM.items()}
_WEEK_LABEL_RE = re.compile(
    r"\((\d{1,2})(?:st|nd|rd|th)\s+to\s+(\d{1,2})(?:st|nd|rd|th)\s+([A-Za-z]+)\s*\)"
)
_EDITION_RE = re.compile(r"Week\s*(\d{1,2})/(\d{4})")
_TOLERANCIA_DA_EDICAO = timedelta(days=7)
_ARREDONDAMENTO_TONELADA = 0.5

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
    avisos_da_linha_total: tuple[tuple[str, str, str], ...] = ()


def _validated_records(
    records: list[dict[str, Any]], model: type[pydantic.BaseModel]
) -> list[dict[str, Any]]:
    try:
        return [model.model_validate(record).model_dump() for record in records]
    except pydantic.ValidationError as exc:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason=f"Linha inválida em {model.__name__}: {exc}",
        ) from exc


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
_MILHAR_RE = re.compile(r"^\d{1,3}(\.\d{3})+$")


def _concat_fragmented_numbers(
    row: list[dict[str, Any]], x_join_threshold: float = 5.0
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for w in row:
        if out:
            prev = out[-1]
            prev_text = prev["text"]
            cur_text = w["text"]
            if (
                _NUM_FRAG_RE.match(prev_text)
                and _NUM_FRAG_RE.match(cur_text)
                and _MILHAR_RE.match(prev_text + cur_text)
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


def _detect_weekly_columns(
    header_row: list[dict[str, Any]],
) -> list[tuple[str, str, float]]:
    """Detecta colunas (produto, periodo, x_center) na linha de header da p2.

    Layout: PORT Soybean Soybean meal Maize Wheat DDGS Sorghum [last_week] |
            Soybean Soybean meal Maize Wheat DDGS Sorghum [current_week]
    Até a W2/2026, sem DDGS e Sorghum.

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


def _column_bounds(centers: list[float]) -> list[tuple[float, float]]:
    edges = [(left + right) / 2 for left, right in zip(centers, centers[1:], strict=False)]
    return list(zip([float("-inf"), *edges], [*edges, float("inf")], strict=True))


def _value_for_column(
    row: list[dict[str, Any]],
    col_x: float,
    bounds: tuple[float, float],
) -> float | None:
    left, right = bounds
    candidates = [
        w
        for w in row
        if left <= (w["x0"] + w.get("x1", w["x0"])) / 2 < right
        and w["text"].strip().lower() not in PORTS_CANON
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda w: abs(((w["x0"] + w.get("x1", w["x0"])) / 2) - col_x))
    return _parse_value(candidates[0]["text"])


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
    no_accent = re.sub(r"[\s/()]+", "", remover_acentos(norm))
    canon_key = _PORTS_LOOKUP_NO_ACCENT.get(no_accent)
    if canon_key is not None:
        return PORTS_CANON[canon_key]
    return None


def _month(year: int, month: int, offset: int) -> tuple[int, int]:
    index = year * 12 + month - 1 + offset
    return index // 12, index % 12 + 1


def _label_readings(first: int, last: int, month: int, reference: date) -> list[tuple[date, date]]:
    offsets = [(0, 0)] if first <= last else [(-1, 0), (0, 1)]
    readings = []
    for year in (reference.year - 1, reference.year, reference.year + 1):
        for start_offset, end_offset in offsets:
            try:
                start = date(*_month(year, month, start_offset), first)
                end = date(*_month(year, month, end_offset), last)
            except ValueError:
                continue
            if end - start == timedelta(days=6):
                readings.append((start, end))
    return readings


def _weekly_periods(
    rows: list[tuple[float, list[dict[str, Any]]]], header_y: float
) -> tuple[int, int, dict[str, tuple[date | None, date | None]]]:
    edition = next(
        (match for _y, row in rows if (match := _EDITION_RE.search(_row_text(row)))), None
    )
    labels = [
        (int(first), int(last), MONTH_ABBR.get(month[:3].lower(), 0))
        for y, row in rows
        if y < header_y
        for first, last, month in _WEEK_LABEL_RE.findall(_row_text(row))
    ]
    if edition is None or len(labels) != 2 or not all(month for *_, month in labels):
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason="Semana do boletim ou rótulos das duas semanas não encontrados em weekly_shipments",
        )
    week, year = int(edition.group(1)), int(edition.group(2))
    reference = date(year, 1, 1) + timedelta(weeks=week - 1)
    pairs = [
        (last, current)
        for last in _label_readings(*labels[0], reference)
        for current in _label_readings(*labels[1], reference)
        if current[0] - last[1] == timedelta(days=1)
        and abs(last[0] - reference) <= _TOLERANCIA_DA_EDICAO
    ]
    if not pairs:
        logger.warning("anec_rotulos_semana_inconsistentes", semana=week, ano=year, rotulos=labels)
        return year, week, {PERIODO_LAST_WEEK: (None, None), PERIODO_CURRENT_WEEK: (None, None)}
    last, current = min(pairs, key=lambda pair: abs((pair[0][0] - reference).days))
    return year, week, {PERIODO_LAST_WEEK: last, PERIODO_CURRENT_WEEK: current}


def _divergencias_da_linha_total(
    df: pd.DataFrame,
    impressos: dict[tuple[str, str], float | None],
    ano: int,
    semana: int,
) -> list[tuple[str, str, str]]:
    if all(valor is None for valor in impressos.values()):
        return []
    avisos = []
    for (produto, periodo), impresso in impressos.items():
        valores = df.loc[
            (df["produto"] == produto) & (df["periodo"] == periodo), "valor_ton"
        ].dropna()
        soma = float(valores.sum())
        publicado = impresso or 0.0
        if abs(soma - publicado) > _ARREDONDAMENTO_TONELADA * (len(valores) + 1):
            avisos.append(
                (
                    produto,
                    periodo,
                    f"ANEC: no boletim {semana}/{ano}, a soma dos portos em {produto} ({periodo}) "
                    f"dá {soma:.0f} t, e a linha TOTAL publicada, {publicado:.0f} t; o agrobr "
                    "repassa os valores por porto.",
                )
            )
    return avisos


def _parse_weekly_shipments(
    words: list[dict[str, Any]],
) -> tuple[pd.DataFrame, list[tuple[str, str, str]]]:
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
    detected = [(product, period) for product, period, _x in columns]
    if not any(
        detected
        == [
            (product, period)
            for period in (PERIODO_LAST_WEEK, PERIODO_CURRENT_WEEK)
            for product in layout
        ]
        for layout in PRODUCT_HEADER_LAYOUTS
    ):
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason=(
                "Cabeçalhos semanais incompatíveis: cada período precisa dos mesmos produtos "
                "nomeados, os 6 ou os 4 publicados até a W2/2026"
            ),
        )

    column_bounds = _column_bounds([col_x for _produto, _periodo, col_x in columns])
    records: list[dict[str, Any]] = []
    impressos: dict[tuple[str, str], float | None] = {}
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
            if value_words or is_numeric or is_pct:
                value_words.append(w)
            else:
                port_words.append(w)
        raw_port_text = " ".join(w["text"] for w in port_words)
        port_text = _normalize_port_text(raw_port_text)
        port_lower = port_text.lower()

        if not port_text:
            continue
        if port_lower.startswith("*"):
            continue
        if port_lower.startswith("total"):
            if port_lower == "total" and not impressos:
                impressos = {
                    (produto, periodo): _value_for_column(value_words, col_x, bounds)
                    for (produto, periodo, col_x), bounds in zip(
                        columns, column_bounds, strict=True
                    )
                }
            continue

        port_canon = resolve_port(port_text)
        if port_canon is None:
            logger.debug("anec_porto_desconhecido", text=port_text, y=y)
            continue

        for (produto, periodo, col_x), bounds in zip(columns, column_bounds, strict=True):
            valor = _value_for_column(value_words, col_x, bounds)
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
    ano, semana, periodos = _weekly_periods(rows, header_y)
    df["ano"] = pd.Series(ano, index=df.index, dtype="Int64")
    df["semana"] = pd.Series(semana, index=df.index, dtype="Int64")
    df["data_inicio"] = pd.to_datetime(df["periodo"].map({p: d[0] for p, d in periodos.items()}))
    df["data_fim"] = pd.to_datetime(df["periodo"].map({p: d[1] for p, d in periodos.items()}))
    return df, _divergencias_da_linha_total(df, impressos, ano, semana) if impressos else []


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
        columns: list[tuple[str, float]] = []
        for _y, row in section_rows:
            text = _row_text(row).lower()
            if not columns:
                if "soybean" in text and "maize" in text and "wheat" in text:
                    columns = _monthly_columns(row)
                elif row and _MONTH_RE.fullmatch(row[0]["text"].strip()):
                    raise ParseError(
                        source="anec",
                        parser_version=PARSER_VERSION,
                        reason=f"Cabeçalhos mensais ausentes para {year}",
                    )
                continue
            if not row:
                continue
            mes_word = row[0]
            mes_text = mes_word["text"].strip().lower()
            if not _MONTH_RE.match(mes_text):
                continue
            eh_estimativa = mes_text.endswith("*")
            mes_clean = mes_text.rstrip("*").strip()
            mes_num = MONTH_NUM.get(mes_clean)
            if mes_num is None:
                continue

            for idx, (produto, center) in enumerate(columns):
                if produto == "total_products":
                    continue
                left = (columns[idx - 1][1] + center) / 2 if idx else mes_word["x1"]
                right = (
                    (center + columns[idx + 1][1]) / 2 if idx + 1 < len(columns) else float("inf")
                )
                valor, minimum, maximum = _monthly_value(_cell_text(row[1:], left, right))
                records.append(
                    {
                        "ano": year,
                        "mes": mes_num,
                        "produto": produto,
                        "valor_ton": valor,
                        "eh_estimativa": eh_estimativa,
                        "valor_min_ton": minimum,
                        "valor_max_ton": maximum,
                    }
                )

    df = pd.DataFrame.from_records(
        _validated_records(records, models.ANECMonthlyShipment),
        columns=[
            "ano",
            "mes",
            "produto",
            "valor_ton",
            "eh_estimativa",
            "valor_min_ton",
            "valor_max_ton",
        ],
    )
    if df.empty:
        logger.warning(
            "anec_monthly_empty", note="seção sem rows de mês — possível semana de transição"
        )
    return df.astype(
        {
            "ano": "Int64",
            "mes": "Int64",
            "valor_ton": "Float64",
            "eh_estimativa": "bool",
            "valor_min_ton": "Float64",
            "valor_max_ton": "Float64",
        }
    )


def _monthly_columns(row: list[dict[str, Any]]) -> list[tuple[str, float]]:
    columns = [(product, center) for product, _period, center in _detect_weekly_columns(row)]
    for index, word in enumerate(row[:-1]):
        if word["text"].lower() == "total" and row[index + 1]["text"].lower() == "products":
            columns.append(("total_products", (word["x0"] + row[index + 1]["x1"]) / 2))
    columns.sort(key=lambda column: column[1])
    if [product for product, _x in columns] not in [
        [*layout, "total_products"] for layout in PRODUCT_HEADER_LAYOUTS
    ]:
        raise ParseError(
            source="anec",
            parser_version=PARSER_VERSION,
            reason=(
                "Cabeçalhos mensais incompatíveis: esperados os 6 produtos, ou os 4 publicados até "
                "a W2/2026, e Total Products"
            ),
        )
    return columns


def _cell_text(row: list[dict[str, Any]], left: float, right: float) -> str:
    return "".join(
        word["text"] for word in row if left <= (word["x0"] + word["x1"]) / 2 < right
    ).strip()


def _monthly_value(text: str) -> tuple[float | None, float | None, float | None]:
    match = re.fullmatch(r"([\d.,]+)[\-\u2013\u2014]([\d.,]+)", text)
    if match:
        minimum, maximum = _parse_value(match[1]), _parse_value(match[2])
        if minimum is None or maximum is None or minimum > maximum:
            raise ParseError(
                source="anec",
                parser_version=PARSER_VERSION,
                reason=f"Faixa mensal inválida: {text}",
            )
        return None, minimum, maximum
    return _parse_value(text), None, None


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

        if product_words:
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
    product_cols: list[tuple[str, float]],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    year_columns: list[tuple[int, float]] = []
    for y, row in rows:
        if y <= header_y or y >= next_header_y:
            continue
        month_words = [w for w in row if _MONTH_RE.match(w["text"].strip().lower())]
        detected = [
            (int(w["text"].rstrip("*")), (w["x0"] + w["x1"]) / 2)
            for w in row
            if re.fullmatch(r"20\d{2}\*?", w["text"])
        ]
        is_year_header = all(
            re.fullmatch(r"20\d{2}\*?", w["text"]) or w["text"].casefold() == "month" for w in row
        )
        if not year_columns and detected and is_year_header:
            years = [year for year, _x in detected]
            if (
                len(years) != 2 * len(product_cols)
                or years[1] != years[0] + 1
                or years != years[:2] * len(product_cols)
            ):
                raise ParseError(
                    source="anec",
                    parser_version=PARSER_VERSION,
                    reason="Cabeçalhos da comparação anual incompatíveis: esperados pares de anos consecutivos",
                )
            year_columns = detected
            continue
        if not month_words:
            continue
        if not year_columns:
            raise ParseError(
                source="anec",
                parser_version=PARSER_VERSION,
                reason="Cabeçalhos de anos ausentes na comparação anual",
            )

        for mw in month_words:
            mes_text = mw["text"].strip().lower().rstrip("*")
            mes_num = MONTH_NUM.get(mes_text)
            if mes_num is None:
                continue
            section_index = next(
                (i for i, (_year, x) in enumerate(year_columns[::2]) if mw["x1"] < x),
                None,
            )
            if section_index is None:
                continue
            (base_year, base_x), (comparison_year, comparison_x) = year_columns[
                2 * section_index : 2 * section_index + 2
            ]
            half_width = (comparison_x - base_x) / 2
            base_value = _parse_value(
                _cell_text(row, base_x - half_width, base_x + half_width).rstrip("-\u2212")
            )
            comparison_value = _parse_value(
                _cell_text(row, comparison_x - half_width, comparison_x + half_width).rstrip(
                    "-\u2212"
                )
            )
            records.append(
                {
                    "mes": mes_num,
                    "produto": product_cols[section_index][0],
                    "valor_base_ton": base_value,
                    "valor_comparacao_ton": comparison_value,
                    "ano_base": base_year,
                    "ano_comparacao": comparison_year,
                    "eh_estimativa": mw["text"].strip().endswith("*"),
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
        records.extend(_parse_yoy_section(rows, header_y, next_header_y, product_cols))
    return records


def _parse_yoy_comparison(
    pages_words: list[list[dict[str, Any]]], page_indexes: list[int]
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for idx in page_indexes:
        records.extend(_parse_yoy_page(pages_words[idx]))
    df = pd.DataFrame.from_records(
        _validated_records(records, models.ANECAnnualComparison),
        columns=[
            "mes",
            "produto",
            "valor_2025",
            "valor_2026",
            "valor_base_ton",
            "valor_comparacao_ton",
            "ano_base",
            "ano_comparacao",
            "eh_estimativa",
        ],
    )
    return df.astype(
        {
            "mes": "Int64",
            "valor_2025": "Float64",
            "valor_2026": "Float64",
            "valor_base_ton": "Float64",
            "valor_comparacao_ton": "Float64",
            "ano_base": "Int64",
            "ano_comparacao": "Int64",
            "eh_estimativa": "bool",
        }
    )


_PRODUCT_FROM_DESTINATIONS_RE = re.compile(r"Brazilian\s+([A-Za-z\s]+?)\s+Importers", re.IGNORECASE)


def _destination_table_rows(
    words: list[dict[str, Any]],
) -> list[tuple[float, list[dict[str, Any]]]]:
    header = next((word for word in words if word["text"].casefold() == "destination"), None)
    if header is None:
        return []
    table_words = [
        word for word in words if word["x0"] >= header["x0"] - 3 and word["top"] > header["top"]
    ]
    return _group_by_row(table_words)


def _parse_destinations_page(
    words: list[dict[str, Any]],
) -> tuple[str | None, list[dict[str, Any]]]:
    rows = _group_by_row(words)
    produto_canon: str | None = None
    year: int | None = None
    first_month: int | None = None
    last_month: int | None = None
    for _, row in rows:
        text = _row_text(row)
        m = _PRODUCT_FROM_DESTINATIONS_RE.search(text)
        if m:
            year, first_month, last_month = _destination_period(text)
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

    for _y, row in _destination_table_rows(words):
        text = _row_text(row).strip()
        if not text or text.lower().startswith("brazilian"):
            continue
        if text.lower().startswith("destination"):
            continue
        if text.lower().startswith("source:") or "associação" in text.lower():
            continue
        if text.lower().startswith("week"):
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
            break
        out.append(
            {
                "produto": produto_canon,
                "destino": destino.upper(),
                "share_pct": share,
                "ano": year,
                "mes_inicio": first_month,
                "mes_fim": last_month,
            }
        )
    return produto_canon, out


def _parse_destinations(
    pages_words: list[list[dict[str, Any]]], page_indexes: list[int]
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for idx in page_indexes:
        _, recs = _parse_destinations_page(pages_words[idx])
        records.extend(recs)
    df = pd.DataFrame.from_records(
        _validated_records(records, models.ANECDestination),
        columns=["produto", "destino", "share_pct", "ano", "mes_inicio", "mes_fim"],
    )
    return df.astype(
        {"share_pct": "Float64", "ano": "Int64", "mes_inicio": "Int64", "mes_fim": "Int64"}
    )


def _destination_period(text: str) -> tuple[int | None, int | None, int | None]:
    match = re.search(r"Importers\s*:\s*(\d{4})(?:\s*\(([^)]+)\))?", text, re.IGNORECASE)
    if match is None:
        return None, None, None
    year = int(match[1])
    if match[2] is None:
        return year, None, None
    months = re.split(r"\s*[-\u2013\u2014]\s*", match[2].lower().strip())
    month_numbers = MONTH_NUM | {name[:3]: number for name, number in MONTH_NUM.items()}
    if len(months) not in (1, 2):
        return year, None, None
    first, last = (
        month_numbers.get(months[0].rstrip(".")),
        month_numbers.get(months[-1].rstrip(".")),
    )
    if first is None or last is None or first > last:
        return year, None, None
    return year, first, last


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
    weekly_df, avisos_da_linha_total = _parse_weekly_shipments(pages_words[weekly_idx])

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
    yoy_df = _parse_yoy_comparison(pages_words, sorted(set(yoy_indexes)))

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
        avisos_da_linha_total=tuple(avisos_da_linha_total),
    )
