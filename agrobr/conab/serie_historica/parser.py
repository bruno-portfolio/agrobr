from __future__ import annotations

import re
from io import BytesIO
from typing import Any, Literal

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import REGIOES_BRASIL, UFS_BRASIL, SafraHistorica, normalize_produto

logger = structlog.get_logger()

PARSER_VERSION = 1

_UF_SET = set(UFS_BRASIL)
_REGIAO_SET = set(REGIOES_BRASIL)
_BRASIL_LABELS = {"BRASIL", "TOTAL", "TOTAL BRASIL", "TOTAL GERAL", "BRASIL/TOTAL"}

_SAFRA_PATTERN = re.compile(r"\d{4}/\d{2,4}")
_YEAR_PATTERN = re.compile(r"^\d{4}$")

SHEET_METRIC_MAP: dict[str, str] = {
    "area": "area_plantada_mil_ha",
    "area plantada": "area_plantada_mil_ha",
    "producao": "producao_mil_ton",
    "produtividade": "produtividade_kg_ha",
}


def _strip_accents(text: str) -> str:
    import unicodedata

    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _detect_metric_from_sheet_name(name: str) -> str | None:
    lower = _strip_accents(name).lower().strip()
    for key, metric in SHEET_METRIC_MAP.items():
        key_clean = _strip_accents(key)
        if key_clean in lower:
            return metric
    return None


def _find_header_row(df_raw: pd.DataFrame) -> int:
    for idx in range(min(20, len(df_raw))):
        row_values = [str(v).strip() for v in df_raw.iloc[idx] if pd.notna(v)]
        safra_count = sum(
            1 for v in row_values if _SAFRA_PATTERN.search(v) or _YEAR_PATTERN.match(v)
        )
        if safra_count >= 2:
            return idx

    raise ParseError(
        source="conab_serie_historica",
        parser_version=PARSER_VERSION,
        reason="Nao foi possivel encontrar linha de cabecalho com safras",
    )


def _normalize_safra_header(value: str) -> str | None:
    value = str(value).strip()

    match = re.match(r"(\d{4})/(\d{4})$", value)
    if match:
        return f"{match.group(1)}/{match.group(2)[2:]}"

    match = re.match(r"(\d{4})/(\d{2})$", value)
    if match:
        return value

    match = re.match(r"(\d{2})/(\d{2})$", value)
    if match:
        year1 = int(match.group(1))
        prefix = "20" if year1 < 50 else "19"
        return f"{prefix}{match.group(1)}/{match.group(2)}"

    match = re.match(r"^(\d{4})$", value)
    if match:
        year = int(match.group(1))
        if 1970 <= year <= 2050:
            return f"{year}/{str(year + 1)[2:]}"

    return None


def _classify_row(label: str) -> tuple[str, str | None, str | None]:
    upper = label.upper().strip()

    if upper in _BRASIL_LABELS:
        return "brasil", None, None

    if upper in _REGIAO_SET:
        return "regiao", upper, None

    for regiao in _REGIAO_SET:
        if regiao in upper:
            return "regiao", regiao, None

    if upper in _UF_SET:
        return "uf", None, upper

    uf_match = re.search(
        r"\b(AC|AL|AM|AP|BA|CE|DF|ES|GO|MA|MG|MS|MT|PA|PB|PE|PI|PR|RJ|RN|RO|RR|RS|SC|SE|SP|TO)\b",
        upper,
    )
    if uf_match:
        return "uf", None, uf_match.group(1)

    return "unknown", None, None


def _safe_float(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if v == 0.0:
            return None
        return v
    if isinstance(value, str):
        cleaned = (
            value.strip()
            .replace(",", ".")
            .replace(" ", "")
            .replace("(", "")
            .replace(")", "")
            .replace("*", "")
        )
        if not cleaned or cleaned == "-" or cleaned == "...":
            return None
        try:
            v = float(cleaned)
            return v if v != 0.0 else None
        except ValueError:
            return None
    return None


def parse_sheet(
    df_raw: pd.DataFrame,
    produto: str,
    metric_field: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf_filter: str | None = None,
) -> list[SafraHistorica]:
    produto_norm = normalize_produto(produto)

    header_idx = _find_header_row(df_raw)
    headers_raw = [str(v) if pd.notna(v) else "" for v in df_raw.iloc[header_idx]]

    safra_columns: list[tuple[int, str]] = []
    for col_idx, h in enumerate(headers_raw):
        safra = _normalize_safra_header(h)
        if safra is not None:
            safra_year = int(safra[:4])
            if inicio is not None and safra_year < inicio:
                continue
            if fim is not None and safra_year > fim:
                continue
            safra_columns.append((col_idx, safra))

    if not safra_columns:
        raise ParseError(
            source="conab_serie_historica",
            parser_version=PARSER_VERSION,
            reason=f"Nenhuma coluna de safra encontrada (metric={metric_field})",
        )

    label_col = 0
    for col_idx, h in enumerate(headers_raw):
        h_lower = h.lower().strip()
        if any(w in h_lower for w in ("região", "regiao", "uf", "estado", "unidade")):
            label_col = col_idx
            break

    records: list[SafraHistorica] = []
    current_regiao: str | None = None

    for row_idx in range(header_idx + 1, len(df_raw)):
        row = df_raw.iloc[row_idx]

        label = str(row.iloc[label_col]).strip() if pd.notna(row.iloc[label_col]) else ""
        if not label:
            continue

        row_type, regiao, uf = _classify_row(label)

        if row_type == "regiao":
            current_regiao = regiao
            continue

        if row_type == "brasil":
            current_regiao = None
            continue

        if row_type == "unknown":
            continue

        if uf_filter and uf != uf_filter.upper():
            continue

        for col_idx, safra in safra_columns:
            value = _safe_float(row.iloc[col_idx]) if col_idx < len(row) else None
            if value is None:
                continue

            kwargs: dict[str, Any] = {
                "produto": produto_norm,
                "safra": safra,
                "uf": uf,
                "regiao": current_regiao,
            }
            kwargs[metric_field] = value

            records.append(SafraHistorica(**kwargs))

    return records


def parse_serie_historica(
    xls: BytesIO,
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
) -> list[SafraHistorica]:
    produto_norm = normalize_produto(produto)

    try:
        header = xls.read(8)
        xls.seek(0)
        engine: Literal["xlrd", "openpyxl"] = (
            "xlrd" if header[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" else "openpyxl"
        )
        xls_file = pd.ExcelFile(xls, engine=engine)
        sheet_names = xls_file.sheet_names
    except ParseError:
        raise
    except Exception as e:
        raise ParseError(
            source="conab_serie_historica",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler Excel ({engine}): {e}",
        ) from e

    if not sheet_names:
        raise ParseError(
            source="conab_serie_historica",
            parser_version=PARSER_VERSION,
            reason="Arquivo Excel sem abas",
        )

    all_records: dict[tuple[str, str, str | None], dict[str, Any]] = {}

    for sheet_name in sheet_names:
        metric = _detect_metric_from_sheet_name(str(sheet_name))
        if metric is None:
            logger.debug(
                "conab_serie_historica_skip_sheet",
                sheet=sheet_name,
                reason="metrica nao detectada",
            )
            continue

        try:
            df_raw = pd.read_excel(xls_file, sheet_name=sheet_name, header=None)
        except Exception as e:
            logger.warning(
                "conab_serie_historica_sheet_error",
                sheet=sheet_name,
                error=str(e),
            )
            continue

        if df_raw.empty:
            continue

        try:
            records = parse_sheet(
                df_raw=df_raw,
                produto=produto_norm,
                metric_field=metric,
                inicio=inicio,
                fim=fim,
                uf_filter=uf,
            )
        except ParseError:
            logger.warning(
                "conab_serie_historica_parse_sheet_error",
                sheet=sheet_name,
                metric=metric,
            )
            continue

        for rec in records:
            key = (rec.safra, rec.uf or "", rec.regiao or "")
            if key not in all_records:
                all_records[key] = {
                    "produto": rec.produto,
                    "safra": rec.safra,
                    "uf": rec.uf,
                    "regiao": rec.regiao,
                }
            all_records[key][metric] = getattr(rec, metric)

    if not all_records:
        raise ParseError(
            source="conab_serie_historica",
            parser_version=PARSER_VERSION,
            reason=f"Nenhum registro extraido do Excel (produto={produto})",
        )

    result = [SafraHistorica(**data) for data in all_records.values()]

    result.sort(key=lambda r: (r.safra, r.uf or "", r.regiao or ""))

    logger.info(
        "conab_serie_historica_parsed",
        produto=produto_norm,
        records=len(result),
        safras=len({r.safra for r in result}),
        ufs=len({r.uf for r in result if r.uf}),
    )

    return result


def records_to_dataframe(records: list[SafraHistorica]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()

    data = [rec.model_dump() for rec in records]
    df = pd.DataFrame(data)

    numeric_cols = ["area_plantada_mil_ha", "producao_mil_ton", "produtividade_kg_ha"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["produto", "safra", "uf"]).reset_index(drop=True)
    return df
