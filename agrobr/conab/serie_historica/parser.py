from __future__ import annotations

import re
import unicodedata
from datetime import date
from io import BytesIO
from typing import Any, Literal

import pandas as pd
import pydantic
import structlog
from xlrd import biffh, compdoc

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.normalize import regions
from agrobr.normalize.numeric import safe_float
from agrobr.utils.io import open_excel_safe
from agrobr.utils.warnings import warn_once

from . import models
from .models import REGIOES_BRASIL, UFS_BRASIL, SafraHistorica, normalize_produto

logger = structlog.get_logger()

PARSER_VERSION = 3

_UF_SET = set(UFS_BRASIL)
_REGIAO_SET = set(REGIOES_BRASIL)
_BRASIL_LABELS = {"BRASIL", "TOTAL", "TOTAL BRASIL", "TOTAL GERAL", "BRASIL/TOTAL"}
_REFERENCIA_RE = re.compile(r"[Ee]stimativa em ([^\W\d_]+)(?:/| de )(\d{4})")
_MESES = {
    nome: numero
    for numero, nome in enumerate(
        (
            "janeiro",
            "fevereiro",
            "marco",
            "abril",
            "maio",
            "junho",
            "julho",
            "agosto",
            "setembro",
            "outubro",
            "novembro",
            "dezembro",
        ),
        start=1,
    )
}

SHEET_METRIC_MAP: dict[str, str] = {
    "area": "area_plantada_mil_ha",
    "area plantada": "area_plantada_mil_ha",
    "producao": "producao_mil_ton",
    "produtividade": "produtividade_kg_ha",
}


def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_sheet_name(name: str) -> str:
    return " ".join(_strip_accents(name).lower().split())


def _detect_metric_from_sheet_name(name: str) -> str | None:
    lower = _normalize_sheet_name(name)
    for key, metric in SHEET_METRIC_MAP.items():
        key_clean = _strip_accents(key)
        if key_clean in lower:
            return metric
    return None


def _parse_error(produto: str, reason: str) -> ParseError:
    return ParseError(
        source="conab_serie_historica",
        parser_version=PARSER_VERSION,
        reason=f"produto={produto}: {reason}",
    )


def _sheet_decision(produto: str, name: str) -> models.SheetDecision:
    explicit = constants.CONAB_SERIE_PRODUCT_SHEETS.get(produto)
    ignored = constants.CONAB_SERIE_IGNORED_SHEETS.get(produto, {})
    if name in ignored:
        return models.SheetDecision(estado="ignorada", motivo=ignored[name])
    if explicit is not None:
        mapped = explicit.get(name)
    else:
        metric = _detect_metric_from_sheet_name(name)
        mapped = (metric, 1.0) if metric is not None else None
    if mapped is not None:
        return models.SheetDecision(estado="mapeada", campo=mapped[0], multiplicador=mapped[1])
    return models.SheetDecision(estado="desconhecida", motivo="aba_sem_mapeamento")


def resolve_sheets(produto: str, sheet_names: list[str]) -> dict[str, models.SheetDecision]:
    produto = normalize_produto(produto)
    decisions: dict[str, models.SheetDecision] = {}
    names: dict[str, str] = {}
    fields: dict[str, str] = {}
    for name in sheet_names:
        normalized = _normalize_sheet_name(name)
        if normalized in names:
            raise _parse_error(produto, f"Abas com nome ambiguo: {names[normalized]!r}, {name!r}")
        names[normalized] = name
        decision = _sheet_decision(produto, normalized)
        if decision.campo is not None:
            if decision.campo in fields:
                raise _parse_error(
                    produto,
                    f"Abas duplicadas para {decision.campo}: {fields[decision.campo]!r}, {name!r}",
                )
            fields[decision.campo] = name
        if decision.estado == "desconhecida":
            logger.warning("conab_serie_historica_unknown_sheet", produto=produto, sheet=name)
        decisions[name] = decision
    required = constants.CONAB_SERIE_PRODUCT_SHEETS.get(produto)
    missing = (
        set(required) - set(names)
        if required is not None
        else set(SHEET_METRIC_MAP.values()) - set(fields)
    )
    if missing:
        raise _parse_error(produto, f"Abas exigidas ausentes: {', '.join(sorted(missing))}")
    return decisions


def _clean_numeric_str(v: Any) -> str:
    s = str(v).strip()
    try:
        f = float(s)
        if f == int(f):
            return str(int(f))
    except (ValueError, OverflowError):
        pass
    return s


def _find_header_row(df_raw: pd.DataFrame) -> int:
    for idx in range(min(20, len(df_raw))):
        if len(resolve_period_columns(df_raw.iloc[idx].tolist())) >= 2:
            return idx

    raise ParseError(
        source="conab_serie_historica",
        parser_version=PARSER_VERSION,
        reason="Nao foi possivel encontrar linha de cabecalho com safras",
    )


def _normalize_safra_header(value: str) -> str | None:
    value = _clean_numeric_str(value)

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
            return str(year)

    return None


def resolve_period_columns(headers: list[Any]) -> dict[int, models.PeriodDecision]:
    decisions = {}
    for column, value in enumerate(headers):
        label = _clean_numeric_str(value)
        safra = _normalize_safra_header(label)
        if safra is not None:
            decisions[column] = models.PeriodDecision(estado="mapeada", safra=safra)
            continue
        match = re.match(r"^(\d{4}(?:/\d{2,4})?)\s*(.*)$", label)
        if match is not None and match[2].strip():
            safra = _normalize_safra_header(match[1])
            if safra is not None:
                decisions[column] = models.PeriodDecision(
                    estado="ignorada", safra=safra, motivo="previsao"
                )
    return decisions


def _classify_row(label: str) -> tuple[str, str | None, str | None]:
    upper = label.upper().strip()

    if upper in _BRASIL_LABELS:
        return "brasil", None, None

    if upper in _REGIAO_SET:
        return "regiao", upper, None

    if upper in _UF_SET:
        return "uf", None, upper

    uf_match = re.search(
        r"\b(AC|AL|AM|AP|BA|CE|DF|ES|GO|MA|MG|MS|MT|PA|PB|PE|PI|PR|RJ|RN|RO|RR|RS|SC|SE|SP|TO)\b",
        upper,
    )
    if uf_match:
        return "uf", None, uf_match.group(1)

    return "unknown", None, None


def parse_sheet(
    df_raw: pd.DataFrame,
    produto: str,
    metric_field: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf_filter: str | None = None,
    value_multiplier: float = 1.0,
    sheet_name: str | None = None,
) -> list[SafraHistorica]:
    produto_norm = normalize_produto(produto)

    header_idx = _find_header_row(df_raw)
    headers_raw = [str(v) if pd.notna(v) else "" for v in df_raw.iloc[header_idx]]

    safra_columns: list[tuple[int, str]] = []
    for col_idx, decision in resolve_period_columns(headers_raw).items():
        if decision.estado == "ignorada":
            logger.info(
                "conab_serie_historica_period_ignored",
                produto=produto_norm,
                sheet=sheet_name or metric_field,
                label=headers_raw[col_idx],
                motivo=decision.motivo,
            )
        else:
            safra_columns.append((col_idx, decision.safra))

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

    labels = df_raw.iloc[header_idx + 1 :, label_col].dropna()
    if not any(_classify_row(str(label))[0] == "uf" for label in labels):
        raise _parse_error(produto_norm, f"Nenhuma linha de UF encontrada (metric={metric_field})")

    safra_columns = [
        (col_idx, safra)
        for col_idx, safra in safra_columns
        if (inicio is None or int(safra[:4]) >= inicio) and (fim is None or int(safra[:4]) <= fim)
    ]
    if not safra_columns:
        return []

    celulas: list[tuple[str | None, str | None, int, str, float]] = []
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

        if current_regiao is not None and current_regiao != regions.uf_para_regiao(str(uf)).upper():
            raise _parse_error(
                produto_norm,
                f"UF {uf} listada sob {current_regiao} na linha {row_idx + 1} (metric={metric_field})",
            )

        for col_idx, safra in safra_columns:
            value = (
                safe_float(row.iloc[col_idx], strip=("(", ")", "*")) if col_idx < len(row) else None
            )
            if value is not None:
                celulas.append((uf, current_regiao, col_idx, safra, value))

    safras_levantadas = {col_idx for _, _, col_idx, _, value in celulas if value}
    return [
        SafraHistorica(
            produto=produto_norm,
            safra=safra,
            uf=uf,
            regiao=regiao,
            **{metric_field: value * value_multiplier},
        )
        for uf, regiao, col_idx, safra, value in celulas
        if col_idx in safras_levantadas and (not uf_filter or uf == uf_filter.upper())
    ]


def _read_selected_sheet(xls_file: pd.ExcelFile, sheet_name: str, produto: str) -> pd.DataFrame:
    try:
        return pd.read_excel(xls_file, sheet_name=sheet_name, header=None)
    except (OSError, ValueError, biffh.XLRDError, compdoc.CompDocError) as exc:
        raise _parse_error(produto, f"Erro ao ler aba {sheet_name!r}: {exc}") from exc


def _parse_selected_sheet(
    xls_file: pd.ExcelFile,
    sheet_name: str,
    produto: str,
    decision: models.SheetDecision,
    inicio: int | None,
    fim: int | None,
    uf: str | None,
) -> list[SafraHistorica]:
    if decision.campo is None or decision.multiplicador is None:
        return []
    df_raw = _read_selected_sheet(xls_file, sheet_name, produto)
    try:
        return parse_sheet(
            df_raw,
            produto,
            decision.campo,
            inicio=inicio,
            fim=fim,
            uf_filter=uf,
            value_multiplier=decision.multiplicador,
            sheet_name=sheet_name,
        )
    except (ParseError, pydantic.ValidationError) as exc:
        raise _parse_error(produto, f"Layout invalido na aba {sheet_name!r}: {exc}") from exc


def _excel_file(raw: bytes) -> pd.ExcelFile:
    engine: Literal["xlrd", "openpyxl"] = (
        "xlrd" if raw[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" else "openpyxl"
    )
    return open_excel_safe(
        raw, source="conab_serie_historica", parser_version=PARSER_VERSION, engine=engine
    )


def referencia_publicacao(raw: bytes) -> tuple[str, date] | None:
    """Mês da legenda "Estimativa em <mês>/<ano>": a série não publica data de atualização."""
    rotulos = pd.read_excel(_excel_file(raw), sheet_name=0, header=None).iloc[:, 0].dropna()
    for rotulo in rotulos.astype(str):
        match = _REFERENCIA_RE.search(rotulo)
        mes = _MESES.get(_strip_accents(match[1]).lower()) if match else None
        if match and mes:
            return f"{match[1].lower()}/{match[2]}", date(int(match[2]), mes, 1)
    return None


def linha_brasil(raw: bytes, produto: str, periodos: tuple[str, ...]) -> dict[str, float | None]:
    produto_norm = normalize_produto(produto)
    xls_file = _excel_file(raw)
    valores: dict[str, float | None] = {}
    for sheet_name, decision in resolve_sheets(
        produto_norm, list(map(str, xls_file.sheet_names))
    ).items():
        if decision.estado != "mapeada" or decision.campo is None or decision.multiplicador is None:
            continue
        df_raw = _read_selected_sheet(xls_file, sheet_name, produto_norm)
        header_idx = _find_header_row(df_raw)
        headers = [str(v) if pd.notna(v) else "" for v in df_raw.iloc[header_idx]]
        colunas = [
            col
            for col, periodo in resolve_period_columns(headers).items()
            if periodo.estado == "mapeada" and periodo.safra in periodos
        ]
        linhas = [
            idx
            for idx in range(header_idx + 1, len(df_raw))
            if pd.notna(df_raw.iloc[idx, 0])
            and _classify_row(str(df_raw.iloc[idx, 0]))[0] == "brasil"
        ]
        if len(colunas) > 1 or len(linhas) != 1:
            raise _parse_error(
                produto_norm, f"Linha BRASIL ou período {periodos} ambíguo na aba {sheet_name!r}"
            )
        valor = (
            safe_float(df_raw.iloc[linhas[0], colunas[0]], strip=("(", ")", "*"))
            if colunas
            else None
        )
        valores[decision.campo] = valor * decision.multiplicador if valor is not None else None
    return valores


def linhas_brasil(raw: bytes, produto: str) -> dict[tuple[str, str], float]:
    produto_norm = normalize_produto(produto)
    xls_file = _excel_file(raw)
    valores: dict[tuple[str, str], float] = {}
    for sheet_name, decision in resolve_sheets(
        produto_norm, list(map(str, xls_file.sheet_names))
    ).items():
        if (
            decision.campo is None
            or decision.multiplicador is None
            or decision.campo == "produtividade_kg_ha"
        ):
            continue
        df_raw = _read_selected_sheet(xls_file, sheet_name, produto_norm)
        header_idx = _find_header_row(df_raw)
        headers = [str(v) if pd.notna(v) else "" for v in df_raw.iloc[header_idx]]
        linhas = [
            idx
            for idx in range(header_idx + 1, len(df_raw))
            if pd.notna(df_raw.iloc[idx, 0])
            and _classify_row(str(df_raw.iloc[idx, 0]))[0] == "brasil"
        ]
        if len(linhas) != 1:
            continue
        for coluna, periodo in resolve_period_columns(headers).items():
            valor = safe_float(df_raw.iloc[linhas[0], coluna], strip=("(", ")", "*"))
            if periodo.estado == "mapeada" and valor is not None:
                valores[(periodo.safra, decision.campo)] = valor * decision.multiplicador
    return valores


def avisar_soma_das_ufs(
    produto: str, publicacao: str, frame: pd.DataFrame, brasil: dict[tuple[str, str], float]
) -> None:
    """Confere, por (safra, coluna) de `brasil`, a soma das UFs de `frame` com o BRASIL publicado
    (boletim ou série). Planilha que lista só parte das UFs deixa as outras no BRASIL, como no
    café: aí só a soma acima dele é incoerente."""
    for (safra, coluna), publicado in sorted(brasil.items()):
        valores = frame.loc[frame["safra"] == safra, coluna].dropna()
        soma = float(valores.sum())
        diferenca = soma - publicado
        parcial = len(valores) < len(constants.CONAB_UFS)
        if (
            valores.empty
            or abs(diferenca) <= constants.CONAB_ARREDONDAMENTO * (len(valores) + 1)
            or (parcial and diferenca < 0)
        ):
            continue
        warn_once(
            f"conab_soma_ufs:{publicacao}:{produto}:{safra}:{coluna}",
            f"CONAB: em {produto} {safra} ({publicacao}), a soma das {len(valores)} UFs em "
            f"{coluna} dá {soma:.1f}, e o BRASIL publicado, {publicado:.1f} (diferença de "
            f"{diferenca:.1f}); o agrobr repassa os números publicados",
        )


def parse_serie_historica(
    xls: BytesIO,
    produto: str,
    inicio: int | None = None,
    fim: int | None = None,
    uf: str | None = None,
) -> list[SafraHistorica]:
    produto_norm = normalize_produto(produto)

    xls_file = _excel_file(xls.getvalue() if isinstance(xls, BytesIO) else xls.read())
    sheet_names = [str(name) for name in xls_file.sheet_names]

    if not sheet_names:
        raise ParseError(
            source="conab_serie_historica",
            parser_version=PARSER_VERSION,
            reason="Arquivo Excel sem abas",
        )

    decisions = resolve_sheets(produto_norm, sheet_names)
    all_records: dict[tuple[str, str, str | None], dict[str, Any]] = {}

    for sheet_name, decision in decisions.items():
        if decision.estado != "mapeada" or decision.campo is None:
            continue
        records = _parse_selected_sheet(
            xls_file, sheet_name, produto_norm, decision, inicio, fim, uf
        )

        for rec in records:
            key = (rec.safra, rec.uf or "", rec.regiao or "")
            if key not in all_records:
                all_records[key] = {
                    "produto": rec.produto,
                    "safra": rec.safra,
                    "uf": rec.uf,
                    "regiao": rec.regiao,
                }
            all_records[key][decision.campo] = getattr(rec, decision.campo)

    if produto_norm.startswith("cafe"):
        for data in all_records.values():
            area_producao = data.get("area_em_producao_mil_ha")
            area_formacao = data.get("area_formacao_mil_ha")
            if area_producao is not None and area_formacao is not None:
                data["area_plantada_mil_ha"] = area_producao + area_formacao

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
    data = [rec.model_dump() for rec in records]
    df = pd.DataFrame(data, columns=list(SafraHistorica.model_fields))

    numeric_cols = [
        "area_plantada_mil_ha",
        "area_em_producao_mil_ha",
        "area_formacao_mil_ha",
        "area_colhida_mil_ha",
        "producao_mil_ton",
        "produtividade_kg_ha",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    df = df.sort_values(["produto", "safra", "uf"]).reset_index(drop=True)
    return df
