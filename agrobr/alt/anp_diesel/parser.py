from __future__ import annotations

import hashlib
import io
import json
import unicodedata
from typing import Any, Literal

import pandas as pd
import structlog
from pydantic import ValidationError

from agrobr.exceptions import ParseError
from agrobr.normalize.encoding import detect_encoding_chain
from agrobr.normalize.regions import normalizar_uf
from agrobr.utils.io import read_excel_safe

from . import models
from .models import normalize_produto

logger = structlog.get_logger()

PARSER_VERSION = 4
_AGREGADO = r"(SUB)?TOTAL\b"


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    upper_cols = {_strip_accents(c.upper()): c for c in df.columns}
    for candidate in candidates:
        key = _strip_accents(candidate.upper())
        if key in upper_cols:
            return upper_cols[key]
    return None


def _strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


_ExcelEngine = Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"]


def _detect_header_row(
    content: bytes,
    markers: list[str],
    engine: _ExcelEngine | None = "calamine",
    max_scan: int = 30,
) -> int:
    df_raw = read_excel_safe(
        content,
        source="anp_diesel",
        parser_version=PARSER_VERSION,
        label="XLSX header scan",
        engine=engine,
        header=None,
        nrows=max_scan,
        dtype=str,
    )
    markers_norm = {_strip_accents(m.upper()) for m in markers}
    for i, row in df_raw.iterrows():
        cells = {_strip_accents(str(c).strip().upper()) for c in row if pd.notna(c)}
        if markers_norm.issubset(cells):
            return int(str(i))
    return 0


def _read_precos_xlsx(content: bytes) -> pd.DataFrame:
    header_row = _detect_header_row(
        content,
        markers=["PRODUTO", "DATA INICIAL"],
        engine="calamine",
    )
    df = read_excel_safe(
        content,
        source="anp_diesel",
        parser_version=PARSER_VERSION,
        label="XLSX precos",
        engine="calamine",
        header=header_row,
        dtype=object,
        keep_default_na=False,
    )

    if df.empty:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="XLSX de precos vazio",
        )

    return _normalize_columns(df)


def _locate_precos_columns(df: pd.DataFrame) -> dict[str, str | None]:
    candidates = {
        "produto": ["PRODUTO"],
        "uf": ["ESTADO - SIGLA", "ESTADO"],
        "municipio": ["MUNICÍPIO", "MUNICIPIO"],
        "periodo_inicio": ["DATA INICIAL"],
        "periodo_fim": ["DATA FINAL"],
        "unidade": ["UNIDADE DE MEDIDA"],
        "preco_venda": ["PREÇO MÉDIO REVENDA"],
        "preco_compra": ["PREÇO MÉDIO DISTRIBUIÇÃO"],
        "n_postos": ["NÚMERO DE POSTOS PESQUISADOS"],
    }
    cols = {key: _find_column(df, names) for key, names in candidates.items()}
    required = set(cols) - {"uf", "municipio", "preco_compra"}
    missing = [candidates[key][0] for key in sorted(required) if cols[key] is None]
    if missing:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )
    return cols


def empty_precos() -> pd.DataFrame:
    return pd.DataFrame(
        {name: pd.Series(dtype=dtype) for name, dtype in models.PRECOS_DTYPES.items()}
    )


def _price_level(cols: dict[str, str | None], nivel: str | None) -> str:
    inferred = "municipio" if cols["municipio"] else "uf" if cols["uf"] else "brasil"
    if nivel is not None and nivel != inferred:
        raise ValueError(f"Nivel solicitado {nivel} diverge do XLSX {inferred}")
    if inferred == "municipio" and cols["uf"] is None:
        raise ValueError("XLSX municipal sem coluna de UF")
    return inferred


def _filter_precos(
    df: pd.DataFrame,
    cols: dict[str, str | None],
    produto: str | None,
    uf: str | None,
    municipio: str | None,
) -> pd.DataFrame:
    products = df[cols["produto"]].map(
        lambda value: normalize_produto(value) if isinstance(value, str) else ""
    )
    diesel = products.str.contains("DIESEL", regex=False)
    if not diesel.any():
        raise ValueError("Nenhum registro de diesel encontrado apos filtro")
    selected = diesel & (products.eq(normalize_produto(produto)) if produto else True)
    if uf is not None:
        if cols["uf"] is None:
            raise ValueError("Filtro de UF exige coluna estadual")
        expected_uf = normalizar_uf(uf)
        if expected_uf is None:
            raise ValueError("Filtro de UF invalido")
        selected &= (
            df[cols["uf"]]
            .map(
                lambda value: (
                    normalizar_uf(value) if isinstance(value, str) and value.strip() else ""
                )
            )
            .eq(expected_uf)
        )
    if municipio is not None:
        if cols["municipio"] is None:
            raise ValueError("Filtro municipal exige coluna de municipio")
        selected &= (
            df[cols["municipio"]]
            .map(lambda value: models.normalize_municipio(value) if isinstance(value, str) else "")
            .eq(models.normalize_municipio(municipio))
        )
    return df[selected]


def _sem_agregados(df: pd.DataFrame, cols: dict[str, str | None]) -> tuple[pd.DataFrame, list[str]]:
    if cols["municipio"] is None:
        return df, []
    nomes = df[cols["municipio"]].map(
        lambda value: models.normalize_municipio(value) if isinstance(value, str) else ""
    )
    agregado = nomes.str.match(_AGREGADO)
    if not agregado.any():
        return df, []
    aviso = (
        f"anp_diesel: {int(agregado.sum())} linha(s) com rótulo de agregado na coluna de município "
        f"descartada(s): {', '.join(sorted(set(nomes[agregado])))}"
    )
    return df[~agregado], [aviso]


def _build_precos_result(df: pd.DataFrame, cols: dict[str, str | None], nivel: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for raw in df.to_dict(orient="records"):
        values = {name: raw[column] for name, column in cols.items() if column is not None}
        values.setdefault("uf", "")
        values.setdefault("municipio", "")
        values.setdefault("preco_compra", None)
        record = models.PrecoSemanal.model_validate({**values, "nivel": nivel}).model_dump()
        record.update(
            data=record["periodo_inicio"],
            agregacao="semanal",
            n_semanas=1,
            n_postos_media=float("nan"),
            margem=record["preco_venda"] - record["preco_compra"]
            if record["preco_compra"] is not None
            else float("nan"),
        )
        rows.append(record)
    if not rows:
        return empty_precos()
    return (
        pd.DataFrame(rows, columns=models.COLUNAS_PRECOS)
        .astype(models.PRECOS_DTYPES)
        .sort_values(["data", "uf", "municipio", "produto"], kind="stable")
        .reset_index(drop=True)
    )


def parse_precos(
    content: bytes,
    produto: str | None = None,
    uf: str | None = None,
    municipio: str | None = None,
    *,
    nivel: str | None = None,
) -> pd.DataFrame:
    df = _read_precos_xlsx(content)
    cols = _locate_precos_columns(df)
    try:
        source_level = _price_level(cols, nivel)
        df = _filter_precos(df, cols, produto=produto, uf=uf, municipio=municipio)
        df, avisos = _sem_agregados(df, cols)
        out = _build_precos_result(df, cols, source_level)
    except (ValidationError, ValueError, TypeError) as exc:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason=f"Preco semanal invalido: {exc}",
        ) from exc
    out.attrs["layout_fingerprint"] = hashlib.sha256(
        json.dumps(list(df.columns), ensure_ascii=True, separators=(",", ":")).encode()
    ).hexdigest()
    out.attrs["avisos"] = avisos
    logger.debug("anp_diesel_parse_precos_ok", records=len(out))
    return out


def parse_vendas(
    content: bytes,
    uf: str | None = None,
) -> pd.DataFrame:
    text = content.decode(detect_encoding_chain(content))

    try:
        df = pd.read_csv(io.StringIO(text), sep=";", dtype=str, keep_default_na=False)
    except Exception as e:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler CSV de vendas: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="CSV de vendas vazio",
        )

    df = _normalize_columns(df)

    col_ano = _find_column(df, ["ANO"])
    col_mes = _find_column(df, ["MES", "MÊS"])
    col_vol = _find_column(df, ["VENDAS", "VOLUME", "TOTAL"])
    col_produto = _find_column(df, ["PRODUTO", "COMBUSTÍVEL", "COMBUSTIVEL"])
    col_uf = _find_column(df, ["UNIDADE DA FEDERACAO", "UN. DA FEDERACAO", "UF", "ESTADO"])
    col_regiao = _find_column(df, ["GRANDE REGIAO", "GRANDE REGIÃO", "REGIAO", "REGIÃO"])

    if not all([col_ano, col_mes, col_vol]):
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason=(
                f"Colunas obrigatorias (ANO/MES/VENDAS) nao encontradas. "
                f"Colunas: {list(df.columns)}"
            ),
        )

    if col_produto:
        diesel_mask = (
            df[col_produto].str.strip().str.upper().str.contains("DIESEL", na=False, regex=False)
        )
        df = df[diesel_mask].copy()

    if df.empty:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Nenhum registro de diesel encontrado em vendas",
        )

    if uf:
        if col_uf is None:
            raise ParseError(
                source="anp_diesel",
                parser_version=PARSER_VERSION,
                reason=f"Coluna de UF não encontrada para aplicar o filtro {uf!r}. Colunas: {list(df.columns)}",
            )
        df["_uf_norm"] = (
            df[col_uf]
            .str.strip()
            .apply(lambda v: normalizar_uf(v) if pd.notna(v) and v.strip() else "")
        )
        df = df[df["_uf_norm"] == uf.upper()]
        df = df.drop(columns=["_uf_norm"])

    assert col_ano is not None and col_mes is not None and col_vol is not None
    return _build_vendas_df(df, col_ano, col_mes, col_vol, col_produto, col_uf, col_regiao)


def _vendas_records(df: pd.DataFrame, columns: dict[str, str | None]) -> list[models.VendaMensal]:
    records: list[models.VendaMensal] = []
    for index, row in zip(df.index.tolist(), df.to_dict("records")):
        values = {
            name: row[column] if column is not None else "" for name, column in columns.items()
        }
        try:
            records.append(models.VendaMensal.model_validate(values))
        except ValidationError as error:
            raise ParseError(
                source="anp_diesel",
                parser_version=PARSER_VERSION,
                reason=f"Venda inválida no registro {index + 1}: {error}",
            ) from error
    if not records:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Nenhuma venda extraida do CSV",
        )
    return records


def _build_vendas_df(
    df: pd.DataFrame,
    col_ano: str,
    col_mes: str,
    col_vol: str,
    col_produto: str | None,
    col_uf: str | None,
    col_regiao: str | None,
) -> pd.DataFrame:
    records = _vendas_records(
        df,
        {
            "ano": col_ano,
            "mes": col_mes,
            "volume_m3": col_vol,
            "uf": col_uf,
            "regiao": col_regiao,
            "produto": col_produto,
        },
    )
    out = pd.DataFrame(
        {
            "data": pd.to_datetime([f"{row.ano:04d}-{row.mes:02d}-01" for row in records]),
            "uf": [row.uf for row in records],
            "regiao": [row.regiao for row in records],
            "produto": [row.produto for row in records],
            "volume_m3": pd.Series([row.volume_m3 for row in records], dtype="float64"),
        }
    )
    out = out.sort_values(["data", "uf"]).reset_index(drop=True)

    logger.debug("anp_diesel_parse_vendas_ok", records=len(out))
    return out


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return empty_precos()
    if not df["agregacao"].eq("semanal").all():
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Agregacao mensal exige observacoes semanais",
        )
    if df.duplicated(["data", "nivel", "uf", "municipio", "produto"]).any():
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Semanas duplicadas na selecao: media mensal seria ambigua",
        )
    frame = df.assign(mes=df["data"].dt.to_period("M"))
    keys = ["mes", "produto", "nivel", "uf", "municipio", "unidade"]
    result = (
        frame.groupby(keys, dropna=False)
        .agg(
            preco_venda=("preco_venda", "mean"),
            preco_compra=("preco_compra", "mean"),
            n_postos_media=("n_postos", "mean"),
            n_semanas=("data", "nunique"),
            periodo_inicio=("periodo_inicio", "min"),
            periodo_fim=("periodo_fim", "max"),
        )
        .reset_index()
    )
    result["data"] = result.pop("mes").dt.to_timestamp()
    result["n_postos"] = pd.NA
    result["agregacao"] = "mensal"
    result["margem"] = result["preco_venda"] - result["preco_compra"]
    return (
        result[models.COLUNAS_PRECOS]
        .astype(models.PRECOS_DTYPES)
        .sort_values("data")
        .reset_index(drop=True)
    )
