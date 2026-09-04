from __future__ import annotations

import io
from typing import Any, Literal

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.normalize.dates import month_to_number
from agrobr.normalize.encoding import detect_encoding_chain
from agrobr.normalize.numeric import parse_numeric_br
from agrobr.utils.io import read_excel_safe

logger = structlog.get_logger()

PARSER_VERSION = 2


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    upper_cols = {c.upper(): c for c in df.columns}
    for candidate in candidates:
        if candidate.upper() in upper_cols:
            return upper_cols[candidate.upper()]
    return None


def _strip_accents(text: str) -> str:
    import unicodedata

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
        dtype=str,
    )

    if df.empty:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="XLSX de precos vazio",
        )

    return _normalize_columns(df)


def _locate_precos_columns(df: pd.DataFrame) -> dict[str, str | None]:
    cols = {
        "produto": _find_column(df, ["PRODUTO"]),
        "uf": _find_column(df, ["ESTADO - SIGLA", "ESTADO"]),
        "municipio": _find_column(df, ["MUNICÍPIO", "MUNICIPIO"]),
        "data_ini": _find_column(df, ["DATA INICIAL"]),
        "data_fim": _find_column(df, ["DATA FINAL"]),
        "preco_venda": _find_column(df, ["PREÇO MÉDIO REVENDA", "PRECO MEDIO REVENDA"]),
        "preco_compra": _find_column(df, ["PREÇO MÉDIO DISTRIBUIÇÃO", "PRECO MEDIO DISTRIBUICAO"]),
        "n_postos": _find_column(
            df, ["NÚMERO DE POSTOS PESQUISADOS", "NUMERO DE POSTOS PESQUISADOS"]
        ),
    }

    if cols["produto"] is None:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason=f"Coluna PRODUTO nao encontrada. Colunas: {list(df.columns)}",
        )

    return cols


def _filter_precos(
    df: pd.DataFrame,
    cols: dict[str, str | None],
    produto: str | None,
    uf: str | None,
    municipio: str | None,
) -> pd.DataFrame:
    col_produto = cols["produto"]
    diesel_mask = (
        df[col_produto].str.strip().str.upper().str.contains("DIESEL", na=False, regex=False)
    )
    df = df[diesel_mask].copy()

    if df.empty:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Nenhum registro de diesel encontrado apos filtro",
        )

    if produto:
        produto_norm = (
            df[col_produto].str.strip().str.upper().str.replace(r"^[OÓ]LEO\s+", "", regex=True)
        )
        df = df[produto_norm == produto.upper()]

    if uf and cols["uf"]:
        from agrobr.normalize.regions import normalizar_uf

        df["_uf_norm"] = (
            df[cols["uf"]]
            .str.strip()
            .apply(lambda v: normalizar_uf(v) if pd.notna(v) and v.strip() else "")
        )
        df = df[df["_uf_norm"] == uf.upper()]
        df = df.drop(columns=["_uf_norm"])

    if municipio and cols["municipio"]:
        municipio_mask = (
            df[cols["municipio"]]
            .str.strip()
            .str.upper()
            .str.contains(municipio.upper(), na=False, regex=False)
        )
        df = df[municipio_mask]

    return df


def _serie_uf_normalizada(serie: pd.Series) -> pd.Series:
    from agrobr.normalize.regions import normalizar_uf

    return serie.str.strip().apply(
        lambda v: normalizar_uf(v) or "" if pd.notna(v) and v.strip() else ""
    )


def _build_precos_result(df: pd.DataFrame, cols: dict[str, str | None]) -> pd.DataFrame:
    result: dict[str, Any] = {}

    date_col = cols["data_ini"] or cols["data_fim"]
    if not date_col:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Coluna de data nao encontrada",
        )
    result["data"] = pd.to_datetime(df[date_col], errors="coerce", format="mixed", dayfirst=True)

    result["uf"] = _serie_uf_normalizada(df[cols["uf"]]) if cols["uf"] else ""
    result["municipio"] = df[cols["municipio"]].str.strip() if cols["municipio"] else ""
    result["produto"] = (
        df[cols["produto"]].str.strip().str.upper().str.replace(r"^[OÓ]LEO\s+", "", regex=True)
    )

    for campo in ("preco_venda", "preco_compra"):
        if cols[campo]:
            result[campo] = pd.to_numeric(df[cols[campo]].str.replace(",", "."), errors="coerce")

    if cols["n_postos"]:
        result["n_postos"] = pd.to_numeric(df[cols["n_postos"]], errors="coerce").astype("Int64")

    return pd.DataFrame(result)


def _finalize_precos(out: pd.DataFrame) -> pd.DataFrame:
    out = out.dropna(subset=["data"])

    if "preco_venda" in out.columns and "preco_compra" in out.columns:
        out["margem"] = out["preco_venda"] - out["preco_compra"]
    else:
        out["margem"] = float("nan")

    for col in ["preco_venda", "preco_compra", "margem"]:
        if col not in out.columns:
            out[col] = float("nan")

    if "n_postos" not in out.columns:
        out["n_postos"] = pd.array([pd.NA] * len(out), dtype="Int64")

    return out.sort_values("data").reset_index(drop=True)


def parse_precos(
    content: bytes,
    produto: str | None = None,
    uf: str | None = None,
    municipio: str | None = None,
) -> pd.DataFrame:
    df = _read_precos_xlsx(content)
    cols = _locate_precos_columns(df)
    df = _filter_precos(df, cols, produto=produto, uf=uf, municipio=municipio)
    out = _build_precos_result(df, cols)
    out = _finalize_precos(out)

    logger.debug(
        "anp_diesel_parse_precos_ok",
        records=len(out),
        produtos=out["produto"].unique().tolist() if not out.empty else [],
    )

    return out


def _resolve_mes(val: str) -> int | None:
    mes = month_to_number(val)
    if mes is not None:
        return mes
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def parse_vendas(
    content: bytes,
    uf: str | None = None,
) -> pd.DataFrame:
    text = content.decode(detect_encoding_chain(content))

    try:
        df = pd.read_csv(io.StringIO(text), sep=";", dtype=str)
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

    if uf and col_uf:
        from agrobr.normalize.regions import normalizar_uf

        df["_uf_norm"] = (
            df[col_uf]
            .str.strip()
            .apply(lambda v: normalizar_uf(v) if pd.notna(v) and v.strip() else "")
        )
        df = df[df["_uf_norm"] == uf.upper()]
        df = df.drop(columns=["_uf_norm"])

    assert col_ano is not None and col_mes is not None and col_vol is not None
    return _build_vendas_df(df, col_ano, col_mes, col_vol, col_produto, col_uf, col_regiao)


def _build_vendas_df(
    df: pd.DataFrame,
    col_ano: str,
    col_mes: str,
    col_vol: str,
    col_produto: str | None,
    col_uf: str | None,
    col_regiao: str | None,
) -> pd.DataFrame:
    from agrobr.normalize.regions import normalizar_uf

    ano = pd.to_numeric(df[col_ano], errors="coerce")
    mes = df[col_mes].astype(str).str.strip().str.lower().map(_resolve_mes)
    volume = df[col_vol].apply(parse_numeric_br)

    valid = ano.notna() & mes.notna() & volume.notna()
    if not valid.any():
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Nenhuma venda extraida do CSV",
        )

    idx = valid[valid].index
    ano_int = ano.loc[idx].astype(int)
    mes_int = mes.loc[idx].astype(int)

    if col_uf:
        uf_vals = (
            df.loc[idx, col_uf]
            .fillna("")
            .str.strip()
            .apply(lambda v: normalizar_uf(v) or "" if v else "")
            .values
        )
    else:
        uf_vals = ""

    regiao_vals = df.loc[idx, col_regiao].fillna("").str.strip().values if col_regiao else ""

    if col_produto:
        produto_vals = (
            df.loc[idx, col_produto]
            .fillna("")
            .str.strip()
            .str.upper()
            .str.replace(r"^[OÓ]LEO\s+", "", regex=True)
            .values
        )
    else:
        produto_vals = ""

    date_str = ano_int.astype(str) + "-" + mes_int.astype(str).str.zfill(2) + "-01"

    out = pd.DataFrame(
        {
            "data": pd.to_datetime(date_str),
            "uf": uf_vals,
            "regiao": regiao_vals,
            "produto": produto_vals,
            "volume_m3": volume.loc[idx].values,
        }
    )
    out = out.sort_values(["data", "uf"]).reset_index(drop=True)

    logger.debug("anp_diesel_parse_vendas_ok", records=len(out))
    return out


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["mes"] = df["data"].dt.to_period("M")

    group_cols = ["mes", "produto"]
    if "uf" in df.columns and df["uf"].ne("").any():
        group_cols.append("uf")
    if "municipio" in df.columns and df["municipio"].ne("").any():
        group_cols.append("municipio")

    agg: dict[str, pd.NamedAgg] = {}

    if "preco_venda" in df.columns:
        agg["preco_venda"] = pd.NamedAgg(column="preco_venda", aggfunc="mean")
    if "preco_compra" in df.columns:
        agg["preco_compra"] = pd.NamedAgg(column="preco_compra", aggfunc="mean")
    if "n_postos" in df.columns:
        agg["n_postos"] = pd.NamedAgg(column="n_postos", aggfunc="mean")

    result = df.groupby(group_cols).agg(**agg).reset_index()

    if "preco_venda" in result.columns and "preco_compra" in result.columns:
        result["margem"] = result["preco_venda"] - result["preco_compra"]

    result["data"] = result["mes"].dt.to_timestamp()
    result = result.drop(columns=["mes"])

    result = result.sort_values("data").reset_index(drop=True)

    return result
