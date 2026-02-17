"""Parser para dados ANP Diesel — XLSX/XLS -> DataFrames."""

from __future__ import annotations

import io
from datetime import date
from typing import Any, Literal

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

logger = structlog.get_logger()

PARSER_VERSION = 2


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas removendo espacos extras e padronizando."""
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Encontra coluna por lista de candidatos (case-insensitive)."""
    upper_cols = {c.upper(): c for c in df.columns}
    for candidate in candidates:
        if candidate.upper() in upper_cols:
            return upper_cols[candidate.upper()]
    return None


def _strip_accents(text: str) -> str:
    """Remove acentos de texto para comparacao fuzzy de headers."""
    import unicodedata

    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


_ExcelEngine = Literal["xlrd", "openpyxl", "odf", "pyxlsb", "calamine"]


def _detect_header_row(
    content: bytes,
    markers: list[str],
    engine: _ExcelEngine | None = "openpyxl",
    max_scan: int = 30,
) -> int:
    """Detecta a linha do header real escaneando por colunas marcadoras.

    Args:
        content: Bytes raw do arquivo.
        markers: Nomes de coluna esperados (case-insensitive, accent-insensitive).
        engine: Engine do pandas para leitura.
        max_scan: Maximo de linhas a escanear.

    Returns:
        Indice da linha header (0-based).
    """
    df_raw = pd.read_excel(
        io.BytesIO(content),
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


def parse_precos(
    content: bytes,
    produto: str | None = None,
    uf: str | None = None,
    municipio: str | None = None,
) -> pd.DataFrame:
    """Converte XLSX de precos ANP em DataFrame filtrado diesel.

    Args:
        content: Bytes raw do arquivo XLSX.
        produto: Filtro de produto ("DIESEL", "DIESEL S10"). None = todos diesel.
        uf: Filtro de UF (sigla). None = todas.
        municipio: Filtro de municipio. None = todos.

    Returns:
        DataFrame com colunas: data, uf, municipio, produto, preco_venda,
        preco_compra, margem, n_postos.

    Raises:
        ParseError: Se dados vazios ou malformados.
    """
    try:
        header_row = _detect_header_row(
            content,
            markers=["PRODUTO", "DATA INICIAL"],
            engine="openpyxl",
        )
        df = pd.read_excel(
            io.BytesIO(content),
            engine="openpyxl",
            header=header_row,
            dtype=str,
        )
    except ParseError:
        raise
    except Exception as e:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler XLSX de precos: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="XLSX de precos vazio",
        )

    df = _normalize_columns(df)

    col_produto = _find_column(df, ["PRODUTO"])
    col_uf = _find_column(df, ["ESTADO - SIGLA", "ESTADO"])
    col_municipio = _find_column(df, ["MUNICÍPIO", "MUNICIPIO"])
    col_data_ini = _find_column(df, ["DATA INICIAL"])
    col_data_fim = _find_column(df, ["DATA FINAL"])
    col_preco_venda = _find_column(df, ["PREÇO MÉDIO REVENDA", "PRECO MEDIO REVENDA"])
    col_preco_compra = _find_column(df, ["PREÇO MÉDIO DISTRIBUIÇÃO", "PRECO MEDIO DISTRIBUICAO"])
    col_n_postos = _find_column(
        df, ["NÚMERO DE POSTOS PESQUISADOS", "NUMERO DE POSTOS PESQUISADOS"]
    )

    if col_produto is None:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason=f"Coluna PRODUTO nao encontrada. Colunas: {list(df.columns)}",
        )

    diesel_mask = df[col_produto].str.strip().str.upper().str.contains("DIESEL", na=False)
    df = df[diesel_mask].copy()

    if df.empty:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Nenhum registro de diesel encontrado apos filtro",
        )

    if produto:
        produto_upper = produto.upper()
        produto_norm = df[col_produto].str.strip().str.upper()
        produto_mask = (produto_norm == produto_upper) | (produto_norm == f"OLEO {produto_upper}")
        df = df[produto_mask].copy()

    if uf and col_uf:
        from agrobr.normalize.regions import normalizar_uf

        df["_uf_norm"] = (
            df[col_uf]
            .str.strip()
            .apply(lambda v: normalizar_uf(v) if pd.notna(v) and v.strip() else "")
        )
        uf_mask = df["_uf_norm"] == uf.upper()
        df = df[uf_mask].copy()
        df = df.drop(columns=["_uf_norm"])

    if municipio and col_municipio:
        municipio_mask = (
            df[col_municipio].str.strip().str.upper().str.contains(municipio.upper(), na=False)
        )
        df = df[municipio_mask].copy()

    result: dict[str, Any] = {}

    if col_data_ini:
        result["data"] = pd.to_datetime(df[col_data_ini], errors="coerce", dayfirst=True)
    elif col_data_fim:
        result["data"] = pd.to_datetime(df[col_data_fim], errors="coerce", dayfirst=True)
    else:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Coluna de data nao encontrada",
        )

    if col_uf:
        from agrobr.normalize.regions import normalizar_uf

        result["uf"] = (
            df[col_uf]
            .str.strip()
            .apply(lambda v: normalizar_uf(v) or v.upper() if pd.notna(v) and v.strip() else "")
        )
    else:
        result["uf"] = ""
    result["municipio"] = df[col_municipio].str.strip() if col_municipio else ""
    result["produto"] = df[col_produto].str.strip().str.upper()

    if col_preco_venda:
        result["preco_venda"] = pd.to_numeric(
            df[col_preco_venda].str.replace(",", "."), errors="coerce"
        )

    if col_preco_compra:
        result["preco_compra"] = pd.to_numeric(
            df[col_preco_compra].str.replace(",", "."), errors="coerce"
        )

    if col_n_postos:
        result["n_postos"] = pd.to_numeric(df[col_n_postos], errors="coerce").astype("Int64")

    out = pd.DataFrame(result)

    out = out.dropna(subset=["data"]).copy()

    if "preco_venda" in out.columns and "preco_compra" in out.columns:
        out["margem"] = out["preco_venda"] - out["preco_compra"]
    else:
        out["margem"] = float("nan")

    for col in ["preco_venda", "preco_compra", "margem"]:
        if col not in out.columns:
            out[col] = float("nan")

    if "n_postos" not in out.columns:
        out["n_postos"] = pd.array([pd.NA] * len(out), dtype="Int64")

    out = out.sort_values("data").reset_index(drop=True)

    logger.debug(
        "anp_diesel_parse_precos_ok",
        records=len(out),
        produtos=out["produto"].unique().tolist() if not out.empty else [],
    )

    return out


def parse_vendas(
    content: bytes,
    uf: str | None = None,
) -> pd.DataFrame:
    """Converte XLS de vendas ANP em DataFrame filtrado diesel.

    Args:
        content: Bytes raw do arquivo XLS.
        uf: Filtro de UF (sigla). None = todas.

    Returns:
        DataFrame com colunas: data, uf, regiao, produto, volume_m3.

    Raises:
        ParseError: Se dados vazios ou malformados.
    """
    try:
        header_row = _detect_header_row(
            content,
            markers=["COMBUSTÍVEL", "ANO"],
            engine=None,
        )
        df = pd.read_excel(
            io.BytesIO(content),
            engine=None,
            header=header_row,
            dtype=str,
        )
    except ParseError:
        raise
    except Exception as e:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason=f"Erro ao ler XLS de vendas: {e}",
        ) from e

    if df.empty:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="XLS de vendas vazio",
        )

    df = _normalize_columns(df)

    col_produto = _find_column(df, ["COMBUSTÍVEL", "COMBUSTIVEL", "PRODUTO"])
    col_uf = _find_column(df, ["UF", "ESTADO", "UN. DA FEDERAÇÃO", "UN. DA FEDERACAO"])
    col_regiao = _find_column(df, ["GRANDE REGIÃO", "GRANDE REGIAO", "REGIÃO", "REGIAO"])

    month_cols = [c for c in df.columns if _is_month_column(c)]

    if col_produto is None:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason=f"Coluna de produto nao encontrada. Colunas: {list(df.columns)}",
        )

    diesel_mask = df[col_produto].str.strip().str.upper().str.contains("DIESEL", na=False)
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
        uf_mask = df["_uf_norm"] == uf.upper()
        df = df[uf_mask].copy()
        df = df.drop(columns=["_uf_norm"])

    if not month_cols:
        col_ano = _find_column(df, ["ANO"])
        col_mes = _find_column(df, ["MÊS", "MES"])
        col_vol = _find_column(df, ["VENDAS", "VOLUME", "TOTAL"])

        if col_ano and col_mes and col_vol:
            return _parse_vendas_long(
                df, col_uf, col_regiao, col_produto, col_ano, col_mes, col_vol
            )

        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Nao encontrou colunas mensais ou formato long (ano/mes/volume)",
        )

    return _parse_vendas_wide(df, col_uf, col_regiao, col_produto, month_cols)


def _is_month_column(col: str) -> bool:
    """Verifica se coluna representa um mes (ex: 'JAN', 'FEV', 'MAR.2024')."""
    meses = {"JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO", "SET", "OUT", "NOV", "DEZ"}
    upper = col.strip().upper()
    return any(upper.startswith(m) for m in meses)


_MONTH_MAP: dict[str, int] = {
    "JAN": 1,
    "FEV": 2,
    "MAR": 3,
    "ABR": 4,
    "MAI": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SET": 9,
    "OUT": 10,
    "NOV": 11,
    "DEZ": 12,
}


def _parse_vendas_wide(
    df: pd.DataFrame,
    col_uf: str | None,
    col_regiao: str | None,
    col_produto: str,
    month_cols: list[str],
) -> pd.DataFrame:
    """Parse vendas em formato wide (meses como colunas)."""
    col_ano = _find_column(df, ["ANO"])

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        uf_val = str(row[col_uf]).strip().upper() if col_uf and pd.notna(row.get(col_uf)) else ""
        regiao_val = (
            str(row[col_regiao]).strip() if col_regiao and pd.notna(row.get(col_regiao)) else ""
        )
        produto_val = str(row[col_produto]).strip().upper()
        ano_val = int(float(row[col_ano])) if col_ano and pd.notna(row.get(col_ano)) else None

        for mc in month_cols:
            vol_str = row.get(mc)
            if pd.isna(vol_str) or str(vol_str).strip() in ("", "-"):
                continue

            try:
                raw = str(vol_str).replace(" ", "")
                if "," in raw and "." in raw:
                    raw = raw.replace(".", "").replace(",", ".")
                elif "," in raw:
                    raw = raw.replace(",", ".")
                volume = float(raw)
            except ValueError:
                continue

            upper_mc = mc.strip().upper()
            mes_key = upper_mc[:3]
            mes_num = _MONTH_MAP.get(mes_key)
            if mes_num is None:
                continue

            if ano_val:
                ano = ano_val
            else:
                parts = upper_mc.split(".")
                if len(parts) == 2:
                    try:
                        ano = int(parts[1])
                    except ValueError:
                        continue
                else:
                    continue

            rows.append(
                {
                    "data": date(ano, mes_num, 1),
                    "uf": uf_val,
                    "regiao": regiao_val,
                    "produto": produto_val,
                    "volume_m3": volume,
                }
            )

    if not rows:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Nenhuma venda extraida do formato wide",
        )

    out = pd.DataFrame(rows)
    out["data"] = pd.to_datetime(out["data"])
    out = out.sort_values(["data", "uf"]).reset_index(drop=True)

    logger.debug("anp_diesel_parse_vendas_ok", records=len(out))
    return out


def _parse_vendas_long(
    df: pd.DataFrame,
    col_uf: str | None,
    col_regiao: str | None,
    col_produto: str,
    col_ano: str,
    col_mes: str,
    col_vol: str,
) -> pd.DataFrame:
    """Parse vendas em formato long (ano, mes, volume como linhas)."""
    rows: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        try:
            ano = int(float(row[col_ano]))
            mes = int(float(row[col_mes]))
        except (ValueError, TypeError):
            continue

        vol_str = row.get(col_vol)
        if pd.isna(vol_str) or str(vol_str).strip() in ("", "-"):
            continue

        try:
            raw = str(vol_str).replace(" ", "")
            if "," in raw and "." in raw:
                raw = raw.replace(".", "").replace(",", ".")
            elif "," in raw:
                raw = raw.replace(",", ".")
            volume = float(raw)
        except ValueError:
            continue

        uf_val = str(row[col_uf]).strip().upper() if col_uf and pd.notna(row.get(col_uf)) else ""
        regiao_val = (
            str(row[col_regiao]).strip() if col_regiao and pd.notna(row.get(col_regiao)) else ""
        )
        produto_val = str(row[col_produto]).strip().upper()

        rows.append(
            {
                "data": date(ano, mes, 1),
                "uf": uf_val,
                "regiao": regiao_val,
                "produto": produto_val,
                "volume_m3": volume,
            }
        )

    if not rows:
        raise ParseError(
            source="anp_diesel",
            parser_version=PARSER_VERSION,
            reason="Nenhuma venda extraida do formato long",
        )

    out = pd.DataFrame(rows)
    out["data"] = pd.to_datetime(out["data"])
    out = out.sort_values(["data", "uf"]).reset_index(drop=True)

    logger.debug("anp_diesel_parse_vendas_ok", records=len(out))
    return out


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega precos semanais em medias mensais.

    Args:
        df: DataFrame semanal (output de parse_precos).

    Returns:
        DataFrame mensal com media ponderada por n_postos.
    """
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
