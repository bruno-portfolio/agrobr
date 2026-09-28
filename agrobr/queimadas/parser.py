from __future__ import annotations

import warnings
from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError
from agrobr.normalize import dates, regions
from agrobr.utils.io import read_csv_safe
from agrobr.utils.result import ATRIBUTO_AVISOS

from .models import CHAVE, COLUNAS_SAIDA, estado_para_uf, normalizar_bioma

logger = structlog.get_logger()

PARSER_VERSION = 2

_LEGACY_COLUMNS = {"latitude": "lat", "longitude": "lon", "data_pas": "data_hora_gmt"}


def _o_que_veio(data: bytes) -> str:
    """O que veio no lugar do CSV, pelo começo do corpo."""
    inicio = data[:512].lstrip().lower()
    if inicio.startswith(b"<") or b"<html" in inicio:
        return "a fonte devolveu uma página HTML, e não o CSV"
    if data.startswith(b"PK"):
        return "a fonte devolveu um ZIP que não se lê como o CSV"
    return "CSV vazio: o arquivo não tem linhas de dado"


def parse_focos_csv(data: bytes) -> pd.DataFrame:
    dtype_map: dict[str, str | type[str] | type[float]] = {
        "id": str,
        "lat": float,
        "lon": float,
        "satelite": str,
        "municipio": str,
        "estado": str,
        "pais": str,
        "municipio_id": "Int64",
        "estado_id": "Int64",
        "pais_id": "Int64",
        "bioma": str,
    }
    df = read_csv_safe(
        data,
        source="queimadas",
        parser_version=PARSER_VERSION,
        dtype=dtype_map,  # type: ignore[arg-type]
    )

    if df.empty:
        raise ParseError(
            source="queimadas",
            parser_version=PARSER_VERSION,
            reason=_o_que_veio(data),
        )

    df = df.rename(columns={old: new for old, new in _LEGACY_COLUMNS.items() if new not in df})
    if "municipio_id" not in df:
        df["municipio_id"] = pd.Series(pd.NA, index=df.index, dtype="Int64")
    df["cod_municipio"] = regions.cod_municipio(df["municipio_id"])

    required = {"lat", "lon", "data_hora_gmt", "satelite"}
    missing = required - set(df.columns)
    if missing:
        raise ParseError(
            source="queimadas",
            parser_version=PARSER_VERSION,
            reason=f"Colunas obrigatorias ausentes: {missing}",
        )

    dates.converter_coluna(df, "data_hora_gmt", fonte="queimadas")
    df["data"] = df["data_hora_gmt"].dt.normalize()
    df["hora_gmt"] = df["data_hora_gmt"].dt.strftime("%H:%M")

    if "estado" in df.columns:
        df["uf"] = df["estado"].fillna("").apply(estado_para_uf)
    else:
        df["uf"] = ""

    if "bioma" in df.columns:
        df["bioma"] = df["bioma"].fillna("").apply(normalizar_bioma)

    for col in ["numero_dias_sem_chuva", "precipitacao", "risco_fogo", "frp"]:
        if col in df.columns:
            values = pd.to_numeric(df[col], errors="coerce")
            df[col] = values.mask(values.eq(-999)).astype("Float64")

    output_cols = [*[c for c in COLUNAS_SAIDA if c in df.columns], "uf", "cod_municipio"]

    df = df[output_cols].copy()

    logger.info("queimadas_parse_ok", records=len(df), columns=list(df.columns))
    return df


def tratar_publicacao(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Trata o foco repetido na chave e o FRP negativo que a fonte publica.

    A cópia igual em todas as colunas sai uma vez só. A chave repetida que difere só no FRP sai
    em 1 linha, com o FRP nulo: o foco existe, e só a potência é ambígua. A que difere em outra
    coluna sai do resultado, porque nenhum dos focos é o certo. O FRP negativo, fisicamente
    impossível, vira nulo. Cada caso anota um aviso em `df.attrs`, de onde o `build_source_meta`
    o leva ao `MetaInfo`, e a contagem no dicionário devolvido, para o `source_details`.
    """
    detalhes: dict[str, Any] = {}
    avisos: list[str] = []
    iguais = df.duplicated(keep="first")
    if colapsadas := int(iguais.sum()):
        df = df[~iguais].reset_index(drop=True)
        detalhes["duplicatas_colapsadas"] = colapsadas
        avisos.append(
            f"queimadas: {colapsadas} foco(s) publicados mais de uma vez, iguais em todas as "
            "colunas, saíram uma vez só."
        )
    repetidas = df.duplicated(CHAVE, keep=False)
    distintas = df.loc[
        repetidas, [coluna for coluna in df.columns if coluna != "frp"]
    ].drop_duplicates()
    em_conflito = distintas.loc[distintas.duplicated(CHAVE, keep=False), CHAVE].drop_duplicates()
    conflitantes = pd.Series(False, index=df.index)
    conflitantes[repetidas] = pd.MultiIndex.from_frame(df.loc[repetidas, CHAVE]).isin(
        pd.MultiIndex.from_frame(em_conflito)
    )
    if removidas := int(conflitantes.sum()):
        chaves = em_conflito.astype(str).to_dict("records")
        df = df[~conflitantes].reset_index(drop=True)
        detalhes["chaves_repetidas"] = {"linhas": removidas, "chaves": chaves}
        avisos.append(
            f"queimadas: {removidas} foco(s) repetem a chave ({', '.join(CHAVE)}) com valores "
            f"diferentes e saíram do resultado ({len(chaves)} chave(s) em "
            "source_details['chaves_repetidas'])."
        )
    divergentes = df.duplicated(CHAVE, keep=False)
    if linhas := int(divergentes.sum()):
        excedentes = df.duplicated(CHAVE, keep="first")
        df = df.assign(frp=df["frp"].mask(divergentes))[~excedentes].reset_index(drop=True)
        focos = linhas - int(excedentes.sum())
        detalhes["frp_divergente"] = {"focos": focos, "linhas": linhas}
        avisos.append(
            f"queimadas: {focos} foco(s) publicados mais de uma vez com FRP diferente saíram uma "
            f"vez só, com frp nulo ({linhas} linha(s))."
        )
    if "frp" in df.columns:
        negativos = df["frp"].lt(0).fillna(False)
        if anulados := int(negativos.sum()):
            df = df.assign(frp=df["frp"].mask(negativos))
            detalhes["frp_negativo_anulado"] = anulados
            avisos.append(
                f"queimadas: {anulados} foco(s) com FRP negativo publicado pela fonte "
                "(fisicamente impossível) saíram com frp nulo."
            )
    for aviso in avisos:
        df.attrs.setdefault(ATRIBUTO_AVISOS, []).append(aviso)
        warnings.warn(aviso, UserWarning, stacklevel=2)
    return df, detalhes
