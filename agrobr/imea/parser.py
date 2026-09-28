from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from agrobr.exceptions import ParseError

from .models import IMEA_COLUMNS_MAP, cadeia_name

logger = structlog.get_logger()

PARSER_VERSION = 2

COLUNAS_SAIDA = [
    "cadeia",
    "indicador_id",
    "indicador",
    "localidade",
    "valor",
    "variacao",
    "safra",
    "unidade",
    "unidade_descricao",
    "data_publicacao",
]

CHAVES_COTACAO = [
    chave for chave in IMEA_COLUMNS_MAP if chave not in ("CadeiaId", "TipoLocalidadeId")
]

CHAVES_INDICADOR = ("Id", "Nome")


def _exigir(registros: list[dict[str, Any]], chaves: Any, rotulo: str) -> None:
    faltam = sorted({chave for registro in registros for chave in chaves if chave not in registro})
    if faltam:
        raise ParseError(
            source="imea",
            parser_version=PARSER_VERSION,
            reason=f"{rotulo} do IMEA sem as chaves {faltam}",
        )


def parse_cotacoes(
    records: list[dict[str, Any]],
    indicadores: list[dict[str, Any]],
    cadeia_id: int,
) -> pd.DataFrame:
    """`cadeia` é a cadeia pedida: o corpo de uma cadeia pode trazer registro que a fonte
    marca com o `CadeiaId` de outra (o frete de grãos vem no milho com o id da soja)."""
    if not records:
        return pd.DataFrame(columns=COLUNAS_SAIDA)

    _exigir(records, CHAVES_COTACAO, "Cotações")
    _exigir(indicadores, CHAVES_INDICADOR, "Catálogo de indicadores")

    df = pd.DataFrame(records).rename(columns=IMEA_COLUMNS_MAP)
    df["cadeia"] = cadeia_name(cadeia_id)

    df["indicador_id"] = df["indicador_id"].map(lambda v: None if pd.isna(v) else str(v))
    nomes = {str(item["Id"]): item["Nome"] for item in indicadores}
    df["indicador"] = df["indicador_id"].map(nomes)
    sem_nome = int(df["indicador"].isna().sum())
    if sem_nome:
        logger.warning("imea_indicador_sem_nome", registros=sem_nome)

    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df["variacao"] = pd.to_numeric(df["variacao"], errors="coerce")

    df = df[COLUNAS_SAIDA].sort_values(["cadeia", "localidade", "unidade"]).reset_index(drop=True)

    logger.info("imea_parse_ok", records=len(df))

    return df


def filter_by_unidade(df: pd.DataFrame, unidade: str) -> pd.DataFrame:
    return df[df["unidade"] == unidade].reset_index(drop=True)


def filter_by_safra(df: pd.DataFrame, safra: str) -> pd.DataFrame:
    return df[df["safra"] == safra].reset_index(drop=True)
