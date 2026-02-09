"""Fallback BigQuery via basedosdados para crédito rural SICOR.

Quando a API Olinda do BCB está indisponível (HTTP 500, timeout),
este módulo busca os mesmos dados via BigQuery usando a biblioteca
basedosdados (Base dos Dados).

Requer instalação opcional:
    pip install agrobr[bigquery]

A biblioteca basedosdados exige autenticação Google Cloud configurada
(GOOGLE_APPLICATION_CREDENTIALS ou `basedosdados config`).

Tabela: basedosdados.br_bcb_sicor.microdados_operacao
Dicionário: https://basedosdados.org/dataset/br-bcb-sicor
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import structlog

from agrobr.exceptions import SourceUnavailableError

logger = structlog.get_logger()

# Tabela SICOR no BigQuery via basedosdados
BQ_DATASET = "br_bcb_sicor"
BQ_TABLE = "microdados_operacao"

# Mapeamento BigQuery → nomes agrobr (colunas do microdados_operacao)
BQ_COLUMNS_MAP: dict[str, str] = {
    "ano": "ano_emissao",
    "mes": "mes_emissao",
    "sigla_uf": "uf",
    "id_municipio": "cd_municipio",
    "nome_produto": "produto",
    "nome_finalidade": "finalidade",
    "valor_parcela": "valor",
    "area_financiada": "area_financiada",
}


def _check_basedosdados() -> None:
    """Verifica se basedosdados está instalado."""
    try:
        import basedosdados  # noqa: F401
    except ImportError as exc:
        raise SourceUnavailableError(
            source="bcb_bigquery",
            url="https://basedosdados.org/dataset/br-bcb-sicor",
            last_error=("basedosdados não instalado. Instale com: pip install agrobr[bigquery]"),
        ) from exc


def _build_query(
    finalidade: str = "custeio",
    produto: str | None = None,
    safra_ano: int | None = None,
    uf: str | None = None,
) -> str:
    """Constrói query SQL para BigQuery.

    A tabela microdados_operacao contém registros individuais de operações.
    Agregamos por UF, produto e ano para obter totais comparáveis
    à API Olinda.

    Args:
        finalidade: custeio, investimento, comercializacao.
        produto: Nome do produto SICOR (ex: "SOJA").
        safra_ano: Ano de emissão (ex: 2023).
        uf: Sigla UF (ex: "MT").

    Returns:
        Query SQL.
    """
    select = """
SELECT
    ano,
    mes,
    sigla_uf,
    id_municipio,
    nome_produto,
    nome_finalidade,
    SUM(valor_parcela) AS valor_parcela,
    SUM(area_financiada) AS area_financiada,
    COUNT(*) AS qtd_contratos
FROM `basedosdados.br_bcb_sicor.microdados_operacao`
""".strip()

    conditions: list[str] = []

    finalidade_map = {
        "custeio": "CUSTEIO",
        "investimento": "INVESTIMENTO",
        "comercializacao": "COMERCIALIZAÇÃO",
        "comercializacão": "COMERCIALIZAÇÃO",
    }
    nome_finalidade = finalidade_map.get(finalidade.lower(), finalidade.upper())
    conditions.append(f"nome_finalidade = '{nome_finalidade}'")

    if produto:
        conditions.append(f"UPPER(nome_produto) LIKE '%{produto.upper()}%'")

    if safra_ano:
        conditions.append(f"ano = {safra_ano}")

    if uf:
        conditions.append(f"sigla_uf = '{uf.upper()}'")

    where = " AND ".join(conditions)

    group_by = """
GROUP BY ano, mes, sigla_uf, id_municipio, nome_produto, nome_finalidade
ORDER BY ano, sigla_uf, nome_produto
""".strip()

    return f"{select}\nWHERE {where}\n{group_by}"


def _query_bigquery_sync(
    query: str,
) -> list[dict[str, Any]]:
    """Executa query no BigQuery via basedosdados (sync).

    Returns:
        Lista de dicts com registros.

    Raises:
        SourceUnavailableError: Se falhar.
    """
    _check_basedosdados()

    try:
        import basedosdados as bd

        logger.info("bcb_bigquery_query", query_length=len(query))

        df = bd.read_sql(query, billing_project_id=bd.config.billing_project_id)

        if df is None or df.empty:
            return []

        rename = {k: v for k, v in BQ_COLUMNS_MAP.items() if k in df.columns}
        df = df.rename(columns=rename)

        if "qtd_contratos" in df.columns:
            df["qtd_contratos"] = df["qtd_contratos"].astype(int)

        records: list[dict[str, Any]] = df.to_dict("records")

        logger.info("bcb_bigquery_ok", records=len(records))
        return records

    except SourceUnavailableError:
        raise
    except Exception as e:
        raise SourceUnavailableError(
            source="bcb_bigquery",
            url="https://basedosdados.org/dataset/br-bcb-sicor",
            last_error=f"BigQuery error: {e}",
        ) from e


async def fetch_credito_rural_bigquery(
    finalidade: str = "custeio",
    produto_sicor: str | None = None,
    safra_sicor: str | None = None,
    cd_uf: str | None = None,
) -> list[dict[str, Any]]:
    """Busca dados de crédito rural via BigQuery (fallback).

    Interface compatível com client.fetch_credito_rural().

    Args:
        finalidade: "custeio", "investimento", "comercializacao".
        produto_sicor: Nome do produto SICOR (ex: "SOJA").
        safra_sicor: Safra SICOR (ex: "2023/2024"). Extrai ano.
        cd_uf: Código UF IBGE ou sigla (ex: "51" ou "MT").

    Returns:
        Lista de dicts com registros de crédito.
    """
    # Extrair ano da safra SICOR (formato "2023/2024")
    safra_ano: int | None = None
    if safra_sicor:
        with contextlib.suppress(ValueError, IndexError):
            safra_ano = int(safra_sicor.split("/")[0])

    # Converter cd_uf numérico para sigla (BigQuery usa sigla_uf)
    uf_sigla: str | None = None
    if cd_uf:
        from agrobr.bcb.models import UF_CODES

        uf_reverse = {v: k for k, v in UF_CODES.items()}
        uf_sigla = uf_reverse.get(cd_uf, cd_uf if len(cd_uf) == 2 else None)

    query = _build_query(
        finalidade=finalidade,
        produto=produto_sicor,
        safra_ano=safra_ano,
        uf=uf_sigla,
    )

    logger.info(
        "bcb_bigquery_fetch",
        finalidade=finalidade,
        produto=produto_sicor,
        safra_ano=safra_ano,
        uf=uf_sigla,
    )

    return await asyncio.to_thread(_query_bigquery_sync, query)


def is_bigquery_available() -> bool:
    """Verifica se o fallback BigQuery está disponível."""
    try:
        _check_basedosdados()
        return True
    except SourceUnavailableError:
        return False
