from __future__ import annotations

import asyncio
import os
import re
from typing import Any

import structlog

from agrobr.exceptions import SourceUnavailableError

logger = structlog.get_logger()

BQ_TIMEOUT = 120.0
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

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-zÀ-ÿ0-9 _\-/(),]+$")


def _check_basedosdados() -> None:
    try:
        import basedosdados  # noqa: F401
    except ImportError as exc:
        raise SourceUnavailableError(
            source="bcb_bigquery",
            url="https://basedosdados.org/dataset/br-bcb-sicor",
            last_error=("basedosdados não instalado. Instale com: pip install agrobr[bigquery]"),
        ) from exc


def _sanitize_bq_str(value: str, field: str) -> str:
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(f"Caractere invalido em {field}: {value!r}")
    return value.replace("'", "\\'")


def _build_query(
    finalidade: str = "custeio",
    produto: str | None = None,
    safra_ano: int | None = None,
    uf: str | None = None,
) -> str:
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
    conditions.append(f"nome_finalidade = '{_sanitize_bq_str(nome_finalidade, 'finalidade')}'")

    if produto:
        safe_produto = _sanitize_bq_str(produto.strip('"').upper(), "produto")
        conditions.append(f"UPPER(nome_produto) = '{safe_produto}'")

    if safra_ano:
        from agrobr.normalize.dates import INICIO_SAFRA_MES

        inicio = int(safra_ano)
        conditions.append(
            f"((ano = {inicio} AND mes >= {INICIO_SAFRA_MES}) "
            f"OR (ano = {inicio + 1} AND mes < {INICIO_SAFRA_MES}))"
        )

    if uf:
        safe_uf = _sanitize_bq_str(uf.upper(), "uf")
        if len(safe_uf) != 2:
            raise ValueError(f"UF deve ter 2 caracteres: {uf!r}")
        conditions.append(f"sigla_uf = '{safe_uf}'")

    where = " AND ".join(conditions)

    group_by = """
GROUP BY ano, mes, sigla_uf, id_municipio, nome_produto, nome_finalidade
ORDER BY ano, sigla_uf, nome_produto
""".strip()

    return f"{select}\nWHERE {where}\n{group_by}"


def _query_bigquery_sync(
    query: str,
) -> list[dict[str, Any]]:
    _check_basedosdados()

    try:
        import basedosdados as bd

        billing_project = os.getenv("AGROBR_BQ_BILLING_PROJECT") or getattr(
            bd.config, "billing_project_id", None
        )
        if not billing_project:
            raise SourceUnavailableError(
                source="bcb_bigquery",
                url="https://basedosdados.org/dataset/br-bcb-sicor",
                last_error=(
                    "BigQuery requer projeto GCP para billing. Defina "
                    "AGROBR_BQ_BILLING_PROJECT=<project-id> ou configure o basedosdados "
                    "(billing_project_id em ~/.basedosdados/config.toml)"
                ),
            )

        logger.info("bcb_bigquery_query", query_length=len(query))

        df = bd.read_sql(query, billing_project_id=billing_project)

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
    safra_ano = int(safra_sicor.split("/")[0]) if safra_sicor else None

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

    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_query_bigquery_sync, query),
            timeout=BQ_TIMEOUT,
        )
    except TimeoutError as exc:
        raise SourceUnavailableError(
            source="bcb_bigquery",
            url="https://basedosdados.org/dataset/br-bcb-sicor",
            last_error=f"BigQuery timeout after {BQ_TIMEOUT}s",
        ) from exc
