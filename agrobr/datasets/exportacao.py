from __future__ import annotations

from typing import Any, Literal, cast, overload

import pandas as pd

from agrobr import _log, contracts
from agrobr.datasets import _comercio_exterior
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrame, DataFrameResult
from agrobr.utils.time import utcnow

logger = _log.get_logger(__name__)


def _produto_para_abiove(produto: str) -> str:
    produto_abiove = {
        "soja": "grao",
        "farelo_soja": "farelo",
        "oleo_soja": "oleo",
        "milho": "milho",
    }.get(produto)
    if produto_abiove is None:
        raise SourceUnavailableError(
            source="abiove",
            last_error=f"Produto {produto!r} não disponível no fallback ABIOVE",
        )
    return produto_abiove


async def _fetch_comexstat(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import comexstat
    from agrobr.comexstat import models

    ano = kwargs.get("ano")
    uf = kwargs.get("uf")

    result = await comexstat.exportacao(produto, ano=ano, uf=uf, return_meta=True)

    df, meta = _unpack_result(result)
    return _comercio_exterior.adapt_comexstat(
        cast("pd.DataFrame", df), meta, combine_ncms=not models.resolve_ncm(produto).codigo_unico
    )


async def _fetch_abiove(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import abiove

    ano: int | None = kwargs.get("ano")
    uf: str | None = kwargs.get("uf")

    if uf is not None:
        raise SourceUnavailableError(
            source="abiove",
            last_error=(
                f"Filtro uf={uf!r} não suportado: a ABIOVE publica apenas totais nacionais"
            ),
        )

    produto_abiove = _produto_para_abiove(produto)

    if ano is None:
        ano = utcnow().year - 1

    result = await abiove.exportacao(
        ano=ano,
        produto=produto_abiove,
        return_meta=True,
    )

    df, meta = _unpack_result(result)
    df["produto"] = produto
    if "uf" not in df.columns:
        df["uf"] = pd.NA
    if "volume_ton" in df.columns and "kg_liquido" not in df.columns:
        df["kg_liquido"] = df["volume_ton"] * 1000
    if "receita_usd_mil" in df.columns and "valor_fob_usd" not in df.columns:
        df["valor_fob_usd"] = df["receita_usd_mil"] * 1000
    return df, meta


EXPORTACAO_INFO = DatasetInfo(
    name="exportacao",
    description="Exportações agrícolas brasileiras por produto, UF e mês",
    sources=[
        DatasetSource(
            name="comexstat",
            priority=1,
            fetch_fn=_fetch_comexstat,
            description="ComexStat/MDIC (dados oficiais de comércio exterior)",
        ),
        DatasetSource(
            name="abiove",
            priority=2,
            fetch_fn=_fetch_abiove,
            description="ABIOVE (complexo soja — farelo, óleo, grão)",
        ),
    ],
    products=["soja", "milho", "cafe", "algodao", "acucar", "farelo_soja", "oleo_soja"],
    contract_version="1.1",
    update_frequency="monthly",
    typical_latency="M+1",
    source_url="https://comexstat.mdic.gov.br",
    source_institution="MDIC/ComexStat",
    min_date="1997-01-01",
    unit="kg / USD",
    license="livre",
)


class ExportacaoDataset(BaseDataset):
    info = EXPORTACAO_INFO

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        ano: int | None = None,
        uf: str | None = None,
        *,
        return_meta: bool = False,
    ) -> DataFrameResult:
        logger.info("dataset_fetch", dataset="exportacao", produto=produto, ano=ano)

        _comercio_exterior.validate_options(return_meta)

        df, source_name, source_meta, attempted = await self._try_sources(produto, ano=ano, uf=uf)

        if source_name == "comexstat":
            df = _comercio_exterior.restore_empty_comexstat(df, source_meta)
        df = self._normalize(df, produto)
        self._validate_contract(df)

        if return_meta:
            return df, _comercio_exterior.dataset_meta(
                self._build_meta(df, source_name, source_meta, attempted, None)
            )

        return df

    def _normalize(self, df: pd.DataFrame, produto: str) -> pd.DataFrame:
        if "produto" not in df.columns:
            df["produto"] = pd.Series(produto, index=df.index, dtype=pd.Series([""]).dtype)

        colunas = [column.name for column in contracts.get_contract("exportacao").columns]
        saida = df[[coluna for coluna in colunas if coluna in df.columns]]
        return saida.astype(
            {
                coluna.name: pd.Series([""]).dtype
                for coluna in contracts.get_contract("exportacao").columns
                if coluna.type == contracts.ColumnType.STRING and coluna.name in saida
            }
        )


_exportacao = ExportacaoDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_exportacao)


@overload
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> DataFrame: ...


@overload
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    *,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult: ...


async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    *,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
        raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
    return await _exportacao.fetch(  # type: ignore[call-arg]
        produto, ano=ano, uf=uf, return_meta=return_meta, as_polars=as_polars
    )
