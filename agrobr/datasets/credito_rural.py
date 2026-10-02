from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils.result import DataFrame, DataFrameResult
from agrobr.utils.validation import validate_uf

logger = _log.get_logger(__name__)


async def _fetch_bcb_odata(produto: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import bcb

    safra = kwargs.get("safra")
    finalidade = kwargs.get("finalidade", "custeio")
    uf = kwargs.get("uf")
    agregacao = kwargs.get("agregacao", "uf")
    programa = kwargs.get("programa")
    tipo_seguro = kwargs.get("tipo_seguro")

    result = await bcb.credito_rural(
        produto,
        safra=safra,
        finalidade=finalidade,
        uf=uf,
        agregacao=agregacao,
        programa=programa,
        tipo_seguro=tipo_seguro,
        return_meta=True,
    )

    return _unpack_result(result)


CREDITO_RURAL_INFO = DatasetInfo(
    name="credito_rural",
    description="Crédito rural SICOR/BCB por UF ou programa, com fallback BigQuery",
    sources=[
        DatasetSource(
            name="bcb",
            priority=1,
            fetch_fn=_fetch_bcb_odata,
            description="BCB API Olinda (OData) com fallback BigQuery",
        ),
    ],
    products=[
        "soja",
        "milho",
        "arroz",
        "feijao",
        "trigo",
        "algodao",
        "cafe",
        "cana",
        "mandioca",
        "sorgo",
    ],
    contract_version="2.0",
    update_frequency="monthly",
    typical_latency="M+1",
    source_url="https://olinda.bcb.gov.br",
    source_institution="BCB/SICOR",
    min_date="2013-01-01",
    unit="BRL",
    license="livre",
)


class CreditoRuralDataset(BaseDataset):
    info = CREDITO_RURAL_INFO
    _modos_de_contrato = {
        "agregacao='uf' ou 'programa'": {},
        "agregacao='registro'": {"agregacao": "registro"},
    }

    async def fetch(  # type: ignore[override]
        self,
        produto: str,
        safra: str | None = None,
        finalidade: str = "custeio",
        uf: str | None = None,
        agregacao: Literal["uf", "programa", "registro"] = "uf",
        programa: str | None = None,
        tipo_seguro: str | None = None,
        *,
        return_meta: bool = False,
    ) -> DataFrameResult:
        logger.info(
            "dataset_fetch",
            dataset="credito_rural",
            produto=produto,
            safra=safra,
            finalidade=finalidade,
        )

        if not isinstance(agregacao, str) or agregacao not in {"uf", "programa", "registro"}:
            hint = (
                "Use agregacao='uf', 'programa' ou 'registro'. O SICOR publica município por produto "
                "(CusteioMunicipioProduto e InvestMunicipioProduto), que o agrobr ainda não lê; "
                "o extra agrobr[bigquery] traz dados municipais."
            )
            raise InvalidParameterError(f"agregacao inválida: {agregacao!r}. {hint}")

        snapshot = get_snapshot()
        if snapshot and safra is None:
            ano_snap = int(snapshot[:4])
            safra = f"{ano_snap - 1}/{ano_snap}"

        uf = validate_uf(uf)

        df, source_name, source_meta, attempted = await self._try_sources(
            produto,
            safra=safra,
            finalidade=finalidade,
            uf=uf,
            agregacao=agregacao,
            programa=programa,
            tipo_seguro=tipo_seguro,
        )

        df = self._normalize(df, produto, finalidade)
        self._validate_contract(df, agregacao=agregacao)

        if return_meta:
            return df, self._build_meta(
                df,
                source_name,
                source_meta,
                attempted,
                snapshot,
                contract_name=self._contract_name(agregacao=agregacao),
            )

        return df

    def _contract_name(self, **kwargs: Any) -> str:
        if kwargs.get("agregacao") == "registro":
            return "bcb_credito_rural_registro"
        return "credito_rural"

    def _normalize(self, df: pd.DataFrame, produto: str, finalidade: str) -> pd.DataFrame:
        if "produto" not in df.columns:
            df["produto"] = produto

        if "finalidade" not in df.columns:
            df["finalidade"] = finalidade

        return df


_credito_rural = CreditoRuralDataset()

from agrobr.datasets.registry import register  # noqa: E402

register(_credito_rural)


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    return_meta: Literal[False] = False,
    as_polars: bool = False,
) -> DataFrame: ...


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: Literal[False] = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    return_meta: Literal[True],
    as_polars: bool = False,
) -> tuple[DataFrame, MetaInfo]: ...


@overload
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult: ...


async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    return_meta: bool = False,
    as_polars: bool = False,
) -> DataFrameResult:
    return await _credito_rural.fetch(  # type: ignore[call-arg]
        produto,
        safra=safra,
        finalidade=finalidade,
        uf=uf,
        agregacao=agregacao,
        programa=programa,
        tipo_seguro=tipo_seguro,
        return_meta=return_meta,
        as_polars=as_polars,
    )
