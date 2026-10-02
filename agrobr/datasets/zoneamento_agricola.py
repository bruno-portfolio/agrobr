from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import constants
from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result
from agrobr.zarc import models as zarc_models


async def _fetch_zarc(
    produto: str,
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import zarc

    fetched = await zarc.zoneamento(
        produto=produto or None, as_polars=False, return_meta=True, **kwargs
    )
    return base._unpack_result(fetched)


ZONEAMENTO_AGRICOLA_INFO = base.DatasetInfo(
    name="zoneamento_agricola",
    description="Tábua ZARC por município, cultura e condições publicadas, com riscos por decêndio",
    sources=[
        base.DatasetSource(
            name="zarc",
            priority=1,
            fetch_fn=_fetch_zarc,
            description="ZARC — Zoneamento Agrícola de Risco Climático (MAPA/Embrapa)",
        ),
    ],
    products=sorted(zarc_models.CULTURAS_CANONICAS),
    contract_version="2.1",
    update_frequency="weekly",
    typical_latency="conforme atualização publicada de cada recurso",
    source_url="https://dados.agricultura.gov.br/dataset/tabua-de-risco-zoneamento-agricola-de-risco-climatico",
    source_institution="MAPA/Embrapa",
    license="livre",
)


class ZoneamentoAgricolaDataset(base.BaseDataset):
    info = ZONEAMENTO_AGRICOLA_INFO

    def _produto_do_dataset(self, produto: Any) -> Any:
        return produto

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str):
            raise InvalidParameterError("produto deve ser texto")

    async def fetch(  # type: ignore[override]
        self,
        produto: str | None = None,
        *,
        uf: str | None = None,
        municipio: int | str | None = None,
        safra: str | None = None,
        solo: int | None = None,
        ciclo: int | None = None,
        use_cache: bool = True,
        as_polars: bool = False,
        return_meta: bool = False,
        **kwargs: Any,
    ) -> result.DataFrameResult:
        from agrobr.zarc import query

        if kwargs:
            raise TypeError(f"Argumentos desconhecidos em zoneamento_agricola: {sorted(kwargs)}")
        for name, value in (
            ("use_cache", use_cache),
            ("as_polars", as_polars),
            ("return_meta", return_meta),
        ):
            if not isinstance(value, bool):
                raise InvalidParameterError(f"{name} deve ser booleano")
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "zoneamento_agricola não suporta deterministic: safra e cache não selecionam "
                "uma revisão histórica da tábua ZARC."
            )
        query.build_query(
            produto=produto, uf=uf, municipio=municipio, safra=safra, solo=solo, ciclo=ciclo
        )
        frame, source_name, source_meta, attempted = await self._try_sources(
            produto or "",
            uf=uf,
            municipio=municipio,
            safra=safra,
            solo=solo,
            ciclo=ciclo,
            use_cache=use_cache,
        )
        self._validate_contract(frame)
        meta = (
            self._build_meta(frame, source_name, source_meta, attempted, None)
            if return_meta
            else None
        )
        return result.finalize_result(
            frame,
            meta,
            as_polars=as_polars,
            return_meta=return_meta,
            string_columns=constants.ZARC_STRING_COLUMNS,
        )


_zoneamento_agricola = ZoneamentoAgricolaDataset()

registry.register(_zoneamento_agricola)


@overload
async def zoneamento_agricola(
    *,
    produto: str | None = None,
    uf: str | None = None,
    municipio: int | str | None = None,
    safra: str | None = None,
    solo: int | None = None,
    ciclo: int | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def zoneamento_agricola(
    *,
    produto: str | None = None,
    uf: str | None = None,
    municipio: int | str | None = None,
    safra: str | None = None,
    solo: int | None = None,
    ciclo: int | None = None,
    use_cache: bool = True,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def zoneamento_agricola(
    *,
    produto: str | None = None,
    uf: str | None = None,
    municipio: int | str | None = None,
    safra: str | None = None,
    solo: int | None = None,
    ciclo: int | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult: ...


async def zoneamento_agricola(
    *,
    produto: str | None = None,
    uf: str | None = None,
    municipio: int | str | None = None,
    safra: str | None = None,
    solo: int | None = None,
    ciclo: int | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    return await _zoneamento_agricola.fetch(
        produto=produto,
        uf=uf,
        municipio=municipio,
        safra=safra,
        solo=solo,
        ciclo=ciclo,
        use_cache=use_cache,
        as_polars=as_polars,
        return_meta=return_meta,
        **kwargs,
    )
