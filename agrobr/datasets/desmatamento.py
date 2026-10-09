from __future__ import annotations

import warnings
from datetime import UTC, date, datetime
from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log, constants
from agrobr.datasets import _desmatamento_aggregation, base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ResourceLimitError
from agrobr.models import MetaInfo
from agrobr.normalize import regions
from agrobr.utils import result

logger = _log.get_logger(__name__)
PRODUCTS = sorted(regions.BIOMAS_VALIDOS)


async def _fetch_desmatamento(bioma: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import desmatamento as source
    from agrobr.desmatamento import client

    consulta = kwargs.get("consulta")
    if consulta is not None and consulta.max_records is not None:
        encontradas = await client.fetch_hits(consulta)
        if encontradas > consulta.max_records:
            raise ResourceLimitError(
                source="desmatamento",
                reason=(
                    f"a seleção tem {encontradas} ocorrências no WFS, acima de "
                    f"max_registros={consulta.max_records}; a agregação exige a seleção inteira. "
                    "Restrinja os filtros ou use max_registros=None"
                ),
            )
    common = {
        "bioma": bioma,
        "uf": kwargs.get("uf"),
        "max_registros": kwargs.get("max_registros", constants.DESMATAMENTO_DEFAULT_MAX_RECORDS),
        "tamanho_pagina": kwargs.get("tamanho_pagina"),
        "return_meta": True,
    }
    if kwargs.get("tipo", "prodes") == "prodes":
        fetched = await source.prodes(ano=kwargs.get("ano"), **common)
    else:
        fetched = await source.deter(
            inicio=kwargs.get("inicio"),
            fim=kwargs.get("fim"),
            classe=kwargs.get("classe"),
            **common,
        )
    return base._unpack_result(fetched)


DESMATAMENTO_INFO = base.DatasetInfo(
    name="desmatamento",
    description="Áreas de feições PRODES e alertas DETER agregadas por seleção — INPE/TerraBrasilis",
    sources=[
        base.DatasetSource(
            name="inpe",
            priority=1,
            fetch_fn=_fetch_desmatamento,
            description="INPE TerraBrasilis — PRODES e DETER",
        ),
    ],
    products=PRODUCTS,
    contract_version="2.0",
    update_frequency="varies_by_series",
    typical_latency="source_dependent",
    source_url="https://terrabrasilis.dpi.inpe.br",
    source_institution="INPE",
    unit="km²",
    license="livre",
)


def _require_reconciled(source_meta: MetaInfo | None, tipo: str, rows: int) -> None:
    details = source_meta.source_details if source_meta is not None else {}
    coverage = details.get("coverage", {})
    if not isinstance(coverage, dict) or not (
        coverage.get("status") == "reconciled"
        and coverage.get("count_reconciled") is True
        and coverage.get("truncated") is False
        and coverage.get("all_logical_requests_succeeded") is True
        and all(
            type(coverage.get(name)) is int and coverage[name] == rows
            for name in ("expected_rows", "accepted_rows", "returned_rows")
        )
    ):
        raise ContractViolationError(
            dataset=f"desmatamento_{tipo}",
            violation=(
                "Agregação exige seleção reconciliada sem corte local. "
                "Restrinja os filtros ou use max_registros=None para solicitar toda a seleção."
            ),
            got=coverage,
        )


class DesmatamentoDataset(base.BaseDataset):
    info = DESMATAMENTO_INFO
    _modos_de_contrato = {"tipo='prodes'": {"tipo": "prodes"}, "tipo='deter'": {"tipo": "deter"}}

    def _contract_name(self, **kwargs: Any) -> str:
        return f"desmatamento_{kwargs.get('tipo', 'prodes')}"

    def _resolve_provenance(
        self, source_name: str, source_meta: MetaInfo | None, attempted: list[str]
    ) -> tuple[str, list[str]]:
        if source_meta is not None and source_meta.selected_source:
            return source_meta.selected_source, list(source_meta.attempted_sources)
        return source_name, attempted

    async def fetch(  # type: ignore[override]
        self,
        bioma: str = "Cerrado",
        *,
        return_meta: bool = False,
        tipo: Literal["prodes", "deter"] = "prodes",
        ano: int | None = None,
        uf: str | None = None,
        inicio: str | date | datetime | None = None,
        fim: str | date | datetime | None = None,
        classe: str | None = None,
        max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
        tamanho_pagina: int | None = None,
        as_polars: bool = False,
        **kwargs: Any,
    ) -> result.DataFrameResult:
        from agrobr.desmatamento import query

        if kwargs:
            raise TypeError(f"Argumentos desconhecidos em desmatamento: {sorted(kwargs)}")
        if not isinstance(as_polars, bool) or not isinstance(return_meta, bool):
            raise InvalidParameterError("as_polars e return_meta devem ser booleanos")
        if tipo not in ("prodes", "deter"):
            raise InvalidParameterError("tipo deve ser prodes ou deter")
        if get_snapshot() is not None:
            raise InvalidParameterError(
                "desmatamento não suporta deterministic: ano/data não selecionam "
                "uma edição imutável do WFS."
            )
        validated = query.build_query(
            product="PRODES" if tipo == "prodes" else "DETER",
            include_geometry=False,
            bioma=bioma,
            ano=ano,
            uf=uf,
            inicio=inicio,
            fim=fim,
            classe=classe,
            max_registros=max_registros,
            tamanho_pagina=tamanho_pagina,
        )
        logger.info("dataset_fetch", dataset="desmatamento", bioma=validated.biome, tipo=tipo)
        try:
            frame, source_name, source_meta, attempted = await self._try_sources(
                validated.biome,
                tipo=tipo,
                ano=ano,
                uf=uf,
                inicio=inicio,
                fim=fim,
                classe=classe,
                max_registros=max_registros,
                tamanho_pagina=tamanho_pagina,
                consulta=validated,
            )
        except ResourceLimitError as erro:
            raise ContractViolationError(
                dataset=f"desmatamento_{tipo}", violation=erro.reason
            ) from erro
        _require_reconciled(source_meta, tipo, len(frame))
        frame, aggregation = _desmatamento_aggregation.aggregate(frame, tipo)
        self._validate_contract(frame, tipo=tipo)
        meta = self._build_meta(
            frame,
            source_name,
            source_meta,
            attempted,
            None,
            contract_name=self._contract_name(tipo=tipo),
        )
        meta.source_details["aggregation"] = aggregation
        meta.source_details["dataset_query"] = validated.model_dump(mode="json")
        if aviso := _desmatamento_aggregation.aviso_cod_municipio(aggregation):
            meta.validation_warnings.append(aviso)
            warnings.warn(aviso, UserWarning, stacklevel=3)
        meta.timestamp = datetime.now(UTC)
        return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)


_desmatamento = DesmatamentoDataset()
registry.register(_desmatamento)


@overload
async def desmatamento(
    bioma: str = "Cerrado",
    *,
    tipo: Literal["prodes", "deter"] = "prodes",
    ano: int | None = None,
    uf: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
    **kwargs: Any,
) -> pd.DataFrame: ...


@overload
async def desmatamento(
    bioma: str = "Cerrado",
    *,
    tipo: Literal["prodes", "deter"] = "prodes",
    ano: int | None = None,
    uf: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def desmatamento(
    bioma: str = "Cerrado",
    *,
    tipo: Literal["prodes", "deter"] = "prodes",
    ano: int | None = None,
    uf: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult: ...


async def desmatamento(
    bioma: str = "Cerrado",
    *,
    tipo: Literal["prodes", "deter"] = "prodes",
    ano: int | None = None,
    uf: str | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    classe: str | None = None,
    max_registros: int | None = constants.DESMATAMENTO_DEFAULT_MAX_RECORDS,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> result.DataFrameResult:
    return await _desmatamento.fetch(
        bioma,
        tipo=tipo,
        ano=ano,
        uf=uf,
        inicio=inicio,
        fim=fim,
        classe=classe,
        max_registros=max_registros,
        tamanho_pagina=tamanho_pagina,
        as_polars=as_polars,
        return_meta=return_meta,
        **kwargs,
    )
