from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd

from agrobr import _log
from agrobr.datasets import base, registry
from agrobr.datasets.deterministic import get_snapshot
from agrobr.exceptions import InvalidParameterError
from agrobr.models import MetaInfo
from agrobr.utils import result
from agrobr.utils.result import DataFrameResult

logger = _log.get_logger(__name__)


async def _fetch_mapbiomas(
    _produto: str,
    **kwargs: Any,
) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import mapbiomas

    bioma = kwargs.get("bioma")
    uf = kwargs.get("uf")
    colecao = kwargs.get("colecao")
    if kwargs.get("tipo", "cobertura") == "cobertura":
        fetched = await mapbiomas.cobertura(
            bioma=bioma,
            uf=uf,
            colecao=colecao,
            return_meta=True,
            ano=kwargs.get("ano"),
            classe_id=kwargs.get("classe_id"),
            nivel=kwargs.get("nivel", "estado"),
            municipio=kwargs.get("municipio"),
        )
    else:
        fetched = await mapbiomas.transicao(
            bioma=bioma,
            uf=uf,
            colecao=colecao,
            return_meta=True,
            periodo=kwargs.get("periodo"),
            classe_de_id=kwargs.get("classe_de_id"),
            classe_para_id=kwargs.get("classe_para_id"),
        )
    return base._unpack_result(fetched)


USO_DO_SOLO_INFO = base.DatasetInfo(
    name="uso_do_solo",
    description="Cobertura e uso da terra (MapBiomas) — cobertura anual e transições entre classes",
    sources=[
        base.DatasetSource(
            name="mapbiomas",
            priority=1,
            fetch_fn=_fetch_mapbiomas,
            description="MapBiomas — Mapeamento anual de cobertura e uso da terra",
        ),
    ],
    products=[],
    contract_version="2.0",
    update_frequency="yearly",
    typical_latency="conforme publicação de cada coleção",
    source_url="https://brasil.mapbiomas.org",
    source_institution="MapBiomas",
    unit="ha",
    license="livre",
)


def _validar_consulta(
    tipo: str,
    nivel: str,
    *,
    cobertura: dict[str, Any],
    transicao: dict[str, Any],
    as_polars: bool,
    return_meta: bool,
) -> None:
    if not isinstance(tipo, str) or tipo not in ("cobertura", "transicao"):
        raise InvalidParameterError("tipo deve ser 'cobertura' ou 'transicao'")
    if not isinstance(nivel, str) or nivel not in ("estado", "municipio"):
        raise InvalidParameterError("nivel deve ser 'estado', 'uf' ou 'municipio'")
    for name, value in (("as_polars", as_polars), ("return_meta", return_meta)):
        if not isinstance(value, bool):
            raise InvalidParameterError(f"{name} deve ser booleano")
    incompatible = transicao if tipo == "cobertura" else cobertura
    provided = [name for name, value in incompatible.items() if value is not None]
    if tipo == "transicao" and nivel != "estado":
        provided.append("nivel")
    if provided:
        raise InvalidParameterError(
            f"Parâmetros incompatíveis com tipo='{tipo}': {', '.join(provided)}"
        )
    if get_snapshot() is not None:
        raise InvalidParameterError(
            "uso_do_solo não suporta deterministic: colecao seleciona a edição da fonte, "
            "mas não um snapshot histórico arbitrário."
        )


class UsodoSoloDataset(base.BaseDataset):
    info = USO_DO_SOLO_INFO
    _modos_de_contrato = {
        "tipo='cobertura'": {"tipo": "cobertura"},
        "tipo='transicao'": {"tipo": "transicao"},
        "nivel='municipio'": {"nivel": "municipio"},
    }

    def _contract_name(self, **kwargs: Any) -> str:
        if kwargs.get("nivel", "estado") == "municipio":
            return "mapbiomas_cobertura_municipal"
        return f"mapbiomas_{kwargs.get('tipo', 'cobertura')}"

    def _validate_produto(self, produto: str) -> None:
        if not isinstance(produto, str) or produto != "":
            raise InvalidParameterError(
                "uso_do_solo não aceita produto; use os filtros de cobertura"
            )

    def _resolve_provenance(
        self, source_name: str, source_meta: MetaInfo | None, attempted: list[str]
    ) -> tuple[str, list[str]]:
        if source_meta and source_meta.attempted_sources:
            resolved = list(dict.fromkeys([*attempted[:-1], *source_meta.attempted_sources]))
            return source_meta.selected_source or source_name, resolved
        return super()._resolve_provenance(source_name, source_meta, attempted)

    async def fetch(  # type: ignore[override]
        self,
        tipo: Literal["cobertura", "transicao"] = "cobertura",
        *,
        bioma: str | None = None,
        uf: str | None = None,
        ano: int | None = None,
        classe_id: int | None = None,
        nivel: str = "estado",
        municipio: str | int | None = None,
        periodo: str | None = None,
        classe_de_id: int | None = None,
        classe_para_id: int | None = None,
        colecao: int | None = None,
        as_polars: bool = False,
        return_meta: bool = False,
    ) -> DataFrameResult:
        coverage = {
            "ano": ano,
            "classe_id": classe_id,
            "municipio": municipio,
        }
        transition = {
            "periodo": periodo,
            "classe_de_id": classe_de_id,
            "classe_para_id": classe_para_id,
        }
        nivel = "estado" if nivel == "uf" else nivel
        _validar_consulta(
            tipo,
            nivel,
            cobertura=coverage,
            transicao=transition,
            as_polars=as_polars,
            return_meta=return_meta,
        )
        logger.info("dataset_fetch", dataset=self.info.name, tipo=tipo, bioma=bioma)
        frame, source_name, source_meta, attempted = await self._try_sources(
            "",
            tipo=tipo,
            bioma=bioma,
            uf=uf,
            nivel=nivel,
            colecao=colecao,
            **coverage,
            **transition,
        )
        self._validate_contract(frame, tipo=tipo, nivel=nivel)
        meta = (
            self._build_meta(
                frame,
                source_name,
                source_meta,
                attempted,
                None,
                contract_name=self._contract_name(tipo=tipo, nivel=nivel),
            )
            if return_meta
            else None
        )
        return result.finalize_result(frame, meta, as_polars=as_polars, return_meta=return_meta)


_uso_do_solo = UsodoSoloDataset()
registry.register(_uso_do_solo)


@overload
async def uso_do_solo(
    *,
    tipo: Literal["cobertura", "transicao"] = "cobertura",
    bioma: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: str = "estado",
    municipio: str | int | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[False] = False,
) -> pd.DataFrame: ...


@overload
async def uso_do_solo(
    *,
    tipo: Literal["cobertura", "transicao"] = "cobertura",
    bioma: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: str = "estado",
    municipio: str | int | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    as_polars: Literal[False] = False,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


@overload
async def uso_do_solo(
    *,
    tipo: Literal["cobertura", "transicao"] = "cobertura",
    bioma: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: str = "estado",
    municipio: str | int | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult: ...


async def uso_do_solo(
    *,
    tipo: Literal["cobertura", "transicao"] = "cobertura",
    bioma: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    classe_id: int | None = None,
    nivel: str = "estado",
    municipio: str | int | None = None,
    periodo: str | None = None,
    classe_de_id: int | None = None,
    classe_para_id: int | None = None,
    colecao: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult:
    return await _uso_do_solo.fetch(
        tipo=tipo,
        bioma=bioma,
        uf=uf,
        ano=ano,
        classe_id=classe_id,
        nivel=nivel,
        municipio=municipio,
        periodo=periodo,
        classe_de_id=classe_de_id,
        classe_para_id=classe_para_id,
        colecao=colecao,
        as_polars=as_polars,
        return_meta=return_meta,
    )
