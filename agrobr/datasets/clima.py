from __future__ import annotations

import copy
from dataclasses import replace
from datetime import date
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr import constants, contracts
from agrobr.datasets.base import BaseDataset, DatasetInfo, DatasetSource, _unpack_result
from agrobr.datasets.deterministic import get_snapshot
from agrobr.datasets.registry import register
from agrobr.exceptions import ContractViolationError, InvalidParameterError, SourceUnavailableError
from agrobr.models import MetaInfo
from agrobr.utils import result as result_utils
from agrobr.utils import validation
from agrobr.utils.time import hoje

logger = structlog.get_logger()


def _normalize_uf_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    frame = df.copy()
    frame["fonte"] = source
    if source == "inmet":
        for column in ("umidade_media", "radiacao_media_mj", "vento_medio_ms", "lat", "lon"):
            if column not in frame:
                frame[column] = pd.Series(pd.NA, index=frame.index, dtype="Float64")
        frame["agregacao_espacial"] = "estacoes"
        frame["base_tempo"] = "UTC"
    else:
        if "num_estacoes" not in frame:
            frame["num_estacoes"] = pd.Series(pd.NA, index=frame.index, dtype="Int64")
        frame["agregacao_espacial"] = "ponto_grade"
        frame["base_tempo"] = "LST"
    return frame


async def _fetch_inmet(key: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import inmet

    if kwargs.get("estacao") is not None:
        result = await inmet.estacao(
            key, kwargs["inicio"], kwargs["fim"], agregacao=kwargs["agregacao"], return_meta=True
        )
        return _unpack_result(result)
    result = await inmet.clima_uf(key, kwargs["ano"], return_meta=True)
    df, meta = _unpack_result(result)
    return _normalize_uf_source(df, "inmet"), meta


async def _fetch_inmet_historico(key: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import inmet

    if kwargs.get("estacao") is not None:
        result = await inmet.historico_periodo(
            key, kwargs["inicio"], kwargs["fim"], agregacao=kwargs["agregacao"], return_meta=True
        )
        return _unpack_result(result)
    result = await inmet.historico_uf(key, kwargs["ano"], return_meta=True)
    df, meta = _unpack_result(result)
    if df.empty:
        raise SourceUnavailableError(
            source="inmet_historico",
            url=meta.source_url if meta else "",
            last_error=f"Sem observações históricas da UF={key} no ano {kwargs['ano']}",
        )
    return _normalize_uf_source(df, "inmet"), meta


async def _fetch_nasa(uf: str, **kwargs: Any) -> tuple[pd.DataFrame, MetaInfo | None]:
    from agrobr import nasa_power

    result = await nasa_power.clima_uf(uf, kwargs["ano"], agregacao="mensal", return_meta=True)
    df, meta = _unpack_result(result)
    return _normalize_uf_source(df, "nasa_power"), meta


def _validate_source(fonte: str | None) -> None:
    if fonte is not None and fonte not in ("inmet", "inmet_historico", "nasa_power"):
        raise InvalidParameterError(
            "fonte deve ser 'inmet', 'inmet_historico', 'nasa_power' ou None"
        )


def _normalize_year(ano: int | None, snapshot: str | None) -> int:
    corrente = hoje().year
    if ano is None:
        ano = int(snapshot[:4]) if snapshot else corrente
    if (
        isinstance(ano, bool)
        or not isinstance(ano, int)
        or not constants.CLIMA_MIN_ANO <= ano <= corrente
    ):
        raise InvalidParameterError(
            f"ano deve ser inteiro entre {constants.CLIMA_MIN_ANO} e {corrente}"
        )
    return ano


def _fill_nullable_columns(df: pd.DataFrame, name: str) -> pd.DataFrame:
    contract = contracts.get_contract(name)
    frame = df.copy()
    empty = contract.empty_frame()
    for column in contract.columns:
        if column.nullable and column.name not in frame:
            frame[column.name] = empty[column.name].reindex(frame.index)
    return frame


CLIMA_INFO = DatasetInfo(
    name="clima",
    description="Clima por UF ou estação: INMET API e histórico público, com NASA por UF",
    sources=[
        DatasetSource(
            name="inmet",
            priority=1,
            fetch_fn=_fetch_inmet,
            description="INMET — API observacional com token",
        ),
        DatasetSource(
            name="inmet_historico",
            priority=2,
            fetch_fn=_fetch_inmet_historico,
            description="INMET — arquivos anuais públicos de estações automáticas",
        ),
        DatasetSource(
            name="nasa_power",
            priority=3,
            fetch_fn=_fetch_nasa,
            description="NASA POWER — ponto de grade representativo da UF",
        ),
    ],
    products=[],
    contract_version="3.1",
    update_frequency="daily",
    typical_latency="variável conforme acesso e edição anual",
    source_url="https://portal.inmet.gov.br",
    source_institution="INMET / NASA",
    min_date="1981-01-01",
    unit="°C / mm",
    license="livre",
)


class ClimaDataset(BaseDataset):
    info = CLIMA_INFO

    def _validate_produto(self, produto: str) -> None:
        pass

    def _contract_name(self, **kwargs: Any) -> str:
        if kwargs.get("estacao") is None:
            return "clima"
        return "clima_estacao_horaria" if kwargs.get("agregacao") == "horario" else "clima_estacao"

    def _runner(self, fonte: str | None, *, station: bool, start_year: int) -> ClimaDataset:
        if fonte == "inmet_historico" and start_year < constants.INMET_HISTORICO_MIN_ANO:
            raise InvalidParameterError("Histórico público INMET disponível a partir de 2000")
        selected = [
            source
            for source in self.info.sources
            if (fonte is None or source.name == fonte)
            and not (station and source.name == "nasa_power")
            and not (
                source.name == "inmet_historico" and start_year < constants.INMET_HISTORICO_MIN_ANO
            )
        ]
        runner = copy.copy(self)
        runner.info = replace(self.info, sources=selected)
        return runner

    async def fetch(  # type: ignore[override]
        self,
        uf: str | None = None,
        ano: int | None = None,
        *,
        estacao: str | None = None,
        inicio: str | date | None = None,
        fim: str | date | None = None,
        agregacao: str = "diario",
        return_meta: bool = False,
        fonte: str | None = None,
        as_polars: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        _validate_source(fonte)
        if estacao is not None:
            if uf is not None or ano is not None:
                raise InvalidParameterError("Modo estação não aceita uf ou ano; use inicio e fim")
            return await self._fetch_estacao(
                estacao,
                inicio=inicio,
                fim=fim,
                agregacao=agregacao,
                return_meta=return_meta,
                fonte=fonte,
                as_polars=as_polars,
            )
        if inicio is not None or fim is not None:
            raise InvalidParameterError("inicio e fim são exclusivos do modo estação")
        if uf is None:
            raise InvalidParameterError("uf é obrigatório para modo UF")
        if not isinstance(uf, str):
            raise InvalidParameterError("uf deve ser uma string de duas letras")
        if agregacao not in ("diario", "mensal"):
            raise InvalidParameterError("Modo UF retorna agregação mensal")
        uf = validation.validate_uf(uf)
        assert uf is not None
        snapshot = get_snapshot()
        year = _normalize_year(ano, snapshot)
        logger.info("dataset_fetch", dataset="clima", uf=uf, ano=year, fonte=fonte)
        runner = self._runner(fonte, station=False, start_year=year)
        df, selected, source_meta, attempted = await runner._try_sources(uf, ano=year)
        df = self._normalize(df, uf)
        return self._finalize(
            df,
            selected,
            source_meta,
            attempted,
            snapshot,
            contract_name="clima",
            aggregation="mensal",
            return_meta=return_meta,
            as_polars=as_polars,
        )

    async def _fetch_estacao(
        self,
        codigo: str,
        *,
        inicio: str | date | None,
        fim: str | date | None,
        agregacao: str,
        return_meta: bool,
        fonte: str | None,
        as_polars: bool,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        from agrobr.inmet import models

        if inicio is None or fim is None:
            raise InvalidParameterError("inicio e fim são obrigatórios para modo estacao")
        if fonte == "nasa_power":
            raise InvalidParameterError("NASA POWER não representa uma estação INMET")
        start, end = models.validate_periodo(inicio, fim)
        models.validate_agregacao(agregacao)
        runner = self._runner(fonte, station=True, start_year=start.year)
        df, selected, source_meta, attempted = await runner._try_sources(
            codigo,
            estacao=codigo,
            inicio=start.isoformat(),
            fim=end.isoformat(),
            agregacao=agregacao,
        )
        name = self._contract_name(estacao=codigo, agregacao=agregacao)
        df = _fill_nullable_columns(df, name)
        return self._finalize(
            df,
            selected,
            source_meta,
            attempted,
            get_snapshot(),
            contract_name=name,
            aggregation=agregacao,
            return_meta=return_meta,
            as_polars=as_polars,
        )

    def _normalize(self, df: pd.DataFrame, uf: str) -> pd.DataFrame:
        frame = _fill_nullable_columns(df, "clima")
        if "uf" in frame:
            frame["uf"] = frame["uf"].str.upper()
            if not frame["uf"].eq(uf).all():
                raise ContractViolationError(dataset="clima", violation="Fonte retornou outra UF")
        else:
            frame["uf"] = uf
        return frame

    def _finalize(
        self,
        df: pd.DataFrame,
        selected: str,
        source_meta: MetaInfo | None,
        attempted: list[str],
        snapshot: str | None,
        *,
        contract_name: str,
        aggregation: str,
        return_meta: bool,
        as_polars: bool,
    ) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
        contracts.validate_dataset(df, contract_name)
        meta = None
        if return_meta:
            meta = self._build_meta(
                df, selected, source_meta, attempted, snapshot, contract_name=contract_name
            )
            meta.dataset = self.info.name
            _add_climate_context(meta, aggregation, station=contract_name != "clima")
        return result_utils.finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)


def _add_climate_context(meta: MetaInfo, aggregation: str, *, station: bool) -> None:
    nasa = meta.selected_source == "nasa_power"
    details = meta.source_details
    details.setdefault("access", meta.selected_source)
    details.setdefault("time_basis", "LST" if nasa else "UTC")
    details.setdefault("temporal_aggregation", aggregation)
    details.setdefault("spatial_scope", "station" if station else "point" if nasa else "stations")
    if not station:
        if not nasa:
            details.setdefault(
                "station_selection",
                "archive_members"
                if meta.selected_source == "inmet_historico"
                else "current_operating_catalog",
            )
        methods = (
            {"all_variables": "single_requested_point"}
            if nasa
            else {
                "precip_acum_mm": "mean_of_complete_station_monthly_totals",
                "temperature": "mean_of_valid_daily_station_records",
            }
        )
        details.setdefault("spatial_aggregation", methods)
    if meta.snapshot:
        details["deterministic"] = {
            "snapshot": meta.snapshot,
            "year_selection_only": not station,
            "freezes_source_revision": False,
            "truncates_observations_at_snapshot": False,
        }


_clima = ClimaDataset()
register(_clima)


@overload
async def clima(
    uf: str | None = None,
    ano: int | None = None,
    *,
    estacao: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "diario",
    return_meta: Literal[False] = False,
    fonte: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame: ...


@overload
async def clima(
    uf: str | None = None,
    ano: int | None = None,
    *,
    estacao: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "diario",
    return_meta: Literal[True],
    fonte: str | None = None,
    as_polars: bool = False,
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def clima(
    uf: str | None = None,
    ano: int | None = None,
    *,
    estacao: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "diario",
    return_meta: bool = False,
    fonte: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    return await _clima.fetch(
        uf,
        ano,
        estacao=estacao,
        inicio=inicio,
        fim=fim,
        agregacao=agregacao,
        return_meta=return_meta,
        fonte=fonte,
        as_polars=as_polars,
    )
