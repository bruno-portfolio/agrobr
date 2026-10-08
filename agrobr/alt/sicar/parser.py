from __future__ import annotations

import warnings
from datetime import datetime
from typing import Any, cast

import pandas as pd
import pydantic

from agrobr import _log
from agrobr.constants import SICAR_MAX_VERSOES_DESCARTADAS
from agrobr.exceptions import ParseError
from agrobr.normalize import regions
from agrobr.utils.geo import check_geopandas, parse_geojson_base

from . import models
from .models import COLUNAS_IMOVEIS, COLUNAS_IMOVEIS_GEO, MAX_FEATURES_GEO, RENAME_MAP

logger = _log.get_logger(__name__)

PARSER_VERSION = models.PARSER_VERSION


def _datetime_series(
    values: list[datetime | None],
    index: pd.Index,
    column: str,
    *,
    require_timezone: bool = False,
) -> pd.Series:
    awareness = {value.utcoffset() is not None for value in values if value is not None}
    if len(awareness) > 1:
        raise ParseError(
            source="sicar",
            parser_version=PARSER_VERSION,
            reason=f"Datas com e sem fuso misturadas em {column}",
        )
    if require_timezone and False in awareness:
        raise ParseError(
            source="sicar", parser_version=PARSER_VERSION, reason=f"Data JSON sem fuso em {column}"
        )
    utc = require_timezone or True in awareness
    try:
        converted = pd.to_datetime(pd.Series(values, index=index, dtype=object), utc=utc)
        converted = converted.astype("datetime64[ns, UTC]" if utc else "datetime64[ns]")
    except (ValueError, OverflowError) as exc:
        raise ParseError(
            source="sicar",
            parser_version=PARSER_VERSION,
            reason=f"Data invalida em {column}: {exc}",
        ) from exc
    return pd.Series(converted, index=index)


def _normalize_columns(
    df: pd.DataFrame,
    output_cols: list[str],
    *,
    require_timezone: bool = False,
) -> pd.DataFrame:
    raw_columns = [column for column in models.SicarImovel.model_fields if column in df.columns]
    records = df[raw_columns].astype(object)
    records[records.isna()] = None
    validated = [models.SicarImovel.model_validate(row) for row in records.to_dict("records")]
    normalized = pd.DataFrame(
        [row.model_dump() for row in validated],
        columns=list(models.SicarImovel.model_fields),
        index=df.index,
    ).rename(columns=RENAME_MAP)
    for column, attribute in (
        ("data_criacao", "dat_criacao"),
        ("data_atualizacao", "data_atualizacao"),
    ):
        normalized[column] = _datetime_series(
            [getattr(row, attribute) for row in validated],
            df.index,
            column,
            require_timezone=require_timezone,
        )
    normalized["cod_municipio_ibge"] = normalized["cod_municipio_ibge"].astype("Int64")
    normalized["cod_municipio"] = regions.cod_municipio(normalized["cod_municipio_ibge"])
    for column in ("area_ha", "modulos_fiscais"):
        normalized[column] = normalized[column].astype("float64")
    for column in ("cod_imovel", "status", "condicao", "uf", "municipio", "tipo"):
        normalized[column] = normalized[column].astype(pd.Series([""]).dtype)
    result = df.copy()
    for output_column in normalized:
        result[output_column] = normalized[output_column]
    return result[output_cols].copy().reset_index(drop=True)


_REQUIRED_COLS_RAW = {"cod_imovel", "status_imovel", "dat_criacao", "area", "uf"}


def _group_discarded_versions(
    group: pd.DataFrame, feature_ids: list[str]
) -> tuple[int, list[tuple[int, str]]]:
    field = next(
        (name for name in ("data_atualizacao", "data_criacao") if group[name].notna().all()),
        "feature_id",
    )

    def order(index: int) -> tuple[int, int, str]:
        timestamp = (
            0 if field == "feature_id" else int(cast(pd.Timestamp, group.at[index, field]).value)
        )
        identifier = feature_ids[index]
        return timestamp, int(identifier.rsplit(".", 1)[1]), identifier

    indices = [int(index) for index in group.index]
    winner = max(indices, key=order)
    discarded = []
    for index in sorted(indices, key=lambda row: order(row)[1:]):
        if index == winner:
            continue
        criterion = (
            field
            if field != "feature_id" and group.at[index, field] != group.at[winner, field]
            else "feature_id"
        )
        discarded.append((index, criterion))
    return winner, discarded


def _discarded_version_details(
    frame: pd.DataFrame, index: int, identifier: str, winner_id: str, criterion: str
) -> dict[str, Any]:
    item = {
        "cod_imovel": frame.at[index, "cod_imovel"],
        "feature_id": identifier,
        "feature_id_mantida": winner_id,
        "criterio": criterion,
    }
    for name in ("data_atualizacao", "data_criacao"):
        value = cast(pd.Timestamp, frame.at[index, name])
        item[name] = None if pd.isna(value) else value.isoformat().replace("+00:00", "Z")
    return item


def _collapse_versions(
    frame: pd.DataFrame,
    feature_ids: list[str],
    details: dict[str, Any],
    validation_warnings: list[str] | None,
) -> pd.DataFrame:
    repeated = frame[frame["cod_imovel"].duplicated(keep=False)]
    criteria = {"data_atualizacao": 0, "data_criacao": 0, "feature_id": 0}
    discarded_indices: list[int] = []
    discarded_versions: list[dict[str, Any]] = []
    groups = repeated.groupby("cod_imovel", sort=True)
    for _code, group in groups:
        winner, discarded = _group_discarded_versions(group, feature_ids)
        for index, criterion in discarded:
            discarded_indices.append(index)
            criteria[criterion] += 1
            if len(discarded_versions) < SICAR_MAX_VERSOES_DESCARTADAS:
                discarded_versions.append(
                    _discarded_version_details(
                        frame, index, feature_ids[index], feature_ids[winner], criterion
                    )
                )
    details.update(
        features_unicas=len(frame),
        codigos_colapsados=len(groups),
        versoes_descartadas_total=len(discarded_indices),
        versoes_descartadas=discarded_versions,
        versoes_descartadas_truncadas=len(discarded_indices) > len(discarded_versions),
        criterios=criteria,
    )
    if discarded_indices:
        message = (
            f"{len(groups)} codigos de imovel com mais de uma versao publicada; "
            "mantida a mais recente por data de atualizacao quando presente em todo o grupo, "
            "senao por data de criacao; ausencia de datas ou empate resolvido pelo maior "
            "sufixo numerico do id da feature"
        )
        logger.warning(
            "sicar_versions_collapsed",
            codes=len(groups),
            discarded=len(discarded_indices),
            criteria=criteria,
        )
        if validation_warnings is not None:
            validation_warnings.append(message)
    return frame.drop(index=discarded_indices).reset_index(drop=True)


def parse_imoveis_json(
    pages: list[bytes],
    *,
    source_details: dict[str, Any] | None = None,
    validation_warnings: list[str] | None = None,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    feature_ids: list[str] = []
    seen: set[str] = set()
    announced: int | None = None if pages else 0
    for page in pages:
        try:
            collection = models.SicarFeatureCollection.model_validate_json(page)
        except pydantic.ValidationError as exc:
            raise ParseError(
                source="sicar",
                parser_version=PARSER_VERSION,
                reason=f"FeatureCollection SICAR invalida: {exc}",
            ) from exc
        models.validate_feature_ids(collection.features, seen)
        records.extend(feature.properties for feature in collection.features)
        feature_ids.extend(feature.id for feature in collection.features)
        if isinstance(collection.numberMatched, int):
            announced = collection.numberMatched
    frame = pd.DataFrame(records) if records else pd.DataFrame(columns=COLUNAS_IMOVEIS)
    result = _normalize_columns(frame, COLUNAS_IMOVEIS, require_timezone=True)
    details = source_details if source_details is not None else {}
    details.setdefault("anunciados", announced)
    result = _collapse_versions(result, feature_ids, details, validation_warnings)
    logger.info("sicar_json_parse_ok", records=len(result))
    return result


def parse_geo_pages(
    pages: list[bytes], *, seen: set[str], max_features: int | None = None
) -> tuple[Any, list[str], int | None]:
    """Valida as paginas GeoJSON e devolve todas as ocorrencias, sem colapsar versoes.

    Returns:
        GeoDataFrame normalizado, os ids das features na mesma ordem das linhas e o
        ``numberMatched`` da última página que o publica (o total da consulta na fonte). ``seen``
        acumula os ids entre chamadas para recusar id repetido como a varredura tabular.
    """
    gpd = check_geopandas()
    per_page_max = max_features if len(pages) == 1 else None
    gdfs = []
    feature_ids: list[str] = []
    anunciado: int | None = None
    for page in pages:
        gdf = parse_geojson_base(
            page,
            gpd,
            source="sicar",
            parser_version=PARSER_VERSION,
            required_cols=_REQUIRED_COLS_RAW,
            max_features=per_page_max,
            output_cols_empty=COLUNAS_IMOVEIS_GEO,
            truncation_event="sicar_geo_truncated",
        )
        try:
            collection = models.SicarFeatureCollection.model_validate_json(page)
        except pydantic.ValidationError as exc:
            raise ParseError(
                source="sicar",
                parser_version=PARSER_VERSION,
                reason=f"FeatureCollection SICAR invalida: {exc}",
            ) from exc
        models.validate_feature_ids(collection.features, seen)
        if isinstance(collection.numberMatched, int):
            anunciado = collection.numberMatched
        declared = collection.crs.properties.get("name") if collection.crs else None
        if collection.features and declared not in models.SICAR_CRS_NAMES:
            raise ParseError(
                source="sicar",
                parser_version=PARSER_VERSION,
                reason=f"CRS declarado {declared} diverge do {models.SICAR_CRS} solicitado",
            )
        if not gdf.empty:
            gdfs.append(gdf)
            feature_ids.extend(feature.id for feature in collection.features)

    if not gdfs:
        empty = gpd.GeoDataFrame(columns=COLUNAS_IMOVEIS_GEO)
        return (
            gpd.GeoDataFrame(
                _normalize_columns(empty, COLUNAS_IMOVEIS_GEO, require_timezone=True),
                geometry="geometry",
                crs=models.SICAR_CRS,
            ),
            feature_ids,
            anunciado,
        )

    gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=models.SICAR_CRS)
    return (
        _normalize_columns(gdf, COLUNAS_IMOVEIS_GEO, require_timezone=True),
        feature_ids,
        anunciado,
    )


def select_versions(
    frame: Any,
    feature_ids: list[str],
    *,
    source_details: dict[str, Any] | None = None,
    validation_warnings: list[str] | None = None,
) -> Any:
    details = source_details if source_details is not None else {}
    return _collapse_versions(frame, feature_ids, details, validation_warnings)


def _marcar_corte(
    recebidas: int,
    total: int | None,
    max_features: int | None,
    details: dict[str, Any],
    validation_warnings: list[str] | None,
) -> None:
    if (
        max_features is None
        or recebidas < max_features
        or (total is not None and total <= max_features)
    ):
        return
    fonte = (
        f"a fonte tem {total} imóveis nesta consulta"
        if total is not None
        else "a fonte não informou o total, e pode haver mais imóveis"
    )
    mensagem = (
        f"sicar: o resultado parou em max_registros={max_features}; {fonte}. "
        "Use max_registros=None ou filtre por município para trazer todos."
    )
    details.update(truncado=True, max_registros=max_features, total_fonte=total)
    if validation_warnings is not None:
        validation_warnings.append(mensagem)
    warnings.warn(mensagem, UserWarning, stacklevel=4)


def parse_imoveis_geojson(
    pages: list[bytes],
    *,
    max_features: int | None = MAX_FEATURES_GEO,
    source_details: dict[str, Any] | None = None,
    validation_warnings: list[str] | None = None,
) -> Any:
    frame, feature_ids, anunciado = parse_geo_pages(pages, seen=set(), max_features=max_features)
    details = source_details if source_details is not None else {}
    _marcar_corte(len(frame), anunciado, max_features, details, validation_warnings)
    gdf = select_versions(
        frame,
        feature_ids,
        source_details=details,
        validation_warnings=validation_warnings,
    )
    logger.info("sicar_geojson_parse_ok", records=len(gdf))
    return gdf


def agregar_resumo(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)

    if total == 0:
        return pd.DataFrame(
            [
                {
                    "total": 0,
                    "ativos": 0,
                    "pendentes": 0,
                    "suspensos": 0,
                    "cancelados": 0,
                    "area_total_ha": 0.0,
                    "area_media_ha": 0.0,
                    "modulos_fiscais_medio": 0.0,
                    "por_tipo_IRU": 0,
                    "por_tipo_AST": 0,
                    "por_tipo_PCT": 0,
                }
            ]
        )

    status_counts = df["status"].value_counts()
    tipo_counts = df["tipo"].value_counts() if "tipo" in df.columns else pd.Series(dtype=int)

    resumo = {
        "total": total,
        "ativos": int(status_counts.get("AT", 0)),
        "pendentes": int(status_counts.get("PE", 0)),
        "suspensos": int(status_counts.get("SU", 0)),
        "cancelados": int(status_counts.get("CA", 0)),
        "area_total_ha": float(df["area_ha"].sum()) if "area_ha" in df.columns else 0.0,
        "area_media_ha": float(df["area_ha"].mean()) if "area_ha" in df.columns else 0.0,
        "modulos_fiscais_medio": (
            float(df["modulos_fiscais"].mean()) if "modulos_fiscais" in df.columns else 0.0
        ),
        "por_tipo_IRU": int(tipo_counts.get("IRU", 0)),
        "por_tipo_AST": int(tipo_counts.get("AST", 0)),
        "por_tipo_PCT": int(tipo_counts.get("PCT", 0)),
    }

    return pd.DataFrame([resumo])
