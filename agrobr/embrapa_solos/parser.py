from __future__ import annotations

import hashlib
import json
import re
from typing import Any

import pandas as pd
from pydantic import ValidationError

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.normalize.regions import UFS_VALIDAS

from . import _geometry, _json, models

PARSER_VERSION = 3

_DUPLA_CODIFICACAO = re.compile("[ÃÂ][\x80-\xbf]")


def _diagnostic(values: dict[str, Any], category: str, row: int) -> None:
    item = values.setdefault(category, {"count": 0, "examples": [], "examples_omitted": 0})
    item["count"] += 1
    if len(item["examples"]) < constants.EMBRAPA_SOLOS_MAX_DIAGNOSTIC_EXAMPLES:
        item["examples"].append({"row_index": row})
    else:
        item["examples_omitted"] += 1


def normalize_uf(value: str | None) -> str | None:
    normalized = value.strip().upper() if value is not None else None
    return normalized if normalized in UFS_VALIDAS else None


def _statistics(
    properties: models.Properties, statistics: dict[str, Any], diagnostics: dict[str, Any], row: int
) -> None:
    values = properties.model_dump()
    for field, value in values.items():
        counts = statistics.setdefault(
            field, {"null_count": 0, "empty_count": 0, "whitespace_count": 0}
        )
        counts["null_count"] += value is None
        counts["empty_count"] += value == "" if isinstance(value, str) else False
        counts["whitespace_count"] += (
            value != "" and value.strip() == "" if isinstance(value, str) else False
        )
    if "uf" in values and normalize_uf(values["uf"]) is None:
        _diagnostic(diagnostics, "unknown_uf", row)
    for name, bound, category in (
        ("gcs_latitu", 90, "latitude_outside_range"),
        ("gcs_longit", 180, "longitude_outside_range"),
    ):
        if values.get(name) is not None and abs(values[name]) > bound:
            _diagnostic(diagnostics, category, row)
    if values.get("area_km2") is not None and values["area_km2"] < 0:
        _diagnostic(diagnostics, "negative_area", row)


def _parse(content: bytes, product: models.Product, include_geometry: bool) -> models.ParsedPage:
    fields = models.layout_properties(product)
    if type(include_geometry) is not bool:
        raise ValueError("Modo geométrico deve ser bool")
    raw = _json.decode(content)
    envelope = models.PageEnvelope.model_validate(raw)
    next_links = [link.href for link in envelope.links if link.rel == "next"]
    if envelope.next is not None:
        next_links.append(envelope.next)
    if len(set(next_links)) > 1:
        raise ValueError("Links de continuação contraditórios")
    size = len(envelope.features)
    if envelope.numberReturned is not None and envelope.numberReturned != size:
        raise ValueError("Contagem retornada diverge da página")
    if envelope.numberMatched is not None and envelope.numberMatched < size:
        raise ValueError("Página excede contagem publicada")
    if (
        envelope.totalFeatures is not None
        and envelope.numberMatched is not None
        and envelope.totalFeatures != envelope.numberMatched
    ):
        raise ValueError("Contagens publicadas divergentes")
    if (
        include_geometry
        and (envelope.crs is not None or size)
        and (
            envelope.crs is None
            or envelope.crs.properties.name not in constants.EMBRAPA_SOLOS_CRS_NAMES
        )
    ):
        raise ValueError("CRS observado incompatível com EPSG4326")
    property_class = models.PerfisProperties if product == "perfis" else models.MapaProperties
    records = []
    signatures = []
    geometries: list[dict[str, Any] | None] | None = [] if include_geometry else None
    diagnostics: dict[str, Any] = {}
    statistics: dict[str, Any] = {
        field: {"null_count": 0, "empty_count": 0, "whitespace_count": 0} for field in fields
    }
    feature_keys: set[str] = set()
    warnings = []
    for index, feature in enumerate(envelope.features):
        feature_keys.update(feature)
        properties = property_class.model_validate(feature.get("properties"))
        if "geometry" not in feature:
            raise ValueError("Membro geometry ausente")
        geometry = _geometry.validate(feature["geometry"], product)
        normalized = {**feature, "properties": properties, "geometry": geometry}
        if "bbox" in feature:
            normalized["bbox"] = _geometry.bbox_values(feature["bbox"])
        record = models.Feature.model_validate(normalized)
        signature = {"id": feature["id"], "properties": feature["properties"]}
        if include_geometry:
            signature["geometry"] = feature["geometry"]
        signatures.append(
            hashlib.sha256(
                json.dumps(
                    _json.canonical(signature), separators=(",", ":"), ensure_ascii=False
                ).encode()
            ).hexdigest()
        )
        _statistics(properties, statistics, diagnostics, index)
        if geometries is not None:
            geometries.append(geometry)
        elif geometry is not None:
            _diagnostic(diagnostics, "unexpected_geometry", index)
            record.geometry = None
        records.append(record)
    if envelope.model_extra or feature_keys - {"type", "id", "properties", "geometry", "bbox"}:
        warnings.append("Membros adicionais do envelope preservados no recurso bruto")
    warnings += [
        f"Diagnóstico Embrapa {name}: {value['count']} ocorrência(s)"
        for name, value in diagnostics.items()
    ]
    layout = {
        "product": product,
        "property_names": list(fields),
        "envelope_keys": sorted(raw),
        "feature_keys": sorted(feature_keys),
        "include_geometry": include_geometry,
    }
    fingerprint = {
        "algorithm": "sha256",
        "version": 1,
        "parser_version": PARSER_VERSION,
        **layout,
        "sha256": hashlib.sha256(
            json.dumps(layout, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    return models.ParsedPage(
        records=records,
        source_rows=size,
        reported_count=envelope.numberMatched,
        returned_count=envelope.numberReturned,
        signatures=signatures,
        geometries=geometries,
        layout_fingerprint=fingerprint,
        diagnostics=diagnostics,
        statistics=statistics,
        warnings=warnings,
        crs=envelope.crs.model_dump() if envelope.crs else None,
        bbox=_geometry.bbox_values(envelope.bbox),
        next_link=next_links[0] if next_links else None,
        parser_version=PARSER_VERSION,
    )


def parse_page(
    content: bytes, *, product: models.Product, include_geometry: bool
) -> models.ParsedPage:
    try:
        return _parse(content, product, include_geometry)
    except (ValueError, TypeError, KeyError, ValidationError, OverflowError, RecursionError) as exc:
        raise ParseError(
            source="embrapa_solos",
            parser_version=PARSER_VERSION,
            reason=f"Página JSON Embrapa Solos inválida: {str(exc)[:1000]}",
        ) from exc


def _desfazer_dupla_codificacao(texto: str) -> str:
    if not _DUPLA_CODIFICACAO.search(texto):
        return texto
    try:
        return texto.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return texto


def reparar_texto(frame: pd.DataFrame) -> tuple[dict[str, int], dict[str, int]]:
    reparos: dict[str, int] = {}
    sem_reparo: dict[str, int] = {}
    for coluna in [
        nome for nome in frame.columns if pd.api.types.is_string_dtype(frame[nome].dtype)
    ]:
        original = frame[coluna]
        reparado = original.map(_desfazer_dupla_codificacao, na_action="ignore").astype(
            original.dtype
        )
        mudou = (reparado != original).fillna(False)
        assinatura = original.str.contains(_DUPLA_CODIFICACAO.pattern, regex=True).fillna(False)
        if trocas := int(mudou.sum()):
            frame[coluna] = reparado
            reparos[coluna] = trocas
        if restantes := int((assinatura & ~mudou).sum()):
            sem_reparo[coluna] = restantes
    return reparos, sem_reparo


def build_frame(records: list[models.Feature], *, product: models.Product) -> pd.DataFrame:
    fields = models.layout_properties(product)
    rename = (
        constants.EMBRAPA_SOLOS_PERFIS_RENAME_MAP
        if product == "perfis"
        else constants.EMBRAPA_SOLOS_MAPA_RENAME_MAP
    )
    columns = (
        constants.EMBRAPA_SOLOS_PERFIS_COLUMNS
        if product == "perfis"
        else constants.EMBRAPA_SOLOS_MAPA_COLUMNS
    )
    values: dict[str, list[Any]] = {name: [] for name in columns}
    for record in records:
        source = record.properties.model_dump()
        for field in fields:
            values[rename.get(field, field)].append(source[field])
        if product == "perfis":
            values["uf"].append(normalize_uf(source["uf"]))
        values["feature_id"].append(record.id)
    text_dtype = pd.Series([""]).dtype
    dtypes = {
        rename.get(field, field): "Int64"
        if field in constants.EMBRAPA_SOLOS_INTEGER_BITS
        else "float64"
        if field in constants.EMBRAPA_SOLOS_FLOAT_PROPERTIES
        else text_dtype
        for field in fields
    }
    frame = pd.DataFrame(
        {
            name: pd.Series(value, dtype=dtypes.get(name, text_dtype))
            for name, value in values.items()
        }
    )
    if product == "perfis":
        for column, pattern in (
            ("ano", r"[0-9]{4}"),
            ("data_colet", r"[0-9]{4}-[0-9]{2}-[0-9]{2}"),
        ):
            text = frame[column].mask(frame[column] == "NULL")
            try:
                if not text.dropna().str.fullmatch(pattern).all():
                    raise ValueError(f"{column} contém texto fora do formato publicado")
                frame[column] = (
                    pd.to_numeric(text, errors="raise").astype("Int64")
                    if column == "ano"
                    else pd.to_datetime(text, format="%Y-%m-%d", errors="raise").astype(
                        "datetime64[ns]"
                    )
                )
            except (TypeError, ValueError, OverflowError) as exc:
                raise ParseError(
                    source="embrapa_solos", parser_version=PARSER_VERSION, reason=str(exc)
                ) from exc
    return frame
