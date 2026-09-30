from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd
from pydantic import ValidationError

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.normalize import regions

from . import _geometry, _json, models

PARSER_VERSION = 3

TEXTO = pd.Series([""]).dtype


def _diagnostic(values: dict[str, Any], category: str, row: int) -> None:
    entry = values.setdefault(category, {"count": 0, "examples": [], "examples_omitted": 0})
    entry["count"] += 1
    if len(entry["examples"]) < constants.FUNAI_MAX_DIAGNOSTIC_EXAMPLES:
        entry["examples"].append({"row_index": row})
    else:
        entry["examples_omitted"] += 1


def _statistics(
    properties: models.Properties, statistics: dict[str, Any], diagnostics: dict[str, Any], row: int
) -> None:
    for name, value in properties.model_dump().items():
        item = statistics[name]
        item["null_count"] += value is None
        item["empty_count"] += isinstance(value, str) and value == ""
        item["whitespace_count"] += isinstance(value, str) and value != "" and not value.strip()
    uf = properties.uf_sigla
    if uf is not None and any(
        token.strip().upper() not in regions.UFS_VALIDAS for token in uf.split(",")
    ):
        _diagnostic(diagnostics, "unknown_uf_token", row)
    area = properties.superficie_perimetro_ha
    if area is not None and area < 0:
        _diagnostic(diagnostics, "negative_area", row)


def _hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(_json.canonical(value), ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _envelope(
    content: bytes, include_geometry: bool
) -> tuple[dict[str, Any], models.PageEnvelope, str | None]:
    raw = _json.decode(content)
    envelope = models.PageEnvelope.model_validate(raw)
    size = len(envelope.features)
    if envelope.numberReturned is not None and envelope.numberReturned != size:
        raise ValueError("numberReturned diverge das ocorrências")
    totals = [
        value for value in (envelope.numberMatched, envelope.totalFeatures) if value is not None
    ]
    if len(set(totals)) > 1 or any(value < size for value in totals):
        raise ValueError("Contagens de população divergentes")
    links = [link.href for link in envelope.links if link.rel == "next"]
    if envelope.next is not None:
        links.append(envelope.next)
    if len(set(links)) > 1:
        raise ValueError("Links de continuação contraditórios")
    if (
        include_geometry
        and (envelope.crs is not None or size)
        and (envelope.crs is None or envelope.crs.properties.name not in constants.FUNAI_CRS_NAMES)
    ):
        raise ValueError("CRS declarado incompatível com EPSG4326 solicitado")
    return raw, envelope, links[0] if links else None


def _parse(content: bytes, include_geometry: bool) -> models.ParsedPage:
    if type(include_geometry) is not bool:
        raise ValueError("include_geometry deve ser bool")
    raw, envelope, next_link = _envelope(content, include_geometry)
    records: list[models.Feature] = []
    signatures, identifiers = [], []
    geometries: list[dict[str, Any] | None] | None = [] if include_geometry else None
    statistics = {
        name: {"null_count": 0, "empty_count": 0, "whitespace_count": 0}
        for name in constants.FUNAI_PROPERTIES
    }
    diagnostics: dict[str, Any] = {}
    feature_keys: set[str] = set()
    for index, feature in enumerate(envelope.features):
        feature_keys.update(feature)
        properties = models.Properties.model_validate(feature.get("properties"))
        if "geometry" not in feature:
            raise ValueError("Membro geometry ausente")
        geometry = _geometry.validate(feature["geometry"])
        normalized = {**feature, "properties": properties, "geometry": geometry}
        if "bbox" in feature:
            normalized["bbox"] = _geometry.bbox_values(feature["bbox"])
        record = models.Feature.model_validate(normalized)
        signature = {
            "properties": {
                **feature["properties"],
                **{name: getattr(properties, name) for name in constants.FUNAI_INTEGER_BITS},
            }
        }
        if include_geometry:
            signature["geometry"] = feature["geometry"]
        signatures.append(_hash(signature))
        identifiers.append(_hash(feature["id"]))
        _statistics(properties, statistics, diagnostics, index)
        if geometries is not None:
            geometries.append(geometry)
            if geometry is None:
                _diagnostic(diagnostics, "null_geometry", index)
            elif not geometry["coordinates"]:
                _diagnostic(diagnostics, "empty_geometry", index)
        elif geometry is not None:
            _diagnostic(diagnostics, "unexpected_geometry", index)
            record.geometry = None
        records.append(record)
    layout = {
        "property_names": list(constants.FUNAI_PROPERTIES),
        "envelope_keys": sorted(raw),
        "feature_keys": sorted(feature_keys),
        "include_geometry": include_geometry,
    }
    fingerprint = {
        "algorithm": "sha256",
        "version": 1,
        "parser_version": PARSER_VERSION,
        **layout,
        "sha256": _hash(layout),
    }
    return models.ParsedPage(
        records=records,
        source_rows=len(records),
        reported_count=envelope.numberMatched,
        returned_count=envelope.numberReturned,
        signatures=signatures,
        identifier_signatures=identifiers,
        geometries=geometries,
        layout_fingerprint=fingerprint,
        diagnostics=diagnostics,
        statistics=statistics,
        warnings=[
            f"Diagnóstico FUNAI {name}: {item['count']} ocorrência(s)"
            for name, item in diagnostics.items()
        ],
        crs=envelope.crs.model_dump() if envelope.crs else None,
        bbox=_geometry.bbox_values(envelope.bbox),
        next_link=next_link,
        source_timestamp=envelope.timeStamp,
        parser_version=PARSER_VERSION,
    )


def parse_page(content: bytes, *, include_geometry: bool) -> models.ParsedPage:
    try:
        return _parse(content, include_geometry)
    except (ValueError, TypeError, KeyError, ValidationError, OverflowError, RecursionError) as exc:
        raise ParseError(
            source="funai",
            parser_version=PARSER_VERSION,
            reason=f"Página FUNAI inválida: {str(exc)[:1000]}",
        ) from exc


def build_frame(records: list[models.Feature]) -> pd.DataFrame:
    values: dict[str, list[Any]] = {name: [] for name in constants.FUNAI_COLUMNS}
    for record in records:
        for name, value in record.properties.model_dump().items():
            values[constants.FUNAI_RENAME_MAP.get(name, name)].append(value)
        values["feature_id"].append(record.id)
    integer_names = {
        constants.FUNAI_RENAME_MAP.get(name, name) for name in constants.FUNAI_INTEGER_BITS
    }
    return pd.DataFrame(
        {
            name: pd.Series(
                items,
                dtype="Int64"
                if name in integer_names
                else "float64"
                if name == "area_ha"
                else TEXTO,
            )
            for name, items in values.items()
        }
    )
