from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Sequence
from typing import Any

import pandas as pd
from pydantic import ValidationError

from agrobr import constants
from agrobr.desmatamento import json_numbers, models
from agrobr.exceptions import ParseError

PARSER_VERSION = 2


def _hash(value: Any) -> str:
    content = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(content).hexdigest()


def _counts(envelope: models.PageEnvelope) -> int | None:
    totals = [
        value for value in (envelope.numberMatched, envelope.totalFeatures) if type(value) is int
    ]
    if len(set(totals)) > 1:
        raise ValueError("Declared totals disagree")
    received = len(envelope.features)
    if envelope.numberReturned is not None and envelope.numberReturned != received:
        raise ValueError("Declared returned count disagrees with features")
    if totals and totals[0] < received:
        raise ValueError("Declared total smaller than returned features")
    return totals[0] if totals else None


def _bbox(value: list[float] | None) -> None:
    if value is not None and len(value) not in (4, 6):
        raise ValueError("Invalid bbox dimension")


def _next_link(envelope: models.PageEnvelope) -> str | None:
    candidates = [link.href for link in envelope.links if link.rel == "next"]
    if envelope.next is not None:
        candidates.append(envelope.next)
    if len(set(candidates)) > 1:
        raise ValueError("Ambiguous next links")
    return candidates[0] if candidates else None


def _signature(raw: dict[str, Any], include_geometry: bool) -> str:
    selected = {"id": raw["id"], "properties": raw["properties"]}
    scene_id = raw["properties"].get("scene_id")
    if type(scene_id) is json_numbers.RawNumber:
        selected["scene_id_lexeme"] = str(scene_id)
    if include_geometry:
        selected["geometry"] = raw["geometry"]
    return _hash(json_numbers.canonical(selected))


def _page_records(
    envelope: models.PageEnvelope,
    property_model: type[models.Properties],
    include_geometry: bool,
    geometry_column: str,
) -> tuple[list[models.Feature], list[str], list[dict[str, Any] | None] | None, dict[str, Any]]:
    records = []
    signatures = []
    geometries: list[dict[str, Any] | None] | None = [] if include_geometry else None
    nulls: Counter[str] = Counter()
    empty: Counter[str] = Counter()
    types: dict[str, set[str]] = {name: set() for name in property_model.model_fields}
    geometry_count = 0
    geometry_bytes = 0
    unknown_uf_count = 0
    geometry_names: Counter[str] = Counter()
    for raw in envelope.features:
        signature = _signature(raw, include_geometry)
        external = models.ResponseFeature.model_validate(raw)
        if external.geometry_name is not None:
            if external.geometry_name != geometry_column:
                raise ValueError("Unexpected geometry column name")
            geometry_names[external.geometry_name] += 1
        _bbox(external.bbox)
        properties = property_model.model_validate(external.properties)
        uf_original = external.properties.get("state", external.properties.get("uf"))
        if uf_original is not None and _normalized_uf(uf_original) is None:
            unknown_uf_count += 1
        feature = models.Feature(type=external.type, id=external.id, properties=properties)
        signatures.append(signature)
        records.append(feature)
        if external.geometry is not None:
            geometry_count += 1
            geometry = external.geometry.model_dump()
            geometry_bytes += len(json.dumps(geometry, separators=(",", ":")).encode())
            if geometries is not None:
                geometries.append(geometry)
        elif geometries is not None:
            geometries.append(None)
        for name, value in external.properties.items():
            nulls[name] += value is None
            empty[name] += type(value) is str and value == ""
            kind = "number" if type(value) is json_numbers.RawNumber else type(value).__name__
            types[name].add(kind)
    return (
        records,
        signatures,
        geometries,
        {
            "null_counts": dict(nulls),
            "empty_string_counts": dict(empty),
            "observed_types": {name: sorted(values) for name, values in types.items()},
            "geometry_received_count": geometry_count,
            "geometry_serialized_bytes_estimate": geometry_bytes,
            "geometry_bytes_basis": "compact serialization after numeric validation; not raw body bytes",
            "geometry_names": dict(geometry_names),
            "unknown_uf_count": unknown_uf_count,
        },
    )


def parse_page(
    content: bytes,
    *,
    product: str,
    biome: str,
    include_geometry: bool = False,
) -> models.ParsedPage:
    try:
        if type(include_geometry) is not bool:
            raise ValueError("include_geometry must be boolean")
        if len(content) > constants.DESMATAMENTO_MAX_BODY_BYTES:
            raise ValueError("Body exceeds parser limit")
        property_model = models.PROPERTY_MODELS[(product, biome)]
        envelope = models.PageEnvelope.model_validate(json_numbers.decode(content))
        reported = _counts(envelope)
        _bbox(envelope.bbox)
        if include_geometry:
            if envelope.crs is None and envelope.features:
                raise ValueError("Geo response is missing its requested CRS")
            if envelope.crs is not None and envelope.crs.properties.name not in (
                "EPSG:4326",
                "urn:ogc:def:crs:EPSG::4326",
                "http://www.opengis.net/def/crs/EPSG/0/4326",
            ):
                raise ValueError("Geo response does not declare requested EPSG:4326")
        records, signatures, geometries, details = _page_records(
            envelope,
            property_model,
            include_geometry,
            models.layout_geometry_column(product, biome),
        )
        if (
            include_geometry
            and product == "PRODES"
            and biome == "Pampa"
            and geometries is not None
            and any(value is None for value in geometries)
        ):
            raise ValueError("Pampa geometry is not nullable in the geo layout")
        fields = models.layout_properties(product, biome)
        union = set().union(
            *(
                model.model_fields
                for (kind, _), model in models.PROPERTY_MODELS.items()
                if kind == product
            )
        )
        details.update(
            source_rows=len(records),
            fields_not_in_layout=sorted(union - set(fields)),
            declared_counts={
                name: getattr(envelope, name)
                for name in ("numberMatched", "totalFeatures", "numberReturned")
                if name in envelope.model_fields_set
            },
            envelope_fields=sorted(envelope.model_fields_set),
            feature_id_policy="required nonblank JSON string; narrower than general GeoJSON; no uniqueness",
            geometry_omitted_from_tabular=not include_geometry,
            source_timestamp=envelope.timeStamp,
        )
        fingerprint = {
            "algorithm": "sha256",
            "version": 1,
            "parser_version": PARSER_VERSION,
            "product": product,
            "biome": biome,
            "properties": fields,
        }
        fingerprint["sha256"] = _hash(fingerprint)
        warnings = []
        if not include_geometry and details["geometry_received_count"]:
            warnings.append("Geometria recebida fora da projeção tabular foi validada e omitida")
        if details["unknown_uf_count"]:
            warnings.append(
                "UF publicada não reconhecida; texto original preservado e UF normalizada nula"
            )
        return models.ParsedPage(
            records=records,
            source_rows=len(records),
            reported_count=reported,
            returned_count=envelope.numberReturned,
            signatures=signatures,
            geometries=geometries,
            layout_fingerprint=fingerprint,
            warnings=warnings,
            details=details,
            crs=envelope.crs.model_dump() if envelope.crs else None,
            bbox=envelope.bbox,
            next_link=_next_link(envelope),
        )
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError, ValidationError) as exc:
        raise ParseError(
            source="desmatamento",
            parser_version=PARSER_VERSION,
            reason=f"Página {product}/{biome} inválida: {exc}",
        ) from exc


def _normalized_uf(value: str | None) -> str | None:
    if value is None:
        return None
    candidate = models.estado_para_uf(value).upper()
    return candidate if candidate in models.UF_ESTADO.values() else None


def _record_row(feature: models.Feature, product: str, biome: str) -> dict[str, Any]:
    values = feature.properties.model_dump()
    if product == "PRODES":
        return {
            "ano": values["year"],
            "uf": _normalized_uf(values["state"]),
            "classe": values["main_class"],
            "area_km2": values["area_km"],
            "satelite": values["satellite"],
            "sensor": values["sensor"],
            "bioma": biome,
            "feature_id": feature.id,
            "uuid": values["uuid"],
            "fid": values["fid"],
            "estado_original": values["state"],
            "path_row": values["path_row"],
            "class_name": values["class_name"],
            "def_cloud": values["def_cloud"],
            "julian_day": values["julian_day"],
            "image_date": values["image_date"],
            "scene_id": values["scene_id"],
            "publish_year": values["publish_year"],
            "source": values["source"],
            "pub_date": values.get("pub_date"),
        }
    return {
        "data": values["view_date"],
        "classe": values["classname"],
        "uf": _normalized_uf(values["uf"]),
        "municipio": values["municipality"],
        "municipio_id": values.get("mun_geocod"),
        "area_km2": values["areamunkm"],
        "satelite": values["satellite"],
        "sensor": values["sensor"],
        "bioma": biome,
        "feature_id": feature.id,
        "gid": values["gid"],
        "uf_original": values["uf"],
        "quadrant": values["quadrant"],
        "path_row": values["path_row"],
        "areauckm": values["areauckm"],
        "uc": values["uc"],
        "publish_month": values["publish_month"],
        "created_date": values.get("created_date"),
        "areatotalkm": values.get("areatotalkm"),
    }


def build_frame(records: Sequence[models.Feature], *, product: str, biome: str) -> pd.DataFrame:
    model = models.PROPERTY_MODELS[(product, biome)]
    if any(type(record.properties) is not model for record in records):
        raise ParseError(
            source="desmatamento",
            parser_version=PARSER_VERSION,
            reason="Layout dos registros difere da consulta",
        )
    columns = (
        constants.DESMATAMENTO_PRODES_COLUMNS
        if product == "PRODES"
        else constants.DESMATAMENTO_DETER_COLUMNS
    )
    rows = [_record_row(record, product, biome) for record in records]
    if any(row.get("ano") is not None and row["ano"] % 1 for row in rows):
        raise ParseError(
            source="desmatamento",
            parser_version=PARSER_VERSION,
            reason="Ano do PRODES não inteiro",
        )
    floats = {"area_km2", "def_cloud", "julian_day", "areauckm", "areatotalkm"}
    dates = {"data", "image_date", "publish_year", "publish_month", "created_date"}
    return pd.DataFrame(
        {
            name: pd.Series(
                [row[name] for row in rows],
                dtype="float64"
                if name in floats
                else "datetime64[ns]"
                if name in dates
                else "Int64"
                if name in {"fid", "ano"}
                else pd.StringDtype(storage="python"),
            )
            for name in columns
        }
    )
