from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import structlog
from pydantic import ValidationError

from agrobr.exceptions import ParseError
from agrobr.nasa_power import models

logger = structlog.get_logger()


PARSER_VERSION = 2


def parse_daily(
    data: dict[str, Any],
    lat: float,
    lon: float,
    uf: str = "",
    parameters: list[str] | None = None,
) -> pd.DataFrame:

    if not data:
        raise ParseError(
            source="nasa_power",
            parser_version=PARSER_VERSION,
            reason="Resposta NASA POWER vazia",
        )

    response = validate_response(data)

    selected = parameters if parameters is not None else list(response.properties.parameter)

    catalog = {item.codigo: item for item in models.CATALOGO}

    rows: dict[str, dict[str, Any]] = {}

    for code in selected:
        if code not in catalog:
            continue

        if code not in response.properties.parameter:
            raise ParseError(
                source="nasa_power",
                reason=f"Parâmetro solicitado ausente: {code}",
                parser_version=PARSER_VERSION,
            )

        definition = catalog[code]

        metadata = response.parameters.get(code)

        if metadata is None:
            raise ParseError(
                source="nasa_power",
                reason=f"Unidade ausente para {code}",
                parser_version=PARSER_VERSION,
            )
        if metadata.units != definition.unidade:
            raise ParseError(
                source="nasa_power",
                reason=f"Unidade inesperada para {code}: {metadata.units}",
                parser_version=PARSER_VERSION,
            )

        for date_str, value in response.properties.parameter[code].items():
            rows.setdefault(date_str, {})[definition.coluna] = (
                None if value == response.header.fill_value or value == models.SENTINEL else value
            )

    if not rows:
        raise ParseError(
            source="nasa_power",
            parser_version=PARSER_VERSION,
            reason="Nenhuma data encontrada nos dados",
        )

    records: list[dict[str, Any]] = []

    for date_str, values in sorted(rows.items()):
        try:
            if len(date_str) != 8 or not date_str.isascii() or not date_str.isdigit():
                raise ValueError("data inválida")

            dt = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))

        except (ValueError, IndexError) as exc:
            raise ParseError(
                source="nasa_power",
                reason=f"Data inválida: {date_str}",
                parser_version=PARSER_VERSION,
            ) from exc

        record = {"data": dt, "lat": lat, "lon": lon, "uf": uf}

        record.update(values)

        records.append(record)

    df = pd.DataFrame(records)

    for col in (item.coluna for item in models.CATALOGO):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="raise").astype("float64")

    df["data"] = pd.to_datetime(df["data"])

    df = df.sort_values("data").reset_index(drop=True)

    logger.debug(
        "nasa_power_parse_ok",
        records=len(df),
        params=selected,
    )

    return df


def agregar_mensal(df: pd.DataFrame) -> pd.DataFrame:

    if df.empty:
        return df

    df = df.copy()

    df["mes"] = df["data"].dt.to_period("M")

    agg: dict[str, pd.NamedAgg] = {}

    for item in models.CATALOGO:
        if item.coluna in df.columns:
            agg[item.coluna_mensal] = pd.NamedAgg(
                column=item.coluna,
                aggfunc=(lambda values: values.sum(min_count=1))
                if item.codigo == "PRECTOTCORR"
                else "mean",
            )

    if not agg:
        return df

    group_cols = ["uf"] if "uf" in df.columns and df["uf"].ne("").any() else []

    result = df.groupby(["mes"] + group_cols).agg(**agg).reset_index()

    result["mes"] = result["mes"].dt.to_timestamp()

    if "lat" in df.columns and "lon" in df.columns:
        coords = df.groupby(["mes"] + group_cols)[["lat", "lon"]].first().reset_index()

        coords["mes"] = coords["mes"].dt.to_timestamp()

        result = result.merge(coords, on=["mes"] + group_cols, how="left")

    medidas = [item.coluna for item in models.CATALOGO if item.coluna in df.columns]
    cobertura = (
        df[df[medidas].notna().any(axis=1)]
        .groupby(["mes"] + group_cols)["data"]
        .agg(dias="nunique", data_inicio="min", data_fim="max")
        .reset_index()
    )
    cobertura["mes"] = cobertura["mes"].dt.to_timestamp()
    result = result.merge(cobertura, on=["mes"] + group_cols, how="left")
    result["dias"] = result["dias"].fillna(0).astype("int64")

    return result


def validate_response(data: dict[str, Any]) -> models.DailyResponse:

    try:
        response = models.DailyResponse.model_validate(data)

    except ValidationError as exc:
        raise ParseError(
            source="nasa_power",
            reason="Resposta diária NASA POWER inválida",
            parser_version=PARSER_VERSION,
        ) from exc
    if response.header.time_standard != "LST":
        raise ParseError(
            source="nasa_power",
            reason="Base de tempo inesperada: esperado LST",
            parser_version=PARSER_VERSION,
        )
    return response


def validate_period(data: dict[str, Any], start: date, end: date) -> None:
    response = validate_response(data)
    for values in response.properties.parameter.values():
        for key in values:
            try:
                if len(key) != 8 or not key.isascii() or not key.isdigit():
                    raise ValueError("data inválida")
                day = date(int(key[:4]), int(key[4:6]), int(key[6:]))
            except ValueError as exc:
                raise ParseError(
                    source="nasa_power",
                    reason=f"Data inválida: {key}",
                    parser_version=PARSER_VERSION,
                ) from exc
            if not start <= day <= end:
                raise ParseError(
                    source="nasa_power",
                    reason=f"Data fora do intervalo solicitado: {key}",
                    parser_version=PARSER_VERSION,
                )
