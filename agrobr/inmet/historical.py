from __future__ import annotations

import time
from datetime import date
from typing import Any

import pandas as pd

from agrobr.exceptions import ParseError

from . import client, parser


def _combine(frames: list[pd.DataFrame]) -> tuple[pd.DataFrame, int]:
    if not frames:
        return parser.empty_historico(), 0
    data = pd.concat(frames, ignore_index=True)
    unique = data.drop_duplicates()
    duplicates = len(data) - len(unique)
    if unique.duplicated(["estacao", "data", "hora_utc"]).any():
        raise ParseError(
            source="inmet",
            parser_version=2,
            reason="Observações conflitantes para a mesma estação/data/hora",
        )
    return unique.sort_values(["estacao", "data", "hora_utc"]).reset_index(drop=True), duplicates


def _coverage(
    data: pd.DataFrame, inicio: date, fim: date, station_codes: set[str]
) -> list[dict[str, Any]]:
    coverage = []
    for code in sorted(station_codes):
        station = data.loc[data["estacao"].eq(code)]
        coverage.append(
            {
                "codigo": code,
                "first_observation": None
                if station.empty
                else station["data"].min().date().isoformat(),
                "last_observation": None
                if station.empty
                else station["data"].max().date().isoformat(),
                "observed_hours": len(station),
                "requested_calendar_hours": ((fim - inicio).days + 1) * 24,
                "valid_hours": {
                    column: int(station[column].notna().sum())
                    for column in parser.COLUNAS_NUMERICAS
                },
            }
        )
    return coverage


async def collect(
    inicio: date, fim: date, *, codigo: str | None = None, uf: str | None = None
) -> tuple[pd.DataFrame, dict[str, Any], int, int]:
    frames: list[pd.DataFrame] = []
    resources: list[dict[str, Any]] = []
    stations: list[dict[str, Any]] = []
    missing_years: list[int] = []
    fetch_seconds = 0.0
    parse_seconds = 0.0
    for year in range(inicio.year, fim.year + 1):
        started = time.monotonic()
        archive = await client.fetch_historico_arquivo(year)
        fetch_seconds += time.monotonic() - started
        started = time.monotonic()
        members = client.historico_membros(archive, codigo=codigo, uf=uf)
        resources.append(
            {
                "url": archive.url,
                "sha256": archive.sha256,
                "bytes": len(archive.content),
                "members": [member[0] for member in members],
                "fetched_at": archive.fetched_at.isoformat(),
                "from_cache": archive.from_cache,
            }
        )
        if not members:
            missing_years.append(year)
        try:
            for name, member_code, member_uf, raw in members:
                metadata = parser.parse_historico_metadata(raw)
                if metadata.uf != member_uf or metadata.codigo != member_code:
                    raise ParseError(
                        source="inmet",
                        parser_version=2,
                        reason=f"Identidade do CSV difere do nome do membro {name}",
                    )
                frame = parser.parse_historico_csv(raw, member_code)
                if not frame.empty and not frame["data"].dt.year.eq(year).all():
                    raise ParseError(
                        source="inmet",
                        parser_version=2,
                        reason=f"Observação fora do ano do ZIP: {name}",
                    )
                stations.append(
                    {
                        **metadata.model_dump(),
                        "ano": year,
                        "member": name,
                        "layout_fingerprint": parser.historico_layout_fingerprint(raw),
                    }
                )
                mask = frame["data"].between(pd.Timestamp(inicio), pd.Timestamp(fim))
                frames.append(frame.loc[mask])
        except ParseError:
            client.invalidate_historico(year)
            raise
        parse_seconds += time.monotonic() - started
    try:
        data, duplicates = _combine(frames)
    except ParseError:
        for year in range(inicio.year, fim.year + 1):
            client.invalidate_historico(year)
        raise
    coverage = _coverage(data, inicio, fim, {station["codigo"] for station in stations})
    warnings = [
        "Dados históricos publicados pelo INMET podem ser revistos e não representam uma série consistida."
    ]
    if missing_years:
        warnings.append(
            "Ausência de membro no ZIP nos anos indicados; não comprova ausência de observações nem data de ativação/desativação."
        )
    if data.empty or any(
        item["observed_hours"] < item["requested_calendar_hours"] for item in coverage
    ):
        warnings.append(
            "Cobertura inferior ao calendário solicitado; lacunas não foram preenchidas com zero e podem incluir períodos anteriores à implantação ou ainda não publicados."
        )
    if duplicates:
        warnings.append(
            "Observações idênticas repetidas entre membros foram contadas apenas uma vez."
        )
    details = {
        "access": "inmet_historico",
        "time_basis": "UTC",
        "requested_period": {"inicio": inicio.isoformat(), "fim": fim.isoformat()},
        "resources": resources,
        "stations": stations,
        "coverage": {
            "missing_station_years" if codigo else "missing_uf_years": missing_years,
            "stations": coverage,
            "identical_duplicates_removed": duplicates,
            "observed_hours": len(data),
            "complete_calendar": bool(coverage)
            and not missing_years
            and all(
                item["observed_hours"] == item["requested_calendar_hours"] for item in coverage
            ),
            "complete_measurements": bool(coverage)
            and all(
                item["observed_hours"] > 0
                and all(count == item["observed_hours"] for count in item["valid_hours"].values())
                for item in coverage
            ),
        },
        "warnings": warnings,
    }
    return data, details, int(fetch_seconds * 1000), int(parse_seconds * 1000)


def aggregate(data: pd.DataFrame, details: dict[str, Any], agregacao: str) -> pd.DataFrame:
    details["temporal_aggregation"] = agregacao
    details["spatial_aggregation"] = {}
    if agregacao == "horario":
        return data
    daily = parser.agregar_diario(data) if not data.empty else parser.empty_historico("diario")
    if agregacao == "diario":
        return daily
    details["spatial_aggregation"] = spatial_aggregation()
    return parser.agregar_mensal_uf(daily) if not daily.empty else parser.empty_historico("mensal")


def spatial_aggregation() -> dict[str, str]:
    return {
        "precip_acum_mm": "mean_of_complete_station_monthly_totals",
        "temp_media": "mean_of_available_station_daily_means",
        "temp_max_media": "mean_of_available_station_daily_maxima",
        "temp_min_media": "mean_of_available_station_daily_minima",
        "num_estacoes": "distinct_stations_with_rows_in_month",
        "estacoes_chuva": "stations_with_valid_rain_on_every_day_of_month",
        "estacoes_chuva_parciais": "stations_with_valid_rain_on_some_days_excluded",
    }
