from __future__ import annotations

import asyncio
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agrobr import conab
from scripts.update_explorer_data import (
    ROOT,
    canonical_json,
    explorer_options,
    fingerprint,
    read_explorer_input,
    write_explorer_output,
)

CROPS = ("soja", "milho", "cafe", "arroz", "trigo")
ANNUAL_CROPS = {"cafe", "trigo"}
CACHE = ROOT / "reports/explorer-conab-audit"


async def collect_history(
    crop: str,
    resume: bool,
    *,
    cache: Path | None = None,
    year: int = 2026,
) -> dict[str, Any]:
    cache = CACHE if cache is None else cache
    target = cache / f"history-{crop}-{year}.json"
    request = {
        "call": "conab.serie_historica",
        "params": {"produto": crop, "inicio": 2017, "fim": year, "return_meta": True},
    }
    if resume and target.exists():
        capture: dict[str, Any] = json.loads(target.read_text(encoding="utf-8"))
        if (
            capture["request"] != request
            or fingerprint(capture["records"]) != capture["records_sha256"]
        ):
            raise ValueError(f"Captura inválida: {target}")
        return capture
    frame, meta = await conab.serie_historica(crop, inicio=2017, fim=year, return_meta=True)
    records = json.loads(frame.to_json(orient="records", force_ascii=False, double_precision=15))
    capture = {
        "request": request,
        "records": records,
        "records_sha256": fingerprint(records),
        "meta": meta.to_dict(),
    }
    target.write_text(canonical_json(capture), encoding="utf-8")
    return capture


async def collect_current(
    crop: str,
    resume: bool,
    *,
    cache: Path | None = None,
    year: int = 2026,
) -> dict[str, Any]:
    cache = CACHE if cache is None else cache
    target = cache / f"explorer-current-{crop}-{year}.json"
    request = {
        "call": "conab.safras",
        "params": {"produto": crop, "safra": f"{year - 1}/{year % 100:02d}", "return_meta": True},
    }
    if resume and target.exists():
        capture: dict[str, Any] = json.loads(target.read_text(encoding="utf-8"))
        if (
            capture["request"] != request
            or fingerprint(capture["records"]) != capture["records_sha256"]
        ):
            raise ValueError(f"Captura inválida: {target}")
        return capture
    frame, meta = await conab.safras(crop, safra=f"{year - 1}/{year % 100:02d}", return_meta=True)
    records = json.loads(
        frame.to_json(orient="records", force_ascii=False, double_precision=15, date_format="iso")
    )
    capture = {
        "request": request,
        "records": records,
        "records_sha256": fingerprint(records),
        "meta": meta.to_dict(),
    }
    target.write_text(canonical_json(capture), encoding="utf-8")
    return capture


def panel_year(crop: str, season: str) -> int:
    first = int(season[:4])
    return first if crop in ANNUAL_CROPS else first + 1


def metric(value: float | None, multiplier: int = 1) -> float | None:
    if value is None:
        return None
    if not math.isfinite(value) or value < 0:
        raise ValueError("Métrica inválida")
    return value * multiplier


def national_metrics(
    records: dict[str, list[float | None]], available: list[str]
) -> list[float | None]:
    totals = []
    for column in (0, 1):
        values = [records[uf][column] for uf in available]
        totals.append(
            math.fsum(value for value in values if value is not None)
            if values and all(value is not None for value in values)
            else None
        )
    production, area = totals
    productivity = production * 1000 / area if production is not None and area else None
    return [production, area, productivity]


def build_history(
    captures: list[dict[str, Any]], states: set[str], *, year: int = 2026
) -> dict[str, Any]:
    history_years = range(2018, year)
    current_year = str(year)
    history: dict[str, Any] = {str(year): {} for year in history_years}
    metadata: dict[str, Any] = {str(year): {} for year in history_years}
    sources: list[dict[str, Any]] = []
    for capture in captures:
        crop = capture["request"]["params"]["produto"]
        source_id = f"history-{crop}-{capture['records_sha256'][:12]}"
        sources.append(
            {
                "id": source_id,
                **{key: capture[key] for key in ("request", "meta", "records_sha256")},
            }
        )
        area_column = "area_em_producao_mil_ha" if crop == "cafe" else "area_plantada_mil_ha"
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in capture["records"]:
            if row["produto"] != crop:
                raise ValueError(f"Produto divergente: {row['produto']} / {crop}")
            year = panel_year(crop, row["safra"])
            if year in history_years:
                grouped.setdefault(year, []).append(row)
        if set(grouped) != set(history_years):
            raise ValueError(f"Anos incompletos: {crop}")
        for year, rows in grouped.items():
            records: dict[str, list[float | None]] = {
                uf: [None, None, None] for uf in sorted(states)
            }
            available = []
            seasons = set()
            for row in rows:
                uf = row["uf"]
                if uf not in states or uf in available:
                    raise ValueError(f"UF desconhecida ou duplicada: {crop}/{year}/{uf}")
                available.append(uf)
                seasons.add(row["safra"])
                records[uf] = [
                    metric(row["producao_mil_ton"], 1000),
                    metric(row[area_column], 1000),
                    metric(row["produtividade_kg_ha"]),
                ]
            if len(seasons) != 1:
                raise ValueError("Safras diferentes para o mesmo ano")
            records["BR"] = national_metrics(records, available)
            history[str(year)][crop] = records
            metadata[str(year)][crop] = {
                "safraOriginal": next(iter(seasons)),
                "yearRule": "first_year_annual_header"
                if crop in ANNUAL_CROPS
                else "harvest_end_year",
                "collection": "conab.serie_historica",
                "areaLabel": "Área em produção" if crop == "cafe" else "Área plantada",
                "coverage": {
                    "availableUFs": sorted(available),
                    "missingUFs": sorted(states - set(available)),
                    "nationalMethod": "sum_available_ufs",
                },
                "sourceAuditId": source_id,
            }
    return {
        "history": history,
        "historyMeta": metadata,
        "current": {current_year: {}},
        "currentMeta": {current_year: {}},
        "sourceCalls": sources,
    }


def add_current(
    production: dict[str, Any],
    captures: list[dict[str, Any]],
    states: set[str],
    *,
    year: int = 2026,
) -> None:
    for capture in captures:
        crop = capture["request"]["params"]["produto"]
        source_id = f"current-{crop}-{capture['records_sha256'][:12]}"
        production["sourceCalls"].append(
            {
                "id": source_id,
                **{key: capture[key] for key in ("request", "meta", "records_sha256")},
            }
        )
        values: dict[str, list[float | None]] = {uf: [None, None, None] for uf in sorted(states)}
        available = []
        releases = set()
        for row in capture["records"]:
            uf = row["uf"]
            if (
                uf not in states
                or uf in available
                or row["safra"] != f"{year - 1}/{year % 100:02d}"
                or row["produto"] != crop
            ):
                raise ValueError(f"Observação atual inválida: {crop}/{uf}")
            available.append(uf)
            releases.add(row["levantamento"])
            values[uf] = [
                metric(row["producao"], 1000),
                metric(row["area_plantada"], 1000),
                metric(row["produtividade"]),
            ]
        if not available or len(releases) != 1:
            raise ValueError(f"Levantamento vazio ou misturado: {crop}")
        values["BR"] = national_metrics(values, available)
        production["current"][str(year)][crop] = values
        production["currentMeta"][str(year)][crop] = {
            "safraOriginal": f"{year - 1}/{year % 100:02d}",
            "yearRule": f"annual_{year}_column" if crop == "trigo" else "harvest_end_year",
            "collection": "conab.safras",
            "areaLabel": "Área plantada",
            "levantamento": next(iter(releases)),
            "coverage": {
                "availableUFs": sorted(available),
                "missingUFs": sorted(states - set(available)),
                "nationalMethod": "sum_available_ufs",
            },
            "sourceAuditId": source_id,
        }


def build_data(
    original: dict[str, Any],
    captures: list[dict[str, Any]],
    current: list[dict[str, Any]],
    *,
    year: int = 2026,
) -> dict[str, Any]:
    data = {
        key: value
        for key, value in original.items()
        if key not in {"crops", "estimates", "estimatePeriods", "sources", "audit", "production"}
    }
    data["production"] = build_history(captures, set(original["states"]), year=year)
    add_current(data["production"], current, set(original["states"]), year=year)
    map_calls = [
        call
        for call in original.get("audit", {}).get("calls", [])
        if call.get("call") == "mapbiomas.cobertura"
    ]
    data["audit"] = {
        "verified_at": datetime.now(UTC).isoformat(),
        "generator": "scripts/update_conab_explorer_data.py",
        "calls": map_calls,
        "productionCalls": len(captures) + len(current),
        "coverage_records": sum(len(records) for records in data.get("coverage", {}).values()),
    }
    data["version"] = 4
    return data


async def main() -> None:
    options = explorer_options("explorer-conab-audit", include_year=True)
    year = options.year
    if not 2019 <= year <= datetime.now(UTC).year:
        raise ValueError("Ano deve estar entre 2019 e o ano corrente")
    html, original = read_explorer_input(options.input_path)
    target = options.output or options.input_path
    cache = options.cache_dir
    cache.mkdir(parents=True, exist_ok=True)
    captures = [
        await collect_history(crop, options.resume, cache=cache, year=year) for crop in CROPS
    ]
    current = [
        await collect_current(crop, options.resume, cache=cache, year=year)
        for crop in CROPS
        if crop != "cafe"
    ]
    data = build_data(original, captures, current, year=year)
    (cache / "explorer-data.json").write_text(canonical_json(data), encoding="utf-8")
    report = {
        "generated_at": data["audit"]["verified_at"],
        "source_calls": len(captures) + len(current),
        "history_years": list(data["production"]["history"]),
        "crops": list(CROPS),
        "history_territory_records": sum(
            len(records)
            for crops in data["production"]["history"].values()
            for records in crops.values()
        ),
        "current_territory_records": sum(
            len(records) for records in data["production"]["current"][str(year)].values()
        ),
        "current_unavailable_crops": [
            crop for crop in CROPS if crop not in data["production"]["current"][str(year)]
        ],
        "national_method": "sum_available_ufs",
        "independent_national_checks": 0,
        "coverage_unchanged": all(
            data.get(key) == original.get(key)
            for key in ("coverage", "coverageClasses", "coverageGroups")
        ),
    }
    (cache / "explorer-report.json").write_text(canonical_json(report), encoding="utf-8")
    if not options.check:
        write_explorer_output(target, html, data)
    print(canonical_json(report), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
