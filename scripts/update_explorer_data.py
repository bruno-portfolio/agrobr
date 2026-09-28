from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import re
from collections import defaultdict
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog

from agrobr import ibge, mapbiomas
from agrobr.normalize import regions
from agrobr.utils.atomic import atomic_output

ROOT = Path(__file__).resolve().parent.parent
DATA_PATTERN = re.compile(
    r'(<script type="application/json" id="agroExplorerData">)(.*?)(</script>)', re.S
)
YEARS = list(range(2018, 2025))
CROPS = ("soja", "milho", "cafe")
COVERAGE_GROUPS = {
    "native": [3, 4, 5, 6, 11, 12, 49, 50],
    "agriculture": [20, 35, 39, 40, 41, 46, 47, 48, 62],
    "pasture": [15],
}
MONTHS = (
    "janeiro",
    "fevereiro",
    "março",
    "abril",
    "maio",
    "junho",
    "julho",
    "agosto",
    "setembro",
    "outubro",
    "novembro",
    "dezembro",
)
CALLS: dict[str, Callable[..., Awaitable[Any]]] = {
    "ibge.pam": ibge.pam,
    "ibge.lspa": ibge.lspa,
    "mapbiomas.cobertura": mapbiomas.cobertura,
}


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False
    )


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


async def collect_call(
    name: str, params: dict[str, Any], cache: Path, resume: bool
) -> dict[str, Any]:
    request: dict[str, Any] = {"call": name, "params": params | {"return_meta": True}}
    target = cache / (fingerprint(request)[:20] + ".json")
    if resume and target.exists():
        result: dict[str, Any] = json.loads(target.read_text(encoding="utf-8"))
        if (
            result["request"] != request
            or fingerprint(result["records"]) != result["records_sha256"]
        ):
            raise ValueError(f"Captura inválida: {target}")
        return result
    frame, meta = await CALLS[name](**request["params"])
    records = json.loads(frame.to_json(orient="records", force_ascii=False, double_precision=15))
    result = {
        "request": request,
        "records": records,
        "meta": meta.to_dict(),
        "records_sha256": fingerprint(records),
    }
    target.write_text(canonical_json(result), encoding="utf-8")
    print(json.dumps({"captured": name, "params": params, "records": len(records)}), flush=True)
    return result


def sum_complete(values: list[float | None], expected: int) -> float | None:
    if len(values) != expected:
        raise ValueError(f"Esperadas {expected} observações; recebidas {len(values)}")
    if any(value is None for value in values):
        return None
    numbers = [float(value) for value in values if value is not None]
    if any(not math.isfinite(value) or value < 0 for value in numbers):
        raise ValueError("Métrica não finita ou negativa")
    return sum(numbers)


def lspa_metrics(records: list[dict[str, Any]], crop: str) -> dict[str, list[float | None]]:
    components = {
        "soja": {"soja"},
        "milho": {"milho_1", "milho_2"},
        "cafe": {"cafe_arabica", "cafe_canephora"},
    }[crop]
    observations: dict[tuple[str, int], list[float | None]] = defaultdict(list)
    identities: set[tuple[str, str, int]] = set()
    for row in records:
        if row["produto"] not in components:
            raise ValueError(f"Componente LSPA inesperado: {row['produto']}")
        period = f"{row['ano']}{row['mes']:02d}"
        variable = row["variavel_cod"]
        identity = (period, row["produto"], variable)
        if identity in identities:
            raise ValueError(f"Observação LSPA duplicada: {identity}")
        identities.add(identity)
        if variable not in (35, 216):
            continue
        expected_unit = "Toneladas" if variable == 35 else "Hectares"
        if row["unidade"] != expected_unit:
            raise ValueError(f"Unidade inesperada: {row['unidade']}")
        observations[period, variable].append(row["valor"])
    result = {}
    count = len(components)
    for period in sorted({period for period, _ in observations}):
        production = sum_complete(observations[period, 35], count)
        area = sum_complete(observations[period, 216], count)
        productivity = round(production * 1000 / area) if production is not None and area else None
        result[period] = [production, area, productivity]
    return result


def coverage_metrics(
    records: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, list[float]]], dict[str, dict[str, dict[str, float]]]]:
    groups: dict[int, int] = {}
    for index, codes in enumerate(COVERAGE_GROUPS.values()):
        for code in codes:
            if code in groups:
                raise ValueError(f"Classe em dois grupos: {code}")
            groups[code] = index
    coverage: dict[str, dict[str, list[float]]] = {}
    classes: dict[str, dict[str, dict[str, float]]] = {}
    seen: set[tuple[int, str, str, int]] = set()
    for row in records:
        if row["ano"] not in YEARS:
            continue
        identity = (row["ano"], row["estado"], row["bioma"], row["classe_id"])
        if identity in seen:
            raise ValueError(f"Observação MapBiomas duplicada: {identity}")
        seen.add(identity)
        area = float(row["area_ha"])
        if not math.isfinite(area) or area < 0:
            raise ValueError("Área MapBiomas inválida")
        if row["classe_id"] in (1, 10, 14, 18, 19, 22, 26, 36):
            raise ValueError("A captura inclui classes agregadas; risco de dupla contagem")
        year = str(row["ano"])
        for uf in (row["estado"], "BR"):
            values = coverage.setdefault(year, {}).setdefault(uf, [0.0] * 4)
            values[groups.get(row["classe_id"], 3)] += area
            leaf = classes.setdefault(year, {}).setdefault(uf, {})
            class_code = str(row["classe_id"])
            leaf[class_code] = leaf.get(class_code, 0.0) + area
    return coverage, classes


def verify_totals(series: dict[str, dict[str, list[float | None]]], states: set[str]) -> int:
    count = 0
    for period, records in series.items():
        if set(records) != states | {"BR"}:
            raise ValueError(f"Cobertura territorial incompleta: {period}")
        for column in (0, 1):
            national = records["BR"][column]
            values = [records[uf][column] for uf in states]
            if national is None or any(value is None for value in values):
                continue
            total = sum(value for value in values if value is not None)
            if abs(total - national) > 1:
                raise ValueError(f"Total nacional divergente: {period}, coluna {column}")
        count += len(records)
    return count


def compact_provenance(snapshot: dict[str, Any]) -> dict[str, Any]:
    meta = snapshot["meta"]
    return (
        dict(snapshot["request"])
        | {
            key: meta.get(key)
            for key in (
                "source",
                "source_url",
                "fetched_at",
                "selected_source",
                "schema_version",
                "parser_version",
            )
        }
        | {"records_sha256": snapshot["records_sha256"]}
    )


async def collect_all(data: dict[str, Any], cache: Path, resume: bool) -> list[dict[str, Any]]:
    semaphore = asyncio.Semaphore(3)

    async def limited(name: str, params: dict[str, Any]) -> dict[str, Any]:
        async with semaphore:
            return await collect_call(name, params, cache, resume)

    requests: list[tuple[str, dict[str, Any]]] = [("mapbiomas.cobertura", {"colecao": 10})]
    requests += [
        ("ibge.pam", {"produto": crop, "ano": YEARS, "nivel": level})
        for crop in CROPS
        for level in ("uf", "brasil")
    ]
    requests += [
        (
            "ibge.lspa",
            {
                "produto": crop,
                "ano": year,
                "mes": 12 if year == 2025 else None,
                "uf": None if uf == "BR" else uf,
            },
        )
        for crop in CROPS
        for year in (2025, 2026)
        for uf in ["BR", *data["states"]]
    ]
    return await asyncio.gather(*(limited(name, params) for name, params in requests))


def build_data(original: dict[str, Any], captures: list[dict[str, Any]]) -> dict[str, Any]:
    data = dict(original)
    data["crops"] = {crop: {} for crop in CROPS}
    data["estimates"] = {crop: {} for crop in CROPS}
    data["sources"] = {}
    for snapshot in captures:
        name = snapshot["request"]["call"]
        params = snapshot["request"]["params"]
        records = snapshot["records"]
        if name == "mapbiomas.cobertura":
            data["coverage"], data["coverageClasses"] = coverage_metrics(records)
            data["sources"]["mapbiomas"] = snapshot["meta"]
        elif name == "ibge.pam":
            crop = params["produto"]
            data["sources"][f"pam_{crop}_{params['nivel']}"] = snapshot["meta"]
            for row in records:
                if row["unidade_producao"] != "ton" or row["unidade_rendimento"] != "kg/ha":
                    raise ValueError("Unidade PAM inesperada")
                uf = (
                    "BR"
                    if params["nivel"] == "brasil"
                    else regions.normalizar_uf(row["localidade"])
                )
                series = data["crops"][crop].setdefault(str(row["ano"]), {})
                if uf in series:
                    raise ValueError("Observação PAM duplicada")
                series[uf] = [
                    row.get(column) for column in ("producao", "area_colhida", "rendimento")
                ]
        else:
            crop = params["produto"]
            uf = params["uf"] or "BR"
            expected_code = 1 if uf == "BR" else int(data["states"][uf]["code"])
            if any(
                row["localidade_cod"] != expected_code or row["ano"] != params["ano"]
                for row in records
            ):
                raise ValueError("Território ou ano LSPA divergente da chamada")
            for period, values in lspa_metrics(records, crop).items():
                data["estimates"][crop].setdefault(period, {})[uf] = values
            if uf == "BR":
                data["sources"][f"lspa_{crop}_{params['ano']}"] = snapshot["meta"]
    count = sum(
        verify_totals(series, set(data["states"]))
        for series in [*data["crops"].values(), *data["estimates"].values()]
    )
    if any(set(series) != {str(year) for year in YEARS} for series in data["crops"].values()):
        raise ValueError("Anos PAM incompletos")
    if set(data["coverage"]) != {str(year) for year in YEARS}:
        raise ValueError("Anos MapBiomas incompletos")
    for year, records in data["coverage"].items():
        if set(records) != set(data["states"]) | {"BR"}:
            raise ValueError(f"Cobertura MapBiomas incompleta: {year}")
        for uf, values in records.items():
            if abs(sum(values) - sum(data["coverageClasses"][year][uf].values())) > 0.01:
                raise ValueError("Grupos MapBiomas alteram a área total")
        for column, national in enumerate(records["BR"]):
            if abs(sum(records[uf][column] for uf in data["states"]) - national) > 0.01:
                raise ValueError("Total nacional MapBiomas divergente")
    periods = sorted(data["estimates"]["soja"])
    if any(sorted(data["estimates"][crop]) != periods for crop in CROPS):
        raise ValueError("Períodos diferentes entre culturas")
    previous_periods = {item["id"]: item for item in original["estimatePeriods"]}
    data["estimatePeriods"] = [
        previous_periods.get(
            period, {"id": period, "literals": [f"{MONTHS[int(period[4:]) - 1]} {period[:4]}"]}
        )
        for period in periods
    ]
    data["coverageGroups"] = COVERAGE_GROUPS
    data["years"] = YEARS
    data["version"] = 3
    data["audit"] = {
        "verified_at": datetime.now(UTC).isoformat(),
        "generator": "scripts/update_explorer_data.py",
        "production_records": count,
        "coverage_records": sum(len(records) for records in data["coverage"].values()),
        "calls": [compact_provenance(snapshot) for snapshot in captures],
    }
    return data


def comparison_report(original: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
    differences: list[dict[str, Any]] = []
    exact_changes = 0
    for section in ("crops", "estimates", "coverage"):
        for group, periods in data[section].items():
            for period, values in periods.items():
                previous = original.get(section, {}).get(group, {}).get(period)
                territories = values.items() if isinstance(values, dict) else [(None, values)]
                for uf, metrics in territories:
                    old_metrics = previous.get(uf) if uf is not None and previous else previous
                    for column, value in enumerate(metrics):
                        old_value = old_metrics[column] if old_metrics is not None else None
                        if value == old_value:
                            continue
                        exact_changes += 1
                        tolerance = 0.01 if section == "coverage" else (0.5 if column == 2 else 1)
                        if (
                            value is not None
                            and old_value is not None
                            and abs(value - old_value) <= tolerance
                        ):
                            continue
                        path = [section, group, period]
                        if uf is not None:
                            path.append(uf)
                        differences.append(
                            {"path": [*path, column], "previous": old_value, "verified": value}
                        )
    checked = skipped = 0
    for series in [*data["crops"].values(), *data["estimates"].values()]:
        for records in series.values():
            for column in (0, 1):
                if any(values[column] is None for values in records.values()):
                    skipped += 1
                else:
                    checked += 1
    return {
        "checked_at": data["audit"]["verified_at"],
        "calls": len(data["audit"]["calls"]),
        "production_records": data["audit"]["production_records"],
        "coverage_records": data["audit"]["coverage_records"],
        "production_national_sums_checked": checked,
        "production_national_sums_skipped_null": skipped,
        "coverage_group_conservation_checks": data["audit"]["coverage_records"],
        "coverage_national_sums_checked": len(data["coverage"]) * 4,
        "previous_values_equal": not differences,
        "scalar_changes_before_tolerance": exact_changes,
        "material_scalar_changes": len(differences),
        "new_null_values": sum(
            item["verified"] is None and item["previous"] is not None for item in differences
        ),
        "differences": differences,
    }


def explorer_options(cache_name: str, *, include_year: bool = False) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        dest="input_path",
        help="Arquivo JSON ou HTML que contém agroExplorerData",
    )
    parser.add_argument(
        "--output", type=Path, help="JSON ou HTML de saída; padrão: atualizar a entrada"
    )
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "reports" / cache_name)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--check", action="store_true", help="Validar e gerar relatório sem alterar a entrada"
    )
    if include_year:
        parser.add_argument(
            "--year", type=int, default=datetime.now(UTC).year, help="Ano de término da safra atual"
        )
    return parser.parse_args()


def read_explorer_input(target: Path) -> tuple[str, dict[str, Any]]:
    html = target.read_text(encoding="utf-8")
    if target.suffix == ".json":
        data = json.loads(html)
        if not isinstance(data, dict):
            raise ValueError(f"Esperado um objeto JSON em {target}")
        return html, data
    matches = list(DATA_PATTERN.finditer(html))
    if len(matches) != 1:
        raise ValueError(f"Esperado um bloco agroExplorerData em {target}")
    data = json.loads(matches[0][2])
    if not isinstance(data, dict):
        raise ValueError(f"agroExplorerData deve ser um objeto JSON em {target}")
    return html, data


def write_explorer_output(target: Path, html: str, data: dict[str, Any]) -> None:
    updated = (
        canonical_json(data) + "\n"
        if target.suffix == ".json"
        else DATA_PATTERN.sub(
            lambda part: part[1] + canonical_json(data).replace("</", "<\\/") + part[3],
            html,
            count=1,
        )
    )
    with atomic_output(target) as temporary:
        temporary.write_text(updated, encoding="utf-8")


async def main() -> None:
    options = explorer_options("explorer-source-audit")
    html, original = read_explorer_input(options.input_path)
    target = options.output or options.input_path
    cache = options.cache_dir / "calls"
    cache.mkdir(parents=True, exist_ok=True)
    captures = await collect_all(original, cache, options.resume)
    data = build_data(original, captures)
    report = comparison_report(original, data)
    (cache.parent / "report.json").write_text(canonical_json(report), encoding="utf-8")
    (cache.parent / "verified-data.json").write_text(canonical_json(data), encoding="utf-8")
    if not options.check:
        write_explorer_output(target, html, data)
    print(
        json.dumps({key: value for key, value in report.items() if key != "differences"}),
        flush=True,
    )


if __name__ == "__main__":
    structlog.configure(processors=[structlog.processors.JSONRenderer()])
    asyncio.run(main())
