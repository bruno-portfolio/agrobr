# ZARC (Agricultural Climate Risk Zoning — Zoneamento Agricola de Risco Climatico)

## About

**ZARC** is the official system of MAPA (Ministry of Agriculture) and Embrapa
that defines recommended planting windows by municipality, crop, soil type
and cultivar cycle. Published as an Ordinance (Portaria) in the Official Gazette of the Union,
ZARC is a requirement for access to subsidized rural credit (Proagro, PSR).

Data published as CSV on the [dados.agricultura.gov.br](https://dados.agricultura.gov.br) portal
(CKAN), CC-BY license, weekly update.

## Available data

- **Risk Table:** planting windows (36 ten-day periods) by municipality/crop/soil/cycle
- **Crops:** 40+ crops (soybean, corn, wheat, coffee, sugarcane, beans, rice, etc.)
- **Crop years:** 2016/2017 to current + perennial (coffee, sugarcane, banana, etc.)
- **Soils:** 3 classic types (sandy/medium/clayey) + 6 AW levels (available water)
- **Coverage:** all Brazilian municipalities (~5,600)

## Returned fields

| Field | Type | Description |
|-------|------|-----------|
| cultura | string | Canonical crop name (e.g. "soja", "milho_1", "trigo") |
| safra | string | "2025/2026" or "perene" |
| geocodigo | string | IBGE code of the municipality (7 digits) |
| uf | string | State abbreviation |
| municipio | string | Municipality name |
| solo_codigo | int | Soil type (1-3 classic, 11-16 AW) |
| ciclo_codigo | int | Cultivar cycle (20, 21, 22, 24) |
| clima | string | Climate restriction (e.g. "Sem restricao") |
| manejo | string | Specific management (e.g. "Sem restricao", "Irrigado") |
| portaria | string | MAPA ordinance number |
| dec1-dec36 | int | Risk per ten-day period (0=not recommended, 20/30/40=% risk) |

## Ten-day periods (decendios)

Each month is divided into 3 ten-day periods:

- dec1-dec3: January
- dec4-dec6: February
- ...
- dec34-dec36: December

Values: 0 (not recommended), 20 (high risk), 30 (medium risk), 40 (low risk).

## Notes

- **Large CSV:** files of ~224MB per crop year. A session cache avoids re-downloading within the same Python session
- **CKAN discovery:** URLs change with each publication; the client performs discovery via the CKAN API
- **User-Agent:** the portal requires browser-like headers (returns 403 with a bot UA)
- **Encoding:** UTF-8 with BOM, separator `;`
- **Yield:** field almost always 0 in the current dataset (reserved for a future ZarcPRO), intentionally dropped

## License

Brazilian federal government public data (CC-BY). Free use with citation of the source.

## Links

- [CKAN portal](https://dados.agricultura.gov.br/dataset/tabua-de-risco-zoneamento-agricola-de-risco-climatico)
- [ZARC - MAPA](https://www.gov.br/agricultura/pt-br/assuntos/riscos-seguro/programa-nacional-de-zoneamento-agricola-de-risco-climatico)
