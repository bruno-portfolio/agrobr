# USDA PSD — International Estimates

United States Department of Agriculture — Production, Supply, Distribution.
International estimates of agricultural production, supply and demand.

## Configuration

Requires a free API key. The FAS OpenData page points to api.data.gov for the key:

1. Register at [api.data.gov/signup](https://api.data.gov/signup/)
2. Set the environment variable:

```bash
export AGROBR_USDA_API_KEY="sua-key-aqui"
```

Or pass it directly:

```python
df = await usda.psd("soja", api_key="sua-key-aqui")
```

The key goes only in the `X-Api-Key` header, never in the URL. Without a key, `usda.psd` raises `SourceUnavailableError`
before any request. A key rejected by the gateway (HTTP 403 `API_KEY_INVALID` or `API_KEY_MISSING`) also raises
`SourceUnavailableError`, with the gateway code.

## API

```python
from agrobr import usda

# PSD data for Brazil for soybeans
df = await usda.psd("soja", country="BR", market_year=2024)

# All countries
df = await usda.psd("soja", country="all", market_year=2024)

# Aggregated world data
df = await usda.psd("milho", country="world")

# Filter by attributes: official PSD name or agrobr label
df = await usda.psd("soja", attributes=["Production", "exportacao"])

# Pivot attributes as columns
df = await usda.psd("soja", pivot=True)
```

Without `market_year`, the query asks for the current calendar year and, when the PSD has published nothing for it
yet, the previous year: from January until the May WASDE, which opens the new marketing year, the current year comes
back empty. `MetaInfo.source_details` records the year used (`market_year`), the years requested
(`market_year_tentados`) and whether the default applied (`market_year_padrao`). An explicit `market_year` never
falls back: a year without publication returns the empty table. The gateway answers HTTP 404 for a year without
data (Brazilian soybeans in 1950, for example): the empty table comes with a warning in `validation_warnings` and a
`UserWarning`, because the same 404 comes out if the API URL changes.

## Columns — `psd`

| Column | Type | Description |
|---|---|---|
| `commodity_code` | str | PSD commodity code (7 digits) |
| `commodity` | str | agrobr name (`soja`, `milho`...); outside the registry, the official catalog name |
| `country_code` | str | PSD country code, which is not ISO (`CH` is China, `E4` the European Union); `00` is the world |
| `country` | str | Official name from the countries catalog; `World` for the world aggregate |
| `market_year` | int | USDA marketing year, literal from the body (`2024` is the 2024/25 season) |
| `attribute` | str | Official attribute name (`Production`, `Exports`...) |
| `attribute_br` | str | agrobr label for the balance-sheet attributes (table below); null for the others |
| `value` | float | Published value, in the `unit` unit |
| `unit` | str | Official catalog unit: `(1000 MT)`, `(1000 HA)`, `(MT/HA)`, `1000 480 lb. Bales`, `(1000 60 KG BAGS)`... |
| `attribute_id` | int | PSD `attributeId` |
| `unit_id` | int | PSD `unitId` |
| `last_update_year` | int | Year of the series' last update (country × marketing year); not the queried edition |
| `last_update_month` | Int64 | Month of the series' last update; null in old series, where the PSD publishes `00` |

## Balance-sheet attributes

| `attribute_br` | ID | Official name |
|---|---:|---|
| `area_colhida` | 4 | Area Harvested |
| `estoque_inicial` | 20 | Beginning Stocks |
| `producao` | 28 | Production |
| `importacao` | 57 | Imports |
| `oferta_total` | 86 | Total Supply |
| `exportacao` | 88 | Exports |
| `consumo_domestico` | 125 | Domestic Consumption |
| `consumo_domestico` (sugar) | 126 | Total Disappearance |
| `consumo_domestico` (cotton) | 142 | Domestic Use |
| `perdas` (cotton) | 150 | Loss |
| `estoque_final` | 176 | Ending Stocks |
| `distribuicao_total` | 178 | Total Distribution |
| `produtividade` | 184 | Yield |

For the 9 products in the commodities table, `estoque_inicial + producao + importacao = oferta_total` and
`exportacao + consumo_domestico + perdas + estoque_final = distribuicao_total = oferta_total`. Other published attributes
(crush, feed use, stocks-to-use etc.) come with the official name in `attribute` and a null `attribute_br`.

## Commodities

| agrobr name | Code | USDA Commodity | Unit |
|---|---|---|---|
| `soja` | 2222000 | Oilseed, Soybean | 1000 t |
| `milho` | 0440000 | Corn | 1000 t |
| `trigo` | 0410000 | Wheat | 1000 t |
| `algodao` | 2631000 | Cotton | 1000 480-lb bales |
| `arroz` | 0422110 | Rice, Milled | 1000 t |
| `cafe` / `coffee` | 0711100 | Coffee, Green | 1000 60-kg bags |
| `acucar` / `sugar` | 0612000 | Sugar, Centrifugal | 1000 t |
| `farelo_soja` / `soybean_meal` | 0813100 | Meal, Soybean | 1000 t |
| `oleo_soja` / `soybean_oil` | 4232000 | Oil, Soybean | 1000 t |

Area in 1000 ha and yield in t/ha (cotton in kg/ha). Any other `commodityCode` from the official catalog is also
accepted. A commodity, country or attribute outside the catalogs raises `InvalidParameterError` before the network; the
gateway would return `[]` without an error.

Livestock also comes from the source, by catalog code: cattle (`0011000`) and swine (`0013000`) herds, in
`(1000 HEAD)`, and beef (`0111000`), pork (`0113000`) and chicken (`0115000` and `0114200`) meat, in
`(1000 MT CWE)`. For herds, `producao` counts heads, not meat, and slaughter (`Total Slaughter`) and losses come with
a null `attribute_br`: the balance identity above does not close through the labels. The `oferta_demanda_global`
dataset accepts only the 9 products in the table.

USDA's cattle herd is not IBGE's PPM (`datasets.pecuaria_municipal`): `Beginning Stocks` is USDA's estimate for the start of the year, and the PPM is the herd surveyed by IBGE per municipality for the reference year. The gap is material: 186.9 million head in the 2025 `Beginning Stocks` against 238.2 million in the 2024 PPM (21.5% lower at USDA). Do not compare or join the 2 series as if they were the same measure.

## Catalogs

Names come from the gateway's official catalogs (`commodityAttributes`, `commodities`, `countries` and
`unitsOfMeasure`), stored in the package under `agrobr/usda/catalogos/`, with the bytes captured on 2026-09-25 and the
SHA in the golden. No extra network call is made. A code the USDA publishes later that is not in the local catalog raises
`ParseError`, never an empty label.

`python -m scripts.reconciliar_usda --output result.json` checks the catalogs and 34 cuts (9 products) live against the
agrobr output and the balance identity. It needs `AGROBR_USDA_API_KEY`; without it, the weekly reconciliation marks USDA
as "não verificado" (not verified).

## MetaInfo

```python
df, meta = await usda.psd("soja", return_meta=True)
print(meta.source)  # "usda"
print(meta.source_url)  # exact gateway URL queried, without the key
print(meta.raw_content_hash, meta.raw_content_size)  # SHA-256 and size of the received body
```

## Source

- API: `https://api.fas.usda.gov/api/psd` (FAS gateway; the old `apps.fas.usda.gov/OpenData/api` returns 500)
- Format: JSON (REST), camelCase with IDs only
- Update: monthly (WASDE); each series keeps the month of its own last update
- History: 1960+
