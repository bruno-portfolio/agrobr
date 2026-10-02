# ANP Diesel — Prices and Volumes

> **License:** Brazilian federal government public data (Decree 8.777/2016).
> Classification: `livre`

National Agency for Petroleum, Natural Gas and Biofuels (Agencia Nacional do
Petroleo, Gas Natural e Biocombustiveis). Diesel resale price and sales volume
data in Brazil. Proxy for mechanized agricultural activity.

The official sales CSV may contain accented headers; state filters recognize the published spelling. Published zeros and negative values are preserved: the September 2026 CSV contains −70 m³ of maritime diesel in Sergipe, December 2025, without an explanation in the CSV. Do not automatically interpret that observation as a valid physical volume or silently replace it with zero.

## Installation

Does not require optional dependencies. Uses httpx + pandas + calamine, with openpyxl as fallback.

## API

```python
from agrobr.alt import anp_diesel

# Diesel S10 prices — municipality level
df = await anp_diesel.precos_diesel(produto="DIESEL S10")

# Diesel prices by state
df = await anp_diesel.precos_diesel(nivel="uf")

# Prices filtered by state and period
df = await anp_diesel.precos_diesel(
    uf="MT",
    inicio="2024-01-01",
    fim="2024-06-30",
)

# Monthly-aggregated prices
df = await anp_diesel.precos_diesel(agregacao="mensal")

# Sales volumes by state
df = await anp_diesel.vendas_diesel()

# Filtered sales volumes
df = await anp_diesel.vendas_diesel(uf="SP", inicio="2024-01-01")

# Synchronous API
from agrobr.sync import alt
df = alt.anp_diesel.precos_diesel(uf="MT")
df = alt.anp_diesel.vendas_diesel()
```

## Parameters — `precos_diesel`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `uf` | str \| None | None | Filter by state (e.g. SP, MT, PR) |
| `municipio` | int \| str \| None | None | Municipality by its 7-digit IBGE code or full name, resolved by `normalize.resolver_municipio` before the request and compared with the spreadsheet name ignoring case, accents and punctuation (`"Sant'Ana do Livramento"` matches `SANTANA DO LIVRAMENTO`); a fragment of a name raises `InvalidParameterError` listing the candidates |
| `produto` | str | "DIESEL S10" | `DIESEL`, `OLEO DIESEL`, `OLEO DIESEL S10`, or `DIESEL S10` |
| `inicio` | str \| date \| None | None | Start date (YYYY-MM-DD) |
| `fim` | str \| date \| None | None | End date (YYYY-MM-DD) |
| `agregacao` | str | "semanal" | "semanal" or "mensal" |
| `nivel` | str | "municipio" | "municipio", "uf" or "brasil" |
| `as_polars` | bool | False | If True, returns a `polars.DataFrame` |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

## Columns — `precos_diesel`

Source contract `anp_diesel_precos` 2.0 has 15 columns; the `precos_diesel` dataset keeps its own 1.0 contract with the same columns. `data` is the start of the published week or the monthly reference; it is not acquisition time. Weekly interval, geographic level, unit and aggregation coverage remain explicit. See the [dataset contract](../contracts/precos_diesel.md) and [price-period semantics](../api/anp_diesel.md).

## Parameters — `vendas_diesel`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `uf` | str \| None | None | Filter by state (e.g. SP, MT, PR) |
| `inicio` | str \| date \| None | None | Start date |
| `fim` | str \| date \| None | None | End date |
| `as_polars` | bool | False | If True, returns a `polars.DataFrame` |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

## Columns — `vendas_diesel`

| Column | Type | Nullable | Description |
|---|---|---|---|
| `data` | datetime | No | First day of the month |
| `uf` | str | Yes | State abbreviation |
| `regiao` | str | Yes | Region, with the canonical name (`Norte`, `Nordeste`, `Centro-Oeste`, `Sudeste`, `Sul`) |
| `produto` | str | Yes | Diesel type |
| `volume_m3` | float | Yes | Sold volume in m3 |

## Data pipeline

### Prices
1. Bulk XLSX download from the gov.br portal (files by period: 2022-2023, 2024-2025, 2026)
2. Parse with calamine (openpyxl fallback), filter for diesel products (DIESEL, DIESEL S10, OLEO DIESEL, OLEO DIESEL S10); a row with an aggregate label ("TOTAL", "SUBTOTAL") in the municipality column is dropped, with a warning in `validation_warnings` and a `UserWarning`
3. Normalization: "OLEO"/"ÓLEO" prefix removed, state names converted to state abbreviation
4. Margin calculation (preco_venda - preco_compra)
5. Weekly or monthly aggregation according to the parameter

### Volumes
1. CSV download of diesel sales by type (ANP open data)
2. Parse semicolon-delimited CSV (ANO, MES, GRANDE REGIAO, UNIDADE DA FEDERACAO, PRODUTO, VENDAS)
3. Diesel filter (OLEO DIESEL and variants)
4. Normalization: "OLEO"/"ÓLEO" prefix removed from the product and `DIESEL S-10` written as `DIESEL S10`, as in prices; the other fuels (`DIESEL S-500`, `DIESEL S-1800`, `DIESEL MARÍTIMO`, `DIESEL (OUTROS )`) stay as published; `REGIÃO CENTRO-OESTE` becomes `Centro-Oeste`; state names converted to state abbreviation
5. Conversion to standard format (data, uf, regiao, produto, volume_m3)

## MetaInfo

```python
df, meta = await anp_diesel.precos_diesel(return_meta=True)
print(meta.source)           # "anp_diesel"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 4
print(meta.records_count)    # varies by filter
```

## Performance note

ANP XLSX files can be large (50-100MB for municipality-level prices).
Required periods are downloaded concurrently and processed with calamine.
There is no persistent cache; filters are applied after download.

## Source

- Prices URL: `https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/`
- Volumes URL: `https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2025.csv`
- Format: XLSX (prices 2013+), CSV (volumes 2013+)
- Update frequency: weekly (prices), monthly (volumes)
- History: 2013+ (prices and volumes)
- License: `livre` (federal government public data, Decree 8.777/2016)

## Sale price by level

`preco_venda` does not have the same nature at every level. For a municipality, it is the simple arithmetic mean of the sampled stations' prices. For a state and for Brazil, since 2004-10-31, it is the mean weighted by the sales that distributors report to ANP (note on the [historical series page](https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/serie-historica-do-levantamento-de-precos)). `n_postos` is the sample size and adds up across levels: a state sums its municipalities' stations, and Brazil sums the states'. But it is not the weight, and rebuilding a state from its municipalities weighted by `n_postos` is wrong: in the week of 2026-09-06, S10 diesel in AL comes out at 6.84 R$/l with 25 stations, while the station-weighted mean of its 4 municipalities is 7.20 R$/l. `produto="DIESEL"` is common S500 B diesel oil, as the spreadsheet itself says; `"DIESEL S10"` is S10.

## Weeks at year boundaries

In municipal workbooks, a week starting in late December may appear in the following period's file. Selection includes the available adjacent workbook when needed: the week from 2023-12-31 to 2024-01-06 is in `2024–2025`, matches a December 2023 filter and contributes to that month's mean. A query may download two workbooks even when both date bounds are in the same year.

Monthly output from this API is calculated from the selected weeks; it does not use the separate monthly workbooks also published by ANP. Missing distribution prices in municipal files leave `preco_compra` and `margem` null. Acquisition receipts identify every workbook used.

A week belongs to the month of its start date, even when it ends in the next month: the week of 2026-03-29 to 2026-04-04, with 4 of its 7 days in April, counts entirely in March. Monthly values may therefore differ from ANP's monthly workbook. From Jan/2025 to Aug/2026 (Brazil, MT, and SP, S500 and S10 diesel), the mean difference is between R$ 0.006 and R$ 0.018/l per series. In a month with a price shock it reaches 2%: MT S500, Mar/2026, 7.138 R$/l in agrobr × 7.00 R$/l at ANP (7.045 R$/l if weeks counted by their end month).

## Repeated published rows

ANP may repeat an entire weekly row in its workbooks. agrobr retains one occurrence in the selection,
records the number removed in `MetaInfo.validation_warnings`, and emits a `UserWarning`. This avoids
counting the same week twice in monthly means. A weekly identity with conflicting values still raises
`ParseError`, including overlaps between workbooks.

Price columns use the installed pandas version's default text dtype (`str` in pandas 3 and `object`
in pandas 2), `datetime64[ns]` dates, `float64` values, and `Int64` counts, including empty results.
Pass `as_polars` and `return_meta` by name. The municipal catalog's year boundary follows the
Brasília civil date.

`inicio` and `fim` accept `date`, `datetime` (its civil date counts) and `YYYY-MM-DD` text. A period starting after today raises `InvalidParameterError` before any network call, at every level; at the municipal level, a boundary outside 2022 to the current year does too. A year inside that range whose file ANP has not published yet is still `SourceUnavailableError`.
