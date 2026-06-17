# ANP Diesel — Prices and Volumes

> **License:** Brazilian federal government public data (Decree 8.777/2016).
> Classification: `livre`

National Agency for Petroleum, Natural Gas and Biofuels (Agencia Nacional do
Petroleo, Gas Natural e Biocombustiveis). Diesel resale price and sales volume
data in Brazil. Proxy for mechanized agricultural activity.

## Installation

Does not require optional dependencies. Uses only httpx + pandas + openpyxl (core, calamine fallback).

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
| `municipio` | str \| None | None | Filter by municipality (substring) |
| `produto` | str | "DIESEL S10" | "DIESEL" or "DIESEL S10" |
| `inicio` | str \| date \| None | None | Start date (YYYY-MM-DD) |
| `fim` | str \| date \| None | None | End date (YYYY-MM-DD) |
| `agregacao` | str | "semanal" | "semanal" or "mensal" |
| `nivel` | str | "municipio" | "municipio", "uf" or "brasil" |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

## Columns — `precos_diesel`

| Column | Type | Nullable | Description |
|---|---|---|---|
| `data` | datetime | No | Collection date |
| `uf` | str | Yes | State abbreviation (2 chars) |
| `municipio` | str | Yes | Municipality name |
| `produto` | str | Yes | "DIESEL" or "DIESEL S10" |
| `preco_venda` | float | Yes | Average resale price (R$/liter) |
| `preco_compra` | float | Yes | Average distribution price (R$/liter) |
| `margem` | float | Yes | preco_venda - preco_compra |
| `n_postos` | int | Yes | Number of stations surveyed |

## Parameters — `vendas_diesel`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `uf` | str \| None | None | Filter by state (e.g. SP, MT, PR) |
| `inicio` | str \| date \| None | None | Start date |
| `fim` | str \| date \| None | None | End date |
| `return_meta` | bool | False | Returns a (DataFrame, MetaInfo) tuple |

## Columns — `vendas_diesel`

| Column | Type | Nullable | Description |
|---|---|---|---|
| `data` | datetime | No | First day of the month |
| `uf` | str | Yes | State abbreviation |
| `regiao` | str | Yes | Geographic region |
| `produto` | str | Yes | Diesel type |
| `volume_m3` | float | Yes | Sold volume in m3 |

## Data pipeline

### Prices
1. Bulk XLSX download from the gov.br portal (files by period: 2022-2023, 2024-2025, 2026)
2. Parse with openpyxl (calamine fallback), filter for diesel products (DIESEL, DIESEL S10, OLEO DIESEL, OLEO DIESEL S10)
3. Normalization: "OLEO"/"ÓLEO" prefix removed, state names converted to state abbreviation
4. Margin calculation (preco_venda - preco_compra)
5. Weekly or monthly aggregation according to the parameter

### Volumes
1. CSV download of diesel sales by type (ANP open data)
2. Parse semicolon-delimited CSV (ANO, MES, GRANDE REGIAO, UNIDADE DA FEDERACAO, PRODUTO, VENDAS)
3. Diesel filter (OLEO DIESEL and variants)
4. Normalization: "OLEO"/"ÓLEO" prefix removed from the product, state names converted to state abbreviation
5. Conversion to standard format (data, uf, regiao, produto, volume_m3)

## MetaInfo

```python
df, meta = await anp_diesel.precos_diesel(return_meta=True)
print(meta.source)           # "anp_diesel"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 1
print(meta.records_count)    # varies by filter
```

## Performance note

ANP XLSX files can be large (50-100MB for municipality-level prices).
The module caches by file period (e.g. 2022-2023), not by filter
parameter. State/municipality/product filters are applied during parsing after download.
Cache TTL: 7 days.

## Source

- Prices URL: `https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/`
- Volumes URL: `https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2025.csv`
- Format: XLSX (prices 2013+), CSV (volumes 2013+)
- Update frequency: weekly (prices), monthly (volumes)
- History: 2013+ (prices and volumes)
- License: `livre` (federal government public data, Decree 8.777/2016)
