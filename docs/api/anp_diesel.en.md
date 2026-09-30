# ANP Diesel API

The ANP Diesel module provides diesel retail price and sales volume data for Brazil, published by the National Petroleum Agency. Namespace: `agrobr.alt.anp_diesel`.

## Functions

### `precos_diesel`

Diesel retail prices by municipality, state or Brazil level.

```python
async def precos_diesel(
    uf: str | None = None,
    municipio: int | str | None = None,
    produto: str = "DIESEL S10",
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "semanal",
    nivel: str = "municipio",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame | tuple[pd.DataFrame | pl.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `uf` | `str \| None` | Filter by state (e.g. SP, MT, PR) |
| `municipio` | `int \| str \| None` | Municipality by its 7-digit IBGE code or full name, resolved by `normalize.resolver_municipio` before the request and compared with the spreadsheet name ignoring case, accents and punctuation (`"Sant'Ana do Livramento"` matches `SANTANA DO LIVRAMENTO`); a fragment of a name raises `InvalidParameterError` listing the candidates |
| `produto` | `str` | "DIESEL" or "DIESEL S10" (default) |
| `inicio` | `str \| date \| None` | Start date (YYYY-MM-DD) |
| `fim` | `str \| date \| None` | End date (YYYY-MM-DD) |
| `agregacao` | `str` | "semanal" (default) or "mensal" |
| `nivel` | `str` | "municipio" (default), "uf" or "brasil" |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

Source contract `anp_diesel_precos` 2.0, with 15 columns: `data`, `uf`, `municipio`, `produto`, `preco_venda`, `preco_compra`, `n_postos`, `margem`, `periodo_inicio`, `periodo_fim`, `nivel`, `unidade`, `agregacao`, `n_semanas`, `n_postos_media`. The [`precos_diesel` dataset](../contracts/precos_diesel.md) has its own 1.0 contract, with the same columns.

**Example:**

```python
from agrobr.alt import anp_diesel

# DIESEL S10 prices
df = await anp_diesel.precos_diesel()

# Filter by state and period
df = await anp_diesel.precos_diesel(
    uf="MT",
    inicio="2024-01-01",
    fim="2024-06-30",
)

# State level with monthly aggregation
df = await anp_diesel.precos_diesel(nivel="uf", agregacao="mensal")
```

### `vendas_diesel`

Diesel sales volumes by state (monthly).

```python
async def vendas_diesel(
    uf: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame | tuple[pd.DataFrame | pl.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `uf` | `str \| None` | Filter by state (e.g. SP, MT, PR) |
| `inicio` | `str \| date \| None` | Start date |
| `fim` | `str \| date \| None` | End date |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `data`, `uf`, `regiao`, `produto`, `volume_m3`

**Example:**

```python
from agrobr.alt import anp_diesel

# Diesel volumes
df = await anp_diesel.vendas_diesel()

# Filter by state
df = await anp_diesel.vendas_diesel(uf="MT")
```

## Synchronous Version

```python
from agrobr.sync import alt

df = alt.anp_diesel.precos_diesel(uf="MT")
df = alt.anp_diesel.vendas_diesel()
```

## Notes

- Source: [ANP Gov.br](https://www.gov.br/anp/) — `livre` license (Decree 8,777/2016)
- Data: bulk XLSX (prices 2013+), CSV (volumes)
- Large municipal workbooks are downloaded in full; there is no persistent cache.

## Price periods and catalogue

`data` is the published start of the week, not acquisition time. Date filters select that start and preserve the published end. Monthly output uses the month of each weekly start and the simple mean of available weekly prices, without weighting by station count or days. `n_postos` becomes null; `n_postos_media` and `n_semanas` describe the selected observations. No missing weeks are invented.

For municipality prices, year ranges are resolved through published workbook links; years beyond the configured catalogue trigger official catalogue discovery. A valid period with no published resource raises `SourceUnavailableError` with catalogue coverage. Malformed or reversed dates raise `InvalidParameterError`.

Metadata retains requested/final resource URLs, acquisition time, hashes, weekly population and selected coverage. See the [dataset contract](../contracts/precos_diesel.md).

## Sale price by level

`preco_venda` does not have the same nature at every level. For a municipality, it is the simple arithmetic mean of the sampled stations' prices. For a state and for Brazil, since 2004-10-31, it is the mean weighted by the sales that distributors report to ANP (note on the [historical series page](https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/serie-historica-do-levantamento-de-precos)). `n_postos` is the sample size and adds up across levels: a state sums its municipalities' stations, and Brazil sums the states'. But it is not the weight, and rebuilding a state from its municipalities weighted by `n_postos` is wrong: in the week of 2026-09-06, S10 diesel in AL comes out at 6.84 R$/l with 25 stations, while the station-weighted mean of its 4 municipalities is 7.20 R$/l. `produto="DIESEL"` is common S500 B diesel oil, as the spreadsheet itself says; `"DIESEL S10"` is S10.

## Weeks at year boundaries

In municipal workbooks, a week starting in late December may appear in the following period's file. Selection includes the available adjacent workbook when needed: the week from 2023-12-31 to 2024-01-06 is in `2024–2025`, matches a December 2023 filter and contributes to that month's mean. A query may download two workbooks even when both date bounds are in the same year.

Monthly output from this API is calculated from the selected weeks; it does not use the separate monthly workbooks also published by ANP. Missing distribution prices in municipal files leave `preco_compra` and `margem` null. Acquisition receipts identify every workbook used.

## Repeated rows and output types

Entirely identical weekly rows are removed from the selection before monthly averaging, with
a `UserWarning` and an entry in `MetaInfo.validation_warnings`. Conflicting values for the same
week still raise `ParseError`. Receipts and raw file hashes are preserved.

Price text columns use the installed pandas version's default dtype (`str` in pandas 3 and `object`
in pandas 2), including empty results. Dates use `datetime64[ns]`, monetary values use `float64`,
and counts use `Int64`. Pass `as_polars` and `return_meta` by name in both functions.
The municipal catalog's year limit follows the Brasília civil date.
