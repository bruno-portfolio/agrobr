# CEPEA — Center for Advanced Studies in Applied Economics

> **License:** CEPEA/ESALQ data licensed under
> [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/deed.pt-br).
> Commercial use requires authorization from CEPEA (cepea@usp.br).
> Ref: [Data use license](https://www.cepea.org.br/br/licenca-de-uso-de-dados.aspx)

## Overview

| Field | Value |
|-------|-------|
| **Institution** | ESALQ/USP |
| **Website** | [cepea.org.br](https://www.cepea.org.br) |
| **License** | CC BY-NC 4.0 |
| **agrobr access** | Direct CEPEA access, with Notícias Agrícolas fallback when available |

## Data Origin

### Primary Source

- **Official page**: CEPEA indicators
- **Access**: Direct HTTP attempt; blocks and unavailability may trigger the fallback

### Alternative Source

- **Page**: Notícias Agrícolas quotations
- **Type**: Authorized mirror of CEPEA indicators
- **Use**: When enabled and available for the requested product

## Available Products (22 products)

Calf also carries `valor_usd` (Valor US$ column) and `peso_medio_kg` (the page's Peso Médio table); other products fill `valor_usd` whenever CEPEA publishes the dollar price.

| Product | Main Market | Unit | Frequency |
|---------|-----------------|---------|------------|
| Soybean | Paranagua/PR | BRL/sc 60kg | Daily |
| Soybean Parana | Parana | BRL/sc 60kg | Daily |
| Corn | Campinas/SP | BRL/sc 60kg | Daily |
| Calf | Mato Grosso do Sul | BRL/cabeca | Daily |
| Live Cattle | Sao Paulo/SP | BRL/@ | Daily |
| Arabica Coffee | Sao Paulo/SP | BRL/sc 60kg | Daily |
| Robusta Coffee | Espirito Santo | BRL/sc 60kg | Daily |
| Wheat | Parana + RS | BRL/ton | Daily |
| Cotton | Sao Paulo/SP | cBRL/lb | Daily |
| Rough rice | Rio Grande do Sul | BRL/sc 50kg | Daily |
| Crystal sugar | Sao Paulo/SP | BRL/sc 50kg | Daily |
| Refined sugar | São Paulo/SP | BRL/kg | Daily |
| Hydrous ethanol | Sao Paulo/SP | BRL/L | Weekly |
| Anhydrous ethanol | Sao Paulo/SP | BRL/L | Weekly |
| Frozen chicken | Sao Paulo/SP | BRL/kg | Daily |
| Chilled chicken | Sao Paulo/SP | BRL/kg | Daily |
| Live hog | MG, PR, RS, SC, and SP (source market terms preserved) | BRL/kg | Daily |
| Milk | State and BRASIL, at the producer | BRL/L | Monthly |
| Orange (industry) | Sao Paulo/SP | BRL/cx 40,8kg | Daily |
| Orange (fresh) | Sao Paulo/SP | BRL/cx 40,8kg | Daily |

## CEPEA Methodology

CEPEA calculates indicators based on:

- Daily survey with market agents
- Weighted average by traded volume
- Adjustment to standard quality

Source: [CEPEA Methodology](https://www.cepea.esalq.usp.br/br/metodologia.aspx)

## Update and Lag

| Aspect | Value |
|---------|-------|
| **Update time** | ~17:00 - 18:00 (business days) |
| **Typical lag** | D+0 (same day) |
| **Days without publication** | Weekends, national holidays |
| **agrobr cache** | Valid until the next 18:00 BRT turnover on a business day, counted from the product's last collection (Smart TTL) |

## Usage

### Basic

```python
import asyncio
from agrobr import cepea

async def main():
    # Recent window available from the source and cache
    df = await cepea.indicador('soja')

    # Specific period
    df = await cepea.indicador('milho', inicio='2024-01-01', fim='2024-12-31')

    # Latest available value
    ultimo = await cepea.ultimo('boi')
    print(f"Live cattle: R$ {ultimo.valor}")

asyncio.run(main())
```

### With Metadata

```python
df, meta = await cepea.indicador('soja', return_meta=True)

print(meta.source)
print(meta.source_url)
print(meta.fetched_at)
print(meta.from_cache)  # True/False
```

## Data Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `data` | date | No | Indicator date |
| `produto` | str | No | Product name |
| `praca` | str | Yes | Reference market |
| `valor` | float | No | Price in the row's unit; cotton uses Brazilian real cents per pound |
| `unidade` | str | No | Unit (BRL/sc60kg, etc) |
| `fonte` | str | No | Data source |
| `metodologia` | str | Yes | Methodology description |

## Cache

CEPEA uses Smart TTL - the cache expires automatically at 18:00:

```
08:00 - Fetch soybean -> Cache valid until 18:00
10:00 - Fetch soybean -> Uses cache
17:59 - Fetch soybean -> Uses cache
18:01 - Fetch soybean -> Cache expired -> Fetch source -> Valid until 18:00 next business day
```

A collection after 18:00, on Saturday or on Sunday is valid until 18:00 of the next business day (Monday to Friday; holidays are not taken into account). A closed period (`fim` before the last 25 calendar days) does not query the page: it comes from the cache, or from the historical series downloaded when it does not yet cover the period, with a null `cache_expires_at`.

`boi` and `boi_gordo`, and `cafe` and `cafe_arabica`, are the same CEPEA indicator, and the cache stores each series once, under the name `datasets.preco_diario` uses (`boi` and `cafe`). Asking by the other name reads the same rows, without downloading the page or the series again, and the `produto` column carries the requested name. Rows that earlier versions stored under the other name remain valid, without a database migration; with both on the same day and location, the usual precedence rule applies (the latest collection from the same source).

## Helper Functions

```python
# List available products
produtos = await cepea.produtos()
# ['soja', 'soja_parana', 'milho', 'bezerro', 'boi', 'boi_gordo',
#  'cafe', 'cafe_arabica', 'cafe_robusta', 'algodao',
#  'trigo', 'arroz', 'acucar', 'acucar_refinado', 'etanol_hidratado',
#  'etanol_anidro', 'frango_congelado', 'frango_resfriado', 'suino', 'leite',
#  'laranja_industria', 'laranja_in_natura']

# List markets for a product
pracas = await cepea.pracas('soja')
# ['paranagua'] - corresponds to "Paranaguá/PR" in the praca column
```

## Coverage and series selection

The indicator page publishes a recent window, usually around 15 trading days. The period before it comes from CEPEA's historical series, the spreadsheet CEPEA publishes for each indicator (from 1996 to 2010, depending on the product): on the first query that needs it, agrobr downloads the product's whole series and stores in DuckDB only the days the cache does not have yet, and later queries are served from the cache. The series is downloaded again when the requested period goes past the last day it published (the cache records that date on download), and never before the CEPEA 18:00 turnover following the previous download. Oranges have no series: before the window, the result carries only what the cache accumulated, with a warning in `validation_warnings` and `UserWarning`. With `fim` within the last 25 calendar days (the roughly 15 published dates), the page is fetched when the product's last collection has expired or, with no recorded collection, when a business day of that part of the requested period is missing from the cache; within the validity, the answer comes from the cache, even with a holiday or with today not yet published. With an earlier `fim`, the page is not fetched. `force_refresh=True` always fetches the source, and `offline=True` never does.

In the series, a day with value 0 (no quote) is left out, as on the page. Milk comes with 2 decimals in the series and 4 on the page: when both exist, the page wins, and `source_details["pagina_desde"]` carries the first month that came from it. Series rows store `parser_version` 101, and the `MetaInfo` of the query that downloaded the series lists each spreadsheet in `source_details["resources"]` (URL, SHA-256, bytes and time; the calf weight one with `papel` `serie_peso`). An unavailable series warns in `validation_warnings` and `UserWarning`; if the period ends up without data, `indicador` raises `SourceUnavailableError` (or `ParseError`, if the spreadsheet layout changed).

With no network and nothing cached for the period, `indicador` and `ultimo` raise `SourceUnavailableError`, with `attempted_sources` (the sources tried and the cache); with cache, the answer comes from it with a `StaleDataWarning`. With `offline=True` and an empty cache, `indicador` returns the empty table, with `from_cache=False` and a null `cache_expires_at`, and `ultimo` raises `SourceUnavailableError` ("offline sem dado no cache").

Pages with several indicators are selected by product title, never table position. Live hog preserves the five locations in the State column. Cache migration removes old hog records carrying incorrect location labels; subsequent calls collect corrected records.

For milk, `data` is the first day of the reference month, `praca` preserves the state (or BRASIL), and the unit is `BRL/L`. The spot-milk table is excluded. `ultimo("leite")` uses a monthly window and may return a month preceding publication.

Milk does not use the Notícias Agrícolas fallback in the CEPEA API. The NA
page supplies closing and reference dates separately, but its standalone
parser exposes the closing date. Migration 9 preserves these legacy rows in
quarantine without assuming a fixed lag to convert them to reference months.

Refined sugar uses its dedicated indicator page on the CEPEA website, in `BRL/kg`, not the crystal-sugar table. HTTP 200 without a recognized table also triggers the Notícias Agrícolas fallback when enabled and available for the product, with the usual license warning. So does a table without the value column in reais recognized by its header ("Valor R$", "R$/litro", "Preço médio" for milk, "A Prazo" for oranges): the parser raises `ParseError` instead of using another number from the row, and a US$ column never becomes a BRL price.

## Price validation

`validate_sanity=True` checks unit and range for all 22 identifiers, including
the `cafe_arabica` and `boi_gordo` aliases. Incompatible units are flagged before
comparing values. Monthly milk and weekly ethanol do not receive daily change
thresholds. See the [ranges and their interpretation limits](../advanced/resilience.md#statistical-validation).

The DataFrame's `anomalies` column contains the list serialized as JSON text,
or `None` when empty, as required by the `preco_diario` contract.

## Legacy cache history

Migration automatically preserves affected originals in
`indicadores_quarentena`, in the same DuckDB file and outside normal queries.
This includes live hog, CEPEA series with old table selection, and legacy Notícias
Agrícolas refined sugar labeled per bag instead of per kg. The upgrade is
transactional; failures raise `CacheMigrationError`. Records already removed by
earlier implementations are not recreated. See [preservation and recovery](../guides/migracao-2.md#18-automatic-preservation-of-existing-caches).
Migration 10 adds `valor_usd` and `peso_medio_kg` to `indicadores` before the other
migrations, as an idempotent step outside the main transaction; earlier rows stay null
in those columns until the next collection.
Migration 11 adds `anomalies` to `indicadores` and to the quarantine, in the same step, and
stores the weekly-average marker in the cache. Rows saved before it receive
`["media_semanal"]` when they are Notícias Agrícolas hydrous or anhydrous ethanol, whose
two pages publish only weekly averages, and an empty list otherwise. Without it, cache
reads (`offline` or within the expiry) returned the weekly average as a daily quote.

Corrected parsers identify new observations with versions CEPEA 2 and Notícias
Agrícolas 3. `MetaInfo.data_sources` retains row origins even when
`selected_source="cache"`. Fallback warnings alone do not block offline reading
of another provider; applications with provider restrictions must inspect this
metadata or the `fonte` column.

For the same date, product, and location, CEPEA takes precedence over Notícias
Agrícolas in normal and offline queries; both observations remain stored.
Migration 9 also corrects legacy wheat (`BRL/sc60kg` → `BRL/ton`) and cotton
(`BRL/@` → `cBRL/lb`) labels from CEPEA before parser 2, without changing
values and with originals retained in quarantine.
