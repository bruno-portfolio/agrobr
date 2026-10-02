# CONAB - Companhia Nacional de Abastecimento

## Overview

| Field | Value |
|-------|-------|
| **Institution** | Ministério da Agricultura |
| **Website** | [conab.gov.br](https://www.conab.gov.br) |
| **agrobr access** | Direct (public Excel workbooks) |

## Data Origin

### Source

- **URL**: `https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/safra-de-graos/boletim-da-safra-de-graos`
- **Format**: Excel workbooks (XLSX and legacy XLS attachments)
- **Access**: Public, no restrictions

## Browser Requirement

The `safras`, `balanco`, `brasil_total`, and `levantamentos` functions try HTTP
first. If the page or workbook is unavailable or invalid over HTTP, they use
Playwright with Chromium as a fallback:

```bash
pip install agrobr[browser]
python -m playwright install chromium
```

When this fallback is needed, a missing browser causes
`SourceUnavailableError`. The
`estimativa_safra` dataset may try IBGE LSPA when no source or reference is explicitly selected; with `fonte="conab"` or `levantamento`, it does not change origin. `balanco` has no fallback.

In metadata, `source_method` identifies the workbook transport: `httpx` or
`playwright`, independently of the transport used to discover its link.

## Surveys

CONAB publishes monthly crop surveys:

| Month | Survey |
|-----|--------------|
| October | 1st Survey |
| November | 2nd Survey |
| December | 3rd Survey |
| January | 4th Survey |
| February | 5th Survey |
| March | 6th Survey |
| April | 7th Survey |
| May | 8th Survey |
| June | 9th Survey |
| July | 10th Survey |
| August | 11th Survey |
| September | 12th Survey |

## Available Data

### Dataset edition selection

`datasets.estimativa_safra("soja", safra="2024/25", uf="MT", levantamento=1)` selects the first CONAB survey; `levantamento=11` selects the eleventh. `fonte="conab"` without a survey selects the most recent publication carrying the crop year: for a past crop year, the latest survey of the next crop year, which revises it. `levantamento` and `data_publicacao` belong to the CONAB bulletin that published the number, not to the crop year: without `levantamento`, a past crop year comes from the most recent publication carrying it, revised. Crop year 2024/25 served by the 12th survey of 2025/26 comes with `levantamento=12` and `data_publicacao=2026-09-15`; the 12th survey of 2024/25 is another bulletin, with another number. The bulletin crop year is in `meta.source_details["publicacao"]["safra"]`. Two or more crop years behind the latest edition, without `levantamento`, the number comes from the historical series, and `levantamento` and `data_publicacao` are null (see `conab.safras`).

The LSPA calendar month is a separate selector: `mes` routes the dataset to IBGE LSPA and does not represent a CONAB survey number. Incompatible selectors are rejected before network access. Without `uf`, CONAB returns state rows; specify the same state in both queries when comparing with LSPA.

Catalog discovery follows pages and recognizes download links even when their paths have no extension. Availability depends on the editions discovered; a complete historical archive is not guaranteed. Publication dates are preserved when supplied by the source, without substituting the query date.

The [`estimativa_safra` dataset](../contracts/estimativa_safra.md) uses contract 3.1 and adds null `ano_lspa`/`mes_lspa` fields and the units (`mil_ton`, `mil_ha`) to CONAB rows. The `conab.safras` API and `CONAB_SAFRA_V2` remain at 2.0.

### Crops

CONAB publishes one area measure, labelled "ÁREA (Em mil ha)", retained as `area_plantada`. `area_colhida` is null on the CONAB route; a separate harvested area is available only on the LSPA route. Neither area is copied or imputed from the other. CONAB contract V2 already permits this null value.

In sheets with calendar-year headers (wheat, oats, canola, rye, barley and triticale), the published year is the contract crop year's final year (`Safra 2026` → `2025/26`). A survey that does not yet publish that year supplies no estimate for the requested crop year. The historical series retains its own annual period.

From Oct 2019 to Jan 2022, these six cereals' sheets carry the year in their name ("Trigo 2021"), and the edition may also carry the previous year's sheet, sometimes with a broken crop-year header. agrobr reads the most recent sheet whose header publishes the requested crop year: in the 12th survey of 2020/21 (Sep 2021), wheat 2019/20 comes from the "Safra 2020" column of "Trigo 2021", not from the "Trigo 2020" copy, whose header reads "23". In surveys 1 to 4 of 2019/20, 3 and 4 of 2020/21 and 2 to 4 of 2021/22, the sheet does not yet carry the crop year's own winter crop, and the query returns empty. CONAB published surveys 7, 8, 9 and 12 of 2019/20, 1 and 2 of 2020/21 and 1 of 2021/22 only as PDF, so they are not in the catalog.


- Planted area (thousand hectares)
- Null harvested area: surveys publish a single area measure
- Yield (kg/ha)
- Production (thousand tonnes)

### Supply and Demand Balance

- Opening stock
- Production
- Imports
- Consumption
- Exports
- Closing stock

## Usage

### Crops by Product

```python
import asyncio
from agrobr import conab

async def main():
    # Soybean crop data
    df = await conab.safras('soja')

    # Specific crop year
    df = await conab.safras('milho', safra='2025/26')

    # Filter by state
    df = await conab.safras('soja', uf='MT')

    # With metadata
    df, meta = await conab.safras('soja', return_meta=True)

asyncio.run(main())
```

### Supply/Demand Balance

```python
# Balance for all products
df = await conab.balanco()

# Balance for a specific product
df = await conab.balanco(produto='soja')
```

### Brazil Totals

```python
# National totals by product
df = await conab.brasil_total()
```

`produto` contains normalized identifiers (`soja`, `algodao_caroco`, `brasil`), while `rotulo` preserves the original text, including footnotes. `grupo` retains the hierarchy of details and subtotals. Area, production and yield use `float64`; their units are `mil_ha`, `mil_ton` and `kg/ha`. Brazil totals schema 2.0 keeps all nine columns in empty results; an unrecognized header raises `ParseError`.

The `as_polars` and `return_meta` flags are keyword-only. Empty `safras`, `balanco` and `serie_historica` results also retain columns and types. Actual dates use `datetime64[ns]`, survey numbers use `Int64`, measurements use `float64`, and text follows the installed pandas version's default dtype.

## Schema - Crops

| Column | Type | Description |
|--------|------|-----------|
| `fonte` | str | "conab" |
| `produto` | str | Product name |
| `safra` | str | Crop year (e.g. "2024/25") |
| `uf` | str | State code |
| `area_plantada` | float64 | Thousand hectares |
| `area_colhida` | float64 | Null: not separately published in the survey |
| `produtividade` | float64 | kg/ha |
| `producao` | float64 | Thousand tonnes |
| `levantamento` | int | Survey number (1-12) |
| `data_publicacao` | date | Publication date |

## Available Products

```python
produtos = await conab.produtos()
# ['soja', 'milho', 'arroz', 'feijao', 'algodao', 'trigo', ...]
```

## Available States

```python
ufs = await conab.ufs()
# ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', ...]
```

## Available Surveys

```python
levs = await conab.levantamentos()
for lev in levs[:5]:
    print(f"{lev['safra']} - survey #{lev['levantamento']}")
```

## Production Costs

Contract **3.0**, with 25 columns preserving workbook, sheet, location, production system, price reference and physical rows. Select `planilha` and `aba` unambiguously; broad selections list candidates rather than returning the first sheet.

```python
from agrobr import conab

catalogo = await conab.catalogo_custos("soja")
df, meta = await conab.custo_producao(
    "soja", uf="BA",
    planilha="serie-historica-custos-soja-1997-a-2025.xls",
    aba="Barreiras-BA-2025", return_meta=True,
)
```

No primary key is asserted. Repeated rows and literal item labels are preserved. `safra` is nullable and comes from the sheet; the price-reference year is separate. `tipo_linha` distinguishes items, subtotals and totals, which must not be blindly summed together. Negative revenue and missing values remain as published.

See the [25-column contract](../contracts/custo_producao.md) for units, nullability, CV/CT bases and provenance. `custo_producao_total` reports selected published totals instead of reconstructing totals from all rows.

## Historical Series (v0.8.0)

Historical crop data since ~1976, published in Excel spreadsheets (.xls legacy).
The parser automatically detects the format (OLE2/BIFF → xlrd, OOXML → openpyxl with calamine fallback).

```python
# Soybean historical series
df = await conab.serie_historica("soja", ano_inicio=2020, ano_fim=2025)

# Filter by state
df = await conab.serie_historica("soja", ano_inicio=2020, uf="MT")
```

`conab.produtos_serie_historica()` lists the product, category and URL of each series. Use the functions through `agrobr.conab`; the former public `custo_producao` and `serie_historica` subpackages have been removed.

### Schema - serie_historica

| Column | Type | Description |
|--------|------|-----------|
| `safra` | str | Crop year (e.g. "2024/25") |
| `produto` | str | Product name |
| `uf` | str | State code |
| `regiao` | str | State's region |
| `area_plantada_mil_ha` | float | Thousand hectares |
| `producao_mil_ton` | float | Thousand tonnes |
| `produtividade_kg_ha` | float | kg/ha |
| `area_em_producao_mil_ha` | float, optional | Coffee: producing area, in thousand hectares |
| `area_formacao_mil_ha` | float, optional | Coffee: developing area, in thousand hectares |
| `area_colhida_mil_ha` | float, optional | Sugarcane: harvested area, in thousand hectares |

Names containing units remain in contract 1.1. Use this correspondence to compare metrics with `safras` and `estimativa_safra`; publication and period must also match:

| Historical series | Safras / estimate | Unit |
|---|---|---|
| `area_plantada_mil_ha` | `area_plantada` | thousand ha |
| `producao_mil_ton` | `producao` | thousand t |
| `produtividade_kg_ha` | `produtividade` | kg/ha |
| `area_colhida_mil_ha` | `area_colhida` | thousand ha; coverage may differ across products/sources |

`regiao` is the macro-region under which the spreadsheet lists the state, recognized only by the exact label (NORTE, NORDESTE, CENTRO-OESTE, SUDESTE, SUL). Sub-regions, such as the coffee ones in Bahia and Minas Gerais ("Sul e Centro-Oeste", "Norte, Jequitinhonha e Mucuri"), and aggregates ("NORTE/NORDESTE", "CENTRO-SUL", "OUTROS") do not change the region and are not published.

For coffee, planted area is the sum of producing and developing areas when both are available. Thousand 60 kg bags of processed coffee are converted to thousand tonnes (× 0.06), and bags/ha to kg/ha (× 60). Yield refers to producing area; see [contract 1.1](../contracts/serie_historica_safra.md).

Each product is a CONAB series: a crop, a season (`milho_1` through `milho_3`, bean seasons), or a selection (`cana_area_total`, `algodao`, `algodao_pluma`, `algodao_caroco`).

The `safra` period follows the publication: calendar year `YYYY` for `cafe`, `cafe_arabica`, `cafe_conilon`, `trigo`, `aveia`, `cevada`, `canola`, `centeio`, `triticale`; other products use `YYYY/YY`. Inclusive `ano_inicio`/`ano_fim` filters use the starting year and require integers; reversed intervals raise `InvalidParameterError` before network access.

The forecast column (labels such as `Previsão` or `(¹)`) is excluded from the historical series; the exclusion is logged with its product, sheet and label. For the current crop year, use `estimativa_safra` for products available in that dataset.

`algodao` represents seed cotton; `algodao_pluma`, cotton lint; `algodao_caroco`, cottonseed. All three share the same area, with production and yield from their respective selection.

`cana` publishes the Área sheet in `area_colhida_mil_ha`: its title in the official spreadsheet is "Série Histórica de Área Colhida" (harvested area). `area_plantada_mil_ha` stays null for this product; for total area, use `cana_area_total`.

`cana_area_total` reads only the Área Total sheet, whose composition changes over the series: harvested + planting + seedlings through 2021/22 (except 2016/17), and equal to harvested area in 19 to 23 states in 2016/17 and from 2023/24 to 2025/26 (details in the [contract](../contracts/serie_historica_safra.md)). Empty cells are not filled from Área Colhida; production and yield remain null. Seedling and harvesting method sheets are excluded.

Unreadable, missing or ambiguous selected sheets raise `ParseError`; filters with no observations return an empty table with its schema. Unknown sheets produce a warning.

`arroz_sequeiro`: the official spreadsheet has incorrect unit labels in the Produtividade and Produção sheets (September 2026); values are kg/ha and thousand tonnes, as confirmed by the production/area ratio.

## Cache

CONAB queries keep no local copy: each call downloads the publication. `meta.cache_expires_at` is null; in `conab.safras`, `meta.cache_key`
identifies the query (product, crop year, publication, survey and state), and in the other functions it is null.

## Update

| Aspect | Value |
|---------|-------|
| **Frequency** | Monthly |
| **Publication** | Generally between days 10-15 |

## Datasets

- [`estimativa_safra`](../contracts/estimativa_safra.md) — contract 3.1 with an explicit CONAB survey or LSPA month

- [`serie_historica_safra`](../contracts/serie_historica_safra.md) — wraps `conab.serie_historica()` (45 products; `cana_industria` removed from advertised support in version 2.0)

## Semantics in version 2.0

Supply and demand recognizes accented cotton and bean names, preserves calendar years for wheat, and selects the latest revision of each crop year, including merged cells. Progress distinguishes `Safra YYYY` from `Safra YYYY/YY` headings, preventing wheat rows from entering second-crop corn.

`semana_url` takes the URL of the weekly bulletin page (`acompanhamento-das-lavouras-…`); the client resolves its metadata page and spreadsheet link. Historical progress bulletins: the file link is resolved on the official page, including its download metadata page. Content without an XLSX/XLS signature is rejected before parsing, with the URL and an initial excerpt in the error message. Only pages under `https://www.gov.br/conab/` are followed: any other URL, or a redirect or link that leaves it, raises `InvalidParameterError` before the request.

`tecnologia` is not a selector; the output qualification is only populated when explicitly published. The active cost contract is 3.0.

Publication date is null when download metadata does not provide it. The fetch date (`MetaInfo.fetched_at`) is never used as a substitute. `Safra.data_publicacao` accepts `None`; a publication date supplied by source metadata is preserved.

## Sociobiodiversity

`conab.custo_sociobiodiversidade(produto, uf=None, ano=None, *, local=None, planilha=None, aba=None, use_cache=True, as_polars=False, return_meta=False)` returns the published extraction costs. The dataset has the same selectors. `conab.catalogo_sociobiodiversidade()` lists all resource revisions with an active flag; with a product it inventories the active workbook, including unresolved contexts. Select an exact historical resource with `planilha=`. The 20 captured products, literal units, selection rules and nominal limitations are documented in the [1.0 contract](../contracts/custo_sociobiodiversidade.md). No hectare/crop conversion or revision merging. Catalogue cache: 1 hour, separate from agricultural costs; workbooks are always downloaded. `use_cache=False` bypasses the catalogue cache.

Agricultural costs use parser 5: merged headers, coffee identified by official
resource, literal annual or two-year crop tokens, and exchange-rate notes kept
outside cost items. Excel percentage scaling applies only to numeric cells.
Sociobiodiversity costs use parser 2 and preserve explicit archived-revision
selection. Contracts remain 3.0 and 1.0, with text in the installed pandas default dtype; a third monetary measure is rejected.
