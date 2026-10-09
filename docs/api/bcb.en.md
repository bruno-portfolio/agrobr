# BCB/SICOR API

The BCB module provides Banco Central do Brasil data: rural credit (SICOR), time series (SGS), USD exchange rate (PTAX) and market expectations (Focus).

## Functions

### `credito_rural`

Rural financing data by product, crop year, and state, aggregated by state or by program, or record by record.

```python
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product key (soja, milho, arroz, feijao, trigo, algodao, cafe, cana, mandioca, sorgo) or SICOR investment item (e.g. "BOVINOS"); for keys, accents, case and surrounding spaces are ignored; for other items, case and surrounding spaces are ignored, and accents must match SICOR |
| `safra` | `str \| None` | Crop year `"YYYY/YY"`, `"YYYY/YYYY"` (consecutive years) or `"YYYY"` (ending year: `"2025"` = 2024/2025); any other form raises `InvalidParameterError` before any request. `None` (default) applies no crop-year filter |
| `finalidade` | `str` | `"custeio"`, `"investimento"` or `"comercializacao"`; agro-industrialisation is not published by product and raises `InvalidParameterError` pointing to `credito_rural_total` |
| `uf` | `str \| None` | State abbreviation (e.g. "MT", "PR"); whitespace/case are normalized and invalid values raise `ValueError` |
| `agregacao` | `str` | `"uf"` (default), `"programa"` or `"registro"` (SICOR records, without aggregation; other columns and another contract, below) |
| `programa` | `str \| None` | Filter by the published program name, case-insensitive (e.g. "PRONAMP", "RenovAgro"; table in [sources/BCB](../sources/bcb.en.md#sicor-dimensions)) |
| `tipo_seguro` | `str \| None` | Filter by the official insurance-type description, case-insensitive (e.g. "Proagro tradicional", "Sem adesão a seguro") |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns:

| Column | Type | Description |
|--------|------|-------------|
| `safra` | str | Crop year "2024/25" (YYYY/YY, July to June), as in the other datasets |
| `produto` | str | Requested product key, unaccented and lower-case (e.g. `algodao`, `cana`), identical for both sources; the filter uses the official SICOR spelling (`"ALGODÃO"`, `"CANA-DE-AÇUCAR"`) |
| `uf` | str | State |
| `finalidade` | str | Requested purpose, lower-case for both sources (`custeio`, `investimento`, or `comercializacao`) |
| `agregacao` | str | Output level: `uf` or `programa` |
| `programa` | str | SICOR program; null for state aggregation |
| `cd_programa` | str | Program code; null for state aggregation |
| `qtd_contratos` | int | Number of contracts |
| `valor` | float | Financed amount (BRL) |
| `area_financiada` | float | Financed area (ha). Null through OData: for custeio the source publishes an empty `AreaCusteio` (none of 2,583 records from 10 queries for the 2024/25 crop year, in Sep 2026, has an area), and investimento and comercializacao carry no area; only the BigQuery fallback fills it |
| `fonte` | str | `bcb_odata` or `bcb_bigquery` |

`programa` uses the current name from the official table for every crop year: `0152` is published as PROIRRIGA even before 07/2021, when the code was Moderinfra (the official description records the change on 2021-07-01).

**Absence is not zero.** A `uf`, `programa` or `tipo_seguro` filter on a source body without the matching column raises `ParseError` instead of returning the total of all. In the aggregation by state or by program, `valor`, `area_financiada` and `qtd_contratos` are null in a group where any record lacks the value; when the same group has both known and missing values, `MetaInfo.validation_warnings` records a warning. A missing value is null, but a year, month or contract count published as nonnumeric text or as a fraction raises `ParseError` with the column, the record and the published value.

**Crop year in progress.** The crop year containing today (July to June) is still receiving contracts, and its total changes until the crop year ends. When the result includes it, `credito_rural` warns in `validation_warnings` and `UserWarning` and records in `source_details` the crop year (`safra_em_curso`) and the issuance months covered (`meses_cobertos`, `"YYYY-MM"`).

**Record by record (`agregacao="registro"`).** Returns the records of the `*RegiaoUFProduto` entities after the `uf`, `programa` and `tipo_seguro` filters, without aggregation: the cut that the default 1.1.0 call returned. There are 23 columns, the 11 above plus `ano_emissao`, `mes_emissao`, `regiao`, `cd_sub_programa`, `cd_fonte_recurso`, `fonte_recurso`, `cd_tipo_seguro`, `tipo_seguro`, `cd_modalidade`, `modalidade`, `cd_atividade` and `atividade`, in the [bcb.credito_rural_registro](../contracts/bcb_credito_rural_registro.en.md) 1.0 contract, with that contract's `MetaInfo`. Funding source, modality and activity names come from the description in BCB's domain tables, and a code outside the table gets a null name. Summed by crop year, state, product and purpose, it equals `agregacao="uf"`.

**BigQuery fallback only for the state aggregation without a programme or insurance filter.** The Base dos Dados table aggregates by municipality and carries no programme, funding source, insurance type, modality or activity. With `agregacao="programa"`, `agregacao="registro"`, `programa=` or `tipo_seguro=`, OData being down raises `SourceUnavailableError`, with the reason in the message, instead of returning the total of all programmes.

**Example:**

```python
from agrobr import bcb

# Working-capital credit, soybean, MT
df = await bcb.credito_rural("soja", safra="2024/25", uf="MT")

# Aggregated by state
df = await bcb.credito_rural("milho", agregacao="uf")

# Aggregated by program
df = await bcb.credito_rural("soja", safra="2024/25", agregacao="programa")

# Filter by program
df = await bcb.credito_rural("soja", safra="2024/25", programa="Pronamp")

# Filter by insurance type
df = await bcb.credito_rural("soja", safra="2024/25", tipo_seguro="Proagro tradicional")

# Record by record, with month, funding source, modality and activity
df = await bcb.credito_rural("soja", safra="2024/25", uf="MT", agregacao="registro")

# With metadata
df, meta = await bcb.credito_rural("soja", return_meta=True)
print(meta.schema_version)  # "2.0"
```

`agregacao="municipio"` raises `InvalidParameterError`. The by-product entities agrobr reads (`*RegiaoUFProduto`) have no municipality. SICOR publishes municipality by product (`CusteioMunicipioProduto` and `InvestMunicipioProduto`), which agrobr does not read; the `agrobr[bigquery]` extra has municipality-level data.

### `credito_rural_total`

Rural credit by state and purpose, without product: SICOR's `RegiaoUF` entity, with the four purposes, including agro-industrialisation, which SICOR does not publish by product. Contract [bcb.credito_rural_total](../contracts/bcb_credito_rural_total.en.md) 1.0.

```python
async def credito_rural_total(
    safra: str | None = None,
    finalidade: str | None = None,
    uf: str | None = None,
    agregacao: Literal["uf", "programa"] = "uf",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-----------|
| `safra` | `str \| None` | Same formats as `credito_rural`; the crop year runs from July to June. `None` (default) does not filter by crop year and reads the whole series, from Jan 2013 |
| `finalidade` | `str \| None` | `"custeio"`, `"investimento"`, `"comercializacao"` or `"industrializacao"`; `None` (default) returns all four. Any other value raises `InvalidParameterError` before network access |
| `uf` | `str \| None` | State abbreviation; `None` (default) returns all 27 |
| `agregacao` | `str` | `"uf"` (default) or `"programa"` |
| `as_polars` | `bool` | Return a polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:** one row per crop year, state and purpose (and programme, with `agregacao="programa"`), with the `credito_rural` columns minus `produto` and `area_financiada`: `safra`, `uf`, `finalidade`, `agregacao`, `programa`, `cd_programa`, `qtd_contratos`, `valor` (BRL, to the cent) and `fonte` (`bcb_odata`).

- **A purpose without operations is absent.** In the wide row, the source fills purposes without operations with 0; that pair does not become a row. In crop year 2022/23, agro-industrialisation in AM, AP and RR and marketing in AP are absent.
- **No Brazil row.** SICOR publishes no national total: the Brazil total is the sum of the states.
- **Partial crop year.** The current crop year is partial; `MetaInfo.source_details["meses"]` holds the first and last month with data and the number of months.
- **Crop-year query × sum of the monthly queries.** The function requests the crop year in a single query (split by month only when the response hits the Olinda record limit), and SICOR may return numbers that differ from the sum of month-by-month queries. On 2026-09-26, for crop year 2026/27 (July and August), 51 state × purpose pairs diverged: for `custeio` in AC, 274 contracts and R$ 56,788,261.98 in the crop-year query, against 272 and R$ 56,541,830.82 in the monthly ones. The cause was not identified, and agrobr reproduces the body received.
- **Year, month or count that is not an integer.** `AnoEmissao`, `MesEmissao` or the contract count published as nonnumeric text or as a fraction raise `ParseError` with the column, the record and the published value, instead of truncating the fraction or failing with a raw error. In the crop-year filter (here and in `credito_rural`), a missing year or month also raises `ParseError`: without them, the record has no crop year.
- **No BigQuery fallback**: `attempted_sources` is `["bcb_odata"]`.
- The total by state and purpose matches the sum of the municipalities (`CusteioInvestimentoComercialIndustrialSemFiltros`) and, for operating costs, investment and marketing, the by-product sum of `credito_rural` (checked for 2022 and 2023).

**Example:**

```python
from agrobr import bcb

# The four purposes by state
df = await bcb.credito_rural_total(safra="2022/23")

# Agro-industrialisation, which is not published by product
df = await bcb.credito_rural_total(safra="2022/23", finalidade="industrializacao")

# One state by programme
df = await bcb.credito_rural_total(safra="2022/23", uf="MT", agregacao="programa")
```

### `sgs`

BCB time series, selected by a positive integer code or one of the 17 existing aliases. Long ranges use calendar blocks with validated reconciliation and provenance per response.

```python
async def sgs(
    codigo: int | str,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

| Parameter | Rule |
|-----------|------|
| `codigo` | Integer from 1 to 2**63−1 or a case/whitespace-normalized alias; numeric strings and bool are rejected |
| `inicio`, `fim` | Inclusive civil dates: `date`, `datetime`, ISO or DD/MM/YYYY; inverted ranges are invalid |
| `ultimos` | Positive integer; without dates uses `/ultimos/N`; with dates applies a tail after sorted reconciliation |
| `as_polars` | Boolean; True requires `agrobr[polars]` |
| `return_meta` | Boolean; True also returns MetaInfo |

Without dates or `ultimos`, the default range starts ten years before the query's Brasília civil date (UTC−3) and ends on that date, adjusting February 29 to February 28 if needed. With only a start date, the end defaults to the query's Brasília civil date (UTC−3). With only an end date, the request keeps the start omitted: the source may reject this selection. This does not select a frozen historical revision.

Each block ends on December 31 of its starting year + 9, or the requested end if earlier; the next starts on January 1. This respects the ten-year request limit for daily queries without assuming every code is daily. Calendar alignment means a ten-year range can require two requests. A failed block aborts the query without partial output.

The latest-values route has a documented and verified limit of 20 for series 1. agrobr preserves remote rejection without imposing that maximum on codes with unknown frequency. To request more observations, provide both dates and `ultimos`.

**Aliases:** `selic`, `ipca`, `ipca_alimentacao`, `ipa_agricola`, `pib_agropecuaria`, `credito_rural_concessoes_pf`, `credito_rural_saldo_pf`, `dolar_ptax_venda`, `dolar_ptax_compra`, `cambio_mensal_compra`, `cambio_mensal_venda`, `igpm`, `igpdi`, `inpc`, `cdi`, `tjlp`, `tr`. `ipa_agricola` is series 7460 (IPA-DI by origin, agricultural products, without livestock);
the old name `ipa_agropecuario` is still accepted with a `FutureWarning` and returns `nome_serie="ipa_agricola"`.

**Output — contract 3.0:** `data` is a civil reference date with timezone-naive `datetime64[ns]` dtype; `valor` uses float64 and permits explicit nulls and negative values; `codigo` uses Int64; `nome_serie` contains a known alias or null. Empty output retains all four columns. When the body publishes `dataFim`, the end of the rate period (e.g. TR, code 226), the output gains `data_fim` (`datetime64[ns]`, optional) after them; rows without the field are null. Only fields other than `data`, `valor` and `dataFim` raise a warning. Frequency and unit depend on the series and are not inferred from date spacing.

Published references outside the requested daily bounds are preserved with a warning and per-block diagnostics. Duplicate dates within one body raise `ParseError`. Identical references and values across blocks are reconciled with every origin retained; conflicting values raise an error. The `ultimos` tail follows this union.

An empty JSON list is valid. The official HTTP404 envelope containing `SGSNegocioException: Value(s) not found` also returns empty output with a warning; it does not establish that the code exists. Other HTTP errors are not treated as missing observations. Invalid selections raise `InvalidParameterError`, network/service failures raise `SourceUnavailableError`, and incompatible data raises `ParseError`.

**Provenance:** `source_details` includes requested/effective selection, blocks, URLs, parameters, body hashes and sizes, UTC acquisition, layout, reference diagnostics, and reconciliation. `coverage.request_status="all_blocks_succeeded"` describes acquisition of all planned blocks; `completeness="unknown"` and `expected_count=None` indicate that the source provides no independent total. Observed extremes and unique count refer to the union before the tail; `returned_count` describes output. The top hash and size identify a canonical query/resources manifest, not concatenated bodies. Warnings are emitted even without `return_meta`.

```python
from agrobr import bcb

df, meta = await bcb.sgs(
    1, inicio="01/01/2010", fim="31/12/2024",
    return_meta=True,
)
recent = await bcb.sgs(1, ultimos=3)
subset = await bcb.sgs(
    1, inicio="01/01/2024", fim="31/12/2024", ultimos=30,
)
```

See the [SGS contract](../contracts/bcb_sgs.en.md), [source](../sources/bcb.en.md#sgs-time-series), and [migration guide](../guides/migracao-2.en.md).

In the semantic layer, [`datasets.series_economicas`](series_economicas.en.md) offers the same selection and reuses contract 3.0, preserving provenance. The dataset rejects `deterministic` context because a current query cannot retrieve earlier revisions.

---

### `ptax`

Published PTAX quotes and parities for one currency, with explicit bulletin selection.

```python
async def ptax(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

| Parameter | Rule |
|-----------|------|
| `data` | Single civil date: `date`, `datetime`, ISO or DD/MM/YYYY; mutually exclusive with either interval bound |
| `inicio`, `fim` | Inclusive bounds; start alone fills the end with today (Brasília civil date, UTC−3), end alone fills the start with end minus 30 days |
| No dates | Today (Brasília civil date, UTC−3) minus 30 days through today, using one reference date |
| `moeda` | Three ASCII letters; case normalized to uppercase, without whitespace trimming or name/numeric aliases; default USD |
| `boletim` | `fechamento` (default), `todos`, `abertura`, or `intermediario` |
| `top` | Strict positive integer, requested quote page size; default 1000 |
| `as_polars` | Boolean; True requires `agrobr[polars]` |
| `return_meta` | Boolean; True also returns MetaInfo |

Invalid calendars, reversed/conflicting dates, fractional/boolean page sizes, and malformed currencies fail before network access. Explicit future dates may return empty output. No supplied bound is ignored. Unknown arguments raise TypeError. The default interval is defined by bounds 30 days apart; it does not promise 30 dates or rows.

Each quote acquisition first reads the current OData currency catalogue, with its own pagination and page size 1000. A symbol missing from a non-empty catalogue raises InvalidParameterError before requesting quotes. This establishes absence from the current service catalogue, without proving historical invalidity. An unavailable or empty catalogue prevents quote validation; there is no silent currency substitution. Currency validity cannot be inferred from `value:[]`, which the source also returns for unsupported symbols.

**Output — contract 2.0:** eight columns, preserving the previous four as the prefix: `cotacao_compra`, `cotacao_venda`, `data_hora`, `data`, `moeda`, `paridade_compra`, `paridade_venda`, `tipo_boletim`. Four measures use finite nullable float64. Both dates use timezone-naive datetime64[ns]; `data` is the civil date of `data_hora`. Timestamps retain up to nine fractional digits without truncation. Currency is non-null text; bulletin type is nullable published text. Empty output keeps all columns and dtypes.

The default USD closing preserves the legacy quote values and timestamps in recent and 1994 cases. `todos` also returns opening/intermediate bulletins. The generic day route publishes `Fechamento PTAX`, while the period route publishes `Fechamento`; the closing selector recognizes both and retains the original label. This variation matters when joining day and period output. An unknown, empty, or null bulletin is retained with a warning in `todos`; a specific selector raises ParseError when classification is impossible.

**Units:** quotes use the domestic monetary unit at the reference date per unit of the selected currency. Do not label the entire historical series BRL. Type A parities use selected currency/USD; type B uses USD/selected currency. The catalogue type and applicable units are recorded in metadata; the SDK does not calculate conversions, invert parities, or recompute closing rates. Published clock time remains naive and distinct from UTC acquisition.

**Pagination and coverage:** catalogue symbols are requested ascending; quotes use timestamp and bulletin label ascending. The full page is validated before selecting bulletins. Without an independent count, short pages advance by the number received until an empty page. Duplicates, selection changes, count contradictions, and failed pages abort acquisition. There is no local row limit or automatic return of an incomplete interval.

`source_details.coverage` describes quote acquisition before and after bulletin selection; `catalog.coverage` describes the catalogue separately. An intentional bulletin filter is not truncation. `complete` requires a reconciled source count; without one, including after an empty page, coverage remains `unknown`. Known queries returned no count/nextLink annotations; if present, they are validated. No atomic revision snapshot is claimed.

**Provenance and errors:** resources are flattened in catalogue-then-quotes order, each with role, page index, URL, parameters, body hash/bytes, UTC acquisition, received/retained counts, and layout. Top-level hash/size identify a canonical UTF-8 query/resources manifest; body bytes are summed separately. HTTP/network errors raise SourceUnavailableError; malformed HTTP200 bodies, ambiguous JSON, non-finite numbers, or invalid required fields raise ParseError. Finite non-positive values or inverted bid/ask pairs remain published with diagnostics. Warnings are emitted even without metadata.

```python
from agrobr import bcb

usd = await bcb.ptax(data="04/09/2026")
eur, meta = await bcb.ptax(
    moeda="EUR", boletim="todos",
    inicio="03/09/2026", fim="06/09/2026",
    top=3, return_meta=True,
)
jpy = await bcb.ptax(moeda="JPY", boletim="intermediario", data="04/09/2026")
```

### `ptax_moedas`

```python
async def ptax_moedas(
    *, top: int = 1000, as_polars: bool = False, return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

Returns `moeda`, `nome`, and `tipo_moeda`, all non-null text, with contract 1.0 and parser 2. Symbols are unique and sorted. Names and types remain as published; unknown types produce a diagnostic instead of an invented parity unit. Empty output retains three columns. This API accepts no quote/date filters and describes the current OData catalogue, distinct from the portal's broader historical currency table.

```python
moedas, meta = await bcb.ptax_moedas(return_meta=True)
```

See [contracts and identity](../contracts/bcb_ptax.en.md), [source and license](../sources/bcb.en.md#ptax-currencies-quotes-and-bulletins), and [migration](../guides/migracao-2.en.md).

---

### `focus`

Aggregated statistics from BCB's Market Expectations System, selected by indicator and annual or monthly forecast horizon. These are survey participants' forecasts.

```python
async def focus(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

| Parameter | Rule |
|-----------|------|
| `indicador` | Exact nonempty text; default `"PIB Agropecuária"`. No case/accent normalization or invented aliases |
| `periodicidade` | `"anual"` or `"mensal"`; selects the entity and forecast horizon granularity |
| `top` | Strict positive integer; requested page size, default 1000 |
| `inicio` | Civil date (`date`, `datetime`, ISO or DD/MM/YYYY), inclusive filter on survey date; does not filter forecast horizon |
| `max_registros` | Positive integer or None; limits output after the entire received page is validated |
| `as_polars` | Boolean; True requires `agrobr[polars]` |
| `return_meta` | Boolean; True also returns MetaInfo |

Boolean/float counts, impossible dates, and unknown options are rejected. Empty output does not trigger another frequency automatically. Future dates are valid filters and may have no surveys. Units depend on the indicator and detail; the function neither converts units nor freezes historical revisions.

**Output — contract 2.0:** the original ten columns remain: `indicador`, `data`, `data_referencia`, `media`, `mediana`, `desvio_padrao`, `minimo`, `maximo`, `numero_respondentes`, `base_calculo`. The added columns are `periodicidade` and `indicador_detalhe`. Survey dates use timezone-naive civil datetime64[ns]; five statistics use nullable float64; counts/bases use nullable Int64. Detail is nullable text, retained for annual output and null for monthly output; a published empty string remains distinct from null. Empty output retains all twelve columns and dtypes.

`data` identifies the survey. `data_referencia` preserves the textual YYYY or MM/YYYY horizon, including future periods. Different calculation bases on the same date/horizon remain separate. In annual Balança comercial, Exportações, Importações, and Saldo are distinct details; do not sum or deduplicate them as a single forecast.

**Pagination and limits:** order is Data descending, DataReferencia ascending, baseCalculo ascending, and annual IndicadorDetalhe ascending. MM/YYYY tie-breaking is textual, not chronological horizon ordering. `max_registros` selects the first records in that order. A short page alone does not stop acquisition: without a total/continuation, the client advances by the received count until an empty page. Duplicate identities within/across pages, oversized responses, failed pages, and missing `value` raise errors without partial output.

`source_details.coverage` distinguishes:

| State | Evidence |
|-------|----------|
| `unknown` | No independent total; observed termination or an exact local limit without evidence of remaining data |
| `partial` | Local limit below a declared total, or discarded rows/continuation prove additional data |
| `complete` | Declared count reconciled with unique identities and full returned output |

Known queries returned no count: `$count=true` added no total and `/$count` was refused. Count and continuation annotations are validated if present. A count also does not guarantee an atomic revision across pages.

**Provenance and quality:** selection, entity, filter, order, URLs, offsets, requested/received/retained sizes, status, body hashes, UTC acquisition, and layout are in `source_details`. The top hash/size identify a canonical query/resources manifest. Local limits and statistical inconsistencies emit warnings even without `return_meta`. Finite negative values are valid; inconsistent mean, median, bounds, or deviation remain published with diagnostics. Missing values do not become zero; invalid JSON, nonfinite values, or invalid required fields raise `ParseError`. Empty output does not establish that an indicator exists.

```python
from agrobr import bcb

annual = await bcb.focus(
    "Balança comercial", inicio="2026-08-28", max_registros=6,
)
monthly, meta = await bcb.focus(
    "IPCA", periodicidade="mensal",
    inicio="2026-08-28", top=100, max_registros=30, return_meta=True,
)
```

See [contract and identity](../contracts/bcb_focus.en.md), [source and license](../sources/bcb.en.md#focus-market-expectations), and [migration guide](../guides/migracao-2.en.md).

---

## Synchronous Version

```python
from agrobr.sync import bcb

df = bcb.credito_rural("soja", safra="2024/25")
serie = bcb.sgs("ipca", inicio="01/01/2024")
cambio = bcb.ptax(inicio="01/01/2024", fim="31/01/2024")
expectativas = bcb.focus("PIB Agropecuária")
```

## Fallback

When the SICOR OData API fails, agrobr automatically uses BigQuery (Base dos Dados) as a fallback, only in `credito_rural` with `agregacao="uf"` and without `programa` or `tipo_seguro` (the table does not carry those dimensions). Requires `pip install agrobr[bigquery]` and a GCP project for billing: set `AGROBR_BQ_BILLING_PROJECT=<project-id>` or configure `billing_project_id` in basedosdados (`~/.basedosdados/config.toml`).

## Notes

- Source: [BCB/SICOR](https://olinda.bcb.gov.br) — free license
- Data available from 2013
- Contract v2.0 — output aligned with the actual SICOR aggregations

`inicio` and `fim` replace the old period names without aliases. They accept ISO, DD/MM/YYYY, `date` and `datetime`; time is discarded and `01/02/2024` means February 1. Focus only uses `inicio`; PTAX keeps `data` for a single day. `as_polars` and `return_meta` require keyword arguments. Focus periodicity and PTAX bulletin selectors normalize case; the bulletin also accepts accents. SICOR envelopes without `value` or with an incorrect type raise `ParseError`; `value=[]` remains a typed empty result.
