# ANEC — Shipments, Monthly Volumes and Destinations

> **License:** `zona_cinza`. Public reports without explicit public reuse terms located.

!!! warning "Gray-area status"
    The first ANEC call emits a `UserWarning`. Commercial use or redistribution may require authorization from ANEC. Public access does not establish permission for commercial redistribution.

ANEC (Associação Nacional dos Exportadores de Cereais) publishes PDF reports with weekly shipments by port, monthly volumes, annual comparisons and destination shares.

## Coverage and report selection

- **Catalog:** editions from **2026 through the current year**, with annual categories discovered in the official catalogue. On 6 September 2026, the 2026 public catalogue contained 34 PDFs; the latest was W34, created on 2 September 2026. Publication and layout compatibility in later years are not guaranteed.
- **`ano`:** required, keyword-only report edition year. An edition can include data from another year; this parameter does not request a historical year independently of the edition.
- **`semana`:** report week, from 1 to 53; `None` selects the latest available edition in the selected catalog. A report's week number is not its publication date.
- **Products:** soybean, soybean meal, maize, DDGS, sorghum and wheat, according to the selected table. Annual comparisons may also include `total_products`. Up to W2/2026, the weekly and monthly tables publish only soybean, soybean meal, maize and wheat; DDGS and sorghum start in W3/2026.
- **Weekly ports:** 19 recognized Brazilian ports, including Santos, Paranaguá and Rio Grande.
- **Dependency:** `pip install agrobr[pdf]`.

## API

```python
from agrobr import anec, datasets

# Weekly API and dataset: schema 1.1, with year, week and the dates of each period
df = await anec.embarques(ano=2026, semana=13, porto="paranagua", produto="soja")
df = await anec.embarques(ano=2026, tipo="efetivado")  # or "programado"

# Source schemas: monthly/destinations 1.1; annual comparison 1.2
monthly = await anec.embarques_mensais(ano=2026, semana=34, produto="soja")
comparison = await anec.comparacao_anual(ano=2026, produto="total_products")
destinations = await anec.destinos(ano=2026, semana=34, produto="soja")

# New semantic datasets: contracts 1.0
monthly, meta = await datasets.embarques_mensais_anec(
    ano=2026, semana=34, produto="soja", return_meta=True
)
comparison = await datasets.comparacao_anual_anec(ano=2026)
destinations, meta = await datasets.destinos_anec(ano=2026, return_meta=True)
print(meta.validation_warnings)

items = await anec.articles_disponiveis(2026)
```

The three new datasets' main parameters are keyword-only: required `ano: int`, `semana: int | None = None`, `produto: str | None = None`, `use_cache: bool = True`, `as_polars: bool = False` and `return_meta: bool = False`. `as_polars=True` requires `pip install agrobr[polars]`. Comparison also accepts `produto="total_products"`. `produto=None` selects all products available in that table.

## Weekly schema — `embarques()`

| Column | Type | Meaning |
|---|---|---|
| `porto` | str | Canonical port, uppercase |
| `produto` | str | Canonical product |
| `periodo` | str | `last_week` (efetivado) or `current_week` (programado) |
| `valor_ton` | Float64, nullable | Volume in tonnes |
| `ano` | Int64 | Edition year printed on the bulletin ("Week 36/2026") |
| `semana` | Int64 | Edition week printed on the bulletin |
| `data_inicio` | datetime | First day of the period, read from the bulletin label |
| `data_fim` | datetime | Last day of the period, read from the bulletin label |

The [`embarques_anec` contract](../contracts/embarques_anec.md) is 1.1, and the four columns above are optional in it. Dates come from the bulletin labels, such as "Last Week (13th to 19th Sep)", never from ISO weeks. The bulletin for week w carries week w in `last_week` and the next one in `current_week`, and the ANEC week runs from Sunday to Saturday. When a label crosses a month, the bulletin sometimes gives the month of the end ("26th to 01st Aug" is 26/07 to 01/08, in W30/2026) and sometimes the month of the start ("30th to 05th Aug" is 30/08 to 05/09, in W34/2026). agrobr keeps the reading in which the two weeks follow each other and the start of `last_week` falls within 7 days of the edition week (January 1 plus week − 1 weeks), and the year at the December turn comes from the edition. When no reading meets both conditions, `data_inicio` and `data_fim` are null, with a `UserWarning` and the same message in `MetaInfo.validation_warnings`. W35/2026 prints August in both weeks, which are in September ("30th to 05th Aug" and "06th to 12th Aug"): the only consecutive reading, 30/07 to 12/08, is 28 days from the edition, and the dates come out null.

To join editions, use `ano`, `semana` and `periodo`, or the dates: the same week comes as `current_week` (scheduled) in one edition and as `last_week` (shipped) in the next. The table's TOTAL row is not part of the result and may differ by 1 t from the sum of the ports (3 cases in W30/2026 and 1 in W36/2026).

## Monthly volumes — `embarques_mensais()`

The columns `ano`, `mes`, `produto`, `valor_ton` and `eh_estimativa` are retained. The source adds nullable `valor_min_ton` and `valor_max_ton`, plus report provenance below.

Volumes are **monthly tonnes**, not cumulative across months. A published range keeps both bounds with a null `valor_ton`; no midpoint is fabricated. Missing values do not mean zero. Estimate markers are preserved; `eh_estimativa=False` does not guarantee a realized volume. A month without "*" can still be revised: soybeans for July 2026 came as 12,182,916 t in bulletin 30, 12,040,279 t in bulletins 31 to 34 and 12,108,521 t in 35 to 37, always without "*". Shipped weeks do not close the month: prorated by day, soybean weeks add up to 1.5% (July) and 3.4% (August) less than the published monthly figure, and maize weeks to 0.4% more and 2.5% less.

The 2025 monthly chart on page 3 of W34/2026 is not extracted by the current monthly table parser. The presence of 2025 data in `comparacao_anual()` does not imply 2025 coverage in `embarques_mensais()`.

See the complete [`embarques_mensais_anec` contract](../contracts/embarques_mensais_anec.md).

## Annual comparison — `comparacao_anual()`

Source schema **1.2** includes `valor_base_ton`, `valor_comparacao_ton`, `ano_base`, `ano_comparacao` and `eh_estimativa`, alongside `mes`, `produto` and legacy `valor_2025`/`valor_2026`. Both the source API and dataset expose the year-independent volumes. Legacy fields refer to literal years and remain null when that year is absent.

Years come from consecutive pairs in the table header. `eh_estimativa` refers to the comparison year. Editorial lines are ignored, while incomplete or inconsistent headers raise `ParseError`. Values are monthly tonnes; `total_products` is a published aggregate and must not be added to individual products again. See the [dataset contract](../contracts/comparacao_anual_anec.md).

## Destinations — `destinos()`

The source retains `produto`, `destino` and nullable `share_pct` (0–100). It adds nullable `ano`, `mes_inicio` and `mes_fim` from the period header.

Shares describe the **cumulative period in the header**, not monthly tonnage. W34/2026 reports January–July 2026. Missing period information stays null; it is not inferred from the report week. `OTHERS` is valid, and rounded shares can sum to 99% or 101%.

Some editions use charts that the parser does not extract, such as W08 and W12 of 2026. When no destination share is extracted from the edition, the result includes a notice in `meta.validation_warnings`. An empty result does not establish an absence of shipments or destinations. Request `return_meta=True` to inspect this limitation.

See the complete [`destinos_anec` contract](../contracts/destinos_anec.md).

## Edition and revision provenance

All three source tables (monthly/destinations 1.1; annual comparison 1.2) and new datasets (contracts 1.0) include:

| Column | Meaning |
|---|---|
| `ano_relatorio` | Report edition year |
| `semana_relatorio` | Report edition week |
| `edicao_id` | ANEC article `cuid` |
| `publicado_em` | Article `created_at`, UTC datetime |
| `revisado_em` | File `media_updated_at`, UTC datetime |

Dataset keys include `edicao_id` and `revisado_em`. Preserve them when storing report snapshots to avoid overwriting earlier projections. File updates may precede article creation; these timestamps describe different source objects.

## Accepted product aliases

| Input | Canonical |
|---|---|
| `soja`, `soja grão`, `soja grao`, `soja em grão`, `soja em grao`, `soybean`, `soybeans` | `soybean` |
| `farelo`, `farelo de soja`, `soybean meal`, `soybean_meal`, `soybeanmeal`, `soymeal`, `meal` | `soybean_meal` |
| `milho`, `maize`, `corn` | `maize` |
| `trigo`, `wheat` | `wheat` |
| `sorgo`, `sorghum` | `sorghum` |
| `ddgs` | `ddgs` |

## agrobr product names

| ANEC code (`produto`) | Product | agrobr name |
|---|---|---|
| `soybean` | soybeans | `soja` |
| `soybean_meal` | soybean meal | `farelo_soja` |
| `maize` | corn | `milho` |
| `wheat` | wheat | `trigo` |
| `sorghum` | sorghum | `sorgo` |
| `ddgs` | DDGS (distillers dried grains) | none |

`produto` keeps the ANEC code in English. The agrobr name is the canonical one in `normalize.crops`, the same as in `exportacao` and `estimativa_safra`; `normalizar_cultura` converts each code into the name in the table, and `ddgs` stays as it is.

## Cache

PDF cached in `~/.agrobr/cache/anec/{year}/week_{NN}/` with:
- `shipment.pdf` — PDF bytes
- `meta.json` — metadata + SHA256 + `media_updated_at` from the source

Where it lives and how to clean it: [What agrobr writes to disk](../advanced/disco.md).

Each write uses its own temporary file and atomically replaces the destination.
Cleanup removes only that operation's temporary file; temporary files from other
calls or previous runs are preserved.

Stale detection compares cached `media_updated_at` vs ANEC. ANEC revises
retroactively: an old cache invalidates automatically when the remote
`updated_at` advances.

The parsed report cache also identifies revisions by article, `media_updated_at`,
URL and SHA-256 of the received bytes. A revision refreshes both the data and
the provenance URL, even when the article keeps the same `cuid`.

The JSON listing is cached in memory for 5 minutes (configurable via
`AGROBR_ANEC_LIST_TTL`). The PDF disk cache can be disabled via
`AGROBR_ANEC_CACHE_DISABLED=1`.

When the PDF comes from the disk cache, the `MetaInfo` carries `from_cache=True` and, in `fetched_at`, the
original collection recorded in `meta.json`, not the call time; `source_details["media_updated_at"]` is the
edition revision checked against the listing. On a download, `from_cache=False` and `fetched_at` is the download.

## MetaInfo

```python
df, meta = await anec.embarques_mensais(ano=2026, return_meta=True)
print(meta.source)              # "anec"
print(meta.schema_version)      # "1.1" for this source API; "1.0" for the dataset
print(meta.source_url)          # Selected PDF
print(meta.validation_warnings)
```

`raw_content_hash` is the full SHA-256 of the PDF, and `raw_content_size` is its size in bytes, from the download or from the disk cache (the same `pdf_sha256` as in `meta.json`). The PDF layout fingerprint (MD5 of the structure) is in `source_details["layout_fingerprint"]`.

## Limits and official examples

PDF layouts and published product sets vary between editions. ANEC revises figures retroactively; using the latest report does not reconstruct all past projections. Historical snapshots require preserving individual editions and revisions.

The monthly and annual-comparison tables are not interchangeable: in W13/2026, for example, January wheat is 279,699 t in the monthly table and 279,499 t in the comparison. agrobr preserves the values published in each table without automatic reconciliation. The total printed on the January row of the monthly table (7,727,420 t) matches the comparison values (wheat 279,499 t and DDGS 80,057 t), not the monthly table's own values (279,699 t and 80,141 t), which add up to 284 t more (W35 and W37/2026).

Official PDFs cited on this page: [W13/2026](https://www.anec.com.br/uploads/cmnrsz3eu00004htx396a7you.pdf) and [W34/2026](https://www.anec.com.br/uploads/cmtkhj4p50000y9tx5nnn6hhi.pdf). Source: [ANEC](https://www.anec.com.br/).

Annual categories not in the configured map are discovered from the official catalogue. Explicit years are exclusive: missing publication does not fall back to a previous year. Empty year results share the listing TTL (default 300 seconds, `AGROBR_ANEC_LIST_TTL=0` disables caching). Supported edition years run from 2026 through the current year; catalogue discovery does not itself guarantee a compatible PDF layout.

## Reading the bulletins

Each table's independently published values are preserved. Metric tons are literal;
blanks and dashes remain missing. Displayed destination shares may sum to 99%,
101% or 102% because published percentages are rounded; agrobr does not
redistribute them to force 100%.

The parser accepts both published headers: the six products in both weekly
periods (plus Total Products in the monthly table), from W3/2026 on, and the four
of the editions up to W2/2026, read by name. A column without a product name, such
as the soybean one in the current week of W14/2026, raises `ParseError`: agrobr
does not guess the product. An incomplete header also raises `ParseError`; the
dataset preserves its cause in `SourceUnavailableError`. The sum of the ports in
each weekly column is checked against the bulletin's TOTAL row: a difference above
rounding (0.5 t per port) becomes a warning (`UserWarning` and
`meta.validation_warnings`), and the per-port values are passed on unchanged.
pdfplumber sometimes splits a number into 2 adjacent words (`3` and `72.958`, in the
TOTAL row of [W36/2026](https://www.anec.com.br/uploads/cmu47h0m500016vtxg76q4eeb.pdf)) and, in the small 2025 font, merges BELÉM with the RIO of the
row below. The parser joins 2 neighbouring numeric words when they form a number
with a thousands separator and reads words with a 1 pt vertical tolerance. In 89
editions (W1/2025 to W37/2026, except W14/2026), all 16,112 cells of the weekly
table match the PDF and no warning is raised; W25/2026 published
the TOTAL row blank, so there is nothing to check there. In
W1 and W2/2026, the "Monthly shipments 2026" table is on the second page and is not
read: `embarques_mensais` for those editions returns only 2025, and January 2026
appears in the following bulletins. Destination parsing
is confined to the table beside the map, so map percentage labels cannot become
destinations. Editions 08 and 12/2026 contain raster destination panels without
extractable text tables: an empty result with a warning remains a limitation and
does not establish absence of shipments.

Future layouts are not guaranteed, nor is equivalence between ANEC shipments and
ComexStat customs exports.
From January to August 2026, the two series diverge month by month by up to 49% (maize: −44% in April and +49% in
July) and close within 2.5% cumulatively (soybeans +1.6%, soybean meal +0.2% and maize −2.4%).
