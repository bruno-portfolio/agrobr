# UN Comtrade API

Bilateral merchandise trade by HS and a mirror of exports against reverse imports. Source, mirror and dataset use contract **2.0**, parser **2**.

## Bilateral query

```python
from agrobr import comtrade

df, meta = await comtrade.comercio(
    "1201,1005,0901,1701,2304",
    reporter="BR", partner="all", fluxo="X", periodo=2023, freq="A",
    require_complete=True, return_meta=True,
)
print(meta.source_details["coverage"])
```

| Argument | Default | Meaning |
|---|---|---|
| `produto: str` | required | Alias from `produtos()`, ASCII HS with 2/4/6 digits, or a comma-separated string of those codes |
| `reporter: str` | `"BR"` | Known alias or positive numeric country code as text |
| `partner: str \| None` | `None` | None/world/mundo/"0": explicit World aggregate; all/todos: all published partners |
| `fluxo: str` | `"X"` | Exports X or imports M |
| `periodo: str \| int \| None` | previous UTC year | Year/month, homogeneous list or inclusive range |
| `freq: str` | `"A"` | Annual A or monthly M |
| `api_key: str \| None` | `None` | Nonblank key; None reads `AGROBR_COMTRADE_API_KEY` |
| `require_complete: bool` | `False` | True rejects partial/unknown coverage; False keeps records and emits a warning |
| `as_polars: bool` | `False` | Converts validated output to Polars when the extra is installed |
| `return_meta: bool` | `False` | Returns `(DataFrame, MetaInfo)` |

Repeated selections are normalized; returned records are never silently deduplicated. Do not add World to other partners or aggregate HS to its descendants. Unknown arguments, boolean periods, odd-length HS, invalid flows and reversed ranges fail before network access.

## Product aliases

Each alias sums the HS codes in the table. The same alias means the same thing in [ComexStat](comexstat.md): where a
period's HS does not separate the product, the smallest code that contains it is used. Descriptions checked against the
official Comtrade references H0 to H6 (2026-09-25).

| Alias | HS | Not included |
|---|---|---|
| `soja` | `120190` (since HS 2012) and `120100` (up to 2011) | seed `120110`; up to 2011 the HS does not separate seed (0.01% of the value in 2025) |
| `complexo_soja` | `soja` + `farelo_soja` + `oleo_soja` | — |
| `farelo_soja` | `2304` | — |
| `oleo_soja` | `1507` | — |
| `milho` | `1005` | — |
| `arroz` | `1006` | — |
| `trigo` | `1001` (wheat and meslin) | — |
| `cafe` | `090111`, `090112`, `090121`, `090122` | husks, skins and substitutes (`090190`; `090130`/`090140` in HS 1992) |
| `acucar` | `1701` | — |
| `etanol` | `2207` | — |
| `algodao` | `5201` and `5203` (raw, carded or combed) | waste `5202`; yarn and fabrics |
| `carne_bovina` | `0201` and `0202` | — |
| `carne_frango` | `020711` to `020714` (fowls, *Gallus domesticus*) | turkeys, ducks, geese and guinea fowls (US$ 212 million in 2025); periods before 1996 raise `InvalidParameterError`, and so does 1996 when the reporter is Brazil, which reported in HS 1992 |
| `carne_suina` | `0203` | — |
| `celulose` | `4703` (chemical pulp, soda or sulphate) | dissolving grades `4702` (US$ 1.08 billion in 2025, 10.5% of pulp), mechanical `4701`, sulphite `4704`, semi-chemical `4705` and other fibres `4706` |
| `tabaco` | `2401` (unmanufactured and refuse) | cigars and cigarettes `2402`, manufactured `2403` and products for inhalation `2404` |
| `suco_laranja` | `200911`, `200912`, `200919` | juices of other fruits and vegetables (US$ 357 million in 2025) |

Before 1996 (HS 1992), Comtrade does not separate fresh chicken meat from other poultry, and the code of frozen chicken
cuts (`020741`) became ducks in HS 2012. For those years, request the codes explicitly (`"020721,020741"`).

The guard follows the classification the country reported for the year, not the calendar. Brazil reported 1996 in HS 1992 (H0), where none of the four codes exists: `comercio("carne_frango", periodo=1996)` raises `InvalidParameterError` with the hint above, instead of returning empty with complete coverage. Brazil's classification comes from Comtrade's official availability (H0 in 1996, H1 from 1997 to 2001, H2 from 2002 to 2006, H3 from 2007 to 2011, H4 from 2012 to 2016, H5 from 2017 to 2021 and H6 from 2022 to 2024). For other countries, the classification of each year is not known before the query, and the guard applies only before 1996. The other aliases have at least one code in every classification Brazil reported: `soja` and `complexo_soja` until 2011 with only `120100`, and `suco_laranja` until 2001 without `200912`, are partial coverage.

## Periods

| Frequency | Accepted examples |
|---|---|
| A | `2023`, `"2022,2023"`, `"2021-2023"` |
| M | `202301`, `"202301,202303"`, `"202211-202302"` |
| M, full year | `2023`, `"2022-2023"` expand all months |

Calendar validity does not establish source availability. The previous year is a selection default, not a promise of complete publication. Preview requests contain one period each; annual totals are not reconstructed from monthly data.

## Coverage and acquisition

The client requests `countOnly=true` with the same filters as each initial partition and compares it to the data union. The regular response count describes returned rows. The [official preview](https://uncomtrade.org/docs/what-is-data-preview/) caps a response at 500 records. Missing rows trigger disjoint subdivision of requested periods and HS codes; parent evidence is retained and only leaves contribute records.

A minimal partition may remain partial. There is no offset pagination or automatic enumeration of every partner. HTTP errors, invalid envelopes, conflicting dimensions and parent/child revisions interrupt acquisition. Coverage describes this collection and its independent count; it does not establish an atomic revision snapshot.

Without a configured key, the public preview is used. Authenticated transport requests up to 100,000 records and plans up to 12 periods per block; its effective limit was not verified using a real key in this increment. An authenticated 401/403 restarts the entire plan in preview and preserves discarded attempts. Account quotas are not guaranteed by agrobr.

## Columns and metadata

The previous 22 columns remain, adding `classificacao`, `classificacao_original` and, in contract 2.1, the 3 UN estimation flags (`peso_liquido_estimado`, `peso_bruto_estimado`, and `quantidade_estimada`), for **27** total. Integers use `Int64`, measures `float64`, and nullable flags `boolean`. The reported revision, such as H6, is retained; HS in the URL is an alias. ISO labels and names may be null. See the [complete contract](../contracts/comercio_internacional.md).

`MetaInfo` includes schema/contract 2.1 (2.0 for the mirror), the actual guest/authenticated channel, UTC acquisition time and warnings. `source_details` contains query, resources, coverage, fallback and parser/layout diagnostics. Resources retain URLs, SHA256 and sizes. `raw_content_hash` hashes a canonical UTF-8 JSON manifest of query and resources; `raw_content_size` measures that manifest, while `resource_bytes` sums response bodies. Keys are excluded from metadata.

Resources describe each logical request's final response and discarded partitions. Intermediate HTTP retry bodies are not retained in API metadata; the validation runner captures every GET, including recovered 429 responses.

## Trade mirror

```python
df, meta = await comtrade.trade_mirror(
    "soja", reporter="BR", partner="CN", periodo=2023,
    require_complete=True, return_meta=True,
)
```

Accepts bilateral arguments except `fluxo`; partner defaults to CN and must identify a positive country distinct from reporter. The outer join is 1:1 by period/HS between exports and reverse imports. The previous 18 columns gain revision and original-classification flags per leg, plus numeric reporter/partner codes, for **24** total.

`ratio_valor` divides reporter FOB by partner CIF; `ratio_peso` divides their weights. No normal range is guaranteed. Missing inputs or zero denominators yield null. Different HS revisions in the same cell raise an error; this function does not harmonize classifications.

When a leg comes back empty (China ← Brazil for soybean in April 2025, with guest access), the partner columns, `diff_*` and `ratio_*` come out null, not zero, with no warning and with `complete` coverage. Each leg's count is in `meta.source_details["coverage"]["legs"]` (`received_count` and `expected_count`), and the nulls are in `meta.source_details["parsing"]["null_counts"]`.

Both acquisitions remain under `source_details["legs"]`, and the cells with a UN-estimated net weight, per leg, under `source_details["peso_estimado"]`: `ratio_peso` uses the published weight, estimated or not. Joint coverage is complete only if both legs are complete. Different channels are identified as `comtrade_mixed`.

## Dataset, sync and catalogs

`datasets.comercio_internacional(...)` supports the bilateral selectors, multiple textual HS, completeness and Polars, preserving the contract and provenance. A deterministic snapshot only fills an omitted year; it does not freeze source revisions.

```python
from agrobr.sync import comtrade

df = comtrade.comercio("soja", partner="world", periodo=2023, require_complete=True)
```

`paises()` lists ISO aliases in the local map, not a dynamic worldwide catalog. `produtos()` returns a copy of agricultural aliases and HS selections. The internal license category is `zona_cinza`, with a first-call warning; see [verified terms](../licenses.md#un-comtrade).
