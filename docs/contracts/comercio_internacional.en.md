# Contract: comercio_internacional

**Version 2.1**, shared with `comtrade.comercio()` and the `comercio_bilateral` registry entry. Implementation: `agrobr.contracts.comtrade.COMERCIO_BILATERAL_V2`. V1 remains historical in `contracts.datasets`.

## Schema

All **27 columns** are stable, including empty output.

| Column | Pandas dtype | Nullable | Unit | Bounds |
|---|---|---|---|---|
| `periodo` | `str` | No | — | — |
| `ano` | `Int64` | No | — | >= 1, <= 9999 |
| `mes` | `Int64` | Yes | — | >= 1, <= 12 |
| `reporter_code` | `Int64` | No | — | >= 1 |
| `reporter_iso` | `str` | Yes | — | — |
| `reporter` | `str` | Yes | — | — |
| `partner_code` | `Int64` | No | — | >= 0 |
| `partner_iso` | `str` | Yes | — | — |
| `partner` | `str` | Yes | — | — |
| `fluxo_code` | `str` | No | — | — |
| `fluxo` | `str` | Yes | — | — |
| `hs_code` | `str` | No | — | — |
| `produto_desc` | `str` | Yes | — | — |
| `nivel_hs` | `Int64` | No | — | >= 2, <= 6 |
| `peso_liquido_kg` | `float64` | Yes | kg | >= 0 |
| `peso_bruto_kg` | `float64` | Yes | kg | >= 0 |
| `volume_ton` | `float64` | Yes | ton | >= 0 |
| `valor_fob_usd` | `float64` | Yes | USD | >= 0 |
| `valor_cif_usd` | `float64` | Yes | USD | >= 0 |
| `valor_primario_usd` | `float64` | Yes | USD | >= 0 |
| `quantidade` | `float64` | Yes | — | >= 0 |
| `unidade_qtd` | `str` | Yes | — | — |
| `classificacao` | `str` | No | — | — |
| `classificacao_original` | `boolean` | Yes | — | — |
| `peso_liquido_estimado` | `boolean` | Yes | — | — |
| `peso_bruto_estimado` | `boolean` | Yes | — | — |
| `quantidade_estimada` | `boolean` | Yes | — | — |

**Primary key:** `periodo, reporter_code, partner_code, hs_code, fluxo_code, classificacao`.

Period is annual YYYY or monthly YYYYMM, coherent with year/month. HS level equals its 2/4/6 ASCII digit length, preserving leading zeros. Flow is X/M; classification retains the reported Hn revision. ISO labels and names are optional descriptions and are not replaced by fabricated codes. Measures are finite and missing values remain null.

The 3 estimation flags (2.1) come from the UN (`isNetWgtEstimated`, `isGrossWgtEstimated`, and `isQtyEstimated`). `True` means the published measure is a UN estimate, not the value declared by the country: for Brazil's chicken in 2024, the net weight of HS 020714 is estimated. When the result has an estimated net weight, `meta.validation_warnings` names the HS and period, and `peso_liquido_kg` and `volume_ton` carry the estimated value. `trade_mirror` lists those cells per leg in `source_details["peso_estimado"]`.

## Selection and provenance

`partner=None/world/mundo/"0"` selects the explicit World aggregate. `partner="all"/"todos"` preserves all published partners. Do not add aggregate rows to their components. Product accepts agricultural aliases or textual HS, including comma-separated codes.

`require_complete=True` requires independent count and disjoint-union evidence. False permits partial output with a warning; HTTP, layout and identity failures interrupt collection. Complete describes the requested slice, without promising final publication for the reporter.

The dataset preserves actual channel, query, resources, hashes, UTC acquisition, coverage and warnings. The top hash and size identify a resource manifest, not a single response body. Deterministic snapshot only supplies an omitted year and does not freeze revisions. Polars conversion follows validation.

```python
from agrobr import datasets

df, meta = await datasets.comercio_internacional(
    "1201,1005,0901,1701,2304", partner="all", periodo=2023,
    require_complete=True, return_meta=True,
)
```

## Related mirror

`TRADE_MIRROR_V2` registers `trade_mirror` with 24 columns and primary key `periodo, hs_code, reporter_code, partner_code`. It preserves the previous 18 columns, adding `classificacao_reporter`, `classificacao_partner`, `classificacao_original_reporter`, `classificacao_original_partner` and both numeric country codes.

The outer 1:1 join compares exports and reverse imports. Incompatible HS revisions fail; a missing leg remains null. Ratios with zero or missing denominators are null. See the [API](../api/comtrade.md).

## Relationship to ComexStat

| Aspect | comercio_internacional | exportacao / importacao |
|---|---|---|
| Source | UN Comtrade | ComexStat/MDIC |
| Scope | Bilateral, subject to reporter availability | Brazil |
| Classification | HS | NCM |
| Geography | Numeric country codes | Destination/origin country and Brazilian state |

The internal Comtrade license category is `zona_cinza`; see [Licenses](../licenses.md#un-comtrade) and [migration](../guides/migracao-2.md).
