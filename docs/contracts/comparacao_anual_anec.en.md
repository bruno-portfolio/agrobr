# comparacao_anual_anec v1.0

Comparison of monthly volumes for the two years published in the same ANEC edition.

## Source

| Priority | Source | Method |
|---|---|---|
| 1 | ANEC | PDF (`anec.comparacao_anual`) |

Requires `pip install agrobr[pdf]`. See the [source documentation](../sources/anec.md).

## Schema

| Column | Type | Description |
|---|---|---|
| `mes` | int | Month (1–12) |
| `produto` | str | Canonical product or `total_products` aggregate |
| `ano_base` | int | Baseline year |
| `ano_comparacao` | int | Comparison year |
| `valor_base_ton` | Float64, nullable | Baseline monthly volume, in tonnes |
| `valor_comparacao_ton` | Float64, nullable | Comparison monthly volume, in tonnes |
| `eh_estimativa` | bool | Estimate flag applying to `ano_comparacao` |
| `ano_relatorio` | int | Selected report edition year |
| `semana_relatorio` | int | Report edition week (1–53) |
| `edicao_id` | str | ANEC article `cuid` |
| `publicado_em` | datetime UTC | Article `created_at` |
| `revisado_em` | datetime UTC | Source file `media_updated_at` |

**Primary key:** `[edicao_id, revisado_em, ano_base, ano_comparacao, mes, produto]`.

The key preserves separate editions and revisions when appending report snapshots. `revisado_em` is the file update timestamp supplied by ANEC; it need not be later than the article creation time. Nullable columns always exist, even when their values are missing.

## Meaning and limits

Years come from the published table; the call's `ano` selects the edition, not a historical interval. `eh_estimativa` applies only to the comparison-year value. Missing values remain null. `total_products` is a published aggregate: do not add it to the individual products again. Products and aggregate availability vary by edition.

## Products

| ANEC code (`produto`) | Product | agrobr name |
|---|---|---|
| `soybean` | soybeans | `soja` |
| `soybean_meal` | soybean meal | `farelo_soja` |
| `maize` | corn | `milho` |
| `wheat` | wheat | `trigo` |
| `sorghum` | sorghum | `sorgo` |
| `ddgs` | DDGS (distillers dried grains) | none |
| `total_products` | published table total | none |

`produto` keeps the ANEC code in English. The agrobr name is the canonical one in `normalize.crops`, the same as in `exportacao` and `estimativa_safra`; `normalizar_cultura` converts each code into the name in the table, and `ddgs` stays as it is.

## Parameters

```python
async def comparacao_anual_anec(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]: ...
```

All arguments are keyword-only. `ano` is required and selects the **report edition year**, from 2026 through the current year. Annual categories are discovered in the official catalogue; an explicit year without publication raises `SourceUnavailableError` without returning another year's edition. `semana=None` selects the latest available report in that catalogue. `produto=None` returns all available products; aliases such as `soja` and `milho` are accepted. `produto="total_products"` selects the published aggregate when present.

## Example

`as_polars=True` returns a Polars DataFrame and requires `pip install agrobr[polars]`.

```python
from agrobr import datasets

df, meta = await datasets.comparacao_anual_anec(
    ano=2026, semana=34, produto="soja", return_meta=True
)
print(df.head())
print(meta.source_url)
```

## JSON schema

`agrobr/schemas/comparacao_anual_anec.json`, also available through `get_contract("comparacao_anual_anec")`.

## License

`zona_cinza`: public reports without explicit public reuse terms located. The first ANEC call emits a warning. Commercial use or redistribution may require ANEC authorization; publication on the website does not establish permission for commercial redistribution.

## Bulletin verification

Verification of editions 04, 08, 12, 13, 34 and 36/2026 includes every month and product actually published: edition 04 has no annual sorghum panel. Total Products is a separate series; annual totals and difference bars do not become months. January wheat and DDGS may differ from the monthly table in the same PDF; the published tables are not forced to agree.
