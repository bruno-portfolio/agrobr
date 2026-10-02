# embarques_mensais_anec v1.0

Monthly volumes by product as published in one ANEC report edition, including estimates and published ranges.

## Source

| Priority | Source | Method |
|---|---|---|
| 1 | ANEC | PDF (`anec.embarques_mensais`) |

Requires `pip install agrobr[pdf]`. See the [source documentation](../sources/anec.md).

## Schema

| Column | Type | Description |
|---|---|---|
| `ano` | int | Volume reference year |
| `mes` | int | Month (1–12) |
| `produto` | str | Canonical product |
| `valor_ton` | Float64, nullable | Monthly volume in tonnes; null for a range without a point value |
| `valor_min_ton` | Float64, nullable | Published range lower bound, in tonnes |
| `valor_max_ton` | Float64, nullable | Published range upper bound, in tonnes |
| `eh_estimativa` | bool | Whether the report marks the month as an estimate |
| `ano_relatorio` | int | Selected report edition year |
| `semana_relatorio` | int | Report edition week (1–53) |
| `edicao_id` | str | ANEC article `cuid` |
| `publicado_em` | datetime UTC | Article `created_at` |
| `revisado_em` | datetime UTC | Source file `media_updated_at` |

**Primary key:** `[edicao_id, revisado_em, ano, mes, produto]`.

The key preserves separate editions and revisions when appending report snapshots. `revisado_em` is the file update timestamp supplied by ANEC; it need not be later than the article creation time. Nullable columns always exist, even when their values are missing.

## Meaning and limits

Values are monthly, not cumulative across months. A published range keeps its bounds and leaves `valor_ton` null: no midpoint is calculated. Missing values do not mean zero. The report's estimate marker is preserved; it must not be inferred solely from the query date. `eh_estimativa=False` means the marker is absent, without guaranteeing a realized volume.

## Products

| ANEC code (`produto`) | Product | agrobr name |
|---|---|---|
| `soybean` | soybeans | `soja` |
| `soybean_meal` | soybean meal | `farelo_soja` |
| `maize` | corn | `milho` |
| `wheat` | wheat | `trigo` |
| `sorghum` | sorghum | `sorgo` |
| `ddgs` | DDGS (distillers dried grains) | none |

`produto` keeps the ANEC code in English. The agrobr name is the canonical one in `normalize.crops`, the same as in `exportacao` and `estimativa_safra`; `normalizar_cultura` converts each code into the name in the table, and `ddgs` stays as it is.

## Parameters

```python
async def embarques_mensais_anec(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]: ...
```

All arguments are keyword-only. `ano` is required and selects the **report edition year**, from 2026 through the current year. Annual categories are discovered in the official catalogue; an explicit year without publication raises `SourceUnavailableError` without returning another year's edition. `semana=None` selects the latest available report in that catalogue. `produto=None` returns all available products; aliases such as `soja` and `milho` are accepted.

## Example

`as_polars=True` returns a Polars DataFrame and requires `pip install agrobr[polars]`.

```python
from agrobr import datasets

df, meta = await datasets.embarques_mensais_anec(
    ano=2026, semana=34, produto="soja", return_meta=True
)
print(df.head())
print(meta.source_url)
```

## JSON schema

`agrobr/schemas/embarques_mensais_anec.json`, also available through `get_contract("embarques_mensais_anec")`.

## License

`zona_cinza`: public reports without explicit public reuse terms located. The first ANEC call emits a warning. Commercial use or redistribution may require ANEC authorization; publication on the website does not establish permission for commercial redistribution.

## Reading the bulletins

The header must contain the six products (or the four of the editions up to W2/2026) plus Total Products; a missing column interrupts extraction. Total Products and annual totals do not become product observations. Every month is preserved, including empty December cells, and so are intervals such as edition 13's April soybean interval. Differences between this table and the annual comparison remain as published.
