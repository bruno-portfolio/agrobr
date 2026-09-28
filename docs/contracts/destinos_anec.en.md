# destinos_anec v1.0

Destination shares of cumulative exports by product, for the period indicated in the ANEC report.

## Source

| Priority | Source | Method |
|---|---|---|
| 1 | ANEC | PDF (`anec.destinos`) |

Requires `pip install agrobr[pdf]`. See the [source documentation](../sources/anec.md).

## Schema

| Column | Type | Description |
|---|---|---|
| `produto` | str | Canonical product |
| `destino` | str | Uppercase destination; includes the `OTHERS` aggregate |
| `share_pct` | Float64, nullable | Percentage share (0–100) |
| `ano` | Int64, nullable | Cumulative-period year, when identified in the header |
| `mes_inicio` | Int64, nullable | First month of the cumulative period (1–12) |
| `mes_fim` | Int64, nullable | Last month of the cumulative period (1–12) |
| `ano_relatorio` | int | Selected report edition year |
| `semana_relatorio` | int | Report edition week (1–53) |
| `edicao_id` | str | ANEC article `cuid` |
| `publicado_em` | datetime UTC | Article `created_at` |
| `revisado_em` | datetime UTC | Source file `media_updated_at` |

**Primary key:** `[edicao_id, revisado_em, produto, destino]`.

The key preserves separate editions and revisions when appending report snapshots. `revisado_em` is the file update timestamp supplied by ANEC; it need not be later than the article creation time. Nullable columns always exist, even when their values are missing.

## Meaning and limits

ANEC publishes importer tables only for soybeans (`soybean`), soybean meal (`soybean_meal`), maize (`maize`) and wheat (`wheat`) in the W13 and W34/2026 editions. The destinations catalog advertises these four products. DDGS and sorghum remain in `embarques_mensais_anec` and `comparacao_anual_anec`. For destinations, `ddgs`, `sorgo` and their aliases raise `InvalidParameterError` before any network access.

The unit is a cumulative percentage for the header's period, not tonnage or a monthly flow. The report edition's week or month is not automatically assigned to the destination period. `OTHERS` is a valid aggregate, not a country. Rounded percentages may total 99% or 101%, without an artificial adjustment to 100%.

Some reports use charts that the parser does not extract (as in editions W08 and W12 of 2026). Results may be empty with a notice in `meta.validation_warnings`; this does not establish an absence of shipments or destinations. Use `return_meta=True` to inspect warnings. Edition W34/2026 reports January–July 2026.

## Products

| ANEC code (`produto`) | Product | agrobr name |
|---|---|---|
| `soybean` | soybeans | `soja` |
| `soybean_meal` | soybean meal | `farelo_soja` |
| `maize` | corn | `milho` |
| `wheat` | wheat | `trigo` |

`produto` keeps the ANEC code in English. The agrobr name is the canonical one in `normalize.crops`, the same as in `exportacao` and `estimativa_safra`; `normalizar_cultura` converts each code into the name in the table.

## Parameters

```python
async def destinos_anec(
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

df, meta = await datasets.destinos_anec(
    ano=2026, semana=34, produto="soja", return_meta=True
)
print(df.head())
print(meta.source_url)
```

## JSON schema

`agrobr/schemas/destinos_anec.json`, also available through `get_contract("destinos_anec")`.

## License

`zona_cinza`: public reports without explicit public reuse terms located. The first ANEC call emits a warning. Commercial use or redistribution may require ANEC authorization; publication on the website does not establish permission for commercial redistribution.

## Reading the bulletins

The text table is returned in full, including OTHERS. Map percentages and the Total row are outside the contract. Rounded shares may sum to 99%, 101% or 102% without renormalization. Editions 08 and 12/2026 contain raster panels: an empty result with a warning indicates missing extraction, not absence of trade.
