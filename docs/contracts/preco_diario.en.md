# preco_diario v1.1

Daily spot prices of Brazilian agricultural commodities.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CEPEA/ESALQ | Direct collection, with Notícias Agrícolas fallback |
| 2 | Local cache | DuckDB; returns the same columns and dtypes as the CEPEA fetch |

## Products

`soja`, `milho`, `boi`, `bezerro`, `cafe`, `cafe_robusta`, `trigo`, `algodao`

## Schema

| Column | Type | Nullable | Unit | Description |
|--------|------|----------|------|-------------|
| `data` | date | ❌ | - | Indicator date |
| `produto` | str | ❌ | - | Product name |
| `praca` | str | ✅ | - | Reference market |
| `valor` | float64 | ❌ | As specified by `unidade` | Price in the unit specified in the same row |
| `unidade` | str | ❌ | - | E.g. "BRL/sc60kg", "BRL/ton", "cBRL/lb" (Brazilian real cents per pound) |
| `fonte` | str | ❌ | - | Data origin |
| `metodologia` | str | ✅ | - | Indicator methodology, when available |
| `anomalies` | str | ✅ | - | Anomaly list serialized as JSON text; null when empty |
| `valor_usd` | float64 | ✅ | USD | Dollar price published by CEPEA in the same row (optional, since 1.1); null on the Notícias Agrícolas fallback and in history cached before migration 10 |
| `peso_medio_kg` | float64 | ✅ | kg | Average calf weight (MS) from CEPEA's auxiliary table (optional, since 1.1); null for other products |

**Precision note:** `valor` uses `float64` (not `Decimal`) for
compatibility with pandas/polars and pipeline performance. IEEE 754
precision is sufficient for agricultural prices (max ~R$ 999,999.99).
For accounting use that requires exact precision, convert with
`df["valor"].apply(Decimal)` after the fetch.

The optional 1.1 columns are filled only by CEPEA collection: `valor_usd` follows the
`Valor US$` column of the indicator table and `peso_medio_kg` comes from the `Peso Médio`
table on the calf page. Notícias Agrícolas fallback rows and records cached before
migration 10 stay null until the next collection.

## Guarantees

- `data` is always a business day
- `valor` is always positive
- Sorted by `data` descending

## Observation selection

The key remains `data` and `produto`. For the same observation and location,
CEPEA takes precedence over Notícias Agrícolas, including when both are
already cached. New collection from the same provider replaces its earlier
revision. Both providers' observations remain in DuckDB.

Use `praca=` to select a location. Without that filter, the dataset prioritizes
the product's existing reference location: Paranaguá/PR for soybean,
Campinas/SP for corn, São Paulo/SP for cattle, coffee, and cotton, Mato Grosso
do Sul for calves, Espírito Santo for robusta coffee, and Paraná for wheat.
If that reference is absent, ties use the location slug in alphabetical
order; this does not turn a regional series into a national average.
Use `cepea.indicador()` to retrieve every location.

`data_sources` includes only providers of selected rows. `valor` remains
`float64`, including direct DuckDB fallback. With sanity enabled, use
`json.loads(df.loc[index, "anomalies"])` to read non-null markers.

## Example

```python
from agrobr import datasets

# Async
df = await datasets.preco_diario("soja")
df, meta = await datasets.preco_diario("soja", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.preco_diario("soja")
```

## Deterministic Mode

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    df = await datasets.preco_diario("soja")
    # Filters data <= 2025-12-31
    # Uses local cache only; without the product in the cache, raises SourceUnavailableError
```

## JSON Schema

Available at `agrobr/schemas/preco_diario.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("preco_diario")
print(contract.primary_key)  # ['data', 'produto']
print(contract.to_json())
```

## MetaInfo

When `return_meta=True`, returns a `(DataFrame, MetaInfo)` tuple:

```python
df, meta = await datasets.preco_diario("soja", return_meta=True)

print(meta.source)            # "datasets.preco_diario/cepea"
print(meta.dataset)           # "preco_diario"
print(meta.contract_version)  # "1.1"
print(meta.records_count)     # 365
print(meta.from_cache)        # False
print(meta.snapshot)          # None (or "2025-12-31" if deterministic)
```

`as_polars` and `return_meta` are keyword arguments. Cached responses, including local fallback, preserve the latest original collection time among returned rows in both `fetched_at` and `fetch_timestamp`. Empty results use the same dtypes as populated results: `datetime64[ns]` dates, `float64` values and the installed pandas default for text.

The exception is `anomalies`, a serialized structured field: it keeps the pandas `object` dtype, with JSON text or `None`, preserving absence without converting it to `NaN`. In Polars, it uses `String` with JSON or `null`; empty results also use `String`.
