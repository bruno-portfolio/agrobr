# preco_diario v1.0

Daily spot prices of Brazilian agricultural commodities.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CEPEA/ESALQ | Via Notícias Agrícolas |
| 2 | Local cache | DuckDB |

## Products

`soja`, `milho`, `boi`, `cafe`, `cafe_robusta`, `trigo`, `algodao`

## Schema

| Column | Type | Nullable | Unit | Description |
|--------|------|----------|------|-------------|
| `data` | datetime64 | ❌ | - | Indicator date |
| `produto` | str | ❌ | - | Product name |
| `praca` | str | ✅ | - | Reference market |
| `valor` | float64 | ❌ | BRL | Price in reais |
| `unidade` | str | ❌ | - | E.g. "BRL/sc60kg" |
| `fonte` | str | ❌ | - | Data origin |

**Precision note:** `valor` uses `float64` (not `Decimal`) for
compatibility with pandas/polars and pipeline performance. IEEE 754
precision is sufficient for agricultural prices (max ~R$ 999,999.99).
For accounting use that requires exact precision, convert with
`df["valor"].apply(Decimal)` after the fetch.

## Guarantees

- `data` is always a business day
- `valor` is always positive
- Sorted by `data` descending

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
    # Uses local cache only
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
print(meta.contract_version)  # "1.0"
print(meta.records_count)     # 365
print(meta.from_cache)        # False
print(meta.snapshot)          # None (or "2025-12-31" if deterministic)
```
