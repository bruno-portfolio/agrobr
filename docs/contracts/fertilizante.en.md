# fertilizante v2.0

Monthly fertilizer deliveries to the Brazilian market (national total, `uf="BR"`).

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | ANDA | National Association for Fertilizer Distribution |

## Products

`total`

!!! warning "Migrating from v1.0"
    `npk`, `ureia`, `map`, `dap`, `ssp`, `tsp`, and `kcl` were never filtered
    at the source: the total volume was merely relabeled with the requested
    value. v2.0 rejects them with `ValueError` to prevent incorrect data.

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `ano` | int | ❌ | - | Yes |
| `mes` | int | ❌ | - | Yes |
| `uf` | str | ✅ | - | Yes |
| `produto_fertilizante` | str | ❌ | - | Yes |
| `volume_ton` | float | ✅ | ton | Yes |

**Primary key:** `[ano, mes, uf, produto_fertilizante]`

**Constraints:** `ano >= 2000`, `mes` between 1 and 12, `volume_ton >= 0`

## Guarantees

- Column names never change (additions only)
- `ano` is always >= 2000
- `mes` between 1 and 12
- Numeric values are always >= 0
- `produto_fertilizante` is always `total`

Without `ano`, the dataset uses the current year by the Brasília date. The current year is partial: the result warns
in `validation_warnings` and `UserWarning`, carries `ano_em_curso` and `meses_cobertos` in `source_details` and changes
until the closed-year edition.

## Example

```python
from agrobr import datasets

# Async
df = await datasets.fertilizante(ano=2024)
df = await datasets.fertilizante(ano=2024, produto="total")

# With metadata
df, meta = await datasets.fertilizante(ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.fertilizante(ano=2024)
```

## JSON Schema

Available at `agrobr/schemas/fertilizante.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("fertilizante")
print(contract.to_json())
```

## Requirements

```bash
pip install agrobr[pdf]
```


## Publication coverage and validation

The public catalog contains 11 PDFs covering 2016–2026, all with monthly national deliveries (`uf="BR"`). The 2026 bulletin publishes January through June; blank later months are not zero. Since none of them publishes a state breakdown, 2.0.0 removed the `uf` argument from the source and from the dataset (2.0 migration guide, section 50).

Parser 3 requires the `Fertilizantes Entregues ao Mercado (em toneladas de produto)` section and searches for the year only within it. If that year or section identity is missing, the source raises `ParseError`, and so does the dataset (`"Todas as fontes falharam por layout"`), with the source's reason in `errors`. Production, imports, exports and exchange ratios from the same PDF cannot substitute for deliveries. Published values and contract 2.0 are unchanged.

Output flags are keyword-only; years and months use nullable `Int64`, and `volume_ton` uses `float64`.
