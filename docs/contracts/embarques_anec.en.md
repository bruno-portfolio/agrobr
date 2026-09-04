# embarques_anec v1.0

Weekly ANEC shipments by port, product, and report period.

## Source

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | ANEC | Weekly PDF report |

The dataset requires the PDF extra:

```bash
pip install agrobr[pdf]
```

## Schema

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `porto` | str | ❌ | — | Canonical port name |
| `produto` | str | ❌ | — | Canonical ANEC product |
| `periodo` | str | ❌ | — | `last_week` or `current_week` |
| `valor_ton` | float64 | ✅ | ton | >= 0 when present |

**Primary key:** `[porto, produto, periodo]`

## Products

`soybean`, `soybean_meal`, `maize`, `wheat`, `ddgs`, `sorghum`.

## Guarantees

- Column names never change; additions are allowed.
- `valor_ton` is greater than or equal to zero when present.
- There is one row per port, product, and period combination.

## Example

```python
from agrobr import datasets

df = await datasets.embarques_anec(ano=2026)
df = await datasets.embarques_anec(
    ano=2026,
    semana=4,
    produto="soja",
    tipo="efetivado",
)
df, meta = await datasets.embarques_anec(ano=2026, return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/embarques_anec.json`.

```python
from agrobr.contracts import get_contract

contract = get_contract("embarques_anec")
print(contract.primary_key)  # ["porto", "produto", "periodo"]
print(contract.to_json())
```

## License

`zona_cinza` classification: ANEC does not publish explicit terms of use.
Commercial use may require authorization from the association.
