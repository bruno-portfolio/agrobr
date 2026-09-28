# embarques_anec v1.1

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
| `ano` | int | ❌ | — | Optional column; edition year printed on the bulletin |
| `semana` | int | ❌ | — | Optional column; edition week, 1 to 53 |
| `data_inicio` | date | ✅ | — | Optional column; first day of the period, read from the label |
| `data_fim` | date | ✅ | — | Optional column; last day of the period, read from the label |

**Primary key:** `[porto, produto, periodo]`

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

## Guarantees

- Column names never change; additions are allowed.
- `valor_ton` is greater than or equal to zero when present.
- There is one row per port, product, and period combination.
- Dates come from the bulletin labels, never from ISO weeks, and are null when the two labels do not form
  consecutive 7-day weeks (see the [source](../sources/anec.md)).

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

## Bulletin verification

Headers must contain all six products in both periods. A missing column interrupts extraction, with the ParseError cause preserved by SourceUnavailableError. Totals are not ports and blank cells in the final published port remain null. Verification covers editions 04, 08, 12, 13, 34 and 36/2026; periods are bulletin labels, without deriving dates from ISO week numbers.
