# balanco v1.1

Supply/demand balance for commodities.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB | Supply and Demand Balance |

CONAB uses HTTP first. Playwright and Chromium are optional transport fallback dependencies:

```bash
pip install agrobr[browser]
python -m playwright install chromium
```

`balanco` has no alternative fallback source. If HTTP and the optional transport fail, the dataset raises `SourceUnavailableError`.

Without `levantamento`, `safra` selects the most recent publication whose Suprimento sheet carries that crop year; the current edition covers the last seven crop years (six for soybean), already revised. The returned table is that publication's and may contain rows for several periods. With `levantamento=N`, the Nth survey of the crop year itself is returned, which is the original edition. Wheat balance periods remain annual. The last published revision of each product and period takes precedence.

## Products

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `safra` | str | ❌ | Published crop year, e.g. "2024/25", or calendar year for wheat |
| `produto` | str | ❌ | Product name |
| `estoque_inicial` | float64 | ✅ | Opening stock (thousand tons) |
| `producao` | float64 | ✅ | Production (thousand tons) |
| `importacao` | float64 | ✅ | Imports (thousand tons) |
| `suprimento` | float64 | ✅ | Total supply (thousand tons), as published in the Suprimento sheet; in soybean's own sheet, opening stock + production + imports added up by agrobr, null if a term is missing |
| `consumo` | float64 | ✅ | Domestic consumption (thousand tons), as published; in the soybean sheet, seeds/other + crushing added up by agrobr (soybean 2025/26, Sep/26: 3,766 + 62,137.7 = 65,903.7), null if a term is missing |
| `exportacao` | float64 | ✅ | Exports (thousand tons) |
| `estoque_final` | float64 | ✅ | Ending stock (thousand tons) |
| `demanda_total` | float64 | ✅ | Published demand (thousand tons); null in wide/legacy layouts |
| `levantamento` | str | ✅ | Textual revision label, such as `set/26`; null when unpublished |
| `unidade` | str | ❌ | Metric unit: `mil_ton` |
| `fonte` | str | ❌ | Selected dataset source: `conab` |

All numeric columns use float64, including empty results. Contract 1.1 adds optional columns without changing the required 1.0 columns; `CONAB_BALANCO_V1` remains available and `CONAB_BALANCO_V1_1` is active. Unpublished demand is left null rather than derived; `levantamento` is a revision label, not the survey number.

## Guarantees

Pass `as_polars`, `return_meta` and `levantamento` by name. Text follows the installed pandas version's default dtype without an explicit `StringDtype` conversion; empty results retain contract columns and types.

- Complete supply/demand balance
- Updated monthly along with CONAB surveys

## Example

```python
from agrobr import datasets

# Current crop-year balance
df = await datasets.balanco("soja")

# Specific crop-year balance (latest revision)
df = await datasets.balanco("soja", safra="2024/25")

# Original edition: the crop year's own 12th survey
df = await datasets.balanco("soja", safra="2024/25", levantamento=12)

# With metadata
df, meta = await datasets.balanco("soja", return_meta=True)
```

## Balance Components

```
Supply = Opening Stock + Production + Imports
Demand = Consumption + Exports
Ending Stock = Supply - Demand
```

## JSON Schema

Available at `agrobr/schemas/balanco.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("balanco")
print(contract.primary_key)  # ['safra', 'produto']
print(contract.to_json())
```

## Units

All numeric values are in **thousand tons**.
