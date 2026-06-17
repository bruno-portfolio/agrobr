# progresso_safra v1.0

Weekly sowing and harvest progress (CONAB).

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB | Companhia Nacional de Abastecimento |

## Products (crops)

`algodao`, `arroz`, `feijao_1`, `milho_1`, `milho_2`, `soja`, `trigo`

The dataset's `produto` parameter is normalized via `normalizar_cultura()` to the name expected by the CONAB API (e.g. `"soja"` → `"Soja"`).

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `cultura` | str | ❌ | - | Yes |
| `safra` | str | ❌ | - | Yes |
| `operacao` | str | ❌ | - | Yes |
| `estado` | str | ❌ | - | Yes |
| `semana_atual` | str | ❌ | - | Yes |
| `pct_ano_anterior` | float | ✅ | fraction | Yes |
| `pct_semana_anterior` | float | ✅ | fraction | Yes |
| `pct_semana_atual` | float | ✅ | fraction | Yes |
| `pct_media_5_anos` | float | ✅ | fraction | Yes |

**Primary key:** `[cultura, safra, operacao, estado, semana_atual]`

**Constraints:** percentage values between 0.0 and 1.0 (fraction, not %)

## Guarantees

- Unique PK per crop + crop year + operation + state + week combination
- Percentage values between 0.0 and 1.0 (fraction, not %)
- Weekly data published by CONAB
- `estado` is a 2-letter state code (e.g. MT, GO, PR)

## Example

```python
from agrobr import datasets

# Async
df = await datasets.progresso_safra("soja")
df = await datasets.progresso_safra("milho_1", estado="MT", operacao="Semeadura")

# With metadata
df, meta = await datasets.progresso_safra("soja", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.progresso_safra("soja")
```

## JSON Schema

Available at `agrobr/schemas/progresso_safra.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("progresso_safra")
print(contract.to_json())
```
