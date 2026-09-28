# progresso_safra v2.0

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
| `revisado` | bool | ✅ | - | No |
| `n_estados` | int | ✅ | - | No |
| `cobertura_area_pct` | float | ✅ | fraction | No |

`revisado` is optional in contract 1.1 and is returned as a nullable boolean. CONAB marks revised percentage values with `*`: `10%*` is read as 0.10. The flag is `True` if any percentage cell in the row has that suffix, `False` if none does, and null if the row has no numeric percentage. Contract 1.0 remains available as `CONAB_PROGRESSO_V1` and 1.1 as `CONAB_PROGRESSO_V1_1`; the active contract is
`CONAB_PROGRESSO_V2`.

In contract 2.0, the spreadsheet's "N estados" row is returned with `estado = "MEDIA_ESTADOS"`: it is CONAB's own average of the
monitored states, not Brazil. `n_estados` and `cobertura_area_pct` come from the note "(Esses N estados correspondem a X% da
área cultivada)", not recomputed (0.98 = 98%), and are null on state rows. `BR` only appears if CONAB publishes the "Brasil"
row; filtering `estado="BR"` without it raises `InvalidParameterError` with the published coverage.

`MEDIA_ESTADOS` is not the simple mean of the block's states. The numbers are consistent with an area-weighted mean, with
weights that CONAB does not publish: for wheat in the week of 2026-09-14 to 09-20, CONAB's average is 21.6% and the simple mean
of the states is 44.4%.

Text containing `%` is always divided by 100, including `0.5%` → `0.005` and `1%` → `0.01`. Numeric Excel cells already expressed as fractions retain their values. Blocks with incomplete weekly dates or duplicate keys raise a parsing error.

**Primary key:** `[cultura, safra, operacao, estado, semana_atual]`

**Constraints:** percentage values between 0.0 and 1.0 (fraction, not %)

## Guarantees

- Unique PK per crop + crop year + operation + state + week combination
- Percentage values between 0.0 and 1.0 (fraction, not %)
- Weekly data published by CONAB
- `estado` is the 2-letter state code (e.g. MT, GO, PR); `MEDIA_ESTADOS` is CONAB's own average of the monitored states
  (`n_estados`, `cobertura_area_pct`), neither the simple mean of the states nor Brazil; `BR` only when CONAB publishes Brazil
- `n_estados` and `cobertura_area_pct` come from the published note, not recomputed
- Percentages with the `*` suffix keep their value and CONAB's revision flag

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
