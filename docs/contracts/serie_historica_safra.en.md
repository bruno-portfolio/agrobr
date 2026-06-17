# serie_historica_safra v1.0

Crop historical series by product, crop year, region and state.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CONAB | Crop Historical Series |

## Products

32 crops: `soja`, `milho`, `milho_1`, `milho_2`, `milho_3`, `arroz`, `arroz_irrigado`, `arroz_sequeiro`, `feijao`, `feijao_1`, `feijao_2`, `feijao_3`, `algodao`, `trigo`, `sorgo`, `aveia`, `cevada`, `canola`, `girassol`, `mamona`, `amendoim`, `amendoim_1`, `amendoim_2`, `centeio`, `triticale`, `gergelim`, `cafe`, `cafe_arabica`, `cafe_conilon`, `cana`, `cana_area_total`, `cana_industria`

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `produto` | str | ❌ | - | Yes |
| `safra` | str | ❌ | - | Yes |
| `regiao` | str | ✅ | - | Yes |
| `uf` | str | ✅ | - | Yes |
| `area_plantada_mil_ha` | float | ✅ | thousand ha | Yes |
| `producao_mil_ton` | float | ✅ | thousand tons | Yes |
| `produtividade_kg_ha` | float | ✅ | kg/ha | Yes |

**Primary key:** `[produto, safra, regiao, uf]`

**Constraints:** `area_plantada_mil_ha >= 0`, `producao_mil_ton >= 0`, `produtividade_kg_ha >= 0`

## Guarantees

- Unique PK per product + crop year + region + state combination
- `produto` is lowercase (e.g. soja, milho_2)
- `safra` in YYYY/YY format (e.g. 2023/24)
- `regiao` when present: NORTE, NORDESTE, CENTRO-OESTE, SUDESTE, SUL
- `uf` when present: 2-letter uppercase state code
- Metrics (area, production, yield) >= 0 when present

## Example

```python
from agrobr import datasets

# Async
df = await datasets.serie_historica_safra("soja")
df = await datasets.serie_historica_safra("soja", inicio=2020, fim=2024, uf="MT")

# With metadata
df, meta = await datasets.serie_historica_safra("soja", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.serie_historica_safra("soja")
```

## JSON Schema

Available at `agrobr/schemas/serie_historica_safra.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("serie_historica_safra")
print(contract.to_json())
```
