# credito_rural v1.1

Rural credit by crop and state, via the semantic layer. Enriched SICOR dimensions (program, funding source, insurance type, modality, activity).

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | BCB/SICOR (OData) | Official Banco Central API |
| 2 | BigQuery (basedosdados) | Fallback when OData returns 500 |

## Products

`soja`, `milho`, `cafe`, `algodao`, `trigo`, `arroz`, `feijao`, `cana`, `sorgo`

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `safra` | str | ❌ | - | Yes |
| `produto` | str | ❌ | - | Yes |
| `uf` | str | ✅ | - | Yes |
| `finalidade` | str | ❌ | - | Yes |
| `agregacao` | str | ✅ | - | Yes |
| `volume` | float | ✅ | - | Yes |
| `valor` | float | ✅ | BRL | Yes |
| `cd_programa` | str | ✅ | - | Yes |
| `programa` | str | ✅ | - | Yes |
| `cd_fonte_recurso` | str | ✅ | - | Yes |
| `fonte_recurso` | str | ✅ | - | Yes |
| `cd_tipo_seguro` | str | ✅ | - | Yes |
| `tipo_seguro` | str | ✅ | - | Yes |
| `cd_modalidade` | str | ✅ | - | Yes |
| `modalidade` | str | ✅ | - | Yes |
| `cd_atividade` | str | ✅ | - | Yes |
| `atividade` | str | ✅ | - | Yes |
| `regiao` | str | ✅ | - | Yes |

**Primary key:** `[safra, produto, uf, finalidade]`

**Constraints:** `volume >= 0`, `valor >= 0`

## Guarantees

- Column names never change (additions only)
- `safra` is always in YYYY/YY format
- `uf` is always a valid Brazilian state code when present
- Numeric values are always >= 0

## Version history

| Version | Change |
|---------|--------|
| v1.0 | Initial schema: safra, produto, uf, finalidade, agregacao, volume, valor |
| v1.1 | +11 nullable columns: cd_programa, programa, cd_fonte_recurso, fonte_recurso, cd_tipo_seguro, tipo_seguro, cd_modalidade, modalidade, cd_atividade, atividade, regiao |

## Example

```python
from agrobr import datasets

# Async
df = await datasets.credito_rural("soja", safra="2024/25")
df = await datasets.credito_rural("soja", safra="2024/25", uf="MT")

# Filter by program
df = await datasets.credito_rural("soja", safra="2024/25", programa="Pronamp")

# Aggregate by program
df = await datasets.credito_rural("soja", safra="2024/25", agregacao="programa")

# With metadata
df, meta = await datasets.credito_rural("soja", safra="2024/25", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.credito_rural("soja", safra="2024/25")
```

## JSON Schema

Available at `agrobr/schemas/credito_rural.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("credito_rural")
print(contract.to_json())
```

## Requirements

The BigQuery fallback requires:

```bash
pip install agrobr[bigquery]
```
