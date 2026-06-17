# seguro_rural v1.0

Rural insurance — PSR policies and claims (MAPA).

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | MAPA PSR | Rural Insurance Premium Subsidy Program |

## Products

100+ dynamic PSR crops (validation delegated to the source).

## Query types

The dataset supports two query types via the `tipo` parameter:

- `tipo="apolices"` (default) — all policies with federal subsidy
- `tipo="sinistros"` — reported claims

Each type has its own contract (`mapa_psr_apolices` and `mapa_psr_sinistros`).

## Schema — Policies

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `nr_apolice` | str | ❌ | - | Yes |
| `ano_apolice` | int | ❌ | - | Yes |
| `uf` | str | ❌ | - | Yes |
| `municipio` | str | ✅ | - | Yes |
| `cd_ibge` | str | ✅ | - | Yes |
| `cultura` | str | ❌ | - | Yes |
| `classificacao` | str | ✅ | - | Yes |
| `area_total` | float | ✅ | ha | Yes |
| `valor_premio` | float | ✅ | BRL | Yes |
| `valor_subvencao` | float | ✅ | BRL | Yes |
| `valor_limite_garantia` | float | ✅ | BRL | Yes |
| `valor_indenizacao` | float | ✅ | BRL | Yes |
| `evento` | str | ✅ | - | Yes |
| `produtividade_estimada` | float | ✅ | - | Yes |
| `produtividade_segurada` | float | ✅ | - | Yes |
| `nivel_cobertura` | float | ✅ | - | Yes |
| `taxa` | float | ✅ | - | Yes |
| `seguradora` | str | ✅ | - | Yes |

## Schema — Claims

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `nr_apolice` | str | ❌ | - | Yes |
| `ano_apolice` | int | ❌ | - | Yes |
| `uf` | str | ❌ | - | Yes |
| `municipio` | str | ✅ | - | Yes |
| `cd_ibge` | str | ✅ | - | Yes |
| `cultura` | str | ❌ | - | Yes |
| `classificacao` | str | ✅ | - | Yes |
| `evento` | str | ❌ | - | Yes |
| `area_total` | float | ✅ | ha | Yes |
| `valor_indenizacao` | float | ❌ | BRL | Yes |
| `valor_premio` | float | ✅ | BRL | Yes |
| `valor_subvencao` | float | ✅ | BRL | Yes |
| `valor_limite_garantia` | float | ✅ | BRL | Yes |
| `produtividade_estimada` | float | ✅ | - | Yes |
| `produtividade_segurada` | float | ✅ | - | Yes |
| `nivel_cobertura` | float | ✅ | - | Yes |
| `seguradora` | str | ✅ | - | Yes |

## Guarantees

- `uf` is a valid state code
- Monetary values in BRL
- Data since 2006 (PSR inception)
- Claims: `valor_indenizacao` is always > 0, `evento` is always filled
- Policies: `valor_indenizacao` may be null/0

## Example

```python
from agrobr import datasets

# Policies (default)
df = await datasets.seguro_rural()
df = await datasets.seguro_rural("soja", uf="MT", ano=2023)

# Claims
df = await datasets.seguro_rural(tipo="sinistros")
df = await datasets.seguro_rural(tipo="sinistros", evento="SECA")

# With metadata
df, meta = await datasets.seguro_rural(return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.seguro_rural()
```

## JSON Schema

```python
from agrobr.contracts import get_contract
# Policies
contract = get_contract("mapa_psr_apolices")
# Claims
contract = get_contract("mapa_psr_sinistros")
```
