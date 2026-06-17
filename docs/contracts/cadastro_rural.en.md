# cadastro_rural v1.0

Rural property records from the Rural Environmental Registry (CAR) by state.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | SICAR/GeoServer WFS | Brazilian Forest Service / MMA |

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `cod_imovel` | str | ❌ | Unique property code in CAR |
| `status` | str | ❌ | Record status: AT, PE, SU, CA |
| `data_criacao` | datetime | ✅ | Record creation date (may be null in old records) |
| `data_atualizacao` | datetime | ✅ | Last update date |
| `area_ha` | float64 | ❌ | Property area in hectares (>= 0) |
| `condicao` | str | ✅ | Property condition |
| `uf` | str | ❌ | State abbreviation |
| `municipio` | str | ❌ | Municipality name |
| `cod_municipio_ibge` | int | ❌ | IBGE municipality code |
| `modulos_fiscais` | float64 | ❌ | Number of fiscal modules (>= 0) |
| `tipo` | str | ❌ | Property type: IRU, AST, PCT |

## Primary Key

`[cod_imovel]`

## Filters

| Parameter | Type | Description |
|-----------|------|-------------|
| `uf` | str | State abbreviation (required) |
| `municipio` | str | Partial municipality filter (case-insensitive) |
| `status` | str | AT (Active), PE (Pending), SU (Suspended), CA (Cancelled) |
| `tipo` | str | IRU (Rural), AST (Settlement), PCT (Indigenous Land) |
| `area_min` | float | Minimum area in hectares |
| `area_max` | float | Maximum area in hectares |
| `criado_apos` | str | Minimum creation date (ISO format) |

## Guarantees

- `cod_imovel` is always non-empty
- `status` is always AT, PE, SU or CA
- `tipo` is always IRU, AST or PCT
- `area_ha` is always >= 0
- `uf` is always a valid Brazilian state code
- 7.4M+ properties available (27 states)

## Example

```python
from agrobr import datasets

# Rural properties in Mato Grosso
df = await datasets.cadastro_rural("MT")

# Filtered by municipality and status
df = await datasets.cadastro_rural("MT", municipio="Cuiaba", status="AT")

# With metadata
df, meta = await datasets.cadastro_rural("DF", return_meta=True)
```

## JSON Schema

Available at `agrobr/schemas/cadastro_rural.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("cadastro_rural")
print(contract.to_json())
```
