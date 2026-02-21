# cadastro_rural v1.0

Registros de imoveis rurais do Cadastro Ambiental Rural (CAR) por UF.

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | SICAR/GeoServer WFS | Servico Florestal Brasileiro / MMA |

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `cod_imovel` | str | ❌ | Codigo unico do imovel no CAR |
| `status` | str | ❌ | Status do registro: AT, PE, SU, CA |
| `data_criacao` | datetime | ❌ | Data de criacao do registro |
| `data_atualizacao` | datetime | ✅ | Data da ultima atualizacao |
| `area_ha` | float64 | ❌ | Area do imovel em hectares (>= 0) |
| `condicao` | str | ✅ | Condicao do imovel |
| `uf` | str | ❌ | Sigla da UF |
| `municipio` | str | ❌ | Nome do municipio |
| `cod_municipio_ibge` | int | ❌ | Codigo IBGE do municipio |
| `modulos_fiscais` | float64 | ❌ | Quantidade de modulos fiscais (>= 0) |
| `tipo` | str | ❌ | Tipo do imovel: IRU, AST, PCT |

## Primary Key

`[cod_imovel]`

## Filtros

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | str | Sigla da UF (obrigatorio) |
| `municipio` | str | Filtro parcial de municipio (case-insensitive) |
| `status` | str | AT (Ativo), PE (Pendente), SU (Suspenso), CA (Cancelado) |
| `tipo` | str | IRU (Rural), AST (Assentamento), PCT (Terra Indigena) |
| `area_min` | float | Area minima em hectares |
| `area_max` | float | Area maxima em hectares |
| `criado_apos` | str | Data minima de criacao (ISO format) |

## Garantias

- `cod_imovel` sempre nao-vazio
- `status` sempre AT, PE, SU ou CA
- `tipo` sempre IRU, AST ou PCT
- `area_ha` sempre >= 0
- `uf` sempre codigo valido de estado brasileiro
- 7.4M+ imoveis disponiveis (27 UFs)

## Exemplo

```python
from agrobr import datasets

# Imoveis rurais do Mato Grosso
df = await datasets.cadastro_rural("MT")

# Filtrado por municipio e status
df = await datasets.cadastro_rural("MT", municipio="Cuiaba", status="AT")

# Com metadados
df, meta = await datasets.cadastro_rural("DF", return_meta=True)
```

## Schema JSON

Disponivel em `agrobr/schemas/cadastro_rural.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("cadastro_rural")
print(contract.to_json())
```
