# abate_trimestral v1.0

Abate de animais por especie, trimestre e UF (bovino, suino, frango).

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | IBGE Abate | Pesquisa Trimestral do Abate de Animais |

## Especies

`bovino`, `suino`, `frango`

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `trimestre` | str | ❌ | Trimestre no formato YYYYQQ |
| `localidade` | str | ✅ | UF |
| `localidade_cod` | int | ✅ | Codigo IBGE |
| `especie` | str | ❌ | bovino, suino ou frango |
| `animais_abatidos` | float64 | ✅ | Quantidade de animais abatidos (cabecas) |
| `peso_carcacas` | float64 | ✅ | Peso total das carcacas (kg) |
| `fonte` | str | ❌ | Origem dos dados |

## Primary Key

`[trimestre, especie, localidade]`

## Garantias

- Dados trimestrais consolidados
- Latencia tipica: T+2 meses
- Serie historica desde 1997

## Exemplo

```python
from agrobr import datasets

# Abate bovino por UF
df = await datasets.abate_trimestral("bovino", trimestre="202303")

# Abate de frango no Parana
df = await datasets.abate_trimestral("frango", trimestre="202303", uf="PR")

# Com metadados
df, meta = await datasets.abate_trimestral("bovino", trimestre="202303", return_meta=True)
```

## Schema JSON

Disponivel em `agrobr/schemas/abate_trimestral.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("abate_trimestral")
print(contract.to_json())
```
