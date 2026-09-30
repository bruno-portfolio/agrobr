# abate_trimestral v2.0

Abate de animais por especie, trimestre e UF (bovino, suino, frango).

Na API 2.0, somente `produto`, `trimestre` aceitam posição; os demais filtros e flags são passados por nome. Retornos vazios preservam os dtypes do contrato: inteiros em `Int64`, medidas em `float64` e texto no padrão do pandas instalado.

A versão 2.0 troca `animais_abatidos` de `float64` para `Int64`. O vazio também usa `Int64`; valor fracionário gera `ParseError` sem truncamento. O contrato 1.0 permanece disponível como `IBGE_ABATE_V1`.

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
| `localidade_cod` | Int64 | ✅ | Codigo IBGE |
| `especie` | str | ❌ | bovino, suino ou frango |
| `animais_abatidos` | Int64 | ✅ | Quantidade de animais abatidos (cabecas) |
| `peso_carcacas` | float64 | ✅ | Peso total das carcacas (kg) |
| `fonte` | str | ❌ | Origem dos dados |

O dataset entrega as UFs e não tem linha do Brasil. O IBGE omite por sigilo (célula `X`) as UFs com poucos informantes, que saem nulas (bovino 2025: AP, DF e PB). Por isso a soma das UFs fica abaixo do total Brasil publicado: no bovino dos dois primeiros trimestres de 2025, 0,4 % abaixo. Para o total nacional, use a tabela 1092 do SIDRA no nível Brasil.

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
