# leite_industrial v1.0

Aquisicao e industrializacao trimestral de leite por UF.

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | IBGE Leite | Pesquisa Trimestral do Leite |

## Produtos

`leite`

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `trimestre` | str | ❌ | Trimestre YYYYQQ |
| `localidade` | str | ✅ | UF |
| `localidade_cod` | int | ✅ | Codigo IBGE |
| `leite_adquirido` | float64 | ✅ | Leite cru adquirido (mil litros) |
| `leite_industrializado` | float64 | ✅ | Leite cru industrializado (mil litros) |
| `preco_medio` | float64 | ✅ | Preco medio pago ao produtor (R$/litro) |
| `fonte` | str | ❌ | Origem dos dados |

## Primary Key

`[trimestre, localidade]`

## Garantias

- Dados trimestrais com 3 variaveis em formato wide
- Latencia tipica: T+2 meses
- Serie historica desde 1997

## Notas

- A UF é a do laticínio que adquiriu o leite, não a da produção.
- Não há linha Brasil: a consulta é por UF (nível territorial 3 do SIDRA).
- UF em sigilo sai nula, porque o SIDRA publica "X" (por exemplo, RR e DF), e há UF que o SIDRA não publica (o AP não
  aparece). Por isso a soma das UFs fica abaixo do total do Brasil divulgado pelo IBGE.

## Exemplo

```python
from agrobr import datasets

# Leite trimestral por UF
df = await datasets.leite_industrial(trimestre="202303")

# Filtrar por UF
df = await datasets.leite_industrial(trimestre="202303", uf="MG")

# Com metadados
df, meta = await datasets.leite_industrial(trimestre="202303", return_meta=True)
```

## Schema JSON

Disponivel em `agrobr/schemas/leite_industrial.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("leite_industrial")
print(contract.to_json())
```
