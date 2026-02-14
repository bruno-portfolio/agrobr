# custo_producao v1.0

Custos de produção detalhados por cultura, UF e safra.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CONAB | Custos de produção por hectare |

## Produtos

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`, `cafe`

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `cultura` | str | ❌ | - | Sim |
| `uf` | str | ❌ | - | Sim |
| `safra` | str | ❌ | - | Sim |
| `tecnologia` | str | ✅ | - | Sim |
| `categoria` | str | ❌ | - | Sim |
| `item` | str | ❌ | - | Sim |
| `unidade` | str | ✅ | - | Sim |
| `quantidade_ha` | float | ✅ | unidade/ha | Sim |
| `preco_unitario` | float | ✅ | BRL | Sim |
| `valor_ha` | float | ✅ | BRL/ha | Sim |
| `participacao_pct` | float | ✅ | % | Sim |

**Primary key:** `[cultura, uf, safra, categoria, item]`

**Constraints:** todos os numéricos >= 0, `participacao_pct` entre 0 e 100

## Garantias

- Nomes de coluna nunca mudam (só adicionam)
- `safra` sempre no formato YYYY/YY
- `uf` sempre é código de estado brasileiro válido
- Valores numéricos sempre >= 0
- `participacao_pct` entre 0 e 100

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.custo_producao("soja", uf="MT", safra="2024/25")
df = await datasets.custo_producao("milho", uf="PR")

# Com metadados
df, meta = await datasets.custo_producao("soja", uf="MT", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.custo_producao("soja", uf="MT", safra="2024/25")
```

## Schema JSON

Disponível em `agrobr/schemas/custo_producao.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("custo_producao")
print(contract.to_json())
```
