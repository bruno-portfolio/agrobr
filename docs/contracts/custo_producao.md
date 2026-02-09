# custo_producao

Custos de produção por cultura e UF, via camada semântica.

## Fontes

| Prioridade | Fonte | Descrição |
|---|---|---|
| 1 | CONAB | Custos de produção por hectare |

## API

```python
from agrobr import datasets

df = await datasets.custo_producao("soja", uf="MT", safra="2024/25")
df = await datasets.custo_producao("milho", uf="PR")
```

## Colunas

| Coluna | Tipo | Estável |
|---|---|---|
| `cultura` | str | Sim |
| `uf` | str | Sim |
| `safra` | str | Sim |
| `tecnologia` | str | Sim |
| `componente` | str | Sim |
| `valor_por_ha` | float | Sim |

## Produtos

soja, milho, arroz, feijao, trigo, algodao, cafe
