# exportacao

Exportações agrícolas brasileiras, via camada semântica.

## Fontes

| Prioridade | Fonte | Descrição |
|---|---|---|
| 1 | ComexStat | Dados oficiais MDIC por NCM |
| 2 | ABIOVE | Fallback com normalização de unidades |

## API

```python
from agrobr import datasets

df = await datasets.exportacao("soja", ano=2024)
df = await datasets.exportacao("soja", ano=2024, uf="MT")
```

## Colunas

| Coluna | Tipo | Estável |
|---|---|---|
| `ano` | int | Sim |
| `mes` | int | Sim |
| `produto` | str | Sim |
| `kg_liquido` | float | Sim |
| `valor_fob_usd` | float | Sim |

## Produtos

soja, milho, cafe, algodao, acucar, farelo_soja, oleo_soja
