# fertilizante

Entregas de fertilizantes por UF, via camada semântica.

## Fontes

| Prioridade | Fonte | Descrição |
|---|---|---|
| 1 | ANDA | Associação Nacional para Difusão de Adubos |

## API

```python
from agrobr import datasets

df = await datasets.fertilizante(ano=2024)
df = await datasets.fertilizante(ano=2024, uf="MT")
df = await datasets.fertilizante(ano=2024, produto="ureia")
```

## Colunas

| Coluna | Tipo | Estável |
|---|---|---|
| `ano` | int | Sim |
| `mes` | int | Sim |
| `uf` | str | Sim |
| `produto_fertilizante` | str | Sim |
| `volume_ton` | float | Sim |

## Produtos

total, npk, ureia, map, dap, ssp, tsp, kcl

## Requisitos

```bash
pip install agrobr[pdf]
```
