# Contrato: queimadas

Focos de calor detectados por satélite — INPE Queimadas.

## Schema

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `data` | DATE | Não | — | Data válida |
| `hora_gmt` | STRING | Sim | — | — |
| `lat` | FLOAT | Não | — | -35 a 6 |
| `lon` | FLOAT | Não | — | -74 a -30 |
| `satelite` | STRING | Não | — | — |
| `municipio` | STRING | Sim | — | — |
| `municipio_id` | INTEGER | Sim | — | — |
| `estado` | STRING | Sim | — | — |
| `uf` | STRING | Sim | — | — |
| `bioma` | STRING | Sim | — | — |
| `numero_dias_sem_chuva` | FLOAT | Sim | dias | ≥ 0 |
| `precipitacao` | FLOAT | Sim | mm | ≥ 0 |
| `risco_fogo` | FLOAT | Sim | — | 0 a 1 |
| `frp` | FLOAT | Sim | MW | ≥ 0 |

**PK:** `(data, lat, lon, satelite, hora_gmt)`

## Parâmetros obrigatórios

- `ano: int` — ano dos focos
- `mes: int` — mês dos focos

## Filtro de bioma

`bioma` aceita Amazônia, Cerrado, Mata Atlântica, Caatinga, Pampa e Pantanal, com ou sem acentos e sem distinção entre maiúsculas e minúsculas. Valores desconhecidos levantam `ValueError` antes da consulta à fonte.

## Exemplo

```python
from agrobr import datasets

# Focos de agosto/2024
df = await datasets.queimadas(ano=2024, mes=8)

# Com filtros
df = await datasets.queimadas(ano=2024, mes=8, uf="TO", bioma="Cerrado")

# Com metadados
df, meta = await datasets.queimadas(ano=2024, mes=8, return_meta=True)
```
