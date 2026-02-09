# credito_rural

Crédito rural por cultura e UF, via camada semântica.

## Fontes

| Prioridade | Fonte | Descrição |
|---|---|---|
| 1 | BCB/SICOR (OData) | API oficial do Banco Central |
| 2 | BigQuery (basedosdados) | Fallback quando OData retorna 500 |

## API

```python
from agrobr import datasets

df = await datasets.credito_rural("soja", safra="2024/25")
df = await datasets.credito_rural("soja", safra="2024/25", uf="MT")
```

## Colunas

| Coluna | Tipo | Estável |
|---|---|---|
| `safra` | str | Sim |
| `uf` | str | Sim |
| `produto` | str | Sim |
| `finalidade` | str | Sim |
| `valor` | float | Sim |
| `area_financiada` | float | Sim |
| `qtd_contratos` | int | Sim |

## Requisitos

O fallback BigQuery requer:

```bash
pip install agrobr[bigquery]
```
