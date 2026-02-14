# credito_rural v1.0

Crédito rural por cultura e UF, via camada semântica.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | BCB/SICOR (OData) | API oficial do Banco Central |
| 2 | BigQuery (basedosdados) | Fallback quando OData retorna 500 |

## Produtos

`soja`, `milho`, `cafe`, `algodao`, `trigo`, `arroz`

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `safra` | str | ❌ | - | Sim |
| `produto` | str | ❌ | - | Sim |
| `uf` | str | ✅ | - | Sim |
| `finalidade` | str | ❌ | - | Sim |
| `agregacao` | str | ✅ | - | Sim |
| `volume` | float | ✅ | - | Sim |
| `valor` | float | ✅ | BRL | Sim |

**Primary key:** `[safra, produto, uf, finalidade]`

**Constraints:** `volume >= 0`, `valor >= 0`

## Garantias

- Nomes de coluna nunca mudam (só adicionam)
- `safra` sempre no formato YYYY/YY
- `uf` sempre é código de estado brasileiro válido quando presente
- Valores numéricos sempre >= 0

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.credito_rural("soja", safra="2024/25")
df = await datasets.credito_rural("soja", safra="2024/25", uf="MT")

# Com metadados
df, meta = await datasets.credito_rural("soja", safra="2024/25", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.credito_rural("soja", safra="2024/25")
```

## Schema JSON

Disponível em `agrobr/schemas/credito_rural.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("credito_rural")
print(contract.to_json())
```

## Requisitos

O fallback BigQuery requer:

```bash
pip install agrobr[bigquery]
```
