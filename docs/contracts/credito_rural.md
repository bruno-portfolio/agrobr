# credito_rural v1.1

Credito rural por cultura e UF, via camada semantica. Dimensoes SICOR enriquecidas (programa, fonte de recurso, tipo de seguro, modalidade, atividade).

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | BCB/SICOR (OData) | API oficial do Banco Central |
| 2 | BigQuery (basedosdados) | Fallback quando OData retorna 500 |

## Produtos

`soja`, `milho`, `cafe`, `algodao`, `trigo`, `arroz`, `feijao`, `cana`, `sorgo`

## Schema

| Coluna | Tipo | Nullable | Unidade | Estavel |
|--------|------|----------|---------|---------|
| `safra` | str | --- | - | Sim |
| `produto` | str | --- | - | Sim |
| `uf` | str | Sim | - | Sim |
| `finalidade` | str | --- | - | Sim |
| `agregacao` | str | Sim | - | Sim |
| `volume` | float | Sim | - | Sim |
| `valor` | float | Sim | BRL | Sim |
| `cd_programa` | str | Sim | - | Sim |
| `programa` | str | Sim | - | Sim |
| `cd_fonte_recurso` | str | Sim | - | Sim |
| `fonte_recurso` | str | Sim | - | Sim |
| `cd_tipo_seguro` | str | Sim | - | Sim |
| `tipo_seguro` | str | Sim | - | Sim |
| `cd_modalidade` | str | Sim | - | Sim |
| `modalidade` | str | Sim | - | Sim |
| `cd_atividade` | str | Sim | - | Sim |
| `atividade` | str | Sim | - | Sim |
| `regiao` | str | Sim | - | Sim |

**Primary key:** `[safra, produto, uf, finalidade]`

**Constraints:** `volume >= 0`, `valor >= 0`

## Garantias

- Nomes de coluna nunca mudam (so adicionam)
- `safra` sempre no formato YYYY/YY
- `uf` sempre e codigo de estado brasileiro valido quando presente
- Valores numericos sempre >= 0

## Historico de versoes

| Versao | Mudanca |
|--------|---------|
| v1.0 | Schema inicial: safra, produto, uf, finalidade, agregacao, volume, valor |
| v1.1 | +11 colunas nullable: cd_programa, programa, cd_fonte_recurso, fonte_recurso, cd_tipo_seguro, tipo_seguro, cd_modalidade, modalidade, cd_atividade, atividade, regiao |

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.credito_rural("soja", safra="2024/25")
df = await datasets.credito_rural("soja", safra="2024/25", uf="MT")

# Filtrar por programa
df = await datasets.credito_rural("soja", safra="2024/25", programa="Pronamp")

# Agregar por programa
df = await datasets.credito_rural("soja", safra="2024/25", agregacao="programa")

# Com metadados
df, meta = await datasets.credito_rural("soja", safra="2024/25", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.credito_rural("soja", safra="2024/25")
```

## Schema JSON

Disponivel em `agrobr/schemas/credito_rural.json`.

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
