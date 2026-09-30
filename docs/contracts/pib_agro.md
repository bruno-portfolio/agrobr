# pib_agro v1.0

PIB agropecuário brasileiro por setor e trimestre.

Na API 2.0, somente `produto` aceita posição; os demais filtros e flags são passados por nome. Retornos vazios preservam os dtypes do contrato: inteiros em `Int64`, medidas em `float64` e texto no padrão do pandas instalado.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | IBGE | Contas Nacionais Trimestrais (SIDRA) |

## Produtos (setores)

`agropecuaria`, `industria`, `servicos`, `pib_total`

O parâmetro `produto` do dataset mapeia para o parâmetro `setor` da API IBGE.

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `trimestre` | str | ❌ | - | Sim |
| `valor` | float | ✅ | R$ (milhões) | Sim |
| `unidade` | str | ❌ | - | Sim |
| `setor` | str | ❌ | - | Sim |
| `precos` | str | ❌ | - | Sim |
| `fonte` | str | ❌ | - | Sim |

**Primary key:** `[trimestre, setor, precos]`

**Nota:** `precos` na PK é essencial — sem ele, calls com `precos` diferentes geram PKs idênticas com `valor` semanticamente diferente. `precos` é metadata de contexto injetada pelo dataset (não vem diretamente do source).

## Garantias

- Nomes de coluna nunca mudam (só adicionam)
- `trimestre` sempre no formato YYYYQQ
- `setor` é uma das chaves válidas: agropecuaria, industria, servicos, pib_total
- `precos` indica o tipo de deflator: corrente, real_1995

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.pib_agro("agropecuaria")
df = await datasets.pib_agro("agropecuaria", trimestre="202401", precos="real_1995")

# Com metadados
df, meta = await datasets.pib_agro("agropecuaria", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.pib_agro("agropecuaria")
```

## Schema JSON

Disponível em `agrobr/schemas/pib_agro.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("pib_agro")
print(contract.to_json())
```
