# preco_atacado v1.0

Preços de atacado em CEASAs brasileiras (CONAB/PROHORT).

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CONAB CEASA | PROHORT — preços diários de hortifrúti |

## Produtos

48+ produtos dinâmicos do PROHORT (validação delegada à source).

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `data` | date | ❌ | - | Sim |
| `produto` | str | ❌ | - | Sim |
| `categoria` | str | ❌ | - | Sim |
| `unidade` | str | ❌ | - | Sim |
| `ceasa` | str | ❌ | - | Sim |
| `ceasa_uf` | str | ❌ | - | Sim |
| `preco` | float | ❌ | BRL | Sim |

**Primary key:** `[data, produto, ceasa]`

**Constraints:** `preco >= 0`

## Garantias

- Nomes de coluna nunca mudam (só adicionam)
- `data` sempre no formato date
- `ceasa_uf` código UF de 2 letras uppercase
- Valores monetários em BRL
- `preco` sempre > 0 (nulls filtrados)

## Exemplo

```python
from agrobr import datasets

# Async — todos os produtos
df = await datasets.preco_atacado()

# Filtro por produto
df = await datasets.preco_atacado("TOMATE")

# Filtro por CEASA
df = await datasets.preco_atacado(ceasa="CEAGESP")

# Com metadados
df, meta = await datasets.preco_atacado(return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.preco_atacado()
```

## Schema JSON

Disponível em `agrobr/schemas/preco_atacado.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("preco_atacado")
print(contract.to_json())
```
