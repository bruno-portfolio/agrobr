# exportacao v1.0

Exportações agrícolas brasileiras por produto, UF e mês.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | ComexStat | Dados oficiais MDIC por NCM |
| 2 | ABIOVE | Fallback com normalização de unidades |

## Produtos

`soja`, `milho`, `cafe`, `algodao`, `acucar`, `farelo_soja`, `oleo_soja`

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `ano` | int | ❌ | - | Sim |
| `mes` | int | ❌ | - | Sim |
| `produto` | str | ❌ | - | Sim |
| `uf` | str | ✅ | - | Sim |
| `kg_liquido` | float | ✅ | kg | Sim |
| `valor_fob_usd` | float | ✅ | USD | Sim |

**Primary key:** `[ano, mes, produto, uf]`

**Constraints:** `ano >= 1997`, `mes` entre 1 e 12, `kg_liquido >= 0`, `valor_fob_usd >= 0`

## Garantias

- Nomes de coluna nunca mudam (só adicionam)
- `ano` sempre >= 1997
- `mes` entre 1 e 12
- Valores numéricos sempre >= 0

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.exportacao("soja", ano=2024)
df = await datasets.exportacao("soja", ano=2024, uf="MT")

# Com metadados
df, meta = await datasets.exportacao("soja", ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.exportacao("soja", ano=2024)
```

## Schema JSON

Disponível em `agrobr/schemas/exportacao.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("exportacao")
print(contract.to_json())
```
