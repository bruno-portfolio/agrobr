# fertilizante v2.0

Entregas de fertilizantes por UF e mês.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | ANDA | Associação Nacional para Difusão de Adubos |

## Produtos

`total`

!!! warning "Migração da v1.0"
    `npk`, `ureia`, `map`, `dap`, `ssp`, `tsp` e `kcl` nunca eram filtrados
    na fonte: o volume total era apenas reetiquetado com o valor solicitado.
    A v2.0 recusa esses valores com `ValueError` para impedir dado incorreto.

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `ano` | int | ❌ | - | Sim |
| `mes` | int | ❌ | - | Sim |
| `uf` | str | ✅ | - | Sim |
| `produto_fertilizante` | str | ❌ | - | Sim |
| `volume_ton` | float | ✅ | ton | Sim |

**Primary key:** `[ano, mes, uf, produto_fertilizante]`

**Constraints:** `ano >= 2000`, `mes` entre 1 e 12, `volume_ton >= 0`

## Garantias

- Nomes de coluna nunca mudam (só adicionam)
- `ano` sempre >= 2000
- `mes` entre 1 e 12
- Valores numéricos sempre >= 0
- `produto_fertilizante` é sempre `total`

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.fertilizante(ano=2024)
df = await datasets.fertilizante(ano=2024, uf="MT")
df = await datasets.fertilizante(ano=2024, produto="total")

# Com metadados
df, meta = await datasets.fertilizante(ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.fertilizante(ano=2024)
```

## Schema JSON

Disponível em `agrobr/schemas/fertilizante.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("fertilizante")
print(contract.to_json())
```

## Requisitos

```bash
pip install agrobr[pdf]
```
