# preco_diario v1.0

Preço diário spot de commodities agrícolas brasileiras.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CEPEA/ESALQ | Via Notícias Agrícolas |
| 2 | Cache local | DuckDB |

## Produtos

`soja`, `milho`, `boi`, `cafe`, `trigo`, `algodao`

## Schema

| Coluna | Tipo | Nullable | Unidade | Descrição |
|--------|------|----------|---------|-----------|
| `data` | datetime64 | ❌ | - | Data do indicador |
| `produto` | str | ❌ | - | Nome do produto |
| `praca` | str | ✅ | - | Praça de referência |
| `valor` | float64 | ❌ | BRL | Preço em reais |
| `unidade` | str | ❌ | - | Ex: "BRL/sc60kg" |
| `fonte` | str | ❌ | - | Origem dos dados |

**Nota sobre precisão:** `valor` usa `float64` (não `Decimal`) para
compatibilidade com pandas/polars e performance em pipelines. Precisão
IEEE 754 é suficiente para preços agrícolas (máx ~R$ 999.999,99).
Para uso contábil que exija precisão exata, converter com
`df["valor"].apply(Decimal)` após o fetch.

## Garantias

- `data` é sempre dia útil
- `valor` é sempre positivo
- Ordenado por `data` decrescente

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.preco_diario("soja")
df, meta = await datasets.preco_diario("soja", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.preco_diario("soja")
```

## Modo Determinístico

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    df = await datasets.preco_diario("soja")
    # Filtra data <= 2025-12-31
    # Usa apenas cache local
```

## Schema JSON

Disponível em `agrobr/schemas/preco_diario.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("preco_diario")
print(contract.primary_key)  # ['data', 'produto']
print(contract.to_json())
```

## MetaInfo

Quando `return_meta=True`, retorna tupla `(DataFrame, MetaInfo)`:

```python
df, meta = await datasets.preco_diario("soja", return_meta=True)

print(meta.source)            # "datasets.preco_diario/cepea"
print(meta.dataset)           # "preco_diario"
print(meta.contract_version)  # "1.0"
print(meta.records_count)     # 365
print(meta.from_cache)        # False
print(meta.snapshot)          # None (ou "2025-12-31" se determinístico)
```
