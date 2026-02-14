# balanco v1.0

Balanço de oferta e demanda de commodities.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CONAB | Balanço de Oferta e Demanda |

## Produtos

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `safra` | str | ❌ | Safra no formato "2024/25" |
| `produto` | str | ❌ | Nome do produto |
| `estoque_inicial` | float64 | ✅ | Estoque inicial (mil ton) |
| `producao` | float64 | ✅ | Produção (mil ton) |
| `importacao` | float64 | ✅ | Importação (mil ton) |
| `suprimento` | float64 | ✅ | Suprimento total (mil ton) |
| `consumo` | float64 | ✅ | Consumo interno (mil ton) |
| `exportacao` | float64 | ✅ | Exportação (mil ton) |
| `estoque_final` | float64 | ✅ | Estoque final (mil ton) |
| `fonte` | str | ❌ | Origem dos dados |

## Garantias

- Balanço completo de oferta/demanda
- Atualizado mensalmente junto com levantamentos CONAB

## Exemplo

```python
from agrobr import datasets

# Balanço safra corrente
df = await datasets.balanco("soja")

# Balanço safra específica
df = await datasets.balanco("soja", safra="2024/25")

# Com metadados
df, meta = await datasets.balanco("soja", return_meta=True)
```

## Componentes do Balanço

```
Suprimento = Estoque Inicial + Produção + Importação
Demanda = Consumo + Exportação
Estoque Final = Suprimento - Demanda
```

## Schema JSON

Disponível em `agrobr/schemas/balanco.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("balanco")
print(contract.primary_key)  # ['safra', 'produto']
print(contract.to_json())
```

## Unidades

Todos os valores em **mil toneladas** (exceto estoque que pode ser em mil sacas para alguns produtos).
