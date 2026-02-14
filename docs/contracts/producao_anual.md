# producao_anual v1.0

Produção agrícola anual consolidada por UF ou município.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | IBGE PAM | Produção Agrícola Municipal |
| 2 | CONAB | Acompanhamento de Safras |

## Produtos

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`, `cafe`

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `ano` | int | ❌ | Ano de referência |
| `produto` | str | ❌ | Nome do produto |
| `localidade` | str | ✅ | UF ou município |
| `area_plantada` | float64 | ✅ | Área plantada (ha) |
| `area_colhida` | float64 | ✅ | Área colhida (ha) |
| `producao` | float64 | ✅ | Produção (toneladas) |
| `rendimento` | float64 | ✅ | Rendimento (kg/ha) |
| `fonte` | str | ❌ | Origem dos dados |

## Garantias

- Dados consolidados do ano agrícola completo
- Latência típica: Y+1 (dados disponíveis no ano seguinte)

## Exemplo

```python
from agrobr import datasets

# Produção por UF
df = await datasets.producao_anual("soja", ano=2023)

# Produção por município
df = await datasets.producao_anual("milho", ano=2023, nivel="municipio")

# Filtrar por UF
df = await datasets.producao_anual("soja", ano=2023, uf="MT")

# Com metadados
df, meta = await datasets.producao_anual("soja", ano=2023, return_meta=True)
```

## Schema JSON

Disponível em `agrobr/schemas/producao_anual.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("producao_anual")
print(contract.to_json())
```

## Níveis Territoriais

| Nível | Descrição |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa (default) |
| `municipio` | Por município |
