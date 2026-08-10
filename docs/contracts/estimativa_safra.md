# estimativa_safra v1.0

Estimativas de safra corrente por UF.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CONAB | Acompanhamento de Safra |
| 2 | IBGE LSPA | Levantamento Sistemático da Produção Agrícola |

> A CONAB cobre a safra corrente e as recentes. Para safras que a CONAB não disponibiliza mais, o fallback IBGE LSPA fornece o agregado nacional (`uf` nulo, exceto se você filtrar por UF), com `levantamento` e `data_publicacao` nulos.

## Produtos

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `safra` | str | ❌ | Safra no formato "2024/25" |
| `produto` | str | ❌ | Nome do produto |
| `uf` | str | ✅ | Unidade Federativa |
| `area_plantada` | float64 | ✅ | Área plantada (mil ha) |
| `area_colhida` | float64 | ✅ | Área colhida (mil ha) |
| `produtividade` | float64 | ✅ | Produtividade (kg/ha) |
| `producao` | float64 | ✅ | Produção (mil ton) |
| `levantamento` | int | ✅ | Número do levantamento CONAB (1-12); nulo quando a fonte é o LSPA |
| `data_publicacao` | date | ✅ | Data de publicação CONAB; nula quando a fonte é o LSPA |
| `fonte` | str | ❌ | Origem dos dados |

## Garantias

- Estimativas atualizadas mensalmente
- Latência típica: M+0 (dados do mês corrente)

## Exemplo

```python
from agrobr import datasets

# Safra corrente
df = await datasets.estimativa_safra("soja")

# Safra específica
df = await datasets.estimativa_safra("soja", safra="2024/25")

# Filtrar por UF
df = await datasets.estimativa_safra("milho", uf="MT")

# Com metadados
df, meta = await datasets.estimativa_safra("soja", return_meta=True)
```

## Schema JSON

Disponível em `agrobr/schemas/estimativa_safra.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("estimativa_safra")
print(contract.primary_key)  # ['safra', 'produto', 'uf', 'levantamento']
print(contract.to_json())
```

## Formato de Safra

O formato de safra segue o padrão brasileiro: `AAAA/AA`

- `2024/25` = safra que começa em 2024 e termina em 2025
- Safra de verão: plantio set-dez, colheita jan-mai
- Safra de inverno: plantio fev-abr, colheita jun-set
