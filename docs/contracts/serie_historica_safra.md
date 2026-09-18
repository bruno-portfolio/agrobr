# serie_historica_safra v1.0

Série histórica de safras por produto, safra, região e UF.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CONAB | Séries Históricas de Safras |

## Produtos

44 culturas: `soja`, `milho`, `milho_1`, `milho_2`, `milho_3`, `arroz`, `arroz_irrigado`, `arroz_sequeiro`, `feijao`, `feijao_1`, `feijao_2`, `feijao_3`, `feijao_caupi`, `feijao_caupi_1`, `feijao_caupi_2`, `feijao_caupi_3`, `feijao_cores`, `feijao_cores_1`, `feijao_cores_2`, `feijao_cores_3`, `feijao_preto`, `feijao_preto_1`, `feijao_preto_2`, `feijao_preto_3`, `algodao`, `trigo`, `sorgo`, `aveia`, `cevada`, `canola`, `girassol`, `mamona`, `amendoim`, `amendoim_1`, `amendoim_2`, `centeio`, `triticale`, `gergelim`, `cafe`, `cafe_arabica`, `cafe_conilon`, `cana`, `cana_area_total`, `cana_industria`

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `produto` | str | ❌ | - | Sim |
| `safra` | str | ❌ | - | Sim |
| `regiao` | str | ✅ | - | Sim |
| `uf` | str | ✅ | - | Sim |
| `area_plantada_mil_ha` | float | ✅ | mil ha | Sim |
| `producao_mil_ton` | float | ✅ | mil ton | Sim |
| `produtividade_kg_ha` | float | ✅ | kg/ha | Sim |

**Primary key:** `[produto, safra, regiao, uf]`

**Constraints:** `area_plantada_mil_ha >= 0`, `producao_mil_ton >= 0`, `produtividade_kg_ha >= 0`

## Garantias

- PK única por combinação produto + safra + região + uf
- `produto` lowercase (ex: soja, milho_2)
- `safra` segue a fonte: YYYY/YY (grãos, ex: 2023/24) ou YYYY (culturas anuais, ex: 2024)
- `regiao` quando presente: NORTE, NORDESTE, CENTRO-OESTE, SUDESTE, SUL
- `uf` quando presente: código UF de 2 letras uppercase
- Métricas (area, produção, produtividade) >= 0 quando presentes

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.serie_historica_safra("soja")
df = await datasets.serie_historica_safra("soja", inicio=2020, fim=2024, uf="MT")

# Com metadados
df, meta = await datasets.serie_historica_safra("soja", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.serie_historica_safra("soja")
```

## Schema JSON

Disponível em `agrobr/schemas/serie_historica_safra.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("serie_historica_safra")
print(contract.to_json())
```
