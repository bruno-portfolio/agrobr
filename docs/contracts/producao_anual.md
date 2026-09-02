# producao_anual v1.0

Produção agrícola anual consolidada por UF ou município.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | IBGE PAM | Produção Agrícola Municipal |
| 2 | CONAB | Acompanhamento de Safras |

No fallback CONAB, o ano civil corresponde ao segundo ano da safra:
`ano=2023` consulta a safra `2022/23`. A fonte fornece dados estaduais; o nível
`brasil` é calculado pela soma das UFs e o nível `municipio` não possui fallback.
Como o boletim não publica área colhida, `area_colhida` fica nula nesse fallback.

## Produtos

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`, `cafe`, `cacau`

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
| `valor_producao` | float64 | ✅ | Valor da produção (mil reais) |
| `fonte` | str | ❌ | Origem dos dados: `ibge_pam` ou `conab` |

## Primary Key

`[ano, produto, localidade]`

## Garantias

- Dados consolidados do ano agrícola completo
- Latência típica: Y+1 (dados disponíveis no ano seguinte)
- Área e produção da CONAB são convertidas de mil ha/mil ton para ha/ton

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
| `municipio` | Por município; disponível apenas na fonte primária IBGE PAM |
