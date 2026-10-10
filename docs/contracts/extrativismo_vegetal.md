# extrativismo_vegetal v1.1

Produção extrativista vegetal (acai, castanha-do-para, erva-mate, palmito, etc) por UF ou município.

Na API 2.0, somente `produto`, `ano` aceitam posição; os demais filtros e flags são passados por nome. Retornos vazios preservam os dtypes do contrato: inteiros em `Int64`, medidas em `float64` e texto no padrão do pandas instalado.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | IBGE PEVS | Produção da Extração Vegetal e da Silvicultura |

## Produtos

`acai`, `castanha_para`, `erva_mate`, `palmito`, `pequi_fruto`, `babacu`, `piacava`, `carnauba_cera`, `carvao`, `lenha`, `madeira_tora`, `hevea_coagulado`

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `ano` | Int64 | ❌ | Ano de referencia |
| `localidade` | str | ✅ | UF ou município |
| `localidade_cod` | Int64 | ✅ | Código IBGE |
| `cod_municipio` | Int64 | ✅ | Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo fora da linha de município |
| `produto` | str | ❌ | Nome do produto |
| `valor` | float64 | ✅ | Quantidade produzida (toneladas ou metros cúbicos) ou, com `variavel="valor_producao"`, valor da produção em mil reais; a escala vem em `unidade` |
| `unidade` | str | ❌ | Unidade de medida |
| `fonte` | str | ❌ | Origem dos dados |

## Primary Key

`[ano, produto, localidade]`

## Garantias

- Dados consolidados anuais
- Latencia tipica: Y+1 (dados disponíveis no ano seguinte)
- Serie histórica desde 1986

## Exemplo

```python
from agrobr import datasets

# Producao de acai por UF
df = await datasets.extrativismo_vegetal("acai", ano=2023)

# Castanha-do-Para no Amazonas
df = await datasets.extrativismo_vegetal("castanha_para", ano=2023, uf="AM")

# Filtrar por UF
df = await datasets.extrativismo_vegetal("erva_mate", ano=2023, uf="PR")

# Com metadados
df, meta = await datasets.extrativismo_vegetal("acai", ano=2023, return_meta=True)
```

## Schema JSON

Disponível em `agrobr/schemas/extrativismo_vegetal.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("extrativismo_vegetal")
print(contract.to_json())
```

## Níveis Territoriais

| Nível | Descrição |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa (default) |
| `municipio` | Por município |
