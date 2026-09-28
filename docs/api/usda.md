# API USDA PSD

O modulo USDA fornece dados do Production, Supply and Distribution (PSD) — estimativas internacionais de oferta e demanda agricola do Departamento de Agricultura dos EUA.

## API Key

Requer chave gratuita do USDA:

1. Registre em [api.data.gov/signup](https://api.data.gov/signup/)
2. Configure: `export AGROBR_USDA_API_KEY=sua_chave`

A chave vai só no cabeçalho `X-Api-Key` do gateway `https://api.fas.usda.gov/api/psd`. Sem chave, ou com chave
recusada (HTTP 403 `API_KEY_INVALID`), sai `SourceUnavailableError`.

## Funcoes

### `psd`

Dados de producao, oferta e distribuicao por commodity e pais.

```python
async def psd(
    commodity: str,
    *,
    country: str = "BR",
    market_year: int | None = None,
    attributes: list[str] | None = None,
    pivot: bool = False,
    api_key: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `commodity` | `str` | Commodity: `"soja"`, `"milho"`, `"trigo"`, `"cafe"`, `"arroz"`, `"algodao"`, `"acucar"`, `"farelo_soja"`, `"oleo_soja"` ou `commodityCode` do catálogo oficial do PSD |
| `country` | `str` | Pais: `"BR"`, `"US"`, `"world"` (agregado), `"all"` (todos) ou `countryCode` do catálogo do PSD (não é ISO: `"CH"` é a China, `"E4"` a UE). Default: `"BR"` |
| `market_year` | `int \| None` | Ano de comercialização. `None` usa o ano-calendário corrente e, se o PSD ainda não publicou nada dele (de janeiro até o WASDE de maio), o ano anterior; o ano usado vai em `source_details["market_year"]` |
| `attributes` | `list[str] \| None` | Filtrar atributos pelo nome oficial (ex: `["Production", "Exports"]`) ou pelo rótulo do agrobr (`"producao"`, `"consumo_domestico"`...) |
| `pivot` | `bool` | Se True, pivota atributos como colunas |
| `api_key` | `str \| None` | Chave API (ou usa `AGROBR_USDA_API_KEY`) |
| `as_polars` | `bool` | Se True, retorna polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `commodity_code`, `commodity`, `country_code`, `country`, `market_year`, `attribute`, `attribute_br`, `value`, `unit`, `attribute_id`, `unit_id`, `last_update_year`, `last_update_month`. Rótulos pelos catálogos oficiais; `last_update_*` é a última atualização da série, não a edição consultada. Detalhes em [USDA PSD](../sources/usda.md).

**Erros:** commodity, país ou atributo fora dos catálogos levanta `InvalidParameterError` antes da rede; corpo fora do layout do gateway ou código que o catálogo local não conhece levanta `ParseError`.

**Exemplo:**

```python
from agrobr import usda

# Soja Brasil
df = await usda.psd("soja")

# Milho mundial pivotado
df = await usda.psd("milho", country="world", pivot=True)

# Atributos especificos
df = await usda.psd("soja", attributes=["Production", "Exports"])

# Varios paises
df = await usda.psd("soja", country="all", market_year=2024)
```

## Versao Sincrona

```python
from agrobr.sync import usda

df = usda.psd("soja")
```

## Notas

- Fonte: [USDA FAS](https://apps.fas.usda.gov/psdonline/) — licenca livre
- Dados globais de ~180 paises
- Atualizado mensalmente (WASDE report)
