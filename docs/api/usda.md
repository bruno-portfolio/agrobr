# API USDA PSD

O modulo USDA fornece dados do Production, Supply and Distribution (PSD) — estimativas internacionais de oferta e demanda agricola do Departamento de Agricultura dos EUA.

## API Key

Requer chave gratuita do USDA:

1. Registre em [api.data.gov/signup](https://api.data.gov/signup/)
2. Configure: `export AGROBR_USDA_API_KEY=sua_chave`

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
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `commodity` | `str` | Commodity: `"soja"`, `"milho"`, `"trigo"` ou codigo USDA |
| `country` | `str` | Pais: `"BR"`, `"US"`, `"world"` (agregado), `"all"` (todos). Default: `"BR"` |
| `market_year` | `int \| None` | Ano de comercializacao. None usa mais recente |
| `attributes` | `list[str] \| None` | Filtrar atributos (ex: `["Production", "Exports"]`) |
| `pivot` | `bool` | Se True, pivota atributos como colunas |
| `api_key` | `str \| None` | Chave API (ou usa `AGROBR_USDA_API_KEY`) |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `commodity_code`, `commodity`, `country_code`, `country`, `market_year`, `attribute`, `attribute_br`, `value`, `unit`

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
