# USDA PSD — Estimativas Internacionais

United States Department of Agriculture — Production, Supply, Distribution.
Estimativas internacionais de produção, oferta e demanda agrícola.

## Configuração

Requer API key gratuita do USDA:

1. Registre-se em [api.data.gov/signup](https://api.data.gov/signup/)
2. Configure a variável de ambiente:

```bash
export AGROBR_USDA_API_KEY="sua-key-aqui"
```

Ou passe diretamente:

```python
df = await usda.psd("soja", api_key="sua-key-aqui")
```

## API

```python
from agrobr import usda

# Dados PSD do Brasil para soja
df = await usda.psd("soja", country="BR", market_year=2024)

# Todos os países
df = await usda.psd("soja", country="all", market_year=2024)

# Dados mundiais agregados
df = await usda.psd("milho", country="world")

# Filtrar por atributos
df = await usda.psd("soja", attributes=["Production", "Exports"])

# Pivotar atributos como colunas
df = await usda.psd("soja", pivot=True)
```

## Colunas — `psd`

| Coluna | Tipo | Descrição |
|---|---|---|
| `commodity_code` | str | Código USDA da commodity |
| `commodity` | str | Nome da commodity |
| `country_code` | str | Código ISO do país |
| `country` | str | Nome do país |
| `market_year` | int | Ano safra (marketing year) |
| `attribute` | str | Atributo (Production, Exports, etc.) |
| `attribute_br` | str | Atributo traduzido (PT-BR) |
| `value` | float | Valor |
| `unit` | str | Unidade |

## Commodities

| Nome agrobr | Commodity USDA |
|---|---|
| `soja` | Oilseed, Soybean |
| `milho` | Corn |
| `trigo` | Wheat |
| `algodao` | Cotton |
| `arroz` | Rice, Milled |
| `cafe` | Coffee, Green |
| `acucar` | Sugar, Centrifugal |

## MetaInfo

```python
df, meta = await usda.psd("soja", return_meta=True)
print(meta.source)  # "usda"
print(meta.source_method)  # "httpx"
```

## Fonte

- API: `https://apps.fas.usda.gov/OpenData/api/psd`
- Formato: JSON (REST API)
- Atualização: mensal (WASDE reports)
- Histórico: 1960+
