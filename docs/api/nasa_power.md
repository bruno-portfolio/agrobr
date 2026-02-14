# API NASA POWER

O modulo NASA POWER fornece dados climatologicos gridded globais da NASA — temperatura, precipitacao, radiacao, umidade e vento. Alternativa ao INMET que nao requer token.

## Funcoes

### `clima_ponto`

Dados climatologicos para um ponto geografico (latitude/longitude).

```python
async def clima_ponto(
    lat: float,
    lon: float,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "diario",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `lat` | `float` | Latitude (-90 a 90) |
| `lon` | `float` | Longitude (-180 a 180) |
| `inicio` | `str \| date` | Data inicial (YYYY-MM-DD) |
| `fim` | `str \| date` | Data final (YYYY-MM-DD) |
| `agregacao` | `str` | `"diario"` (default) ou `"mensal"` |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `data`, `lat`, `lon`, `temp_media`, `temp_max`, `temp_min`, `precip_mm`, `umidade_rel`, `radiacao_mj`, `vento_ms`

**Exemplo:**

```python
from agrobr import nasa_power

# Clima diario para Sorriso-MT
df = await nasa_power.clima_ponto(
    lat=-12.55, lon=-55.72,
    inicio="2024-01-01", fim="2024-03-31"
)

# Clima mensal
df = await nasa_power.clima_ponto(
    lat=-12.55, lon=-55.72,
    inicio="2023-01-01", fim="2023-12-31",
    agregacao="mensal"
)
```

---

### `clima_uf`

Dados climatologicos agregados por UF (usa centroide do estado).

```python
async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str` | Sigla UF (ex: "MT", "SP") |
| `ano` | `int` | Ano de referencia |
| `agregacao` | `str` | `"diario"` ou `"mensal"` (default) |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Exemplo:**

```python
from agrobr import nasa_power

df = await nasa_power.clima_uf("MT", 2024)
```

## Versao Sincrona

```python
from agrobr.sync import nasa_power

df = nasa_power.clima_ponto(lat=-12.55, lon=-55.72, inicio="2024-01-01", fim="2024-03-31")
df = nasa_power.clima_uf("MT", 2024)
```

## Notas

- Dados da [NASA POWER](https://power.larc.nasa.gov/) — licenca livre
- Usa coordenadas do centroide para `clima_uf()` — para analises precisas, use `clima_ponto()` com coordenadas especificas
- Alternativa ao INMET para quem nao tem token
