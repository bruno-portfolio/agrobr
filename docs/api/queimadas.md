# API Queimadas/INPE

O modulo Queimadas fornece acesso aos dados de focos de calor detectados por satelite, disponibilizados pelo INPE (Instituto Nacional de Pesquisas Espaciais) via BDQueimadas.

## Funcoes

### `focos`

Busca focos de calor detectados por satelite no Brasil.

```python
async def focos(
    *,
    ano: int,
    mes: int,
    dia: int | None = None,
    uf: str | None = None,
    bioma: str | None = None,
    satelite: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ano` | `int` | Ano (ex: 2024) |
| `mes` | `int` | Mes (1-12) |
| `dia` | `int \| None` | Dia especifico (1-31). Se None, busca mes completo |
| `uf` | `str \| None` | Filtrar por UF (ex: "MT", "SP"). Case insensitive |
| `bioma` | `str \| None` | Filtrar por bioma (ex: "Amazonia", "Cerrado") |
| `satelite` | `str \| None` | Filtrar por satelite (ex: "AQUA_M-T", "NOAA-20") |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas:
- `data`: Data do foco (date)
- `hora_gmt`: Horario GMT (str, formato "HH:MM")
- `lat`: Latitude (float)
- `lon`: Longitude (float)
- `satelite`: Nome do satelite detector (str)
- `municipio`: Nome do municipio (str)
- `municipio_id`: Codigo IBGE do municipio (Int64)
- `estado`: Nome do estado (str)
- `bioma`: Bioma (str) — Amazonia, Cerrado, Mata Atlantica, Caatinga, Pampa, Pantanal
- `numero_dias_sem_chuva`: Dias sem precipitacao (float)
- `precipitacao`: Precipitacao em mm (float)
- `risco_fogo`: Indice de risco de fogo 0-1 (float)
- `frp`: Fire Radiative Power em MW (float)
- `uf`: Sigla da UF (str, 2 caracteres)

**Exemplo:**

```python
from agrobr import queimadas

# Todos os focos de setembro/2024
df = await queimadas.focos(ano=2024, mes=9)

# Focos de um dia especifico
df = await queimadas.focos(ano=2024, mes=9, dia=15)

# Filtrar por UF
df = await queimadas.focos(ano=2024, mes=9, uf="MT")

# Filtrar por bioma
df = await queimadas.focos(ano=2024, mes=9, bioma="Cerrado")

# Filtrar por satelite
df = await queimadas.focos(ano=2024, mes=9, satelite="AQUA_M-T")

# Combinar filtros
df = await queimadas.focos(ano=2024, mes=9, uf="MT", bioma="Amazonia")

# Com metadados de proveniencia
df, meta = await queimadas.focos(ano=2024, mes=9, return_meta=True)
print(meta.source, meta.records_count)
```

---

## Satelites Disponiveis

| Satelite | Descricao |
|----------|-----------|
| `AQUA_M-T` | AQUA MODIS (referencia) |
| `AQUA_M-M` | AQUA MODIS (Morning) |
| `TERRA_M-T` | TERRA MODIS (Tarde) |
| `TERRA_M-M` | TERRA MODIS (Morning) |
| `NOAA-20` | NOAA-20 VIIRS |
| `NOAA-21` | NOAA-21 VIIRS |
| `GOES-16` | GOES-16 (geoestacionario) |
| `GOES-19` | GOES-19 (geoestacionario) |
| `METOP-B` | MetOp-B |
| `METOP-C` | MetOp-C |
| `MSG-03` | Meteosat |
| `NPP-375` | Suomi NPP 375m |
| `NPP-375D` | Suomi NPP 375m (diurno) |

## Biomas

| Bioma | Area Aproximada |
|-------|-----------------|
| Amazonia | 4.2M km2 |
| Cerrado | 2.0M km2 |
| Mata Atlantica | 1.1M km2 |
| Caatinga | 844k km2 |
| Pampa | 176k km2 |
| Pantanal | 150k km2 |

## Versao Sincrona

```python
from agrobr.sync import queimadas

df = queimadas.focos(ano=2024, mes=9, uf="MT")
df, meta = queimadas.focos(ano=2024, mes=9, return_meta=True)
```
