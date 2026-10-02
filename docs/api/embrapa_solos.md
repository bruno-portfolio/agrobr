# API EMBRAPA Solos

O modulo embrapa_solos fornece perfis de solo e mapa pedologico do Brasil via WFS EMBRAPA GeoInfo.

## Funcoes

### `perfis`

Perfis de solo PronaSolos (tabular).

```python
async def perfis(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = 50000,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str \| None` | Filtro por UF (sigla, ex: "MT") |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `max_registros` | `int \| None` | Teto de ocorrencias lidas em ordem de `fid` (padrao 50.000; `None` le a camada inteira). Os filtros locais atuam so sobre esse prefixo; corte que deixa a selecao parcial emite `UserWarning` |
| `tamanho_pagina` | `int \| None` | Ocorrencias por pagina: padrao 250 (perfis) / 500 (mapa), maximo 1000; com geometria (`_geo` ou `bbox`), 100 / 25, maximo 100 |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

A coluna `ano` usa `Int64` anulável, e `data_colet` usa `datetime64[ns]`. Nulos da fonte e o literal `NULL` viram ausentes nessas duas colunas. Ano ou data fora do formato esperado levanta `ParseError`; não vira ausente silenciosamente. O texto usa o dtype nativo do pandas (`str` no pandas 3, `object` no pandas 2). Tabelas vazias têm os mesmos dtypes das tabelas com registros.

**Retorno:** DataFrame com 85 colunas (contrato `embrapa_solos.perfis` 3.0), uma linha por horizonte ou camada. Comeca por `fid`, `uf`, `municipio`, `latitude`, `longitude`, `horizonte`, `profundidade`, `areia_total`, `silte`, `argila`, `ph_h2o`, `carbono_organico`, `ctc`, `saturacao_bases`, `aluminio`, `fosforo`, `classe_textural`, `nivel_levantamento`, `uso_atual`, e segue com os demais atributos publicados, `uf_original` e `feature_id`. Os valores laboratoriais sao o texto publicado (inclusive `NULL`); veja a [pagina da fonte](../sources/embrapa_solos.md#colunas-perfis)

**Exemplo:**

```python
from agrobr import embrapa_solos

# Todos os perfis
df = await embrapa_solos.perfis()

# Perfis do Mato Grosso
df = await embrapa_solos.perfis(uf="MT")
```

---

### `perfis_geo`

Perfis de solo com geometria (GeoDataFrame). Requer `pip install agrobr[geo]`.

```python
async def perfis_geo(
    *,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = 5000,
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
) -> gpd.GeoDataFrame | tuple[gpd.GeoDataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str \| None` | Filtro por UF (sigla) |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `max_registros` | `int \| None` | Teto de ocorrencias lidas em ordem de `fid` (padrao 5.000; `None` le a camada inteira). Os filtros locais atuam so sobre esse prefixo; corte que deixa a selecao parcial emite `UserWarning` |
| `tamanho_pagina` | `int \| None` | Ocorrencias por pagina: padrao 250 (perfis) / 500 (mapa), maximo 1000; com geometria (`_geo` ou `bbox`), 100 / 25, maximo 100 |
| `return_meta` | `bool` | Retorna tupla (GeoDataFrame, MetaInfo) |

**Retorno:** GeoDataFrame (Point, EPSG:4326) com mesmas colunas de `perfis()` + `geometry`

**Exemplo:**

```python
from agrobr import embrapa_solos

# Perfis com geometria em bbox
gdf = await embrapa_solos.perfis_geo(bbox=(-56, -16, -54, -14))
```

---

### `mapa_solos`

Mapa pedologico do Brasil — classificacao SiBCS (tabular).

```python
async def mapa_solos(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = 50000,
    tamanho_pagina: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ordem` | `str \| None` | Filtro pela ordem do 1o componente (`ordem1`): a classe inteira, uma das 15 publicadas (13 ordens mais `AFLORAMENTOS DE ROCHAS` e `DUNAS`), com caixa, acento e singular aceitos (`"latossolo"` vale `LATOSSOLOS`), aplicado localmente ao prefixo lido. Trecho (`"latos"`), texto vazio, não textual ou fora das classes levanta `InvalidParameterError` antes da coleta, com a lista das classes. Se o filtro não casar nada após uma leitura completa, `UserWarning` e `MetaInfo.validation_warnings` informam o valor pedido e os valores de `ordem1` observados. |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `max_registros` | `int \| None` | Teto de ocorrencias lidas em ordem de `fid` (padrao 50.000; `None` le a camada inteira). Os filtros locais atuam so sobre esse prefixo; corte que deixa a selecao parcial emite `UserWarning` |
| `tamanho_pagina` | `int \| None` | Ocorrencias por pagina: padrao 250 (perfis) / 500 (mapa), maximo 1000; com geometria (`_geo` ou `bbox`), 100 / 25, maximo 100 |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

**Retorno:** DataFrame com colunas: `fid`, `simbolos`, `comp1`, `comp2`, `comp3`, `legenda`, `area_km2`, `ordem1`, `subordem1`, `gdegrupo1`, `ordem2`, `subordem2`, `gdegrupo2`, `legenda_sinotica`, `classe_dom`, `ordem3`, `subordem3`, `gdegrupo3`, `feature_id`

**Exemplo:**

```python
from agrobr import embrapa_solos

# Mapa completo
df = await embrapa_solos.mapa_solos()

# Latossolos
df = await embrapa_solos.mapa_solos(ordem="LATOSSOLO")
```

---

### `mapa_solos_geo`

Mapa pedologico com geometria (GeoDataFrame). Requer `pip install agrobr[geo]`.

```python
async def mapa_solos_geo(
    *,
    ordem: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    max_registros: int | None = 3000,
    tamanho_pagina: int | None = None,
    return_meta: bool = False,
) -> gpd.GeoDataFrame | tuple[gpd.GeoDataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ordem` | `str \| None` | Filtro pela ordem do 1o componente (`ordem1`): a classe inteira, uma das 15 publicadas (13 ordens mais `AFLORAMENTOS DE ROCHAS` e `DUNAS`), com caixa, acento e singular aceitos (`"latossolo"` vale `LATOSSOLOS`), aplicado localmente ao prefixo lido. Trecho (`"latos"`), texto vazio, não textual ou fora das classes levanta `InvalidParameterError` antes da coleta, com a lista das classes. Se o filtro não casar nada após uma leitura completa, `UserWarning` e `MetaInfo.validation_warnings` informam o valor pedido e os valores de `ordem1` observados. |
| `bbox` | `tuple \| None` | Bounding box (lon_min, lat_min, lon_max, lat_max) |
| `max_registros` | `int \| None` | Teto de ocorrencias lidas em ordem de `fid` (padrao 3.000; `None` le a camada inteira). Os filtros locais atuam so sobre esse prefixo; corte que deixa a selecao parcial emite `UserWarning` |
| `tamanho_pagina` | `int \| None` | Ocorrencias por pagina: padrao 250 (perfis) / 500 (mapa), maximo 1000; com geometria (`_geo` ou `bbox`), 100 / 25, maximo 100 |
| `return_meta` | `bool` | Retorna tupla (GeoDataFrame, MetaInfo) |

**Retorno:** GeoDataFrame (MultiPolygon, EPSG:4326) com mesmas colunas de `mapa_solos()` + `geometry`

**Exemplo:**

```python
from agrobr import embrapa_solos

# Poligonos de solo em bbox
gdf = await embrapa_solos.mapa_solos_geo(bbox=(-56, -16, -54, -14))
```

## Versao Sincrona

```python
from agrobr.sync import embrapa_solos

df = embrapa_solos.perfis(uf="MT")
df = embrapa_solos.mapa_solos()
```

## Notas

- Fonte: [EMBRAPA GeoInfo](https://geoinfo.dados.embrapa.br) — licenca `nc` (CC BY-NC 3.0 BR)
- Funcoes `_geo()` requerem `pip install agrobr[geo]` (geopandas)
- 34.464 registros de horizontes/camadas (~9 mil pontos, PronaSolos 2020) e 2.852 poligonos pedologicos
- Paginacao WFS automatica
- `bbox` e GeoDataFrame em EPSG:4326, o CRS padrao das duas camadas no WFS
