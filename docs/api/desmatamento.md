# Desmatamento (PRODES/DETER)

Dados de desmatamento do INPE via TerraBrasilis — PRODES (consolidado anual) e DETER (alertas em tempo real).

## `desmatamento.prodes()`

Dados anuais de desmatamento consolidado por poligono.

```python
import agrobr

df = await agrobr.desmatamento.prodes(bioma="Cerrado", ano=2022, uf="MT")
```

### Parâmetros

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `bioma` | `str` | Não | Bioma: "Amazonia", "Cerrado", "Caatinga", "Mata Atlantica", "Pantanal", "Pampa". Default: "Cerrado" |
| `ano` | `int` | Não | Ano (ex: 2022). Se None, todos os anos, até o teto de `max_registros` |
| `uf` | `str` | Não | Filtrar por UF (ex: "MT") |
| `max_registros` | `int \| None` | Não | Teto de feições lidas; padrão 50.000. `None` lê a seleção inteira. Se o teto cortar a seleção, sai `UserWarning` com o total do WFS e o número retornado |
| `tamanho_pagina` | `int \| None` | Não | Feições por página do WFS; padrão 500, máximo 2.000 |
| `as_polars` | `bool` | Não | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Não | Se True, retorna `(DataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ano` | int | Ano do desmatamento |
| `uf` | str | Código UF (ex: "MT") |
| `classe` | str | Classe de cobertura (ex: "desmatamento") |
| `area_km2` | float | Area desmatada em km2 |
| `satelite` | str | Satelite utilizado |
| `sensor` | str | Sensor do satelite |
| `bioma` | str | Bioma consultado |

Cada linha é uma feição publicada. Além dessas, saem `feature_id`, `uuid`, `fid`, `estado_original`, `path_row`, `class_name`, `def_cloud`, `julian_day`, `image_date`, `scene_id`, `publish_year`, `source` e `pub_date` (20 colunas, contrato `desmatamento_prodes_feicoes` 2.0). `uf` sai nulo quando o estado publicado não é reconhecido; o texto original fica em `estado_original`.

### Biomas Disponíveis (PRODES)

| Bioma | Workspace GeoServer | Layer | Serie Histórica |
|-------|--------------------|----|---|
| Amazonia | prodes-amazon-nb | yearly_deforestation_biome | 2000+ |
| Cerrado | prodes-cerrado-nb | yearly_deforestation | 2000+ |
| Caatinga | prodes-caatinga-nb | yearly_deforestation | 2000+ |
| Mata Atlantica | prodes-mata-atlantica-nb | yearly_deforestation | 2000+ |
| Pantanal | prodes-pantanal-nb | yearly_deforestation | 2000+ |
| Pampa | prodes-pampa-nb | yearly_deforestation | 2000+ |

---

## `desmatamento.prodes_geo()`

Dados PRODES com geometria (poligonos). Retorna `GeoDataFrame` com coluna `geometry` (MultiPolygon EPSG:4326).

Requer dependencia opcional: `pip install agrobr[geo]`

```python
import agrobr

gdf = await agrobr.desmatamento.prodes_geo(
    bioma="Cerrado",
    ano=2022,
    uf="MT",
)

# Cruzamento geoespacial com CAR/SICAR
import geopandas as gpd
car = gpd.read_file("imoveis_car.geojson")
desmatamento_em_car = gpd.sjoin(gdf, car)
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `bioma` | `str` | Nao | Bioma: "Amazonia", "Cerrado", "Caatinga", "Mata Atlantica", "Pantanal", "Pampa". Default: "Cerrado" |
| `ano` | `int` | Nao | Ano (ex: 2022). Se None, todos os anos |
| `uf` | `str` | Nao | Filtrar por UF (ex: "MT") |
| `max_registros` | `int \| None` | Nao | Teto de feições lidas; padrão 10.000. `None` lê a seleção inteira |
| `tamanho_pagina` | `int \| None` | Nao | Feições por página do WFS; padrão 100, máximo 500 |
| `return_meta` | `bool` | Nao | Se True, retorna `(GeoDataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano do desmatamento |
| `uf` | str | Codigo UF (ex: "MT") |
| `classe` | str | Classe de cobertura (ex: "desmatamento") |
| `area_km2` | float | Area desmatada em km2 |
| `satelite` | str | Satelite utilizado |
| `sensor` | str | Sensor do satelite |
| `bioma` | str | Bioma consultado |
| `geometry` | geometry | MultiPolygon EPSG:4326 |

Cada linha é uma feição publicada, com as mesmas 20 colunas de `prodes()` mais `geometry`.

### Notas

- Padrão `max_registros=10000`, em páginas de `tamanho_pagina=100`; use filtros (uf, ano) ou `max_registros=None`
- Se `max_registros` cortar a seleção, sai `UserWarning` com o total do WFS e o número retornado
- Coluna de geometria no GeoServer e `geom` para todos os 6 biomas (uniforme)

---

## `desmatamento.deter()`

Alertas diários de desmatamento em tempo real.

```python
import agrobr

df = await agrobr.desmatamento.deter(
    bioma="Amazônia",
    uf="PA",
    inicio="2024-01-01",
    fim="2024-06-30",
)
```

### Parâmetros

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| `bioma` | `str` | Não | Bioma: "Amazonia", "Cerrado". Default: "Amazonia" |
| `uf` | `str` | Não | Filtrar por UF (ex: "PA") |
| `inicio` | `str`, `date` ou `datetime` | Não | Data inicial: `date`, `datetime` (a hora é descartada) ou texto `AAAA-MM-DD` ou `DD/MM/AAAA` |
| `fim` | `str`, `date` ou `datetime` | Não | Data final, nos mesmos formatos; anterior ao `inicio` levanta `InvalidParameterError` |
| `classe` | `str` | Não | Filtrar por classe de alerta |
| `max_registros` | `int \| None` | Não | Teto de feições lidas; padrão 50.000. `None` lê a seleção inteira. Se o teto cortar a seleção, sai `UserWarning` com o total do WFS e o número retornado |
| `tamanho_pagina` | `int \| None` | Não | Feições por página do WFS; padrão 500, máximo 2.000 |
| `as_polars` | `bool` | Não | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Não | Se True, retorna `(DataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `data` | date | Data do alerta |
| `classe` | str | Tipo de alerta (DESMATAMENTO_CR, DEGRADACAO, MINERACAO, etc.) |
| `uf` | str | Código UF |
| `municipio` | str | Nome do município |
| `municipio_id` | str | Código IBGE do município como publicado (texto; nulo no DETER Cerrado) |
| `area_km2` | float | Area em km2 |
| `satelite` | str | Satelite utilizado |
| `sensor` | str | Sensor do satelite |
| `bioma` | str | Bioma consultado |

Cada linha é uma feição publicada. Além dessas, saem `feature_id`, `gid`, `uf_original`, `quadrant`, `path_row`, `areauckm`, `uc`, `publish_month`, `created_date` e `areatotalkm` (19 colunas, contrato `desmatamento_deter_feicoes` 2.0). `uf` sai nulo quando a UF publicada não é reconhecida; o texto original fica em `uf_original`.

### Classes DETER

| Classe | Descrição |
|--------|-----------|
| `DESMATAMENTO_CR` | Desmatamento com corte raso |
| `DESMATAMENTO_VEG` | Desmatamento com vegetacao secundaria |
| `DEGRADACAO` | Degradacao florestal |
| `MINERACAO` | Atividade de mineracao |
| `CICATRIZ_DE_QUEIMADA` | Cicatriz de queimada |
| `CS_DESORDENADO` | Corte seletivo desordenado |
| `CS_GEOMETRICO` | Corte seletivo geometrico |

---

## `desmatamento.deter_geo()`

Alertas DETER com geometria (poligonos). Retorna `GeoDataFrame` com coluna `geometry` (MultiPolygon EPSG:4326).

Requer dependencia opcional: `pip install agrobr[geo]`

```python
import agrobr

gdf = await agrobr.desmatamento.deter_geo(
    bioma="Amazônia",
    uf="PA",
    inicio="2024-01-01",
    fim="2024-06-30",
)

# Cruzamento geoespacial com os imóveis rurais do CAR (SICAR, também em EPSG:4326).
# O SICAR traz o perímetro do imóvel (tipo IRU, AST ou PCT), não a reserva legal.
import geopandas as gpd
imoveis = await agrobr.alt.sicar.imoveis_geo(
    "PA", municipio="Altamira", tipo="IRU", max_registros=None
)
alertas_em_imoveis = gpd.sjoin(gdf, imoveis[["cod_imovel", "geometry"]], predicate="intersects")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `bioma` | `str` | Nao | Bioma: "Amazonia", "Cerrado". Default: "Amazonia" |
| `uf` | `str` | Nao | Filtrar por UF (ex: "PA") |
| `inicio` | `str`, `date` ou `datetime` | Nao | Data inicial: `date`, `datetime` (a hora é descartada) ou texto `AAAA-MM-DD` ou `DD/MM/AAAA` |
| `fim` | `str`, `date` ou `datetime` | Nao | Data final, nos mesmos formatos; anterior ao `inicio` levanta `InvalidParameterError` |
| `classe` | `str` | Nao | Filtrar por classe de alerta |
| `max_registros` | `int \| None` | Nao | Teto de feições lidas; padrão 10.000. `None` lê a seleção inteira |
| `tamanho_pagina` | `int \| None` | Nao | Feições por página do WFS; padrão 100, máximo 500 |
| `return_meta` | `bool` | Nao | Se True, retorna `(GeoDataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `data` | date | Data do alerta |
| `classe` | str | Tipo de alerta (DESMATAMENTO_CR, DEGRADACAO, MINERACAO, etc.) |
| `uf` | str | Codigo UF |
| `municipio` | str | Nome do municipio |
| `municipio_id` | str | Código IBGE do município como publicado (texto; nulo no DETER Cerrado) |
| `area_km2` | float | Area em km2 |
| `satelite` | str | Satelite utilizado |
| `sensor` | str | Sensor do satelite |
| `bioma` | str | Bioma consultado |
| `geometry` | geometry | MultiPolygon EPSG:4326 |

Cada linha é uma feição publicada, com as mesmas 19 colunas de `deter()` mais `geometry`.

### Notas

- Padrão `max_registros=10000`, em páginas de `tamanho_pagina=100`; use filtros (uf, inicio/fim, classe) ou `max_registros=None`
- Se `max_registros` cortar a seleção, sai `UserWarning` com o total do WFS e o número retornado
- Volume aproximado: ~1.1 KB por feature com geometria

---

## Uso Síncrono

```python
from agrobr import sync

df = sync.desmatamento.prodes(bioma="Cerrado", ano=2022, uf="MT")
gdf_prodes = sync.desmatamento.prodes_geo(bioma="Cerrado", ano=2022, uf="MT")
df_deter = sync.desmatamento.deter(bioma="Amazônia", uf="PA", inicio="2024-01-01")
gdf = sync.desmatamento.deter_geo(bioma="Amazônia", uf="PA", inicio="2024-01-01")
```

## Fonte dos Dados

- **PRODES**: Programa de Monitoramento da Floresta Amazonica e demais Biomas Brasileiros por Satelite
- **DETER**: Sistema de Deteccao de Desmatamento em Tempo Real
- **Provedor**: INPE — Instituto Nacional de Pesquisas Espaciais
- **API**: TerraBrasilis GeoServer (WFS)
- **Licença**: CC BY-SA 4.0 (INPE), classificação `livre`: uso comercial permitido com atribuição ao INPE; adaptações compartilhadas seguem CompartilhaIgual. Veja [Licenças](../licenses.md)
