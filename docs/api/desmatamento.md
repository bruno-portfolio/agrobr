# Desmatamento (PRODES/DETER)

Dados de desmatamento do INPE via TerraBrasilis — PRODES (consolidado anual) e DETER (alertas em tempo real).

## `desmatamento.prodes()`

Dados anuais de desmatamento consolidado por poligono.

```python
import agrobr

df = await agrobr.desmatamento.prodes(bioma="Cerrado", ano=2022, uf="MT")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `bioma` | `str` | Nao | Bioma: "Cerrado", "Caatinga", "Mata Atlantica", "Pantanal", "Pampa". Default: "Cerrado" |
| `ano` | `int` | Nao | Ano (ex: 2022). Se None, todos os anos |
| `uf` | `str` | Nao | Filtrar por UF (ex: "MT") |
| `return_meta` | `bool` | Nao | Se True, retorna `(DataFrame, MetaInfo)` |

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

### Biomas Disponiveis (PRODES)

| Bioma | Workspace GeoServer | Serie Historica |
|-------|--------------------|----|
| Cerrado | prodes-cerrado-nb | 2000+ |
| Caatinga | prodes-caatinga-nb | 2000+ |
| Mata Atlantica | prodes-mata-atlantica-nb | 2000+ |
| Pantanal | prodes-pantanal-nb | 2000+ |
| Pampa | prodes-pampa-nb | 2000+ |

---

## `desmatamento.deter()`

Alertas diarios de desmatamento em tempo real.

```python
import agrobr

df = await agrobr.desmatamento.deter(
    bioma="Amazônia",
    uf="PA",
    data_inicio="2024-01-01",
    data_fim="2024-06-30",
)
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `bioma` | `str` | Nao | Bioma: "Amazonia", "Cerrado". Default: "Amazonia" |
| `uf` | `str` | Nao | Filtrar por UF (ex: "PA") |
| `data_inicio` | `str` | Nao | Data inicial YYYY-MM-DD |
| `data_fim` | `str` | Nao | Data final YYYY-MM-DD |
| `classe` | `str` | Nao | Filtrar por classe de alerta |
| `return_meta` | `bool` | Nao | Se True, retorna `(DataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `data` | date | Data do alerta |
| `classe` | str | Tipo de alerta (DESMATAMENTO_CR, DEGRADACAO, MINERACAO, etc.) |
| `uf` | str | Codigo UF |
| `municipio` | str | Nome do municipio |
| `municipio_id` | int | Codigo IBGE do municipio |
| `area_km2` | float | Area em km2 |
| `satelite` | str | Satelite utilizado |
| `sensor` | str | Sensor do satelite |
| `bioma` | str | Bioma consultado |

### Classes DETER

| Classe | Descricao |
|--------|-----------|
| `DESMATAMENTO_CR` | Desmatamento com corte raso |
| `DESMATAMENTO_VEG` | Desmatamento com vegetacao secundaria |
| `DEGRADACAO` | Degradacao florestal |
| `MINERACAO` | Atividade de mineracao |
| `CICATRIZ_DE_QUEIMADA` | Cicatriz de queimada |
| `CS_DESORDENADO` | Corte seletivo desordenado |
| `CS_GEOMETRICO` | Corte seletivo geometrico |

---

## Uso Sincrono

```python
from agrobr import sync

df = sync.desmatamento.prodes(bioma="Cerrado", ano=2022, uf="MT")
df_deter = sync.desmatamento.deter(bioma="Amazônia", uf="PA", data_inicio="2024-01-01")
```

## Fonte dos Dados

- **PRODES**: Programa de Monitoramento da Floresta Amazonica e demais Biomas Brasileiros por Satelite
- **DETER**: Sistema de Deteccao de Desmatamento em Tempo Real
- **Provedor**: INPE — Instituto Nacional de Pesquisas Espaciais
- **API**: TerraBrasilis GeoServer (WFS)
- **Licenca**: Dados publicos governo federal — uso livre com citacao
