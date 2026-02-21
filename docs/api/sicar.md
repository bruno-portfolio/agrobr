# SICAR (Cadastro Ambiental Rural)

Dados tabulares de imoveis rurais do CAR via WFS do GeoServer SICAR.

## imoveis

Registros individuais de imoveis rurais (sem geometria).

```python
import agrobr

df = await agrobr.alt.sicar.imoveis("DF")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| uf | str | Sim | Sigla da UF (ex: "MT", "DF", "BA") |
| municipio | str | Nao | Filtro parcial de municipio (case-insensitive) |
| status | str | Nao | AT, PE, SU ou CA |
| tipo | str | Nao | IRU, AST ou PCT |
| area_min | float | Nao | Area minima em hectares |
| area_max | float | Nao | Area maxima em hectares |
| criado_apos | str | Nao | Data minima de criacao (ISO, ex: "2020-01-01") |
| return_meta | bool | Nao | Se True, retorna (DataFrame, MetaInfo) |

### Colunas de retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| cod_imovel | str | Codigo unico do imovel |
| status | str | AT/PE/SU/CA |
| data_criacao | datetime | Data de criacao |
| data_atualizacao | datetime | Ultima atualizacao (nullable) |
| area_ha | float | Area em hectares |
| condicao | str | Condicao do cadastro (nullable) |
| uf | str | Sigla UF |
| municipio | str | Nome do municipio |
| cod_municipio_ibge | int | Codigo IBGE |
| modulos_fiscais | float | Modulos fiscais |
| tipo | str | IRU/AST/PCT |

### Exemplos

```python
# Imoveis ativos em Sorriso-MT
df = await agrobr.alt.sicar.imoveis(
    "MT", municipio="Sorriso", status="AT"
)

# Imoveis grandes (>1000 ha) no DF
df = await agrobr.alt.sicar.imoveis("DF", area_min=1000)

# Cadastros criados apos 2020
df = await agrobr.alt.sicar.imoveis(
    "GO", criado_apos="2020-01-01"
)

# Com metadados de proveniencia
df, meta = await agrobr.alt.sicar.imoveis("DF", return_meta=True)
print(meta.records_count, meta.fetch_duration_ms)
```

## resumo

Estatisticas agregadas por UF ou municipio.

```python
df = await agrobr.alt.sicar.resumo("MT")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| uf | str | Sim | Sigla da UF |
| municipio | str | Nao | Filtro de municipio |
| return_meta | bool | Nao | Se True, retorna (DataFrame, MetaInfo) |

### Retorno sem municipio (UF-level)

Usa `resultType=hits` (4 requests rapidos, sem download de dados):

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| total | int | Total de imoveis |
| ativos | int | Imoveis com status AT |
| pendentes | int | Imoveis com status PE |
| suspensos | int | Imoveis com status SU |
| cancelados | int | Imoveis com status CA |

### Retorno com municipio

Busca dados e agrega client-side:

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| total | int | Total de imoveis |
| ativos | int | Imoveis com status AT |
| pendentes | int | Imoveis com status PE |
| suspensos | int | Imoveis com status SU |
| cancelados | int | Imoveis com status CA |
| area_total_ha | float | Soma das areas |
| area_media_ha | float | Media das areas |
| modulos_fiscais_medio | float | Media de modulos fiscais |
| por_tipo_IRU | int | Imoveis rurais |
| por_tipo_AST | int | Assentamentos |
| por_tipo_PCT | int | Terras indigenas |

### Exemplos

```python
# Resumo do DF (rapido, sem download)
df = await agrobr.alt.sicar.resumo("DF")

# Resumo de Sorriso-MT (com agregacao)
df = await agrobr.alt.sicar.resumo("MT", municipio="Sorriso")
```

## Uso sincrono

```python
from agrobr import sync

df = sync.alt.sicar.imoveis("DF")
df = sync.alt.sicar.resumo("MT", municipio="Sorriso")
```

## Fonte de dados

- **Provedor:** Servico Florestal Brasileiro (SFB) / SICAR
- **API:** WFS 2.0.0 (OGC GeoServer)
- **Licenca:** CC-BY (dados abertos governo federal)
- **Atualizacao:** continua (cadastros em tempo real)
