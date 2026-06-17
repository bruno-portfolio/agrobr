# SFB — Servico Florestal Brasileiro

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | SFB (Servico Florestal Brasileiro) |
| Dados | Florestas publicas (CNFP), concessoes florestais, Inventario Florestal Nacional (IFN) |
| Acesso | ArcGIS REST API |
| Formato | JSON (tabular) / GeoJSON (geo) |
| Autenticacao | Nenhuma |
| Licenca | Dados publicos |

## Layers

| Layer | Features | Geometria | Filtros |
|-------|----------|-----------|---------|
| `cnfp` | ~20.8K polygons | Polygon | uf, bioma, categoria, bbox |
| `concessoes` | ~8 polygons | Polygon | uf, bbox |
| `ifn_conglomerados` | ~14.5K points | Point | uf, bioma, bbox |

## Acesso via ArcGIS REST

| Parametro | Valor |
|-----------|-------|
| Base URL | `https://mapas.florestal.gov.br/server/rest/services` |
| CNFP Service | `Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9` |
| Concessoes Service | `Hosted/unidades_concessoes_florestais/FeatureServer/0` |
| IFN Service | `DadosAbertos-IFN/Conglomerado/FeatureServer/0` |
| Paginacao | Automatica (2K features/pagina) |
| Throttle | 2s delay apos 5 paginas |

## Exemplo de Uso

```python
import asyncio
from agrobr import sfb

async def main():
    # CNFP — Cadastro Nacional de Florestas Publicas
    df = await sfb.cnfp(uf="AM")
    df = await sfb.cnfp(bioma="Amazonia", categoria="B")

    # CNFP com geometria
    gdf = await sfb.cnfp_geo(uf="PA")

    # Concessoes florestais
    df = await sfb.concessoes()
    gdf = await sfb.concessoes_geo()

    # IFN — Inventario Florestal Nacional (conglomerados)
    df = await sfb.ifn_conglomerados(uf="MG")
    df = await sfb.ifn_conglomerados(bioma="Cerrado")

    # IFN com geometria
    gdf = await sfb.ifn_conglomerados_geo(uf="SP")

    # Filtrar por bbox
    df = await sfb.cnfp(bbox=(-60, -10, -55, -5))

    # Com metadados
    df, meta = await sfb.cnfp(uf="AM", return_meta=True)

    # Polars
    df = await sfb.cnfp(as_polars=True)

asyncio.run(main())
```

## Colunas por Layer

### cnfp

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| fid | int | ID do registro |
| nome | str | Nome da floresta publica |
| uf | str | UF (sigla) |
| bioma | str | Bioma |
| categoria | str | Categoria da floresta |
| tipo | str | Tipo |
| governo | str | Esfera de governo |
| classe | str | Classe |
| area_ha | float | Area em hectares |
| ano_criacao | int | Ano de criacao |
| municipio | str | Municipio |

### concessoes

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| fid | int | ID do registro |
| nome | str | Nome da unidade |
| uf | str | UF (sigla) |
| bioma | str | Bioma |
| area_ha | float | Area em hectares |
| ano_criacao | int | Ano de criacao |
| grupo | str | Grupo |
| categoria | str | Categoria |

### ifn_conglomerados

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| id | int | ID do registro |
| codigo_lote | int | Codigo do lote |
| lote | str | Lote |
| conglomerado | str | Conglomerado |
| uf | str | UF (sigla) |
| municipio | str | Municipio |
| bioma | str | Bioma |

## Particularidades

- **CNFP service name**: inclui data de retificacao no path (`CNFP_v19_03_retificado_17072025`)
- **Paginacao automatica**: 2K features por pagina com connection reuse
- **Filtros compostos**: CNFP e IFN aceitam filtro por bioma alem de uf e bbox

## Limitacoes

- Dados refletem o estado atual do ArcGIS Server do SFB
- Concessoes florestais tem poucos registros (~8 poligonos)
- Throttle de 2s apos 5 paginas para nao sobrecarregar o servidor
