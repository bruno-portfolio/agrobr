# EMBRAPA Solos/GeoInfo — Perfis de Solo e Mapa Pedologico

> **Licenca:** CC BY-NC 3.0 BR.
> Classificacao: `nc`

Perfis de solo do PronaSolos e mapa pedologico do Brasil via WFS OGC
da EMBRAPA GeoInfo.

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | EMBRAPA (Empresa Brasileira de Pesquisa Agropecuaria) |
| Dados | Perfis de solo (pontos) + mapa pedologico (poligonos) |
| Acesso | WFS OGC (GeoServer) |
| Formato | CSV (tabular) / GeoJSON (geo) |
| Autenticacao | Nenhuma |
| Licenca | CC BY-NC 3.0 BR |
| Features | ~34K perfis + ~2.8K poligonos |

## Acesso via WFS

| Parametro | Valor |
|-----------|-------|
| Endpoint | `geoinfo.dados.embrapa.br/geoserver/ows` |
| WFS Version | 2.0.0 |
| Layer perfis | `geonode:perfis_pronasolos_2020` |
| Layer mapa | `geonode:brasil_solos_5m_20201104` |
| CRS | EPSG:4674 |
| Paginacao | Sim (count/startIndex) |

## Exemplo de Uso

```python
import asyncio
from agrobr import embrapa_solos

async def main():
    # Perfis de solo (tabular)
    df = await embrapa_solos.perfis()

    # Filtrar por UF
    df = await embrapa_solos.perfis(uf="MT")

    # Perfis com geometria (requer geopandas)
    gdf = await embrapa_solos.perfis_geo(bbox=(-56, -16, -54, -14))

    # Mapa pedologico (tabular)
    df = await embrapa_solos.mapa_solos()

    # Mapa pedologico com geometria
    gdf = await embrapa_solos.mapa_solos_geo(bbox=(-56, -16, -54, -14))

    # Com metadados
    df, meta = await embrapa_solos.perfis(return_meta=True)

    # Polars
    df = await embrapa_solos.perfis(as_polars=True)

asyncio.run(main())
```

## Colunas — Perfis

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| fid | int | Identificador do perfil |
| uf | str | UF (sigla) |
| municipio | str | Municipio |
| latitude | float | Latitude |
| longitude | float | Longitude |
| horizonte | str | Simbolo do horizonte |
| profundidade | str | Profundidade |
| areia_total | float | Teor de areia total (g/kg) |
| silte | float | Teor de silte (g/kg) |
| argila | float | Teor de argila (g/kg) |
| ph_h2o | float | pH em agua |
| carbono_organico | float | Carbono organico (g/kg) |
| ctc | float | Capacidade de troca cationica |
| saturacao_bases | float | Saturacao por bases (V%) |
| aluminio | float | Aluminio trocavel |
| fosforo | float | Fosforo assimilavel |
| classe_textural | str | Classe textural |
| nivel_levantamento | str | Nivel do levantamento |
| uso_atual | str | Uso atual do solo |

## Colunas — Mapa Pedologico

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| fid | int | Identificador do poligono |
| simbolos | str | Simbolos SiBCS |
| comp1 | str | Componente 1 |
| comp2 | str | Componente 2 |
| comp3 | str | Componente 3 |
| legenda | str | Legenda descritiva |
| area_km2 | float | Area em km2 |
| ordem1 | str | Ordem pedologica 1 |
| subordem1 | str | Subordem 1 |
| gdegrupo1 | str | Grande grupo 1 |
| ordem2 | str | Ordem pedologica 2 |
| subordem2 | str | Subordem 2 |
| gdegrupo2 | str | Grande grupo 2 |
| legenda_sinotica | str | Legenda sinotica |
| classe_dom | str | Classe dominante |

## Particularidades

- **Funcoes `_geo()` requerem [geo]**: `pip install agrobr[geo]` (geopandas)
- **Paginacao**: 34K+ perfis exigem paginacao automatica via WFS
- **CRS**: EPSG:4674 (SIRGAS 2000)
- **Licenca NC**: uso comercial requer autorizacao da EMBRAPA

## Limitacoes

- Cobertura de perfis nao e uniforme (PronaSolos ainda em execucao)
- Mapa pedologico na escala 1:5.000.000 (visao nacional, nao cadastral)
- CC BY-NC 3.0 BR: redistribuicao comercial requer autorizacao
