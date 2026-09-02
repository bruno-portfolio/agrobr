# ICMBio — Unidades de Conservacao Federais

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | ICMBio (Instituto Chico Mendes de Conservacao da Biodiversidade) |
| Dados | Limites de UCs federais |
| Acesso | WFS OGC (INDE GeoServer) |
| Formato | CSV (tabular) / GeoJSON (geo) |
| Autenticacao | Nenhuma |
| Licenca | Dados publicos governo federal |
| Features | 344 UCs federais |

## Acesso via WFS

| Parametro | Valor |
|-----------|-------|
| Endpoint | `geoservicos.inde.gov.br/geoserver/ICMBio/ows` |
| WFS Version | 1.1.0 |
| Layer | `ICMBio:limiteucsfederais_a` |
| CRS | EPSG:4674 |

## Exemplo de Uso

```python
import asyncio
from agrobr import icmbio

async def main():
    # Todas as UCs federais
    df = await icmbio.ucs()

    # Filtrar por grupo (PI = protecao integral, US = uso sustentavel)
    df = await icmbio.ucs(grupo="PI")

    # Filtrar por UF (usa LIKE, funciona com UCs multi-UF)
    df = await icmbio.ucs(uf="MT")

    # Com geometria (requer geopandas)
    gdf = await icmbio.ucs_geo(bbox=(-56, -16, -54, -14))

    # Com metadados
    df, meta = await icmbio.ucs(return_meta=True)

asyncio.run(main())
```

## Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| codigo | str | CNUC (codigo unico) |
| nome | str | Nome da UC |
| categoria | str | Sigla da categoria (PARNA, ESEC, FLONA, etc) |
| grupo | str | PI (protecao integral) ou US (uso sustentavel) |
| uf | str | UF(s) abrangidas (separadas por /) |
| bioma | str | Bioma IBGE |
| area_ha | float | Area em hectares |
| ano_criacao | Int64 | Ano de criacao |
| ato_criacao | str | Ato legal de criacao |

## Limitacoes

- Apenas UCs federais (344). Estaduais e municipais nao estao neste WFS.
- Campo `uf` pode conter multiplas UFs (ex: "MT/PA")
- Dados refletem o estado atual do GeoServer INDE/ICMBio
