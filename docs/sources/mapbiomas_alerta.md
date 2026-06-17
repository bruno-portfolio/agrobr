# MapBiomas Alerta — Alertas de Desmatamento

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | MapBiomas |
| Dados | Alertas de desmatamento com geometria |
| Acesso | GraphQL API |
| Formato | JSON (GraphQL) |
| Autenticacao | Token (env AGROBR_MAPBIOMAS_ALERTA_TOKEN) |
| Licenca | Livre (citacao obrigatoria) |
| Features | ~500K+ alertas |

## Acesso via GraphQL

| Parametro | Valor |
|-----------|-------|
| Endpoint | `https://plataforma.alerta.mapbiomas.org/api/v2/graphql` |
| Auth | Bearer token |

## Exemplo de Uso

```python
import asyncio
from agrobr import mapbiomas_alerta

async def main():
    # Alertas tabulares (requer token)
    df = await mapbiomas_alerta.alertas(
        token="seu-token",
        start_date="2024-01-01",
        end_date="2024-06-30",
    )

    # Filtrar por fonte de deteccao
    df = await mapbiomas_alerta.alertas(
        sources=["DETER", "SAD"],
        start_date="2024-01-01",
    )

    # Filtrar por bounding box
    df = await mapbiomas_alerta.alertas(
        bbox=(-56, -16, -54, -14),
    )

    # Com geometria WKT (requer geopandas)
    gdf = await mapbiomas_alerta.alertas_geo(
        start_date="2024-01-01",
        end_date="2024-03-31",
    )

    # Com metadados
    df, meta = await mapbiomas_alerta.alertas(return_meta=True)

    # Polars
    df = await mapbiomas_alerta.alertas(as_polars=True)

    # Info (date range + ultima publicacao)
    info = await mapbiomas_alerta.alerta_info()

asyncio.run(main())
```

## Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| alert_code | str | Codigo do alerta |
| area_ha | float | Area em hectares |
| data_deteccao | datetime | Data de deteccao |
| data_publicacao | datetime | Data de publicacao |
| status | str | Status do alerta |
| fonte | str | Fonte de deteccao (DETER, SAD, GLAD, SAD Caatinga) |
| lat | float | Latitude |
| lon | float | Longitude |
| geometry | Polygon | Geometria WKT (apenas alertas_geo) |

## Limitacoes

- Requer token de autenticacao (env `AGROBR_MAPBIOMAS_ALERTA_TOKEN` ou parametro `token=`)
- Paginacao limitada a 50 paginas por consulta
- Throttle apos 5 paginas (3s delay)
