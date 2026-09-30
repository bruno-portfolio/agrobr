# MapBiomas Alerta — Alertas de Desmatamento

## Visão Geral

| Item | Detalhe |
|------|---------|
| Provedor | MapBiomas |
| Dados | Alertas de desmatamento com geometria |
| Acesso | GraphQL API |
| Formato | JSON (GraphQL) |
| Autenticação | Token (env AGROBR_MAPBIOMAS_ALERTA_TOKEN) |
| Licença | Livre (citação obrigatória) |
| Volume | 533.140 alertas publicados desde 2019 (26/09/2026) |

## Acesso via GraphQL

| Parâmetro | Valor |
|-----------|-------|
| Endpoint | `https://plataforma.alerta.mapbiomas.org/api/v2/graphql` |
| Auth | Bearer token |

O token é pessoal e expira. Ele vem da mutation `signIn` da API, com o e-mail e a senha da conta na plataforma. Com o token
vencido ou inválido, a API responde "Token de acesso inválido", e o agrobr levanta `SourceUnavailableError` com essa
mensagem. O `alerta_info()` é público e não usa token.

## Exemplo de Uso

```python
import asyncio
from agrobr import mapbiomas_alerta

async def main():
    # Alertas detectados no período (requer token)
    df = await mapbiomas_alerta.alertas(
        token="seu-token",
        inicio="2025-01-01",
        fim="2025-01-31",
    )

    # Alertas do mês passado: filtre pela publicação
    df = await mapbiomas_alerta.alertas(
        inicio="2026-08-01",
        fim="2026-08-31",
        tipo_data="publicacao",
    )

    # Filtrar por fonte de detecção (valores do enum da API)
    df = await mapbiomas_alerta.alertas(
        sources=["DeterbAmazonia", "Sad"],
        inicio="2025-01-01",
        fim="2025-01-31",
    )

    # Filtrar por caixa (minlon, minlat, maxlon, maxlat)
    df = await mapbiomas_alerta.alertas(
        bbox=(-55, -8, -50, -3),
        inicio="2025-01-01",
        fim="2025-01-31",
    )

    # Coleção inteira, sem o teto padrão de 5.000
    df = await mapbiomas_alerta.alertas(
        inicio="2025-01-01",
        fim="2025-12-31",
        max_registros=None,
    )

    # Com geometria WKT (requer geopandas)
    gdf = await mapbiomas_alerta.alertas_geo(
        inicio="2025-01-01",
        fim="2025-01-31",
    )

    # Com metadados
    df, meta = await mapbiomas_alerta.alertas(inicio="2025-01-01", return_meta=True)

    # Polars
    df = await mapbiomas_alerta.alertas(inicio="2025-01-01", as_polars=True)

    # Info (intervalo de datas + última publicação)
    info = await mapbiomas_alerta.alerta_info()

asyncio.run(main())
```

## Parâmetros

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `inicio`, `fim` | str \| date \| datetime \| None | None | `date`, `datetime` (a hora é descartada) ou texto `AAAA-MM-DD` ou `DD/MM/AAAA` (o agrobr manda em ISO). Início depois do fim, ou outro tipo ou formato, levanta `InvalidParameterError` antes da rede. Sem `inicio`, a API começa em 2019-01-01 |
| `tipo_data` | str | `"deteccao"` | `"deteccao"` filtra a data de detecção; `"publicacao"`, a de publicação. Fora dos 2, `InvalidParameterError` |
| `sources` | list[str] \| None | None | Valores do enum da API: `DeterbAmazonia`, `DeterCerrado`, `DeterPantanal`, `Glad`, `IefMg`, `InemaBa`, `ProdesAmazonia`, `ProdesCerrado`, `ProdesMataAtlantica`, `ProdesPampa`, `ProdesPantanal`, `ProdesCaatinga`, `Sad`, `SadCaatinga`, `SadCerrado`, `SadMataAtlantica`, `SadPampa`, `SadPantanal`, `SipamSar`, `SiradX`, `SosAtlas` e `SosInpe`, e `All` (todas). Valor fora do enum (inclusive os nomes da coluna `fonte`, como `DETERB-AMAZONIA`) levanta `InvalidParameterError` antes da rede |
| `bbox` | tuple \| None | None | `(minlon, minlat, maxlon, maxlat)` em graus |
| `max_registros` | int \| None | 5000 | Teto de linhas. Acima dele, saem os alertas de menor código, com aviso em `validation_warnings` e `UserWarning` ("N de M alertas"). `None` traz a coleção inteira |

## Detecção × publicação

O alerta é publicado meses depois de detectado. Em 26/09/2026, nos 27.368 alertas detectados de setembro de 2024 a
fevereiro de 2025: a publicação saiu em mediana 152 dias depois da detecção; 90% em até 205 dias, 95% em até 223 e **99% em até
293 dias** (o máximo foi 607). Por isso, um período recente pela detecção vem quase vazio: agosto de 2026 tinha 0 alerta pela
detecção e 2.118 pela publicação.

Com `tipo_data="deteccao"` e um período que termina a menos de 293 dias de hoje (ou sem `fim`), o resultado sai com o aviso
"este período ainda ganha alertas enquanto a publicação chega", em `validation_warnings` e `UserWarning`. Para "os alertas do
mês passado", use `tipo_data="publicacao"`.

## Paginação e completude

O agrobr pagina em páginas de 500, na ordem do código do alerta (`ALERT_CODE ASC`). A ordem padrão da API, pela data de
detecção, admite empates, e as páginas repetiam e perdiam alertas (janeiro de 2025 saía com 4.196 dos 4.633). Cada consulta é
conferida contra o `totalCount` anunciado:

- código repetido entre páginas, ou menos alertas que o anunciado, levanta `ParseError` (repita a consulta);
- `totalCount` que muda no meio da paginação (publicação durante a consulta) sai como aviso;
- `meta.source_details` traz o `tipo_data`, o `total_anunciado` e, em `corpos`, a URL, o SHA-256 e o tamanho de cada página. Com
  uma página só, o SHA e o tamanho também vão em `raw_content_hash` e `raw_content_size`.

## Colunas

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| alert_code | Int64 | Código do alerta |
| area_ha | float | Área em hectares |
| data_deteccao | datetime | Data de detecção |
| data_publicacao | datetime | Data de publicação |
| status | str | Status do alerta (`published`) |
| fonte | str | Fontes de detecção como publicadas, separadas por ", " (ex.: `DETERB-AMAZONIA, SAD`) |
| lat | float | Latitude |
| lon | float | Longitude |
| geometry | Polygon | Geometria WKT (apenas alertas_geo; WKT inválido vira geometria nula, e o alerta fica) |

A consulta sem alertas devolve as mesmas colunas e os mesmos tipos (`alert_code` em `Int64`, datas em `datetime64[ns]`, `area_ha`,
`lat` e `lon` em `float64` e o texto no dtype padrão do pandas instalado); no `alertas_geo`, com o CRS `EPSG:4326`. O
`alerta_info()` levanta `ParseError` quando a resposta não traz `alertDateRange` ou `lastAlertPublication` com os campos, em vez
de devolver dicionários vazios.

## Limitações

- Requer token de autenticação (env `AGROBR_MAPBIOMAS_ALERTA_TOKEN` ou parâmetro `token=`), que expira
- A consulta não filtra por UF nem por município; a API tem `territoryIds`, que o agrobr não expõe
- Throttle após 5 páginas (3s de espera)
