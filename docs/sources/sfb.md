# SFB — Servico Florestal Brasileiro

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | SFB (Servico Florestal Brasileiro) |
| Dados | Florestas publicas (CNFP), concessoes florestais, Inventario Florestal Nacional (IFN) |
| Acesso | ArcGIS REST API |
| Formato | JSON sem geometria (tabular) / GeoJSON (geo) |
| Autenticacao | Nenhuma |
| Licenca | Dados publicos |

## Layers

| Layer | Features | Geometria | Filtros |
|-------|----------|-----------|---------|
| `cnfp` | 20.829 polígonos (23/09/2026) | Polygon | uf, bioma, categoria, bbox |
| `concessoes` | 8 polígonos | Polygon | uf, bbox |
| `ifn_conglomerados` | ~14,5 mil pontos (não conferido: serviço fora do ar) | Point | uf, bioma, bbox |

## Acesso via ArcGIS REST

| Parametro | Valor |
|-----------|-------|
| Base URL | `https://mapas.florestal.gov.br/server/rest/services` |
| CNFP Service | `Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9` |
| Concessoes Service | `Hosted/unidades_concessoes_florestais/FeatureServer/0` |
| IFN Service | `DadosAbertos-IFN/Conglomerado/FeatureServer/0` |
| Paginação | Por chave no CNFP e nas concessões (`fid > último`, `orderByFields=fid`), 2.000 feições por página; offset no IFN |
| Throttle | 2s delay apos 5 paginas |

## Exemplo de Uso

```python
import asyncio
from agrobr import sfb

async def main():
    # CNFP — Cadastro Nacional de Florestas Publicas
    df = await sfb.cnfp(uf="AM")
    df = await sfb.cnfp(bioma="Amazonia", categoria="FLONA")

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
| ano_criacao | Int64 | Ano de criação extraído da data publicada (ver Particularidades) |
| municipio | str | Municipio |

### concessoes

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| fid | int | ID do registro |
| nome | str | Nome da unidade |
| uf | str | UF (sigla) |
| bioma | str | Bioma |
| area_ha | float | Area em hectares |
| ano_criacao | Int64 | Ano de criação (o serviço publica o ano como inteiro) |
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
- **Paginação por chave**: no CNFP e nas concessões, as páginas seguem `fid` crescente (`fid > último` com `orderByFields=fid`). Se somarem menos feições que a contagem oficial, a consulta levanta `SourceUnavailableError` dizendo quantas faltam, em vez de devolver resultado parcial. Resposta HTML (manutenção ou bloqueio de WAF, mesmo com status 200) também vira `SourceUnavailableError`
- **Ano de criação no CNFP**: o campo `anocriacao` do serviço é texto com a data inteira (`DD/MM/AAAA`, `DD-MM-AAAA`; raramente `AAAA-MM-DD`, `AAAA/MM/DD` ou só o ano). O agrobr publica o ano quando o texto tem um único ano. Fica nulo quando o campo está em branco ou é `-`, e quando a data é composta com anos diferentes (sobreposição de unidades, por exemplo `22/06/2011 / 10-01-2002` numa "PA / APA"). Neste último caso, um `UserWarning` e `MetaInfo.validation_warnings` informam a contagem e até três exemplos do texto publicado; o log `sfb_ano_criacao_ambiguo` também é mantido. Na camada de 23/09/2026: 15.068 de 20.829 registros com ano, 4.718 em branco ou `-` e 1.043 compostos com anos diferentes
- **Tabular sem geometria**: `cnfp()`, `concessoes()` e `ifn_conglomerados()` pedem `returnGeometry=false` (a 1ª página do CNFP nacional cai de 378 MB para 0,5 MB); a geometria só vem nas funções `_geo`
- **Unidades e CRS**: área em hectares como publicada (`area_ha` no CNFP, `hectares` nas concessões), sem recálculo pela geometria. A geometria é pedida em EPSG:4326 (`outSR=4326`) e reprojetada pelo servidor (o CNFP é guardado em 3857 e as concessões em 4674)
- **Parâmetros**: argumento desconhecido levanta `TypeError` antes da rede; `uf`, `bioma` e `categoria` inválidos levantam `InvalidParameterError`; `bbox` inválido levanta `ValueError`
- **Filtros compostos**: CNFP e IFN aceitam filtro por bioma alem de uf e bbox

Identificadores, códigos e anos usam `Int64` anulável; áreas usam `float64`. O texto usa o dtype nativo do pandas (`str` no pandas 3, `object` no pandas 2), inclusive nos resultados vazios.

## Limitacoes

- Em 02/09/2026 e de novo em 23/09/2026, `ifn_conglomerados()` e `ifn_conglomerados_geo()` estão indisponíveis
  porque o serviço ArcGIS IFN informa `MapServer not started`. Enquanto o servico nao for
  restaurado, essas chamadas levantam `SourceUnavailableError`.
- Dados refletem o estado atual do ArcGIS Server do SFB
- Concessoes florestais tem poucos registros (~8 poligonos)
- Throttle de 2s apos 5 paginas para nao sobrecarregar o servidor
