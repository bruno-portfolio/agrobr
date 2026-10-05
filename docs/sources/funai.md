# FUNAI — Terras Indigenas

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | FUNAI (Fundacao Nacional dos Povos Indigenas) |
| Dados | Terras Indigenas poligonais |
| Acesso | WFS OGC (GeoServer) |
| Formato | GeoJSON do WFS 2.0 (`application/json`) nos dois modos |
| Autenticacao | Nenhuma |
| Licenca | Termo da FUNAI: reprodução com citação da fonte ([detalhes](../licenses.md#funai)) |
| Features | 665 TIs (23/09/2026) |

## Acesso via WFS

| Parametro | Valor |
|-----------|-------|
| Endpoint | `geoserver.funai.gov.br/geoserver/Funai/ows` |
| WFS Version | 2.0.0 |
| Layer | `Funai:tis_poligonais` |
| CRS | EPSG:4674 na camada; o agrobr pede `srsName=EPSG:4326` e devolve EPSG:4326 (reprojecao do GeoServer) |

## Exemplo de Uso

```python
import asyncio
from agrobr import funai

async def main():
    # Todas as TIs
    df = await funai.terras_indigenas()

    # Filtrar por UF
    df = await funai.terras_indigenas(uf="MT")

    # Filtrar por fase
    df = await funai.terras_indigenas(fase="Regularizada")

    # Com geometria (requer geopandas)
    gdf = await funai.terras_indigenas_geo(bbox=(-56, -16, -54, -14))

    # Com metadados
    df, meta = await funai.terras_indigenas(return_meta=True)

asyncio.run(main())
```

## Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| codigo | int | Codigo da TI |
| nome | str | Nome da TI |
| etnia | str | Etnia predominante |
| municipio | str | Municipio sede |
| uf | str | UF da TI como publicada; 18 TIs trazem mais de uma (ex. "AM, RR") |
| area_ha | float | Área declarada pela FUNAI em hectares (`superficie_perimetro_ha`), não a do polígono |
| fase | str | Fase do processo |
| modalidade | str | Modalidade da TI |
| data_atualizacao | datetime64[ns] | Data de atualização publicada em dd/mm/aaaa; nula em 146 das 665 TIs |
| feature_id | str | Identificador da feicao no WFS (texto; pode variar entre requisicoes) |
| gid | int | Identificador do registro na camada |
| reestudo_ti | str | Situacao de reestudo como publicada (vazio, "Reestudo" ou "Principal") |
| cr | str | Coordenacao Regional da FUNAI |
| faixa_fronteira | str | "Sim"/"Não", como publicado |
| undadm_codigo | int | Codigo da unidade administrativa |
| undadm_nome | str | Nome da unidade administrativa |
| undadm_sigla | str | Sigla da unidade administrativa |
| dominio_uniao | str | "t"/"f", como publicado |
| epsg | int | EPSG da geometria na fonte (4674 em todas as TIs) |

A UF e os indicadores administrativos saem com o texto publicado, no dtype padrão do pandas instalado (`str` no
pandas 3, `object` no 2); a data de atualização sai em `datetime64[ns]`, e a data ilegível vira `NaT` com
`UserWarning` e aviso em `meta.validation_warnings` (contrato `funai.terras_indigenas` 2.0).

`area_ha` é a área declarada pela FUNAI (`superficie_perimetro_ha`), repassada sem recálculo, e pode divergir do
polígono publicado: na TI Mashco do Rio Chandless (AC), 421 ha declarados contra 543.430 ha no polígono (26/09/2026).
Em `terras_indigenas_geo`, a terra cuja área declarada difere mais de 5 % da área do polígono (projeção Albers do IBGE)
sai com aviso em `validation_warnings` e `UserWarning`, e a lista com as 2 áreas fica em `source_details["area_divergente"]`.
`terras_indigenas`, sem geometria, não faz a comparação. No AC, 3 das 34 terras passam dos 5 %.

## Parametros

| Parametro | Padrao | Descricao |
|-----------|--------|-----------|
| `uf` | `None` | Sigla; casa qualquer UF do campo publicado (TIs em mais de um estado vem como "AM, RR") |
| `fase` | `None` | Uma das fases abaixo, igualdade exata |
| `bbox` | `None` | (lon_min, lat_min, lon_max, lat_max) em EPSG:4326 |
| `max_registros` | 10.000 (1.000 em `_geo`) | Teto de TIs lidas em ordem de codigo; `uf` e `fase` filtram localmente esse prefixo, e o corte que deixa a selecao parcial emite `UserWarning` |
| `tamanho_pagina` | 250 (10 em `_geo` ou com `bbox`) | Máximo 1.000 (100 em `_geo` ou com `bbox`) |

## Fases

Regularizada, Homologada, Declarada, Delimitada, Em Estudo, Encaminhada RI.

## Coleta bruta

`agrobr.bruto.coletar("funai", "terras_indigenas", ...)` guarda as páginas GeoJSON originais do WFS da camada `Funai:tis_poligonais`, no CRS nativo (`EPSG:4674`) e com todos os atributos, sem o `propertyName` e o `srsName` que `terras_indigenas()` e `terras_indigenas_geo()` usam. `agrobr.bruto.coletar("funai", "terras_indigenas_pontos", ...)` faz o mesmo com a camada `Funai:tis_pontos`, um ponto por TI na fase `Em Estudo` (163 em 04/10/2026), que a API tabular não lê. As duas coletas são sempre nacionais (UF e bbox recusadas). As páginas vêm em `sortBy=gid`, e a coleta exige `gid` inteiro, presente, único e estritamente crescente entre as páginas; como o GeoServer recusa `resultType=hits` (HTTP 403), as contagens antes e depois são o `numberMatched` de uma página de 1 feição. Os polígonos usam 20 feições por página no padrão: a maior TI passa de 2,9 MB, e páginas de 100 chegariam a 14 MB, acima do teto de 8 MiB. Veja a [API da coleta bruta](../api/bruto.md) e o [contrato do manifesto](../contracts/bruto.md).

## Limitacoes

- Apenas TIs poligonais (pontos e linhas excluidos)
- Dados refletem o estado atual do GeoServer FUNAI
- Licença: reprodução com citação da fonte, pelo termo da FUNAI para geoprocessamento e mapas; o rodapé do portal gov.br declara CC BY-ND 3.0 para o conteúdo do site
