# EMBRAPA Solos/GeoInfo — Perfis de Solo e Mapa Pedologico

> **Licença:** CC BY-NC 3.0 BR.
> Classificação: `nc`

Perfis de solo do PronaSolos e mapa pedologico do Brasil via WFS OGC
da EMBRAPA GeoInfo.

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | EMBRAPA (Empresa Brasileira de Pesquisa Agropecuária) |
| Dados | Perfis de solo (pontos) + mapa pedologico (poligonos) |
| Acesso | WFS OGC (GeoServer) |
| Formato | GeoJSON do WFS 2.0 (`application/json`) nos dois modos |
| Autenticação | Nenhuma |
| Licença | CC BY-NC 3.0 BR |
| Features | 34.464 registros de horizontes/camadas (~9 mil pontos) + 2.852 poligonos |

## Acesso via WFS

| Parâmetro | Valor |
|-----------|-------|
| Endpoint | `geoinfo.dados.embrapa.br/geoserver/ows` |
| WFS Version | 2.0.0 |
| Layer perfis | `geonode:perfis_pronasolos_2020` |
| Layer mapa | `geonode:brasil_solos_5m_20201104` |
| CRS | EPSG:4326 (declarado pelo WFS e pelo metadado ISO) |
| Paginação | Sim (count/startIndex) |

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

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| fid | int | Identificador do registro (horizonte ou camada) |
| uf | str | UF (sigla) |
| municipio | str | Município |
| latitude | float | Latitude |
| longitude | float | Longitude |
| horizonte | str | Simbolo do horizonte |
| profundidade | str | Profundidade |
| areia_total | str | Areia total (g/kg) |
| silte | str | Silte (g/kg) |
| argila | str | Argila (g/kg) |
| ph_h2o | str | pH em agua |
| carbono_organico | str | Carbono organico (unidade não declarada pela fonte) |
| ctc | str | Capacidade de troca cationica (T) |
| saturacao_bases | str | Saturacao por bases (V, %) |
| aluminio | str | Aluminio trocavel |
| fosforo | str | Fosforo assimilavel |
| classe_textural | str | Classe textural |
| nivel_levantamento | str | Nível do levantamento |
| uso_atual | str | Uso atual do solo |

A tabela mostra as colunas principais. O resultado tem 85 colunas, na ordem do contrato
`embrapa_solos.perfis` 3.0: os 83 atributos publicados (com os nomes acima ou o nome original da camada),
`uf_original` e `feature_id`. Cada linha e um horizonte ou camada; `codigo_pon` identifica o ponto de
amostragem.

A coluna `ano` usa `Int64` anulável, e `data_colet` usa `datetime64[ns]`. Nulos da fonte e o literal `NULL` viram ausentes nessas duas colunas. Pela [regra de datas](../guides/normalizacao.md#datas-das-fontes), `data_colet` de formato válido com ano fora de 1900–2099 vira `NaT`, com `UserWarning` e a mesma mensagem em `meta.validation_warnings` (em 07/10/2026, 73 dos 34.464 registros da camada, com datas como `0982-11-01` e `1892-07-14`). Ano ou data fora do formato esperado, ou data impossível (`2024-02-30`), levanta `ParseError`; não vira ausente silenciosamente. O texto usa o dtype nativo do pandas (`str` no pandas 3, `object` no pandas 2). Tabelas vazias têm os mesmos dtypes das tabelas com registros.

Os valores laboratoriais são o texto publicado pela Embrapa (o WFS declara `xsd:string`), sem conversão:
números com ponto decimal, as vezes com ruido de float32 (`4.400000095367432`), e o texto `NULL` para
ausência. Em `fosforo` também aparecem valores censurados (`<1`, `<0.5`), virgula decimal (`0,19`) e marcas
como `x`. Converta explicitamente, por exemplo `pd.to_numeric(df["argila"].replace("NULL", pd.NA))`; em
`fosforo`, trate antes os censurados e a virgula.

O WFS não declara unidades. Na camada inteira, areia + silte + argila somam 1.000 (g/kg) em 99,2 % dos
horizontes com as tres medidas; `saturacao_bases` = 100 x S / T e `ctc` = S + H + Al (colunas `valor_s`,
`hidrogenio` e `aluminio`) em mais de 98 % dos horizontes.

## Colunas — Mapa Pedologico

| Coluna | Tipo | Descrição |
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
| ordem3 | str | Ordem pedologica 3 |
| subordem3 | str | Subordem 3 |
| gdegrupo3 | str | Grande grupo 3 |
| feature_id | str | Identificador da feicao no WFS (texto, não e chave) |

## Particularidades

- **Funções `_geo()` requerem [geo]**: `pip install agrobr[geo]` (geopandas)
- **Filtro de ordem**: `ordem` casa a classe inteira de `ordem1`, uma das 15 publicadas na camada `brasil_solos_5m_20201104` (13 ordens de solo mais `AFLORAMENTOS DE ROCHAS` e `DUNAS`; leitura completa em 01/10/2026: 2.852 polígonos, 177 sem `ordem1`). Caixa, acento e singular são aceitos (`"latossolo"` vale `LATOSSOLOS`); trecho (`"latos"`), texto vazio, não textual ou fora das classes levanta `InvalidParameterError` antes da coleta, com a lista das classes. Uma leitura completa sem correspondência emite `UserWarning` e registra o valor pedido e as classes observadas em `MetaInfo.validation_warnings`; uma leitura parcial continua avisando sobre o prefixo remoto.
- **Paginação**: count/startIndex ordenado por `fid`, com 1 registro de sobreposicao entre páginas. `max_registros` (padrão 50.000; 5.000 perfis e 3.000 poligonos nas funções `_geo`) corta o prefixo remoto; os filtros `uf` e `ordem` são aplicados localmente sobre esse prefixo e, quando o corte deixa a selecao parcial, sai um `UserWarning` (`max_registros=None` varre a camada inteira)
- **CRS**: EPSG:4326, o CRS padrão das duas camadas no WFS; o `bbox` também e EPSG:4326
- **Licença NC**: uso comercial requer autorizacao da EMBRAPA
- **Texto com dupla codificação**: a Embrapa publica parte dos textos dos perfis com dupla codificação (UTF-8 lido como Latin-1: "AptidÃ£o", "SÃ£o Carlos"), no JSON e no CSV do WFS. O agrobr repara só o texto que volta inteiro por Latin-1 → UTF-8 e tem a assinatura ("Ã" ou "Â" seguido de um caractere entre U+0080 e U+00BF). Texto legítimo com "Ã" fica como está. A contagem por coluna sai em `MetaInfo.validation_warnings`. Ficam sem reparo, contados à parte no mesmo aviso ("com a assinatura e sem reparo"), 3 casos que a fonte publica assim: texto cortado a cerca de 254 caracteres no meio de uma sequência UTF-8, texto com "�" publicado e texto com "€" (33 células no DF em 26/09/2026)

## Limitacoes

- Cobertura de perfis não e uniforme (PronaSolos ainda em execução)
- Mapa pedologico na escala 1:5.000.000 (visao nacional, não cadastral)
- CC BY-NC 3.0 BR: redistribuicao comercial requer autorizacao

## Geometria inválida

A geometria das saídas `_geo` sai como a fonte publica, sem reparo; geometria inválida gera aviso e contagem no `MetaInfo`. Veja [Geometrias publicadas](../guides/normalizacao.md#geometrias-publicadas).
