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
| `ifn_conglomerados` | 68 pontos no DF (02/10/2026; total nacional não conferido) | Point | uf, bioma, bbox |

## Acesso via ArcGIS REST

| Parametro | Valor |
|-----------|-------|
| Base URL | `https://mapas.florestal.gov.br/server/rest/services` |
| CNFP Service | `Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9` |
| Concessoes Service | `Hosted/unidades_concessoes_florestais/FeatureServer/0` |
| IFN Service | `DadosAbertos-IFN/dataset_ifn_tb_pontos_lote/FeatureServer/0` |
| Paginação | Por chave no CNFP e nas concessões (`fid > último`, `orderByFields=fid`), 2.000 feições por página; IFN por `co_pontos_lote` e cadastro auxiliar por `co_lote` |
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
| ano_criacao_texto | str | Texto publicado em `anocriacao`, como veio da fonte (nulo quando a fonte publica o campo vazio; feição sem o campo `anocriacao` levanta `ParseError`) |
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
| lote | str, anulável | Nome publicado no cadastro auxiliar, por junção em `co_lote` |
| conglomerado | str | Conglomerado |
| uf | str | UF (sigla) |
| municipio | str | Municipio |
| bioma | str | Bioma |
| ciclo | str, anulável | Texto de `nu_ciclo_execucao` publicado pela fonte |

## Particularidades

- **IFN, schema 1.1**: preserva as sete colunas anteriores e acrescenta `ciclo` como texto anulável. `lote` vem de `DadosAbertos-IFN/dataset_ifn_tb_lote/FeatureServer/23`, consultado sem geometria, somente para os códigos referenciados, em grupos de até 100. A junção muitos para um preserva a ordem e o número de pontos. Código sem correspondente, código duplicado no cadastro ou campo ausente levanta `ParseError`; falha na consulta auxiliar não devolve uma tabela parcial. Código ou nome de lote publicado como nulo permanece nulo e é registrado em `MetaInfo.validation_warnings`.
- **Proveniência do IFN**: `source_details.resources` lista as páginas de dados dos pontos e dos lotes, cada uma com `role`, URL de consulta, `sha256` e `bytes`. As respostas de contagem não integram essa lista. `raw_content_hash` é o SHA-256 da lista serializada como JSON UTF-8 com chaves ordenadas, sem espaços e com caracteres Unicode preservados (`sort_keys=True`, `separators=(",", ":")`, `ensure_ascii=False`); `raw_content_size` mede esse manifesto. `source_details.hash_kind` é `resource_manifest_sha256`, e `source_details.resource_bytes` soma os corpos originais. O recorte vazio tem a lista `[]` e não consulta o cadastro.
- **Reconstrução do digest IFN**: `source_details.manifest_encoding="canonical_json_utf8"` e `manifest_fields=["resources"]` identificam a serialização e sua origem. `manifest_root="resources"` indica que o conteúdo serializado é diretamente a lista em `source_details.resources`, sem envolvê-la em um objeto. A codificação UTF-8 preserva Unicode, ordena chaves e usa separadores compactos, conforme os parâmetros descritos acima.
- **Filtro de bioma no IFN**: usa `UPPER(no_bioma)`; o texto devolvido preserva a caixa publicada, como `Cerrado` no DF. A variante geo pede EPSG:4326; o CRS nativo dos pontos é EPSG:4674. O health consulta a contagem de pontos do DF, sem certificar a junção; a reconciliação confere IDs, atributos e nomes de lote por leitura independente.

- **CNFP service name**: inclui data de retificacao no path (`CNFP_v19_03_retificado_17072025`)
- **Paginação por chave**: no CNFP e nas concessões, as páginas seguem `fid` crescente (`fid > último` com `orderByFields=fid`). Se somarem menos feições que a contagem oficial, a consulta levanta `SourceUnavailableError` dizendo quantas faltam, em vez de devolver resultado parcial. Resposta HTML (manutenção ou bloqueio de WAF, mesmo com status 200) também vira `SourceUnavailableError`
- **Ano de criação no CNFP**: o campo `anocriacao` do serviço é texto com a data inteira (`DD/MM/AAAA`, `DD-MM-AAAA`; raramente `AAAA-MM-DD`, `AAAA/MM/DD` ou só o ano). O agrobr publica o ano quando o texto tem um único ano. Fica nulo quando o campo está em branco ou é `-`, e quando a data é composta com anos diferentes (sobreposição de unidades, por exemplo `22/06/2011 / 10-01-2002` numa "PA / APA"). Neste último caso, um `UserWarning` e `MetaInfo.validation_warnings` informam a contagem e até três exemplos do texto publicado; o log `sfb_ano_criacao_ambiguo` também é mantido. Na camada de 23/09/2026: 15.068 de 20.829 registros com ano, 4.718 em branco ou `-` e 1.043 compostos com anos diferentes. A coluna `ano_criacao_texto` traz o texto publicado em toda linha, inclusive nas compostas, e o `MetaInfo.schema_version` do CNFP é `1.1`
- **Campos obrigatórios**: todo campo pedido ao serviço (`outFields`) tem de vir em cada feição de cada página no tabular e em cada página nas variantes `_geo`. O serviço manda todos, nulos inclusive, então campo ausente é mudança de layout e levanta `ParseError` com o nome do campo, em vez de devolver a coluna a menos
- **Tabular sem geometria**: `cnfp()`, `concessoes()` e `ifn_conglomerados()` pedem `returnGeometry=false` (a 1ª página do CNFP nacional cai de 378 MB para 0,5 MB); a geometria só vem nas funções `_geo`
- **Unidades e CRS**: área em hectares como publicada (`area_ha` no CNFP, `hectares` nas concessões), sem recálculo pela geometria. A geometria é pedida em EPSG:4326 (`outSR=4326`) e reprojetada pelo servidor (o CNFP é guardado em 3857 e as concessões em 4674)
- **Parâmetros**: argumento desconhecido levanta `TypeError` antes da rede; `uf`, `bioma` e `categoria` inválidos levantam `InvalidParameterError`; `bbox` inválido levanta `ValueError`
- **Filtros compostos**: CNFP e IFN aceitam filtro por bioma alem de uf e bbox

Identificadores, códigos e anos usam `Int64` anulável; áreas usam `float64`. O texto usa o dtype nativo do pandas (`str` no pandas 3, `object` no pandas 2), inclusive nos resultados vazios.

## Limitacoes

- O IFN usa as camadas ativas de pontos e lotes desde esta migração. No DF, a captura de 02/10/2026 contém 68 pontos do lote `DF-01`. Os goldens históricos do serviço antigo contêm apenas erro de indisponibilidade: não há correspondência histórica comprovada de IDs ou de população.
- Pontos e lotes são publicações correntes consultadas separadamente; a junção não promete um snapshot transacional entre as duas camadas.
- Dados refletem o estado atual do ArcGIS Server do SFB
- Concessoes florestais tem poucos registros (~8 poligonos)
- Throttle de 2s apos 5 paginas para nao sobrecarregar o servidor

## Coleta bruta

`agrobr.bruto.coletar("sfb", "cnfp", ...)` guarda as páginas originais da camada do CNFP (`Hosted/CNFP_v19_03_retificado_17072025/FeatureServer/9`) em Esri JSON, no CRS nativo (`wkid` 102100, `latestWkid` 3857, registrado como `EPSG:3857`), com todos os atributos e a geometria; `cnfp_geo` segue em EPSG:4326. Sempre nacional: UF e bbox são recusadas. A edição no manifesto é `20250717`, a data de retificação no nome do serviço; a última edição da camada vem no `etag` das respostas. A cobertura é conferida por `returnCountOnly` antes e depois das páginas e pela lista oficial de `fid` (`returnIdsOnly`). As páginas são faixas dessa lista, pedidas com `orderByFields=fid`, e cada uma tem de trazer exatamente os `fid` da faixa, em ordem crescente.

Em 04/10/2026 a camada tinha 20.829 feições; a maior tinha 5,52 MB, e páginas de 100 chegavam a 37,7 MB. Com o padrão (`tamanho_pagina=100`, `max_bytes_pagina` de 8 MiB), a coleta para cedo, na 4ª página, com `ResourceLimitError` que informa a página, a faixa de `fid` e o limite. Nenhum tamanho de página cabe nos limites padrão: 3 ou mais feições por página passam de 8 MiB, e 2 ou menos passam de `max_paginas`. Para o Brasil inteiro:

```python
from agrobr import bruto

coleta = await bruto.coletar(
    "sfb",
    "cnfp",
    destino="coleta",
    tamanho_pagina=20,
    limites=bruto.LimitesBrutos(max_bytes_pagina=24 * 1024**2),
)
```

A coleta de 04/10/2026 fechou `ok` com 1.042 páginas (980 MB), a maior com 19,3 MB, em 37,5 minutos. Veja a [API da coleta bruta](../api/bruto.md) e o [contrato do manifesto](../contracts/bruto.md).
