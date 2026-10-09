# ANA/SNIRH — Agencia Nacional de Aguas

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | ANA (Agencia Nacional de Aguas e Saneamento Basico) |
| Dados | Hidrografia, pivos de irrigacao, demanda de irrigacao, disponibilidade hidrica, massas d'água |
| Acesso | ArcGIS REST MapServer API |
| Formato | JSON (tabular) / GeoJSON (geo) |
| Autenticacao | Nenhuma |
| Licenca | Dados publicos |

## Layers

| Layer | Features | Geometria | bbox obrigatorio |
|-------|----------|-----------|------------------|
| `hidrografia` | ~620K polylines | Polyline | Sim |
| `pivos_irrigacao` | ~19.9K polygons | Polygon | Nao |
| `demanda_irrigacao` | ~265K polygons | Polygon | Sim |
| `disponibilidade_hidrica` | ~42K polylines | Polyline | Nao |
| `massas_dagua` | ~241K polygons | Polygon | `uf` ou `bbox` |

## Epoca e referencia dos dados

`pivos_irrigacao` e `pivos_irrigacao_geo` usam o mapeamento de **2014**,
produzido pela ANA em parceria com a Embrapa Milho e Sorgo, conforme os
[metadados oficiais de Pivos_Mapeados](https://portal1.snirh.gov.br/arcgis/rest/services/DADOSABERTOS/Pivos_Mapeados/MapServer?f=pjson).

Nas camadas de demanda e disponibilidade, `versao` preserva o campo oficial
`DSVERSAO`. O valor observado `BHO 2013 versao 1.3 de 22/07/2014` identifica a
versao da base hidrografica; ele, sozinho, nao determina o ano das estimativas
de demanda. As datas de consulta em `MetaInfo`, como `fetched_at`, indicam quando
os dados foram obtidos, nao quando o levantamento foi realizado. Essas camadas
nao representam medicoes hidrologicas em tempo real.

## Acesso via ArcGIS REST

| Parametro | Valor |
|-----------|-------|
| Base URL | `https://portal1.snirh.gov.br/server/rest/services/dados_abertos` |
| Servico | `MapServer/0` |
| Paginacao | Por chave: `orderByFields=OBJECTID` e `OBJECTID > ultimo` (ate 1K features/pagina) |
| Throttle | Pausa de 2s apos a sexta pagina e as seguintes |
| Timeout de leitura | 180s |

A coleta exige `OBJECTID` em ordem crescente estrita dentro de cada pagina e
acima da ultima chave da pagina anterior. Chaves repetidas, sobrepostas ou
fora de ordem geram `SourceUnavailableError`.

As funcoes tabulares solicitam JSON com `returnGeometry=false`; as variantes
`_geo` solicitam GeoJSON e mantem a geometria em EPSG:4326. O filtro espacial
`bbox` e aplicado em ambas as modalidades.
As duas modalidades exigem todos os atributos pedidos em cada feicao. Um
atributo ausente gera `ParseError`, mesmo que esteja presente em outra
feicao da mesma pagina; um valor nulo declarado continua permitido.

## Exemplo de Uso

```python
import asyncio
from agrobr import ana

async def main():
    # Hidrografia (bbox obrigatorio — dataset grande)
    df = await ana.hidrografia(bbox=(-50, -20, -48, -18))

    # Com geometria
    gdf = await ana.hidrografia_geo(bbox=(-50, -20, -48, -18))

    # Pivos de irrigacao
    df = await ana.pivos_irrigacao(uf="GO")

    # Pivos com geometria
    gdf = await ana.pivos_irrigacao_geo(uf="SP", bbox=(-50, -22, -48, -20))

    # Demanda de irrigacao (bbox obrigatorio)
    df = await ana.demanda_irrigacao(bbox=(-50, -20, -48, -18))

    # Disponibilidade hidrica
    df = await ana.disponibilidade_hidrica(bbox=(-46, -20, -44, -18))
    gdf = await ana.disponibilidade_hidrica_geo(bbox=(-46, -20, -44, -18))

    # Com metadados
    df, meta = await ana.pivos_irrigacao(return_meta=True)

    # Polars
    df = await ana.pivos_irrigacao(as_polars=True)

    # Limitar features
    df = await ana.hidrografia(bbox=(-50, -20, -48, -18), max_registros=500)

    # Massas d'água (uf ou bbox obrigatório)
    df = await ana.massas_dagua(uf="DF")
    gdf = await ana.massas_dagua_geo(bbox=(-47.6, -16.0, -47.5, -15.9))

asyncio.run(main())
```

## Colunas por Layer

### hidrografia

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| OBJECTID | int | ID do registro |
| codigo_curso | str | Codigo do curso d'agua |
| codigo_bacia | str | Codigo da bacia |
| nome_rio | str | Nome do rio |
| dominio | str | Dominio |

### pivos_irrigacao

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| OBJECTID | int | ID do registro |
| codigo_municipio | str | Codigo do municipio |
| municipio | str | Municipio |
| estado | str | Estado |
| regiao_hidro | str | Regiao hidrografica |
| area_ha | float | Area em hectares |

### demanda_irrigacao

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| OBJECTID | int | ID do registro |
| ID | int | ID |
| codigo_bacia | str | Codigo da bacia |
| versao | str | Identificador original DSVERSAO da base hidrografica |
| vazao_max_mensal | float | Vazao de retirada maxima mensal (m3/s) |
| vazao_mes_seco | float | Vazao de retirada no mes seco (m3/s) |
| vazao_mes_irrigacao | float | Vazao de retirada no mes de irrigacao (m3/s) |
| vazao_media_anual | float | Vazao de retirada media anual (m3/s) |

### disponibilidade_hidrica

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| OBJECTID | int | ID do registro |
| ID | int | ID |
| area_montante_km2 | float | Area de montante em km2 |
| disponibilidade_m3_s | float | Disponibilidade em m3/segundo |
| nome_rio | str | Nome do rio |
| dominio | str | Dominio |
| versao | str | Identificador original DSVERSAO da base hidrografica |

## Massas d'água

`massas_dagua` e `massas_dagua_geo` leem os polígonos de lagos, lagoas, reservatórios e trechos de rio representados
como polígono, do tema oficial "Massas d'Água" da ANA (versão 2019, escala 1:100.000, [metadado no catálogo da ANA](https://metadados.snirh.gov.br/geonetwork/srv/api/records/7d054e5a-8cc9-403c-9f1a-085fd933610c),
licença: "O acesso ao dado é livre."). A `hidrografia` é outra camada: só as linhas da drenagem (BHO), sem polígono.

| Item | Detalhe |
|------|---------|
| Serviço | `https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0` (o mesmo que o metadado aponta) |
| Feições | 240.899 polígonos (a contagem do shapefile nacional 2019) em 01/10/2026 |
| Volume | DF: 228 feições, ~0,9 MB de GeoJSON; RS: 33.467 feições, ~140 MB pela média do DF |
| Recorte obrigatório | `uf` ou `bbox` (o Brasil inteiro passa de 1 GB); os dois juntos = interseção |

- **`uf`**: o campo `nmufe` traz o nome da UF em maiúsculas, com acento, e lista as UFs de uma feição de divisa separadas
  por vírgula (`"DISTRITO FEDERAL, GOIÁS"`). O filtro casa o nome inteiro como item da lista: `uf="MT"` não pega
  `MATO GROSSO DO SUL`, e `uf="DF"` pega a feição de divisa. Na saída, a coluna `uf` traz as siglas (`"DF/GO"`): quem
  filtra o resultado com `df["uf"] == "DF"` perde as feições de divisa. 106 feições não têm UF e só saem por `bbox`.
  Nome fora do cadastro deixa a `uf` da feição nula e vira aviso no `MetaInfo.validation_warnings`.
- **Paginação por faixa de `FID`**: este servidor recusa `resultRecordCount` e `orderByFields` ("Pagination is not
  supported"). A consulta conta as feições, lê a lista oficial de `FID` (`returnIdsOnly`) e pede cada faixa de até 1.000
  `FID` no `where`. A página tem de trazer exatamente os `FID` da faixa; faltou algum, ou a lista não bate com a contagem,
  levanta `SourceUnavailableError`. Página ilegível, feição sem algum dos 19 campos pedidos (a fonte manda todos, nulos inclusive) ou geometria
  ilegível levanta `ParseError`. `max_registros` corta a lista de
  `FID`, em ordem crescente.
- **Valores publicados**: a fonte publica texto vazio como `" "` (sai nulo) e muitos numéricos ausentes como `0`
  (no DF, em 01/10/2026: `volume_hm3` 0 em 198 das 228 feições, `codigo_snisb` em 188 e `codigo_trecho` em 222); os zeros são preservados. O
  `FID` é a chave interna do serviço e não sai; o código oficial é `codigo`.
- **Fora da saída**: o nome do empreendedor (`nmemp`, que traz nome de pessoa física) não é pedido ao serviço.
- O serviço `dados_abertos/Massa_d_Agua` do portal das outras camadas respondia erro 500 em 01/10/2026; por isso o
  agrobr usa o `SPR/Massa_dagua`. O shapefile nacional do metadado (426 MB, sem divisão por UF) não é baixado.

| Coluna | Tipo | Campo da fonte | Descrição |
|--------|------|----------------|-----------|
| codigo | Int64 | `esp_cd` | Código da massa d'água |
| nome | str | `nmoriginal` | Nome original |
| nome_alternativo | str | `nmalternat` | Nome alternativo |
| tipo | str | `detipomass` | `Natural` ou `Artificial` |
| dominio | str | `dedominial` | Dominialidade (`Federal` ou `Estadual`), como na `hidrografia` |
| entidade_fiscalizadora | str | `defiscaliz` | Entidade fiscalizadora |
| uso_principal | str | `usoprinc` | Uso principal da água |
| volume_hm3 | float | `nuvolumhm3` | Capacidade de armazenamento (hm³) |
| area_km2 | float | `nuareakm2` | Área (km²) |
| area_ha | float | `nuareaha` | Área (ha) |
| perimetro_km | float | `nuperimkm` | Perímetro (km) |
| data_construcao | datetime | `dtreserv` | Data de construção do reservatório (`dd/mm/aaaa` na fonte) |
| nome_rio | str | `nmriocomp` | Nome do rio |
| codigo_snisb | Int64 | `cod_snisb` | Código do SNISB |
| codigo_trecho | Int64 | `cotrecho` | Código do trecho de drenagem da BHO |
| uf | str | `nmufe` | Siglas das UFs, em ordem alfabética e separadas por `/` (`"DF/GO"`) |
| municipios | str | `nmmun` | Municípios, como publicados (`"BRASÍLIA, FORMOSA"`) |
| fonte_geometria | str | `deversao` | Origem da geometria (`FBDS (2017)`, `bho_massa_dagua_2019`…) |

## Particularidades

- **bbox obrigatorio**: `hidrografia` e `demanda_irrigacao` requerem bbox (datasets grandes)
- **Paginacao por chave**: cada pagina pede ate 1K features ordenadas por `OBJECTID` e a seguinte continua do ultimo `OBJECTID` recebido, ate completar a contagem oficial. O servidor da Hidrografia devolve uma feicao a menos que o pedido em cada pagina; com paginacao por offset, a feicao da fronteira se perdia (1.046 de 1.047 no recorte de teste). Se a paginacao parar antes da contagem oficial, ou se uma pagina nao avancar o `OBJECTID`, a consulta levanta `SourceUnavailableError` dizendo quantas feicoes faltam, em vez de devolver resultado parcial. Pagina ilegivel (JSON cortado, HTML de proxy) ou sem `OBJECTID` levanta `ParseError`
- **max_registros**: inteiro positivo que limita as feições retornadas, ou `None` para não limitar. Zero, negativos, booleanos e valores não inteiros levantam `InvalidParameterError` antes da coleta. O argumento anterior `max_features` não é mais aceito. Quando a camada tem mais feições que o limite, a consulta emite `UserWarning` (`"ANA hidrografia: retornadas 5 de 63 feições por limite local; restrinja filtros ou use max_registros=None."`), põe a mesma frase em `MetaInfo.validation_warnings` e publica `source_details["coverage"]` com `expected_rows` (total da camada), `returned_rows`, `local_limit` e `truncated=True`. O total é a contagem que a consulta já faz antes de baixar as páginas, a mesma que decide a coleta; não há contagem extra. Sem corte, o resultado e o `MetaInfo` não mudam
- **Campos obrigatórios**: todo campo pedido ao serviço (`outFields`) tem de vir em cada feição de cada página no tabular e em cada página nas variantes `_geo`. O serviço manda todos, nulos inclusive, então campo ausente é mudança de layout e levanta `ParseError` com o nome do campo, em vez de devolver a coluna a menos

| Layer | Campos pedidos |
|-------|----------------|
| `hidrografia` | `OBJECTID`, `COCURSODAG`, `COBACIA`, `NORIOCOMP`, `DEDOMINIAL` |
| `pivos_irrigacao` | `OBJECTID`, `CD_GEOCMU`, `NM_MUNICIP`, `NM_ESTADO`, `REGIAO_HID`, `HECTARES` |
| `demanda_irrigacao` | `OBJECTID`, `ID`, `COBACIA`, `DSVERSAO`, `VZMAXMEN`, `VZMESSEC`, `VZMESIRR`, `VZMEDANO` |
| `disponibilidade_hidrica` | `OBJECTID`, `ID`, `NUAREAMONT`, `DISPQ95`, `NMRIO`, `DEDOMINIAL`, `DSVERSAO` |
| `massas_dagua` | os 19 campos da tabela de colunas das massas d'água |

Contagem oficial zero devolve um resultado vazio valido, com as colunas publicadas;
nas variantes `_geo`, o resultado vazio tambem sai em EPSG:4326. A presenca
de um campo com valor nulo e diferente da ausencia desse campo; valores nulos e
zeros sao preservados.

`OBJECTID` e `ID` mantêm os nomes da fonte e usam `Int64` anulável. Medidas usam `float64`; códigos textuais preservam o texto publicado. O texto usa o dtype nativo do pandas (`str` no pandas 3, `object` no pandas 2), e os resultados vazios têm os mesmos dtypes dos resultados com registros.

## Limitacoes

- Hidrografia e demanda de irrigacao exigem bbox (sem filtro retornaria centenas de milhares de features)
- Apenas pivos e massas d'água oferecem filtro por UF; todas as camadas aceitam bbox e max_registros
- Nao ha filtro de ano ou intervalo de datas; a consulta usa a edicao de cada camada configurada
- A consulta conta os registros antes de baixar as paginas; mesmo um max_registros pequeno pode exigir aguardar essa contagem
- As paginas sao acumuladas em memoria antes de construir o resultado; nao ha streaming nem cache persistente ANA
- Pausa de 2s apos a sexta pagina e as seguintes para nao sobrecarregar o servidor

## Coleta bruta

```python
from agrobr import bruto

coleta = await bruto.coletar(
    "ana", "massas_dagua", destino="dados", uf="DF",
    bbox=(-47.466, -15.993, -47.462, -15.988),
    bbox_crs="EPSG:4674", nome="barragem_df",
)
```

A coleta guarda os corpos Esri JSON sem renomear atributos, sem remover campos como `gid` e sem transformar as geometrias. Pede todos os atributos, geometria e `outSR=4674`; o CRS é conferido no corpo recebido. Os mesmos filtros e CRS de entrada são usados nas contagens, na lista oficial de FIDs e nas páginas. A ordenação dos FIDs serve para definir as faixas dos pedidos; a ordem e os bytes das feições recebidas são preservados. O recurso só fecha com contagens e FIDs concordantes. Zero feições exige duas contagens zero e a lista oficial vazia.

Veja a [API da coleta bruta](../api/bruto.md) e o [contrato do manifesto](../contracts/bruto.md).

## Proveniência de consultas com várias páginas

`return_meta=True` agora preenche `raw_content_hash` e `raw_content_size` também quando a ANA retorna mais de uma página. Nesse caso, o hash identifica a serialização UTF-8 de `{"query": ..., "resources": ...}`, com chaves ordenadas, `ensure_ascii=False` e separadores `(',', ':')`. `raw_content_size` mede essa serialização. O tamanho total dos corpos originais está em `source_details["resource_bytes"]`.

`source_details` publica `hash_kind="resource_manifest_sha256"`, `manifest_encoding="canonical_json_utf8"`, `manifest_fields=["query", "resources"]`, `query` e `resources`. A consulta lógica contém `fonte`, `recurso`, `where`, `bbox`, `max_registros` e `formato`; `resources` preserva a ordem da aquisição e contém `pagina` (a partir de 1), `sha256` e `bytes`.

Uma única página conserva o hash e o tamanho do corpo; zero páginas conserva hash nulo e tamanho zero. Dados, colunas, tipos, geometrias, CRS, requisições e `source_url` das APIs tabulares/geográficas permanecem iguais. O manifesto de hashes do `MetaInfo` não armazena corpos; use a coleta bruta para preservá-los em disco.
