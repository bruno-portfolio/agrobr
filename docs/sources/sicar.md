# SICAR (Cadastro Ambiental Rural)

## Sobre

O **Sistema Nacional de Cadastro Ambiental Rural (SICAR)** e o registro eletronico
obrigatorio de todos os imoveis rurais do Brasil, conforme a Lei 12.651/2012
(Codigo Florestal). Administrado pelo Servico Florestal Brasileiro (SFB),
o sistema contem mais de **7.4 milhoes de imoveis** cadastrados em 27 UFs.

O CAR inclui informacoes sobre:

- Identificacao do imovel rural
- Status do cadastro (Ativo, Pendente, Suspenso, Cancelado)
- Area total em hectares
- Modulos fiscais
- Tipo de imovel (Rural, Assentamento, Terra Indigena)
- Municipio e codigo IBGE

## Acesso via WFS

O agrobr acessa o GeoServer WFS do SICAR diretamente, sem necessidade de
CAPTCHA ou autenticacao. O protocolo OGC WFS permite consultas padronizadas
com filtros server-side (CQL_FILTER) e paginacao transparente.

**Endpoint:** `https://geoserver.car.gov.br/geoserver/sicar/wfs`

## Campos disponiveis

| Campo | Tipo | Descricao |
|-------|------|-----------|
| cod_imovel | string | Codigo unico do imovel (UF-IBGE-hash) |
| status | string | AT (Ativo), PE (Pendente), SU (Suspenso), CA (Cancelado) |
| data_criacao | datetime UTC | Instante de criação do cadastro |
| data_atualizacao | datetime UTC | Última atualização (nullable) |
| area_ha | float | Area total em hectares |
| condicao | string | Condicao do cadastro (nullable) |
| uf | string | Sigla da UF |
| municipio | string | Nome do municipio |
| cod_municipio_ibge | int | Codigo IBGE do municipio |
| modulos_fiscais | float | Numero de modulos fiscais |
| tipo | string | IRU (Rural), AST (Assentamento), PCT (Terra Indigena) |
| cod_municipio | int | Código IBGE de 7 dígitos (igual a `cod_municipio_ibge`); anulável |

## Notas

- **Formato tabular 2.0:** `imoveis()` usa GeoJSON apenas com os atributos, sem geometria e
  sem GeoPandas. Datas são UTC, inclusive colunas nulas e resultados vazios. O CSV oficial
  mostrou horários sem fuso diferentes dos instantes UTC do JSON e dos limites CQL; não se
  aplica deslocamento fixo para converter capturas CSV antigas
- **Atualizacao incremental:** `imoveis()`, `imoveis_geo()` e `imoveis_geo_stream()` aceitam
  `atualizado_apos` (CQL `data_atualizacao>'...'`, ISO date ou datetime) para buscar apenas
  registros atualizados depois de uma data. A coluna é solicitada nas 15 camadas que a oferecem.
  O campo não existe em PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP e TO; nesses estados o filtro
  gera erro antes da rede e a coluna permanece nula nas consultas sem esse filtro. Essa cobertura
  vem do `DescribeFeatureType` das 27 camadas em 06/09/2026
- **Estado corrente:** criação (`>=`) e atualização (`>`) são filtros do cadastro disponível
  no momento da consulta. Não recuperam revisões anteriores nem exclusões. O dataset
  `cadastro_rural` também aceita município (nome ou código) e atualização, e rejeita `deterministic`
- **Geometria disponivel:** `imoveis_geo()` retorna `GeoDataFrame` com poligonos MultiPolygon
  (EPSG:4326) via WFS GeoJSON. Requer `pip install agrobr[geo]`. Limite padrão de 5.000 features
  no resultado; `max_registros` maior que 10.000 ou `None` usa paginação. O corte avisa
  (`validation_warnings`, `UserWarning` e `source_details["sicar"]["truncado"]`). As 27 camadas declaram
  SIRGAS 2000 (`DefaultCRS` EPSG:4674); o agrobr pede `srsName=EPSG:4326`
  e recusa com `ParseError` a página com feições que declare outro CRS
- **Precisão do filtro:** `atualizado_apos` aceita milissegundos, com zeros adicionais opcionais.
  `.212000` é enviado como `.212`; valores submilissegundo geram erro, sem arredondamento
- **Paginação:** consultas grandes usam páginas de 10.000 registros, ordenadas por
  `cod_imovel` nos caminhos tabular e geoespacial. A fonte declara
  `PagingIsTransactionSafe=FALSE`: a ordenação fixa a sequência dos registros, mas não garante
  um snapshot entre páginas quando há atualizações concorrentes
- **Contagem durante a varredura tabular:** mudanças em `numberMatched` geram log de aviso e
  `MetaInfo.validation_warnings` (com `return_meta=True`). O maior total observado determina
  as páginas a consultar; a quantidade final de ids únicos de feature deve coincidir com a última
  contagem anunciada. Repetição do mesmo id ou divergência final geram `ParseError` com orientação para
  repetir a consulta. Não há reexecução automática nem garantia de completude de um snapshot;
  os avisos também são preservados em `cadastro_rural` e no resumo por município
- **Contagem durante a varredura geoespacial:** `imoveis_geo()` paginado (`max_registros=None` ou acima
  de 10.000) e `imoveis_geo_stream()` seguem a mesma regra: as páginas seguem a contagem inicial, e a
  quantidade final de ids únicos tem de coincidir com a última contagem anunciada (ou com `max_registros`,
  se menor); senão, `ParseError` com orientação para repetir a consulta. A mudança de `numberMatched` vai
  ao log e, em `imoveis_geo(return_meta=True)`, a `MetaInfo.validation_warnings`. No stream, a conferência
  vem depois da última página, então o erro chega depois dos lotes já entregues
- **Ocorrências repetidas:** após a varredura, `imoveis()`, o resumo municipal, `imoveis_geo()`
  e `imoveis_geo_stream()` selecionam uma ocorrência por `cod_imovel`. A base é escolhida por grupo: atualização se presente em todas;
  senão criação se presente em todas; senão maior sufixo numérico do id. Empates na data usam
  esse mesmo desempate por id. As 12 UFs sem atualização estão listadas acima. `cadastro_rural`
  mantém a chave e as doze colunas do contrato 2.1. Avisos e `source_details["sicar"]` registram
  contagens de features, códigos colapsados, descartes e critérios; a lista de descartes tem
  até 1.000 itens e sinaliza truncagem. Veja a [regra e os campos de proveniência](../contracts/cadastro_rural.md#ocorrencias-do-mesmo-imovel-e-proveniencia)
- **Resumo da UF:** `resumo(uf)` sem município conta feições publicadas (`resultType=hits`), sem a seleção acima:
  versões do mesmo `cod_imovel` contam separado. A saída diz isso em `source_details["sicar"]["unidade"] =
  "feicoes_publicadas"` e em `validation_warnings`; para contar imóveis, use o resumo por município
- **Sem cache:** cada chamada consulta o GeoServer do CAR; repetir a consulta baixa tudo de novo
- **Timeout estendido:** read timeout de 180s para UFs com muitos registros (BA, MG, MT)
- **SSL:** o GeoServer do CAR usa cipher suite legado que rejeita handshake TLS padrao.
  O client usa `SSLContext` com `@SECLEVEL=1`, mantendo verificação do certificado e do hostname.
  Falhas de confiança no certificado não ativam fallback com verificação desabilitada
- **Relevancia EUDR:** dados essenciais para compliance com o EU Deforestation Regulation

## Licenca

A classificação do SICAR é `livre` pela base pública federal: LAI, Decreto 8.777/2016 e publicidade do CAR na Lei 12.651/2012. A consulta pública oferece visualização e download da base por UF sem termo específico de reutilização localizado. Campos `Fees` e `AccessConstraints` vazios no WFS não são uma concessão de licença. Não foi comprovada uma CC BY da base; o BY-ND do rodapé gov.br é do conteúdo do site. Preservar SFB/CAR, camada, UF, extração e transformações. [Geosserviços do CAR](https://consultapublica.car.gov.br/publico/geoservicos/index).

## Links

- [Portal CAR](https://www.car.gov.br)
- [SICAR Consulta Publica](https://www.car.gov.br/publico/imoveis/index)
- [Dados Abertos SFB](https://www.gov.br/agricultura/pt-br/assuntos/servico-florestal-brasileiro)

## Contagens e proveniência

Em 18/09/2026, a consulta integral do DF tinha 21.006 feições em três páginas, com 513 atualizações nulas. Numa consulta de MT, o valor zero de módulos fiscais sai preservado. Em duas páginas de 10.000 feições de GO e RS, a escolha de ocorrências por atualização e por criação deixa 9.999 imóveis em cada uma.

Contagens coincidentes não garantem snapshot transacional. Com várias páginas, o `MetaInfo` traz cada uma em `source_details["resources"]` (SHA-256 e bytes) e, no topo, o hash do manifesto `{query, resources}` (`hash_kind` `resource_manifest_sha256`).
