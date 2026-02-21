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
| data_criacao | datetime | Data de criacao do cadastro |
| data_atualizacao | datetime | Ultima atualizacao (nullable) |
| area_ha | float | Area total em hectares |
| condicao | string | Condicao do cadastro (nullable) |
| uf | string | Sigla da UF |
| municipio | string | Nome do municipio |
| cod_municipio_ibge | int | Codigo IBGE do municipio |
| modulos_fiscais | float | Numero de modulos fiscais |
| tipo | string | IRU (Rural), AST (Assentamento), PCT (Terra Indigena) |

## Notas

- **Sem geometria:** o agrobr extrai apenas dados tabulares (sem poligonos/shapefiles)
- **Paginacao transparente:** queries grandes sao paginadas automaticamente (10.000 registros por pagina)
- **Timeout estendido:** read timeout de 180s para UFs com muitos registros (BA, MG, MT)
- **Relevancia EUDR:** dados essenciais para compliance com o EU Deforestation Regulation

## Licenca

Dados abertos do governo federal brasileiro. Disponivel via portal CKAN gov.br.
Licenca: **CC-BY** — uso livre com citacao da fonte.

## Links

- [Portal CAR](https://www.car.gov.br)
- [SICAR Consulta Publica](https://www.car.gov.br/publico/imoveis/index)
- [Dados Abertos SFB](https://www.gov.br/agricultura/pt-br/assuntos/servico-florestal-brasileiro)
