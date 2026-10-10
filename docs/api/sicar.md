# SICAR (Cadastro Ambiental Rural)

Dados tabulares de imoveis rurais do CAR via WFS do GeoServer SICAR.

## imoveis

Registros individuais de imóveis rurais, via GeoJSON projetado somente nos atributos. A consulta tabular não requer GeoPandas. Contrato 2.1, com criação e atualização em UTC, inclusive colunas nulas e retornos vazios.

O filtro `atualizado_apos` aceita precisão de milissegundos. Zeros adicionais são aceitos sem alterar o instante: `.212000` é enviado como `.212`. Frações submilissegundo, como `.212001`, geram `InvalidParameterError` antes da rede; o GeoServer não comparou essas representações com a semântica ISO esperada. A regra vale também para as duas APIs de geometria.

```python
import agrobr

df = await agrobr.alt.sicar.imoveis("DF")
```

### Parâmetros

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| uf | str | Sim | Sigla da UF (ex: "MT", "DF", "BA") |
| municipio | int \| str | Não | Código IBGE de 7 dígitos (int ou str) ou nome inteiro do município, sem diferenciar caixa e acento (`normalize.resolver_municipio`); precisa ser da UF. Nome ambíguo, inexistente ou pedaço de nome (`"Santa Rita"` não casa com `"Santa Rita do Sapucaí"`) gera `InvalidParameterError` com os candidatos. Filtra pelo código na camada |
| status | str | Não | AT, PE, SU, CA ou RE |
| tipo | str | Não | IRU, AST ou PCT |
| area_min | float | Não | Area mínima em hectares |
| area_max | float | Não | Area máxima em hectares |
| criado_apos | str | Não | Data válida `YYYY-MM-DD`; criação maior ou igual ao corte (`>=`) |
| atualizado_apos | str | Não | Atualização estritamente posterior (`>`), em data/datetime ISO, com fração e `Z`/offset opcionais; sem fuso, interpreta UTC. O campo é solicitado onde existe. Indisponível em PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP e TO |
| as_polars | bool | Não | Se True, retorna polars.DataFrame |
| return_meta | bool | Não | Se True, retorna (DataFrame, MetaInfo) |

Datas impossíveis, áreas negativas/não finitas, intervalos invertidos e tipos inválidos são rejeitados antes da rede. O código municipal é conferido no cadastro de municípios do IBGE antes da rede: código inexistente ou de outra UF gera `InvalidParameterError`.

Os filtros consultam registros correntes e não reconstituem versões passadas. O dataset [`cadastro_rural`](../contracts/cadastro_rural.md) expõe os mesmos filtros tabulares e rejeita um contexto `deterministic` ativo.

Ocorrências publicadas com o mesmo `cod_imovel` são selecionadas pela atualização mais recente
quando todas as ocorrências do grupo têm atualização; senão pela criação quando todas a têm;
senão pelo maior sufixo numérico do id, que também desempata datas. Com `return_meta=True`, os
avisos e `source_details["sicar"]` mostram os critérios e descartes (lista de até 1.000 itens,
com contagem total e indicador de truncagem). A paginação rejeita repetição da mesma feature.
Veja a [regra completa](../contracts/cadastro_rural.md#ocorrencias-do-mesmo-imovel-e-proveniencia).

### Colunas de retorno

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| cod_imovel | str | Código único do imovel |
| status | str | AT/PE/SU/CA/RE |
| data_criacao | datetime64[ns, UTC] | Instante UTC de criação (nullable) |
| data_atualizacao | datetime64[ns, UTC] | Instante UTC de atualização (nullable) |
| area_ha | float | Area em hectares |
| condicao | str | Condição do cadastro (nullable) |
| uf | str | Sigla UF |
| municipio | str | Nome do município |
| cod_municipio_ibge | int | Código IBGE |
| modulos_fiscais | float | Módulos fiscais |
| tipo | str | IRU/AST/PCT |
| cod_municipio | int | Código IBGE de 7 dígitos (igual a `cod_municipio_ibge`); anulável |

### Exemplos

```python
# Imoveis ativos em Sorriso-MT
df = await agrobr.alt.sicar.imoveis(
    "MT", municipio="Sorriso", status="AT"
)

# Filtro por codigo IBGE (evita problemas com acentos)
df = await agrobr.alt.sicar.imoveis("PA", municipio="Uruará")  # ou municipio=1508159

# Imoveis grandes (>1000 ha) no DF
df = await agrobr.alt.sicar.imoveis("DF", area_min=1000)

# Cadastros criados apos 2020
df = await agrobr.alt.sicar.imoveis(
    "GO", criado_apos="2020-01-01"
)

# Cadastros atualizados apos uma data (util para sincronizar a base incrementalmente)
df = await agrobr.alt.sicar.imoveis(
    "MG", atualizado_apos="2026-06-07T00:00:00"
)

# Com metadados de proveniencia
df, meta = await agrobr.alt.sicar.imoveis("DF", return_meta=True)
print(meta.records_count, meta.fetch_duration_ms)
```

## resumo

Estatísticas agregadas por UF ou município.

```python
df = await agrobr.alt.sicar.resumo("MT")
```

### Parâmetros

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| uf | str | Sim | Sigla da UF |
| municipio | int \| str | Não | Código IBGE de 7 dígitos (int ou str) ou nome inteiro do município, sem diferenciar caixa e acento (`normalize.resolver_municipio`); precisa ser da UF. Nome ambíguo, inexistente ou pedaço de nome (`"Santa Rita"` não casa com `"Santa Rita do Sapucaí"`) gera `InvalidParameterError` com os candidatos. Filtra pelo código na camada |
| as_polars | bool | Não | Se True, retorna polars.DataFrame |
| return_meta | bool | Não | Se True, retorna (DataFrame, MetaInfo) |

### Retorno sem município (UF-level)

Usa `resultType=hits` (cinco consultas: total e quatro status, sem download de registros). A contagem é de **feições publicadas**: versões do mesmo `cod_imovel` em vigor na camada contam separado, então o total pode passar do número de imóveis e da soma dos resumos por município, que contam uma versão por `cod_imovel`. A saída diz isso em `MetaInfo.source_details["sicar"]["unidade"] = "feicoes_publicadas"` e num aviso em `MetaInfo.validation_warnings` (com `return_meta=True`):

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| total | int | Feições publicadas; conta todos os status, e os que estão fora das quatro colunas abaixo (como `RE`) entram só aqui |
| ativos | int | Feições com status AT |
| pendentes | int | Feições com status PE |
| suspensos | int | Feições com status SU |
| cancelados | int | Feições com status CA |

### Retorno com município

Busca dados, aplica a seleção de ocorrências de `imoveis()` e agrega client-side; os avisos e detalhes da seleção acompanham `return_meta=True`:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| total | int | Total de imoveis; conta todos os status, e os que estão fora das quatro colunas abaixo (como `RE`) entram só aqui |
| ativos | int | Imoveis com status AT |
| pendentes | int | Imoveis com status PE |
| suspensos | int | Imoveis com status SU |
| cancelados | int | Imoveis com status CA |
| area_total_ha | float | Soma das areas |
| area_media_ha | float | Media das areas |
| modulos_fiscais_medio | float | Media de módulos fiscais |
| por_tipo_IRU | int | Imoveis rurais |
| por_tipo_AST | int | Assentamentos |
| por_tipo_PCT | int | Imoveis com `tipo` = `PCT` |

### Exemplos

```python
# Resumo do DF (rapido, sem download)
df = await agrobr.alt.sicar.resumo("DF")

# Resumo de Sorriso-MT (com agregacao)
df = await agrobr.alt.sicar.resumo("MT", municipio="Sorriso")
```

## imoveis_geo

Registros individuais com geometria (poligonos MultiPolygon). Requer `pip install agrobr[geo]`.

```python
import agrobr

gdf = await agrobr.alt.sicar.imoveis_geo("DF")
```

### Parâmetros

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| uf | str | Sim | Sigla da UF (ex: "MT", "DF", "BA") |
| municipio | int \| str | Não | Código IBGE de 7 dígitos (int ou str) ou nome inteiro do município, sem diferenciar caixa e acento (`normalize.resolver_municipio`); precisa ser da UF. Nome ambíguo, inexistente ou pedaço de nome (`"Santa Rita"` não casa com `"Santa Rita do Sapucaí"`) gera `InvalidParameterError` com os candidatos. Filtra pelo código na camada |
| status | str | Não | AT, PE, SU, CA ou RE |
| tipo | str | Não | IRU, AST ou PCT |
| area_min | float | Não | Area mínima em hectares |
| area_max | float | Não | Area máxima em hectares |
| criado_apos | str | Não | Data mínima de criacao (ISO, ex: "2020-01-01") |
| atualizado_apos | str | Não | Atualização estritamente posterior (`>`), em data/datetime ISO, com fração e `Z`/offset opcionais; sem fuso, interpreta UTC. O campo é solicitado onde existe. Indisponível em PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP e TO |
| max_registros | int \| None | Não | Limite de feições retornadas. Default: 5000. `None` desativa o limite |
| return_meta | bool | Não | Se True, retorna (GeoDataFrame, MetaInfo) |

### Colunas de retorno

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| cod_imovel | str | Código único do imovel |
| status | str | AT/PE/SU/CA/RE |
| data_criacao | datetime | Data de criacao |
| data_atualizacao | datetime | Última atualização (nullable) |
| area_ha | float | Area em hectares |
| condicao | str | Condição do cadastro (nullable) |
| uf | str | Sigla UF |
| municipio | str | Nome do município |
| cod_municipio_ibge | int | Código IBGE |
| modulos_fiscais | float | Módulos fiscais |
| tipo | str | IRU/AST/PCT |
| cod_municipio | int | Código IBGE de 7 dígitos (igual a `cod_municipio_ibge`); anulável |
| geometry | MultiPolygon | Poligono do imovel (EPSG:4326) |

### Exemplos

```python
# Imoveis com geometria no DF
gdf = await agrobr.alt.sicar.imoveis_geo("DF")
gdf.plot()

# Filtrar por municipio (nome)
gdf = await agrobr.alt.sicar.imoveis_geo(
    "MT", municipio="Sorriso", status="AT"
)

# Filtrar por codigo IBGE (evita problemas com acentos)
gdf = await agrobr.alt.sicar.imoveis_geo("PA", municipio=1508159)

# Com metadados
gdf, meta = await agrobr.alt.sicar.imoveis_geo("DF", return_meta=True)
```

### Notas

- `max_registros=5000` é o limite padrão do resultado; aceita inteiro positivo ou `None`
- Resultado que para em `max_registros` sai com aviso em `validation_warnings` e `UserWarning`, e `source_details["sicar"]` traz `truncado=True`, o `max_registros` e `total_fonte`, o total da consulta na fonte (o `numberMatched` do WFS); sem esse total, o aviso diz que pode haver mais. No DF, o padrão traz 5.000 dos 21.011 imóveis (captura de 22/09/2026)
- Até 10.000 features, usa uma requisição; limites maiores e `None` usam paginação de até 10.000 por página
- CRS: EPSG:4326 (WGS84). As camadas do SICAR são publicadas em SIRGAS 2000 (EPSG:4674); o agrobr pede `srsName=EPSG:4326` e confere o CRS declarado em cada página com feições (outra declaração gera `ParseError`). A reprojeção é do GeoServer: nas capturas de 22/09/2026 as coordenadas diferem no máximo 1e-8 grau das publicadas em SIRGAS 2000. Resultado vazio também sai com o CRS
- Ocorrências repetidas do mesmo `cod_imovel` seguem a regra de [`imoveis()`](#imoveis); com `return_meta=True`, `validation_warnings` e `source_details["sicar"]` registram os descartes. Id de feature repetido gera `ParseError`, como na paginação tabular
- Datas são instantes UTC; data sem fuso gera `ParseError`, como no tabular

## imoveis_geo_stream

Itera sobre os imoveis com geometria de uma UF em batches, sem acumular tudo em
memoria antes de comecar a usar os dados. Requer `pip install agrobr[geo]`.

```python
import agrobr

async for gdf in agrobr.alt.sicar.imoveis_geo_stream("MT"):
    print(len(gdf))
```

### Parâmetros

| Parâmetro | Tipo | Obrigatório | Descrição |
|-----------|------|-------------|-----------|
| uf | str | Sim | Sigla da UF (ex: "MT", "DF", "BA") |
| municipio | int \| str | Não | Código IBGE de 7 dígitos (int ou str) ou nome inteiro do município, sem diferenciar caixa e acento (`normalize.resolver_municipio`); precisa ser da UF. Nome ambíguo, inexistente ou pedaço de nome (`"Santa Rita"` não casa com `"Santa Rita do Sapucaí"`) gera `InvalidParameterError` com os candidatos. Filtra pelo código na camada |
| status | str | Não | AT, PE, SU, CA ou RE |
| tipo | str | Não | IRU, AST ou PCT |
| area_min | float | Não | Area mínima em hectares |
| area_max | float | Não | Area máxima em hectares |
| criado_apos | str | Não | Data mínima de criacao (ISO, ex: "2020-01-01") |
| atualizado_apos | str | Não | Atualização estritamente posterior (`>`), em data/datetime ISO, com fração e `Z`/offset opcionais; sem fuso, interpreta UTC. O campo é solicitado onde existe. Indisponível em PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP e TO |

Cada item gerado e um `GeoDataFrame` com as mesmas colunas de [`imoveis_geo`](#imoveis_geo).

### Exemplos

```python
# Acumula o total de imoveis de Sorriso-MT sem guardar tudo em memoria
total = 0
async for gdf in agrobr.alt.sicar.imoveis_geo_stream("MT", municipio="Sorriso"):
    total += len(gdf)
print(total)
```

### Notas

- Sem limite de `max_registros`: pagina até esgotar todos os registros da UF
- Cada yield corresponde a uma página WFS (até 10.000 features), baixadas sequencialmente com throttle. As ocorrências do último `cod_imovel` de cada página passam para o lote seguinte, porque as páginas vêm ordenadas por `cod_imovel` e uma versão repetida pode cair na página seguinte; o último lote traz só esse código
- Uma ocorrência por `cod_imovel`, pela regra de [`imoveis()`](#imoveis); id de feature repetido entre páginas gera `ParseError`
- CRS: EPSG:4326 (WGS84), com a mesma conferência de [`imoveis_geo`](#imoveis_geo)
- Async-only: `agrobr.sync` não suporta async generators

## Uso síncrono

```python
from agrobr import sync

df = sync.sicar.imoveis("DF")
gdf = sync.sicar.imoveis_geo("DF")
df = sync.sicar.resumo("MT", municipio="Sorriso")
```

## Coleta bruta

Para guardar as páginas originais do WFS, com todas as versões de cada imóvel e o manifesto, use a
[coleta bruta](bruto.md): `bruto.coletar("sicar", "imoveis", uf="DF", destino=...)`.

## Fonte de dados

- **Provedor:** Serviço Florestal Brasileiro (SFB) / SICAR
- **API:** WFS 2.0.0 (OGC GeoServer)
- **Licença:** `livre` pela base federal de dados públicos; CC BY da base não comprovada. Preservar fonte e proveniência; veja [Licenças](../licenses.md#sicar).
- **Atualização:** continua (cadastros em tempo real)
