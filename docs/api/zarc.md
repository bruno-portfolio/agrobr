# ZARC (Zoneamento Agricola de Risco Climatico)

Janelas de plantio recomendadas por municipio, cultura, tipo de solo e ciclo do cultivar.

## zoneamento

Consulta a Tabua de Risco ZARC.

```python
import agrobr

df = await agrobr.zarc.zoneamento(produto="soja", uf="MT", safra="2025/2026")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| produto | str | Nao | Nome canonico da cultura (ex: "soja", "milho_1", "trigo"); a coluna de saída continua `cultura` |
| uf | str | Nao | Sigla da UF (ex: "MT", "SP"); outra gera `InvalidParameterError` com a lista das válidas |
| municipio | int \| str | Nao | Código IBGE de 7 dígitos (`int` ou `str`) ou nome inteiro do município, sem diferenciar caixa nem acento; o filtro compara o `geocodigo`. Pedaço de nome, nome inexistente ou nome repetido sem `uf` geram `InvalidParameterError` com os candidatos, antes da rede |
| safra | str | Nao | "2025/2026" ou "perene" (default: safra mais recente) |
| solo | int | Nao | Codigo tipo de solo (1-3 classico, 11-16 novo 6-AD) |
| ciclo | int | Nao | Codigo ciclo do cultivar (13, 19, 20, 21, 22, 24, 25, 26) |
| as_polars | bool | Nao | Se True, retorna polars DataFrame |
| return_meta | bool | Nao | Se True, retorna (DataFrame, MetaInfo) |
| use_cache | bool | Não | Padrão True; False ignora o cache do catálogo e da tábua |

A primeira consulta de cada revisão baixa e parseia a tábua inteira. As seguintes consultam os dados validados no cache local DuckDB, inclusive em outro processo Python: TTL de 24 horas desde a aquisição e até três revisões. O arquivo ZARC é separado do cache CEPEA. O catálogo usa cache em memória por uma hora. `use_cache=False` ignora leitura e gravação de ambos; falhas no cache local geram log de aviso e seguem por download e parse. Metadados preservam SHA, aquisição original e culturas observadas na tábua inteira. O download é conferido contra o tamanho que o servidor publica (o `Content-Range` do portal do MAPA, ou o `Content-Length`): corpo menor levanta `SourceUnavailableError` e não vai para o cache. Sem o tamanho publicado, o resultado avisa em `validation_warnings` ("tamanho do arquivo não conferido") e não é gravado; entrada do cache sem tamanho conferido ou sem registros é baixada de novo.

### Colunas de retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| cultura | str | Nome canonico (ex: "soja", "milho_1") |
| safra | str | "2025/2026" ou "perene" |
| geocodigo | str | Codigo IBGE do municipio (7 digitos) |
| uf | str | Sigla UF |
| municipio | str | Nome do municipio |
| solo_codigo | int | Tipo de solo |
| ciclo_codigo | int | Ciclo do cultivar |
| clima | str | Restricao climatica |
| manejo | str | Manejo especifico |
| portaria | str | Numero da portaria MAPA |
| dec1-dec36 | int | Risco por decendio (0/20/30/40/50) |

### Exemplos

```python
# Soja em Mato Grosso
df = await agrobr.zarc.zoneamento(produto="soja", uf="MT")

# Municipio especifico por geocodigo
df = await agrobr.zarc.zoneamento(municipio=5107925, safra="2025/2026")

# Busca pelo nome inteiro do municipio
df = await agrobr.zarc.zoneamento(municipio="Sorriso", produto="soja")

# Filtro por solo e ciclo
df = await agrobr.zarc.zoneamento(produto="milho_1", solo=2, ciclo=20)

# Culturas perenes
df = await agrobr.zarc.zoneamento(produto="cafe_arabica", safra="perene")

# Com metadados
df, meta = await agrobr.zarc.zoneamento(produto="soja", uf="MT", return_meta=True)
print(meta.records_count, meta.fetch_duration_ms)
```

## culturas

Culturas fora do catálogo são rejeitadas antes de acessar a rede, com sugestões quando houver nomes semelhantes. Culturas válidas ausentes na safra só são rejeitadas após a leitura da tábua, com a indicação da tábua perene ou das safras em que a cultura aparece (tábuas publicadas até 23/09/2026). Nos 11 rótulos de cultura renomeados na safra 2024/2025, a mensagem também aponta a chave equivalente da tábua consultada (tabela na [página da fonte](../sources/zarc.md)).

Lista de 107 culturas representadas por aliases canônicos, uma para cada rótulo publicado nas 12 tábuas oficiais, incluindo dois nomes legados de 2016/2017 e os nove rótulos das safras 2017/2018 a 2023/2024 (`milho`, `arroz_irrigado`, `feijao_1`, `trigo_irrigado`, `mamona_semiarido_sequeiro`, `cevada_graos_irrigada`, `cevada_graos_sequeiro`, `aveia_sequeiro`, `aveia_irrigada`). Frutas e café usam `safra="perene"`. A presença varia por safra; cultura ausente na safra pedida informa em quais safras aparece. Com `return_meta=True`, `meta.source_details["parser"]["culturas_observadas"]` lista as culturas observadas na tábua inteira, antes dos filtros.

```python
culturas = agrobr.zarc.culturas()
# ['abacaxi', 'acai', 'acai_implantacao', 'algodao', 'alho_nobre', ...]
```

Funcao sincrona (sem await).

## safras_disponiveis

Safras disponiveis no portal CKAN (faz discovery online).

```python
safras = await agrobr.zarc.safras_disponiveis()
# ['2016/2017', '2017/2018', ..., '2025/2026', 'perene']
```

## Uso sincrono

```python
from agrobr import sync

df = sync.zarc.zoneamento(produto="soja", uf="MT")
culturas = sync.zarc.culturas()
safras = sync.zarc.safras_disponiveis()
```

## Fonte de dados

- **Provedor:** MAPA / Embrapa
- **Portal:** [dados.agricultura.gov.br](https://dados.agricultura.gov.br/dataset/tabua-de-risco-zoneamento-agricola-de-risco-climatico)
- **Licenca:** CC-BY (dados publicos governo federal)
- **Atualizacao:** semanal no catálogo CKAN; PDF declara diária, sem comprovar a cadência efetiva

## Culturas legadas e identidade dos registros

O catálogo de filtros inclui `Arroz Sequeiro`/`arroz_sequeiro` e `Trigo Sequeiro`/`trigo_sequeiro`, publicados na tábua de 2016/2017. Esses aliases conservam os valores já retornados pelo parser e não são convertidos para `arroz`/`trigo`. Nomes desconhecidos continuam sendo recusados antes da rede; a presença de cada cultura depende da tábua consultada.

As 59 colunas do contrato 2.1 preservam os 55 campos publicados, além de cultura normalizada, safra derivada, posição no CSV e `cod_municipio` (o `geocodigo` em inteiro). Registros repetidos são mantidos. A posição `registro_origem` é válida somente junto a `meta.raw_content_hash`: os três corpos de 18/09/2026 tinham SHA diferente dos de 07/09, com os mesmos registros em outra ordem. Uma alteração de SHA não demonstra mudança dos valores.

Aquisição UTC e hash do corpo permanecem em `meta.fetched_at`, `meta.raw_content_hash` e `meta.source_details["resource"]`, inclusive no cache. A leitura até EOF comprova que o corpo recebido foi processado, sem certificar total externo de municípios ou snapshot transacional. O catálogo CKAN consultado pela API declara frequência semanal, enquanto o dicionário PDF declara diária. O dataset usa `update_frequency="weekly"`, tomando o catálogo ativo de descoberta como referência operacional; a declaração conflitante do PDF permanece registrada. Nenhuma das declarações comprova a cadência efetiva de revisão de cada safra. Produtividade e códigos NM são preservados literalmente, sem inferir unidade ausente no dicionário.
