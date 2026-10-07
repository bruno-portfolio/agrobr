# INCRA — Territórios Quilombolas

!!! warning "Mudança breaking — fases no formato humanizado"
    Versões anteriores aceitavam fases em formato humanizado (`"Titulada"`,
    `"Em Titulacao"`, `"Decreto Publicado"`, `"RTID em Elaboracao"`,
    `"RTID Publicado"`). Esses valores **nunca casaram** com os dados do
    servidor CMR/FUNAI (que publica em CAIXA ALTA) e o filtro devolvia
    resultado vazio sem erro. Hoje eles levantam `InvalidParameterError`
    (subclasse de `ValueError`). Veja a [lista canônica](#fases-validas).

## Visão Geral

| Item | Detalhe |
|------|---------|
| Provedor | INCRA (camada publicada no GeoServer do CMR/FUNAI) e página de quilombolas do INCRA |
| Dados | Perímetros de territórios quilombolas, andamento dos processos (PDF) e vínculos entre os dois |
| Acesso | WFS 2.0.0 em JSON (`cmr.funai.gov.br/geoserver/ows`) e PDF em `gov.br/incra` |
| Autenticação | Nenhuma |
| Licença | Dados públicos do governo federal |
| Tamanho | 445 perímetros no WFS e 649 processos no PDF (22/09/2026) |

| Função | Retorno |
|--------|---------|
| `quilombolas()` | `DataFrame` com 22 colunas, uma linha por perímetro publicado |
| `quilombolas_geo()` | `GeoDataFrame` com as mesmas 22 colunas + `geometry` (EPSG:4326) |
| `andamento_quilombola()` | `DataFrame` com as 15 colunas do quadro "Andamento dos processos" (requer `agrobr[pdf]`) |
| `vinculos_quilombolas()` | `DataFrame` com 46 colunas relacionando perímetros e processos pelo NUP (requer `agrobr[pdf]`) |

## Exemplo de Uso

```python
import asyncio
from agrobr import incra

async def main():
    df = await incra.quilombolas()
    df = await incra.quilombolas(uf="BA", fase="TITULADO")
    gdf = await incra.quilombolas_geo(bbox=(-42, -15, -40, -13))
    andamento, meta = await incra.andamento_quilombola(return_meta=True)
    vinculos = await incra.vinculos_quilombolas()

asyncio.run(main())
```

## Perímetros (`quilombolas` e `quilombolas_geo`)

Camada `CMR-PUBLICO:lim_quilombolas_a`, consultada em WFS 2.0.0/JSON com ordenação
`cd_quilomb, nu_processo, no_comunidade`, contagem antes e depois da coleta e páginas
sobrepostas em uma ocorrência para detectar mudança durante a paginação.

| Parâmetro | Tipo | Padrão | Descrição |
|-----------|------|--------|-----------|
| `uf` | str \| None | None | Sigla da UF; compara sem diferenciar caixa nem espaços externos |
| `fase` | str \| None | None | Um dos [7 seletores](#fases-validas), comparação literal |
| `bbox` | tuple \| None | None | `(minlon, minlat, maxlon, maxlat)` em **EPSG:4326**; o servidor pré-seleciona e o agrobr confirma por interseção |
| `max_registros` | int \| None | 1500 | Teto de perímetros lidos; `None` retira o teto |
| `tamanho_pagina` | int \| None | 250 (tabular) / 10 (geo ou com `bbox`) | Máximo 1000 (tabular) e 100 (geo ou com `bbox`) |

Os filtros `uf` e `fase` são aplicados localmente depois do download: o servidor não
respeita `CQL_FILTER` nesses campos. Parâmetro inválido levanta `InvalidParameterError`
antes de qualquer requisição. Quando `max_registros` corta a população, um
`UserWarning` informa que a seleção veio de um prefixo remoto. Se a leitura completa não casar
nenhuma comunidade com a `fase` pedida, o resultado vem vazio com `UserWarning` e aviso em `validation_warnings` que
listam os valores de `ds_fase` lidos (um rótulo novo do INCRA aparece ali). `deterministic()` não é
suportado (o WFS não publica edição imutável).

### Colunas

| Coluna | Atributo da fonte | Tipo | Observação |
|--------|-------------------|------|------------|
| `codigo` | `cd_quilomb` | Int64 | Nulo em 64 % dos perímetros e 0 em 9 (22/09/2026); não é chave primária |
| `nome` | `no_comunidade` | texto | |
| `municipio` | `no_municipio` | texto | |
| `uf` | `sg_uf` | texto | Texto publicado, sem normalização |
| `area_ha` | `nu_area_ha` | float64 | Hectares publicados, sem recálculo |
| `familias` | `nu_familia` | Int64 | |
| `fase` | `ds_fase` | texto | Ver [fases](#fases-validas) |
| `titulado` | `st_titulad` | texto | `T`/`F` (a fonte também publica `t`/`f`), sem conversão para booleano |
| `data_publicacao` | `dt_publica` | datetime64[ns] | Data XSD (`AAAA-MM-DD`) |
| `data_titulo` | `dt_titulo` | datetime64[ns] | Idem |
| `feature_id` | id da feição | texto | Identificador recebido do servidor; estabilidade não comprovada |
| `regional` | `co_sr` | texto | Superintendência regional (`SR-05`, …) |
| `processo` | `nu_processo` | texto | NUP como publicado (pode ter mais de um ou formato atípico) |
| `data_publicacao_2` | `dt_public1` | datetime64[ns] | Data XSD |
| `responsavel` | `no_responsavel` | texto | Órgão responsável (INCRA, ITERPA, …) |
| `esfera` | `no_esfera` | texto | Texto publicado (`FEDERAL`, `Federal`, …) |
| `data_cadastro` | `dt_cadastro` | datetime64[ns, UTC] | dateTime XSD, em UTC (sem fuso, lido como UTC); a fonte grava o mesmo horário de carga em todas as feições e ele muda a cada recarga |
| `codigo_sipra` | `cd_sipra` | texto | |
| `descricao` | `ds_descricao` | texto | |
| `data_decreto` | `dt_decreto` | datetime64[ns] | Data XSD |
| `tipo_levantamento` | `tp_levanta` | texto | |
| `escala` | `nr_escalao` | texto | Escala do levantamento (`1:15.000`, …) |

O agrobr valida cada data contra o XSD e a entrega em `datetime64[ns]`; o cadastro sai em
`datetime64[ns, UTC]`. A fonte usa `0001-01-01` como marcador de "sem data" em `data_titulo` (3
perímetros) e `data_decreto` (2): ele vira `NaT` sem aviso. Outra data fora de 1900–2099 (em
08/09/2026, `0205-01-28` e `2201-02-15` em `data_publicacao_2` e `0222-11-11` em `data_titulo`,
erros de digitação da fonte) vira `NaT` com `UserWarning` e aviso em `meta.validation_warnings`, com
a coluna e a quantidade. O texto sai no dtype padrão do pandas instalado (`str` no pandas 3,
`object` no 2); nulo, zero, texto vazio e o texto `NULL` são preservados como publicados.

### Geometria

`quilombolas_geo()` pede `srsName=EPSG:4326` e confere o CRS declarado em cada página. As
coordenadas saem como recebidas: **sem reprojeção e sem reparo topológico** — polígonos
inválidos na fonte chegam inválidos. Geometria nula vira `None`; geometria vazia vira
geometria vazia. Requer `agrobr[geo]`.

### Fases válidas

| Valor | Significado |
|-------|-------------|
| `CCDRU` | Concessão de Direito Real de Uso |
| `DECRETO` | Decreto de desapropriação publicado |
| `PORTARIA` | Portaria de reconhecimento publicada |
| `RTID` | Relatório Técnico de Identificação e Delimitação |
| `TITULADO` | Território com título emitido |
| `TITULO ANULADO` | Título anulado |
| `TITULO PARCIAL` | Titulação parcial |

Em 22/09/2026 a camada também tinha 15 perímetros com fase nula e 1 com o texto `INCRA`.
Essas linhas saem em `quilombolas()` sem `fase`, mas nenhum seletor as alcança.

## Andamento dos processos (`andamento_quilombola`)

Lê o PDF "Andamento dos processos — Quadro geral" ligado na página de quilombolas do
INCRA. Cada linha é um registro do quadro, na ordem publicada (um processo pode aparecer em
mais de uma linha); o número de linhas é conciliado com o total declarado no rodapé ("N
processos com algum tipo de andamento no INCRA").

| Coluna | Tipo | Conteúdo |
|--------|------|----------|
| `regional` | texto | Rótulo do grupo regional desenhado no PDF (`SR(05)BA`, …) |
| `numero_publicado` | Int64 | Posição publicada (1…N) |
| `processo`, `comunidade`, `municipio` | texto | Texto da célula; quebras de linha viram `\n` |
| `area_ha_texto`, `familias_texto` | texto | Número no formato publicado (`2.629,0532`), sem conversão |
| `edital_rtid_1`, `edital_rtid_2`, `retificacao_edital_1`, `retificacao_edital_2`, `portaria`, `retificacao_portaria`, `decreto`, `titulo` | texto | Texto publicado: datas, vários atos, anotações (`Não precisa`, `Em Elaboração`, `**`) ou vazio |

Texto parcialmente cortado pela grade do PDF é preservado inteiro.

**Edição.** A edição é a **data interna** do PDF (a data impressa acima de "Fonte:
INCRA-DQ"). A data do nome do arquivo é só o localizador do download. Quando divergem, um
`UserWarning` e `meta.validation_warnings` citam as duas datas, e `source_details` guarda
`publication.file_date` e `publication.internal_edition`. `edicao=` (data ou
`"AAAA-MM-DD"`) precisa casar a data interna; uma edição que não está mais publicada
levanta `InvalidParameterError` citando a atual. A página liga um único PDF: em
22/09/2026 o arquivo chamado `08_06_2026` trazia o conteúdo de 03/09/2026 (649 processos). Um redirecionamento
(3xx) da página ou do PDF levanta `SourceUnavailableError` ("Redirecionamento administrativo não demonstrado").

O download administrativo tem teto local de 16 MiB por resposta e 32 MiB no total.
Ultrapassar qualquer um deles levanta `ResourceLimitError`, com os recibos das tentativas
em `resources`, sem repetir o pedido por esse motivo.

## Vínculos (`vinculos_quilombolas`)

Compõe `quilombolas()` (população inteira, sem filtros) e `andamento_quilombola()` pela
referência NUP literal (`NNNNN.NNNNNN/AAAA-DD`) encontrada em `processo` dos dois lados. Uma
linha por par de ocorrências (produto cartesiano quando o NUP se repete) mais uma linha para
cada referência sem par e para cada célula sem NUP reconhecível.

| `estado_vinculo` | Significado |
|------------------|-------------|
| `vinculo_exato` | O mesmo NUP aparece no perímetro e no processo |
| `sem_referencia_administrativa` | NUP do perímetro ausente no PDF |
| `sem_referencia_geografica` | NUP do PDF ausente na camada |
| `referencia_nao_reconhecida` | Célula com texto fora do padrão NUP (sem reparo de pontuação) |
| `referencia_ausente` | Célula vazia ou nula |

As 46 colunas são as 9 da relação (`estado_vinculo`, `referencia_tipo`,
`referencia_literal`, posições e contagens de ocorrência, `referencia_repetida`) seguidas
das 22 do perímetro com prefixo `perimetro_` e das 15 do andamento com prefixo
`administrativo_`. NUP em comum não prova identidade territorial. `max_vinculos` (padrão
50.000) interrompe com `ResourceLimitError` se a expansão passar do teto; `max_vinculos=None` tira o
teto de linhas, e o de memória continua. O `validation_warnings` do composto traz o aviso do vínculo e, em
seguida, os das duas fontes, com o prefixo de cada uma (`incra_geoserver: …`, `incra_andamento_pdf: …`). Em
22/09/2026: 817 linhas, 286 vínculos exatos.

## Coleta bruta

`agrobr.bruto.coletar("incra", "quilombolas", ...)` guarda a resposta GeoJSON original do WFS do CMR para a camada `CMR-PUBLICO:lim_quilombolas_a`, no CRS nativo (`EPSG:4674`) e com todos os atributos, sem o `propertyName`, o `srsName` e a ordenação que `quilombolas()` e `quilombolas_geo()` usam. A coleta é sempre nacional (UF e bbox recusadas) e vem numa página única de até 1.000 feições, porque a camada não tem campo único e ordenável que permita paginar; por isso `tamanho_pagina` não é aceito. Ela fecha com as contagens `hits` antes e depois iguais às feições recebidas e com `feature.id` presentes e distintos; mais de 1.000 feições na contagem é `ParseError`. Veja a [API da coleta bruta](../api/bruto.md) e o [contrato do manifesto](../contracts/bruto.md).

## Limitações

- Perímetros e PDF são adquiridos em momentos distintos, sem snapshot conjunto.
- `codigo`, `processo` e `feature_id` não são chaves primárias; ocorrências repetidas são mantidas.
- A contagem do WFS e o PDF mudam sem aviso; o agrobr registra hashes e datas de cada recurso no `MetaInfo`.
