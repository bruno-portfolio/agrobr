# Coleta bruta

O modo bruto guarda os arquivos e as respostas originais das fontes, com proveniência e
verificação de integridade. A API assíncrona é `agrobr.bruto.coletar`; a mesma operação está
disponível em `agrobr.sync.bruto.coletar`. Não requer o extra `geo`.

O contrato público do `manifesto.jsonl` é **1.0.0**, independente da versão da biblioteca e
dos contratos de tabelas. Ele descreve a aquisição, não o schema dos atributos publicados
pela fonte. Os formatos originais são ZIP, Esri JSON, GML e GeoJSON.

## API e recursos

```python
from os import PathLike
from typing import Literal

from agrobr import bruto

async def coletar(
    fonte: str,
    recurso: str,
    *,
    destino: str | PathLike[str],
    nome: str | None = None,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    bbox_crs: Literal["EPSG:4674", "EPSG:4326"] = "EPSG:4674",
    tamanho_pagina: int | None = None,
    compactar: bool = True,
    retomar: bool = False,
    limites: bruto.LimitesBrutos | None = None,
) -> bruto.ColetaBruta: ...
```

`fonte` e `recurso` são os identificadores exatos da tabela. Não são URLs ou nomes livres.
`bbox_crs` aceita somente `EPSG:4674` e `EPSG:4326`.

| Fonte | Recurso | Seleção | Formato / identidade de paginação |
|---|---|---|---|
| `ana` | `massas_dagua` | UF e/ou bbox obrigatórios | `esri_json` / `FID` |
| `cnuc` | `ucs` | UF, bbox, ambos ou Brasil; filtro obrigatório `limite=uc` | `gml` / `cd_cnuc` |
| `ibge` | `malha_municipal` | UF, bbox, ambos ou Brasil | `geojson` / `cd_mun` |
| `ibge` | `areas_urbanizadas` | bbox ou Brasil; UF não é aceita | `geojson` / `fid` |
| `acervo_fundiario` | `sigef_publico` | UF obrigatória; bbox não é aceita | `zip` |
| `acervo_fundiario` | `sigef_privado` | UF obrigatória; bbox não é aceita | `zip` |
| `acervo_fundiario` | `snci_publico` | UF obrigatória; bbox não é aceita | `zip` |
| `acervo_fundiario` | `snci_privado` | UF obrigatória; bbox não é aceita | `zip` |
| `acervo_fundiario` | `snci_brasil` | UF obrigatória; arquivo Brasil da UF | `zip` |
| `sicar` | `imoveis` | UF obrigatória; bbox opcional | `geojson` / `feature.id` |

UF aceita sigla, convertida para maiúsculas. UF e bbox combinados significam interseção.
A bbox é `(minx, miny, maxx, maxy)`, sempre longitude/latitude na entrada, com números
finitos, `-180 <= minx < maxx <= 180` e `-90 <= miny < maxy <= 90`. Não cruza o
antimeridiano. O adaptador serializa a ordem de eixos exigida pelo protocolo; não reprojeta
a resposta. Sem bbox, `bbox_crs` fica `null` no manifesto.

`nome` identifica a seleção: o padrão é a UF, ou `brasil` quando não há recorte.
Com bbox, `nome` é obrigatório, mesmo com UF. Aceita `[A-Za-z0-9][A-Za-z0-9_-]{0,63}`;
nomes de dispositivo do Windows, como `CON`, `NUL` e `COM1`, são recusados sem distinguir
maiúsculas. Dois nomes que diferem apenas por maiúsculas colidem em qualquer plataforma.
O nome não aplica filtro.

`tamanho_pagina=None` resolve para **100** em consultas paginadas. Um valor explícito deve
ser inteiro entre 1 e 1.000, nunca booleano. Em ZIP, deve ser `None`. Ele divide a aquisição;
não limita o total de feições. `compactar=True` aplica gzip às páginas e controles, com
`mtime=0` e sem nome original no cabeçalho; ZIP não recebe compactação adicional.
`compactar=False` preserva os mesmos corpos sem gzip.

A malha municipal usa a camada `CGMAT:qg_2025_030_munic`, edição 2025; áreas urbanizadas
usam `CGEO:AU_2026_AreasUrbanizadas2022_Brasil`, edição 2022. A edição está em
`selecao.edicao`; não é inferida do ano de coleta. O bruto pede todos os atributos e a
geometria. ANA pede explicitamente `outSR=4674` e `returnGeometry=true`; CNUC e IBGE
não pedem CRS de saída, e conferem o CRS nativo declarado, `EPSG:4674`. Um CRS
incompatível encerra a consulta com erro. O ZIP conserva seu PRJ; `crs=null` significa
que o CRS do arquivo não foi verificado.

O SICAR (`sicar`/`imoveis`) usa a camada da UF, `sicar:sicar_imoveis_<uf>`, sem CRS de saída, e confere o CRS
nativo `EPSG:4674`. Guarda todas as versões publicadas de um imóvel e conta pelo `feature.id`, não pelo
`cod_imovel`. Como o SICAR não publica um identificador ordenável, as páginas vêm em
`sortBy=cod_imovel A,dat_criacao A`, e a coleta só fecha `ok` com o par (`cod_imovel`, `dat_criacao`)
estritamente crescente em toda a coleta: duas versões com o mesmo `dat_criacao`, par fora de ordem ou data
fora do formato encerram a consulta com erro. Um `ok` prova a ordem das feições recebidas naquela coleta,
não que o par seja único na base inteira.

O retorno `ColetaBruta` tem três atributos obrigatórios:

| Atributo | Tipo Python | Exemplo / significado |
|---|---|---|
| `manifesto` | `pathlib.Path` | Caminho absoluto de `destino/manifesto.jsonl` |
| `entrada` | `bruto.RecursoBruto` | Modelo Pydantic da linha; `entrada.status` e `entrada.model_dump(mode="json")` |
| `reutilizado` | `bool` | `True` somente quando `retomar=True` reutilizou um recurso `ok` verificado, sem rede |

A função retorna para `ok` e `ausente_na_fonte`. Falhas de aquisição registram `erro`,
quando possível, e levantam a exceção indicada adiante. Não há retorno parcial de sucesso.

## Representação e layout

O manifesto usa UTF-8 sem BOM, LF e uma linha JSON completa por recurso, inclusive a
última terminada em LF. Não contém comentários, linhas vazias, cabeçalho ou linha de áreas
de estudo. A chave é `(fonte, recurso, nome)`, com uma única entrada corrente.
A ordem das linhas não tem significado. A ordem de `paginas` tem: números contíguos
a partir de 1, na ordem da aquisição, sem ordenar ou alterar os bytes das feições.

```text
coleta/
  manifesto.jsonl
  ana/massas_dagua/AL/<coleta_id>/p000001.esri.json.gz
  ana/massas_dagua/AL/<coleta_id>/controles/c000001.json.gz
  cnuc/ucs/brasil/<coleta_id>/p000001.gml.gz
  ibge/malha_municipal/AL/<coleta_id>/p000001.geojson.gz
  ibge/areas_urbanizadas/area_teste/<coleta_id>/p000001.geojson.gz
  acervo_fundiario/snci_publico/AL/<coleta_id>/original.zip
  .bruto/lock
  .bruto/<coleta_id>/checkpoint.json
  .bruto/<coleta_id>/diagnostico/
```

Todo campo `arquivo` contém um caminho relativo a `destino`, com `/`, nunca absoluto,
`..` ou escape por link/junction. `coleta_id` é opaco: não extraia data dele. O sufixo
`.gz` só existe com `compressao="gzip"`. Controles usam a extensão de seu corpo, JSON
ou XML. O leitor deve usar `formato` e `compressao`, não adivinhar pela extensão.
`.bruto` é estado privado, sem schema público; não é fonte de recursos consumíveis.

Corpos são gravados em temporários `.part` e publicados depois de fechados e conferidos.
Só depois é substituído o manifesto, de forma atômica. Uma falha pode deixar artefatos
órfãos, mas nunca deve publicar uma linha `ok` apontando para um corpo parcial.
A substituição de uma entrada mantém as outras. Há uma trava exclusiva por destino,
válida entre threads e processos e liberada pelo sistema operacional ao encerrar o
processo. Outro gravador falha imediatamente com `ResourceLimitError`; não espera uma fila.

`bytes` e `sha256` descrevem o **corpo após decodificar o Content-Encoding HTTP e antes
do gzip local**. Não são bytes da conexão, hash de ZIP extraído ou hash do gzip local.
Encoding textual, espaços, propriedades, fins de linha, atributos e geometrias permanecem
intactos. `bytes_armazenados` é o tamanho do artefato no disco; pode ser maior que `bytes`
em corpos pequenos comprimidos. Ler, descomprimir quando indicado e calcular SHA-256 deve
reproduzir `sha256`. ETag não substitui esse hash.

## Schema 1.0.0: entrada de recurso

**Todas as chaves das tabelas são obrigatórias.** `null` é um valor permitido onde
explicitado, não autorização para omitir a chave. Objetos vazios e listas vazias são
`{}` e `[]`. Inteiros não aceitam booleanos; números não aceitam NaN ou infinito.
`UTC` nas tabelas significa string RFC 3339 terminada em `Z`, com fração de segundo opcional de 1 a 6
dígitos, como `2026-10-02T20:00:00Z` ou `2026-10-02T20:00:00.125Z`. Hash é string SHA-256 de 64 dígitos hexadecimais minúsculos.

| Campo | Tipo JSON | Obrigatório | Exemplo / regra |
|---|---|---|---|
| `schema_version` | string | Sim | `"1.0.0"` |
| `tipo` | string literal | Sim | `"recurso"` |
| `fonte` | string | Sim | `"ana"`; identificador registrado da tabela da API |
| `recurso` | string | Sim | `"massas_dagua"`; permitido para aquela fonte |
| `nome` | string | Sim | `"AL"` |
| `consulta_id` | hash | Sim | Identidade opaca da consulta normalizada; não é hash de dados |
| `coleta_id` | string não vazia | Sim | `"exemplo-ana-001"`; distingue tentativas completas |
| `status` | enum | Sim | `"ok"`, `"erro"` ou `"ausente_na_fonte"` |
| `inicio` | UTC | Sim | Início da tentativa do recurso |
| `fim` | UTC | Sim | Fim da tentativa; `inicio <= fim` |
| `registrado_em` | UTC | Sim | Publicação da entrada; `fim <= registrado_em` |
| `url_solicitada` | string URL HTTPS | Sim | URL do arquivo; em paginado, URL do endpoint sem query string |
| `url` | string URL HTTPS | Sim | URL final do GET de arquivo; na consulta paginada, mesma URL lógica de `url_solicitada` |
| `parametros` | objeto string → string | Sim | `{"outFields":"*","outSR":"4674"}`; todos os parâmetros lógicos, sem os de paginação; `{}` para ZIP |
| `selecao` | objeto Seleção | Sim | Recorte e publicação escolhidos, tabela abaixo |
| `opcoes` | objeto Opções | Sim | Opções efetivas e limites desta tentativa |
| `modo` | enum | Sim | `"arquivo"` ou `"paginado"` |
| `formato` | enum | Sim | `"zip"`, `"esri_json"`, `"gml"` ou `"geojson"` |
| `crs` | string ou null | Sim | `"EPSG:4674"` quando verificado; `null` quando não verificado |
| `crs_evidencia` | objeto EvidênciaCRS ou null | Sim | `null` exatamente quando `crs=null` |
| `http_status` | inteiro 100–599 ou null | Sim | Último GET de arquivo, p.ex. `200` ou `404`; `null` em paginado ou sem resposta |
| `http_inicio` | UTC ou null | Sim | Início do último GET de arquivo; `null` em paginado ou GET não iniciado |
| `http_fim` | UTC ou null | Sim | Fim desse GET; `null` nas mesmas condições |
| `arquivo` | string de caminho ou null | Sim | `"acervo_fundiario/snci_publico/AL/exemplo-001/original.zip"` |
| `sha256` | hash ou null | Sim | Hash do arquivo completo; `null` em paginado ou sem arquivo válido |
| `bytes` | inteiro >= 0 ou null | Sim | `11900`; tamanho original, não uma contagem de feições |
| `bytes_armazenados` | inteiro >= 0 ou null | Sim | Igual a `bytes` no ZIP sem compactação adicional |
| `compressao` | enum | Sim | `"nenhuma"` para arquivo e para o topo de paginado; gzip consta em cada artefato |
| `cabecalhos` | objeto string → string | Sim | Cabeçalhos do último GET de arquivo, mesmo 404; `{}` em paginado |
| `paginas` | lista de Página | Sim | `[]` em arquivo e em seleção paginada com zero feições |
| `controles` | lista de Controle | Sim | Respostas de contagem, IDs e/ou CRS; `[]` quando não houver |
| `feicoes` | inteiro >= 0 ou null | Sim | Total declarado antes da paginação; `null` para arquivo ou contagem desconhecida |
| `cobertura` | objeto Cobertura | Sim | Resultado da conferência, não uma promessa transacional |
| `erro` | objeto Erro ou null | Sim | `null` somente em `ok` |
| `avisos` | lista de strings | Sim | `[]` quando vazio; nunca substitui uma falha de integridade |
| `agrobr_version` | string | Sim | `"2.0.0"`; versão que adquiriu os corpos |

Os quatro campos `arquivo`, `sha256`, `bytes` e `bytes_armazenados` são todos preenchidos
ou todos `null`. Em `modo="arquivo", status="ok"`, são preenchidos e `http_status=200`.
Em paginado são sempre `null`: não existe um arquivo concatenado nem hash agregado implícito.
No topo de paginado, URLs representam o pedido lógico; as URLs efetivamente executadas,
incluindo redirecionamento e paginação, estão nas páginas e controles.


### Identidade da consulta

`consulta_id` é o SHA-256 de um objeto JSON canônico em UTF-8 com exatamente estas chaves:
`fonte`, `recurso`, `nome`, `url_solicitada`, `parametros`, `selecao`, `modo`, `formato`,
`tamanho_pagina` e `compactar`. Os oito primeiros valores vêm da entrada; os dois últimos,
de `opcoes`. A serialização usa `ensure_ascii=False`, `sort_keys=True`,
`separators=(",", ":")` e `allow_nan=False`, sem LF final.
A normalização converte UF para maiúsculas, coordenadas da bbox para float (zero negativo
para `0.0`), preenche a seleção com `null` onde não se aplica e resolve opções efetivas.
O nome mantém sua grafia; uma grafia diferente apenas na caixa é colisão.
Limites, horários, versão da biblioteca e `retomar` ficam fora dessa identidade.

Em paginado, as URLs do topo identificam o endpoint sem query string;
`parametros` contém a consulta lógica completa. URLs de página/controle contêm a
query string realmente pedida. `parametros` da página registra o pedido original,
inclusive se houver redirecionamento; `url` registra a URL final da resposta.

### Seleção, opções e evidência de CRS

| Objeto.campo | Tipo | Obrigatório | Exemplo / regra |
|---|---|---|---|
| `selecao.uf` | string ou null | Sim | `"AL"` |
| `selecao.bbox` | lista de quatro números ou null | Sim | `[-48.1,-16.1,-47.9,-15.9]` |
| `selecao.bbox_crs` | enum ou null | Sim | `"EPSG:4674"`, `"EPSG:4326"`; `null` sem bbox |
| `selecao.camada` | string ou null | Sim | `"CGMAT:qg_2025_030_munic"`; `null` para ZIP |
| `selecao.edicao` | inteiro ou null | Sim | `2025`; `null` quando a fonte não identifica edição |
| `selecao.natureza` | enum ou null | Sim | `"publico"`, `"privado"`; `null` em SNCI Brasil e demais fontes |
| `opcoes.tamanho_pagina` | inteiro 1–1000 ou null | Sim | `100`; `null` em arquivo |
| `opcoes.compactar` | booleano | Sim | `true`; `false` efetivo em arquivo |
| `opcoes.limites` | objeto Limites | Sim | Todos os valores efetivos definidos na seção Limites |
| `crs_evidencia.tipo` | enum | Sim, no objeto | `"pagina"`, `"controle"` ou `"prj"` |
| `crs_evidencia.arquivo` | caminho | Sim, no objeto | Um artefato desta entrada |
| `crs_evidencia.localizador` | string não vazia | Sim, no objeto | `"/spatialReference/wkid"`, XPath de GML/XML ou nome do membro PRJ |
| `crs_evidencia.valor` | string não vazia | Sim, no objeto | `"4674"` ou URN/WKT realmente lido |

A evidência aponta para bytes preservados, não para uma suposição a partir do endpoint.
Para uma consulta sem feições, pode haver `crs=null` se nenhum controle verificou o CRS.
Para consulta com feições, todas as páginas precisam declarar o CRS esperado e concordar;
o topo pode apontar para a primeira evidência. Ler PRJ é opcional, passa por teto de
expansão e não extrai o ZIP.

### Página e controle

Cada Página ou Controle contém **todos** os campos comuns a seguir:

| Campo | Tipo | Obrigatório | Exemplo / regra |
|---|---|---|---|
| `numero` | inteiro >= 1 | Sim | `1`; sequencial dentro de sua própria lista |
| `url_solicitada` | URL HTTPS | Sim | GET completo, incluindo query realmente enviada |
| `url` | URL HTTPS | Sim | URL final da resposta |
| `parametros` | objeto string → string | Sim | Query efetiva do pedido original, com `count`/`startIndex` ou faixa FID |
| `inicio` | UTC | Sim | Início do pedido que forneceu o corpo |
| `fim` | UTC | Sim | Fim da leitura do corpo; dentro do intervalo do recurso |
| `http_status` | inteiro literal | Sim | `200`; corpos de erro ficam no diagnóstico privado |
| `arquivo` | caminho | Sim | `"ibge/malha_municipal/AL/exemplo-001/p000001.geojson.gz"` |
| `sha256` | hash | Sim | SHA-256 do corpo original completo |
| `bytes` | inteiro >= 0 | Sim | `73038` |
| `bytes_armazenados` | inteiro >= 0 | Sim | Tamanho do corpo guardado, incluindo gzip local |
| `compressao` | enum | Sim | `"gzip"` ou `"nenhuma"` |
| `formato` | enum | Sim | Página: `esri_json`/`gml`/`geojson`; controle: `json`/`xml` |
| `cabecalhos` | objeto string → string | Sim | `{"etag":"W/\"abc\""}` ou `{}` |

Além dos campos comuns, uma Página contém:

| Campo | Tipo | Obrigatório | Exemplo / regra |
|---|---|---|---|
| `paginacao` | objeto discriminado | Sim | Um dos dois formatos abaixo |
| `feicoes_recebidas` | inteiro >= 0 | Sim | Número de feições no envelope, antes de qualquer tratamento |
| `ids_distintos` | inteiro >= 0 | Sim | IDs distintos desta página, não o acumulado |
| `total_declarado` | inteiro >= 0, literal `"unknown"` ou null | Sim | `"unknown"` no GML CNUC; `null` se o envelope não declara total |
| `crs` | string ou null | Sim | `"EPSG:4674"`; `null` só pode ocorrer em página de recurso com erro |

As duas formas completas de `paginacao` são:

| Forma | Campos obrigatórios, tipos e exemplo |
|---|---|
| WFS | `{"tipo":"offset","inicio":0,"quantidade":100}`; `inicio` inteiro >= 0, `quantidade` inteiro >= 1, ambos do pedido |
| ANA | `{"tipo":"fid","min":1,"max":100,"quantidade":100}`; `min`/`max` inteiros, `min <= max`; `quantidade` conta IDs esperados, não a largura numérica |

Um Controle acrescenta `papel` (obrigatório, enum `contagem_antes`, `contagem_depois`,
`ids` ou `crs`) e `valor_declarado` (obrigatório, inteiro >= 0, `"unknown"` ou `null`).
Contagem usa o valor literal do envelope; IDs e CRS usam `null`. Controle não tem
`paginacao`, `feicoes_recebidas`, `ids_distintos`, `total_declarado` ou `crs`.

`cabecalhos` admite apenas as chaves minúsculas `last-modified`, `etag`,
`content-type`, `content-length`, `content-encoding`, `date`, `retry-after` e `location`.
Só inclui as recebidas, com seu valor textual, preservando aspas e `W/` do ETag.
Não contém cookies, Authorization ou segredos. Os cabeçalhos são da resposta GET
correspondente; um HEAD posterior não é atribuído àqueles bytes.
`content-length` não é comparável a `bytes` quando Content-Encoding altera a representação.

### Cobertura e erro

| Objeto.campo | Tipo | Obrigatório | Exemplo / regra |
|---|---|---|---|
| `cobertura.total_antes` | inteiro >= 0 ou null | Sim | Total do controle anterior; `null` quando desconhecido |
| `cobertura.total_depois` | inteiro >= 0 ou null | Sim | Total do controle posterior |
| `cobertura.recebidas` | inteiro >= 0 ou null | Sim | Soma das feições das páginas preservadas |
| `cobertura.ids_distintos` | inteiro >= 0 ou null | Sim | Cardinalidade da união dos IDs recebidos |
| `cobertura.ids_repetidos` | inteiro >= 0 ou null | Sim | Ocorrências excedentes, inclusive entre páginas |
| `cobertura.campo_id` | string ou null | Sim | `"FID"`, `"cd_cnuc"`, `"cd_mun"` ou `"fid"` |
| `cobertura.estado` | enum | Sim | `"conferida"`, `"divergente"`, `"nao_comprovada"` ou `"nao_aplicavel"` |
| `cobertura.completa` | booleano | Sim | `true` apenas em recurso `ok` |
| `cobertura.snapshot_transacional` | booleano literal | Sim | `false` |
| `cobertura.controles` | lista de caminhos | Sim | Referências aos controles usados; todos devem existir em `controles` |
| `erro.tipo` | enum | Sim, no objeto | `"HTTP404"`, `"SourceUnavailableError"`, `"ParseError"`, `"ResourceLimitError"`, `"ContractViolationError"`, `"OSError"`, `"CancelledError"` ou `"KeyboardInterrupt"` |
| `erro.mensagem` | string não vazia | Sim, no objeto | `"Contagem mudou durante a coleta"`; em português |
| `erro.http_status` | inteiro 100–599 ou null | Sim, no objeto | `404`; `null` se não houver resposta HTTP causadora |
| `erro.url` | URL HTTPS ou null | Sim, no objeto | Pedido que falhou; `null` para falha exclusivamente local |

Para arquivo, contagens e `campo_id` são `null`, estado é `nao_aplicavel` e referências
de cobertura são `[]`. `completa=true` comprova aquisição do arquivo, não completude
temática ou validade de todas as geometrias.

Em paginado `ok`, a igualdade exigida é
`total_antes == total_depois == feicoes == recebidas == ids_distintos`,
com `ids_repetidos=0`, IDs presentes e `estado="conferida"`. As referências incluem
contagem antes/depois e, na ANA, a lista oficial de FIDs. FIDs recebidos devem ser
exatamente os listados, por faixa e no conjunto. Para WFS, ordem e avanço devem ser
conferidos pelo campo da tabela da API. ID duplicado, nulo, página repetida, página curta
antes do fim, total alterado, CRS inesperado ou continuidade não demonstrada impedem `ok`.
Um total numérico declarado em página precisa coincidir com a contagem de controle.
`"unknown"` não vale zero: só os controles com total conhecido permitem fechar a coleta.

Zero confirmado antes/depois é `ok` com `paginas=[]` e contagens zero; ANA também
confere a lista de IDs vazia. Sem páginas, `recebidas`, `ids_distintos` e
`ids_repetidos` começam em zero em paginado. Uma tentativa incompleta usa
`nao_comprovada`; inconsistência observada usa `divergente`.
Inconsistências comprovadas pelo adaptador, incluindo FIDs fora da faixa e alteração de contagem, encerram a coleta com `ParseError`, `status="erro"` e `cobertura.estado="divergente"`. Uma resposta ilegível, sem evidência suficiente para comparar a cobertura, permanece `nao_comprovada`.
Conferir quantidades e IDs não detecta toda alteração de atributos feita durante a coleta.
Por isso `snapshot_transacional=false` permanece obrigatório.

## Status, erros e limites

| Situação | Manifesto / retorno |
|---|---|
| Arquivo íntegro ou consulta inteiramente conferida | `ok`; retorna `ColetaBruta` |
| GET do arquivo original retorna 404 | `ausente_na_fonte`, `erro.tipo="HTTP404"`; retorna normalmente |
| 404 em página ou controle | `erro`; levanta `SourceUnavailableError` |
| 403, timeout, falha de rede, 429/5xx após tentativas | `erro`; levanta `SourceUnavailableError` |
| Erro OGC/ArcGIS em HTTP 200, envelope inválido ou cobertura divergente | `erro`; levanta `ParseError` |
| Orçamento excedido | `erro` se a aquisição começou; levanta `ResourceLimitError` |
| Argumento, seleção, destino, colisão de chave ou versão de escrita inválidos | `InvalidParameterError` antes da rede e sem mudar o manifesto |
| Integridade de entrada existente ou invariante do manifesto inválida | `ContractViolationError`; preserva o manifesto anterior |
| Disco, permissão ou publicação falha | Propaga `OSError`; tenta registrar `erro` somente se ainda puder publicar com segurança |
| Cancelamento ou interrupção cooperativa | Propaga `CancelledError`/`KeyboardInterrupt`; registro `erro` em melhor esforço |
| Encerramento forçado, queda de energia | Pode não haver linha de erro; manifesto anterior e checkpoint ficam para retomada |

404 é uma observação com horário, não prova de ausência permanente. Não há blacklist de
UF nem cache negativo: a próxima retomada tenta novamente. HTTP 200 com zero feições é
sucesso vazio, nunca ausência.

Há uma requisição ativa por coleta, páginas sequenciais, sem pool de processos. Retry,
user agent, TLS e pausas respeitam a política da fonte e o rate limiter compartilhado.
O padrão HTTP é de três tentativas totais por pedido, configurável por
`HTTPSettings.max_retries` (zero vale uma tentativa); apenas falhas transitórias são
repetidas. 403/404, divergência, limite e erro de disco não são repetidos automaticamente.
Espera de retry e de rate limiter consome o prazo total. Não há fallback para outra fonte.

`LimitesBrutos` contém os campos abaixo; todos aparecem em `opcoes.limites`.
Inteiros são positivos, o prazo é positivo e finito; booleanos são recusados.

| Campo | Tipo | Padrão e exemplo | Aplicação |
|---|---|---|---|
| `max_bytes_recurso` | int | `4294967296` (4 GiB) | Contadores separados de bytes HTTP codificados e decodificados, ambos limitados, somando controles, páginas, arquivo e tentativas falhas |
| `max_bytes_pagina` | int | `8388608` (8 MiB) | Cada página/controle, nos dois contadores; não limita o ZIP original a 8 MiB |
| `max_paginas` | int | `10000` | Páginas lógicas de dados; controles não contam; tentativas contam em bytes e prazo |
| `max_ids` | int | `500000` | Total declarado e número de IDs retidos; também limita a lista ANA antes da paginação |
| `max_bytes_ids` | int | `67108864` (64 MiB) | Memória contabilizada das estruturas de IDs, incluindo chaves e recipientes; além dela só a página limitada em processamento |
| `max_segundos` | número | `3600.0` | Prazo monotônico da operação, incluindo verificação local, rede, esperas e publicação |

O teto efetivo de ZIP é o menor entre `max_bytes_recurso` e 4 GiB. Tetos são conferidos
durante o stream, inclusive expansão HTTP, antes de reter blocos além do orçamento.
O orçamento de IDs não é um teto do RSS de todo o processo. Gzip local é lido
incrementalmente na retomada, limitado pelo tamanho declarado e pelo teto de página/recurso.
Membros ZIP, quando lidos, passam pelos helpers de expansão do agrobr e seus tetos.

O manifesto tem teto de leitura e escrita de **64 MiB**, verificado antes da publicação.
Limites não truncam seleções nem removem páginas para produzir sucesso. Para um recurso
maior, reduza o recorte ou configure limites adequados; o teto máximo de ZIP permanece.
O orçamento vale por chamada, não é uma quota cumulativa do destino: evidências de
tentativas antigas e overhead de compactação também ocupam disco. Se o limite impedir
até a publicação de uma linha de erro, o manifesto anterior é preservado e a
`ResourceLimitError` propaga. O prazo é conferido entre passos de I/O e processamento;
não promete interromper uma chamada bloqueante do sistema operacional no instante exato.

## Retomada

1. Em destino novo, valide argumentos e crie a coleta. É possível acrescentar recursos
   diferentes sequencialmente no mesmo destino.
2. Se a chave já existe, `retomar=False` recusa a operação sem rede. `retomar=True`
   exige a mesma consulta normalizada: fonte, recurso, nome, seleção, camada/edição,
   parâmetros lógicos, formato, tamanho de página e compactação efetiva.
   Limites podem mudar; não fazem parte dessa identidade.
3. Se a entrada é `ok`, confira schema, invariantes, caminhos, tamanho armazenado, hash
   e tamanho original de **todos** os artefatos, inclusive controles. Reutilize sem
   HEAD ou GET, com os mesmos IDs e horários e `reutilizado=True`. O manifesto não muda.
   Isso verifica o acervo local; não confirma que a fonte continua igual.
4. Se falta arquivo, há corrupção ou hash incorreto em `ok`, levante
   `ContractViolationError`; não substitua silenciosamente a evidência. Um limite menor
   que o necessário à verificação levanta `ResourceLimitError`, também sem alterar a entrada.
5. Erro, ausência, checkpoint ou `.part` recomeçam o **recurso inteiro**, em novo
   `coleta_id`. ZIP recomeça do byte zero e consulta paginada da primeira página, com
   novos controles. A nova entrada substitui a anterior somente ao concluir a tentativa.
   Artefatos anteriores permanecem separados; não são mesclados nem apagados automaticamente.

Uma nova captura temporal usa outro destino. Não há Range, retomada remota por página,
revalidação condicional de recursos concluídos ou coleta incremental na versão 1.0.0.

## Uso completo

As chamadas abaixo cobrem todos os recursos do núcleo. A bbox urbana é apenas um
exemplo de recorte, não uma garantia de quantidade ou tempo de aquisição.

```python
import asyncio
import json
from pathlib import Path

from agrobr import bruto


async def main() -> None:
    destino = Path("coleta-exemplo")
    pedidos = [
        ("ana", "massas_dagua", {"uf": "AL"}),
        ("cnuc", "ucs", {"uf": "AL"}),
        ("ibge", "malha_municipal", {"uf": "AL"}),
        (
            "ibge",
            "areas_urbanizadas",
            {"nome": "area_teste", "bbox": (-48.1, -16.1, -47.9, -15.9)},
        ),
        ("acervo_fundiario", "sigef_publico", {"uf": "AL"}),
        ("acervo_fundiario", "sigef_privado", {"uf": "AL"}),
        ("acervo_fundiario", "snci_publico", {"uf": "AL"}),
        ("acervo_fundiario", "snci_privado", {"uf": "AL"}),
        ("acervo_fundiario", "snci_brasil", {"uf": "AL"}),
    ]
    for fonte, recurso, selecao in pedidos:
        resultado = await bruto.coletar(
            fonte, recurso, destino=destino, retomar=True, **selecao
        )
        print(recurso, resultado.entrada.status, resultado.reutilizado)
    with (destino / "manifesto.jsonl").open(encoding="utf-8") as arquivo:
        entradas = [json.loads(linha) for linha in arquivo]
    print([(e["fonte"], e["recurso"], e["nome"], e["status"]) for e in entradas])


asyncio.run(main())
```

Em script síncrono, a mesma chamada devolve o mesmo tipo:

```python
from agrobr import sync

resultado = sync.bruto.coletar(
    "acervo_fundiario",
    "snci_publico",
    uf="AL",
    destino="coleta-exemplo",
    retomar=True,
)
print(resultado.manifesto, resultado.entrada.status)
```

Para reduzir memória por página, por exemplo, use `tamanho_pagina=10` nos recursos
paginados. Ao mudar esse valor, use outro nome/destino, pois a identidade de paginação
da coleta anterior é diferente.

## Exemplos completos de manifesto

Os exemplos a seguir são **ilustrativos e sintéticos**: não são respostas observadas,
goldens, contagens atuais ou hashes de arquivos oficiais. Os endpoints e perfis de pedido
seguem os adaptadores; os valores de corpos, totais, horários, IDs e hashes servem somente
para mostrar o contrato. Cada bloco contém uma linha JSON completa, sem campos omitidos.
As nove linhas podem formar um manifesto. Seus hashes foram calculados sobre corpos
sintéticos de exemplo, não sobre arquivos oficiais. O exemplo de uso acima faz requisições
reais e produzirá contagens, hashes e horários diferentes.

### ana / massas_dagua

```json
{"agrobr_version":"2.0.0","arquivo":null,"avisos":[],"bytes":null,"bytes_armazenados":null,"cabecalhos":{},"cobertura":{"campo_id":"FID","completa":true,"controles":["ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000001.json.gz","ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000002.json.gz","ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000003.json.gz"],"estado":"conferida","ids_distintos":1,"ids_repetidos":0,"recebidas":1,"snapshot_transacional":false,"total_antes":1,"total_depois":1},"coleta_id":"exemplo-ana-massas_dagua","compressao":"nenhuma","consulta_id":"5611a8c3667d66d56bbf8bbb390e2449c408bc376c228661f621be70b6cb44a7","controles":[{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000001.json.gz","bytes":11,"bytes_armazenados":31,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","fim":"2026-10-02T20:00:02Z","formato":"json","http_status":200,"inicio":"2026-10-02T20:00:01Z","numero":1,"papel":"contagem_antes","parametros":{"f":"json","outFields":"*","outSR":"4674","returnCountOnly":"true","returnGeometry":"true","where":"(nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')"},"sha256":"6aea6dfe6561984cdc5c54ead84d47d2cf29e48253ae282aef237404adad4661","url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnCountOnly=true","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnCountOnly=true","valor_declarado":1},{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000002.json.gz","bytes":43,"bytes_armazenados":56,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","fim":"2026-10-02T20:00:04Z","formato":"json","http_status":200,"inicio":"2026-10-02T20:00:03Z","numero":2,"papel":"ids","parametros":{"f":"json","outFields":"*","outSR":"4674","returnGeometry":"true","returnIdsOnly":"true","where":"(nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')"},"sha256":"7a3c54cbac7a424181b4b1604e2d49ac58188682f239d00b15e3ad754bba4ac2","url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnIdsOnly=true","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnIdsOnly=true","valor_declarado":null},{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/controles/c000003.json.gz","bytes":11,"bytes_armazenados":31,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","fim":"2026-10-02T20:00:08Z","formato":"json","http_status":200,"inicio":"2026-10-02T20:00:07Z","numero":3,"papel":"contagem_depois","parametros":{"f":"json","outFields":"*","outSR":"4674","returnCountOnly":"true","returnGeometry":"true","where":"(nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')"},"sha256":"6aea6dfe6561984cdc5c54ead84d47d2cf29e48253ae282aef237404adad4661","url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnCountOnly=true","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29&outFields=%2A&outSR=4674&f=json&returnGeometry=true&returnCountOnly=true","valor_declarado":1}],"crs":"EPSG:4674","crs_evidencia":{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/p000001.esri.json.gz","localizador":"/spatialReference/wkid","tipo":"pagina","valor":"4674"},"erro":null,"feicoes":1,"fim":"2026-10-02T20:00:09Z","fonte":"ana","formato":"esri_json","http_fim":null,"http_inicio":null,"http_status":null,"inicio":"2026-10-02T20:00:00Z","modo":"paginado","nome":"AL","opcoes":{"compactar":true,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":100},"paginas":[{"arquivo":"ana/massas_dagua/AL/exemplo-ana-massas_dagua/p000001.esri.json.gz","bytes":179,"bytes_armazenados":158,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","crs":"EPSG:4674","feicoes_recebidas":1,"fim":"2026-10-02T20:00:06Z","formato":"esri_json","http_status":200,"ids_distintos":1,"inicio":"2026-10-02T20:00:05Z","numero":1,"paginacao":{"max":1,"min":1,"quantidade":1,"tipo":"fid"},"parametros":{"f":"json","outFields":"*","outSR":"4674","returnGeometry":"true","where":"((nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')) AND FID >= 1 AND FID <= 1"},"sha256":"9470996f4054dc75f0bffb7d229836d567f8a30168021f627469baaeda329671","total_declarado":null,"url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29%29+AND+FID+%3E%3D+1+AND+FID+%3C%3D+1&outFields=%2A&outSR=4674&f=json&returnGeometry=true","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query?where=%28%28nmufe+%3D+%27ALAGOAS%27+OR+nmufe+LIKE+%27ALAGOAS%2C+%25%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%27+OR+nmufe+LIKE+%27%25%2C+ALAGOAS%2C+%25%27%29%29+AND+FID+%3E%3D+1+AND+FID+%3C%3D+1&outFields=%2A&outSR=4674&f=json&returnGeometry=true"}],"parametros":{"f":"json","outFields":"*","outSR":"4674","returnGeometry":"true","where":"(nmufe = 'ALAGOAS' OR nmufe LIKE 'ALAGOAS, %' OR nmufe LIKE '%, ALAGOAS' OR nmufe LIKE '%, ALAGOAS, %')"},"recurso":"massas_dagua","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":"Massa_dagua/MapServer/0","edicao":null,"natureza":null,"uf":"AL"},"sha256":null,"status":"ok","tipo":"recurso","url":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query","url_solicitada":"https://www.snirh.gov.br/arcgis/rest/services/SPR/Massa_dagua/MapServer/0/query"}
```

### cnuc / ucs

```json
{"agrobr_version":"2.0.0","arquivo":null,"avisos":[],"bytes":null,"bytes_armazenados":null,"cabecalhos":{},"cobertura":{"campo_id":"cd_cnuc","completa":true,"controles":["cnuc/ucs/AL/exemplo-cnuc-ucs/controles/c000001.xml.gz","cnuc/ucs/AL/exemplo-cnuc-ucs/controles/c000002.xml.gz"],"estado":"conferida","ids_distintos":1,"ids_repetidos":0,"recebidas":1,"snapshot_transacional":false,"total_antes":1,"total_depois":1},"coleta_id":"exemplo-cnuc-ucs","compressao":"nenhuma","consulta_id":"0bf3f67dfeb6a36c6d111cac85136e18a86aee2c04702752cea1ef117b4bc299","controles":[{"arquivo":"cnuc/ucs/AL/exemplo-cnuc-ucs/controles/c000001.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:02Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:01Z","numero":1,"papel":"contagem_antes","parametros":{"FILTER":"<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0' xmlns:gml='http://www.opengis.net/gml/3.2'><fes:And><fes:PropertyIsEqualTo><fes:ValueReference>limite</fes:ValueReference><fes:Literal>uc</fes:Literal></fes:PropertyIsEqualTo><fes:PropertyIsLike wildCard='%' singleChar='_' escapeChar='!'><fes:ValueReference>uf</fes:ValueReference><fes:Literal>%ALAGOAS%</fes:Literal></fes:PropertyIsLike></fes:And></fes:Filter>","MAP":"/var/www/storage/app/mapfiles/ucs.map","REQUEST":"GetFeature","RESULTTYPE":"hits","SERVICE":"WFS","TYPENAMES":"ms:ucs_selected","VERSION":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&RESULTTYPE=hits","url_solicitada":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&RESULTTYPE=hits","valor_declarado":1},{"arquivo":"cnuc/ucs/AL/exemplo-cnuc-ucs/controles/c000002.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:08Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:07Z","numero":2,"papel":"contagem_depois","parametros":{"FILTER":"<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0' xmlns:gml='http://www.opengis.net/gml/3.2'><fes:And><fes:PropertyIsEqualTo><fes:ValueReference>limite</fes:ValueReference><fes:Literal>uc</fes:Literal></fes:PropertyIsEqualTo><fes:PropertyIsLike wildCard='%' singleChar='_' escapeChar='!'><fes:ValueReference>uf</fes:ValueReference><fes:Literal>%ALAGOAS%</fes:Literal></fes:PropertyIsLike></fes:And></fes:Filter>","MAP":"/var/www/storage/app/mapfiles/ucs.map","REQUEST":"GetFeature","RESULTTYPE":"hits","SERVICE":"WFS","TYPENAMES":"ms:ucs_selected","VERSION":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&RESULTTYPE=hits","url_solicitada":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&RESULTTYPE=hits","valor_declarado":1}],"crs":"EPSG:4674","crs_evidencia":{"arquivo":"cnuc/ucs/AL/exemplo-cnuc-ucs/p000001.gml.gz","localizador":"//*[local-name()='Polygon']/@srsName","tipo":"pagina","valor":"urn:ogc:def:crs:EPSG::4674"},"erro":null,"feicoes":1,"fim":"2026-10-02T20:00:09Z","fonte":"cnuc","formato":"gml","http_fim":null,"http_inicio":null,"http_status":null,"inicio":"2026-10-02T20:00:00Z","modo":"paginado","nome":"AL","opcoes":{"compactar":true,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":100},"paginas":[{"arquivo":"cnuc/ucs/AL/exemplo-cnuc-ucs/p000001.gml.gz","bytes":582,"bytes_armazenados":324,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","crs":"EPSG:4674","feicoes_recebidas":1,"fim":"2026-10-02T20:00:06Z","formato":"gml","http_status":200,"ids_distintos":1,"inicio":"2026-10-02T20:00:05Z","numero":1,"paginacao":{"inicio":0,"quantidade":100,"tipo":"offset"},"parametros":{"COUNT":"100","FILTER":"<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0' xmlns:gml='http://www.opengis.net/gml/3.2'><fes:And><fes:PropertyIsEqualTo><fes:ValueReference>limite</fes:ValueReference><fes:Literal>uc</fes:Literal></fes:PropertyIsEqualTo><fes:PropertyIsLike wildCard='%' singleChar='_' escapeChar='!'><fes:ValueReference>uf</fes:ValueReference><fes:Literal>%ALAGOAS%</fes:Literal></fes:PropertyIsLike></fes:And></fes:Filter>","MAP":"/var/www/storage/app/mapfiles/ucs.map","REQUEST":"GetFeature","SERVICE":"WFS","SORTBY":"cd_cnuc","STARTINDEX":"0","TYPENAMES":"ms:ucs_selected","VERSION":"2.0.0"},"sha256":"3969b3ffe691dff13fad926492fe18bb1f0499bd38172fd3df8d7f659f400903","total_declarado":"unknown","url":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&SORTBY=cd_cnuc&COUNT=100&STARTINDEX=0","url_solicitada":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv?MAP=%2Fvar%2Fwww%2Fstorage%2Fapp%2Fmapfiles%2Fucs.map&SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&TYPENAMES=ms%3Aucs_selected&FILTER=%3Cfes%3AFilter+xmlns%3Afes%3D%27http%3A%2F%2Fwww.opengis.net%2Ffes%2F2.0%27+xmlns%3Agml%3D%27http%3A%2F%2Fwww.opengis.net%2Fgml%2F3.2%27%3E%3Cfes%3AAnd%3E%3Cfes%3APropertyIsEqualTo%3E%3Cfes%3AValueReference%3Elimite%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3Euc%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsEqualTo%3E%3Cfes%3APropertyIsLike+wildCard%3D%27%25%27+singleChar%3D%27_%27+escapeChar%3D%27%21%27%3E%3Cfes%3AValueReference%3Euf%3C%2Ffes%3AValueReference%3E%3Cfes%3ALiteral%3E%25ALAGOAS%25%3C%2Ffes%3ALiteral%3E%3C%2Ffes%3APropertyIsLike%3E%3C%2Ffes%3AAnd%3E%3C%2Ffes%3AFilter%3E&SORTBY=cd_cnuc&COUNT=100&STARTINDEX=0"}],"parametros":{"FILTER":"<fes:Filter xmlns:fes='http://www.opengis.net/fes/2.0' xmlns:gml='http://www.opengis.net/gml/3.2'><fes:And><fes:PropertyIsEqualTo><fes:ValueReference>limite</fes:ValueReference><fes:Literal>uc</fes:Literal></fes:PropertyIsEqualTo><fes:PropertyIsLike wildCard='%' singleChar='_' escapeChar='!'><fes:ValueReference>uf</fes:ValueReference><fes:Literal>%ALAGOAS%</fes:Literal></fes:PropertyIsLike></fes:And></fes:Filter>","MAP":"/var/www/storage/app/mapfiles/ucs.map","REQUEST":"GetFeature","SERVICE":"WFS","SORTBY":"cd_cnuc","TYPENAMES":"ms:ucs_selected","VERSION":"2.0.0"},"recurso":"ucs","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":"ms:ucs_selected","edicao":null,"natureza":null,"uf":"AL"},"sha256":null,"status":"ok","tipo":"recurso","url":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv","url_solicitada":"https://cnuc-mapserv.mma.gov.br/cgi-bin/mapserv"}
```

### ibge / malha_municipal

```json
{"agrobr_version":"2.0.0","arquivo":null,"avisos":[],"bytes":null,"bytes_armazenados":null,"cabecalhos":{},"cobertura":{"campo_id":"cd_mun","completa":true,"controles":["ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/controles/c000001.xml.gz","ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/controles/c000002.xml.gz"],"estado":"conferida","ids_distintos":1,"ids_repetidos":0,"recebidas":1,"snapshot_transacional":false,"total_antes":1,"total_depois":1},"coleta_id":"exemplo-ibge-malha_municipal","compressao":"nenhuma","consulta_id":"bb53d383d933f7bd1789bbe15a80f635980df52b04e9dd29766e88ead1ab423b","controles":[{"arquivo":"ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/controles/c000001.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:02Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:01Z","numero":1,"papel":"contagem_antes","parametros":{"CQL_FILTER":"sigla_uf='AL'","request":"GetFeature","resultType":"hits","service":"WFS","typeNames":"CGMAT:qg_2025_030_munic","version":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&CQL_FILTER=sigla_uf%3D%27AL%27&resultType=hits","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&CQL_FILTER=sigla_uf%3D%27AL%27&resultType=hits","valor_declarado":1},{"arquivo":"ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/controles/c000002.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:08Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:07Z","numero":2,"papel":"contagem_depois","parametros":{"CQL_FILTER":"sigla_uf='AL'","request":"GetFeature","resultType":"hits","service":"WFS","typeNames":"CGMAT:qg_2025_030_munic","version":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&CQL_FILTER=sigla_uf%3D%27AL%27&resultType=hits","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&CQL_FILTER=sigla_uf%3D%27AL%27&resultType=hits","valor_declarado":1}],"crs":"EPSG:4674","crs_evidencia":{"arquivo":"ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/p000001.geojson.gz","localizador":"/crs/properties/name","tipo":"pagina","valor":"urn:ogc:def:crs:EPSG::4674"},"erro":null,"feicoes":1,"fim":"2026-10-02T20:00:09Z","fonte":"ibge","formato":"geojson","http_fim":null,"http_inicio":null,"http_status":null,"inicio":"2026-10-02T20:00:00Z","modo":"paginado","nome":"AL","opcoes":{"compactar":true,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":100},"paginas":[{"arquivo":"ibge/malha_municipal/AL/exemplo-ibge-malha_municipal/p000001.geojson.gz","bytes":337,"bytes_armazenados":232,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","crs":"EPSG:4674","feicoes_recebidas":1,"fim":"2026-10-02T20:00:06Z","formato":"geojson","http_status":200,"ids_distintos":1,"inicio":"2026-10-02T20:00:05Z","numero":1,"paginacao":{"inicio":0,"quantidade":100,"tipo":"offset"},"parametros":{"CQL_FILTER":"sigla_uf='AL'","count":"100","outputFormat":"application/json","request":"GetFeature","service":"WFS","sortBy":"cd_mun","startIndex":"0","typeNames":"CGMAT:qg_2025_030_munic","version":"2.0.0"},"sha256":"7df21d94f4b8d6bddc13ffd21967fe1a2c9f4601c6be9588c1c900c4c41a13f7","total_declarado":1,"url":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&outputFormat=application%2Fjson&sortBy=cd_mun&CQL_FILTER=sigla_uf%3D%27AL%27&count=100&startIndex=0","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGMAT%3Aqg_2025_030_munic&outputFormat=application%2Fjson&sortBy=cd_mun&CQL_FILTER=sigla_uf%3D%27AL%27&count=100&startIndex=0"}],"parametros":{"CQL_FILTER":"sigla_uf='AL'","outputFormat":"application/json","request":"GetFeature","service":"WFS","sortBy":"cd_mun","typeNames":"CGMAT:qg_2025_030_munic","version":"2.0.0"},"recurso":"malha_municipal","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":"CGMAT:qg_2025_030_munic","edicao":2025,"natureza":null,"uf":"AL"},"sha256":null,"status":"ok","tipo":"recurso","url":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverIBGE/wfs"}
```

### ibge / areas_urbanizadas

```json
{"agrobr_version":"2.0.0","arquivo":null,"avisos":[],"bytes":null,"bytes_armazenados":null,"cabecalhos":{},"cobertura":{"campo_id":"fid","completa":true,"controles":["ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/controles/c000001.xml.gz","ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/controles/c000002.xml.gz"],"estado":"conferida","ids_distintos":1,"ids_repetidos":0,"recebidas":1,"snapshot_transacional":false,"total_antes":1,"total_depois":1},"coleta_id":"exemplo-ibge-areas_urbanizadas","compressao":"nenhuma","consulta_id":"f55a4460e91eec1c635203ddc587e5eb2b7b67df52e76f0116b721eacbb0bee2","controles":[{"arquivo":"ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/controles/c000001.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:02Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:01Z","numero":1,"papel":"contagem_antes","parametros":{"CQL_FILTER":"BBOX(geom,-48.1,-16.1,-47.9,-15.9,'EPSG:4674')","request":"GetFeature","resultType":"hits","service":"WFS","typeNames":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","version":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&resultType=hits","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&resultType=hits","valor_declarado":1},{"arquivo":"ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/controles/c000002.xml.gz","bytes":104,"bytes_armazenados":112,"cabecalhos":{"content-type":"text/xml"},"compressao":"gzip","fim":"2026-10-02T20:00:08Z","formato":"xml","http_status":200,"inicio":"2026-10-02T20:00:07Z","numero":2,"papel":"contagem_depois","parametros":{"CQL_FILTER":"BBOX(geom,-48.1,-16.1,-47.9,-15.9,'EPSG:4674')","request":"GetFeature","resultType":"hits","service":"WFS","typeNames":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","version":"2.0.0"},"sha256":"b9eafd2e580456593d4eecd2d6ca89abd174349ffce0a18f2dfb00289ec4bbbf","url":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&resultType=hits","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&resultType=hits","valor_declarado":1}],"crs":"EPSG:4674","crs_evidencia":{"arquivo":"ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/p000001.geojson.gz","localizador":"/crs/properties/name","tipo":"pagina","valor":"urn:ogc:def:crs:EPSG::4674"},"erro":null,"feicoes":1,"fim":"2026-10-02T20:00:09Z","fonte":"ibge","formato":"geojson","http_fim":null,"http_inicio":null,"http_status":null,"inicio":"2026-10-02T20:00:00Z","modo":"paginado","nome":"area_teste","opcoes":{"compactar":true,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":100},"paginas":[{"arquivo":"ibge/areas_urbanizadas/area_teste/exemplo-ibge-areas_urbanizadas/p000001.geojson.gz","bytes":318,"bytes_armazenados":209,"cabecalhos":{"content-type":"application/json"},"compressao":"gzip","crs":"EPSG:4674","feicoes_recebidas":1,"fim":"2026-10-02T20:00:06Z","formato":"geojson","http_status":200,"ids_distintos":1,"inicio":"2026-10-02T20:00:05Z","numero":1,"paginacao":{"inicio":0,"quantidade":100,"tipo":"offset"},"parametros":{"CQL_FILTER":"BBOX(geom,-48.1,-16.1,-47.9,-15.9,'EPSG:4674')","count":"100","outputFormat":"application/json","request":"GetFeature","service":"WFS","sortBy":"fid","startIndex":"0","typeNames":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","version":"2.0.0"},"sha256":"0b688742a3f7a99e91dae04260370827649a2475790b157b5bf1ca9d1f897f96","total_declarado":1,"url":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&outputFormat=application%2Fjson&sortBy=fid&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&count=100&startIndex=0","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=CGEO%3AAU_2026_AreasUrbanizadas2022_Brasil&outputFormat=application%2Fjson&sortBy=fid&CQL_FILTER=BBOX%28geom%2C-48.1%2C-16.1%2C-47.9%2C-15.9%2C%27EPSG%3A4674%27%29&count=100&startIndex=0"}],"parametros":{"CQL_FILTER":"BBOX(geom,-48.1,-16.1,-47.9,-15.9,'EPSG:4674')","outputFormat":"application/json","request":"GetFeature","service":"WFS","sortBy":"fid","typeNames":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","version":"2.0.0"},"recurso":"areas_urbanizadas","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":[-48.1,-16.1,-47.9,-15.9],"bbox_crs":"EPSG:4674","camada":"CGEO:AU_2026_AreasUrbanizadas2022_Brasil","edicao":2022,"natureza":null,"uf":null},"sha256":null,"status":"ok","tipo":"recurso","url":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs","url_solicitada":"https://geoservicos.ibge.gov.br/geoserverCGEO/wfs"}
```

### acervo_fundiario / sigef_publico

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/sigef_publico/AL/exemplo-acervo_fundiario-sigef_publico/original.zip","avisos":[],"bytes":1220,"bytes_armazenados":1220,"cabecalhos":{"content-length":"1220","content-type":"application/x-zip-compressed","etag":"\"exemplo-sigef_publico\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-sigef_publico","compressao":"nenhuma","consulta_id":"090ff8e709fb9534ad3e40d8bd925cd7c5ea396043896cd485c87342f31154ef","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"sigef_publico","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":"publico","uf":"AL"},"sha256":"7f17928b731c64b486004a94e74194ef6037acf7fe57c0ec70433d4117f27f47","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20P%C3%BAblico_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20P%C3%BAblico_AL.zip"}
```

### acervo_fundiario / sigef_privado

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/sigef_privado/AL/exemplo-acervo_fundiario-sigef_privado/original.zip","avisos":[],"bytes":1220,"bytes_armazenados":1220,"cabecalhos":{"content-length":"1220","content-type":"application/x-zip-compressed","etag":"\"exemplo-sigef_privado\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-sigef_privado","compressao":"nenhuma","consulta_id":"b4babc34fe42b1f367a37f197ad34ea6d9acc79edd78e6705671540e74536a77","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"sigef_privado","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":"privado","uf":"AL"},"sha256":"d2d49150be8ec0612161b1a8506ceb3e9c2a96297eec39f69463a3177b7e04fc","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20Privado_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Sigef%20Privado_AL.zip"}
```

### acervo_fundiario / snci_publico

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/snci_publico/AL/exemplo-acervo_fundiario-snci_publico/original.zip","avisos":[],"bytes":1200,"bytes_armazenados":1200,"cabecalhos":{"content-length":"1200","content-type":"application/x-zip-compressed","etag":"\"exemplo-snci_publico\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-snci_publico","compressao":"nenhuma","consulta_id":"2f24eecf6f90be43d50a64ee4ded68d323d68dc8eefbb30f1624ae5225924d25","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"snci_publico","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":"publico","uf":"AL"},"sha256":"85843e54d72b4cd5d6397bda0bda995f9a356668bcc489f5f70ab5815aa34e25","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20P%C3%BAblico_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20P%C3%BAblico_AL.zip"}
```

### acervo_fundiario / snci_privado

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/snci_privado/AL/exemplo-acervo_fundiario-snci_privado/original.zip","avisos":[],"bytes":1200,"bytes_armazenados":1200,"cabecalhos":{"content-length":"1200","content-type":"application/x-zip-compressed","etag":"\"exemplo-snci_privado\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-snci_privado","compressao":"nenhuma","consulta_id":"6004975a7070092d9c0c32870150bf54196b711b27853db9032be8ce009e5130","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"snci_privado","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":"privado","uf":"AL"},"sha256":"0a4ce6e02cb4ef3cb49187accdd78a328066854abaab11fe087c2e062a1af651","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20Privado_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20Privado_AL.zip"}
```

### acervo_fundiario / snci_brasil

```json
{"agrobr_version":"2.0.0","arquivo":"acervo_fundiario/snci_brasil/AL/exemplo-acervo_fundiario-snci_brasil/original.zip","avisos":[],"bytes":1180,"bytes_armazenados":1180,"cabecalhos":{"content-length":"1180","content-type":"application/x-zip-compressed","etag":"\"exemplo-snci_brasil\"","last-modified":"Fri, 02 Oct 2026 20:00:00 GMT"},"cobertura":{"campo_id":null,"completa":true,"controles":[],"estado":"nao_aplicavel","ids_distintos":null,"ids_repetidos":null,"recebidas":null,"snapshot_transacional":false,"total_antes":null,"total_depois":null},"coleta_id":"exemplo-acervo_fundiario-snci_brasil","compressao":"nenhuma","consulta_id":"e808c06ffe8258cc91b2093abe6522022ceb697c3d1285db4cb76e157398129f","controles":[],"crs":null,"crs_evidencia":null,"erro":null,"feicoes":null,"fim":"2026-10-02T20:00:09Z","fonte":"acervo_fundiario","formato":"zip","http_fim":"2026-10-02T20:00:08Z","http_inicio":"2026-10-02T20:00:01Z","http_status":200,"inicio":"2026-10-02T20:00:00Z","modo":"arquivo","nome":"AL","opcoes":{"compactar":false,"limites":{"max_bytes_ids":67108864,"max_bytes_pagina":8388608,"max_bytes_recurso":4294967296,"max_ids":500000,"max_paginas":10000,"max_segundos":3600.0},"tamanho_pagina":null},"paginas":[],"parametros":{},"recurso":"snci_brasil","registrado_em":"2026-10-02T20:00:10Z","schema_version":"1.0.0","selecao":{"bbox":null,"bbox_crs":null,"camada":null,"edicao":null,"natureza":null,"uf":"AL"},"sha256":"2427aab938541297a1d359016d1e5c90180ad1879af9e7d0a870417f318f5124","status":"ok","tipo":"recurso","url":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20Brasil_AL.zip","url_solicitada":"https://certificacao.incra.gov.br/csv_shp/zip/Im%C3%B3vel%20certificado%20SNCI%20Brasil_AL.zip"}
```


## Escopo e compatibilidade

O modo bruto não normaliza atributos, não reprojeta ou repara geometrias, não deduplica
versões, não une ZIPs público/privado e não promete que sua união equivale ao arquivo
Brasil. Não compara coletas nem detecta mudança temática: um ZIP regravado ou um
`timeStamp` WFS pode alterar o hash sem alterar o dado que interessa ao consumidor.

Não oferece DataFrame, Parquet, `as_polars`, `return_meta`, SQL/CQL livre, URL arbitrária,
filtro local disfarçado de seleção remota ou escolha de áreas de estudo. A adaptação de
leitores externos pertence ao consumidor. Não usa o formato de
snapshots semânticos do agrobr.

As APIs de tabelas conservam seus contratos e CRS. `acervo_fundiario.snci()` e
`snci_geo()` sem `natureza` continuam lendo o arquivo Brasil da UF. O opt-in
`natureza="publico"` ou `"privado"` é separado desta API de arquivos.
Os hashes de páginas ANA no `MetaInfo` também não são um manifesto de arquivos:
não oferecem caminhos nem autorizam inferir que os corpos foram guardados.

Todas as fontes do modo bruto têm licença `livre` nas [licenças das fontes](../licenses.md); por isso
não há aviso de licença na coleta. Preservar o corpo não altera a licença nem valida seu conteúdo temático.

Nomes, tipos, obrigatoriedade, significado de status, hashes, CRS, caminhos e cobertura
integram o contrato. Mudança incompatível exige major do schema; campos opcionais novos,
minor; correções compatíveis, patch. Payloads da fonte não são versionados pelo agrobr.
Leitores aceitam campos adicionais opcionais em major conhecida e recusam major desconhecida.
O gravador só modifica manifestos em versões que sabe preservar integralmente; na 1.0.0,
outra versão é recusada antes da rede. Consulte a [política de versionamento](semver.md).
