# Datasets de cultivares RNC e SNPC

`datasets.cultivares_registradas` entrega o cadastro corrente do Registro Nacional de Cultivares (RNC). `datasets.cultivares_protegidas` entrega a consulta corrente de proteção do Serviço Nacional de Proteção de Cultivares (SNPC). Ambos reutilizam a [API de fonte `rnc`](rnc.md), mas mantêm tabelas, identificadores e contratos distintos. O SDK não une as famílias pelo nome da cultivar nem interpreta sua situação jurídica.

## Consultas

```python
from agrobr import datasets

registradas, meta = await datasets.cultivares_registradas(
    nr_registro="42039", return_meta=True,
)
protegidas = await datasets.cultivares_protegidas(
    nr_processo="21806.000202/2014",
)
por_certificado = await datasets.cultivares_protegidas(nr_certificado="20190277")
```

Todos os argumentos são nomeados. Filtros combinados usam interseção: cada linha deve satisfazer todos eles. Os filtros são locais; uma aquisição nova baixa a exportação completa antes do recorte.

| Parâmetro | Registradas | Protegidas | Semântica |
|---|---|---|---|
| `cultivar` | Sim | Sim | Substring literal no nome da cultivar, sem caixa e acento |
| `especie` | Sim | Sim | Substring literal em `nome_comum`, sem caixa e acento e sem ampliar para nome científico |
| `grupo` | Sim | Não | Substring literal no grupo da espécie, sem caixa e acento |
| `situacao` | Sim | Sim | Substring literal na situação publicada, sem caixa e acento |
| `mantenedor` | Sim | Não | Substring literal no texto de mantenedores, sem caixa e acento |
| `titular` | Não | Sim | Substring literal no texto de titulares, sem caixa e acento |
| `nr_registro` | Sim | Não | Igualdade textual completa do número de registro |
| `nr_formulario` | Sim | Não | Igualdade textual completa do número de formulário; não é chave única |
| `nr_processo` | Não | Sim | Igualdade textual completa do número de processo |
| `nr_certificado` | Não | Sim | Igualdade textual completa do certificado; pode selecionar vários processos |

Os filtros têm padrão `None`. Exigem strings não vazias após remoção de espaços externos; não aceitam números ou booleanos em lugar de texto. O strip também se aplica aos IDs, seguido de igualdade exata: `" 42039 "` consulta `"42039"`, enquanto `"042039"` continua distinto. Pontuação, zeros e caixa dos identificadores são preservados. Os demais filtros ignoram caixa e acento (`"feijao"` acha `"Feijão"`) e tratam ponto, parênteses e outros caracteres literalmente, sem regex.

| Flag | Padrão | Comportamento |
|---|---|---|
| `use_cache` | `True` | Reutiliza uma aquisição válida; `False` evita leitura e escrita do cache |
| `as_polars` | `False` | Converte após a validação do contrato pandas |
| `return_meta` | `False` | Retorna `(df, MetaInfo)` quando verdadeiro |

As três flags são booleanos estritos. Parâmetros inválidos geram `InvalidParameterError` antes do acesso ao cache ou à rede. Argumentos posicionais, desconhecidos ou da outra família geram `TypeError` na assinatura pública. Não há parâmetro `produto`, período ou seleção histórica.

## Contratos e ausências

Registradas reutiliza [`rnc_registradas` 1.0](../contracts/cultivares_registradas.md), com dez colunas e chave `nr_registro`. Protegidas reutiliza [`rnc_protegidas` 1.0](../contracts/cultivares_protegidas.md), com doze colunas e chave `nr_processo`. Os wrappers não criam aliases no registry de contratos.

Ambos validam o contrato mesmo sem metadados e em resultados vazios. A fonte valida a população inteira antes da seleção: um filtro não oculta erro em outra linha. Ausência de correspondência produz um DataFrame vazio tipado; não comprova ausência em outra publicação ou cadastro.

Strings vazias publicadas permanecem `""`. Datas são civis em `datetime64[ns]`, sem timezone ou horário; ausência permanece `NaT`. A décima segunda coluna de protegidas, `termino_protecao_texto`, preserva a célula original com strip, inclusive datas e vazios. A expressão oficial `até a emissão do certificado definitivo` mantém esse texto e deixa a data escalar nula. Ela não é convertida em vencimento presumido nem associada automaticamente a uma única situação.

Certificados repetidos entre processos não são deduplicados. Formulários também podem repetir ou estar vazios. Textos de mantenedores, titulares, representantes e melhoristas não são divididos nem têm nomes repetidos removidos.

## Cache e proveniência

O cache de cada família guarda o CSV bruto e um manifesto de aquisição, com hash, URL, coleta UTC, parser e schema. O TTL é de 24 horas desde a coleta, não desde a última leitura. O conteúdo é revalidado antes de uso. CSVs normalizados do cache legado não são tratados como aquisições identificadas. `use_cache=False` não lê nem atualiza o cache existente.

`meta.dataset` identifica o wrapper; `meta.source` é `datasets.cultivares_registradas/rnc_registradas` ou `datasets.cultivares_protegidas/rnc_protegidas`. `selected_source` e `attempted_sources` preservam a rota da família inclusive em cache. `source_method="dataset"` identifica a camada semântica, e `from_cache` distingue reaproveitamento da aquisição.

`fetched_at` conserva a coleta original em UTC com fuso. `fetch_timestamp` é a mesma aquisição, em UTC com fuso. Hash e tamanho descrevem o CSV bruto, não o DataFrame filtrado. Chave/expiração de cache e durações de aquisição/parsing são preservadas; o parsing é realizado novamente ao ler o CSV do cache.

`source_details` contém `acquisition.resource/search`, diagnósticos do `parser`, filtros e linhas em `selection`, estado do cache e `coverage`. Se houver total comprovado na busca pública, ele deve coincidir com as linhas do CSV (`count_matched`); divergência falha. Sem total verificável, a cobertura é `unknown`. Essa conferência não transforma busca e exportação em um snapshot transacional. As contagens da população e do recorte são separadas; os metadados são copiados de forma independente.

Falhas de transporte ou de contrato da fonte são encapsuladas pela base em `SourceUnavailableError`, com sua classificação em `errors`; falha de layout ou de conteúdo do CSV sai como `ParseError`. Não há fallback para a outra família. Consulte a [descrição da fonte](../sources/rnc.md) e as [licenças](../licenses.md); acesso ao CSV não é uma declaração de direitos sobre a cultivar.

## Determinismo, sync e Polars

Qualquer contexto `datasets.deterministic(...)` é recusado antes de I/O. A exportação corrente e seu cache não permitem selecionar revisões históricas arbitrárias; `snapshot=None` nos resultados. `update_frequency="continuous"` descreve um cadastro corrente, sem prometer agenda de publicação ou SLA.

```python
from agrobr.sync import datasets

df = datasets.cultivares_registradas(nr_registro="42039")
polars_df = datasets.cultivares_protegidas(
    nr_certificado="20190277", as_polars=True,
)
```

As consultas pandas funcionam no core. Polars requer `agrobr[polars]`; a conversão conserva IDs textuais e datas, inclusive em vazio. Ambos os wrappers aparecem em `datasets.list_datasets()` e em `datasets.info(...)`.

## Exportação de 18/09/2026

Os CSVs da exportação de 18/09/2026 têm 38.325 registros RNC e 5.424 registros SNPC. A contagem HTML da pesquisa que precede cada CSV coincide com ele; essa concordância não garante snapshot transacional nem cobertura histórica.

No RNC, os 22.946 formulários, 4.256 nomes de cultivar e 4.271 mantenedores vazios publicados permanecem texto vazio; ambas as datas estão preenchidas nessa exportação. Há 924 grupos de formulários repetidos, com 15.142 ocorrências. No SNPC, o término tem 5.267 datas, 155 condições e dois vazios; as 155 condições e os dois vazios produzem data nula, preservando o texto. Há 102 certificados repetidos entre processos distintos, com 204 ocorrências. Nenhuma dessas repetições de identificadores secundários é deduplicada.

A aquisição original em `fetched_at` conserva UTC com fuso explícito, inclusive no cache e no dataset. Na fonte e no dataset, `fetch_timestamp` coincide com a aquisição. As datas civis da tabela são independentes desses instantes.
