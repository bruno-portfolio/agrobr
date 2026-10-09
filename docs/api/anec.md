# API ANEC

O módulo ANEC fornece embarques semanais de grãos e oleaginosas por porto,
agregados mensais, comparação anual, destinos e acesso aos relatórios PDF.

Os dados são classificados como `zona_cinza`; a primeira chamada às funções
tabulares emite `UserWarning`. Veja a [página da fonte](../sources/anec.md).

## Embarques semanais

### `embarques`

```python
async def embarques(
    *,
    ano: int,
    semana: int | None = None,
    porto: str | None = None,
    produto: str | None = None,
    tipo: Literal["efetivado", "programado"] | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `ano` | `int` | Ano da edição, de 2026 ao ano corrente; publicação depende do catálogo |
| `semana` | `int \| None` | Semana do relatório; `None` seleciona o artigo mais recente do ano |
| `porto` | `str \| None` | Filtro de porto, sem distinção de caixa ou acento |
| `produto` | `str \| None` | Alias de produto aceito pela [fonte ANEC](../sources/anec.md#aliases-de-produto-aceitos) |
| `tipo` | `str \| None` | `efetivado` (`last_week`) ou `programado` (`current_week`) |
| `use_cache` | `bool` | Usa o cache de PDF quando `True` |
| `as_polars` | `bool` | Retorna `polars.DataFrame` quando `True` |
| `return_meta` | `bool` | Retorna `(DataFrame, MetaInfo)` quando `True` |

`ano`, `semana` fora de 1–53, `produto` e `tipo` inválidos levantam `InvalidParameterError` antes de consultar o
catálogo ou baixar o PDF.

Colunas: `porto`, `produto`, `periodo`, `valor_ton`, `ano`, `semana`, `data_inicio` e `data_fim` (veja a [fonte](../sources/anec.md)). `valor_ton` usa dtype
`Float64` e recebe `pd.NA` quando o PDF não informa o volume.

## Tabelas agregadas do relatório

### `embarques_mensais`

```python
async def embarques_mensais(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

Colunas: `ano`, `mes`, `produto`, `valor_ton`, `eh_estimativa`, `valor_min_ton`, `valor_max_ton`.

### `comparacao_anual`

```python
async def comparacao_anual(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

Colunas: `mes`, `produto`, `valor_2025`, `valor_2026`, `valor_base_ton`, `valor_comparacao_ton`, `ano_base`, `ano_comparacao`, `eh_estimativa`.

Schema de fonte **1.2**. Prefira `valor_base_ton` e `valor_comparacao_ton` com seus anos explícitos. As colunas legadas `valor_2025`/`valor_2026` contêm somente valores daqueles anos literais; ano ausente fica nulo. Anos em rodapés editoriais não substituem cabeçalhos da tabela. Cabeçalhos ausentes ou incompatíveis continuam gerando `ParseError`.

### `destinos`

```python
async def destinos(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
    **kwargs: Any,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

Colunas: `produto`, `destino`, `share_pct`, `ano`, `mes_inicio`, `mes_fim`.

As três funções usam `ano`, `semana`, `use_cache`, `as_polars` e `return_meta` como
`embarques()`. `produto` aceita os mesmos aliases, com duas diferenças: `comparacao_anual()` aceita também
`"total_products"`, o agregado publicado; `destinos()` aceita só soja, farelo de soja, milho e trigo, e outro
produto levanta `InvalidParameterError` antes do download.

## Artigos e PDFs

### `articles_disponiveis`

```python
async def articles_disponiveis(year: int) -> list[dict[str, Any]]
```

Retorna dicionários com `id`, `title`, `slug`, `pdf_url`, `created_at`,
`media_updated_at`, `week` e `year`.

### `list_articles`

```python
async def list_articles(year: int) -> list[ANECArticle]
```

Retorna os boletins semanais do ano como modelos `ANECArticle`. Artigo cujo título não segue `ANEC - NN.AAAA`
é ignorado com `UserWarning` que cita o título.

### `fetch_latest_pdf`

```python
async def fetch_latest_pdf(
    year: int | None = None,
    *,
    use_cache: bool = True,
) -> tuple[bytes, str, ANECArticle]
```

Retorna os bytes, a URL e o modelo do artigo mais recente. `year=None` usa o
ano-calendário corrente.

### `fetch_pdf_bytes`

```python
async def fetch_pdf_bytes(
    article: ANECArticle,
    *,
    use_cache: bool = True,
) -> tuple[bytes, str]
```

Retorna os bytes do PDF e sua URL.

### `ANECArticle`

```python
ANECArticle(
    *,
    id: int,
    cuid: str,
    title_en: str,
    slug_en: str,
    created_at: datetime,
    pdf_url: str,
    media_updated_at: datetime,
)
```

A propriedade `week_year` extrai a tupla `(semana, ano)` do título.

## Exemplos

```python
from agrobr import anec

df = await anec.embarques(ano=2026, produto="soja", tipo="efetivado")
mensal = await anec.embarques_mensais(ano=2026, produto="milho")
comparacao = await anec.comparacao_anual(ano=2026)
destinos = await anec.destinos(ano=2026, produto="farelo de soja")
artigos = await anec.articles_disponiveis(2026)
```

## Versão síncrona

```python
from agrobr.sync import anec

df = anec.embarques(ano=2026, produto="soja")
```

## Proveniência da edição e catálogo

As três tabelas agregadas também incluem `ano_relatorio`, `semana_relatorio`, `edicao_id`, `publicado_em` e `revisado_em`. Schemas de fonte mensal/destinos permanecem 1.1; comparação anual é 1.2. Os contratos dos datasets continuam em 1.0.

Categorias anuais ausentes do mapa configurado são descobertas no catálogo oficial. Ano explícito sem publicação gera `SourceUnavailableError`, sem retornar edição anterior. `fetch_latest_pdf(year=None)` pode procurar anos anteriores suportados. Resultados vazios do catálogo são cacheados por ano durante `AGROBR_ANEC_LIST_TTL` segundos (padrão 300); zero desativa esse cache. Falhas de parsing ou rede não são cacheadas como ano sem publicação.

## Integridade da leitura do PDF

Cabeçalhos semanais e mensais incompletos geram `ParseError`, em vez de devolver
apenas as colunas reconhecidas. Vale o cabeçalho com os seis produtos ou com os
quatro das edições até a W2/2026; coluna sem nome de produto gera `ParseError`. A
soma dos portos de cada coluna semanal é conferida com a linha TOTAL publicada, e
a divergência sai em aviso, sem mudar os valores. Em destinos, a leitura termina no total da tabela
e exclui os números desenhados no mapa. Quadros rasterizados ainda podem retornar
vazio com aviso. Os valores e percentuais de cada quadro são preservados, inclusive
quando os totais publicados em outros quadros divergem.
