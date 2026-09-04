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
| `ano` | `int` | Ano dos artigos; o mapa atual cobre 2026 |
| `semana` | `int \| None` | Semana do relatório; `None` seleciona o artigo mais recente do ano |
| `porto` | `str \| None` | Filtro de porto, sem distinção de caixa ou acento |
| `produto` | `str \| None` | Alias de produto aceito pela [fonte ANEC](../sources/anec.md#aliases-de-produto-aceitos) |
| `tipo` | `str \| None` | `efetivado` (`last_week`) ou `programado` (`current_week`) |
| `use_cache` | `bool` | Usa o cache de PDF quando `True` |
| `as_polars` | `bool` | Retorna `polars.DataFrame` quando `True` |
| `return_meta` | `bool` | Retorna `(DataFrame, MetaInfo)` quando `True` |

Colunas: `porto`, `produto`, `periodo`, `valor_ton`. `valor_ton` usa dtype
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

Colunas: `ano`, `mes`, `produto`, `valor_ton`, `eh_estimativa`.

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

Colunas: `mes`, `produto`, `valor_2025`, `valor_2026`.

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

Colunas: `produto`, `destino`, `share_pct`.

As três funções usam `ano`, `semana`, `produto`, `use_cache`, `as_polars` e
`return_meta` com o mesmo comportamento documentado em `embarques()`.

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

Retorna os artigos do ano como modelos `ANECArticle`.

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
