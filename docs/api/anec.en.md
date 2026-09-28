# ANEC API

The ANEC module provides weekly grain and oilseed shipments by port, monthly
aggregates, year-over-year comparisons, destinations, and access to report PDFs.

The data is classified as `zona_cinza`; the first call to a tabular function
emits a `UserWarning`. See the [source page](../sources/anec.md).

## Weekly shipments

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

| Parameter | Type | Description |
|-----------|------|-------------|
| `ano` | `int` | Edition year, from 2026 to the current year; publication depends on the catalogue |
| `semana` | `int \| None` | Report week; `None` selects the latest article for the year |
| `porto` | `str \| None` | Case- and accent-insensitive port filter |
| `produto` | `str \| None` | Product alias accepted by the [ANEC source](../sources/anec.md#accepted-product-aliases) |
| `tipo` | `str \| None` | `efetivado` (`last_week`) or `programado` (`current_week`) |
| `use_cache` | `bool` | Uses the PDF cache when `True` |
| `as_polars` | `bool` | Returns a `polars.DataFrame` when `True` |
| `return_meta` | `bool` | Returns `(DataFrame, MetaInfo)` when `True` |

Invalid `ano`, `semana` outside 1–53, `produto` and `tipo` raise `InvalidParameterError` before the catalog is
queried or the PDF is downloaded.

Columns: `porto`, `produto`, `periodo`, `valor_ton`. `valor_ton` uses the
`Float64` dtype and receives `pd.NA` when the PDF does not report a volume.

## Aggregated report tables

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

Columns: `ano`, `mes`, `produto`, `valor_ton`, `eh_estimativa`, `valor_min_ton`, `valor_max_ton`.

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

Columns: `mes`, `produto`, `valor_2025`, `valor_2026`, `valor_base_ton`, `valor_comparacao_ton`, `ano_base`, `ano_comparacao`, `eh_estimativa`.

Source schema **1.2**. Prefer the year-independent `valor_base_ton` and `valor_comparacao_ton` with their explicit years. Legacy `valor_2025`/`valor_2026` contain only values belonging to those literal years; an absent year stays null. Editorial footer years do not replace table headers. Missing or inconsistent year headers still raise `ParseError`.

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

Columns: `produto`, `destino`, `share_pct`, `ano`, `mes_inicio`, `mes_fim`.

All three functions use `ano`, `semana`, `produto`, `use_cache`, `as_polars`,
and `return_meta` with the same behavior documented for `embarques()`.

## Articles and PDFs

### `articles_disponiveis`

```python
async def articles_disponiveis(year: int) -> list[dict[str, Any]]
```

Returns dictionaries with `id`, `title`, `slug`, `pdf_url`, `created_at`,
`media_updated_at`, `week`, and `year`.

### `list_articles`

```python
async def list_articles(year: int) -> list[ANECArticle]
```

Returns the year's weekly bulletins as `ANECArticle` models. An article whose title does not follow
`ANEC - NN.YYYY` is skipped with a `UserWarning` naming the title.

### `fetch_latest_pdf`

```python
async def fetch_latest_pdf(
    year: int | None = None,
    *,
    use_cache: bool = True,
) -> tuple[bytes, str, ANECArticle]
```

Returns the bytes, URL, and model for the latest article. `year=None` uses the
current calendar year.

### `fetch_pdf_bytes`

```python
async def fetch_pdf_bytes(
    article: ANECArticle,
    *,
    use_cache: bool = True,
) -> tuple[bytes, str]
```

Returns the PDF bytes and URL.

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

The `week_year` property extracts the `(week, year)` tuple from the title.

## Examples

```python
from agrobr import anec

df = await anec.embarques(ano=2026, produto="soja", tipo="efetivado")
monthly = await anec.embarques_mensais(ano=2026, produto="milho")
comparison = await anec.comparacao_anual(ano=2026)
destinations = await anec.destinos(ano=2026, produto="farelo de soja")
articles = await anec.articles_disponiveis(2026)
```

## Synchronous version

```python
from agrobr.sync import anec

df = anec.embarques(ano=2026, produto="soja")
```

## Edition provenance and catalogue

The three aggregate tables also include `ano_relatorio`, `semana_relatorio`, `edicao_id`, `publicado_em` and `revisado_em`. Monthly/destination source schemas remain 1.1; the annual comparison is 1.2. Their dataset contracts remain 1.0.

Annual categories are discovered in the official catalogue when absent from the configured map. An explicit year without publication raises `SourceUnavailableError`, without returning a previous edition. `fetch_latest_pdf(year=None)` may search previous supported years. Empty catalogue results are cached by year for `AGROBR_ANEC_LIST_TTL` seconds (default 300); zero disables this cache. Parsing or network failures are not cached as an unpublished year.

## PDF extraction integrity

Incomplete weekly or monthly headers raise `ParseError` instead of returning
only recognized columns. A header with the six products or with the four of the
editions up to W2/2026 is valid; a column without a product name raises
`ParseError`. The sum of the ports in each weekly column is checked against the
published TOTAL row, and a difference becomes a warning without changing values. Destination extraction ends at the table total and
excludes numbers drawn on the map. Raster panels may still return an empty frame
with a warning. Each table's values and percentages are preserved even when
totals published in other tables differ.
