# ANTT Toll

Vehicle counts at highway toll plazas. `frequencia="mensal"` selects published monthly resources; `"diaria"` selects daily resources. A missing resource raises an error without switching frequency.

## `fluxo_pedagio`

```python
from agrobr.alt import antt_pedagio

df = await antt_pedagio.fluxo_pedagio(ano=2023)
df, meta = await antt_pedagio.fluxo_pedagio(
    ano=2025, frequencia="diaria",
    inicio="2025-08-01", fim="2025-08-01",
    enriquecer=False, return_meta=True,
)
```

### Parameters

| Parameter | Type | Default |
|---|---|---|
| `ano` | `int \| None` | `None` |
| `ano_inicio` | `int \| None` | `None` |
| `ano_fim` | `int \| None` | `None` |
| `concessionaria` | `str \| None` | `None` |
| `rodovia` | `str \| None` | `None` |
| `uf` | `str \| None` | `None` |
| `praca` | `str \| None` | `None` |
| `tipo_veiculo` | `str \| None` | `None` |
| `apenas_pesados` | `bool` | `False` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |
| `frequencia` | `Literal['mensal', 'diaria']` | `'mensal'` |
| `tipo_cobranca` | `str \| None` | `None` |
| `inicio` | `str \| date \| datetime \| None` | `None` |
| `fim` | `str \| date \| datetime \| None` | `None` |
| `enriquecer` | `bool` | `True` |
| `max_linhas` | `int` | `500000` |
| `max_memoria_bytes` | `int` | `268435456` |

Use `ano` or `ano_inicio`/`ano_fim`, without combining them. With no year selection, the default is the previous and current calendar years. `as_polars`, `return_meta` and everything after them are keyword-only. `inicio` and `fim` are inclusive civil dates (`date`, `datetime`, whose time is dropped, or `YYYY-MM-DD` or `DD/MM/YYYY` text) within the selected years; monthly references must use day 1. `enriquecer=False` skips the plaza registry and cannot be combined with state/highway filters. Text filters are literal, not regular expressions. `tipo_veiculo` accepts `Comercial`, `Moto` or `Passeio`, ignoring case and accents; any other value raises `InvalidParameterError` before the request. `tipo_cobranca` matches the full name, ignoring case and accents; `concessionaria` and `praca` match a fragment, ignoring case. When these filters match no record, the result is empty with a warning in `UserWarning` and in `meta.validation_warnings`.

### Output — contract 3.0

| Column | Type | Nullable |
|---|---|---|
| `data` | date | No |
| `concessionaria` | str | No |
| `praca` | str | No |
| `sentido` | str | Yes |
| `n_eixos` | int | Yes |
| `tipo_veiculo` | str | Yes |
| `volume` | int | No |
| `rodovia` | str | Yes |
| `uf` | str | Yes |
| `municipio` | str | Yes |
| `categoria_eixo` | str | Yes |
| `tipo_cobranca` | str | Yes |
| `frequencia` | str | No |

**Primary key:** `data`, `concessionaria`, `praca`, `sentido`, `tipo_veiculo`, `categoria_eixo`, `tipo_cobranca`, `frequencia`.

All 13 columns are required, including nullable columns. `data` is the published day or the first day of the month. `volume` is an exact integer count; all validated occurrences contribute within the key, except the second copy of an operator × month block published twice with identical rows (one copy is kept, with a warning). A row whose volume is not a count (fractional or negative) is dropped with a warning, and a monthly reference published on a day other than 1 counts for its month, with a warning; the year is not discarded and the affected rows are listed in `source_details`. Manual, automatic and other collection types remain separate through `tipo_cobranca`; `frequencia` also belongs in identity.

Source text and surrounding spaces are retained. Text comes as `string[python]`, not in the installed pandas default dtype: the memory limit counts each retained text by object identity. `n_eixos` is populated only by an explicit textual axle count. A numeric tariff category, including a standalone number, does not establish a physical count and stays null. `apenas_pesados=True` requires commercial type and an explicit count or range guaranteeing at least three axles. Unknown categories do not acquire an invented count.

### Acquisition and limits

CSV downloads use temporary disk files, capped at 512 MiB per CSV, 1 GiB of retained temporary files and 3 GiB of transferred bytes per acquisition, including retries. The parser's default limits are 500,000 selected rows and 256 MiB of estimated retained working memory; this estimate is not a process RSS cap. Exceeding a budget raises `ResourceLimitError` rather than returning a partial result. A small date selection still downloads and validates the entire annual file.

There is no persistent CSV cache. Each acquisition discovers the CKAN catalogue again, reuses a session and closes temporary files on completion, error or cancellation. Redirects are rejected. HTTP 200 HTML from a WAF or maintenance page raises `SourceUnavailableError`; malformed CSV raises `ParseError`.

`MetaInfo` identifies schema/contract 3.0, source `antt_pedagio`, the first traffic CSV, acquisition resources and hashes. `raw_content_hash` and `raw_content_size` are those of the query and acquisition manifest; total catalogue and CSV bytes received across attempts are in `source_details["received_bytes"]`, and the bytes of the CSVs that went into the data, in `data_file_bytes`; `source_details` retains the query, manifest, CSV validation/EOF statistics, coverage and enrichment diagnostics. The current plaza registry is not historical geography. Unavailable optional enrichment is diagnosed; with `uf`/`rodovia`, an unavailable registry raises an error. These filters return only plazas proven to be in the state/highway by the registry: a record without a unique literal link (or without the requested registry field) is dropped with a warning, and the excluded rows, volume and pairs are reported in `source_details["geographic_filter"]`. `deterministic` is unsupported for mutable CKAN resources.

See the [source and dictionary](../sources/antt_pedagio.md).

## `pracas_pedagio`

The registry contract is 2.0. The official `municipal` header supplies `municipio`, with canonical-column precedence when both exist; `municipal` stays in the output, as published. `km_m` comes as `float64` (kilometre), `ano_do_pnv_snv` as `Int64` and `data_da_inativacao` as `datetime64[ns]` (published as `DD/MM/YYYY`; an impossible date, such as `31/02/2024`, becomes null with a warning, and text outside that form raises `ParseError`). Text comes in the installed pandas default dtype (`str` on pandas 3, `object` on 2). `rodovia` matches ignoring case, spaces, hyphens and leading zeros (`"BR 40"`, `"br-040"` and `"BR-40"` are the same highway); `situacao` matches a fragment, ignoring case. A filter that matches no plaza returns empty with a warning listing the published values. `as_polars` and `return_meta` are keyword-only. The current registry enriches traffic and does not necessarily represent the historical geography of the requested year.

```python
from agrobr.alt import antt_pedagio

# All plazas
df = await antt_pedagio.pracas_pedagio()

# Filter by state
df = await antt_pedagio.pracas_pedagio(uf="SP")

# Filter by highway
df = await antt_pedagio.pracas_pedagio(rodovia="BR-163")
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uf` | `str \| None` | `None` | State filter |
| `rodovia` | `str \| None` | `None` | Highway filter |
| `situacao` | `str \| None` | `None` | E.g. "Ativo" |
| `as_polars` | `bool` | `False` | Return as polars.DataFrame |
| `return_meta` | `bool` | `False` | Returns MetaInfo |

## Synchronous Usage

```python
from agrobr import sync

df = sync.alt.antt_pedagio.fluxo_pedagio(ano=2023, apenas_pesados=True)
df_pracas = sync.alt.antt_pedagio.pracas_pedagio(uf="SP")
```
