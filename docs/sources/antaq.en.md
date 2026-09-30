# ANTAQ — Port Cargo Movement

> **License:** Public data from the federal government.
> Classification: `livre`

!!! warning "Source unavailable since 2026-06-23"
    ANTAQ took the Estatistico Aquaviario offline ([official notice](https://www.gov.br/antaq/pt-br/central-de-conteudos/publicacoes-da-antaq/publicacoes-off/painel-estatistico-aquaviario-indisponivel)).
    The `estatistica.antaq.gov.br` host no longer serves the files: it returns `403`
    (Cloudflare challenge) or redirects to the unavailability notice, depending on the client.
    Calls to `antaq.movimentacao()` raise `SourceUnavailableError`. No alternative source
    offers equivalent coverage — Base dos Dados only covers 2014-2020.
    Last checked: 2026-08-31.

National Waterway Transport Agency. Port cargo movement data
(solid bulk, liquid, general, container) since 2010.

## Installation

Does not require optional dependencies. Uses requests + pandas (core) — ANTAQ's WAF rejects httpx clients.

## API

```python
from agrobr import antaq

# Port cargo movement for a year
df = await antaq.movimentacao(2024)

# Filter by navigation type
df = await antaq.movimentacao(2024, tipo_navegacao="longo_curso")

# Filter by cargo nature
df = await antaq.movimentacao(2024, natureza_carga="granel_solido")

# Filter by commodity (case-insensitive substring)
df = await antaq.movimentacao(2024, mercadoria="soja")

# Filter by port
df = await antaq.movimentacao(2024, porto="Santos")

# Filter by state
df = await antaq.movimentacao(2024, uf="SP")

# Filter by direction
df = await antaq.movimentacao(2024, sentido="embarque")

# Multiple filters
df = await antaq.movimentacao(
    2024,
    tipo_navegacao="longo_curso",
    natureza_carga="granel_solido",
    mercadoria="soja",
    uf="PR",
    sentido="embarque",
)

# Synchronous API
from agrobr.sync import antaq as antaq_sync
df = antaq_sync.movimentacao(2024, uf="SP")
```

## Parameters — `movimentacao`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ano` | int | required | Data year, from 2010 to the latest published year; a year ANTAQ has not published yet raises `SourceUnavailableError` |
| `tipo_navegacao` | str \| None | None | longo_curso, cabotagem, interior, apoio_maritimo, apoio_portuario |
| `natureza_carga` | str \| None | None | granel_solido, granel_liquido, carga_geral, conteiner |
| `mercadoria` | str \| None | None | Filter by commodity (case-insensitive substring); no static catalog, the list comes in the ZIP's `Mercadoria.txt`, and a name outside it only shows up as an empty result |
| `porto` | str \| None | None | Filter by port (case-insensitive substring) |
| `uf` | str \| None | None | Filter by state (e.g. SP, PR, MT); an unknown state is rejected before the download |
| `sentido` | str \| None | None | embarque or desembarque |
| `as_polars` | bool | False | If True, returns a `polars.DataFrame` |
| `return_meta` | bool | False | Returns a tuple (DataFrame, MetaInfo) |

## Columns — `movimentacao`

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ano` | int | No | Year |
| `mes` | int | No | Month (1-12) |
| `data_atracacao` | str | Yes | Berthing date |
| `tipo_navegacao` | str | Yes | Navigation type |
| `tipo_operacao` | str | Yes | Cargo operation type |
| `natureza_carga` | str | Yes | Cargo nature |
| `sentido` | str | Yes | Loaded or Unloaded |
| `porto` | str | Yes | Port name |
| `complexo_portuario` | str | Yes | Port complex |
| `terminal` | str | Yes | Terminal |
| `municipio` | str | Yes | Municipality |
| `uf` | str | Yes | Port state |
| `regiao` | str | Yes | Geographic region |
| `cd_mercadoria` | str | Yes | NCM SH4 code of the commodity |
| `mercadoria` | str | Yes | Simplified nomenclature |
| `grupo_mercadoria` | str | Yes | Commodity group |
| `origem` | str | Yes | Cargo origin |
| `destino` | str | Yes | Cargo destination |
| `peso_bruto_ton` | float | Yes | Gross weight in tonnes |
| `qt_carga` | float | Yes | Cargo quantity |
| `teu` | int | Yes | TEU (containers) |

## Data pipeline

The module joins 3 tables from the Waterway Statistics (Estatístico Aquaviário):

1. **Atracacao** — port, terminal, municipality, state, date data
2. **Carga** — weight, navigation type, cargo nature, direction, commodity
3. **Mercadoria** — NCM SH4 reference table

Join via `IDAtracacao` (FK Carga → Atracacao), lookup via `CDMercadoria`.

## MetaInfo

```python
df, meta = await antaq.movimentacao(2024, return_meta=True)
print(meta.source)           # "antaq"
print(meta.source_method)    # "requests+zip"
print(meta.parser_version)   # 2
print(meta.records_count)    # ~2.4M for a full year
```

## Fields, joins and units

Of the 62 published fields across the three TXT members (29 in atracacao, 27 in carga, 6 in
mercadoria), 21 become output columns, 3 are join keys (`IDAtracacao` twice, `CDMercadoria`) and 38
are ignored.

**Joins and cardinality.** The output starts from carga: `carga -> atracacao` on `IDAtracacao` and
`carga -> mercadoria` on `CDMercadoria`, both `left`. One atracacao may carry several cargas (in a
January 2024 excerpt, atracacao `1406197` has 5), so the row count is the number of cargas, not of atracacoes; an
atracacao without carga never shows up. A carga without atracacao keeps the row with null `ano`/`mes`
in the source API and is dropped by the dataset, which requires `ano` and `mes`. A carga whose
`CDMercadoria` is absent from the table keeps the row with null `mercadoria`/`grupo_mercadoria`.

**Columns with two origins.** `tipo_navegacao` comes from `Tipo Navegacao` (carga); atracacao
publishes `Tipo de Navegacao da Atracacao`, which is read and dropped in the join projection - the
two disagree when the same atracacao moves cargo of different natures. `uf` comes from `SGUF` (the
code), not from `UF` (the spelled-out name). `mercadoria` is the
`Nomenclatura Simplificada Mercadoria`; the full NCM description (`Mercadoria`) is not published, and
the `mercadoria` filter only matches the short name.

**Units and labels.** `peso_bruto_ton` is in tonnes: the published text loses the thousands dot and
the decimal comma becomes a dot. `QTCarga` has no unit published by ANTAQ nor stated in the body
(the scale suggests kilograms for fertilizers and pieces for support cargo; that is an inference,
not a published unit) - `qt_carga` is copied unconverted. A missing
`TEU` becomes 0. `ano`/`mes` are the published atracacao period (`Ano` and `Mes`, the latter as
pt-BR text such as `jan`), not the date: an atracacao started on 2023-12-22 appears with `ano=2024`
and `mes=1`.

**Limits.** The excerpt behind the examples covers January 2024 in AM and PA; `apoio_maritimo`,
containerised cargo and `TEU > 0` have no positive case in it.

## Performance note

ANTAQ's annual ZIPs are large (~80MB compressed, ~450MB uncompressed
for Carga.txt). The download may take a few seconds. The parser uses
`usecols` to load only the necessary columns, optimizing memory.

## Source

- URL: `https://estatistica.antaq.gov.br/ea/sense/download.html`
- Download: `https://estatistica.antaq.gov.br/ea/txt/{ANO}.zip`
- Format: TXT (CSV with `;` separator, UTF-8-sig encoding, `,` decimal)
- Update: annual (consolidated data)
- History: 2010+
- License: `livre` (public data, federal government)

`data_atracacao` preserves the published date and time as `datetime64[ns]`; a calendar typo becomes `NaT` with a warning in `MetaInfo.validation_warnings`. Non-date text raises `ParseError`. `ano`, `mes`, and `teu` use `Int64`; `peso_bruto_ton` and `qt_carga` use `float64`. Populated and empty results have the same dtypes. `sentido`, `tipo_navegacao`, and `natureza_carga` accept their documented aliases and complete published labels, ignoring case, accents and surrounding whitespace. Unsupported values raise `InvalidParameterError` before downloading the ZIPs.
