# ANTAQ API

!!! warning "Source unavailable since 2026-06-23"
    ANTAQ took the Estatistico Aquaviario offline ([official notice](https://www.gov.br/antaq/pt-br/central-de-conteudos/publicacoes-da-antaq/publicacoes-off/painel-estatistico-aquaviario-indisponivel)).
    The `estatistica.antaq.gov.br` host no longer serves the files: it returns `403`
    (Cloudflare challenge) or redirects to the unavailability notice, depending on the client.
    Calls to `antaq.movimentacao()` raise `SourceUnavailableError`. No alternative source
    offers equivalent coverage — Base dos Dados only covers 2014-2020.
    Last checked: 2026-08-31.

The ANTAQ module provides port cargo movement data from the Estatístico Aquaviário, published by the National Waterway Transportation Agency.

## Functions

### `movimentacao`

Port cargo movement for a single year.

```python
async def movimentacao(
    ano: int,
    *,
    tipo_navegacao: str | None = None,
    natureza_carga: str | None = None,
    mercadoria: str | None = None,
    porto: str | None = None,
    uf: str | None = None,
    sentido: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ano` | `int` | Data year, from 2010 to the latest published year |
| `tipo_navegacao` | `str \| None` | longo_curso, cabotagem, interior, apoio_maritimo, apoio_portuario |
| `natureza_carga` | `str \| None` | granel_solido, granel_liquido, carga_geral, conteiner |
| `mercadoria` | `str \| None` | Filter by commodity (case-insensitive substring) |
| `porto` | `str \| None` | Filter by port (case-insensitive substring) |
| `uf` | `str \| None` | Filter by state (e.g. SP, PR, MT) |
| `sentido` | `str \| None` | embarque or desembarque |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `ano`, `mes`, `data_atracacao`, `tipo_navegacao`, `tipo_operacao`,
`natureza_carga`, `sentido`, `porto`, `complexo_portuario`, `terminal`, `municipio`, `uf`,
`regiao`, `cd_mercadoria`, `mercadoria`, `grupo_mercadoria`, `origem`, `destino`,
`peso_bruto_ton`, `qt_carga`, `teu`

**Example:**

```python
from agrobr import antaq

# 2024 movement
df = await antaq.movimentacao(2024)

# Filter by state
df = await antaq.movimentacao(2024, uf="SP")

# Filter by navigation type and cargo nature
df = await antaq.movimentacao(
    2024,
    tipo_navegacao="longo_curso",
    natureza_carga="granel_solido",
)

# Filter by commodity
df = await antaq.movimentacao(2024, mercadoria="soja")
```

## Synchronous Version

```python
from agrobr.sync import antaq

df = antaq.movimentacao(2024)
```

## Notes

- Source: ANTAQ Estatístico Aquaviário (`estatistica.antaq.gov.br`) — `livre` license. [Offline since 2026-06-23](https://www.gov.br/antaq/pt-br/central-de-conteudos/publicacoes-da-antaq/publicacoes-off/painel-estatistico-aquaviario-indisponivel)
- Data: bulk ZIP (`;`-delimited TXT, UTF-8-sig encoding)
- History: 2010+
- Annual ZIPs (~80MB) — download may take a few seconds

`data_atracacao` preserves the published date and time as `datetime64[ns]`; a calendar typo becomes `NaT` with a warning in `MetaInfo.validation_warnings`. Non-date text raises `ParseError`. `ano`, `mes`, and `teu` use `Int64`; `peso_bruto_ton` and `qt_carga` use `float64`. Populated and empty results have the same dtypes. `sentido`, `tipo_navegacao`, and `natureza_carga` accept their documented aliases and complete published labels, ignoring case, accents and surrounding whitespace. Unsupported values raise `InvalidParameterError` before downloading the ZIPs.
