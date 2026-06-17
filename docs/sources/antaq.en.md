# ANTAQ — Port Cargo Movement

> **License:** Public data from the federal government.
> Classification: `livre`

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
| `ano` | int | required | Data year (2010-2025) |
| `tipo_navegacao` | str \| None | None | longo_curso, cabotagem, interior, apoio_maritimo, apoio_portuario |
| `natureza_carga` | str \| None | None | granel_solido, granel_liquido, carga_geral, conteiner |
| `mercadoria` | str \| None | None | Filter by commodity (case-insensitive substring) |
| `porto` | str \| None | None | Filter by port (case-insensitive substring) |
| `uf` | str \| None | None | Filter by state (e.g. SP, PR, MT) |
| `sentido` | str \| None | None | embarque or desembarque |
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
print(meta.parser_version)   # 1
print(meta.records_count)    # ~2.4M for a full year
```

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
