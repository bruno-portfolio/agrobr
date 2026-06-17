# ANEC — Weekly Grain and Oilseed Shipments

> **License:** No public terms of use located. No formal contact with the
> association. Classification: `zona_cinza`

!!! warning "Gray-area status"
    The first call per session emits a `UserWarning` noting that ANEC
    publishes the data without explicit terms. Verify directly with ANEC
    before commercial use. ANEC is not in the automatic fallback of other
    datasets.

Associação Nacional dos Exportadores de Cereais. Publishes weekly PDF reports
with shipments by port for soybean, soybean meal, maize, DDGS, sorghum and wheat.

## Coverage

- **Frequency:** weekly (published on Wednesdays)
- **Supported years:** 2026+ (`MIN_YEAR=2026`). Earlier years have a different
  layout — `NotImplementedError` with an update instruction
- **Products:** Soybean, Soybean Meal, Maize, DDGS, Sorghum, Wheat
- **Ports:** 19 Brazilian ports (Santos, Paranaguá, Rio Grande, etc.)

## API

```python
from agrobr import anec

# Weekly shipments (port x product x period: efetivado/programado)
df = await anec.embarques(ano=2026)

# Specific week
df = await anec.embarques(ano=2026, semana=13)

# Filters: porto (case/accent-insensitive), produto, tipo
df = await anec.embarques(ano=2026, porto="paranagua", produto="soja")
df = await anec.embarques(ano=2026, tipo="efetivado")  # or "programado"

# Monthly aggregates
df = await anec.embarques_mensais(ano=2026, produto="soja")

# 2025 vs 2026 comparison by month x product
df = await anec.comparacao_anual(ano=2026)

# Top destinations by product (share %)
df = await anec.destinos(ano=2026, produto="soybean")

# List articles available for the year
items = await anec.articles_disponiveis(2026)
```

## Schema — `embarques()`

| Column | Type | Description |
|---|---|---|
| `porto` | str | Canonical port name (UPPER) |
| `produto` | str | `soybean`, `soybean_meal`, `maize`, `wheat`, `ddgs`, `sorghum` |
| `periodo` | str | `last_week` (efetivado) or `current_week` (programado) |
| `valor_ton` | float | Shipped volume (tonnes), `NaN` when empty |

## Schema — `embarques_mensais()`

| Column | Type | Description |
|---|---|---|
| `ano` | int | Reference year |
| `mes` | int | Month (1-12) |
| `produto` | str | Same list as embarques() |
| `valor_ton` | float | Accumulated monthly volume |
| `eh_estimativa` | bool | `True` when the month carries `*` (programming still in progress) |

## Schema — `comparacao_anual()`

| Column | Type | Description |
|---|---|---|
| `mes` | int | Month (1-12) |
| `produto` | str | Canonical product |
| `valor_2025` | float | 2025 monthly volume (reference) |
| `valor_2026` | float | 2026 monthly volume (current or estimate) |

## Schema — `destinos()`

| Column | Type | Description |
|---|---|---|
| `produto` | str | Canonical product |
| `destino` | str | Destination country (UPPER) |
| `share_pct` | float | % of the product's total exports (0-100) |

## Accepted product aliases

| Input | Canonical |
|---|---|
| `soja`, `soybean`, `soybeans`, `soja grao` | `soybean` |
| `farelo`, `farelo de soja`, `meal`, `soybean meal` | `soybean_meal` |
| `milho`, `maize`, `corn` | `maize` |
| `trigo`, `wheat` | `wheat` |
| `sorgo`, `sorghum` | `sorghum` |
| `ddgs` | `ddgs` |

## Cache

PDF cached in `~/.agrobr/cache/anec/{year}/week_{NN}/` with:
- `shipment.pdf` — PDF bytes
- `meta.json` — metadata + SHA256 + `media_updated_at` from the source

Stale detection compares cached `media_updated_at` vs ANEC. ANEC revises
retroactively: an old cache invalidates automatically when the remote
`updated_at` advances.

The JSON listing is cached in memory for 5 minutes (configurable via
`AGROBR_ANEC_LIST_TTL`). The PDF disk cache can be disabled via
`AGROBR_ANEC_CACHE_DISABLED=1`.

## MetaInfo

```python
df, meta = await anec.embarques(ano=2026, return_meta=True)
print(meta.source)              # "anec"
print(meta.source_method)       # "httpx+pdfplumber"
print(meta.parser_version)      # 1
print(meta.raw_content_hash)    # PDF fingerprint (md5 of the headers)
```

## Risk Notes

- **PDF layout**: may change in a future ANEC release. The parser computes
  an MD5 fingerprint of the structure — a divergence triggers `ParseError`
  instead of returning incorrect data.
- **Retroactive revision**: ANEC states that figures are revised at the end of
  each month. The cache invalidates automatically when `media_updated_at` advances.
- **DDGS/Sorgo in the YoY**: ANEC changed the layout between weeks — sometimes
  DDGS+Total Products appears (W04), sometimes DDGS+Sorgo (W08+). The parser
  detects both layouts.
- **`destinations` may come back empty** in weeks before the month closes (a
  legitimate case, not a parser error).

## Source

- URL: `https://www.anec.com.br/`
- Format: PDF (14 pages in 2026)
- Update: weekly (Wednesdays)
- History in agrobr: 2026+
- License: `zona_cinza`
