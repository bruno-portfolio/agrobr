# Contract: comercio_internacional

Bilateral international trade of agricultural commodities — UN Comtrade.

Reuses the `COMERCIO_BILATERAL_V1` contract (same schema as `comtrade.comercio()`).

## Schema

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `periodo` | STRING | No | — | — |
| `ano` | INTEGER | No | — | >= 1988 |
| `mes` | INTEGER | Yes | — | 1-12 |
| `reporter_iso` | STRING | No | — | — |
| `reporter` | STRING | Yes | — | — |
| `partner_iso` | STRING | No | — | — |
| `partner` | STRING | Yes | — | — |
| `fluxo_code` | STRING | No | — | X or M |
| `hs_code` | STRING | No | — | — |
| `produto_desc` | STRING | Yes | — | — |
| `peso_liquido_kg` | FLOAT | Yes | kg | >= 0 |
| `volume_ton` | FLOAT | Yes | ton | >= 0 |
| `valor_fob_usd` | FLOAT | Yes | USD | >= 0 |
| `valor_cif_usd` | FLOAT | Yes | USD | >= 0 |
| `valor_primario_usd` | FLOAT | Yes | USD | >= 0 |

**PK:** `(periodo, reporter_iso, partner_iso, hs_code, fluxo_code)`

## How it differs from exportacao/importacao

| | `comercio_internacional` | `exportacao` / `importacao` |
|---|---|---|
| Source | UN Comtrade | ComexStat/MDIC |
| Coverage | Global bilateral (any reporter/partner) | Brazil only |
| Classification | HS codes | NCM |
| Breakdown | Country to country | Brazilian state |

## Example

```python
from agrobr import datasets

# Brazil's soybean exports to China
df = await datasets.comercio_internacional("soja", partner="CN")

# US imports
df = await datasets.comercio_internacional("milho", reporter="US", fluxo="M")

# Monthly
df = await datasets.comercio_internacional("cafe", freq="M", periodo="2024")

# With metadata
df, meta = await datasets.comercio_internacional("soja", return_meta=True)
```
