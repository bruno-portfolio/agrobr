# UN Comtrade — International Trade

United Nations Comtrade Database. Bilateral international trade data reported by ~200 countries.

## Configuration

API key optional. Works in guest mode (without a key):

```python
df = await comtrade.comercio("soja", partner="CN")
```

For more capacity (100k records/call vs 500):

```bash
export AGROBR_COMTRADE_API_KEY="sua-key-aqui"
```

Register for free at [comtradeplus.un.org](https://comtradeplus.un.org).

## API

```python
from agrobr import comtrade

# Bilateral trade
df = await comtrade.comercio("soja", reporter="BR", partner="CN", periodo=2024)

# Trade mirror (BR exports vs CN imports)
df = await comtrade.trade_mirror("soja", reporter="BR", partner="CN", periodo=2024)

# Monthly with automatic pagination
df = await comtrade.trade_mirror("soja", partner="CN", freq="M", periodo="2022-2024")

# Soybean complex (HS 1201, 1507, 2304)
df = await comtrade.comercio("complexo_soja", partner="CN")
```

## Columns — `comercio`

| Column | Type | Description |
|---|---|---|
| `periodo` | str | Period: "2024" (annual) or "202401" (monthly) |
| `ano` | int | Year extracted from the period |
| `mes` | int | Month (monthly) or null (annual) |
| `reporter_iso` | str | ISO3 of the reporter country (e.g. "BRA") |
| `partner_iso` | str | ISO3 of the partner country (e.g. "CHN") |
| `fluxo_code` | str | "X" (export) or "M" (import) |
| `hs_code` | str | HS code (4 digits) |
| `produto_desc` | str | Product description |
| `peso_liquido_kg` | float | Net weight in kg |
| `volume_ton` | float | Volume in tonnes (peso_liquido_kg / 1000) |
| `valor_fob_usd` | float | FOB value in USD |
| `valor_cif_usd` | float | CIF value in USD |
| `valor_primario_usd` | float | FOB for exports, CIF for imports |

## Columns — `trade_mirror`

Columns from both sides + computed discrepancies:

| Column | Description |
|---|---|
| `peso_liquido_kg_reporter` / `_partner` | Weight declared by each side |
| `valor_fob_usd_reporter` / `_partner` | FOB value of each side |
| `valor_cif_usd_partner` | CIF of the importing side |
| `diff_peso_kg` | reporter - partner |
| `diff_valor_fob_usd` | FOB reporter - FOB partner |
| `ratio_valor` | FOB reporter / CIF partner (expected ~0.85-0.95) |
| `ratio_peso` | Weight reporter / weight partner (expected ~1.0) |

## Mapped Products

| agrobr name | HS Code(s) |
|---|---|
| `soja` | 1201 |
| `complexo_soja` | 1201, 1507, 2304 |
| `milho` | 1005 |
| `cafe` | 0901 |
| `acucar` | 1701 |
| `carne_bovina` | 0201, 0202 |
| `carne_frango` | 0207 |
| `celulose` | 4703 |

Use `comtrade.produtos()` for the full list.

## MetaInfo

```python
df, meta = await comtrade.comercio("soja", partner="CN", return_meta=True)
print(meta.source)  # "comtrade"
print(meta.source_method)  # "httpx"
```

## Source

- API: `https://comtradeapi.un.org/data/v1/get/C/{freq}/HS`
- Format: JSON (REST API)
- Update: monthly (countries report with a 2-6 month lag)
- History: monthly since 2000, annual since 1988
- Coverage: ~200 countries, HS classification 2-6 digits
