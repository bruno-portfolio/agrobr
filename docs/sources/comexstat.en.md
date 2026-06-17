# ComexStat — Exports and Imports

Foreign trade data from MDIC/SECEX. Exports and imports
by product (NCM), state and country.

## API

```python
from agrobr import comexstat

# Monthly soybean exports in 2024
df = await comexstat.exportacao("soja", ano=2024, agregacao="mensal")

# Monthly soybean imports in 2024
df = await comexstat.importacao("soja", ano=2024, agregacao="mensal")

# Detailed exports (by country/state/transport mode)
df = await comexstat.exportacao("soja", ano=2024, agregacao="detalhado")

# Filter by state
df = await comexstat.exportacao("soja", ano=2024, uf="MT")
```

## Columns — `exportacao` / `importacao` (monthly)

| Column | Type | Description |
|---|---|---|
| `ano` | int | Year |
| `mes` | int | Month (1-12) |
| `ncm` | str | NCM code (8 digits) |
| `uf` | str | State of origin |
| `kg_liquido` | float | Net weight (kg) |
| `valor_fob_usd` | float | FOB value (USD) |
| `volume_ton` | float | Volume in tonnes |

## Products

16 agricultural products mapped by NCM prefix (full mapping: 30 keys, including fertilizers and pesticides):

| Product | NCM | Type |
|---|---|---|
| soja | 12019000 | exact |
| soja_semeadura | 12011000 | exact |
| oleo_soja_bruto | 15071000 | exact |
| farelo_soja | 23040010 | exact |
| milho | 10059010 | exact |
| cafe | 09011110 | exact |
| cafe_conilon | 09011190 | exact |
| algodao | 520100 | prefix (5201.00.20 + 5201.00.90) |
| algodao_cardado | 520300 | prefix (5203.00.00) |
| trigo | 10019900 | exact |
| arroz | 10063021 | exact |
| acucar | 17011400 | exact |
| etanol | 22071000 | exact |
| carne_bovina | 02023000 | exact |
| carne_frango | 02071400 | exact |
| carne_suina | 02032900 | exact |

> **Note:** The NCM filter uses `str.startswith(prefix)`. 8-digit prefixes
> are equivalent to an exact match; shorter prefixes (6 digits) capture all
> subheadings. This is necessary because some products (e.g. algodao) do not
> have a generic NCM in the CSV — Brazil uses detailed subheadings.

## MetaInfo

```python
df, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)
print(meta.source)  # "comexstat"
```

## Technical notes

- The site `balanca.economia.gov.br` has an incomplete SSL certificate.
  The client uses `verify=False` in httpx to work around the issue.
- Each annual CSV is ~100MB. The download is done once and filtered in memory.

## Source

- Bulk CSV: `https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm`
- Update: weekly/monthly
- History: 1997+
