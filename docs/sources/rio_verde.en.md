# Fundacao Rio Verde — Soybean Cultivar Trials

> **License:** No public terms.
> Classification: `zona_cinza`

Results of soybean cultivar trials conducted by Fundacao Rio Verde
in Lucas do Rio Verde, MT.

## Overview

| Field | Value |
|-------|-------|
| **Operator** | Fundacao Rio Verde (Lucas do Rio Verde, MT) |
| **Website** | [fundacaorioverde.com.br](https://fundacaorioverde.com.br) |
| **License** | `zona_cinza` — No public terms |
| **Format** | Text-based PDF |
| **Update** | Annual (per season) |
| **Coverage** | ~97 cultivars x 4 sowing windows (2025/26 season) |

## Available Data

### Soybean Trial

Yield results by cultivar and sowing window.

**Columns:** `safra`, `empresa`, `cultivar`, `grupo_maturacao`, `ciclo_dias`,
`produtividade_1_epoca_sc_ha`, `produtividade_2_epoca_sc_ha`, `produtividade_3_epoca_sc_ha`,
`produtividade_4_epoca_sc_ha`, `produtividade_media_sc_ha`

## API

```python
import asyncio
from agrobr import rio_verde

async def main():
    # 2025/2026 season trial
    df = await rio_verde.ensaio_soja("2025/2026")

    # Specific season
    df = await rio_verde.ensaio_soja("2024/2025")

    # List available seasons
    safras = await rio_verde.safras_disponiveis()

    # With metadata
    df, meta = await rio_verde.ensaio_soja("2025/2026", return_meta=True)

    # Polars
    df = await rio_verde.ensaio_soja("2025/2026", as_polars=True)

asyncio.run(main())
```

## Technical Notes

- Requires `pip install agrobr[pdf]` (pdfplumber)
- Text-based PDF (no OCR required)
- The parser extracts yield tables by sowing window
- Yield in bags/hectare (sc/ha)
- Available seasons depend on the PDFs published by the foundation

## Source

- URL: `https://fundacaorioverde.com.br`
- Format: PDF
- Update: annual (per season)
- License: `zona_cinza` — No public terms (verify with the foundation)
