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
| **Coverage** | Seasons 2023/24 (76 rows), 2024/25 (94) and 2025/26 (107); up to 4 sowing windows |

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

    # Partial, case-insensitive filters
    df = await rio_verde.ensaio_soja("2025/2026", cultivar="neo")
    df = await rio_verde.ensaio_soja("2025/2026", empresa="agroeste")

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
- Available seasons depend on the PDFs published by the foundation: 2023/2024, 2024/2025 and 2025/2026. The foundation also
  publishes the 2022/23 season in a layout agrobr does not read (3 sowing windows and no average yield);
  `ensaio_soja("2022/2023")` raises `InvalidParameterError` saying so, before any request. agrobr does not compute an
  average the source does not publish.
- The list of seasons is fixed in each agrobr version: a new season published by the foundation needs a new version, and until then `ensaio_soja` rejects it with `InvalidParameterError`
- 2025/26 also publishes the estimated maturity group; `grupo_maturacao` is the declared G.M., as text ("6.7")
- Some yields come without a decimal in the PDF ("87"); they are returned as 87.0
- An unknown argument raises `TypeError` before any request
- Terms of use: the website had no terms page on September 23, 2026

## Source

- URL: `https://fundacaorioverde.com.br`
- Format: PDF
- Update: annual (per season)
- License: `zona_cinza` — No public terms (verify with the foundation)

The parser extracts summary-table cells, preserving compound company and cultivar names. The 2024/25 and 2025/26 layouts differ: the latter adds estimated maturity group; `grupo_maturacao` retains the declared G.M. Missing sowing-date measurements remain null. Row counts represent observations, not unique cultivars: a cultivar may appear more than once in a report.
