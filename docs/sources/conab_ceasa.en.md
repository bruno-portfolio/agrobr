# CONAB CEASA/PROHORT

## Overview

| Field | Value |
|-------|-------|
| **Provider** | CONAB — Companhia Nacional de Abastecimento |
| **Data** | Daily wholesale prices for fruits and vegetables at CEASAs |
| **Access** | Pentaho CDA REST API (JSON) |
| **Format** | JSON (doQuery endpoint) |
| **Authentication** | Public credentials embedded in the frontend |
| **License** | zona_cinza |
| **Frequency** | Daily |

## Data Origin

CONAB's PROHORT system (Programa Brasileiro de Modernizacao do Mercado Hortigranjeiro) collects daily wholesale prices for fruits and vegetables at 43 CEASAs (Centrais de Abastecimento) across Brazil. The data feeds the public dashboard of CONAB's Portal de Informacoes.

agrobr accesses the data via the Pentaho BA Server (the portal's backend), using the CDA doQuery API to obtain the price matrix (48 products x 43 CEASAs) in JSON format.

## Monitored Products

| Category | Quantity | Examples |
|-----------|------------|----------|
| Fruits | 20 | Abacaxi, Banana Nanica, Laranja Pera, Manga, Melancia, Tomate, Uva |
| Vegetables | 28 | Alface, Batata, Cebola, Cenoura, Mandioca, Milho Verde, Ovos, Repolho |

**Units:** KG (most), UN (abacaxi, coco verde, couve-flor), DZ (alface, ovos)

## Covered CEASAs

43 CEASAs across 20 states, including:
- CEAGESP (12 units in SP)
- CEASAMINAS (3 units in MG)
- State CEASAs (PR, RS, SC, RJ, BA, CE, GO, DF, etc.)

## Data Structure

The API returns a pivot matrix (48 rows x 44 columns):
- Column 0: product name with unit (e.g. "TOMATE (KG)")
- Columns 1-43: price per CEASA (null = not traded)
- Column headers contain the date per CEASA (e.g. "CEAGESP - SAO PAULO\r(13/02/2026)")

The parser unpivots the matrix into long-form format with 7 columns.

## Limitations

- Only the most recent prices (daily snapshot, no time series in this version)
- Dates vary by CEASA (some inactive since 2023)
- Pentaho credentials embedded in the public frontend, but the API is not officially documented
- Occasional text corruption in headers (e.g. "ARACAT UBA" -> "ARACATUBA")

## Cache and Update

- **TTL:** 4 hours (prices updated daily)
- Recommended: use once per day for a price snapshot

## Datasets

- [`preco_atacado`](../contracts/preco_atacado.md) — wraps `ceasa.precos()` (48+ PROHORT products)

## Links

- [Portal de Informacoes CONAB](https://portaldeinformacoes.conab.gov.br/mercado-atacadista-hortigranjeiro.html)
- [CONAB](https://www.gov.br/conab/pt-br)
