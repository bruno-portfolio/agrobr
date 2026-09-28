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

The credential goes in the `Authorization: Basic` header, never in the URL: the recorded URL (httpx log, error,
`MetaInfo`) does not carry it. The default is the portal's public credential; your own credential comes from
`AGROBR_CONAB_CEASA_USER` and `AGROBR_CONAB_CEASA_PASS` and follows the same path.

## Monitored Products

| Category | Quantity | Examples |
|-----------|------------|----------|
| Fruits | 21 | Abacaxi, Banana Nanica, Coco Verde, Laranja Pera, Manga, Melancia, Uva |
| Vegetables | 26 | Alface, Batata, Cebola, Cenoura, Mandioca, Milho Verde, Repolho, Tomate |
| Eggs | 1 | Ovos |

Fruits and vegetables follow the groups of PROHORT's official "Hortaliças e Frutas" panel (Conab's Portal de Informações); eggs are outside both official groups. Brócolo, cará, couve, jiló, mandioquinha, quiabo and vagem do not appear in the panel and stay in Vegetables as agrobr's own classification. A new PROHORT product outside this table gets a null `categoria` and a warning.

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

- The CEASA of each price column comes from the column header itself (`colName`), checked against the `MDXceasa` catalog; a header missing from the catalog or duplicated raises `ParseError` instead of assigning the price to another market.

- Only the most recent prices (daily snapshot, no time series in this version)
- Dates vary by CEASA (some inactive since 2023)
- Pentaho credentials embedded in the public frontend, but the API is not officially documented
- Occasional text corruption in headers (e.g. "ARACAT UBA" -> "ARACATUBA")

## Cache and Update

- There is no local cache: every call downloads prices from CONAB.
- The source updates prices daily; one call per day is recommended for the snapshot.

## Datasets

- [`preco_atacado`](../contracts/preco_atacado.md) — wraps `ceasa.precos()` (48+ PROHORT products)

## Links

- [Portal de Informacoes CONAB](https://portaldeinformacoes.conab.gov.br/mercado-atacadista-hortigranjeiro.html)
- [CONAB](https://www.gov.br/conab/pt-br)
