# CONAB Progresso de Safra

## Overview

| Field | Value |
|-------|-------|
| **Provider** | CONAB — Companhia Nacional de Abastecimento |
| **Data** | weekly % seeding and harvest by crop and state |
| **Access** | XLSX via the gov.br portal (Plone CMS) |
| **Format** | XLSX (openpyxl, calamine fallback) |
| **Authentication** | None |
| **License** | Public federal government data (free) |
| **Frequency** | Weekly |

## Data Origin

CONAB publishes the "Progresso de Safra" weekly with information on the planting and harvest percentages of Brazil's main annual crops. The data is collected by the company's regional offices and consolidated nationally.

agrobr accesses the XLSX files published on the Progresso de Safra page of the gov.br/conab portal. Each week has an XLSX file with seeding and harvest data by crop and state.

## Monitored Crops

| Crop | Period | States |
|---------|---------|---------|
| Soja | Summer crop (Oct-Mar) | 12 states |
| Milho 1a | Summer crop (Sep-Mar) | 9 states |
| Milho 2a | Safrinha (Jan-Jul) | 9 states |
| Arroz | Summer crop (Oct-Apr) | 6 states |
| Feijao 1a | Summer crop (Sep-Mar) | 8 states |
| Algodao | Summer crop (Nov-Mar) | 7 states |
| Trigo | Winter crop (Apr-Nov) | Variable |

## Data Structure

The weekly XLSX contains a "Progresso de safra" sheet with repeated blocks per crop:

1. **Crop header**: "Soja - Safra 2025/26"
2. **Coverage note**: "(Esses N estados correspondem a X% da area)"
3. **Seeding**: table with State, previous year, previous week, current week, 5-year average
4. **Harvest**: same structure (when applicable)

Values are fractions (0.0-1.0), not percentages.

## Access Flow

1. Listing page on gov.br (Plone pagination `?b_start:int=N`)
2. Each week has a "Plantio e Colheita" sub-link that returns the XLSX directly
3. HEAD returns 403 (Plone quirk), GET returns 200

## Limitations

- Only annual crops monitored by CONAB (6-7 crops)
- The number of states varies by crop (only the most representative ones)
- Data is published only during the crop season (no data in the off-season)
- The XLSX URL is not predictable — requires crawling the listing page
- Trigo only appears during the winter crop season

## Cache and Update

- **TTL**: 12 hours (weekly publication, typically on Fridays)
- Recommended: use `semanas_disponiveis()` to list dates and fetch a specific one

## Links

- [Progresso de Safra](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra)
- [CONAB](https://www.gov.br/conab/pt-br)
