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

State percentages are compiled by CONAB from the state surveys, and the column date is the week of CONAB's publication. For
Paraná, the value repeats DERAL's survey of the previous Monday (in the 2026-09-18 bulletin, DERAL's 2026-09-14 survey, in 5 of
5 crops); `deral.condicao_lavouras` carries the survey date.

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
2. **Coverage note**: "(Esses N estados correspondem a X% da área cultivada)", read into `n_estados` and `cobertura_area_pct`
3. **Seeding**: table with State, previous year, previous week, current week, 5-year average
4. **Harvest**: same structure (when applicable); the percentage of blocks marked with `*` is computed over the cumulative sown area
5. **"N estados" row** at the end of each block: CONAB's own average of the monitored states, returned as `uf =
   "MEDIA_ESTADOS"`, not as Brazil

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

- There is no local cache: every call downloads the data from CONAB.
- Publication is weekly, typically on Fridays.
- Use `semanas_disponiveis()` to list dates and fetch a specific week.

## Links

- [Progresso de Safra](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra)
- [CONAB](https://www.gov.br/conab/pt-br)
