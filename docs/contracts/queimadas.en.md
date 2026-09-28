# Contract: queimadas

Satellite-detected fire hotspots — INPE Queimadas.

## Schema

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `data` | DATE | No | — | valid date |
| `hora_gmt` | STRING | Yes | — | — |
| `lat` | FLOAT | No | — | -35 to 6 |
| `lon` | FLOAT | No | — | -74 to -30 |
| `satelite` | STRING | No | — | — |
| `municipio` | STRING | Yes | — | — |
| `municipio_id` | INTEGER | Yes | — | — |
| `cod_municipio` | INTEGER | Yes | — | 7-digit IBGE code, from `municipio_id` |
| `estado` | STRING | Yes | — | — |
| `uf` | STRING | Yes | — | — |
| `bioma` | STRING | Yes | — | — |
| `numero_dias_sem_chuva` | FLOAT | Yes | days | ≥ 0 |
| `precipitacao` | FLOAT | Yes | mm | ≥ 0 |
| `risco_fogo` | FLOAT | Yes | — | 0 to 1 |
| `frp` | FLOAT | Yes | MW | ≥ 0 |

**PK:** `(data, lat, lon, satelite, hora_gmt)`

## Timezone and sentinels

`data` and `hora_gmt` come from `data_hora_gmt`, published by INPE in **GMT**; there is no
conversion to local time. `numero_dias_sem_chuva`, `precipitacao`, `risco_fogo` and `frp` use `-999`
as the published missing-value sentinel and are returned as **null**, never zero; an empty cell is
also null. `bioma` may be published empty (it happens for hotspots over water bodies, for example)
and is preserved as empty text, neither dropped nor replaced.

## Negative FRP and repeated hotspot

In some months the source publishes hotspots outside the contract. In 2.0, `queimadas.focos` and the dataset handle the cases
and go on, instead of rejecting the whole month. The key is `(data, hora_gmt, lat, lon, satelite)`:

- **copy equal in every column:** comes out once (`source_details["duplicatas_colapsadas"]`);
- **repeated key that differs only in FRP:** comes out as 1 row, with `frp` **null** (`source_details["frp_divergente"]`, with
  the hotspots and rows). The hotspot exists; only its power is ambiguous;
- **repeated key that differs in another column:** neither hotspot is the right one, and the key is dropped from the result
  (`source_details["chaves_repetidas"]`, with the keys). No case in the 2023–2026 scan;
- **negative FRP**, physically impossible: comes out **null** (`source_details["frp_negativo_anulado"]`). `-999` remains a
  sentinel and comes out null without entering that count.

Each case comes in `meta.validation_warnings` and as a `UserWarning`, with the count. The count refers to the returned result,
after the `uf`, `bioma`, and `satelite` filters. Reading 20 months (Jun–Oct 2023 to 2025, Jun–Aug 2026, Feb 2024, and Mar 2025),
these 7 were rejected whole by the dataset before the fix, with `ContractViolationError`; the repeated keys differ only in FRP:

| Month | Hotspots | Negative FRP | Equal copies | Keys with different FRP (rows) |
|-------|---------:|-------------:|-------------:|-------------------------------:|
| Jun 2023 | 206,059 | 0 | 32 | 0 |
| Jul 2023 | 308,393 | 0 | 52 | 51 (102) |
| Aug 2024 | 2,263,154 | 28 | 0 | 1 (2) |
| Sep 2024 | 2,567,880 | 7 | 0 | 2 (4) |
| Oct 2024 | 1,006,520 | 2 | 0 | 0 |
| Mar 2025 | 49,265 | 0 | 0 | 1 (2) |
| Sep 2025 | 833,039 | 0 | 9 | 1 (2) |

In Aug 2024, the repeated key is the one of Aug 29, 17:07 GMT, NOAA-20, in São Félix do Xingu (FRP 5.5 × 8.5), and the
negative FRP ranges from −3.8 to −0.1, always on VIIRS satellites.

## Partial current month

INPE publishes the file of the current period, updates it during the period, and closes it after the period ends: the monthly
file closes on the 1st day of the next month at 23:56 GMT (Jul to Dec 2025, Jul and Aug 2026), and the daily file at D+1 12:05
GMT (the 25 closed daily files of Sep 2026). On Sep 26, 2026, the September monthly file had a `Last-Modified` of Sep 26 21:56
GMT and went up to the hotspot of Sep 25 23:50 GMT; that day's daily file had a `Last-Modified` of 23:06 GMT and went up to the
hotspot of 22:40 GMT.

The result is partial when the file's `Last-Modified` is before the close, with a few minutes of margin (the 1st day of the next
month at 23:50 GMT for the monthly file; D+1 at 12:00 GMT for the daily file), or, without the header, when the clock is before
the close plus 1 h. It then comes with a warning in `meta.validation_warnings` and as a `UserWarning`, and `source_details` has
`mes_parcial` (`dia_parcial` for the daily file), `ultimo_foco` (GMT date and time of the file's last hotspot, before the
filters), and `last_modified`. A monthly file republished later (Jan to Jun 2026 were republished on Jul 17 and 21, 2026) is a
revision, not partial.

The daily file of a past day is a snapshot of D+1: for a day of a closed month, the monthly file filtered by that day is the most
complete. On Sep 14, 2026, the monthly file has 27,953 hotspots, 506 (1.8%) more than the daily file (27,447).

## Required parameters

- `ano: int` — hotspot year
- `mes: int` — hotspot month

## Biome filter

`bioma` accepts Amazônia, Cerrado, Mata Atlântica, Caatinga, Pampa, and Pantanal, with optional accents and case-insensitively. Unknown values raise `ValueError` before the source is queried.

## Example

```python
from agrobr import datasets

# August 2024 hotspots
df = await datasets.queimadas(ano=2024, mes=8)

# With filters
df = await datasets.queimadas(ano=2024, mes=8, uf="TO", bioma="Cerrado")

# With metadata
df, meta = await datasets.queimadas(ano=2024, mes=8, return_meta=True)
```
