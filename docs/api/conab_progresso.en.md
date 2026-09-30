# CONAB Crop Progress

Weekly planting and harvest progress data for the main annual crops, published by CONAB.

## `conab.progresso_safra()`

Sowing and harvest percentages per crop x state x week.

```python
import agrobr

df = await agrobr.conab.progresso_safra(produto="Soja", uf="MT")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `produto` | `str` | No | Published crop, ignoring case and accents: "Soja", "Milho 1a", "Milho 2a", "Arroz", "Algodao", "Feijao 1a", "Trigo". The dataset names (`milho_1`, `milho_2`, `feijao_1`) are also accepted, as are `feijao` for the 1st crop and `milho` for the 1st and 2nd crops. Any other value, including part of a name, raises `InvalidParameterError` listing the published crops, before any request. If None, all |
| `uf` | `str` | No | State code (e.g. "MT", "GO", "PR"), or "MEDIA_ESTADOS" for CONAB's own average of the monitored states, which is not the simple mean of the states ([contract](../contracts/progresso_safra.en.md)). A full state name or an unknown code raises `InvalidParameterError` before any request. "BR" is refused with `InvalidParameterError`, because CONAB does not publish Brazil. If None, all |
| `operacao` | `str` | No | "Semeadura" or "Colheita" (any other value raises `InvalidParameterError` before any request). If None, both |
| `semana_url` | `str` | No | URL of a specific week, under `https://www.gov.br/conab/` (any other URL raises `InvalidParameterError` before the request). If None, fetches the most recent |
| `as_polars` | `bool` | No | If True, returns a `polars.DataFrame` |
| `return_meta` | `bool` | No | If True, returns `(DataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `cultura` | str | Crop name (e.g. "Soja", "Milho 2a") |
| `safra` | str | Crop year in "YYYY/YY" format (e.g. "2025/26") |
| `operacao` | str | "Semeadura" or "Colheita" |
| `uf` | str | State code (e.g. "MT", "GO"); "MEDIA_ESTADOS" on the spreadsheet's "N estados" row (CONAB's own average of the monitored states, neither the simple mean of the states nor Brazil); "BR" only if the spreadsheet publishes "Brasil" |
| `semana_atual` | str | Week reference date (YYYY-MM-DD) |
| `pct_ano_anterior` | float | % same week of the previous year (0.0-1.0) |
| `pct_semana_anterior` | float | % previous week (0.0-1.0) |
| `pct_semana_atual` | float | % current week (0.0-1.0) |
| `pct_media_5_anos` | float | % average of the last 5 years (0.0-1.0) |
| `revisado` | bool | Some percentage in the row carries CONAB's `*` revision mark; null without a numeric percentage |
| `n_estados` | int | States in the average ("7 estados"), read from the spreadsheet; null for states |
| `cobertura_area_pct` | float | Share of the planted area covered by those states, read from the note "(Esses N estados correspondem a X% da área cultivada)" (0.98 = 98%), not recomputed; null for states |

The "N estados" row is an average computed by CONAB itself and cannot be reproduced from the survey areas: it is not Brazil's
total. The harvest percentage of blocks marked with `*` is computed over the cumulative sown area (spreadsheet note), not over the
total area.

State percentages are compiled by CONAB from the state surveys, and `semana_atual` is the week of CONAB's publication. For Paraná,
the value repeats DERAL's survey of the previous Monday (in the 2026-09-18 bulletin, DERAL's 2026-09-14 survey): for the survey
date, use `deral.condicao_lavouras`.

### Available Crops

| Crop | States | Operations |
|------|--------|------------|
| Soja | 12 states (96% of the area) | Semeadura, Colheita |
| Milho 1ª | 9 states (92% of the area) | Semeadura, Colheita |
| Milho 2ª | 9 states (91% of the area) | Semeadura, Colheita |
| Arroz | 6 states (88% of the area) | Semeadura, Colheita |
| Feijão 1ª | 8 states (91% of the area) | Semeadura, Colheita |
| Algodão | 7 states (98% of the area) | Semeadura, Colheita |
| Trigo | 8 states (99.9% of the area) | Colheita |

The table holds for the bulletins of 2025-09-27, 2026-02-22, 2026-08-28 and 2026-09-18, and wheat only appears with harvest in those
bulletins. The states and coverage of each block come from the spreadsheet note and may change from one crop year to the next.

---

## `conab.semanas_disponiveis()`

Lists the weeks available on the CONAB Crop Progress portal.

```python
import agrobr

semanas = await agrobr.conab.semanas_disponiveis()
for s in semanas[:3]:
    print(s["descricao"], s["url"])
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `max_pages` | `int` | No | Maximum number of pages to fetch (default 4 = ~80 weeks). Must be a positive integer: 0 or negative raises `InvalidParameterError` |

### Returns

List of dicts with `descricao` and `url` for each available week.

---

## Synchronous Usage

```python
from agrobr import sync

df = sync.conab.progresso_safra(produto="Soja")
semanas = sync.conab.semanas_disponiveis()
```

## Examples

### Soybean progress in Mato Grosso

```python
import agrobr

df = await agrobr.conab.progresso_safra(
    produto="Soja",
    uf="MT",
    operacao="Colheita",
)
print(f"Colheita soja MT: {df.iloc[0]['pct_semana_atual']:.1%}")
```

### Fetch a specific week

```python
import agrobr

semanas = await agrobr.conab.semanas_disponiveis(max_pages=1)
url_semana = semanas[0]["url"]

df = await agrobr.conab.progresso_safra(semana_url=url_semana)
```

### Compare progress across states

```python
import agrobr

df = await agrobr.conab.progresso_safra(
    produto="Soja",
    operacao="Colheita",
)
pivot = df[["uf", "pct_semana_atual"]].sort_values(
    "pct_semana_atual", ascending=False
)
print(pivot.to_string(index=False))
```

## Data Source

- **Provider:** CONAB — Companhia Nacional de Abastecimento
- **Frequency:** Weekly (published on Fridays)
- **Data:** % planting and harvest per crop x state
- **Format:** XLSX
- **Series:** Current crop year + previous-year comparison + 5-year average
- **License:** Public federal government data (livre)
- **Portal:** [Progresso de Safra](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra)
