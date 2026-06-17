# CONAB Crop Progress

Weekly planting and harvest progress data for the main annual crops, published by CONAB.

## `conab.progresso_safra()`

Sowing and harvest percentages per crop x state x week.

```python
import agrobr

df = await agrobr.conab.progresso_safra(cultura="Soja", estado="MT")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `cultura` | `str` | No | Crop: "Soja", "Milho 1a", "Milho 2a", "Arroz", "Algodao", "Feijao 1a", "Trigo". If None, all |
| `estado` | `str` | No | Filter by state (e.g. "MT", "GO", "PR"). If None, all |
| `operacao` | `str` | No | "Semeadura" or "Colheita". If None, both |
| `semana_url` | `str` | No | URL of a specific week. If None, fetches the most recent |
| `as_polars` | `bool` | No | If True, returns a `polars.DataFrame` |
| `return_meta` | `bool` | No | If True, returns `(DataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `cultura` | str | Crop name (e.g. "Soja", "Milho 2a") |
| `safra` | str | Crop year in "YYYY/YY" format (e.g. "2025/26") |
| `operacao` | str | "Semeadura" or "Colheita" |
| `estado` | str | State code (e.g. "MT", "GO") |
| `semana_atual` | str | Week reference date (YYYY-MM-DD) |
| `pct_ano_anterior` | float | % same week of the previous year (0.0-1.0) |
| `pct_semana_anterior` | float | % previous week (0.0-1.0) |
| `pct_semana_atual` | float | % current week (0.0-1.0) |
| `pct_media_5_anos` | float | % average of the last 5 years (0.0-1.0) |

### Available Crops

| Crop | States | Operations |
|------|--------|------------|
| Soja | 12 states (~96% area) | Semeadura, Colheita |
| Milho 1a | 9 states (~92% area) | Semeadura, Colheita |
| Milho 2a | 9 states (~91% area) | Semeadura |
| Arroz | 6 states (~88% area) | Semeadura, Colheita |
| Feijao 1a | 8 states (~91% area) | Semeadura, Colheita |
| Algodao | 7 states (~98% area) | Semeadura |
| Trigo | Variable (winter crop) | Semeadura, Colheita |

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
| `max_pages` | `int` | No | Maximum number of pages to fetch (default 4 = ~80 weeks) |

### Returns

List of dicts with `descricao` and `url` for each available week.

---

## Synchronous Usage

```python
from agrobr import sync

df = sync.conab.progresso_safra(cultura="Soja")
semanas = sync.conab.semanas_disponiveis()
```

## Examples

### Soybean progress in Mato Grosso

```python
import agrobr

df = await agrobr.conab.progresso_safra(
    cultura="Soja",
    estado="MT",
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
    cultura="Soja",
    operacao="Colheita",
)
pivot = df[["estado", "pct_semana_atual"]].sort_values(
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
