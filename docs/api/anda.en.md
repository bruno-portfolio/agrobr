# ANDA API

The ANDA module provides monthly fertilizer deliveries to the Brazilian market (national total), published by the Associacao Nacional para Difusao de Adubos.

!!! warning "zona_cinza license"
    Terms of use not found publicly. Formal authorization requested in Feb/2026 — awaiting response.

## Dependency

Requires `pdfplumber`:

```bash
pip install agrobr[pdf]
```

## Functions

### `entregas`

Monthly volume of fertilizer deliveries in Brazil.

```python
async def entregas(
    ano: int,
    *,
    produto: str = "total",
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ano` | `int` | Reference year. A year unavailable on the site raises `InvalidParameterError` listing the available years |
| `produto` | `str` | Kept for compatibility. The only available value is `"total"`; other values raise `ValueError` before download |
| `agregacao` | `str` | `"detalhado"` (one row per month) or `"mensal"` (sum per month, without the `uf` column); other values raise `InvalidParameterError` before the download |
| `as_polars` | `bool` | If True, returns `polars.DataFrame` |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `ano`, `mes`, `uf` (always `"BR"`), `produto_fertilizante`, `volume_ton`

`produto_fertilizante` is always `"total"`: ANDA's delivery bulletins do not
publish this indicator broken down by formulation. Previous versions merely
copied the `produto` argument into this column without filtering the data.

**Example:**

```python
from agrobr import anda

# 2024 deliveries
df = await anda.entregas(2024)

# Equivalent; the argument is kept for compatibility
df = await anda.entregas(2024, produto="total")

# Monthly aggregate
df = await anda.entregas(2024, agregacao="mensal")
```

## Synchronous Version

```python
from agrobr.sync import anda

df = anda.entregas(2024)
```

## Notes

- Source: [ANDA](https://anda.org.br) — `zona_cinza` license
- Data extracted from PDF via `pdfplumber`
- Public catalog checked on 2026-09-18: PDFs covering 2016–2026


## Publication coverage and validation

The public catalog checked on 2026-09-18 contains 11 PDFs covering 2016–2026, all with monthly national deliveries (`uf="BR"`). The 2026 bulletin publishes January through June; blank later months are not zero. In the current year (Brasília date), `entregas` and the `fertilizante` dataset warn that the bulletin is partial, in `validation_warnings` and `UserWarning`, and record `ano_em_curso` and `meses_cobertos` (the published months) in `source_details`. Since none of them publishes a state breakdown, 2.0.0 removed the `uf` argument from the source and from the `fertilizante` dataset (2.0 migration guide, section 50).

Parser 3 requires the `Fertilizantes Entregues ao Mercado (em toneladas de produto)` section and searches for the year only within it. If that year or section identity is missing, the source raises `ParseError`; the dataset retains the reason in `SourceUnavailableError`. Production, imports, exports and exchange ratios from the same PDF cannot substitute for deliveries. Published values and contract 2.0 are unchanged.
