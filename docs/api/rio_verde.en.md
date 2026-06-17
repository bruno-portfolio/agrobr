# Rio Verde API

The rio_verde module provides results of soybean cultivar trials from Fundacao Rio Verde (Lucas do Rio Verde, MT).

## Functions

### `ensaio_soja`

Yield results by cultivar and sowing window.

```python
async def ensaio_soja(
    safra: str,
    *,
    cultivar: str | None = None,
    empresa: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `safra` | `str` | Crop year (e.g. "2025/2026"). **Required** |
| `cultivar` | `str \| None` | Filter by cultivar (contains, case-insensitive) |
| `empresa` | `str \| None` | Filter by breeder company |
| `as_polars` | `bool` | Return as polars DataFrame |
| `return_meta` | `bool` | Returns a (DataFrame, MetaInfo) tuple |

**Returns:** DataFrame with columns: `safra`, `empresa`, `cultivar`, `grupo_maturacao`, `ciclo_dias`, `produtividade_1_epoca_sc_ha`, `produtividade_2_epoca_sc_ha`, `produtividade_3_epoca_sc_ha`, `produtividade_4_epoca_sc_ha`, `produtividade_media_sc_ha`

**Example:**

```python
from agrobr import rio_verde

# 2025/2026 crop year trial
df = await rio_verde.ensaio_soja("2025/2026")

# Filter by company
df = await rio_verde.ensaio_soja("2025/2026", empresa="Brasmax")

# Available crop years
safras = await rio_verde.safras_disponiveis()
```

---

### `safras_disponiveis`

Lists the crop years with available data.

```python
async def safras_disponiveis() -> list[str]
```

**Returns:** List of strings with the available crop years (e.g. `["2024/2025", "2025/2026"]`)

**Example:**

```python
from agrobr import rio_verde

safras = await rio_verde.safras_disponiveis()
```

## Synchronous Version

```python
from agrobr.sync import rio_verde

df = rio_verde.ensaio_soja(safra="2025/2026")
safras = rio_verde.safras_disponiveis()
```

## Notes

- Source: [Fundacao Rio Verde](https://fundacaorioverde.com.br) — `zona_cinza` license
- Requires `pip install agrobr[pdf]` (pdfplumber)
- ~97 cultivars x 4 sowing windows per crop year
- Yield in bags/hectare (sc/ha)
- Text-based PDF (no OCR required)
