# CONAB API

The CONAB module provides access to crop surveys, supply/demand balance, Brazil totals, production costs, historical series, crop progress and wholesale (CEASA) prices from the National Supply Company.

## Functions

### `safras`

Retrieves crop survey data by product and state.

```python
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product: 'soja', 'milho', 'arroz', etc. |
| `safra` | `str \| None` | Crop year in '2024/25' format. Default: latest |
| `uf` | `str \| None` | State (e.g. 'MT', 'PR'). Default: all |
| `levantamento` | `int \| None` | Survey number (1-12). Default: latest |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Returns:**

DataFrame with columns:
- `fonte`: Data source
- `produto`: Product
- `safra`: Crop year
- `uf`: State
- `area_plantada`: Planted area (thousand ha)
- `area_colhida`: Harvested area (thousand ha)
- `produtividade`: Yield (kg/ha)
- `producao`: Production (thousand t)
- `levantamento`: Survey number
- `data_publicacao`: Publication date

**Example:**

```python
from agrobr import conab

# All states
df = await conab.safras('soja', safra='2024/25')

# Mato Grosso only
df = await conab.safras('soja', safra='2024/25', uf='MT')

# Specific survey
df = await conab.safras('soja', safra='2024/25', levantamento=5)
```

---

### `balanco`

Retrieves the supply and demand balance.

```python
async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str \| None` | Specific product or all |
| `safra` | `str \| None` | Crop year. Default: latest |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Returns:**

DataFrame with columns:
- `produto`: Product
- `safra`: Crop year
- `levantamento`: Survey number
- `estoque_inicial`: Initial stock (thousand t)
- `producao`: Production (thousand t)
- `importacao`: Imports (thousand t)
- `suprimento`: Total supply (thousand t)
- `consumo`: Consumption (thousand t)
- `exportacao`: Exports (thousand t)
- `demanda_total`: Total demand (thousand t)
- `estoque_final`: Ending stock (thousand t)
- `unidade`: Unit (`mil_ton`)

**Example:**

```python
from agrobr import conab

# Soybean balance
df = await conab.balanco('soja')

# All products
df = await conab.balanco()
```

---

### `brasil_total`

Retrieves national production totals.

```python
async def brasil_total(
    safra: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Returns:**

DataFrame with Brazil totals for all products.

---

### `levantamentos`

Lists available surveys.

```python
async def levantamentos() -> list[dict]
```

**Returns:**

List of dicts describing each published survey (crop year, number and metadata).

---

### `produtos`

Lists available products (codes accepted by `safras()`).

```python
async def produtos() -> list[str]
```

---

### `ufs`

Lists the 27 available states.

```python
async def ufs() -> list[str]
```

---

## Related Functions

The CONAB module also exposes (documented in their own pages or in the contracts):

- `custo_producao(cultura, uf=...)` / `custo_producao_total(cultura, uf=...)` — production costs per hectare. See the [custo_producao](../contracts/custo_producao.md) contract
- `serie_historica(produto, ...)` — crop historical series (32 crops since 1976). See the [serie_historica_safra](../contracts/serie_historica_safra.md) contract
- `progresso_safra(...)` / `semanas_disponiveis()` — weekly planting/harvest progress. See the [CONAB Progress API](conab_progresso.md)
- `ceasa_precos(...)` / `ceasa_produtos()` / `ceasa_categorias()` / `lista_ceasas()` — wholesale produce prices. See the [CONAB CEASA API](conab_ceasa.md)

---

## Models

### `Safra`

```python
class Safra(BaseModel):
    fonte: Fonte
    produto: str
    safra: str = Field(..., pattern=r"^\d{4}/\d{2}$")
    uf: str | None = Field(None, min_length=2, max_length=2)
    area_plantada: Decimal | None = Field(None, ge=0)
    producao: Decimal | None = Field(None, ge=0)
    produtividade: Decimal | None = Field(None, ge=0)
    unidade_area: str = Field(default="mil_ha")
    unidade_producao: str = Field(default="mil_ton")
    levantamento: int = Field(..., ge=1, le=12)
    data_publicacao: date
    meta: dict[str, Any] = Field(default_factory=dict)
    parsed_at: datetime = Field(default_factory=utcnow)
    parser_version: int = Field(default=1)
    anomalies: list[str] = Field(default_factory=list)
```

## Available Products

`produtos()` returns 25 codes (including aggregate aliases and sub-crops):

| Code | Product |
|------|---------|
| `soja` | Soybean |
| `milho` | Corn (total) |
| `milho_1` | Corn 1st crop |
| `milho_2` | Corn 2nd crop |
| `milho_3` | Corn 3rd crop |
| `arroz` | Rice (total) |
| `arroz_irrigado` | Irrigated rice |
| `arroz_sequeiro` | Upland rice |
| `feijao` | Beans (total) |
| `feijao_1` | Beans 1st crop |
| `feijao_2` | Beans 2nd crop |
| `feijao_3` | Beans 3rd crop |
| `algodao` | Cotton (total) |
| `algodao_pluma` | Cotton lint |
| `trigo` | Wheat |
| `sorgo` | Sorghum |
| `aveia` | Oats |
| `cevada` | Barley |
| `canola` | Canola |
| `girassol` | Sunflower |
| `mamona` | Castor bean |
| `amendoim` | Peanut |
| `centeio` | Rye |
| `triticale` | Triticale |
| `gergelim` | Sesame |

## Synchronous Version

```python
from agrobr.sync import conab

df = conab.safras('soja', safra='2024/25')
df = conab.balanco('milho')
```
