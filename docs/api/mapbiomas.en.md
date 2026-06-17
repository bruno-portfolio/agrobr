# MapBiomas (Land Cover and Use)

Tabular data from the MapBiomas Project — area (ha) by land cover and use class, biome and state, with an annual historical series since 1985.

## `mapbiomas.cobertura()`

Area by land cover and use class x biome x state x year.

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", ano=2020, estado="GO")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bioma` | `str` | No | Biome: "Amazonia", "Cerrado", "Caatinga", "Mata Atlantica", "Pampa", "Pantanal". If None, all |
| `estado` | `str` | No | Filter by state (e.g. "MT", "SP") or state name |
| `ano` | `int` | No | Year (1985-2024). If None, all years |
| `classe_id` | `int` | No | MapBiomas class code (e.g. 15 for Pasture) |
| `nivel` | `str` | No | `"estado"` (default) or `"municipio"`. Municipal downloads ~660 MB |
| `municipio` | `str` | No | Partial filter by municipality name (case-insensitive). Requires `nivel="municipio"` |
| `colecao` | `int` | No | Reserved for collection selection (not applied yet; always serves collection 10) |
| `as_polars` | `bool` | No | Return as polars.DataFrame |
| `return_meta` | `bool` | No | If True, returns `(DataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `bioma` | str | Biome name |
| `estado` | str | State code (e.g. "MT") |
| `municipio` | str | Municipality name (only when `nivel="municipio"`) |
| `classe_id` | int | MapBiomas class code |
| `classe` | str | Class name (e.g. "Pastagem", "Formacao Florestal") |
| `nivel_0` | str | Category: "Natural", "Antropico", "Natural/Antropico", "Indefinido" |
| `ano` | int | Reference year |
| `area_ha` | float | Area in hectares |

### MapBiomas Classes (main)

| Code | Class | Level 0 |
|--------|--------|---------|
| 3 | Formacao Florestal | Natural |
| 4 | Formacao Savanica | Natural |
| 12 | Formacao Campestre | Natural |
| 15 | Pastagem | Antropico |
| 18 | Agricultura | Antropico |
| 39 | Soja | Antropico |
| 20 | Cana | Antropico |
| 40 | Arroz | Antropico |
| 9 | Silvicultura | Antropico |
| 21 | Mosaico de Usos | Antropico |
| 24 | Area Urbanizada | Antropico |
| 33 | Rio, Lago e Oceano | Natural |

---

## `mapbiomas.transicao()`

Area of transitions between land use classes by biome x state x period.

```python
import agrobr

df = await agrobr.mapbiomas.transicao(bioma="Cerrado", periodo="2019-2020")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bioma` | `str` | No | Filter by biome. If None, all |
| `estado` | `str` | No | Filter by state or state name |
| `periodo` | `str` | No | Period (e.g. "2019-2020", "1985-2024") |
| `classe_de_id` | `int` | No | Source class code |
| `classe_para_id` | `int` | No | Target class code |
| `colecao` | `int` | No | Reserved for collection selection (not applied yet; always serves collection 10) |
| `as_polars` | `bool` | No | Return as polars.DataFrame |
| `return_meta` | `bool` | No | If True, returns `(DataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `bioma` | str | Biome name |
| `estado` | str | State code |
| `classe_de_id` | int | Source class code |
| `classe_de` | str | Source class name |
| `classe_para_id` | int | Target class code |
| `classe_para` | str | Target class name |
| `periodo` | str | Period in "YYYY-YYYY" format |
| `area_ha` | float | Area in hectares |

### Available Periods

- **Consecutive annual:** 1985-1986, 1986-1987, ..., 2023-2024
- **Five-year:** 1985-1990, 1990-1995, ..., 2020-2024
- **Ten-year:** 1985-2000, 2000-2024, 1990-2000, 2000-2010, 2010-2020
- **Total:** 1985-2024

---

## Synchronous Usage

```python
from agrobr import sync

df = sync.mapbiomas.cobertura(bioma="Cerrado", ano=2020)
df_trans = sync.mapbiomas.transicao(bioma="Amazonia", periodo="2019-2020")
```

## Examples

### Deforestation in the Cerrado (loss of native vegetation)

```python
import agrobr

# Transition from Formacao Florestal (3) to Pastagem (15) in the Cerrado
df = await agrobr.mapbiomas.transicao(
    bioma="Cerrado",
    classe_de_id=3,
    classe_para_id=15,
    periodo="2019-2020",
)
print(f"Area convertida: {df['area_ha'].sum():,.0f} ha")
```

### Municipal coverage (Belem, PA)

```python
import agrobr

# Downloads ~660 MB on the first call — filter biome/state/municipality to reduce
df = await agrobr.mapbiomas.cobertura(
    nivel="municipio", estado="PA", municipio="Belém", ano=2020
)
print(df[["municipio", "classe", "area_ha"]].head())
```

### Soybean evolution in Brazil

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(classe_id=39)  # Soja
pivot = df.groupby("ano")["area_ha"].sum()
print(pivot)
```

## Data Source

- **Project:** MapBiomas — Annual Mapping of Land Cover and Use in Brazil
- **Collection:** 10 (August 2025)
- **Historical series:** 1985-2024
- **Resolution:** 30m (Landsat)
- **Provider:** Multi-institutional collaborative network
- **Data:** [brasil.mapbiomas.org/estatisticas](https://brasil.mapbiomas.org/estatisticas/)
- **License:** Public data — free to use with attribution to the MapBiomas Project
