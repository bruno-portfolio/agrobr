# ZARC (Agroclimatic Risk Zoning)

Recommended planting windows by municipality, crop, soil type and cultivar cycle.

## zoneamento

Query the ZARC Risk Table (Tabua de Risco).

```python
import agrobr

df = await agrobr.zarc.zoneamento(cultura="soja", uf="MT", safra="2025/2026")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|-------------|-----------|
| cultura | str | No | Canonical crop name (e.g. "soja", "milho_1", "trigo") |
| uf | str | No | State abbreviation (e.g. "MT", "SP") |
| municipio | int \| str | No | 7-digit IBGE code (int) or partial name (str) |
| safra | str | No | "2025/2026" or "perene" (default: most recent crop year) |
| solo | int | No | Soil type code (1-3 legacy, 11-16 new 6-AD) |
| ciclo | int | No | Cultivar cycle code (20, 21, 22, 24) |
| as_polars | bool | No | If True, return a polars DataFrame |
| return_meta | bool | No | If True, return (DataFrame, MetaInfo) |

### Returned columns

| Column | Type | Description |
|--------|------|-----------|
| cultura | str | Canonical name (e.g. "soja", "milho_1") |
| safra | str | "2025/2026" or "perene" |
| geocodigo | str | Municipality IBGE code (7 digits) |
| uf | str | State abbreviation |
| municipio | str | Municipality name |
| solo_codigo | int | Soil type |
| ciclo_codigo | int | Cultivar cycle |
| clima | str | Climatic restriction |
| manejo | str | Specific management |
| portaria | str | MAPA ordinance number |
| dec1-dec36 | int | Risk per 10-day period (0/20/30/40) |

### Examples

```python
# Soybean in Mato Grosso
df = await agrobr.zarc.zoneamento(cultura="soja", uf="MT")

# Specific municipality by geocode
df = await agrobr.zarc.zoneamento(municipio=5107925, safra="2025/2026")

# Search by partial municipality name
df = await agrobr.zarc.zoneamento(municipio="Sorriso", cultura="soja")

# Filter by soil and cycle
df = await agrobr.zarc.zoneamento(cultura="milho_1", solo=2, ciclo=20)

# Perennial crops
df = await agrobr.zarc.zoneamento(cultura="cafe_arabica", safra="perene")

# With metadata
df, meta = await agrobr.zarc.zoneamento(cultura="soja", uf="MT", return_meta=True)
print(meta.records_count, meta.fetch_duration_ms)
```

## culturas

List of crops available in ZARC (agrobr canonical names).

```python
culturas = agrobr.zarc.culturas()
# ['algodao', 'amendoim', 'arroz', 'aveia', 'banana_cavendish', ...]
```

Synchronous function (no await).

## safras_disponiveis

Crop years available in the CKAN portal (performs online discovery).

```python
safras = await agrobr.zarc.safras_disponiveis()
# ['2016/2017', '2017/2018', ..., '2025/2026', 'perene']
```

## Synchronous usage

```python
from agrobr import sync

df = sync.zarc.zoneamento(cultura="soja", uf="MT")
culturas = sync.zarc.culturas()
safras = sync.zarc.safras_disponiveis()
```

## Data source

- **Provider:** MAPA / Embrapa
- **Portal:** [dados.agricultura.gov.br](https://dados.agricultura.gov.br/dataset/tabua-de-risco-zoneamento-agricola-de-risco-climatico)
- **License:** CC-BY (federal government public data)
- **Update:** weekly
