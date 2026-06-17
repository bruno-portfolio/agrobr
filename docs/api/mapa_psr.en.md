# MAPA PSR API

The MAPA PSR module provides data on Brazilian rural insurance policies and claims with federal premium subsidy, published by SISSER/MAPA. Namespace: `agrobr.alt.mapa_psr`.

## Functions

### `sinistros`

Rural insurance claims — indemnities paid by crop/municipality.

```python
async def sinistros(
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: str | None = None,
    evento: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `cultura` | `str \| None` | Crop filter (partial, accent-insensitive match, e.g. "cafe" matches "CAFE ARABICA") |
| `uf` | `str \| None` | State filter (abbreviation, e.g. "MT") |
| `ano` | `int \| None` | Single-year filter (e.g. 2023) |
| `ano_inicio` | `int \| None` | Start year of the range (inclusive) |
| `ano_fim` | `int \| None` | End year of the range (inclusive) |
| `municipio` | `str \| None` | Municipality filter (partial match) |
| `evento` | `str \| None` | Filter by predominant event (e.g. "seca") |
| `as_polars` | `bool` | If True, returns a polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `nr_apolice`, `ano_apolice`, `uf`, `municipio`, `cd_ibge`,
`cultura`, `classificacao`, `evento`, `area_total`, `valor_indenizacao`, `valor_premio`,
`valor_subvencao`, `valor_limite_garantia`, `produtividade_estimada`,
`produtividade_segurada`, `nivel_cobertura`, `seguradora`

**Example:**

```python
from agrobr.alt import mapa_psr

# All claims
df = await mapa_psr.sinistros()

# Soybean claims in MT
df = await mapa_psr.sinistros(cultura="SOJA", uf="MT")

# Drought claims in 2023
df = await mapa_psr.sinistros(evento="seca", ano=2023)

# Year range
df = await mapa_psr.sinistros(ano_inicio=2020, ano_fim=2024)
```

### `apolices`

All rural insurance policies with federal subsidy.

```python
async def apolices(
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `cultura` | `str \| None` | Crop filter (partial, accent-insensitive match, e.g. "cafe" matches "CAFE ARABICA") |
| `uf` | `str \| None` | State filter (abbreviation, e.g. "MT") |
| `ano` | `int \| None` | Single-year filter (e.g. 2023) |
| `ano_inicio` | `int \| None` | Start year of the range (inclusive) |
| `ano_fim` | `int \| None` | End year of the range (inclusive) |
| `municipio` | `str \| None` | Municipality filter (partial match) |
| `as_polars` | `bool` | If True, returns a polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `nr_apolice`, `ano_apolice`, `uf`, `municipio`, `cd_ibge`,
`cultura`, `classificacao`, `area_total`, `valor_premio`, `valor_subvencao`,
`valor_limite_garantia`, `valor_indenizacao`, `evento`, `produtividade_estimada`,
`produtividade_segurada`, `nivel_cobertura`, `taxa`, `seguradora`

**Example:**

```python
from agrobr.alt import mapa_psr

# All policies
df = await mapa_psr.apolices()

# Corn policies in PR
df = await mapa_psr.apolices(cultura="MILHO", uf="PR")

# 2023 policies
df = await mapa_psr.apolices(ano=2023)
```

## Synchronous Version

```python
from agrobr.sync import alt

df = alt.mapa_psr.sinistros(cultura="SOJA", uf="MT")
df = alt.mapa_psr.apolices(ano=2023)
```

## Notes

- Source: [SISSER/MAPA](https://dados.agricultura.gov.br/dataset/sisser3) — `livre` license (CC-BY)
- Data: bulk CSV (3 files: 2006-2015, 2016-2024, 2025)
- PII removed automatically (NM_SEGURADO, NR_DOCUMENTO_SEGURADO)
- Geolocation removed (LATITUDE, LONGITUDE, degrees/min/sec)
- CSVs can be large (~500k rows in the 2006-2015 period)
- Read timeout: 180 seconds
