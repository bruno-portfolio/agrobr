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
    cd_ibge: str | None = None,
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
| `municipio` | `str \| None` | Filter by the published municipality label (partial match) |
| `evento` | `str \| None` | Filter by predominant event (e.g. "seca") |
| `cd_ibge` | `str \| None` | Filter by the municipality's IBGE code, 7 digits as text (e.g. "4305108") |
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
    cd_ibge: str | None = None,
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
| `municipio` | `str \| None` | Filter by the published municipality label (partial match) |
| `cd_ibge` | `str \| None` | Filter by the municipality's IBGE code (7 digits as text) |
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
- Temporary-file downloads and reading in chunks of 10,000 rows, with filters before numeric conversion
- Each selected period is still downloaded in full; temporary disk space is required, and result memory grows with the selected rows
- Read timeout: 180 seconds
- `municipio` compares the published label, which for some policies is the district name; for the whole municipality, use `cd_ibge` (see [MAPA PSR](../sources/mapa_psr.md#municipality-and-ibge-code))

## Policy integrity and periods

The complete CSV is validated before filters are applied. Duplicate headers, records with too many or too few fields, and invalid policy years raise `ParseError` with the record position; these rows are not silently discarded. Quoted fields may contain delimiters and line breaks. The parser is version 4; the policies contract is at 1.1 and the claims contract remains at 1.0.

`ano_apolice` is the year the policy was contracted, according to the SISSER dictionary; it is not the event or payment date. `sinistros` selects positive indemnities with a non-empty event. Published zeros remain zero in `apolices`; missing values remain null. Monetary values are not rounded to cents. Policy numbers and geographic codes retain leading zeros. Only the record published twice and identical in every column is returned once (see [MAPA PSR](../sources/mapa_psr.md)); no other row is deduplicated.

In the 2026-09-18 capture, the catalogue offered three CSVs, through 2025. The 2025 file had missing indemnity amounts, which does not establish that no claims occurred. EOF confirms that the published file was fully read; it does not guarantee complete programme coverage or up-to-date payments.

Text fields preserve literals such as `NULL`, `NA`, `None` and `N/A`, subject only to the existing whitespace and case normalization; the CSV reader does not convert them into missing values. Empty text remains empty, except `cd_ibge`, which becomes null. Numeric fields retain the existing conversion: missing or uninterpretable values remain null, without turning text tokens into zero.
