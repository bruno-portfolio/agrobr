# MAPA PSR API

The MAPA PSR module provides data on Brazilian rural insurance policies and claims with federal premium subsidy, published by SISSER/MAPA. Namespace: `agrobr.alt.mapa_psr`.

## Functions

### `sinistros`

Rural insurance claims — indemnities paid by crop/municipality.

```python
async def sinistros(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    evento: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str \| None` | Crop filter (partial, accent-insensitive match, e.g. "cafe" matches "CAFE ARABICA") |
| `uf` | `str \| None` | State filter (abbreviation, e.g. "MT") |
| `ano` | `int \| None` | Single-year filter (e.g. 2023) |
| `ano_inicio` | `int \| None` | Start year of the range (inclusive) |
| `ano_fim` | `int \| None` | End year of the range (inclusive) |
| `municipio` | `int \| str \| None` | Municipality by its 7-digit IBGE code (`int` or `str`) or its full name, ignoring case and accents (e.g. `4305108` or `"Caxias do Sul"`); a fragment of a name, a name from another state or a repeated name without `uf` raise `InvalidParameterError` listing the candidates |
| `evento` | `str \| None` | Filter by predominant event (e.g. "seca") |
| `as_polars` | `bool` | If True, returns a polars.DataFrame; keyword-only, like `return_meta` |
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
df = await mapa_psr.sinistros(produto="SOJA", uf="MT")

# Drought claims in 2023
df = await mapa_psr.sinistros(evento="seca", ano=2023)

# Year range
df = await mapa_psr.sinistros(ano_inicio=2020, ano_fim=2024)
```

### `apolices`

All rural insurance policies with federal subsidy.

```python
async def apolices(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str \| None` | Crop filter (partial, accent-insensitive match, e.g. "cafe" matches "CAFE ARABICA") |
| `uf` | `str \| None` | State filter (abbreviation, e.g. "MT") |
| `ano` | `int \| None` | Single-year filter (e.g. 2023) |
| `ano_inicio` | `int \| None` | Start year of the range (inclusive) |
| `ano_fim` | `int \| None` | End year of the range (inclusive) |
| `municipio` | `int \| str \| None` | Municipality by its 7-digit IBGE code (`int` or `str`) or its full name, ignoring case and accents (e.g. `4305108` or `"Caxias do Sul"`); a fragment of a name, a name from another state or a repeated name without `uf` raise `InvalidParameterError` listing the candidates |
| `as_polars` | `bool` | If True, returns a polars.DataFrame; keyword-only, like `return_meta` |
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
df = await mapa_psr.apolices(produto="MILHO", uf="PR")

# 2023 policies
df = await mapa_psr.apolices(ano=2023)
```

## Synchronous Version

```python
from agrobr.sync import alt

df = alt.mapa_psr.sinistros(produto="SOJA", uf="MT")
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
- `municipio` filters by the IBGE code published in `CD_GEOCMU`, which also covers policies labelled with a district name; a row without a code is included when its label is the full municipality name in the same state (see [MAPA PSR](../sources/mapa_psr.md#municipality-and-ibge-code))

## Policy integrity and periods

The complete CSV is validated before filters are applied. Duplicate headers, records with too many or too few fields, and invalid policy years raise `ParseError` with the record position; these rows are not silently discarded. Quoted fields may contain delimiters and line breaks. The parser is version 4; the policies contract is at 1.2 and the claims contract at 1.1. `ano_apolice` comes as `Int64`, with rows and when empty.

`ano_apolice` is the year the policy was contracted, according to the SISSER dictionary; it is not the event or payment date. `sinistros` selects positive indemnities with a non-empty event. Published zeros remain zero in `apolices`; missing values remain null. Monetary values are not rounded to cents. Policy numbers and geographic codes retain leading zeros. Only the record published twice and identical in every column is returned once (see [MAPA PSR](../sources/mapa_psr.md)); no other row is deduplicated.

In the 2026-09-18 capture, the catalogue offered three CSVs, through 2025. The 2025 file had missing indemnity amounts, which does not establish that no claims occurred. EOF confirms that the published file was fully read; it does not guarantee complete programme coverage or up-to-date payments.

Text fields preserve literals such as `NULL`, `NA`, `None` and `N/A`, subject only to the existing whitespace and case normalization; the CSV reader does not convert them into missing values. Empty text remains empty, except `cd_ibge`, which becomes null. Numeric fields retain the existing conversion: missing or uninterpretable values remain null, without turning text tokens into zero.
