# cadastro_rural v2.1

Rural property records from the Rural Environmental Registry (CAR) by state.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | SICAR/GeoServer WFS | Brazilian Forest Service / MMA |

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `cod_imovel` | str | ❌ | Unique property code in CAR |
| `status` | str | ❌ | Record status: AT, PE, SU, CA |
| `data_criacao` | datetime64[ns, UTC] | ✅ | UTC instant of record creation |
| `data_atualizacao` | datetime64[ns, UTC] | ✅ | UTC instant of last update, where available |
| `area_ha` | float64 | ❌ | Property area in hectares (>= 0) |
| `condicao` | str | ✅ | Property condition |
| `uf` | str | ❌ | State abbreviation |
| `municipio` | str | ❌ | Municipality name |
| `cod_municipio_ibge` | int | ❌ | IBGE municipality code |
| `cod_municipio` | int | ✅ | IBGE municipality code (7 digits), the common key of the municipal datasets; null outside municipality rows; equal to `cod_municipio_ibge` |
| `modulos_fiscais` | float64 | ❌ | Number of fiscal modules (>= 0) |
| `tipo` | str | ❌ | Property type: IRU, AST, PCT |

## Primary Key

`[cod_imovel]`

## Filters

| Parameter | Type | Description |
|-----------|------|-------------|
| `uf` | str | State abbreviation (required) |
| `municipio` | str | Partial municipality filter (case-insensitive) |
| `status` | str | AT (Active), PE (Pending), SU (Suspended), CA (Cancelled) |
| `cod_municipio` | int | Seven-digit IBGE code matching the state; mutually exclusive with `municipio` |
| `tipo` | str | IRU, AST or PCT, following SICAR classification |
| `area_min` | float | Minimum area in hectares |
| `area_max` | float | Maximum area in hectares |
| `criado_apos` | str | `YYYY-MM-DD`; creation on or after the date (`>=`) |
| `atualizado_apos` | str | ISO date or datetime with optional fraction and `Z`/offset; without a timezone, interpreted as UTC. Update strictly after the cutoff (`>`) |
| `as_polars` | bool | Returns Polars after contract validation; requires the `[polars]` extra |
| `return_meta` | bool | Also returns `MetaInfo`, including the URL with the effective filters |

`cod_municipio`, `atualizado_apos` and `as_polars` are keyword-only arguments. The eight previous positional arguments remain valid. State codes accept lowercase letters and surrounding whitespace. Municipality codes require integers; strings, floats and booleans are not coerced. Format and state-prefix validation do not establish that a municipality exists.

Dates must be valid calendar dates. Areas must be finite, nonnegative and ordered with minimum no greater than maximum. Unknown parameters raise an error instead of being discarded. The update field does not exist in the **PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP and TO** WFS layers: `atualizado_apos` raises `InvalidParameterError` before network access. In the other 15 layers, the column is requested and preserves the values supplied by the service; nulls are possible.

## Temporal semantics

The WFS returns current registry records. Creation and update filters select records from that current registry; they do not recover previous versions, deletions or the complete registry at a past date.

`cadastro_rural` rejects an active `datasets.deterministic(...)` context with `InvalidParameterError` before network access, including when explicit dates are supplied. The previous implementation converted the snapshot into `criado_apos`, selecting records created after the cutoff. That conversion has been removed. Normal queries return `meta.snapshot=None`.

The contract moves to **2.0** to require explicit UTC in both date columns; the eleven columns and primary key remain unchanged. All-null columns and empty results also use a UTC dtype. The tabular transport uses GeoJSON projected to attributes only, without geometry or a GeoPandas dependency. See the [migration guide](../guides/migracao-2.md).

Official captures showed different clocks in CSV and GeoJSON for the same record, with offsets of two and three hours. CQL compared cutoffs against the GeoJSON UTC instant. The tabular API therefore stopped using CSV: returned UTC timestamps can now feed `atualizado_apos` directly through `.isoformat()`. Naive dates from old CSV captures are not assigned an assumed timezone or a fixed offset.

The filter supports millisecond precision. Zeros beyond the third decimal place are removed without changing the instant (`.212000` becomes `.212`); submillisecond fractions are rejected before network access, including fractions beyond Python's microsecond precision. Probing showed that GeoServer compared `.212000` differently from `.212`; agrobr therefore sends three fractional digits without rounding more precise values. This does not assert a maximum precision for the source's internal storage.

## Multiple occurrences and provenance

For a tabular query using a single route, the dataset identifies the source as `sicar` in `attempted_sources` and `selected_source`; the `sicar.imoveis()` API uses `sicar_wfs`. Both preserve acquisition details in `source_details["sicar"]`.

The source may publish multiple occurrences of one `cod_imovel` with different feature IDs.
The dataset and `sicar.imoveis()` return one row per code among occurrences matching the query
filters. After validating the complete scan, they choose one comparison field for the entire
group: `data_atualizacao` if every occurrence has an update timestamp; otherwise `data_criacao`
if every occurrence has a creation timestamp; otherwise the feature ID. The latest timestamp in
the chosen field wins. A date tie, or the lack of a complete temporal comparison field, is resolved
by the highest numeric ID suffix (the full text ID breaks equal suffixes). Selection by ID is
deterministic; it does not establish which occurrence was most recently updated.

PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP and TO do not publish `data_atualizacao` in their layers;
selection in these states uses creation when available throughout the group. Invalid dates in
any occurrence still raise before selection, including occurrences that would be discarded.

With `return_meta=True`, `MetaInfo.validation_warnings` reports the number of codes with multiple
occurrences and the selection rule. `MetaInfo.source_details["sicar"]` contains:

- `anunciados`: last observed WFS count; `features_unicas`: number of unique feature IDs received;
- `codigos_colapsados`: number of codes with multiple occurrences;
- `versoes_descartadas_total`: number of removed occurrences before the list limit;
- `versoes_descartadas`: up to 1,000 items with `cod_imovel`, `feature_id`, `feature_id_mantida`,
  `data_atualizacao`, `data_criacao` and `criterio` (`data_atualizacao`, `data_criacao` or `feature_id`);
- `versoes_descartadas_truncadas`: whether more occurrences were discarded than the list contains;
- `criterios`: discarded-occurrence counts by criterion, including entries beyond the list limit.

Each discarded occurrence is compared with the final winner: different timestamps record the
chosen date field; a date tie records `feature_id`. Therefore,
`len(df) == features_unicas - versoes_descartadas_total`. Without repeated property codes, collapse
and discard counts are zero, the list is empty, and no selection warning is emitted.

Pagination identity is the feature ID, without adding a DataFrame column. Repeated IDs or a final
unique-ID count different from the last announced count raise `ParseError` with instructions to
repeat the query. Count changes produce warnings; the largest observed total determines the pages
requested. The source advertises `PagingIsTransactionSafe=FALSE`: count reconciliation does not
guarantee a consistent snapshot across pages. Contract 2.0 and primary key `[cod_imovel]` are unchanged.

## Guarantees

- `cod_imovel` is always non-empty
- `status` is always AT, PE, SU or CA
- `tipo` is always IRU, AST or PCT
- `area_ha` is always >= 0
- `uf` is always a valid Brazilian state code
- Empty results preserve all contract columns

## Example

```python
from agrobr import datasets

df, meta = await datasets.cadastro_rural(
    "DF", cod_municipio=5300108,
    atualizado_apos="2026-09-01", return_meta=True,
)

recentes = await datasets.cadastro_rural(
    "MT", municipio="Cuiabá", criado_apos="2026-09-01"
)

print(meta.source_url)
```

## JSON Schema

Available at `agrobr/schemas/cadastro_rural.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("cadastro_rural")
print(contract.to_json())
```
