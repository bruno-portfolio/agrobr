# zoneamento_agricola v2.1

Agricultural Climate Risk Zoning, preserving published occurrences by municipality, crop, soil and cycle (ZARC/MAPA).

## Source

| Priority | Source | Method |
|---|---|---|
| 1 | ZARC / MAPA | CSV (`zarc.zoneamento`) |

See the [source documentation](../sources/zarc.en.md).

## Schema

The **59 columns** below follow contract order. The ten-day period row represents 36 individual columns.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `cultura` | STRING | No | Canonical crop name |
| `safra` | STRING | No | Annual YYYY/YYYY season or perene, olericola or sem_safra category |
| `geocodigo` | STRING | No | Municipality IBGE code with 7 ASCII digits |
| `uf` | STRING | No | Official uppercase state abbreviation |
| `municipio` | STRING | No | Published municipality name; empty text preserved |
| `solo_codigo` | INTEGER | No | Published soil code |
| `ciclo_codigo` | INTEGER | No | Published cycle code |
| `clima` | STRING | No | Published climate restriction; empty text preserved |
| `manejo` | STRING | No | Published management; empty text preserved |
| `portaria` | STRING | No | Published ordinance identifier |
| `dec1`..`dec36` | INTEGER | Yes | Published risk: 0/20/30/40/50; empty cells are null |
| `cultura_original` | STRING | No | Crop name as published in the table |
| `safra_inicio` | STRING | No | Original SafraIni text; empty text preserved |
| `safra_fim` | STRING | No | Original SafraFin text, including non-annual categories |
| `cultura_codigo` | STRING | No | Text crop code; leading zeros preserved |
| `clima_codigo` | STRING | No | Text climate code; leading zeros and empty text preserved |
| `manejo_codigo` | STRING | No | Text management code; leading zeros and empty text preserved |
| `produtividade_texto` | STRING | No | Published text; decimal commas preserved; unit not inferred |
| `nm_codigo` | STRING | No | Text NM code; leading zeros and empty text preserved |
| `municipio_sicor_codigo` | STRING | No | Text SICOR municipality code; leading zeros and empty text preserved |
| `mesorregiao_codigo` | STRING | No | Text mesoregion code; leading zeros and empty text preserved |
| `microrregiao_codigo` | STRING | No | Text microregion code; leading zeros and empty text preserved |
| `registro_origem` | INTEGER | No | CSV position before filters; only identifiable together with the body hash |
| `cod_municipio` | INTEGER | Yes | `geocodigo` as an integer, the common key of the municipal datasets |

Every INTEGER column uses the pandas `Int64` dtype; only risk columns allow nulls. Empty text fields remain empty strings without conversion to null.

## Meaning and limits

No primary key is asserted; `registro_origem` is the position in the CSV, valid only together with the body hash. Positions are positive, unique and increasing within the processed body before filtering; its SHA-256 is in `meta.raw_content_hash`. A position does not identify the same observation across revisions.

The 36 ten-day periods cover the year, with three per month. Published risk values are **0, 20, 30, 40 and 50**, using `Int64`; an empty cell is null, distinct from zero. The value 50 occurs in the perennial table. Published values 0 and 50 are preserved without assuming an agronomic interpretation.

The `cultura` key of 11 crop labels changes in the 2024/2025 season (table on the [source page](../sources/zarc.md)). For series across seasons, join by `cultura_codigo` and `manejo`.

`safra_inicio` and `safra_fim` retain the original text. `safra` represents the annual pair or the `perene`, `olericola` or `sem_safra` category; the `safra="perene"` selector queries the table containing all three categories. `produtividade_texto` preserves published text, including decimal commas and empty cells, without inferring a unit. Repeated occurrences are preserved, text codes retain leading zeros, and validation through EOF confirms processing of the body without asserting an external total or historical snapshot.

Contract guarantees:

- Every published occurrence is preserved, including literal duplicates and distinct risk values
- No unique semantic key is asserted for the processed editions
- The source record is positional and scoped to the CSV hash, with no stability across revisions
- Risk columns use Int64; an empty cell is null and distinct from zero
- Published values 0 and 50 are preserved without assuming an agronomic interpretation
- Text codes retain leading zeros and preserve published absences as empty strings
- Yield remains published text, including decimal commas, without inferring a unit
- Annual seasons and non-annual categories retain both original fields
- Validation through EOF confirms processing of the body without asserting an external total or snapshot

## Example

`as_polars=True` requires `pip install agrobr[polars]`.

```python
from agrobr import datasets

df, meta = await datasets.zoneamento_agricola(
    cultura="soja", municipio=5107925, safra="2025/2026", return_meta=True
)
print(df.head())
print(meta.raw_content_hash)
```

## JSON Schema

`agrobr/schemas/zoneamento_agricola.json`, also available through `get_contract("zoneamento_agricola")`.

## Legacy crops and record identity

The filter catalogue includes `Arroz Sequeiro`/`arroz_sequeiro` and `Trigo Sequeiro`/`trigo_sequeiro`, published in the 2016/2017 table. These aliases retain values already returned by the parser and are not converted to `arroz`/`trigo`. Unknown names are still rejected before network access; crop availability depends on the selected table.

The 59 columns in contract 2.1 retain all 55 published fields, plus normalized crop, derived season, CSV position and `cod_municipio` (the `geocodigo` as an integer). Repeated records are preserved. `registro_origem` is meaningful only together with `meta.raw_content_hash`: the three bodies of 2026-09-18 had different hashes from those of September 7, with the same records in a different order. A changed hash does not establish that values changed.

UTC acquisition time and body hash remain in `meta.fetched_at`, `meta.raw_content_hash` and `meta.source_details["resource"]`, including cache hits. Reading to EOF confirms that the received body was processed, without certifying an external municipality total or transactional snapshot. The CKAN catalogue queried by the API declares weekly updates, while the PDF dictionary declares daily updates. The dataset uses `update_frequency="weekly"`, taking the active discovery catalogue as its operational reference; the conflicting PDF declaration remains recorded. Neither statement establishes the actual revision cadence of each season. Productivity and NM codes retain the literal text, without inferring a unit missing from the dictionary.
