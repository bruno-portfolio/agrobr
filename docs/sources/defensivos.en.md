# Agrofit/MAPA — Agricultural pesticides

The [MAPA Open Data Portal](https://dados.agricultura.gov.br/dataset/sistema-de-agrotoxicos-fitossanitarios-agrofit) publishes two federal registration CSVs: formulated and technical products. The portal states daily updates, UTF-8, and Creative Commons Attribution licensing; the license link does not specify a version. agrobr retains its `livre` classification.

## Source coverage and agrobr output

The counts below are from the September 18, 2026 export and may change.

| Information | Captured export | agrobr output |
|---|---|---|
| Formulated products | 4,403 registrations, 15 source columns | `formulados()`, one row per registration, schema 1.1 |
| Use relations | 279,707 rows in the formulated CSV | `autorizacoes()`, retaining multiplicity, schema 1.1 |
| Technical products | 2,992 registrations, 8 source columns | `tecnicos()`, schema 1.1 |
| Composition | Ingredient, group, and concentration embedded in text in both families | `composicao()`: 5,678 formulated and 2,993 technical components, schema 1.0 |
| Situation | `SITUACAO=TRUE` in every formulated row; absent from technical export | Original text in formulated products and authorizations, with a text filter |
| Companies / countries / roles | Composite field in both CSVs | Not structured; the ignored column is identified in metadata |
| API fields absent from the current layout | No use-modality field in formulated products or separate scientific-name field in technical products | `modalidade_de_emprego` and `nome_cientifico` remain null, respectively |

The formulated export contained 392,110,268 bytes and the technical export 712,935 bytes. The capture includes mixtures of up to six components, nested parentheses in chemical groups, biological units, and products with repeated ingredients at different positions. Composition retains those positions and the published text.

The parser interprets numbers and explicit scientific notation where possible. Ambiguous expressions retain text and null values with diagnostics. Units are not converted, and potentially inconsistent source units are not implicitly corrected.

## Usage

```python
from agrobr import defensivos

products = await defensivos.formulados(situacao="TRUE")
uses = await defensivos.autorizacoes(cultura="soja")
components, meta = await defensivos.composicao(
    tipo="tecnicos", nr_registro="00301", return_meta=True,
)
```

See filters, columns, and contracts in the [Defensivos API](../api/defensivos.en.md). The same tables are available through [four Agrofit datasets](../api/defensivos_datasets.en.md), preserving their single source and original acquisition provenance.

## Integrity and provenance

The client retains project HTTP timeouts, retries, and identification. CSV reading uses the shared encoding chain. Existing fields retain their previous text cleanup; `composicao_texto`, `componente_texto`, `concentracao_texto`, and `situacao` preserve the corresponding export content.

Pydantic validates external records. Duplicate headers, incorrect field counts, invalid registrations, and conflicting product attributes raise `ParseError`. Contracts check columns, types, and keys. Metadata records the SHA-256 header fingerprint, counts, ignored columns, and parsing diagnostics.

The 24-hour cache uses `formulados.v3.zip` and `tecnicos.v3.zip`. Each file bundles related tables, a versioned manifest, hashes, types, and provenance from the same acquisition. Replacement is atomic; expired, corrupt, or incompatible files are not reused. Legacy files remain on disk and require a fresh acquisition for the current API. `agrobr.defensivos.cache.invalidate()` deletes both snapshots and the legacy files. `use_cache=False` skips reading and writing. Where it lives and how to clean it: [What agrobr writes to disk](../advanced/disco.md).

## Limits

The capture only demonstrated the `TRUE` token; its meaning was not equated with registration validity. The technical CSV does not publish situation. These are registration and use-relation data, not agronomic recommendations.

A hash identifies acquired content; the current portal interface does not provide a reproducible historical cutoff date. Formulated, technical, authorization, and composition datasets reject `deterministic` before cache/network access. Cancellation history, product labels, and structured company fields are not part of these tables.

## Export of 2026-09-18

Two ambiguous expressions in the 2026-09-18 export, `1.9 10*10 UFC/g` and `200 1x10E10 UFC/g`, retain their text and come out with null value and unit and a diagnostic. Numeric interpretation is not guaranteed for the whole component population.

The cache retains non-null values, dtypes, component position and UTC provenance; `None` and `pd.NA` are equivalent only in nullable fields. Composition and situation text retain the literal; other fields retain the documented cleanup. Authorizations are not deduplicated.

A suffix published in the concentration fields can include an ambiguous expression and does not by itself certify a measurement unit or numeric interpretation. Catalogue, CSV and cache share an origin: none of them confirms the historical population.
