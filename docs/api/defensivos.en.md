# Defensivos API

The `defensivos` module reads current Agrofit/MAPA CSV exports. All four functions are asynchronous and keyword-only. They return pandas; `as_polars=True` requests Polars, and `return_meta=True` adds `MetaInfo`.

## Formulated products

`formulados()` returns one row per `nr_registro`. Filters: `ingrediente_ativo`, `classe_toxicologica`, `classe_ambiental`, `titular`, `organicos`, `marca`, `formulacao`, `classe`, `nr_registro`, and `situacao`.

The ten existing columns remain: `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `formulacao`, `classe_toxicologica`, `classe_ambiental`, `organicos`, and `modo_de_acao`. Schema **1.1** adds nullable `situacao` and `composicao_texto`.

`composicao_texto` preserves the original cell, including whitespace and characters. `ingrediente_ativo` retains the previous formulated-product representation. Conflicting product attributes across rows of the same registration raise `ParseError`.

```python
from agrobr import defensivos

products, meta = await defensivos.formulados(
    ingrediente_ativo="glifosato", situacao="TRUE", return_meta=True,
)
```

## Use authorizations

`autorizacoes()` accepts `nr_registro`, `cultura`, `ingrediente_ativo`, `classe`, and `situacao`. It retains every published row, including duplicates created by projecting columns; no unique key is declared for this relation.

Columns are `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `cultura`, `praga`, `praga_nome_comum`, `modalidade_de_emprego`, and the nullable addition `situacao`. Schema version is **1.1**.

```python
uses = await defensivos.autorizacoes(cultura="soja")
```

## Technical products

`tecnicos()` accepts `ingrediente_ativo`, `titular`, `classe`, `marca`, and `nr_registro`. It returns `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `grupo_quimico`, `nome_cientifico`, `classe_toxicologica`, `classe_ambiental`, and nullable `composicao_texto`, added in schema **1.1**.

The parser handles groups with nested parentheses. Multiple component names and groups retain their order, joined by ` + `; the function below provides component details. Fields absent from the export remain null. The technical export inspected does not publish situation, and this function does not accept `situacao`.

## Composition

```python
components, meta = await defensivos.composicao(
    tipo="tecnicos", nr_registro="00301", return_meta=True,
)
```

`composicao()` accepts `tipo="formulados"` (default) or `tipo="tecnicos"`, `nr_registro`, and `ingrediente_ativo`. Schema **1.0** has one row per component position in a product, keyed by `[tipo, nr_registro, ordem_componente]`.

| Column | Type / meaning |
|---|---|
| `tipo` | Text: `formulados` or `tecnicos` |
| `nr_registro` | Text identifier, retaining leading zeros |
| `ordem_componente` | `Int64`, position starting at 1 |
| `ingrediente_ativo` | Parsed ingredient name; nullable |
| `grupo_quimico` | Parsed chemical group; nullable |
| `componente_texto` | Original component text |
| `concentracao_texto` | Published concentration before parsing; nullable |
| `concentracao_valor` | `float64`, null when the concentration is not explicit, without dimensional conversion |
| `concentracao_unidade` | Published unit when separable; nullable |

Repeated ingredients at different positions retain separate rows. Composition is not multiplied by use authorizations. Explicit scientific notation can be parsed: `.001 x 10^9 UFC/mL` yields `1000000.0` and `UFC/mL`. Ambiguous expressions retain their text and null values, with diagnostics in `meta.source_details`. `Kg` remains `Kg`; `g/kg` is not inferred. Missing values do not become zero.

## Filters, cache, and provenance

Filters accept `str | None`. Empty text, numbers, booleans, invalid `tipo`, and unknown arguments raise `InvalidParameterError` before cache or network access. Registration and `organicos` use exact equality; other text filters search literal substrings, ignoring case. `situacao` uses textual equality, ignoring surrounding whitespace and case.

Situation remains the original text. The September 6, 2026 capture contained only `TRUE` for formulated products. This token is not converted into a registration-validity classification or an application recommendation.

All functions accept `use_cache=True`. The first query downloads the entire family export, even with filters; the formulated CSV was about 391 MB in the capture. The cache lasts 24 hours from acquisition and bundles related tables, composition, types, hashes, and metadata in one ZIP. Legacy files are preserved, but the current API requires the new format. `use_cache=False` skips both reading and writing, leaving any stored edition intact.

`MetaInfo` includes attempted/selected source, versions, raw hash, and `from_cache`. `fetched_at` and `fetch_timestamp` are the original UTC acquisition time, including when the query is served from cache. `source_details` includes resource, size, hash, layout fingerprint, counts, ignored columns, filters, and diagnostics. A content hash identifies received bytes; it cannot reconstruct a historical export.

Contracts are available through `get_contract("agrofit_formulados")`, `agrofit_autorizacoes`, `agrofit_tecnicos`, and `agrofit_composicao`. The [four Agrofit datasets](defensivos_datasets.en.md) reuse these contracts and preserve filters, cache, and provenance. The source functions above retain their interfaces.

## Synchronous version

```python
from agrobr.sync import defensivos

components = defensivos.composicao(tipo="tecnicos", nr_registro="00301")
```

See [source coverage and limits](../sources/defensivos.md).

## Export of 2026-09-18

The two CSVs of the 2026-09-18 export contain 4,403 formulated products, 279,707 authorization occurrences and 2,992 technical products. Two ambiguous expressions in that export, `1.9 10*10 UFC/g` and `200 1x10E10 UFC/g`, retain their text and come out with null value and unit and a diagnostic. Numeric interpretation is not guaranteed for the whole component population.

The cache retains non-null values, dtypes, component position and UTC provenance; `None` and `pd.NA` are equivalent only in nullable fields. Composition and situation text retain the literal; other fields retain the documented cleanup. Authorizations are not deduplicated.

A suffix published in the concentration fields can include an ambiguous expression and does not by itself certify a measurement unit or numeric interpretation. Catalogue, CSV and cache share an origin: none of them confirms the historical population.
