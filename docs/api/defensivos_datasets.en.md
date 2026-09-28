# Agrofit datasets

Four datasets expose current Agrofit/MAPA tables through the semantic layer. They preserve the filters, values, multiplicities, and contracts of the [Defensivos source APIs](defensivos.en.md), using the single `defensivos` source. No generic product argument is required.

| Function in `agrobr.datasets` | Content and identity | Validated contract | Columns |
|---|---|---|---:|
| `defensivos_formulados` | One formulated product per textual registration | `agrofit_formulados` 1.1 | 12 |
| `defensivos_tecnicos` | One technical product per textual registration | `agrofit_tecnicos` 1.1 | 10 |
| `autorizacoes_defensivos` | Published product, crop, and pest relationships; repetitions retained | `agrofit_autorizacoes` 1.1 | 10 |
| `composicao_defensivos` | One component per family, registration, and position | `agrofit_composicao` 1.0 | 9 |

These are the existing source contracts, available under their `agrofit_*` names; the wrappers do not create duplicate schemas named after datasets. Validation also runs without metadata requested. See the [current dataset and contract index](../contracts/index.en.md).

## Queries and filters

```python
from agrobr import datasets

technical, meta = await datasets.defensivos_tecnicos(
    nr_registro="00301", return_meta=True,
)
components = await datasets.composicao_defensivos(
    tipo="tecnicos", nr_registro="00301",
)

formulated = await datasets.defensivos_formulados(nr_registro="08725")
authorizations = await datasets.autorizacoes_defensivos(nr_registro="08725")
```

| Dataset | Available filters, all keyword-only |
|---|---|
| `defensivos_formulados` | `ingrediente_ativo`, `classe_toxicologica`, `classe_ambiental`, `titular`, `organicos`, `marca`, `formulacao`, `classe`, `nr_registro`, `situacao` |
| `defensivos_tecnicos` | `ingrediente_ativo`, `titular`, `classe`, `marca`, `nr_registro` |
| `autorizacoes_defensivos` | `nr_registro`, `cultura`, `ingrediente_ativo`, `classe`, `situacao` |
| `composicao_defensivos` | `tipo="formulados"` or `"tecnicos"`, `nr_registro`, `ingrediente_ativo` |

Optional filters accept `str | None`, defaulting to `None`. The source strips surrounding whitespace from filter values. Registration and `organicos` use exact equality; `situacao` ignores case and surrounding whitespace; other filters search literal substrings without regular expressions. Registration remains textual, preserving leading zeros and Unicode. The technical dataset does not accept `situacao`.

All functions are asynchronous and accept boolean flags `use_cache=True`, `as_polars=False`, and `return_meta=False`. Non-boolean flags, invalid filters, and unknown composition types raise `InvalidParameterError` before cache/network access. Unknown parameters and positional arguments raise `TypeError` from the public signature, also before I/O.

## Cache and provenance

The first query downloads the entire family CSV, even with filters. The September 2026 capture was approximately 391 MB for formulated products and 0.7 MB for technical products; this is not a limit or a guaranteed future size. The formulated bundle serves products, authorizations, and composition; the technical bundle serves technical products and their composition. Subsequent queries can reuse the same cache for 24 hours from acquisition. `use_cache=False` skips cache reads and writes, as in the source API.

`return_meta=True` returns `(DataFrame, MetaInfo)`. The selected/attempted source is `defensivos`; `source` identifies `datasets.<name>/defensivos`, and `dataset` identifies the function. Schema/contract versions match the table, while parser version remains 3. `fetched_at` retains the original acquisition with a UTC offset; `fetch_timestamp` is the same acquisition. Reading cache does not claim a fresh acquisition.

Raw hash/size, cache key/expiry, and acquisition/parsing durations propagate through the dataset layer. They still describe the resource and source work, rather than the filtered DataFrame, a separate dataset cache, or total wrapper duration. `from_cache` reflects the actual result. Resource details, layout, counts, filters, ignored fields, and diagnostics in `source_details` are preserved in an independent copy.

`datasets.deterministic(...)` is rejected before any I/O: the CSV is a current export, and its cache retains one acquisition identified by content. Neither can reconstruct an arbitrary historical register. These datasets return `snapshot=None`.

## Identity and types

Text columns, including registrations and original composition, retain source types. `ordem_componente` uses `Int64`; `concentracao_valor` uses nullable `Float64`. Empty results retain every column and its type, including optional contract additions. See the [complete column description](defensivos.en.md#composition).

Repeated ingredients remain at separate positions. Authorizations are not assigned an artificial key or deduplicated after projection. Components and authorizations are not joined automatically. Literal units, ambiguous text, and null values retain source semantics; `TRUE` is not converted into a validity classification or an application recommendation.

## Sync, Polars, and discovery

```python
from agrobr.sync import datasets

components = datasets.composicao_defensivos(
    tipo="tecnicos", nr_registro="00301", as_polars=True,
)
print(datasets.list_datasets())
print(datasets.info("composicao_defensivos"))
```

Polars requires its optional dependency and is applied after pandas validation. The single source has no fallback to another register. Companies, labels, cancellation history, and the official interpretation of situation remain outside these tables; see [coverage and limits](../sources/defensivos.en.md) and the [source license](../licenses.en.md#defensivos-agrofit).

## Reconciliation of the 2026-09-18 capture

The two complete CSVs in this capture contain 4,403 formulated products, 279,707 authorization occurrences and 2,992 technical products. An independent oracle checks all 12 formulated-product columns, all ten columns of every authorization and the eight direct technical-product fields. All ten technical columns, including ingredient and group extracted from composition, are checked in nine explicit complete-register cohorts.

Composition has independent reconciliation of 57 components in 32 complete product cohorts: nine technical and 23 formulated. Coverage includes file endpoints, leading zeros, an accented premix identifier, nested parentheses, repeated components, zero concentration, scientific notation and published units. Two real ambiguous expressions, `1.9 10*10 UFC/g` and `200 1x10E10 UFC/g`, retain text, null numeric value/unit and diagnostics. The complete component population has not received independent numeric interpretation; the validated scope is explicit in the manifest.

Replays use complete CSV bodies with identical hashes after gzip decompression, through public source and dataset APIs. Cache retains non-null values, dtypes, component position and UTC provenance; pandas `None`/`pd.NA` sentinels are equivalent only in nullable fields. Composition and situation text retain the literal; other fields retain the documented cleanup. Authorizations are not deduplicated.

The structural comparator inventories all columns, both CKAN resources and published suffixes in concentration fields. A suffix can include an ambiguous expression and does not by itself certify a measurement unit or numeric interpretation. Undecided formats, columns, resources or expressions require review. Catalogue, CSV and cache share an origin and do not independently confirm the historical population. Parser 3 and contracts 1.1/1.0 remain unchanged.
