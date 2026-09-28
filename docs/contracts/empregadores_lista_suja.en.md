# empregadores_lista_suja v2.0

Current employer registry published by Brazil's Ministry of Labour.

Source: **MTE**. Contract registry key: `lista_suja_empregadores`.

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `empregador` | str | No | — |
| `cpf_cnpj` | str | No | — |
| `estabelecimento` | str | Yes | — |
| `uf` | str | Yes | — |
| `cnae` | str | Yes | — |
| `data_inclusao` | date | Yes | — |
| `trabalhadores_resgatados` | int | Yes | — |
| `ano_acao_fiscal` | int | Yes | — |
| `id_registro` | str | No | — |
| `data_decisao` | date | Yes | — |
| `data_atualizacao` | date | Yes | — |
| `data_inclusao_texto` | str | No | — |

**Primary key:** `id_registro`

All stable columns must exist, including nullable columns and empty results. Breaking schema changes require a major contract version.

## Semantics and provenance

`id_registro` identifies a row only within one publication, tied to its resource hash. Repeated CPF/CNPJ values are retained. Compound inclusion dates preserve their complete text without selecting an arbitrary date. CSV is the primary route; PDF is an explicit alternative or an eligible fallback. The full resource is acquired before filtering.

`return_meta=True` returns data and `MetaInfo`, including attempted/selected sources, acquisition time, contract version and source diagnostics. These current publications do not support selecting a historical revision through `deterministic`.

## Parameters

| Parameter | Type | Default |
|---|---|---|
| `uf` | `str \| None` | `None` |
| `id_registro` | `str \| None` | `None` |
| `formato` | `str` | `'auto'` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.empregadores_lista_suja(uf="MT", formato="csv", return_meta=True)
contracts.validate_dataset(df, "lista_suja_empregadores")
```

Use `from agrobr.sync import datasets` for synchronous calls, without `await`. `as_polars=True` requires `pip install agrobr[polars]`.

## JSON schema and licence

`agrobr/schemas/lista_suja_empregadores.json` · `get_contract("lista_suja_empregadores")`.

`livre` — see [data licences](../licenses.md) and the [API/source details](../api/empregadores_lista_suja.md).

On 2026-09-18, the publication had 579 records in CSV/TXT and PDF. Each format retains its own text, including line breaks. Both source and dataset APIs preserve `lista_suja_csv` or `lista_suja_pdf` in attempted/selected sources. Edition date, registry update and acquisition instant remain distinct; see the [source](../sources/lista_suja.en.md).

PDF `data_inclusao_texto` preserves cell line breaks; other text fields use the whitespace normalization documented for the source.
