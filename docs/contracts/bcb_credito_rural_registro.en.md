# bcb.credito_rural_registro v1.0

SICOR rural credit record by record, without aggregation: the output of `bcb.credito_rural(..., agregacao="registro")` and of `datasets.credito_rural(..., agregacao="registro")`. It is the cut that the default 1.1.0 call returned (`agregacao="municipio"`, which did not aggregate), with names from BCB's official tables.

## Source

| Source | Entity | Description |
|---|---|---|
| BCB/SICOR (OData v2, Olinda) | `CusteioRegiaoUFProduto`, `InvestRegiaoUFProduto` and `ComercRegiaoUFProduto` | Count, value and, for operating costs, area by month, state, product, programme, sub-programme, funding source, insurance type, activity and modality |
| BCB/SICOR (domain tables) | `Programa.csv`, `FonteRecursos.csv`, `TipoGarantiaEmpreendimento.csv`, `Modalidade.csv` and `Atividade.csv` | Code names |

No fallback: the Base dos Dados table aggregates by municipality and carries no programme, sub-programme, funding source, insurance type, modality or activity. With OData down, the call raises `SourceUnavailableError`, with the reason in the message.

## Schema

| Column | Type | Nullable | Unit | Stable |
|---|---|---|---|---|
| `safra` | str | No | — | Yes |
| `ano_emissao` | int | No | — | Yes |
| `mes_emissao` | int | No | — | Yes |
| `produto` | str | No | — | Yes |
| `regiao` | str | Yes | — | Yes |
| `uf` | str | No | — | Yes |
| `finalidade` | str | No | — | Yes |
| `agregacao` | str | No | — | Yes |
| `programa` | str | Yes | — | Yes |
| `cd_programa` | str | Yes | — | Yes |
| `cd_sub_programa` | str | Yes | — | Yes |
| `fonte_recurso` | str | Yes | — | Yes |
| `cd_fonte_recurso` | str | Yes | — | Yes |
| `tipo_seguro` | str | Yes | — | Yes |
| `cd_tipo_seguro` | str | Yes | — | Yes |
| `modalidade` | str | Yes | — | Yes |
| `cd_modalidade` | str | Yes | — | Yes |
| `atividade` | str | Yes | — | Yes |
| `cd_atividade` | str | Yes | — | Yes |
| `qtd_contratos` | int | Yes | — | Yes |
| `valor` | float | Yes | BRL | Yes |
| `area_financiada` | float | Yes | ha | Yes |
| `fonte` | str | No | — | Yes |

**Primary key:** `[ano_emissao, mes_emissao, uf, produto, finalidade, cd_programa, cd_sub_programa, cd_fonte_recurso, cd_tipo_seguro, cd_atividade, cd_modalidade]`

**Constraints:** `ano_emissao >= 2013`, `1 <= mes_emissao <= 12`, `qtd_contratos >= 0`, `valor >= 0`, `area_financiada >= 0`

- **The key is the entities' grain.** agrobr's `$select` is the whole entity, with every field in the OData `$metadata`. The 11 dimensions do not repeat across 3,462 records: 12 queries of crop year 2024/25 by product and state, and 2 months of 2024 without a state filter (soybean operating costs in October, with 18 states, and cattle investment in March, with 25). `regiao` depends on the state and `safra` on the month, so they stay out of the key.
- **Names from BCB's domain tables:**
  - `programa`: the part of the official description before the first " - ", as in `agregacao="programa"`;
  - `tipo_seguro`: the official description;
  - `fonte_recurso`, `modalidade` and `atividade`: the whole official description. For funding sources, the part before " - " would merge distinct codes: 4 sources would become "POUPANÇA RURAL", and 3, "LETRA DE CRÉDITO DO AGRONEGÓCIO (LCA)";
  - code outside the table: `fonte_recurso`, `modalidade` and `atividade` stay null, without a guess; `programa` and `tipo_seguro` come out as `Desconhecido (<code>)`, as in the other aggregations.
- **Codes as the source publishes them.** `cd_modalidade` comes out as `"01"`, and the official table uses `"1"`: the name is resolved by the number. The purpose × activity × modality combination of the 2,508 records of the 11 queries exists in the official table.
- **Sub-programme:** code only, as in 1.1.0. The name is in BCB's `Subprograma` domain table and depends on the programme.
- **Area:** through OData, `area_financiada` is null. Operating costs publish an empty `AreaCusteio`, and investment and marketing have no area.
- **Sum:** summed by crop year, state, product and purpose, the records equal `agregacao="uf"` of the same call.
- **Current crop year:** the same warning as the other aggregations, in `MetaInfo.validation_warnings` and in `source_details["safra_em_curso"]`.
- **pandas types:** those of the empty contract (`str` → `object`, `int` → `Int64`, `float` → `Float64`), including the empty result. With `as_polars=True`: `Utf8`, `Int64` and `Float64`.
- **`MetaInfo`:** `schema_version` and `contract_version` are `1.0`, and `source_details["contract"]` is `bcb.credito_rural_registro`.

## Version history

| Version | Change |
|---|---|
| v1.0 | Initial contract (2.0.0) |

## Example

```python
from agrobr import bcb, datasets

df = await bcb.credito_rural("soja", safra="2024/25", uf="MT", agregacao="registro")
by_source = df.groupby(["mes_emissao", "fonte_recurso"])["valor"].sum()

from_dataset = await datasets.credito_rural("milho", safra="2024/25", agregacao="registro")
```

## JSON Schema

Available at `agrobr/schemas/bcb_credito_rural_registro.json`.

```python
from agrobr.contracts import get_contract

contract = get_contract("bcb_credito_rural_registro")
print(contract.to_json())
```
