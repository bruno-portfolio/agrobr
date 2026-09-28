# custo_producao v3.0

CONAB production costs, preserving the selected workbook, sheet, price reference and every published item, subtotal and total. The active contract is `CONAB_CUSTOS_V3` in `agrobr.contracts.conab_custos`.

Select an unambiguous workbook and sheet with `planilha` and `aba`; broad selections list candidates instead of choosing silently. `tecnologia` is an output qualification only, not a parameter.

## Products

The dataset accepts 8 products:

| Code | Product |
|---|---|
| `soja` | Soybean |
| `milho` | Corn |
| `arroz` | Rice |
| `feijao` | Beans |
| `trigo` | Wheat |
| `algodao` | Cotton |
| `cafe_arabica` | Arabica coffee |
| `cafe_conilon` | Conilon coffee |

Corn, rice and beans each have two workbooks: first/second crop corn,
irrigated/upland rice, and first/second+third crop beans. Call
`conab.catalogo_custos(cultura)` and pass `planilha=`; then call
`conab.catalogo_custos(cultura, planilha=...)` to choose a recognized `aba=`.
The filters must identify a single context.

For coffee, pass `cafe_arabica` or `cafe_conilon`. The generic `cafe` request
lists these alternatives. The source catalog also includes other agricultural
crops beyond the dataset's eight products.

## Cache

Catalog cache: **1 hour in process**. The keyword-only parameter `use_cache=False` in `conab.catalogo_custos`, `conab.custo_producao`, and `datasets.custo_producao` forces a fresh read without consulting or updating the cache. Workbooks are downloaded for every query. `meta.source_details["catalog_cache"]` reports `hit`, `miss`, or `bypass`; original catalog receipts remain in `manifest.acquisition.catalog_acquisition`, separate from the current call's requests.

## Workbook reading and limits

Parser 5 reads agricultural XLS/BIFF and XLSX costs with merged headers.
Each header span accepts at most one value per row; collisions, measures
outside recognized columns and Excel errors cause an explicit rejection.
Monetary units retain the published basis without tonne, sack or hectare conversion.
Old wheat sheets with three monetary measures remain unsupported: contract
3.0 represents two measures and never discards a third column.

Arabica/conilon coffee is identified by the official resource even when the
system prints only `CAFÉ`. `safra` retains a single year or the published
two-year token. Text price references such as `13/12/2013` remain text;
`data_referencia` is null unless the source cell is an Excel date.

Numeric cells with an Excel `%` format are multiplied by 100. Numbers already
expressed as percentages and textual numbers such as `100,00` are not scaled.
Null items and zeros remain distinct. CV, CT and other totals are published
rows without re-summing subtotals. Repeated headers, pagination, attribution
and exchange-rate notes remain in diagnostics, outside cost observations.
The contract remains 3.0.

### Subtotals, groups and memo blocks

Since parser 5, the items of each section add up to the published subtotal whenever the workbook itself does:

- a Roman section header (`I -` to `VI -`) opens the section even when the workbook prints zeros on it; the zeros stay in
  the diagnostics;
- a cost row after the subtotal or total of the same section, such as the "Gestão da propriedade familiar" block after H or
  I, leaves the observations and stays in the diagnostics as `memo_after_total`;
- each section takes the reading that closes with the published subtotal. A group `N - …` with sub-items `N.M - …`
  (including `' N.M`) enters with the sub-items only, the group only or both, and the "Gestão da propriedade familiar"
  aggregate enters or leaves. The row that leaves stays in the diagnostics (`group_header`, `group_component` or
  `aggregate_row`) with its published value, and `meta.source_details["parser"]["subtotal_checks"]` records the reading of
  each section;
- when no reading closes, every published row stays and a warning is issued (`UserWarning` and
  `meta.validation_warnings`) with the published subtotal, the sum of the items and the difference. The same applies to
  formula totals such as `(E+F = G)`. agrobr passes the published numbers through, without recomputing.

Across the 11 coffee, corn, cotton, soybean, rice, bean and wheat series, 45 subtotals or totals do
not close in the workbook itself, for example Barreiras-BA-2011 (cotton, B), Patrocínio-MG-2022 (arabica coffee, E) and
S. Mateus do Sul-PR-2008 (beans, G and H).

### Category

`categoria` comes from the published section: `IV - DEPRECIAÇÕES` and `V - OUTROS CUSTOS FIXOS` give `custos_fixos`;
`II`, `III` and `VI` give `outros`. In the operating costs (`I`), the label decides through the map, with accents, hyphens
and plurals normalized. So "Mão-de-obra temporária c/encargos", "Mão-de-obra" and "Mão de obra" give `mao_de_obra`;
"Defensivos", "Agrotóxicos", "Mudas de Café", "Sementes e mudas", "Semente de arroz" and "Royalties" give `insumos`;
"Tratores e Colheitadeiras" and "Máquinas Próprias" give `operacoes`. Total rows (`tipo_linha = "total"`) do not inherit
the section. An operating-cost label outside the map stays `outros` in every year.

Irrigation changes category when the layout changes. In the old spreadsheet, it is inside the single line "Operação com
máquinas próprias" (sugarcane, S. M. dos Campos-AL-2017), which gives `operacoes`; in the new layout, it is the subitem
"Conjunto de Irrigação", which gives `outros`. So the `operacoes` series changes content when the layout changes.

### Identified and rejected sheets

In the same 11 series, 116 sheets have a recognized context and a rejected body, with no partial frame:

| Series | Sheets | Reason and sheets |
|---|---:|---|
| upland rice | 40 | unrecognized `kg/sc 60 kg` header: Inhumas-GO, Itapuranga-GO, Palmeiras-GO and Piranhas-GO 2007–2014; Bacabal-MA 2007 and 2009–2014; Esperantina-PI-2007 (`kg/sc 50 kg`) |
| beans, 1st crop | 28 | `kg/sc 60 kg` header: Brejo Santo-CE and Crateús-CE 2007–2014, Icó-CE 2012–2014; invalid measure (`#REF!` or `.`): Estrela-RS 2008–2014, Canoinhas-SC-2013; measure without description: Unaí-MG-2009 |
| wheat | 16 | duplicated cost-per-unit column: Toledo-PR 2002–2004, Ubiratã-PR 2005–2007, Londrina-PR 2002–2007, Cascavel-PR 2004–2007 |
| soybean | 9 | measure in a column without header: OGM-Toledo-PR-2008, Cruz Alta-RS 2008–2010; invalid measure (`#REF!`): 1-Ijuí-RS 2007–2010; label outside the columns: Sorriso-MT-2014 |
| corn, 1st crop | 8 | measure in a column without header: Rio Verde-GO 2007–2009, Passo Fundo-RS 2008–2010; invalid measure (`#REF!`): Rio Verde-GO-2001; measure without description: Passo Fundo-RS-2012 |
| arabica coffee | 7 | measure in a column without header: Patrocínio-MG 2006–2008, Franca-SP 2006–2008, Três Pontas-MG-2022 |
| irrigated rice | 7 | measure in a column without header: Cachoeira do Sul-RS 2009–2010; invalid measure (`#REF!` or `.`): Camaquã-RS 2014–2016, Massaranduba-SC-2013, Meleiro-SC-2013 |
| conilon coffee | 1 | measure in a column without header: Ji-Paraná-RO-2014 |

Corn 2nd crop, cotton and beans 2nd/3rd crops have no rejected sheet in these series. The unrecognized
`kg/sc 60 kg` yield header accounts for 59 of the rejections.

The list covers only these 11 series. Other crops have rejections for the same reasons, for example
C. de Camaragibe-AL 2014–2016 and S. L. do Quitunde-AL-2017 for sugarcane (measure in a column without header) and Cruz das
Almas-BA 2008 and 2010–2013 for cassava (unrecognized `R$t` header).

## Schema

| Column | Type | Nullable | Unit |
|---|---|---|---|
| `cultura` | str | No | — |
| `uf` | str | No | — |
| `safra` | str | Yes | — |
| `tecnologia` | str | Yes | — |
| `categoria` | str | No | — |
| `item` | str | No | — |
| `unidade` | str | No | — |
| `quantidade_ha` | float | Yes | — |
| `preco_unitario` | float | Yes | — |
| `valor_ha` | float | Yes | BRL/ha |
| `participacao_pct` | float | Yes | % |
| `local` | str | No | — |
| `ano_referencia` | int | No | — |
| `referencia` | str | No | — |
| `data_referencia` | datetime | Yes | — |
| `planilha` | str | No | — |
| `aba` | str | No | — |
| `sistema` | str | No | — |
| `linha` | int | No | — |
| `tipo_linha` | str | No | — |
| `secao` | str | Yes | — |
| `unidade_produto` | str | Yes | — |
| `valor_unidade_produto` | float | Yes | — |
| `participacao_cv_pct` | float | Yes | % CV |
| `participacao_ct_pct` | float | Yes | % CT |

## Semantics and provenance

No primary key is asserted. The output preserves source occurrences and physical row numbers; do not deduplicate by crop/state/season/item.

`safra` is a nullable published token, including unusual spelling, and is not inferred from `ano_referencia`. `referencia` preserves the price reference; `data_referencia` is populated only when the workbook supplies a date. `local` is not necessarily an IBGE municipality.

`valor_ha` preserves zero, negative revenue and missing amounts. `quantidade_ha` and `preco_unitario` remain null when not published; cost per production unit belongs in `valor_unidade_produto`. Read the complete literal header in `unidade_produto`, for example `CUSTO /  60 kg`, `R$/1 kg` or `(R$/t)`, without assuming equivalent units. `participacao_cv_pct` and `participacao_ct_pct` preserve distinct CV/CT bases; CV is not COE.

`tipo_linha` distinguishes `item`, `subtotal` and `total`; summing all rows double-counts components. `linha` is the physical sheet row, starting at 1. Metadata retains workbook/sheet selection, hashes, acquisition time and parsing diagnostics.


When `LOCAL:` does not publish a state, the reader uses the state in a sheet name
matching `Local-UF-Year`. The locality still comes from the cell, removing any
descriptive parenthesis. Selection metadata records
`celulas_contexto["uf_origem"] = "nome_da_aba"`; the `local` coordinate still
points to the cell. Reference dates and crop seasons are never inferred from the name.

### Corn context limitations

In `milho_1a_safra_serie_historica_1997-2025.xls`,
246 contexts are identified. `P. do Leste-MT-1997` and
`Campo Mourão-PR-1997` still lack complete context. `Balsas-MA-2013`
retains its published regional location; `Unaí-MG-2005` and `Unaí-MG-2006`
retain the literal system `MILHO1 - PLANTIO DIRETO (100%)`.

Identifying context does not validate the entire body. `Rio Verde-GO-2007`,
`Rio Verde-GO-2008` and `Rio Verde-GO-2009` still publish zero in E29
without a recognized measure header and remain rejected.

## Example

```python
from agrobr import contracts, datasets

df, meta = await datasets.custo_producao(
    "soja", uf="BA",
    planilha="serie-historica-custos-soja-1997-a-2025.xls",
    aba="Barreiras-BA-2025", return_meta=True,
)
contracts.validate_dataset(df, "custo_producao")
```

`agrobr/schemas/custo_producao.json` · `get_contract("custo_producao")`.

See [CONAB API](../api/conab.md), [migration](../guides/migracao-2.md) and [data licence](../licenses.md).
