# custo_sociobiodiversidade v1.0

CONAB extraction costs with their published values and units. Active contract: `CONAB_SOCIOBIO_V1`. No hectare/crop/product-unit conversion. Filter by `unidade_valor` before comparing values. This dataset uses its own catalogue and parser; agricultural costs remain in `custo_producao`.

**Primary key:** none. Published sections, items and totals are retained.

## Products

20 products, as listed in the official tab:

| Code | Product |
|---|---|
| `acai` | Açaí |
| `andiroba` | Andiroba (amêndoa) |
| `babacu` | Babaçu (amêndoa) |
| `baru` | Baru (amêndoa) |
| `borracha` | Borracha |
| `buriti` | Buriti (fruto) |
| `cacau` | Cacau |
| `carnauba` | Carnaúba |
| `castanha_do_brasil` | Castanha-do-brasil |
| `fava_danta` | Fava d'anta |
| `jucara` | Juçara (fruto) |
| `licuri` | Licuri |
| `macauba` | Macaúba |
| `mangaba` | Mangaba |
| `murumuru` | Murumuru |
| `pequi` | Pequi |
| `piacava` | Piaçava |
| `pirarucu` | Pirarucu |
| `pinhao` | Pinhão de Araucária |
| `umbu` | Umbu |

## Selection and coverage

`conab.catalogo_sociobiodiversidade()` lists all 37 captured resources and flags the 20 active links with `ativo`. With a product, it inventories the resource linked by the official tab. `planilha=` selects an exact archived revision. Multiple active resources for one product fail explicitly; revisions are never merged.

`uf`, `ano`, `local` and `aba` filter printed contexts. All matching identified sheets are returned in workbook order. If an unresolved sheet could match the filters, the query fails; use an exact `aba` to select a known context. `ano` is the first year of the published crop year. `safra_publicada` retains the literal token with a slash (`2018/19`, `2016/2017`, including `2018/18`); it is null when only one year is printed. The filename and price date never fill a missing crop year. Catalogue inventory does not validate all body cells.

Locations expressed as regions or without a recognized terminal state abbreviation retain their printed text, excluding only the `REGIÃO:` prefix and surrounding whitespace. Descriptive parentheses remain in `local`, including any state text within that description. The state then comes from the worksheet name and `celulas_contexto["uf_origem"] = "nome_da_aba"` records that fallback. A recognized state in the header takes precedence. Neither the filename nor the price date supplies a missing crop year.

## Revisions and percentages

Parser 2 distinguishes numeric cells with an Excel `%` format (multiplied by 100)
from textual numbers or values already expressed as percentages (not scaled).
Monetary headers retain their published unit with whitespace normalization only.
The contract remains 1.0. Three monetary measures, extra unheaded columns,
Excel errors and missing crop years are rejected without partial output.

`planilha="acai_serie_historica_2008-2024.xlsx"` selects the archived revision,
including sheet `Codajás-AM-2008` with `ano=2008`. Without `planilha`, the
official link selects the active revision; an old year does not automatically
switch to an archived workbook. In the current files for all 20 products and in
this açaí revision, old and new layouts have two monetary columns; none publishes
a single column. For the other 16 catalogued archived revisions, the reading is not guaranteed.

## Schema

| Column | Type | Nullable | Description |
|---|---|---|---|
| `produto` | str | No | Canonical product identified in the official catalogue. |
| `local` | str | No | Published location from the sheet context. |
| `uf` | str | No | Published state; fallback to the worksheet name is recorded in provenance. |
| `ano` | int | No | First year of the crop year printed in the header. |
| `safra_publicada` | str | Yes | Literal crop-year token with a slash; null for a single printed year. |
| `sistema` | str | No | Published production or extraction system. |
| `tipo_relatorio` | str | Yes | Published report type, null when absent. |
| `mes_ano_referencia` | str | Yes | Published reference text without an invented day. |
| `data_precos` | datetime | Yes | Price date from an Excel date cell; null for textual references. |
| `produtividade` | float | Yes | Published productivity without unit conversion. |
| `unidade_produtividade` | str | Yes | Literal productivity unit, unrestricted text. |
| `secao` | str | Yes | Published section heading, including family property management. |
| `item` | str | No | Literal row label, including original whitespace and signs. |
| `tipo_linha` | str | No | item, total or secao; summing all row types double counts components. |
| `linha` | int | No | Physical worksheet row, starting at 1. |
| `valor` | float | Yes | First monetary value in its published basis. |
| `unidade_valor` | str | No | Literal first monetary header with collapsed whitespace; filter by this unit. |
| `valor_unidade_produto` | float | Yes | Second monetary value in its published unit, when present. |
| `unidade_produto` | str | Yes | Literal second monetary header with collapsed whitespace. |
| `participacao_pct` | float | Yes | Published participation: CV in modern layouts, generic published basis in old layouts. |
| `participacao_ct_pct` | float | Yes | Published participation in total cost, when present. |
| `planilha` | str | No | Exact resource identifier in the catalogue. |
| `aba` | str | No | Literal source worksheet name. |

Pandas uses `string[python]`, nullable `Int64`, `float64` and `datetime64[ns]`, in the order above. Excel percentages are converted to percentage points only when the cell format is a percentage. Zeros, negative values and nulls are retained. Sections and totals are not recomputed.

## Observed units

All monetary headers below come from the 20 active workbooks, including both value columns. These are observed labels, not an allowed-values list.

| Product | Published monetary headers |
|---|---|
| acai | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| andiroba | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| babacu | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/safra` |
| baru | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/dia`; `R$/safra` |
| borracha | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/kg`; `R$/safra` |
| buriti | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/kg`; `R$/safra` |
| cacau | `CUSTO / kg`; `CUSTO / kg/ha`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/safra` |
| carnauba | `(R$/60 kg)`; `(R$/70 kg)`; `(R$/80 kg)`; `(R$/safra ano)`; `CUSTO / 15 kg`; `CUSTO / kg`; `CUSTO POR HA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/kg`; `R$/safra` |
| castanha_do_brasil | `CUSTO / 10 kg`; `CUSTO / hl`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/1 lata`; `R$/Safra`; `R$/ha`; `R$/safra`; `R$/safra/família (1 pessoa)` |
| fava_danta | `CUSTO / kg`; `CUSTO POR HA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra/pessoa` |
| jucara | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| licuri | `CUSTO / kg`; `CUSTO POR HA`; `R$/1 kg`; `R$/safra` |
| macauba | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| mangaba | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra`; `R$/safra ano` |
| murumuru | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/safra` |
| pequi | `CUSTO / 25 kg`; `CUSTO / 28 kg`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/1kg`; `R$/25 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| piacava | `CUSTO / 15 kg`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/15 @`; `R$/KG`; `R$/Safra`; `R$/extrativista/safra-ano`; `R$/ha`; `R$/safra`; `R$/safra ano` |
| pirarucu | `CUSTO / kg`; `CUSTO POR HA`; `R$/1 kg`; `R$/Safra/ano`; `R$/ano`; `R$/safra` |
| pinhao | `CUSTO / 15 kg`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/50 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| umbu | `CUSTO / 1 kg`; `CUSTO / 25 kg`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/25 kg`; `R$/Safra`; `R$/ha`; `R$/safra`; `R$/safra ano` |

Productivity also retains literal units, such as `kg`, `kg/ha`, `kg/safra`, `kg/safra ano` and `kg/10 milheiros palha`. No equivalence between these units is assumed.

## Unresolved worksheets

Nominal inventory from the captured active resources. `unresolved` means context/header identification failed; `parse` means context was identified but body cells could not be represented safely. The catalogue includes the full reason for unresolved contexts.

| Product | Stage | Worksheets | Exact error reason |
|---|---|---|---|
| acai | parse | `Igarapé-Miri-PA-2008`; `Igarapé-Miri-PA-2009` | `Medida sem descrição na linha 49` |
| baru | unresolved | `Amêndoa-Iporá-GO-2010`; `Amêndoa-Iporá-GO-2011`; `Amêndoa-Pirenópolis-GO-2010`; `Amêndoa-Pirenópolis-GO-2011`; `Fruto-Pirenópolis-GO-2010`; `Fruto-Pirenópolis-GO-2011` | `Safra/local não reconhecidos: 'SAFRA'` |
| borracha | unresolved | `Sena Madureira-AC-2016` | `Título de custo ausente ou ambíguo` |
| castanha_do_brasil | parse | `Sena Madureira-AC-2009`; `Sena Madureira-AC-2010`; `Sena Madureira-AC-2011`; `Sena Madureira-AC-2012`; `Sena Madureira-AC-2013`; `Sena Madureira-AC-2014`; `Sena Madureira-AC-2015` | `Medida fora dos cabeçalhos mapeados em R22C5: 0` |
| castanha_do_brasil | parse | `Sena Madureira-AC-2016` | `Medida fora dos cabeçalhos mapeados em R13C5: 0` |
| piacava | unresolved | `Belmonte-BA-2011`; `Belmonte-BA-2012`; `Belmonte-BA-2013`; `Belmonte-BA-2014`; `Belmonte-BA-2015`; `Belmonte-BA-2016`; `Cairu-BA-2011`; `Cairu-BA-2012`; `Cairu-BA-2013`; `Cairu-BA-2014`; `Cairu-BA-2015`; `Cairu-BA-2016` | `Cabeçalhos monetários não reconhecidos: 3` |
| pirarucu | parse | `Carauari-AM-2022` | `Medida fora dos cabeçalhos mapeados em R34C5: 0` |
| pinhao | unresolved | `São Joaquim-SC-2015`; `São Joaquim-SC-2016` | `Coluna sem mapeamento em R8C4: '1 kg'` |
| umbu | parse | `Uauá-BA-2011` | `Medida sem descrição na linha 55` |

Orphan numbers are rejected even when zero. Pinhão São Joaquim 2015/2016 publishes an additional `1 kg` value column beyond the two supported monetary columns: both are `unresolved` with `Coluna sem mapeamento em R8C4`, a representation limit, not a rejection of their monetary basis. The extra column is neither discarded nor converted.


Inventory: **955 identified contexts/headers and 21 nominal unresolved worksheets** among 976 data sheets. Six baru sheets print only `SAFRA`, without a crop year; one rubber sheet has an unrecognized cost title; two pinhão sheets retain a known limitation. The other 12 are piaçava Belmonte/Cairu 2011–2016: the geographic/crop context is recognized, but three published monetary columns exceed the two-column representation. For example, Belmonte 2016 prints `R$/Safra`, `R$/15 @` and `R$/KG`. None is discarded or converted. The 12 body parse rejections remain separate.

## Worksheets whose names differ from their headers

The printed header determines `local`, `uf` and `ano`; `aba` retains the literal worksheet name. `celulas_contexto["conflito_rotulo"]` records `nome da aba: …` when the name differs. The table includes spelling, abbreviation and scope differences; a difference alone does not establish a factual error. In Juçara 2025 the two geographic labels are crossed: values are preserved with the printed context, without asserting which attribution is factually correct.

| Product | Worksheet name | Published location / state / year |
|---|---|---|
| acai | `Abaetuba-PA-2016` | Abaetetuba / PA / 2016 |
| acai | `Abaetuba-PA-2017` | ABAETETUBA / PA / 2017 |
| acai | `Abaetuba-PA-2018` | Abaetetuba / PA / 2018 |
| acai | `Abaetuba-PA-2019` | Abaetetuba / PA / 2019 |
| acai | `Abaetuba-PA-2020` | Abaetetuba / PA / 2020 |
| acai | `Abaetuba-PA-2021` | Abaetetuba / PA / 2021 |
| acai | `Abaetuba-PA-2022` | Abaetetuba / PA / 2022 |
| acai | `Cametá-PA-2009` | Cametá / PA / 2008 |
| acai | `Igarapé-Miri-PA-2015` | Igarapé - Miri / PA / 2015 |
| acai | `Igarapé-Miri-PA-2016` | Igarapé - Miri / PA / 2016 |
| andiroba | `Santarém (F. Tapajós) -PA-2018` | Santarém / PA / 2018 |
| andiroba | `Santarém (F. Tapajós) -PA-2019` | Santarém / PA / 2019 |
| andiroba | `Santarém (F. Tapajós) -PA-2020` | Santarém / PA / 2020 |
| andiroba | `Santarém (F. Tapajós) -PA-2021` | Santarém / PA / 2021 |
| babacu | `Vargem Grande-MA-2010` | Vargem Grande / MA / 2008 |
| babacu | `S. Miguel do TO-TO-2011` | São Miguel / TO / 2011 |
| babacu | `S. Miguel do TO-TO-2012` | São Miguel / TO / 2012 |
| babacu | `S. Miguel do TO-TO-2013` | São Miguel / TO / 2013 |
| babacu | `S. Miguel do TO-TO-2014` | São Miguel / TO / 2014 |
| babacu | `S. Miguel do TO-TO-2015` | São Miguel / TO / 2015 |
| babacu | `S. Miguel do TO-TO-2016` | SÃO MIGUEL DO TOCANTINS / TO / 2016 |
| babacu | `S. Miguel do TO-TO-2018` | São Miguel do Tocantins / TO / 2018 |
| baru | `Amêndoa-B. Jardim de GO-GO-2018` | BOM JARDIM DE GOIÁS / GO / 2018 |
| baru | `Amêndoa-B. Jardim de GO-GO-2019` | Bom Jardim de Goiás / GO / 2019 |
| baru | `Amêndoa-B. Jardim de GO-GO-2020` | Bom Jardim de Goiás / GO / 2020 |
| baru | `Amêndoa-B. Jardim de GO-GO-2021` | Bom Jardim de Goiás / GO / 2021 |
| baru | `Amêndoa-B. Jardim de GO-GO-2022` | Bom Jardim de Goiás / GO / 2022 |
| baru | `Amêndoa-B. Jardim de GO-GO-2023` | Bom Jardim de Goiás / GO / 2023 |
| baru | `Amêndoa-B. Jardim de GO-GO-2024` | Bom Jardim de Goiás / GO / 2024 |
| baru | `Amêndoa-B. Jardim de GO-GO-2025` | Bom Jardim de Goiás / GO / 2025 |
| baru | `Amêndoa-Poconé-MT-2010` | Poconé/S.Sra.do Livramento / MT / 2010 |
| baru | `Amêndoa-Poconé-MT-2011` | Poconé/S.Sra.do Livramento / MT / 2011 |
| baru | `Amêndoa-Poconé-MT-2012` | Poconé/S.Sra.do Livramento / MT / 2012 |
| baru | `Amêndoa-Poconé-MT-2013` | Poconé/S.Sra.do Livramento / MT / 2013 |
| baru | `Amêndoa-Poconé-MT-2014` | Poconé/S.Sra.do Livramento / MT / 2014 |
| baru | `Amêndoa-Poconé-MT-2015` | Poconé/S.Sra.do Livramento / MT / 2015 |
| baru | `Amêndoa-Poconé-MT-2016` | Poconé/S.Sra.do Livramento / MT / 2016 |
| buriti | `Fruto-Buritizeiro-MG-2015` | Burutizeiro / MG / 2015 |
| buriti | `Fruto-Buritizeiro-MG-2016` | Burutizeiro / MG / 2016 |
| buriti | `Fruto-Buritizeiro-MG-2017` | Burutizeiro / MG / 2017 |
| buriti | `Polpa-Buritizeiro-MG-2013` | Buritizeiro / MG / 2012 |
| buriti | `Polpa-Buritizeiro-MG-2015` | Burutizeiro / MG / 2015 |
| buriti | `Polpa-Buritizeiro-MG-2016` | Burutizeiro / MG / 2016 |
| buriti | `Polpa-Buritizeiro-MG-2017` | Burutizeiro / MG / 2017 |
| buriti | `Fruto-Igarapé-Miri-PA-2015` | Igarapé - Miri / PA / 2015 |
| buriti | `Fruto-Igarapé-Miri-PA-2016` | Igarapé - Miri / PA / 2016 |
| buriti | `Fruto-Igarapé-Miri-PA-2017` | Igarapé - Miri / PA / 2017 |
| buriti | `Fruto-Iagarapé-Miri-PA-2019` | Igarapé-Miri / PA / 2019 |
| carnauba | `Pó Cerífero-Campo Maior-PI-2016` | CAMPO MAIOR PI / PI / 2016 |
| carnauba | `Pó Cerífero-Piripiri-PI-2016` | PIRIPIRI PI / PI / 2016 |
| carnauba | `Cera-Açu-RN-2015` | ASSÚ / RN / 2015 |
| carnauba | `Cera-Açu-RN-2016` | ASSÚ / RN / 2016 |
| carnauba | `Pó-Cerífero-Açu-RN-2014` | ASSÚ / RN / 2014 |
| carnauba | `Pó-Cerífero-Açu-RN-2015` | ASSÚ / RN / 2015 |
| carnauba | `Pó-Cerífero-Açu-RN-2016` | ASSÚ / RN / 2016 |
| carnauba | `Pó Cerífero-Mossoró-RN-2009` | MOSSORÓ/APODI / RN / 2009 |
| carnauba | `Pó Cerífero-Mossoró-RN-2010` | MOSSORÓ/APODI / RN / 2010 |
| carnauba | `Pó Cerífero-Mossoró-RN-2011` | MOSSORÓ/APODI / RN / 2011 |
| carnauba | `Pó Cerífero-Mossoró-RN-2012` | MOSSORÓ/APODI / RN / 2012 |
| carnauba | `Pó Cerífero-Mossoró-RN-2013` | MOSSORÓ/APODI / RN / 2013 |
| jucara | `Fruto-Três Cachoeiras-RS-2025` | Ubatuba / SP / 2025 |
| jucara | `Fruto-Ubatuba-SP-2023` | TRÊS CACHOEIRAS / RS / 2023 |
| jucara | `Fruto-Ubatuba-SP-2025` | Três Cachoeiras / RS / 2025 |
| macauba | `Mirabela-MG_2014` | MIRABELA / MG / 2013 |
| macauba | `Corumbá-MS-2014` | CORUMBÁ / MS / 2013 |
| mangaba | `Barra dos Coqueiros-SE-2010` | Barra do Coqueiros / SE / 2010 |
| mangaba | `Barra dos Coqueiros-SE-2011` | Barra do Coqueiros / SE / 2011 |
| mangaba | `Barra dos Coqueiros-SE-2012` | Barra do Coqueiros / SE / 2012 |
| mangaba | `Barra dos Coqueiros-SE-2013` | Barra do Coqueiros / SE / 2013 |
| mangaba | `Barra dos Coqueiros-SE-2014` | Barra do Coqueiros / SE / 2014 |
| murumuru | `Carauari-AM-2018` | CARAUARI-AM (Comunidade do Roque) / AM / 2018 |
| pequi | `Crato-CE-2008` | Crato - CE (Distrito Horizonte(Cacimba) - Municipio : Jardim / CE / 2008 |
| pequi | `Crato-CE-2010` | Crato - CE (Distrito Horizonte(Cacimba) - Municipio : Jardim / CE / 2010 |
| pequi | `Crato-CE-2011` | Crato - CE (Distrito Horizonte(Cacimba) - Municipio : Jardim / CE / 2011 |
| pequi | `Crato-CE-2012` | Jardim/Crato / CE / 2012 |
| pequi | `Crato-CE-2013` | Jardim/Crato / CE / 2013 |
| pequi | `Crato-CE-2014` | Jardim/Crato / CE / 2014 |
| pequi | `Crato-CE-2015` | Jardim/Crato / CE / 2015 |
| pequi | `Janpovar-MG-2008` | Japonvar / MG / 2008 |
| pequi | `Janpovar-MG-2010` | Japonvar / MG / 2010 |
| pequi | `Janpovar-MG-2011` | Japonvar / MG / 2011 |
| pequi | `Janpovar-MG-2012` | Japonvar / MG / 2012 |
| pequi | `Janpovar-MG-2013` | Japonvar / MG / 2013 |
| pequi | `Janpovar-MG-2014` | Japonvar / MG / 2014 |
| pequi | `Janpovar-MG-2015` | Japonvar / MG / 2015 |
| pequi | `Janpovar-MG-2016` | Japonvar / MG / 2016 |
| pequi | `Janpovar-MG-2017` | JAPONVAR / MG / 2017 |
| pequi | `Janpovar-MG-2018` | Japonvar / MG / 2018 |
| pequi | `Janpovar-MG-2019` | Japonvar / MG / 2019 |
| pequi | `Janpovar-MG-2020` | Japonvar / MG / 2020 |
| pequi | `Janpovar-MG-2021` | Japonvar / MG / 2021 |
| pequi | `Janpovar-MG-2022` | Japonvar / MG / 2022 |
| pequi | `Janpovar-MG-2023` | Japonvar / MG / 2023 |
| pequi | `Janpovar-MG-2024` | Japonvar / MG / 2024 |
| pequi | `Janpovar-MG-2025` | Japonvar / MG / 2025 |
| pequi | `Poconé-MT-2010` | Poconé\Ns.Sra.Livramento / MT / 2010 |
| pequi | `Poconé-MT-2011` | Poconé\Ns.Sra.Livramento / MT / 2011 |
| pequi | `Poconé-MT-2012` | Poconé\Ns.Sra.Livramento / MT / 2012 |
| pequi | `Poconé-MT-2013` | Poconé\Ns.Sra.Livramento / MT / 2013 |
| pequi | `Poconé-MT-2014` | Poconé\Ns.Sra.Livramento / MT / 2014 |
| pequi | `Poconé-MT-2015` | Poconé\Ns.Sra.Livramento / MT / 2015 |
| pequi | `Poconé-MT-2016` | Poconé\Ns.Sra.Livramento / MT / 2016 |
| pequi | `N. S. do Livramento-MT-2019` | Nossa Senhora do Livramento / MT / 2019 |
| pequi | `N. S. do Livramento-MT-2020` | Nossa Senhora do Livramento / MT / 2020 |
| pequi | `N. S. do Livramento-MT-2021` | Nossa Senhora do Livramento / MT / 2021 |
| pequi | `N. S. do Livramento-MT-2022` | Nossa Senhora do Livramento / MT / 2022 |
| pequi | `N. S. do Livramento-MT-2023` | Nossa Senhora do Livramento / MT / 2023 |
| pequi | `N. S. do Livramento-MT-2024` | Nossa Senhora do Livramento / MT / 2024 |
| pirarucu | `Tefé-AM-2015` | Reservas de Mamirauá e Maraã - Tefé / AM / 2015 |
| pirarucu | `Tefé-AM-2016` | Reservas de Mamirauá e Maraã - Tefé / AM / 2016 |
| pirarucu | `Tefé-AM-2024` | Tefé / AM / 2023 |
| pinhao | `S. J. dos Pinhais-PR-2012` | São José dos Pinhais / PR / 2012 |
| pinhao | `S. J. dos Pinhais-PR-2013` | São José dos Pinhais / PR / 2013 |
| pinhao | `S. J. dos Pinhais-PR-2014` | São José dos Pinhais / PR / 2014 |
| pinhao | `S. J. dos Pinhais-PR-2015` | São José dos Pinhais / PR / 2015 |
| pinhao | `S. J. dos Pinhais-PR-2016` | São José dos Pinhais / PR / 2016 |
| pinhao | `S. J. dos Pinhais-PR-2017` | São José dos Pinhais / PR / 2016 |
| pinhao | `S. F. de Paula-RS-2017` | SÃO FRANCISCO DE PAULA / RS / 2017 |
| pinhao | `S. F. de Paula-RS-2019` | São Francisco de Paula / RS / 2019 |
| pinhao | `S. F. de Paula-RS-2020` | São Francisco de Paula / RS / 2020 |
| pinhao | `S. F. de Paula-RS-2021` | São Francisco de Paula / RS / 2021 |
| pinhao | `S. F. de Paula-RS-2022` | São Francisco de Paula / RS / 2022 |
| pinhao | `S. F. de Paula-RS-2023` | São Francisco de Paula / RS / 2023 |
| pinhao | `S. F. de Paula-RS-2024` | São Francisco de Paula / RS / 2024 |
| umbu | `S. M. do Gostoso-RN-2017` | São Miguel do Gostoso / RN / 2017 |
| umbu | `S. M. do Gostoso-RN-2018` | São Miguel do Gostoso / RN / 2018 |
| umbu | `S. M. do Gostoso-RN-2019` | São Miguel do Gostoso / RN / 2019 |
| umbu | `S. M. do Gostoso-RN-2020` | São Miguel do Gostoso / RN / 2020 |
| umbu | `S. M. do Gostoso-RN-2021` | São Miguel do Gostoso / RN / 2021 |
| umbu | `S. M. do Gostoso-RN-2022` | São Miguel do Gostoso / RN / 2022 |
| umbu | `S. M. do Gostoso-RN-2023` | São Miguel do Gostoso / RN / 2023 |
| umbu | `S. M. do Gostoso-RN-2024` | São Miguel do Gostoso / RN / 2024 |

## Cache and provenance

Catalogue cache: 1 hour in process, isolated from agricultural costs. `use_cache=False` bypasses reads and writes. Workbooks are downloaded on every query. Metadata keeps request receipts, byte SHA-256, selected contexts, physical value cells, schema 1.0 and the selected source. `as_polars=True`, `return_meta=True` and the sync wrapper are supported; immutable snapshots are unavailable and `deterministic` is rejected before network calls. Licence: CONAB public federal data (`livre`), as listed in [source licences](../licenses.md).

## Example

```python
from agrobr import conab, datasets

catalog = await conab.catalogo_sociobiodiversidade("acai")
df, meta = await datasets.custo_sociobiodiversidade(
    "acai", uf="AM", ano=2024, return_meta=True
)
old = await datasets.custo_sociobiodiversidade("acai", uf="AM", ano=2008)
carnauba = await datasets.custo_sociobiodiversidade("carnauba", ano=2022)
babacu = await datasets.custo_sociobiodiversidade("babacu", ano=2018)
```
