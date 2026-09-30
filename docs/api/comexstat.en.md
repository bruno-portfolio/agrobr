# ComexStat API

The ComexStat module provides Brazilian export and import data from MDIC/SECEX — volumes, FOB values (USD) by product, state and country.

## Functions

### `exportacao`

Export data by agricultural product.

```python
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult
```

### `importacao`

Import data by agricultural product. Same interface as `exportacao()`.

```python
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult
```

**Parameters (both):**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Alias from the table below or an NCM prefix of 2 to 8 digits, without dots (e.g. `"1507"`, `"22071010"`) |
| `ano` | `int \| None` | Reference year (1997 through the current year in Brasília). Default: previous year |
| `uf` | `str \| None` | Filter by state |
| `agregacao` | `str` | `"mensal"` (default) or `"detalhado"` |
| `as_polars` | `bool` | If True, returns polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Product aliases:**

| Alias | NCM (prefixes) | Included | Not included |
|-------|----------------|----------|--------------|
| `soja`, `soja_grao` | `12019000`; `12010090` (code until 2013) | soybeans, whether or not broken | seed (`soja_semeadura`), oil (`1507`), meal (`2304`) |
| `soja_semeadura` | `12011000`; `12010010` (code until 2011) | soybeans for sowing | — |
| `oleo_soja` | `1507` | crude, refined and other soybean oil | — |
| `oleo_soja_bruto` | `15071000` | crude oil, whether or not degummed | refined |
| `farelo_soja` | `2304` | flours and pellets (`23040010`) and cake and other solid residues (`23040090`) | — |
| `milho` | `1005` | grain, seed and other maize | flour, starch and oil (other chapters) |
| `arroz` | `1006` | paddy, husked, semi-milled or milled (parboiled or not) and broken rice | flour (`1102`) |
| `trigo` | `1001` | durum and other wheat, including seed, and meslin | flour (`1101`) |
| `algodao` | `5201`, `5203` | not carded or combed; carded or combed | waste (`5202`), yarn and fabrics |
| `algodao_cardado` | `520300` | carded or combed | — |
| `cafe` | `09011`, `09012` | not roasted and roasted, decaffeinated or not | husks, skins and substitutes (`09019000`), soluble (`2101`) |
| `acucar` | `1701` | raw cane and beet sugar and refined sugar | molasses (`1703`) |
| `etanol` | `2207` | undenatured and denatured ethyl alcohol | — |
| `carne_bovina` | `0201`, `0202` | fresh, chilled and frozen | offal (`0206`), salted, dried or smoked (`0210`), preparations (`1602`) |
| `carne_frango` | `02071` | whole birds and cuts of fowls, with offal, fresh and frozen | turkey, duck and other poultry; salted (`0210`); preparations (`1602`) |
| `carne_suina` | `0203` | fresh, chilled and frozen | offal (`0206`), salted (`0210`), preparations (`1602`) |
| `fertilizantes` | `31` | the whole of chapter 31 | — |
| `ureia` | `310210` | urea with any nitrogen content | — |
| `sulfato_amonio` | `31022100` | ammonium sulphate | double salts and mixtures (`310229`) |
| `nitrato_amonio` | `31023000` | ammonium nitrate | mixtures with calcium carbonate (`31024000`) |
| `ssp` | `31031900` (since 2017) | superphosphates with less than 35 % P2O5 | years before 2017: `InvalidParameterError` (use the `310310` prefix) |
| `tsp` | `31031100` (since 2017) | superphosphates with 35 % or more P2O5 | years before 2017: `InvalidParameterError` (use the `310310` prefix) |
| `kcl` | `310420` | potassium chloride with any K2O content | — |
| `map` | `31054000` | monoammonium phosphate | — |
| `dap` | `310530` | diammonium phosphate (`31053000`; `31053010` and `31053090` until 2019) | — |
| `npk` | `31052000` | fertilisers containing the three elements N, P and K | MAP, DAP and the other fertilisers of heading `3105` (use the `3105` prefix) |
| `defensivos`, `agrotoxicos` | `3808` | insecticides, fungicides, herbicides, plant-growth regulators, disinfectants and rodenticides | 27 codes put up exclusively for household sanitation use (e.g. `38089119`, `38089419`) |

`cafe_arabica` and `cafe_conilon` were removed: the NCM does not separate coffee
species (`09011110` is green coffee of both species), and the call raises
`InvalidParameterError` explaining this.

**Returns:**

- `agregacao="mensal"`: `ano`, `mes`, `ncm`, `uf`, `kg_liquido`,
  `valor_fob_usd`, `volume_ton`.
- `agregacao="detalhado"`: `ano`, `mes`, `ncm`, `cod_unidade`, `cod_pais`,
  `uf`, `cod_via`, `cod_urf`, `qtd_estatistica`, `kg_liquido`,
  `valor_fob_usd`.

**Example:**

```python
from agrobr import comexstat

# Soybean exports 2024
df = await comexstat.exportacao("soja", ano=2024)

# Soybean imports 2024
df = await comexstat.importacao("soja", ano=2024)

# Filter by state
df = await comexstat.exportacao("milho", ano=2024, uf="MT")

# Detailed (per record)
df = await comexstat.exportacao("cafe", ano=2024, agregacao="detalhado")
```

## Synchronous Version

```python
from agrobr.sync import comexstat

df = comexstat.exportacao("soja", ano=2024)
df = comexstat.importacao("soja", ano=2024)
```

## Notes

- Source: [ComexStat/MDIC](https://comexstat.mdic.gov.br) — free license
- Each alias sums the codes in force in each year: when the nomenclature splits or
  renumbers a code (ethanol `22071000` → `22071010`/`22071090` in 2011; soybeans
  `12010090` → `12019000` in 2012; chicken `02071400` → 14 subitems in 2024), the
  alias covers both periods, including the transition year
- A year without an equivalent code in the nomenclature raises
  `InvalidParameterError` before any network call (`ssp` and `tsp` before 2017)
- An NCM prefix passed as `produto` selects every code starting with it, without
  the aliases' exclusions
- `MetaInfo.source_details["query"]` records `ncm_prefixos` and `ncm_excluidos`
- Annual CSV files of ~100MB each
- Data available from 1997 onward

### `dicionario`

`await comexstat.dicionario(tabela, *, as_polars=False, return_meta=False)` reads an official lookup table. `tabela` accepts `unidades`, `paises`, `vias`, or `urfs`. Literal codes retain leading zeros; rows preserve source order and duplicates. The same default memory and row limits used by the source apply. Invalid tables fail before download.

`kg_liquido`, monetary values and `volume_ton` use `float64`; years, months and statistical quantities use `Int64`. Source and dictionary text deliberately retain `string[python]`: the memory guard counts pooled Python strings. This exception applies to populated and empty results. The `exportacao` and `importacao` datasets convert final selected text columns to the installed pandas default. Output flags are keyword-only.
