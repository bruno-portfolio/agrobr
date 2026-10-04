# CONAB API

The CONAB module provides access to crop surveys, supply/demand balance, Brazil totals, production costs, historical series, crop progress and wholesale (CEASA) prices from the National Supply Company.

## HTTP transport and optional browser

The crop and balance APIs use HTTP first. Playwright and Chromium are optional transport fallback dependencies:

```bash
pip install agrobr[browser]
python -m playwright install chromium
```

If HTTP and the optional transport fail, the APIs raise `SourceUnavailableError`. In the dataset layer, `estimativa_safra` may try IBGE LSPA when the selection permits fallback; `balanco` has no alternative source.

## Functions

### `safras`

Retrieves crop survey data by product and state.

```python
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product: 'soja', 'milho', 'arroz', etc. |
| `safra` | `str \| None` | Crop year in '2024/25' format. Default: latest |
| `uf` | `str \| None` | State (e.g. 'MT', 'PR'). Default: all |
| `levantamento` | `int \| None` | Survey of the crop year itself (1-12). Default: most recent publication carrying the crop year |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Returns:**

DataFrame with columns:
- `fonte`: Data source
- `produto`: Product
- `safra`: Crop year
- `uf`: State
- `area_plantada`: Planted area (thousand ha)
- `area_colhida`: Null; surveys do not publish a separate harvested area
- `produtividade`: Yield (kg/ha)
- `producao`: Production (thousand t)
- `levantamento`: Survey number of the publication used
- `data_publicacao`: Date of the publication used

CONAB publishes one area measure, labelled "ÁREA (Em mil ha)", retained as `area_plantada`. `area_colhida` is null on the CONAB route; a separate harvested area is available only on the LSPA route. Neither area is copied or imputed from the other. CONAB contract V2 already permits this null value.

In sheets with calendar-year headers (wheat, oats, canola, rye, barley and triticale), the published year is the contract crop year's final year (`Safra 2026` → `2025/26`). A survey that does not yet publish that year supplies no estimate for the requested crop year. The historical series retains its own annual period.

From Oct 2019 to Jan 2022, these six cereals' sheets carry the year in their name ("Trigo 2021"), and the edition may also carry the previous year's sheet, sometimes with a broken crop-year header. agrobr reads the most recent sheet whose header publishes the requested crop year: in the 12th survey of 2020/21 (Sep 2021), wheat 2019/20 comes from the "Safra 2020" column of "Trigo 2021", not from the "Trigo 2020" copy, whose header reads "23". In surveys 1 to 4 of 2019/20, 3 and 4 of 2020/21 and 2 to 4 of 2021/22, the sheet does not yet carry the crop year's own winter crop, and the query returns empty. CONAB published surveys 7, 8, 9 and 12 of 2019/20, 1 and 2 of 2020/21 and 1 of 2021/22 only as PDF, so they are not in the catalog.

**Past crop years.** Without `levantamento`, a crop year comes from the most recent publication that carries it. CONAB republishes the previous crop year, revised, in the next crop year's surveys: `conab.safras('gergelim', safra='2024/25', uf='MT')` returns 695 thousand ha (12th survey of 2025/26, Sep 2026); with `levantamento=12`, it returns the 12th survey of 2024/25 itself (Sep 2025, 401.2 thousand ha) and a warning that a more recent publication exists. `levantamento` and `data_publicacao` always describe the publication used; `MetaInfo.source_details["publicacao"]` records its `levantamento`, `safra` (of the publication), `data_publicacao` and `url`. `brasil_total(safra=...)` and `balanco(safra=...)` follow the same rule.

**Older crop years.** Each edition's product sheet only carries the current and the previous crop year. Two or more crop years behind the catalog's most recent edition, CONAB's revision only appears in the historical series (by state) and in the Suprimento sheet (Brazil, production only, for six products). In that case, without `levantamento`, `safras` reads the historical series of the same product, the one `serie_historica_safra` reads, provided its reference is later than the bulletin edition that would carry the crop year. The reference is the spreadsheet legend ("Estimativa em setembro/2026"), because the series publishes no date. If the series is not newer, the bulletin edition is used, with a warning. On rows from the series, `levantamento` and `data_publicacao` are null, and `MetaInfo.source_details["publicacao"]` holds `origem="serie_historica"` and, per series, `produto`, `url`, `sha256` and `referencia`. Example: soybean 2022/23 comes out as 159,154.3 thousand t (series and Suprimento sheet), not the 154,609.5 of the 12th survey of 2023/24. With `levantamento=N`, the original edition is returned. `datasets.estimativa_safra` and the CONAB route of `datasets.producao_anual` follow the same rule. For the crop year that has just left the bulletin, the series is checked against the last edition that published it (see `brasil_total`).

**Sum of states × BRASIL.** When the sum of the states returned does not match the published BRASIL row beyond rounding (0.05 per state summed, plus 0.05 for BRASIL itself), agrobr issues one warning per crop year and column (`area_plantada`, `producao`) and passes the published figures through. A sheet that lists only some states leaves the others in BRASIL (the coffee series lists 10 or 11); there, only a sum above it is inconsistent. With `uf`, the check is the same, over the state returned. Examples: second-crop corn 2020/21 in the 12th survey of 2021/22 (the 27 states add up to 59,981.5 thousand t, and the published BRASIL is 60,741.6) and wheat 2003/04 from the historical series (the published BRASIL for 2004 does not match the states). `serie_historica` checks each area and production period and column it returns in the same way.


**Example:**

```python
from agrobr import conab

# All states
df = await conab.safras('soja', safra='2024/25')

# Mato Grosso only
df = await conab.safras('soja', safra='2024/25', uf='MT')

# Specific survey
df = await conab.safras('soja', safra='2024/25', levantamento=5)
```

---

### `balanco`

Retrieves the supply and demand balance.

Without `levantamento`, `safra` selects the most recent publication whose Suprimento sheet carries that crop year: the current edition covers the last seven crop years (six for soybean), already revised (wheat 2024/25: 7,873.4 thousand t in Sep 2026, against 7,536.1 in the Sep 2025 edition). The returned table is that publication's and may contain rows for several periods. With `levantamento=N`, the Nth survey of the crop year itself is returned, which is the original edition, with a warning when a more recent publication exists. Wheat balance periods remain annual (`2025` stands for 2024/25). The last published revision of each product and period takes precedence. `MetaInfo.source_details["publicacao"]` records the edition used. When a published row does not close one of the balance identities beyond rounding (0.05 thousand t per term), agrobr issues one warning per row and passes the published figures through. The identities are: initial stock + production + imports = supply; consumption + exports = total demand; supply − consumption − exports = ending stock. Example: rice 2024/25 in Sep 2026, off by −363.8 thousand t in the last one.


```python
async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    *,
    as_polars: bool = False,
    levantamento: int | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str \| None` | Specific product or all; without it, soybean comes from its own "Suprimento - Soja" sheet. Accepts `soja`, `milho`, `arroz`, `feijao`, `trigo`, and `algodao`, with or without accents; any other value raises `InvalidParameterError` before the network |
| `safra` | `str \| None` | Crop year. Default: most recent publication |
| `levantamento` | `int \| None` | Survey of the crop year itself (1-12), for the original edition |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | Returns a `(df, MetaInfo)` tuple with provenance |

**Returns:**

DataFrame with columns:
- `produto`: Product
- `safra`: Crop year
- `levantamento`: Published row revision label when present (text/date; not a selector)
- `estoque_inicial`: Initial stock (thousand t)
- `producao`: Production (thousand t)
- `importacao`: Imports (thousand t)
- `suprimento`: Total supply (thousand t), as published in the Suprimento sheet; in soybean's own sheet, opening stock + production + imports, added up by agrobr
- `consumo`: Consumption (thousand t), as published; in the soybean sheet, seeds/other + crushing, added up by agrobr
- `exportacao`: Exports (thousand t)
- `demanda_total`: Total demand (thousand t); null in the soybean wide layout and the legacy long layout without this column
- `estoque_final`: Ending stock (thousand t)
- `unidade`: Unit (`mil_ton`)

All eight metrics use float64, including empty results. `levantamento` and `demanda_total` are always present and remain null when unpublished. The balance's `schema_version` metadata is `1.1`.

**Example:**

```python
from agrobr import conab

# Soybean balance
df = await conab.balanco('soja')

# All products
df = await conab.balanco()
```

---

### `brasil_total`

Retrieves national production totals.

```python
async def brasil_total(
    safra: str | None = None,
    *,
    as_polars: bool = False,
    levantamento: int | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

**Returns:**

DataFrame with Brazil totals for all products. With `safra`, the figures come from the most recent publication carrying the crop year, as in `safras`; with `levantamento=N`, from the original edition.

For an older crop year (two or more behind the most recent edition), without `levantamento`, each of the 36 product rows comes from the BRASIL row of the matching historical series. The two `SUBTOTAL` rows and `BRASIL (2)` are recomputed as sums, following CONAB's own rule: top-level product rows, without cotton lint. The yield of those three rows is production ÷ area. This costs about 35 series downloads per call, one at a time through the CONAB rate limiter. If one series fails, the whole call raises `SourceUnavailableError`, naming the missing product row and suggesting `levantamento=`; it never returns with rows missing. `MetaInfo.source_details["publicacao"]` holds `origem="serie_historica"` and the `url`, `sha256` and `referencia` of each series.

In the sum, absence is not zero. A crop whose series starts after the requested crop year (not yet surveyed, such as sesame, whose series starts in 2018/19) is left out of the subtotal, with a warning in `MetaInfo.validation_warnings` naming it with the first period of its series. Any other part without the crop year makes the subtotal and `BRASIL (2)` null, with a warning; a subtotal with no parts is also null, never 0. Example: 1975/76 precedes every summer series, so the summer subtotal and `BRASIL (2)` come out null, and the winter one is 142.5 thousand ha.

On this path, the parts are checked against the published total: Cores + Preto + Caupi = each bean season, the three seasons = FEIJÃO TOTAL, and the peanut and corn seasons and the rice systems = their totals. Each series' BRASIL row is the sum of the rounded states, and the identity inherits that rounding: beyond 1.4 (0.05 per state, plus 0.05 for the total), agrobr issues one warning per identity and column and passes the published figures through. Example: 2021/22, where the third-season bean series publishes 707.2 thousand t and the types add up to 748.0 (Cores 700.6).

For the crop year that has just left the bulletin (two behind the most recent edition, the newest one served by the series), `safras` and `brasil_total` also download the last bulletin edition that published it and check each row's BRASIL against the series: the series legend may be later without the column having been revised. The data still comes from the series; a divergence beyond 0.1 raises a warning with both values, and `MetaInfo.source_details["publicacao"]["conferencia"]` records the edition checked and the `divergencias` (an empty list when they match). Example, with the September 2026 series and the 12th survey of 2025/26: sesame 2024/25 with 399.4 thousand t in the series and 610.9 in the bulletin.

`produto` is the normalized identifier without footnotes (`soja`, `algodao_caroco`, `feijao_cores_1`, `subtotal`, `brasil`). `rotulo` preserves the original text from column A, such as `ALGODÃO - CAROÇO (1)` and `BRASIL (2)`. `grupo` retains the published block title: the bean season for `Cores`, `Preto` and `Caupi`, the parent product for details such as `Milho 1ª Safra`, and `CULTURAS DE INVERNO` for winter crops and their subtotal; it is null on other rows. `produto`, `grupo` and `safra` identify the row.

**Types and empty results:** `area_plantada`, `produtividade` and `producao` use `float64`, in `mil_ha`, `kg/ha` and `mil_ton`. Missing values remain null. Schema 2.0, also described by `CONAB_BRASIL_TOTAL_V2`, retains the same nine columns in empty results. A missing or changed header raises `ParseError`.

### `serie_historica`

```python
async def serie_historica(
    produto: str,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) when return_meta=True
```

Bounds are inclusive integer years, applied to the starting year of `safra`. Replace `inicio=`/`fim=` with `ano_inicio=`/`ano_fim=`. Metric names and units remain as defined in [contract 1.1](../contracts/serie_historica_safra.md).

### `produtos_serie_historica`

`conab.produtos_serie_historica()` returns a list of dictionaries containing `produto`, `categoria` and `url`, without a request. It lists historical series; `conab.produtos()` lists the products in monthly surveys.

Use the functions in `agrobr.conab`, including `custo_producao` and `serie_historica`. Their former namesake subpackages have been removed. Canonical CEASA functions are `ceasa_precos`, `ceasa_produtos`, `ceasa_categorias` and `lista_ceasas`; the equivalent attributes in `conab.ceasa` remain accessible outside `__all__`.

---

### `levantamentos`

Lists available surveys.

```python
async def levantamentos() -> list[dict]
```

**Returns:**

List of dicts describing each published survey (crop year, number and metadata).

---

### `produtos`

Lists available products (codes accepted by `safras()`).

```python
async def produtos() -> list[str]
```

---

### `ufs`

Lists the 27 available states.

```python
async def ufs() -> list[str]
```

---

## Related Functions

The CONAB module also exposes (documented in their own pages or in the contracts):

- `custo_producao(produto, uf=..., planilha=..., aba=...)` / `custo_producao_total(...)` — production costs per hectare; `as_polars` and `return_meta` are keyword-only. `catalogo_custos(produto)` lists agricultural workbooks, and a product with no workbook in the catalog raises `InvalidParameterError` listing the published crops; with `planilha=...`, it lists sheets and identified or unresolved contexts, with `data_referencia` as `datetime64[ns]`. Any product with multiple candidate workbooks or contexts requires explicit `planilha` and `aba` selection until a single context is identified; the API lists candidates and does not select a revision automatically. Coffee uses `cafe_arabica` or `cafe_conilon`. A subtotal or formula total that does not close with the published items issues a warning in `meta.validation_warnings`. See the dataset's eight products and their semantics in the [custo_producao](../contracts/custo_producao.md) contract
- `serie_historica(produto, ...)` — crop historical series (45 products, with coverage depending on the product). Coffee includes producing/developing areas and explicit conversions to thousand ha, thousand tonnes and kg/ha; sugarcane publishes harvested area in `area_colhida_mil_ha`. Warns when the sum of the states does not match the published BRASIL, under the `safras` rule. See the [serie_historica_safra](../contracts/serie_historica_safra.md) contract
- `progresso_safra(...)` / `semanas_disponiveis()` — weekly planting/harvest progress. See the [CONAB Progress API](conab_progresso.md)
- `ceasa_precos(...)` / `ceasa_produtos()` / `ceasa_categorias()` / `lista_ceasas()` — wholesale produce prices. See the [CONAB CEASA API](conab_ceasa.md)

### Cache

Catalog cache: **1 hour in process**. The keyword-only parameter `use_cache=False` in `conab.catalogo_custos`, `conab.custo_producao`, and `datasets.custo_producao` forces a fresh read without consulting or updating the cache. Workbooks are downloaded for every query. `meta.source_details["catalog_cache"]` reports `hit`, `miss`, or `bypass`; original catalog receipts remain in `manifest.acquisition.catalog_acquisition`, separate from the current call's requests.

---

## Models

### `Safra`

```python
class Safra(BaseModel):
    fonte: Fonte
    produto: str
    safra: str = Field(..., pattern=r"^\d{4}/\d{2}$")
    uf: str | None = Field(None, min_length=2, max_length=2)
    area_plantada: Decimal | None = Field(None, ge=0)
    producao: Decimal | None = Field(None, ge=0)
    produtividade: Decimal | None = Field(None, ge=0)
    unidade_area: str = Field(default="mil_ha")
    unidade_producao: str = Field(default="mil_ton")
    levantamento: int = Field(..., ge=1, le=12)
    data_publicacao: date | None = None
    meta: dict[str, Any] = Field(default_factory=dict)
    parsed_at: datetime = Field(default_factory=utcnow)
    parser_version: int = Field(default=1)
    anomalies: list[str] = Field(default_factory=list)
```

## Available Products

`produtos()` returns 25 codes (including aggregate aliases and sub-crops):

| Code | Product |
|------|---------|
| `soja` | Soybean |
| `milho` | Corn (total) |
| `milho_1` | Corn 1st crop |
| `milho_2` | Corn 2nd crop |
| `milho_3` | Corn 3rd crop |
| `arroz` | Rice (total) |
| `arroz_irrigado` | Irrigated rice |
| `arroz_sequeiro` | Upland rice |
| `feijao` | Beans (total) |
| `feijao_1` | Beans 1st crop |
| `feijao_2` | Beans 2nd crop |
| `feijao_3` | Beans 3rd crop |
| `algodao` | Cotton (total) |
| `algodao_pluma` | Cotton lint |
| `trigo` | Wheat |
| `sorgo` | Sorghum |
| `aveia` | Oats |
| `cevada` | Barley |
| `canola` | Canola |
| `girassol` | Sunflower |
| `mamona` | Castor bean |
| `amendoim` | Peanut |
| `centeio` | Rye |
| `triticale` | Triticale |
| `gergelim` | Sesame |

## Synchronous Version

```python
from agrobr.sync import conab

df = conab.safras('soja', safra='2024/25')
df = conab.balanco('milho')
```

## Sociobiodiversity

`conab.custo_sociobiodiversidade(produto, uf=None, ano=None, *, local=None, planilha=None, aba=None, use_cache=True, as_polars=False, return_meta=False)` returns the published extraction costs. The dataset has the same selectors. `conab.catalogo_sociobiodiversidade()` lists all resource revisions with an active flag; with a product it inventories the active workbook, including unresolved contexts. Select an exact historical resource with `planilha=`. The 20 captured products, literal units, selection rules and nominal limitations are documented in the [1.0 contract](../contracts/custo_sociobiodiversidade.md). No hectare/crop conversion or revision merging. Catalogue cache: 1 hour, separate from agricultural costs; workbooks are always downloaded. `use_cache=False` bypasses the catalogue cache.

Agricultural costs use parser 5: merged headers, coffee identified by official
resource, literal annual or two-year crop tokens, and exchange-rate notes kept
outside cost items. Excel percentage scaling applies only to numeric cells.
Sociobiodiversity costs use parser 2 and preserve explicit archived-revision
selection. Contracts remain 3.0 and 1.0, with text in the installed pandas default dtype; a third monetary measure is rejected.

Survey parser 3 recognizes historical wheat sheets such as `Trigo 2021`, selected
by the requested harvest ending year. Two matching sheets raise `ParseError`;
worksheet order never determines the selected year.

## Catalogs and product normalization

`produtos()` and `ufs()` are local catalogs with no network access and retain their asynchronous
signatures: use `await conab.produtos()` and `await conab.ufs()`. Through the synchronous facade,
use `sync.conab.produtos()` and `sync.conab.ufs()`.

`safras()` normalizes case, surrounding whitespace, and accented aliases such as `" FEIJÃO "`
before requesting data and selecting rows. `ceasa_precos(produto=...)` checks the product after the
network call, against the received publication, ignoring accents and case on both sides: a published
product outside `ceasa_produtos()` also filters, and one missing from the publication raises
`InvalidParameterError` listing the published ones.
