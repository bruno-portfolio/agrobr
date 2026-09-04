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
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

### `importacao`

Import data by agricultural product. Same interface as `exportacao()`.

```python
async def importacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters (both):**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Alias from the table below or an NCM prefix |
| `ano` | `int \| None` | Reference year. Default: previous year |
| `uf` | `str \| None` | Filter by state |
| `agregacao` | `str` | `"mensal"` (default) or `"detalhado"` |
| `as_polars` | `bool` | If True, returns polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Product aliases:**

| Alias | NCM or prefix |
|-------|---------------|
| `soja` | `12019000` |
| `soja_grao` | `12019000` |
| `soja_semeadura` | `12011000` |
| `oleo_soja` | `1507` |
| `oleo_soja_bruto` | `15071000` |
| `farelo_soja` | `23040010` |
| `milho` | `10059010` |
| `arroz` | `10063021` |
| `trigo` | `10019900` |
| `algodao` | `520100` |
| `algodao_cardado` | `520300` |
| `cafe` | `09011110` |
| `cafe_arabica` | `09011110` |
| `cafe_conilon` | `09011190` |
| `acucar` | `17011400` |
| `etanol` | `22071000` |
| `carne_bovina` | `02023000` |
| `carne_frango` | `02071400` |
| `carne_suina` | `02032900` |
| `fertilizantes` | `31` |
| `ureia` | `31021010` |
| `sulfato_amonio` | `31022100` |
| `nitrato_amonio` | `31023000` |
| `ssp` | `31031900` |
| `tsp` | `31031100` |
| `kcl` | `31042090` |
| `map` | `31054000` |
| `dap` | `31053000` |
| `npk` | `3105` |
| `defensivos` | `3808` |
| `agrotoxicos` | `3808` |

**Returns:**

- `agregacao="mensal"`: `ano`, `mes`, `ncm`, `uf`, `kg_liquido`,
  `valor_fob_usd`, `volume_ton`.
- `agregacao="detalhado"`: `ano`, `mes`, `ncm`, `cod_unidade`, `cod_pais`,
  `uf`, `cod_via`, `cod_porto`, `qtd_estatistica`, `kg_liquido`,
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
- 31 aliases mapped by NCM prefix; `oleo_soja` covers `1507`, while
  `oleo_soja_bruto` remains specific to `15071000`
- Annual CSV files of ~100MB each
- Data available from 1997 onward
