# UNICA — Center-South Crushing and Production

Biweekly tracking of the Center-South sugarcane crop (crushing, sugar, ethanol, mix, ATR)
and annual production history by state, from the Sugarcane and Bioenergy Industry Association.

> Data classified as `zona_cinza` (no public terms of use) — the module emits a
> `UserWarning` on the first call. See [licenses](../licenses.md).

### `moagem_quinzenal`

Biweekly cumulative series for the current crop year, extracted from the most recent PDF report.

```python
async def moagem_quinzenal(
    produto: str = "cana",
    *,
    regiao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | `cana`, `acucar`, `etanol_total`, `etanol_anidro`, `etanol_hidratado` |
| `regiao` | `str \| None` | `sao_paulo`, `centro_sul` or `demais_estados`. None returns all |
| `as_polars` | `bool` | If True, returns `polars.DataFrame` |
| `return_meta` | `bool` | If True, returns a `(DataFrame, MetaInfo)` tuple |

**Returns:**

DataFrame with columns: `data`, `quinzena`, `safra`, `produto`, `regiao`, `valor`,
`valor_safra_anterior`, `variacao_pct`, `unidade` (t for cane/sugar, m³ for ethanol).

Values are **cumulative** for the crop year up to each biweekly period. Covers only the
crop year of the current report + comparison with the previous one — the source does not
publish a long biweekly history.

### `safra_resumo`

Summary of the crop year position (Tables 1-2 of the report): crushing, production, ATR,
sugar/ethanol mix and yields by region.

```python
async def safra_resumo(
    *,
    periodo: Literal["acumulado", "quinzena"] = "acumulado",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Returns:**

DataFrame with columns: `produto`, `regiao`, `safra`, `periodo`, `valor`,
`valor_safra_anterior`, `variacao_pct`, `unidade`. Products include `cana`, `acucar`,
`etanol_*`, `atr`, `atr_por_tonelada`, `kg_acucar_por_tonelada`, `mix_acucar`, `mix_etanol`.

### `producao_historica`

Annual production by state, crop years 1980/1981 to 2020/2021 (XLSX export from the classic site).

```python
async def producao_historica(
    produto: str = "cana",
    *,
    safra_inicio: str | None = None,
    safra_fim: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | `cana`, `acucar`, `etanol_anidro`, `etanol_hidratado`, `etanol_total` |
| `safra_inicio` | `str \| None` | Format `"2015/2016"` |
| `safra_fim` | `str \| None` | Format `"2020/2021"` |

**Returns:**

DataFrame with columns: `safra`, `localidade` (state or aggregates `centro_sul`,
`norte_nordeste`, `brasil`), `produto`, `valor`, `unidade` (`mil_t` or `mil_m3`).

> The source's database is **frozen**: crop years from 2021/2022 onward were not
> published in this format. For the current crop year use `moagem_quinzenal`/`safra_resumo`.

**Example:**

```python
from agrobr import unica

# Cumulative crushing for the current crop year, Center-South
df = await unica.moagem_quinzenal("cana", regiao="centro_sul")

# Sugar/ethanol mix and ATR for the biweekly period
df = await unica.safra_resumo(periodo="quinzena")

# Sugar production history by state
df = await unica.producao_historica("acucar", safra_inicio="2010/2011")
```

## Synchronous Version

```python
from agrobr.sync import unica

df = unica.moagem_quinzenal("cana")
df = unica.safra_resumo()
df = unica.producao_historica("etanol_total")
```

## Notes

- Source: [unicadata.com.br](https://unicadata.com.br) — biweekly report (PDF) and history (XLSX)
- The biweekly PDF requires the `[pdf]` extra: `pip install agrobr[pdf]`
- Biweekly publication during the crop year (positions on the 1st and 16th of each month)
- Sanity checks validated in the parser: CS crushing < 700M t, mix 0-100%, ATR 60-160 kg/t
