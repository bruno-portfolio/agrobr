# UNICA — Center-South Sugarcane Season

União da Indústria de Cana-de-Açúcar e Bioenergia. Biweekly monitoring of the
Center-South season (crushing, sugar, ethanol, production mix, ATR) via a public
PDF report, and annual historical production by state via an export from the classic site.

> `zona_cinza` classification: no public terms of use found. The module
> emits a `UserWarning` on the first call. See [licenses](../licenses.md#unica).

## API

```python
from agrobr import unica

# Accumulated crushing for the current season (most recent biweekly report)
df = await unica.moagem_quinzenal("cana", regiao="centro_sul")

# Season position: production, ATR, sugar/ethanol mix
df = await unica.safra_resumo(periodo="acumulado")
df = await unica.safra_resumo(periodo="mensal")  # or "quinzena", depending on the edition

# Annual history by state (1980/1981 to 2020/2021)
df = await unica.producao_historica("acucar", safra_inicio="2010/2011")
```

Requires the `[pdf]` extra for the biweekly report: `pip install agrobr[pdf]`.

## Parameters

| Function | Parameter | Type | Default | Description |
|----------|-----------|------|---------|-------------|
| `moagem_quinzenal` | `produto` | str | `"cana"` | `cana`, `acucar`, `etanol_total`, `etanol_anidro`, or `etanol_hidratado` |
| `moagem_quinzenal` | `regiao` | str \| None | None | `sao_paulo`, `centro_sul`, `demais_estados`, or all regions |
| `safra_resumo` | `periodo` | str | `"acumulado"` | `acumulado`, `quinzena` or `mensal`; a period the current edition does not publish → `InvalidParameterError` listing the edition's periods |
| `producao_historica` | `produto` | str | `"cana"` | `cana`, `acucar`, `etanol_anidro`, `etanol_hidratado`, or `etanol_total` |
| `producao_historica` | `safra_inicio` | str \| None | None | Initial crop year (`YYYY/YYYY`, `YYYY/YY` or `YY/YY`), from 1980/1981 to 2020/2021 |
| `producao_historica` | `safra_fim` | str \| None | None | Final crop year, same formats and range, not before the initial one |
| All | `as_polars` | bool | False | If True, returns a `polars.DataFrame` |
| All | `return_meta` | bool | False | If True, returns `(DataFrame, MetaInfo)` |

## Columns — `moagem_quinzenal`

| Column | Type | Description |
|---|---|---|
| `data` | datetime | Position date (biweek) |
| `quinzena` | str | Biweek label (e.g., `01/05`) |
| `safra` | str | Report season (e.g., `2026/2027`) |
| `produto` | str | `cana`, `acucar`, `etanol_total`, `etanol_anidro`, `etanol_hidratado` |
| `regiao` | str | `sao_paulo`, `centro_sul`, `demais_estados` |
| `valor` | float | Current-season accumulated value up to the biweek |
| `valor_safra_anterior` | float | Equivalent accumulated value for the previous season |
| `variacao_pct` | float | Percentage change |
| `unidade` | str | `t` (cane/sugar) or `m3` (ethanol) |

## Columns — `safra_resumo`

| Column | Type | Description |
|---|---|---|
| `produto` | str | `cana`, `acucar`, `etanol_anidro`, `etanol_hidratado`, `etanol_total`, `atr`, `atr_por_tonelada`, `mix_acucar`, `mix_etanol`, `litros_etanol_por_tonelada`, `kg_acucar_por_tonelada` |
| `regiao` | str | `centro_sul`, `sao_paulo`, `demais_estados` |
| `safra` | str | Report season (e.g., `2026/2027`) |
| `periodo` | str | `acumulado`, `quinzena` or `mensal`, read from the table title |
| `data_inicio` | datetime | Period start, read from the title (e.g., `2026-06-01` for "junho de 2026") |
| `data_fim` | datetime | Period end, read from the title (e.g., `2026-07-01` for the accumulated "até 01 de julho de 2026") |
| `valor` | float | Current-season value for the period |
| `valor_safra_anterior` | float | Equivalent value for the previous season |
| `variacao_pct` | float | Percentage change; null for the mix, which the source does not compare |
| `unidade` | str | `mil_t`, `mi_litros`, `kg_t` (ATR or sugar per ton), `l_t` or `pct` (mix) |

In the accumulated period, `data_fim` is the title date, which UNICA uses as an exclusive bound: the accumulated "até 01 de agosto de 2026" runs through July 31 and equals the accumulated "até 01 de julho" plus the July monthly value, which comes with `data_fim=2026-07-31`. The data stays as published.

## Columns — `producao_historica`

| Column | Type | Description |
|---|---|---|
| `safra` | str | E.g., `2019/2020` |
| `localidade` | str | State or aggregates `centro_sul`, `norte_nordeste`, `brasil` |
| `produto` | str | Requested product |
| `valor` | float | Season production |
| `unidade` | str | `mil_t` or `mil_m3` |

## Limitations

- **Biweekly**: the PDF covers the current season + comparison with the previous one; the source
  does not provide a long biweekly history.
- **Biweekly or monthly edition**: summary Table 2 carries the biweek (e.g., "2ª quinzena de abril de 2026", 2026-05-01
  edition) or the month (e.g., "junho de 2026", 2026-07-01 edition). agrobr reads the period and the dates from each
  table title, and a title it does not recognize becomes a `ParseError`. The listing keeps only the current edition: on
  2026-09-23, the position up to 2026-07-01, published on 2026-08-06.
- **Revisions**: UNICA revises past biweeks in every edition. Example: Center-South cane accumulated up to 05/01,
  60,457,836 t in the 2026-05-01 edition and 60,412,599 t in the 2026-07-01 one. agrobr always serves the current edition.
- **Missing value**: an absence mark (`n/d`, `-`) in place of a number in the summary or in the biweekly series becomes
  a null value, and the row is kept.
  A mandatory product (cane, sugar, total ethanol, mix) missing from a period becomes a `ParseError`.
- **Out of scope**: report Tables 8 (corn ethanol) and 9 (monthly ethanol sales).
- **History**: the classic site database is **frozen at 2020/2021** — later seasons
  return empty from the source. For the current season use the biweekly functions.

## MetaInfo

```python
df, meta = await unica.moagem_quinzenal("cana", return_meta=True)
print(meta.source)  # "unica"
```

## Source

- Biweekly report: `https://unicadata.com.br/listagem.php?idMn=63` (PDF, rotating URL)
- History: `https://unicadata.com.br/xlsHPM.php` (XLSX)
- Update: per season-report edition, biweekly or monthly; the listing only carries the current edition
- License: `zona_cinza` — educational/research use; for commercial use, consult UNICA
