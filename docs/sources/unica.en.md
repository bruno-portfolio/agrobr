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

# Annual history by state (1980/1981 to 2020/2021)
df = await unica.producao_historica("acucar", safra_inicio="2010/2011")
```

Requires the `[pdf]` extra for the biweekly report: `pip install agrobr[pdf]`.

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
- Update: biweekly during the season (positions on the 1st and 16th of each month)
- License: `zona_cinza` — educational/research use; for commercial use, consult UNICA
