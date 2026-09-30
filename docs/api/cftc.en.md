# CFTC — Commitments of Traders

Weekly trader positioning in agricultural and livestock contracts from Chicago and New York,
via the CFTC's COT report (Disaggregated format) from the Commodity Futures Trading Commission.

### `cot`

Weekly positions by trader category: managed money (funds), producer/merchant
(commercial hedgers), swap dealers, other reportables and non-reportable.

```python
async def cot(
    produto: str | None = None,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combined: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str \| None` | Canonical name (`"soja"`), EN alias (`"soybeans"`) or CFTC code (`"005602"`). `None` returns all 12 mapped agricultural contracts |
| `inicio` | `str \| date \| datetime \| None` | Start date (`YYYY-MM-DD` or `DD/MM/YYYY`). `None` returns from 2006 onward |
| `fim` | `str \| date \| datetime \| None` | End date. `None` up to the most recent report |
| `combined` | `bool` | `True` includes options (futures+options); default is futures only |
| `as_polars` | `bool` | If True, returns `polars.DataFrame` |
| `return_meta` | `bool` | If True, returns a `(DataFrame, MetaInfo)` tuple |

**Returns:**

DataFrame with columns: `data`, `commodity`, `contrato`, `codigo_cftc`, `open_interest`,
`managed_money_long`, `managed_money_short`, `managed_money_spread`, `managed_money_net`,
`producer_long`, `producer_short`, `swap_long`, `swap_short`, `swap_spread`, `other_long`, `other_short`,
`other_spread`, `nonreportable_long`, `nonreportable_short`, `change_managed_money_long`,
`change_managed_money_short`, `change_open_interest`.

Positions in number of contracts (int64). The `change_*` columns are nullable (Int64) —
null in the first week of each contract in the series. The columns follow the CFTC report's names; the
`posicionamento_fundos` dataset delivers them in Portuguese.

A `produto` without a mapped contract, a date outside the accepted formats, and `inicio` after `fim` raise
`InvalidParameterError` before the request, with the valid values in the message.

**Example:**

```python
from agrobr import cftc

# Fund positioning in soybeans since May
df = await cftc.cot("soja", inicio="2026-05-01")

# Fund net (long - short), already computed
df[["data", "managed_money_net", "open_interest"]]

# All 12 agricultural contracts, futures+options
df = await cftc.cot(combined=True)

# Directly by CFTC code
df = await cftc.cot("005602")
```

## Mapped Contracts

| Canonical | CFTC Contract | Code |
|---|---|---|
| `soja` | SOYBEANS (CBOT) | 005602 |
| `farelo_soja` | SOYBEAN MEAL (CBOT) | 026603 |
| `oleo_soja` | SOYBEAN OIL (CBOT) | 007601 |
| `milho` | CORN (CBOT) | 002602 |
| `trigo` | WHEAT-SRW (CBOT) | 001602 |
| `acucar` | SUGAR NO. 11 (ICE) | 080732 |
| `cafe` | COFFEE C (ICE) | 083731 |
| `algodao` | COTTON NO. 2 (ICE) | 033661 |
| `boi` | LIVE CATTLE (CME) | 057642 |
| `suino` | LEAN HOGS (CME) | 054642 |
| `laranja` | FCOJ-A (ICE) | 040701 |
| `arroz` | ROUGH RICE (CBOT) | 039601 |

## Semantic Dataset

```python
from agrobr import datasets

df = await datasets.posicionamento_fundos("milho")
df = await datasets.posicionamento_fundos("soja", inicio="2026-01-01", combinado=True)
```

Contract `cftc.cot` v2.0 — primary key `data` + `codigo_cftc`, 22 validated columns (the 2 spread columns, new in 1.1, are optional).
In the dataset, the columns are in Portuguese (`fundos_compra`, `fundos_saldo`, `posicoes_abertas`…), and `combined` is
called `combinado`; the map is `agrobr.contracts.datasets.POSICIONAMENTO_FUNDOS_COLUNAS_V2`.

## Synchronous Version

```python
from agrobr.sync import cftc

df = cftc.cot("soja", inicio="2026-05-01")
```

## Notes

- Source: [CFTC Public Reporting](https://publicreporting.cftc.gov) — public domain (U.S. government)
- Publication: Fridays 15:30 ET, with data as of Tuesday of the same week
- History: June 2006 onward (Disaggregated format)
- No authentication required; internal 2s rate limit between requests
