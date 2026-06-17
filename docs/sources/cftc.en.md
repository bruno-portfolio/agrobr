# CFTC COT — Fund Positioning

Commodity Futures Trading Commission — weekly Commitments of Traders report
(Disaggregated format), with positioning by trader category in the
agricultural contracts of CBOT, CME and ICE. Managed money is the category that the agricultural market
refers to as the "fund position".

## API

```python
from agrobr import cftc

# Fund positions in soybeans since May
df = await cftc.cot("soja", start="2026-05-01")

# All 12 mapped agricultural contracts
df = await cftc.cot()

# Futures + options combined
df = await cftc.cot("milho", combined=True)
```

Semantic dataset with versioned contract:

```python
from agrobr import datasets

df = await datasets.posicionamento_fundos("soja")
```

## Columns — `cot`

| Column | Type | Description |
|---|---|---|
| `data` | datetime | Reference Tuesday of the report |
| `commodity` | str | agrobr canonical name (`soja`, `milho`, ...) |
| `contrato` | str | Contract and exchange name (e.g.: `SOYBEANS - CHICAGO BOARD OF TRADE`) |
| `codigo_cftc` | str | CFTC contract code |
| `open_interest` | int64 | Open contracts |
| `managed_money_long/short/spread` | int64 | Fund positions |
| `managed_money_net` | int64 | Long − short (calculated) |
| `producer_long/short` | int64 | Commercial hedgers |
| `swap_long/short` | int64 | Swap dealers |
| `other_long/short` | int64 | Other reportables |
| `nonreportable_long/short` | int64 | Non-reportable positions |
| `change_managed_money_long/short` | Int64 | Weekly change (nullable) |
| `change_open_interest` | Int64 | Weekly change in OI (nullable) |

## Contracts

`soja`, `farelo_soja`, `oleo_soja`, `milho`, `trigo` (SRW), `acucar` (no. 11),
`cafe` (C), `algodao` (no. 2), `boi` (live cattle), `suino` (lean hogs),
`laranja` (FCOJ-A), `arroz` (rough rice). Accepts canonical name, EN alias or CFTC code.

## MetaInfo

```python
df, meta = await cftc.cot("soja", return_meta=True)
print(meta.source)  # "cftc"
```

## Source

- API: `https://publicreporting.cftc.gov/resource/72hh-3qpy.json` (Socrata, no authentication)
- Combined (futures+options): `kh3c-gbw2`
- Update: weekly — Friday 15:30 ET, data from Tuesday
- History: June/2006 onward
- License: `livre` (public domain, U.S. government)
