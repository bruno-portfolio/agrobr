# posicionamento_fundos

Weekly trader positioning in Chicago/NY agricultural futures,
via the CFTC Commitments of Traders (COT Disaggregated) report.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | CFTC | Public Socrata API (publicreporting.cftc.gov), no authentication |

## Usage

```python
df = await datasets.posicionamento_fundos("soja")
df = await datasets.posicionamento_fundos("milho", start="2026-01-01")
df = await datasets.posicionamento_fundos("acucar", combined=True)  # futures + options
```

## Contract `cftc.cot` v1.0

PK: `[data, codigo_cftc]` — effective from 1.1.0

| Column | Type | Nullable | Unit |
|--------|------|----------|------|
| `data` | DATE | N | — |
| `commodity` | STRING | N | — |
| `contrato` | STRING | N | — |
| `codigo_cftc` | STRING | N | — |
| `open_interest` | INTEGER | N | contracts |
| `managed_money_long` | INTEGER | N | contracts |
| `managed_money_short` | INTEGER | N | contracts |
| `managed_money_spread` | INTEGER | N | contracts |
| `managed_money_net` | INTEGER | N | contracts |
| `producer_long` | INTEGER | N | contracts |
| `producer_short` | INTEGER | N | contracts |
| `swap_long` | INTEGER | N | contracts |
| `swap_short` | INTEGER | N | contracts |
| `other_long` | INTEGER | N | contracts |
| `other_short` | INTEGER | N | contracts |
| `nonreportable_long` | INTEGER | N | contracts |
| `nonreportable_short` | INTEGER | N | contracts |
| `change_managed_money_long` | INTEGER | Y | contracts |
| `change_managed_money_short` | INTEGER | Y | contracts |
| `change_open_interest` | INTEGER | Y | contracts |

The `change_*` columns are null in the first week of each contract in the
series (no prior week for the delta).

## Semantics

- `managed_money_*` — funds (the "fund positioning" cited by the agri market)
- `producer_*` — commercial hedgers (producers, processors, trading firms)
- `swap_*` — swap dealers
- `managed_money_net` = `managed_money_long` − `managed_money_short` (computed)
- Positions in number of contracts; `data` is the report's reference Tuesday

## Deterministic Mode

In deterministic mode (`datasets.deterministic()`), the snapshot sets the query
`end` when not provided — an explicit `end` takes precedence.
