# futuros_agricolas

B3 agricultural futures — daily settlements, history and open interest.

## Source

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | B3 | Brazilian stock exchange |

## Modes (`tipo=`)

### Settlements (default)

```python
df = await datasets.futuros_agricolas("boi", data="2025-03-05")
```

### History

```python
df = await datasets.futuros_agricolas("boi", tipo="historico", inicio="2025-01-01", fim="2025-03-05")
```

### Open interest

```python
df = await datasets.futuros_agricolas("boi", tipo="posicoes", data="2025-03-05")
```

### Open interest history

```python
df, meta = await datasets.futuros_agricolas(
    "boi", tipo="oi_historico", inicio="2026-09-03", fim="2026-09-04", return_meta=True
)
```

Provide a product and inclusive start/end dates in `YYYY-MM-DD`; `data` does
not apply to this mode. `vencimento` takes the contract month code (e.g.
`V26`), which returns the future and the options of that month, or an option's
published code (e.g. `VVJK`); any other format is rejected before the network.
Weekdays are queried sequentially; both
futures and options are returned and identified by the `tipo` column.

`data` with `tipo="historico"` or `"oi_historico"`, and `inicio` or `fim` with `"ajustes"` or `"posicoes"`, raise
`InvalidParameterError` before the network: the argument that does not apply to the type used to be silently dropped.

The source retains a recent window without guaranteeing older dates. Use
recent dates when running the example. Weekdays with no positions matching
the filter are listed in `meta.validation_warnings`; this may mean an
unpublished file or an absent instrument/expiry, and does not establish that
a trading session occurred. Weekend-only ranges return an empty frame.
Reversed dates are rejected before network access.

Network failures, HTTP 400 during download, and parsing errors abort the
request even if earlier days succeeded. Snapshot context cannot recover
expired files and does not replace the explicitly supplied interval.

## Products

`boi`, `milho`, `cafe_arabica`, `cafe_conillon`, `etanol`, `soja_cross`, `soja_fob`

> `soja_fob` has no open interest data (SOY absent from `TICKERS_AGRO_OI`).

## Contracts

### `tipo="ajustes"` / `tipo="historico"` → `AJUSTE_DIARIO_V1`

PK: `[data, ticker, vencimento_codigo]`

| Column | Type | Nullable |
|--------|------|----------|
| `data` | DATE | N |
| `ticker` | STRING | N |
| `descricao` | STRING | Y |
| `vencimento_codigo` | STRING | N |
| `vencimento_mes` | INTEGER | N |
| `vencimento_ano` | INTEGER | N |
| `ajuste_anterior` | FLOAT | Y |
| `ajuste_atual` | FLOAT | Y |
| `variacao` | FLOAT | Y |
| `ajuste_por_contrato` | FLOAT | Y |
| `unidade` | STRING | Y |

For live cattle (BGI), the `ajuste_atual` of the last trading day is not the contract's final settlement, which uses the
5-business-day average of the Live Cattle Indicator. For corn (CCM), the settlement at expiry is the final settlement.

`ajuste_por_contrato` is the settlement value per contract, in the quote currency: reais for BGI, CCM, CNL and ETH, and dollars
for ICF, SJC and SOY. The currency is the prefix of `unidade` (`BRL` or `USD`); the value is per contract, not per quote unit.
Summing the column across products mixes currencies.

### `tipo="posicoes"` / `tipo="oi_historico"` → `POSICOES_ABERTAS_V1`

PK: `[data, ticker_completo]`

| Column | Type | Nullable |
|--------|------|----------|
| `data` | DATE | N |
| `ticker` | STRING | N |
| `descricao` | STRING | Y |
| `ticker_completo` | STRING | N |
| `vencimento_codigo` | STRING | N |
| `vencimento_mes` | INTEGER | N |
| `vencimento_ano` | INTEGER | N |
| `tipo` | STRING | N |
| `posicoes_abertas` | INTEGER | N |
| `variacao_posicoes` | INTEGER | Y |
| `unidade` | STRING | Y |

`unidade` is the contract's price unit (e.g., `BRL/@`). `posicoes_abertas` and `variacao_posicoes` count contracts and are not expressed in that unit.

## License

`zona_cinza` — B3 is a private company. Public data without clear terms for programmatic access.
