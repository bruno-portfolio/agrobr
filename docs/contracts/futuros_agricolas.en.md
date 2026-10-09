# futuros_agricolas

B3 agricultural futures — daily settlements, history and open interest.

## Source

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | B3 | Brazilian stock exchange |

## Modes (`tipo=`)

### Settlements (default)

```python
from agrobr import datasets

df = await datasets.futuros_agricolas("boi", data="2025-03-05")
```

### History

```python
df = await datasets.futuros_agricolas("boi", tipo="historico", inicio="2025-01-01", fim="2025-03-05")
```

`vencimento` takes only the contract month code (e.g. `V26`): settlements carry only futures, and any other format,
an option code included, is rejected before the network.

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

Provide a product and inclusive start/end dates in `YYYY-MM-DD` or `DD/MM/YYYY` (or `date`); `data` does
not apply to this mode. `vencimento` takes the contract month code (e.g.
`V26`), which returns the future and the options of that month, or an option's
published code (e.g. `VVJK`); any other format is rejected before the network.
Weekdays are queried sequentially; both
futures and options are returned and identified by the `tipo` column.

`data` with `tipo="historico"` or `"oi_historico"`, and `inicio`, `fim`, or `vencimento` with `"ajustes"` or
`"posicoes"`, raise `InvalidParameterError` before the network: the argument that does not apply to the type used to
be silently dropped. These two types return every expiry of the session; for a single one, filter the
`vencimento_codigo` column.
For every type, `data`, `inicio`, and `fim` outside the accepted formats, and `inicio` after `fim`, also raise
`InvalidParameterError` before the network.

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
average of 5 publications of the settlement index: the DATAGRO Live Cattle Indicator from the February 2025 expiry (BGIG25)
onward, and the CEPEA/B3 Live Cattle Indicator up to the January 2025 expiry (BGIF25), per B3 Circular Letter 135/2024-PRE.
agrobr's `preco_diario` publishes the CEPEA indicator, not DATAGRO's. For corn (CCM), the settlement at expiry is the final
settlement.

`ajuste_por_contrato` is the settlement value per contract in reais for every contract, including the dollar-quoted ones
(ICF, SJC and SOY): it is the value B3 publishes, already converted. On 2026-09-15, ICF K27 changed by −7.75 USD/bag
(100 bags, US$ −775) and `ajuste_por_contrato` was −3,991.79. The prefix of `unidade` (`BRL` or `USD`) is the currency of
`ajuste_anterior`, `ajuste_atual` and `variacao`, not of this column. The value is per contract, not per quote unit.

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

Classification: `zona_cinza`. The D-1 FAQ waiver and website terms have different scopes; check channel, use and policy dates in [Licenses](../licenses.md#b3-brasil-bolsa-balcao).
