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
df = await datasets.posicionamento_fundos("milho", inicio="2026-01-01")
df = await datasets.posicionamento_fundos("acucar", combinado=True)  # futures + options
```

`inicio` and `fim` accept `date`, `datetime`, and `YYYY-MM-DD` or `DD/MM/YYYY` text; any other format, or `inicio`
after `fim`, raises `InvalidParameterError` before the request.

## Contract `cftc.cot` v2.0

PK: `[data, codigo_cftc]` — effective from 2.0.0

| Column | Type | Nullable | Unit |
|--------|------|----------|------|
| `data` | DATE | N | — |
| `produto` | STRING | N | — |
| `contrato` | STRING | N | — |
| `codigo_cftc` | STRING | N | — |
| `posicoes_abertas` | INTEGER | N | contracts |
| `fundos_compra` | INTEGER | N | contracts |
| `fundos_venda` | INTEGER | N | contracts |
| `fundos_spread` | INTEGER | N | contracts |
| `fundos_saldo` | INTEGER | N | contracts |
| `produtores_compra` | INTEGER | N | contracts |
| `produtores_venda` | INTEGER | N | contracts |
| `swap_compra` | INTEGER | N | contracts |
| `swap_venda` | INTEGER | N | contracts |
| `swap_spread` | INTEGER | N | contracts |
| `outros_compra` | INTEGER | N | contracts |
| `outros_venda` | INTEGER | N | contracts |
| `outros_spread` | INTEGER | N | contracts |
| `nao_reportaveis_compra` | INTEGER | N | contracts |
| `nao_reportaveis_venda` | INTEGER | N | contracts |
| `variacao_fundos_compra` | INTEGER | Y | contracts |
| `variacao_fundos_venda` | INTEGER | Y | contracts |
| `variacao_posicoes` | INTEGER | Y | contracts |

The `variacao_*` columns are null in the first week of each contract in the
series (no prior week for the delta).

In 2.0, the columns moved to Portuguese. The `cftc.cot` source keeps the report's names (`open_interest`,
`managed_money_long`…); the map between the two is `agrobr.contracts.datasets.POSICIONAMENTO_FUNDOS_COLUNAS_V2`, and
the from/to table is in the migration guide.

## Semantics

- `fundos_*` — managed money, the funds (the "fund positioning" cited by the agri market)
- `produtores_*` — producer/merchant, the commercial hedgers (producers, processors, trading firms)
- `swap_*` — swap dealers; `outros_*` — other reportables; `nao_reportaveis_*` — nonreportable
- `compra`/`venda` are the long and short positions; `spread`, the spread positions
- `fundos_saldo` = `fundos_compra` − `fundos_venda` (computed)
- `posicoes_abertas` = longs (`produtores_compra` + `swap_compra` + `fundos_compra` + `outros_compra` +
  `nao_reportaveis_compra`) + spreads (`swap_spread` + `fundos_spread` + `outros_spread`), and likewise for shorts.
  The identity is exact for futures; in the combined report (`combinado=True`) the CFTC itself leaves up to 1 contract
  of residual. `swap_spread` and `outros_spread` were added in version 1.1 (before, the OI did not close with the
  categories)
- Positions in number of contracts; `data` is the report's reference Tuesday

## Deterministic Mode

In deterministic mode (`datasets.deterministic()`), the snapshot sets the query
`fim` when not provided — an explicit `fim` takes precedence.
