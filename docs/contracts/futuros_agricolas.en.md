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

### `tipo="posicoes"` → `POSICOES_ABERTAS_V1`

PK: `[data, ticker_completo]`

| Column | Type | Nullable |
|--------|------|----------|
| `data` | DATE | N |
| `ticker` | STRING | N |
| `descricao` | STRING | Y |
| `ticker_completo` | STRING | N |
| `vencimento_codigo` | STRING | N |
| `vencimento_mes` | INTEGER | Y |
| `vencimento_ano` | INTEGER | Y |
| `tipo` | STRING | N |
| `posicoes_abertas` | INTEGER | N |
| `variacao_posicoes` | INTEGER | Y |
| `unidade` | STRING | Y |

## License

`zona_cinza` — B3 is a private company. Public data without clear terms for programmatic access.
