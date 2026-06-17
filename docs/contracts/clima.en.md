# clima

Monthly climate data by state or daily by station.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | INMET | Automatic stations aggregated by state |
| 2 | NASA POWER | Satellite reanalysis by state (fallback) |

## Modes

### State mode (multi-source, automatic fallback)

```python
df = await datasets.clima(uf="SP", ano=2024)
```

### Station mode (INMET only)

```python
df = await datasets.clima(estacao="A301", inicio="2024-01-01", fim="2024-12-31")
```

## Contract `CLIMA_V1` (state mode)

PK: `[mes, uf]`

| Column | Type | Nullable | INMET | NASA | Unit |
|--------|------|----------|-------|------|------|
| `mes` | DATE | N | ✅ | ✅ | — |
| `uf` | STRING | N | ✅ | ✅ | — |
| `precip_acum_mm` | FLOAT | N | ✅ | ✅ | mm |
| `temp_media` | FLOAT | N | ✅ | ✅ | °C |
| `temp_max_media` | FLOAT | N | ✅ | ✅ | °C |
| `temp_min_media` | FLOAT | N | ✅ | ✅ | °C |
| `num_estacoes` | INTEGER | Y | ✅ | — | — |
| `umidade_media` | FLOAT | Y | — | ✅ | % |
| `radiacao_media_mj` | FLOAT | Y | — | ✅ | MJ/m² |
| `vento_medio_ms` | FLOAT | Y | — | ✅ | m/s |
| `fonte` | STRING | N | ✅ | ✅ | — |

## Contract `CLIMA_ESTACAO_V1` (station mode)

PK: `[data, estacao]`

| Column | Type | Nullable |
|--------|------|----------|
| `data` | DATE | N |
| `estacao` | STRING | N |
| `uf` | STRING | Y |
| `temp_media` | FLOAT | Y |
| `temp_max` | FLOAT | Y |
| `temp_min` | FLOAT | Y |
| `precipitacao_mm` | FLOAT | Y |
| `umidade_media` | FLOAT | Y |
| `radiacao_total_kj_m2` | FLOAT | Y |

Validation only for `agregacao="diario"`.

## License

`livre` — both sources are public and free.
