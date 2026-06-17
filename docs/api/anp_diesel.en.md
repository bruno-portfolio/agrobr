# ANP Diesel API

The ANP Diesel module provides diesel retail price and sales volume data for Brazil, published by the National Petroleum Agency. Namespace: `agrobr.alt.anp_diesel`.

## Functions

### `precos_diesel`

Diesel retail prices by municipality, state or Brazil level.

```python
async def precos_diesel(
    uf: str | None = None,
    municipio: str | None = None,
    produto: str = "DIESEL S10",
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "semanal",
    nivel: str = "municipio",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `uf` | `str \| None` | Filter by state (e.g. SP, MT, PR) |
| `municipio` | `str \| None` | Filter by municipality (case-insensitive substring) |
| `produto` | `str` | "DIESEL" or "DIESEL S10" (default) |
| `inicio` | `str \| date \| None` | Start date (YYYY-MM-DD) |
| `fim` | `str \| date \| None` | End date (YYYY-MM-DD) |
| `agregacao` | `str` | "semanal" (default) or "mensal" |
| `nivel` | `str` | "municipio" (default), "uf" or "brasil" |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `data`, `uf`, `municipio`, `produto`, `preco_venda`,
`preco_compra`, `margem`, `n_postos`

**Example:**

```python
from agrobr.alt import anp_diesel

# DIESEL S10 prices
df = await anp_diesel.precos_diesel()

# Filter by state and period
df = await anp_diesel.precos_diesel(
    uf="MT",
    inicio="2024-01-01",
    fim="2024-06-30",
)

# State level with monthly aggregation
df = await anp_diesel.precos_diesel(nivel="uf", agregacao="mensal")
```

### `vendas_diesel`

Diesel sales volumes by state (monthly).

```python
async def vendas_diesel(
    uf: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `uf` | `str \| None` | Filter by state (e.g. SP, MT, PR) |
| `inicio` | `str \| date \| None` | Start date |
| `fim` | `str \| date \| None` | End date |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `data`, `uf`, `regiao`, `produto`, `volume_m3`

**Example:**

```python
from agrobr.alt import anp_diesel

# Diesel volumes
df = await anp_diesel.vendas_diesel()

# Filter by state
df = await anp_diesel.vendas_diesel(uf="MT")
```

## Synchronous Version

```python
from agrobr.sync import alt

df = alt.anp_diesel.precos_diesel(uf="MT")
df = alt.anp_diesel.vendas_diesel()
```

## Notes

- Source: [ANP Gov.br](https://www.gov.br/anp/) — `livre` license (Decree 8,777/2016)
- Data: bulk XLSX (prices 2013+), XLS (volumes)
- Municipality price XLSX files can be large (50-100MB) — cached per file period
- Cache TTL: 7 days
