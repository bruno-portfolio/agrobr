# ANTT Toll

Vehicle flow at highway toll plazas.

## `fluxo_pedagio`

```python
from agrobr.alt import antt_pedagio

# Full year flow
df = await antt_pedagio.fluxo_pedagio(ano=2023)

# Heavy vehicles only (proxy for grain transport)
df = await antt_pedagio.fluxo_pedagio(
    ano_inicio=2022,
    ano_fim=2023,
    apenas_pesados=True,
    uf="MT",
)

# With metadata
df, meta = await antt_pedagio.fluxo_pedagio(ano=2023, return_meta=True)
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ano` | `int \| None` | `None` | Single year |
| `ano_inicio` | `int \| None` | `None` | Range start year |
| `ano_fim` | `int \| None` | `None` | Range end year |
| `concessionaria` | `str \| None` | `None` | Partial match |
| `rodovia` | `str \| None` | `None` | E.g. "BR-163" |
| `uf` | `str \| None` | `None` | E.g. "MT" |
| `praca` | `str \| None` | `None` | Partial match |
| `tipo_veiculo` | `str \| None` | `None` | "Passeio"/"Comercial"/"Moto" |
| `apenas_pesados` | `bool` | `False` | n_eixos >= 3 AND Comercial |
| `as_polars` | `bool` | `False` | Return as polars.DataFrame |
| `return_meta` | `bool` | `False` | Returns MetaInfo |

### Output columns

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `data` | DATE | N | 1st day of the month |
| `concessionaria` | STRING | N | |
| `praca` | STRING | N | |
| `sentido` | STRING | Y | Crescente/Decrescente |
| `n_eixos` | INTEGER | N | 2-18 |
| `tipo_veiculo` | STRING | Y | Passeio/Comercial/Moto |
| `volume` | INTEGER | N | >= 0 |
| `rodovia` | STRING | Y | From join with registry |
| `uf` | STRING | Y | From join with registry |
| `municipio` | STRING | Y | From join with registry |

## `pracas_pedagio`

```python
from agrobr.alt import antt_pedagio

# All plazas
df = await antt_pedagio.pracas_pedagio()

# Filter by state
df = await antt_pedagio.pracas_pedagio(uf="SP")

# Filter by highway
df = await antt_pedagio.pracas_pedagio(rodovia="BR-163")
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `uf` | `str \| None` | `None` | State filter |
| `rodovia` | `str \| None` | `None` | Highway filter |
| `situacao` | `str \| None` | `None` | E.g. "Ativa" |
| `as_polars` | `bool` | `False` | Return as polars.DataFrame |
| `return_meta` | `bool` | `False` | Returns MetaInfo |

## Synchronous Usage

```python
from agrobr import sync

df = sync.alt.antt_pedagio.fluxo_pedagio(ano=2023, apenas_pesados=True)
df_pracas = sync.alt.antt_pedagio.pracas_pedagio(uf="SP")
```
