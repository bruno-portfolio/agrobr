# RNC API

The rnc module provides data on cultivars registered and protected in Brazil via CultivarWeb/MAPA.

## Functions

### `registradas`

Cultivars registered in the RNC (Registro Nacional de Cultivares).

```python
async def registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `cultivar` | `str \| None` | Filter by cultivar name (contains, case-insensitive) |
| `especie` | `str \| None` | Filter by species / common name |
| `grupo` | `str \| None` | Filter by group |
| `situacao` | `str \| None` | Filter by status (e.g. "REGISTRADA") |
| `mantenedor` | `str \| None` | Filter by maintainer |
| `as_polars` | `bool` | Return a polars DataFrame |
| `return_meta` | `bool` | Return a (DataFrame, MetaInfo) tuple |

**Returns:** DataFrame with columns: `cultivar`, `nome_comum`, `nome_cientifico`, `grupo`, `situacao`, `nr_formulario`, `nr_registro`, `data_registro`, `data_validade`, `mantenedor`

**Example:**

```python
from agrobr import rnc

# All soybean cultivars
df = await rnc.registradas(especie="soja")

# Embrapa cultivars
df = await rnc.registradas(mantenedor="Embrapa")
```

---

### `protegidas`

Cultivars with intellectual property protection (SNPC).

```python
async def protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `cultivar` | `str \| None` | Filter by cultivar name (contains, case-insensitive) |
| `especie` | `str \| None` | Filter by species / common name |
| `situacao` | `str \| None` | Filter by status (e.g. "PROTECAO DEFINITIVA") |
| `titular` | `str \| None` | Filter by protection holder |
| `as_polars` | `bool` | Return a polars DataFrame |
| `return_meta` | `bool` | Return a (DataFrame, MetaInfo) tuple |

**Returns:** DataFrame with columns: `cultivar`, `nome_cientifico`, `nome_comum`, `nr_processo`, `situacao`, `nr_certificado`, `inicio_protecao`, `termino_protecao`, `titular`, `representante_legal`, `melhoristas`

**Example:**

```python
from agrobr import rnc

# All protected cultivars
df = await rnc.protegidas()

# Filter by holder
df = await rnc.protegidas(titular="Embrapa")
```

## Synchronous Version

```python
from agrobr.sync import rnc

df = rnc.registradas(especie="soja")
df = rnc.protegidas(titular="Embrapa")
```

## Notes

- Source: [CultivarWeb/MAPA](https://sistemas.agricultura.gov.br/snpc/cultivarweb) — license `livre` (federal government public data)
- Access via 2 POSTs with a session (empty search + CSV export)
- User-Agent required
- Dates in DD/MM/YYYY format (converted automatically)
- ~37K registered cultivars, ~5K protected cultivars
