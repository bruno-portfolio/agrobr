# Defensivos API

The defensivos module provides data on pesticides registered in Brazil via Agrofit/MAPA.

## Functions

### `formulados`

Registered formulated (commercial) products.

```python
async def formulados(
    *,
    ingrediente_ativo: str | None = None,
    classe_toxicologica: str | None = None,
    classe_ambiental: str | None = None,
    titular: str | None = None,
    organicos: str | None = None,
    marca: str | None = None,
    formulacao: str | None = None,
    classe: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ingrediente_ativo` | `str \| None` | Filter by active ingredient (contains, case-insensitive) |
| `classe_toxicologica` | `str \| None` | Filter by toxicological class |
| `classe_ambiental` | `str \| None` | Filter by environmental class |
| `titular` | `str \| None` | Filter by holder company |
| `organicos` | `str \| None` | Exact filter: `"SIM"` or `"NAO"` |
| `marca` | `str \| None` | Filter by commercial brand |
| `formulacao` | `str \| None` | Filter by formulation type |
| `classe` | `str \| None` | Filter by class (herbicide, insecticide, fungicide, etc.) |
| `as_polars` | `bool` | Return a polars DataFrame |
| `return_meta` | `bool` | Return a (DataFrame, MetaInfo) tuple |

**Returns:** DataFrame with columns: `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `formulacao`, `classe_toxicologica`, `classe_ambiental`, `organicos`, `modo_de_acao`

**Example:**

```python
from agrobr import defensivos

# All formulated products with glyphosate
df = await defensivos.formulados(ingrediente_ativo="glifosato")

# Organic herbicides only
df = await defensivos.formulados(classe="herbicida", organicos="SIM")
```

---

### `autorizacoes`

Use authorizations by crop and pest.

```python
async def autorizacoes(
    *,
    nr_registro: str | None = None,
    cultura: str | None = None,
    ingrediente_ativo: str | None = None,
    classe: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `nr_registro` | `str \| None` | Exact filter by registration number |
| `cultura` | `str \| None` | Filter by crop (contains, case-insensitive) |
| `ingrediente_ativo` | `str \| None` | Filter by active ingredient |
| `classe` | `str \| None` | Filter by class |
| `as_polars` | `bool` | Return a polars DataFrame |
| `return_meta` | `bool` | Return a (DataFrame, MetaInfo) tuple |

**Returns:** DataFrame with columns: `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `cultura`, `praga`, `praga_nome_comum`, `modalidade_de_emprego`

**Example:**

```python
from agrobr import defensivos

# All products authorized for soybean
df = await defensivos.autorizacoes(cultura="soja")

# Authorizations for a specific product
df = await defensivos.autorizacoes(nr_registro="000190")
```

---

### `tecnicos`

Technical products (active ingredients before formulation).

```python
async def tecnicos(
    *,
    ingrediente_ativo: str | None = None,
    titular: str | None = None,
    classe: str | None = None,
    marca: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `ingrediente_ativo` | `str \| None` | Filter by active ingredient |
| `titular` | `str \| None` | Filter by holder company |
| `classe` | `str \| None` | Filter by class |
| `marca` | `str \| None` | Filter by commercial brand |
| `as_polars` | `bool` | Return a polars DataFrame |
| `return_meta` | `bool` | Return a (DataFrame, MetaInfo) tuple |

**Returns:** DataFrame with columns: `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `grupo_quimico`, `nome_cientifico`, `classe_toxicologica`, `classe_ambiental`

**Example:**

```python
from agrobr import defensivos

# All technical products
df = await defensivos.tecnicos()

# Filter by class
df = await defensivos.tecnicos(classe="inseticida")
```

## Synchronous Version

```python
from agrobr.sync import defensivos

df = defensivos.formulados(ingrediente_ativo="glifosato")
```

## Notes

- Source: [Agrofit/MAPA](https://dados.agricultura.gov.br) — license `livre` (CC-BY 4.0)
- Large CSV (~100MB formulados) — the first download may take a while
- 24h local cache to avoid re-downloads
- ~8K formulated products, ~267K authorizations, ~2.8K technical products
