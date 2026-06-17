# API Defensivos

O modulo defensivos fornece dados de agrotoxicos registrados no Brasil via Agrofit/MAPA.

## Funcoes

### `formulados`

Produtos formulados (comerciais) registrados.

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

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ingrediente_ativo` | `str \| None` | Filtro por ingrediente ativo (contains, case-insensitive) |
| `classe_toxicologica` | `str \| None` | Filtro por classe toxicologica |
| `classe_ambiental` | `str \| None` | Filtro por classe ambiental |
| `titular` | `str \| None` | Filtro por empresa titular |
| `organicos` | `str \| None` | Filtro exato: `"SIM"` ou `"NAO"` |
| `marca` | `str \| None` | Filtro por marca comercial |
| `formulacao` | `str \| None` | Filtro por tipo de formulacao |
| `classe` | `str \| None` | Filtro por classe (herbicida, inseticida, fungicida, etc.) |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

**Retorno:** DataFrame com colunas: `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `formulacao`, `classe_toxicologica`, `classe_ambiental`, `organicos`, `modo_de_acao`

**Exemplo:**

```python
from agrobr import defensivos

# Todos os formulados com glifosato
df = await defensivos.formulados(ingrediente_ativo="glifosato")

# Apenas herbicidas organicos
df = await defensivos.formulados(classe="herbicida", organicos="SIM")
```

---

### `autorizacoes`

Autorizacoes de uso por cultura e praga.

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

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `nr_registro` | `str \| None` | Filtro exato por numero de registro |
| `cultura` | `str \| None` | Filtro por cultura (contains, case-insensitive) |
| `ingrediente_ativo` | `str \| None` | Filtro por ingrediente ativo |
| `classe` | `str \| None` | Filtro por classe |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

**Retorno:** DataFrame com colunas: `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `cultura`, `praga`, `praga_nome_comum`, `modalidade_de_emprego`

**Exemplo:**

```python
from agrobr import defensivos

# Todos os produtos autorizados para soja
df = await defensivos.autorizacoes(cultura="soja")

# Autorizacoes de um produto especifico
df = await defensivos.autorizacoes(nr_registro="000190")
```

---

### `tecnicos`

Produtos tecnicos (ingredientes ativos antes da formulacao).

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

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ingrediente_ativo` | `str \| None` | Filtro por ingrediente ativo |
| `titular` | `str \| None` | Filtro por empresa titular |
| `classe` | `str \| None` | Filtro por classe |
| `marca` | `str \| None` | Filtro por marca comercial |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

**Retorno:** DataFrame com colunas: `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `grupo_quimico`, `nome_cientifico`, `classe_toxicologica`, `classe_ambiental`

**Exemplo:**

```python
from agrobr import defensivos

# Todos os tecnicos
df = await defensivos.tecnicos()

# Filtrar por classe
df = await defensivos.tecnicos(classe="inseticida")
```

## Versao Sincrona

```python
from agrobr.sync import defensivos

df = defensivos.formulados(ingrediente_ativo="glifosato")
```

## Notas

- Fonte: [Agrofit/MAPA](https://dados.agricultura.gov.br) — licenca `livre` (CC-BY 4.0)
- CSV grande (~100MB formulados) — primeiro download pode demorar
- Cache local 24h para evitar re-downloads
- ~8K produtos formulados, ~267K autorizacoes, ~2.8K tecnicos
