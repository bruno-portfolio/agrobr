# API ANP Diesel

O modulo ANP Diesel fornece dados de precos de revenda e volumes de venda de diesel no Brasil, publicados pela Agencia Nacional do Petroleo. Namespace: `agrobr.alt.anp_diesel`.

## Funcoes

### `precos_diesel`

Precos de revenda de diesel por municipio, UF ou nivel Brasil.

```python
async def precos_diesel(
    uf: str | None = None,
    municipio: str | None = None,
    produto: str = "DIESEL S10",
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "semanal",
    nivel: str = "municipio",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str \| None` | Filtro por UF (ex: SP, MT, PR) |
| `municipio` | `str \| None` | Filtro por municipio (substring case-insensitive) |
| `produto` | `str` | "DIESEL" ou "DIESEL S10" (default) |
| `inicio` | `str \| date \| None` | Data inicial (YYYY-MM-DD) |
| `fim` | `str \| date \| None` | Data final (YYYY-MM-DD) |
| `agregacao` | `str` | "semanal" (default) ou "mensal" |
| `nivel` | `str` | "municipio" (default), "uf" ou "brasil" |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `data`, `uf`, `municipio`, `produto`, `preco_venda`,
`preco_compra`, `margem`, `n_postos`

**Exemplo:**

```python
from agrobr.alt import anp_diesel

# Precos de DIESEL S10
df = await anp_diesel.precos_diesel()

# Filtrar por UF e periodo
df = await anp_diesel.precos_diesel(
    uf="MT",
    inicio="2024-01-01",
    fim="2024-06-30",
)

# Nivel UF com agregacao mensal
df = await anp_diesel.precos_diesel(nivel="uf", agregacao="mensal")
```

### `vendas_diesel`

Volumes de venda de diesel por UF (mensal).

```python
async def vendas_diesel(
    uf: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str \| None` | Filtro por UF (ex: SP, MT, PR) |
| `inicio` | `str \| date \| None` | Data inicial |
| `fim` | `str \| date \| None` | Data final |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `data`, `uf`, `regiao`, `produto`, `volume_m3`

**Exemplo:**

```python
from agrobr.alt import anp_diesel

# Volumes de diesel
df = await anp_diesel.vendas_diesel()

# Filtrar por UF
df = await anp_diesel.vendas_diesel(uf="MT")
```

## Versao Sincrona

```python
from agrobr.sync import alt

df = alt.anp_diesel.precos_diesel(uf="MT")
df = alt.anp_diesel.vendas_diesel()
```

## Notas

- Fonte: [ANP Gov.br](https://www.gov.br/anp/) — licenca `livre` (Decreto 8.777/2016)
- Dados: XLSX bulk (precos 2013+), XLS (volumes)
- XLSX de precos por municipio podem ser grandes (50-100MB) — cache por periodo do arquivo
- TTL cache: 7 dias
