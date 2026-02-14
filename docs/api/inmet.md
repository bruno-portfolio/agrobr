# API INMET

O modulo INMET fornece dados meteorologicos de 600+ estacoes do Instituto Nacional de Meteorologia.

## Token

Dados observacionais requerem token:

```bash
export AGROBR_INMET_TOKEN=seu_token
```

A listagem de estacoes funciona sem token.

## Funcoes

### `estacoes`

Lista estacoes meteorologicas disponiveis.

```python
async def estacoes(
    tipo: str = "T",
    uf: str | None = None,
    apenas_operantes: bool = True,
) -> pd.DataFrame
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `tipo` | `str` | `"T"` para automaticas, `"M"` para convencionais |
| `uf` | `str \| None` | Filtrar por UF |
| `apenas_operantes` | `bool` | Se True, retorna apenas estacoes ativas |

**Retorno:**

DataFrame com colunas: `codigo`, `nome`, `uf`, `situacao`, `tipo`, `latitude`, `longitude`, `altitude`, `inicio_operacao`

---

### `estacao`

Dados observacionais de uma estacao especifica.

```python
async def estacao(
    codigo: str,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "horario",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `codigo` | `str` | Codigo da estacao (ex: `"A001"`) |
| `inicio` | `str \| date` | Data inicial (YYYY-MM-DD) |
| `fim` | `str \| date` | Data final (YYYY-MM-DD) |
| `agregacao` | `str` | `"horario"` (default) ou `"diario"` |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com observacoes meteorologicas (temperatura, precipitacao, umidade, vento, radiacao, pressao).

---

### `clima_uf`

Clima agregado por UF a partir de todas as estacoes do estado.

```python
async def clima_uf(
    uf: str,
    ano: int,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str` | Sigla UF (ex: `"MT"`, `"SP"`) |
| `ano` | `int` | Ano de referencia |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `mes`, `uf`, `precip_acum_mm`, `temp_media`, `temp_max_media`, `temp_min_media`, `num_estacoes`

**Exemplo:**

```python
from agrobr import inmet

# Listar estacoes do MT
est = await inmet.estacoes(uf="MT")

# Dados de uma estacao
df = await inmet.estacao("A001", inicio="2024-01-01", fim="2024-01-31")

# Clima mensal por UF
df = await inmet.clima_uf("MT", 2024)
```

## Versao Sincrona

```python
from agrobr.sync import inmet

est = inmet.estacoes(uf="MT")
df = inmet.clima_uf("MT", 2024)
```

## Notas

- Fonte: [INMET](https://portal.inmet.gov.br) — licenca livre
- 600+ estacoes automaticas e convencionais
- Para dados sem token, use [NASA POWER](nasa_power.md) como alternativa
