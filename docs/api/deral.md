# API DERAL

O modulo DERAL fornece dados de condicao de lavouras, progresso de plantio e colheita do Departamento de Economia Rural do Parana.

## Funcoes

### `condicao_lavouras`

Condicao semanal das lavouras paranaenses.

```python
async def condicao_lavouras(
    produto: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str \| None` | Filtrar por produto (`"soja"`, `"milho"`, `"milho_1"`, `"milho_2"`, `"trigo"`, `"feijao"`, `"cana"`, `"cafe"`, etc.). None retorna todos |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `produto`, `data`, `condicao`, `pct`, `plantio_pct`, `colheita_pct`

**Exemplo:**

```python
from agrobr import deral

# Todas as lavouras
df = await deral.condicao_lavouras()

# Apenas soja
df = await deral.condicao_lavouras("soja")

# Com metadados
df, meta = await deral.condicao_lavouras("milho", return_meta=True)
```

## Versao Sincrona

```python
from agrobr.sync import deral

df = deral.condicao_lavouras("soja")
```

## Notas

- Fonte: [DERAL/SEAB-PR](https://www.agricultura.pr.gov.br) — licenca livre
- Dados exclusivos do Parana
- Publicado em Excel (PC.xls) — layout pode variar entre safras
