# API ANDA

O modulo ANDA fornece dados de entregas de fertilizantes por UF e mes, publicados pela Associacao Nacional para Difusao de Adubos.

!!! warning "Licenca zona_cinza"
    Termos de uso nao localizados publicamente. Autorizacao formal solicitada em fev/2026 — aguardando resposta.

## Dependencia

Requer `pdfplumber`:

```bash
pip install agrobr[pdf]
```

## Funcoes

### `entregas`

Volume de entregas de fertilizantes por UF e mes.

```python
async def entregas(
    ano: int,
    *,
    uf: str | None = None,
    produto: str = "total",
    agregacao: str = "detalhado",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ano` | `int` | Ano de referencia (2010+) |
| `uf` | `str \| None` | Filtrar por UF. None retorna todos |
| `produto` | `str` | Tipo de fertilizante. Default: `"total"` |
| `agregacao` | `str` | `"detalhado"` (por UF/mes) ou `"mensal"` (soma por mes) |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `ano`, `mes`, `uf`, `produto_fertilizante`, `volume_ton`

**Exemplo:**

```python
from agrobr import anda

# Entregas 2024
df = await anda.entregas(2024)

# Filtrar por UF
df = await anda.entregas(2024, uf="MT")

# Agregado mensal
df = await anda.entregas(2024, agregacao="mensal")
```

## Versao Sincrona

```python
from agrobr.sync import anda

df = anda.entregas(2024)
```

## Notas

- Fonte: [ANDA](https://anda.org.br) — licenca `zona_cinza`
- Dados extraidos de PDF via `pdfplumber`
- Dados disponiveis a partir de 2009
