# API ABIOVE

O modulo ABIOVE fornece dados de exportacao do complexo soja — grao, farelo, oleo e milho — publicados pela Associacao Brasileira das Industrias de Oleos Vegetais.

!!! warning "Licenca zona_cinza"
    Termos de uso nao localizados publicamente. Autorizacao formal solicitada em fev/2026 — aguardando resposta.

## Funcoes

### `exportacao`

Volumes e receita de exportacao do complexo soja.

```python
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ano` | `int` | Ano de referencia |
| `mes` | `int \| None` | Mes especifico (1-12). None retorna todos |
| `produto` | `str \| None` | Filtrar: `"grao"`, `"farelo"`, `"oleo"`, `"milho"` |
| `agregacao` | `str` | `"detalhado"` (por produto/mes) ou `"mensal"` (soma) |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `ano`, `mes`, `produto`, `volume_ton`, `receita_usd_mil`

**Exemplo:**

```python
from agrobr import abiove

# Exportacao completa 2024
df = await abiove.exportacao(2024)

# Apenas farelo
df = await abiove.exportacao(2024, produto="farelo")

# Mes especifico
df = await abiove.exportacao(2024, mes=6)
```

## Versao Sincrona

```python
from agrobr.sync import abiove

df = abiove.exportacao(2024)
```

## Notas

- Fonte: [ABIOVE](https://abiove.org.br) — licenca `zona_cinza`
- Dados em Excel com formato multi-secao
