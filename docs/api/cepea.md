# API CEPEA

O módulo CEPEA fornece acesso aos indicadores de preços do Centro de Estudos Avançados em Economia Aplicada (ESALQ/USP).

## Funções

### `indicador`

Obtém série histórica de indicadores de preço.

```python
async def indicador(
    produto: str,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    moeda: str = 'BRL',
    as_polars: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    validate_sanity: bool = True,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Produto: 'soja', 'milho', 'boi_gordo', 'cafe', 'algodao', 'trigo' |
| `inicio` | `str \| date \| None` | Data inicial (YYYY-MM-DD). Default: 30 dias atrás |
| `fim` | `str \| date \| None` | Data final. Default: hoje |
| `moeda` | `str` | Moeda: 'BRL' ou 'USD'. Default: 'BRL' |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `force_refresh` | `bool` | Ignorar cache e buscar dados frescos |
| `offline` | `bool` | Usar apenas cache/histórico local |
| `validate_sanity` | `bool` | Executar validação estatística |

**Retorno:**

DataFrame com colunas:
- `data`: Data do indicador
- `produto`: Nome do produto
- `valor`: Valor em R$/unidade
- `variacao_pct`: Variação percentual (quando disponível)
- `fonte`: Fonte dos dados ('cepea' ou 'noticias_agricolas')

**Exemplo:**

```python
from agrobr import cepea

# Básico
df = await cepea.indicador('soja')

# Com período
df = await cepea.indicador(
    'soja',
    inicio='2024-01-01',
    fim='2024-06-30'
)

# Forçar atualização
df = await cepea.indicador('soja', force_refresh=True)

# Modo offline (sem network)
df = await cepea.indicador('soja', offline=True)
```

---

### `ultimo`

Obtém o indicador mais recente disponível.

```python
async def ultimo(
    produto: str,
    offline: bool = False,
) -> Indicador
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Produto desejado |
| `offline` | `bool` | Usar apenas cache local |

**Retorno:**

Objeto `Indicador` com:
- `data`: Data do indicador
- `valor`: Valor em Decimal
- `unidade`: Unidade (ex: 'BRL/sc60kg')
- `produto`: Nome do produto
- `fonte`: Fonte dos dados

**Exemplo:**

```python
from agrobr import cepea

ultimo = await cepea.ultimo('soja')
print(f"Soja em {ultimo.data}: R$ {ultimo.valor}/sc")
```

---

### `produtos`

Lista produtos disponíveis.

```python
async def produtos() -> list[str]
```

**Retorno:**

Lista de strings com nomes dos produtos.

**Exemplo:**

```python
from agrobr import cepea

prods = await cepea.produtos()
# ['soja', 'milho', 'boi_gordo', 'cafe', 'algodao', 'trigo']
```

---

### `pracas`

Lista praças disponíveis para um produto.

```python
async def pracas(produto: str) -> list[str]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Produto |

**Retorno:**

Lista de praças disponíveis.

---

## Modelos

### `Indicador`

```python
class Indicador(BaseModel):
    fonte: Fonte
    produto: str
    praca: str | None
    data: date
    valor: Decimal
    unidade: str
    metodologia: str | None
    revisao: int = 0
    meta: dict[str, Any] = {}
    parsed_at: datetime
    parser_version: int = 1
    anomalies: list[str] = []
```

## Versão Síncrona

```python
from agrobr.sync import cepea

# Mesmas funções, sem async/await
df = cepea.indicador('soja')
ultimo = cepea.ultimo('milho')
produtos = cepea.produtos()
```

## Comportamento de Cache

1. **Cache fresh**: Retorna imediatamente do cache (< 4h para CEPEA diário)
2. **Cache stale**: Tenta atualizar, mas retorna cache se falhar
3. **Sem cache**: Busca da fonte e salva no cache

O histórico é acumulado progressivamente no DuckDB local, permitindo consultas a períodos antigos sem novas requisições.

## Fallback

Quando o CEPEA está indisponível (Cloudflare), o agrobr automaticamente usa o Notícias Agrícolas como fonte alternativa, que republica os mesmos indicadores CEPEA/ESALQ.
