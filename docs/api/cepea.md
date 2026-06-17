# API CEPEA

O módulo CEPEA fornece acesso aos indicadores de preços do Centro de Estudos Avançados em Economia Aplicada (ESALQ/USP).

## Funções

### `indicador`

Obtém série histórica de indicadores de preço.

```python
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Produto CEPEA (21 disponíveis). Veja `produtos()` para lista completa |
| `praca` | `str \| None` | Praça de cotação. `None` retorna todas |
| `inicio` | `str \| date \| None` | Data inicial (YYYY-MM-DD). Default: 365 dias atrás |
| `fim` | `str \| date \| None` | Data final. Default: hoje |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `validate_sanity` | `bool` | Executar validação estatística (outliers, gaps). Default: `False` |
| `force_refresh` | `bool` | Ignorar cache e buscar dados frescos |
| `offline` | `bool` | Usar apenas cache/histórico local |
| `return_meta` | `bool` | Retorna tupla `(df, MetaInfo)` com proveniência |

**Retorno:**

DataFrame com colunas:
- `data`: Data do indicador
- `produto`: Nome do produto
- `praca`: Praça de cotação
- `valor`: Valor em R$/unidade
- `unidade`: Unidade (ex: 'BRL/sc60kg')
- `fonte`: Fonte dos dados ('cepea' ou 'noticias_agricolas')
- `metodologia`: Metodologia do indicador
- `anomalies`: Anomalias/marcadores detectados (ex: `media_semanal` do fallback, ou outliers do `validate_sanity`); `None` quando vazio

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
    praca: str | None = None,
    offline: bool = False,
) -> Indicador
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Produto desejado |
| `praca` | `str \| None` | Praça de cotação. `None` não filtra por praça |
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
# ['soja', 'soja_parana', 'milho', 'cafe', 'cafe_arabica', 'cafe_robusta',
#  'boi', 'boi_gordo', 'trigo', 'algodao', 'arroz', 'acucar', 'acucar_refinado',
#  'frango_congelado', 'frango_resfriado', 'suino', 'etanol_hidratado',
#  'etanol_anidro', 'leite', 'laranja_industria', 'laranja_in_natura']
# 'cafe'/'cafe_arabica' = Arábica (SP); 'cafe_robusta' = Robusta/Conilon (ES)
# Aliases: boi_gordo → boi, cafe_arabica → cafe
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

Lista de praças disponíveis — vazia para produto válido sem praças mapeadas. Produto desconhecido levanta `ValueError`.

---

## Modelos

### `Indicador`

```python
class Indicador(BaseModel):
    fonte: Fonte
    produto: str = Field(..., min_length=2)
    praca: str | None = None
    data: date
    valor: Decimal = Field(..., gt=0)
    unidade: str
    metodologia: str | None = None
    revisao: int = Field(default=0, ge=0)
    meta: dict[str, Any] = Field(default_factory=dict)
    parsed_at: datetime = Field(default_factory=utcnow)
    parser_version: int = Field(default=1)
    anomalies: list[str] = Field(default_factory=list)
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

1. **Cache fresh**: Retorna imediatamente do cache (smart expiry — válido até as 18h BRT, horário de atualização do CEPEA)
2. **Cache stale**: Tenta atualizar, mas retorna cache se falhar
3. **Sem cache**: Busca da fonte e salva no cache

O histórico é acumulado progressivamente no DuckDB local, permitindo consultas a períodos antigos sem novas requisições.

## Fallback

Quando o CEPEA está indisponível (Cloudflare), o agrobr automaticamente usa o Notícias Agrícolas como fonte alternativa, que republica os mesmos indicadores CEPEA/ESALQ.
