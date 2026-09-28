# API CEPEA

O módulo CEPEA fornece acesso aos indicadores de preços do Centro de Estudos Avançados em Economia Aplicada (ESALQ/USP).

## Funções

### `indicador`

Obtém série histórica de indicadores de preço.

A página publica uma janela recente, normalmente cerca de 15 pregões; o período
anterior vem da série histórica do CEPEA, baixada inteira na primeira vez e guardada
no cache (a laranja não tem série; ver [a fonte](../sources/cepea.md#cobertura-e-selecao-das-series)). Etanol é semanal; leite é mensal, com
`data` no primeiro dia do mês de referência e `praca` por UF.

```python
async def indicador(
    produto: str,
    praca: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    _moeda: str = "BRL",
    as_polars: bool = False,
    validate_sanity: bool = False,
    force_refresh: bool = False,
    offline: bool = False,
    *,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Produto CEPEA (22 disponíveis). Veja `produtos()` para lista completa |
| `praca` | `str \| None` | Praça de cotação. Aceita o slug de `pracas()` ou o rótulo exibido pela fonte; `None` retorna todas |
| `inicio` | `str \| date \| None` | Data inicial (YYYY-MM-DD). Default: 365 dias atrás |
| `fim` | `str \| date \| None` | Data final. Default: hoje |
| `_moeda` | `str` | Reservado para conversão futura de moeda; atualmente não altera o resultado |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `validate_sanity` | `bool` | Conferir unidade, faixa de preço e variação temporal quando houver regra. Default: `False` |
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
- `anomalies`: Texto JSON com a lista de anomalias/marcadores (ex: `["out_of_range: valor"]`); `None` quando vazio. Use `json.loads()` para recuperar a lista. O modelo `Indicador` retornado por `ultimo()` mantém `list[str]`.
- `valor_usd`: Preço em US$ publicado na mesma linha pelo CEPEA; `NaN` quando a fonte não divulga (fallback Notícias Agrícolas, histórico em cache anterior à migração 10)
- `peso_medio_kg`: Peso médio do animal na tabela auxiliar da página (bezerro MS); `NaN` para os demais produtos

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

# Filtrar pela praça retornada por pracas(); o DataFrame preserva "Paranaguá/PR"
df = await cepea.indicador('soja', praca='paranagua')

# Forçar atualização
df = await cepea.indicador('soja', force_refresh=True)

# Modo offline (sem network)
df = await cepea.indicador('soja', offline=True)
```

---

### `ultimo`

Obtém o indicador mais recente disponível.

Para leite, considera a defasagem mensal e retorna o mês de referência mais
recente, não a data de publicação. Use `praca` para selecionar o estado.

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
| `praca` | `str \| None` | Praça de cotação. Aceita o slug de `pracas()` ou o rótulo exibido pela fonte; `None` não filtra |
| `offline` | `bool` | Usar apenas cache local |

**Retorno:**

Objeto `Indicador` com:
- `data`: Data do indicador
- `valor`: Valor em Decimal
- `unidade`: Unidade (ex: 'BRL/sc60kg')
- `produto`: Nome do produto
- `fonte`: Fonte dos dados

Sem rede e sem indicador no cache recente, levanta `SourceUnavailableError`, com `attempted_sources` (até a 1.1.0, `ParseError`). Com `offline=True` e sem indicador no cache, o mesmo erro, com o motivo "offline sem dado no cache".

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
# ['soja', 'soja_parana', 'milho', 'bezerro', 'cafe', 'cafe_arabica', 'cafe_robusta',
#  'boi', 'boi_gordo', 'trigo', 'algodao', 'arroz', 'acucar', 'acucar_refinado',
#  'frango_congelado', 'frango_resfriado', 'suino', 'etanol_hidratado',
#  'etanol_anidro', 'leite', 'laranja_industria', 'laranja_in_natura']
# 'bezerro' = animal de 8–12 meses, Mato Grosso do Sul, BRL/cabeca; expõe valor_usd e peso_medio_kg
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

Lista de praças mapeadas pelo parser, como slugs normalizados aceitos por `indicador()` e `ultimo()`. O DataFrame e o modelo `Indicador` preservam o rótulo exibido pela fonte. A lista é vazia para produto válido sem praça mapeada; produto desconhecido levanta `ValueError`.

```python
pracas_soja = await cepea.pracas('soja')
# ['paranagua'] — corresponde ao rótulo "Paranaguá/PR" nos dados

pracas_trigo = await cepea.pracas('trigo')
# ['parana', 'rio_grande_do_sul'] — a página publica uma tabela por praça, e o
# indicador() traz as duas; datasets.preco_diario usa o Paraná
```

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

No `Indicador` coletado do CEPEA, o `meta` traz as variações como texto publicado: `variacao` é a do dia ("Var./Dia"),
`variacao_mes` a do mês ("Var./Mês") e `variacao_semana` a da semana ("Var./semana", nos indicadores semanais do etanol).
As variações vêm só no `Indicador` recém-coletado da fonte. A leitura do cache, morna ou `offline`, traz valor, unidade,
`anomalies` e, no `meta`, só `valor_usd` e `peso_medio_kg`.

## Versão Síncrona

```python
from agrobr.sync import cepea

# Mesmas funções, sem async/await
df = cepea.indicador('soja')
ultimo = cepea.ultimo('milho')
produtos = cepea.produtos()
```

## Comportamento de Cache

1. **Cache fresh**: Retorna imediatamente do cache. A última coleta do produto vale até as 18h BRT do dia útil seguinte (horário de atualização do CEPEA; sábado e domingo não contam)
2. **Cache stale**: Busca de novo; se a fonte falhar, devolve o cache com `StaleDataWarning` e `source="cache_fallback"`
3. **Sem cache**: Busca da fonte e salva no cache

Com `return_meta=True`, o `MetaInfo` de uma resposta do cache traz em `fetched_at` a coleta real (a mais recente entre as linhas devolvidas), não o instante da chamada, e `cache_expires_at` é a virada das 18h BRT seguinte a essa coleta. `ultimo()` segue a mesma virada. Com `fim` anterior à janela recente de 25 dias corridos (período fechado, que não volta à fonte), `cache_expires_at` sai nulo: a validade não se aplica.

O histórico é acumulado progressivamente no DuckDB local, permitindo consultas a períodos antigos sem novas requisições.

## Fallback

Quando o CEPEA está indisponível (Cloudflare), o agrobr automaticamente usa o Notícias Agrícolas como fonte alternativa, que republica os mesmos indicadores CEPEA/ESALQ.

Na primeira chamada de `indicador()` ou `ultimo()`, o módulo emite um `UserWarning`: os dados CEPEA estão sob CC BY-NC 4.0, e o uso comercial requer autorização do CEPEA (`cepea@usp.br`). O fallback Notícias Agrícolas mantém seu próprio aviso de licença `restrito`; consulte `docs/licenses.md`.

Leite mensal usa somente a coleta CEPEA e seu cache: o fallback Notícias Agrícolas está desabilitado para esse produto. Seu parser autônomo expõe a data de fechamento, enquanto CEPEA usa o mês de referência. Linhas NA de leite já armazenadas ficam preservadas em quarentena na migração 9. O histórico do leite vem da série com 2 casas; a página, com 4, prevalece quando os 2 existem.

A atualização do cache preserva os originais afetados em `indicadores_quarentena`;
falhas da migração levantam `CacheMigrationError` sem concluir a retirada das
linhas. Consulte [auditoria e recuperação](../guides/migracao-2.md#18-preservacao-automatica-do-cache-existente).

Com `return_meta=True`, `selected_source="cache"` identifica leitura local,
`attempted_sources` distingue cache normal de uso após falha, e `data_sources`
informa as fontes das linhas retornadas. Cache quente/offline do Notícias
Agrícolas não é apresentado como coleta CEPEA. Numa resposta do cache,
`parser_version` é a versão do parser gravada nas linhas devolvidas (a maior,
se houver mais de uma); quando as linhas trazem outra versão além da informada,
`source_details["parser_versions"]` lista as versões por fonte, por exemplo
`{"cepea": [2], "noticias_agricolas": [3]}`. A API direta emite
`StaleDataWarning` ao usar cache após falha; `datasets.preco_diario` emite também
`SourceFallbackWarning`, que pode ser convertido em erro.

Para a mesma data, produto e praça normalizada, a observação CEPEA tem
precedência sobre Notícias Agrícolas, inclusive no cache quente e offline.
Uma nova coleta da mesma fonte substitui sua observação anterior. O DuckDB
preserva as observações dos dois provedores; a seleção ocorre na consulta.
`indicador()` mantém todas as praças distintas, e `data_sources` descreve
somente as linhas retornadas. `force_refresh=True` continua ignorando a leitura
inicial do cache e retornando a coleta solicitada.
