# destinos_anec v1.0

Participações percentuais dos destinos nas exportações acumuladas por produto, conforme o período indicado no boletim ANEC.

## Fonte

| Prioridade | Fonte | Método |
|---|---|---|
| 1 | ANEC | PDF (`anec.destinos`) |

Requer `pip install agrobr[pdf]`. Consulte a [documentação da fonte](../sources/anec.md).

## Schema

| Coluna | Tipo | Descrição |
|---|---|---|
| `produto` | str | Produto canônico |
| `destino` | str | Destino em maiúsculas; inclui o agregado `OTHERS` |
| `share_pct` | Float64, nullable | Participação percentual (0–100) |
| `ano` | Int64, nullable | Ano do período acumulado, quando identificado no cabeçalho |
| `mes_inicio` | Int64, nullable | Primeiro mês do período acumulado (1–12) |
| `mes_fim` | Int64, nullable | Último mês do período acumulado (1–12) |
| `ano_relatorio` | int | Ano da edição selecionada |
| `semana_relatorio` | int | Semana da edição (1–53) |
| `edicao_id` | str | Identificador `cuid` do artigo ANEC |
| `publicado_em` | datetime UTC | `created_at` do artigo |
| `revisado_em` | datetime UTC | `media_updated_at` do arquivo na origem |

**Chave primária:** `[edicao_id, revisado_em, produto, destino]`.

A chave preserva edições e revisões distintas ao concatenar retratos dos boletins. `revisado_em` é a atualização do arquivo informada pela ANEC; não precisa ser posterior à criação do artigo. Colunas nullable sempre existem, mesmo sem valores.

## Semântica e limites

A ANEC publica importadores apenas para soja (`soybean`), farelo de soja (`soybean_meal`), milho (`maize`) e trigo (`wheat`), nas edições W13 e W34/2026 conferidas. O catálogo de destinos anuncia esses quatro produtos. DDGS e sorgo continuam em `embarques_mensais_anec` e `comparacao_anual_anec`. Em destinos, `ddgs`, `sorgo` e seus aliases são recusados com `InvalidParameterError` antes de qualquer acesso à rede.

A unidade é percentual acumulado no período do cabeçalho, não tonelagem nem fluxo mensal. Não se atribui automaticamente a semana ou o mês da edição ao período dos destinos. `OTHERS` é um agregado válido; não é um país. Percentuais arredondados podem somar 99% ou 101%, sem ajuste artificial para 100%.

Alguns boletins usam gráficos que o parser ainda não extrai (observado nas edições W08 e W12 de 2026). O retorno pode ser vazio com aviso em `meta.validation_warnings`; isso não comprova ausência de embarques ou destinos. Use `return_meta=True` para inspecionar os avisos. A edição W34/2026 informa janeiro–julho de 2026.

## Produtos

| Código ANEC (`produto`) | Produto | Nome no agrobr |
|---|---|---|
| `soybean` | soja em grão | `soja` |
| `soybean_meal` | farelo de soja | `farelo_soja` |
| `maize` | milho | `milho` |
| `wheat` | trigo | `trigo` |

`produto` conserva o código da ANEC, em inglês. O nome no agrobr é o canônico de `normalize.crops`, o mesmo de `exportacao` e `estimativa_safra`; o `normalizar_cultura` converte cada código no nome da tabela.

## Parâmetros

```python
async def destinos_anec(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]: ...
```

Todos os argumentos são nomeados. `ano` é obrigatório e seleciona o **ano da edição do boletim**, de 2026 ao ano corrente. Categorias anuais são descobertas no catálogo oficial; ano explícito sem publicação gera `SourceUnavailableError`, sem recuar para outra edição anual. `semana=None` seleciona o boletim mais recente disponível nesse catálogo. `produto=None` retorna todos os produtos disponíveis; aliases como `soja` e `milho` são aceitos.

## Exemplo

`as_polars=True` retorna um DataFrame Polars e requer `pip install agrobr[polars]`.

```python
from agrobr import datasets

df, meta = await datasets.destinos_anec(
    ano=2026, semana=34, produto="soja", return_meta=True
)
print(df.head())
print(meta.source_url)
```

## Schema JSON

`agrobr/schemas/destinos_anec.json`, também disponível via `get_contract("destinos_anec")`.

## Licença

`zona_cinza`: boletins públicos sem termos públicos explícitos de reutilização localizados. A primeira chamada ANEC emite aviso. Uso ou redistribuição comercial pode exigir autorização da ANEC; publicação no site não comprova permissão de redistribuição comercial.

## Conferência dos boletins

As tabelas de texto das edições 04, 13, 34 e 36/2026 foram conferidas integralmente, incluindo OTHERS. Percentuais no mapa e a linha Total ficam fora do contrato. As participações arredondadas podem somar 99%, 101% ou 102%, sem renormalização. As edições 08 e 12/2026 têm quadros em imagem: vazio com aviso continua sendo ausência de extração, não ausência de comércio.
