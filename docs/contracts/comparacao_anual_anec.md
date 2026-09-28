# comparacao_anual_anec v1.0

Comparação dos volumes mensais de dois anos publicados na mesma edição ANEC.

## Fonte

| Prioridade | Fonte | Método |
|---|---|---|
| 1 | ANEC | PDF (`anec.comparacao_anual`) |

Requer `pip install agrobr[pdf]`. Consulte a [documentação da fonte](../sources/anec.md).

## Schema

| Coluna | Tipo | Descrição |
|---|---|---|
| `mes` | int | Mês (1–12) |
| `produto` | str | Produto canônico ou agregado `total_products` |
| `ano_base` | int | Ano de referência da comparação |
| `ano_comparacao` | int | Ano comparado |
| `valor_base_ton` | Float64, nullable | Volume mensal do ano-base, em toneladas |
| `valor_comparacao_ton` | Float64, nullable | Volume mensal do ano comparado, em toneladas |
| `eh_estimativa` | bool | Estimativa referente a `ano_comparacao` |
| `ano_relatorio` | int | Ano da edição selecionada |
| `semana_relatorio` | int | Semana da edição (1–53) |
| `edicao_id` | str | Identificador `cuid` do artigo ANEC |
| `publicado_em` | datetime UTC | `created_at` do artigo |
| `revisado_em` | datetime UTC | `media_updated_at` do arquivo na origem |

**Chave primária:** `[edicao_id, revisado_em, ano_base, ano_comparacao, mes, produto]`.

A chave preserva edições e revisões distintas ao concatenar retratos dos boletins. `revisado_em` é a atualização do arquivo informada pela ANEC; não precisa ser posterior à criação do artigo. Colunas nullable sempre existem, mesmo sem valores.

## Semântica e limites

Os anos vêm da tabela publicada; `ano` na chamada seleciona a edição, não um intervalo histórico. `eh_estimativa` aplica-se somente ao valor do ano comparado. Valores ausentes permanecem nulos. `total_products` representa um agregado publicado: não deve ser somado novamente aos produtos individuais. A presença dos produtos e do agregado varia conforme a edição.

## Produtos

| Código ANEC (`produto`) | Produto | Nome no agrobr |
|---|---|---|
| `soybean` | soja em grão | `soja` |
| `soybean_meal` | farelo de soja | `farelo_soja` |
| `maize` | milho | `milho` |
| `wheat` | trigo | `trigo` |
| `sorghum` | sorgo | `sorgo` |
| `ddgs` | DDGS (grãos secos de destilaria) | sem equivalente |
| `total_products` | total publicado da tabela | sem equivalente |

`produto` conserva o código da ANEC, em inglês. O nome no agrobr é o canônico de `normalize.crops`, o mesmo de `exportacao` e `estimativa_safra`; o `normalizar_cultura` converte cada código no nome da tabela, e `ddgs` fica como está.

## Parâmetros

```python
async def comparacao_anual_anec(
    *,
    ano: int,
    semana: int | None = None,
    produto: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]: ...
```

Todos os argumentos são nomeados. `ano` é obrigatório e seleciona o **ano da edição do boletim**, de 2026 ao ano corrente. Categorias anuais são descobertas no catálogo oficial; ano explícito sem publicação gera `SourceUnavailableError`, sem recuar para outra edição anual. `semana=None` seleciona o boletim mais recente disponível nesse catálogo. `produto=None` retorna todos os produtos disponíveis; aliases como `soja` e `milho` são aceitos. `produto="total_products"` seleciona o agregado publicado quando presente.

## Exemplo

`as_polars=True` retorna um DataFrame Polars e requer `pip install agrobr[polars]`.

```python
from agrobr import datasets

df, meta = await datasets.comparacao_anual_anec(
    ano=2026, semana=34, produto="soja", return_meta=True
)
print(df.head())
print(meta.source_url)
```

## Schema JSON

`agrobr/schemas/comparacao_anual_anec.json`, também disponível via `get_contract("comparacao_anual_anec")`.

## Licença

`zona_cinza`: boletins públicos sem termos públicos explícitos de reutilização localizados. A primeira chamada ANEC emite aviso. Uso ou redistribuição comercial pode exigir autorização da ANEC; publicação no site não comprova permissão de redistribuição comercial.

## Leitura dos boletins

Saem todos os meses e produtos efetivamente publicados: sorgo não tem quadro anual na edição 04. Total Products é uma série própria; totais anuais e barras de diferença não viram meses. Janeiro de trigo e DDGS pode divergir do quadro mensal no mesmo PDF; as duas publicações não são forçadas à igualdade.
