# embarques_mensais_anec v1.0

Volumes mensais por produto, conforme uma edição do boletim ANEC, com estimativas e faixas publicadas.

## Fonte

| Prioridade | Fonte | Método |
|---|---|---|
| 1 | ANEC | PDF (`anec.embarques_mensais`) |

Requer `pip install agrobr[pdf]`. Consulte a [documentação da fonte](../sources/anec.md).

## Schema

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência do volume |
| `mes` | int | Mês (1–12) |
| `produto` | str | Produto canônico |
| `valor_ton` | Float64, nullable | Volume mensal em toneladas; nulo para faixa sem valor pontual |
| `valor_min_ton` | Float64, nullable | Limite inferior da faixa publicada, em toneladas |
| `valor_max_ton` | Float64, nullable | Limite superior da faixa publicada, em toneladas |
| `eh_estimativa` | bool | Indica mês marcado como estimativa no boletim |
| `ano_relatorio` | int | Ano da edição selecionada |
| `semana_relatorio` | int | Semana da edição (1–53) |
| `edicao_id` | str | Identificador `cuid` do artigo ANEC |
| `publicado_em` | datetime UTC | `created_at` do artigo |
| `revisado_em` | datetime UTC | `media_updated_at` do arquivo na origem |

**Chave primária:** `[edicao_id, revisado_em, ano, mes, produto]`.

A chave preserva edições e revisões distintas ao concatenar retratos dos boletins. `revisado_em` é a atualização do arquivo informada pela ANEC; não precisa ser posterior à criação do artigo. Colunas nullable sempre existem, mesmo sem valores.

## Semântica e limites

Valores são mensais, não acumulados de vários meses. Uma faixa publicada preserva seus limites e deixa `valor_ton` nulo: não se calcula ponto médio. Ausência de valor não equivale a zero. O marcador de estimativa é preservado; não deve ser inferido apenas pela data da consulta. `eh_estimativa=False` indica ausência do marcador, sem garantir volume realizado.

## Produtos

| Código ANEC (`produto`) | Produto | Nome no agrobr |
|---|---|---|
| `soybean` | soja em grão | `soja` |
| `soybean_meal` | farelo de soja | `farelo_soja` |
| `maize` | milho | `milho` |
| `wheat` | trigo | `trigo` |
| `sorghum` | sorgo | `sorgo` |
| `ddgs` | DDGS (grãos secos de destilaria) | sem equivalente |

`produto` conserva o código da ANEC, em inglês. O nome no agrobr é o canônico de `normalize.crops`, o mesmo de `exportacao` e `estimativa_safra`; o `normalizar_cultura` converte cada código no nome da tabela, e `ddgs` fica como está.

## Parâmetros

```python
async def embarques_mensais_anec(
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

df, meta = await datasets.embarques_mensais_anec(
    ano=2026, semana=34, produto="soja", return_meta=True
)
print(df.head())
print(meta.source_url)
```

## Schema JSON

`agrobr/schemas/embarques_mensais_anec.json`, também disponível via `get_contract("embarques_mensais_anec")`.

## Licença

`zona_cinza`: boletins públicos sem termos públicos explícitos de reutilização localizados. A primeira chamada ANEC emite aviso. Uso ou redistribuição comercial pode exigir autorização da ANEC; publicação no site não comprova permissão de redistribuição comercial.

## Leitura dos boletins

O cabeçalho deve conter os seis produtos (ou os quatro das edições até a W2/2026) e Total Products; a ausência de uma coluna interrompe a leitura. Total Products e totais anuais não viram observações por produto. Todos os meses são preservados, inclusive dezembro vazio, e também as faixas, como a da soja de abril na edição 13. Diferenças entre este quadro e o comparativo anual permanecem como publicadas.
