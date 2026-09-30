# expectativas_mercado v2.0

Expectativas anuais e mensais de mercado do Focus/BCB

Fonte: **BCB**. Registro do contrato: `bcb_focus`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `indicador` | str | Não | — | — |
| `data` | date | Não | — | — |
| `data_referencia` | str | Não | — | — |
| `media` | float | Sim | do indicador | — |
| `mediana` | float | Sim | do indicador | — |
| `desvio_padrao` | float | Sim | do indicador | — |
| `minimo` | float | Sim | do indicador | — |
| `maximo` | float | Sim | do indicador | — |
| `numero_respondentes` | int | Sim | — | — |
| `base_calculo` | int | Sim | — | — |
| `periodicidade` | str | Não | — | — |
| `indicador_detalhe` | str | Sim | — | — |

As estatísticas (`media`, `mediana`, `desvio_padrao`, `minimo` e `maximo`) estão na unidade do indicador: o IPCA em %, o câmbio em R$/US$.

**Chave primária:** `periodicidade`, `indicador`, `indicador_detalhe`, `data`, `data_referencia`, `base_calculo`

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

A data da pesquisa difere do horizonte de `data_referencia`, que preserva YYYY ou MM/YYYY. Detalhe, base de cálculo e periodicidade integram a identidade. Estatísticas negativas publicadas são preservadas; inconsistências finitas recebem diagnóstico. Limite local e cobertura sem total independente são explicitados nos metadados.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte. Estas publicações correntes não permitem selecionar uma revisão histórica via `deterministic`.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `indicador` | `str` | `'PIB Agropecuária'` |
| `periodicidade` | `Literal['anual', 'mensal']` | `'anual'` |
| `top` | `int` | `1000` |
| `inicio` | `str \| date \| datetime \| None` | `None` |
| `max_registros` | `int \| None` | `None` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.expectativas_mercado("PIB Agropecuária", inicio="2026-08-24", max_registros=6, return_meta=True)
contracts.validate_dataset(df, "bcb_focus")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/bcb_focus.json` · `get_contract("bcb_focus")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da API/fonte](../contracts/bcb_focus.md).
