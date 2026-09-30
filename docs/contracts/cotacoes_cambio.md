# cotacoes_cambio v2.0

Cotações e paridades cambiais dos boletins PTAX/BCB

Fonte: **BCB**. Registro do contrato: `bcb_ptax`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `cotacao_compra` | float | Sim | — | — |
| `cotacao_venda` | float | Sim | — | — |
| `data_hora` | datetime | Não | — | — |
| `data` | date | Não | — | — |
| `moeda` | str | Não | — | — |
| `paridade_compra` | float | Sim | — | — |
| `paridade_venda` | float | Sim | — | — |
| `tipo_boletim` | str | Sim | — | — |

**Chave primária:** `moeda`, `data_hora`, `tipo_boletim`

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

USD e fechamento são os padrões. Use `data` ou o par `inicio`/`fim`, como `date`, `datetime`, ISO ou DD/MM/AAAA. O relógio publicado tem precisão de nanossegundos, sem fuso inferido. A unidade monetária depende da referência histórica e da moeda; paridade não é cotação. Rótulos de boletim são literais e não devem ser descartados da chave.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte. Estas publicações correntes não permitem selecionar uma revisão histórica via `deterministic`.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `data` | `str \| date \| datetime \| None` | `None` |
| `inicio` | `str \| date \| datetime \| None` | `None` |
| `fim` | `str \| date \| datetime \| None` | `None` |
| `moeda` | `str` | `'USD'` |
| `boletim` | `Literal['todos', 'fechamento', 'abertura', 'intermediario']` | `'fechamento'` |
| `top` | `int` | `1000` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.cotacoes_cambio(moeda="EUR", boletim="todos", data="04/09/2026", return_meta=True)
contracts.validate_dataset(df, "bcb_ptax")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/bcb_ptax.json` · `get_contract("bcb_ptax")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da API/fonte](../contracts/bcb_ptax.md).
