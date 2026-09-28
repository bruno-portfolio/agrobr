# moedas_cambio v1.0

Catálogo corrente de moedas do serviço PTAX/BCB

Fonte: **BCB**. Registro do contrato: `bcb_ptax_moedas`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `moeda` | str | Não | — | — |
| `nome` | str | Não | — | — |
| `tipo_moeda` | str | Não | — | — |

**Chave primária:** `moeda`

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

Uma linha por símbolo ASCII de três letras, em ordem de moeda. Nomes e tipos são preservados como publicados. O catálogo corrente não comprova vigência histórica; `top` controla o tamanho de página e não substitui a paginação.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte. Estas publicações correntes não permitem selecionar uma revisão histórica via `deterministic`.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `top` | `int` | `1000` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.moedas_cambio(top=1000, return_meta=True)
contracts.validate_dataset(df, "bcb_ptax_moedas")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/bcb_ptax_moedas.json` · `get_contract("bcb_ptax_moedas")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da API/fonte](../contracts/bcb_ptax.md).
