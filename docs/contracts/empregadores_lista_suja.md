# empregadores_lista_suja v2.0

Cadastro corrente de empregadores publicado pelo MTE

Fonte: **MTE**. Registro do contrato: `lista_suja_empregadores`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `empregador` | str | Não | — | — |
| `cpf_cnpj` | str | Não | — | — |
| `estabelecimento` | str | Sim | — | — |
| `uf` | str | Sim | — | — |
| `cnae` | str | Sim | — | — |
| `data_inclusao` | date | Sim | — | — |
| `trabalhadores_resgatados` | int | Sim | — | Campo oficial Trabalhadores envolvidos, com nome legado da API |
| `ano_acao_fiscal` | int | Sim | — | — |
| `id_registro` | str | Não | — | ID textual local à exportação identificada pelo hash do recurso |
| `data_decisao` | date | Sim | — | — |
| `data_atualizacao` | date | Sim | — | — |
| `data_inclusao_texto` | str | Não | — | Texto original de inclusão, preservando intervalos e múltiplas datas |

**Chave primária:** `id_registro`

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

`id_registro` identifica a linha somente dentro de uma publicação, vinculada ao hash do recurso. CPF/CNPJ repetidos são preservados. Inclusões compostas mantêm o texto integral e não recebem uma data escolhida arbitrariamente. CSV é a rota principal; PDF é alternativa explícita ou fallback elegível. O recurso completo é adquirido antes dos filtros.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte. Estas publicações correntes não permitem selecionar uma revisão histórica via `deterministic`.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `uf` | `str \| None` | `None` |
| `id_registro` | `str \| None` | `None` |
| `formato` | `str` | `'auto'` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.empregadores_lista_suja(uf="MT", formato="csv", return_meta=True)
contracts.validate_dataset(df, "lista_suja_empregadores")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/lista_suja_empregadores.json` · `get_contract("lista_suja_empregadores")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da API/fonte](../api/empregadores_lista_suja.md).

Em 18/09/2026, a publicação tinha 579 registros em CSV/TXT e PDF. Cada formato conserva seu texto, incluindo quebras de linha. Tanto a API da fonte quanto o dataset preservam `lista_suja_csv` ou `lista_suja_pdf` em fontes tentadas/selecionada. Data da edição, atualização cadastral e instante de aquisição são distintos; veja a [fonte](../sources/lista_suja.md).

`data_inclusao_texto` via PDF preserva as quebras de linha da célula; os demais campos textuais usam a normalização de espaços descrita na fonte.
