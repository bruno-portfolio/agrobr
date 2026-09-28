# composicao_defensivos v1.0

Componentes por posição na composição de produtos do Agrofit/MAPA

Fonte: **MAPA**. Registro do contrato: `agrofit_composicao`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `tipo` | str | Não | — | — |
| `nr_registro` | str | Não | — | — |
| `ordem_componente` | int | Não | — | — |
| `ingrediente_ativo` | str | Sim | — | — |
| `grupo_quimico` | str | Sim | — | — |
| `componente_texto` | str | Não | — | — |
| `concentracao_texto` | str | Sim | — | — |
| `concentracao_valor` | float | Sim | — | — |
| `concentracao_unidade` | str | Sim | — | — |

**Chave primária:** `tipo`, `nr_registro`, `ordem_componente`

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

A posição distingue ingredientes repetidos dentro de cada família e registro. `tipo` aceita `formulados` ou `tecnicos`. Texto e unidade de concentração são preservados; valor ambíguo fica nulo, com diagnóstico, sem conversão implícita de unidades.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte. Estas publicações correntes não permitem selecionar uma revisão histórica via `deterministic`.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `tipo` | `str` | `'formulados'` |
| `nr_registro` | `str \| None` | `None` |
| `ingrediente_ativo` | `str \| None` | `None` |
| `use_cache` | `bool` | `True` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.composicao_defensivos(tipo="tecnicos", nr_registro="00301", return_meta=True)
contracts.validate_dataset(df, "agrofit_composicao")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/agrofit_composicao.json` · `get_contract("agrofit_composicao")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da API/fonte](../api/defensivos_datasets.md).

## Exportação de 18/09/2026

Os dois CSVs da exportação de 18/09/2026 contêm 4.403 produtos formulados, 279.707 ocorrências de autorização e 2.992 produtos técnicos. Duas expressões ambíguas dessa exportação, `1.9 10*10 UFC/g` e `200 1x10E10 UFC/g`, mantêm o texto e saem com valor e unidade nulos e diagnóstico. A interpretação numérica não é garantida para toda a população de componentes.

O cache preserva valores não nulos, tipos, posição dos componentes e proveniência UTC; `None` e `pd.NA` só se equivalem em campos anuláveis. Colunas textuais de composição e situação preservam o literal; outros campos mantêm a limpeza já documentada. Autorizações não são deduplicadas.

Um sufixo publicado nos campos de concentração pode conter expressão ambígua e não certifica, por si, uma unidade ou interpretação numérica. Catálogo, CSV e cache têm a mesma origem: nenhum deles confirma a população histórica.
