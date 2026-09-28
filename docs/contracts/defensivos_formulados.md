# defensivos_formulados v1.1

Cadastro corrente de produtos formulados do Agrofit/MAPA

Fonte: **MAPA**. Registro do contrato: `agrofit_formulados`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `nr_registro` | str | Não | — | — |
| `marca_comercial` | str | Sim | — | — |
| `ingrediente_ativo` | str | Sim | — | — |
| `titular` | str | Sim | — | — |
| `classe` | str | Sim | — | — |
| `formulacao` | str | Sim | — | — |
| `classe_toxicologica` | str | Sim | — | — |
| `classe_ambiental` | str | Sim | — | — |
| `organicos` | str | Sim | — | — |
| `modo_de_acao` | str | Sim | — | — |
| `situacao` | str | Sim | — | Texto publicado, sem inferência de vigência cadastral |
| `composicao_texto` | str | Sim | — | Célula original de ingrediente/composição |

**Chave primária:** `nr_registro`

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

Uma linha por registro publicado, mantido como texto. `situacao` e `composicao_texto` conservam o conteúdo original. Para componentes e concentrações, use `composicao_defensivos`; a tabela de produtos não cria uma linha por ingrediente.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte. Estas publicações correntes não permitem selecionar uma revisão histórica via `deterministic`.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `ingrediente_ativo` | `str \| None` | `None` |
| `classe_toxicologica` | `str \| None` | `None` |
| `classe_ambiental` | `str \| None` | `None` |
| `titular` | `str \| None` | `None` |
| `organicos` | `str \| None` | `None` |
| `marca` | `str \| None` | `None` |
| `formulacao` | `str \| None` | `None` |
| `classe` | `str \| None` | `None` |
| `nr_registro` | `str \| None` | `None` |
| `situacao` | `str \| None` | `None` |
| `use_cache` | `bool` | `True` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.defensivos_formulados(nr_registro="08725", return_meta=True)
contracts.validate_dataset(df, "agrofit_formulados")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/agrofit_formulados.json` · `get_contract("agrofit_formulados")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da API/fonte](../api/defensivos_datasets.md).

## Exportação de 18/09/2026

Os dois CSVs da exportação de 18/09/2026 contêm 4.403 produtos formulados, 279.707 ocorrências de autorização e 2.992 produtos técnicos. Duas expressões ambíguas dessa exportação, `1.9 10*10 UFC/g` e `200 1x10E10 UFC/g`, mantêm o texto e saem com valor e unidade nulos e diagnóstico. A interpretação numérica não é garantida para toda a população de componentes.

O cache preserva valores não nulos, tipos, posição dos componentes e proveniência UTC; `None` e `pd.NA` só se equivalem em campos anuláveis. Colunas textuais de composição e situação preservam o literal; outros campos mantêm a limpeza já documentada. Autorizações não são deduplicadas.

Um sufixo publicado nos campos de concentração pode conter expressão ambígua e não certifica, por si, uma unidade ou interpretação numérica. Catálogo, CSV e cache têm a mesma origem: nenhum deles confirma a população histórica.
