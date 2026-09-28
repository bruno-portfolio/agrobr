# defensivos_tecnicos v1.1

Cadastro corrente de produtos técnicos do Agrofit/MAPA

Fonte: **MAPA**. Registro do contrato: `agrofit_tecnicos`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `nr_registro` | str | Não | — | — |
| `marca_comercial` | str | Sim | — | — |
| `ingrediente_ativo` | str | Sim | — | — |
| `titular` | str | Sim | — | — |
| `classe` | str | Sim | — | — |
| `grupo_quimico` | str | Sim | — | — |
| `nome_cientifico` | str | Sim | — | — |
| `classe_toxicologica` | str | Sim | — | — |
| `classe_ambiental` | str | Sim | — | — |
| `composicao_texto` | str | Sim | — | Célula original de ingrediente/composição |

**Chave primária:** `nr_registro`

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

Uma linha por registro textual. Ingredientes e grupos químicos compostos não são separados por um corte ingênuo em parênteses. `composicao_texto` preserva a célula publicada; a composição detalhada está em `composicao_defensivos(tipo="tecnicos")`.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte. Estas publicações correntes não permitem selecionar uma revisão histórica via `deterministic`.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `ingrediente_ativo` | `str \| None` | `None` |
| `titular` | `str \| None` | `None` |
| `classe` | `str \| None` | `None` |
| `marca` | `str \| None` | `None` |
| `nr_registro` | `str \| None` | `None` |
| `use_cache` | `bool` | `True` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.defensivos_tecnicos(nr_registro="00301", return_meta=True)
contracts.validate_dataset(df, "agrofit_tecnicos")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/agrofit_tecnicos.json` · `get_contract("agrofit_tecnicos")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da API/fonte](../api/defensivos_datasets.md).

## Exportação de 18/09/2026

Os dois CSVs da exportação de 18/09/2026 contêm 4.403 produtos formulados, 279.707 ocorrências de autorização e 2.992 produtos técnicos. Duas expressões ambíguas dessa exportação, `1.9 10*10 UFC/g` e `200 1x10E10 UFC/g`, mantêm o texto e saem com valor e unidade nulos e diagnóstico. A interpretação numérica não é garantida para toda a população de componentes.

O cache preserva valores não nulos, tipos, posição dos componentes e proveniência UTC; `None` e `pd.NA` só se equivalem em campos anuláveis. Colunas textuais de composição e situação preservam o literal; outros campos mantêm a limpeza já documentada. Autorizações não são deduplicadas.

Um sufixo publicado nos campos de concentração pode conter expressão ambígua e não certifica, por si, uma unidade ou interpretação numérica. Catálogo, CSV e cache têm a mesma origem: nenhum deles confirma a população histórica.
