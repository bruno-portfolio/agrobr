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

## Reconciliação da captura de 18/09/2026

Os dois CSVs integrais desta captura contêm 4.403 produtos formulados, 279.707 ocorrências de autorização e 2.992 produtos técnicos. Um oráculo independente confere as 12 colunas dos formulados, as dez colunas de todas as autorizações e os oito campos diretos dos técnicos. As dez colunas técnicas completas, incluindo ingrediente e grupo extraídos da composição, são conferidas em nove coortes explícitas de registro.

A composição tem reconciliação independente de 57 componentes em 32 coortes completas de produto: nove técnicas e 23 formuladas. Inclui pontas dos arquivos, zeros iniciais, identificador acentuado de pré-mistura, parênteses internos, componentes repetidos, concentração zero, notação científica e unidades publicadas. Duas expressões ambíguas reais, `1.9 10*10 UFC/g` e `200 1x10E10 UFC/g`, mantêm texto, valor/unidade nulos e diagnóstico. Não há interpretação numérica independente de toda a população de componentes; o escopo validado está explicitado no manifesto.

Os replays usam os corpos CSV completos, com hash idêntico após descompactação gzip, pela API pública da fonte e dos datasets. Cache preserva valores não nulos, tipos, posição dos componentes e proveniência UTC; os marcadores pandas `None`/`pd.NA` são equivalentes apenas em campos anuláveis. Colunas textuais de composição e situação preservam o literal; outros campos mantêm a limpeza já documentada. Autorizações não são deduplicadas.

O comparador estrutural inventaria todas as colunas, os dois recursos CKAN e os sufixos publicados nos campos de concentração. Um sufixo pode conter expressão ambígua e não certifica, por si, uma unidade ou interpretação numérica. Formato, coluna, recurso ou expressão sem decisão exige revisão. Catálogo, CSV e cache têm a mesma origem; não oferecem confirmação independente da população histórica. Parser 3 e contratos 1.1/1.0 permanecem inalterados.
