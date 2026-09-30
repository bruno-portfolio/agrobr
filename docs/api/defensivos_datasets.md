# Datasets Agrofit

Quatro datasets disponibilizam as tabelas correntes do Agrofit/MAPA na camada semântica. Eles preservam os filtros, valores, multiplicidades e contratos das [APIs Defensivos](defensivos.md), com fonte única `defensivos`. Não é necessário informar um produto genérico.

| Função em `agrobr.datasets` | Conteúdo e identidade | Contrato validado | Colunas |
|---|---|---|---:|
| `defensivos_formulados` | Um produto formulado por registro textual | `agrofit_formulados` 1.1 | 12 |
| `defensivos_tecnicos` | Um produto técnico por registro textual | `agrofit_tecnicos` 1.1 | 10 |
| `autorizacoes_defensivos` | Relações publicadas de produto, cultura e praga; repetições preservadas | `agrofit_autorizacoes` 1.1 | 10 |
| `composicao_defensivos` | Um componente por família, registro e posição | `agrofit_composicao` 1.0 | 9 |

Os contratos são os mesmos da fonte, consultáveis por seus nomes `agrofit_*`; os wrappers não criam schemas duplicados com os nomes dos datasets. A validação é executada também sem retorno de metadados. Consulte o [índice atual de datasets e contratos](../contracts/index.md).

## Consulta e filtros

```python
from agrobr import datasets

tecnicos, meta = await datasets.defensivos_tecnicos(
    nr_registro="00301", return_meta=True,
)
componentes = await datasets.composicao_defensivos(
    tipo="tecnicos", nr_registro="00301",
)

formulados = await datasets.defensivos_formulados(nr_registro="08725")
autorizacoes = await datasets.autorizacoes_defensivos(nr_registro="08725")
```

| Dataset | Filtros disponíveis, todos nomeados |
|---|---|
| `defensivos_formulados` | `ingrediente_ativo`, `classe_toxicologica`, `classe_ambiental`, `titular`, `organicos`, `marca`, `formulacao`, `classe`, `nr_registro`, `situacao` |
| `defensivos_tecnicos` | `ingrediente_ativo`, `titular`, `classe`, `marca`, `nr_registro` |
| `autorizacoes_defensivos` | `nr_registro`, `cultura`, `ingrediente_ativo`, `classe`, `situacao` |
| `composicao_defensivos` | `tipo="formulados"` ou `"tecnicos"`, `nr_registro`, `ingrediente_ativo` |

Filtros opcionais usam `str | None`, com `None` como padrão. A fonte remove espaços externos dos valores de filtro. Registro e `organicos` usam igualdade exata; `situacao` compara texto sem distinguir caixa ou espaços externos; os demais filtros procuram trechos literais sem expressão regular. Registro continua textual, com zeros e Unicode preservados. O técnico não aceita `situacao`.

Todas as funções são assíncronas e aceitam as flags booleanas `use_cache=True`, `as_polars=False` e `return_meta=False`. Valores não booleanos nessas flags, filtros inválidos e tipo de composição desconhecido geram `InvalidParameterError` antes de cache/rede. Parâmetros desconhecidos e argumentos posicionais geram `TypeError` da assinatura pública, também antes de I/O.

## Cache e proveniência

A primeira consulta baixa o CSV integral da família, mesmo com filtros. A captura de setembro de 2026 tinha aproximadamente 391 MB para formulados e 0,7 MB para técnicos; isso não é um limite ou tamanho futuro garantido. O bundle formulado alimenta formulados, autorizações e composição; o técnico alimenta técnicos e sua composição. Consultas seguintes podem reutilizar o mesmo cache por 24 horas desde a coleta. `use_cache=False` ignora leitura e gravação do cache, conforme a fonte.

`return_meta=True` retorna `(DataFrame, MetaInfo)`. A fonte selecionada/tentada é `defensivos`; `source` identifica `datasets.<nome>/defensivos`, e `dataset` identifica a função. Versões de schema/contrato correspondem à tabela, e o parser continua 3. `fetched_at` conserva a aquisição original com offset UTC; `fetch_timestamp` é a mesma aquisição. Uma leitura de cache não anuncia uma nova aquisição.

Hash/tamanho bruto, chave/expiração de cache e durações de aquisição/parsing atravessam a camada de datasets. Eles continuam descrevendo o recurso e o trabalho da fonte, não o DataFrame filtrado, um cache separado do dataset ou o tempo total do wrapper. `from_cache` vem do resultado efetivo. Recurso, layout, contagens, filtros, campos ignorados e diagnósticos em `source_details` são preservados em cópia independente.

O contexto `datasets.deterministic(...)` é recusado antes de qualquer I/O: o CSV entrega uma exportação corrente, e seu cache conserva uma coleta identificada por conteúdo. Nenhum deles permite reconstruir arbitrariamente o cadastro de outra data. Esses datasets retornam `snapshot=None`.

## Identidade e tipos

As colunas textuais, inclusive registros e composição original, saem no dtype padrão do pandas instalado (`str` no pandas 3, `object` no 2). `ordem_componente` é `Int64`; `concentracao_valor` é `float64`. Resultados vazios conservam todas as colunas e seus tipos, incluindo adições opcionais dos contratos. Veja a [descrição completa das colunas](defensivos.md#composicao).

Ingredientes repetidos permanecem em posições distintas. Autorizações não recebem chave artificial ou deduplicação após projeção. Não há join automático entre autorizações e componentes. Unidades literais, texto ambíguo e valores nulos permanecem como na fonte; `TRUE` não é convertido em classificação de vigência ou recomendação de aplicação.

## Sync, Polars e descoberta

```python
from agrobr.sync import datasets

componentes = datasets.composicao_defensivos(
    tipo="tecnicos", nr_registro="00301", as_polars=True,
)
print(datasets.list_datasets())
print(datasets.info("composicao_defensivos"))
```

Polars exige a dependência opcional correspondente e é aplicado depois da validação pandas. A fonte única não tem fallback para outro cadastro. Empresas, bulas, histórico de cancelamentos e interpretação oficial da situação continuam fora destas tabelas; consulte [cobertura e limites](../sources/defensivos.md) e [licença da fonte](../licenses.md#defensivos-agrofit).

## Exportação de 18/09/2026

Os dois CSVs da exportação de 18/09/2026 contêm 4.403 produtos formulados, 279.707 ocorrências de autorização e 2.992 produtos técnicos. Duas expressões ambíguas dessa exportação, `1.9 10*10 UFC/g` e `200 1x10E10 UFC/g`, mantêm o texto e saem com valor e unidade nulos e diagnóstico. A interpretação numérica não é garantida para toda a população de componentes.

O cache preserva valores não nulos, tipos, posição dos componentes e proveniência UTC; `None` e `pd.NA` só se equivalem em campos anuláveis. Colunas textuais de composição e situação preservam o literal; outros campos mantêm a limpeza já documentada. Autorizações não são deduplicadas.

Um sufixo publicado nos campos de concentração pode conter expressão ambígua e não certifica, por si, uma unidade ou interpretação numérica. Catálogo, CSV e cache têm a mesma origem: nenhum deles confirma a população histórica.
