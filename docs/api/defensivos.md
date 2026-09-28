# API Defensivos

O módulo `defensivos` consulta os CSVs correntes do Agrofit/MAPA. As quatro funções são assíncronas, aceitam somente argumentos nomeados e retornam pandas; `as_polars=True` solicita Polars e `return_meta=True` acrescenta `MetaInfo`.

## Produtos formulados

`formulados()` retorna uma linha por `nr_registro`. Aceita `ingrediente_ativo`, `classe_toxicologica`, `classe_ambiental`, `titular`, `organicos`, `marca`, `formulacao`, `classe`, `nr_registro` e `situacao`.

As dez colunas anteriores continuam presentes: `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `formulacao`, `classe_toxicologica`, `classe_ambiental`, `organicos` e `modo_de_acao`. O schema **1.1** acrescenta `situacao` e `composicao_texto`, ambas anuláveis.

`composicao_texto` conserva a célula original, inclusive espaços e caracteres. `ingrediente_ativo` mantém a representação anterior dos formulados. Se atributos do mesmo registro divergirem entre linhas, a coleta gera `ParseError`.

```python
from agrobr import defensivos

produtos, meta = await defensivos.formulados(
    ingrediente_ativo="glifosato", situacao="TRUE", return_meta=True,
)
```

## Autorizações de uso

`autorizacoes()` aceita `nr_registro`, `cultura`, `ingrediente_ativo`, `classe` e `situacao`. Preserva todas as linhas publicadas, inclusive repetições resultantes da seleção das colunas; não há chave única declarada para essa relação.

As colunas são `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `cultura`, `praga`, `praga_nome_comum`, `modalidade_de_emprego` e a adição anulável `situacao`. O schema é **1.1**.

```python
usos = await defensivos.autorizacoes(cultura="soja")
```

## Produtos técnicos

`tecnicos()` aceita `ingrediente_ativo`, `titular`, `classe`, `marca` e `nr_registro`. Retorna `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`, `classe`, `grupo_quimico`, `nome_cientifico`, `classe_toxicologica`, `classe_ambiental` e a adição anulável `composicao_texto` no schema **1.1**.

O parser reconhece grupos com parênteses internos. Quando há vários componentes, os nomes e grupos mantêm a ordem, separados por ` + `; a composição detalhada fica na função abaixo. Campos ausentes na exportação permanecem nulos. A exportação técnica sondada não publica situação, e essa função não aceita `situacao`.

## Composição

```python
componentes, meta = await defensivos.composicao(
    tipo="tecnicos", nr_registro="00301", return_meta=True,
)
```

`composicao()` aceita `tipo="formulados"` (padrão) ou `tipo="tecnicos"`, `nr_registro` e `ingrediente_ativo`. O schema **1.0** tem uma linha por posição do componente no produto, com chave `[tipo, nr_registro, ordem_componente]`.

| Coluna | Tipo / significado |
|---|---|
| `tipo` | Texto: `formulados` ou `tecnicos` |
| `nr_registro` | Identificador textual, com zeros iniciais preservados |
| `ordem_componente` | `Int64`, posição a partir de 1 |
| `ingrediente_ativo` | Nome interpretado; anulável |
| `grupo_quimico` | Grupo interpretado; anulável |
| `componente_texto` | Trecho original do componente |
| `concentracao_texto` | Concentração publicada, antes da interpretação; anulável |
| `concentracao_valor` | `Float64` anulável, sem conversão dimensional |
| `concentracao_unidade` | Unidade publicada, quando separável; anulável |

Ingredientes repetidos em posições diferentes continuam como linhas distintas. A composição não é multiplicada pelas autorizações de uso. Notação científica explícita pode ser interpretada; por exemplo, `.001 x 10^9 UFC/mL` produz valor `1000000.0` e unidade `UFC/mL`. Expressões ambíguas conservam o texto e valores nulos, com diagnóstico em `meta.source_details`. `Kg` permanece `Kg`: não se presume `g/kg`. Ausência não recebe zero.

## Filtros, cache e proveniência

Todos os filtros aceitam `str | None`. Texto vazio, números, booleanos, `tipo` inválido e parâmetros desconhecidos geram `InvalidParameterError` antes de acessar cache ou rede. Registro e `organicos` usam comparação exata; os demais filtros textuais buscam trechos literais sem distinguir maiúsculas. `situacao` usa igualdade textual sem espaços externos ou diferença de caixa.

A situação original é preservada como texto. A captura de 06/09/2026 apresentou apenas `TRUE` nos formulados. Esse token não é convertido em uma classificação de vigência ou em recomendação de aplicação.

Todas as funções aceitam `use_cache=True`. A primeira consulta baixa o CSV completo da família, mesmo com filtro; formulados tinham cerca de 391 MB na captura. O cache dura 24 horas desde a coleta e armazena tabelas, composição, tipos, hashes e metadados no mesmo ZIP. Arquivos legados são preservados, mas a API atual exige o formato novo. `use_cache=False` ignora leitura e gravação, sem substituir uma edição já armazenada.

`MetaInfo` informa fonte tentada/selecionada, versões, hash bruto e `from_cache`. `fetched_at` e `fetch_timestamp` identificam a coleta original em UTC, inclusive quando a consulta vem do cache. `source_details` inclui recurso, tamanho, hash, assinatura de layout, contagens, colunas ignoradas, filtros e diagnósticos. O hash identifica o conteúdo recebido; não reconstitui uma exportação histórica.

Os contratos estão disponíveis via `get_contract("agrofit_formulados")`, `agrofit_autorizacoes`, `agrofit_tecnicos` e `agrofit_composicao`. Os [quatro datasets Agrofit](defensivos_datasets.md) reutilizam esses contratos e preservam filtros, cache e proveniência. As funções da fonte acima mantêm suas interfaces.

## Versão síncrona

```python
from agrobr.sync import defensivos

componentes = defensivos.composicao(tipo="tecnicos", nr_registro="00301")
```

Veja [a fonte e seus limites](../sources/defensivos.md).

## Reconciliação da captura de 18/09/2026

Os dois CSVs integrais desta captura contêm 4.403 produtos formulados, 279.707 ocorrências de autorização e 2.992 produtos técnicos. Um oráculo independente confere as 12 colunas dos formulados, as dez colunas de todas as autorizações e os oito campos diretos dos técnicos. As dez colunas técnicas completas, incluindo ingrediente e grupo extraídos da composição, são conferidas em nove coortes explícitas de registro.

A composição tem reconciliação independente de 57 componentes em 32 coortes completas de produto: nove técnicas e 23 formuladas. Inclui pontas dos arquivos, zeros iniciais, identificador acentuado de pré-mistura, parênteses internos, componentes repetidos, concentração zero, notação científica e unidades publicadas. Duas expressões ambíguas reais, `1.9 10*10 UFC/g` e `200 1x10E10 UFC/g`, mantêm texto, valor/unidade nulos e diagnóstico. Não há interpretação numérica independente de toda a população de componentes; o escopo validado está explicitado no manifesto.

Os replays usam os corpos CSV completos, com hash idêntico após descompactação gzip, pela API pública da fonte e dos datasets. Cache preserva valores não nulos, tipos, posição dos componentes e proveniência UTC; os marcadores pandas `None`/`pd.NA` são equivalentes apenas em campos anuláveis. Colunas textuais de composição e situação preservam o literal; outros campos mantêm a limpeza já documentada. Autorizações não são deduplicadas.

O comparador estrutural inventaria todas as colunas, os dois recursos CKAN e os sufixos publicados nos campos de concentração. Um sufixo pode conter expressão ambígua e não certifica, por si, uma unidade ou interpretação numérica. Formato, coluna, recurso ou expressão sem decisão exige revisão. Catálogo, CSV e cache têm a mesma origem; não oferecem confirmação independente da população histórica. Parser 3 e contratos 1.1/1.0 permanecem inalterados.
