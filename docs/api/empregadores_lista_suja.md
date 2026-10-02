# Dataset de empregadores da Lista Suja

`datasets.empregadores_lista_suja` disponibiliza o cadastro principal corrente do MTE na camada semântica, reutilizando a [API `lista_suja.empregadores`](../sources/lista_suja.md). A abrangência é nacional: a presença no dataset não identifica automaticamente uma atividade agropecuária. CEAC é outro cadastro e não participa desta consulta.

## Consulta

```python
from agrobr import datasets

df, meta = await datasets.empregadores_lista_suja(
    uf="PA", formato="csv", return_meta=True,
)
registro = await datasets.empregadores_lista_suja(id_registro="41")
```

| Parâmetro | Padrão | Comportamento |
|---|---|---|
| `uf` | `None` | Sigla normalizada pela fonte; filtro local |
| `id_registro` | `None` | ID textual exato dentro da exportação, sem conversão numérica |
| `formato` | `"auto"` | `"auto"`, `"csv"` ou `"pdf"`, com a seleção da fonte |
| `as_polars` | `False` | Booleano estrito; conversão após validação pandas |
| `return_meta` | `False` | Booleano estrito; retorna `(df, MetaInfo)` quando verdadeiro |

Todos os argumentos são nomeados. UF e ID podem ser combinados. O ID é comparado literalmente: espaços externos não são removidos desse seletor. Ausência de correspondência produz um quadro vazio tipado; não comprova ausência de uma pessoa ou empresa em outras publicações.

As duas flags precisam ser booleanas; os filtros e o formato seguem a validação da fonte. Seleções inválidas geram `InvalidParameterError` antes da rede. Argumentos posicionais ou desconhecidos, incluindo `produto` e `use_cache`, geram `TypeError` da assinatura antes de I/O.

## Formatos e erros

Cada chamada consulta a página oficial e baixa o arquivo completo, mesmo com filtros. Não há cache persistente nem paginação. CSV funciona com a instalação core. O TXT companheiro confere o contexto da publicação; ele não é uma segunda tabela retornada.

`formato="auto"` prioriza CSV. A fonte pode escolher PDF quando CSV não é anunciado ou ocorre indisponibilidade elegível, preservando a causa nos metadados. `formato="csv"` e `formato="pdf"` são exclusivos. Um CSV malformado com resposta HTTP bem-sucedida ou um TXT divergente gera erro; não provoca troca silenciosa para PDF. Somente a rota PDF exige `agrobr[pdf]`.

A validação cobre a publicação integral antes dos filtros. Falhas de aquisição e de contrato da fonte são encapsuladas pela base em `SourceUnavailableError`, com a classificação em `errors`; falha de layout chega como `ParseError` (também com `errors`). Sem `agrobr[pdf]`, a rota PDF levanta `ImportError`. Não há outra instituição como fallback do dataset. A causa e a rota do fallback de formato continuam sendo responsabilidade da fonte.

## Contrato e identidade

O dataset valida o contrato existente `lista_suja_empregadores` **2.0**, com doze colunas, inclusive quando `return_meta=False`. Não cria alias de contrato nem novo schema com seu próprio nome. As [colunas e suas garantias](../sources/lista_suja.md#contrato-20) permanecem iguais às da fonte.

Documentos, CNAE e ID continuam textuais, com zeros preservados. A chave `id_registro` vale somente dentro do conteúdo identificado por hash; não é identificador permanente de empregador. Documentos repetidos não são deduplicados. Campos ausentes permanecem nulos, contagens/anos usam `Int64` e datas civis usam `datetime64[ns]` sem fuso.

Inclusões com intervalo ou múltiplas datas conservam `data_inclusao_texto` e deixam o escalar `data_inclusao` nulo. O nome legado `trabalhadores_resgatados` corresponde ao campo oficial “Trabalhadores envolvidos”; o wrapper não altera seu significado. Resultados vazios mantêm as doze colunas e seus tipos.

## Proveniência e limites temporais

`meta.dataset` identifica `empregadores_lista_suja`; `meta.source` usa `datasets.empregadores_lista_suja/<rota>`. `selected_source` e `attempted_sources` preservam `lista_suja_csv` ou `lista_suja_pdf`, inclusive quando há uma única tentativa. Quando uma tentativa CSV falha e a fonte usa PDF, ambas as tentativas e a causa são conservadas. Se CSV não estava anunciado, somente PDF consta nas tentativas, com essa causa registrada no fallback.

`fetched_at` conserva a aquisição original em UTC com fuso. `fetch_timestamp` é a mesma aquisição, em UTC com fuso. Hash, tamanho e durações descrevem o recurso e o trabalho da fonte, não o DataFrame filtrado nem o tempo total do wrapper. `from_cache=False`; não se inventa chave ou expiração de cache. `source_details` e avisos são preservados em cópias independentes.

Atualização periódica, atualização do cadastro e aquisição são referências distintas. A data comprovada no corpo da publicação não é substituída pela data da página ou pelo relógio local. TXT ausente ou indisponível nos casos elegíveis deixa o contexto não comprovado e o diagnóstico correspondente; TXT inválido ou divergente falha.

A informação `update_frequency="semiannual"` é uma convenção para o intervalo máximo de seis meses previsto no art. 2º, § 5º, da [Portaria Interministerial nº 18/2024](https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/portaria-interministerial-mte-mdhc-mir-n-18-2024.pdf), que admite atualizações a qualquer tempo. Não representa agenda fixa nem SLA de publicação; `typical_latency` mantém essa ressalva.

`datasets.deterministic(...)` é recusado antes de I/O e `snapshot=None` no resultado. Esta interface não seleciona edições antigas nem reconstrói revisões. Consulte a [licença e suas ressalvas](../licenses.md#lista-suja); o wrapper preserva o aviso existente da fonte sobre CPF/CNPJ e não classifica automaticamente os registros como agropecuários.

## Sync e Polars

```python
from agrobr.sync import datasets

df = datasets.empregadores_lista_suja(uf="PA", formato="csv")
polars_df = datasets.empregadores_lista_suja(
    id_registro="41", formato="csv", as_polars=True,
)
```

Polars requer `agrobr[polars]`. A conversão ocorre depois da validação do contrato pandas. O dataset também aparece em `datasets.list_datasets()` e `datasets.info("empregadores_lista_suja")`.

Em 18/09/2026, a publicação tinha 579 registros em CSV/TXT e PDF. Cada formato conserva seu texto, incluindo quebras de linha. Tanto a API da fonte quanto o dataset preservam `lista_suja_csv` ou `lista_suja_pdf` em fontes tentadas/selecionada. Data da edição, atualização cadastral e instante de aquisição são distintos; veja a [fonte](../sources/lista_suja.md).

`data_inclusao_texto` via PDF preserva as quebras de linha da célula; os demais campos textuais usam a normalização de espaços descrita na fonte.
