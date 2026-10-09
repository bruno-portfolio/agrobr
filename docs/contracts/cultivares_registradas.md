# Contrato: cultivares_registradas

O dataset reutiliza o contrato de fonte **`rnc_registradas` 1.0**, constante `RNC_REGISTRADAS_V1` e nome interno `rnc.registradas`. Não há alias de contrato com o nome do dataset. Consulte a [API dos datasets de cultivares](../api/cultivares.md).

## Schema

As dez colunas mantêm a ordem e os nomes da API RNC. Campos textuais são strings, no dtype padrão do pandas instalado (`str` no pandas 3, `object` no 2). Datas civis são `datetime64[ns]`, sem timezone ou horário.

| Coluna | Tipo pandas | Nulo | Significado |
|---|---|---|---|
| `cultivar` | str | Não | Nome publicado; string vazia legítima é preservada |
| `nome_comum` | str | Não | Nome comum da espécie, com brancos internos repetidos juntados em 1 espaço |
| `nome_cientifico` | str | Não | Nome científico publicado, com brancos internos repetidos juntados em 1 espaço |
| `grupo` | str | Não | Grupo da espécie |
| `situacao` | str | Não | Situação cadastral textual |
| `nr_formulario` | str | Não | Número de formulário; pode repetir ou ser vazio |
| `nr_registro` | str | Não | Número de registro, chave textual não vazia |
| `data_registro` | datetime64[ns] | Sim | Data civil do registro |
| `data_validade` | datetime64[ns] | Sim | Data civil de validade publicada |
| `mantenedor` | str | Não | Texto de mantenedores; vazio e nomes compostos são preservados |

**Chave primária:** `nr_registro`, na família e recurso processados. Registro e formulário são identificadores diferentes. Número de formulário não é chave nem uma alternativa automática quando falta registro. Não converter identificadores para números; zeros e pontuação permanecem textuais.

## Garantias e limites

O contrato é obrigatório na fonte e no dataset, inclusive sem metadados. A fonte valida todas as linhas antes dos filtros. Colunas obrigatórias ausentes, rótulos duplicados, chave ausente/vazia/repetida ou tipos incompatíveis falham; não há deduplicação por cultivar ou mantenedor.

Ausência textual publicada é `""`, diferente de nulo. Datas ausentes permanecem `NaT`; textos que não representam data válida não são convertidos silenciosamente. As duas datas admitem ausência, sem imputação. O contrato exige datas civis de precisão ns e rejeita timezone ou horário, inclusive colunas inteiramente nulas com tipo físico errado.

O resultado vazio por filtro conserva as dez colunas e seus tipos. Um CSV fisicamente vazio ou inválido não é tratado como resultado legítimo da busca. Situação, validade e mantenedor não são interpretados como autorização de comercialização, direito de propriedade ou recomendação agronômica. Campos com múltiplos nomes não são divididos.

## Metadados e seleção

`schema_version` e `contract_version` são `"1.0"`. No dataset, `selected_source="rnc_registradas"` e `attempted_sources=["rnc_registradas"]`; cache não troca a rota para outra família. Hash, tamanho e coleta descrevem o CSV completo, enquanto `records_count` e `columns` descrevem o resultado selecionado. Diagnósticos da população, comparação com o total da busca e filtros ficam em `source_details`.

`nr_registro` e `nr_formulario` fazem igualdade textual completa após strip externo. Os demais filtros são substrings literais sem distinção de caixa; `especie` consulta `nome_comum`. Parâmetros vazios/inválidos são recusados antes de cache ou rede, sem proibir valores vazios publicados na tabela.

A exportação é corrente. Um hash identifica uma aquisição, não garante revisão histórica selecionável; o dataset rejeita contexto `deterministic` antes de I/O. Consulte a [fonte RNC/SNPC](../sources/rnc.md) e o [contrato separado de protegidas](cultivares_protegidas.md).

## Exportação de 18/09/2026

Os CSVs da exportação de 18/09/2026 têm 38.325 registros RNC e 5.424 registros SNPC. A contagem HTML da pesquisa que precede cada CSV coincide com ele; essa concordância não garante snapshot transacional nem cobertura histórica.

No RNC, os 22.946 formulários, 4.256 nomes de cultivar e 4.271 mantenedores vazios publicados permanecem texto vazio; ambas as datas estão preenchidas nessa exportação. Há 924 grupos de formulários repetidos, com 15.142 ocorrências. No SNPC, o término tem 5.267 datas, 155 condições e dois vazios; as 155 condições e os dois vazios produzem data nula, preservando o texto. Há 102 certificados repetidos entre processos distintos, com 204 ocorrências. Nenhuma dessas repetições de identificadores secundários é deduplicada.

A aquisição original em `fetched_at` conserva UTC com fuso explícito, inclusive no cache e no dataset. Na fonte e no dataset, `fetch_timestamp` coincide com a aquisição. As datas civis da tabela são independentes desses instantes.
