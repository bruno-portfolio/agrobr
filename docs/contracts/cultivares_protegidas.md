# Contrato: cultivares_protegidas

O dataset reutiliza o contrato de fonte **`rnc_protegidas` 1.0**, constante `RNC_PROTEGIDAS_V1` e nome interno `rnc.protegidas`. A tabela descreve a consulta corrente SNPC; não se une ao RNC por nome da cultivar. Consulte a [API dos datasets](../api/cultivares.md).

## Schema

As onze colunas anteriores permanecem como prefixo. `termino_protecao_texto` é a décima segunda coluna. Textos são strings, no dtype padrão do pandas instalado (`str` no pandas 3, `object` no 2); datas são civis `datetime64[ns]`, sem timezone ou horário.

| Coluna | Tipo pandas | Nulo | Significado |
|---|---|---|---|
| `cultivar` | str | Não | Nome publicado da cultivar |
| `nome_cientifico` | str | Não | Nome científico publicado, com brancos internos repetidos juntados em 1 espaço |
| `nome_comum` | str | Não | Nome comum da espécie, com brancos internos repetidos juntados em 1 espaço |
| `nr_processo` | str | Não | Número de processo, chave textual não vazia |
| `situacao` | str | Não | Situação de proteção publicada |
| `nr_certificado` | str | Não | Número de certificado; pode repetir entre processos |
| `inicio_protecao` | datetime64[ns] | Sim | Data civil inicial publicada |
| `termino_protecao` | datetime64[ns] | Sim | Data civil de término quando determinada |
| `titular` | str | Não | Texto de titulares, sem divisão de nomes |
| `representante_legal` | str | Não | Texto de representantes legais |
| `melhoristas` | str | Não | Texto de melhoristas; vazio publicado é preservado |
| `termino_protecao_texto` | str | Não | Célula original do término após strip, inclusive data, condição ou vazio |

**Chave primária:** `nr_processo`, dentro da família e recurso processados. Certificado não é chave: processos distintos podem publicar o mesmo número. Não deduplicar por certificado, cultivar, titular ou data. IDs permanecem strings; pontuação e zeros são preservados.

## Término determinado, condicional e ausente

| Texto publicado após strip | `termino_protecao` | `termino_protecao_texto` |
|---|---|---|
| `08/08/2034` | Data civil 2034-08-08 | `"08/08/2034"` |
| `até a emissão do certificado definitivo` | `NaT` | A expressão integral |
| Célula vazia | `NaT` | `""` |

Somente o vazio e a expressão oficial exata permitem término nulo. Uma data textual válida exige escalar correspondente; texto inválido, data impossível ou divergência entre texto e escalar falham. Não escolher uma data presumida nem converter indiscriminadamente texto desconhecido em `NaT`. A condição não determina automaticamente a situação: registros com situações diferentes podem mantê-la.

`inicio_protecao` admite ausência sem imputação. Ambas as datas exigem precisão ns, sem timezone ou horário, inclusive em vazio. Datas civis não recebem o instante UTC da coleta.

## Validação, vazios e limites

Fonte e dataset validam o contrato sempre, mesmo com `return_meta=False`. A população integral é validada antes dos filtros. Colunas obrigatórias ausentes, rótulos duplicados, chave ausente/vazia/repetida ou tipos incompatíveis causam erro. A fonte também valida o layout e os campos externos com modelos de linha.

Strings vazias permanecem strings, não números ou marcadores de nulidade. Campos de nomes compostos não são divididos e repetições publicadas não são removidas. Recortes sem correspondência conservam as doze colunas e seus tipos; CSV inválido não é reinterpretado como recorte vazio.

A tabela não é uma avaliação de vigência jurídica. Situação, datas e titulares são os campos publicados, sem inferência de direito de comercialização, extensão de proteção ou vínculo automático com registro RNC.

## Metadados e filtros

`schema_version` e `contract_version` são `"1.0"`; não há alias de contrato denominado `cultivares_protegidas`. A rota é `rnc_protegidas`, preservada em `selected_source` e `attempted_sources`, inclusive ao reutilizar cache.

Hash e tamanho representam o CSV bruto; coleta original, expiração e diagnósticos da fonte permanecem nos metadados. `records_count` descreve o recorte; a contagem integral e seu confronto com o total da busca ficam em `source_details.coverage`. Igualdade de contagens não garante snapshot transacional.

`nr_processo` e `nr_certificado` selecionam igualdade textual completa após strip das bordas. Outros filtros são substrings literais sem distinção de caixa; `especie` consulta nome comum. O dataset rejeita contexto `deterministic` antes de I/O porque não seleciona revisões históricas. Consulte a [fonte](../sources/rnc.md) e o [contrato de registradas](cultivares_registradas.md).

## Exportação de 18/09/2026

Os CSVs da exportação de 18/09/2026 têm 38.325 registros RNC e 5.424 registros SNPC. A contagem HTML da pesquisa que precede cada CSV coincide com ele; essa concordância não garante snapshot transacional nem cobertura histórica.

No RNC, os 22.946 formulários, 4.256 nomes de cultivar e 4.271 mantenedores vazios publicados permanecem texto vazio; ambas as datas estão preenchidas nessa exportação. Há 924 grupos de formulários repetidos, com 15.142 ocorrências. No SNPC, o término tem 5.267 datas, 155 condições e dois vazios; as 155 condições e os dois vazios produzem data nula, preservando o texto. Há 102 certificados repetidos entre processos distintos, com 204 ocorrências. Nenhuma dessas repetições de identificadores secundários é deduplicada.

A aquisição original em `fetched_at` conserva UTC com fuso explícito, inclusive no cache e no dataset. Na fonte e no dataset, `fetch_timestamp` coincide com a aquisição. As datas civis da tabela são independentes desses instantes.
