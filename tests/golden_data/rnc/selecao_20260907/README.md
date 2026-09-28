# RNC/SNPC — recortes oficiais de 07/09/2026

Uma aquisição pública completa por família, pelo client agrobr vigente, produziu 38.335 registros RNC e 5.424 registros SNPC. Os CSVs integrais e a observação HTTP ficam fora do repositório. Os bytes coincidiram com a captura de 06/09, mas estes foram novos downloads, com horário e hash próprios.

Os CSVs deste diretório são **derivados**: 29 registros RNC e 27 SNPC, extraídos por `csv.reader`/`csv.writer` da biblioteca padrão, conservando cabeçalho e todas as células. Não são respostas HTTP integrais. `expected.json` liga cada célula a `source_record` (registro de dados contado a partir de 1, sem cabeçalho) e à coluna do CSV completo.

O oráculo não importa agrobr nem pandas. `expected[family].rows` contém dicionários finais, na ordem do subset; `columns` define as dez/doze colunas; `records` contém os dados brutos, posição e razões da seleção. Textos usam `str.strip` e conservam vazios como `""`. Datas civis usam ISO; `None` representa data ausente ou o término textual oficial. `termino_protecao_texto`, a 12ª coluna, conserva todo texto de término, inclusive quando a célula contém data válida. A expressão `até a emissão do certificado definitivo` não é convertida em data inventada.

A seleção cobre todos os oito grupos RNC e seis situações SNPC observados, ausências legítimas, datas mínimas/máximas e bissextas existentes, identificadores curtos/pontuados, espaços, aspas e campos compostos. Inclui dois pares completos de certificados repetidos, 20190277 e 20260093; seus processos são distintos. Não houve soma, deduplicação ou divisão de listas de pessoas.

Os HTMLs são estruturas reconstruídas e sanitizadas de formulários de sessão pública: cookies nunca foram gravados e tokens/valores de controles foram substituídos. `*_search_official_total.html` preserva a contagem textual observada em uma busca suplementar, posterior à exportação (38.335/5.424). `*_search_subset.html` é um **template de replay com total sintético 29/27**, ajustado ao recorte; não deve ser apresentado como HTML original nem como população oficial completa. O manifesto distingue hash bruto, hash sanitizado, sessões e transformação.

Os goldens anteriores permanecem intactos. O gerador independente fica fora do repositório.
