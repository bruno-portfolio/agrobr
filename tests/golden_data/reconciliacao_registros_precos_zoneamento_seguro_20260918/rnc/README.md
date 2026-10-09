# RNC/SNPC — reconciliação

Formato próprio `agrobr.reconciliation.family` v1; ver `../FORMAT.md`.
Captura integral de 18/09/2026: 38.325 registros RNC e 5.424 SNPC; dez e onze
campos CSV, respectivamente. A API SNPC expõe também o texto do término.

Os oito corpos HTTP estão armazenados em gzip determinístico. Os CSVs
descompactam para os mesmos bytes/SHA dos originais, sem recorte, exceto uma
célula do CSV de registradas (abaixo). Nos seis
HTMLs, somente valores de tokens CSRF foram substituídos; os demais bytes,
formulários e totais são preservados. O gzip reconstitui transporte para teste,
não representa os bytes gzip originais na rede. Cada GET/POST da aquisição
tem resposta própria, incluindo os dois GETs diferentes no mesmo endereço.

Oráculos JSONL compactados contêm cada registro, posição lógica, linha física
e todas as células esperadas. Datas civis usam ISO; datas ausentes/condicionais
usam JSON null, enquanto textos vazios permanecem strings. A geração usa
stdlib csv/datetime sem importar agrobr, contrato ou parser. Treze consultas
abrangem população integral, primeiros/últimos registros, soja, campos vazios,
condição de término, situação e identificadores secundários repetidos.

N2: `pytest tests/test_rnc/test_reconciliacao.py`.
N1: `scripts/reconciliar_rnc.py --capture-dir DIRETORIO --output ARQUIVO`.
Geração: `build_rnc_oracle.py` (fora do repositório), incluindo as
coortes completas de formulário/certificado, que já exercitam identificadores
secundários repetidos; aliases de casos idênticos foram removidos.

Nenhum N3: HTML, CSV e cache têm a mesma origem. Igualdade do total da pesquisa
com o CSV não é prova de snapshot transacional, integralidade histórica nem
interpretação jurídica da situação. Hashes e posições valem apenas nesta edição.

Máscara: no CSV de registradas, o CPF de pessoa física dentro da razão
social de 1 mantenedor virou CPF sintético com DV válido (série fixa
`000.000.NNN-DV`), e o resto do nome e as demais células ficam como publicados. O
oráculo sai da mesma troca. O item tem `masked: true`; `original` guarda o SHA do
corpo publicado, que fica fora do repositório, e a seção `masking` do manifesto
dá a posição.

Treze consultas somam 50.284 registros esperados por camada, com sobreposição;
43.749 registros distintos nos dois corpos completos. As duas antigas cópias
`*_repeated_secondary_id` não ampliavam cobertura e foram retiradas. O teste
no-op reserializa o CSV SNPC inteiro antes da chamada HTTP pública e exige todas
as células iguais; assim a mudança de BOM/aspas não explica as falhas de mutação.
Os originais são UTF-8 sem BOM; o decoder utf-8-sig aceita ambos.

Limite da sequência: a captura utilizou agrobr.rnc.client. A contagem de quatro
requisições não fornece um oráculo independente do protocolo. `compare_html`
é a guarda independente dos formulários, controles, formato de exportação e
totais capturados. O replay distingue as duas ocorrências GET, sem validar os
corpos POST nem impor uma ordem global a métodos diferentes.
