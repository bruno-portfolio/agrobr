# Agrofit — reconciliação

Formato próprio `agrobr.reconciliation.r11.family` v1; ver `../FORMAT.md`.
Dois CSVs completos e catálogo CKAN capturados em 18/09/2026. O gzip é somente
armazenamento/transporte reconstruído: a descompactação dos CSVs produz os
mesmos bytes e SHA dos originais. Limites mínimos reais do downloader mantidos.

Oráculos JSONL independentes usam csv/Decimal, sem importar agrobr ou contratos:
4.403 produtos formulados com 12 campos; 279.707 autorizações no total, das quais
10.232 linhas com dez campos são comparadas na coorte de autorizações;
2.992 técnicos com oito campos diretos. Ingrediente e grupo derivados dos técnicos
são comparados nas dez colunas completas das nove coortes técnicas selecionadas.

As 32 coortes de registro preservam todas as ocorrências publicadas: nove técnicas
e 23 formuladas, incluindo o último registro de ambos os CSVs. Seus 57 componentes
foram transcritos/conferidos independentemente, com posição, texto, unidade e valor.
A anotação curada de setembro/6 só foi reutilizada após igualdade literal com a
composição atual. Componentes adicionais das pontas e pré-mistura foram lidos
diretamente; duas expressões ambíguas publicadas têm valor e unidade nulos.
Não se afirma interpretação numérica independente de todos os componentes.

Todas as coortes são comparadas nas saídas públicas integrais; oito também usam
o filtro público de registro, tanto na fonte quanto nos datasets. Cache frio,
quente e bypass preservam as células relevantes. Não se deduplicam autorizações,
nem ingredientes iguais em posições diferentes. Nulos e zeros permanecem distintos.

N2: `pytest tests/test_defensivos/test_reconciliacao.py`.
N1: `scripts/reconciliar_agrofit.py --capture-dir DIRETORIO --output ARQUIVO`.
Geração: `build_agrofit_oracle.py` e suplemento
`extend_agrofit_ambiguous.py` (a definição futura já inclui as duas coortes extras).
N1 declara rótulos estruturais de concentração, que podem incluir expressões
ambíguas; não certifica número nem unidade somente por reconhecer um sufixo.

Os corpos completos (fora do repositório) e os gzips portáteis têm recibos próprios.
A reexecução do transporte obtém um novo horário de aquisição; o recibo da
captura de referência continua separado. Nenhum N3 concedido: publicações,
catálogo e cache têm a mesma origem, sem garantia de população histórica.

## Coorte de autorizações (F2)

O oráculo integral (279.707 linhas) permanece byte a byte em
autorizacoes_oracle_full.jsonl.gz.
O golden guarda 10.232 linhas em 329.210 B gz; `rows` continua sendo 279.707,
`subset_rows` registra 10.232 e `subset_rule` guarda a regra reproduzível e os localizadores.
O teste exige total integral e inclusão do multiconjunto completo da coorte (dez colunas,
com multiplicidades), preservando igualdade integral nos demais oráculos.

A seleção é a união, na ordem de `original_record`, de:

- Primeiros e últimos 50 registros lógicos.
- Todas as ocorrências dos 23 ids formulados curados, incluindo 12516 e 9815.
- Todas as ocorrências dos ids com mais/menos autorizações (7316 e 00322),
  com desempate lexicográfico pelo id textual, preservando zeros à esquerda.
- Todas as ocorrências dos ids nas posições 0, 200, 400, ... e última da lista
  ordenada de 4.403 ids.
- Todas as linhas com cultura, praga ou nome comum ausentes; literal NULL seria
  incluído, mas nenhuma coluna do CSV capturado contém esse token textual.
- Todas as 322 linhas com aspas internas e todas as multiline (nenhuma nesta captura);
  primeiras e últimas 25 linhas por coluna com delimitador dentro do campo, incluindo
  as dez ocorrências em INGREDIENTE_ATIVO. O manifesto identifica as posições.

Linhas selecionadas por mais de um critério aparecem uma só vez, mas autorizações
iguais de registros diferentes mantêm sua multiplicidade. As linhas JSONL são copiadas
sem reserialização; gzip usa filename vazio, mtime=0 e nível 9. Corpos e demais oráculos intactos.

Perda deliberada: células de autorizações fora da coorte, incluindo outros registros
com delimitador interno, não são mais comparadas individualmente; o total de saída
continua verificado. Contagem integral não substitui a comparação das células omitidas.

Regeneração para conferência, sem modificar o golden:
`python build_agrofit_oracle.py --authorization-cohort --csv-delimiter-edge-rows 25 --output-dir DIRETORIO_NOVO`

The complete authorization oracle remains outside the repository. Only the declared
cohort is checked cell by cell, retaining multiplicities and the complete output row
count. Cells outside that cohort are deliberately no longer compared individually;
the original CSV and all other oracles remain unchanged.
