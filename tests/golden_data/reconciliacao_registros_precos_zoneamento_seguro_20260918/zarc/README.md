# ZARC — reconciliação

Três CSVs integrais, catálogo CKAN e dicionário PDF oficiais capturados em
18/09/2026. Originais e recibos ficam fora do repositório.
Inventário: 53.583 + 957.490 + 2.289.882 registros de 55 campos. Os corpos
mudaram de hash desde 07/09; comparação integral preservando multiplicidade
comprovou apenas reordenação. Posições pertencem ao SHA do respectivo corpo.

Derivados principais: 1.397 registros literais, com todas as ocorrências dos
casos limítrofes antigos e coortes municipais completas das primeiras/últimas
linhas atuais. Campos preservados; CSV reserializado UTF-8/LF. Suplemento:
15 registros de Arroz/Trigo Sequeiro, com sobreposição explícita à seleção principal.
Cada arquivo principal inclui a última linha real do corpo integral.

Oráculo independente csv/int, sem agrobr: todas as 58 colunas, 15 consultas e
2.759 registros esperados por camada. Fonte/dataset, sem cache/cache frio/quente;
catálogo real e apenas HTTP substituído. Suplemento exercita nomes literais e
canônicos legados nas duas camadas e três modos de cache. Produtividade segue
texto sem unidade inferida, risco vazio não é zero, 50 é preservado, duplicatas
não são eliminadas. Manifesto de formato próprio v1 inclui decisões para 55 colunas e 13 recursos.

N2: `pytest tests/test_zarc/test_reconciliacao.py`.
N1: `scripts/reconciliar_zarc.py --capture-dir DIRETORIO --output ARQUIVO`.
Geração: `select_zarc.py` e `build_zarc_oracle.py`, fora do repositório.
Replays integrais e a comparação de multiconjuntos ficam no mesmo relatório.
N3 não concedido; publicações/formatos da mesma origem não são independentes.

Formato identificado: `agrobr.reconciliation.family`, versão 1, documentado
em `../FORMAT.md`; distinto do manifesto genérico v2. A mudança de rótulo não
recalcula esperados nem modifica os corpos CSV/XLSX.

Replays dos corpos integrais fazem parte da suíte em `tests/test_zarc/test_reconciliacao_originais.py`.
Aponte `AGROBR_RECONCILIACAO_ZARC_ORIGINALS` para o diretório com os corpos originais para executá-los; o modo padrão registra skips explícitos.
Os arquivos têm os nomes de `resources[].original.file`; se o modo
for ativado sem os arquivos, os testes falham indicando o corpo ausente.

Cadência: CKAN declara semanal, PDF declara diária. O dataset adota semanal,
pois o catálogo ativo é usado na descoberta dos recursos; o conflito e os recibos
ficam em `update_frequency_decision`. A cadência real de cada safra não foi inferida.
O suplemento legado é reproduzível por `rebuild_zarc_legacy.py --output-dir DIRETORIO_NOVO`; CSV e esperados iguais à
entrega foram verificados sem reescrever os goldens aceitos.
