# PSR — reconciliação

Três CSVs integrais, catálogo CKAN e dicionário PDF capturados em 18/09/2026,
preservados com recibos/SHA.
Inventário independente: 46.137 + 617.683 + 1.048.565 registros de 38 campos.

Os CSVs deste diretório são derivados explícitos: coortes completas por ano/UF/
município/cultura, transcodificadas a UTF-8. Nome e documento do segurado são
esvaziados; todos os campos usados pela API conservam o literal publicado.
Os originais completos permanecem preservados; não confundir hashes do recorte
com hashes do corpo HTTP. Cada arquivo inclui seu último registro real.

Oráculo sem agrobr, por csv/Decimal: 12 coortes, 24 consultas, 913 registros
esperados por camada (fonte e dataset). Comparação de todas as células e da
multiplicidade, sem impor unicidade ao número da apólice. Inclui valores ausentes,
zeros, indenizações positivas, zeros iniciais e pontas dos três arquivos.

N2: `pytest tests/test_mapa_psr/test_reconciliacao.py`.
N1: `scripts/reconciliar_psr.py --capture-dir DIRETORIO --output ARQUIVO`.
Geração: `build_psr_oracle.py`, fora do repositório.
Replays adicionais dos corpos integrais ficam fora do repositório.
N3 não concedido; versões e formatos da mesma origem não são independentes.

Formato identificado: `agrobr.reconciliation.family`, versão 1, documentado
em `../FORMAT.md`; distinto do manifesto genérico v2. A mudança de rótulo não
recalcula esperados nem modifica os corpos CSV/XLSX.

884 registros derivados. A coorte completa 2014/SP/PIRASSUNUNGA/FEIJÃO 1ª SAFRA
acrescenta a apólice literal `NULL`, registro original 303152 (linha física 303153).
O leitor preserva tokens em campos textuais; conversão numérica mantém a política anterior.
Replays do corpo integral e controles dos tokens ficam fora do repositório.

Replays dos corpos integrais fazem parte da suíte em `tests/test_mapa_psr/test_reconciliacao_originais.py`.
Aponte `AGROBR_RECONCILIACAO_PSR_ORIGINALS` para o diretório com os corpos originais para executá-los; o modo padrão registra skips explícitos.
Os arquivos têm os nomes de `resources[].original.file`; se o modo
for ativado sem os arquivos, os testes falham indicando o corpo ausente.
