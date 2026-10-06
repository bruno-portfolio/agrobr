# Reconciliação — IBGE e fallback CONAB

## Lote 1: PAM, PEVS, PPM e fallback anual

Os três manifestos `lot1*manifest.json` somam 1.340.356 bytes: 41 casos e
423 amostras numéricas/semânticas na CI. O oráculo completo da PAM/PEVS tem
1.639 amostras;
foi conferido antes da redução das amostras da CI, que conserva todas as chaves.

- `lot1_manifest.json`: 15 casos PAM e dois PEVS monetários. Os CSVs PAM são
  registros crus exportados do cliente, sem corpo HTTP original preservado.
  Os JSONs PEVS são originais; sua proveniência antiga não informa instante UTC.
- `lot1_conab_manifest.json`: 18 seleções, seis produtos, XLSX do levantamento
  2025/26. Corpos e inventário da CONAB são referenciados por hash, sem duplicação.
- `lot1_conab_legacy_manifest.json`: seis seleções de soja/trigo no XLS original
  de 2020/21, capturado em 18/09/2026. Cabeçalhos e células lidos com xlrd.

O transporte de replay remove somente o cabeçalho descritivo dos JSONs PEVS e
serializa os registros CSV para a resposta SIDRA `h/n`; isso não constitui prova
da aquisição original da PAM. O fallback CONAB usa os corpos HTTP originais.

N1 integral SIDRA, PPM e PEVS quantidade continuam pendentes: as amostras antigas
desses dois últimos grupos são declaradamente simuladas. Não se concede N2 de
fonte a elas. `lot1_coverage.json` (fora do repositório) discrimina as 14 variantes e
produtos faltantes. `lot1_n3.json` mantém seis fichas CONAB/PAM sem equivalência
concedida, pois os períodos capturados não coincidem e falta ficha metodológica.

Os geradores independentes ficam fora do repositório; não importam parsers de agrobr.
As falhas anteriores às correções são preservadas. O fechamento e as limitações
de cada execução constam de `lot1_checkpoint.json`.

## Lote 2: trimestrais

`lot2_manifest.json` contém três replays locais e 35 amostras. Leite e PIB
são explicitamente simulados; abate bovino tem proveniência incompleta. Nenhum
recebe certificado N2 de fonte. As seis variantes pendentes estão discriminadas
em `lot2_coverage.json`, fora do repositório.

## Lote 3: censos SIDRA

`lot3_manifest.json`: 1 CSV legado, 40 amostras, sem proveniência suficiente
para N2 de aquisição. `lot3_synthetic_regressions.json` contém somente cenários
sintéticos de regressão. As 34 variantes pendentes estão em `lot3_coverage.json`
(fora do repositório). O corpo vazio de 1995 no replay só isola 2017; não prova ausência
de publicação oficial.

## Lote 4: legado 1995/96 / legacy census

169 URLs e todos os membros ZIP inventariados. O manifesto referencia o expected
UF aprovado e registra a transcrição independente das sete tabelas nacionais.
ZIP de máquinas PA permanece recusado. Timestamps antigos ausentes não foram
inventados. Todos os bytes/oráculos existentes foram preservados.

All 169 ZIP members are inventoried. State expectations reuse the approved
independent oracle; national expectations transcribe raw BIFF headers and cells.
The incorrect PA machinery archive remains rejected. Missing historical capture
timestamps remain missing. Existing source files and oracles are unchanged.
