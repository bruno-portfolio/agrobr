# Recorte municipal oficial da Coleção 11

`municipal11_selected.xlsx` é um pacote XLSX **derivado**, com 142 linhas escolhidas da aba `COVERAGE_11`. Não é o corpo HTTP original nem a população municipal completa. Os valores XML numéricos, textos e 41 anos publicados foram preservados; os índices de linha foram remapeados no recorte.

`raw_oracle.json` identifica a origem de cada célula selecionada e seus tipos/lexemas. `verification.json` registra 7.810 células comparadas independentemente com openpyxl, sem parsing agrobr. `manifest.json` registra a URL oficial, hashes distintos do ZIP adquirido e XLSX original, bytes, seleção e hashes dos arquivos permanentes.

A seleção inclui Distrito Federal, Sorriso, Comodoro, Cáceres, quatro linhas de classe zero e as dez linhas do geocódigo `2703007`, incluindo as interseções publicadas em Alagoas e Pernambuco. Estas não devem ser corrigidas ou deduplicadas por prefixo do geocódigo. A identidade representa interseções territoriais MapBiomas; não se promete um cadastro IBGE.

O download original foi único e público. Replays podem colocar este XLSX em um ZIP de transporte artificial, explicitamente derivado, para testar a extração sem copiar o recurso integral. Nenhum teste deve depender dos arquivos em `reports`.
