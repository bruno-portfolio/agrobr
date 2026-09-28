# Legenda oficial MapBiomas (aba LEGEND_CODE), coleções 10 e 11

`legend_code.json` transcreve, com openpyxl (`data_only=True`), todas as linhas da aba `LEGEND_CODE` dos workbooks estaduais
oficiais capturados em 18/09/2026 (coleção 10: `data.mapbiomas.org/api/access/datafile/457?format=original`, 24.290.425
bytes; coleção 11: `MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx`, 17.947.768 bytes, mesmo SHA-256 do recorte
`collection11_official`). Cada célula traz a coordenada de origem e o valor sem edição (inclusive espaços e numeração).
`classes_nos_dados` lista os códigos presentes em `COVERAGE_*` e `TRANSITION_*` do mesmo workbook. O workbook municipal
da coleção 11 tem a mesma legenda em português (diferem só espaços à esquerda e o tipo do código).

O construtor e os workbooks originais ficam fora do repositório.
