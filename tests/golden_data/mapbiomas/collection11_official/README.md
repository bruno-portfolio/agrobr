# Recorte oficial MapBiomas coleção 11

`response.xlsx` contém o cabeçalho e a linha original 3 das abas `COVERAGE_11`
e `TRANSITION_11` do arquivo estadual oficial, capturado em 06/09/2026. A
extração utilizou openpyxl com `data_only=True`, sem importar o parser agrobr.
As linhas foram preservadas integralmente: 41 anos de cobertura e 62 períodos
de transição. Não é um workbook estadual completo nem contém dados municipais.

`provenance.json` registra URL oficial, hashes original/recorte, coordenadas,
cabeçalhos e valores usados como oráculo independente. A linha original 3
ocupa a linha 2 no recorte. O hash do parser anterior é apenas evidência da
investigação; nenhum teste depende desse arquivo de implementação.

Este caso é exercitado diretamente por `tests/test_mapbiomas/test_official_columns.py`.
O descobridor genérico MapBiomas em `tests/test_golden.py` continua dedicado à
coleção 10; por isso este recorte usa provenance próprio, sem metadata/expected
que o fariam ser interpretado como uma fixture da coleção 10.
