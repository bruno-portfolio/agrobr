# Reconciliação — uso do solo, desmatamento e queimadas

Oráculo N2 (`manifest.json`, formato v2) e corpos usados no replay público de
`datasets.desmatamento`, `datasets.queimadas` e `datasets.uso_do_solo`. O esperado vem de leitura
independente dos bytes (`csv`/`json`/`zipfile`/`decimal` da stdlib e `openpyxl`), sem importar
`agrobr`. Os construtores (`build_oracle.py`,
`build_excerpts.py` e `capture_live.py`) ficam fora do repositório.

## Desmatamento (INPE TerraBrasilis, capturas de 18/09/2026)

`desmatamento/` traz os corpos HTTP **originais**, sem reserialização: um `resultType=hits` e as
páginas de dados de cada seleção, nas URLs exatas que `agrobr.desmatamento.client` emite. Cada
seleção é uma **população completa e reconciliada** (`numberMatched` = feições recebidas), não uma
semente: PRODES Amazônia TO/2025 (54), Cerrado SP/2024 (51, também em três páginas com
`tamanho_pagina=20`), Caatinga SE/2025 (410), Mata Atlântica GO/2025 (11), Pantanal MS/2002 (6),
Pampa RS/2003 (8); DETER Amazônia AP em 01/2026 (4) e Cerrado SP em 01–03/2026 (14).

O cliente consulta `resultType=hits` antes e depois das páginas; a captura preservou **uma** resposta
`hits` por seleção (anterior às páginas, com recibo próprio) e o replay serve esse mesmo corpo às duas
consultas — não há segunda aquisição real nem segundo recibo.

As sementes de `../desmatamento/selecao_20260907/` **não** são usadas aqui: elas têm filtro de
`fid/gid IS NOT NULL` e `count6`, sem o corpo `hits` correspondente, e não sustentam a agregação do
dataset, que exige seleção reconciliada.

## Queimadas (INPE BDQueimadas)

`queimadas/*_recorte.csv` são **recortes byte a byte** dos CSVs oficiais: cabeçalho mais faixas
contíguas (início, meio, fim e, no mensal, as duas linhas em torno da 21.126, a única do mês com
`bioma` publicado vazio), com os offsets de byte e a linha de origem registrados em `files[].blocos`.
O SHA-256 e o tamanho do corpo completo ficam em `files[].corpo_original_*`
(mensal 04/2025: 4.736.146 bytes, 29.774 linhas; diário 10/09/2026: 3.171.313 bytes, 20.400 linhas).
Nenhum mês publicado cabe em 2 MB — o menor arquivo mensal disponível em 18/09/2026 tem 4,7 MB.
O recorte preserva a primeira e a última linha reais, mas **não é a população do período**.

## MapBiomas

Sem captura nova no golden. A coleção 11 estadual reaproveita
`../mapbiomas/collection11_official/response.xlsx` (recorte oficial de 06/09/2026), cuja identidade
foi reconferida ao vivo em 18/09/2026: o workbook publicado de 17.947.768 bytes tem SHA-256 idêntico
ao registrado na captura. A coleção 10 estadual entra como recorte derivado
(`mapbiomas/mapbiomas_col10_biome_state_recorte.xlsx`, cabeçalho + linhas 2–4 das abas `COVERAGE_10`
e `TRANSITION_10`) do workbook oficial de 24.290.425 bytes capturado em 18/09/2026. O municipal 10
reaproveita `../mapbiomas/municipal10_official/municipal10_classes_0_13.xlsx` e o municipal 11
reembala `../mapbiomas/municipal11_official/municipal11_selected.xlsx` em
`mapbiomas/municipal11_transporte.zip`, o ZIP de transporte derivado previsto no README daquele
recorte — o Drive entrega o XLSX municipal dentro de um ZIP de membro único.

## Limites

- Recortes e reembalagens estão marcados em `files[]`; nenhum é o corpo completo da publicação.
- A página HTML de confirmação do Google Drive não foi capturada: o replay municipal 11 entrega o
  ZIP diretamente.
- `prodes_geo`/`deter_geo` (geometria) e as rotas `.zip` mensal/anual dos focos ficam sem corpo
  oficial; constam em `pendencias[]`.
- N3 não se aplica: cobertura anual, desmatamento consolidado, alerta e foco de calor medem
  entidades e eventos distintos, sem ficha de equivalência.
