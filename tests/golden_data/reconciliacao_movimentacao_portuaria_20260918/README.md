# Reconciliação — movimentação portuária (ANTAQ)

Oráculo N2 (`manifest.json`, formato v2, e `oracle.json`) do replay público de `antaq.movimentacao` e
`datasets.movimentacao_portuaria`. O esperado vem de leitura independente dos bytes com `csv`,
`decimal`, `datetime` e `zipfile` da stdlib, sem importar `agrobr`. Construtor:
`build_oracle.py`, fora do repositório.

## Corpos

Nenhum arquivo novo de fonte. Os três TXT ficam em `../antaq/movimentacao_sample/` e são
referenciados por caminho relativo em `files[]` com SHA-256, bytes e data de captura
(21/02/2026, recorte real do ano 2024): `atracacao.txt` (3.713 B, 10 registros, 29 campos),
`carga.txt` (2.110 B, 10 registros, 27 campos) e `mercadoria.txt` (318.472 B, 1.403 registros,
6 campos). `oracle.json` é o único arquivo de dados deste diretório.

## Container de transporte

O ZIP oficial **não** foi preservado em 2026-02 — só os TXT já extraídos. O replay monta em memória
dois containers `zipfile` com os membros `2024Atracacao.txt`, `2024Carga.txt` e `Mercadoria.txt`,
exatamente os nomes que `agrobr.antaq.client` procura, e substitui apenas `requests.get`
(`agrobr.antaq.client` usa `requests`, não `httpx`, então `tests.helpers.install_replay_http` não se
aplica). Download, validação de magic bytes `PK\x03\x04`, tamanho mínimo, cadeia de encoding, parser,
join, filtros, agregação do dataset e validação de contrato rodam inteiros. O container é
**derivado**, declarado em `transport_container`, e não é o ZIP oficial da ANTAQ.

## Cobertura

`fonte_2024_completo` compara 10 × 21 = 210 células na ordem final do parser e como multiconjunto;
`dataset_2024_agregado` compara 6 × 21 = 126 células depois da agregação por PK. Doze casos de filtro
exercitam os seis seletores públicos (`mercadoria`, `porto`, `uf`, `sentido`, `tipo_navegacao`,
`natureza_carga`) com um recorte positivo e um vazio cada.

Cardinalidade registrada em `oracle.json`: 10 atracações (10 IDs distintos), 10 cargas referenciando
6 atracações, fan-out máximo 5 na atracação `1406197`, 4 atracações sem carga (não aparecem na saída,
porque o join parte da carga), 0 cargas órfãs e 1.403 códigos de mercadoria distintos
(o `drop_duplicates` do join é no-op neste recorte).

## Decisões que o inventário registra

- `tipo_navegacao` vem de `Tipo Navegação` (carga). A atracação publica `Tipo de Navegação da
  Atracação`, que é lida e descartada na projeção do join; as duas divergem na atracação `1406197`
  (`Longo Curso` contra `Apoio Portuário` em quatro das cinco cargas).
- `mercadoria` é a `Nomenclatura Simplificada Mercadoria`. A descrição NCM completa (`Mercadoria`)
  é lida e descartada; o filtro público `mercadoria` casa só com o nome curto.
- `uf` vem de `SGUF` (sigla); `UF` (nome por extenso) é lida e descartada.
- `peso_bruto_ton` remove ponto de milhar e troca vírgula por ponto; `qt_carga` só troca a vírgula.
- `QTCarga` não tem unidade publicada nem informada no corpo: na carga 35452565 vale 27.500.000 contra
  27.501,04 t de peso bruto, e nas cargas de apoio vale inteiros pequenos. A leitura "kg" e "unidades" é
  inferência do revisor pela escala, não unidade publicada.
- `Ano`/`Mes` são o período publicado da atracação, não a data: a atracação `1406197` começou em
  22/12/2023 e é publicada com `Ano=2024`, `Mes=jan`.

## Limites

- Os TXT são extrações; o ZIP oficial não existe no repositório e a fonte está fora do ar desde
  23/06/2026. Sondagem de 18/09/2026:
  HTTP 200 redirecionado para o aviso oficial, `text/html`, 174.818 B, sem assinatura ZIP.
- O recorte tem 10 cargas de janeiro/2024 em AM e PA. Não cobre `apoio_maritimo`, carga
  conteinerizada, `TEU > 0`, outros meses, outros anos nem outras UFs — está em `pendencias[]`.
- Carga órfã sem atracação e `CDMercadoria` repetido não ocorrem no corpo; só por mutação sintética.
- `qt_carga` é declarado FLOAT no contrato e sai `int64` neste recorte porque todo `QTCarga`
  publicado é inteiro; `ColumnType.FLOAT` aceita qualquer dtype numérico. Registrado em
  `dtypes_observados`.
- N3 não se aplica: movimentação portuária conta atracação e peso bruto movimentado, não exportação
  aduaneira. Sem ficha de equivalência.
