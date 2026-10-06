# Lista Suja — capturas integrais de 18/09/2026

`manifest.json` registra cinco respostas: dois portais HTML idênticos, CSV,
TXT companheiro e PDF. Cada `.gz` é compressão determinística do corpo HTTP
decodificado completo, sem recorte nem reserialização. CSV, TXT e PDF saem com a
máscara de CPF (abaixo); o resto dos registros fica como publicado.
`files[].original` conserva método/URL/data/status e SHA/bytes originais;
`file`, `sha256` e `bytes` descrevem o gzip; `decoded_*`, o corpo guardado.

São 579 registros, 567 documentos distintos, 4.706 trabalhadores no campo
publicado e dez inclusões compostas. A edição periódica é 06/04/2026 e a
atualização cadastral 04/09/2026. Os corpos originais têm os mesmos hashes da
captura anterior. A recaptura é nova; a publicação não é uma nova revisão.

`resources[].oracle` identifica um array JSON comprimido por formato, com as
doze células de cada linha em `values`. CSV registra linha lógica/física e
linha TXT. PDF registra página (base 1) e bbox da linha no sistema PDF, com
limites de coluna no manifesto. `cases[].expected_positions` usa índices
zero-based nesses arrays; os filtros sobrepostos não aumentam a população.
`requests` segue o formato compartilhado em `../FORMAT.md`.

O gerador `build_mte_oracle.py` (fora do repositório) usa apenas
stdlib e os caracteres/coordenadas extraídos por `mte_pdf_cells.py` com
pdfminer, sem importar agrobr nem usar o extrator de tabelas da produção.
pdfminer é também dependência do pdfplumber; o algoritmo de leitura é distinto,
mas não há independência de motor PDF. Os 5.790 campos CSV/TXT coincidem.
PDF tem 45 páginas. Após normalizar espaços nos campos de origem, restam
duas diferenças de quebra após hífen (IDs 181 e 190). A comparação literal
das doze colunas finais tem 12 diferenças, detalhadas abaixo, sem correção
para igualar formatos.

O N1 inventaria todos os links do portal (inclusive links repetidos), campos,
tipos, edição, notas e grid PDF. CEAC e o XLSX alternativo são exclusões
nominais. O comparador `scripts.reconciliar_lista_suja` opera sobre estes
corpos locais, sem rede. Testes públicos cobrem população, filtros, nulos,
texto composto, último registro, vazio tipado e metadados. Controle noop
prova que reserializar CSV/TXT não causa as falhas das mutações.

Não há cache ou seleção histórica. Replay gera novos instantes de aquisição;
recibos preservam os originais. A captura usa o client e o matcher HTTP não
comprova ordem global, mas verifica URLs, métodos e consumo de respostas.
CSV/TXT/PDF são a mesma publicação; N3 não se aplica.

## Normalização e diferenças literais

Nos campos textuais derivados, espaços internos são colapsados como na
produção. Isso afeta seis células de `estabelecimento` no CSV desta captura:
IDs 170, 180, 356, 368, 410 e 525. Corpos originais permanecem idênticos byte a
byte; o manifesto declara a transformação e os valores crus/normalizados.
`data_inclusao_texto` via PDF preserva as quebras de linha da célula. A comparação
literal dos oráculos tem 12 diferenças: dez textos de inclusão composta e os
dois estabelecimentos citados. A comparação dos dez campos de origem após normalizar
espaços tem duas diferenças; a comparação literal das doze colunas finais
registra as 12 diferenças descritas acima.

`csv_whitespace_normalization` lista as seis células e os valores crus e
normalizados. `column_decisions[].transformation` descreve cada transformação.
`csv_pdf_differences` inventaria as 12 diferenças finais literais;
`csv_pdf_normalized_source_differences` conserva a comparação normalizada de
dez campos crus, com duas diferenças. Os oráculos não foram regravados para
esconder essas diferenças; a única regravação é a máscara de CPF.

## Máscara

Por decisão do mantenedor (25/09/2026), o CSV, o TXT e o PDF guardados aqui
são cópias mascaradas:

- o CPF de pessoa física (389 linhas, 383 CPFs) vira CPF sintético com DV válido
  da série fixa `000.000.NNN-DV`;
- o nome da pessoa física vira `PESSOA NNN`, também nos 49 estabelecimentos que o
  citam;
- o CPF dentro da razão social de 7 empresas vira o CPF sintético, e o resto do
  nome fica como publicado;
- CNPJ e demais dados de pessoa jurídica ficam como publicados.

O layout é o mesmo: no TXT, a largura de cada campo; no PDF, os mesmos glifos,
posições e quebras de linha (22 nomes em duas linhas e 1 que cruza a quebra). Os
oráculos saem da mesma troca, célula a célula. `files[].masked` marca os corpos
mascarados, e `files[].original` guarda o SHA do corpo publicado, que fica fora do
repositório. A seção `masking` do manifesto traz a regra e as contagens.
