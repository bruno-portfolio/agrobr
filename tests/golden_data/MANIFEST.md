# Manifesto de reconciliação v2

Formato para N1 (estrutura) e N2 (valor/rótulo na saída pública). Não substitui
`expected.json` dos goldens antigos. A existência de um arquivo não comprova
cobertura; o esperado deve vir de leitura independente dos bytes crus, nunca do
parser, normalizador ou contrato sob teste.

## Campos

| Campo | Regra |
|---|---|
| `format_version` | `2`; o manifesto v1 (série histórica da CONAB) segue em `1`. |
| `oracle`, `coordinate_system` | Ferramenta/método independente e convenção de posições. Excel A1; JSON Pointer; CSV linha de dados a partir de 1; PDF página/tabela/linha/coluna a partir de 1; HTML seletor e ocorrência a partir de 1. |
| `files[]` | `file` único, relativo ao diretório do golden, `url`, `sha256`, `bytes`, `captured_at` com fuso. Preservar corpo original completo; ZIP/PDF não pode ser substituído apenas por tabela extraída. Método, parâmetros de consulta, revisão e limitações da captura entram quando necessários. |
| `cases[]` | `id` único, `dataset`, `selection` (argumentos públicos), `file` principal, `layout`; `files` opcional para casos que juntam vários arquivos. Toda referência aponta para `files[]`. |
| `structure[]` no caso | Inventário por item: `kind`, `locator`, `estado`, `campo`, `multiplicador`, `motivo`. Tipos: `sheet`, `column`, `row`, `variable`, `classification`, `series`, `pdf_table`, `html_selector`, `archive_member`. O localizador deve identificar o item sem depender da ordem de saída. |
| `period_columns` no caso | Perfil Excel compatível com o v1: mapa aba → lista de `column` (índice a partir de 0), `cell` A1, `raw`, `normalized`, `estado`, `motivo`. Outros formatos usam `periods[]` com `locator` no lugar de aba/célula. Guardar rótulo publicado e decisão, inclusive períodos excluídos. |
| `samples[]` no caso | `key` completa e única na saída, `column`, `value`, `raw_cells[]`, `conversion` textual e revisada. `null` é valor nulo esperado; ausência de linha usa `absent_rows`. Não executar a expressão de conversão como código. |
| `raw_cells[]` | Valor cru, rótulos/chaves, unidade publicada e localização da unidade. Excel mantém `sheet`, `cell`, `row_label`, `row_label_cell`, `header`, `header_cell`, `value`, `published_unit`, `unit_cell` do v1. Demais formatos usam `locator` (ex.: `{"pointer":"/data/7/primaryValue"}`), `value`, rótulo e unidade, com `file` quando diferente do principal. |
| `absent_rows[]`, `null_columns[]` | Ausência por `key`, `motivo` e evidência crua; colunas integralmente nulas por nome. Nulo, zero, não publicado, suprimido e filtrado precisam de expectativas distintas. |
| `unit_exceptions[]` | Exceção nominal por produto/item/célula, unidade publicada e canônica, razão, revisor/data e evidência com URL/hash. Uma captura externa ao golden pode ser citada por URL/hash/data; não conta como fixture offline. Mudança posterior do rótulo exige nova revisão. |

`mapeada` exige destino (`campo` ou dimensão explicitamente descrita). Multiplicador
é numérico apenas quando há conversão linear; soma/join/pivot fica descrito em
`conversion`. `ignorada` e `desconhecida` exigem motivo nominal e destino nulo.
`desconhecida` é achado aberto: não concede aceite N1. Não usar wildcard para
ignorar itens novos. Item obrigatório ausente e dois itens disputando um campo
sem regra explícita falham; repetição legítima por dimensão tem chave própria.

## Aceite por família

Pelo menos três amostras por família: período publicado, cada conversão existente
e posição difícil (nulo/supressão/total/geografia). A **última coluna ou linha real
da fonte**, mesmo excluída, precisa de decisão e amostra de inclusão/ausência;
também conferir a última aceita. Não procurar o extremo apenas na saída do parser.
Asserções no parser e no dataset público, mock apenas no transporte, com MetaInfo
e contrato. Mutar um valor e um período do manifesto deve provocar falha.
Tolerância só explícita, justificada e específica; sem arredondamento para esconder erro.

N3 usa ficha separada de equivalência (recorte, território, período, revisão,
unidade, base estatística e junção) antes de definir tolerâncias. Não promover
reprodução/fallback/cache a fonte independente.

## Compatibilidade com o manifesto v1

O manifesto v1 (série histórica da CONAB) e o helper de `tests/helpers.py` que o lê
seguem inalterados. A migração de um manifesto v1 para o v2 consiste em: mudar a
versão para 2; adicionar `dataset="serie_historica_safra"` e
`selection={"produto": product}`; transformar cada entrada de `sheets` em item
`structure` com `kind="sheet"` e `locator={"sheet": nome}`, mantendo `sheets` como
espelho, igual a `structure` quando os dois existem. `product`, `period_columns`,
amostras, ausências e exceções não mudam; as duas ausências de `cana_area_total`
(MG 2005/06 e 2006/07), cujo valor cru vazio já está registrado, ganham
`motivo="celula_fonte_vazia"`. Nenhum esperado é recalculado.

## Fixtures aposentadas

As retiradas abaixo foram conferidas contra os bytes ou a tabela extraída e os
oráculos de reconciliação. Os consumidores restantes usam os corpos canônicos;
não precisam das cópias antigas. Demais expected.json
continuam válidos, inclusive quando o manifesto reutiliza seu corpo por referência.

| Caso aposentado | Corpo e oráculo canônicos | Equivalência |
|---|---|---|
| `anda/entregas_sample` | `reconciliacao_r7_20260918/anda/anda_Principais_Indicadores_2024.pdf` e `anda_manifest.json`, caso `anda_2024` | Mesma tabela extraída; 12 meses, valores e rótulos conferidos. Replay exige `[pdf]`. |
| `icmbio/ucs_20260908` | `reconciliacao_r11_20260918/icmbio/icmbio_national_007.csv` e seu `manifest.json`, caso `icmbio_national` | Corpo idêntico; 347 registros com todas as nove colunas. |
| `lista_suja/official_20260905` | `reconciliacao_r11_20260918/mte/mte_pdf_016.pdf.gz` e seu `manifest.json`, caso `pdf_all` | PDF descompactado idêntico; 579 registros com todas as 12 colunas. SHA do gzip e do PDF são distintos. |

Os manifestos R11 usam o formato próprio family v1 descrito em
`reconciliacao_r11_20260918/FORMAT.md`; a retirada não altera sua versão.
