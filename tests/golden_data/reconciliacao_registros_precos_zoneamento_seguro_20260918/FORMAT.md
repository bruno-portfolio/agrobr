# Formato próprio de famílias — versão 1

Identificador: `agrobr.reconciliation.family`, `format_version: 1`.
Este formato é distinto do [manifesto genérico v2](../MANIFEST.md). Os primeiros
artefatos usavam o rótulo `2` indevidamente; a correção de identificação não
recalcula os esperados. A alternativa de formato próprio vale para a
ANP desde 18/09/2026.

Cada família conserva `oracle`, `resources[]` e `cases[]`. O oráculo deve resultar
de leitura independente do corpo original, sem importar o parser ou o contrato
sob teste. Contagens, hashes ou a mera existência do manifesto não substituem
a comparação numérica e de rótulos nas APIs públicas.

`resources[].original` identifica URL, arquivo integral preservado, tamanho,
SHA256 e aquisição UTC. Corpos integrais fora do repositório são evidência preservada
localmente; sua referência não os transforma em fixture portátil offline.
`derived_file`, `derived_sha256` e o tamanho quando presente identificam o
recorte separado. `column_decisions` explica cada coluna original; catálogo tem
decisões nominais ou inventário de links. Colunas/recursos novos exigem revisão N1.

As coordenadas do Excel são aba e linha/coluna a partir de 1; cabeçalhos identificam
as colunas. `selected_rows` mapeia linha original e derivada; `locators` nos
esperados identifica as linhas originais usadas na agregação. Unidades e decisões
ficam no inventário de colunas/recursos. XLSX é reserializado: igualdade dos campos
usados precisa ser verificada, e não implica igualdade de todos os floats ignorados.

CSV usa registros lógicos a partir de 1, depois do cabeçalho. Uma quebra de linha
entre aspas não inicia outro registro. `selected_original_records` mapeia a
posição derivada para a original. Ambas são restritas ao SHA do respectivo corpo;
ordinais não identificam observações entre revisões.

`cases[].query` contém argumentos públicos, `id` identifica o caso e `r2_id` a
variante. ANP/PSR guardam as linhas em `expected`; o ZARC usa
`expected_derived_positions` para selecionar as linhas independentes de
`oracle.json`. Suplementos declaram arquivo e oráculo próprio no manifesto.
Casos com seleções sobrepostas não contam como observações independentes.

ANP confere chave de semana/geografia/produto e cada campo, com tolerância
float64 explicitamente limitada. PSR compara multiconjuntos de todas as células,
preservando duplicatas. ZARC compara todas as colunas e ordem/posição do corpo.
Nulos, zeros e textos vazios permanecem distintos. Limites de aquisição,
reescrita, anonimização e população devem ser declarados por família.

N1 verifica estrutura e decisões; N2 executa as APIs públicas com corpos
capturados ou derivados declarados. Replays integrais separados conferem
extremos/coortes quando o corpo original não integra o golden portátil.
Formatos, cache e revisões da mesma origem não concedem N3.


## Corpos integrais e sessões HTML/CSV

RNC/SNPC e Agrofit usam `manifest_schema` com o identificador acima e
`schema_document: ../FORMAT.md`. `files[]` descreve cada resposta original e
sua representação portátil: `original` é o recibo da aquisição;
`golden_file`/`golden_sha256`/`golden_bytes` identificam o gzip armazenado;
`decoded_sha256`/`decoded_bytes` identificam seu conteúdo descompactado.
`csrf_values_redacted` conta substituições apenas nos valores de tokens HTML.
Zero significa nenhuma substituição; não pressupõe que um HTML tenha tokens.
CSV e JSON descompactados são byte a byte iguais ao corpo HTTP decodificado
original. O gzip determinístico de armazenamento não é o gzip original da rede.
A exceção é o item com `masked: true`: cópia com CPF e nome de pessoa física
mascarados, com a regra e as contagens na seção `masking` do manifesto.
Nele, `decoded_*` descreve o corpo guardado, e `original` continua com o SHA e os
bytes do corpo publicado, que fica fora do repositório.

`requests[]` declara cada resposta usada no replay. `match` contém caminho
absoluto, parâmetros, `skip`, método (GET por padrão) e `occurrence` opcional:
1 e 2 distinguem respostas sucessivas do mesmo método/endereço; zero/ausente
permite reutilização. `file`, `content_type`, `status` e `response_headers`
reconstituem a resposta. Os parâmetros de tamanho `$top`/`$limit` e credenciais
são excluídos da assinatura compartilhada; os demais, inclusive filtros e
paginação WFS, são conferidos. Corpo POST e ordem global entre métodos não são
validados pelo matcher. Uma captura feita pelo próprio cliente não estabelece
independentemente a sequência; o limite deve constar em `coverage_limits`.

`html_decisions[]` inventaria formulários, ações, controles nomeados, formato
de exportação e contagens publicadas. Valores de tokens não são oráculos.
No RNC, `compare_html` é a guarda independente desse inventário, sem comprovar
por si só um protocolo de sessão completo. `catalog_decisions[]` do Agrofit
explicita recursos CKAN usados e ignorados, URLs e formatos.

`resources[].oracle` (RNC) ou `resources[].oracles` por papel (Agrofit) descreve
arquivo, SHA, colunas e população esperada. `oracle.file` pode ser JSONL gzip:
um objeto por registro lógico original, com todas as células em `values` e
posição `original_record` (base 1 após o cabeçalho); `physical_line` é o fim do
registro no CSV quando disponível. Hash do oráculo comprimido é distinto do
hash do corpo fonte. Datas civis são strings ISO, ausências são JSON null e
textos vazios continuam strings vazias.

`cases[].expected_original_records` seleciona posições do oráculo integral RNC;
`selection: all` escolhe toda a população. Posições valem apenas para o SHA
especificado. Casos idênticos não aumentam cobertura; somas entre filtros
sobrepostos são ocorrências, não registros independentes. Agrofit declara em
`case_semantics` e `public_filter_exercised` quais coortes foram comparadas no
retorno completo e quais passaram por filtro público, sem confundir os escopos.

ICMBio usa corpos sem compressão e `files[].file`/`original`; os seus oráculos
são arrays JSON com posição, FID/ogc_fid e `values`. `cases[].requests` limita
cada consulta aos corpos correspondentes. `schema` inventaria todas as
propriedades do XSD, inclusive as ignoradas fora do contrato tabular.

## SICAR: schemas estaduais e páginas GeoJSON

`schemas[]` associa UF, arquivo XSD e `target_namespace`. `elements` conserva
os atributos de cada declaração (nome, tipo, ordem, ocorrência e nulabilidade);
`decisions` associa cada propriedade a `mapped` e coluna de destino ou a
`ignored_geometry`. A atualização não existe em todas as camadas; ausência
de campo e nulabilidade não são a mesma decisão.

`resources[].pages` lista corpos integrais GeoJSON em gzip. `oracle` identifica
JSONL gzip com `file`, `feature_index` zero-based, `feature_id` e onze `values`.
`statistics.features` conta feições antes da seleção; `rows`, imóveis depois.
`nulls` conta nulos por coluna de saída; `zero_area` e `zero_fiscal_modules`
contam zeros reais. `repeated_codes` conta códigos com várias ocorrências.
`discarded` identifica código, feature descartada/mantida e critério;
`criteria` soma descartes por atualização, criação ou feature ID.

`cases[].announced_features` registra o total WFS para a seleção completa.
Nas páginas históricas suplementares, `original` e `scope` preservam a origem
e o limite: página completa não significa população histórica completa.
Esses suplementos usam o parser, sem simular hits nem páginas inexistentes.

As assinaturas HTTP e seletores `occurrence` distinguem requisições/respostas,
mas não impõem ordem global. No DF há um preflight fora do fluxo público e
dois hits reais no replay com a mesma contagem; trocar esses dois corpos não
comprova outra sequência. O consumo de respostas é conferido separadamente.

