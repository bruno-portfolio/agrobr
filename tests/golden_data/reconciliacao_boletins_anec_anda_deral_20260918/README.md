# Reconciliação — ANDA, DERAL e ANEC (18/09/2026)

Os manifestos preservam seletores, URLs, SHA-256, localização da célula, decisão
de mapeamento/omissão, valor publicado e valor esperado. Oráculos independentes
leem caracteres/coordenadas dos PDFs e células xlrd, sem importar parsers agrobr.
Os testes novos usam os bytes originais e substituem somente o transporte HTTP.
Comparações numéricas têm tolerância absoluta e relativa zero.

| Fonte | Corpus | Conferência numérica |
|---|---|---|
| ANDA | 11 PDFs originais de 2016–2026 | 126 meses nacionais; totais publicados e unidades |
| DERAL | 2 arquivos BIFF, 26 abas | 438 registros, 730 células; datas reais das células |
| ANEC | 6 PDFs, 84 páginas | 2.454 registros nas quatro tabelas; 162 destinos em texto |

Os cinco PDFs ANEC antigos foram recapturados e comparados byte a byte. Os
manifestos referenciam esses arquivos originais por caminho relativo. A edição
36/2026 e os PDFs ANDA foram preservados aqui. Os arquivos DERAL originais também
são referenciados, sem cópias redundantes. `response.xlsx` do exemplo DERAL antigo
é XLS por assinatura; seu nome e seus bytes foram conservados, corrigindo só o
metadata. Recibos novos estão nos manifestos.

O catálogo ANEC contém quatro páginas, 36 artigos e datas de edição/revisão.
O HTML de replay remove somente objetos `author` alheios à seleção dos dados;
os recibos registram SHA/tamanho original e derivado separadamente. Os HTMLs
originais ficam fora do repositório.
Datas, IDs, anexos e paginação permanecem os publicados. Essa derivação não é
descrita como um HTML original byte a byte.

## Controles e recusas

ANDA parser 3 seleciona somente a seção de entregas e exige o ano solicitado.
DERAL parser 2 recusa perder qualquer cabeçalho Ruim/Média/Boa/Plantada/Colhida.
ANEC parser 2 exige seis produtos em cada período semanal e seis mais Total
Products no quadro mensal; destinos são extraídos da tabela, sem rótulos do mapa.
As contraprovas antes/depois estão nos testes da família. Erros de layout mantêm
ParseError na fonte e a causa no SourceUnavailableError do dataset.

Mutações estão explicitamente identificadas como sintéticas. As da ANEC usam uma
atualização incremental de um stream Form do PDF; todos os bytes originais são
prefixo do derivado. O oráculo por caracteres confirmou cada mudança. Um ensaio
inicial com PDFium não persistia mudanças internas e foi descartado antes dos
testes: somente `build_anec_stream_mutations.py` gera o corpus de mutações final.
Não usar o protótipo `build_anec_mutations.py` para regenerar essas fixtures.

## Limites

- ANDA: `uf_linhas`, `uf_colunas` e `generico` não tiveram PDF original localizado.
  Mantêm cobertura sintética, sem certificado N2. O XLSX agregado do catálogo não
  pertence aos ramos PDF; 2026 tem somente janeiro–junho publicados nesta captura.
- DERAL: XLSX verdadeiro e abas por cultura não foram encontrados. Datas do
  conteúdo prevalecem sobre nomes antigos de abas. Batata, soja 2, fases e
  comercialização estão nominalmente fora do contrato. Nenhuma observação é
  criada para a aba de feriado; a escala 0..100 é literal nesses dois arquivos.
- ANEC: W08/W12 têm oito painéis de destinos em imagem. Retorno vazio com aviso
  não comprova zero comércio. W04 não publica comparação anual de sorgo. Ranges
  permanecem ranges; totais e células vazias não recebem valores de vizinhos.
  Diferenças mensais/anuais de trigo/DDGS e percentuais somando 99/101/102 são da
  fonte, documentadas nominalmente. Cada quadro conserva seus próprios números.
- N3: nenhuma equivalência entre fontes foi certificada. A soma interna dos meses
  não é N3; realizado ANEC e programação/estimativa não são intercambiáveis com
  exportações aduaneiras ComexStat sem estudo próprio.

## Inventário N1 e reprodução

`structure_profiles.json` cobre todas as páginas/abas dos 19 documentos. O
verificador compara rótulos/unidades, tipos e posições de células, formatos
percentuais, nomes de abas e contagem de imagens. Não aprende automaticamente um
layout alterado. Números/anos são excluídos dos rótulos de N1 e conferidos por N2;
mudanças rotineiras de mês ou células ocupadas também podem exigir revisão.

```powershell
.venv\Scripts\python.exe scripts/reconciliar_boletins.py --output n1_fixtures.json
.venv\Scripts\python.exe scripts/reconciliar_boletins.py --live --output n1_live.json
.venv\Scripts\python.exe -m pytest tests/test_anda/test_reconciliacao.py tests/test_deral/test_reconciliacao.py tests/test_anec/test_reconciliacao.py tests/test_reconciliar_boletins.py
```

`--live` compara somente os três boletins correntes; seu sucesso não certifica
todos os anos e layouts. O overlay em
`coverage_update.json` (fora do repositório) preserva os 10 IDs da matriz de variantes e
suas pendências. Não alterar a matriz original para ocultar lacunas.
