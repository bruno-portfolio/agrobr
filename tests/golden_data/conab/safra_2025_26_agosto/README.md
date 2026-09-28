# CONAB — 11º levantamento 2025/26, agosto de 2026

`response.xlsx` e `response.html` preservam a captura oficial realizada em
06/09/2026. A URL e o SHA-256 originais estão em `provenance.json`.
`metadata.json` identifica a fonte CONAB e o formato XLSX; o HTML é a página de
descoberta do boletim, não uma resposta para o parser CEPEA.

`expected.json` foi escrito a partir de leitura independente das células
armazenadas no workbook com openpyxl e `data_only=True`, sem gerar expectativas
a partir do parser agrobr. Na linha 6 de Soja/Milho Total, C/F/I correspondem à
safra 25/26; a linha 5 explicita mil ha, kg/ha e mil t.

| Cenário golden | Referência independente |
|---|---|
| Soja | 27 UFs nas linhas listadas em `oracle.soja.uf_rows`, mais Norte/Nordeste, Centro-Sul e Brasil nas linhas 40–42: 30 registros. RR: C9=150, F9=3420, I9=513. |
| Milho Total | Mesmas 30 linhas territoriais. RR: C9=20, F9=6000, I9=120. Brasil: C42=22598, I42=142955. |
| Suprimento | Cinco produtos iniciados nas linhas 6, 14, 22, 30 e 38. Cada bloco tem sete safras distintas; a oitava linha é revisão de agosto da última safra. Resultado: 35 pares produto/safra, mantendo a revisão mais recente. Milho E37=142955; trigo E45=5813.1. A soja fica na aba "Suprimento - Soja", com as safras 2020/21 a 2025/26 em B6:G6 (produção 2025/26 em G9=180463.5): sem produto, o parser soma 35 + 6 = 41 registros. |
| Brasil Total | Dados nas linhas 8–38 e 42–49: 39 linhas para duas safras, 78 registros. Cabeçalho de inverno na linha 39 e rodapés 50–52 não são observações. Âncoras de produção: soja I36=180463.5 e trigo I46=5813.1. |

A cardinalidade de Brasil Total exclui cabeçalhos e rodapés. Subtotais e o total
Brasil são linhas de dados oficiais, distintas das notas explicativas. Não
equivale a 39 produtos distintos, pois há agregados e categorias de feijão
repetidas dentro de diferentes subsafras.

Os testes dirigidos em `test_current_provenance.py` e `test_http_current.py`
continuam cobrindo os bytes oficiais, a identificação do boletim e a origem do
download. Nenhum caso foi removido ou marcado como skip para integrar a captura.
