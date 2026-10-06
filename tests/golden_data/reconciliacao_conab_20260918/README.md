# Reconciliação — CONAB, progresso e seleção LSPA

`manifest.json` segue o [formato v2](../MANIFEST.md). Preserva referências a goldens
existentes e adiciona corpos oficiais completos adquiridos em 18/09/2026. Os recibos
trazem URL, SHA-256, tamanho e data/hora disponíveis. Capturas legadas que só
registraram o dia ficam explicitamente sem horário; não foi inventado um timestamp.

Os esperados vieram de leitura independente por xlrd/openpyxl, JSON Pointer e XML
OOXML. Os geradores `oracle_*.py` não importam
agrobr, pandas, parser nem contrato. O XLSX CONAB de setembro/2022 tem estilos que
openpyxl rejeita; o oráculo lê seus XMLs, inclusive valores em cache, sem modificar
os bytes. A produção exerce seu fallback calamine existente.

N1 registra todas as abas, colunas/períodos e linhas das tabelas selecionadas,
além de variáveis e classificações LSPA. Notas, agregados, farelo/óleo, revisões
anteriores, variações percentuais e espaços de formatação têm decisões nominais.
Os extremos são calculados na planilha crua; dimensões de formatação e última
linha/coluna preenchida são guardadas separadamente.

N2 confere valores e rótulos nos parsers e nas saídas públicas com mock somente
no transporte HTTP. Os catálogos e arquivos CONAB conservam suas URLs e edições.
LSPA usa projeções por categoria/período de respostas oficiais completas: nenhum
valor ou mês é criado. A projeção não comprova aquisição de meses ausentes.

Limites aceitos na revisão:

- Balanço wide/XLS de abril/2022: oráculo no parser. `balanco` não possui seletor
  de levantamento; a API escolhe setembro/XLSX para essa safra, conferido no replay
  público. Não há catálogo fabricado nem arquivo servido sob URL de outra edição.
  Um seletor novo ficou para depois.
- LSPA atual: metadados de 18/09 indicam agosto/2026, mas a consulta de valores
  recebeu 403/Cloudflare. Os valores offline vão até julho/2026; aquisição atual
  permanece pendente. A comparação entre fontes depende da ficha N3, fora do repositório.
- Os seis produtos LSPA preservados não contêm componente numérico nulo. A
  propagação do nulo é testada com dados sintéticos em `test_lspa_missing.py`,
  sem certificar esse cenário como N2 de publicação oficial.
- Edições CONAB de 2019/20 e 2020/21 não publicam soja na tabela selecionada;
  os casos verificam ausência. Trigo no levantamento de janeiro/2026 ainda está
  rotulado Safra 2025; a seleção 2025/26 fica vazia, sem reaproveitar outro período.

`area_colhida` CONAB é nula: só existe uma área publicada. Percentuais numéricos
Excel são frações; texto com `%` é dividido por 100, mantendo a nota de revisão.
Unidades de balanço são comprovadas por célula, texto BIFF ou caixa de texto OOXML.

Os testes de mutação alteram um valor e um período do manifesto e exigem falha.
A tolerância numérica cobre somente representação binária/float64, não discrepâncias
entre fontes. R1 e seus goldens permanecem preservados.
