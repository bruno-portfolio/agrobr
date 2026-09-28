# CEPEA — trigo com as duas praças (25/09/2026)

Página do indicador do trigo do CEPEA, capturada na rodada 21 de conferência (25/09/2026). Ela traz duas tabelas: "PREÇO MÉDIO
DO TRIGO CEPEA/ESALQ - PARANÁ" e "... - RIO GRANDE DO SUL", cada uma com R$/t, variação do dia e do mês e US$/t.

O arquivo é o texto decodificado pelo client do agrobr na captura. A R21 o gravou com CRLF (`write_text` no Windows); aqui ele
volta a LF, que é o conteúdo cujo SHA-256 a R21 registrou.

O `manifest.json` traz o SHA do arquivo e o oráculo: as 30 linhas das duas tabelas (15 por praça), transcritas com o `html.parser`
da biblioteca padrão, sem o agrobr, com data, praça, valor em R$/t e valor em US$/t.
