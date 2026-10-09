# CEPEA — cache vencido × virada das 18h (23/09/2026)

Oráculo da conferência de 23/09/2026.

- `cache_soja_20260922.json`: as 15 linhas de soja de um cache real (`ttl_seed_original.duckdb`, cópia do cache
  de conferência), coletadas em 22/09/2026 22:56:05 UTC (19:56 BRT), com todas as colunas da tabela `indicadores`. A
  consulta e o SHA-256 do arquivo de origem estão em `manifest.json`.
- `soja_20260923.html`: a página oficial do indicador capturada em 23/09/2026 23:05 UTC (20:05 BRT), depois da
  publicação de 23/09. Cópia byte a byte; URL, cabeçalhos e SHA-256 em `manifest.json`.

A coleta de 22/09 vale até a virada de 23/09 às 18h BRT (21:00 UTC). Antes dela, o agrobr serve o cache e publica
`fetched_at` = coleta real. Depois dela, busca de novo; se a fonte falhar, serve a coleta antiga com
`StaleDataWarning`.
