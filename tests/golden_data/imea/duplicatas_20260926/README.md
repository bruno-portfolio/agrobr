# IMEA — registros publicados em duplicata

Cadeia 4 (soja) da API pública do IMEA, capturada em 26/09/2026 às 04:13 UTC. Em 25/09/2026 a fonte publicou 23 registros
do indicador `708192508838936580` (R$/sc, 23 localidades de Mato Grosso) quatro vezes cada, iguais em todos os campos.

- `indicadores_4.json`: catálogo de indicadores inteiro, sem transformação.
- `cotacoes_4.json`: recorte byte a byte da resposta de 1.113.818 bytes (SHA-256 `578c2fcc…`, 4.629 registros) com os 136
  registros publicados em 24 e 25/09/2026, na ordem da origem. O IMEA é `restrito`: o golden guarda só o necessário.
- `manifest.json`: URL, hora, bytes e SHA-256 de cada arquivo e da origem, posições do recorte na origem e a contagem das
  duplicatas, transcrita com `json` e `collections.Counter`, sem o agrobr.
