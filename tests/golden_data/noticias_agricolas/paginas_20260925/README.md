# Notícias Agrícolas — páginas de cotação das 22 chaves (25/09/2026)

Corpos oficiais das páginas de cotação do Notícias Agrícolas que o agrobr lê como fallback do CEPEA. Foram capturados em
25/09/2026, uma página por vez, todas com HTTP 200. Cada arquivo é a resposta como recebida,
em UTF-8. O `manifest.json` traz a URL, o status, os cabeçalhos, os bytes e o SHA-256 de cada uma.

- **12 páginas novas:** soja Paraná, arroz, açúcar cristal, açúcar refinado, etanol hidratado e anidro (semanais), frango
  congelado e resfriado, suíno (5 praças por dia), leite (10 estados e Brasil por mês) e laranja indústria e in natura.
- **8 páginas da R6,** por referência a `tests/golden_data/reconciliacao_r6_20260918/`: soja Paranaguá, milho, bezerro, boi,
  café arábica, café robusta, algodão e trigo.
- **Aliases:** `boi_gordo` e `cafe_arabica` são aliases de `boi` e `cafe`; a página é a mesma.

O oráculo está nos `cases` do `manifest.json`. Ele foi transcrito com o `html.parser` da biblioteca padrão, sem o agrobr, e
cobre todas as linhas de todas as tabelas de cotação: 340 linhas nas 20 páginas, 360 com os aliases. Cada linha traz:
- **Data:** a data da linha. No etanol, que é semanal, vale o último dia da semana. No leite, vale o "Fechamento" do bloco, que é
  a data da publicação.
- **Praça:** no trigo, no suíno e no leite, a coluna da própria linha. Nas demais, a praça que o título da página ou a série do
  CEPEA declara.
- **Valor.**
- **Variação:** ausente quando a página traz "-". A variação 0,00% fica como zero.
- **Unidade:** a do cabeçalho.

No leite, cada bloco termina com a linha "Referência: <mês>" sem valor. Essa linha fica fora das linhas do oráculo e é
registrada em `ausentes`.
