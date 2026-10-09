# CONAB série histórica — zero publicado × safra não levantada (23/09/2026)

Oráculo da conferência de 23/09/2026. As três planilhas oficiais (amendoim 2ª safra, feijão
3ª safra e mamona), copiadas byte a byte dessa conferência; URL, data e SHA-256 em `manifest.json`, com as células
A1 lidas por xlrd (tipo numérico, valor 0.0):

- amendoim 2ª BA 2011/12: área 3,8; produtividade 0,0; produção 0,0;
- mamona RN 2007/08: área 0,1; produtividade 635; produção 0,0;
- feijão 3ª RS 2000/01: área 0,0; produtividade 550; produção 0,0.

Esses zeros saem `0.0`. A coluna de safra zerada em todas as UFs é safra não levantada e não gera linha; o
exemplo está em `../serie_historica_20260917/trigo.xls` (1976, zero até na linha BRASIL).
