# PAM: cana, mandioca e laranja

Capturas oficiais da tabela SIDRA 5457 obtidas em 06/09/2026 pelo client do agrobr no Docker. `proveniencia.json` registra URLs, parâmetros, contagens, instante de captura e SHA-256 de cada CSV. Os dados brutos mantêm os rótulos e símbolos da fonte, sem normalização.

- `brasil_2024.csv`: os três produtos, quatro variáveis, total Brasil.
- `uf_2024.csv`: os três produtos nas 27 UFs.
- `municipios_ro_2024.csv`: os três produtos nos 52 municípios de Rondônia; cana tem cinco zeros e laranja doze zeros de produção.
- `laranja_2000_2001.csv`: fronteira de mudança de unidade da fruta.
- `brasil_1974.csv`: ausência de área plantada no início da série.

As [notas oficiais da PAM](https://sidra.ibge.gov.br/pesquisa/pam/tabelas/) prevalecem sobre o rótulo genérico de unidade retornado pela API: laranja anterior a 2001 usa mil frutos e frutos/ha, embora o campo `MN` do SIDRA apresente toneladas e kg/ha. Não se aplica fator de conversão ao valor bruto. Cana e mandioca contabilizam colheitas no ano civil; sua área plantada é a área destinada à colheita. Área plantada só é informada desde 1988.

Os oráculos numéricos dos testes foram conferidos nessas capturas. Revisões futuras da fonte exigem conferir os novos valores e atualizar a proveniência; não relaxar as asserções para acomodar uma diferença sem diagnóstico.
