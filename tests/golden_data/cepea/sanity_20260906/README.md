# CEPEA — unidades e faixas de sanidade

Captura pública oficial de 22 produtos em 06/09/2026 UTC. `observations.json` contém 337 registros únicos extraídos por seleção independente de tabela/célula, sem importar agrobr. Cada entrada registra URL, horário, SHA-256, título e linhas originais. Os HTML são os bytes capturados, incluindo respostas distintas da mesma URL. As duas linhas repetidas de citros foram confrontadas e deduplicadas por data/praça sem alterar seus valores.

`historical_extremes.json` contém 22 células oficiais das planilhas históricas, com coordenadas, URL e hash do XLS. Os bytes completos dos XLS e a comparação de todas as linhas por xlrd/Calamine foram preservados. Não é necessário HTTP para os testes. Soja Paraná/café usam extremos desde 2015 para preservar limites legados; os demais nove mercados usam extremos positivos de toda a série baixada. Leite XLS tem duas casas; leite HTML tem quatro. Os 36 zeros oficiais de leite não são preços válidos no modelo positivo e foram registrados na pesquisa.

As faixas são limiares de engenharia com margem, não intervalos estatísticos publicados. Para os nove mercados novos com XLS: metade do mínimo positivo e dobro do máximo, arredondados para fora a um algarismo significativo. Citros usa calibração parcial documental (anuário HF Brasil 2014/15, p.13; CEPEA 2024), sem alegação de mínimo/máximo histórico. Leite/etanol e os novos mercados sem calibração temporal não recebem limite diário. Política, fontes e limitações completas: POLICY.md.

Fonte: CEPEA/ESALQ-USP. Licença dos dados: CC BY-NC 4.0.
