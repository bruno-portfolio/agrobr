# Sementes oficiais PRODES/DETER de 07/09/2026

Oito corpos JSON HTTP originais, seis feições por layout, copiados byte a byte das capturas originais. O manifesto registra caminhos de origem, SHA-256 e tamanho. Os filtros de ID não nulo e ordenação de cada semente introduzem viés; não são populações completas nem evidência de PK.

Pampa preserva o corpo cujo recebimento encerrou a captura original com erro: o servidor incluiu seis geometrias apesar da projeção tabular. Esse arquivo não é captura aprovada retroativamente. O parser produtivo2 aplica a decisão posterior explícita de validar e liberar geometria no tabular.

Os corpos mantêm todas as propriedades, lexemas, null e multiplicidade. DETER Amazônia inclui duas ocorrências com Feature.id/gid iguais e conteúdo diferente. Nenhum atributo foi gerado e nenhuma linha foi deduplicada. Os goldens legados fora deste diretório permanecem inalterados, inclusive as evidências CSV que não são equivalentes em precisão ao novo caminho JSON.

Atualização de 26/09/2026: os goldens legados do desmatamento (`deter_sample`, `deter_geo_sample`, `prodes_sample` e `prodes_geo_sample`) saíram do repositório na 2.0.0, junto com o parser v1 que os lia. Ficam no histórico do git (tag `v1.1.0`).
