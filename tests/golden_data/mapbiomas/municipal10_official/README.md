# Recorte municipal oficial da Coleção 10

Pacote XLSX derivado de cinco linhas publicadas: um par e um trio com a mesma chave territorial/classe, mas IDs e vetores anuais distintos. Os registros não são duplicatas exatas e não devem ser agregados nem descartados. `id_registro` preserva o `ID` da fonte; não é uma posição gerada pelo parser nem uma identidade estável entre coleções.

O arquivo preserva os quarenta anos, os cabeçalhos numéricos 1985–2024 e o layout original da Coleção 10, incluindo `feature_id` e `municipality - state`, sem coluna `region`. O oráculo registra células, tipos XML e linhas originais. Manifesto e oráculo distinguem o corpo oficial completo do pequeno pacote derivado.

Amostra de regressão, sem pretensão de representar toda a população. O tamanho pequeno exige redução explícita do limiar de download somente nos fixtures de replay; a proteção de produção não deve ser alterada para aceitar este arquivo de teste.
