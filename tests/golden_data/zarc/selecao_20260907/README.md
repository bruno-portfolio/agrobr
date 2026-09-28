# Recortes oficiais ZARC — 07/09/2026

Três aquisições integrais oficiais, reduzidas por posições explícitas: 10 registros de 2016/2017, 21 de 2026/2027 e 75 do recurso agrupado perene/olerícola/sem safra. `manifest.json` conserva URL, SHA256, tamanho e população do corpo integral, além dos hashes/tamanhos dos recortes e posições originais.

Os arquivos CSV são artefatos derivados: serialização UTF-8 sem BOM e terminador LF, preservando integralmente os valores das células selecionadas. Não são downloads parciais byte-idênticos ao recurso completo. A seleção cobre códigos de clima/manejo/NM/solo/ciclo, produtividade textual, modalidades sem safra anual, ausência real de risco, 50 publicado e o par literalmente duplicado de 2016/2017. As linhas mantêm a ordem original. O gerador está.

O `registro_origem` emitido pelo parser sobre estes recortes refere-se à posição local no CSV reduzido, base 1; as posições no CSV integral estão separadamente no manifesto/oráculo. Não confundir os hashes ou atribuir estabilidade entre revisões a esses números.

Em 23/09/2026, `raw_oracles.json` e o golden legado `tabua_risco_sample` foram aposentados: os recortes seguem só como corpos do replay dos testes de comportamento, e o oráculo das tábuas oficiais é o de `../../reconciliacao_r11_20260918/zarc/` e `../edicoes_20260923/`.
