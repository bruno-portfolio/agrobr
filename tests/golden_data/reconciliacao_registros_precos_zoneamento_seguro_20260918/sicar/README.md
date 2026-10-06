# SICAR — reconciliação tabular de 18/09/2026

Capturas completas das seis seleções atuais: DF recente (62), MT recente (79),
SP recente (31), DF integral (21.006), GO recente (4) e RS recente (1).
São 21.183 ocorrências, com sobreposição DF. Os corpos completos originais
estão comprimidos com gzip determinístico; contagens, feições e datas não foram
alteradas. O DF tem três páginas de 10.000, 10.000 e 1.006 feições. O manifesto
separa a sondagem prévia do fluxo público, com dois hits reais no replay DF.

Cada JSONL esperado identifica arquivo, posição zero-based, feature ID e as
onze células finais. O gerador stdlib `build_sicar_oracle.py` (fora do repositório)
não importa agrobr. JSON, conversões
numéricas e instantes UTC são lidos independentemente. Todos os valores,
incluindo zeros, 513 atualizações nulas do DF e ausências de campo SP/RS,
são comparados no retorno público da fonte e do dataset, sem tolerância float.

Dois suplementos históricos contêm páginas integrais de GO/RS de 10.000
feições, com 9.999 imóveis após a seleção de versões. Número total publicado,
posições e ids originais foram preservados. São suplementos do parser, não
consultas históricas completas: não se inventaram hits nem páginas restantes.
GO seleciona por atualização e RS por criação; ambos registram o descarte.

Os 27 XSDs têm inventário nominal completo, inclusive a geometria excluída da
saída tabular. O N1 compara XSDs e as oito páginas atuais contra essa baseline;
o script usa corpos locais, sem recaptura. Projeções de atributos não devem
ganhar campos silenciosamente; datas precisam indicar fuso.

Fonte: `sicar_wfs` nos metadados diretos, `sicar` na rota única do dataset.
Ambos preservam `source_details["sicar"]`. Não há cache nesta API; hashes e
tamanhos por página estão nos recibos/manifesto, não em MetaInfo. A paginação
não é transacional, e contagens coincidentes não provam snapshot consistente.
Geometria e cruzamento N3 entre fontes não fazem parte desta variante.

Mutações da última página mudam área, chave, instante, removem registro ou
repetem feature ID. As comparações públicas devem detectar os cinco cenários.
Fixtures alteradas são gravadas apenas em diretório temporário pelo teste.
