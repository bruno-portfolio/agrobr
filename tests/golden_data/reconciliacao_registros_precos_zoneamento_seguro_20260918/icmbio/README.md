# ICMBio — corpos integrais de 18/09/2026

Formato próprio de família v1, conforme `../FORMAT.md`.
Sete respostas HTTP integrais e sem reserialização: hits + CSV em três consultas,
mais DescribeFeatureType da camada. SHA, bytes, URL e aquisição UTC no manifesto.
O header HTTP Content-Encoding dos recibos pertence à transmissão original;
as fixtures são os bytes já descomprimidos devolvidos por httpx.

`build_icmbio_oracle.py` (fora do repositório)
usa somente a biblioteca padrão e lê todas as células, sem importar agrobr.
Os oráculos registram posição lógica (base 1 após o cabeçalho), FID e ogc_fid.
As 419 ocorrências incluem 347 + 50 + 22; consultas sobrepostas, 347 CNUCs distintos.
As nove saídas preservam texto e valores, com grupo em maiúsculas, área float64
e ano Int64. Comparação numérica exata, sem tolerância, escala ou agregação.
Nenhum campo vazio ocorre nestes corpos; casos sintéticos de nulidade não são
evidência de ocorrência oficial. A fonte não impõe unicidade artificial ao CNUC.

N1: `python -m scripts.reconciliar_icmbio --output CAMINHO.json`.
`--input-dir` permite comparar outro diretório com os mesmos nomes de arquivos;
a execução lê corpos locais, não recaptura a rede. O schema inteiro é comparado,
inclusive tipos/nulabilidade/geometria ignorada; códigos vazios/repetidos são
apontados para revisão da identidade observada nesta captura, sem deduplicação.

N2: `pytest tests/test_icmbio/test_reconciliacao.py`.
HTTP real da fonte interceptado com os parâmetros completos. Fonte e dataset
comparam todas as colunas, extremos, dtypes e proveniência; mutações no corpo
alteram área, rótulo, código, ano ou removem a última linha. Não há cache nesta API.

Limites: camada corrente, hits apenas anterior, sem snapshot transacional;
geometria e histórico fora do escopo; não há N3 entre filtros da mesma origem.
