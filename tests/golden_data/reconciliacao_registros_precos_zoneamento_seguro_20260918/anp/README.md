# ANP preços — reconciliação

Captura HTTP de 18/09/2026:
catálogo integral e cinco XLSX integrais, URL/horário UTC/status/cabeçalhos/SHA.
Os XLSX deste diretório são **recortes derivados**, nunca corpos HTTP originais.
Manifesto de formato próprio v1 (`../FORMAT.md`) conserva recibos e mapeamento
de linhas, incluindo última linha real e últimas linhas dos dois diesels. As
células são reserializadas por openpyxl; os campos da saída pública foram
reconciliados contra os originais. Na coluna ignorada DESVIO PADRÃO REVENDA,
211 células diferem na precisão de serialização (máximo 4,8e-16, conferido na
revisão); o recorte não é byte-idêntico nem conserva todo float cru literalmente.

Oráculo: leitura dos originais com openpyxl; cálculos por Decimal, sem importar
agrobr. 25 seleções, 873 registros esperados, seis variantes R2. O teste executa
fonte e dataset reais, substituindo somente o transporte HTTP. Médias admitem
apenas erro de representação float64 limitado pelo número de operações.
Seleções sobrepostas não representam 873 observações independentes.

Gerador: `build_anp_oracle.py`, fora do repositório.
N1: `scripts/reconciliar_anp_precos.py --capture-dir DIRETORIO_COM_RECIBOS --output RELATORIO`.
N2: `pytest tests/test_anp_diesel/test_reconciliacao.py`.
Para replay local dos originais integrais, aponte `AGROBR_R11_ANP_ORIGINALS` para o diretório deles.

N1 não rejeita atualização normal de preços, quantidade de linhas ou cobertura
temporal. Rejeita cabeçalho, aba, produto, unidade, UF ou link sem decisão.
N3 não certificado: dados da mesma publicação não são fontes independentes.

Formato identificado: `agrobr.reconciliation.r11.family`, versão 1, documentado
em `../FORMAT.md`; distinto do manifesto genérico v2. A mudança de rótulo não
recalcula esperados nem modifica os corpos CSV/XLSX.
