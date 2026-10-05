# Licenças e Termos de Uso das Fontes

> **Aviso:** O agrobr é licenciado sob MIT, mas os **dados** acessados pertencem
> às suas respectivas fontes e estão sujeitos aos termos de cada uma.
> É responsabilidade do usuário verificar se seu caso de uso está em conformidade.

## Tabela de Fontes

A mesma tabela está no código (`agrobr.constants.LICENCAS`), conferida por teste contra esta página, e a
classificação do dado chega ao `MetaInfo.license`.

| Fonte | Licença | Uso Comercial | Classificação | URL dos Termos |
|-------|---------|---------------|---------------|----------------|
| **CEPEA/ESALQ** | CC BY-NC 4.0 | Requer autorização do titular | `nc` | [Termos/fundamento](#cepeaesalq) |
| **CONAB** | Estatísticas públicas; LAI e busca sem restrição específica | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/conab/pt-br/acesso-a-informacao/dados-abertos) |
| **IBGE/SIDRA** | Dados abertos: LAI, Decreto 8.777/2016 e PDA IBGE; sem CC nominal geral | Sim, com fonte e proveniência | `livre` | [Termos/base](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf) |
| **NASA POWER** | Declaração específica de domínio público do POWER | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://forum.earthdata.nasa.gov/viewtopic.php?p=635) |
| **BCB/SICOR** | ODbL 1.0 — Matriz de Dados do Crédito Rural | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://dadosabertos.bcb.gov.br/api/3/action/package_show?id=matrizdadoscreditorural) |
| **ComexStat** | Dados públicos federais; sem CC nominal comprovada | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta) |
| **ANDA** | Fonte privada; licença de reutilização não localizada | Não comprovado por licença | `zona_cinza` | [Termos/fundamento](https://anda.org.br/recursos/) |
| **ANTAQ** | Estatístico Aquaviário: dados abertos no PDA | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/antaq/pt-br/acesso-a-informacao/dados-abertos/PDA20262028ANTAQOCR.pdf) |
| **ANP Diesel** | Preços e vendas: política de dados abertos, PDA 2026–2028 | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/home/pda-2026-2028.pdf) |
| **ANTT Pedagio** | CC BY — versão não indicada nos dois catálogos | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://dados.antt.gov.br/api/3/action/package_show?id=volume-trafego-praca-pedagio) |
| **MAPA PSR** | CC BY — versão não indicada no catálogo PSR | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://dados.agricultura.gov.br/api/3/action/package_search?fq=id:baefdc68-9bad-4204-83e8-f2888b79ab48&rows=0&facet.field=%5B%22license_id%22%5D) |
| **SICAR** | Dados públicos federais; licença CC da base não comprovada | Sim, com fonte e proveniência | `livre` | [Termos/base](https://consultapublica.car.gov.br/publico/geoservicos/index) |
| **ABIOVE** | Fonte privada; licença de reutilização não localizada | Não comprovado por licença | `zona_cinza` | [Termos/fundamento](https://abiove.org.br/estatisticas/) |
| **ANEC** | Fonte privada; sem licença de reutilização dos boletins localizada | Permissão comercial não estabelecida | `zona_cinza` | [Termos/base](https://www.anec.com.br/) |
| **USDA PSD** | CC BY 4.0 no registro oficial PSD preservado | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://catalog.data.gov/harvest_record/40692b29-74dd-4e52-bd1e-a5873ea9e010/raw) |
| **UN Comtrade** | Redistribuição condicionada; dispensas expressas | Depende do uso e das dispensas | `restrito` | [Termos/fundamento](https://uncomtrade.org/docs/policy-on-use-and-re-dissemination/) |
| **CFTC COT** | Domínio público da informação governamental CFTC | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.cftc.gov/WebPolicy/index.htm) |
| **IMEA** | Séries públicas sem licença comprovada; arquivos não públicos têm restrição expressa | Verificar o recorte e os termos | `zona_cinza` | [Termo de Uso](https://imea.com.br/imea-site/termo-de-uso.html) |
| **DERAL** | Dados públicos estaduais; LAI e Decreto PR 10.285 | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.legislacao.pr.gov.br/legislacao/listarAtosAno.do?action=exibirImpressao&codAto=114209) |
| **INMET** | Observações públicas próprias do INMET | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://portal.inmet.gov.br/dadoshistoricos) |
| **Notícias Agrícolas** | Fonte privada sem licença própria de cotações; origem CEPEA CC BY-NC 4.0 | Origem CEPEA: autorização para uso comercial | `zona_cinza` | — |
| **Queimadas/INPE** | Dados públicos; FAQ e fundamento federal | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://data.inpe.br/queimadas/faq/) |
| **Desmatamento PRODES/DETER** | CC BY-SA 4.0 | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://terrabrasilis.dpi.inpe.br/citacoes-e-licenca-de-uso/) |
| **MapBiomas** | CC BY 4.0 — cobertura e uso da terra | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://brasil.mapbiomas.org/faq/?tema=dados) |
| **CONAB Progresso** | CC BY-ND 3.0 no portal/ficha; sem licença individual do XLSX | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra/acompanhamento-das-lavouras-21-09-a-27-09-26/plantio-e-colheita-19-09-a-25-09/view) |
| **IBGE PPM** | Base federal e PDA IBGE 2024–2025; sem CC nominal comprovada | Sim, preservando fonte e proveniência | `livre` | [Termos/base](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf) |
| **IBGE Abate** | Base federal e PDA IBGE 2024–2025; sem CC nominal comprovada | Sim, preservando fonte e proveniência | `livre` | [Termos/base](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf) |
| **IBGE Censo Agro** | Base federal e PDA IBGE 2024–2025; sem CC nominal comprovada | Sim, preservando fonte e proveniência | `livre` | [Termos/base](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf) |
| **IBGE Censo Agro Histórico** | Base federal e PDA IBGE 2024–2025; sem CC nominal comprovada | Sim, preservando fonte e proveniência | `livre` | [Termos/base](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf) |
| **IBGE Censo Agro Municipal 1985** | Base federal e PDA IBGE 2024–2025; sem CC nominal comprovada | Sim, preservando fonte e proveniência | `livre` | [Termos/base](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf) |
| **B3 Futuros Agro** | FAQ D-1 e termos do site com alcances distintos | Conferir canal, uso e vigência com B3 | `zona_cinza` | [B3](https://www.b3.com.br) |
| **CONAB CEASA/PROHORT** | Painel e licença específica do PROHORT não confirmados; origem inclui terceiros | Verificar origem e condições | `zona_cinza` | [Termos/base](https://portaldeinformacoes.conab.gov.br/home) |
| **MAPA Agrofit (Defensivos)** | CC BY, sem versão; CSVs de formulados e técnicos | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://dados.agricultura.gov.br/api/3/action/package_show?id=sistema-de-agrotoxicos-fitossanitarios-agrofit) |
| **ZARC** | CC BY, sem versão; tábua de risco do ZARC | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://dados.agricultura.gov.br/api/3/action/package_show?id=tabua-de-risco-zoneamento-agricola-de-risco-climatico) |
| **ANA/SNIRH** | Dados públicos; base legal e política institucional | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/ana/pt-br/todos-os-documentos-do-portal/documentos-cor/planos-de-dados-abertos/plano-de-dados-abertos-2025-2027) |
| **FUNAI Terras Indigenas** | Termo específico da camada: reprodução com citação | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/funai/pt-br/atuacao/terras-indigenas/geoprocessamento-e-mapas) |
| **IBAMA Embargos** | Outra (Aberta), com selo Open Definition no catálogo | Sim, preservando fonte e condições | `livre` | [Termos/base](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-termo-de-embargo) |
| **ICMBio UCs Federais** | Política de dados abertos para limites federais | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/icmbio/pt-br/acesso-a-informacao/dados-abertos) |
| **CNUC (MMA)** | WFS: base legal; catálogo CKAN: CC BY sem versão | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://dados.mma.gov.br/api/3/action/package_show?id=unidadesdeconservacao) |
| **INCRA Quilombolas** | Dados públicos do INCRA; base legal federal | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/incra/pt-br/acesso-a-informacao/dados-abertos) |
| **Lista Suja** | CC BY-ND 3.0 no portal; sem licença individual dos arquivos | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/areas-de-atuacao/combate-ao-trabalho-escravo-e-analogo-ao-de-escravo) |
| **MapBiomas Alerta** | CC BY-SA 3.0 BR — dados, inclusive via API | Sim, com atribuição e SA nas adaptações | `livre` | [Termos/base](https://plataforma.alerta.mapbiomas.org/terms) |
| **SFB** | Dados públicos federais: CNFP, concessões e IFN; sem CC nominal comprovada | Sim, preservando fonte e proveniência | `livre` | [Termos/base](https://mapas.florestal.gov.br) |
| **RNC/CultivarWeb** | Cadastros públicos RNC/SNPC; sem CC nominal | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://sistemas.agricultura.gov.br/snpc/cultivarweb/cultivares_registradas.php) |
| **EMBRAPA Solos** | CC BY-NC 3.0 BR nas duas camadas | Requer autorização do titular | `nc` | [Termos/fundamento](https://geoinfo.dados.embrapa.br/datasets/geoinfo_data%3Ageonode%3Aperfis_pronasolos_2020/metadata_detail) |
| **Acervo Fundiario/INCRA** | Dados públicos SIGEF/SNCI/assentamentos; base legal e INCRA | Sim, nas condições descritas | `livre` | [Termos/fundamento](https://www.gov.br/incra/pt-br/acesso-a-informacao/dados-abertos) |
| **Fundacao Rio Verde** | Fonte privada; licença de reutilização não localizada | Não comprovado por licença | `zona_cinza` | [Termos/fundamento](https://fundacaorioverde.com.br/publicacoes/) |
| **UNICA** | Fonte privada; licença de reutilização não localizada | Não comprovado por licença | `zona_cinza` | [Termos/fundamento](https://unicadata.com.br/) |

### Legenda de Classificação

| Classificação | Significado |
|---|---|
| `livre` | Uso comercial admitido no escopo descrito. Atribuição, preservação de avisos e indicação de alterações podem ser obrigatórias. SA/ODbL podem exigir compartilhamento nas mesmas condições; ND pode impedir distribuir adaptações protegidas. A categoria não significa ausência de condições. |
| `nc` | A licença permite uso não comercial nas condições publicadas; uso comercial depende de autorização do titular. |
| `zona_cinza` | Licença de reutilização ausente ou alcance dos termos ambíguo. A classificação não concede permissão nem prova proibição geral. |
| `restrito` | Redistribuição proibida ou condicionada a autorização/licença, ressalvadas as dispensas expressas da fonte. A forma escrita só é exigida quando prevista no termo aplicável. |

## Detalhes por Fonte

### CEPEA/ESALQ

Os indicadores e séries CEPEA/ESALQ usam CC BY-NC 4.0: atribuição obrigatória, indicação de alterações e uso não comercial. Uso comercial exige autorização expressa do CEPEA. A licença recuperada não impõe literalmente forma escrita; documentar a autorização por escrito é uma recomendação. Preserve os créditos e avisos.

### IMEA

As séries públicas do IMEA são classificadas como `zona_cinza`: não foi comprovada licença de reutilização nem que a cláusula de arquivos não públicos alcance esse recorte. Os termos condicionam o compartilhamento de arquivos não públicos à autorização prévia por escrito; essa restrição permanece para tais arquivos. As reservas sobre bases de dados e outros ativos não são uma licença aberta. O módulo avisa na primeira chamada. [Termo de Uso do IMEA](https://imea.com.br/imea-site/termo-de-uso.html).

### ANDA

A ANDA é uma entidade privada e não foi localizada licença de reutilização das estatísticas de entregas de fertilizantes. A classificação é zona_cinza. A reserva genérica do portal não foi convertida em uma cláusula NC dos números; a ausência de licença tampouco autoriza a reprodução integral de relatórios ou de uma base protegida. Registrar fonte, mês/ano e extração. A categoria registra a ausência de licença publicada.

### ABIOVE

A ABIOVE é uma associação privada. Não foi localizada licença específica de reutilização das estatísticas do Complexo Soja; a classificação é zona_cinza. Distinguir os valores extraídos da estrutura de planilhas, textos e relatórios. Citar ABIOVE, arquivo, período e transformações documenta a origem, mas não substitui eventual permissão necessária. A categoria registra a ausência de licença publicada.

### ANEC

A ANEC permanece `zona_cinza`. O portal e a área de estatísticas anuais consultados não apresentam licença de reutilização nem cláusula específica sobre os relatórios; o rodapé contém apenas a reserva genérica de direitos da associação. Acesso público não concede permissão sobre relatórios ou bases protegidas. Preservar ANEC, publicação, edição, período e transformações. O aviso de licença na primeira chamada permanece.

### Notícias Agrícolas

Notícias Agrícolas é classificado como `zona_cinza` porque não foi localizada licença própria de reutilização das cotações. A reserva genérica de direitos não comprova uma proibição específica de reutilizar todo fato numérico, nem concede permissão sobre relatórios ou bases protegidas. Dados de origem CEPEA conservam a CC BY-NC 4.0, com atribuição e autorização para uso comercial. O fallback automático permanece e emite o aviso do publicador, além do aviso da origem. Quando CEPEA e Notícias Agrícolas constam em `MetaInfo.data_sources`, prevalece `nc` em `MetaInfo.license`.

### B3 (Brasil, Bolsa, Balcão)

A B3 mantém a classificação `zona_cinza`. A FAQ dispensa autorização prévia para determinados dados de fim de dia e históricos a partir de D-1 obtidos das plataformas Market Data B3, mas os termos do website condicionam uso comercial e redistribuição. Não foi demonstrado que a dispensa cubra os arquivos de ajustes e posições em aberto usados pelo agrobr. A política vigente até 31/10/2026 e a política anunciada para 01/11/2026 têm vigências separadas; o conteúdo da política futura não fundamenta esta classificação. Conferir o canal e o uso concreto nos [documentos oficiais da B3](https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/distribuidores/politica-comercial-e-contratos/). A categoria não afirma permissão comercial nem ausência de termos.

### CONAB CEASA/PROHORT

CEASA/PROHORT permanece `zona_cinza`. O portal de informações da CONAB identifica os painéis visíveis como informação pública e reserva direitos sobre o Sistema, sem cláusula específica sobre os dados. A página antiga redireciona para a página inicial, e o painel do PROHORT não foi localizado na consulta de 02/10/2026; a prova permanece limitada. Dados das CEASAs podem ter origem em terceiros. Esse alcance não recebe automaticamente a classificação das estatísticas próprias da CONAB. Preservar CONAB/PROHORT, CEASA de origem, produto, período e extração.

### UNICA

A UNICA é uma associação privada; não foi localizada licença específica de reutilização das séries históricas e dos relatórios quinzenais. A classificação é zona_cinza. O rodapé reserva direitos, sem demonstrar proibição comercial específica dos números ou dispensa educacional geral. Citar UNICA/UNICAdata, relatório ou série, safra/período e tratamento. A atribuição não substitui eventual permissão.

### FUNAI

O termo específico de geoprocessamento da FUNAI, repetido no Abstract de Funai:tis_poligonais e de Funai:tis_pontos, permite reprodução com citação, ressalvados casos expressamente contrários e conteúdos de terceiros. Esse termo precede o rodapé genérico BY-ND para a camada própria identificada. Citar FUNAI, camada, edição e tratamento, sem apresentar modificações próprias como novo documento oficial. Não estender a licença às camadas de outros produtores.

### IBAMA

O conjunto [Fiscalização — termo de embargo](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-termo-de-embargo) declara `Outra (Aberta)` com link de conformidade à Open Definition. A classificação é `livre`, apoiada na declaração aberta do conjunto e na base federal; não se atribui CC nominal ou ODbL de outro conjunto. Preservar IBAMA, conjunto, edição, acesso e transformações. A página individual do recurso retornou 502 na conferência de 02/10/2026: o vínculo com o CSV utilizado foi identificado pelo nome, sem comprovação byte a byte do download nessa conferência.

### Lista Suja

A página oficial do Cadastro de Empregadores do MTE indica CC BY-ND 3.0 no rodapé, sem licença individual dos arquivos localizada. A categoria livre admite uso comercial com atribuição, preservada a vedação de distribuir adaptação protegida. Mudança técnica de formato não é, por si, derivação. Citar MTE, edição, publicação e tratamento; a saída do agrobr não deve ser apresentada como documento oficial modificado. O dataset que reutiliza a publicação não cria outra licença. O aviso operacional sobre dados pessoais permanece.

### BCB SGS

Em 07/09/2026, o [catálogo oficial da série SGS 1](https://dadosabertos.bcb.gov.br/dataset/1-taxa-de-cambio---livre---dolar-americano-venda---diario) e seus metadados CKAN indicavam **Open Data Commons Open Database License (ODbL)**. Essa indicação identifica a licença daquele conjunto; não foi confirmada individualmente para todos os códigos aceitos pela API genérica.

A categoria institucional preexistente `livre` é mantida no agrobr. Ela não substitui as condições da licença do conjunto consultado. A resposta de observações não contém licença, unidade ou frequência; preserve o código e a proveniência para relacionar o resultado ao catálogo específico.

O dataset [`series_economicas`](api/series_economicas.md) reutiliza esses recursos e conserva a identificação do código e da aquisição. O wrapper não estabelece uma licença própria nem amplia a verificação da série 1 aos demais códigos.

### BCB Focus

Em 07/09/2026, o [catálogo oficial Expectativas de Mercado](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado) e os recursos [anual](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado/resource/57d46ecb-4d27-45e9-8145-c173e0b94ff5) e [mensal](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado/resource/c26059cc-2b28-41c5-a258-88a7c2b664d0) declaram **Open Data Commons Open Database License (ODbL)**. O agrobr mantém a categoria interna `livre`, que não substitui as condições dessa licença. Preserve a atribuição ao BCB e a proveniência dos recursos.

Esta API trata estatísticas agregadas das entidades anual/mensal. O catálogo atual marca o serviço de microdados institucionais como desativado; essa outra família fica fora desta API e não é acionada como fallback.

### BCB PTAX

Em 07/09/2026, o [conjunto Taxas de Câmbio — todos os boletins diários](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios) e os recursos [Moedas](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios/resource/9d07b9dc-c2bc-47ca-af92-10b18bcd0d69), [por data](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios/resource/db9b40bf-9b8f-47c4-a82d-3a3afab52e90) e [por período](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios/resource/0439af6a-d9be-4bf7-bf1a-60583e5f4c1c) declaram **Open Data Commons Open Database License (ODbL)**. A categoria interna `livre` permanece; ela não substitui as condições da licença. Conservar atribuição e proveniência dos recursos consultados.

Esta verificação cobre o catálogo OData e as rotas genéricas usadas pelo agrobr. Não generaliza seus termos para serviços de conversão, outras bases de cotações, CSV de todas as moedas ou a tabela histórica de exclusões. O SDK preserva cotações/paridades e não calcula taxas comerciais ou conversões.

### Fontes Governamentais Brasileiras

A licença deve ser verificada para o conjunto e o canal efetivamente usados. A LAI disciplina transparência e acesso, inclusive automatizado; não elimina termos específicos ou direitos de terceiros. O Decreto 8.777/2016 fornece a base federal de dados abertos e livre utilização no seu âmbito, com as ressalvas de titularidade previstas. Empresas públicas e fontes estaduais exigem delimitação própria. A classificação de uma fonte não estende automaticamente a licença a outros produtos da instituição. Citar fonte, edição e transformações; cumprir as condições específicas de atribuição, ND, SA, NC ou autorização descritas em cada fonte.

### NASA POWER

A declaração específica do projeto POWER identifica seus dados como domínio público; não foi comprovada a CC BY 4.0 antes atribuída ao produto. O aviso histórico foi conferido e deve ser lido com o guia de referência do POWER e a regra americana para obras federais, 17 U.S.C. § 105. Reconhecer NASA POWER Project, os produtos e versões usados e as transformações. A conclusão não licencia marcas nem materiais de terceiros.

### USDA PSD

O registro oficial preservado do conjunto USDA/FAS Production, Supply and Distribution, USDA-26341, indica CC BY 4.0. Usar esse rótulo com atribuição, link e alterações; a antiguidade do registro e a consulta atual do portal são limites explícitos da prova. A regra de domínio público de obras federais nos EUA é fundamento adicional delimitado, não uma declaração de CC0 ou de domínio público mundial.

### UN Comtrade

A classificação UN Comtrade é restrito: a redistribuição depende, como regra, de permissão/licenciamento. A política dispensa autorização/licença de distribuição em poucas tabelas ou gráficos de publicações; extração/streaming gratuitos de até 100.000 registros; extração/streaming gratuitos de muitos registros para assinantes ativos existentes; visualização/análise gratuitas; e uso interno, inclusive em modelo de IA. Há previsão de compartilhamento em projeto conjunto. Transformação substancial afasta a licença de distribuição com taxa segundo a política, sem apagar as demais condições. Aplicações com fins lucrativos são listadas como sujeitas a licença com taxa. A exigência geral de assinante premium ativo para redistribuição precisa ser lida junto às dispensas, sem presumir sua inaplicabilidade. Citar UN Comtrade e preservar o disclaimer. Acesso gratuito não equivale a redistribuição irrestrita.

### CFTC COT

A Web Policy da CFTC coloca sua informação governamental em domínio público e permite cópia e distribuição, solicitando reconhecimento da fonte. Essa declaração alcança os relatórios COT próprios conferidos, com apoio em 17 U.S.C. § 105. Citar CFTC, família Futures/Combined, data e contrato. A ressalva de material de terceiros continua aplicável; não há CC nominal para afirmar.

### CONAB

As estatísticas próprias de safra, séries e custos da CONAB são classificadas como livre com base na LAI, na natureza de empresa pública e na busca sem restrição específica dos valores. Não foi comprovada licença nominal de cada planilha. A CONAB declara não integrar o escopo institucional do Decreto 8.777/2016; esse decreto não é apresentado como licença automática da empresa. Citar publicação, safra, edição e tratamento. Conteúdo editorial do portal pode estar sujeito a BY-ND; CEASA/PROHORT e Progresso têm análises próprias.

### IBGE/SIDRA

O SIDRA e sua API são canais de dados abertos no [PDA do IBGE 2024–2025](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf). A classificação é `livre`, com base na LAI, no Decreto 8.777/2016 e na política institucional, sem atribuir uma licença CC nominal a todas as estatísticas. O [termo do portal, versão de 11/03/2024](https://www.ibge.gov.br/acesso-informacao/acoes-e-programas/politica-de-privacidade.html) trata de privacidade e serviços e não traz restrição à reutilização das estatísticas ou vetores publicados. Preservar IBGE, pesquisa, tabela, período, edição e transformações. A antiguidade do PDA e a ausência de licença nominal por conjunto delimitam a prova; a licença deve ser verificada no produto efetivamente usado. A página de Áreas Urbanizadas também não apresenta licença ou restrição específica e remete ao mesmo termo do portal; esse recorte é `livre` pela base federal e institucional. A licença nominal de um produto, como a MMD 2025, não é generalizada aos demais.

### BCB/SICOR

O catálogo da Matriz de Dados do Crédito Rural/SICOR v2 declara ODbL 1.0. Uso comercial é permitido com atribuição ao BCB, link da licença e preservação dos avisos. Bases derivadas compartilhadas devem observar as condições de compartilhamento pela mesma licença quando aplicáveis; resultados produzidos e bases derivadas não são hipóteses idênticas. Esta prova não licencia automaticamente todas as séries SGS ou outros produtos do BCB.

### ComexStat

Os arquivos brutos próprios de importação, exportação e tabelas auxiliares Comex Stat são classificados como livre pela base federal de dados públicos e pela busca documentada no catálogo MDIC. Não foi demonstrada licença CC dos CSVs. Preservar MDIC/SECEX-Comex Stat, fluxo, período, classificação e transformações, além de eventuais direitos de terceiros expressamente indicados.

### ANTAQ

O Estatístico Aquaviário é identificado como base aberta no PDA 2026–2028 da ANTAQ. A classificação livre mantém fundamento na publicação própria da autarquia, na legislação federal e na busca documentada. O catálogo direto de downloads permaneceu bloqueado nesta conferência; isso não prova licença nem indisponibilidade total do serviço. Citar ANTAQ, ano, arquivo e tratamento.

### ANP Diesel

A Série Histórica de Preços de Combustíveis e de GLP e as Vendas de derivados de petróleo e biocombustíveis são identificadas como conjuntos já abertos no PDA ANP 2026–2028. A classificação livre decorre dessa política específica, lida com o Decreto 8.777/2016. Os termos gerais do portal contêm restrições comerciais; foram confrontados com a política dos conjuntos, não declarados inexistentes. Não foi comprovada CC nominal. Citar ANP, família, período, edição, unidade e alterações. A conclusão não abrange todos os sistemas ou serviços da ANP.

### ANTT Pedagio

Os dois conjuntos ANTT, Volume de tráfego por praça de pedágio e Praça de pedágio, declaram Creative Commons Atribuição. Uso comercial com crédito e indicação de alterações. A versão não é informada nos catálogos; não a completar como 4.0. Ao combinar os conjuntos, conservar a identificação e o link de ambos.

### MAPA PSR

O conjunto MAPA/SISSER-PSR identificado no catálogo declara Creative Commons Atribuição, sem versão numérica. Uso comercial permitido com crédito, identificação do conjunto/recurso, período e alterações. A licença é do conjunto PSR conferido; não uma licença geral de todas as bases do MAPA.

### SICAR

A classificação do SICAR é `livre` pela base pública federal: LAI, Decreto 8.777/2016 e publicidade do CAR na Lei 12.651/2012. A consulta pública oferece visualização e download da base por UF sem termo específico de reutilização localizado. Campos `Fees` e `AccessConstraints` vazios no WFS não são uma concessão de licença. Não foi comprovada uma CC BY da base; o BY-ND do rodapé gov.br é do conteúdo do site. Preservar SFB/CAR, camada, UF, extração e transformações. [Geosserviços do CAR](https://consultapublica.car.gov.br/publico/geoservicos/index).

### DERAL

As estatísticas próprias de Previsão de Safras do DERAL/SEAB-PR são classificadas como livre pela natureza pública estadual, pela LAI e pelo Decreto estadual 10.285/2014, com busca sem restrição específica localizada. A norma estadual complementa a LAI. Preservar safra, data, publicação e alterações; não atribuir uma licença CC inexistente.

### INMET

A conclusão livre abrange as observações próprias publicadas pelo INMET, com base federal e busca documentada. Conservar estação/arquivo, período, variáveis e transformações. O contrato operacional de emissão e uso do token não foi auditado; licença dos dados e condições de acesso ao serviço são assuntos distintos. Não se afirma CC nominal ou ausência de limites operacionais.

### Queimadas/INPE

Os dados próprios do Programa Queimadas/INPE são classificados como livre pela publicação oficial, FAQ e base federal. Seguir a orientação de referência do produto e citar INPE, produto, período e satélite, com indicação das transformações. Não importar a licença BY-SA de outro programa. Falhas de transporte na reabertura da FAQ foram registradas e não apagam a evidência preservada.

### Desmatamento PRODES/DETER

Os dados PRODES/DETER abrangidos pela política do Programa de Monitoramento dos Biomas Brasileiros usam CC BY-SA 4.0. Uso comercial permitido com atribuição ao INPE, identificação do produto/bioma/camada/período, link da licença e alterações. Adaptações compartilhadas devem observar CompartilhaIgual quando aplicável. A categoria livre não elimina essas condições.

### MapBiomas

A FAQ atual dos dados de cobertura e uso da terra MapBiomas declara CC BY 4.0, prevalecendo sobre a reserva genérica do rodapé. Citar Projeto MapBiomas, coleção, recurso, recorte, data de acesso e alterações. A política geral fundamenta também a coleção 11; não se declara leitura dos termos internos de cada planilha. Esta conclusão não abrange MapBiomas Alerta.

### CONAB Progresso

A ficha de publicação do XLSX de Progresso de Safra vincula o conteúdo ao rodapé CC BY-ND 3.0. A categoria livre registra permissão de reprodução comercial com atribuição, preservada a condição de não distribuir adaptações protegidas. Não foi encontrada licença individual incorporada à planilha. Mera alteração técnica de formato não cria derivado por si só. Citar CONAB, semana e edição, e distinguir extração/tratamento do documento oficial.

### IBGE PPM

O SIDRA e sua API são canais de dados abertos no [PDA do IBGE 2024–2025](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf). A classificação é `livre`, com base na LAI, no Decreto 8.777/2016 e na política institucional, sem atribuir uma licença CC nominal a todas as estatísticas. O [termo do portal, versão de 11/03/2024](https://www.ibge.gov.br/acesso-informacao/acoes-e-programas/politica-de-privacidade.html) trata de privacidade e serviços e não traz restrição à reutilização das estatísticas ou vetores publicados. Preservar IBGE, pesquisa, tabela, período, edição e transformações. A antiguidade do PDA e a ausência de licença nominal por conjunto delimitam a prova; a licença deve ser verificada no produto efetivamente usado. O PDA nomeia a Pesquisa da Pecuária Municipal e a tabela SIDRA 3939.

### IBGE Abate

O SIDRA e sua API são canais de dados abertos no [PDA do IBGE 2024–2025](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf). A classificação é `livre`, com base na LAI, no Decreto 8.777/2016 e na política institucional, sem atribuir uma licença CC nominal a todas as estatísticas. O [termo do portal, versão de 11/03/2024](https://www.ibge.gov.br/acesso-informacao/acoes-e-programas/politica-de-privacidade.html) trata de privacidade e serviços e não traz restrição à reutilização das estatísticas ou vetores publicados. Preservar IBGE, pesquisa, tabela, período, edição e transformações. A antiguidade do PDA e a ausência de licença nominal por conjunto delimitam a prova; a licença deve ser verificada no produto efetivamente usado. O PDA nomeia a Pesquisa Trimestral do Abate de Animais e a tabela SIDRA 1092.

### IBGE Censo Agro

O SIDRA e sua API são canais de dados abertos no [PDA do IBGE 2024–2025](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf). A classificação é `livre`, com base na LAI, no Decreto 8.777/2016 e na política institucional, sem atribuir uma licença CC nominal a todas as estatísticas. O [termo do portal, versão de 11/03/2024](https://www.ibge.gov.br/acesso-informacao/acoes-e-programas/politica-de-privacidade.html) trata de privacidade e serviços e não traz restrição à reutilização das estatísticas ou vetores publicados. Preservar IBGE, pesquisa, tabela, período, edição e transformações. A antiguidade do PDA e a ausência de licença nominal por conjunto delimitam a prova; a licença deve ser verificada no produto efetivamente usado. O PDA nomeia o Censo Agropecuário e a tabela SIDRA 6846, além da abertura do Censo 2017.

### IBGE Censo Agro Histórico

O SIDRA e sua API são canais de dados abertos no [PDA do IBGE 2024–2025](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf). A classificação é `livre`, com base na LAI, no Decreto 8.777/2016 e na política institucional, sem atribuir uma licença CC nominal a todas as estatísticas. O [termo do portal, versão de 11/03/2024](https://www.ibge.gov.br/acesso-informacao/acoes-e-programas/politica-de-privacidade.html) trata de privacidade e serviços e não traz restrição à reutilização das estatísticas ou vetores publicados. Preservar IBGE, pesquisa, tabela, período, edição e transformações. A antiguidade do PDA e a ausência de licença nominal por conjunto delimitam a prova; a licença deve ser verificada no produto efetivamente usado. Para o Censo Agro histórico, preservar o ano, a pesquisa e a tabela de cada edição; a política institucional não torna comparáveis recortes metodológicos distintos.

### IBGE Censo Agro Municipal 1985

A classificação é `livre` pela LAI, pelo Decreto 8.777/2016 e pelo [PDA do IBGE 2024–2025](https://www.ibge.gov.br/np_download/novoportal/documentos_institucionais/Plano_de_Dados_Abertos_IBGE_2024_2025.pdf). O PDA, p. 12, declara o objetivo de acesso livre à produção institucional da Biblioteca, que alcança os volumes digitalizados. Para os volumes do Censo Agropecuário 1985, preservar volume, UF, página/tabela e transformações. Não foi identificada licença nominal desses volumes.

### ZARC

O conjunto da tábua de risco ZARC declara Creative Commons Attribution, sem versão numérica. Atribuir MAPA/ZARC, safra/família, identificador do recurso e alterações. Não há condição SA demonstrada. A conclusão acompanha os recursos vinculados ao conjunto, inclusive o recurso 2026/2027 conferido.

### ANA/SNIRH

As cinco famílias ANA/SNIRH utilizadas — hidrografia, pivôs, demanda de irrigação, disponibilidade e massas d'água — têm classificação livre pela base federal, política institucional e metadados acessíveis. Não foi demonstrada licença nominal individual. Atribuir ANA/SNIRH, camada e edição, preservando autores adicionais expressos. Campos vazios de licença não foram tratados como concessão.

### ICMBio UCs Federais

A política oficial de dados abertos do ICMBio nomeia o conjunto Limites oficiais das Unidades de Conservação Federais e admite acesso, uso, modificação e compartilhamento. A classificação é livre, preservando ICMBio, camada, edição e alterações. Ressalvas técnicas dos metadados não foram convertidas em NC. Não há licença CC nominal ou SA comprovados.

### CNUC (MMA)

A camada WFS ms:ucs_selected do CNUC é classificada como livre pela base legal federal e busca documentada. O catálogo CKAN Unidades de Conservação declara CC BY para seus recursos, sem versão numérica; não foi demonstrado que esse campo licencia também o WFS. Atribuir MMA/CNUC, camada/edição e tratamento; acrescentar CC BY e identificador quando o recurso efetivo for o do catálogo.

### INCRA Quilombolas

A camada de Territórios Quilombolas publicada pelo INCRA e distribuída pelo CMR/FUNAI tem classificação livre pela base federal e política do produtor. Atribuir a origem ao INCRA e identificar separadamente o distribuidor, camada, edição e alterações. A autorização da FUNAI para dados próprios não foi aplicada ao conteúdo do INCRA; não se afirma CC BY 4.0.

### MapBiomas Alerta

Os dados do MapBiomas Alerta usam [CC BY-SA 3.0 BR](https://creativecommons.org/licenses/by-sa/3.0/br/), conforme o link do item 3.1 dos [termos](https://plataforma.alerta.mapbiomas.org/terms). O texto escreve `CC-CY-SA`, mas aponta para essa licença; o item 3.2 inclui expressamente o acesso pela API. A classe é `livre`: uso comercial permitido, com atribuição ao MapBiomas Alerta, link da licença, indicação de alterações e compartilhamento de adaptações sob as condições SA aplicáveis. A licença específica dos dados delimita a reserva geral de propriedade intelectual do site (8.2/8.3). A vedação de vender ou alugar o Serviço (7.1, ix) não é tratada como cláusula NC dos dados. Imagens, bases e laudos de terceiros exigem a verificação própria indicada no item 6.7.

### SFB

CNFP, concessões florestais e IFN são classificados como `livre` pela base federal de dados públicos, LAI e Decreto 8.777/2016, com busca sem restrição específica nos metadados consultados. Não foi comprovada uma licença CC nominal. No IFN, `copyrightText` vazio não é uma concessão de licença; a fundamentação é institucional. Preservar SFB, família, camada/edição, território, extração e transformações. Disponibilidade do serviço e classificação de licença são condições distintas.

### EMBRAPA Solos

Os metadados de perfis_pronasolos_2020 e brasil_solos_5m_20201104 declaram separadamente CC BY-NC 3.0 BR. Manter atribuição à Embrapa e aos autores indicados, camada/edição, link da licença e alterações. Uso comercial depende de permissão do titular. Não generalizar essa condição para todos os recursos GeoInfo nem acrescentar SA não demonstrada.

### Acervo Fundiario/INCRA

Os dados públicos SIGEF, SNCI e assentamentos do Acervo Fundiário/INCRA são classificados como livre pela LAI, pelo Decreto 8.777/2016 e pela política institucional do INCRA, após busca sem restrição comercial específica localizada. A alegação anterior de veto comercial não tinha cláusula comprovada. A indicação histórica de CC BY não foi recapturada e não sustenta versão numérica. Citar INCRA, família, UF/abrangência, arquivo, edição e transformações, preservando direitos de terceiros expressos. O PDA 2021–2023 prova política e origem, não atualidade de todo serviço em 2026.

### Fundacao Rio Verde

A Fundação Rio Verde é um publicador privado sem licença específica de reutilização localizada para os resultados de competição de cultivares de soja. A classificação é zona_cinza. A reserva genérica do portal não comprova NC dos valores, e a ausência de licença não libera a reprodução integral da base ou dos PDFs. Citar fundação, safra, publicação, página/tabela e extração.

## Defensivos Agrofit

O catálogo MAPA/Agrofit declara Creative Commons Attribution, sem versão, para os CSVs de produtos formulados e técnicos identificados no conjunto. Atribuição obrigatória, com recurso, acesso e alterações. Os datasets derivados desses dois CSVs não criam licença própria. A prova não abrange automaticamente bulas, marcas, imagens ou arquivos de terceiros.

Os quatro [datasets Agrofit](api/defensivos_datasets.md) reutilizam esses mesmos dois CSVs. Produtos formulados e autorizações de uso vêm da exportação de formulados; produtos técnicos vêm da exportação de técnicos; composição é extraída do texto publicado de componentes de cada família. A camada de datasets não cria uma licença nova nem comprova vigência legal a partir do valor bruto de `situacao`.

Esta verificação não se estende a registros de empresas, rótulos ou bulas, nem ao histórico de alterações. Esses recursos adicionais exigem verificação própria de escopo e termos.

## RNC/SNPC — CultivarWeb

Os cadastros públicos RNC e SNPC do CultivarWeb são classificados como livre pela base federal e busca documentada nos formulários. Não foi localizada licença CC nominal dos CSVs. Citar MAPA, família registradas/protegidas, data de extração e tratamento. A conclusão não concede direito de exploração de material propagativo nem abrange processos ou anexos restritos.

Os dois [datasets de cultivares](api/cultivares.md) reutilizam as exportações públicas e conservam sua origem, aquisição e hashes. Não criam licença separada nem interpretam direitos de exploração de cultivares a partir do cadastro. Processos restritos, anexos e histórico de alterações ficam fora.
