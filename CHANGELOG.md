# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [2.0.0] - Não lançado

Catálogo desta versão: **53 datasets e 88 contratos registrados**. Mudanças incompatíveis com 1.x estão em Changed; consulte o [guia de migração](docs/guides/migracao-2.md).

### Destaques

- **17 datasets novos (36 → 53) e 88 contratos registrados:** Agrofit (produtos formulados e técnicos, autorizações e composição), cultivares (RNC e SNPC), Lista Suja, unidades de conservação federais (ICMBio), ANEC (embarques mensais, destinos e comparação anual), BCB (cotações e moedas, Focus e séries econômicas), preços do diesel (ANP) e custos da sociobiodiversidade (CONAB).
- **Correções de dado** em valor, rótulo, unidade e recorte nas principais fontes (CONAB, IBGE, CEPEA, SICOR, PSR, ANTT, Comex Stat, SICAR e outras), entre elas o ano civil do café e dos cereais de inverno (#109), a área total da cana (#111) e o algodão em pluma e em caroço (#112). Detalhes em Fixed.
- **Incompatibilidades com a 1.x** (em Changed): contratos com versão major nova (BCB PTAX, Focus e SGS, clima, estimativa de safra, SICAR, Lista Suja, Comtrade e outros); argumento desconhecido vira `TypeError`; `InvalidParameterError` sai antes da rede e interrompe a cascata de fontes; `as_polars=True` sem Polars levanta `ImportError`; `agrobr.configure()` e módulos sem uso saem. **Leia o [guia de migração](docs/guides/migracao-2.md) antes de atualizar.**
- **Proveniência no `MetaInfo`:** SHA-256 e tamanho do corpo recebido em 21 fontes, `fetch_timestamp` da aquisição real (inclusive no acerto de cache), `license` com a classificação do dado e `SourceFallbackWarning` quando o dataset devolve uma fonte alternativa.
- **Resiliência:** o IBGE cai para a API de agregados (`servicodados`) quando o SIDRA falha; o cache degradado avisa; datas das fontes seguem a mesma regra no pandas 2 e no 3.
- **Segurança:** teto de descompressão para membro de ZIP e XLSX, credencial da CEASA fora da URL, `semana_url` restrita ao domínio da CONAB e pisos do `pyarrow` (CVE-2023-47248) e do `soupsieve`.
- **Reconciliação semanal:** o workflow `reconciliacao.yml` compara, toda segunda-feira, a saída com as fontes oficiais e abre issue quando diverge.
- **Documentação:** guia de migração e página "O que o agrobr grava no disco", em PT e EN.

### Added

- **BCB — `bcb.credito_rural_total`** — crédito rural por UF e finalidade, sem produto, da entidade `RegiaoUF` do SICOR, com as quatro finalidades, inclusive a industrialização (R$ 28,1 bi em 2023), que não sai por produto. Agregação por UF ou por programa, filtros de safra (julho a junho), finalidade e UF validados antes da rede, finalidade sem operação ausente (o zero de preenchimento da fonte não vira linha), sem linha Brasil e com o primeiro e o último mês da safra em `source_details["meses"]`. Contrato novo `bcb_credito_rural_total` 1.0. Conferido contra os corpos oficiais do SICOR na safra 2022/23, por UF e finalidade, e, em jan/2023, contra a soma dos municípios e a soma por produto do `credito_rural`.
- **BCB — `agregacao="registro"` no `credito_rural`** — `bcb.credito_rural` e `datasets.credito_rural` devolvem os registros do SICOR depois dos filtros de UF, programa e tipo de seguro, sem agregar: as 21 colunas da chamada padrão da 1.1.0 (mês, região, subprograma, fonte de recursos, tipo de seguro, modalidade e atividade, com os nomes), mais `agregacao` e `fonte`, no contrato novo `bcb.credito_rural_registro` 1.0, cujo nome e versão vão ao `MetaInfo`. Os nomes de fonte de recursos, modalidade e atividade saem da descrição das tabelas de domínio do BCB (37, 64 e 2 códigos), e código fora da tabela fica com nome nulo. Conferido contra o OData ao vivo em 20 células, e a soma por safra, UF, produto e finalidade é igual à `agregacao="uf"`. Sem fallback BigQuery, que não traz essas dimensões.
- **IBGE — canal de fallback `servicodados`** — quando a API SIDRA falha (403 Cloudflare, HTML, 5xx ou rede), `fetch_sidra` consulta a mesma tabela na API de agregados do IBGE (`servicodados.ibge.gov.br/api/v3/agregados`) com os seletores traduzidos e devolve o mesmo formato de colunas; `MetaInfo.attempted_sources`/`selected_source` passam a `ibge_servicodados`, `source_details["canal"]`/`consultas` e `source_url` registram cada consulta, o dataset herda a proveniência e a primeira queda emite um aviso único
- **CONAB série histórica** — `algodao_pluma` e `algodao_caroco` expõem os recortes de pluma e semente do mesmo arquivo oficial, completando 45 produtos (#112)

- **Custos da sociobiodiversidade CONAB** — novo dataset `custo_sociobiodiversidade` e catálogo próprio: 20 produtos comprovados por captura, 37 recursos históricos/ativos, seleção pelo link oficial e contrato 1.0 com 23 colunas. Preserva valores, unidades e contextos dos layouts antigos/novos, inclusive cabeçalhos mesclados, sem converter hectare/safra/unidade. Recusas nominais, proveniência, cache de catálogo isolado, golden completo de açaí/carnaúba e matriz live por produto. Safras em dois anos conservam `safra_publicada` e usam o primeiro ano em `ano`; regiões e UF do nome da aba têm proveniência explícita. Tipagem alinhada em mypy 1.19/2.x; limitações nominais sem descartar a terceira medida publicada.
- **Censo municipal 1985** — `ibge.cobertura_censo_agro_municipal_1985()` lista, por tema, as UFs com casas no pacote.
- **Dataset de séries econômicas** — `series_economicas` consulta um código ou alias SGS com intervalo e `ultimos`, reutilizando o contrato `bcb_sgs` 2.0 de quatro colunas. Preserva referências civis, valores negativos/nulos, partições, limites e proveniência do manifesto e dos recursos. Flags estritas, sync e Polars após validação; parâmetros inválidos e contexto `deterministic` falham antes de I/O. Frequência e unidade dependem da série, sem inferência genérica ou snapshot de revisões.
- **Datasets de cultivares** — `cultivares_registradas` e `cultivares_protegidas` reutilizam RNC/SNPC e os contratos `rnc_registradas`/`rnc_protegidas` 1.0, sem aliases. Filtros nomeados, flags booleanas estritas, saída vazia tipada, sync e Polars após validação, com proveniência da aquisição preservada. Contexto `deterministic` é recusado antes de I/O porque o cadastro corrente não reconstitui versões históricas.
- **Dataset Lista Suja** — `empregadores_lista_suja` leva o cadastro corrente do MTE à camada semântica, reutilizando o contrato de fonte 2.0 de doze colunas. Preserva UF, ID textual exato, seleção CSV/PDF, rotas tentadas, recursos e contexto da publicação. Flags booleanas estritas, sync e Polars após validação; `deterministic` é recusado antes de I/O. O arquivo completo é adquirido a cada consulta.
- **Datasets Agrofit** — `defensivos_formulados`, `defensivos_tecnicos`, `autorizacoes_defensivos` e `composicao_defensivos` reutilizam as quatro APIs e contratos de fonte existentes (1.1/1.0). Preservam filtros, registro textual, multiplicidade de autorizações, posição de componentes, tipos e proveniência do cache. Interfaces nomeadas, três flags booleanas estritas, sync e Polars após validação. Contexto `deterministic` é recusado antes de I/O; o cadastro corrente não reconstitui versões históricas.
- **Agrofit composição** — `defensivos.composicao(tipo="formulados"|"tecnicos", ...)` com contrato 1.0 por família, registro e posição; preserva componentes repetidos, texto de concentração e unidades, interpreta notação científica explícita e deixa valores ambíguos nulos com diagnóstico. A fonte publica quatro contratos, também reutilizados pelos datasets Agrofit.
- **INMET histórico** — `historico_periodo(codigo, inicio, fim)` recorta intervalos inclusivos entre arquivos anuais antes de agregar, e `historico_uf(uf, ano)` calcula os meses pelas estações do arquivo. Dados públicos de automáticas desde 2000, sem token; membros ausentes e cobertura parcial são diagnosticados, sem imputação de chuva ou datas. `historico(codigo, ano)` conserva a interface anual e o erro quando a estação não consta do arquivo.
- **ANEC** — datasets `embarques_mensais_anec`, `comparacao_anual_anec` e `destinos_anec` com contratos 1.0, ano da edição obrigatório e proveniência por edição/revisão; preservam estimativas, faixas sem ponto médio e participações acumuladas dos destinos. Mensal e destinos usam schema de fonte 1.1; comparação anual usa 1.2, com volumes independentes do ano e colunas legadas preservadas; embarques semanais permanece 1.0. Extração de destinos em gráficos ainda pode retornar vazio com aviso; licença permanece `zona_cinza`.
- **MapBiomas** — coleção 11 (1985–2025) passa a padrão, com seleção explícita da coleção 10; proveniência identifica a edição e regressões usam células oficiais dos anos `y1985`–`y2025` e períodos de transição
- **produção anual** — `datasets.producao_anual` passa a aceitar `cana`, `mandioca` e `laranja` via IBGE PAM nos níveis Brasil, UF e município, preservando unidades históricas; produtos ausentes do boletim de grãos não acionam downloads CONAB sem cobertura
- **futuros agrícolas** — `tipo="oi_historico"` consulta posições abertas B3 por intervalo inclusivo e vencimento; valida produto/datas e preserva o contrato 1.0; os metadados identificam dias úteis sem posições para o filtro

- **testes Censo** — fixtures oficiais dos seis temas nas 27 UFs, com oráculos de células, cabeçalhos, unidades, escalas e zeros; 161 combinações válidas e uma rejeição esperada por arquivo incorreto na fonte do Pará
- **CONAB café** — contrato `serie_historica_safra` 1.1 acrescenta `area_em_producao_mil_ha` e `area_formacao_mil_ha` opcionais; área plantada é a soma quando ambos os componentes estão disponíveis
- **CONAB série histórica** — 12 subprodutos de feijão por tipo: `feijao_caupi`, `feijao_caupi_1`/`_2`/`_3`, `feijao_cores`, `feijao_cores_1`/`_2`/`_3`, `feijao_preto`, `feijao_preto_1`/`_2`/`_3`, recortes por tipo a partir de 2015/16; o catálogo passa de 31 para 43 produtos (#106)
- **tests** — matriz live parametrizada cobre todos os datasets registrados, com argumentos mínimos, limite de 300 segundos por fonte e diagnóstico explícito de violações de contrato
- **cepea** — indicador do bezerro CEPEA/ESALQ de Mato Grosso do Sul (issue #102), para animais de 8–12 meses, em `BRL/cabeca`, disponível na API CEPEA e no dataset `preco_diario` com fallback Notícias Agrícolas, com `valor_usd` (coluna Valor US$) e `peso_medio_kg` (tabela Peso Médio da página) nas colunas opcionais do contrato 1.1
- **exceptions** — `InvalidParameterError`, compatível com `AgrobrError` e `ValueError`, distingue erros de parâmetros do usuário de falhas de dados ou layout sem quebrar handlers existentes
- **datasets** — `SourceFallbackWarning` avisa quando a fonte primária falha e uma fonte alternativa é selecionada, com categoria e resumo do erro original
- **INCRA** — `incra.andamento_quilombola()` lê o quadro "Andamento dos processos" (PDF do INCRA, 15 colunas de texto publicado, regional pelo agrupamento gráfico, contrato 1.0) e `incra.vinculos_quilombolas()` relaciona perímetros e processos pela referência NUP literal (46 colunas, produto cartesiano por ocorrência, contrato 1.0); ambas exigem `agrobr[pdf]`
- **seguro_rural / mapa_psr, município pelo código** — filtro `cd_ibge=` (7 dígitos em texto) em `mapa_psr.apolices`, `mapa_psr.sinistros` e `seguro_rural`: `municipio=` compara o rótulo publicado, e o MAPA rotula parte das apólices com o distrito (Caxias do Sul, 2024: 274 das 693 apólices do código pelo nome). Apólices publicadas com "-" no geocódigo só aparecem pelo rótulo
- **posicionamento_fundos / cftc, spreads de swap e de other reportables** — colunas `swap_spread` e `other_spread` (contrato 1.1): com elas, o open interest fecha com as categorias (exato nos futuros; o relatório combinado da CFTC deixa até 1 contrato de resíduo). A `MetaInfo` da fonte passa a informar a versão do contrato
- **producao_anual / ibge.pam, código IBGE da localidade** — coluna opcional `localidade_cod` (contrato 2.1), o D1C que o SIDRA publica: junta municípios entre anos sem casar por nome. Só nas linhas do IBGE; o fallback da CONAB não tem código
- **clima / nasa_power / inmet, cobertura do mês** — colunas `dias`, `data_inicio` e `data_fim` no mensal (contrato `clima` 3.1; schema 1.2 do mensal do `nasa_power`): o mês cortado pelo período pedido ou o mês corrente saíam rotulados como o mês inteiro. `nasa_power.clima_ponto` de 15/01 a 05/02/2025 dá fevereiro com 5 dias e 17,81 mm, contra 28 dias e 52,33 mm do mês inteiro
- **ci — reconciliação semanal ao vivo (FR)** — o workflow `reconciliacao.yml` roda às segundas (09:00 UTC) e por `workflow_dispatch`, com `contents: read` e `issues: write` e sem secrets. Ele executa os `scripts/reconciliar_*.py` contra as fontes oficiais, um por vez, pelo `scripts/reconciliacao_semanal.py`, e cada fonte sai em um de 5 estados: `ok`, `mismatch`, `indisponível`, `não verificado` ou `erro do script`. O resumo em JSON e Markdown vai como artefato. Cada fonte com `mismatch` abre uma issue, ou comenta na que já estiver aberta, só com estado, contagem, casos e o link do artefato, sem valores da fonte. Credencial ausente (USDA, MapBiomas Alerta) e script fora do semanal (sem modo ao vivo, ou com perfil fixo de uma edição) saem como `não verificado`, nunca como sucesso.
- **embarques_anec / anec.embarques, semana e datas** — colunas `ano` e `semana` (a edição impressa no boletim), `data_inicio` e `data_fim` (lidas dos rótulos "Last Week (…)" e "Current Week (…)", nunca da semana ISO), com contrato `embarques_anec` 1.1 e parser 4. No rótulo que cruza o mês, vale a leitura em que as duas semanas se seguem, porque o boletim ora dá o mês do fim, ora o do início (W34/2026); a leitura só vale com o início da `last_week` a até 7 dias da semana da edição, e sem nenhuma as datas saem nulas com aviso (a W35/2026 imprime agosto nas duas semanas de setembro). A doc da fonte diz que o mensal sem "*" ainda muda, que as semanas não fecham o mês, qual quadro de janeiro fecha com o total impresso e quanto a ANEC difere do ComexStat
- **Comtrade, marcas de estimativa da ONU** — `comtrade.comercio` e `datasets.comercio_internacional` ganham `peso_liquido_estimado`, `peso_bruto_estimado` e `quantidade_estimada` (flags anuláveis, de `isNetWgtEstimated`, `isGrossWgtEstimated` e `isQtyEstimated`; contrato 2.1). Com peso líquido estimado no resultado, `validation_warnings` traz o HS e o período, e o `trade_mirror` lista essas células por perna em `source_details["peso_estimado"]`. No frango do Brasil em 2024 (HS 020714) e na importação chinesa de soja do Brasil em 2023, o peso publicado é estimado.
- **IBGE SIDRA, edição do período** — cada consulta pede `/agregados/{tabela}/periodos` e o `MetaInfo` traz a data de modificação dos períodos devolvidos em `source_details["periodos_modificacao"]` (`{tabela: {período: data}}`) e em cada consulta: a PAM 2024, revista em 17/09/2026, sai com `2026-09-17`. A falha no pedido de metadado não derruba a consulta: o motivo vai em `periodos_modificacao_erro`.
- **MapBiomas Alerta, `tipo_data`** — `alertas()` e `alertas_geo()` filtram pela data de publicação com `tipo_data="publicacao"`; o padrão segue a detecção e avisa (`validation_warnings` e `UserWarning`) quando o período termina a menos de 293 dias de hoje, porque a publicação atrasa meses (99% dos alertas saem em até 293 dias da detecção, medido em 26/09/2026).
- **CEPEA, série histórica** — `cepea.indicador` (e o `datasets.preco_diario`) com período anterior à janela recente da página passa a buscar a série histórica que o CEPEA publica por indicador (desde 1996 a 2010, conforme o produto), baixada inteira na primeira vez e guardada no cache DuckDB; antes, o período vinha vazio ou incompleto. Série indisponível avisa no `MetaInfo`, e período que fica sem dado levanta `SourceUnavailableError`. O leite tem 2 casas na série e 4 na página, que prevalece; a laranja não tem série (guia de migração, seção 70).
- **`cod_municipio` comum nos datasets municipais** — coluna nova e opcional `cod_municipio` (`Int64`, o código IBGE de 7 dígitos) em `producao_anual`, `pecuaria_municipal`, `extrativismo_vegetal`, `silvicultura`, `censo_agropecuario`, `censo_agropecuario_historico`, `censo_agropecuario_legado`, `cadastro_rural`, `queimadas`, `desmatamento` (DETER), `seguro_rural`, `uso_do_solo` (municipal) e `zoneamento_agricola`, e nas fontes que dividem o contrato com eles (SICAR, queimadas, PSR, MapBiomas municipal e ZARC). O join entre eles sai sem conversão; fora da linha de município, a coluna é nula. As colunas de antes ficam, e cada contrato sobe uma versão menor. A conversão é `normalize.regions.cod_municipio`; o guia de normalização ganha "Cruzar por município".

### Improved

- **anp_diesel / precos_diesel, natureza do preço** — a doc e o contrato dizem que `preco_venda` é a média simples dos postos no município e a média ponderada pelas vendas das distribuidoras na UF e no Brasil (ANP, desde 31/10/2004), que `n_postos` é o tamanho da amostra e não o peso (AL, S10, 06/09/2026: 6,84 R$/l publicado × 7,20 pela média dos municípios por postos), e que `DIESEL` é o óleo diesel B S500 comum. No ICMBio, a doc diz que `area_ha` é a área da UC inteira, não a parte dentro da UF
- **zarc, culturas renomeadas na safra 2024/2025** — o ZARC renomeou 11 rótulos de cultura (`milho` → `milho_1`, `feijao_1` → `feijao`, as versões irrigada e de sequeiro de arroz, aveia, cevada grãos e trigo numa cultura só, separadas por `manejo`, e `mamona_semiarido_sequeiro` dentro de `mamona`). Pedir a chave de uma safra na outra levanta `InvalidParameterError` com a chave equivalente daquela tábua, sem alias silencioso. A doc traz a tabela de pares e a chave de junção entre safras (`cultura_codigo` e `manejo`), conferida nas tábuas oficiais de 2023/2024 e 2024/2025, que têm os mesmos registros em cada par
- **CONAB balanço incoerente (identidades do balanço)** — `conab.balanco` emite um aviso por linha quando a tabela publicada não fecha estoque inicial + produção + importação = suprimento, consumo + exportação = demanda total ou suprimento − consumo − exportação = estoque final, além do arredondamento de 0,05 mil t por termo. O dado não muda. Exemplo: arroz 2024/25 em set/2026, com −363,8 mil t na última identidade.
- **CONAB soma das UFs × BRASIL** — `conab.safras`, `conab.serie_historica` e os datasets que os leem emitem um aviso por safra e coluna quando a soma das UFs entregues não fecha com a linha BRASIL publicada, além do arredondamento de 0,05 por termo; com parte das UFs listada (o café da série), só a soma acima do BRASIL avisa. O dado não muda. Exemplos: milho 2ª safra 2020/21 no 12º levantamento de 2021/22 (59.981,5 × 60.741,6 mil t) e trigo 2003/2004 na série.
- **CONAB brasil_total, partes × total** — no caminho da série (safra antiga), `conab.brasil_total` avisa quando tipos de feijão × safra, safras × FEIJÃO TOTAL, amendoim, arroz ou milho não fecham com o total publicado além de 1,4 (0,05 por UF somada em cada linha BRASIL). O dado não muda. Exemplo: 2021/22, feijão 3ª safra com 707,2 mil t e tipos somando 748,0.
- **CONAB série × última edição do boletim** — na safra que acabou de sair do boletim, `conab.safras` e `conab.brasil_total` conferem o BRASIL da série com a última edição que publicou a safra (um download a mais); divergência além de 0,1 gera aviso e fica em `source_details["publicacao"]["conferencia"]`. O dado segue saindo da série. Exemplo: gergelim 2024/25, 399,4 mil t na série de setembro/2026 e 610,9 no 12º levantamento de 2025/26.
- **Docs conferidas contra a fonte** — as docs passam a dizer: o ponto fixo por UF da NASA POWER, que não é o centroide; a soma de todos os satélites nas queimadas e o horário das passagens do AQUA; o PRODES do bioma, não da Amazônia Legal; o ajuste do último dia do BGI, que não é a liquidação; a UF do laticínio, o sigilo e as UFs ausentes no `leite_industrial`; o Censo 2017 × Total com sigilo e × PAM/PPM; o 1995 com números diferentes nos dois contratos do Censo; a área mapeada do `uso_do_solo`; `indicador_id` e `indicador` no IMEA; `"mensal"`, `data_inicio` e `data_fim` no resumo da UNICA; o lado Brasil do Comtrade × ComexStat e a perna vazia do espelho; o `MEDIA_ESTADOS`, que não é a média simples das UFs; o dia válido da chuva do INMET; a irrigação e as recusas do `custo_producao`; e a consulta por safra × soma das mensais no `credito_rural_total`. Só doc, PT e EN.

- **Lista Suja — reconciliação** — corpos integrais CSV/TXT/PDF e oráculos das doze colunas de 579 registros, com replays públicos, filtros e proveniência; datas compostas, nulos e diferenças de quebra de linha preservados, sem alterar parser 4 ou contrato 2.0.

- **SICAR — reconciliação tabular** — seis seleções completas, incluindo 21.006 feições do DF em três páginas, com oráculos de todas as células e inventário dos 27 XSDs; duas páginas históricas verificam a seleção de versões, com limites de proveniência explícitos e sem alterar parser 2 ou contrato 2.0.

- **ANTAQ — reconciliação offline** — inventário nominal dos 62 campos publicados de atracação, carga e mercadoria, com oráculo independente dos joins `IDAtracacao`/`CDMercadoria`, cardinalidade 1:N, toneladas, rótulo de período e os seis filtros públicos conferidos célula a célula na fonte e no dataset agregado; o ZIP oficial segue indisponível desde 23/06/2026 e a captura live continua como pendência nominal, contrato 1.0 inalterado

- **ICMBio — reconciliação tabular** — captura integral de 347 UCs e dois filtros sobrepostos, com oráculos de todas as células, proveniência e inventário das 22 propriedades XSD; contagem documental datada e variável, sem alterar parser 3 ou contrato 1.0.

- **Agrofit — reconciliação** — corpos integrais e oráculos independentes para 4.403 formulados, 279.707 autorizações, campos diretos de 2.992 técnicos e 57 componentes de 32 coortes explícitas; preservação de multiplicidade, unidades, nulos, zeros e expressões ambíguas conferida nas APIs e cache, sem alterar parser 3 ou contratos 1.1/1.0.

- **RNC/SNPC — reconciliação** — oráculo independente de todas as células dos 43.749 registros capturados, com replays HTTP completos de fonte, datasets e cache; contagens da mesma sessão, datas condicionais, campos vazios e identificadores secundários repetidos verificados, sem alterar parser 2 ou contratos 1.0.

- **Testes** — inventário AST reproduzível de comparações rasas, fontes sem golden e mocks do alvo; removidas cópias literais de constantes e acrescentado golden reduzido do Acervo Fundiário com oráculo DBF independente.

- **ZARC** — tábua validada em DuckDB próprio, com TTL de 24 h e três revisões; consultas repetidas aplicam filtros SQL sem novo download ou parse, inclusive em outro processo. Aquisição/SHA e metadados preservados; falhas no cache seguem pelo fluxo normal.

- **CONAB custos** — catálogo completo em cache por 1 h no processo; chamadas repetidas evitam sete requisições. `use_cache=False` força leitura sem atualizar o cache; aquisição original e estado do cache ficam nos metadados. Planilhas continuam sendo baixadas por consulta.

- **ZARC** — culturas fora do catálogo falham antes de acessar a rede, com sugestões de nomes semelhantes quando disponíveis; aliases e orientações de safra preservados.

- **IBGE SIDRA** — respostas 403 de challenge do Cloudflare passam a informar o bloqueio e orientar a redução da taxa de consultas; permanecem sem retry automático, preservando as tentativas das demais falhas transitórias.
- **Downloads e helpers** — wrappers do MAPA PSR reutilizam o stream com orçamento; ZARC aplica tetos durante a leitura. Estatísticas WFS resolvem nomes e aliases sem transformar campos desconhecidos em nulos; transporte preserva status HTTP sem escrever atributos privados do HTTPX. Cache RNC deixa de depender de um módulo de encaminhamento, e caminhos Agrofit são definidos em um único helper.
- **Helpers compartilhados** — FUNAI e INCRA reutilizam coleta, seleção espacial, limites de memória e proveniência WFS, mantendo ordenação e filtros próprios. Desmatamento delega os números JSON ao helper comum, com equivalência de zero explícita; gravações atômicas compartilham retry de `PermissionError`, inclusive na variante assíncrona do Acervo.
- **ANP preços** — aquisição e preparação da consulta ficam disponíveis na fonte para a camada semântica. Validação semanal precede os filtros, e cada saída mensal ou de dataset é validada na sua fronteira; conversão usa o finalizador compartilhado.

- **CONAB safras** — página e planilha usam HTTP direto primeiro, com navegador opcional como fallback; os metadados identificam o transporte efetivo do XLSX
- **ANA tabular** — consultas JSON solicitam apenas atributos, preservando valores, filtros e o comportamento das funções geográficas; documentação informa que o mapeamento atual de pivôs é de 2014
- **ANTT conexões** — uma aquisição reutiliza a sessão HTTP nos catálogos e CSVs, preservando rate limit, revisão dos recursos e proveniência; sessões são isoladas por consulta e fechadas em erro ou cancelamento, sem cache persistente adicional
- **ANTT** — seleção CKAN por ano, frequência e revisão preserva recursos, hashes, bytes e estatísticas da aquisição; metadados identificam o primeiro CSV de tráfego e o manifesto completo. Redirecionamentos são rejeitados, e recursos anuais são integralmente validados antes do resultado filtrado
- **mapa_psr** — download sequencial em arquivo temporário e parsing por blocos com filtros antecipados reduzem o pico de memória em consultas seletivas; CSV truncado não devolve resultado parcial, e esquema, valores e ausências são preservados
- **CEPEA sanity** — unidade e faixa passam a cobrir os 22 identificadores, incluindo os 13 antes sem regra; faixas novas usam referências oficiais, com calibração parcial de citros documentada e sem impor variação diária a leite mensal ou etanol semanal
- **anp_diesel** — planilhas de preços municipais usam calamine como engine primária, com openpyxl como fallback, e os períodos necessários são baixados e processados concorrentemente com limite de três operações

- **WFS** — desmatamento migra para o transporte compartilhado com FUNAI, INCRA e Embrapa Solos; limites de bytes interrompem a leitura em blocos, com hash/tamanho parcial e encerramento da resposta. `geo.fetch_wfs` e o transporte usam o mesmo detector de erros HTML/XML/ArcGIS, preservando prefixos XML e envelopes JSON formatados sem decodificar novamente páginas normais de feições
- **BCB SGS — `data_fim` da TR** — séries que publicam `dataFim` (a TR, código 226, traz o fim do período de cada taxa) ganham a coluna opcional `data_fim` (`datetime64[ns]`) depois das quatro estáveis; a 1.1 descartava o campo. Só campo desconhecido gera aviso. Contrato `bcb_sgs` 2.0 → 2.1 (também em `series_economicas`), com schema JSON e docs PT/EN; o quadro vazio do contrato declara `data_fim`.
- **SFB, consulta tabular sem geometria e paginação por chave** — `cnfp()`, `concessoes()` e `ifn_conglomerados()` pedem `returnGeometry=false` (1ª página do CNFP nacional: 378 MB → 0,5 MB, 38,6 s → 0,1 s); CNFP e concessões paginam por `fid` (`fid > último`, `orderByFields=fid`), como o ANA, em vez de offset sem ordem.
- **`import agrobr` sem criar o contexto SSL do SICAR** — o contexto nascia na importação: custava ~260 ms e, com `SSL_CERT_FILE` inválido, derrubava o `import agrobr` inteiro, até para uso sem rede. Agora nasce na 1ª consulta ao SICAR, com cache no módulo, e o erro cita a variável.
- **Cache degradado avisa** — sem escrita na pasta, com disco cheio ou com o arquivo em uso, o agrobr seguia sem cache, e o único sinal era o log `cache_degraded`. Agora avisa uma vez (`UserWarning`) com o caminho, o motivo e a dica `AGROBR_CACHE_CACHE_DIR`.
- **Rebanho bovino, USDA × PPM** — a doc do `usda` e a do `pecuaria_municipal` (PT/EN) dizem que o `Beginning Stocks` do USDA e o efetivo da PPM do IBGE não são a mesma medida, com a ordem de grandeza (186,9 × 238,2 milhões de cabeças).
- **Licença da fonte no `MetaInfo` e tabela única** — a classificação de cada fonte (`livre`, `nc`, `zona_cinza`, `restrito`) vivia só no texto dos avisos e no `DatasetInfo.license`. Agora há uma tabela única, `constants.LICENCAS`, conferida por teste contra a `docs/licenses.md` (PT/EN); o `MetaInfo.license` diz a classificação do dado (a mais restritiva entre as `data_sources`, senão a da fonte selecionada), e o `datasets.info()` traz `licenses` por fonte: o `exportacao` diz `livre` (ComexStat) e `zona_cinza` (ABIOVE, no fallback).
- **Tipos por `return_meta` nos datasets e nas fontes** — os 51 datasets sem `@overload` e 17 funções de 8 fontes (INMET, ANP, ANTT, PSR, NASA POWER, INCRA, IBGE legado e o catálogo de sociobiodiversidade da CONAB) declaravam `DataFrame | tuple[DataFrame, MetaInfo]` em qualquer modo, e o padrão da doc (`df, meta = await datasets.preco_diario(..., return_meta=True)` e o `df.head()` da chamada simples) falhava no mypy `--strict` e no pyright. Agora os 2 `@overload` por `return_meta` dão `pd.DataFrame` e `tuple[pd.DataFrame, MetaInfo]`, e um teste pega o dataset ou a fonte nova sem eles. O `agrobr.sync` segue sem tipos (o espelho é dinâmico), e a doc diz isso. O `ano` do `producao_anual` declara `int | list[int] | None`, como já aceitava.
- **`cepea.indicador` sem `as_polars` declara só pandas** — antes, `pd.DataFrame | pl.DataFrame` também no modo pandas. Com `as_polars`, segue `pd.DataFrame | pl.DataFrame`. Nas outras funções, `as_polars=True` ainda sai tipado como pandas; o `@overload` por `as_polars` fica para depois da 2.0.
- **Página "O que o agrobr grava no disco"** — `docs/advanced/disco.md` (PT/EN) lista cada arquivo que o agrobr grava (o `agrobr.duckdb` do CEPEA, o store do ZARC, os PDFs da ANEC, os ZIPs do Acervo Fundiário, os pacotes do RNC e do Agrofit e os snapshots), com o caminho, a validade, o tamanho e como limpar, e o que fica só em memória. O troubleshooting e as páginas da ANEC, do Acervo, do ZARC, do RNC e do Agrofit apontam para ela. O comando `agrobr cache clear` fica para depois da 2.0.
- **CLI, a descrição de cada comando** — os 19 comandos ganham 1 linha no `--help` que diz o que sai (antes, só os grupos tinham); a do `health` diz que ele testa cada fonte e sai com 1 se alguma falhar, e a do `doctor`, que ele diagnostica o ambiente (fontes, cache local e próxima atualização); o grupo `ibge` cita os censos.

### Changed

- **MetaInfo** — `fetched_at`, `timestamp`, `cache_expires_at` e `fetch_timestamp` passam a UTC com fuso na construção e em atribuições; offsets preservam o instante, `from_dict()` aceita ISO sem fuso e `to_dict()` emite `+00:00`.
- **health** — o probe do IBGE passa a consultar a API de agregados (`servicodados`), o canal que a biblioteca efetivamente usa enquanto a SIDRA bloqueia clientes programáticos
- **Censo municipal 1985 — contrato 2.0, casa a casa** — os 53 temas voltam a partir dos 28 PDFs do IBGE (27 UFs; eram 22), num pacote Parquet local com 1 linha por casa, a chave `(volume, tabela, pagina_pdf, linha, coluna)` e o `status` de cada casa. `valor` só na casa confirmada pelas somas impressas (0 erro contra os oráculos cegos); `valor_lido` sempre; `coluna_nome` só quando ≥ 3 volumes leem o mesmo nome na mesma posição; a unidade pela marca da nota do volume. O tema segue o título impresso (19 nomes corrigidos). Proveniência pelo PDF do IBGE e o SHA-256 dele. Os CSVs antigos saem; o contrato 1.0 fica em `_legacy`; guia §81.

- **Contrato `progresso_safra` 1.0 → 1.1**: coluna opcional e nulável `revisado` informa a marca `*` da CONAB. Contrato histórico 1.0 preservado; schemas, metadados e docs PT/EN atualizados.
- **destinos_anec** — catálogo passa de 6 para 4 produtos, conforme as tabelas de importadores ANEC conferidas nas edições W13 e W34/2026: soja, farelo de soja, milho e trigo. DDGS e sorgo continuam anunciados em embarques_mensais_anec e comparacao_anual_anec; `ddgs`/`sorgo` passam a ser recusados em destinos antes da rede com `InvalidParameterError` (antes: DataFrame vazio sem aviso).
- **condicao_lavouras** — catálogo passa a anunciar as 8 culturas presentes no relatório semanal do DERAL; aveia, cana, canola, mandioca e totais milho/feijão não foram observados nas capturas analisadas (fevereiro e setembro de 2026). O parser mantém seus aliases.
- **Imports históricos de contratos** — política única de `DeprecationWarning` para imports nomeados supersedidos; históricos e aliases ficam fora de `__all__`. Schemas ativos independem de `_legacy`, preservando as 85 definições registradas. Os aliases CONAB Safra, IBGE LSPA e Censo Legado mantêm seu destino atual; contratos históricos mantêm seu schema original.

- **RNC/SNPC** — parser 2 valida a exportação inteira antes dos filtros, exige o layout completo e preserva identificadores textuais. Registradas mantém dez colunas; protegidas acrescenta `termino_protecao_texto`, conservando a condição publicada sem inventar uma data. Registro RNC e processo SNPC são as chaves; certificados repetidos permanecem. Novos contratos de fonte 1.0, filtros exatos por IDs e validação de parâmetros antes de I/O. Cache bruto versionado preserva aquisição UTC, hashes, contagem da pesquisa e TTL de 24 horas; `use_cache=False` ignora leitura e gravação. Contagem coincidente entre pesquisa e CSV é reportada sem garantir snapshot transacional.
- **MapBiomas municipal** — cobertura das coleções 10 e 11 ganha contrato próprio 1.0 de dez colunas, com `geocodigo` textual e `id_registro` publicado. Linhas com a mesma classificação territorial e IDs diferentes são preservadas; o ID vale dentro da coleção e do recurso. O leitor valida identidade e todos os anos antes dos filtros, preserva zeros e recusa áreas ausentes ou inválidas. Filtro exato por geocódigo complementa busca municipal literal. ZIP e XLSX têm hashes/tamanhos separados, aquisição UTC, fingerprint e estatísticas da população lida. `uso_do_solo` valida o contrato municipal, inclusive vazio, entrega Polars após validação e recusa combinações incompatíveis, parâmetros desconhecidos e contexto `deterministic` antes de I/O. Os contratos estaduais permanecem 1.0.
- **MapBiomas, classe fora da legenda** — o rótulo (`classe`, `classe_de`, `classe_para`) de um código fora da legenda conhecida sai nulo, com o `classe_id` publicado e aviso (`UserWarning` e `meta.validation_warnings`) com os códigos, nos recortes estadual e municipal. Antes, o estadual devolvia `Classe {id}` e o municipal recusava a planilha. Os contratos `mapbiomas_cobertura` e `mapbiomas_transicao` passam a 2.0 (`classe` anulável); os V1 ficam como históricos. Guia de migração, seção 63.
- **Datasets / proveniência** — `BaseDataset` passa a preservar hash/tamanho bruto, chave/expiração de cache e durações de aquisição/parsing da fonte selecionada. Sem metadados de origem, os defaults permanecem None/0. Esses campos descrevem o recurso e o trabalho da fonte; aquisição original, chamada atual, cascata e cópia independente de diagnósticos mantêm suas semânticas.
- **BCB PTAX 2.0** — seleção de moeda e boletim, catálogo dinâmico `ptax_moedas` 1.0 e paginação validada antes do filtro. Fechamento USD permanece como padrão; oito colunas preservam paridades, texto do boletim e precisão ns, com valores finitos/anuláveis e vazio estável. Rótulos de fechamento diferentes entre dia/período são reconhecidos sem reescrever o publicado. Datas parciais deixam de ser ignoradas; catálogo separa símbolo não suportado de vazio legítimo. Contratos, recursos/manifesto SHA256, coleta UTC e cobertura por stream explicitam seleção, unidade histórica e falta de total independente. Não há conversão monetária ou snapshot de revisão.
- **BCB Focus 2.0** — nova seleção mensal e preservação do detalhe anual em doze colunas com tipos estáveis, inclusive no vazio. Identidade inclui periodicidade, indicador, detalhe, pesquisa, referência e base. Paginação valida páginas inteiras, avança pela quantidade recebida e distingue limite local de completude desconhecida; duplicatas, conflitos de contagem e seleção inválida geram erro. Valores negativos são preservados; inconsistências estatísticas finitas recebem diagnóstico com origem. Novo contrato `bcb_focus`, recursos/manifesto SHA256 e coleta UTC. Anotações de contagem/continuação têm testes defensivos offline; não há total independente ou snapshot de revisão comprovado na sondagem.
- **BCB SGS 2.0** — intervalos longos são adquiridos por blocos de calendário e reconciliados antes de `ultimos`, com parâmetros validados antes da rede. Quatro colunas mantidas com data civil ns, valor float64 finito/anulável e código int64; referências mensais/trimestrais fora da janela diária são preservadas com diagnóstico. Duplicatas no corpo e conflitos entre blocos geram erro. Vazio oficial 404 conserva a limitação de não provar existência do código. Novo contrato `bcb_sgs`, recursos/manifesto SHA256, coleta UTC e cobertura distinguem blocos obtidos de completude desconhecida. Não há catálogo genérico de frequência/unidade nem snapshot de revisões.
- **UN Comtrade 2.0** — comércio, espelho e dataset com contagem independente, refinamento por período/HS e aviso de truncamento; `require_complete=True` rejeita cobertura não comprovada. World passa a agregado explícito e `all` mantém todos os parceiros publicados. Períodos mensais e listas são validados antes da rede, sem blocos guest com múltiplos períodos. Contratos de 24 colunas preservam códigos numéricos, revisão HS, flags e tipos em vazio; espelho exige junção 1:1 e revisões compatíveis. Recursos, manifesto SHA256, cobertura, tentativas e aquisição UTC atravessam API/dataset/Polars. Erros de bloco, dimensão ou revisão deixam de ser ignorados. Licença interna revista para `zona_cinza` conforme termos condicionais; autenticação homologada somente por replay nesta etapa.
- **Lista Suja 2.0** — CSV principal com descoberta na página oficial e TXT conferido integralmente para edição; PDF como alternativa em falhas de transporte elegíveis ou escolha explícita. Acrescenta ID da exportação, decisão, atualização do cadastro e texto integral da inclusão. Números usam `Int64`, ausências textuais ficam nulas e inclusão composta mantém texto sem escolher uma data. Contrato próprio, hash/URLs/aquisição UTC, notas e diagnósticos preservam a proveniência; formatos inválidos e linhas incompatíveis geram erro. Filtros `uf` e `id_registro` exato, formato exclusivo e argumentos desconhecidos são validados antes de rede/aviso. CSV funciona no core; CEAC e histórico de revisões permanecem fora desta API.
- **preco_diario / cepea.indicador 1.1** — colunas opcionais `valor_usd` (preço em dólar publicado na mesma linha pelo CEPEA, em todos os produtos que o divulgam) e `peso_medio_kg` (bezerro MS), sempre `float64` e nulas no fallback Notícias Agrícolas; `MetaInfo.contract_version` passa a `1.1`
- **cache** — migração 10 acrescenta `valor_usd` e `peso_medio_kg` às tabelas `indicadores` e `indicadores_quarentena` (quando existe), somente colunas faltantes, em passo idempotente antes da transação das demais migrações; registros anteriores ficam nulos até nova coleta
- **Agrofit / defensivos** — formulados, autorizações e técnicos passam ao schema 1.1: situação textual nos dois primeiros e composição original nos produtos. Técnicos corrigem ingredientes/grupos com parênteses internos ou múltiplos componentes. Registro exato preserva zeros, códigos alfanuméricos e acentos. Parâmetros inválidos ou desconhecidos falham antes de cache/rede. Bundles versionados preservam aquisição UTC, hash, tipos, layout e diagnósticos por coleta; `use_cache=False` ignora leitura e gravação. Licença documentada como Creative Commons Attribution, sem presumir uma versão ausente no portal.
- **clima 3.0** — agrega a rota `inmet_historico` entre a API INMET e NASA POWER por UF, e após a API no modo estação; `fonte` explícita é exclusiva. Temperaturas mensais passam a nullable; `lat`, `lon`, `agregacao_espacial` e `base_tempo` preservam a diferença entre estações UTC e ponto NASA LST. O client NASA solicita LST explicitamente. O diário por estação mantém contrato 1.0 e o horário recebe contrato próprio 1.0 com hora na chave; agregação mensal por estação e combinações ignoradas agora geram erro. Contratos históricos permanecem disponíveis.
- **INMET integridade e proveniência** — seleção histórica usa todos os membros pertinentes do arquivo, incluindo estações fora do catálogo atual de operantes; Pydantic valida identidade, metadados e observações. Falhas de HTTP, ZIP/CRC, conflito entre observações, identidade ou intervalo interrompem a coleta. Cache anual limitado a 256 MiB, com TTL de 1h para ano corrente e 24h para anos encerrados. `MetaInfo.source_details` preserva recursos/hash, cobertura, ausências, agregação e contexto temporal; o campo vazio mantém a serialização anterior. O contexto determinístico em clima seleciona o ano padrão, sem congelar revisões nem truncar observações.
- **cadastro_rural / SICAR 2.0** — encaminha `cod_municipio` e `atualizado_apos`, oferece Polars após validação e preserva os oito argumentos posicionais. Coleta tabular passa a GeoJSON de atributos, sem GeoPandas; contrato 2.0 mantém colunas/chave e exige datas UTC, inclusive nulos/vazios, evitando a diferença de horário entre CSV e limites CQL. O filtro de atualização aceita Z/offset e sua própria saída ISO, normaliza zeros adicionais para três casas de milissegundos e rejeita submilissegundos sem arredondar. No dataset, filtros inválidos ou desconhecidos falham antes da rede; o dataset rejeita `deterministic`, removendo a conversão incorreta de snapshot em criação posterior. A projeção solicita atualização nas 15 camadas com o campo e rejeita o filtro nas 12 sem suporte, conforme DescribeFeatureType oficial.
- **SICAR TLS** — verificação de certificado e hostname restaurada após sondagem das 27 camadas; contexto mantém a compatibilidade de cifras `SECLEVEL=1`, sem fallback para conexão sem verificação.
- **estimativa_safra 3.0** — contrato próprio com `ano_lspa` e `mes_lspa` e chave que preserva fonte e referência temporal; seletores nomeados `fonte`, `levantamento` e `mes` distinguem edições CONAB de meses LSPA, validam combinações antes da rede e impedem substituir referência explícita indisponível por outra. A normalização LSPA verifica componentes, unidades e localidades e preserva ausências; `CONAB_SAFRA_V2` e o schema da fonte CONAB permanecem 2.0.
- **estimativa_safra 3.1, unidade em cada linha** — `unidade_producao` (`mil_ton`) e `unidade_area` (`mil_ha`), opcionais no contrato, com os mesmos valores nas 2 fontes (CONAB e LSPA, que o dataset já convertia para mil) e os de `conab.brasil_total`. O `producao_anual` sai em t e ha.

- **INMET observacional** — falha de transporte em qualquer bloco do período interrompe a consulta, em vez de devolver somente os blocos obtidos; respostas válidas sem medições continuam vazias
- **Constantes de contrato** — nomes ativos: `CLIMA_V3`, `IBGE_PAM_V2`, `ANTT_PEDAGIO_FLUXO_V3` e `agrobr.contracts.conab_custos.CONAB_CUSTOS_V3`; consulte módulos e nomes históricos no mapa de imports do guia de migração
- **ANTT fluxo 3.0** — treze colunas preservam `categoria_eixo`, `tipo_cobranca` e `frequencia`; cobranças distintas não são somadas. `n_eixos` vem somente de contagem textual explícita: códigos tarifários e números isolados permanecem nulos. Categorias, espaços e tipos publicados são preservados; mensal e diário são seleções exclusivas, sem troca automática de frequência
- **LSPA 2.0** — mês sempre presente, dimensões SIDRA corrigidas, variável e unidade explícitas; chave por ano, mês, produto, localidade e variável
- **Censo legado 2.0** — categorias, variáveis e geografia passam a refletir os cabeçalhos oficiais; `nivel="uf"` retorna totais estaduais e a nova coluna `uf` integra a chave para distinguir municípios homônimos
- **CONAB série histórica** — `cana_industria` removida do catálogo e rejeitada antes da rede; as tabelas industriais não eram interpretadas pelo parser agrícola
- **PAM / produção anual 2.0** — unidades históricas e condição do café são identificadas por linha, sem conversão implícita; não se deve presumir toneladas, kg/ha ou reais para todo produto e período
- **Clima 3.0** — precipitação mensal pode ser nula quando não há medições; chuva por UF usa média dos acumulados por estação, não soma espacial
- **CONAB custo de produção 3.0** — 25 colunas preservam contexto de planilha/aba, referência de preços, linhas, itens, subtotais e totais; sem chave primária artificial. Safra e medidas ausentes permanecem nulas, receitas negativas são preservadas e bases CV/CT ficam separadas. Removido `tecnologia=`; a qualificação de saída depende do sistema publicado. Seleções ambíguas exigem `planilha`/`aba` explícitas
- **BCB** — removidos os aliases `cafe_arabica` e `cafe_conilon` do crédito rural; o SICOR publica `CAFÉ` sem essa distinção, acessível por `cafe`
- **Tipos contratuais** — inteiros fracionários, colunas de texto com valores não textuais e booleanos representados por números ou strings passam a ser rejeitados
- **ANDA** — o parâmetro `produto` aceita somente `"total"`; produtos específicos passam a ser rejeitados porque a fonte publica apenas entregas totais
- **Contratos estritos** — toda coluna `stable` precisa estar presente no DataFrame, inclusive quando aceita valores nulos
- **`conab.safras` 2.0** — `levantamento` e `data_publicacao` passam a opcionais e ficam nulos quando a fonte é o IBGE LSPA
- **`bcb.credito_rural` 2.0** — `agregacao` passa a usar `"uf"` por padrão, `"municipio"` é rejeitada (na 1.1.0 ela não agregava e devolvia os registros do SICOR, hoje no `agregacao="registro"`), `volume` foi removida e a saída ganha `agregacao` e `fonte`
- **DERAL** — o vocabulário de `produto` distingue as safras como `feijao_1`, `feijao_2`, `milho_1` e `milho_2`
- **CEPEA** — `indicador()`, `ultimo()` e `pracas()` levantam `InvalidParameterError` para produto, praça ou datas inválidos; antes, esses casos podiam retornar DataFrame vazio ou erros não normalizados
- **Datasets** — `InvalidParameterError` interrompe imediatamente a cascata de fontes, em vez de ser tratado como indisponibilidade elegível para fallback
- **Polars opcional** — `as_polars=True` sem Polars instalado levanta `ImportError`, em vez de devolver pandas silenciosamente
- **Módulos removidos** — `quality`, `sla`, `export`, `plugins`, `validators.semantic` e `validators.validate_safra` foram eliminados por não terem consumidores internos nem documentação pública
- **Configuração** — `agrobr.configure()` foi removida; estava deprecada desde a 1.1.0 e nunca controlou fetch, cache ou fallback
- **Novos avisos** — a primeira chamada ao CEPEA informa a licença dos dados, e `SourceFallbackWarning` sinaliza quando um dataset devolve uma fonte alternativa

- **Acervo TLS** — validação de certificado restaurada nos downloads e no health após confirmação dos endpoints oficiais no Windows e Linux; removida a exceção de TLS que deixou de ser necessária
- **ci** — lint passa a incluir scripts e exemplos; deploy da documentação usa o mesmo modo estrito do gate de qualidade
- **limpeza** — removido o helper privado `_empty_legacy_df`, sem consumidores; catálogos e funções públicas usados dinamicamente foram preservados
- **CEPEA leite** — Notícias Agrícolas deixa de participar do fallback CEPEA para evitar misturar fechamento e mês de referência; o módulo NA autônomo continua disponível com sua semântica de fechamento
- **ci** — publicação depende do workflow reutilizável de qualidade do mesmo commit, incluindo documentação estrita; build, smoke e publicação usam o mesmo artefato por ID, com falha em divergência de digest
- **tests live** — indisponibilidades inesperadas falham; USDA sem chave e indisponibilidade tipada no arquivo ANTAQ de 2024 (HTTP 522 ou aviso oficial) têm exceções explícitas. Zero datasets validados falha; JSON registra cobertura e motivos, além do JUnit
- **testes** — `pytest-socket` no extra dev bloqueia rede por padrão; apenas testes marcados `integration` habilitam sockets de rede. O par local do `asyncio` permanece disponível no Windows, com gate nativo para execução assíncrona e bloqueio de TCP/UDP/DNS. CI testa também o parsing decimal e os controles numéricos da calculadora com Node 24
- **segurança de dependências** — mínimos de HTTPX 0.28.1, httpcore 1.0.9, lxml 6.1.0, requests 2.33.0 e GeoPandas 1.1.4 excluem versões com avisos de segurança conhecidos; httpcore já era transitivo e recebe limite explícito para exigir h11 corrigido; CI atualiza as ferramentas de instalação antes dos gates
- **dependências** — mínimos de pandas 2.2.2, Typer 0.26.0, pdfplumber 0.11.10 e pyogrio 0.8.0 excluem falhas de import, CLI e extração numérica; SIDRA usa HTTP assíncrono direto e deixa de depender de sidrapy
- **dependências, piso do `soupsieve`** — o BeautifulSoup 4.12.0, piso do agrobr, aceita `soupsieve>1.2`, e do 1.2.1 ao 1.6 o `soupsieve` não importa em Python 3.10 ou superior: o BeautifulSoup desliga os seletores CSS e o `select` da CONAB levanta `NotImplementedError`. O core passa a declarar `soupsieve>=1.6.1`, que já vinha com o BeautifulSoup; com ele, as 12 famílias que usam BeautifulSoup dão o mesmo resultado no 4.12.0 e no 4.15.0.
- **ci** — perfil mínimo fixa dependências core e extras PDF, geo e Polars com NumPy 2; instalação somente core e teste do wheel fora dos imports do checkout cobrem Python 3.11–3.13
- **dependências** — DuckDB mínimo passa a 1.5.2 para excluir regressões de alteração de tabelas indexadas nas versões 1.5.0 e 1.5.1, incluindo bancos legados com colunas faltantes
- **ci** — compatibilidade de cache, CEPEA e datasets passa a ser verificada também com DuckDB 1.5.2 e mypy 1.19.0, com Polars instalado
- **proveniência** — `MetaInfo.data_sources` identifica as fontes dos registros CEPEA retornados, separadamente de `selected_source="cache"`; parsers CEPEA e Notícias Agrícolas passam às versões 2 e 3 para distinguir novas observações do legado
- **contracts** — `required_columns` dos schemas JSON lista todas as colunas estáveis; `schema_version` acompanha a versão do contrato nos metadados dos datasets e de `conab.safras`; custo de produção usa contrato 3.0, e clima mensal 3.1; produção anual usa 2.1
- **polars** — o extra passa a incluir `pyarrow` para conversão de colunas pandas com tipos anuláveis; erros de dependências da conversão preservam a mensagem original
- **config** — `agrobr.configure()` removida; deprecada na 1.1.0, a função nunca teve efeito sobre fetch, cache ou fallback de fontes
- **limpeza** — os módulos `quality`, `sla`, `export`, `plugins` e `validators.semantic`, nunca documentados, nunca exportados em `agrobr` e sem uso interno, foram removidos; `validators.validate_safra` e `SAFRA_RULES`, também sem consumidores de produção, foram eliminados junto com seus testes autocontidos
- **bcb.credito_rural** — contrato atualizado de 1.1 para 2.0 e alinhado às saídas reais do SICOR: agregação padrão por UF, opção por programa, 11 colunas documentadas e remoção de `volume`; as 12 dimensões da chamada padrão da 1.1.0 (mês, região, subprograma, fonte de recursos, tipo de seguro, modalidade e atividade) saem das agregações `uf` e `programa` e ficam no `agregacao="registro"`; `agregacao="municipio"` agora é rejeitada com orientação para o extra `agrobr[bigquery]`
- **deral** — `produto` preserva a distinção entre primeira e segunda safra de feijão e milho (`feijao_1`, `feijao_2`, `milho_1`, `milho_2`), eliminando combinações ambíguas na chave primária
- **estimativa_safra** — contrato `conab.safras` atualizado de 1.0 para 2.0: `levantamento` e `data_publicacao` passam a opcionais e ficam nulos quando a fonte é o IBGE LSPA
- **anda / fertilizante (breaking)** — `produto` não é mais copiado para os registros como se tivesse filtrado o PDF. Os boletins da ANDA publicam apenas entregas totais: `produto="total"` permanece aceito e qualquer produto específico agora levanta `ValueError` antes do download. O parser grava `produto_fertilizante="total"` por construção, e o contrato do dataset passa a `2.0`; antes, por exemplo, o volume total podia ser devolvido falsamente rotulado como `ureia`
- **packaging** — o sdist contém apenas o pacote, README, LICENSE e CHANGELOG; o extra `all` contém apenas integrações opcionais de runtime, sem `dev`/`docs`, e o extra `app` sem código correspondente foi removido
- **ci** — o `pip-audit` não ignora mais uma vulnerabilidade já corrigida do pip, e os nomes dos artefatos de health e integração incluem `github.run_attempt` para não colidirem em re-runs
- **contracts** — toda coluna `stable` passa a ser obrigatória no DataFrame, mesmo quando `nullable`; `nullable` governa apenas os valores nulos. Chaves primárias incompletas agora geram erro próprio antes da validação de duplicatas
- **cepea** — `indicador()` e `ultimo()` emitem aviso de licença na primeira chamada: dados sob CC BY-NC 4.0, com autorização do CEPEA necessária para uso comercial. O fallback automático Notícias Agrícolas foi mantido com seu aviso próprio de fonte `restrito`
- **ci** — actions JavaScript atualizadas para releases compatíveis com Node 24 e `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` habilitado em todos os workflows. A integração semanal agora instala Playwright/Chromium e exercita a CONAB ao vivo
- **health** — os probes de ICMBio, SFB e ANA agora exercitam consultas reais (`GetFeature` ou contagem ArcGIS) e inspecionam o corpo de respostas HTTP 200 em busca de erros da fonte. Antes, `GetCapabilities` e diretórios ArcGIS podiam ficar verdes enquanto as operações de dados estavam quebradas
- **health** — alertas passam a disparar apenas no **cruzamento** dos limiares (`consecutive_failures_warning`, `consecutive_failures_critical`), não em todo run acima deles. Antes, uma fonte fora do ar por semanas repetia o mesmo alerta a cada execução
- **acervo_fundiario** — probe de health passa a bater em um ZIP real via `HEAD`, com a mesma validação de certificado do client, e sobe para `tier: best_effort`: a indisponibilidade esperada nos runners do GitHub Actions vira `warning` em vez de derrubar o workflow
- **testes** — os 5 testes live do `acervo_fundiario` ganharam o marker `integration_br` (a fonte não responde a partir dos runners do GitHub Actions). O CI roda `-m "integration and not integration_br"`; localmente basta `-m integration_br` para exercitá-los
- **ci** — o workflow semanal de integração reusa uma única issue com a label `integration-tests` (atualiza, e fecha sozinha quando os testes voltam a passar) em vez de abrir uma issue nova a cada tentativa. Re-runs vinham gerando issues duplicadas toda semana
- **docs** — ANTAQ marcada como fonte indisponível (desde 23/06/2026) no README PT/EN, nos índices de fontes e contratos e nas páginas da fonte, com link para o aviso oficial; nota sobre a restrição de rede do INCRA em `acervo_fundiario`; link de licença do MapBiomas atualizado para o FAQ (a página `/termos-de-uso/` saiu do ar na reformulação do site), declarando CC BY 4.0
- **INCRA (breaking)** — `quilombolas()`/`quilombolas_geo()` leem a camada em WFS 2.0.0/JSON e devolvem 22 colunas (contrato `incra_quilombolas` 2.0): as 10 anteriores seguidas de 12 atributos da fonte; datas passam a literal XSD em texto (antes `datetime64`), `codigo`/`familias` `Int64`, `bbox` em EPSG:4326 e geometria sem reparo topológico (o `make_valid` saiu). Veja o guia de migração, seção 25
- **IBGE PPM** — a espécie da categoria 32793 da tabela 3939 passa a se chamar `galinhas`, como no rótulo oficial "Galináceos - galinhas", que inclui poedeiras e matrizeiras (notas técnicas da PPM). `galinhas_poedeiras` continua aceito em `ibge.ppm` e `datasets.pecuaria_municipal` como alias com `FutureWarning`; os números não mudam, mas a coluna `especie` sai `"galinhas"` também pelo alias
- **BCB SGS** — o alias da série 7460 passa a se chamar `ipa_agricola`: o nome oficial é "Índice de Preços ao Produtor Amplo por Origem - Disponibilidade Interna - Produtos agrícolas" (sem os pecuários). `ipa_agropecuario` segue aceito com `FutureWarning` e sai como `nome_serie="ipa_agricola"`; guia de migração §31.
- **CONAB, safra passada** — `conab.safras(produto, safra=X)` sem `levantamento` lê X da publicação mais recente que a traz: para a safra anterior à corrente, o último levantamento da safra seguinte, que a CONAB revisa (gergelim MT 2024/25: 695 mil ha no 12º levantamento de 2025/26, 401,2 no 12º de 2024/25). `estimativa_safra`, `producao_anual` e `brasil_total(safra=X)` seguem a regra; `balanco` não muda. `levantamento` fixa a edição da própria safra e avisa quando há publicação mais recente; `MetaInfo.source_details["publicacao"]` registra a publicação usada
- **ANTT, código legado removido** — `parser.parse_trafego`, `parse_trafego_v1`, `parse_trafego_v2`, `join_fluxo_pracas`, `heavy_vehicle_mask`, `client.download_csv` e as constantes `CATEGORIA_MAP`, `EIXOS_TIPO_MAP`, `COLUNAS_FLUXO`, `COLUNAS_V2` e `ANO_INICIO_V2` não tinham uso na produção nem nas docs; use `fluxo_pedagio()` ou `parser.parse_trafego_file()`
- **Comex Stat, aliases pelo produto inteiro** — cada alias de `comexstat.exportacao`/`importacao` e cada produto de `datasets.exportacao`/`importacao` soma todos os códigos NCM do produto: `milho` = `1005`, `arroz` = `1006`, `trigo` = `1001`, `algodao` = `5201` + `5203`, `cafe` = `0901.1` + `0901.2`, `acucar` = `1701`, `etanol` = `2207`, `carne_bovina` = `0201` + `0202`, `carne_frango` = `0207.1`, `carne_suina` = `0203`, `farelo_soja` = `2304` (antes só `23040010`, 22 % das exportações de 2025; o fallback ABIOVE já usava o agregado), `ureia` = `310210` e `kcl` = `310420`; `npk` passa a ser só `31052000` (antes a posição `3105` inteira, com MAP e DAP: 6,1× o NPK importado em 2025). Os datasets consolidam os códigos de cada produto e deixam de devolver a coluna `ncm`; `MetaInfo.source_details["query"]` troca `ncm_prefixo` por `ncm_prefixos` e `ncm_excluidos`. Tabela "entra / não entra" na doc da API; antes e depois de cada alias no guia de migração §28.
- **Comex Stat, espécies de café** — `cafe_arabica` e `cafe_conilon` saem: a NCM não separa espécie (`09011110` é café em grão das duas espécies e saía rotulado como arábica; `cafe_conilon` entregava `09011190`, outras apresentações). A chamada levanta `InvalidParameterError` com a explicação; use `cafe` ou o código NCM.
- **Comex Stat, defensivos** — `defensivos` e `agrotoxicos` excluem os 27 códigos da posição `3808` que a nomenclatura define como de uso exclusivamente domissanitário (ex.: `38089119`, `38089419`): em jun/2025, 2,98 mi kg e US$ 8,3 mi das importações.
- **Embrapa Solos, `perfis` e `mapa_solos` no contrato 2.0** — `perfis` publica 85 colunas (as 19 da 1.x, os demais atributos publicados, `uf_original` e `feature_id`), uma linha por horizonte ou camada; as 9 medidas laboratoriais passam de `float` (a 1.x convertia com `errors="coerce"` e anulava em silêncio `<1`, `0,19` e marcas) ao texto publicado, inclusive `NULL`; `mapa_solos` ganha `ordem3`, `subordem3`, `gdegrupo3` e `feature_id`; `max_registros` e `tamanho_pagina` viram parâmetros (guia de migração §32)
- **SICOR / credito_rural — `produto` publica a chave pedida** — saía a grafia da fonte (`algodão`, `café`, `cana-de-açucar`, `mandioca (aipim, macaxeira)`), e o join por `produto` com `producao_anual` e `estimativa_safra`, que publicam a chave, falhava. Agora sai a chave sem acento e em minúsculas (`algodao`, `cafe`, `cana`, `mandioca`), igual pelo OData e pelo BigQuery; o filtro segue com a grafia oficial e o lookup usa a mesma chave, então `'"cafe"'` deixa de ir à fonte como `"CAFE"`; guia de migração §33.
- **FUNAI, `terras_indigenas` no contrato 2.0** — 19 colunas (as 9 da 1.x e os demais atributos publicados); `data_atualizacao` passa de `datetime64` (a 1.x usava `pd.to_datetime` sem `dayfirst` e trocava dia e mês até o dia 12) ao texto `dd/mm/aaaa` publicado; `uf` é o texto publicado, com várias UFs nas TIs interestaduais; `max_registros` e `tamanho_pagina` viram parâmetros (guia de migração §34)
- **IBAMA, `embargos_geo(bbox=...)` pela interseção do polígono** — o filtro usava o ponto de referência do termo (`NUM_LATITUDE_TAD`/`NUM_LONGITUDE_TAD`) antes de ler o WKT: no bbox `(-56, -16, -54, -14)`, 9 dos 36 polígonos devolvidos não tocavam a caixa e 4 dos 31 que a cruzam ficavam de fora; 2.589 polígonos sem ponto nunca saíam numa consulta por bbox. Agora `embargos_geo` filtra pela interseção do polígono (WKT vetorizado, ~2 s para o Brasil inteiro); `embargos` (tabular) segue pelo ponto; guia de migração §36.
- **IMEA, identidade do indicador** — `imea.cotacoes` publica `indicador_id` (texto, como a fonte) e `indicador` (nome oficial, lido de `v2/mobile/cadeias/{id}/indicadores`, URL em `meta.source_details`), em 10 colunas de ordem fixa: sem o indicador, milhares de linhas repetiam localidade, data, safra e unidade, e `unidade="R$/sc"` misturava o grão (saca de 60 kg) com a semente de soja (saca de 40 kg). `cadeia` passa a ser a cadeia pedida (o frete de grãos vinha como `soja` numa consulta de milho). Argumento desconhecido levanta `TypeError` antes da rede.
- **Argumento desconhecido vira `TypeError` em 38 funções públicas** — `abiove.exportacao`, as 6 do `acervo_fundiario`, `sicar.imoveis`/`imoveis_geo`/`resumo`, as 8 da `ana`, `anda.entregas`, as 4 da `b3`, `conab.ceasa.precos`, `conab.progresso.progresso_safra`, `deral.condicao_lavouras` e o dataset `condicao_lavouras`, `ibama.embargos`/`embargos_geo`, `icmbio.ucs_geo`, as 3 do `inmet`, `mapbiomas_alerta.alertas`/`alertas_geo`, `queimadas.focos`/`focos_geo` e `usda.psd` aceitavam qualquer argumento nomeado e o descartavam em silêncio (`ibama.embargos(municipio="X")` devolvia o Brasil inteiro); agora o argumento fora da assinatura levanta `TypeError` antes da rede.
- **CEASA/PROHORT, categoria oficial do produto** — `categoria` de `conab.ceasa_precos` e do `preco_atacado` passa a seguir os grupos do painel oficial "Hortaliças e Frutas" do PROHORT: **OVOS saiu de HORTALICAS** e ganhou a categoria própria `OVOS` (`ceasa_categorias()` com 3 chaves), e COCO VERDE passou de HORTALICAS para FRUTAS; brócolo, cará, couve, jiló, mandioquinha, quiabo e vagem, sem grupo no painel, seguem em HORTALICAS, declarados como classificação do agrobr. Produto fora da tabela deixa de virar HORTALICAS em silêncio: sai com `categoria` nula e um aviso único com os nomes (contrato `preco_atacado` 1.1)
- **SFB, `ano_criacao` em `Int64` e argumento desconhecido** — `ano_criacao` passa de `float64` (CNFP) e `int64` (concessões) a `Int64`, nulo quando a data publicada não tem um único ano; as 6 funções públicas levantam `TypeError` para argumento desconhecido antes da rede.
- **UNICA, resumo com o período da edição** — `safra_resumo` ganha `data_inicio` e `data_fim` (lidas do título de cada tabela) e aceita `periodo="mensal"`; pedir um período que a edição corrente não publica levanta `InvalidParameterError` com os períodos da edição, em vez de tabela vazia.
- **ANDA, proveniência do PDF** — `anda.entregas` e o dataset `fertilizante` passam a pôr em `MetaInfo.source_url` o PDF concreto usado (antes, a página de recursos), com `raw_content_hash` = SHA-256 do PDF e `source_details["pdf"]` = URL, rótulo do catálogo, edição impressa ("Janeiro a Junho" em ano parcial, "Total do Ano" em ano fechado), SHA, tamanho e página de recursos
- **Datasets: argumento desconhecido vira `TypeError`** — 25 datasets repassavam argumento desconhecido ao fetcher, que lia só as chaves conhecidas: `datasets.progresso_safra("soja", uf="MT")` saía com o Brasil inteiro, e 14 outros (entre eles `producao_anual`, `serie_historica_safra`, `credito_rural` e `queimadas`) ampliavam o recorte do mesmo jeito. Agora o `fetch` e a função pública de cada dataset têm assinatura explícita, e o argumento fora dela levanta `TypeError` antes da rede, como já faziam os demais. `cadastro_rural`, `clima`, `exportacao`, `importacao`, `comercio_internacional` e `uso_do_solo` passam de `InvalidParameterError` a `TypeError` nesse caso. O `ano` dos censos e o `semana_url` do `progresso_safra`, aceitos antes só pelo repasse, viram parâmetros nomeados; as funções públicas ganham `as_polars` explícito.
- **Censo Agropecuário, linha Total** — `ibge.censo_agro` e `datasets.censo_agropecuario` descartavam a categoria "Total" de toda classificação. Para medidas não aditivas, como número de estabelecimentos, o total oficial não se recompõe somando as categorias (irrigação, Brasília 2017: 2.726 no Total × 3.224 somando os métodos). Agora a linha Total sai como a fonte publica (`categoria="Total"`), com parser 3 do censo SIDRA atual e contrato `censo_agropecuario` 1.1, que diz que ela não se soma às demais e que estabelecimentos não somam entre categorias.
- **ABIOVE, edição mais recente** — `abiove.exportacao` e o fallback ABIOVE de `datasets.exportacao` leem a edição mais recente da planilha que publica o ano pedido (a do ano seguinte traz o ano como comparação), em vez de escolher o arquivo por `ano`/`mes`: em setembro/2026, `exp_202608.xlsx` revê 19 das 96 células de 2025 (farelo, dez/2025: 1.990.304,323 t, e não 2.020.365,023 t de `exp_202512.xlsx`), e `ano=2026, mes=1` traz janeiro em vez de `SourceUnavailableError`. `mes` só filtra os dados e é validado antes da rede; `edicao="AAAA-MM"` pede uma edição específica; falha que não seja 404 na edição mais recente não cai para uma anterior. `MetaInfo.source_details["edicao"]` registra arquivo e mês, e `raw_content_hash` o SHA-256 da planilha
- **Comtrade, aliases com o significado da ComexStat** — `suco_laranja` passa de `2009` (sucos de todas as frutas: +US$ 357 mi, +11,4 %, em 2025) para `200911`/`200912`/`200919`; `carne_frango` de `0207` (aves em geral) para `020711`–`020714`, com `InvalidParameterError` antes de 1996 (a HS 1992 não separa a galinha fresca e o `020741` virou pato na HS 2012) e em 1996 no Brasil, que reportou na HS 1992 (a guarda segue a classificação reportada, da disponibilidade oficial, e dá a dica de `020721`/`020741`); `cafe` sem cascas e sucedâneos (`090111`/`12`/`21`/`22`); `algodao` com `5203`; `soja` sem a semente (`120190`, e `120100` até 2011) e `complexo_soja` como soma das partes. Descrições conferidas nas referências oficiais H0–H6 da Comtrade; `celulose` (`4703`) e `tabaco` (`2401`) ficam e a doc diz o que sai. `scripts/reconciliar_comercio.py` confere HS de 6 dígitos
- **CONAB progresso, a linha "N estados" deixa de ser `BR`** — em `conab.progresso_safra` e `datasets.progresso_safra` (contrato 2.0, `parser_version` 3), a última linha de cada bloco ("12 estados" etc.) sai com `estado = "MEDIA_ESTADOS"`: é a média da própria CONAB dos estados monitorados (88% a 99,9% da área), não o Brasil. As colunas opcionais `n_estados` e `cobertura_area_pct` vêm da nota "(Esses N estados correspondem a X% da área cultivada)", sem recálculo; `estado="BR"` sem linha "Brasil" levanta `InvalidParameterError` com a cobertura publicada. A doc passa a listar culturas × operações conferidas nos boletins (algodão e milho 2ª também com colheita; trigo só com colheita), a base da colheita sobre o semeado acumulado e a origem dos percentuais por UF (no PR, o DERAL da segunda-feira anterior)

- **CONAB custos, itens pelo subtotal publicado e categoria pela seção** — em `conab.custo_producao`, `conab.custo_producao_total` e `datasets.custo_producao` (parser 5, contrato 3.0 mantido), o cabeçalho romano com zeros abre a seção; a linha depois do subtotal ou do total da seção (o bloco "Gestão da propriedade familiar" depois do H) sai das observações como nota; em cada seção vale a leitura de grupo valorado, subitens ou agregado de gestão que fecha com o subtotal publicado (café em L.Eduardo-BA 2003–2012, arroz em Massaranduba-SC e Meleiro-SC), e o subtotal ou total de fórmula que não fecha na própria planilha gera aviso com os números publicados (45 nas 11 séries conferidas). `categoria` sai da seção publicada e, no custeio, do rótulo com acento, hífen e plural normalizados: mão de obra, agrotóxicos, mudas e máquinas próprias deixam de mudar de categoria com a grafia do ano. A doc do contrato lista as 116 abas identificadas e recusadas
- **Datas das fontes, a mesma regra no pandas 2 e no 3** — IBAMA, Acervo Fundiário, CFTC, INMET, MapBiomas Alerta e Queimadas convertiam datas com `pd.to_datetime(errors="coerce")`: a data fora do intervalo do `datetime64[ns]` (1667 e 2925 no CSV do IBAMA) virava `NaT` no pandas 2 e saía como publicada no pandas 3. Agora passam por `normalize.dates.converter_coluna`: valor ilegível ou com ano fora de 1900–2099 vira `NaT`, e no IBAMA também a data de ato de dia posterior à edição do próprio arquivo (2063, 2080 e 2090 na edição de 23/09/2026); a consulta emite `UserWarning` e põe a mesma mensagem em `meta.validation_warnings` (fonte, coluna e quantidade). O INMET descarta a observação sem data e o CFTC recusa a resposta com `ParseError`, como já faziam com data ilegível. **Toda coluna de data das saídas públicas, de fontes e datasets, sai em `datetime64[ns]`** (`Datetime("ns")` no polars): com o pandas 3, a unidade variava por fonte (`s`, `ms`, `us`), e o join de 2 datasets pela data falhava no polars (guia de migração, seção 52).
- **MetaInfo, `fetch_timestamp` pela aquisição** — o dataset repassa o `fetch_timestamp` da fonte, em vez de gravar a hora em que monta o `MetaInfo`: no acerto de cache, declarava uma aquisição que não houve (CEPEA pelo DuckDB, INMET pelo ZIP). A fonte também segue a regra: com o corpo lido do cache (INMET, Acervo Fundiário, ANEC, ZARC e Agrofit), a hora da aquisição original; com vários corpos (BCB Focus, PTAX e SGS, Comtrade, PRODES/DETER, Embrapa Solos, FUNAI e INCRA), a aquisição mais recente, igual ao `fetched_at`. Os registros do cache DuckDB do CEPEA seguem com `fetch_timestamp` nulo, também no `datasets.preco_diario`. Veja o guia de migração, seção 53.
- **MapBiomas Alerta, `max_registros` no lugar de `limit`** — `limit` era o tamanho da página, e a consulta parava calada em 50 páginas (5.000 alertas no padrão); `max_registros` (padrão 5000) é o teto de linhas, com aviso e `MetaInfo` quando corta, e `None` traz a coleção inteira. `limit=` levanta `TypeError`. Página interna de 500; o `source_details` traz o `total_anunciado` e, em `corpos`, o SHA-256 e o tamanho de cada página.
- **Colunas dos datasets na ordem do contrato** — 8 datasets devolviam a ordem da fonte com dado e a do contrato sem dado. Agora todo dataset segue a ordem do contrato, com as extras ao fim, e o `MetaInfo.columns` acompanha. Veja o guia §74.
- **Cache: a política só do CEPEA** — `get_policy`, `calculate_expiry` e `get_next_update_info` levantam `InvalidParameterError` para fonte sem cache, em vez de devolver calados a política do CEPEA; `POLICIES` e `SOURCE_POLICY_MAP` ficam só com `cepea_diario`, e `load_baseline_fingerprint` e `save_baseline_fingerprint` saem de `cepea.parsers`, sem uso. Veja o guia §83.

### Fixed

- **clima / inmet, chuva mensal com estação parcial** — a chuva da UF passa a ser a média dos totais das estações com chuva válida em todos os dias do mês (contrato `clima` 3.1): a estação com o mês incompleto entrava na média como completa e puxava o valor para baixo (MT, fev/2026: 254,1 mm com as 12 parciais, 305,7 mm só com as 22 completas). As parciais ficam fora e são contadas em `estacoes_chuva_parciais`, e as que entraram em `estacoes_chuva`; sem nenhuma completa, o valor sai nulo, com `UserWarning` e a mesma mensagem em `MetaInfo.validation_warnings`
- **CEPEA — `parser_version` do cache** — numa resposta do cache (`cepea.indicador` e a fonte `cache` do `datasets.preco_diario`), o `MetaInfo` informava `parser_version=1` para linhas gravadas pelo parser 2 do CEPEA ou pelo 3 do Notícias Agrícolas. Agora sai a versão gravada nas linhas devolvidas (a maior, se houver mais de uma), e `source_details["parser_versions"]` traz as versões por fonte quando as linhas trazem outra versão além da informada. O `ParseError` de `cepea.ultimo()` sem dado informa a versão do parser do CEPEA.
- **CEPEA e Notícias Agrícolas — marca semanal no cache** — a média semanal do etanol pelo fallback Notícias Agrícolas saía da coleta com `anomalies=["media_semanal"]` e voltava do cache (`offline` ou dentro do prazo) sem a marca, como cotação do dia. A migração 11 do cache acrescenta a coluna `anomalies` a `indicadores` (e à quarentena), a coleta grava a marca e a leitura a restaura. Os registros anteriores à migração recebem `["media_semanal"]` no etanol hidratado e anidro do Notícias Agrícolas, que só publica médias semanais, e lista vazia nos demais.
- **CEPEA, trigo nas duas praças** — a página do trigo publica uma tabela para o Paraná e outra para o Rio Grande do Sul, e o parser lia só a do Paraná: `cepea.pracas("trigo")` listava só `parana`, `praca="rio_grande_do_sul"` era recusada, e o RS só chegava pelo fallback Notícias Agrícolas. Agora o `cepea.indicador("trigo")` traz as duas praças, com os mesmos rótulos do Notícias Agrícolas ("Paraná" e "Rio Grande do Sul"), e o `pracas` lista as duas; `datasets.preco_diario("trigo")` segue no Paraná.
- **CEPEA, variação do dia no `Indicador`** — as tabelas trazem "Var./Dia" e "Var./Mês", e o parser guardava em `meta["variacao"]` a última (a do mês): no `Indicador` de coleta nova, como o do `cepea.ultimo()`, a soja de 04/09/2026 saía com 0,90% em vez de 0,46%. Agora `variacao` é a do dia, `variacao_mes` a do mês e `variacao_semana` a da semana (etanol).
- **SICOR — município e industrialização** — a doc e as mensagens diziam que o OData não tem município e que a industrialização não existe mais. O SICOR publica município por produto (`CusteioMunicipioProduto`, `InvestMunicipioProduto`), que o agrobr ainda não lê, e a industrialização na `RegiaoUF`, agora pelo `bcb.credito_rural_total`. `credito_rural(finalidade="industrializacao")` levanta `InvalidParameterError` antes da rede, apontando a função nova; finalidade inválida também sai como `InvalidParameterError` (subclasse de `ValueError`), e não mais como `ValueError` do client.
- **seguro_rural / mapa_psr** — `seguradora` entra na chave das apólices (contrato `mapa_psr_apolices` 1.1): o dataset deixava de entregar 2007, 2008, 2009, 2011 e 2012 porque o MAPA publica o mesmo número de apólice em duas seguradoras. O registro publicado em dobro e idêntico (1, em 2009) sai uma vez, com aviso e contagem no `MetaInfo`, e a fonte levanta `ContractViolationError` se a chave repetir com valores diferentes
- **seguro_rural / mapa_psr, escala** — a doc dava `nivel_cobertura` e `taxa` em percentual, mas os valores são frações (0,65 = 65%): a razão entre produtividade segurada e estimada e entre prêmio líquido e limite de garantia, como o MAPA publica. Os dados não mudam
- **seguro_rural / mapa_psr, geocódigo** — `cd_ibge` sai nulo quando o MAPA publica "-" no lugar do geocódigo (1.516 apólices entre 2006 e 2025) ou deixa a célula vazia; antes saíam os textos "-" e "" numa coluna nulável
- **CONAB safras** — nas abas com cabeçalho em ano civil (aveia, canola, centeio, cevada e triticale), `conab.safras` rotulava a coluna "Safra 2025" como `2025/26` e a corrente "Safra 2026" como `2026/27`, entregando a safra anterior como corrente sem aviso (12º levantamento 2025/26, RS: canola 340,2 mil t no lugar de 593 mil t); o ano civil do cabeçalho passa a ser sempre o ano de encerramento, como já era no trigo.
- **Censo Agropecuário** — `datasets.censo_agropecuario` e `datasets.censo_agropecuario_historico` descartavam o `ano` pedido e devolviam todos os censos (histórico com `ano=1920`: 240 linhas de dez censos, 216 fora do filtro); o `ano` passa a chegar à fonte.
- **Censo Agropecuário 2017** — `irrigacao` pedia as variáveis 183/184, que não existem na tabela 6857, e o tema inteiro falhava com `SourceUnavailableError` atribuído à fonte; passa a pedir 2372/2373 (estabelecimentos com irrigação e área irrigada; Brasília: 2.726 estabelecimentos e 25.626 ha no total). `adubacao`, `calagem` e `agrotoxicos` de 2017 deixam de pedir a variável 184 (área), ausente das tabelas 6848, 6849 e 6851 nos metadados oficiais.
- **Censo Agropecuário 2006** — `irrigacao` de 2006 pedia a variável 183, que não existe na tabela 855, e o tema ficava indisponível como o de 2017; passa a pedir 2372/2373. As outras cinco tabelas de 2006 (791, 1249, 1245, 1459 e 837) conferem com os metadados oficiais.
- **Catálogo `silvicultura`** — `datasets.info("silvicultura")` anunciava área e hectares, mas o dataset só aceita produtos da produção (carvão, lenha, madeira, resina etc.); descrição e unidade passam a refletir o que ele entrega. Área plantada de eucalipto e pinus continua em `ibge.silvicultura(..., variavel="area")`.
- **IBGE, resposta sem observações** — com a SIDRA respondendo `[]` (ex.: trimestre ou ano ainda não publicado), `ibge.abate`, `ibge.leite_trimestral`, `ibge.silvicultura`, `ibge.extracao_vegetal` e `ibge.pib_agro` levantavam `KeyError` cru e `ibge.ppm` devolvia só `especie`/`unidade`/`fonte`; no fallback de agregados, abate quebrava e leite vinha sem colunas. Nos dois canais, as pesquisas SIDRA passam a devolver DataFrame vazio com as mesmas colunas de uma resposta com dados e a avisar (`warnings.warn`, uma vez por tabela e período) que o período não tem dado.
- **IBGE abate, layout anômalo** — resposta não vazia sem as variáveis 284/285, sem coluna de variável reconhecível ou sem trimestre e localidade devolvia DataFrame sem colunas ou `KeyError` cru; passa a levantar `ParseError` dizendo o que falta, como as outras pesquisas.
- **CONAB série histórica** — `conab.serie_historica` e `datasets.serie_historica_safra` entregavam, sem aviso, safras de grãos que o levantamento mensal ainda publica e revisa (gergelim MT 2024/25: 401,2 mil ha no XLS anual × 695 mil ha no 12º levantamento de setembro/2026); a consulta que inclui essas safras passa a avisar e a apontar `conab.safras`/`estimativa_safra`, sem trocar o número da série.
- **CONAB série histórica, região** — a macrorregião era reconhecida por substring, e a sub-região mineira "Norte, Jequitinhonha e Mucuri" punha ES, RJ e SP do café (total, arábica e conilon) em `NORTE`: 223 das 33.718 linhas de UF das 45 séries. A região passa a mudar só com o rótulo exato; sub-regiões e agregados não a alteram.
- **CONAB série histórica, cana** — `cana` entregava a aba Área, que a planilha oficial intitula "Série Histórica de Área Colhida", como `area_plantada_mil_ha` (SP 2020/21: 4.444,21 mil ha). O valor passa para a coluna opcional nova `area_colhida_mil_ha` do contrato `serie_historica_safra` 1.1, e `area_plantada_mil_ha` fica nula nesse produto; a área total segue em `cana_area_total`.
- **CONAB safra antiga** — duas ou mais safras atrás da edição mais recente, a revisão da CONAB só aparece na série histórica, e `conab.safras`, `datasets.estimativa_safra` e a rota CONAB de `datasets.producao_anual` entregavam o número congelado do último boletim que trazia a safra (soja 2022/23: 154.609,5 contra 159.154,3 mil t). Sem `levantamento`, essas safras passam a sair da série do mesmo produto, se ela for posterior ao boletim, com `origem`, URL, SHA-256 e referência no `MetaInfo`. `conab.brasil_total` faz o mesmo linha a linha (36 séries) e recalcula os subtotais e o BRASIL; se uma série falhar, a chamada inteira falha.
- **CONAB balanço** — `conab.balanco(safra=S)` lia a edição da própria safra e ignorava a revisão publicada depois (trigo 2024/25: 7.536,1 contra 7.873,4 mil t). Passa a ler a edição mais recente cuja aba Suprimento traz S; `levantamento=` (novo em `conab.balanco`, `conab.brasil_total` e `datasets.balanco`) pede a original.
- **CONAB balanço sem produto** — `conab.balanco()` sem `produto` lia só a aba Suprimento e omitia a soja, que a CONAB publica na aba própria "Suprimento - Soja" (desde a 1.1.0). Passa a incluí-la, igual a `conab.balanco('soja')` (set/2026: seis safras, 2025/26 = 180.406,6 mil t).
- **CONAB cereais de inverno de out/2019 a jan/2022** — nas edições em que as abas levam o ano ("Trigo 2021"), `conab.safras` levantava erro para trigo, aveia, canola, centeio, cevada e triticale (o trigo 2019/20 do 12º levantamento de 2020/21 caía na cópia "Trigo 2020", de cabeçalho quebrado). Agora vale a aba mais recente cujo cabeçalho publica a safra; edição que ainda não publica o inverno da safra sai vazia, como nos outros anos.
- **Produção anual, fallback CONAB sem `ano`** — entregava a estimativa da safra em curso rotulada com o ano seguinte (trigo: 5.739,7 mil t em colheita como 2026); passa a entregar o ano civil anterior, safra já colhida.
- **CONAB série histórica, âncora da região** — UF listada sob macrorregião que não é a sua (IBGE) passa a ser `ParseError` de layout, em vez de sair com a região errada.
- **Catálogo e contrato de `silvicultura` e `extrativismo_vegetal`** — com `variavel="valor_producao"` os dois datasets entregam o valor da produção em mil reais, com `unidade="Mil Reais"`, como as tabelas 291 e 289 do IBGE (PR 2023, madeira em tora: 4.206.266; PA 2023, açaí: 651.058); o número e a unidade na saída já estavam certos, mas o contrato dizia só toneladas ou metros cúbicos e o catálogo não citava Mil Reais. Contrato PT+EN e catálogo passam a declarar a escala.
- **ANTAQ** — no pandas 3, `astype(str)` preserva o mês ausente como `float('nan')` e a resolução textual do mês quebrava com `AttributeError` para cargas sem atracação correspondente (a linha deveria sair com `ano`/`mes` nulos); `mes` passa a ser resolvido com `na_action="ignore"` e o parser sobe para 2
- **MapBiomas** — `uso_do_solo` no recorte estadual da coleção 10 devolvia `Classe 0` no lugar de `Não observado`, o rótulo que a própria publicação usa (`class_level_1 = "6. Not Observed"`) e que o recorte municipal da mesma coleção já devolvia. Afetava 37 linhas de cobertura (1.480 linhas anuais) e 680 linhas de transição do workbook oficial; a legenda da coleção passa a valer nos dois recortes
- **MapBiomas** — o rótulo publicado em `classe`, `classe_de` e `classe_para` (cobertura, transição e `uso_do_solo`, estadual e municipal) passa a ser o da aba `LEGEND_CODE` de cada coleção: a classe 62 sai como `Algodão (beta)` nas coleções 10 e 11 (antes `Algodão`; quem filtra pelo rótulo passa a ver o qualificador) e a 91 como `Parque eólico (beta)` na 11; `Corpo D'água`, `Área não vegetada` (11) e `Formação Herbáceo Arbustiva` (11) seguem a grafia oficial. As classes sem rótulo em português na legenda (0, 75 e 13 na 10; 0 e 13 na 11) mantêm a tradução do rótulo inglês, agora declarada nas docs
- **PSR** — parser 4 recusa CSV com cabeçalho duplicado, largura irregular ou ano inválido antes dos filtros; preserva cardinalidade, zeros iniciais, tokens textuais como `NULL` e valores publicados, com reconciliação independente de apólices e indenizações.
- **ZARC** — filtros aceitam os nomes legados Arroz Sequeiro e Trigo Sequeiro mantendo os aliases já publicados; periodicidade declarada alinhada ao dicionário oficial diário; reconciliação independente preserva duplicatas, riscos, campos textuais e posição associada ao SHA, com e sem cache.
- **ANP preços** — seleções semanais e mensais recusam a mesma semana/geografia/produto publicada em arquivos sobrepostos; a média mensal é uma derivação aritmética simples do agrobr.

- **ANP preços** — consultas municipais no fim de dezembro incluem o arquivo adjacente disponível, preservando semanas publicadas no período seguinte (31/12/2023 no arquivo 2024–2025) e sua contribuição à média mensal; reconciliação independente cobre preços semanais/mensais em Brasil, UF e município.

- **Boletins ANDA/DERAL/ANEC** — entregas ANDA restritas à seção e ao ano corretos; DERAL recusa cabeçalhos de condição/progresso ausentes; ANEC exclui percentuais do mapa dos destinos e recusa cabeçalhos semanais/mensais incompletos. Oráculos de PDFs e planilhas originais, replays públicos, inventário N1 e limites documentados por variante, sem equiparar cobertura sintética a publicação real.

- **IBGE, SIDRA atrás do Cloudflare** — os doze datasets que dependem da SIDRA (produção anual, LSPA, PPM, abate, PEVS, leite, PIB agro e censos) falhavam com `SourceUnavailableError` porque `apisidra.ibge.gov.br` responde 403 com desafio Cloudflare a qualquer User-Agent programático desde setembro de 2026; o fallback para a API de agregados restabelece as onze consultas verificadas (PAM, LSPA, PPM, abate, silvicultura, extração vegetal, leite, PIB agro, censo 2017 e censo histórico) com valores idênticos aos publicados pela SIDRA no caso conferido
- **IBGE censos SIDRA:** zeros (`-`) preservados; chaves duplicadas rejeitadas, inclusive entre tabelas complementares; preparo do solo preserva o ano informado na resposta. Parsers 2, contratos preservados.


- **IBGE trimestrais:** abate preserva zero SIDRA (`-`) e peso sem observação de cabeças; abate/leite rejeitam duplicatas antes do join; valores monetários de leite/PIB são `float64`. Parsers 2, contratos preservados.


- **CONAB — trigo histórico com ano no nome da aba:** parser de levantamento 3 seleciona `Trigo YYYY` pelo encerramento da safra e rejeita abas equivalentes ambíguas; fallback anual volta a atender o XLS oficial de 2020/21. Contratos preservados.


- **IBGE PAM/PEVS**: PAM preserva localidades e medidas inteiramente nulas ou suprimidas, rejeita duplicatas, aliases que colidem e variáveis desconhecidas. PEVS entrega valores monetários em `float64`, com a unidade publicada. Parsers 2; contratos 2.0/1.0 preservados.

- **Preço diário** — `cepea.indicador` só consultava a fonte quando `fim` estava nos últimos 10 dias, mas a página publica 15 pregões (cerca de 21 dias corridos): pedidos com `fim` entre 11 e 21 dias atrás devolviam vazio em silêncio. A janela passa a 25 dias e uma janela sem cache e fora da fonte emite aviso. A fonte `cache` de `preco_diario` devolvia as colunas cruas do DuckDB (`variacao_percentual`, `collected_at`, `parser_version`, sem `anomalies`); agora reutiliza o construtor do contrato e sai com as mesmas colunas e tipos da coleta
- **Futuros agrícolas** — `b3.ajustes` e `futuros_agricolas(tipo="ajustes")` devolviam também os registros do after-hours datados de D+1 que a B3 inclui no arquivo do pregão (em 17/09/2026, 10 contratos agro saíam duplicados com data 18/09 e o mesmo ajuste); agora só saem as linhas do pregão pedido, e o `historico` deixa de repetir contratos entre dias consecutivos
- **Crédito rural** — as agregações por UF e por programa de `credito_rural` somavam grupos inteiramente nulos como `0.0` (soja/MT 2024/2025: `AreaCusteio` nulo em todos os 277 registros saía como área financiada zero); agora somas de grupos sem valor publicado ficam nulas e zeros publicados continuam zero
- **Preço de atacado** — o parser CEASA/PROHORT atribuía a CEASA de cada coluna de preço pela posição na lista da consulta `MDXceasa`; uma lista fora de ordem trocaria praça, preço e data em silêncio. A CEASA passa a vir do cabeçalho da coluna e uma coluna cujo nome não esteja no catálogo (ou apareça duplicada) levanta `ParseError`
- **Futuros agrícolas** — a unidade do café conillon (CNL) saía como `USD/ton`; o arquivo oficial de ajustes da B3 publica o contrato em reais (`AdjstdQt Ccy="BRL"`) por saca de 60 kg, e a coluna `unidade` passa a `BRL/sc60kg`
- **CONAB custos** — parser agrícola 4 corrige faixas de cabeçalhos mesclados, café com safra anual e referência textual, rótulo MILHO1, percentuais textuais e notas cambiais emitidas indevidamente como itens. Parser sociobio 2 aplica escala Excel apenas a percentuais numéricos. Totais, unidades e revisões históricas permanecem publicados; terceira medida e erros reais continuam recusados. Contratos 3.0/1.0 preservados, com manifesto independente e replays dos dois datasets.

- Balanço CONAB: todas as métricas, incluindo `demanda_total`, saem como float64; contrato 1.1 documenta demanda, revisão, unidade e fonte, mantendo 1.0 disponível.

- **CONAB reconciliação** — `area_colhida` fica nula nos levantamentos e em `estimativa_safra` pela CONAB: o boletim publica uma única área. Corrigidos nomes de abas com espaços, balanço legado sem demanda total e proveniência da edição; progresso converte todo percentual textual com `%`, inclusive valores até 1%.

- **CONAB série histórica** — preserva o ano civil publicado nos nove produtos de café/cereais de inverno, usa somente Área Total em `cana_area_total` e representa algodão em caroço em `algodao`; abas selecionadas ausentes, ambíguas ou ilegíveis passam a falhar com contexto (#109, #110, #111, #112)
- **CONAB série histórica — previsão** — o descarte das colunas de previsão passa a ser nominal, com log contextual e inventário por coluna de período; a previsão continua fora do contrato histórico.

- **Progresso de safra CONAB — boletins históricos**: resolve o download real nas fichas oficiais e valida a assinatura XLSX/XLS antes do parse. HTML, inclusive com tamanho ou Content-Type de planilha, gera erro tipado com URL e trecho do conteúdo. Regressão com boletim real de setembro/2025. Percentual com nota de revisão `*` deixava de ser lido e virava nulo; agora preserva o valor e a indicação `revisado`.
- **SICAR / cadastro_rural** — varredura tabular identifica cada feature pelo id WFS e mantém uma ocorrência por `cod_imovel`: atualização mais recente se disponível em todo o grupo, senão criação, com desempate ou ausência de datas resolvidos pelo sufixo numérico do id. Avisos e `MetaInfo.source_details["sicar"]` expõem critérios, contagens e até mil descartes identificados, com total e truncagem explícitos; contrato 2.0, colunas e chave preservados.
- **SICAR** — paginação WFS ordenada por `cod_imovel`; mudanças de contagem na varredura tabular geram avisos e ajustam o número de páginas. Repetição do mesmo id de feature e total final divergente continuam causando erro com orientação de repetir a consulta; não há garantia de snapshot entre páginas.
- **SICAR geo — versões repetidas** — `imoveis_geo()` e `imoveis_geo_stream()` mantinham a primeira ocorrência de um `cod_imovel` repetido (em 22/09/2026 havia códigos com duas versões publicadas em GO, MS, MT e RS; no MS a mais recente vinha em segundo); agora seguem a regra de `imoveis()` (atualização, criação, sufixo do id), com avisos e `source_details["sicar"]` no `MetaInfo`. O stream compara também versões que caem em páginas diferentes (o último código de cada página passa para o lote seguinte); id de feature repetido gera `ParseError`, como no tabular.
- **SICAR geo — CRS** — as 27 camadas publicam em SIRGAS 2000 (EPSG:4674) e o geo rotulava EPSG:4326 sem pedir reprojeção; o pedido passa a levar `srsName=EPSG:4326` e cada página com feições precisa declarar esse CRS (outra declaração gera `ParseError`). Nas capturas de 22/09/2026 a reprojeção do GeoServer muda as coordenadas em no máximo 1e-8 grau.
- **SICAR geo — datas e vazio** — data sem fuso gera `ParseError`, como no tabular (antes saía datetime sem fuso, fora do contrato); resultado vazio sai com o CRS.
- **SICAR — `condicao` nula** — `condicao` nula ou ausente (anulável no `DescribeFeatureType` da fonte e no contrato 2.0) virava `""`; agora fica nula em `imoveis()`, `imoveis_geo()` e `cadastro_rural`.
- **SICOR / credito_rural — código nulo** — `cdPrograma` (e os demais códigos de dimensão) nulo virava `"Desconhecido (nan)"` com aviso de código desconhecido, e o rótulo inventado entrava na chave `programa` do `credito_rural`; agora código nulo fica com nome nulo, sem aviso. Códigos válidos e desconhecidos não mudam.
- **SICOR / credito_rural — programa e tipo de seguro pela tabela oficial** — os dicionários digitados trocavam programas (`0152` saía "RenovAgro", mas é PROIRRIGA; `0156` "Moderagro/Moderfrota", mas é ABC+; `0100`, `0110` e `0200` idem), traziam 8 códigos que não existem e publicavam "Desconhecido" para Moderagro, Inovagro e RenovAgro (`0153`, `0162`, `0222`); o tipo de seguro `2` saía "Sem seguro" (é Proagro Mais) e o `9`, "Nao se aplica" (é Sem adesão a seguro), então os filtros `programa=`/`tipo_seguro=` selecionavam outro conjunto. Agora os nomes saem das tabelas de domínio do BCB (`Programa.csv`, `TipoGarantiaEmpreendimento.csv`) por regra fixa, com filtro sem diferenciar maiúsculas; guia de migração §30.

- **docs contratos** — `producao_anual` e `importacao` documentam todas as colunas do contrato; teste offline compara cada página de contrato com o código; chave de DETER inclui `municipio_id` em PT/EN; título de `importacao` em 1.1 e tabela DETER com os tipos/nulabilidade do contrato; página de desmatamento aponta para os contratos 2.0 registrados; tipos e nulabilidade das tabelas de contrato conferidos com o código, distinguindo os modos e a representação pandas.

- **Censo municipal 1985** — a UF cujo volume não traz a tabela recusa com as UFs que a têm (`InvalidParameterError`), e a tabela que está no volume sem casa lida levanta `ParseError`. Na 1.1.0, a UF sem a tabela dava `ValueError`; das 4 tabelas sem casa lida, AM 80, AP 80 e RR 80 davam `ValueError`, e a RR 119 devolvia 36 linhas. O nível estadual passa de `total` a `uf`.
- **docs zarc** — contrato `zoneamento_agricola` documenta as 58 colunas e a ausência de chave primária do contrato 2.0; riscos incluem o valor 50 publicado; `produtividade_texto` documentada como texto preservado; domínio de ciclos documentado (13–26) e página EN do contrato em inglês.
- **zarc** — `culturas()` passa de 32 para 96 culturas, conforme os rótulos das tábuas anual e perene; café, cana, laranja, banana e mandioca passam a resolver os nomes publicados. Cultura ausente indica quando usar `safra="perene"` ou uma safra anual. Os metadados incluem `culturas_observadas` antes dos filtros.
- **condicao_lavouras** — aba ilegível no PC.xls passa a interromper a leitura com ParseError em vez de omitir linhas em silêncio.
- **condicao_lavouras** — data passa a ser a data de referência da aba em dd/mm/yyyy (antes misturava dd-mm-yy e os rótulos Atual/Anterior); "-" publicado vira 0, células vazias continuam nulas e source_method informa o leitor Excel real.
- **custo_producao** — abas cujo LOCAL: não traz a UF passam a usar a UF do nome da aba, com origem registrada nos metadados. Sistemas publicados logo abaixo do título também são reconhecidos: 9 contextos de milho recuperados; 3 abas com contexto incompleto continuam listadas como não reconhecidas. Rio Verde 2007–2009 continua recusado por medida sem cabeçalho em E29.
- **custo_producao** — o catálogo passa a descer nas subpastas milho/arroz/feijão da CONAB (6 planilhas que nunca resolviam), café vira `cafe_arabica`/`cafe_conilon` e títulos 'Série Histórica - Custos - X' deixam de gerar culturas inválidas. Os quatro arquivos sem extensão são preservados; a seleção explícita de planilha e aba continua obrigatória quando há ambiguidade.
- **Polars nos datasets** — `as_polars=True` passa a valer nos 52 datasets registrados; os 28 que não expunham o parâmetro convertem após normalização e validação do contrato, preservando fonte selecionada, cache e metadados. `preco_diario` deixa de acionar o fallback para o cache DuckDB apenas porque Polars foi solicitado.
- **CEPEA datas** — `datetime` e `pandas.Timestamp` em `inicio`/`fim` são reduzidos à data civil; `NaT` levanta `InvalidParameterError` antes de consultar o cache.
- **Compatibilidade de tipos** — normalização de nulos em BCB, Comtrade, SICAR e Lista Suja mantém os valores e passa com pandas-stubs 2.x e 3.x. Colunas categóricas vazias são aceitas como STRING; categorias numéricas ou mistas continuam rejeitadas.
- **CONAB Brasil** — cabeçalho repetido e rodapés deixam de virar produtos em `brasil_total`; limites da tabela preservam as culturas com valores ausentes, e goldens oficiais antigos/atuais têm oráculos independentes
- **Acervo parâmetros** — UF, cobertura e bbox são validados antes da dependência geográfica; parâmetros inválidos deixam de ser encobertos por `ImportError` em instalações sem o extra, e consultas válidas continuam exigindo o extra antes do download
- **CONAB trigo** — cabeçalhos anuais do quadro agrícola usam o ano de encerramento (`Safra 2026` → `2025/26`), preservando os valores oficiais e a associação ao levantamento solicitado
- **CONAB série histórica** — a URL do `gergelim` apontava para `graos/girassol/` e retornava 404: o produto constava em `produtos_disponiveis()` mas nunca baixava. Corrigido para `graos/gergelim/`, onde o arquivo existe e parseia normalmente (21 registros, safras 2018/19 a 2024/25) (#105)
- **ANA parser** — campos obrigatórios ausentes em qualquer feição/página produzem `ParseError`; resultados vazios legítimos, valores nulos e zeros continuam preservados
- **LSPA catálogo** — onze códigos de culturas corrigidos contra metadados e respostas oficiais de 2010, 2023 e 2026; trigo, algodão e batata deixam de receber dados de outras culturas, produtos com código inexistente voltam a retornar valores, e o alias batata inclui a terceira safra
- **LSPA café e parâmetros** — café consulta o total oficial antes de 2012 e preserva arábica/canephora desde então; códigos de produção e rendimento corrigidos, meses inválidos deixam de virar consulta anual ou ser truncados, e a seleção territorial usa a validação comum
- **estimativa_safra** — valores ausentes propagam nulos por métrica, e linhas ou sub-safras faltantes impedem apresentar um total parcial; sem `mes`, seleciona o último mês com observação e preserva essa referência nas colunas LSPA. Um mês explícito sem dados nunca recua para outro mês
- **cache CEPEA** — lotes com revisões repetidas preservam a última observação válida por chave, inclusive entre blocos de gravação; a ordenação da fonte mantém o mesmo valor entre coleta nova e leitura offline
- **contratos** — nomes de colunas duplicados produzem violação de contrato explícita; strings vazias ou `NaT` não passam como datas válidas por serem convertidas silenciosamente em ausência
- **RNC e Defensivos cache** — CSVs são publicados atomicamente e falhas de leitura preservam os arquivos; formulados e autorizações são publicados no mesmo ZIP para impedir mistura de lotes, com leitura compatível de caches legados e tratamento de conteúdo comprimido corrompido
- **Acervo e ANEC cache** — locks de aquisição deixam de ser reutilizados em outro event loop; ANEC usa temporários exclusivos, remove apenas os próprios e repete falhas transitórias de publicação no Windows; Acervo exige validadores presentes e compatíveis para reutilizar um download
- **INMET segurança** — token presente na URL é removido das mensagens, URLs de erro, cadeias de exceção, histórico de redirecionamento e logs HTTPX/retry; prévias de respostas não JSON também ocultam a credencial
- **CEPEA e preço diário** — seleção explícita CEPEA sobre Notícias Agrícolas por observação/praça evita que fallback antigo vença a primária; cache quente/offline e `ultimo()` seguem a mesma regra, e revisões da mesma fonte substituem a observação anterior sem apagar o outro provedor
- **preco_diario** — seleção por praça de referência e desempate por slug tornam a redução por data/produto determinística; metadados refletem somente linhas escolhidas, e valores do fallback DuckDB são expostos como `float64`
- **CEPEA sanity** — `anomalies` no DataFrame passa a texto JSON, com nulo quando vazio, preservando os marcadores e o contrato; `Indicador.anomalies` permanece lista
- **cache** — migração 9 conserva originais de leite NA, trigo e algodão CEPEA em quarentena verificada antes de retirar publicações de leite ou corrigir rótulos legados (`BRL/sc60kg` → `BRL/ton`, `BRL/@` → `cBRL/lb`), sem alterar valores; toda a sequência é transacional
- **contratos e docs** — `IBGE_LSPA_V2`, `IBGE_CENSO_AGRO_LEGADO_V2` e `CONAB_SAFRA_V2` identificam os contratos 2.0; nomes `_V1` desses três permanecem aliases de compatibilidade, sem representar schemas antigos. README atualiza testes/cobertura e o guia esclarece o código de saída do `doctor`
- **tests Windows** — teste de espera do rate limiter usa relógio controlado e verifica o intervalo residual sem depender de resolução submilissegundo; a política live é exercitada em subprocessos com encoding explícito de saída
- **Windows** — fixtures JSON de RNC, ABIOVE e MAPA PSR lidas explicitamente em UTF-8; streams temporários do PSR tipados como `IO[bytes]`, compatíveis com o wrapper de arquivo do Windows no mypy; CI Windows verifica tipagem nativa e essas fixtures com modo UTF-8 desativado
- **RNC / SNPC** — pesquisa e exportação acompanham a sessão pública, o action do formulário e tokens CSRF renovados por tentativa; cultivares protegidas e registradas voltam a ser obtidas sem reutilizar tokens consumidos
- **ANTT** — cabeçalho oficial `municipal` alimenta `municipio`; categorias textuais de eixo são preservadas e contagens explícitas são interpretadas sem juntar classes diferentes; o filtro de pesados reconhece faixas cujo limite inferior é de pelo menos três eixos; esgotar as tentativas do cadastro mantém o tráfego obtido, sem enriquecimento geográfico
- **acervo_fundiario** — publicação atômica de metadados e downloads repete `PermissionError` transitório até cinco tentativas no Windows; erros persistentes e cancelamentos preservam o destino anterior, e os cenários de concorrência e retry entram no CI Windows
- **ibge Censo legado** — caixas de texto BIFF8 sobrepostas na mesma altura deixam de combinar variáveis paralelas de máquinas em Sergipe como uma hierarquia; valores e unidades oficiais permanecem iguais

- **ABIOVE** — ano selecionado em cabeçalhos Excel com datas, mil toneladas convertidas para toneladas e receita FOB separada do preço médio; tabelas mensais limitadas ao produto, sem linhas comparativas artificiais; fallback de exportação preserva kg/USD
- **CONAB café** — abas de produção e formação identificadas por nome exato; hectares convertidos para mil hectares, mil sacas beneficiadas de 60 kg para mil toneladas e sacas/ha para kg/ha, preservando a área de referência da produtividade
- **IBGE PEVS** — unidade vem do campo `MN` da resposta SIDRA ou da variável; valor de produção deixa de ser rotulado como quantidade física do produto
- **Censo legado** — coluna de ordinais ignorada, cabeçalhos BIFF5/BIFF8 e HTML reconstruídos, tabelas Brasil/UF selecionadas corretamente e escalas numéricas Excel aplicadas por célula; contagens e valores monetários deixam de ocupar as variáveis erradas, zeros HTML são preservados e URLs respeitam a capitalização dos arquivos estaduais
- **LSPA / estimativa_safra** — parser próprio da tabela 6588 preserva meses, produtos, variáveis e unidades; adaptador do dataset usa essas dimensões para selecionar o último mês e consolidar as sub-safras
- **MAPA PSR** — cabeçalhos acentuados normalizados e `sinistros` exige indenização reconhecida e positiva, em vez de retornar todas as apólices quando a coluna está ausente
- **FUNAI** — filtro por UF inclui terras multiestaduais nas APIs tabular e geográfica; datas dia/mês/ano deixam de inverter dia e mês
- **ICMBio** — filtro geográfico de bioma usa correspondência por conteúdo sem distinguir maiúsculas, incluindo biomas compostos
- **Acervo Fundiário** — cada download limpa somente seu próprio temporário exclusivo, inclusive quando cancelado, preservando downloads concorrentes
- **Lista Suja** — cabeçalhos e faixas de título repetidos deixam de ser registros; somente linhas com ID numérico entram no resultado
- **ANEC** — revisões do mesmo artigo invalidam o parse por timestamp, URL e hash dos bytes, renovando dados e proveniência
- **ANP diesel** — API e parser normalizam igualmente `DIESEL S10`, `OLEO DIESEL S10` e `ÓLEO DIESEL S10`
- **importacao** — óleo de soja consolida NCMs por ano/mês/produto/UF, incluindo UF nula, com helper compartilhado com exportação
- **B3 / futuros_agricolas** — histórico com todos os dias em falha levanta `SourceUnavailableError` com a última causa; busca automática de pregão continua após dias vazios na janela de cinco dias úteis
- **sanity** — limites de trigo e algodão usam BRL/ton e cBRL/lb; variação temporal compara registros do mesmo produto, praça e unidade
- **normalização / cache** — nomes completos de UF mais específicos têm precedência; `nan_as_none` cobre strings e Decimal NaN; registros com campos obrigatórios nulos são rejeitados individualmente sem impedir gravações válidas
- **calculadora** — ponto e vírgula funcionam como separadores decimais; controles de incremento e decremento operam sobre números e preservam CTC e dose calculadas
- **landing page** — ticker formata preços pela unidade da linha e converte cBRL/lb para BRL/lb; o contrato `preco_diario` documenta a unidade real do valor
- **docs / exemplos** — risco ZARC 20%/40%, código de Brasília de Minas, uso de `offline=True` e colunas da análise de soja corrigidos em PT/EN; captura golden IBGE usa o client assíncrono nativo e preserva a primeira linha
- **packaging** — wheel e sdist excluem configurações locais e arquivos `.env` explicitamente, inclusive em cópias de build sem `.gitignore`; smoke do pacote instalado rejeita esses arquivos

- **nasa_power** — falha em qualquer bloco interrompe a consulta completa, sem devolver séries parciais como sucesso; `clima_uf` valida agregação antes da rede
- **ibge** — consultas SIDRA têm timeout efetivo e cancelamento sem threads bloqueadas; parâmetros em lista, classificações e cabeçalhos são preservados
- **anp_diesel** — cabeçalhos acentuados do CSV oficial de vendas passam a identificar a UF, evitando devolver dados nacionais para um filtro estadual
- **datasets / contracts** — retornos vazios resolvem o contrato da modalidade solicitada; números complexos, booleanos e infinitos não passam por reais válidos, e datas numéricas são rejeitadas
- **snapshots** — publicação usa diretório temporário e renomeação final; cancelamentos e falhas limpam a área de preparação. Arquivos novos têm SHA-256 conferido na leitura, mantendo compatibilidade com snapshots antigos
- **doctor / cli** — erros de cache são explícitos e afetam o diagnóstico; probes respeitam método HTTP, timeout, TLS, credenciais e erros no corpo. Falhas reais retornam código 1; JSON/CSV não mistura progresso com dados e preserva saída estruturada vazia
- **testes** — checksums divergentes e falhas de parsing reprovam goldens e benchmarks; fixtures são selecionadas por produto e formato, multiplicação de linhas confere contagem, e o CSV oficial de vendas ANP substitui a fixture ausente. Integrações registram proveniência e rejeitam consultas de referência vazias
- **cepea** — seleção de tabelas pelo título distingue soja Paraná, frango resfriado, etanol anidro e demais indicadores; açúcar refinado usa página própria e unidade por kg; laranjas usam a página de citros. Suíno preserva as praças da fonte, com invalidação do cache antigo; leite mensal usa o mês de referência, unidade `BRL/L` consistente com o fallback e exclui a tabela spot. Fallback também cobre páginas HTTP 200 sem dados reconhecidos; o aviso da janela histórica exige início explícito
- **conab** — balanço normaliza acentos, reconhece trigo em ano civil e mantém a última revisão de safras mescladas; progresso distingue cabeçalhos anuais, reconhece Unidade da Federação e ignora estados desconhecidos; tecnologia dos custos é derivada do título da aba; data de publicação ausente permanece nula, sem usar a data da consulta
- **bcb** — filtro exato de produto SICOR preserva grafia e aspas literais, evitando incluir silagem ou sarraceno nos agregados; ausência legítima de registros devolve esquema vazio com orientação sobre finalidade
- **zarc** — nomes oficiais com ordinais e acentos convergem para os aliases; erros de cultura ausente listam as opções da safra consultada
- **queimadas** — CSVs legados dos arquivos anuais são normalizados para o esquema atual, preservando campos opcionais ausentes
- **ibge** — SIDRA preserva o símbolo de zero numérico e os municípios correspondentes; PAM histórica identifica unidades, moedas e condição do café e preserva área plantada e valor de produção ausentes como nulos; PPM rejeita anos futuros; formatos usuais de trimestre são normalizados antes da rede; levantamento ausente no fallback LSPA usa inteiro anulável
- **desmatamento** — PRODES valida tipo e intervalo de ano contra a camada do bioma, com cache dos limites publicados
- **b3** — solicitação de token com HTTP 400/404 indica arquivo não publicado após validação da data; download com HTTP 400 continua sendo erro. Token e download são serializados no processo e o histórico consulta dias sequencialmente, evitando invalidar tokens concorrentes; demais falhas não são ocultadas como vazio
- **snapshots** — criação verifica engine Parquet e fontes antes de criar diretórios; zero arquivos remove o diretório e levanta `SnapshotError` com erros por fonte, com saída 1 na CLI
- **sync** — chamadas fora de um loop ativo usam `asyncio.run` diretamente, sem criar loops abandonados; navegador e driver são encerrados no ciclo da página, inclusive em falha ou cancelamento, sem reutilizar recursos de loops fechados
- **rio_verde** — extração por células da tabela-resumo distingue os layouts 2024/25 e 2025/26, preserva nomes compostos e medições ausentes e rejeita linhas inválidas; textos de gráficos deixam de ser aceitos como cultivares
- **inmet / nasa_power** — grupos sem medições mantêm precipitação nula; radiação diária INMET também preserva ausência. Chuva mensal por UF acumula por estação e depois calcula a média espacial, sem multiplicar pelo número de estações
- **cache** — atualização persiste valor, unidade, metodologia e versão do parser conjuntamente; migrações 5–8 preservam os originais em quarentena antes de retirá-los das consultas, incluindo suíno e refinado legado do Notícias Agrícolas. Preservação, retirada e registro da versão são atômicos; falhas levantam `CacheMigrationError`, sem repetir exclusões silenciosamente. Migração 2 adiciona somente colunas faltantes, preservando valores existentes em bancos parcialmente migrados e evitando alterações desnecessárias em bancos novos
- **datasets / geo** — preço sem fonte nem cache disponível propaga indisponibilidade; o fallback interno para outra fonte ou cache respeita `SourceFallbackWarning`, inclusive convertido em erro. Metadados distinguem o caminho de leitura da origem dos registros, preservada também em cache quente/offline; adaptador CEPEA explicita o tipo pandas na fronteira de desempacotamento. Envelopes JSON de erro geográfico são reconhecidos independentemente da formatação e não viram dados vazios válidos
- **contracts** — validação confere valores inteiros, textuais e booleanos; fontes sem registros recebem DataFrames vazios tipados pelo contrato, mantendo o aviso de resultado vazio
- **docker** — imagem inclui módulos de diagnóstico e alertas e atualiza os componentes de instalação nos dois estágios; exemplos de extras e escrita de outputs na CI foram alinhados à configuração existente
- **inmet / sfb / icmbio / anp_diesel / ibge / comexstat / queimadas** — o INMET aceita ZIPs históricos válidos menores que 1 MB e rejeita conteúdo não ZIP; filtros SFB/ICMBio validam allowlists e escapam apóstrofos; ANP Diesel, IBGE PAM e ComexStat normalizam ou rejeitam entradas inválidas antes da rede; o erro 404 do arquivo diário de queimadas orienta o uso da série mensal
- **producao_anual** — adaptador da PAM inclui `valor_producao` como `Float64` nulo quando a fonte não devolve a variável, mantendo o schema estrito do contrato
- **queimadas** — sentinela `-999` do CSV do INPE é convertida em nulo nas colunas numéricas, inclusive `risco_fogo` e `numero_dias_sem_chuva`
- **embarques_anec** — associação das colunas semanais do PDF a `last_week` e `current_week` corrigida; o relatório real deixa de produzir 38 linhas duplicadas na chave primária
- **seguro_rural** — parser de apólices do MAPA/PSR inclui `valor_indenizacao` como `Float64` nulo quando ausente na fonte, conforme o contrato estável
- **bcb.sgs** — consultas sem janela passam a usar os últimos dez anos; respostas 4xx da API viram `InvalidParameterError` com a mensagem do BCB e demais falhas HTTP viram `SourceUnavailableError`
- **cepea** — `indicador()`, `ultimo()` e `pracas()` validam produto, praça e datas antes da rede; intervalos invertidos e formatos inválidos geram `InvalidParameterError`, e resultados vazios preservam as colunas do contrato
- **polars** — `as_polars=True` sem a dependência opcional agora levanta `ImportError` com a instrução `pip install agrobr[polars]`, em vez de devolver pandas silenciosamente
- **conab.custo_producao** — UFs inválidas são rejeitadas antes da rede e UFs válidas sem aba levantam `SourceUnavailableError` com a lista disponível; `uf=None` mantém a seleção explícita da primeira aba compatível
- **validação de parâmetros** — IBGE PAM, Queimadas, Comex Stat, NASA POWER, MapBiomas, desmatamento, ANDA, ANEC, Comtrade, USDA, UNICA, INMET, CONAB série histórica, Rio Verde e ZARC passam a classificar entradas inválidas antes do download como `InvalidParameterError`; o desmatamento também avisa quando atinge o teto de 50 mil registros
- **compatibilidade** — limpeza textual inclui colunas pandas `StringDtype` nos parsers de defensivos e RNC, e o wrapper síncrono usa `inspect.iscoroutinefunction`, compatível com as versões novas do Python
- **bcb** — a coleta SICOR deixa de enviar `$skip`, que passou a causar HTTP 500 na Olinda; usa uma busca de até 100.000 registros e, ao atingir o limite, refaz a consulta em fatias mensais; o health check agora consulta o endpoint real de crédito rural
- **cli** — a saída redirecionada ou encadeada no Windows não falha mais com `UnicodeEncodeError` em terminais `cp1252`; caracteres indisponíveis são substituídos sem alterar o encoding
- **docs** — documentação PT/EN alinhada ao código e aos contratos: exemplos inválidos, caches/TTLs inexistentes, contagens, parâmetros, colunas e páginas contratuais corrigidos; referência de API e contrato do dataset `embarques_anec` adicionadas à navegação
- **preco_diario** — o modo determinístico limita a consulta ao CEPEA à data do snapshot sem sobrescrever um `fim` anterior informado pelo usuário
- **docs/snapshots** — documentação alinhada à cobertura real do modo determinístico, à coleta limitada a CEPEA/CONAB/IBGE e aos nomes efetivos dos arquivos Parquet
- **sync** — `agrobr.sync.anec` passa a expor as corrotinas públicas da ANEC como funções síncronas; um teste estrutural mantém o registry sync alinhado aos módulos públicos, enquanto integrações de `alt` permanecem em `sync.alt`
- **contracts** — schemas JSON regenerados: 13 contratos antes ausentes foram adicionados, 6 schemas órfãos foram removidos, divergências de `credito_rural` e `custo_producao` foram corrigidas e um teste passa a impedir drift entre o registry e os arquivos distribuídos
- **filtros** — buscas textuais em defensivos, RNC, Rio Verde, ZARC, ANTT Pedágio, MAPA/PSR, ANTAQ, Embrapa Solos, ANP Diesel, MapBiomas e CONAB Progresso tratam parâmetros como texto literal, não expressão regular, e toleram valores nulos
- **encoding** — leitura de CSV aplica a cadeia UTF-8 → Windows-1252 → ISO-8859-1 → chardet em `utils/io`, RNC, defensivos, ANTAQ e ANP Diesel; a normalização de UF usa nomes completos delimitados e não confunde texto corrompido como `paran�` com Pará
- **inmet** — consultas por UF sem `AGROBR_INMET_TOKEN` falham antes de listar ou consultar estações, e falhas de fonte cancelam imediatamente as tarefas irmãs em vez de deixar requests continuarem após o erro
- **cache** — a ausência esperada da tabela `schema_version` no primeiro uso do DuckDB deixa de imprimir traceback no stdout, evitando `UnicodeEncodeError` em terminais Windows com encoding cp1252
- **ibge** — falhas 5xx e páginas HTML viram `SourceUnavailableError` após as tentativas com backoff antes de chegar às APIs SIDRA
- **http** — respostas HTTP 200 que não são JSON agora viram indisponibilidade com `Content-Type` e preview em BCB/SICOR, SGS, Focus, PTAX, ZARC, NASA POWER, USDA, CFTC, MapBiomas Alerta, Comtrade, CONAB/CEASA, B3, IMEA, ArcGIS e ANTT Pedágio; o IMEA também rejeita JSON que não seja lista
- **downloads** — IBGE legado, DERAL, Lista Suja, Rio Verde, ANP Diesel, MAPA/PSR, ZARC, MapBiomas, CONAB, ANDA, B3 e IBAMA validam magic bytes ou rejeitam HTML antes do parser, classificando páginas de manutenção como `SourceUnavailableError`
- **deral / abiove** — o parser DERAL não trata mais arquivo ilegível ou nenhuma aba reconhecida como sucesso vazio, e a ABIOVE registra a exceção original ao ignorar uma aba que falhou
- **conab / acervo_fundiario / zarc / ibge** — custo de produção valida a safra e rejeita planilhas sem período identificável; o Acervo Fundiário exige `pyogrio` antes do download; ZARC sem recursos de safra vira indisponibilidade; marcadores não numéricos do Censo legado viram nulos
- **conab** — downloads XLSX via Playwright repetem timeouts e falhas transitórias com backoff, mas continuam falhando imediatamente quando o executável Chromium não está instalado
- **health** — o Comtrade passa a ser sondado pelo endpoint guest sem chave, o dataset `embarques_anec` entra no mapa de impacto, o DuckDB é fechado antes do cache e falhas de abertura tornam o workflow explicitamente degradado. Alertas de recuperação recebem a contagem anterior e `is_recovery`, enquanto probes HTTP lentos repetem a consulta uma vez para descontar cold starts como o da ANA
- **inmet** — `clima_uf()` valida a UF antes de qualquer request, e a ausência de estações operantes passa a levantar `SourceUnavailableError`, permitindo o fallback do dataset em vez de aparecer como erro inesperado
- **datasets** — validações de parâmetros do usuário propagam `InvalidParameterError` imediatamente e não são mais engolidas pela cascata como `SourceUnavailableError: All sources failed`; erros derivados de parsing e dados continuam elegíveis para fallback
- **datasets** — `MetaInfo` preserva cascatas internas das fontes em `attempted_sources`/`selected_source` e propaga `from_cache`, sem renomear adaptadores de fonte simples
- **ana** — consultas ArcGIS migradas de `FeatureServer/0`, indisponível nas quatro camadas publicadas, para `MapServer/0`; os endpoints voltam a responder e a latência deixa de sofrer o timeout do serviço inexistente
- **conab** — ausência do executável Chromium agora falha na primeira tentativa e informa `python -m playwright install chromium`, em vez de repetir um erro permanente até esgotar os retries
- **icmbio** — nomes de campos atualizados para o layout atual do WFS (`sigla_cate` e `uf`): todas as chamadas falhavam porque a fonte aceita HTTP 200 com `ExceptionReport` quando recebe os nomes antigos (`siglacateg` e `ufabrang`). O separador de UFs múltiplas foi alinhado de `;` para `/`
- **utils/geo** — respostas ArcGIS HTTP 200 com objeto `error` agora levantam `SourceUnavailableError` nas consultas de contagem e de página, em vez de serem interpretadas como zero registros. Isso torna explícita a indisponibilidade atual do IFN/SFB (`MapServer not started`) e protege também as camadas da ANA
- **comexstat / exportacao** — `oleo_soja` agora resolve para toda a posição NCM `1507` (óleo bruto, refinado e outras apresentações), enquanto `oleo_soja_bruto` permanece específico para `15071000`. O adaptador do dataset consolida os diferentes NCMs por ano, mês e UF, soma peso líquido e valor FOB e recalcula toneladas; antes, `oleo_soja` era anunciado pelo dataset, mas rejeitado pela fonte primária, e simplesmente adicionar o prefixo geraria chaves contratuais duplicadas
- **exportacao** — o adaptador do fallback ABIOVE agora converte os produtos canônicos (`soja` → `grao`, `farelo_soja` → `farelo`, `oleo_soja` → `oleo` e `milho` → `milho`) e restaura o nome solicitado na saída; antes, somente `milho` era aceito diretamente e os demais fallbacks falhavam ou preservavam o vocabulário da fonte. Totais nacionais agora incluem `uf` nula, e produtos sem cobertura ABIOVE (`cafe`, `algodao`, `acucar`) são recusados antes da rede
- **exportacao** — o fallback ABIOVE não ignora mais o filtro `uf`: como a fonte publica apenas totais nacionais, consultas estaduais agora a consideram indisponível antes da rede, em vez de devolver o total Brasil como se atendesse ao recorte. `ano=None` também volta a usar o último ano civil completo; antes, a chave presente com valor nulo contornava o default e causava `TypeError` ao formatar o nome do XLSX
- **mapbiomas** — os filtros `estado` de `cobertura()` e `transicao()` agora aceitam sigla ou nome completo da UF, com caixa e acentos opcionais (`"Goiás"`, `"Goias"` e `"GO"` convergem para `"GO"`). O parser já gravava apenas siglas, mas a API comparava o nome recebido diretamente com essa coluna e devolvia vazio. Estado inválido agora levanta `ValueError` antes do download, inclusive no caminho municipal de aproximadamente 660 MB
- **cepea** — os filtros `praca` de `indicador()` e `ultimo()` agora comparam slugs sem acento e sem o sufixo `/UF`, inclusive no cache DuckDB: `praca="paranagua"` encontra os registros que preservam o rótulo oficial `"Paranaguá/PR"`. `pracas()` passa a derivar a lista do mapeamento do parser, eliminando locais anunciados que não apareciam nos dados e incluindo os que o parser realmente grava
- **estimativa_safra** — o fallback IBGE LSPA agora consulta o ano civil de colheita: `safra="2022/23"` usa LSPA 2023, não 2022. Sem safra explícita, o LSPA do ano corrente recebe o rótulo da safra que termina nesse ano (`2026` → `2025/26`), evitando resultados plausíveis associados ao período errado
- **queimadas** — o filtro `bioma` agora normaliza aliases sem acento (`"Amazonia"` → `"Amazônia"`) e rejeita valores desconhecidos antes do download. Antes, ambos produziam um conjunto de correspondências vazio e desligavam silenciosamente o filtro, devolvendo todos os biomas
- **bcb.credito_rural** — o filtro `uf` volta a funcionar: o client convertia `"MT"` para o código IBGE `"51"`, mas os endpoints OData `*RegiaoUFProduto` só retornam `nomeUF` e não expõem `cdEstado`; por isso toda consulta com UF terminava vazia. O OData agora recebe `nomeUF eq 'MT'`, enquanto o código IBGE permanece na interface do fallback BigQuery. UF e safra são filtradas server-side e conferidas novamente no client; UF inválida levanta `ValueError` antes de qualquer download
- **producao_anual** — o fallback CONAB agora cumpre o contrato `ibge.pam`: `ano` é mapeado para o segundo ano da safra (`2023` → `2022/23`), UF vira nome de `localidade`, área/produção são convertidas de mil ha/mil ton para ha/ton, produtividade vira `rendimento` e `fonte="conab"` é preservada. `area_colhida` fica nula porque o boletim não a publica (a API CONAB a preenchia artificialmente copiando a área plantada). O nível Brasil agrega as UFs com rendimento ponderado; município falha antes do download porque a CONAB não oferece essa granularidade. Antes, `ano`/`nivel` eram ignorados e o DataFrame sem `ano` sempre terminava em `ContractViolationError`; o teste de fallback também mascarava o defeito usando um mock já no formato IBGE
- **antaq** — `_download_zip` valida os magic bytes de ZIP (`PK\x03\x04`) antes de repassar o conteúdo. O guard de tamanho mínimo deixava passar HTML servido no lugar do ZIP, e o `zipfile.ZipFile` estourava `BadZipFile` cru na API direta — ou `source_unexpected_error` no `_try_sources` do dataset. Agora vira `SourceUnavailableError` categorizado como fonte indisponível, com o motivo real: tamanho, `Content-Type` e **URL final** após os redirects. Quando o host redireciona para o aviso oficial de indisponibilidade, a mensagem diz isso explicitamente em vez de atribuir a um challenge de WAF — a ANTAQ responde tanto `403 Cf-Mitigated: challenge` (UA simples) quanto `301` para o aviso (UA de browser), e a mensagem anterior fixava uma das hipóteses
- **health** — o estado do health check nunca era persistido: o step usava `actions/cache@v4`, que só salva no post-job de job bem-sucedido, e o script encerrava com `sys.exit(1)` sempre que alguma fonte falhava. Como o `acervo_fundiario` falha em todo run, o job nunca ficava verde e todo `record_check()` era descartado — contadores parados, escalonamento e recuperação não confiáveis. Agora o workflow usa `cache/restore` + `cache/save` com `if: always()` e falha em um step final, depois de gravar o estado
- **health** — o alerta de recuperação nunca disparava: `record_check()` gravava o `ok` antes de `should_send_alert()` consultar `get_consecutive_failures()`, e a query conta as falhas posteriores ao último `ok` — que passava a ser o registro recém-inserido, zerando o contador. O contador anterior agora é lido antes da gravação e passado em `prior_failures`
- **health** — recuperação deixou de disparar para incidentes que nunca alertaram. O filtro de categoria avaliava a categoria do check **atual** (um `ok`, sem categoria), então falhas suprimidas — `api_key_missing`, ou uma categoria com a flag desligada — produziam um "Source recovered" sem que nenhum alerta tivesse sido enviado. O novo `get_alertable_failures()` conta apenas as falhas do incidente cuja categoria permitiria alerta, e é essa contagem que alimenta a decisão de recuperação
- **estimativa_safra** — o fallback IBGE LSPA passou a normalizar a resposta SIDRA para o schema do contrato. Para safras que a CONAB não disponibiliza mais (ex.: `safra="2022/23"`), o fallback devolvia o SIDRA cru (`nivel_territorial`, `localidade`, `valor`, `variavel`…) e estourava `ContractViolationError: Missing required columns {safra, levantamento, data_publicacao}`. `_normalize_lspa` reduz a série mensal ao levantamento mais recente, soma as sub-safras que o LSPA separa (milho 1ª/2ª, algodão) e converte Hectares/Toneladas para mil_ha/mil_ton (produtividade recalculada como produção/área colhida). `levantamento` e `data_publicacao` passam a `nullable` no contrato `conab.safras` — o LSPA não os possui. Produtos sem série mensal nacional no LSPA (ex.: trigo, que retorna todos os valores nulos) fazem o fallback levantar `SourceUnavailableError` em vez de `ContractViolationError` por dtype de coluna vazia
- **ibge / producao_anual** — nível territorial inválido (typo ou capitalização, ex.: `"Brasil"`, `"estado"`) caía em UF silenciosamente no `resolve_ibge_code` (`.get(nivel, "3")`), retornando dados de UF rotulados como o nível pedido. Agora `resolve_ibge_code` levanta `ValueError` com os níveis válidos (respeitando `NIVEL_MAP_HISTORICO`, que inclui `regiao`), e `producao_anual` valida o nível antes de tentar as fontes — evita o `ContractViolationError` enganoso do fallback CONAB
- **abiove / anec** — produto desconhecido em `exportacao()`/`embarques()` e afins passava cru pelo filtro (`ABIOVE_PRODUTOS.get(key, key)` / `PRODUTO_ALIASES.get(key, key)`) e retornava DataFrame vazio, indistinguível de "fonte sem dados". Novo `resolve_produto` valida o input do usuário e levanta `ValueError` com a lista de produtos válidos, consistente com as demais fontes. O `normalize_produto` usado pelos parsers para normalizar os dados brutos mantém o passthrough

- **ANTT diário** — teto por CSV passa a 512 MiB, spool a 1 GiB e transferência a 3 GiB para comportar os recursos oficiais diários; tamanhos do catálogo 2024–2026 entram em regressão, e o arquivo diário de 2025 foi lido e validado até EOF ao vivo. Limites do parser continuam explícitos
- **ANTT WAF** — HTML HTTP 200 volta a gerar `SourceUnavailableError`, com recibos preservados; enriquecimento opcional pode falhar sem perder tráfego, enquanto filtros geográficos continuam exigindo cadastro disponível
- **ANEC** — rodapés com anos não são confundidos com o cabeçalho da comparação anual; pares ausentes/incompatíveis continuam sendo erros. Anos sem categoria publicada usam cache negativo com o TTL da listagem; zero desativa o cache
- **ANP catálogo** — período válido sem publicação gera `SourceUnavailableError` com endereço e cobertura do catálogo; datas malformadas ou invertidas continuam gerando `InvalidParameterError`
- **tipagem e scripts** — aliases de DataFrame são explícitos em custos CONAB, inclusive sem Polars; `scripts` passa a pacote, o import do gerador CONAB é único, e os consumidores de NASA POWER/ComexStat acompanham os retornos atuais. `mypy agrobr/ scripts/` valida o conjunto
- **documentação** — migração e contratos ANTT/CONAB alinhados à versão 3.0, ANEC à comparação anual 1.2; dez datasets recebem páginas de contrato PT/EN com tipos, nulabilidade, identidade, limites e exemplos, e a navegação cobre os 52 datasets
- **INCRA andamento** — `andamento_quilombola` e `vinculos_quilombolas` aceitam o PDF republicado pelo INCRA em 17/09/2026 sob o nome de 08/06/2026 (conteúdo de 03/09/2026, 649 processos, exportação nativa do Excel): o rótulo regional mesclado que o PDF redesenha oculto nas páginas de continuação e a nova caixa de contagens da última página são reconhecidos. A edição passa a ser a data interna do PDF; quando diverge do nome do arquivo, `warn_once` e `validation_warnings` citam as duas datas, que ficam em `source_details` (`publication.file_date`, `publication.internal_edition`); `edicao=` casa a data interna e uma edição não publicada gera `InvalidParameterError` com a atual
- **INCRA andamento** — redirecionamento da página ou do PDF do INCRA passa a `SourceUnavailableError` ("Redirecionamento administrativo não demonstrado"); antes o `raise_for_status` levantava `HTTPStatusError` genérico e o ramo que classificava o 3xx nunca era alcançado
- **ANEC embarques** — `embarques()` e `datasets.embarques_anec` recusam `tipo`, `produto` e `semana` inválidos com `InvalidParameterError` antes de consultar o catálogo ou baixar o PDF; antes o produto desconhecido saía como `ValueError` depois do download (no dataset, como `SourceUnavailableError`) e a semana fora de 1–53 como edição indisponível
- **ANEC catálogo** — artigo da categoria anual cujo título não segue `ANEC - NN.AAAA` é ignorado com `UserWarning` que cita o título; antes derrubava a busca por semana e `articles_disponiveis` e, sendo o mais recente, virava a edição padrão
- **CONAB, quadro Brasil** — `brasil_total` ganha a coluna `grupo`, com o título do bloco da linha: `Cores`, `Preto` e `Caupi` das três épocas do feijão e os dois `SUBTOTAL` (verão e inverno) saíam com a mesma chave produto/safra; agora `FEIJÃO 1ª/2ª/3ª SAFRA` e `CULTURAS DE INVERNO` os distinguem
- **IBGE, medidas em `float64`** — `abate_trimestral` (`animais_abatidos`, `peso_carcacas`), `censo_agropecuario`, `pecuaria_municipal` e as demais pesquisas SIDRA entregavam `int64` quando todos os valores da resposta eram inteiros, apesar de os contratos prometerem `float64`; a camada IBGE fixa `float64` nas medidas (valores inalterados)
- **ANTT fluxo, referência mensal fora do dia 1** — os CSVs mensais de 2020, 2021 e 2023 trazem 56, 3.258 e 734 linhas com `mes_ano` fora do dia 1 (fim de mês, série de preenchimento, digitação) e o parser recusava o arquivo inteiro: nenhuma consulta de fluxo desses anos funcionava. A linha passa a valer para o seu mês, com aviso que nomeia a linha e contagem em `source_details`
- **ANTT fluxo, volume que não é contagem** — volume fracionário (2013: 4 linhas; 2023: 36) ou negativo (2015: 3) derrubava o ano inteiro; a linha sai com aviso que nomeia arquivo, linha, valor e concessionária, e o ano segue
- **ANTT fluxo, filtro geográfico** — com `uf`/`rodovia`, qualquer praça do país sem vínculo único no cadastro (em 2026, 15 pares em GO, MG e PR) levantava `SourceUnavailableError`, e o filtro não funcionava em nenhum ano; o registro sem vínculo sai do filtro com aviso e contadores em `source_details["geographic_filter"]`
- **ANTT fluxo, bloco publicado duas vezes** — ECOSUL dez/2021 está no CSV oficial duas vezes com linhas idênticas e o mês saía dobrado; bloco concessionária × mês repetido com linhas idênticas mantém uma cópia, com aviso. Repetição com valores diferentes (CONCEBRA 2021–2023, linhas diárias rotuladas no dia 1) continua somando
- **ANTT fluxo, CSV sem cabeçalho** — arquivo sem cabeçalho era lido por uma ordem de colunas tirada de uma amostra sintética (`fluxo_v2_sample`, `needs_real_data: true`); a ANTT publica com cabeçalho e em outra ordem, então um arquivo assim sairia com colunas trocadas em silêncio. Passa a `ParseError` ("CSV sem cabeçalho; layout não suportado"); as duas amostras sintéticas saem da suíte
- **Comex Stat, códigos extintos e desdobrados** — aliases presos a um código voltavam vazios ou parciais depois de mudança na nomenclatura: `etanol` (`22071000`, desdobrado em 2011) trazia menos de 1 % da exportação desde 2012 e nada em 12 dos 14 anos; `carne_frango` (`02071400`, desdobrado em 14 subitens em 2024) trazia 0,02 % dos pedaços congelados de 2025; `soja`, `soja_semeadura`, `trigo` e `acucar` saíam vazios de 1997 a 2011 e `dap` de 1997 a 2018. Cada alias soma os códigos vigentes em cada ano, inclusive no ano de transição (soja, jan/2012: 1.012 mi kg, antes 593). `ssp` e `tsp`, sem equivalente exato antes de 2017, levantam `InvalidParameterError` antes da rede.
- **Comex Stat, prefixo NCM** — `produto` aceita prefixo NCM de 2 a 8 dígitos, como a documentação prometia (antes só alias); o prefixo seleciona todos os códigos que começam por ele.
- **docs** — o exemplo `comexstat.importacao('fertilizante', ...)` do README PT/EN usava um alias inexistente; agora usa `fertilizantes`.
- **Acervo Fundiário, UFs publicadas** — SIGEF e SNCI recusavam 12 e 17 UFs por uma lista fixa (`SIGEF_UFS_DISPONIVEIS`, `SNCI_UFS_DISPONIVEIS`), com mensagem que culpava a fonte; o INCRA publica os dois temas nas 27 UFs (sondagem de 22/09/2026). As listas saem: UF válida vai à rede e arquivo ausente continua `SourceUnavailableError` (HTTP 404)
- **Acervo Fundiário, assentamento sem geometria** — o shapefile nacional traz um registro sem geometria (MT0349000) e `assentamentos_geo()` sem filtro ou com `uf="MT"` levantava `AttributeError`; o registro sai com geometria nula e o reparo de geometria inválida só atua nas não nulas
- **Acervo Fundiário, proveniência** — `MetaInfo.source_url` passa a ser a URL codificada que o cliente baixa, não o nome do arquivo sem percent-encoding
- **Censo agropecuário, tema `despesa_adubos`** — `datasets.censo_agropecuario('despesa_adubos')` era recusado antes da rede, embora o contrato do dataset e a fonte ofereçam o tema; agora entrega as 54 células da tabela SIDRA 6899 por UF (estabelecimentos e valor da despesa com adubos e corretivos, em mil reais).
- **Municípios IBGE, nomes da DTB 2025** — 1400605, 2400208 e 2401206 passam a sair com o nome oficial atual (São Luiz do Anauá, Assú e Arez); `municipio_para_ibge` continua aceitando a grafia anterior (São Luiz, Açu, Arês)
- **Municípios IBGE, busca sem repetição** — `buscar_municipios` devolvia o mesmo município duas vezes quando o termo casava o nome atual e o anterior (ex.: "São Luiz" em RR); agora cada `codigo_ibge` aparece uma vez
- **Acervo Fundiário, doc do SNCI** — a doc dizia "parcelas certificadas pré-2013"; os DBFs oficiais têm certificações SNCI de 2014 a 2016 (SC, AL e RR): o SNCI é o sistema anterior ao SIGEF, sem corte por data
- **ANA, paginação da Hidrografia** — o servidor devolve uma feição a menos que o pedido em cada página da camada Hidrografia com filtro espacial, e a paginação por offset perdia a feição da fronteira (1.046 de 1.047 trechos num recorte real; `max_features=1020` entregava 1.019); as camadas da ANA passam a paginar por chave (`orderByFields=OBJECTID`, `OBJECTID > último`) até completar a contagem oficial. ANA e SFB conferem a completude: página que para antes da contagem ou que não avança o OID levanta `SourceUnavailableError` com quantas feições faltam, em vez de devolver resultado parcial; página ilegível ou sem `OBJECTID` levanta `ParseError`
- **ANA e SFB, resultado `_geo` vazio sem CRS** — recorte sem feições (contagem oficial zero, como um bbox no Atlântico) devolvia `GeoDataFrame` com `crs=None`; agora sai em EPSG:4326, como os não vazios
- **ANA, unidade das vazões da demanda na doc** — as colunas `vazao_*` de `demanda_irrigacao` não tinham unidade na doc; a camada oficial é "Vazão de Retirada para Irrigação (m³/s)" e os valores saem em m³/s, sem conversão
- **Embrapa Solos, docs** — tipos das 9 medidas (texto, não `float`), CRS EPSG:4326 (a doc dizia 4674, inclusive para o `bbox`), linhas como horizontes/camadas (34.464 registros de ~9 mil pontos, não 34 mil perfis), formato JSON e as colunas e parâmetros que faltavam, em PT e EN
- **FUNAI, docs e licença** — data de atualização declarada como texto, 665 TIs (não ~740), JSON, CRS 4674 na camada com saída em 4326, as 19 colunas e os parâmetros; a licença passa a citar o termo da FUNAI para geoprocessamento e mapas (reprodução com citação da fonte) em vez de só o rodapé CC BY-ND 3.0 do portal, em PT e EN
- **SICOR / credito_rural — safra validada antes da rede** — `"2023/25"` era lido em silêncio como 2023/2024, `"2024/2023"` consultava 2024/2025, `"23/24"` voltava vazio e `"abc"` estourava `ValueError` no cliente. Agora só `AAAA`, `AAAA/AA` e `AAAA/AAAA` com anos consecutivos passam; o resto, inclusive `safra=""` (antes lido como sem filtro), levanta `InvalidParameterError` antes da rede.
- **SICOR / credito_rural — `finalidade` igual nas duas fontes** — pelo OData saía a finalidade como foi passada (`Custeio`) e pelo fallback BigQuery o valor da fonte (`CUSTEIO`); agora as duas publicam a finalidade pedida em minúsculas.
- **BCB, docs do crédito rural** — `area_financiada` sai sempre nula pelo OData (o custeio publica `AreaCusteio` vazio em 2.583 de 2.583 registros de 10 consultas; investimento e comercialização não trazem área) e só o fallback BigQuery a preenche; o `0152` sai como PROIRRIGA também antes de 07/2021, quando era o Moderinfra; `safra` aceita `AAAA`, `AAAA/AA` e `AAAA/AAAA` e, sem ela, não há filtro (a doc dizia "safra mais recente"); `mandioca` na lista de produtos; PT e EN.
- **credito_rural — `mandioca` no catálogo** — `datasets.info`/`list_products` omitiam `mandioca`, que o SICOR mapeia e o dataset aceita; `products` agora são as culturas de `SICOR_PRODUTOS`, com teste de guarda.
- **ZARC — nove culturas das safras 2017/2018 a 2023/2024** — o censo das 12 tábuas oficiais (as 9 edições intermediárias capturadas em 23/09/2026 e os 3 corpos da reconciliação de 18/09/2026) achou 107 rótulos, 9 fora do catálogo: `Milho`, `Arroz Irrigado`, `Feijão 1ª Safra`, `Trigo Irrigado`, `Mamona Semi-árido Sequeiro`, `Cevada Grãos Irrigada`/`Sequeiro` e `Aveia Sequeiro`/`Irrigada`. O filtro `cultura=` os recusava antes da rede e, sem filtro, saíam com nome improvisado (`feijao_1a_safra`, `mamona_semi-arido_sequeiro`). Agora são 107 culturas e a cultura ausente na safra pedida informa as safras em que aparece; guia de migração §35.
- **ZARC — nome do município sem acento** — `municipio="cerqueira cesar"` ou `"sao paulo"` devolviam tábua vazia, sem aviso, porque o nome era comparado só sem diferenciar caixa; agora a comparação ignora caixa e acento no download, no cache DuckDB e com `use_cache=False`, como o filtro `cultura=`.
- **IBAMA, arquivo vigente** — `embargos()`/`embargos_geo()` liam o ZIP `dadosabertos.ibama.gov.br/dados/SIFISC/.../termo_embargo_csv.zip`, abandonado pela fonte e congelado em 03/05/2026 (113.878 termos); em 23/09/2026 faltavam 2.454 termos (2.248 embargos lavrados em 2026), 525 desembargos e 237 cancelamentos. Agora leem o CSV vigente do conjunto "Fiscalização - termo de embargo" (atualização diária; ~208 MB sem compressão, a fonte não publica ZIP), e `meta.source_details["ultima_atualizacao_relatorio"]` informa a edição lida.
- **IBAMA, docs e licença** — fonte, índices e READMEs diziam WFS, ~89K/~114K termos, atualização mensal e ODbL; agora: CSV de dados abertos, ~116 mil termos, atualização diária, licença "Outra (Aberta)" do conjunto vigente com o Decreto 8.777/2016 (a ODbL era do conjunto antigo em shapefile), fuso das datas, SRID não declarado no CSV, ponto `0`/vazio, datas sujas e a diferença de `bbox` entre tabular e `_geo`, em PT e EN
- **IMEA, cadeias 5 e 10** — a Conjuntura Econômica (5) só era aceita pelo número e o Custo de Produção (10) não era aceito; agora `cotacoes("conjuntura")` e `cotacoes("custo_producao")` (ou `"5"`/`"10"`); cadeias inativas na fonte (6, 9, 11) levantam `ValueError`.
- **IMEA, chave ausente** — chave publicada ausente no JSON (ex.: `Valor`) sumia com a coluna em silêncio; agora levanta `ParseError` com o nome da chave.
- **IMEA, registro publicado em duplicata** — em 25/09/2026 o IMEA publicou 23 registros do indicador `708192508838936580` (R$/sc) quatro vezes cada, e `imea.cotacoes` repassava as 69 cópias. O registro igual em todas as colunas sai uma vez só, com aviso e a contagem em `source_details["duplicatas_colapsadas"]`. A chave (indicador, localidade, data, safra e unidade) repetida com valores diferentes sai inteira, com aviso e a contagem em `source_details["chaves_repetidas"]`, sem erro, que derrubaria a cadeia inteira por um indicador. O `scripts/reconciliar_imea.py` compara contra as linhas oficiais únicas e confere a contagem colapsada.
- **MetaInfo, proveniência física** — 21 fontes passam a informar o corpo que deu o dado: `raw_content_hash` com o SHA-256 completo, `raw_content_size` e `fetch_timestamp` em UTC. São ABIOVE, ANDA, ANEC, INMET, CEPEA, Acervo Fundiário, ANA, ANTT (praças), B3, CFTC, CONAB, DERAL, IBAMA, IBGE, ICMBio (`ucs_geo`), IMEA, PSR, Queimadas, SICAR, UNICA e Rio Verde. No CEPEA, o hash deixa o prefixo `sha256:` de 64 bits. Na ANEC, deixa o fingerprint MD5 da estrutura do PDF, que vai para `source_details["layout_fingerprint"]`. `source_url` passa a ser o recurso: no CFTC, a consulta com os filtros; na B3, o download com o token como `[REDACTED]` e o ticket em `source_details["ticket_url"]`; no IBGE, a consulta SIDRA, com a página em `source_details["pagina"]` e SHA e bytes por consulta. Com vários corpos, o hash e o tamanho do topo ficam nulo e zero. No IMEA, o catálogo de indicadores entra em `source_details` com SHA-256 e bytes. `build_source_meta` recebe `raw_content_size`.
- **Código morto, parte 2** — guardas redundantes ou inalcançáveis saem de INCRA, FUNAI, Embrapa Solos, ANTT, Acervo Fundiário, ANA, IMEA, Rio Verde e SFB, sem mudar dado. `utils.validate_bbox` levanta `InvalidParameterError` (subclasse de `ValueError`). No IBAMA, área com ponto levanta `ParseError` em vez de "12.5" virar 125. A paginação ArcGIS geo deixa de logar `<fonte>_truncated` em página cheia quando o resultado está completo.
- **Código morto, parte 3** — código sem consumidor e guardas redundantes ou inalcançáveis saem de ANEC, SICAR, BCB/SICOR, Defensivos, Lista Suja, RNC e PSR, sem mudar dado; duas guardas ficam como defesa declarada. Saem nomes públicos sem uso na produção (lista e alternativas no guia de migração, §50): `utils.concat_csv_pages`, `alt.sicar.parser.parse_imoveis_csv`, `anec.models.normalize_produto`, os invólucros `*_csv` do Defensivos e do RNC, `rnc.client.fetch_registradas`/`fetch_protegidas`, `alt.mapa_psr.client.download_csv`/`fetch_periodo` e 3 constantes da Lista Suja. O parser do SICOR deixa de resolver fonte de recurso, modalidade e atividade nas agregações `uf` e `programa`, que não as publicam, e o aviso "código desconhecido" sobre essas 3 dimensões deixa de sair (54 por consulta no corpo real); os nomes voltam no `agregacao="registro"`, pelas tabelas oficiais, e o `defensivos.cache.invalidate()` passa a constar da doc da fonte.
- **Código morto, parte 1** — código sem consumidor, guardas redundantes ou inalcançáveis e layouts sem publicação saem de IBGE, CONAB, `contracts`, `datasets`, `normalize`, desmatamento, MapBiomas, queimadas, B3, CONAB CEASA, ANDA, DERAL, INMET, NASA POWER, ComexStat, ABIOVE e CEPEA, sem mudar dado publicado; uma guarda fica como defesa declarada. `anda.entregas` e `datasets.fertilizante` perdem `uf` (a ANDA só publica o total nacional), a ABIOVE valida `produto` e `agregacao` antes da rede e as docs passam a descrever a cadeia de encoding que roda (UTF-8, Windows-1252, ISO-8859-1). Nomes públicos que saem e alternativas no guia de migração, §50.
- **ANEC, boletins com 4 produtos** — as W1 e W2/2026, com soja, farelo, milho e trigo (DDGS e sorgo entram na W3/2026), eram recusadas com `ParseError`; agora saem lidas pelo nome, com o valor na coluna pelo intervalo entre os rótulos e o porto pelas palavras antes do 1º valor (sem isso, na W1, o trigo da semana corrente e o farelo de São Francisco do Sul sumiriam). Coluna sem nome de produto segue recusada (W14/2026). A soma dos portos de cada coluna semanal passa a ser conferida com a linha TOTAL do boletim, com aviso na divergência. As 26 edições de 6 produtos da W3 à W29/2026 dão a mesma saída de antes.
- **ANEC, número partido e linhas fundidas no quadro semanal** — o pdfplumber parte números em 2 palavras coladas (`3` + `72.958`), e o parser só juntava o pedaço que começava com ".": na W36/2026, a conferência da linha TOTAL avisava que o boletim publicou 3, 5 e 9 t onde publicou de 58.300 a 984.936 t; em 2025, 57 valores de porto saíam só com o 1º dígito (W14/2025: milho de São Francisco do Sul 6 t × 62.185 t), sem aviso, porque o TOTAL partia igual. Agora 2 palavras numéricas vizinhas se juntam quando formam um número com separador de milhar. Com y_tolerance de 2 pt, o BELÉM se fundia com o RIO da linha de baixo e as 2 linhas saíam da tabela em 48 edições (7 valores do Rio perdidos, como os 11.435 t de farelo da W9/2025); agora 1 pt. Conferido pelo texto do PDFium nas 89 edições de W1/2025 a W37/2026 (menos a W14/2026): 16.112 células iguais e nenhum aviso. `parser_version` 6.
- **Desmatamento, seleção acima do limite recusada antes da descarga** — `datasets.desmatamento` baixava até o `max_registros` (padrão 50.000) e só depois recusava a agregação com `ContractViolationError`: o exemplo da doc (Cerrado, PRODES 2023, 68.620 feições no WFS) levava 6,5 min para falhar. Agora a contagem do WFS é conferida antes, e a recusa vem na hora, com a contagem na mensagem e nenhuma feição baixada; os exemplos da doc cabem no limite (Cerrado 2023 no DF, DETER do Acre no 1º trimestre de 2024) e a doc dá o `max_registros=None` para a seleção inteira.
- **Rio Verde, safra 2023/24** — a competição de cultivares de soja 2023/24, publicada pela fundação, era recusada como "não disponível"; agora `rio_verde.ensaio_soja("2023/2024")` a lê (76 linhas). A 2022/23, publicada num layout de 3 épocas e sem média, é recusada com esse motivo em vez de "não disponível". Argumento desconhecido levanta `TypeError` antes da rede.
- **ICMBio, CRS da geometria** — `ucs_geo()` não pedia `srsName` ao WFS, recebia o corpo declarado em EPSG:4674 e o publicava com o rótulo EPSG:4326, porque `parse_geojson_base` ignorava o `crs` do corpo (no corpo oficial de 23/09/2026 as coordenadas diferem só 1e-8 grau, mas o rótulo não era o declarado); agora o pedido leva `srsName=EPSG:4326` e `parse_geojson_base` levanta `ParseError` quando o corpo declara CRS diferente do pedido ou ilegível.
- **SFB, ano de criação do CNFP** — o campo `anocriacao` do serviço é texto com a data inteira (`11-06-1999`, `22/06/2011`…) e virava nulo em `pd.to_numeric`: só 123 de 20.829 registros saíam com ano. Agora sai o ano quando o texto tem um único ano (15.068 registros); branco ou `-` fica nulo, e data composta com anos diferentes (sobreposição de unidades, 1.043) fica nula com o aviso `sfb_ano_criacao_ambiguo`.
- **CEPEA, cache vencido** — `indicador()` servia do DuckDB a coleta anterior à virada das 18h BRT sem buscar de novo (decidia só pela presença das datas úteis) e publicava `fetched_at` = instante da chamada e `cache_expires_at` = próxima virada contada da chamada; agora a última coleta do produto vale até as 18h BRT do dia útil seguinte (sábado e domingo não contam), depois disso o agrobr busca de novo (fonte fora → cache com `StaleDataWarning`), e o `MetaInfo` do cache traz a coleta real e o vencimento contado dela. `ultimo()` segue a mesma virada.
- **Acervo Fundiário, metadado do cache** — um ZIP servido do cache depois de o HEAD confirmar `ETag`/`Last-Modified` saía com `from_cache=False`, `fetched_at` = instante da chamada e `source_details` vazio; agora o `MetaInfo` traz `from_cache=True`, a coleta original do ZIP e a revalidação (`revalidado_em`, `etag`, `last_modified`), e num download novo `fetched_at` é o próprio download. Cache antigo sem `fetched_at` no `{UF}.json` é baixado de novo.
- **CONAB série histórica, zero publicado** — todo zero da planilha virava nulo (fora o café): amendoim 2ª BA 2011/12 saía com produção e produtividade nulas em vez de 0,0, e a média de 2010–2011 saía 6,2 em vez de 3,1; agora o zero publicado sai `0.0` (inclusive UF sem produção naquela safra), e só a coluna de safra zerada em todas as UFs, que é safra não levantada (trigo 1976, primeiras safras de canola, triticale, girassol, feijão 3ª e milho 2ª), fica fora. Medido nas 40 planilhas de grãos e cana: 7.533 zeros de safra não levantada e 43.981 publicados; a série tem mais linhas, com `0.0`.
- **UNICA, período do resumo** — a Tabela 2 saía sempre como `quinzena`, pelo número da tabela; na edição de 01/07/2026 ela é a posição MENSAL de junho (cana 69.791 mil t), que o agrobr rotulava como quinzena. Agora o período vem do título (ACUMULADA, QUINZENAL ou MENSAL) e título desconhecido vira `ParseError`.
- **UNICA, valor ausente no resumo e nas séries** — o rótulo da linha terminava no primeiro número, então `n/d` ou `-` no primeiro valor viravam parte do rótulo e a linha sumia em silêncio; nas séries quinzenais, a mesma marca levantava `ParseError` e derrubava a leitura da edição inteira; a checagem dos produtos obrigatórios olhava a união dos períodos. Agora marca de ausência vira valor nulo (linha mantida) no resumo e nas séries, e produto obrigatório faltando num período vira `ParseError`.
- **ANEC, metadado do cache** — o PDF servido do cache de disco saía com `from_cache=False` e `fetched_at` = instante da chamada; agora o `MetaInfo` traz `from_cache=True`, a coleta original gravada no `meta.json` e `source_details["media_updated_at"]` (revisão da edição conferida na listagem). `fetch_pdf_bytes` e `fetch_latest_pdf` mantêm o retorno.
- **B3, vencimento das opções** — `posicoes_abertas` e `oi_historico` publicavam `vencimento_mes`/`vencimento_ano` nulos em 96 % das opções: na opção, o código do OI é MYOA (ex.: `VVJK`), não MYY, e o erro de leitura era engolido; o filtro `vencimento="V26"` comparava o código bruto e devolvia só o futuro. Agora mês e ano são os do contrato também na opção (do ticker da série, conferido com a letra do mês do código), ticker ou código fora do padrão levanta `ParseError`, e o filtro casa o futuro e as opções do mês do contrato, ou o código publicado de uma opção; outro formato levanta `InvalidParameterError` antes da rede. Contrato `b3.posicoes_abertas` 1.1, com `vencimento_mes`/`vencimento_ano` não nulos.
- **USDA PSD no gateway novo, com o cadastro pelos catálogos oficiais** — `usda.psd` e `datasets.oferta_demanda_global` (parser 2, contrato 1.1) passam a consultar `https://api.fas.usda.gov/api/psd` com a chave no cabeçalho `X-Api-Key`; a OpenData antiga dava 500 em toda chamada. O corpo em camelCase, só com IDs, é rotulado pelos catálogos oficiais guardados no pacote (`commodityAttributes`, `commodities`, `countries` e `unitsOfMeasure`). O cadastro deixa de trocar 6 dos 9 atributos (a produção saía como estoque inicial, o consumo como produção e o estoque final como oferta total), o farelo de soja (`4233000` é o óleo de algodão; o farelo é `0813100`) e a UE (`E2` é a EU-15; a UE é `E4`). O consumo é o 125, o 126 no açúcar e o 142 no algodão, que ganha `perdas` (150); a oferta fecha com a distribuição nos 34 recortes conferidos. Colunas novas `attribute_id`, `unit_id`, `last_update_year` e `last_update_month` (última atualização da série, nula quando o PSD publica `00`). Commodity, país e atributo fora dos catálogos levantam `InvalidParameterError` antes da rede (o gateway devolveria `[]`); corpo fora do layout e código fora do catálogo local levantam `ParseError`, e o pivot deixa de cair no formato longo em silêncio. Nomes públicos: `usda.models.PSD_COLUMNS_MAP` sai, e os `usda.client.fetch_psd_*` devolvem `RespostaPSD(url, corpo, dados)` em vez da lista (guia §49). `MetaInfo` com a URL consultada, o SHA-256 e o tamanho do corpo. `scripts/reconciliar_usda.py` confere ao vivo os catálogos e os recortes. Sem `AGROBR_USDA_API_KEY`, o health, o `agrobr doctor` e a matriz live marcam o USDA como `not_verified` (não verificado), em vez de aviso ou indisponibilidade conhecida
- **B3, proveniência de `ajustes` e `historico`** — `b3.ajustes` e `futuros_agricolas(tipo="ajustes")` saíam com `raw_content_hash` nulo e `raw_content_size` zero. Passam a informar o `PRyymmdd.zip` recebido: SHA-256 completo, tamanho e `fetched_at`/`fetch_timestamp` na hora da aquisição. A B3 remonta o ZIP externo a cada pedido (mesmo tamanho, SHA diferente), então a identidade estável vai em `source_details`: nome, SHA-256 e bytes do ZIP interno e do XML lido. `b3.historico` lista cada dia recebido em `source_details["corpos"]` (data, URL, SHA-256, bytes, hora da aquisição, ZIP interno e XML); com um corpo só, o topo é o dele; com vários, o topo fica nulo e zero, e `fetched_at`/`fetch_timestamp` são a aquisição mais recente.
- **`as_polars` pelo contrato** — com `as_polars=True`, os 53 datasets tipam cada coluna pelo contrato (`int` → `Int64`, `float` → `Float64`, `str` → `String`, `bool` → `Boolean`; data toda nula → `Datetime("ns")`), em vez de inferir do dado: a coluna preenchida com `pd.NA` saía `Null`, e o `pl.concat` quebrava (`producao_anual("soja")` com `producao_anual("cafe")` no `condicao_produto`; a `exportacao` da ComexStat com a do fallback ABIOVE no `uf`). Guia de migração, seção 55.
- **`preco_diario`, `fetched_at` da fonte `cache`** — quando o CEPEA falha e o dataset lê o DuckDB, o `fetched_at` passa a ser a coleta original dos registros (`max` do `collected_at`), como no `cepea.indicador`; era a hora da chamada.
- **Embrapa Solos, texto com dupla codificação** — a Embrapa publica parte dos textos dos perfis com dupla codificação (UTF-8 lido como Latin-1: "AptidÃ£o", "SÃ£o Carlos"), e o agrobr repassava assim; filtro e junção por nome de município saíam vazios. O texto que volta inteiro por Latin-1 → UTF-8 e tem a assinatura ("Ã" ou "Â" seguido de U+0080 a U+00BF) passa reparado, com a contagem por coluna em `validation_warnings`; texto legítimo com "Ã" fica. O `reconciliar_embrapa_solos.py` usa a mesma regra no CSV oficial.
- **Focus, `source_url`** — apontava a última página da consulta, vazia (`$skip`). Passa a ser a consulta, sem `$top` e `$skip`; as páginas seguem em `source_details["resources"]`.
- **B3, corpos do `oi_historico`** — `b3.oi_historico` lista cada dia recebido em `source_details["corpos"]` (data, URL com o token como `[REDACTED]`, SHA-256, bytes, hora da aquisição e `ticket_url`). Com um corpo só, o topo é o dele; com vários, nulo e zero, e `fetched_at`/`fetch_timestamp` são a aquisição mais recente.
- **ANTT, tamanho do `fluxo_pedagio`** — `raw_content_size` somava todos os bytes recebidos, com tentativas e falhas, enquanto `raw_content_hash` é o SHA do manifesto. Passa ao tamanho do manifesto; o total recebido vai em `source_details["received_bytes"]`, e o dos CSVs que entraram no dado, em `data_file_bytes`.
- **CONAB, `safras` com produto desconhecido** — levantava `ParseError` depois de 4 requisições; passa a levantar `InvalidParameterError` antes da rede, com os produtos válidos.
- **SGS e PTAX, `source_url`** — apontavam o último recurso (o último bloco de datas no SGS; a última página na PTAX); passam a ser a consulta: no SGS, o intervalo inteiro; na PTAX, sem `$top` e `$skip`, como no Focus.
- **Custos CONAB, tamanho do `MetaInfo`** — `raw_content_size` passa ao tamanho do manifesto; o total recebido vai em `source_details["received_bytes"]`, e o da planilha, em `data_file_bytes`.
- **Embrapa Solos, o que sobra com dupla codificação** — o aviso de dupla codificação ganha a contagem por coluna do texto com a assinatura que a fonte publica sem volta (cortado no meio de uma sequência UTF-8, com "�" ou "€"), que fica como publicado. O `PARSER_VERSION` da Embrapa passa a 3, porque o texto reparado muda a saída.
- **NASA POWER, fonte por período** — `nasa_power.clima_ponto`, `nasa_power.clima_uf` e o `datasets.clima` pela rota NASA publicam as fontes que o cabeçalho de cada consulta declara (`source_details["fontes"]` e `["fontes_por_bloco"]`) e o trecho de baixa latência (`["periodos_baixa_latencia"]`), com um aviso por fonte em `validation_warnings`: o mês corrente é GEOS-IT, que a NASA substitui pelo MERRA-2 depois, e a radiação recente é FLASHFlux, que o SYN1deg substitui. O cabeçalho só dá as fontes da janela; num bloco com as 2, o início do GEOS-IT sai da regra mensal da NASA quando há um só dia 1 no bloco. A doc dizia que toda a série era MERRA-2.
- **`exportacao` e `importacao` pelo contrato** — saem só com as colunas do contrato, na ordem dele, pelas 2 fontes da exportação. O `volume_ton` (t = `kg_liquido` / 1000), que a ComexStat e a ABIOVE já entregavam, entra no contrato como coluna opcional (exportação 1.1, importação 1.2), e o `receita_usd_mil` do fallback ABIOVE (o `valor_fob_usd` em mil) sai. Guia de migração, seção 60.
- **ANDA, `agregacao` desconhecida** — `anda.entregas(agregacao="semanal")` baixava o PDF e devolvia o detalhado em silêncio; passa a levantar `InvalidParameterError` antes da rede, como a ABIOVE. Guia de migração, seção 61.
- **`normalizar_cultura` e os códigos da ANEC** — `normalize.crops.normalizar_cultura("soybean_meal")` devolvia o próprio código; passa a `farelo_soja`, como `soybean meal`. Os 5 códigos de produto da ANEC chegam ao nome canônico do agrobr (`ddgs` não tem equivalente), e a tabela das docs da ANEC é conferida por teste.
- **MapBiomas estadual, código de classe não inteiro** — uma célula `class`, `class_from` ou `class_to` sem código inteiro recusava a consulta com o `ContractViolationError` genérico do contrato; passa a `ParseError` com a linha e o valor publicado, como no municipal.
- **CEPEA, sem rede e sem cache** — `cepea.indicador` devolvia a tabela vazia, sem erro nem aviso e com `from_cache=True`, e `cepea.ultimo` levantava `ParseError`; os 2 passam a levantar `SourceUnavailableError`, com `attempted_sources`. `from_cache` só é verdadeiro quando algo veio do cache, e `cache_expires_at` sai nulo quando nada foi servido (inclusive com `offline=True` e cache vazio). Guia de migração, seção 64.
- **CEPEA, coleta dentro da validade** — a 2ª chamada idêntica voltava à fonte enquanto faltasse no cache um dia útil da janela, o que acontece sempre num feriado ou antes da publicação do dia; com a última coleta do produto dentro da validade, a resposta passa a sair do cache. Sem coleta registrada, a checagem de lacuna continua.
- **CONAB, validade sem cache** — `conab.safras` carimbava 24 h de `cache_expires_at`, e a doc prometia um cache que a fonte não tem; o campo passa a nulo, e a doc diz que cada chamada baixa a publicação.
- **`preco_diario` pela fonte "cache"** — sem `inicio` e `fim`, lia o histórico inteiro do cache; passa a usar a mesma janela padrão do `cepea.indicador` (365 dias até hoje). Com período explícito, nada muda.
- **IBGE, `uf` com nível sem filtro** — `pam`, `ppm`, `censo_agro`, `censo_agro_historico`, `silvicultura`, `extracao_vegetal` e o `producao_anual` com `uf` e `nivel="brasil"` (ou `"regiao"`) devolviam o agregado, sem o filtro e sem aviso; passam a levantar `InvalidParameterError` antes da rede. Guia de migração, seção 65.
- **CLI, `cepea indicador --ultimo`** — pegava a última linha da tabela (`tail(1)`), outra praça que a do `cepea.ultimo`, e no leite uma UF em vez do Brasil; passa a chamar o `cepea.ultimo`, e a linha sai com as mesmas colunas, a mesma ordem e os mesmos tipos da tabela sem `--ultimo`. Com `--inicio` ou `--fim`, que o `ultimo` não usa, a CLI recusa em vez de ignorar.
- **CEPEA, `ultimo(offline=True)` sem cache** — levantava `ParseError` (erro de layout) pela falta de dado local; passa a `SourceUnavailableError`, com o motivo "offline sem dado no cache".
- **docs** — o hash de manifesto (`resource_manifest_sha256`) leva a hora de cada recurso e identifica a aquisição; o conteúdo se compara pelo `sha256` de cada recurso. O `sync` não embrulha o `sicar.imoveis_geo_stream`, que segue gerador assíncrono. O `ajuste_por_contrato` da B3 está na moeda do prefixo de `unidade` (R$ ou US$). A produtividade do PSR não tem unidade publicada. As estatísticas do Focus estão na unidade do indicador.
- **Queimadas, mês recusado inteiro pela publicação do INPE** — `datasets.queimadas(ano=2024, mes=8)`, o exemplo da doc, levantava `ContractViolationError` depois de baixar os 359 MB do mês, por 28 focos VIIRS com FRP negativo e 1 foco publicado 2 vezes com FRP diferente (5,5 × 8,5). Na leitura de 20 meses de 2023 a 2026, 7 eram recusados assim (jun e jul/2023, ago a out/2024, mar e set/2025), e as 56 chaves repetidas diferiam só no FRP. `queimadas.focos` e o dataset passam a colapsar a cópia igual, publicar em 1 linha com `frp` nulo a chave que difere só no FRP, tirar do resultado a que difere em outra coluna e anular o FRP negativo, com aviso e a contagem em `meta.validation_warnings` e `source_details`, contados depois dos filtros. O `PARSER_VERSION` da Queimadas passa a 2.
- **CONAB, `balanco` com produto desconhecido** — baixava a planilha e devolvia vazio (o filtro é por substring do rótulo); passa a levantar `InvalidParameterError` antes da rede, com os 6 produtos da aba Suprimento (`soja`, `milho`, `arroz`, `feijao`, `trigo` e `algodao`, com ou sem acento), como o `safras`.
- **Fingerprint de layout, similaridade no Python 3.11** — `compare_fingerprints` somava os pesos com `sum()`, e no 3.11 duas páginas idênticas saíam com similaridade `0.9999999999999999` (do 3.12 em diante, `1.0`): o `fingerprint_similarity` do health check mudava com a versão do Python. Passa a `math.fsum`, que dá `1.0` nas 4 versões suportadas; nenhum limiar muda.
- **Reconciliação da Lista Suja, fragmento vazio no Python 3.11 a 3.13** — `scripts/reconciliar_lista_suja.py` comparava os links do portal com o manifesto gravado no 3.14, e só o `urljoin` do 3.14 mantém o `#` de `href="#"`: no 3.11 a 3.13, o portal intacto dava `mismatch`. O fragmento vazio sai dos 2 lados antes da comparação; os outros fragmentos continuam contando.
- **Queimadas, mês corrente sem aviso de parcial** — o INPE publica o arquivo mensal do mês corrente e o atualiza durante o mês (set/2026: `Last-Modified` de 26/09 21:56 GMT, até o foco de 25/09 23:50 GMT), e `queimadas.focos` e `datasets.queimadas` o entregavam como o mês inteiro. Quando o `Last-Modified` é anterior ao fechamento observado do mensal (o dia 1 do mês seguinte às 23:56 GMT; limite às 23:50) ou, sem ele, o relógio está antes do fechamento mais 1 h, o resultado sai com aviso e com `mes_parcial`, `ultimo_foco` e `last_modified` no `source_details`, como o mensal parcial do NASA POWER. O diário tem a mesma regra.
- **SICOR, proveniência do `credito_rural`** — o `MetaInfo` do `bcb.credito_rural`, do `credito_rural_total` e do dataset não guardava nada que distinguisse uma versão da base (o BCB revê meses fechados). Passa a ter o `source_url` da consulta (com `$filter`, sem `$top`), o hash e o tamanho do manifesto `{query, resources}` e cada página (URL, SHA-256, bytes e hora) no `source_details`, como o Focus.
- **IBGE, `cache_expires_at` sem cache** — as 10 funções do IBGE carimbavam um vencimento de cache (7 dias na PAM) sem cache nenhum; passam a deixá-lo nulo, e a tabela "Cache" da doc da fonte deixa de prometer TTL.
- **Queimadas, diário do dia corrente** — o diário também é atualizado durante o dia e só fecha em D+1 às 12:05 GMT (os 25 diários fechados de set/2026); quando o `Last-Modified` é anterior a D+1 às 12:00 GMT (ou, sem ele, o relógio está antes de D+1 às 13:05), sai com aviso e `dia_parcial`, `ultimo_foco` e `last_modified`. A doc diz que, para um dia de mês fechado, o mensal é o mais completo (14/09: 506 focos a mais).
- **CONAB, erro sem rede** — sem rede ou com HTTP 404/500, o erro mandava instalar o Playwright e escondia a causa; passa a trazer primeiro a falha HTTP (tipo, status e URL), com o navegador como fallback que também falhou, e encadeia com `from` a falha HTTP, no boletim e na planilha.
- **Acervo Fundiário, rede e 5xx** — saíam como `httpx` cru; o HEAD e o download passam a repetir nas falhas de rede e nos status transitórios e a levantar `SourceUnavailableError` com `from` a causa.
- **Parâmetro impossível devolvia vazio calado** — `conab.serie_historica` recusa `inicio > fim` e UF inválida antes da rede; o PRODES recusa ano posterior ao corrente antes da rede e avisa quando o WFS devolve 0 feição para um ano válido; a CEASA recusa produto ou CEASA fora do que a CONAB/PROHORT publica, com os válidos.
- **Erro de parâmetro como indisponibilidade ou depois da descarga** — `ibge.abate` e `ibge.leite_trimestral` com UF desconhecida levantavam `SourceUnavailableError` (400 do SIDRA): passam a `InvalidParameterError` antes da rede; o CFTC recusa `start > end`, a sociobiodiversidade recusa ano posterior ao corrente e a ANTAQ recusa UF inexistente antes da descarga. A cultura do PSR e a mercadoria da ANTAQ não têm catálogo estático (vêm no corpo): a doc registra.
- **B3, `data` junto com `inicio`/`fim`** — o dataset `futuros_agricolas` descartava em silêncio o parâmetro que não se aplica ao tipo; passa a levantar `InvalidParameterError` antes da rede.
- **Queimadas, HTTP 200 com corpo vazio** — saía como "não encontrado"; passa a dizer que o servidor respondeu sem o arquivo, com a URL e o tamanho, no diário e no mensal.
- **USDA, ano padrão de janeiro a abril** — sem `market_year`, `usda.psd` e o `datasets.oferta_demanda_global` pediam o ano-calendário, que o PSD só publica a partir do WASDE de maio, e devolviam vazio, sem aviso, de janeiro a abril. Agora, se o ano corrente vem vazio, a consulta pede o anterior; o ano usado, os pedidos e se o padrão valeu vão em `source_details` (`market_year`, `market_year_tentados` e `market_year_padrao`). Com `market_year` explícito, nada muda (guia de migração, seção 67).
- **SICOR e ANDA, período em curso sem marca** — o `credito_rural` somava a safra em curso (julho a junho) ao lado das fechadas, e o `fertilizante` sem `ano` (e o `anda.entregas` do ano corrente) devolvia o ano parcial, sem sinal. Agora os dois avisam em `validation_warnings` e em `UserWarning` e registram o período e os meses cobertos em `source_details` (`safra_em_curso` e `meses_cobertos`; `ano_em_curso` e `meses_cobertos`).
- **DERAL, ordem pela data em texto** — o `condicao_lavouras` saía ordenado pela `data` "dd/mm/aaaa" como texto, e a última linha de cada produto era de 2021. Agora a ordem é cronológica, e a coluna segue texto, como no contrato 1.0. A aba cujo nome difere da data da célula (`18-12-2017` com 08/01/2018; `19-09-2021` com 20/09/2021) segue a célula, agora com aviso em `validation_warnings` e em `UserWarning`.
- **Ano corrente em 2 relógios** — o padrão do `fertilizante` e do `clima` saía do ano em UTC, e a validação da ANDA e do INMET usava o relógio local: das 21h à meia-noite de 31/12 (BRT), o padrão era recusado. Os 2 lados passam a usar a data de Brasília (UTC−3 fixo); os carimbos seguem em UTC.
- **CEPEA, validade em período fechado** — com `fim` anterior à janela recente, `cepea.indicador` e o `datasets.preco_diario` serviam o cache com um `cache_expires_at` já vencido, embora esse período não volte à fonte. Agora o campo sai nulo nesse caso.
- **CLI, data do JSON em milissegundos** — `--formato json` saía com as datas em milissegundos desde 1970, o padrão do `DataFrame.to_json`. Agora sai em ISO 8601 (`"2026-09-22T00:00:00.000"`), em todos os comandos (guia de migração, seção 68).
- **SICAR, corte em `max_features` calado** — `imoveis_geo` parava em `max_features` (5.000 no padrão) sem sinal fora do log: no DF, 5.000 dos 21.011 imóveis. Agora o corte sai com aviso em `validation_warnings` e em `UserWarning`, e `source_details["sicar"]` traz `truncado`, o `max_features` e o total da consulta na fonte (o `numberMatched` do WFS), quando publicado.
- **FUNAI, área declarada × polígono** — `area_ha` é a área declarada pela FUNAI (`superficie_perimetro_ha`) e pode divergir muito do polígono (TI Mashco do Rio Chandless: 421 ha declarados, 543.430 ha no polígono). A doc diz isso, e `terras_indigenas_geo` avisa as terras com diferença acima de 5 % (Albers do IBGE), com as 2 áreas em `source_details["area_divergente"]`.
- **docs (produção e USDA)** — `producao_anual` e `estimativa_safra` dizem que CONAB e IBGE estimam níveis diferentes (soja: CONAB 3,8 % acima da PAM em 2025 e 3,1 % acima do LSPA em 2025/26), e que a série que mistura as fontes muda de nível no ano da troca. O porte para R manda a credencial da CEASA no cabeçalho `Authorization: Basic` (`req_auth_basic`), e não na URL. O USDA documenta a pecuária pelo código do catálogo e restringe a identidade do balanço aos 9 produtos da tabela.
- **MapBiomas Alerta, paginação repetia e perdia alertas** — janeiro de 2025 saía com 4.196 dos 4.633 alertas e 2.220 ha a menos; a paginação passa a `ALERT_CODE ASC` e é conferida contra o `totalCount` (código repetido ou coleção incompleta levantam `ParseError`; `totalCount` que muda no meio vira aviso).
- **MapBiomas Alerta, `bbox` na ordem errada** — ia como `[ymin, xmin, ymax, xmax]`, e toda consulta com caixa voltava vazia; passa na ordem da API.
- **MapBiomas Alerta, data invertida** — devolvia vazio calado; data invertida ou fora de `AAAA-MM-DD`/`DD/MM/AAAA` levanta `InvalidParameterError` antes da rede, e a data vai em ISO.
- **MapBiomas Alerta, doc da fonte** — `alert_code` é inteiro, `sources` aceita o enum da API (o exemplo `["DETER", "SAD"]` falhava), a coluna `fonte` traz os nomes publicados e o token expira.
- **IBGE, pedido acima do limite da SIDRA** — a SIDRA recusa consulta com mais de 50.000 valores (HTTP 400), e o agrobr tratava a recusa como indisponibilidade: `ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')`, o exemplo da doc, falhava sempre com "ibge unavailable". Agora, em toda consulta SIDRA, o pedido acima do limite é dividido por variável, categoria listada ou município (pela lista de localidades da tabela) e as partes são juntadas sem duplicar, com cada consulta na proveniência; outro 400 sai como `InvalidParameterError` com o motivo da SIDRA, sem cair na API de agregados.
- **docs** — o exemplo da PTAX no README usa `DD/MM/YYYY`; o README diz que a CONAB usa HTTP primeiro e o navegador só como fallback opcional; o quickstart diz o que o INMET publica sem token (catálogo e ZIPs anuais) e o que exige o token, em vez de "API fora do ar".
- **Modo determinístico, o que ele faz** — o README prometia "consultas usam apenas o snapshot ativo, sem rede", e o modo não lê snapshot: ele fixa a data de referência, e só o `preco_diario` a honra (cache local, sem rede). Os datasets que consultam a fonte corrente dentro de `datasets.deterministic(...)` passam a avisar em `validation_warnings` e com `UserWarning` que o dado é o corrente, e não o da data; o README (PT/EN) e o exemplo "Papers" da reprodutibilidade dizem isso.
- **`preco_diario` determinístico sem cache** — noutra máquina, sem o produto no cache local, voltava vazio e calado. Agora levanta `SourceUnavailableError` com o motivo; o período sem dado, com o produto no cache, segue vazio.
- **Snapshot, caixa do nome** — `load_from_snapshot("CEPEA", ...)` num snapshot gravado como `cepea/...` acusava adulteração no Windows e no macOS, porque o manifesto era consultado pela chave exata. A conferência do SHA-256 passa a achar a entrada sem distinguir a caixa.
- **SICAR e ZARC, versão do contrato no `MetaInfo`** — vinha fixa em `2.0` no código da fonte; passa a ser lida do contrato, e não fica para trás quando ele sobe.
- **IBGE, URL longa rejeitada pelo WAF da SIDRA** — a divisão por localidade fazia partes de ~657 municípios, com URLs de ~5.300 caracteres, e o WAF da SIDRA respondia "Request Rejected" (HTTP 200 com HTML): `ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='MG')` falhava inteiro. Agora cada parte por localidade cabe em 2.500 caracteres de URL (o WAF rejeitou 5.332 e aceitou 3.276), e o "Request Rejected" numa URL acima desse teto divide o pedido de novo, em vez de cair nos agregados; o MG sai com os 853 municípios.
- **IBGE, censo de 1995: `informantes`** — a variável 151 das lavouras (tabelas 492 e 504), que a SIDRA chama de "Número de informantes", saía como `estabelecimentos`; passa a sair como `informantes`. As demais variáveis de 1995 conferem com os rótulos da SIDRA.
- **CEPEA, sem a coluna de valor em R$** — sem a coluna "Valor R$" (removida, trocada por "US$" ou renomeada para "Cotação"), o parser pegava o 1º número positivo da linha, a variação do dia, e o entregava como preço em BRL (soja a 0,98 R$/sc), calado. Agora a tabela sem coluna de valor em reais reconhecida pelo cabeçalho levanta `ParseError` ("layout do CEPEA mudou: coluna de valor em R$ não encontrada"), que segue o caminho de sempre (Notícias Agrícolas, quando habilitada, e o cache); o US$ nunca vira BRL. A laranja, cuja coluna é "A Prazo", passa a ser lida pelo cabeçalho.
- **`agrobr health --deep` sem comparar o fingerprint** — lia `.structures/baseline.json` pela pasta corrente e no formato errado: da raiz do repositório, falhava com erro de validação; de outra pasta, passava verde sem comparar. Agora a baseline do CEPEA vai no pacote (`agrobr/health/baselines/cepea_baseline.json`, a página da soja de 27/09/2026), lida pelo `validators.load_baseline` (que passa a abrir o arquivo em UTF-8). Sem baseline, ou com a página vinda da Notícias Agrícolas, o check sai `warning` com o motivo.
- **ANP, linha TOTAL como município** — uma linha com rótulo de agregado ("TOTAL", "SUBTOTAL") na coluna de município da planilha semanal entrava como mais um município. Agora sai, com aviso em `validation_warnings` e `UserWarning`.
- **CEPEA, anomalia da sanidade no `MetaInfo`** — com `validate_sanity=True`, a anomalia só aparecia na coluna `anomalies`; agora um resumo (quantas linhas e quais regras) vai também para `validation_warnings`, com `UserWarning`.
- **docs** — as camadas da doc de resiliência passam a ser as que existem: o fingerprint roda só no `health --deep` e no Structure Monitor, a sanidade é opcional e a checagem de completude sai.
- **Rate limiter zerava a cada `asyncio.run`** — o horário do último pedido e os semáforos eram apagados quando o loop mudava, e o `agrobr.sync` abre um loop por chamada: o BCB (1 s, concorrência 1) saía a 0,4 s em sequência e com 2 pedidos simultâneos em 2 threads. O intervalo, a reserva do próximo horário e as vagas por fonte passam a valer para o processo inteiro, entre loops e threads. A espera por vaga entre threads tem teto (`timeout_read`), e no teto o pedido segue com aviso, sem travar o loop que chamou o `sync`.
- **`agrobr.sync` com um loop rodando** — pedia o `nest_asyncio`, que no Python 3.14 quebra fora do Jupyter: a 1ª chamada terminava em `RuntimeError`, e o 2º `asyncio.run` quebrava as consultas. Agora a corrotina roda numa thread própria, com o seu `asyncio.run` e o contexto de quem chamou (modo determinístico), e avisa uma vez recomendando o `await`.
- **Cache do CEPEA travado pelo 1º processo e upsert parcial** — a conexão ficava aberta enquanto o processo vivesse, e o 2º notebook ou worker seguia sem cache; o upsert gravava em blocos com commit próprio, e um processo morto no meio deixava metade da série, que o modo offline entregava como completa. A conexão passa a abrir e fechar por operação, e o upsert é tudo ou nada.
- **Relógio da máquina nos padrões de data** — o fim padrão do `cepea.indicador`, a janela da CONAB (boletim e série histórica), o ano padrão da PAM, do `ibge.lspa`, da `estimativa_safra` e do ANTT e a `safra_atual()` saíam do fuso da máquina: às 23h de Nova York, o CEPEA devolvia um dia a mais, e às 23h30 de 30/06 a safra atual ainda era a anterior. Passam ao `hoje()` de Brasília, e o teto de ano do IBGE, que valida o ano padrão do `lspa`, vai junto.
- **`estimativa_safra` da safra corrente escondia a falha da CONAB** — de julho a dezembro, o fallback LSPA pedia ao `ibge.lspa` o ano final da safra, que o IBGE ainda não publica. O `InvalidParameterError` resultante era relançado e escondia a falha da CONAB. Agora o fallback recusa esse ano com `SourceUnavailableError` ("o LSPA ainda não publica <ano>"), e o erro final lista as 2 fontes.
- **ZARC, CSV truncado envenenava o cache** — o portal do MAPA manda o CSV em gzip e por partes, sem Content-Length; um corte numa quebra de linha passava, a consulta devolvia 0 linhas e o pacote truncado ficava no store, servido de novo sem rede até a revisão seguinte do catálogo. Agora o download é conferido contra o tamanho que o servidor publica (`Content-Range: bytes 0-N/T`, que o portal manda no 200, ou o `Content-Length`); corpo menor levanta `SourceUnavailableError` e não vai ao store. Sem tamanho publicado, o resultado avisa em `validation_warnings` e `UserWarning` e não é gravado; entrada do store sem tamanho conferido ou sem registros é baixada de novo. O catálogo CKAN não traz o tamanho (`size` nulo nos 13 recursos).
- **ComexStat, corte sem Content-Length** — sem o cabeçalho no GET, um corte numa quebra de linha entregava dado parcial calado. Agora o tamanho vem do HEAD do mesmo arquivo; sem ele também, o resultado avisa ("tamanho do arquivo não conferido") em `validation_warnings` e `UserWarning`. A proveniência diz de onde veio o tamanho (`size_check`). A doc da fonte deixa de dizer `verify=False`: o TLS é verificado por inteiro, com o certificado intermediário conferido por SHA-256.
- **ZARC e SICAR, memória e proveniência** — no ZARC, o buffer do download é reservado pelo tamanho publicado (sem as realocações que dobravam o pico), a detecção de codificação confere por blocos, sem uma cópia do CSV em texto, e o corpo sai da memória antes da gravação no store. Para o CSV de 212 MB, o pico de memória privada cai de 1.955 para 1.752 MiB no padrão e de 1.026 para 811 MiB com `use_cache=False`. No SICAR com várias páginas, o `MetaInfo` passa a trazer cada página em `source_details["resources"]` e o hash do manifesto no topo (`resource_manifest_sha256`), em vez de hash nulo e tamanho 0.
- **Timeout de leitura** — `AGROBR_HTTP_TIMEOUT_READ` não valia nos clientes que fixam a leitura (ComexStat, ZARC, PSR, INMET e outros). Agora vale como mínimo em todos, e a doc de resiliência diz o valor de cada um.
- **Logs no uso como biblioteca** — fora da CLI, o structlog ficava no padrão e imprimia debug e info na saída padrão, sem que o `logging.basicConfig` da doc os controlasse (`python exporta.py > dados.csv` saía com log no topo). Agora o agrobr roteia o structlog pelo `logging` da biblioteca padrão, em JSON, só se ninguém o configurou antes: sem configuração, só avisos e erros saem, na saída de erro; `logging.basicConfig` e `logging.getLogger("agrobr")` controlam o resto. A CLI segue com `--verbose` e tudo na saída de erro.
- **README no PyPI** — os 29 links relativos do `README.md` (27 no `README.pt-BR.md`) e a imagem de capa quebravam na página do pacote. Passam a URLs absolutas: a doc publicada nas páginas, conferidas no site gerado, e o GitHub nos arquivos do repositório e na imagem.
- **`structure_monitor`** — no runner, o CEPEA dá 403 e a página vem da Notícias Agrícolas, que o monitor comparava com a baseline antiga do CEPEA ("DRIFT 21 %" e alerta a cada execução). Agora compara com a baseline do pacote (`agrobr/health/baselines/cepea_baseline.json`), e a página da NA é pulada, com aviso e sem alerta. A `.structures/baseline.json`, do layout antigo, sai.
- **`health --deep` com a página da NA** — pulava o fingerprint, mas lia a página da NA com o parser do CEPEA e saía `failed`. Agora a página da NA é lida pelo parser da NA, e o motivo ("a página veio de noticias_agricolas") aparece mesmo com a latência alta. A página do CEPEA vinda do navegador volta a ter o fingerprint comparado.
- **docs** — o exemplo do `MetaInfo` da fonte ANP mostrava `parser_version` 2; o código está no 4. As demais docs de fonte com a versão no exemplo conferem.
- **Data do Queimadas em `object`** — a coluna `data` do `queimadas` e das fontes `queimadas.focos`/`focos_geo` saía como `datetime.date` numa coluna `object` (e `Date` no polars), contra a regra de toda data pública em `datetime64[ns]`. Agora sai em `datetime64[ns]`/`Datetime("ns")`, e o ponto comum dos datasets converte qualquer coluna de data do contrato que chegue assim.
- **Status HTTP de erro, um caminho só** — na 1.1.0, o 403, o 404 ou os dois saíam como `httpx.HTTPStatusError` cru em 31 fontes (ABIOVE, ANDA, ANEC, ANP (diesel), ANTT (pedágio), B3, BCB (SGS, PTAX e Focus), CFTC, ComexStat, Defensivos, DERAL, Desmatamento, Embrapa Solos, FUNAI, IBAMA, IBGE (Censo legado, pelo FTP), ICMBio, IMEA, INCRA, INMET (arquivo histórico e API), Lista Suja, MapBiomas, MAPA PSR, NASA POWER, Queimadas, RNC, SFB, SICAR, UNICA, USDA e ZARC), e em outras como `SourceUnavailableError`. Agora o status HTTP de erro passa por `agrobr.http.responses.raise_for_status`, que levanta `SourceUnavailableError` com a fonte, a URL e o status ("HTTP 403: a fonte recusou o pedido (bloqueio de WAF ou permissão)", "HTTP 404: o recurso não existe na URL") e o `httpx.HTTPStatusError` em `__cause__`; um teste estático recusa `raise_for_status()` direto fora da lista de exceções, cada uma com o motivo. Nas 31 fontes, a mensagem traz o status ("HTTP 403", "HTTP 404"), também nos caminhos que não passam pelo helper: a ComexStat, o Censo legado do IBGE, a SIDRA (inclusive no `errors` do `producao_anual`) e o 404 tratado à parte no DERAL, na ABIOVE e no arquivo histórico do INMET; um teste serve 403 e 404 a todas e confere o tipo e o status na mensagem, e o guia deixa de prometer o `__cause__`. Na API do INMET (`estacoes` e `estacao`), o 400, o 401 e o 404 deixam de sair crus, sem expor o token na mensagem. As consultas da SIDRA, que na 1.1.0 levantavam `ValueError` com o corpo da página de erro, também saem como `SourceUnavailableError`. `NetworkError` segue exportado, mas não é levantado; a doc deixa de prometê-lo.
- **USDA, 404 do PSD** — o gateway responde 404 para um ano sem dado, e a tabela vinha vazia e calada. A tabela vazia fica, com aviso em `validation_warnings` e `UserWarning`, porque o mesmo 404 sai se a URL da API mudar.
- **Erro final do dataset** — o `SourceUnavailableError` de quando todas as fontes falham vinha sem `attempted_sources` e sem causa. Agora traz as fontes tentadas, na ordem, e `__cause__` com o erro da última; `errors` segue com o motivo de cada fonte (a que falhou por status HTTP passa a ter o tipo `"unavailable"`, e não `"network"`).
- **SICAR, `query` do manifesto com a paginação** — `source_details["query"]` e `source_url` terminavam em `count=10000&startIndex=0`, da 1ª página. Agora são a consulta sem paginação; a URL de cada página segue em `resources`.
- **Queimadas, mensagem do arquivo ilegível** — o `ParseError` dizia "CSV vazio" também quando vinha uma página HTML ou um ZIP corrompido. Agora diz o que veio: uma página HTML, um ZIP que não se lê como o CSV ou um CSV sem linha de dado.
- **Avisos de licença que faltavam** — `anec.list_articles`, `fetch_latest_pdf` e `fetch_pdf_bytes`, públicas, entregavam o PDF da ANEC (`zona_cinza`) sem o aviso; agora avisam na 1ª chamada, como o resto da ANEC. O aviso da Notícias Agrícolas passa a citar a classificação `restrito` e os direitos reservados (Lei 9.610/98), como a `docs/licenses.md`.
- **MapBiomas Alerta, `sources` fora do enum** — um valor fora do enum `SourceTypes` da API (os 22 nomes de fonte e o `All`) ia à rede e voltava como erro da API. Agora levanta `InvalidParameterError` antes da rede, com a lista aceita; o enum é conferido por teste contra a introspecção capturada.
- **Safra do crédito rural em "AAAA/AAAA"** — o `credito_rural` (fonte e dataset) e o `credito_rural_total` publicavam "2023/2024", fora do "2023/24" do resto da camada, e o `merge` pela `safra` com `estimativa_safra`, `balanco`, `progresso_safra` e `serie_historica_safra` casava 0 linhas, sem erro. Agora saem "AAAA/AA" (`normalize.dates.anos_para_safra`), e a descrição da `safra` nos 2 contratos diz o formato. Veja o guia §76.
- **Café conilon com 3 nomes** — `cafe_robusta` (`preco_diario`), `cafe_conillon` (`futuros_agricolas`) e `cafe_conilon` (`serie_historica_safra`, `custo_producao`): cada dataset só aceitava o seu. O `normalizar_cultura` leva os 3 a `cafe_robusta`, e os 4 datasets aceitam qualquer um, saindo com o nome que o contrato de cada um documenta. O mesmo para a castanha: `castanha_para` (`extrativismo_vegetal`) e `castanha_do_brasil` (`custo_sociobiodiversidade`), com o canônico `castanha_do_brasil`.
- **Snapshot, nomes reservados do Windows** — `NUL`, `CON`, `COM1` e os demais (com ou sem extensão, em qualquer caixa) e os nomes terminados em ponto passavam no validador: no Windows, `a..` colide com `a`, e `NUL` não é pasta. Agora levantam `ValueError` em todo sistema.
- **Tarefas irmãs canceladas quando 1 falha** — com `asyncio.gather`, a falha de 1 download derrubava a chamada e os outros seguiam na rede. Agora `utils.tasks.gather_or_cancel` (um `asyncio.TaskGroup` que sobe o 1º erro sem `ExceptionGroup`, com a causa) cancela os irmãos na ANP (semanas), na CEASA, na LSPA, no Censo Agropecuário (tabelas e anos) e no `mapbiomas_alerta.alerta_info`. Os `gather` que coletam falhas de propósito (B3 histórico, série da CONAB, alertas e health) ficam.
- **ANTAQ com teto de ano fixo em 2025** — a partir de 2027, `antaq.movimentacao(ano=2026)` e o `movimentacao_portuaria` seriam recusados antes da rede, e o dado mais novo ficaria inacessível até outra versão. O teto passa ao ano corrente (`utils.time.hoje()`), como nas outras fontes; o ano que a ANTAQ ainda não publicou sai como `SourceUnavailableError` (o 404 da fonte), e não como erro de parâmetro. Sai o `antaq.models.MAX_ANO_DEFAULT`.
- **PSR com o ano novo vazio e calado** — o `mapa_psr` e o `seguro_rural` só conheciam os 3 CSV do dicionário fixo (até 2025): `ano=2026` devolvia 0 linhas, sem aviso e sem ir à rede, e seguiria assim quando o MAPA publicasse 2026. Agora o ano depois do dicionário (e, sem `ano_fim`, até o ano corrente) é procurado no catálogo do pacote no CKAN do MAPA pelo nome do arquivo; o ano sem arquivo sai com aviso (`UserWarning` e `meta.validation_warnings`), e `meta.source_url` aponta o catálogo quando não há arquivo, nunca vazio.
- **SICOR — guia e CHANGELOG sobre as colunas da 1.1.0** — diziam que as 12 colunas de mês, região, subprograma, fonte de recursos, tipo de seguro, modalidade e atividade não pertenciam à saída, e que o `credito_rural` nunca publicou os nomes de fonte, modalidade e atividade. A chamada padrão da 1.1.0 (`agregacao="municipio"`, que não agregava) devolvia essas colunas, e código que agrupava por mês ou fonte cai em `KeyError` nas agregações da 2.0. O guia (§4 e §50) passa a dizer isso e a apontar o `agregacao="registro"`.
- **SICOR — o fallback BigQuery ignorava `programa=` e `tipo_seguro=`** — com o OData fora, a tabela da Base dos Dados, que não traz programa nem seguro, devolvia para `programa="pronaf"` o total de todos os programas, marcado `bcb_bigquery`, e o mesmo com `tipo_seguro=`; com `agregacao="programa"`, uma linha de programa nulo com o total. Agora o fallback só vale para `agregacao="uf"` sem esses filtros; nos outros casos, levanta `SourceUnavailableError` com o motivo.
- **Guia de migração: lacunas da 1.1.0** — o guia ganha as seções 78 (argumento fora da assinatura levanta `TypeError` nas 76 funções que aceitavam `**kwargs`, com a lista e os 2 argumentos que só chegavam ao fallback, `datasets.exportacao(mes=)` e `datasets.producao_anual(safra=)`), 79 (o ZARC 2.1 não tem chave, e quem deduplicava pelas 5 colunas do 1.0 descarta linhas publicadas) e 80 (corpo fora do formato: `ParseError` no SGS, na ComexStat, no ZARC e no Desmatamento, onde a 1.1.0 levantava `SourceUnavailableError`). A seção 39 deixa de descrever o `mes` do fallback ABIOVE como vivo, e a 66 passa a listar a UF inexistente que ia de vazio a `InvalidParameterError` (crédito, CONAB, Queimadas, ComexStat e os datasets `estimativa_safra` e `producao_anual`), a do `datasets.clima`, que ia de `SourceUnavailableError` a `InvalidParameterError`, e as fontes em que ela já era `ValueError`. Sai o `kwargs.get("mes")` do fallback ABIOVE do `datasets.exportacao`, que o dataset nunca repassava. As landings (PT e EN) deixam de prometer crédito por modalidade sem o modo registro e qualificam o modo determinístico.
- **Exemplos e notebooks presos a versões antigas** — o `agrobr_demo.ipynb`, o do badge do Colab, se apresentava como v0.9.0 (13 fontes, 1.529 testes), gravava um token falso do INMET, dizia que sem token o dado voltava vazio (HTTP 204) e prometia cache DuckDB "com histórico permanente"; o `demo_colab.ipynb` citava a v0.6.0 e a v0.7.0; os 2 instalavam o agrobr sem versão mínima. O `analise_soja.py` rotulava de 30 dias a média do padrão de 365 e só mostrava a variação se existisse `variacao_pct`, coluna que o CEPEA não publica; o `pipeline_cache.py` citava a PAM, que não coleta, e prometia cache hit na 2ª execução sem dizer que só o CEPEA tem cache. Agora os notebooks pedem `agrobr>=2.0`, o texto diz que só o CEPEA tem cache (com TTL, sem histórico) e que o INMET sem token levanta `SourceUnavailableError`, e o `tests/test_examples.py` confere que cada chamada do agrobr nos 4 `.py` de `examples/` existe e aceita os argumentos usados.
- **Cache do CEPEA danificado quebrava o `cepea.indicador`** — com o `agrobr.duckdb` truncado (queda de energia, disco cheio ou antivírus no meio da gravação), o DuckDB abria o arquivo e falhava na leitura, e toda chamada levantava `duckdb.IOException` crua, fora do `AgrobrError`, até o usuário apagar o arquivo à mão. Agora os métodos do cache seguem sem cache em qualquer erro do DuckDB durante a operação, e o banco que o DuckDB acusa como ilegível (leitura incompleta, checksum ou arquivo inválido) vai para `agrobr.duckdb.corrompido-<AAAAMMDDHHMM>`, com o WAL, e um aviso com os 2 caminhos; a consulta seguinte cria um banco novo. Arquivo em uso por outro processo e disco cheio não movem nada.
- **CEPEA com a série histórica, a hora da aquisição** — o `fetched_at` e o `fetch_timestamp` saíam com a hora do fim do processamento, e com a série e a página os 2 divergiam; a hora do recurso `serie` em `source_details` saía sem fuso. Agora os 2 campos são a aquisição mais recente entre os corpos, e a hora de cada recurso sai em UTC com fuso.
- **SFB e ANTAQ sem proveniência física** — `raw_content_hash` nulo e `raw_content_size` 0 mesmo com 1 corpo. O SFB (CNFP, concessões e IFN, tabular e geo) carimba o corpo quando a consulta vem em 1 página, como a ANA; a ANTAQ carimba o ZIP do ano e lista o `Mercadoria.zip`, que entra no dado, em `source_details` (`mercadoria_url`, `mercadoria_sha256` e `mercadoria_bytes`). Junto: o `SourceFallbackWarning` do `preco_diario` com o CEPEA fora saía sem o motivo; agora traz o que o `cepea.indicador` registrou.
- **CLI, `--formato` inválido e a saída do Windows** — `-o xml` saía com código 0 e a tabela; agora os 7 comandos com `--formato` aceitam só `table`, `csv` e `json`, e o `health --output`, só `text` e `json`: outro valor sai com código 2, a mensagem na saída de erro e a saída padrão vazia. No Windows, o CSV saía com `\r\r\n` em cada linha e, redirecionado, em cp1252 (o `pandas.read_csv` padrão falhava em "Paranaguá"); agora a saída padrão da CLI sai em UTF-8, e o CSV com uma quebra por linha. A dica do `snapshot use` com nome inexistente ia para a saída padrão; agora vai para a de erro.
- **Doc PT × EN** — 3 cercas de código sem par no EN (`api/ibge`, `guides/docker` e `porting/r`) faziam o resto da página sair como código; o ZARC dizia "58 colunas do contrato 2.0" nas 2 línguas (contrato, fonte e API), e o contrato 2.1 tem 59, com o `cod_municipio` no fim da tabela, onde o contrato o põe; o README PT prometia "fallback automático" em todo dataset e trazia "7.900+ testes, 93% cobertura", sem conferência, e agora diz o que o EN diz; o exemplo do `geocodigo` do MapBiomas, os parágrafos da Lista Suja e do Agrofit e o parágrafo de apresentação ficam nas 2 línguas, os 2 links do README EN apontam para as páginas em inglês, e a linha da v1.0.1 sai do `index` PT. 2 testes novos: toda página com as cercas em par, e a contagem em negrito das páginas de contrato igual à do contrato.
- **Doc de cache e `doctor`** — o `doctor` listava TTL para as 40 fontes, sem cache fora o CEPEA, e a doc prometia cache que não existe: IBGE ("Cache 7 dias" em PEVS, leite e PIB; 30 e 90 dias nos censos), a resiliência ("CONAB 24h, IBGE 7 dias") e o ComexStat ("o download é feito uma vez"). Agora o `doctor` mostra só o CEPEA, e as páginas do IBGE, do ComexStat, do BCB e do SICAR dizem que cada chamada consulta a fonte.
- **Censo legado, máquinas do Pará** — `censo_agro_legado("maquinas")` sem `uf` (e o `datasets.censo_agropecuario_legado`) falhava inteiro: o IBGE publicou a Tabela 6 (pessoal ocupado) no lugar da 7 em `Para/Tab_7Mn.zip`, e a tabela municipal de maquinaria do Pará não está no FTP. Agora a consulta sem `uf` devolve as outras 26 UFs, com o aviso no `MetaInfo` e um `UserWarning`, e `uf="PA"` levanta `SourceUnavailableError` com o motivo. Na 1.1.0, `uf="PA"` devolvia a Tabela 6 calada como tratores.

### Security

- **health** — campos externos são escapados no relatório HTML e nos alertas por email
- **deps** — o teto `Pygments<2.19` do extra `docs` foi removido: a 2.18 carrega a PYSEC-2026-2987 (corrigida na 2.20) e o mkdocs-material 9.7 constrói normalmente com a 2.21
- **testes, golden do IBAMA sem dado pessoal** — `tests/golden_data/ibama/termo_embargo_sample.csv`, recorte do dump oficial com nome e CPF/CNPJ de embargados, saiu do repositório; o golden novo (`ibama/oficial_20260923`) mascara `NUM_PESSOA_EMBARGO`, `NOME_EMBARGADO` e `CPF_CNPJ_EMBARGADO`, campos que o agrobr não lê nem publica
- **CONAB CEASA/PROHORT, credencial fora da URL** — usuário e senha do Pentaho iam na query string e apareciam no log INFO do httpx e na URL do erro de status esgotado; passam ao cabeçalho `Authorization: Basic`, e nenhuma URL registrada (log, erro, `MetaInfo`) leva a credencial. A doc do INMET registra que o token vai no caminho da URL por exigência da API.
- **Bomba de descompressão** — Queimadas, B3, MapBiomas, ANTAQ e o Censo legado do IBGE liam o membro do ZIP inteiro, sem teto, e o XLSX da ANP pelo calamine derrubava o processo: um corpo de 0,5 MB pedia mais de 0,9 GiB. Agora o membro de ZIP passa por `utils.io.read_zip_member`/`open_zip_member` e o XLSX por `utils.io.check_xlsx_expansion` (em `read_excel_safe`, `open_excel_safe`, na série do CEPEA, no MapBiomas municipal e na UNICA), contra um teto por fonte (`constants.MAX_EXPANDED_BYTES`), medido acima do maior arquivo publicado; `ResourceLimitError` antes de descomprimir. No XLSX, o CRC de cada membro é conferido, porque o calamine não respeita o tamanho declarado. Um teste estático recusa leitura de membro de ZIP fora do helper.
- **`semana_url` só na CONAB** — `conab.progresso_safra(semana_url=...)` aceitava qualquer host e seguia redirecionamento, e o `MetaInfo` dizia CONAB. Agora só páginas em `https://www.gov.br/conab/`; outra URL, ou redirecionamento e link que saiam dela, levantam `InvalidParameterError` antes do pedido.
- **Pisos do `pyarrow` e do `soupsieve`** — o extra `polars` declarava `pyarrow` sem versão, e a doc de snapshot mandava `pip install pyarrow`: o `pyarrow` anterior ao 14.0.1 executa código ao ler Parquet malicioso (CVE-2023-47248), e o `load_from_snapshot` lê com ele. O extra passa a `pyarrow>=14.0.1`, e a doc de snapshot diz para carregar só snapshot de origem confiável e conferir o SHA-256 do `manifest.json` contra o que o autor publicou por outro canal (o do próprio snapshot não autentica a origem). O piso do `soupsieve` sobe de 1.6.1 para 2.9.0 (4 falhas de 2026 no parser de seletor, sem caminho no agrobr, que usa seletor literal), conferido com o BeautifulSoup 4.12.0: 0 diferença nas famílias com seletor CSS.

## [1.1.0] - 2026-06-18

### Added
- **cepea** — Café Robusta/Conilon (Indicador CEPEA/ESALQ, base Espírito Santo): novo produto `cafe_robusta` em `indicador()`, `ultimo()` e `pracas()` e no dataset `preco_diario`. A página `cafe.aspx` traz arábica e robusta em duas tabelas na mesma URL; o parser v1 passou a selecionar a tabela certa pelo título (`div.imagenet-table-titulo`, "robusta"/"conilon" vs "arábica") — arábica e os demais produtos permanecem inalterados (seleção só dispara para robusta). Fallback via Notícias Agrícolas com slug próprio (página conillon). Unidade `BRL/sc60kg`, praça Espírito Santo, regra de sanidade própria. Golden data real com as duas tabelas validando a seleção
- **inmet** — `historico(codigo, ano, agregacao=)`: dados horários de um ano inteiro **sem token**, via os ZIPs anuais públicos do dadoshistoricos do portal (2000+). Mesmo schema de saída de `estacao()` (reusa `agregar_diario`), header com matching por prefixo normalizado (nomes variam entre anos), latin-1/vírgula decimal, ZIP anual (~100 MB, todas as estações) com cache de 1 ano por processo — a segunda estação do mesmo ano não re-baixa. Validado live: A701/2025 com 8.760 horas em ~12s; segunda estação em 0,1s via cache
- **cftc** — Commitments of Traders (CFTC, fonte 39): `cftc.cot(commodity, start=, end=, combined=)` com o posicionamento semanal por categoria de trader (managed money, producer/merchant, swap dealers, other reportables) nos 12 contratos agro de CBOT/CME/ICE mapeados para canônicos (soja, milho, boi, açúcar, café...). Formato Disaggregated via API Socrata (sem autenticação), histórico desde jun/2006, `managed_money_net` calculado, changes nullable na 1ª semana de cada contrato. Dataset semântico `posicionamento_fundos` com contrato versionado (`cftc.cot` v1.0, pk `data`+`codigo_cftc`, 20 colunas). Licença `livre` (domínio público EUA)
- **unica** — Safra de cana Centro-Sul (UNICA, fonte 40): `unica.moagem_quinzenal(produto, regiao=)` (séries acumuladas de cana/açúcar/etanol do relatório PDF quinzenal), `unica.safra_resumo(periodo=)` (posição da safra com mix açúcar/etanol, ATR e rendimentos) e `unica.producao_historica(produto, safra_inicio=, safra_fim=)` (XLSX anual por estado, 1980/1981–2020/2021 — banco da fonte congelado, documentado). Parser PDF via pdfplumber (extra `[pdf]`) com sanidade (moagem < 700M t, mix 0–100%, ATR 60–160 kg/t). Licença `zona_cinza` com `warn_once`
- **comexstat** — aliases `defensivos` e `agrotoxicos` (NCM `3808`) em `NCM_PRODUTOS` para habilitar `comexstat.importacao(produto="defensivos", ...)`. Posicao 4 digitos cobre `3808.50` (POPs), `3808.91` (inseticidas), `3808.92` (fungicidas), `3808.93` (herbicidas), `3808.94` (desinfetantes), `3808.99` (outros)
- **acervo_fundiario** — Acervo Fundiario INCRA via download de shapefile estatico (`certificacao.incra.gov.br/csv_shp/zip/`). `sigef()` + `sigef_geo()` parcelas certificadas pos-2013 (15 UFs disponiveis). `snci()` + `snci_geo()` parcelas certificadas pre-2013 (10 UFs). `assentamentos()` + `assentamentos_geo()` projetos de reforma agraria (Brasil unico, filtro UF client-side). Servidor WFS legacy (`acervofundiario.incra.gov.br/i3geo/ogc.php`) descontinuado. Parser via `pyogrio.read_dataframe` (tabular, sem geometria) + `gpd.read_file` (geo) com `encoding="latin1"` (DBF). Streaming via `httpx.stream` + atomic write + SHA256 incremental. Cache filesystem com revalidacao por `Last-Modified` (HEAD-only quando atualizado), opt-out via `use_cache=False` ou `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED=1`. `asyncio.Lock` por (tema, UF). `bbox` aplicado no pyogrio para pre-filtro espacial (4-9x menos RAM/tempo). UF inválida no dataset levanta `SourceUnavailableError` com lista de disponiveis. Licenca `nc`
- **anec** — Embarques semanais por porto x produto (ANEC). 5 funcoes publicas: `embarques()` (porto x produto x last/current week), `embarques_mensais()`, `comparacao_anual()` (2025 x 2026), `destinos()` (top importadores), `articles_disponiveis()`. Dataset `embarques_anec` no registry. Parser PDF via pdfplumber (extract_words com x_tolerance=12, multi-section YoY parsing). 19 portos x 6 produtos x 2 periodos = 228 rows/semana. Cache filesystem com SHA256 + atomic write + asyncio.Lock por cuid + TTL listagem em memoria (`AGROBR_ANEC_LIST_TTL`, default 300s). MIN_YEAR=2026 (anos antigos `NotImplementedError`). Licenca `zona_cinza` com `UserWarning` na primeira chamada
- **sicar (alt)** — `imoveis_geo_stream()` itera os imoveis com geometria de uma UF em batches (`AsyncGenerator[GeoDataFrame, None]`), sem acumular tudo em memoria. `client.stream_imoveis_geo()` baixa as paginas da WFS sequencialmente com throttle pos-pagina, yieldando uma pagina por vez; `fetch_imoveis_geo()` agora delega a esse gerador. Dedup de `cod_imovel` aplicado entre batches. Async-only (sem suporte em `agrobr.sync`)
- **sicar (alt)** — `atualizado_apos` em `imoveis()`, `imoveis_geo()` e `imoveis_geo_stream()`: filtro CQL `data_atualizacao>'...'` (ISO date ou datetime, ex: `"2026-06-07"` ou `"2026-06-07T00:00:00"`) para sincronizar a base incrementalmente buscando so os registros atualizados apos uma data. UFs sem o campo `data_atualizacao` no layer WFS (SP, RS, PR, SC, RJ, TO) levantam `ValueError` antes do request
- **docs (i18n)** — documentação completa em inglês via mkdocs-static-i18n (sufixo `.en.md`, PT em `/docs/`, EN em `/docs/en/`, language switcher, busca PT+EN, `nav_translations`). Cobre as ~125 páginas — guias, referência de API, contratos de dataset, fontes, portabilidade e avançado. A tradução veio com uma auditoria doc↔código que corrigiu ~70 divergências em PT **e** EN: parâmetros não documentados (`as_polars`/`return_meta` em ~20 APIs), assinaturas erradas (`custo_producao(cultura)`, não `produto`), schemas e nullability desalinhados dos contratos (`estimativa_safra`, `producao_anual`, `preco_atacado`), números factuais (144 variantes de cultura, 5.571 municípios, rate limit CEPEA 5s), dependências desatualizadas, comandos de CLI e exceptions inexistentes na doc (`CacheError`, `agrobr cache clear`), e dead doc (camada de histórico removida em resilience)

### Improved
- **bcb** — `focus()` ganha `data_inicial` (filtro server-side `Data ge 'YYYY-MM-DD'`) e `max_registros` (interrompe a paginação nos N mais recentes): `focus("IPCA", data_inicial="2026-06-01")` baixa 40 registros em <1s em vez de ~47 mil em ~1 min. `sgs()` com `max_attempts=2`: série inexistente (a API pendura sem responder 404) falha em ~61s em vez de ~93s
- **bcb** — fallback BigQuery do crédito rural valida o billing project antes da query: `SourceUnavailableError` com instrução concreta (`AGROBR_BQ_BILLING_PROJECT=<project-id>` ou `billing_project_id` do basedosdados) em vez do erro críptico do Google ("not sure which project should be billed") após o OData falhar
- **b3** — `ajustes()` distingue ZIP vazio ("pregão de DD/MM/AAAA ainda não publicado" — o arquivo sai após o fechamento) de ZIP pequeno corrompido na mensagem de erro
- **queimadas** — read timeout 60s→120s (o CSV mensal de focos chega a ~13 MB e o dataserver do INPE oscila; 60s estourava em dias lentos)
- **b3** — janela de disponibilidade do open interest documentada (docstrings + docs): o arquivo `DerivativesOpenPosition` só existe para o dia corrente, publicado após o fechamento — datas passadas retornam 404 (verificado live: D-1, D-2 e D-5 úteis). `oi_historico()` retroativo retorna vazio por limitação da fonte; para posicionamento semanal histórico, `cftc.cot()` cobre 2006+
- **incra** — `MAX_FEATURES_TABULAR` e `MAX_FEATURES_GEO` aumentados de 500 para 1500 (~250% margem em cima dos 431 territorios atuais). Geometrias invalidas no GeoJSON sao reparadas automaticamente via `shapely.validation.make_valid()` no parser, com log `incra_quilombolas_geo_repaired` indicando quantas foram reparadas (atualmente 1/431, polygon degenerado, area preservada)
- **utils/result** — `build_source_meta()` aceita `raw_content_hash` como kwarg, eliminando o anti-pattern de mutacao pos-construcao (`meta.raw_content_hash = ...`) em `anec/api.py`. `cepea/api.py` mantem build incremental por design (cache + fetch + fallback exigem mutacao progressiva)

### Changed
- **limpeza (Canopy fase 3)** — anda e lista_suja: parsers decompostos em helpers coesos, comportamento idêntico (oráculos de teste intocados, +19 testes cobrindo helpers e edge cases). **anda**: `_parse_indicadores` 21→11 (extraídos `_find_year_anchor`/`_find_month_col`), `_expand_newline_cells` 20→11 (`_clean_cells`/`_max_cell_lines`, threshold mágico nomeado + documentado), `_parse_generic` 18→15 (guards `is not None` mortos removidos após early-return, `max()` retirado do loop), `fetch_entregas_pdf` 16→5 (`_select_pdf_target`/`_extract_ano_real`); `_make_record` elimina o dict de record duplicado em 4 funções; magic numbers (`3`/`5`/`30`) promovidos a constantes nomeadas. **lista_suja**: `parse_empregadores` 24→4 (`_extract_table_rows` para a varredura do PDF + `_build_dataframe` para montagem/tipagem). Score Canopy do módulo: anda 40→63, lista_suja 42→69
- **limpeza (Canopy fase 2)** — segunda rodada guiada por complexidade ciclomática e vulture: CC reduzida em 5 funções centrais com comportamento idêntico (`compare_fingerprints` 30→9, `Column.validate` 23→18, `parse_planilha` 31→26, `_extract_safra_columns` 30→18 incl. remoção de dead branch, `_parse_suprimento_wide` 30→22). Dead code removido símbolo a símbolo, verificado por grep de consumers e code search público (vulture 318→169): 13 models pydantic fora do pipeline de produção (os parsers entregam DataFrame validado via `contracts/`, não validação row-a-row), ~20 funções de client/parser órfãs e constantes não-referenciadas. **API pública removida** (sem uso interno nem externo, verificado): `StructuralMonitor` (de `agrobr.validators` — redundante com `compare_fingerprints`/`validate_against_baseline`) e `ParserDivergence` (de `agrobr.cepea.parsers` — tipo fantasma; o consenso de parsers real representa divergências como `dict`). Membros de classes públicas vivas e cobertura de testes do código vivo preservados
- **limpeza (Canopy)** — `compare_fingerprints` unificado em `validators/structural` (a cópia idêntica em `cepea/parsers/fingerprint` foi removida; o caminho antigo de import segue funcionando via re-export, e o ciclo cepea↔validators foi enfraquecido); dead code do `alt/` removido junto com os testes que só existiam para ele, verificado símbolo a símbolo com vulture + grep de consumers (`PrecoDiesel`/`VendaDiesel`/`MENSAL_*_URL`/`COLUNAS_XLSX_PRECOS*` do anp_diesel; `fetch_trafego` singular, `schema_version`, `build_ckan_resource_url`, `COLUNAS_V1`/`COLUNAS_PRACAS`/`ANO_FIM_V1` do antt_pedagio; `ApoliceSeguroRural`, `CLASSIFICACOES_VALIDAS`, `PERIODOS_VALIDOS` do mapa_psr; `STATUS_LABELS`/`TIPO_LABELS` do sicar — nenhum tinha consumer de produção); complexidade ciclomática do `alt/` reduzida com pipelines de funções coesas e comportamento idêntico (testes intocados): `parse_apolices`, `parse_precos` 27→≤9, `parse_trafego_v2` 27→≤9, `fluxo_pedagio` 23→≤8, `parse_trafego_v1` 21→≤7, `join_fluxo_pracas` 19→≤9, `parse_pracas` 16→≤7, `imoveis_geo` 16→≤10 (validações compartilhadas entre `imoveis`/`imoveis_geo`/`imoveis_geo_stream`), `_fetch_and_parse_municipios` 16→≤6
- **anda** — `entregas(ano)` com ano indisponível levanta `SourceUnavailableError` listando os anos disponíveis no site, em vez de baixar silenciosamente o PDF do ano mais recente — quem pedia 2024 podia receber 2026 sem aviso (só um log warning registrava o desvio)
- **cepea** — `pracas(produto)` levanta `ValueError` para produto desconhecido, consistente com o resto da API; antes retornava `[]`, indistinguível de produto válido sem praças mapeadas
- **config** — `agrobr.configure()` deprecated com `DeprecationWarning`: a função nunca teve efeito sobre fetch, cache ou fallback (nenhum código lia o que ela escrevia — incluindo `alternative_source=False`, que não desligava o fallback de licença nc). Alternativas reais: variáveis de ambiente `AGROBR_*` e `datasets.deterministic()`. Remoção prevista para 2.0
- **cli** — `agrobr snapshot use` virou validador: confirma que o snapshot existe e mostra como ativar o modo determinístico no código. O comando anterior crashava com nomes de snapshot (`date.fromisoformat("2025-Q4")`) e, mesmo corrigido, seria no-op — CLI roda em processo separado e o estado morre no exit; a mensagem ainda apontava um comando inexistente (`agrobr config mode normal`)
- **constants** — `Fonte.MAPA_PSR` adicionado ao enum (faltava, apesar de `agrobr/alt/mapa_psr/` ja existir e gerar o dataset `seguro_rural`). Entrada correspondente em `URLS` com `base` e `dataset`. `HEALTH_REGISTRY` e `SOURCE_DATASET_MAP` em `agrobr/health/registry.py` propagam automaticamente
- **mapbiomas** — `cobertura()` e `transicao()` passam a validar o parâmetro `colecao`: valores diferentes da coleção atual (10) levantam `ValueError` em vez de serem ignorados silenciosamente — antes o caller pedia `colecao=9` e recebia a coleção 10 sem aviso. `colecao=10`/`None` permanece inalterado. Docs PT/EN atualizadas. Seleção real de coleções anteriores fica para o futuro (formato/URL diferem por coleção)

### Fixed
- **validators** — `compare_fingerprints` podia retornar similaridade acima de 1.0 quando o fingerprint atual tinha classes de tabela duplicadas (a contagem de matches iterava o conjunto atual em vez do de referência): inflava a similaridade e podia mascarar drift de layout. Corrigido por construção, com teste de regressão
- **noticias_agricolas** — `parser_version` unificado na constante `PARSER_VERSION = 2`: o `Indicador` reportava versão 2 enquanto o `ParseError` do mesmo parser reportava 1, gerando proveniência inconsistente
- **ibama (fonte restaurada)** — o GeoServer WFS do siscom.ibama.gov.br foi desativado pela fonte (404 definitivo; infra do IBAMA migrou para Azure). `embargos()`/`embargos_geo()` migrados para o dump oficial do SIFISC em dadosabertos.ibama.gov.br (~47 MB zipado, ~114K termos, atualização mensal): schema novo com 15 colunas (seq_tad, status, cancelado, desembargo, lat/lon — `area_embargada_ha` substitui `area_desmatada_ha`; `infracao`/`legislacao`/`respeita_embargo` não existem no dump), geometrias WKT do próprio CSV (sem segundo download), filtros `uf`/`bbox` client-side, validação de truncamento, PII da fonte (nome/CPF/CNPJ) continua fora por política do projeto. Assinaturas públicas inalteradas; validado live (MT 12.970 registros em ~4s; geo RR 2.071 polígonos)
- **inmet** — autenticação dos dados observacionais corrigida em dois pontos: o token agora vai no path (`/token/estacao/.../{token}`, esquema real da API — antes ia como header `Authorization: Bearer`, que a API ignora) e a ordem dos segmentos segue o documentado (`/estacao/{inicio}/{fim}/{codigo}` — antes o código vinha primeiro). HTTP 204 sem token virou `SourceUnavailableError` com instrução (antes retornava DataFrame vazio silencioso); token inválido (200 com body "CHAVE INVÁLIDA!") virou erro claro; o token nunca aparece em logs/erros. `fetch_dados_estacao` re-levanta o erro quando todos os chunks falham (antes engolia e retornava vazio)
- **b3** — 429 sistemático no open interest corrigido: a cota real do `arquivos.b3.com.br` é 2 req/10s com penalidade progressiva (mensagem literal do 429) e cada `posicoes_abertas()` consome 2 requests (token + download) — a lib operava com 1s de espaçamento e 3 concorrentes. Source de rate dedicado `b3_arquivos` (5s entre requests, serializado) respeita a cota; ajustes (`www.b3.com.br`, sem essa cota) mantém o espaçamento de 1s
- **cache** — lock multi-processo do DuckDB tratado na raiz: `DuckDBStore` degrada para no-op (query vazia, upsert ignorado, warning `cache_degraded` único) quando o arquivo não abre — segundo processo agrobr (ex.: MCP server + script simultâneos) funciona via fetch direto em vez de `duckdb.IOException` crua em `cepea.ultimo()`/`datasets.preco_diario`. Generaliza o fix anterior que cobria apenas `cepea.indicador()`, incluindo falha de abertura por corrupção/permissão
- **antaq (fonte restaurada)** — triplo drift corrigido: (1) host atualizado de `web3.antaq.gov.br` (removido do DNS) para `estatistica.antaq.gov.br`; (2) download via `requests` em thread — o WAF do host novo rejeita o fingerprint TLS do httpx com 403, mas aceita requests/curl (retry preservado, status retriable retried, ZIP validado); (3) coluna `Mes` dos TXT mudou de número para nome abreviado PT ("nov") — parser converte via `month_to_number` canônico. Dataset `movimentacao_portuaria` agora agrega pela primary key do contrato (soma de peso/qt/teu por ano×mês×porto×mercadoria×sentido×navegação) — o parser entrega detalhe por carga e qualquer consulta real violava a PK. Validado live: 2024 completo = 25.780 rows agregadas, 202 portos, 1,34 bi ton. `requests>=2.32.0` promovida a dependência direta (era transitiva via sidrapy; ibge e antaq a importam)
- **ci** — action do Canopy pinada por SHA (recebia PAT com write apontando para branch mutável `@main`); `permissions: contents: read` declarado em tests/health_check/structure_monitor; `scripts/create_workflows.py` removido (sobrescreveria os workflows com templates desatualizados); `requirements.txt` removido (pressupunha um `streamlit_app.py` que não faz parte do repo); `examples/pipeline_v07.py` renomeado para `pipeline_cache.py`
- **ibge/cepea** — proveniência completa nos MetaInfo das source APIs: 10 sites do IBGE (PAM, LSPA, PPM, abate, silvicultura, extração vegetal, leite, PIB, censo, censo histórico) e os 2 caminhos do CEPEA (cache e fetch, incluindo fallback) agora preenchem `attempted_sources`/`selected_source` — antes ficavam vazios, violando o contrato de proveniência do projeto
- **http/browser** — reconexão não vaza mais a instância playwright anterior: ao detectar browser desconectado, a instância antiga é parada antes de iniciar a nova
- **cli** — logs do structlog vão para stderr com nível WARNING por padrão (`--verbose` reabilita INFO): a saída útil dos comandos (tabelas/JSON/CSV) não vem mais misturada com logs de debug no stdout
- **cli** — `python -m agrobr` funciona (`agrobr/__main__.py` criado) — relevante no Windows, onde `Scripts/` costuma ficar fora do PATH
- **cepea** — `indicador()` tolera `duckdb.Error` no cache (lock single-writer cross-process, ex.: streamlit + CLI simultâneos): query falha → segue para o fetch; upsert falha → entrega os dados sem persistir, com warning. Antes: traceback cru na API principal
- **cepea** — unidades corrigidas no parser: trigo de `BRL/sc60kg` para `BRL/ton` (o indicador CEPEA Trigo Paraná é cotado por tonelada — R$ 1.372/t, não por saca) e algodão de `BRL/@` para `cBRL/lb` (o indicador é centavos de real por libra-peso — 418,94 ¢/lb ≈ R$ 4,19/lb). Formatos alinhados aos que o parser do Notícias Agrícolas já usava — antes o dataset `preco_diario` entregava unidades diferentes para o mesmo produto dependendo de qual fonte do fallback respondia
- **utils/io** — `read_csv_safe`: o retry com latin-1 agora também converte falhas em `ParseError` (escapava `ParserError` cru classificado como "unexpected")
- **conab** — parser do balanço de oferta/demanda valida o header da aba Suprimento antes do acesso posicional às colunas (drift de layout vira `ParseError` claro em vez de dados deslocados silenciosos) e `_parse_decimal` delega ao `safe_float` canônico (trata milhar BR `"1.234,5"` e zero textual — antes `"0"` virava None)
- **zarc** — filtro `cultura` valida contra as culturas da tábua carregada com normalização de acento (`cultura="feijao"` encontra `"feijão"`); cultura inexistente levanta `ValueError` com a lista disponível — antes retornava 0 rows silencioso (a tábua 2026/2027 ainda não tem soja, por exemplo)
- **datasets** — interface do registry uniformizada: os 5 datasets com `fetch()` keyword-only (`embarques_anec`, `movimentacao_portuaria`, `queimadas`, `uso_do_solo`, `zoneamento_agricola`) agora aceitam o filtro principal como primeiro argumento posicional (`produto`, `mercadoria`, `bioma`, `tipo`, `cultura`) — `get_dataset(nome).fetch(valor)` funciona para todos os 35; chamadas por nome continuam idênticas. Teste paramétrico de assinatura garante a regra para datasets futuros
- **datasets** — `embarques_anec` ganha contrato registrado (era o único dataset cuja `_validate_contract` era no-op silencioso); skip de validação por contrato ausente agora é logado. `TypeError` dentro de um fetcher propaga imediatamente (erro de uso não vira "all sources failed"). Warning `source_empty_result` quando uma fonte retorna 0 rows com sucesso
- **utils/geo** — `fetch_wfs_paginated`: throttle entre páginas corrigido — `throttle_delay` era passado como `base_delay` (parâmetro de retry, sem efeito de espaçamento); agora dorme de fato após `throttle_after_page`. Requisição de contagem (hits) passa o `bbox` além do CQL — sem ele o total vinha superestimado e gerava páginas vazias. `fetch_arcgis_layer`: última página pede `min(max_record_count, total - offset)` — com `max_features` o servidor devolvia a página cheia (pedia 500, recebia 1000). Mesmo fix de throttle no paginador CSV do `alt.sicar`
- **acervo_fundiario** — `warn_once` para o `verify=False` (paridade com sicar/comexstat) e rate limit ligado via `RateLimiter` (a config `rate_limit_acervo_fundiario=3.0` existia desde o início mas nunca foi conectada; downloads do INCRA agora respeitam o espaçamento)
- **http/retry** — malha de retry reativada (era dead code em 4 frentes): nova `RetriableStatusError` (subclasse de `HTTPStatusError`) em `RETRIABLE_EXCEPTIONS` — CEPEA e Notícias Agrícolas a lançam para status retriable e agora são de fato retried com respeito a `Retry-After`, enquanto 403/404 de `raise_for_status` continuam propagando imediato (rotação de endpoint do CEPEA preservada). IBGE/sidrapy: exceções retriable corrigidas para `requests.exceptions.ConnectionError/Timeout` (sidrapy usa requests; os builtins nunca casavam) + `asyncio.wait_for` de 120s impede chamada sem timeout de segurar o semáforo do IBGE indefinidamente. INMET/NASA POWER: skip de chunk corrigido para capturar `SourceUnavailableError` (o `except HTTPStatusError` era inalcançável após `retry_on_status` — uma estação/chunk ruim matava a UF inteira). CONAB série histórica: `fetch_series_page` com `retry_on_status` (única função do arquivo sem retry)
- **datasets** — cascata de fontes classifica `SourceUnavailableError` como `"unavailable"` no log e na lista de erros (antes caía no genérico `"unexpected"`, distorcendo o diagnóstico do erro mais comum)
- **desmatamento** — coluna `uid` removida de `PRODES_COLUNAS_WFS`: o TerraBrasilis a removeu dos layers (drift) e o parser nunca a utilizou; pedir coluna inexistente derrubava todas as consultas PRODES com ServiceException. Dataset `desmatamento` agora agrega os polígonos individuais conforme o contrato (PRODES: soma de `area_km2` por `ano/uf/classe/bioma`; DETER: por `data/classe/uf/municipio/bioma`; `satelite`/`sensor` preservados quando únicos no grupo) — antes qualquer consulta real violava a primary key do contrato. Warning `desmatamento_*_truncated` quando o resultado atinge o teto de 50.000 features do WFS. Nota: os layers de Amazônia, Pantanal, Caatinga e Mata Atlântica estão quebrados no próprio GeoServer do INPE (ServiceException para qualquer cliente); Cerrado e Pampa operacionais
- **icmbio** — coluna `biomaibge` renomeada pelo INDE para `biomas` (drift): `PROPERTY_NAMES`, `RENAME_MAP` e filtro CQL atualizados. Filtro `bioma=` agora usa `ILIKE '%...%'`: os valores reais do campo são compostos e em caixa alta (`'AMAZÔNIA e CERRADO'`), e a igualdade exata retornava 0 rows silencioso (`bioma="Amazônia"` agora retorna 135 UCs)
- **bcb.focus** — query string montada manualmente com `%20`: o parser OData do Olinda rejeita espaço como `+` (encoding padrão do httpx) e respondia HTTP 400 para qualquer chamada com filtro. Indicador default corrigido de `'PIB Agropecuário'` para `'PIB Agropecuária'` (nome real na API; o anterior retornava 0 rows)
- **bcb.credito_rural** — `$select` enxuto por finalidade em todas as consultas OData: o backend SICOR/Olinda degradou e responde HTTP 500 ao serializar todas as colunas com `$top` alto; com `$select` o endpoint volta a aceitar páginas de 10.000. Query string manual (mesmo problema de encoding do focus). `COLUNAS_MAP` ganha aliases `VlInvest`/`QtdInvest`/`VlComerc`/`QtdComerc` — os endpoints de investimento e comercialização usam esses nomes e o parser não os mapeava (valor saía vazio). Página adaptativa: em HTTP 500 persistente reduz o `$top` (10000→2000→500) e segue paginando — o limiar do backend flutua com a carga
- **comtrade** — agregação forçada nos params (`motCode=0`, `partner2Code=0`, `customsCode=C00`): sem eles a API preview retorna breakdown por modo de transporte, todas as rows duplicavam na primary key e o dataset `comercio_internacional` falhava com ContractViolationError em qualquer consulta
- **datasets.futuros_agricolas** — chamada sem `data` agora busca o pregão mais recente (fallback de até 5 dias úteis): antes convertia ausência em `""` e quebrava com ValueError em `strptime` — o dataset nunca funcionou sem `data` explícita. `tipo='historico'` sem `inicio`/`fim` levanta ValueError claro
- **alt.sicar** — preflight de contagem do `imoveis()` usa a sessão com o contexto SSL do SICAR (antes criava client genérico sem `SECLEVEL=1` e falhava com handshake no Windows) e captura `SourceUnavailableError` além de `httpx.HTTPError` (o check de aviso derrubava a chamada inteira quando o GeoServer degradava)
- **normalize/encoding** — `ENCODING_CHAIN` corrigida: `windows-1252` agora vem antes de `iso-8859-1`. ISO-8859-1 decodifica qualquer sequência de bytes e tornava o resto da chain (incl. chardet) inalcançável em `decode_content`; Windows-1252 é superset com os caracteres tipográficos reais de fontes BR (aspas curvas, travessão)
- **tests** — suite default não roda mais testes de rede: marker `integration` adicionado ao `addopts` (os live continuam no CI semanal via `pytest -m integration`). Testes do RNC não gravam mais golden data no cache real do usuário (`~/.agrobr/cache/rnc/`) — fixture isola o cache em `tmp_path`
- **incra (breaking)** — Filtros `uf`/`fase` em `quilombolas()`/`quilombolas_geo()` migrados para client-side: o servidor CMR/FUNAI nao respeita `CQL_FILTER` nesses campos e retornava o dataset completo silenciosamente, mascarando os filtros. `FASES_VALIDAS` realinhado com os valores reais do servidor: `CCDRU`, `DECRETO`, `PORTARIA`, `RTID`, `TITULADO`, `TITULO ANULADO`, `TITULO PARCIAL`. Adicionado log `incra_quilombolas_truncated` quando o resultado tabular atinge `MAX_FEATURES_TABULAR` (espelhando o ja existente para o GeoJSON).

  **Migracao obrigatoria** — chamadas com fases antigas agora levantam `ValueError` (antes retornavam DataFrame vazio silenciosamente):

  | Antes | Depois |
  |-------|--------|
  | `quilombolas(fase="Titulada")` | `quilombolas(fase="TITULADO")` |
  | `quilombolas(fase="Em Titulacao")` | `quilombolas(fase="PORTARIA")` |
  | `quilombolas(fase="Decreto Publicado")` | `quilombolas(fase="DECRETO")` |
  | `quilombolas(fase="RTID em Elaboracao")` | `quilombolas(fase="RTID")` |
  | `quilombolas(fase="RTID Publicado")` | `quilombolas(fase="TITULO PARCIAL")` |

  Veja `docs/sources/incra.md` para a lista canonica e seus significados.
- **sicar (alt)** — `resumo(uf)` (sem filtro de municipio) falhava com `SSLV3_ALERT_HANDSHAKE_FAILURE` porque criava 5 clientes HTTP padrao em vez de reusar o `_ssl_ctx` (com `SECLEVEL=1`) necessario para o servidor `geoserver.car.gov.br`. Agora usa `httpx.AsyncClient` customizado e propaga via `client=http` em todas as chamadas `fetch_hits`
- **sicar (alt)** — `imoveis_geo()`/`imoveis()`/`resumo()` voltam a funcionar em truststores sem o root Sectigo R46 (ex.: `certifi` pre-2024, macOS Python via `uv`). `verify=False` reintroduzido com `warn_once` no padrao `comexstat`. Ref #72
- **antt_pedagio (alt)** — quando `dados.antt.gov.br` esta atras do WAF F5 BIG-IP ou fora do ar, o servidor responde 200 com HTML "Request Rejected" em vez de JSON/CSV. `_get_ckan_resources` agora captura `ValueError` em `response.json()` e levanta `SourceUnavailableError` com Content-Type + preview do body (antes cascateava `JSONDecodeError` confuso). `download_csv` detecta corpo iniciando com `<` ou `<!doctype` (apos strip) e levanta `SourceUnavailableError` mesmo se o tamanho passar do `MIN_CSV_SIZE`. 5 testes novos cobrindo WAF HTML, JSON truncado, `<!DOCTYPE`, e leading whitespace
- **bcb.sgs** — `sgs(serie, ultimos=N)` retornava HTTP 406 porque baixava a serie inteira (ex: SELIC desde 1986) e so depois aplicava `tail(N)`. Agora o client usa o endpoint `/dados/ultimos/{N}` quando `ultimos` e passado sem `data_inicial`/`data_final`, voltando ao endpoint `/dados` com range quando ha datas. Exemplo do README (`bcb.sgs('selic', ultimos=12)`) volta a funcionar
- **packaging** — wheel agora inclui `agrobr/health/**` e `agrobr/alerts/**` (antes ficavam excluidos em `pyproject.toml`). `pip install agrobr && agrobr health`/`agrobr doctor` quebravam com `ImportError` porque `cli.py` importa esses modulos. `agrobr/benchmark/**` continua excluido (dev-only). Aviso obsoleto removido de `docs/guides/docker.md`
- **docs/contagem de fontes** — README, `docs/index.md`, `docs/sources/index.md` e `index.html` alinhados em **40 fontes** (numero canonico a partir da enum `Fonte`). `CONAB Progresso` e `CONAB CEASA` seguem documentados como sub-datasets do CONAB, mas nao sao contados como fontes independentes. `ANEC`, `CFTC COT` e `MAPA PSR` aparecem na lista publica de fontes. Metrica "golden tests" divulgada sem numerador rigido, usando "fixtures de referencia por fonte" em README/docs
- **landing/cache** — card "Cache DuckDB" em `index.html` prometia "Historico permanente local. Sem re-download. Series temporais acumuladas", contradizendo `README.md:573` (historico permanente e responsabilidade do consumidor). Reescrito para "Cache local para CEPEA indicadores (smart TTL, expira 18h). Snapshots opcionais para reprodutibilidade"
- **mypy** — 4 erros de `pandas.DataFrame | polars.DataFrame` union sem narrow em `snapshots.py` (`.empty`, `.to_parquet`, `.columns.tolist`) e `cli.py` (`.empty`). Callers passam `as_polars=False` (default), entao o tipo real e sempre `pd.DataFrame`, mas o overload de `cepea.indicador` retorna union. Narrow via `cast(pd.DataFrame, ...)` (padrao ja usado em `acervo_fundiario/parser.py`, `anec/client.py`, `conab/parsers/v1.py`). Em `snapshots.py`, preservada a semantica original de `df is None` como skip silencioso (`continue`). Em `cli.py`, `pandas` movido para `if TYPE_CHECKING` (cast com string forward ref) para nao puxar pandas no startup do CLI. `mypy agrobr` agora passa limpo em 326 arquivos
- **health/imea** — `HEALTH_REGISTRY` batia em `https://api1.imea.com.br/api` (raiz da API) que retorna HTTP 404, marcando IMEA como `failed` no CI mesmo com servidor saudavel. Override no `_build_registry` agora usa `URLS[Fonte.IMEA]["cotacoes"]` (`/v2/mobile/cadeias`), endpoint canonico que lista cadeias e retorna 200. Validado live: `check_source(Fonte.IMEA)` agora retorna `ok` em ~2s
- **health/checker** — refactor `70d40f7` trocou 3 checkers dedicados por varredura generica usando `URLS[fonte]["base"]`, quebrando probe pra fontes `.gov.br` (pagina HTML raiz com WAF). Restaurada cobertura via overrides em `_build_registry` para endpoints leves (CKAN `package_show`, WFS `GetCapabilities`, ArcGIS `?f=pjson`, CSVs com HEAD, SIDRA query). SICAR usa `SSLContext` com `SECLEVEL=1`. COMEXSTAT usa CSV de `year - 2` com `verify=False`. Semaforo `concurrency=8` no `asyncio.gather`. Excecao sem mensagem (`httpx.ReadTimeout()`) usa `type(e).__name__`
- **health/categorizacao** — `tier="best_effort"` retorna WARNING em vez de FAILED, evitando alerta critico em fonte migrada/intermitente. ANTAQ marcada como `best_effort` (`web3.antaq.gov.br` descontinuado). `SourceHealthConfig.soft_block_codes` mapeia codigos HTTP por fonte como `soft_block` (WARNING) — CEPEA configurada com `(403,)` (Cloudflare)

## [1.0.5] - 2026-03-29

### Added
- **docker** — Dockerfile multi-stage (`python:3.11-slim`, non-root) com Playwright + Chromium + pdfplumber inclusos (default `EXTRAS="browser,pdf"`). `docker build -t agrobr .` + `docker run -it --rm agrobr`. Extras adicionais via `--build-arg EXTRAS="browser,pdf,polars"`. Fecha #54
- **usda** — commodity `cafe`/`coffee` (Coffee, Green, PSD code `0711100`) adicionada a `PSD_COMMODITIES`, `_COMMODITY_NAMES` e dataset `oferta_demanda_global`
- **imea** — aliases `boi`, `boi_gordo`, `bovinos` para cadeia bovinocultura (ID 2), consistente com nomenclatura canonica da lib
- **rnc** — Registro Nacional de Cultivares (CultivarWeb/MAPA). `registradas()` ~37K cultivares. `protegidas()` ~5K cultivares protegidas. Filtros cultivar, especie, grupo, mantenedor. CSV nativo, licenca `livre`
- **embrapa_solos** — Perfis de solo EMBRAPA/PronaSolos via WFS. `perfis()` tabular + `perfis_geo()` GeoDataFrame (34K+ pontos). `mapa_solos()` + `mapa_solos_geo()` classificacao SiBCS (2.8K poligonos). Licenca `nc`
- **rio_verde** — Ensaios cultivares Fundacao Rio Verde (MT). `ensaio_soja()` ~97 cultivares x 4 epocas. PDF parsing via pdfplumber. Licenca `zona_cinza`
- **bcb.sgs** — Series temporais BCB/SGS. `sgs(codigo)` com 17 series agricolas pre-mapeadas (Selic, IPCA, PIB agro, credito rural, cambio, IGP-M). Resolve nome ou codigo numerico. Sem auth, sem paginacao
- **bcb.ptax** — Cotacao dolar PTAX (compra/venda). `ptax()` ultimo mes, `ptax(data="15/01/2026")` dia especifico, `ptax(data_inicial=..., data_final=...)` periodo
- **bcb.focus** — Expectativas Focus/BCB. `focus("PIB Agropecuária")` projecoes consenso de mercado (media, mediana, min, max, respondentes). 9 indicadores economicos

### Improved
- **testes** — cobertura 88% → 92% (+110 testes). Dataset `_fetch_*` fetchers (23 datasets), CEPEA parser v1 edge cases (fallback de tabela, date 2-digit year, unidade detection), RNC filter branches, SFB endpoints, USDA client wrappers, base.py error paths (ContractViolation/Exception), anda/client, cepea/consensus, b3/parser, antt_pedagio/client, defensivos/parser, normalize/encoding. `@overload` excluido de coverage (artefato de medicao). `fail_under` 85 → 92
- **utils/geo** — `fetch_wfs` detecta resposta HTML (manutencao/redirect) e lanca `SourceUnavailableError` em vez de cascatear para `ParseError` confuso. Beneficia todas as fontes WFS (embrapa, ibama, sicar, funai, icmbio, incra)

### Security
- **conab/ceasa** — credenciais Pentaho removidas de logs, exceções e `MetaInfo.source_url`. URL segura (`PENTAHO_BASE`) usada em todos os vetores de exposição
- **b3** — token de sessão removido de logs e exceções em `fetch_posicoes_abertas()` (3 pontos de leak)
- **sicar** — wildcard escaping (`%`, `_`) adicionado ao filtro ILIKE de município para prevenir CQL wildcard injection
- **incra** — validação de `fase` via `FASES_VALIDAS` frozenset, mesmo padrão de FUNAI. Previne CQL injection via parâmetro não validado
- **deps** — bump `playwright>=1.55.1` (CVE-2025-59288 SSL bypass) e `streamlit>=1.54.0` (CVE-2024-42474, CVE-2025-1684, CVE-2026-33682)

### Fixed
- **cepea** — parser v1: coluna USD (`Valor US$*`) sobrescrevia valor BRL (100% dos precos no cache estavam errados). Reordenado condicionais: `var` antes de `data` (evita `Var./Dia` poluir data_value), exclusao explicita de colunas US$/USD
- **cepea** — parser v1: praca agora populada via dict `PRACAS` (20 produtos mapeados). Antes, `praca=None` hardcoded em todos os indicadores
- **cache** — `_to_row` normaliza `praca=None` para `""` (SQL UNIQUE constraint `NULL != NULL` causava duplicatas). Migration 5 limpa 338 linhas corrompidas com `praca IS NULL`
- **preco_diario** — `drop_duplicates(["data", "produto"])` em `_normalize` garante contrato PK 1 preco por (data, produto). Sem isso, dados mistos CEPEA+cache causavam `ContractViolationError`
- **conab** — parser v1 agora detecta safras year-only (`"Safra 2024"` → `2024/25`). Antes, celulas sem `/` no `if "Safra" in cell` branch eram silenciosamente ignoradas, afetando 6 culturas de inverno (trigo, aveia, canola, centeio, cevada, triticale)
- **conab** — serie historica: `_find_header_row` e `_normalize_safra_header` agora tratam float coercion do pandas (`2024.0` → `"2024"` → `"2024/25"`). Antes, `_YEAR_PATTERN` nao matchava `"2024.0"`
- **mapa_psr** — filtro `cultura` agora e accent-insensitive via `remover_acentos`. Antes, `"cafe"` nao matchava `"CAFE ARABICA"` (silent data loss com 0 registros sem aviso)
- **embrapa_solos** — URL WFS corrigida (`geoserver` → `geoserver/ows`), `CQL_FILTER` removido (mesmo fix de FUNAI/IBAMA/ICMBio v1.0.4), filtro UF pos-download
- **ibge** — `fetch_sidra` `iloc[1:]` removia silenciosamente a primeira UF (Rondonia) em toda query SIDRA multi-UF. Afetava 12 funcoes (abate, PAM, LSPA, PPM, PEVS, leite, PIB, censo). Em single-UF, causava 0 registros
- **rio_verde** — parser secao-sumario agora case-insensitive e accent-resilient. Exit trigger so em inicio de linha (evita falso positivo mid-line)
- **desmatamento** — `parse_deter_csv`/`parse_prodes_csv` agora retornam DataFrame vazio com schema correto em vez de `ParseError` quando WFS retorna header-only CSV (filtro sem resultados). Validacao de colunas mantida (CSV invalido ainda raisa)
- **sync** — `agrobr.sync.sicar` agora resolve diretamente (antes so acessivel via `sync.alt.sicar`). Wrapper sync aplicado corretamente

## [1.0.4] - 2026-03-22

### Added
- **normalize** — `coordenada_para_municipio(lat, lon)` geocodificação reversa offline. Lookup brute-force contra 5571 centroides IBGE (sub-ms, zero HTTP). Retorna `MunicipioInfo` ou `None` (threshold 1.5° ~167km)

### Fixed
- **funai/ibama/incra/icmbio** — funcoes `_geo()` removem `CQL_FILTER` da request WFS GeoJSON (geoservers gov.br retornam HTTP 500 ou HTML de erro ao combinar `CQL_FILTER` + `BBOX` + `outputFormat=application/json`). Filtros uf/fase/grupo/bioma agora aplicados pos-download no GeoDataFrame. Funcoes CSV nao afetadas
- **incra** — `GEOM_COLUMN` corrigido de `the_geom` para `geom`

## [1.0.3] - 2026-03-22

### Added
- **ibama** — embargos ambientais via WFS (siscom.ibama.gov.br). `embargos()` tabular + `embargos_geo()` GeoDataFrame. Paginacao WFS 2.0 (~89K features, 10K/pagina). Filtros uf/bbox. Dedup por numero_tad. PII excluido. Null geometry warning. Licenca ODbL
- **queimadas** — `focos_geo()` converte lat/lon existente para GeoDataFrame (Point, EPSG:4326). Wrapper sobre `focos()`, sem novo endpoint HTTP
- **funai** — terras indigenas via WFS (geoserver.funai.gov.br). `terras_indigenas()` tabular + `terras_indigenas_geo()` GeoDataFrame. Filtros uf/fase/bbox. ~740 TIs. Licenca CC BY-ND 3.0
- **icmbio** — unidades de conservacao federais via WFS (geoservicos.inde.gov.br). `ucs()` tabular + `ucs_geo()` GeoDataFrame. Filtros uf/grupo/bioma/bbox. 344 UCs. Dados publicos
- **incra** — territorios quilombolas via WFS (cmr.funai.gov.br). `quilombolas()` tabular + `quilombolas_geo()` GeoDataFrame. Filtros uf/fase/bbox. ~426 territorios. Dados publicos
- **mapbiomas_alerta** — alertas de desmatamento via GraphQL (plataforma.alerta.mapbiomas.org). `alertas()` tabular + `alertas_geo()` GeoDataFrame com WKT geometry + `alerta_info()` publico. Auth via token (AGROBR_MAPBIOMAS_ALERTA_TOKEN). Paginacao, filtros data/fonte/bbox. Fonte: livre (citacao)
- **lista_suja** — cadastro de empregadores (trabalho escravo) via PDF do MTE (gov.br/trabalho-e-emprego). `empregadores()` com filtro UF, warning PII automatico. Parser pdfplumber. Fonte: livre (Lei de Acesso a Informacao)
- **ana** — 4 layers ArcGIS REST do SNIRH: hidrografia (620K polylines), pivos_irrigacao (19.9K polygons), demanda_irrigacao (265K polygons), disponibilidade_hidrica (42K polylines). Paginacao automatica. Fonte: livre
- **sfb** — 3 layers ArcGIS REST do Servico Florestal: CNFP florestas publicas (20.8K polygons), concessoes florestais (8 polygons), IFN conglomerados (14.5K points). Fonte: livre

### Improved
- **utils/geo** — `check_geopandas()` extraido de desmatamento/sicar para `utils/geo.py` (dedup 2 copias). `validate_bbox()` canonica com checagem min<max (3 implementacoes inconsistentes consolidadas). `fetch_wfs()` agora aceita `base_delay` para throttle de paginacao e `client` opcional para connection reuse em paginacao
- **utils/geo** — dedup WFS: `build_wfs_url()` centraliza construcao de URL WFS com dispatch automatico v1/v2 (typeNames/count vs typeName/maxFeatures), elimina 4 copias em funai/icmbio/incra/ibama. `parse_wfs_hits()` centraliza parsing de `numberMatched` (2 copias ibama+sicar). `parse_geojson_base()` centraliza boilerplate GeoJSON (json.loads, empty check, truncation warning, null geom, from_features, required cols) — 5 parsers migrados
- **utils/geo** — infra ArcGIS REST compartilhada: `LayerConfig` TypedDict, `build_arcgis_query_url()`, `fetch_arcgis_count()`, `fetch_arcgis_layer()`, `parse_arcgis_tabular()`, `parse_arcgis_geojson()` — reusados por ANA e SFB
- **utils/validation** — `validate_uf()` com `UFS_VALIDAS` (27 UFs reais) substitui 10 copias de `_UF_RE` regex em 6 modulos WFS
- **utils/io** — `concat_csv_pages()` centraliza loop de concat paginado CSV (2 copias ibama+sicar eliminadas)
- **ibama/sicar** — paginacao WFS agora reutiliza conexao HTTP (1 TLS handshake em vez de N)

## [1.0.2] - 2026-03-20

### Improved
- **http** — log hygiene cross-cutting: URLs movidas de info/warning para debug em 25 source clients. Logs de producao mais limpos e sem vazamento de endpoints

### Security
- **b3** — token de autenticacao JWT removido de logs info. `url=download_url` (que continha `?token=...`) substituido por `source="b3", size=N`

## [1.0.1] - 2026-03-19

### Added
- **defensivos** — dados de agrotoxicos registrados no Brasil (Agrofit/MAPA). 3 funcoes: `formulados()`, `autorizacoes()`, `tecnicos()`. Fonte: Portal de Dados Abertos MAPA (CC-BY). Cache Parquet 24h. ~8K formulados, ~267K autorizacoes, ~2.8K tecnicos. Filtros por ingrediente ativo, classe, titular, cultura, organicos. 51 testes, golden data

### Changed
- **deps** — duckdb `>=1.4.4` → `>=1.5.0`. Non-blocking checkpointing, 17% throughput, K-way merge sort, late materialization. Workaround em conftest.py para `_duckdb._sqltypes` coverage+Python 3.14

### Fixed
- **noticias_agricolas/parser** — `parse_indicador()` agora reconhece 3 layouts adicionais do NA: tabelas com coluna `Vencimento` (acucar, acucar_refinado), tabelas com coluna `Estado` (suino), e tabelas sem coluna de data com data no div `Fechamento` (leite). Novo helper `_extract_parent_date()`. Bug pre-existente corrigido: `has_region_col` vazava entre iteracoes de tabela
- **b3/api** — `ajustes(data=)` agora aceita formato ISO (`"2025-03-07"`) alem de BR (`"07/03/2025"`) e `date` object. Antes: ISO string passava direto pro client e causava `ValueError` no `strptime("%d/%m/%Y")`
- **alt/sicar** — `data_atualizacao` removido de `PROPERTY_NAMES` WFS. Campo nao existe em todos os layers estaduais (SP, RS, PR, SC, RJ, TO), causando 400 Bad Request. Parser ja tratava campo ausente via `.get()`. Dedup em `imoveis()`/`imoveis_geo()` simplificado (sort por `cod_imovel` apenas, sem `data_atualizacao` que era all-NaT)
- **bcb/bigquery_client** — `fetch_credito_rural_bigquery()` agora tem timeout de 120s via `asyncio.wait_for()`. Antes: fallback BigQuery podia travar indefinidamente quando OData retornava 500

## [1.0.0] - 2026-03-10

### Added
- **CLI** — `cepea indicador` funcional (era stub). Suporta `--inicio`, `--fim`, `--ultimo`, `--formato`
- **docs** — guia dedicado de snapshots (`docs/guides/snapshots.md`): criação, listagem, uso, delete, modo determinístico CLI vs programático
- **docs** — docstring Google style em `cepea.indicador()` (10 params, inclui `_moeda`, `validate_sanity`, `force_refresh`, `offline`)
- **py.typed** — PEP 561 marker para suporte a type checking em projetos downstream
- **datasets** — 2 novos datasets na camada semântica (32→34): `movimentacao_portuaria` (ANTAQ, single-source, keyword-only, 21 colunas, 6 filtros opcionais, reutiliza `MOVIMENTACAO_PORTUARIA_V1`), `condicao_lavouras` (SEAB/DERAL, 14 culturas PR, normalização condicao vazia→plantio/colheita, contrato `CONDICAO_LAVOURAS_V1`)
- **datasets** — 3 novos datasets na camada semântica (29→32): `oferta_demanda_global` (USDA PSD, long/pivot format, 8 commodities, skip contract quando `pivot=True`, contrato `OFERTA_DEMANDA_GLOBAL_V1`), `comercio_internacional` (UN Comtrade, bilateral global por HS code, 17 produtos, reutiliza `COMERCIO_BILATERAL_V1`), `zoneamento_agricola` (ZARC/MAPA, janelas de plantio por município/cultura/solo, 36 decêndios, keyword-only, contrato `ZONEAMENTO_AGRICOLA_V1`)
- **datasets** — 3 novos datasets ambientais/ESG na camada semântica (26→29): `desmatamento` (INPE, `tipo=` dispatch prodes/deter, 6 biomas, normalização bioma, fail-fast DETER fora Amazônia/Cerrado, reutiliza `DESMATAMENTO_PRODES_V1`/`DESMATAMENTO_DETER_V1`), `uso_do_solo` (MapBiomas, `tipo=` dispatch cobertura/transição, suporte `nivel="municipio"` com skip contract, reutiliza `MAPBIOMAS_COBERTURA_V1`/`MAPBIOMAS_TRANSICAO_V1`), `queimadas` (INPE, focos de calor por satélite, dual-register contrato, reutiliza `FOCOS_QUEIMADAS_V1`)
- **datasets** — 2 novos datasets na camada semântica (24→26): `clima` (INMET→NASA POWER, dual-mode UF mensal + estação diária, contratos `CLIMA_V1`/`CLIMA_ESTACAO_V1`), `futuros_agricolas` (B3, `tipo=` dispatch ajustes/historico/posicoes, 7 contratos agro, reutiliza `AJUSTE_DIARIO_V1`/`POSICOES_ABERTAS_V1`, fail-fast soja_fob+posicoes)
- **datasets** — 3 novos datasets na camada semântica (21→24): `serie_historica_safra` (CONAB, 32 culturas desde 1976/77, contrato `SERIE_HISTORICA_SAFRA_V1`), `preco_atacado` (CONAB CEASA/PROHORT, preços diários hortifrúti, reutiliza `PRECO_ATACADO_V1`), `seguro_rural` (MAPA PSR, apólices e sinistros com `tipo=` dispatch, reutiliza `MAPA_PSR_APOLICES_V1`/`MAPA_PSR_SINISTROS_V1`)
- **datasets** — 3 novos datasets na camada semântica (18→21): `importacao` (ComexStat, mirror de exportacao), `pib_agro` (IBGE SIDRA, PIB Agropecuária por setor/trimestre com `precos` na PK), `progresso_safra` (CONAB, progresso semanal semeadura/colheita). Contratos `IMPORTACAO_V1`, `PIB_AGRO_V1` + dual register `progresso_safra` para `CONAB_PROGRESSO_V1`
- **comexstat** — `importacao()` para dados de importação ComexStat (MDIC/SECEX). Mesma interface de `exportacao()`: filtro por produto (NCM), ano, UF, agregação mensal/detalhado, `as_polars`, `return_meta`. Parser refatorado com `_parse_comexstat_csv()` compartilhado (mensagens de erro corretas por fluxo). `_fetch_comexstat()` helper elimina duplicação entre export/import. 10 testes novos (6 API + 4 parser)
- **ZARC** — Zoneamento Agricola de Risco Climatico (janelas de plantio por municipio/cultura/solo). Fonte MAPA/Embrapa via CKAN (CC-BY). `zoneamento()` com filtros cultura/uf/municipio/safra/solo/ciclo, `culturas()`, `safras_disponiveis()`. Session cache para CSVs grandes. 31 testes, golden data
- **utils/io** — `open_excel_safe()` e `read_excel_safe()` helpers com fallback automatico para `python-calamine` (Rust, MIT). Se openpyxl falhar (ex: XLSX com estilos/fills malformados), tenta calamine que ignora estilos e extrai apenas dados. Guard xlrd: se `engine="xlrd"`, nao tenta calamine (re-raise direto). 9 parsers migrados (19 operacoes Excel em 9 arquivos): conab/progresso, abiove, conab/serie_historica, deral (multi-sheet via `open_excel_safe`); alt/anp_diesel, mapbiomas, anda, conab/parsers/v1, conab/custo_producao (single-sheet via `read_excel_safe`)
- **deps** — `python-calamine>=0.3.0` como dependencia core (749KB, zero deps Python, engine Rust para leitura Excel)

### Improved
- **docs** — README: seção "Modo Determinístico" expandida com snapshots (CLI + programático). `docs/index.md`: menção a snapshots na feature list. `mkdocs.yml`: nav entry para guia de snapshots
- **docs** — README, docs/index.md, index.html e mkdocs.yml atualizados com todos os 34 datasets. Tabelas ordenadas alfabeticamente, 35 contract docs no nav mkdocs, 34 dataset cards no index.html. Typos de acentuacao corrigidos (Producao→Produção, carvao→carvão, acai→açaí)
- **CI** — Python 3.13 adicionado a matrix de testes. Classifier `Programming Language :: Python :: 3.13` em pyproject.toml
- **coverage** — benchmark/ excluido do coverage (`omit`). 5093+ testes, 88% cobertura (era 4906/87%). ~190 testes novos: datasets/ (snapshots 49→97%, datasets/ multiple files pushed to 80%+, conab/progresso/client 19→60%+). Dead code `_parse_week_date` removido. Test quality audit: singleton mutation fix, `@requires_pyarrow` guards, weak assertion strengthening
- **inmet/api** — `estacoes()` agora suporta `as_polars`, `return_meta` e `build_source_meta()` (unica source API que nao tinha os 3)
- **conab/api** — `balanco()` e `brasil_total()` agora suportam `return_meta` com overloads tipados e `build_source_meta()`. `safras()` migrado de MetaInfo inline para `build_source_meta()`
- **b3** — `ajustes()` agora usa BVBG-086 ZIP como fonte primaria (endpoint `pesquisapregao/download`, XML streaming com `lxml.etree.iterparse`). Fallback automatico para HTML legado em caso de falha. `parse_ajustes_zip()` com filtragem agro, nested ZIP extraction e wrapping de erros em `ParseError`. URL antiga mantida para fallback
- **constants** — `HTTPSettings.max_concurrent_default`, `max_concurrent_b3`, `max_concurrent_ibge` para concorrencia configuravel por fonte no RateLimiter (default 1 = sem mudanca de comportamento)
- **normalize/numeric** — `parse_numeric_br` canonica para parsing numerico formato BR (ponto=milhar, virgula=decimal). Substitui 3 implementacoes duplicadas em `alt/` parsers
- **normalize/encoding** — `detect_encoding_chain` para deteccao rapida de encoding via probe chain (UTF-8, UTF-8-sig, Windows-1252, ISO-8859-1, chardet fallback). Substitui 2 implementacoes duplicadas em `alt/` parsers
- **utils/result** — `finalize_result` helper com overloads tipados para epilogo polars/return_meta. Substitui ~140 linhas de boilerplate em ibge/ (10x), conab/api.py (3x), cepea/api.py (1x)
- **utils/warnings** — `warn_once(key, message)` helper para warnings de licenca. Elimina 7 flags `_WARNED` globais e `global _WARNED # noqa: PLW0603` boilerplate em 6 modulos (abiove, anda, b3, conab/ceasa, imea, noticias_agricolas). `warn_once_reset()` para testes
- **normalize/numeric** — `safe_float` canonico para conversao numerica com suporte a strip chars, null markers configuraveis, nan_as_none, treat_zero_as_none e heuristica ABIOVE (3 digitos apos ponto = milhar). Substitui 5 implementacoes duplicadas `_safe_float` em anda, abiove, deral, conab/serie_historica, conab/custo_producao. `conab/progresso` mantido como wrapper local `_parse_pct`
- **utils/result** — `build_source_meta()` helper para construcao de MetaInfo em source APIs. Absorve 13 campos repetitivos (`fetched_at`, `fetch_timestamp`, `records_count`, `columns`, etc) com defaults inteligentes. Substitui 34 blocos de ~17 linhas em 19 arquivos (abiove, anda, antaq, b3, bcb, comexstat, comtrade, deral, desmatamento, imea, inmet, mapbiomas, nasa_power, queimadas, usda, alt/antt_pedagio, alt/mapa_psr, alt/anp_diesel, alt/sicar)
- **as_polars** — `as_polars=True` suportado em todas as 51 source APIs (37 migradas neste ciclo + 14 anteriores). Todas usam `finalize_result()` para conversao pandas→polars e epilogo return_meta
- **normalize/dates** — `MESES_PT` dict canonico (26 entries: 12 full + 12 abrev + "marco" sem acento) e `month_to_number()` helper. Substitui 3 dicts duplicados em abiove/models, anda/parser, anp_diesel/parser (~60 linhas)
- **normalize/regions** — `UFS_VALIDAS` frozenset canonico com 27 UFs. Substitui 4 frozensets literais identicos em antt_pedagio, mapa_psr, anp_diesel, sicar (~120 linhas)
- **utils/validation** — `validate_year_uf()` helper para validacao de UF e range de anos. Substitui 2 `_validate_params` identicos + 6 inline UF checks em antt_pedagio, mapa_psr, anp_diesel, sicar
- **utils/io** — `read_csv_safe()` helper para leitura CSV com encoding fallback (utf-8 → latin-1). Substitui 4 blocos try/except em desmatamento (2x), sicar, queimadas
- **utils/html** — `parse_links_from_html()` canonico para extracao de links HTML com filtro regex, dedup e base_url. Substitui 3 implementacoes em anda/client, conab/serie_historica/client, conab/custo_producao/client (~120 linhas)
- **normalize/regions** — `normalizar_bioma()`, `BIOMAS` e `BIOMAS_VALIDOS` canonicos. Substitui 3 implementacoes identicas em desmatamento/models, mapbiomas/models, queimadas/models (~45 linhas). Re-exports mantidos para backward compat. Exportado em `agrobr.normalize` como API publica
- **desmatamento/api** — `normalizar_bioma()` wired nos 4 entry points (`prodes`, `prodes_geo`, `deter`, `deter_geo`). Parametro `bioma` aceita lowercase/sem acento (ex: `"cerrado"` → `"Cerrado"`, `"amazonia"` → `"Amazônia"`)
- **http/retry** — `retry_on_status()` agora captura transport exceptions (`TimeoutException`, `NetworkError`, `RemoteProtocolError`) com retry exponencial. Antes: zero retries para falhas de rede, excecao crua propagava. Agora: retries com backoff identico ao de status codes retriable, `SourceUnavailableError` no exhaustion. 24+ source clients beneficiados automaticamente
- **inmet/client** — HTTP 403 agora levanta `SourceUnavailableError` com mensagem sobre `AGROBR_INMET_TOKEN` em vez de `httpx.HTTPStatusError` generico. `fetch_dados_estacoes_uf` propaga 403 em vez de engolir silenciosamente
- **http/rate_limiter** — `RateLimiter` com concorrencia configuravel via `HTTPSettings.max_concurrent_{source}`. `Semaphore(1)` hardcoded substituido por `Semaphore(config)`. Pattern "burst then pause": N requests simultaneos seguidos de pausa de rate_limit delay. Default 1 (zero mudanca de comportamento para fontes nao configuradas). B3 e IBGE configurados com concorrencia 3
- **b3/api** — `historico()` e `oi_historico()` migrados de while-loop sequencial + `asyncio.sleep(1.0)` para `asyncio.gather()` com lista de weekdays. Rate limiting delegado ao RateLimiter (Semaphore(3) para B3). ~4.4x speedup esperado (22 dias: ~44s → ~10s)
- **inmet/client** — `_get_json()` e `fetch_dados_estacao()` aceitam `http: AsyncClient | None` para reuso de conexao. `fetch_dados_estacoes_uf()` cria shared client para todas as estacoes. Elimina criacao de novo AsyncClient por request
- **nasa_power/client** — `_get_json()` aceita `http: AsyncClient | None`. `fetch_daily()` cria shared client para chunking loop. `RATE_LIMIT_DELAY` e `asyncio.sleep()` removidos (RateLimiter ja enforce delay entre requests)
- **ibge/api** — `lspa()` migrado de for-loop sequencial para `asyncio.gather()` (max 3 sub-produtos em paralelo). ~3x speedup para feijao (3 sub-products)
- **ibge/censo_api** — `censo_agro()` e `_fetch_censo_multi_table()` migrados de for-loops sequenciais para `asyncio.gather()` (2-3 fetches em paralelo)
- **ibge/ helpers** — `resolve_ibge_code()` e `resolve_period()` em `_helpers.py` substituem 13 blocos duplicados (6 ibge_code + 7 period) em api.py, pesquisas_api.py, censo_api.py. `calculate_expiry` padronizado para flat strings em 6 call sites (api.py, censo_api.py). `_UF_CODES` promovido a constante module-level em client.py
- **noticias_agricolas** — adicionado a `sync.py` e `__init__.py` (antes ausente do namespace publico e da API sync)
- **conab/api.py** — early returns de `safras()`, `balanco()` e `brasil_total()` agora passam por `finalize_result()`, respeitando `as_polars` e populando MetaInfo completo em resultados vazios
- **contracts/__init__.py** — `_auto_discover_contracts` removido de `__all__` (funcao privada)
- **export.py** — `_get_version()` substituido por `from agrobr import __version__` direto (elimina funcao wrapper)
- **normalize/crops.py** — `_CULTURAS_SEM_ACENTO` dict pre-construido no module level: `normalizar_cultura()` de O(n) loop para O(1) lookup
- **alt/antt_pedagio** — `except Exception:` substituido por `except httpx.HTTPError:` (fetch) e `except (ParseError, KeyError, ValueError):` (parse/join)
- **alt/sicar** — `except Exception:` substituido por `except httpx.HTTPError:` no pre-flight hit count
- **conab/parsers/v1.py** — `except (ParseError, Exception)` simplificado para `except Exception` (ParseError ja e subclasse)
- **ibge/ module split** — `ibge/api.py` (2025 linhas) dividido em `censo_api.py`, `pesquisas_api.py`, `censo_tables.py` e `_helpers.py`. API publica inalterada via re-exports em `__init__.py`. Dead branch e `import re` removidos. `NIVEL_MAP` extraido como constante compartilhada
- **sync.py** — 23 subclasses stub vazias de `_SyncModule` removidas (258→123 linhas). `_MODULE_CLASSES` dict eliminado, `__getattr__` e `_SyncAlt` instanciam `_SyncModule` diretamente. API publica inalterada
- **cli.py** — `_output_df` helper extrai 6 blocos identicos de formatacao output (json/csv/table)
- **CacheSettings** — 32 campos `ttl_*` e `stale_multiplier` removidos (dead code, nunca lidos). `cache/policies.py::POLICIES` e a fonte unica de TTL
- **Test infrastructure** — `tests/helpers.py` com 3 factories (`make_mock_response`, `make_mock_async_client`, `make_alert_settings`). Elimina `_mock_response` duplicado em 18 arquivos e boilerplate `__aenter__`/`__aexit__` em 23 arquivos. `RETRY_SLEEP` e `make_sleep_tracker()` extraidos de 7 test files duplicados (anda, abiove, bcb, usda, imea, deral, comexstat). 4 fixtures mortas + 3 constantes mortas removidas do conftest.py
- **test_dataset_common** — `ALL_DATASETS` dinamico via `registry.list_datasets()` (18 datasets, era 11 hardcoded). `update_frequency` assertion expandida para `continuous`, `decennial`, `never`
- **test_cache/test_migrations** — 9 cenarios para `cache/migrations.py` (fresh DB, partial upgrade, idempotent, column/index creation). Zero cobertura anterior
- **datasets/ MetaInfo dedup** — `BaseDataset._build_meta()` e `_unpack_result()` em `datasets/base.py`. 18 blocos MetaInfo (~18 linhas cada) colapsados para 1 linha. 20 `isinstance(result, tuple)` patterns substituidos por `_unpack_result()`. `datetime`/`UTC` imports removidos de 14 datasets. ~380 linhas de boilerplate eliminadas
- **37 source APIs** migradas para `finalize_result()`: bcb, b3 (4), inmet (2), nasa_power (2), comexstat, usda, anda, deral, abiove, imea, antaq, comtrade (2), desmatamento (2), queimadas, mapbiomas (2), alt/anp_diesel (2), alt/antt_pedagio (2), alt/mapa_psr (2), alt/sicar (2), conab/ceasa, conab/serie_historica, conab/progresso, conab/custo_producao, ibge/legacy_api, ibge/censo_municipal_1985. Inline `if return_meta` boilerplate eliminado, `as_polars` habilitado em todas
- **4 CONAB sub-modulos** (ceasa, serie_historica, progresso, custo_producao) migrados de `MetaInfo(...)` inline com `datetime.now(UTC)` duplicado para `build_source_meta()` com `utcnow()` unico
- **ibge/legacy_api** — inline `as_polars` handling e `MetaInfo` manual substituidos por `finalize_result()` + `build_source_meta()`. Dead branch `len(df) if not isinstance(df, tuple)` removido
- **ibge/censo_municipal_1985** — `MetaInfo` inline substituido por `build_source_meta()` + `finalize_result()`. Timing via `time.perf_counter()` adicionado
- **test_datasets** — `conftest.py` com `mock_source_meta()` + `make_source()` helpers reutilizaveis. 39 testes novos cobrindo 6 datasets (preco_diario 1→9, producao_anual 1→7, estimativa_safra 1→7, balanco 6, extrativismo_vegetal 5, silvicultura 5). Fallback em cascata testado para 3 datasets multi-source. Testes de normalize, invalid produto, return_meta e SourceUnavailableError
- **cepea/api** — `indicador()` (190→~130 linhas) decomposto em 5 helpers: `_normalize_dates` (conversao str→date + defaults), `_needs_fetch` (gap detection com weekday check), `_warn_stale` (deduplica 2 blocos identicos de stale warning), `_FetchResult` NamedTuple (tipo estruturado para 7 campos de retorno), `_fetch_and_parse` (fetch + dispatch parser CEPEA/NA). Imports `warnings`/`StaleDataWarning` promovidos a top-level
- **ibge/api** — `abate()` (160→~85 linhas) decomposto em 2 helpers: `_detect_abate_columns` (col_map estatico + sniffing dinamico D2C/D3C) e `_merge_cabecas_peso` (split por variavel_cod, merge cabecas/peso, formatting output)
- **conab/progresso/parser** — `parse_progresso_xlsx()` (156→~110 linhas) decomposto em 2 helpers: `_read_xlsx_sheet` (abertura workbook + busca sheet + validacao) e `_build_record` (deduplica 2 blocos identicos de record-building BR/estado)
- **http/settings** — `get_timeout(read=...)` factory com override de read timeout. 26 client modules migrados de inline `httpx.Timeout(...)` + `HTTPSettings()` para `get_timeout()`. Elimina `_settings = HTTPSettings()` e `_get_timeout()` locais
- **URL dedup** — 6 constantes de URL duplicadas em modules substituidas por referencias a `URLS` central (PENTAHO_BASE, FTP_BASE, WFS_BASE, CKAN_BASE, SHLP_BASE, VENDAS_DIESEL_CSV_URL). `SIDRA_BASE` em `ibge/_helpers.py` substitui 18 hardcoded `"https://sidra.ibge.gov.br"`
- **Dead code purge** — 5 modulos mortos removidos (stability.py, telemetry/, aliases.py, utils/logging.py). 15+ classes/funcoes mortas removidas (CacheEntry, HistoryEntry, CacheError, 4 dead Warnings, InvalidationReason, Contract.from_json/from_dict, TICKER_PARA_CONTRATO, data_para_safra, mes_para_numero, numero_para_mes, extrair_uf_municipio, validar_regiao, get_rate_limit, get_client_kwargs, cleanup stub). Constantes mortas: TelemetrySettings, CONFIDENCE_MEDIUM, CacheSettings.offline_mode/cache_max_age_days/history_max_age_days, 5 URLs mortas. AgrobrConfig.timeout_seconds removido
- **Dead cache cleanup** — `cache/history.py` deletado (HistoryManager inteiro, 179 linhas, zero callers, bug latente em `query()`). 8 metodos mortos removidos de `DuckDBStore` (`cache_get`, `cache_set`, `cache_invalidate`, `cache_delete`, `cache_clear`, `history_save`, `history_get`, `indicadores_get_dates`). 3 funcoes mortas de `keys.py` (`parse_cache_key`, `is_legacy_key`, `legacy_key_prefix`). 5 funcoes mortas de `policies.py` (`get_ttl`, `is_expired`, `is_stale_acceptable`, `get_stale_max`, `should_refresh`). `CacheSettings.strict_mode` e `save_to_history` removidos (zero callers apos remocao de `cache_get`/`history_save`). ~400 linhas de producao, ~700 linhas de testes. APIs vivas mantidas: `indicadores_query`/`indicadores_upsert` (CEPEA), `get_store`, `build_cache_key`, `calculate_expiry`, `get_policy`, schema init
- **test_datasets (parte 2)** — 33 testes novos para 5 datasets restantes: pecuaria_municipal (8), abate_trimestral (6), censo_agropecuario (6), censo_agropecuario_legado (6), leite_industrial (7). Cobertura fetch/meta/invalid_produto/kwargs/snapshot/source_fail
- **test_golden** — 7 secoes golden wired: b3 (ajustes HTML + posicoes CSV), comtrade (comercio + mirror), queimadas (focos CSV), desmatamento (4 variants: prodes/deter x csv/geojson), conab_ceasa (precos), conab_progresso (xlsx), mapbiomas (cobertura + transicao). `_assert_dataframe_golden` corrigido para None/NaN (pd.NA nullable int, IEEE 754 NaN). 2 golden dirs reestruturados (conab_progresso, mapbiomas → sub-case + metadata.json)
- **alt/ perf** — 27 `.copy()` desnecessarios removidos de 4 parsers (mapa_psr 9, antt_pedagio/api 10, antt_pedagio/parser 3, anp_diesel 5). 6 mantidos por mutation safety. `_build_vendas_df` em anp_diesel vectorizado: iterrows substituido por `pd.to_numeric`/`.str.map`/`.apply` column-wise. Imports mortos `re`/`date` removidos
- **conab/custo_producao/parser** — multi-format support: `_find_header()` substitui `_find_header_row()` com 2-step detection (single-row scan + 2-row sliding window para headers split). 2-row window usa best-quality selection: avalia todos os candidatos via `_identify_columns` e escolhe o que produz col_map mais completo (bonus para candidatos com ambas colunas obrigatorias item+valor_ha). Corrige culturas especiais (abacaxi, banana, cebola) cujo header split em 3 rows causava false positive no window anterior (keyword "preço" casava em "A PREÇOS DE:" sem mapear valor_ha). `_identify_columns()` expandido com patterns para "custo por ha", "custo/ha", "custo por hectare" (valor_ha) e "custo/{unit}" prefix (preco_unitario). Guards `if key not in mapping` em todas as chaves previnem overwrite (ex: PARTICIPAÇÃO CV ganha sobre CT). `_refine_valor_column()` novo para corrigir offset de merged cells (scan col+1/col+2 por dados numericos). `select_data_sheet()` novo para selecao robusta de sheet de dados (skip Indice/Index, filtro por UF/safra, fallback para ultima sheet). `_parse_sheet_info()` extrai UF e ano do nome da sheet via regex. Suporte a safra "2023/24" expandindo ano curto para 4 digitos. COE/COT patterns expandidos para "custo variavel", "total das despesas de custeio", "custo operacional (". Link regex `.xlsx` → `.xlsx?` para suportar .xls. PARSER_VERSION 1→2
- **conab/custo_producao/api** — `custo_producao()` e `custo_producao_total()` integrados com `select_data_sheet()` e `_parse_sheet_info()` para resolucao automatica de sheet, UF e safra

### Changed
- **CLI** — stubs `cache status`/`cache clear` removidos (subcommand `cache` eliminado)
- **CLI** — `config show` não exibe mais AlertSettings (módulo privado)
- **wheel** — `agrobr/health`, `agrobr/alerts` e `agrobr/benchmark` excluídos do wheel distribuído (infra interna)

### Fixed
- **comtrade** — ausente do namespace publico `agrobr.__init__` — `from agrobr import comtrade` falhava com ImportError
- **datasets/silvicultura** — exportado como `silvicultura_dataset` (alias desnecessario, padrao dos outros 33 e sem alias). Corrigido para `silvicultura`
- **b3/parser** — `_parse_bvmf_xml` memory cleanup do `iterparse` fazia `del parent[0]` (filhos do parent) quando a intencao era remover siblings anteriores do parent no nivel do avo. XMLs grandes (BVBG-086, 7-8 MB) crasheavam com `IndexError` quando parent ficava vazio. Corrigido para `grandparent.remove(grandparent[0])`
- **conab/custo_producao/client** — regex `\.xlsx` excluia `.xls` (graos: soja, trigo, algodao, cafe). Corrigido para `\.xlsx?`. Novo: folder crawl lazy para culturas em subpaginas (milho, arroz, feijao) — `_extract_folder_urls()` detecta links de pasta, `_crawl_folder()` busca `.xls` dentro delas com `asyncio.gather()` paralelo, strip `/view`. Zero overhead para culturas com link direto. UF regex hardcoded substituido por `UFS_VALIDAS` canonico
- **sync** — `_get_or_create_event_loop()` corrigido: `except RuntimeError` no outer try engolia o `raise RuntimeError` do nest_asyncio missing. Loop running sem nest_asyncio agora levanta RuntimeError corretamente em vez de fallback silencioso para `get_event_loop()`. Test morto (`test_running_loop_without_nest_asyncio_raises`) reescrito e funcional
- **ibge/client** — `sidrapy.get_table()` agora roda via `asyncio.to_thread()`, desbloqueando o event loop em todas as queries IBGE (pam, lspa, ppm, abate, censo, silvicultura, extracao, leite, pib)
- **http/rate_limiter** — `RateLimiter` detecta troca de event loop e recria Lock/Semaphores automaticamente. Antes crashava em chamadas sequenciais com `asyncio.run()`
- **cache/policies** — timezone mismatch corrigido: `_get_smart_expiry_time()` e 3 funcoes de expiry agora usam UTC consistente com `duckdb_store.py`. `CEPEA_UPDATE_HOUR` renomeado pra `CEPEA_UPDATE_HOUR_BRT` (18) / `CEPEA_UPDATE_HOUR_UTC` (21)
- **utcnow migration** — `datetime.utcnow()` (deprecated Python 3.12+) substituido por `utcnow()` helper (naive UTC) em models.py, fingerprint.py, collector.py, duckdb_store.py. 12 sites `fetched_at=datetime.now()` migrados pra UTC em cepea, conab, ibge (api, pesquisas, censo)
- **conab/parsers/v1** — fallback hardcoded `"2025/26"` removido de `_extract_safra_columns()`. Agora levanta `ParseError` com log estruturado quando nao detecta colunas de safra
- Blocos `if TYPE_CHECKING: pass` mortos removidos de 4 modulos datasets (producao_anual, estimativa_safra, preco_diario, balanco)
- **conab/custo_producao** — URL da pagina de custos tinha path duplicado (`/conab/conab/pt-br/...`) resultando em 404. `BASE_URL` ja continha `/conab`, path literal repetia. Corrigido para `{BASE_URL}/pt-br/...`
- **alt/sicar** — `area_ha` e `modulos_fiscais` falhavam contract validation quando WFS GeoServer retornava valores com formato BR (virgula decimal). `pd.to_numeric` nao parseia `"120,5"` → coerce para NaN → `ContractViolationError`. Fix: `.str.replace(",", ".")` antes de `pd.to_numeric` (vectorizado)
- **normalize/encoding** — `detect_encoding_chain` validava encoding apenas contra sample de 4096 bytes. Bytes invalidos alem do sample (ex: 0x81 na posicao 17.6M do CSV PSR) passavam como windows-1252 no sample mas falhavam no decode completo. Fix: valida encoding contra conteudo completo, windows-1252 cai para iso-8859-1 quando necessario
- **comtrade/api** — `comercio()` default `periodo` corrigido de ano corrente (dados incompletos/vazios) para `year - 1` (ultimo ano completo disponivel)
- **comexstat/api** — `exportacao()` default `ano` corrigido de ano corrente para `utcnow().year - 1`. Import migrado de `datetime.now()` local para `utcnow()` UTC
- **datasets/exportacao** — fallback ABIOVE default `ano` corrigido para `utcnow().year - 1`
- **conab/custo_producao** — 2 `datetime.now(UTC)` restantes em `custo_producao_total()` migrados para `utcnow()`. Import `datetime`/`UTC` removido
- **bcb/client** — `fetch_credito_rural_with_fallback()` agora captura `httpx.HTTPStatusError` (403/404) alem de `SourceUnavailableError`, acionando BigQuery fallback. Antes: HTTP 403 de `raise_for_status()` propagava sem tentar fallback

## [0.12.0] - 2026-02-28

### Added
- **IBGE PEVS Silvicultura** — nova funcao `ibge.silvicultura()` para producao silvicultural
  (eucalipto, pinus, carvao vegetal, madeira). Tabela SIDRA 291 (producao, classificacao c194)
  + tabela 5930 (area plantada, classificacao c734). 14 produtos, 3 especies de area,
  variaveis quantidade_produzida/valor_producao/area. Helpers `produtos_silvicultura()` e
  `especies_silvicultura_area()`. Contrato `ibge.silvicultura` v1.0. Dataset `silvicultura`
  com 8 produtos. Schema JSON. Cache 7d/90d stale. ~57 testes + golden data
- **IBGE PEVS Extracao Vegetal** — nova funcao `ibge.extracao_vegetal()` para producao
  extrativista vegetal (acai, castanha-do-para, erva-mate, palmito, etc). Tabela SIDRA 289
  (classificacao c193). 21 produtos, variaveis quantidade_produzida/valor_producao.
  Helper `produtos_extracao_vegetal()`. Contrato `ibge.extracao_vegetal` v1.0. Dataset
  `extrativismo_vegetal` com 12 produtos. Schema JSON. Cache 7d/90d stale. ~35 testes + golden data
- **IBGE Leite Trimestral** — nova funcao `ibge.leite_trimestral()` para aquisicao e
  industrializacao de leite por UF. Tabela SIDRA 1086. 3 variaveis (leite adquirido,
  industrializado, preco medio) pivotadas em colunas wide. Contrato `ibge.leite_trimestral` v1.0.
  Dataset `leite_industrial`. Schema JSON. Cache 7d/90d stale. ~30 testes + golden data
- **IBGE PIB Agro** — nova funcao `ibge.pib_agro()` para PIB agropecuario trimestral
  (Contas Nacionais Trimestrais). Tabelas SIDRA 1846 (precos correntes) e 6612 (precos reais
  1995). 4 setores (agropecuaria, industria, servicos, pib_total). Sem dataset (macro view).
  Cache 7d/90d stale. ~25 testes + golden data
- **Desmatamento PRODES com geometria** — nova funcao `desmatamento.prodes_geo()` retorna
  `GeoDataFrame` com poligonos MultiPolygon (EPSG:4326) do desmatamento consolidado PRODES.
  Requer `pip install agrobr[geo]`. Todos os 6 biomas suportados (incluindo Amazonia).
  Default `maxFeatures=10000` com warning de truncamento. Mesmos parametros de `prodes()`
  (bioma, ano, uf, return_meta). Sync wrapper automatico. Schema `desmatamento_prodes_geo.json`.
  28 novos testes
- **Desmatamento DETER com geometria** — nova funcao `desmatamento.deter_geo()` retorna
  `GeoDataFrame` com poligonos MultiPolygon (EPSG:4326) dos alertas DETER. Requer
  `pip install agrobr[geo]`. Default `maxFeatures=10000` com warning de truncamento.
  Mesmos parametros de `deter()` (bioma, uf, data_inicio, data_fim, classe, return_meta).
  Sync wrapper automatico. 45 novos testes. Schema `desmatamento_deter_geo.json`
- **SICAR Cadastro Ambiental Rural com geometria** — nova funcao `sicar.imoveis_geo()`
  retorna `GeoDataFrame` com poligonos MultiPolygon (EPSG:4326) dos imoveis rurais.
  Requer `pip install agrobr[geo]`. Mesmos parametros de `imoveis()`. Max 5000 features
  com warning de truncamento. Refator: `_normalize_columns` compartilhado entre
  `parse_imoveis_csv` e `parse_imoveis_geojson`. Schema `sicar_imoveis_geo.json`.
  28 novos testes
- **SICAR filtro por codigo IBGE** — novo parametro `cod_municipio` (int) em
  `imoveis()`, `imoveis_geo()` e `resumo()`. Alternativa ao filtro por nome
  (`municipio`) que nao lida com acentos no GeoServer WFS ILIKE. Mutuamente
  exclusivo com `municipio` (ValueError se ambos). `resumo()` refatorado para
  usar `_build_cql_filter` em vez de CQL hardcoded. 9 novos testes
- **Censo Agropecuario Municipal 1985 — Fase 7 completa: core + integracao** (#17) —
  dados municipais do Censo 1985 extraidos via OCR de PDFs do IBGE. 53 CSVs bundled no
  pacote (`agrobr/data/censo_1985/`), modulo `agrobr/ibge/censo_municipal_1985.py` com
  API async (`censo_agro_municipal_1985()`, `temas_censo_agro_municipal_1985()`),
  constantes (53 temas, tab 67-119), loader CSV com melt wide→long, resolucao de
  `localidade_cod` via `municipio_para_ibge()`, unidade por prefixo semantico. Contrato
  `IBGE_CENSO_AGRO_MUNICIPAL_V1` (13 colunas, PK vazio — labels OCR homonimos,
  effective_from 0.12.0). Dataset `censo_agropecuario_municipal_1985` no registry com
  fallback automatico. CLI: `agrobr ibge censo-municipal-1985 <tema>` (--uf, --nivel,
  --formato) e `agrobr ibge temas-municipal-1985`. Schema JSON gerado. 63 testes
  (constantes, index, CSV, unidade, data dir, contrato, validacao, parsing, dataset, CLI).
  22 UFs cobertas (MA/PI/CE/RN excluidas — sem OCR). Docs completos
- **Censo Agropecuario Serie Historica — Bloco 4: CLI e integracao final** (#17) — comandos
  `agrobr ibge censo-historico <tema>` (com `--ano`, `--uf`, `--nivel`, `--formato`) e
  `agrobr ibge temas-historico`. Sync wrapper via `agrobr.sync.ibge` funciona automaticamente
- **Censo Agropecuario Serie Historica — Bloco 3: contrato, dataset e docs** (#17) — contrato
  `IBGE_CENSO_AGRO_HISTORICO_V1` (9 colunas, PK `[ano, tema, categoria, variavel, localidade]`,
  effective_from 0.13.0, fonte='ibge_censo_agro_historico', anos censitarios 1920-2006, nivel max UF).
  Dataset `censo_agropecuario_historico` com 9 temas, update_frequency='never', license='livre'.
  Registrado no registry. 25 novos testes (14 contrato + 11 dataset). Docs: contrato, licenses,
  index, README, CHANGELOG atualizados
- **Censo Agropecuario Serie Historica — Bloco 2: API, parser e testes** (#17) — nova
  funcao `censo_agro_historico()` para serie historica 1920-2006 (ate UF). Parser dedicado
  `_parse_censo_historico_raw()` com deteccao robusta de dimensoes (ano/variavel/categoria),
  unidade por categoria (UNIDADES_CATEGORIAS como fonte primaria, MN so como ultimo
  fallback — corrige Aves=Mil cabecas vs Cabecas). Helper `temas_censo_agro_historico()`.
  3 classes de teste: validacao (9), parsing (22 com mocks), integracao (1). 126 testes
  no arquivo (era 95)
- **Censo Agropecuario Serie Historica — Bloco 1: constantes SIDRA** (#17) — 9 tabelas
  da serie historica (263-283, 1730, 1731) mapeadas com periodos, variaveis,
  classificacoes, categorias, niveis territoriais e unidades. Cobertura: 1920-2006,
  ate Brasil+Regiao+UF. 95 testes cobrindo todas as constantes
- **PAM cacau** — novo produto `cacau` (código SIDRA 40138, "Cacau em amêndoa") em
  `PRODUTOS_PAM` e na whitelist do dataset `producao_anual`
- **MapBiomas cobertura municipal** — novo parametro `nivel="municipio"` em `cobertura()`.
  Chama `fetch_biome_state_municipality()` (~660 MB), parser detecta coluna `municipality`
  automaticamente. Novo parametro `municipio` para filtro por nome (case-insensitive).
  Warning de download pesado via structlog

### Improved
- **Censo Agro Municipal 1985** — melhoria de qualidade dos dados OCR municipais (53 tabelas, 22 UFs).
  Column bleed fix: 525 valores corrigidos (remoção de dígitos vazados de colunas adjacentes).
  Label D→O fix: `\bOE\b→DE`, `\bOO\b→DO` no OCR (e.g. "PLÁCIDO OE CASTRO" → "DE CASTRO").
  Tolerância adaptativa no validador: `absolute_tolerance=5.0` para valores pequenos (<100),
  `sparse_factor=2.0` para tabelas com >60% de células vazias/zero.
  Thresholds de confiança recalibrados: `_load_stats` 70/40→60/30, `generate_confidence_report`
  suspect_rate 0/0.3→0.10/0.40. Revalidação per-parent-group (upgrade-only).
  Distribuição de confiança: alta 5.4%→25.4%, média 62.0%→65.3%, baixa 32.6%→9.3%

### Changed
- **Rate limiter** — `retry_on_status()` agora aplica rate limiting automático em todas as
  fontes. `RateLimiter._get_delay()` busca settings dinamicamente via `getattr(HTTPSettings)`,
  eliminando dict hardcoded. `RateLimiter.acquire()` aceita `str | Fonte`. 32 call sites
  em 21 clients ganham rate limiting sem alteração de código
- **CONAB Custo Produção** — `fetch_custos_page()` agora usa `retry_on_status()` em cada tab
  e fallback, consistente com `download_xlsx()`. Antes era `client.get()` direto sem retry
- **INMET/NASA POWER** — adicionado `follow_redirects=True` ao `httpx.AsyncClient`
- **ComexStat** — docstring documentando SERPRO TLS (cert intermediário Sectigo ausente,
  `verify=False` justificado)
- **CONAB CEASA** — credenciais Pentaho movidas para env vars (`AGROBR_CONAB_CEASA_USER`,
  `AGROBR_CONAB_CEASA_PASS`) com defaults públicos
- **Structlog** — processador `_scrub_sensitive` filtra campos sensíveis (api_key, token,
  password, authorization) e query params sensíveis em URLs nos logs
- **Golden tests** — checksum exclui `parsed_at` (não-determinístico) para estabilidade
- **ANP Diesel** — `format="mixed"` em `pd.to_datetime()` elimina warning `dayfirst` vs ISO
- **Test performance** — test suite reduzido de ~102s para ~43s (58% mais rápido).
  Fixture autouse `_fast_retry` com env vars para retry delays (0.001s) e rate limits (0.001s).
  Benchmark tests excluídos por default (`-m 'not benchmark and not slow'`).
  ANP diesel golden test marcado como `@pytest.mark.slow` (12s de parse xlsx).
  Testes de settings ajustados para verificar consistência em vez de defaults hardcoded
- **Censo Agropecuario 1995/96 — Bloco 1: config SIDRA** (#16) — tabelas, variaveis,
  classificacoes e indices de coluna para 4 temas (efetivo_rebanho, uso_terra,
  lavoura_temporaria, lavoura_permanente) do Censo 1995. Novo dict `_CENSO_MULTI_TABLE`
  para dispatch multi-tabela (logica no Bloco 2). Contrato `min_value` e dataset `min_date`
  atualizados de 2006 para 1995
- **Censo Agropecuario 1995/96 — Bloco 6: API legado + contrato + dataset** (#16) — nova
  funcao `censo_agro_legado()` para 6 temas FTP (tecnologia, pessoal_ocupado, maquinas,
  producao_animal, valor_producao, financeiro). Contrato `IBGE_CENSO_AGRO_LEGADO_V1` com
  ano fixo 1995, 9 colunas, fonte='ibge_censo_agro_legado'. Dataset
  `censo_agropecuario_legado` com update_frequency='never'. Cache TTL 90 dias. Exports em
  `ibge/__init__` e `datasets/__init__`. Testes completos (API, contrato, dataset, cache,
  exports)
- **Censo Agropecuario 1995/96 — Bloco 2: refatoracao + multi-tabela** (#16) — extraido
  `_parse_censo_raw()` e `_empty_censo_df()` de `_fetch_censo_single()`. Novo
  `_fetch_censo_multi_table()` busca N tabelas SIDRA e concatena. `_fetch_censo_single()`
  simplificado para dispatch multi-table vs single-table. Zero regressao (84 testes)
- **Censo Agropecuario 1995/96 — Bloco 3: testes SIDRA 1995** (#16) — 6 mock builders
  para dados 1995, 12 testes novos em `TestCensoAgro1995Mocked` (single-variable, multi-table,
  multi-year, columns, valor, categorias, unidade). Fix: `_parse_censo_raw` com fallback
  `var_map` para SIDRA single-variable (sem dimensao de variavel na resposta). 104 testes
- **Censo Agropecuario 1995/96 — Bloco 5: FTP client + parser** (#16) — novo
  `ftp_client.py` para download de ZIPs legados do FTP IBGE (padrao ANTAQ: retry, timeout
  180s, validacao tamanho, UserAgentRotator). Novo `legacy_parser.py` com parsing de XLS
  (xlrd) para 6 temas FTP (tecnologia, pessoal_ocupado, maquinas, producao_animal,
  valor_producao, financeiro). Deteccao de hierarquia geografica por indentacao
  (totais/mesorregiao/microrregiao/municipio). Config por tema em `_TEMA_COLS`. URL FTP e
  TTL 90 dias em constants.py. 60 testes novos, suite completa 3811 passed

### Fixed
- **Test isolation** — fixture autouse `_reset_global_state` em conftest.py reseta config,
  RateLimiter, HistoryManager e todas as flags `_WARNED` (6 módulos) entre testes.
  Elimina poluição de estado entre testes e garante isolamento correto
- **HistoryManager** — adicionada `reset_history_manager()` para permitir reset do singleton
- **Contracts DATETIME** — `Column.validate()` agora valida colunas DATETIME (antes caiam
  no fallthrough sem type-check). Afeta 2 colunas SICAR (`data_criacao`, `data_atualizacao`)
- **Contracts auto-discovery** — side-effect imports em `datasets/base.py` substituídos por
  `_auto_discover_contracts()` via `pkgutil`. Descobre automaticamente novos módulos de
  contratos sem precisar editar lista manual
- **IBGE LSPA** — contrato `IBGE_LSPA_V1` agora registrado como `lspa` (antes era definido
  mas nunca registrado)
- **Schema orphan** — removido `antaq_movimentacao.json` duplicado (auto-gerado correto é
  `movimentacao_portuaria.json`)
- **PRODES workspace Amazonia** — `PRODES_WORKSPACES["Amazônia"]` apontava para
  `prodes-cerrado-nb` (workspace do Cerrado). Corrigido para `prodes-amazon-nb` com
  layer `yearly_deforestation_biome`
- **PRODES CQL state filter** — filtro por UF enviava apenas nome completo (ex:
  `state='MATO GROSSO'`), mas WFS do TerraBrasilis tem ambos formatos (UF + nome)
  misturados. Novo `_build_state_cql()` gera `(state='MT' OR state='MATO GROSSO')`
- **`_check_geopandas()` mensagem generica** — mensagem de erro hardcoded para
  `deter_geo()` corrigida para mensagem generica que cobre todas as funcoes geo
- **Censo Agro Legado FTP 404 no nivel Brasil** (#16) — `LEGACY_TEMAS` guardava nomes
  com sufixo `Mn` (ex: `Tab_3Mn`), mas o diretorio `Brasil/` no FTP do IBGE so tem
  arquivos sem sufixo (`Tab_3.zip`). Fix: guardar nome base e adicionar `Mn`
  condicionalmente apenas para diretorios de UF
- **Censo Agro 2006 subcategorias perdidas** (#16) — `_parse_censo_raw()` colidia
  quando SIDRA retorna classificacao em D2 e variavel em D3: o `cat_idx=3` sobrescrevia
  a coluna ja reivindicada como `variavel_cod`, mapeando o nome da variavel como
  categoria e ignorando as subcategorias reais. Fix: deteccao de conflito no `cat_idx`
  com fallback para primeira coluna Dx nao reivindicada. Afeta todos os 6 temas 2006
  (preparo_solo, adubacao, calagem, agrotoxicos, praticas_agricolas, irrigacao)
- **PAM/PPM/Censo Agro municipal — fix SIDRA request** — corrige erro "Unidade territorial
  inexistente" ao usar `nivel='municipio'` com filtro de UF. SIDRA espera notacao
  `in N3 {uf_code}` para filtrar municipios por estado, nao o codigo da UF direto.
  Afeta `pam()`, `ppm()` e `censo_agro()`
- **SICAR ContractViolationError** — `data_criacao` nullable=True no contrato (dados reais
  do GeoServer tem nulls legitimos). Dedup por `cod_imovel` mantendo registro com
  `data_atualizacao` mais recente (resolve duplicatas de paginacao WFS)

### Security
- **Snapshots** — proteção contra path traversal em `create_snapshot()`, `load_from_snapshot()`
  e `delete_snapshot()`. Nomes de snapshot validados por regex whitelist + `Path.resolve().is_relative_to()`.
  Previne `shutil.rmtree` em diretórios arbitrários via nomes como `../../..`
- **RateLimiter** — lock `asyncio.Lock()` agora é lazy (criado no primeiro uso, não no import).
  Evita compartilhamento de primitivas asyncio entre event loops diferentes em testes
- **SICAR** — removido `check_hostname=False` e `verify_mode=CERT_NONE` do SSLContext.
  Certificado Sectigo validado (chain completa). `@SECLEVEL=1` mantido para compatibilidade
  de ciphers do GeoServer
- **B3** — removido `verify=False` do client de ajustes. Certificado GTS validado (ECDSA 256)
- **CONAB** — sanitizado URL interpolada em `page.evaluate()` via `json.dumps()` para
  prevenir JS injection no download headless de XLSX
- **B3** — split de flag `_WARNED` em `_WARNED_AJUSTES` e `_WARNED_POSICOES` para que
  cada funcao emita seu proprio warning de licenca independentemente
- **IBGE** — `retriable_exceptions` restrito a exceções de rede (httpx.TimeoutException,
  NetworkError, ConnectionError, TimeoutError). Evita retry infinito em TypeError/KeyError
- **BCB BigQuery** — sanitização de inputs em `_build_query()` via regex whitelist.
  Previne SQL injection em parâmetros `produto`, `safra_ano`, `uf`
- **BCB OData** — escape de aspas simples em `produto_sicor` no filtro OData `contains()`
- **Desmatamento** — validação regex de UF (`^[A-Z]{2}$`) e datas (`^\d{4}-\d{2}-\d{2}$`)
  nos filtros CQL do DETER e PRODES
- **B3** — `except Exception` restrito a `(httpx.HTTPError, SourceUnavailableError, ParseError)`
  em `historico()` e `oi_historico()`. Bugs de programação não são mais engolidos
- **CEPEA** — `except Exception` restrito a exceções de rede/parse em `indicador()` e `ultimo()`.
  Conversão de indicadores restrita a `(KeyError, ValueError, TypeError)`
- **SICAR** — validação regex de `criado_apos` (`^\d{4}-\d{2}-\d{2}$`) no filtro CQL

## [0.11.3] - 2026-02-24

### Added
- **Censo Agropecuario — 6 novos temas de manejo de solo e irrigacao** (#15) — `preparo_solo`,
  `adubacao`, `calagem`, `agrotoxicos`, `praticas_agricolas`, `irrigacao`. Cada tema com dados de
  2006 (tabelas SIDRA 791/1249/1245/1459/837/855) e 2017 (tabelas 6855/6848/6849/6851/8561/6857).
  Total de temas sobe de 4 para 10. Novo parametro `ano` em `censo_agro()` para filtrar por ano
  censal ou buscar ambos (`ano=None` concatena 2006+2017). Tratamento especial para `preparo_solo`
  2017 onde variaveis SIDRA funcionam como categorias (`_VAR_AS_CATEGORIA`). Helper
  `_fetch_censo_single()` extraido para loop multi-ano. Retrocompativel — temas existentes
  continuam funcionando sem `ano`. 86 testes (era 52). Docs: `api/ibge.md`, `sources/ibge.md`,
  `contracts/censo_agropecuario.md` atualizados

## [0.11.2] - 2026-02-22

### Added
- **Cobertura de testes 80% → 84%** — 157 novos testes (3501 → 3658), 462 linhas adicionais cobertas em 15 módulos. Módulos com maior ganho: telemetry/collector (0%→100%), utils/logging (0%→100%), validators/sanity (59%→100%), mapbiomas/client (39%→100%), desmatamento/client (22%→97%), cache/policies (56%→96%), cache/duckdb_store (83%→94%), validators/structural (18%→85%), http/browser (23%→77%), plugins/__init__ (58%→87%), cepea/parsers/consensus (72%→100%), cepea/parsers/detector (92%→100%)

### Fixed
- **CONAB serie_historica**: URL corrigida — `/conab/conab/pt-br/` duplicado removido (BASE_URL ja inclui `/conab`)
- **MapBiomas**: URLs migradas de GCS (`storage.googleapis.com`, 404) para Dataverse (`data.mapbiomas.org/api/access/datafile/`). File IDs: BIOME_STATE=457, BIOME_STATE_MUNICIPALITY=254
- **SICAR**: SSLContext customizado com `@SECLEVEL=1` para contornar TLS handshake failure do `geoserver.car.gov.br` (servidor usa cipher suite legado)
- **ANTT Pedagio**: slugs CKAN atualizados — `fluxo-de-veiculos-nas-pracas-de-pedagio` → `volume-trafego-praca-pedagio`, `cadastro-de-pracas-de-pedagio` → `praca-de-pedagio`. Parser de pracas ajustado para colunas renomeadas (`latitude`/`longitude` → `lat`/`lon`). Parser V2 ajustado para novo layout CSV: `_parse_date_v2` aceita DD/MM/YYYY, `volume_total` como candidate de volume, `tipo_de_veiculo` usado direto quando presente (fallback para `EIXOS_TIPO_MAP`)
- **ANP Diesel**: `vendas_diesel` migrado de XLS pivot table (quebrado) para CSV dados abertos. Fonte: `vendas-oleo-diesel-tipo-m3-2013-2025.csv` — formato long, flat, semicolon-delimited. Removidos helpers `_parse_vendas_wide`/`_parse_vendas_long`/`_is_month_column`

## [0.11.1] - 2026-02-21

### Changed
- **URLs centralizadas em `constants.py`** — 18 clients migrados de URLs hardcoded locais
  (`BASE_URL = "https://..."`) para `URLS[Fonte.XXX]["chave"]` importado de `agrobr.constants`.
  Dominio ou endpoint muda em UM lugar so. Clients afetados: abiove, anda, antaq, bcb, b3,
  comexstat, comtrade, deral, desmatamento, imea, inmet, mapbiomas, nasa_power, queimadas,
  usda, conab/serie_historica, conab/custo_producao, conab/progresso
- **Timeouts centralizados** — 3 clients (anda, inmet, nasa_power) migrados de
  `httpx.Timeout(connect=10.0, read=X, ...)` hardcoded para `HTTPSettings()` com override
  de `read` onde necessario. Todos os 25 clients agora usam `HTTPSettings`
- **Magic numbers substituidos por constantes nomeadas** — 19 thresholds de tamanho minimo
  (`< 50`, `< 100`, `< 500`, `< 1_000`, `< 5_000`) substituidos por `MIN_WFS_SIZE`,
  `MIN_CSV_SIZE`, `MIN_HTML_SIZE`, `MIN_ZIP_SIZE`, `MIN_XLSX_SIZE`, `MIN_HTML_PAGE_SIZE`
  em 16 clients. Ajuste de threshold agora requer edicao em UM lugar so

### Added
- `Fonte.COMTRADE` no StrEnum + URLs (base, auth, guest) + `rate_limit_comtrade` no HTTPSettings
- `URLS[Fonte.B3]["arquivos"]` — endpoint `arquivos.b3.com.br` centralizado
- `URLS[Fonte.DERAL]["downloads"]` — endpoint de downloads centralizado
- 6 constantes de tamanho minimo em `constants.py`: `MIN_WFS_SIZE` (50), `MIN_CSV_SIZE` (100),
  `MIN_HTML_SIZE` (500), `MIN_ZIP_SIZE` (500), `MIN_XLSX_SIZE` (1000), `MIN_HTML_PAGE_SIZE` (5000)

## [0.11.0] - 2026-02-21

### Fixed
- **CEPEA/NA — parser failure em soft block (#14)** — `cepea.indicador("soja")` e
  `datasets.preco_diario("soja")` falhavam com `ParseError` quando Noticias Agricolas
  retornava pagina de consent/challenge (~10KB sem tabela) em vez dos dados (~75KB com
  tabela). Tres mudancas: (1) `FetchResult(html, source)` NamedTuple no client CEPEA
  identifica explicitamente a fonte do HTML ("cepea", "browser", "noticias_agricolas"),
  eliminando deteccao fragil por markers no conteudo (`"noticiasagricolas" in html`);
  (2) `_validate_html_has_data()` no client NA rejeita respostas < 20KB sem `<table`
  (soft block) com `SourceUnavailableError`, ativando cache fallback; (3) roteamento
  no `api.py` usa `FetchResult.source` em vez de inspecionar HTML
- **ANP Diesel — normalizar produto** — `"OLEO DIESEL"` / `"ÓLEO DIESEL S10"` agora
  normalizado para `"DIESEL"` / `"DIESEL S10"` no output. Afeta `parse_precos`,
  `_parse_vendas_wide`, `_parse_vendas_long`. Regex `^[OÓ]LEO\s+` strip no produto.
  Filtro `produto=` tambem normaliza antes de comparar. Schema guarantee atualizada.
- **ANP Diesel — normalizar UF** — Coluna `ESTADO` com nome completo (ex: `"MATO GROSSO"`)
  agora convertida para sigla via `normalizar_uf()`. Fallback `or v.upper()` corrigido
  para `or ""` (antes retornava nome completo se normalizar_uf falhasse). Fix aplicado
  em `parse_precos`, `_parse_vendas_wide`, `_parse_vendas_long`.
- **CONAB Serie Historica — engine Excel** — `parse_serie_historica()` agora detecta
  formato via magic bytes (OLE2 BIFF = xlrd, senao openpyxl). Antes: `pd.ExcelFile()`
  sem engine falhava com `ValueError` para arquivos `.xls` reais da CONAB. Commit
  `38f5112` adicionou xlrd como dep mas nao corrigiu este parser.
- **Queimadas — fallback historico** — `fetch_focos_mensal()` agora tenta em cascata:
  `.csv` mensal (2024+) → `.zip` mensal (2023) → `.zip` anual (2003-2022). Antes:
  HTTP 404 para qualquer mes <2024 sem fallback. INPE migrou CSVs historicos para
  formato ZIP e dados pre-2023 so disponiveis como ZIP anual. Filtro por mes aplicado
  na api quando fonte e anual.

### Added
- **SICAR — Cadastro Ambiental Rural** — Novo namespace `agrobr/alt/sicar/` para dados do
  Cadastro Ambiental Rural (CAR) via GeoServer WFS (OGC 2.0.0, CSV sem geometria, sem auth).
  Funcoes: `sicar.imoveis(uf, municipio, status, tipo, area_min, area_max, criado_apos)` para
  registros individuais de imoveis rurais e `sicar.resumo(uf, municipio)` para estatisticas
  agregadas (total, por status, area, modulos fiscais, por tipo). Pagination transparente
  (resultType=hits + startIndex/count=10000), progressive delay apos pagina 5, timeout 180s.
  CQL_FILTER server-side para municipio (ILIKE), status, tipo, area range, data criacao.
  Contrato `SICAR_IMOVEIS_V1` (11 colunas, PK cod_imovel). Schema JSON, golden data (DF + MT).
  114 novos testes (models, client, parser, api). Sync wrapper via `agrobr.sync.alt.sicar`.
  Docs: `api/sicar.md`, `sources/sicar.md`
- **Dataset semantico `cadastro_rural`** — `datasets.cadastro_rural(uf, municipio, status, tipo,
  area_min, area_max, criado_apos)` na camada semantica. Wraps `sicar.imoveis()` com validacao
  de contrato, return_meta, modo deterministico e fallback pattern. Registrado no registry com
  contrato `SICAR_IMOVEIS_V1`. 10 novos testes.
- **ANTT Pedagio — Fluxo de Veiculos em Pracas de Pedagio** — Novo namespace `agrobr/alt/antt_pedagio/`
  para dados de fluxo de veiculos em pracas de pedagio rodoviario (ANTT Dados Abertos, CC-BY).
  Funcoes: `antt_pedagio.fluxo_pedagio(ano, ano_inicio, ano_fim, uf, apenas_pesados)` para
  trafego mensal com filtros por UF/concessionaria/rodovia/tipo de veiculo e
  `antt_pedagio.pracas_pedagio(uf, rodovia)` para cadastro georreferenciado (200+ pracas).
  CSV bulk 2010-2025 (16 arquivos), schema V1 (2010-2023) com categorias texto e V2 (2024+)
  com eixos numerico, encoding Windows-1252 com fallback automatico, join com cadastro de
  pracas para enriquecimento geografico (rodovia, UF, municipio).
  Contratos `ANTT_PEDAGIO_FLUXO_V1` (10 colunas) e `ANTT_PEDAGIO_PRACAS_V1` (9 colunas).
  Schemas JSON, golden data. 117 novos testes (models, client, parser, api). Sync wrapper
  via `agrobr.sync.alt.antt_pedagio`. Docs: `api/antt_pedagio.md`, `sources/antt_pedagio.md`
- **MAPA PSR — Seguro Rural** — Novo namespace `agrobr/alt/mapa_psr/` para dados de
  apolices e sinistros do seguro rural brasileiro (SISSER/MAPA, CC-BY). Funcoes:
  `mapa_psr.sinistros(cultura, uf, ano, evento)` para indenizacoes pagas e
  `mapa_psr.apolices(cultura, uf, ano)` para todas as apolices com subvencao federal.
  CSV bulk 2006+ (3 periodos), encoding auto-detect, PII removido automaticamente.
  Contratos `MAPA_PSR_SINISTROS_V1` (17 colunas) e `MAPA_PSR_APOLICES_V1` (18 colunas).
  Schemas JSON, golden data. 104 novos testes (models, client, parser, api). Sync wrapper
  via `agrobr.sync.alt.mapa_psr`. Docs: `api/mapa_psr.md`, `sources/mapa_psr.md`
- **ANP Diesel — Precos + Volumes** — Novo namespace `agrobr/alt/anp_diesel/` para dados de
  precos de revenda e volumes de venda de diesel da ANP. Funcoes:
  `anp_diesel.precos_diesel(uf, municipio, produto, nivel, agregacao)` para precos
  semanais/mensais por municipio/UF/Brasil e `anp_diesel.vendas_diesel(uf)` para volumes
  mensais por UF. XLSX bulk 2013+ (openpyxl), cache por periodo do arquivo.
  Contratos `ANP_DIESEL_PRECOS_V1` (8 colunas) e `ANP_DIESEL_VENDAS_V1` (5 colunas).
  Schemas JSON, golden data. 103 novos testes (models, client, parser, api). Sync wrapper
  via `agrobr.sync.alt.anp_diesel`. Docs: `api/anp_diesel.md`, `sources/anp_diesel.md`
- **UN Comtrade — Trade Mirror** — Novo modulo `comtrade/` para dados de comercio
  internacional bilateral via UN Comtrade API. Funcoes: `comtrade.comercio()` (dados
  bilaterais por HS code/pais/periodo) e `comtrade.trade_mirror()` (compara exportacoes
  do reporter vs importacoes do parceiro, calcula discrepancias peso/valor/ratio).
  Guest mode (sem API key) + `AGROBR_COMTRADE_API_KEY` para rate limit maior.
  Chunking automatico para periodos > 12 meses. 17 produtos agro mapeados por HS code.
  Contratos `COMERCIO_BILATERAL_V1` e `TRADE_MIRROR_V1`. Golden data (comercio + mirror).
  70 novos testes. Sync wrapper. Docs: `api/comtrade.md`, `sources/comtrade.md`
- **ANTAQ — Movimentacao Portuaria** — Novo modulo `antaq/` para dados de movimentacao
  portuaria de carga do Estatistico Aquaviario (ANTAQ). Funcao `antaq.movimentacao(ano)`
  baixa ZIP bulk anual (~80MB), extrai e faz join de 3 tabelas (Atracacao + Carga + Mercadoria).
  Filtros: tipo_navegacao, natureza_carga, mercadoria, porto, uf, sentido.
  Encoding UTF-8-sig, separador `;`, decimal brasileiro (`,`). Historico desde 2010.
  Contrato `MOVIMENTACAO_PORTUARIA_V1` com 21 colunas. Schema JSON, golden data.
  72 novos testes (client, parser, models, api). Sync wrapper. Docs: `sources/antaq.md`
- **B3 Posicoes em Aberto (Open Interest)** — Novas funcoes `b3.posicoes_abertas()` e
  `b3.oi_historico()` para dados de open interest diario de futuros e opcoes agro
  (BGI, CCM, ETH, ICF, SJC). CSV publico via `arquivos.b3.com.br` (2-step: token + download).
  Parser filtra segmento AGRIBUSINESS, classifica futuro/opcao, enriquece com descricao e unidade.
  Contrato `POSICOES_ABERTAS_V1` com PK `[data, ticker_completo]`, 11 colunas. Schema JSON,
  golden data (518 linhas agro, 2025-12-19), 61 novos testes. Docs: `api/b3.md`, `sources/b3.md`
  atualizados
- **BCB/SICOR dimensoes ocultas** — Expoe 5 dimensoes que a API retorna mas eram ignoradas:
  programa, fonte de recurso, tipo de seguro, modalidade e atividade. Cada dimensao gera
  duas colunas: codigo (`cd_programa`) e nome legivel (`programa`). Dicionarios hardcoded
  com fallback `"Desconhecido ({code})"` + log warning para codigos novos. Enriquecimento
  no parser (PARSER_VERSION=2). Novos parametros `programa` e `tipo_seguro` para filtro
  client-side. Nova agregacao `agregacao="programa"`. Contract v1.1 com 11 novas colunas
  nullable (nao quebra consumidores v1.0). Schema JSON regenerado. 87 novos testes
  (models, parser, api). Suite: 2778 passed, 0 failed

### Changed
- `credito_rural` contract bump v1.0 → v1.1 (minor — novas colunas nullable)
- `PARSER_VERSION` bump 1 → 2 (novas colunas + enriquecimento)
- Golden data `custeio_sample/expected.json` atualizado com 20 colunas (era 15)

## [0.10.1] - 2026-02-16

### Fixed
- **DuckDB thread-safety** — `DuckDBStore` agora usa `threading.Lock` em todos os
  métodos que acessam a conexão. `get_store()` usa double-checked locking. Corrige
  segfault/deadlock quando múltiplas threads compartilham o singleton (ex: MCP server
  despachando requests para threads diferentes)
- **Parser NA semanal** — `_parse_date` aceita formato semanal `'09 - 13/02/2026'`
  (média CEPEA semanal). Registros semanais marcados com `anomalies=["media_semanal"]`
  e `meta["tipo"]="media_semanal"`. Antes: linhas ignoradas com warning
  `parse_row_failed`
- **ANDA ano errado** — `fetch_entregas_pdf` agora retorna `tuple[bytes, int]` com
  `ano_real` extraído do texto do link (não da URL de upload que contém o ano do
  upload, não dos dados). Corrige parser buscando header "2026" em PDF de dados 2025

### Changed
- `integration_tests.yml` — timeout global adicionado
- `pyproject.toml` — `pytest-timeout` adicionado como dependência de teste
- 3 testes de thread-safety no DuckDB store (`test_threaded_reads`,
  `test_threaded_writes`, `test_threaded_indicadores` — 5 threads × 10-20 ops cada)
- Suite: 2719 passed, 0 failed (era 2660+)

## [0.10.0] - 2026-02-15

### Added
- **CONAB CEASA/PROHORT (Precos de Atacado Hortifruti)** — Nova fonte: precos diarios de atacado
  de 48 produtos (20 frutas, 28 hortalicas) em 43 CEASAs do Brasil. Modulo `agrobr/conab/ceasa/`
  com client (Pentaho CDA REST API, JSON), parser (pivot 48x43 -> long-form, datas por header,
  mapeamento posicional CEASAs), models (48 produtos, 43 CEASAs, UF map, categorias).
  API publica `conab.ceasa_precos()` com filtros por produto/ceasa, `conab.ceasa_produtos()`,
  `conab.lista_ceasas()`, `conab.ceasa_categorias()`. Contrato `PRECO_ATACADO_V1` com
  PK `[data, produto, ceasa]`. Schema JSON, golden data (Pentaho real), 70 testes. Warning
  zona_cinza na primeira chamada. Docs: `sources/conab.md`, licenses atualizado
- **B3 Futuros Agro** — Nova fonte: ajustes diarios de futuros agricolas (boi gordo, milho,
  cafe arabica, cafe conillon, etanol, soja cross, soja FOB). Modulo `agrobr/b3/` com client
  (HTML parse de `www2.bmf.com.br`, encoding iso-8859-1), parser (tabela `tblDadosAjustes`,
  carry-forward de ticker, numeros BR), models (7 contratos, month codes, unidades).
  API publica `b3.ajustes()` com filtro por contrato, `b3.historico()` para serie temporal,
  `b3.contratos()`. Contrato `AJUSTE_DIARIO_V1` com PK `[data, ticker, vencimento_codigo]`.
  Schema JSON, golden data (dia util + weekend), 71 testes. Warning zona_cinza na primeira
  chamada. Sync wrapper. Docs: `api/b3.md`, `sources/b3.md`, licenses atualizado
- **IBGE Censo Agropecuário (Censo Agro 2017)** — Nova pesquisa no módulo IBGE:
  4 temas (efetivo_rebanho, uso_terra, lavoura_temporaria, lavoura_permanente) via tabelas
  SIDRA 6907/6881/6957/6956. API pública `ibge.censo_agro()` com filtros por tema/UF/nível
  e `ibge.temas_censo_agro()`. Dataset `censo_agropecuario` com contrato `ibge.censo_agro v1.0`,
  schema JSON, golden data. Long format (variável/valor por linha). Cache 30 dias.
  52 testes. Docs: `api/ibge.md`, `sources/ibge.md`, `contracts/censo_agropecuario.md`, licenses
  atualizado
- **IBGE Abate Trimestral**: abate bovino, suíno e frango por UF desde 1997 — 54 testes, contrato, golden data
- **IBGE PPM — Pesquisa da Pecuária Municipal (roadmap 2.8)** — Nova pesquisa no módulo IBGE:
  efetivo de rebanhos (10 espécies, tabela SIDRA 3939) e produção de origem animal (6 produtos,
  tabela 74). API pública `ibge.ppm()` com filtros por espécie/ano/UF/nível e `ibge.especies_ppm()`.
  Dataset `pecuaria_municipal` com contrato `IBGE_PPM_V1`, schema JSON, golden data. Cache 7 dias.
  60 testes. Docs: `api/ibge.md`, `sources/ibge.md`, `contracts/pecuaria_municipal.md`, licenses
  atualizado
- **CONAB Progresso de Safra (roadmap 2.0.5)** — Nova fonte: progresso semanal de plantio
  e colheita por cultura x UF. Modulo `agrobr/conab/progresso/` com client (Plone CMS
  pagination, XLSX download via sub-links), parser (block-based state machine para XLSX
  com blocos repetidos por cultura/operacao), models (6 culturas, 27 UFs, normalizacao
  estado→UF). API publica `conab.progresso_safra()` com filtros por cultura/estado/operacao
  e `conab.semanas_disponiveis()` para listar semanas. Contrato `CONAB_PROGRESSO_V1` com
  PK `[cultura, safra, operacao, estado, semana_atual]`. Golden data, 67 testes. Docs:
  `api/conab_progresso.md`, `sources/conab_progresso.md`, licenses atualizado
- **MapBiomas (roadmap 2.7)** — Nova fonte: cobertura e uso da terra por municipio/ano
  (1985-presente). Modulo `agrobr/mapbiomas/` com client (download XLSX do Google Cloud
  Storage), parser (multi-sheet com classes de cobertura MapBiomas Collection 9), models
  (classes de cobertura, biomas, transicoes). API publica `mapbiomas.cobertura()` com
  filtros por municipio/UF/bioma/classe/ano e `mapbiomas.transicao()`. Contrato
  `MAPBIOMAS_COBERTURA_V1`. Golden data, 66 testes. Docs: `api/mapbiomas.md`,
  `sources/mapbiomas.md`, licenses atualizado
- **Desmatamento PRODES/DETER (roadmap 2.2)** — Nova fonte: dados de desmatamento via
  TerraBrasilis GeoServer (WFS). Modulo `agrobr/desmatamento/` com client (WFS+CSV, CQL_FILTER
  por UF/ano/data), parser (PRODES anual + DETER alertas), models (workspaces por bioma,
  classes DETER, mapeamento UF/estado). API publica `desmatamento.prodes()` (5 biomas: Cerrado,
  Caatinga, Mata Atlantica, Pantanal, Pampa) e `desmatamento.deter()` (Amazonia, Cerrado)
  com filtros por bioma/UF/ano/classe e suporte a `return_meta`. Contratos
  `DESMATAMENTO_PRODES_V1` e `DESMATAMENTO_DETER_V1`. Schemas JSON, golden data (PRODES 10
  registros x 9 UFs, DETER 10 registros x 5 UFs x 4 classes), 56 testes. Export em
  `__init__.py` e sync wrapper. Docs: `api/desmatamento.md`, `sources/desmatamento.md`,
  licenses atualizado
- **Queimadas/INPE (roadmap 2.1)** — Nova fonte: focos de calor detectados por satelite via
  BDQueimadas/INPE. Modulo `agrobr/queimadas/` com client (CSV diario/mensal), parser
  (UTF-8 + latin-1 fallback), models (6 biomas, 27 UFs, 13 satelites), API publica
  `queimadas.focos()` com filtros por UF/bioma/satelite e suporte a `return_meta`.
  Contrato `FOCOS_QUEIMADAS_V1` com PK `[data, lat, lon, satelite, hora_gmt]`.
  Schema JSON, golden data (8 registros x 6 biomas), 43 testes. Export em `__init__.py`
  e sync wrapper. Docs: `api/queimadas.md`, `sources/queimadas.md`, licenses atualizado
- **Schemas JSON formais (roadmap 1.2)** — Contratos Python agora geram schemas JSON em
  `agrobr/schemas/`. 8 contratos com primary_key, min/max constraints, validação automática
  via `_validate_contract()` em todos os 8 datasets. Novos contratos: `credito_rural`,
  `exportacao`, `fertilizante`, `custo_producao`. Registry centralizado com
  `register_contract()` / `get_contract()` / `validate_dataset()`. 60 testes dedicados.
  `Contract.to_json()` / `from_json()` para serialização roundtrip
- **Normalização transversal (roadmap 1.3)** — Dois novos módulos em `agrobr/normalize/`:
  - `municipalities.py` — Mapeamento nome→código IBGE para 5571 municípios brasileiros.
    Busca accent/case insensitive. `municipio_para_ibge()`, `ibge_para_municipio()`,
    `buscar_municipios()`. Dados da API IBGE Localidades (livre para uso)
  - `crops.py` — Dicionário unificado de 140+ variantes→35 culturas canônicas.
    `normalizar_cultura()` resolve "SOJA", "soja em grão", "soybean" → "soja".
    `listar_culturas()`, `is_cultura_valida()`. Substitui aliases dispersos
  - 464 testes novos (100 municípios x 3 variações + culturas). Total suite: 2128 testes
- **Politica de versionamento datasets (roadmap 1.4)** — `docs/contracts/semver.md` expandido
  com tabela detalhada de bump rules (major/minor/patch), principio de `schema_version`
  independente de `lib_version`, criterios de breaking change para datasets
- **Metadados no registry (roadmap 1.9)** — `DatasetInfo` expandido com `source_url`,
  `source_institution`, `min_date`, `unit`, `license`. 8 datasets preenchidos com metadados
  reais (instituição, URL, licença, data mínima, unidade). Registry ganha
  `describe(name)` e `describe_all()` para exibição formatada
- **Testes de integracao formalizados (roadmap 1.8)** — Distinção clara entre unit/golden
  (todo push), integration (cron semanal) e benchmark (manual). Markers `@pytest.mark.integration`
  e `@pytest.mark.benchmark` registrados em `pyproject.toml`. CI padrao exclui ambos:
  `pytest -m "not integration and not benchmark"`
- **CI health check semanal (roadmap 1.5)** — `.github/workflows/integration_tests.yml`:
  cron segunda 08:00 UTC, `pytest -m integration --tb=short --timeout=120`, issue automatica
  com label `source-changed` em caso de falha, alertas Discord/Slack, artefato de resultados
  (30 dias). Nao bloqueia release — apenas alerta
- **Cobertura CLI/alerts/health** — 107 testes novos: `test_cli.py` (51), `test_alerts/test_notifier.py` (17),
  `test_health/test_checker.py` (15), `test_health/test_reporter.py` (24). Total suite: 1640 testes. Closes #11
- **Golden tests com dados reais** para 5 fontes: BCB, IBGE, ComexStat, DERAL, ABIOVE
  (substituindo dados sintéticos). Script `scripts/update_golden.py` expandido com
  captura automatizada para 6 fontes (5 novas + CEPEA existente). Closes #10
- **Audit de licenças** — `docs/licenses.md` com tabela completa das 13 fontes,
  classificação (`livre`, `nc`, `zona_cinza`, `restrito`) e URLs dos termos
- **Aviso CC BY-NC 4.0** no módulo CEPEA (docstrings em `__init__.py` e `api.py`)
  e na documentação (`docs/sources/cepea.md`)
- **Avisos de licença** nos módulos IMEA (`restrito`), Notícias Agrícolas
  (`restrito`, deprecação pendente), ANDA e ABIOVE (`zona_cinza`, autorização
  solicitada fev/2026)
- **Runtime warnings** — `warnings.warn()` no primeiro uso de IMEA e Notícias
  Agrícolas alertando sobre restrições de redistribuição
- **Warning box no README** apontando para `docs/licenses.md`
- **Cache key versionada** — `build_cache_key()` em `agrobr/cache/keys.py`:
  formato `{dataset}|{params_hash}|v{lib_version}|sv{schema_version}`,
  garante invalidação automática entre versões da lib e mudanças de schema
- **Cache versionado completo** — migração automática de keys legacy para formato
  versionado (`legacy_cache_migrated`), strict mode via `AGROBR_CACHE_STRICT=1`
  (rejeita cache de versão divergente), `parse_cache_key()` / `is_legacy_key()`
  / `legacy_key_prefix()` em `cache/keys.py`. 16 testes novos (migração, strict,
  concorrência 3 threads)
- **HTTP settings centralizados** — `agrobr/http/settings.py` com `get_timeout()`,
  `get_rate_limit()`, `get_client_kwargs()`. `rate_limit_default` (1 req/s) no
  `HTTPSettings`. Env vars `AGROBR_HTTP_TIMEOUT_*` e `AGROBR_HTTP_RATE_LIMIT_*`
  configuram tudo. 14 testes novos

### Fixed
- **Pydantic `class Config` → `model_config`** — 4 Settings classes em `constants.py`
  migradas de `class Config` (deprecated) para `model_config = SettingsConfigDict(...)`.
  Elimina 4 `PydanticDeprecatedSince20` warnings em toda importação do agrobr
- **MapBiomas sync wrapper** — `_SyncMapBiomas` adicionado ao `sync.py`. Antes:
  `from agrobr.sync import mapbiomas` lançava `ImportError`
- **Warnings zona_cinza ANDA/ABIOVE** — `_WARNED` + `warnings.warn()` adicionados
  em `anda/api.py` e `abiove/api.py` (padrão já existente em B3, CEASA, IMEA, NA)
- **Schemas JSON desatualizados** — `generate_json_schemas()` regenerou 19 schemas
  (3 novos: mapbiomas_cobertura, mapbiomas_transicao, conab_progresso; 16 atualizados)
- **Pre-commit limpo** — SIM117 (nested `with` combinados), mypy `untyped-decorator`
  no cli.py, erros pré-existentes em `scripts/` e `examples/` corrigidos (27 erros mypy)
- **Parser ABIOVE** — suporte a formato single-sheet multi-seção (meses na coluna 1,
  seções por produto: grão, farelo, óleo, milho, total). Layout novo de 2024/2025.
- **Parser DERAL** — suporte a formato multi-produto por sheet (sheets nomeadas por
  data: "Atual", "Anterior", "10-02-2025"). Layout atual do PC.xls com tabela
  Condição/Fase por cultura em cada sheet.
- **7 clients legados migrados para `retry_on_status()`** — deral, imea, usda,
  abiove, bcb, comexstat, anda. ~445 linhas de retry duplicado removidas.
  Timeout/ConnectError propagam imediatamente (sem retry).
- **`indicadores_upsert` 7x mais rápido** — temp table + INSERT SELECT
  substitui INSERT row-by-row. 10k: 34s→4.8s, 50k: 187s→25.9s.
  Scaling agora linear (ratio 50k/10k ≈ 5.4x).

### Changed
- Retry loops dos 7 clients restantes migrados para `http/retry.py` centralizado
  (todos 13 clients agora usam `retry_on_status()`)
- `indicadores_upsert` usa chunks de 5000 via temp table `_ind_staging`
  com fallback row-by-row para isolamento de erros

## [0.9.0] - 2026-02-11

### Added
- **1529 testes** (era 949), cobertura **~75%** (era 57.5%) — atualizado para 1640 no Unreleased
- **Golden tests** para todas as 13 fontes de dados (era 2/13)
- **Benchmark de escalabilidade** — memory, volume, cache, async, rate limiting, sync, golden
- **Suporte a token INMET** — `AGROBR_INMET_TOKEN` via env var
- `retry_on_status()` e `retry_async()` centralizados em `http/retry.py`
- **Retry-After header** respeitado em respostas HTTP 429
- **Testes de resiliência HTTP** para todos os 13 clients (timeout, 429, 500, 403, resposta vazia)
- **Testes de API pública**: `cepea.indicador()`/`ultimo()`, `conab.safras()`/`balanco()`/`brasil_total()`/`levantamentos()`
- Pre-commit hooks atualizados (ruff v0.15, mypy v1.19)

### Fixed
- **Cache DuckDB** — `history_entries.id` sem autoincrement: histórico permanente nunca salvava dados
- **normalize/dates** — `normalizar_safra()` não fazia strip no input
- **6 clients sem retry para HTTP 429**: inmet, nasa_power, conab_custo, conab_serie, conab main, ibge
- **Graceful degradation silenciosa** trocada por `SourceUnavailableError` quando retry esgota
- **except Exception genérico** em `duckdb_store.py` restringido para exceções específicas
- **INMET** — endpoint `/estacao/dados/` atualizado para `/estacao/` (API mudou)
- **INMET** — tratamento de HTTP 204 (No Content) retorna DataFrame vazio

### Changed
- Retry loops de 5 clients migrados para `http/retry.py` centralizado
- Testes de datasets refatorados: 98 funções duplicadas → 27 parametrizadas (115 cenários)
- mypy override para `tests.*` (`ignore_errors = true`, strict mantido no core)

### Known Issues
- 4 golden tests com dados sintéticos (INMET, USDA, NA, ANDA) — `needs_real_data`
  (BCB, IBGE, ComexStat, DERAL, ABIOVE migrados para dados reais na issue #10)
- ~~DuckDB 1.4.4 incompatível com coverage no Python 3.14~~ (resolvido: bump 1.5.0 + workaround conftest.py)

## [0.8.0] - 2026-02-09

### Added
- **ABIOVE** (`agrobr.abiove`) — Exportação do complexo soja
  - `abiove.exportacao()` — Volume e receita mensal de grão, farelo, óleo e milho
  - Parser Excel com detecção dinâmica de header
- **USDA PSD** (`agrobr.usda`) — Estimativas internacionais de oferta/demanda
  - `usda.psd()` — Dados PSD por commodity/país/ano via API FAS OpenData v2
  - Suporte a pivot, filtro por atributos, mapeamento PT-BR
  - Requer API key gratuita (api.data.gov)
- **IMEA** (`agrobr.imea`) — Cotações e indicadores Mato Grosso
  - `imea.cotacoes()` — Preços, progresso de safra, comercialização (6 cadeias)
  - API REST pública (api1.imea.com.br), sem autenticação
- **DERAL** (`agrobr.deral`) — Condição das lavouras Paraná
  - `deral.condicao_lavouras()` — Condição semanal (boa/média/ruim) + progresso plantio/colheita
  - Parser Excel (PC.xls) com detecção dinâmica de abas e produtos
- **CONAB série histórica** (`agrobr.conab.serie_historica`) — Sub-módulo de safras 2010+
  - `conab.serie_historica()` — Série histórica de safras por UF com filtros
  - Parser Excel com detecção dinâmica de header row
- **BCB BigQuery fallback** — `pip install agrobr[bigquery]`
  - Base dos Dados como fallback quando API OData retorna 500
  - `asyncio.to_thread()` para wrapping do SDK síncrono
- **5 novos datasets semânticos** (camada semântica):
  - `datasets.credito_rural()` — BCB/SICOR com fallback BigQuery
  - `datasets.exportacao()` — ComexStat → ABIOVE (fallback automático)
  - `datasets.fertilizante()` — ANDA (entregas por UF)
  - `datasets.custo_producao()` — CONAB custos de produção
  - Total: 8 datasets (era 4)
- 949 testes passando (era ~804)

### Fixed
- **BCB/SICOR** — Endpoints atualizados para API reestruturada (~2024)
  - `CusteioMunicipio` → `CusteioRegiaoUFProduto`
  - `InvestimentoMunicipio` → `InvestRegiaoUFProduto`
  - `ComercializacaoMunicipio` → `ComercRegiaoUFProduto`
  - `industrializacao` removida (sem endpoint equivalente)
- **BCB parser** — `COLUNAS_MAP` expandido para colunas da API nova (`VlCusteio`→`valor`, `nomeUF`→`uf`, `AreaCusteio`→`area_financiada`, etc.)
- **BCB parser** — Limpeza de aspas embarcadas em `nomeProduto` (`"\"SOJA\""` → `soja`)

### Changed
- **BCB client** — Server-side filter via `contains()` (unico operador suportado pelo Olinda v2); filtragem por ano/UF client-side
- **BCB client** — `MAX_RETRIES` 4→6, `timeout.read` 60→120s, `User-Agent` header adicionado
- **13 fontes** integradas (era 8): +ABIOVE, +USDA PSD, +IMEA, +DERAL, +Notícias Agrícolas
- `agrobr/constants.py` — Fonte enum +4, URLS +4, CacheSettings +4 TTLs, HTTPSettings +4 rate limits
- `agrobr/sync.py` — 4 novas classes _SyncModule (abiove, deral, imea, usda)
- `agrobr/http/rate_limiter.py` — 4 novas entradas no delays dict

## [0.7.1] - 2026-02-07

### Added
- **NASA POWER** (`agrobr.nasa_power`) — Dados climaticos globais como substituto do INMET
  - `nasa_power.clima_ponto()` — Dados diarios/mensais por coordenada (lat/lon)
  - `nasa_power.clima_uf()` — Dados climaticos por UF (ponto central)
  - 7 parametros agroclimaticos: temp (media/max/min), precipitacao, umidade, radiacao, vento
  - API REST pura (NASA LaRC), sem autenticacao, cobertura global desde 1981
  - Chunking automatico para periodos > 365 dias
  - 34 testes unitarios (models, parser, api)
- **NASAPowerCollector** no agrobr-collector (substitui INMETCollector)
- **Alertas automaticos** — health_check.yml e structure_monitor.yml enviam alertas Discord/Slack quando fontes degradam
- **Health checks reais** — CONAB (HTTP HEAD), IBGE (SIDRA API query com validacao de dados)
- **NASA POWER cache policy** dedicada (TTL 7d, stale 30d) em `policies.py`
- **Notebook demo** (`examples/agrobr_demo.ipynb`) — 14 secoes cobrindo todas as fontes, MetaInfo, fallback, cache, pipeline com graficos e modo async
- **Landing page** atualizada — text-shadow para legibilidade, copyright 2026, icone monocromatico, botao Colab no CTA

### Changed
- INMET desabilitado no collector (config.yaml `enabled: false`) — API dados retornando 404
- Docs atualizados: INMET referencia NASA POWER como alternativa
- `docs/index.md` atualizado com 8 fontes (era 3), NASA POWER no uso rapido
- `alert_on_anomaly` habilitado por padrao em `constants.py`
- `CacheSettings.ttl_nasa_power` corrigido de 24h para 7d (consistente com `policies.py`)
- `SOURCE_POLICY_MAP` corrigido: NASA_POWER aponta para `"nasa_power"` (era `"bcb"`)

### Fixed
- **sync.py** — `_SyncNasaPower` adicionado (nasa_power nao funcionava no modo sincrono)
- **Notebook cell 17** — PAM defensivo: detecta `"producao"` ou `"Quantidade produzida"` (SIDRA rename)
- **README** — Colab badge corrigido de `demo_colab.ipynb` para `agrobr_demo.ipynb`
- **cepea/client.py** — Variavel nao usada `produto_key` removida (ruff lint)

## [0.7.0] - 2026-02-07

### Added
- **INMET** (`agrobr.inmet`) — Dados meteorologicos de 600+ estacoes automaticas
  - `inmet.estacoes()` — Listar estacoes por tipo e UF
  - `inmet.estacao()` — Dados horarios/diarios de uma estacao
  - `inmet.clima_uf()` — Clima mensal agregado por UF
- **BCB/SICOR** (`agrobr.bcb`) — Credito rural por municipio e cultura
  - `bcb.credito_rural()` — Dados de credito de custeio por safra
- **ComexStat** (`agrobr.comexstat`) — Exportacoes brasileiras por NCM
  - `comexstat.exportacao()` — Exportacoes mensais com 19 produtos mapeados
  - Filtro por NCM usa prefix match (subposicoes capturadas automaticamente)
- **ANDA** (`agrobr.anda`) — Entregas de fertilizantes por UF/mes
  - `anda.entregas()` — Dados de entregas de fertilizantes
  - Parser suporta multiplas orientacoes de tabela PDF + layout "Principais Indicadores"
  - Requer `pip install agrobr[pdf]` (pdfplumber)
- **CONAB custo_producao** (`agrobr.conab.custo_producao`) — Custos de producao por hectare
  - `conab.custo_producao()` — Dados detalhados de custo por cultura/UF/safra
  - `conab.custo_producao_total()` — Totais COE/COT/CT

### Fixed
- **ComexStat**: NCM algodao corrigido de `52010000` (inexistente) para prefixo `520100` (captura `52010020` + `52010090`)
- **ComexStat**: `verify=False` no httpx para contornar certificado SSL invalido do `balanca.economia.gov.br`
- **ComexStat**: Filtro NCM no parser mudou de match exato (`==`) para prefix match (`str.startswith()`)
- **ANDA**: URL atualizada de `/estatisticas/` para `/recursos/` (reorganizacao do site)
- **ANDA**: Parser expandido com `_expand_newline_cells()` e `_parse_indicadores()` para PDFs com meses/valores concatenados
- **INMET**: User-Agent header adicionado ao client HTTP (previne 403 Forbidden)
- **custo_producao**: URLs migradas de `conab.gov.br` para `gov.br/conab/` com scraping multi-tab (3 abas de planilhas)
- **custo_producao**: `parse_links_from_html()` reescrito com BeautifulSoup + dedup de URLs

### Known Issues
- **INMET**: API de dados (`/estacao/dados/`) retornando 404 em todos endpoints — API fora do ar externamente
- **BCB/SICOR**: API OData retornando 503 Service Unavailable — indisponibilidade temporaria
- **custo_producao**: Graos (soja, milho, cafe, algodao) nao disponiveis como xlsx no gov.br — conteudo carregado via JavaScript dinamico

## [0.6.3] - 2026-02-06

### Fixed
- `__version__` atualizado para `0.6.3` (estava travado em `0.6.0` desde o v0.6.0)
- `.gitignore` corrompido — linhas garbled reescritas, adicionados roadmap v4 e insights
- README: parâmetro inexistente `periodo=` corrigido para `inicio=` na API CEPEA
- README: `cepea.produtos()` agora com `await` (função é async)
- `ruff>=0.14.0` corrigido para `ruff>=0.4.0` (versão 0.14 não existe)
- `site_url` corrigido no mkdocs.yml e pyproject.toml (era `agrobr.dev`, agora aponta para GitHub Pages)
- Testes CEPEA API marcados como `@pytest.mark.integration` (chamavam API real sem mock)

### Changed
- `playwright` movido de dependência core para extra `[browser]` (~50MB a menos no install padrão)
- Notícias Agrícolas client reescrito — Playwright removido, agora usa httpx puro (página é server-side rendered, não precisa de JS)
- `docs/sources/` (4 páginas órfãs) adicionadas ao nav do mkdocs.yml
- `docs/index.md` reescrito — agora reflete estado atual do projeto (datasets, 20 indicadores, features v0.6)
- Documentação atualizada: 20 produtos CEPEA, LSPA aliases, algodão cBRL/lb, troubleshooting sem Playwright
- Arquivo `nul` (artefato Windows) removido do repositório

## [0.6.2] - 2026-02-05

### Fixed
- URLs do Notícias Agrícolas corrigidas para milho, boi, café, algodão e trigo (retornavam 404)
- Unidade do algodão corrigida de `BRL/@` para `cBRL/lb` (centavos de real por libra-peso)
- Parser trigo ajustado para tabela com 4 colunas (Data, Região, R$/t, Variação)
- `wait_for_selector("table.cot-fisicas")` substituído por `wait_for_selector("table td")` — classe CSS não existe mais no site
- IBGE LSPA aceita nomes genéricos de produto (`milho` → `milho_1` + `milho_2`, `feijao` → `feijao_1` + `feijao_2` + `feijao_3`)
- Playwright cleanup no Windows — `atexit` handler evita `ValueError: I/O operation on closed pipe`
- Circuit breaker no CEPEA httpx — pula tentativa direta (403 Cloudflare) por 10min após primeira falha, eliminando ~2s de latência

### Added
- 11 novos produtos CEPEA via Notícias Agrícolas: arroz, açúcar cristal, açúcar refinado, etanol hidratado, etanol anidro, frango congelado, frango resfriado, suíno, leite, laranja indústria, laranja in natura
- Total de 20 indicadores CEPEA/ESALQ disponíveis via fallback Notícias Agrícolas
- Parser v2 com suporte a tabelas multi-região (trigo: Paraná + RS)
- Aliases LSPA: `milho`, `feijao`, `amendoim`, `batata` expandem para sub-safras automaticamente

## [0.6.1] - 2026-02-05

### Fixed
- Playwright graceful degradation — import com try/except, não crasha em Python 3.14+
- Parser Notícias Agrícolas levanta `ParseError` ao invés de retornar lista vazia silenciosamente
- Cache fallback automático com `StaleDataWarning` quando todas as fontes falham

## [0.6.0] - 2026-02-05

### Added
- **Camada Semântica** - 4 datasets padronizados com fallback automático entre fontes
  - `datasets.preco_diario()` - Preços diários (CEPEA → cache)
  - `datasets.producao_anual()` - Produção anual (IBGE PAM → CONAB)
  - `datasets.estimativa_safra()` - Estimativas safra corrente (CONAB → IBGE LSPA)
  - `datasets.balanco()` - Balanço oferta/demanda (CONAB)
  - `datasets.list_datasets()` / `datasets.list_products()` / `datasets.info()`
- **Contratos Públicos** - Garantias formais de schema versionado
  - Documentação em `docs/contracts/` para cada dataset
  - Colunas estáveis, tipos só alargam, breaking changes só em major
- **Modo Determinístico Aprimorado** - Context manager async com contextvars
  - `async with datasets.deterministic("2025-12-31"):` - Isolado por task
  - `@deterministic_decorator("2025-12-31")` - Decorator para funções
  - `is_deterministic()` / `get_snapshot()` - Verificar estado atual
- **Hierarquia de Exceções Expandida**
  - `NetworkError` - Erros de rede (timeout, HTTP error, DNS)
  - `ContractViolationError` - DataFrame não atende contrato do dataset
- **MetaInfo Expandido** - Novos campos de proveniência
  - `dataset` - Nome do dataset
  - `contract_version` - Versão do contrato
  - `snapshot` - Data de corte (modo determinístico)
- **Documentação Avançada**
  - `docs/advanced/reproducibility.md` - Guia de reprodutibilidade
  - `docs/advanced/pipelines.md` - Integração Airflow, Prefect, Dagster
- **Notebook Demo** - Google Colab com exemplos executáveis

### Changed
- `agrobr.sync.datasets` - API síncrona para datasets
- README atualizado com seção de datasets e status das fontes

## [0.5.0] - 2026-02-04

### Added
- **Plugin System** - Arquitetura extensível para fontes e validadores
  - `SourcePlugin` - Interface para novas fontes de dados
  - `ParserPlugin` - Interface para parsers customizados
  - `ExporterPlugin` - Interface para exportadores customizados
  - `ValidatorPlugin` - Interface para validadores customizados
  - `register()`, `get_plugin()`, `list_plugins()` - Gerenciamento de plugins
- **API Stability Decorators** - Marcadores de estabilidade de API
  - `@stable(since="x.y.z")` - Marca API como estável
  - `@experimental(since="x.y.z")` - Marca API como experimental
  - `@deprecated(since, removed_in, replacement)` - Marca API como deprecated
  - `@internal` - Marca API como interna (não pública)
  - `list_stable_apis()`, `list_experimental_apis()`, `list_deprecated_apis()`
- **SLA Documentado** - Contratos de nível de serviço por fonte
  - `SourceSLA` - Definição de SLA com tier, freshness, latency, availability
  - `CEPEA_SLA` - Tier CRITICAL, atualização diária 18h, 99% uptime
  - `CONAB_SLA` - Tier STANDARD, atualização mensal, 98% uptime
  - `IBGE_SLA` - Tier STANDARD, varia por pesquisa
  - `get_sla()`, `list_slas()`, `get_sla_summary()`
- **Certificação de Qualidade** - Sistema de certificação de dados
  - `QualityLevel` - GOLD, SILVER, BRONZE, UNCERTIFIED
  - `QualityCheck` - Check individual com status e detalhes
  - `QualityCertificate` - Certificado completo com score e validade
  - `certify(df)` - Executa checks (completeness, duplicates, schema, freshness, range)
  - `quick_check(df)` - Retorna (level, score) rapidamente

## [0.4.0] - 2026-02-04

### Added
- **Modo Determinístico** - Reprodutibilidade absoluta para backtests
  - `agrobr.set_mode("deterministic", snapshot="2025-01-01")`
  - `agrobr.configure()` para opções globais
  - `agrobr.get_config()` para consultar configuração atual
  - `agrobr.reset_config()` para resetar ao padrão
- **Sistema de Snapshots** - Gerenciamento de versões de dados
  - `create_snapshot()` - Cria snapshot dos dados atuais
  - `load_from_snapshot()` - Carrega dados de um snapshot
  - `list_snapshots()` / `delete_snapshot()` - Gerenciamento
  - CLI: `agrobr snapshot create/list/delete/use`
- **Export Auditável** - Formatos com metadados de proveniência
  - `export_parquet()` - Parquet com metadata embutido
  - `export_csv()` - CSV com arquivo sidecar .meta.json
  - `export_json()` - JSON com metadados opcionais
  - `verify_export()` - Verificação de integridade

## [0.3.0] - 2026-02-04

### Added
- **Stability Contracts** - Garantias formais de schema para todas as fontes
  - `CEPEA_INDICADOR_V1` - Contrato para indicadores de preço CEPEA
  - `CONAB_SAFRA_V1` - Contrato para dados de safra CONAB
  - `CONAB_BALANCO_V1` - Contrato para balanço oferta/demanda CONAB
  - `IBGE_PAM_V1` - Contrato para dados PAM do IBGE
  - `IBGE_LSPA_V1` - Contrato para dados LSPA do IBGE
  - `contract.validate(df)` - Validação automática contra contrato
  - `contract.to_markdown()` - Documentação automática
- **Validação Semântica** - Verificações avançadas de qualidade
  - Validação de preços positivos
  - Validação de faixas de produtividade por cultura
  - Detecção de anomalias em variação diária (>20%)
  - Consistência de sequência de datas
  - Consistência de áreas (colhida <= plantada)
  - Validação de formato de safra
  - `validate_semantic(df)` - Executa todas as regras
  - `get_validation_summary(df)` - Resumo das violações
- **Benchmark Suite** - Ferramentas para medição de performance
  - `benchmark_async()` / `benchmark_sync()` - Benchmark de funções
  - `run_api_benchmarks()` - Benchmark das APIs
  - `run_contract_benchmarks()` - Benchmark de validação de contratos
  - `run_semantic_benchmarks()` - Benchmark de validação semântica

### Changed
- Changelog reestruturado seguindo Keep a Changelog

## [0.2.0] - 2026-02-04

### Added
- **`agrobr doctor`** - Comando CLI para diagnóstico do sistema
  - Verificação de conectividade das fontes
  - Estatísticas do cache (tamanho, registros, por fonte)
  - Status de configuração
  - Output JSON (`--json`) e formatado Rich
- **Parâmetro `return_meta`** - Suporte a data lineage em todas as APIs
  - `cepea.indicador(return_meta=True)` retorna `(DataFrame, MetaInfo)`
  - `conab.safras(return_meta=True)` retorna `(DataFrame, MetaInfo)`
  - `ibge.pam(return_meta=True)` retorna `(DataFrame, MetaInfo)`
  - `ibge.lspa(return_meta=True)` retorna `(DataFrame, MetaInfo)`
- **Classe `MetaInfo`** - Metadados de proveniência e rastreabilidade
  - Informações da fonte (nome, URL, método)
  - Timing (duração fetch, duração parse)
  - Status do cache (from_cache, cache_key, expires_at)
  - Integridade do conteúdo (hash SHA256, tamanho)
  - Versões (agrobr, parser, schema, python)
  - `to_dict()` / `to_json()` para serialização
  - `verify_hash(df)` para verificação de integridade
- **Documentação** - Guias de proveniência e resiliência
  - `docs/sources/cepea.md` - Documentação da fonte CEPEA
  - `docs/sources/conab.md` - Documentação da fonte CONAB
  - `docs/sources/ibge.md` - Documentação da fonte IBGE
  - `docs/advanced/resilience.md` - Documentação de resiliência

### Changed
- `MetaInfo` exportado do pacote principal

## [0.1.2] - 2026-02-04

### Changed
- **Smart TTL** para cache CEPEA - expira às 18:00 (horário de atualização CEPEA)
- Reduz requests desnecessários em ~90%

## [0.1.1] - 2026-02-04

### Fixed
- Browser fallback desabilitado para CEPEA (Cloudflare bloqueia)
- CEPEA agora vai direto para Notícias Agrícolas, evitando timeout

## [0.1.0] - 2026-02-04

### Added
- **CEPEA**: Indicadores de preços agrícolas (soja, milho, boi, café, algodão, trigo)
  - Fallback automático para Notícias Agrícolas quando CEPEA bloqueado
  - Acumulação progressiva de histórico no DuckDB
- **CONAB**: Dados de safras e balanço oferta/demanda
  - Parser para planilhas XLSX do boletim de safras
  - Suporte a todos os produtos principais (soja, milho, arroz, feijão, etc.)
- **IBGE**: Integração com API SIDRA
  - PAM (Produção Agrícola Municipal) - dados anuais
  - LSPA (Levantamento Sistemático) - estimativas mensais
- **Cache**: Sistema de cache com DuckDB
  - Separação entre cache volátil e histórico permanente
  - TTL configurável por fonte
  - Acumulação progressiva de dados
- **HTTP**: Cliente robusto com resiliência
  - Retry com exponential backoff
  - Rate limiting por fonte
  - User-agent rotativo
  - Fallback para Playwright quando necessário
- **CLI**: Interface de linha de comando completa
  - Comandos para CEPEA, CONAB e IBGE
  - Exportação em CSV, JSON e Parquet
- **Validação**: Sistema de validação multinível
  - Pydantic v2 para validação de tipos
  - Validação estatística (sanity checks)
  - Fingerprinting de layout para detecção de mudanças
- **Monitoramento**: Health checks e alertas
  - Health check por fonte
  - Alertas multi-canal (Slack, Discord, Email)
  - Monitoramento de estrutura
- **Suporte Polars**: Todas as APIs suportam `as_polars=True`
- **Testes**: 96 testes passando (~80% cobertura)
- **CI/CD**: GitHub Actions configurados
  - Testes automatizados
  - Health check diário
  - Monitoramento de estrutura

### Technical Details
- Python 3.11+ required
- Async-first design com sync wrapper
- Type hints completos
- Logging estruturado com structlog

[2.0.0]: https://github.com/bruno-portfolio/agrobr/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/bruno-portfolio/agrobr/compare/1.0.5...v1.1.0
[1.0.5]: https://github.com/bruno-portfolio/agrobr/compare/v1.0.4...1.0.5
[1.0.4]: https://github.com/bruno-portfolio/agrobr/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/bruno-portfolio/agrobr/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/bruno-portfolio/agrobr/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/bruno-portfolio/agrobr/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.12.0...v1.0.0
[0.12.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.11.3...v0.12.0
[0.11.3]: https://github.com/bruno-portfolio/agrobr/compare/v0.11.2...v0.11.3
[0.11.2]: https://github.com/bruno-portfolio/agrobr/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.10.1...v0.11.0
[0.10.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.10.0...v0.10.1
[0.10.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.7.1...v0.8.0
[0.7.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.6.3...v0.7.0
[0.6.3]: https://github.com/bruno-portfolio/agrobr/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/bruno-portfolio/agrobr/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.1.0...v0.5.0
[0.1.0]: https://github.com/bruno-portfolio/agrobr/releases/tag/v0.1.0
