# Migração para agrobr 2.0

O agrobr 2.0 torna explícitos erros e garantias que a série 1.x tratava de
forma permissiva. Use este guia para atualizar integrações existentes. As
versões dos contratos de cada dataset continuam independentes da versão da
biblioteca, conforme a [política SemVer](../contracts/semver.md).

**Como testar a migração:** um mock solto (`patch(...)`, `AsyncMock()`) aceita qualquer argumento e esconde a chamada que a
2.0 recusa ([§78](#78-argumento-fora-da-assinatura-levanta-typeerror)). Crie os mocks com `patch(..., autospec=True)`, ou
confira cada chamada com `inspect.signature(funcao).bind(*args, **kwargs)`: os 2 levantam `TypeError` para argumento fora
da assinatura, como a 2.0. Nas funções que mantêm `**kwargs` (como as da
[§78](#78-argumento-fora-da-assinatura-levanta-typeerror)), a assinatura aceita qualquer nome, e só a execução recusa o
desconhecido: nelas, nem o `autospec` nem o `bind` acusam `zarc.zoneamento(cultura=...)`, `mapbiomas.cobertura(estado=...)`
ou `desmatamento.deter(data_inicio=...)`. Confira essas chamadas contra a tabela da
[§85](#85-nomes-de-parametro-o-mesmo-vocabulario-em-toda-a-api).

**Servidor MCP e plugin do QGIS.** Versões compatíveis com a 2.0: [`agrobr-mcp`](https://pypi.org/project/agrobr-mcp/) 0.2.0 ou posterior e [`agrobr-qgis`](https://plugins.qgis.org/plugins/agrobr_qgis/) 0.2.0 ou posterior (publicado depois da agrobr 2.0). As versões 0.1.x dos dois usam a API da 1.x: com elas, mantenha `agrobr<2` até atualizar o servidor ou o plugin.

## Resumo: o que quebra

Em ordem de risco. Cada linha aponta a seção com o detalhe.

**Muda calado (a mesma chamada devolve outra coisa)**

1. `bcb.credito_rural` sem `agregacao` devolve o agregado por UF (11 colunas), e não mais os registros do SICOR (21 colunas):
   para o recorte da 1.1.0, passe `agregacao="registro"` ([§4](#4-bcbcredito_rural-passa-ao-contrato-20)).
2. `comtrade.comercio` com `partner=None` e `datasets.comercio_internacional` com `parceiro=None` devolvem o
   agregado World, e não mais todos os parceiros publicados: para o recorte da 1.1.0, passe `"all"` ou `"todos"`
   ([Comtrade](#comtrade-world-cobertura-e-contratos-20)). O `trade_mirror` exige um parceiro explícito: `None`,
   World, `"all"` e `"todos"` levantam `InvalidParameterError`.
3. A `safra` do crédito rural sai "2023/24", e não "2023/2024": converta o que foi gravado com
   `agrobr.normalize.dates.normalizar_safra` ([§76](#76-credito-rural-a-safra-sai-aaaaaa)).
4. Os datasets devolvem as colunas na ordem do contrato, e a `data` do `queimadas` sai em `datetime64`: leia as colunas pelo
   nome, não pela posição ([§74](#74-datasets-colunas-na-ordem-do-contrato-e-data-em-datetime64)).
5. Nomes de valor trocados: `nome_serie` sai `"ipa_agricola"` (era `"ipa_agropecuario"`), e `especie` sai `"galinhas"` (era
   `"galinhas_poedeiras"`). Os nomes antigos seguem aceitos na entrada, com `FutureWarning`, mas o filtro sobre a saída precisa
   trocar ([§31](#31-sgs-ipa_agricola-no-lugar-de-ipa_agropecuario), [§26](#26-ppm-galinhas-no-lugar-de-galinhas_poedeiras)).
6. `conab.safras(produto, safra=X)` sem `levantamento` lê X da publicação mais recente, com a revisão da CONAB: para o número
   original, passe `levantamento` ([§27](#27-conab-safra-passada-vem-da-revisao-mais-recente)).
7. `ibge.censo_agro_municipal_1985` devolve 1 linha por casa do PDF, com `valor` só na casa confirmada: use `valor_lido` e filtre
   pelo `status` ([§81](#81-censo-municipal-1985-casa-a-casa-com-o-status-de-cada-uma-contrato-20)).
8. Os logs do agrobr não seguem mais a configuração do structlog da aplicação: saem pelo `logging` da biblioteca padrão, em
   JSON, e se controlam por `logging.getLogger("agrobr")` ([§73](#73-logs-fora-da-saida-padrao)).
9. `municipio` casa o nome inteiro ou o código IBGE, e não mais um pedaço do nome, no SICAR, no MapBiomas, no PSR, no ZARC
   e na ANP: a mesma chamada pode trazer outro município, mais linhas ou menos
   ([§86](#86-municipio-nome-inteiro-ou-codigo-ibge)).
10. Colunas renomeadas na saída: `estado` → `uf` no progresso da CONAB e no MapBiomas; colunas em português em
    `posicionamento_fundos`, `oferta_demanda_global` e `comercio_internacional`; `produto` normalizado no
    `conab.brasil_total`. Quem só exporta o frame recebe outro schema, sem erro
    ([§87](#87-colunas-renomeadas-na-saida)).
11. Colunas de data que eram texto saem em `datetime64` (DERAL, IMEA, catálogo do INMET, ANTAQ e praças da ANTT): o
    filtro por texto `dd/mm/aaaa` com o dia até 12 casa outra data, e o do DERAL pelo nome da aba (`"Atual"`, `"02-10-23"`)
    pode voltar vazio, sem aviso ([§89](#89-tipos-da-saida)).
12. Inteiros por natureza saem em `Int64`, texto no dtype padrão do pandas e o vazio com os tipos do cheio; 6 contratos da
    1.1.0 passam a 2.0 por isso ([§89](#89-tipos-da-saida)).
13. `AGROBR_CACHE_DIR` passa a valer, e `true`/`yes` passam a desligar o cache da ANEC e do Acervo Fundiário
    ([§91](#91-variaveis-de-ambiente)).
14. O IBAMA guarda o CSV por 1 hora: dentro da hora, a mesma chamada devolve a coleta anterior
    ([§94](#94-downloads-pesados-e-cache)).
15. `ibge produtos --pesquisa PAM` consulta a PAM, e não mais o LSPA
    ([§92](#92-cli-formato-em-todos-os-comandos-e-snapshot-use-retirado)).
16. O catálogo de datasets e contratos, o `normalize` e a ANEC devolvem cópias: alterar o objeto devolvido não muda mais a
    chamada seguinte ([§95](#95-retornos-como-copia)).
17. `mapbiomas.cobertura`, `mapbiomas.transicao` e `datasets.uso_do_solo` sem `colecao` leem a coleção 11, e não mais a 10: a
    mesma chamada devolve outros números. Para o recorte da 1.1.0, passe `colecao=10`
    ([§63](#63-mapbiomas-classe-fora-da-legenda-sai-nula-com-aviso)).
18. Pela CONAB, `area_colhida` sai nula em `conab.safras` e no `estimativa_safra`, e `data_publicacao` é a data do boletim, e
    não mais o dia da consulta ([§3](#3-conabsafras-passa-ao-contrato-20)).
19. `acervo_fundiario.sigef(uf)` devolve as mesmas parcelas em outra ordem (públicas, depois privadas) e com a coluna nova
    `natureza`; o `source_details` passa a ser por arquivo ([§29](#29-acervo-fundiario-sigef-publico-e-privado-snci-por-uf)).
20. `antt_pedagio.fluxo_pedagio` sai com `sentido` em maiúsculas (`CRESCENTE`, `DECRESCENTE`): o filtro e o `groupby` pelo
    texto publicado mudam, e o volume não ([§24](#24-antt-preserva-cobranca-e-frequencia)).
21. `anp_diesel.vendas_diesel` sai com `produto` `DIESEL S10` (era `DIESEL S-10`) e `regiao` pelo nome canônico
    (`Centro-Oeste`, era `REGIÃO CENTRO-OESTE`): o filtro pelo texto antigo volta vazio ([§97](#97-outras-mudancas-por-fonte)).
22. `agrobr conab levantamentos` lista todos os levantamentos, e não só os 10 primeiros ([§92](#92-cli-formato-em-todos-os-comandos-e-snapshot-use-retirado)).
23. `MetaInfo.license` e `datasets.info()["licenses"]` mudam de classe: IMEA e Notícias Agrícolas passam a `zona_cinza`,
    Comtrade a `restrito` e Acervo Fundiário a `livre`; com CEPEA e Notícias Agrícolas juntos, `nc`. Revise filtros por
    licença ([§11](#11-novos-avisos-de-licenca-e-fallback)).

**Passa a levantar erro**

24. Status HTTP de erro sai como `SourceUnavailableError`, e não como `httpx.HTTPStatusError`: troque o `except`
   ([§75](#75-status-http-de-erro-sourceunavailableerror-nao-httpxhttpstatuserror)).
25. Argumento desconhecido ou com nome errado, que a 1.1.0 descartava em silêncio, levanta erro antes da rede: `TypeError`
    nas 77 funções que perderam o `**kwargs`, e `TypeError` ou `InvalidParameterError` nas 33 que o mantêm: corrija o nome
    ([§78](#78-argumento-fora-da-assinatura-levanta-typeerror)).
26. Parâmetro impossível (UF inexistente, período invertido) levanta `InvalidParameterError` antes da rede, onde a 1.1.0 devolvia
    vazio ou erro de fonte; na 2.0, a regra vale em todas as fontes ([§66](#66-parametro-impossivel-recusado-antes-da-rede),
    [§90](#90-erros-a-classe-diz-a-causa)).
27. `as_polars=True` sem o Polars levanta `ImportError`, e não devolve mais pandas: instale `agrobr[polars]`
    ([§8](#8-as_polarstrue-exige-polars)). O extra passa a exigir `polars>=0.20.3`: com um Polars mais antigo fixado, a
    instalação acusa conflito ([§19](#19-dependencias-falhas-e-saidas-estruturadas)).
28. Os nomes antigos de parâmetro (`data_inicial`, `start`, `commodity`, `cultura`, `estado`, `cod_municipio`,
    `max_features` e outros) levantam `TypeError` (`InvalidParameterError` em `zarc.zoneamento`, `mapbiomas.cobertura` e
    `mapbiomas.transicao`): troque pelo nome novo ([§85](#85-nomes-de-parametro-o-mesmo-vocabulario-em-toda-a-api)).
29. `as_polars`, `return_meta` e as opções depois deles passam a ser só por nome; no IBGE, também os filtros secundários
    ([§88](#88-flags-e-filtros-secundarios-so-por-nome)).
30. Falha de layout em todas as fontes de um dataset levanta `ParseError`, e não mais `SourceUnavailableError`
    ([§90](#90-erros-a-classe-diz-a-causa)).
31. Na CLI, `--output`, `--json` e `snapshot use` saem com código 2: use `--formato`
    ([§92](#92-cli-formato-em-todos-os-comandos-e-snapshot-use-retirado)).
32. `precos_diesel` com período que começa depois de hoje levanta `InvalidParameterError` antes da rede, onde a 1.1.0
    devolvia vazio na UF e no Brasil ([§97](#97-outras-mudancas-por-fonte)).
33. `embrapa_solos.mapa_solos(ordem=...)` casa a classe inteira: um trecho do nome (`"latos"`) levanta
    `InvalidParameterError` com a lista das classes ([§97](#97-outras-mudancas-por-fonte)).
34. `AGROBR_HTTP_RATE_LIMIT_<FONTE>` e o `rate_limit_*` por argumento recusam negativo, `inf` e `nan` com `ValidationError`
    ([§91](#91-variaveis-de-ambiente)).
35. `cadastro_rural`, `desmatamento`, `exportacao`, `importacao`, `uso_do_solo` e `zoneamento_agricola` dentro de
    `datasets.deterministic(...)` levantam `InvalidParameterError` antes da rede, onde a 1.1.0 devolvia o dado corrente: tire
    essas chamadas do bloco ([§71](#71-modo-deterministico-aviso-onde-o-modo-nao-se-aplica)).
36. `normalize.municipio_para_ibge(nome)` sem `uf` levanta `InvalidParameterError` quando o nome é de mais de um município
    (521 municípios dividem 240 nomes), onde a 1.1.0 devolvia calado o código do primeiro da lista: passe `uf`
    ([§90](#90-erros-a-classe-diz-a-causa)).
37. `abiove.exportacao(ano, produto="total")` levanta `InvalidParameterError`, onde a 1.1.0 devolvia vazio: para o total
    dos produtos, use `agregacao="mensal"` ([§39](#39-abiove-edicao-mais-recente-e-edicao)).

**Deixa de levantar erro**

38. `datasets.estimativa_safra` sem observações em todas as fontes devolve o vazio do contrato com `UserWarning`, e não
    mais `SourceUnavailableError`: confira `df.empty` ([estimativa_safra](#estimativa_safra-contrato-31-e-selecao-temporal)).
39. `cftc.cot` sem relatório no recorte devolve vazio tipado, e não mais `SourceUnavailableError`; as contagens saem em
    `Int64` ([§99](#99-cftc-recorte-sem-relatorio-devolve-vazio-tipado)).

**API removida**

40. Saem `agrobr.configure()`, os módulos `quality`, `sla`, `export`, `plugins` e `validators.semantic` e o
    `load_baseline_fingerprint`, e o `cache.get_policy` passa a valer só para o CEPEA: remova os usos
    ([§9](#9-modulos-experimentais-foram-removidos), [§10](#10-agrobrconfigure-foi-removida),
    [§50](#50-limpeza-de-codigo-morto), [§83](#83-cache-a-politica-so-do-cepea-e-sai-o-load_baseline_fingerprint)).
    O `uf=` de `anda.entregas` e de `datasets.fertilizante` também sai: era parâmetro explícito na 1.1.0, e passá-lo levanta
    `TypeError`, porque os PDFs só trazem o total nacional ([§50](#50-limpeza-de-codigo-morto)).
41. Os subpacotes `agrobr.conab.custo_producao` e `agrobr.conab.serie_historica` saem: importe as funções de
    `agrobr.conab` ([§93](#93-conab-um-caminho-por-funcao)).
42. `b3.oi_historico` passa a se chamar `b3.posicoes_abertas_historico`, também em `sync.b3`; o `tipo="oi_historico"`
    do `futuros_agricolas` fica ([§98](#98-b3-oi_historico-passa-a-se-chamar-posicoes_abertas_historico)).
43. O parâmetro `_moeda` de `cepea.indicador` sai: passá-lo levanta `TypeError` ([§6](#6-cepea-rejeita-parametros-invalidos)).

Para ficar na série 1.x enquanto migra: `pip install "agrobr<2"`.

## BCB PTAX: moedas, boletins e contratos explícitos

`bcb.ptax` acrescenta `moeda="USD"`, `boletim="fechamento"` e `top=1000`. O padrão continua fechamento do dólar; use `boletim="todos"` para abertura, intermediários e fechamento, ou selecione `abertura`/`intermediario`. A nova `bcb.ptax_moedas()` expõe o catálogo OData atual em três colunas (`moeda`, `nome`, `tipo_moeda`), com contrato 1.0. A aquisição passa a depender desse catálogo para validar a moeda, com recursos e falhas explícitos; ele não é uma tabela histórica de vigência.

Cotações passam ao contrato **2.0**, parser **2**, mantendo como prefixo `cotacao_compra`, `cotacao_venda`, `data_hora`, `data` e acrescentando `moeda`, `paridade_compra`, `paridade_venda`, `tipo_boletim`. Medidas usam float64 finito anulável; data_hora e data usam datetime64[ns], sem fuso, inclusive vazios. `data` deixa de ser coluna de objetos Python date; use `.dt.date` na aplicação se precisar dessa representação. Atualize schemas e chaves: a identidade é moeda, horário completo e tipo publicado dentro da rota. Não descarte frações de segundo.

As rotas por dia/período podem chamar o mesmo fechamento de `Fechamento PTAX` e `Fechamento`. O seletor reconhece ambos; a saída preserva a diferença para não inventar texto publicado. Em todos, tipo novo, vazio ou null permanece com diagnóstico; seletores específicos geram erro quando não conseguem classificar a linha. Considere a variação de rótulo ao juntar resultados de rotas distintas. Não some os boletins nem use a última linha como média diária.

Datas passam a validação estrita. Data única junto de intervalo gera erro; apenas início completa fim com a data de hoje no calendário de Brasília, apenas fim completa início com fim−30 dias. Os filtros parciais antes ignorados agora são respeitados. Moeda aceita três letras ASCII e normaliza apenas caixa; símbolos ausentes no catálogo atual geram erro antes do GET de cotações. Vazio, erro HTTP e envelope sem value são situações distintas.

`top` controla páginas, sem cortar a saída. A coleta valida todas as páginas antes de filtrar boletins. Preserve metadados: cobertura das cotações e do catálogo é separada, e sem total independente fica unknown mesmo após página vazia. Hash/tamanho superiores representam manifesto de recursos. A cotação histórica usa a unidade monetária corrente daquela data; não rotule toda a série como BRL nem atribua UTC ao relógio publicado.

Use `BCB_PTAX_V2` e `BCB_PTAX_MOEDAS_V1`, de `agrobr.contracts.bcb_ptax`, ou os registros `bcb_ptax`/`bcb_ptax_moedas`. Não existia contrato de cotações V1 registrado. Veja [API](../api/bcb.md#ptax) e [contratos](../contracts/bcb_ptax.md).

## BCB Focus: anual/mensal, identidade e contrato 2.0

`bcb.focus` acrescenta o argumento nomeado `periodicidade="anual"|"mensal"`; o padrão continua anual, com indicador `"PIB Agropecuária"`. A saída mantém as dez colunas anteriores e acrescenta `periodicidade` e `indicador_detalhe`. Atualize seleções de colunas e chaves de armazenamento: o detalhe anual antes descartado distingue Exportações, Importações e Saldo, por exemplo. A chave completa inclui periodicidade, indicador, detalhe, data da pesquisa, referência e base de cálculo. Não some bases ou detalhes diferentes.

Schema/contrato passam a **2.0**, parser a **2**; o novo contrato registrado é `bcb_focus`, constante `BCB_FOCUS_V2`. Datas civis usam `datetime64[ns]`, estatísticas `float64` e contagem/base `Int64`, inclusive no vazio. Use `.isna()` para ausências; detalhe vazio publicado continua distinto de nulo. `data_referencia` mantém texto YYYY ou MM/YYYY: projeções futuras são legítimas e não representam observações realizadas.

Indicadores exigem o nome exato, sem aliases ou ajuste de caixa. Quantidades booleanas/fracionárias, datas inválidas, JSON malformado, campos ausentes e duplicatas agora geram erro explícito. Estatísticas finitas inconsistentes permanecem com aviso. A ordem ganha desempates por referência textual, base e detalhe anual; resultados limitados na mesma data podem mudar em relação à ordem anterior.

`top` é tamanho de página; `max_registros` corta somente após validar a página inteira. Páginas curtas já não encerram a coleta automaticamente: o client avança pela quantidade recebida até vazio, limite ou contagem reconciliada. Preserve `source_details.coverage`: término da aquisição não comprova o total da fonte. O hash/tamanho superiores identificam manifesto, com hashes individuais dos corpos. Veja [API](../api/bcb.md#focus) e [contrato](../contracts/bcb_focus.md).

## BCB SGS: histórico por blocos e contrato 3.0

`bcb.sgs` mantém os 17 aliases e as quatro colunas, mas passa ao schema **3.0** e parser **2**; o período passa a
`inicio`/`fim` ([§85](#85-nomes-de-parametro-o-mesmo-vocabulario-em-toda-a-api)). Séries que publicam `dataFim` (ex.: TR) ganham a coluna opcional `data_fim` depois das quatro; a 1.1 descartava esse campo. Datas civis usam sempre `datetime64[ns]`, inclusive no pandas 3, que antes podia inferir us. Valores usam float64, códigos `Int64` e nomes desconhecidos são nulos. Use `.isna()` para ausências. O novo contrato registrado é `bcb_sgs`, constante `BCB_SGS_V3`; o `BCB_SGS_V2` conserva o schema 2.1, e não existia contrato SGS V1 registrado.

Intervalos longos são divididos por anos civis, unidos e ordenados antes de aplicar `ultimos`. Sem datas, a janela padrão usa uma única data de referência, a de hoje no calendário de Brasília; apenas início preenche o fim, mas apenas fim conserva o início omitido. Para mais de 20 observações na série 1, informe as duas datas com `ultimos`, pois a rota nativa de últimos valores recusa 21.

Parâmetros booleanos usados como código/quantidade, strings numéricas como código, datas malformadas e intervalos invertidos agora falham antes da rede. Dados inválidos deixam de virar nulos silenciosamente. Duplicatas no mesmo corpo e valores conflitantes entre blocos geram erro. Referências mensais/trimestrais anteriores ao dia inicial continuam presentes, com aviso e origem; não as trate automaticamente como observações diárias.

O envelope oficial de ausência de valores, com HTTP 404 ou 200, passa a vazio tipado com aviso, sem comprovar a existência do código. Falhas de blocos não retornam parcial. Preserve os metadados: cobertura distingue aquisição dos blocos de completude da série, que permanece desconhecida; o hash/tamanho superiores identificam um manifesto, com hashes próprios dos corpos. Veja a [API SGS](../api/bcb.md#sgs) e o [contrato](../contracts/bcb_sgs.md).

## Comtrade: World, cobertura e contratos 2.0

`comtrade.comercio` passa ao contrato **2.1** (`comercio_bilateral`), `trade_mirror` ao **2.0** e `datasets.comercio_internacional` ao **3.0**. `partner=None`, `world`, `mundo` e `"0"` agora enviam o agregado World explicitamente. Para conservar a antiga consulta que omitia o parâmetro e retornava todos os parceiros publicados, use `partner="all"` ou `"todos"`. Não some o agregado com seus componentes. No `datasets.comercio_internacional`, os filtros e as colunas saem em português: `parceiro=None` envia o World ([§85](#85-nomes-de-parametro-o-mesmo-vocabulario-em-toda-a-api), [§87](#87-colunas-renomeadas-na-saida)).

O bilateral mantém 22 colunas e acrescenta `classificacao` e `classificacao_original`; o dataset também preserva as 24 em vazio. No contrato 2.1, as 3 marcas de estimativa da ONU entram como flags anuláveis (27 colunas): veja a [seção 58](#58-comtrade-marcas-de-estimativa-da-onu). Sua chave usa `periodo, reporter_code, partner_code, hs_code, fluxo_code, classificacao`. O espelho passa de 18 a 24 colunas, com revisão/flag de cada perna e dois códigos numéricos. ISO descritivo pode ser nulo. Inteiros usam `Int64`, medidas `float64` e flags `boolean` anulável. Use `.isna()` para ausências.

HS aceita 2/4/6 dígitos ASCII, incluindo listas textuais; códigos ímpares e parâmetros desconhecidos agora falham. Períodos mensais distinguem YYYYMM de YYYY, que expande o ano inteiro. Falhas de bloco e respostas incompatíveis deixam de ser ignoradas. Espelho não aceita `fluxo`, World ou países iguais e rejeita revisões HS incompatíveis na mesma célula.

O client compara contagem independente à união de partições. O padrão `require_complete=False` (`exigir_completo` no dataset) devolve parcial com aviso; use True quando a aplicação exigir cobertura comprovada. Preserve `source_details` para conhecer limites e tentativas. O hash superior identifica um manifesto de recursos, com tamanho próprio; cada corpo tem seu hash. Snapshot apenas preenche o ano omitido.

Use `COMERCIO_BILATERAL_V2` e `TRADE_MIRROR_V2` de `agrobr.contracts.comtrade`, ou o registry. As constantes V1 permanecem históricas. A licença interna é `restrito`, com aviso na primeira chamada e as dispensas expressas da política da ONU; veja [Licenças](../licenses.md#un-comtrade). Veja [API, metadados e limites](../api/comtrade.md).

## Lista Suja: CSV, contexto de publicação e contrato 2.0

`lista_suja.empregadores()` passa a usar CSV sem dependência opcional e a conferir o TXT anunciado para obter a edição. `formato="pdf"` conserva acesso ao PDF com `agrobr[pdf]`; formatos explícitos são exclusivos. O modo `auto` pode usar PDF após indisponibilidade elegível do CSV, com a causa nos metadados. Corpo inválido ou descoberta incompatível geram erro.

O contrato **2.0** preserva as oito colunas anteriores e acrescenta `id_registro`, `data_decisao`, `data_atualizacao` e `data_inclusao_texto`. Anos e contagens passam a `Int64`, inclusive nulos/vazios; ausências textuais passam de strings vazias para nulos. Use `.isna()` para ausências. Datas civis têm dtype `datetime64[ns]`. Uma inclusão com intervalo e nova data mantém `data_inclusao=NaT` e conserva todo o campo em `data_inclusao_texto`; não extraia uma data sem uma regra explícita da aplicação.

Informe `id_registro` como texto exato. A chave é local à exportação; preserve também `MetaInfo.raw_content_hash`, pois documentos podem aparecer em várias linhas e datas de atualização podem ter revisões. Atualização periódica e atualização do cadastro são distintas em `source_details.publication`. TXT divergente gera erro; TXT ausente ou indisponível deixa a data nula com diagnóstico. Não há seleção de edições históricas nessa API.

Use `get_contract("lista_suja_empregadores")` ou `LISTA_SUJA_EMPREGADORES_V2`, de `agrobr.contracts.lista_suja`. Filtros inválidos e argumentos desconhecidos agora geram `InvalidParameterError` antes da rede. Veja [colunas, proveniência e limites](../sources/lista_suja.md).

## defensivos: situação, composição e cache por coleta

As APIs `formulados`, `autorizacoes` e `tecnicos` preservam suas colunas anteriores e passam ao schema **1.1**. Formulados e autorizações acrescentam `situacao` textual; formulados e técnicos acrescentam `composicao_texto`, com a célula original. Nos técnicos, o parser também corrige ingredientes e grupos com parênteses internos ou vários componentes. A nova `composicao(tipo="formulados"|"tecnicos")` usa contrato **1.0**, com uma linha por posição de componente, incluindo ingredientes repetidos. Concentração e unidade ficam nessa tabela; valores ambíguos permanecem nulos com diagnóstico.

Informe `nr_registro` como texto exato, preservando zeros e acentos, como `"00301"` ou `"14017/Pré-Mistura"`. Filtros vazios, numéricos ou booleanos e argumentos desconhecidos agora geram `InvalidParameterError` antes da rede. `situacao="TRUE"` compara o texto publicado sem inferir vigência; técnicos não aceitam esse filtro.

O cache passa a ZIPs versionados por família, incluindo composição, tipos e proveniência. Arquivos antigos ficam preservados, mas a primeira chamada com o parser novo precisa coletar novamente. `use_cache=False` ignora leitura e gravação. Em cache, `MetaInfo.fetched_at`, `fetch_timestamp` e o hash identificam a coleta original; `timestamp` é a hora da consulta atual. Os CSVs correntes não reconstituem um cadastro histórico por data. Veja [API e contratos](../api/defensivos.md).

## cadastro_rural: filtros SICAR e contexto temporal

O dataset agora encaminha `atualizado_apos` ao SICAR e aceita `as_polars`, os 2 só por nome, como o `return_meta`. `municipio` aceita o nome inteiro ou o código IBGE ([§86](#86-municipio-nome-inteiro-ou-codigo-ibge)) e segue na 2ª posição: os 7 primeiros posicionais (`uf` a `criado_apos`) continuam válidos. Fonte tabular e dataset passam ao contrato **2.1**, com as mesmas onze colunas mais a opcional `cod_municipio` e chave `[cod_imovel]`, mas datas `datetime64[ns, UTC]`, inclusive nulos e vazios. No dataset `cadastro_rural`, filtros desconhecidos passam a gerar erro antes da rede.

```python
from agrobr import datasets

df, meta = await datasets.cadastro_rural(
    "DF", municipio=5300108,
    atualizado_apos="2026-09-01", return_meta=True,
)
```

`criado_apos` inclui o limite (`>=`); `atualizado_apos` usa limite estrito (`>`). O filtro de atualização é rejeitado nas doze UFs sem esse campo no WFS. Nas demais camadas, a projeção agora solicita a data de atualização que a implementação anterior omitia.

A coleta tabular usa GeoJSON com atributos, sem geometria e sem dependência de GeoPandas. A migração evita o horário sem fuso do CSV, que não coincide com o instante usado pelo CQL. `atualizado_apos` aceita `Z`/offset e normaliza para UTC; entradas sem fuso são interpretadas como UTC. Um timestamp retornado pode ser reutilizado com `.isoformat()`. O filtro aceita precisão de milissegundos: zeros adicionais são normalizados (`.212000` vira `.212`), enquanto valores submilissegundo são rejeitados sem arredondamento. Não use `tz_localize("UTC")` em dados CSV antigos sem conhecer seu fuso; recupere os instantes no novo transporte quando necessário.

Use `get_contract("cadastro_rural")` ou `SICAR_IMOVEIS_V2`, de `agrobr.contracts.sicar`. `SICAR_IMOVEIS_V1`, de `agrobr.contracts.datasets`, permanece como contrato histórico, não como alias para UTC.

Remova `cadastro_rural` de blocos `datasets.deterministic(...)`: o dataset passa a rejeitar esse contexto com `InvalidParameterError`, inclusive com filtros explícitos. Antes, a data de snapshot virava filtro de criação posterior, selecionando outro conjunto de registros. Uma consulta incremental ao cadastro corrente não recupera o estado do cadastro em uma data passada. Veja [filtros e limites](../contracts/cadastro_rural.md).

## clima: histórico público e contrato mensal 3.1

`datasets.clima` passa a consultar a API observacional INMET, os ZIPs públicos e, no modo UF, o NASA POWER, nessa ordem. Use `fonte="inmet_historico"` para escolher os arquivos, `fonte="inmet"` para a API com token ou `fonte="nasa_power"` para o ponto representativo da UF. Uma fonte explícita não aciona outra rota. O ZIP cobre 2000 em diante; anos anteriores não o incluem no fallback.

```python
df, meta = await datasets.clima(
    estacao="A001", inicio="2000-12-30", fim="2001-01-02",
    fonte="inmet_historico", return_meta=True,
)
mensal = await datasets.clima("GO", 2001, fonte="inmet_historico")
```

O contrato mensal **3.1** mantém a chave `[mes, uf]` e as fórmulas de agregação. As três temperaturas passam a aceitar nulos, assim como a precipitação: períodos sem medições não recebem zero. Há quatro colunas opcionais novas de localização e base, `lat`, `lon`, `agregacao_espacial` e `base_tempo`; as de cobertura (`estacoes_chuva`, `estacoes_chuva_parciais`, `dias`, `data_inicio` e `data_fim`) estão na [seção 17](#17-unidades-historicas-cache-e-medicoes-ausentes). Coordenadas NASA são preservadas; no agregado INMET, ficam nulas. As bases diárias são UTC no INMET e LST no NASA; um ponto solicitado à NASA não equivale à média das estações estaduais.

O modo estação valida `clima_estacao` **1.0** para diário e o novo `clima_estacao_horaria` **1.0**, com chave `[data, hora_utc, estacao]`, para horário. `agregacao="mensal"` em estação passa a gerar erro; antes podia devolver horas sem agregá-las. Combinações de UF/ano com estação, datas ignoradas no modo UF e argumentos desconhecidos também falham antes da rede. `as_polars=True` converte após validar o contrato.

O contrato atual está em `agrobr.contracts.clima.CLIMA_V3`; `agrobr.contracts.datasets.CLIMA_V2` permanece histórico. `fonte` continua identificando a instituição (`inmet` ou `nasa_power`), enquanto `meta.selected_source` diferencia o acesso `inmet_historico`. `meta.source_details` preserva recursos, hashes, membros, metadados por edição e cobertura por variável. O catálogo atual de estações operantes da API difere das estações presentes nos arquivos; use a rota explícita para controlar essa seleção.

Em `datasets.deterministic(...)`, clima usa o ano do contexto somente quando `ano` não é informado. O contexto não corta observações na data indicada, não força execução offline nem congela a revisão dos arquivos. Isso é declarado nos metadados; preserve os dados e os hashes para reproduzir a edição consultada. Veja o [contrato e os limites de cobertura](../contracts/clima.md).

## ANA: proveniência de consultas com várias páginas

`return_meta=True` agora preenche `raw_content_hash` e `raw_content_size` também quando a ANA retorna mais de uma página. Nesse caso, o hash identifica a serialização UTF-8 de `{"query": ..., "resources": ...}`, com chaves ordenadas, `ensure_ascii=False` e separadores `(',', ':')`. `raw_content_size` mede essa serialização. O tamanho total dos corpos originais está em `source_details["resource_bytes"]`.

`source_details` publica `hash_kind="resource_manifest_sha256"`, `manifest_encoding="canonical_json_utf8"`, `manifest_fields=["query", "resources"]`, `query` e `resources`. A consulta lógica contém `fonte`, `recurso`, `where`, `bbox`, `max_registros` e `formato`; `resources` preserva a ordem da aquisição e contém `pagina` (a partir de 1), `sha256` e `bytes`.

Uma única página conserva o hash e o tamanho do corpo; zero páginas conserva hash nulo e tamanho zero. Dados, colunas, tipos, geometrias, CRS, requisições e `source_url` das APIs tabulares/geográficas permanecem iguais. O manifesto de hashes do `MetaInfo` não armazena corpos; use a coleta bruta para preservá-los em disco.

## Imports de constantes de contrato

Atualize os imports diretos que mudaram de nome no 2.0:

Os imports nomeados históricos continuam disponíveis com `DeprecationWarning`. Eles conservam o schema da versão indicada e ficam fora de `__all__` e do registro ativo. Os contratos atuais possuem suas próprias colunas; alterar um contrato histórico não modifica o atual. Os três aliases descritos abaixo seguem a mesma política de aviso e exportação, mas continuam apontando ao contrato atual, como já faziam.

| Módulo em `agrobr.contracts` | Nome anterior | Nome atual |
|---|---|---|
| `datasets` → `clima` | `CLIMA_V1` / `CLIMA_V2` | `CLIMA_V3` |
| `ibge` | `IBGE_PAM_V1` | `IBGE_PAM_V2` |
| `datasets` → `antt_pedagio` | `ANTT_PEDAGIO_FLUXO_V1` / `ANTT_PEDAGIO_FLUXO_V2` | `ANTT_PEDAGIO_FLUXO_V3` |
| `conab` → `conab_custos` | `CONAB_CUSTO_PRODUCAO_V1` / `CONAB_CUSTO_PRODUCAO_V2` | `CONAB_CUSTOS_V3` |
| `datasets` | `CREDITO_RURAL_V1_1` | `CREDITO_RURAL_V2` |
| `datasets` | `EXPORTACAO_V1` | `EXPORTACAO_V1_1` |
| `datasets` | `IMPORTACAO_V1` | `IMPORTACAO_V1_2` |
| `datasets` | `MAPBIOMAS_COBERTURA_V1` | `MAPBIOMAS_COBERTURA_V2` |
| `datasets` | `MAPBIOMAS_TRANSICAO_V1` | `MAPBIOMAS_TRANSICAO_V2` |
| `datasets` | `PRECO_ATACADO_V1` | `PRECO_ATACADO_V2` |
| `datasets` | `MAPA_PSR_APOLICES_V1` | `MAPA_PSR_APOLICES_V2` |

`preco_atacado` e `mapa_psr_apolices` passam a 2.0: no atacado, `categoria` sai nula para produto fora da
tabela do agrobr; nas apólices, `seguradora` entra na chave. Quem valida a categoria trata o nulo, e quem junta
apólices pela chave antiga inclui a seguradora. `PRECO_ATACADO_V1` e `MAPA_PSR_APOLICES_V1` devolvem os
contratos 1.0 da 1.1.0, com `DeprecationWarning`; não descrevem a saída atual.

Os nomes canônicos `IBGE_LSPA_V2`, `IBGE_CENSO_AGRO_LEGADO_V2` e
`CONAB_SAFRA_V2` também identificam os contratos atuais (`ibge.lspa` 2.0, `ibge.censo_agro_legado` 2.1, `conab.safras` 2.0). Seus três nomes antigos
terminados em `_V1` permanecem aliases para compatibilidade de import e
apontam ao mesmo contrato atual; não disponibilizam o schema antigo. Para
consultas por dataset, prefira `get_contract(nome)` e leia `contract.version`.

Os contratos que mudam de versão pelas regras de nome e de tipo da 2.0 (`abate_trimestral`, `antt_pedagio_pracas`,
`condicao_lavouras`, `movimentacao_portuaria`, `oferta_demanda_global`, `posicionamento_fundos`, `comercio_internacional`
e `bcb_sgs`) ganham constante nova, e a antiga segue importável, sem aviso, com o schema anterior (2 exceções:
`POSICIONAMENTO_FUNDOS_V1` passou à 1.1, com as colunas opcionais `swap_spread` e `other_spread`, e
`COMERCIO_BILATERAL_V1`, a antiga de `comercio_internacional`, é histórica e emite `DeprecationWarning`): veja a
[seção 89](#89-tipos-da-saida).

## estimativa_safra: contrato 3.1 e seleção temporal

O contrato do dataset passa a ser `ESTIMATIVA_SAFRA_V3_1`, em `agrobr.contracts.estimativa_safra`. A fonte CONAB continua usando `CONAB_SAFRA_V2`; seu alias não passa a representar o dataset novo. Para obter o contrato certo, use `get_contract("estimativa_safra")`, versão `"3.1"`.

As dez colunas anteriores continuam presentes, com duas adições nullable, `ano_lspa` e `mes_lspa`, e duas de unidade, `unidade_producao` (`mil_ton`) e `unidade_area` (`mil_ha`), iguais nas 2 fontes. A chave passa de `[safra, produto, uf, levantamento]` para `[fonte, safra, produto, uf, levantamento, ano_lspa, mes_lspa]`. Atualize chaves de banco, deduplicação e verificações de schema: a mudança de identidade exige uma versão major. Linhas LSPA antigas não contêm informação suficiente para reconstruir o mês somente pela safra; recupere a referência pela proveniência arquivada ou faça nova consulta explícita.

Os 3 argumentos posicionais `produto, safra, uf` são preservados; `return_meta` passa a só por nome ([§88](#88-flags-e-filtros-secundarios-so-por-nome)), como os novos `fonte`, `levantamento`, `mes` e `as_polars`. Use `levantamento` para CONAB e `mes` para LSPA; combinações incompatíveis falham antes da rede, e referências explícitas indisponíveis não são substituídas por outra fonte ou período.

```python
from agrobr import datasets

conab_mt = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", levantamento=1
)
lspa_mt = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", mes="01"
)
```

A segunda chamada consulta janeiro do ano civil 2025. O levantamento 1 CONAB é outra referência; não representa janeiro LSPA. Informe a mesma UF ao comparar fontes, pois `uf=None` retorna UFs na CONAB e Brasil no LSPA. Consulte o [contrato completo](../contracts/estimativa_safra.md).

**Recorte sem observações.** Quando todas as fontes consultadas respondem sem dados para o recorte (por exemplo, a safra que a aba da CONAB ainda não publica, ou a safra seguinte à mais recente dos levantamentos da CONAB, com o LSPA ainda sem o ano), o dataset devolve o vazio do contrato, com `UserWarning` e o aviso em `meta.validation_warnings`, e não mais `SourceUnavailableError`. Quem usava a exceção para detectar "sem dado" passa a conferir `df.empty`. Com uma fonte vazia e a outra fora do ar, segue `SourceUnavailableError`; com a outra falhando por layout, `ParseError`.

## 1. ANDA aceita somente o produto total

Antes, um produto específico era aceito, embora o PDF da ANDA trouxesse apenas
o total de fertilizantes:

```python
df = await anda.entregas(2025, produto="ureia")
```

Agora, use o único valor representado pela fonte:

```python
df = await anda.entregas(2025, produto="total")
```

O que fazer: remova filtros de produto específico nas consultas ANDA e não
interprete o total publicado como volume de uma categoria de fertilizante.

## 2. Toda coluna estável precisa estar presente

Na série 1.x, um adaptador podia omitir uma coluna `stable` que aceitasse
nulos:

```python
df = pd.DataFrame(
    {
        "data": pd.to_datetime(["2026-09-04"]),
        "produto": ["soja"],
        "valor": [135.0],
        "unidade": ["BRL/sc60kg"],
        "fonte": ["cepea"],
    }
)
contracts.get_contract("preco_diario").validate(df)
```

No 2.0, a coluna deve existir; `nullable` permite valor nulo, não ausência da
coluna:

```python
df["praca"] = pd.NA
contracts.get_contract("preco_diario").validate(df)
```

O que fazer: ao produzir DataFrames para validação, crie todas as colunas
`stable`; preencha com `pd.NA` somente aquelas cujo contrato marca como
`nullable`.

## 3. `conab.safras` passa ao contrato 2.0

Código 1.x podia presumir que levantamento e data de publicação sempre
existiam:

```python
levantamentos = df["levantamento"].astype("int64")
datas = pd.to_datetime(df["data_publicacao"])
```

No contrato 2.0, os dois campos são nulos nas linhas da série histórica (§27); no dataset `estimativa_safra`
(contrato 3.1), também nas linhas do fallback IBGE LSPA:

```python
levantamentos = df["levantamento"].astype("Int64")
datas = pd.to_datetime(df["data_publicacao"], errors="coerce")
sem_levantamento = df[df["levantamento"].isna()]
```

O que fazer: trate os dois campos como opcionais e use dtypes pandas que
aceitem nulos.

`area_colhida` sai nula pela CONAB: o boletim publica uma área só, que a 1.x
copiava para as 2 colunas. Use `area_plantada`. `data_publicacao` passa a ser a
data do boletim; na 1.x, era o dia da consulta.

## 4. `bcb.credito_rural` passa ao contrato 2.0

Na 1.1.0, a chamada padrão (`agregacao="municipio"`) não agregava: devolvia os registros do SICOR,
um por mês, UF, programa, subprograma, fonte de recursos, tipo de seguro, atividade e modalidade, com 21 colunas no
custeio (20 no investimento e na comercialização, que não têm área). A coluna `volume` do contrato 1.x nunca saiu:

```python
df = await bcb.credito_rural("soja", agregacao="municipio")
total = df["volume"].sum()
```

No 2.0, `"municipio"` levanta `InvalidParameterError`, e a agregação padrão é por UF; a opção por programa agrupa
também pelo programa:

```python
por_uf = await bcb.credito_rural("soja", agregacao="uf")
por_programa = await bcb.credito_rural("soja", agregacao="programa")

colunas = [
    "programa",
    "cd_programa",
    "qtd_contratos",
    "valor",
    "area_financiada",
    "fonte",
]
resultado = por_programa[colunas]
```

As agregações `uf` e `programa` têm 11 colunas. Das 21 da chamada padrão da 1.1.0, 12 não saem nelas: `regiao`,
`mes_emissao`, `ano_emissao`, `cd_sub_programa`, `cd_fonte_recurso`, `fonte_recurso`, `cd_tipo_seguro`, `tipo_seguro`,
`cd_modalidade`, `modalidade`, `cd_atividade` e `atividade`. Código da 1.1.0 que agrupa por mês, fonte de recursos,
modalidade ou atividade cai em `KeyError` nelas. O recorte da 1.1.0 é o `agregacao="registro"`:

```python
registros = await bcb.credito_rural("soja", safra="2024/25", agregacao="registro")
por_mes_e_fonte = registros.groupby(["mes_emissao", "fonte_recurso"])["valor"].sum()
```

O `registro` devolve as 21 colunas da 1.1.0, mais `agregacao` e `fonte`, no contrato
[bcb.credito_rural_registro](../contracts/bcb_credito_rural_registro.md) 1.0, também no dataset `credito_rural`.
Diferenças em relação à 1.1.0:

- a safra sai `"2024/25"` (seção 76);
- os nomes de programa e tipo de seguro vêm das tabelas oficiais do BCB (seção 30), e os de fonte de recursos,
  modalidade e atividade, da descrição oficial inteira. A 1.1.0 tinha dicionários digitados: a fonte `0403`, de 39 dos
  277 registros do custeio da soja de MT em 2024/25, saía `Desconhecido (0403)`;
- código de fonte de recursos, modalidade ou atividade fora da tabela deixa o nome nulo, em vez de `Desconhecido (…)`;
- `area_financiada` sai em todas as finalidades, nula no investimento e na comercialização;
- sem fallback BigQuery: com o OData fora, levanta `SourceUnavailableError`. O fallback só vale para `agregacao="uf"`
  sem `programa` nem `tipo_seguro`, porque a tabela da Base dos Dados não traz programa nem seguro.

O que fazer: substitua `agregacao="municipio"` por `"registro"` (o mesmo recorte, registro a registro) ou por `"uf"`
ou `"programa"` (agregado), remova o uso de `volume` e adapte a seleção às novas colunas. Para dados municipais via
BigQuery, use a integração opcional `agrobr[bigquery]` fora desse contrato.

## 5. DERAL preserva a safra no produto

Na série 1.x, primeira e segunda safras podiam ser fundidas:

```python
milho = df[df["produto"] == "milho"]
```

Agora, os rótulos identificados pela fonte ficam separados:

```python
milho = df[df["produto"].isin(["milho", "milho_1", "milho_2"])]
feijao = df[df["produto"].isin(["feijao", "feijao_1", "feijao_2"])]
```

O que fazer: inclua os sufixos `_1` e `_2` em filtros, agrupamentos e chaves;
o nome sem sufixo continua possível quando a planilha não identifica a safra.

## 6. CEPEA rejeita parâmetros inválidos

Antes, produto ou praça desconhecidos e alguns intervalos inválidos podiam
resultar em DataFrame vazio:

```python
df = await cepea.indicador("banana", praca="marte")
if df.empty:
    print("Sem dados")
```

Agora, `indicador()`, `ultimo()` e `pracas()` levantam
`InvalidParameterError`:

```python
from agrobr import InvalidParameterError

try:
    df = await cepea.indicador("soja", inicio="2026-01-01")
except InvalidParameterError as exc:
    print(f"Parâmetro inválido: {exc}")
```

O que fazer: valide ou normalize entradas do usuário e trate
`InvalidParameterError` separadamente de ausência de dados e falhas da fonte.

**Sai o `_moeda`.** O parâmetro reservado `_moeda` de `indicador()` não convertia nada e sai. Passá-lo, por nome ou como 5º argumento posicional, levanta `TypeError`: remova o argumento. As unidades publicadas seguem na coluna `unidade`.

## 7. Erros de parâmetros interrompem a cascata dos datasets

Na série 1.x, aplicações podiam tratar uma entrada inválida como falha de
fonte:

```python
try:
    df = await datasets.preco_diario("banana")
except SourceUnavailableError:
    tentar_mais_tarde()
```

No 2.0, uma entrada inválida não tenta fallbacks:

```python
from agrobr import InvalidParameterError, SourceUnavailableError

try:
    df = await datasets.preco_diario("soja")
except InvalidParameterError:
    corrigir_entrada()
except SourceUnavailableError:
    tentar_mais_tarde()
```

O que fazer: capture `InvalidParameterError` antes dos erros de disponibilidade
e solicite a correção do parâmetro em vez de repetir a consulta.

## 8. `as_polars=True` exige Polars

Antes, pedir Polars sem a dependência instalada podia devolver pandas:

```python
df = await cepea.indicador("soja", as_polars=True)
# df podia ser pandas.DataFrame
```

Agora, instale o extra antes de solicitar esse formato:

```bash
pip install "agrobr[polars]>=2.0"
```

```python
df = await cepea.indicador("soja", as_polars=True)
# df é polars.DataFrame; sem o extra, a chamada levanta ImportError
```

O que fazer: instale `agrobr[polars]` ou mantenha `as_polars=False` e trabalhe
com pandas. O extra exige `polars>=0.20.3`.

## 9. Módulos experimentais foram removidos

Imports diretos encontrados por exploração do pacote deixam de existir:

```python
from agrobr.export import export_csv
from agrobr.quality import quick_check
from agrobr.validators import validate_safra
```

Use APIs públicas mantidas ou recursos nativos do DataFrame:

```python
from agrobr import contracts

contracts.get_contract("estimativa_safra").validate(df)
df.to_csv("safra.csv", index=False, encoding="utf-8")
```

O que fazer: remova usos de `quality`, `sla`, `export`, `plugins`,
`validators.semantic` e `validators.validate_safra`. Não há substituto direto
para o sistema de plugins ou para a validação semântica experimental; implemente
regras específicas na aplicação quando necessário.

## 10. `agrobr.configure()` foi removida

A chamada deprecada nunca alterou o comportamento real de fetch, cache ou
fallback:

```python
import agrobr

agrobr.configure(cache_enabled=False, log_level="DEBUG")
```

No 2.0, remova a chamada. Para o modo determinístico suportado, use o contexto
do dataset:

```python
from agrobr import datasets

async with datasets.deterministic("2026-09-04"):
    df = await datasets.preco_diario("soja")
```

O que fazer: use as variáveis de ambiente `AGROBR_*` documentadas para cada
configuração efetiva. O modo determinístico atualmente cobre apenas
`preco_diario`; consulte o [guia de snapshots](snapshots.md).

## 11. Novos avisos de licença e fallback

Antes, uma consulta podia trocar de fonte sem um sinal dedicado ao chamador:

```python
df = await datasets.preco_diario("soja")
```

Agora, é possível tornar qualquer fallback explícito na política da aplicação:

```python
import warnings

from agrobr import datasets
from agrobr.exceptions import SourceFallbackWarning

warnings.filterwarnings("error", category=SourceFallbackWarning)
df = await datasets.preco_diario("soja")
```

A primeira chamada ao CEPEA também emite um aviso sobre a licença CC BY-NC
4.0. O que fazer: mantenha o aviso visível, revise as
[licenças das fontes](../licenses.md) e escolha conscientemente se um fallback
deve ser aceito, registrado ou convertido em erro.

### Classificações de licença na 2.0

Quem filtra por `MetaInfo.license` deve atualizar sua política para estas mudanças:

| Fonte ou conjunto de fontes | Antes | Agora |
|---|---|---|
| IMEA — séries públicas | `restrito` | `zona_cinza` |
| Notícias Agrícolas — publicador | `restrito` | `zona_cinza` |
| UN Comtrade | `zona_cinza` | `restrito` |
| Acervo Fundiário/INCRA | `nc` | `livre` |
| CEPEA e Notícias Agrícolas em `data_sources` | `restrito` | `nc` |

B3 permanece `zona_cinza`. Arquivos não públicos do IMEA continuam sujeitos à autorização escrita prevista nos termos. Dados de origem CEPEA conservam CC BY-NC 4.0, inclusive no fallback; quando as duas fontes aparecem nos metadados, prevalece `nc`. Comtrade conserva as dispensas de redistribuição previstas em sua política. O Acervo deixa de emitir o aviso de vedação comercial.

As novas classes também aparecem em `datasets.info()["licenses"]` e na descrição das fontes; `datasets.info("comercio_internacional")["license"]` e `source_details["license"]["classification"]` das novas consultas Comtrade passam a `restrito`. Arquivos de metadados já salvos não são regravados. `MetaInfo.from_dict()` recalcula `license` pela tabela instalada, mas conserva as classificações antigas dentro de `source_details`. Ao ler arquivos antigos, confira sua proveniência e a [tabela atual de licenças](../licenses.md) antes de aplicar um filtro.

`livre` admite uso comercial no escopo descrito, mas pode exigir atribuição, preservação de avisos, indicação de alterações e condições ND/SA. `zona_cinza` não concede permissão nem estabelece proibição geral. A reclassificação não muda o fluxo de aquisição nem os valores retornados.

O MapBiomas Alerta mantém `livre`, mas seus dados, inclusive os obtidos pela API, estão sob CC BY-SA 3.0 BR: preserve atribuição, link da licença, indicação de alterações e as condições SA para adaptações. Imagens e laudos de terceiros exigem termos próprios. Um filtro baseado só em `license == "livre"` não verifica essas obrigações; consulte o [escopo do MapBiomas Alerta](../licenses.md#mapbiomas-alerta).

## 12. Custos preservam a aba publicada

Remova `tecnologia=` de `conab.custo_producao`, `conab.custo_producao_total` e `datasets.custo_producao`. O contrato ativo é **3.0**, com **25 colunas**. Use `agrobr.contracts.conab_custos.CONAB_CUSTOS_V3` ou `get_contract("custo_producao")`. Selecione `planilha` e `aba` sem ambiguidade; contexto, rótulos literais e números físicos de linha são preservados.

Não há chave primária artificial. Preserve ocorrências repetidas, distinção de subtotal/total, tokens de safra anuláveis e referências de preços separadas. Receitas negativas são válidas; quantidades e preços unitários ausentes não são derivados de outros custos. Veja o [contrato completo](../contracts/custo_producao.md).

## 13. Produtos SICOR e vazio válido

Substitua `cafe_arabica` e `cafe_conilon` por `cafe` no crédito rural. O SICOR não distingue essas modalidades. Milho e trigo agora usam correspondência exata, excluindo silagem e sarraceno dos agregados; totais antigos podem mudar.

Filtros válidos sem registros retornam DataFrame vazio com colunas e tipos do contrato. Na finalidade `investimento`, o produto é um item de investimento, não necessariamente a cultura financiada. Não trate ausência de registros como crédito igual a zero sem avaliar a cobertura da fonte.

## 14. Tipos e proveniência

Contratos rejeitam inteiros fracionários, texto com valores não textuais e booleanos numéricos/textuais. Use tipos anuláveis (`Int64`, `Float64`, `boolean`) para preservar ausências. `required_columns` inclui todas as colunas estáveis, mesmo anuláveis, e `MetaInfo.schema_version` corresponde à versão do contrato do dataset.

Instale `agrobr[polars]` para conversão: o extra inclui `pyarrow`. Erros de conversão não são mais apresentados como ausência de Polars quando a dependência que falta é outra.

### MetaInfo: timestamps UTC com fuso

Os quatro campos temporais de `MetaInfo` (`fetched_at`, `timestamp`, `cache_expires_at` e `fetch_timestamp`) passam a usar sempre UTC com fuso, inclusive em atribuições posteriores. Valores naive de entrada são interpretados como UTC; valores aware com outro offset são convertidos preservando o instante. Por exemplo, `2024-06-15T23:30:00-03:00` torna-se `2024-06-16T02:30:00+00:00`. Campos opcionais mantêm `None`.

`from_dict()` continua aceitando strings ISO antigas sem fuso. No construtor e em atribuições, forneça objetos `datetime`; string crua gera `AttributeError`, sem conversão implícita. `to_dict()` passa a incluir `+00:00` em todos os timestamps preenchidos; atualize comparações de strings e schemas de consumidores. Compare ou subtraia esses campos com `datetime.now(UTC)`, importando `UTC` de `datetime`; `utcnow()` continua retornando datetime naive e não serve para essa comparação direta. Datas civis publicadas nas colunas dos datasets mantêm seus contratos próprios.

### MetaInfo: duas convenções de identidade da fonte

`selected_source` e `attempted_sources` podem identificar o adaptador do dataset ou a rota informada pela fonte. `comercio_internacional`, `desmatamento`, `empregadores_lista_suja`, `unidades_conservacao`, `unidades_conservacao_federais`, `uso_do_solo`, `cultivares_registradas` e `cultivares_protegidas` preservam a proveniência interna quando informada, inclusive com uma única tentativa. Os demais datasets usam o nome do adaptador (`DatasetSource.name`): numa consulta simples de `cadastro_rural`, os campos são `"sicar"` e `["sicar"]`, embora a API da fonte identifique `sicar_wfs`.

Na regra da base, mais de uma tentativa interna ou `selected_source="cache"` faz o dataset incorporar a proveniência da fonte. As tentativas combinam adaptadores anteriores e rotas internas em ordem, sem duplicatas; a seleção usa a rota informada ou o adaptador se ela estiver ausente. `from_cache` continua indicando reutilização da aquisição e é propagado separadamente: seu valor `True`, sozinho, não muda os nomes publicados. Essas convenções permanecem distintas na 2.0; confira os identificadores do dataset consumido ao persistir ou comparar proveniência.

## 15. CEPEA e limites históricos

Revise séries anteriores de soja Paraná, frango resfriado, etanol anidro e açúcar refinado: a seleção antiga podia usar outra tabela. O refinado usa sua página própria e unidade `BRL/kg`. As laranjas são publicadas na página de citros e permanecem disponíveis.

Suíno preserva a coluna Estado como praça; as linhas legadas afetadas ficam em quarentena, fora das consultas normais e sem perder os originais. Para leite, `data` é o primeiro dia do mês de referência, `praca` é a UF ou BRASIL e a unidade é `BRL/L`; a tabela spot não entra. O histórico CEPEA acumula no cache: solicitar um ano não obriga a fonte a publicar um ano de dados.

## 16. Snapshots e parâmetros de período

Snapshots exigem `pyarrow` ou `fastparquet` e aceitam somente CEPEA, CONAB e IBGE. Zero arquivos remove o diretório criado e levanta `SnapshotError`, exportada em `agrobr`; a CLI sai com código 1. Snapshots parciais registram erros por fonte no manifesto. `load_from_snapshot()` recusa `pyarrow` anterior ao 14.0.1 (CVE-2023-47248) com `ImportError`, antes de ler o Parquet; atualize com `pip install "pyarrow>=14.0.1"`.

PPM rejeita anos futuros; PRODES exige ano inteiro dentro do intervalo da camada. Abate, leite trimestral e PIB aceitam formatos como `2025-T4`, normalizados para `202504`. ZARC aceita aliases e nomes da tábua, mas a disponibilidade depende da safra. Arquivos anuais legados de queimadas são suportados; o fallback pode baixar centenas de MB. HTTP 400/404 na solicitação do token indica arquivo não publicado e retorna vazio após validação da data. No download, HTTP 404 retorna vazio, mas HTTP 400 levanta `SourceUnavailableError`. As operações token/download são serializadas no processo; o histórico consulta os dias sequencialmente.

## 17. Unidades históricas, cache e medições ausentes

`ibge.pam` e o contrato `producao_anual` 2.2 mantêm os números publicados e acrescentam `unidade_producao`, `unidade_rendimento`, `unidade_valor_producao` e `condicao_produto`. Agrupe ou converta explicitamente antes de comparar períodos. Laranja muda de mil frutos para toneladas em 2001; café muda de em coco para beneficiado em 2002; moedas anteriores a 1994 não são reais.

O contrato mensal `clima` 3.1 preserva precipitação e temperaturas ausentes. Na 1.1.0, a chuva da UF no INMET era a soma das estações; agora é a média dos totais das estações com chuva válida em todos os dias do mês. A estação com o mês incompleto fica fora e é contada em `estacoes_chuva_parciais` (`estacoes_chuva` conta as que entraram); sem nenhuma completa, como no mês corrente, `precip_acum_mm` sai nulo, com `UserWarning` e a mesma mensagem em `MetaInfo.validation_warnings`. Em MT, fevereiro de 2026, 12 das 34 estações com chuva tinham só 6 a 25 dos 28 dias: com elas na média, o mensal seria 254,1 mm; só com as 22 completas, é 305,7 mm. Somatórios parciais não são extrapolados: `dias`, `data_inicio` e `data_fim` dão a cobertura diária de cada mês, no INMET e no mensal do NASA POWER (schema 1.2). O modo diário por estação permanece no contrato `clima_estacao` 1.0; o horário usa `clima_estacao_horaria` 1.0.

As migrações do cache preservam automaticamente os registros afetados em `indicadores_quarentena`, no mesmo DuckDB, antes de retirá-los da área ativa. Isso inclui suíno e praças ausentes das migrações 5/6, as séries CEPEA da 7 e o refinado legado do Notícias Agrícolas com unidade incorreta, tratado na 8. A preservação não depende de backup manual. Consulte o procedimento de auditoria e recuperação abaixo.

`datasets.preco_diario` levanta `SourceUnavailableError` se fonte e cache falham; resultados legitimamente vazios por filtro continuam válidos. O aviso de fallback também cobre a troca interna CEPEA → Notícias Agrícolas e não é absorvido quando convertido em erro. Publicação CONAB ausente fica nula, não recebe a data da consulta. Navegadores são encerrados ao sair do contexto da página; chamadas sync repetidas não reutilizam recursos de um loop fechado.

## 18. Preservação automática do cache existente

O agrobr 2.0 exige DuckDB 1.5.2 ou mais recente. As versões 1.5.0 e 1.5.1 foram
excluídas por falhas de alteração de tabelas indexadas, inclusive em bancos legados.
A migração 2 adiciona somente colunas faltantes: `hit_count` e `stale` já existentes
não recebem `ALTER`, preservando seus valores em bancos parcialmente migrados.

A primeira abertura do cache atualiza o schema para a versão 11, mesmo quando a
consulta pede um produto não afetado. Toda a sequência pendente é uma única
transação: copiar para quarentena, conferir o conteúdo, retirar ou corrigir a área ativa e
registrar as versões. Acréscimos de coluna (migrações 2, 10 e 11) são idempotentes e
correm antes dessa transação, pois o DuckDB não altera uma tabela já modificada na
mesma transação. Falhas de escrita, permissão ou registro da versão levantam
`CacheMigrationError` (exportada por `agrobr`) e revertem a atualização; não são
tratadas como um cache vazio. Corrija a causa antes de tentar novamente.

A tabela `indicadores_quarentena` fica no mesmo arquivo `agrobr.duckdb` configurado
por `AGROBR_CACHE_DIR` (padrão: `~/.agrobr/cache`). Ela conserva todos os
campos e tipos originais, inclusive ID, praça, valor decimal, unidade, fonte,
metodologia, coleta e versão do parser. Acrescenta `quarantine_migration`,
`quarantined_at` e `quarantine_reason`. Consultas normais e offline leem somente
`indicadores`; a quarentena não é um fallback e não tem expiração automática.

As regras são conservadoras quando o legado não permite distinguir observações
corretas de incorretas:

- Migração 5: registros com praça nula, de qualquer fonte.
- Migração 6: suíno CEPEA anterior ao parser 2 ou Notícias Agrícolas anterior ao 3.
- Migração 7: parser CEPEA anterior ao 2 para `soja_parana`, `frango_resfriado`,
  `etanol_anidro`, `acucar_refinado`, `leite`, `laranja_industria` e
  `laranja_in_natura`.
- Migração 8: refinado CEPEA ou Notícias Agrícolas rotulado `BRL/sc50kg`, em vez
  de `BRL/kg`, independentemente da versão do parser. O legado NA já usava a
  versão 2; o número sozinho não permite reconhecer a unidade correta.
- Migração 9: leite do Notícias Agrícolas sai da área ativa, pois seu parser
  gravou a data de fechamento, enquanto CEPEA usa o mês de referência. Trigo
  CEPEA anterior ao parser 2 rotulado `BRL/sc60kg` passa a `BRL/ton`; algodão
  desse mesmo legado rotulado `BRL/@` passa a `cBRL/lb`. Os números publicados
  permanecem iguais; a migração corrige apenas esses rótulos comprovadamente
  incorretos, conservando também as linhas originais em quarentena.
- Migração 10: acrescenta `valor_usd` e `peso_medio_kg` a `indicadores` e, quando já
  existe, a `indicadores_quarentena`; nada é posto em quarentena, e registros anteriores
  ficam nulos nessas colunas até nova coleta.
- Migração 11: acrescenta `anomalies` às mesmas tabelas e grava a marca de média semanal
  no cache. Os registros anteriores recebem `["media_semanal"]` no etanol hidratado e
  anidro do Notícias Agrícolas, que só publica médias semanais, e lista vazia nos demais;
  nada é posto em quarentena.

Só são executadas as migrações ainda pendentes. Novas observações dos parsers
corrigidos usam CEPEA 2 e Notícias Agrícolas 3. A correção de rótulos da migração
9 não converte valores entre unidades. Os demais registros em quarentena podem
exigir conferência com a fonte antes de qualquer recuperação.

Reserve espaço adicional para a cópia das linhas afetadas, índices, transação e
WAL; o tamanho necessário depende do banco e não há um multiplicador fixo
garantido. A quarentena preserva o histórico da migração, mas não protege contra
perda ou corrupção física do arquivo: mantenha também seus backups externos.

### Auditar e recuperar em uma cópia

Feche normalmente todas as conexões e processos que usam o banco antes de copiá-lo,
para concluir o checkpoint. Não copie apenas o arquivo principal de um banco ainda
em uso ou com transação pendente. Use caminhos explícitos; o exemplo pressupõe que
o arquivo de origem fechado esteja no diretório atual.

```python
import shutil
from pathlib import Path

import duckdb

origem = Path("agrobr.duckdb")
copia = Path("agrobr-auditoria.duckdb")
if copia.exists():
    raise FileExistsError(copia)
shutil.copy2(origem, copia)

with duckdb.connect(str(copia), read_only=True) as conn:
    resumo = conn.execute("""
        SELECT quarantine_migration, produto, fonte, unidade, COUNT(*) AS registros
        FROM indicadores_quarentena
        GROUP BY ALL ORDER BY ALL
    """).fetchall()
    print(resumo)
```

Para reconstruir a área ativa **somente nessa cópia de auditoria**, os originais
podem ser reinseridos sem alterar tipos, chaves, sequência de IDs ou versão do
schema. Execute apenas após avaliar os registros; a operação não corrige seus
valores ou rótulos e não deve ser aplicada ao cache de produção:

```python
with duckdb.connect(str(copia)) as conn:
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute("""
            INSERT INTO indicadores BY NAME
            SELECT * EXCLUDE (quarantine_migration, quarantined_at, quarantine_reason)
            FROM indicadores_quarentena
        """)
        conn.execute("COMMIT")
    except duckdb.Error:
        conn.execute("ROLLBACK")
        raise
```

Se já houver observações com as mesmas chaves, a inserção falha sem sobrescrevê-las.
Nesse caso, consulte os originais diretamente na quarentena e decida quais IDs
reconstruir na cópia. A quarentena permanece disponível após a reinserção.

Instalações que já executaram versões antigas das migrações 5/6/7 podem ter
registros ausentes. A migração 8 preserva o que ainda existe, mas não recria o que
já foi excluído. Reinstalar uma versão anterior da biblioteca não restaura dados.
Sem backup anterior, a quantidade perdida pode permanecer desconhecida; a janela
recente do CEPEA não garante recuperação integral.

### Cache e política de fallback

Em `cepea.indicador(..., return_meta=True)` e `datasets.preco_diario`,
`selected_source="cache"` identifica a leitura local. `data_sources` lista as
fontes efetivas das linhas retornadas, também identificadas na coluna `fonte`.
Uma consulta quente ou offline tem `attempted_sources=["cache"]` e não representa
uma nova tentativa de fallback.

Quando a coleta falha e o cache a substitui, `attempted_sources` preserva as
tentativas e termina em `"cache"`. O dataset emite `SourceFallbackWarning`,
inclusive nesse caminho interno, e respeita sua conversão em erro. A API direta
CEPEA emite `StaleDataWarning` para esse uso de cache após falha. Esses avisos não
são uma política de autorização por provedor: para rejeitar dados anteriormente
obtidos pelo Notícias Agrícolas, verifique `data_sources` ou a coluna `fonte`,
mesmo offline.

Banco ilegível: na 1.1.0, com o `agrobr.duckdb` danificado (queda de energia, disco cheio ou antivírus no meio
da gravação), o `cepea.indicador` seguia sem cache a cada chamada, sem `UserWarning` (só um `logger.warning`), até
alguém apagar o arquivo. Na 2.0,
o banco que o DuckDB acusa como ilegível (leitura incompleta, checksum ou arquivo inválido) vai para
`agrobr.duckdb.corrompido-<AAAAMMDDHHMM>`, com o WAL, o agrobr avisa uma vez com os 2 caminhos (`UserWarning`), e a
consulta seguinte cria um banco novo. A quarentena das migrações vai junto com o arquivo movido. Arquivo em uso
por outro processo e disco cheio não movem nada. Veja [o que o agrobr grava no disco](../advanced/disco.md).

## 19. Dependências, falhas e saídas estruturadas

Os mínimos de segurança também passam a HTTPX 0.28.1, httpcore 1.0.9, lxml 6.1.0, requests 2.33.0 e GeoPandas 1.1.4 no extra geo, e o `certifi` 2026.7.22 (autoridades certificadoras do TLS) entra como dependência direta do core. Veja a justificativa na [política de dependências](dependencies.md).

Atualize as dependências com o pacote: pandas mínimo 2.2.2, Typer 0.26.0, pdfplumber 0.11.10 no extra PDF, pyogrio 0.8.0 no extra geo e polars 0.20.3 no extra polars (o `as_polars=True` dos datasets usa o tipo `String`, que o Polars só tem a partir dele). Esses limites excluem combinações que falhavam no import, na CLI ou na extração numérica de PDFs. O transporte SIDRA passa a HTTP assíncrono direto; sidrapy não é mais uma dependência.

Consultas NASA divididas em blocos falham integralmente se um bloco não puder ser obtido. Erros de agregação são rejeitados antes da rede. Início depois de hoje (calendário de Brasília) em `clima_ponto` e `ano` posterior ao corrente em `clima_uf` também levantam `InvalidParameterError` antes da rede; na 1.1.0, a resposta sem nenhum dia virava `ParseError`. Não interprete ausência de medições como cobertura completa.

Na CLI, JSON/CSV ocupa somente stdout; progresso e erros usam stderr. Resultados vazios produzem `[]` ou o cabeçalho CSV. `snapshot list --formato json` vazio produz `[]`. `doctor` expõe erros de cache, respeita as configurações de health e retorna código 1 para erros locais ou indisponibilidade de qualquer fonte consultada. Isso diagnostica a saúde da coleta e não significa, por si só, instalação defeituosa. Avisos como credencial ausente não são confundidos com queda da fonte.

Retornos vazios usam o contrato da modalidade solicitada, inclusive PRODES/DETER, futuros, seguro rural e MapBiomas estadual. Contratos numéricos rejeitam booleanos, complexos e infinitos; datas numéricas não são interpretadas silenciosamente como timestamps de 1970.

Snapshots novos são publicados após a conclusão do manifesto e verificados por SHA-256 na leitura. Cancelar a criação libera o nome para uma nova tentativa. Snapshots antigos permanecem legíveis; veja as limitações de [integridade e interrupção abrupta](snapshots.md).

## 20. LSPA preserva mês, variável e unidade

O contrato `lspa` passa a 2.0. Use a chave `[ano, mes, produto, localidade, variavel]`; `mes` sempre existe e consultas anuais preservam os meses publicados. `variavel_cod` identifica a variável SIDRA e `unidade` informa a medida de cada linha. Não trate o rótulo do mês como produto ou variável. O dataset `estimativa_safra` continua selecionando o mês mais recente e consolidando as sub-safras nas suas próprias unidades.

## 21. Censo legado usa a geografia e os cabeçalhos oficiais

O contrato `censo_agropecuario_legado` passa a 2.1. `nivel="uf"` retorna totais estaduais; sem `uf`, consulta os 27 diretórios. Para categorias nacionais de atividade, use `nivel="brasil"` sem filtro estadual. Nos municípios, inclua `uf` na chave para distinguir homônimos; códigos históricos ausentes permanecem nulos.

Atualize filtros de `categoria` e `variavel` conforme o [contrato](../contracts/censo_agropecuario_legado.md): eles refletem os rótulos reais, incluindo a hierarquia dos cabeçalhos. Leia `unidade` por linha; contagens, áreas, produção e valores monetários são medidas distintas, com a escala da célula oficial aplicada e precisão decimal preservada.

Na 1.1.0, `censo_agro_legado("maquinas", uf="PA")` devolvia calada a Tabela 6 (pessoal ocupado) como tratores: o `Para/Tab_7Mn.zip` do IBGE é cópia do `Tab_6Mn.zip`, e o pessoal ocupado de Baixo Amazonas (124.592 pessoas) saía como `total_tratores`. Descarte séries de máquinas do Pará gravadas com a 1.x. Na 2.0, `uf="PA"` levanta `SourceUnavailableError`, e a consulta sem `uf` devolve as outras 26 UFs, com o aviso no `MetaInfo` e um `UserWarning`: a tabela municipal de maquinaria do Pará não está no FTP.

## 22. Café, cana e catálogo CONAB

Remova `cana_industria` das consultas de série histórica: as tabelas industriais não eram interpretadas e o produto deixa de ser anunciado na versão 2.0. Não há substituto industrial nessa API.

Para café, o contrato `serie_historica_safra` 1.1 acrescenta as colunas opcionais `area_em_producao_mil_ha` e `area_formacao_mil_ha`. A área plantada total é a soma quando ambas existem; produtividade continua referente à área em produção. Produção e produtividade passam corretamente de mil sacas de 60 kg e sacas/ha para mil toneladas e kg/ha. Revise séries de café persistidas com o parser anterior. O zero publicado na planilha passa a sair `0.0`: antes virava nulo, e a UF sem produção naquela safra não tinha linha. Séries persistidas com o parser anterior têm menos linhas e nulos onde a fonte publica zero. A safra zerada em todas as UFs (não levantada) continua fora.

No café (`cafe`, `cafe_arabica`, `cafe_conilon`), ES, RJ e SP saíam com `regiao="NORTE"`, porque a sub-região mineira "Norte, Jequitinhonha e Mucuri" era lida como macrorregião. Agora saem `SUDESTE`, e a região só muda com o rótulo exato da macrorregião. Refaça agregações por região feitas com séries de café persistidas na 1.x.

Em `cana`, a aba Área da planilha agrícola é a área colhida (título oficial "Série Histórica de Área Colhida"). Ela passa para a coluna opcional nova `area_colhida_mil_ha` do contrato 1.1, e `area_plantada_mil_ha` fica nula nesse produto. Na 1.x, esse valor saía como área plantada: troque a coluna onde lia `area_plantada_mil_ha` da cana. Para a área total, use `cana_area_total`.

**Muda calado: `algodao` na série histórica.** Na 1.1.0, `conab.serie_historica("algodao")` devolvia a produção e a produtividade do caroço de algodão (a semente), porque a última aba de cada métrica na planilha da CONAB sobrescrevia as anteriores. Na 2.0, `algodao` é o algodão em caroço (pluma mais semente), e a semente passa a `algodao_caroco`, com `algodao_pluma` para a pluma (#112); a área é a mesma nos três. Na mesma planilha (setembro de 2026), BA 2023/24, `algodao` dá 1.686,4 mil t e 4.874 kg/ha na 2.0, contra 978,1 mil t e 2.826,9 kg/ha na 1.1.0 (cerca de 1,7×). Para manter a série da 1.x, troque `algodao` por `algodao_caroco`; não junte séries de `algodao` gravadas com a 1.x a consultas da 2.0.

## 23. Preços e valores de produção respeitam a unidade da linha

Em `preco_diario`, `valor` está em `unidade`: algodão em `cBRL/lb` exige divisão por 100 para exibição em BRL/lb. Na PEVS, o valor de produção usa moeda, enquanto quantidade produzida usa a unidade física do produto. Preserve as unidades históricas informadas pelo SIDRA ao combinar períodos.

## 24. ANTT preserva cobrança e frequência

O contrato ativo `antt_pedagio_fluxo` é **3.0**, com 13 colunas. Use `ANTT_PEDAGIO_FLUXO_V3`, de `agrobr.contracts.antt_pedagio`, ou `get_contract("antt_pedagio_fluxo")`. A chave é `data`, `concessionaria`, `praca`, `sentido`, `tipo_veiculo`, `categoria_eixo`, `tipo_cobranca`, `frequencia`. Mantenha cobranças manuais e automáticas separadas.

Somente contagens textuais explícitas alimentam `n_eixos`; números isolados e categorias tarifárias ficam nulos. Rótulos e espaços da categoria são preservados. Faixas comerciais explícitas podem qualificar no filtro de pesados sem receber contagem exata inventada.

Escolha `frequencia="mensal"` ou `"diaria"`; nenhuma substitui a outra silenciosamente. O recorte de datas ainda valida o CSV anual inteiro baixado. Tetos de CSV/spool/transferência: 512 MiB / 1 GiB / 3 GiB. O limite padrão do parser é de 500 mil linhas selecionadas; exceder orçamento gera erro.

O contrato do cadastro de praças passa a 2.0 (`ANTT_PEDAGIO_PRACAS_V2`), com `km_m`, `ano_do_pnv_snv` e `data_da_inativacao` tipados ([§89](#89-tipos-da-saida)); o período do fluxo é `inicio`/`fim`. Metadados do tráfego preservam todos os recursos, hashes, frequência, estatísticas de EOF e cobertura. `raw_content_size` é o tamanho do manifesto, o mesmo objeto do `raw_content_hash`, que identifica a consulta e a aquisição; os bytes de catálogos e CSVs recebidos em todas as tentativas vão em `source_details["received_bytes"]` ([§57](#57-metainfo-source_url-manifestos-e-cache)). Veja a [referência da API](../api/antt_pedagio.md).

Com `uf`/`rodovia`, praça sem vínculo único no cadastro sai do resultado com aviso (antes a consulta inteira falhava). CSV sem cabeçalho passa a `ParseError`. As funções legadas `parser.parse_trafego`, `parse_trafego_v1`, `parse_trafego_v2`, `join_fluxo_pracas`, `heavy_vehicle_mask`, `client.download_csv` e as constantes `CATEGORIA_MAP`, `EIXOS_TIPO_MAP`, `COLUNAS_FLUXO`, `COLUNAS_V2` e `ANO_INICIO_V2` foram removidas: use `fluxo_pedagio()` ou, para um arquivo local, `parser.parse_trafego_file()`.


**Texto do fluxo e coluna `municipal`.** `sentido` sai em maiúsculas: `Crescente` e `CRESCENTE` viram `CRESCENTE` (idem `DECRESCENTE`), e os rótulos de `fluxo_pedagio` saem sem os espaços externos que a ANTT publica. Quem filtrava ou agrupava pelo texto publicado troca o valor; o volume não muda (no CSV mensal oficial de 2023, o total, o volume por sentido e o enriquecido conferem). Concessionária e praça do cadastro também saem sem espaço externo, e o vínculo com UF, rodovia e município se mantém. Em `pracas_pedagio`, a coluna `municipal` continua nesta versão, como cópia de `municipio`, mas emite `FutureWarning` e aviso no `MetaInfo` e sai na próxima versão major: use `municipio`.

## 25. INCRA: WFS 2.0, 22 colunas e o marcador de data vazia

`incra.quilombolas()` e `incra.quilombolas_geo()` passam a ler a camada em WFS 2.0.0/JSON e devolvem **22 colunas** (contrato `incra_quilombolas` 2.0): as 10 anteriores, na mesma ordem, seguidas de `feature_id`, `regional`, `processo`, `data_publicacao_2`, `responsavel`, `esfera`, `data_cadastro`, `codigo_sipra`, `descricao`, `data_decreto`, `tipo_levantamento` e `escala`. Selecione colunas pelo nome.

- `data_publicacao`, `data_titulo`, `data_publicacao_2` e `data_decreto` saem em `datetime64[ns]`, e `data_cadastro` em `datetime64[ns, UTC]`. O marcador `0001-01-01` da fonte ("sem data") vira `NaT` sem aviso; outra data fora de 1900–2099 (erro de digitação da fonte), inclusive em `data_cadastro`, vira `NaT`, com `UserWarning` e aviso em `meta.validation_warnings`. `incra.vinculos_quilombolas` segue a mesma regra nas colunas `perimetro_*`.
- `codigo` e `familias` são `Int64`; `area_ha` é `float64` em hectares publicados; os demais textos usam o dtype padrão do pandas ([§89](#89-tipos-da-saida)).
- `bbox` usa EPSG:4326, e `quilombolas_geo()` devolve as coordenadas nesse CRS **sem reparo topológico** (o `make_valid` da 1.x saiu).
- O Acervo Fundiário segue a mesma regra: `sigef_geo`, `snci_geo` e `assentamentos_geo` deixam de reparar o polígono com `make_valid` e o entregam como o INCRA publica, possivelmente inválido, com aviso e contagem no `MetaInfo`. Antes de área, interseção ou junção espacial, use `gdf["geometry"] = gdf.geometry.make_valid()`.
- Parâmetros inválidos levantam `InvalidParameterError` antes da rede; o corte por `max_registros` emite `UserWarning`.

Novas funções: `incra.andamento_quilombola()` (quadro "Andamento dos processos", PDF) e `incra.vinculos_quilombolas()` (relação por NUP), ambas com `agrobr[pdf]`. Veja a [página da fonte](../sources/incra.md).

## 26. PPM: `galinhas` no lugar de `galinhas_poedeiras`

A categoria 32793 da tabela 3939 é "Galináceos - galinhas" e, pelas notas técnicas da PPM, inclui poedeiras e matrizeiras. Use `ibge.ppm("galinhas")` ou `datasets.pecuaria_municipal("galinhas")`. `galinhas_poedeiras` continua aceito como alias, com `FutureWarning`, e os números não mudam; a coluna `especie` passa a sair `"galinhas"` nos dois casos. Filtros por `especie == "galinhas_poedeiras"` precisam trocar para `"galinhas"`.

## 27. CONAB: safra passada vem da revisão mais recente

`conab.safras(produto, safra=X)` sem `levantamento` passa a ler X da publicação mais recente que a traz. Para a safra anterior à corrente, é o último levantamento da safra seguinte, que a CONAB revisa (gergelim MT 2024/25: 401,2 mil ha no 12º levantamento de 2024/25, 695 no 12º de 2025/26). Antes vinha o último levantamento da própria safra. Duas ou mais safras atrás da edição mais recente, a revisão só existe na série histórica, e o número passa a vir dela (soja 2022/23: 159.154,3 mil t, contra 154.609,5 no 12º levantamento de 2023/24); nessas linhas, `levantamento` e `data_publicacao` ficam nulos. Safra anterior ao início da série do produto (soja: 1976/77) sai vazia, como em `conab.serie_historica`; antes, levantava `SourceUnavailableError`. `datasets.estimativa_safra`, `datasets.producao_anual` (rota CONAB) e `conab.brasil_total(safra=X)` seguem a mesma regra. Em safra antiga, o `brasil_total` lê cerca de 35 séries, uma por linha de produto, e recalcula os subtotais e o BRASIL. `conab.balanco(safra=X)` também passa a ler a publicação mais recente cuja aba Suprimento traz X (trigo 2024/25: 7.873,4 mil t em set/2026, contra 7.536,1 em set/2025); antes vinha a edição da própria safra. `conab.balanco()` sem `produto` passa a incluir a soja, que vem da aba própria "Suprimento - Soja" e antes ficava de fora. Para o número original, passe `levantamento`, que agora também existe em `balanco` e `brasil_total`: o agrobr avisa quando há publicação mais recente. Nesses casos, `levantamento` e `data_publicacao` são os da publicação usada, e `MetaInfo.source_details["publicacao"]` diz qual foi.

No fallback CONAB de `datasets.producao_anual`, a chamada sem `ano` passa a entregar o ano civil anterior (safra já colhida), e não mais a estimativa da safra em curso rotulada com o ano seguinte.

## 28. ComexStat: aliases pelo produto inteiro e códigos de cada período

Os aliases de `comexstat.exportacao`/`importacao` (e os produtos de `datasets.exportacao`/`importacao`) passam a somar todos os códigos NCM do produto, com os códigos vigentes em cada ano. Antes, vários apontavam para um único código, alguns já extintos, e voltavam vazios ou parciais sem aviso. A tabela completa (entra / não entra) está na [API ComexStat](../api/comexstat.md).

| Alias | Antes | Agora |
|-------|-------|-------|
| `soja`, `soja_grao` | `12019000` (vazio até 2011) | `12019000` + `12010090` |
| `soja_semeadura` | `12011000` (vazio até 2011) | `12011000` + `12010010` |
| `farelo_soja` | `23040010` | `2304` (inclui `23040090`, 78 % das exportações de 2025) |
| `milho` | `10059010` | `1005` |
| `arroz` | `10063021` | `1006` |
| `trigo` | `10019900` (vazio até 2011) | `1001` |
| `algodao` | `520100` | `5201` + `5203` |
| `cafe` | `09011110` | `09011` + `09012` |
| `cafe_arabica`, `cafe_conilon` | `09011110`, `09011190` | removidos: `InvalidParameterError` (a NCM não separa espécie) |
| `acucar` | `17011400` (vazio até 2011) | `1701` |
| `etanol` | `22071000` (exportação vazia ou residual desde 2012) | `2207` |
| `carne_bovina` | `02023000` | `0201` + `0202` |
| `carne_frango` | `02071400` (desde 2024, só o resíduo do código antigo) | `02071` |
| `carne_suina` | `02032900` | `0203` |
| `ureia` | `31021010` | `310210` |
| `kcl` | `31042090` | `310420` |
| `dap` | `31053000` (vazio até 2018) | `310530` |
| `npk` | `3105` (incluía MAP e DAP) | `31052000` |
| `ssp`, `tsp` | vazio antes de 2017 | `InvalidParameterError` antes de 2017 |
| `defensivos`, `agrotoxicos` | `3808` | `3808` sem os 27 códigos de uso exclusivamente domissanitário |

Para reproduzir um recorte antigo, passe o código ou prefixo NCM em `produto` (ex.: `comexstat.exportacao("10059010")` para o milho em grão, `"3105"` para a posição inteira dos adubos compostos). Os datasets consolidam os códigos de cada produto e não devolvem mais a coluna `ncm`. Em `MetaInfo.source_details["query"]`, `ncm_prefixo` (texto) virou `ncm_prefixos` (lista), mais `ncm_excluidos`.

## 29. Acervo Fundiário: SIGEF público e privado, SNCI por UF

`acervo_fundiario.sigef` e `snci` (e as variantes `_geo`) deixam de recusar UFs por uma lista fixa: UF sem arquivo no servidor levanta `SourceUnavailableError` (HTTP 404). As constantes `acervo_fundiario.models.SIGEF_UFS_DISPONIVEIS` e `SNCI_UFS_DISPONIVEIS` foram removidas. O SNCI sai por UF, e a lista muda com o tempo: em 01/10/2026, AC, DF e RR estavam sem arquivo (o de RR existia em 22/09/2026). Com `bbox`, `sigef` e `snci` (tabela) filtram pela geometria em qualquer versão do pyogrio e do GDAL: na 1.1.0, com o pyogrio anterior ao 0.10 (GDAL 3.8), voltavam vazios, sem aviso.

O SIGEF deixa de ler o `Sigef Brasil_{UF}.zip` e lê os 2 arquivos em que o INCRA o publica, `Sigef Público_{UF}.zip` e `Sigef Privado_{UF}.zip`, que particionam a UF. O que muda para quem chama `sigef(uf)` como na 1.1.0:

- as mesmas parcelas saem com a coluna nova `natureza` (`"publico"` ou `"privado"`), primeiro as públicas e depois as privadas, cada parte na ordem do arquivo: quem dependia da ordem do arquivo único precisa ordenar (por `codigo_parcela`, por exemplo);
- `natureza="publico"` ou `"privado"` baixa só aquele arquivo;
- `MetaInfo.source_url` é a URL codificada do 1º arquivo lido (`Sigef%20P%C3%BAblico_GO.zip`); `source_details` passa a `source_details["arquivos"]["publico"|"privado"]` (com `url`, `etag`, `last_modified`, `sha256`…); `attempted_sources` lista os arquivos lidos; `schema_version` é `1.1`;
- o cache passa a `acervo_fundiario/sigef_publico/` e `sigef_privado/`; a pasta `acervo_fundiario/sigef/` da 1.1.0 não é mais lida e pode ser apagada.

## 30. Crédito rural (SICOR): programa e tipo de seguro pela tabela oficial do BCB

`bcb.credito_rural` e o dataset `credito_rural` publicam o nome do programa e filtram por `programa=`/`tipo_seguro=`
com os nomes das tabelas de domínio do BCB (`Programa.csv` e `TipoGarantiaEmpreendimento.csv`): programa = trecho da
descrição oficial antes do primeiro " - "; tipo de seguro = descrição oficial. O filtro não diferencia maiúsculas, então
`programa="Pronaf"` e `programa="Pronamp"` continuam valendo. Mudam os valores publicados e o que cada filtro seleciona:

| Código | Antes | Agora |
|---|---|---|
| programa `0001` | Pronaf | PRONAF |
| programa `0050` | Pronamp | PRONAMP |
| programa `0070` | Funcafe | FUNCAFÉ (PROGRAMA DE DEFESA DA ECONOMIA CAFEEIRA) |
| programa `0100` | Moderfrota | PRLC-BA (PROG RECUP LAVOURA CACAUEIRA BAIANA) ENCERRADO — Moderfrota é `0154` |
| programa `0110` | Inovagro | PRODECER III — Inovagro é `0162` |
| programa `0152` | RenovAgro | PROIRRIGA — RenovAgro é `0222` |
| programa `0156` | Moderagro/Moderfrota | ABC + Programa para a Adaptação à Mudança do Clima e Baixa Emissão de Carbono |
| programa `0200` | Proirriga | PROCERA |
| programa `0999` | Sem programa especifico | FINANCIAMENTO SEM VÍNCULO A PROGRAMA ESPECÍFICO |
| programa `0153`, `0162`, `0222` e demais oficiais | Desconhecido (código) | nome da tabela oficial (MODERAGRO, INOVAGRO, RenovAgro…) |
| tipo de seguro `1` | Proagro | Proagro tradicional |
| tipo de seguro `2` | Sem seguro | Proagro mais |
| tipo de seguro `3` | Seguro privado | Outro seguro |
| tipo de seguro `9` | Nao se aplica | Sem adesão a seguro |
| tipo de seguro `0` | Desconhecido (0) | Não se aplica |

Os códigos `0002`, `0102`, `0104`, `0106`, `0108`, `0112`, `0114` e `0150` não existem na tabela oficial e saíram do
dicionário. Quem filtrava `tipo_seguro="Sem seguro"` recebia contratos com Proagro Mais: use `"Sem adesão a seguro"`.
A tabela completa está em [fontes/BCB](../sources/bcb.md#dimensoes-sicor).

## 31. SGS: `ipa_agricola` no lugar de `ipa_agropecuario`

A série 7460 do SGS é o IPA-DI por origem de **produtos agrícolas** (sem os pecuários), conforme o nome oficial.
O alias passa a ser `ipa_agricola`; `ipa_agropecuario` continua aceito, emite `FutureWarning` e devolve
`nome_serie="ipa_agricola"`. Quem filtra `nome_serie == "ipa_agropecuario"` depois de consultar deve trocar
para `"ipa_agricola"`. O código numérico `7460` não muda.

## 32. Embrapa Solos: 85 colunas e medidas laboratoriais como texto

`embrapa_solos.perfis()` passa a ler o WFS 2.0 em JSON e devolve **85 colunas** (contrato `embrapa_solos_perfis` 3.0): as 19 da 1.x, na mesma ordem, seguidas dos demais atributos publicados, `uf_original` e `feature_id`. Cada linha é um horizonte ou camada (34.464 na camada); `codigo_pon` identifica o ponto de amostragem. `mapa_solos()` ganha `ordem3`, `subordem3`, `gdegrupo3` e `feature_id` (19 colunas).

- As 9 medidas laboratoriais (`areia_total`, `silte`, `argila`, `ph_h2o`, `carbono_organico`, `ctc`, `saturacao_bases`, `aluminio`, `fosforo`) deixam de ser `float`: são o texto publicado, inclusive `NULL`. A 1.x convertia com `errors="coerce"` e transformava em nulo, sem aviso, os `<1`, `<0.5`, `0,19` e as marcas de `fosforo`. Converta na aplicação, por exemplo `pd.to_numeric(df["argila"].replace("NULL", pd.NA))`; em `fosforo`, trate antes os censurados e a vírgula.
- `ano` sai em `Int64` e `data_colet` em `datetime64[ns]`; `NULL` vira ausente nessas 2 colunas, e ano ou data fora do formato, ou data impossível (`2024-02-30`), levanta `ParseError`. Pela [regra de datas](normalizacao.md#datas-das-fontes), `data_colet` de formato válido com ano fora de 1900–2099 vira `NaT`, com `UserWarning` e aviso em `meta.validation_warnings` (em 07/10/2026, 73 dos 34.464 registros da camada, como `0982-11-01` e `1892-07-14`).
- `max_registros` (padrão 50.000; 5.000 perfis e 3.000 polígonos nas funções `_geo`) limita o prefixo lido em ordem de `fid`. `uf` e `ordem` filtram localmente esse prefixo, e o corte que deixa a seleção parcial emite `UserWarning`; `max_registros=None` varre a camada inteira. `tamanho_pagina` define o tamanho da página.
- **Custo:** cada página espera 2 s (o ritmo da fonte no agrobr), e `uf` e `ordem` não reduzem as páginas, porque filtram depois
  da leitura. `perfis()` lê em páginas de 250 (a camada inteira são ~138 páginas, mais de 4 min), e `perfis_geo()` em páginas de
  100 até 5.000 linhas (50 páginas, mais de 1,5 min). Exemplos: 310 s em `perfis(uf="GO")` e 113 s em
  `perfis_geo(uf="DF")`. Suba `tamanho_pagina` (até 1.000; 100 nas `_geo`) ou reduza `max_registros`.
- `bbox` e as funções `_geo` usam EPSG:4326. Veja a [página da fonte](../sources/embrapa_solos.md).

## 33. Crédito rural (SICOR): `produto` e `finalidade` como pedidos, safra validada

`bcb.credito_rural` e o dataset `credito_rural` publicam em `produto` a chave pedida, sem acento e em minúsculas, como
`producao_anual` e `estimativa_safra`; o filtro continua com a grafia oficial do SICOR. `finalidade` sai em minúsculas
também pelo fallback BigQuery, que publicava `CUSTEIO`. Quem filtrava pela grafia antiga deve trocar:

| Pedido | Antes | Agora |
|---|---|---|
| `algodao` ou `algodão` | algodão | algodao |
| `cafe` ou `café` | café | cafe |
| `cana` | cana-de-açucar | cana |
| `feijao` ou `feijão` | feijão | feijao |
| `mandioca` | mandioca (aipim, macaxeira) | mandioca |
| item de investimento, ex.: `CANA-DE-AÇUCAR` | cana-de-açucar | cana-de-acucar |

Soja, milho, arroz, trigo e sorgo não mudam. A safra aceita só `AAAA`, `AAAA/AA` e `AAAA/AAAA` com anos consecutivos;
`"2023/25"`, `"2024/2023"`, `"23/24"` e texto livre levantam `InvalidParameterError` antes da rede (antes eram lidos em
silêncio como outra safra, voltavam vazios ou estouravam `ValueError` no cliente).

## 34. FUNAI: 19 colunas e data de atualização com o dia primeiro

`funai.terras_indigenas()` passa a ler o WFS 2.0 em JSON e devolve **19 colunas** (contrato `funai.terras_indigenas` 2.0): as 9 da 1.x, na mesma ordem, seguidas de `feature_id`, `gid`, `reestudo_ti`, `cr`, `faixa_fronteira`, `undadm_codigo`, `undadm_nome`, `undadm_sigla`, `dominio_uniao` e `epsg`.

- `data_atualizacao` segue em `datetime64[ns]`, agora lida com o dia primeiro, como a FUNAI publica (`dd/mm/aaaa`), e nula em parte das TIs. A 1.x convertia com `pd.to_datetime(errors="coerce")` sem `dayfirst` e trocava dia e mês quando o dia era até 12. Data ilegível vira `NaT`, com aviso em `meta.validation_warnings`.
- `uf` é o texto publicado: TIs em mais de um estado vêm como "AM, RR", e o filtro `uf` casa qualquer uma delas.
- `max_registros` (padrão 10.000; 1.000 nas `_geo`) e `tamanho_pagina` são parâmetros novos. `uf` e `fase` filtram localmente o prefixo lido, e o corte que deixa a seleção parcial emite `UserWarning`. Saída geo e `bbox` em EPSG:4326. Veja a [página da fonte](../sources/funai.md).
- **Custo:** cada página espera 2 s (o ritmo da fonte no agrobr), e `uf` e `fase` não reduzem as páginas. `terras_indigenas()` lê em páginas de 250, e `terras_indigenas_geo()` em páginas de 10 TIs, até 1.000 (até 100 páginas). Exemplo: 141 s em `terras_indigenas_geo(uf="AC")`. Suba `tamanho_pagina` (até 1.000; 100 nas `_geo`).

## 35. ZARC: nove culturas das safras 2017/2018 a 2023/2024

As tábuas anuais de 2017/2018 a 2023/2024 publicam nove rótulos que o catálogo não reconhecia: `zarc.zoneamento` e o
dataset `zoneamento_agricola` recusavam o filtro `cultura=` antes da rede e, sem filtro, publicavam um nome improvisado.
Agora `zarc.culturas()` tem 107 culturas e o filtro aceita o rótulo oficial ou a chave:

| Rótulo oficial | Antes (sem filtro) | Agora |
|---|---|---|
| Milho | milho | milho |
| Arroz Irrigado | arroz_irrigado | arroz_irrigado |
| Feijão 1ª Safra | feijao_1a_safra | feijao_1 |
| Trigo Irrigado | trigo_irrigado | trigo_irrigado |
| Mamona Semi-árido Sequeiro | mamona_semi-arido_sequeiro | mamona_semiarido_sequeiro |
| Cevada Grãos Irrigada | cevada_graos_irrigada | cevada_graos_irrigada |
| Cevada Grãos Sequeiro | cevada_graos_sequeiro | cevada_graos_sequeiro |
| Aveia Sequeiro | aveia_sequeiro | aveia_sequeiro |
| Aveia Irrigada | aveia_irrigada | aveia_irrigada |

Quem filtrava `feijao_1a_safra` ou `mamona_semi-arido_sequeiro` depois de consultar deve trocar pelas chaves novas.
Cultura do catálogo ausente na safra pedida continua levantando `InvalidParameterError`, agora com as safras em que
aparece (ex.: `milho` de 2017/2018 a 2023/2024; `milho_1` de 2024/2025 a 2026/2027). Nos 11 rótulos de cultura que o ZARC
renomeou na safra 2024/2025, o erro também aponta a chave equivalente da tábua consultada; a tabela de pares e a chave
de junção entre safras (`cultura_codigo` e `manejo`) estão na [página da fonte](../sources/zarc.md). Na 2.0, o filtro se
chama `produto` ([§85](#85-nomes-de-parametro-o-mesmo-vocabulario-em-toda-a-api)); a coluna de saída segue `cultura`.

## 36. IBAMA: arquivo vigente, edição no `MetaInfo` e `bbox` da `_geo` pelo polígono

`ibama.embargos()` e `ibama.embargos_geo()` passam a ler o CSV vigente do conjunto "Fiscalização - termo de embargo"
(atualização diária). A 1.x lia um ZIP que a fonte deixou de atualizar em 03/05/2026; quem guardou resultados da 1.x
tem a foto dessa data (em 23/09/2026 faltavam 2.454 termos, 525 desembargos e 237 cancelamentos).

- O download passa de ~47 MB (ZIP) a ~208 MB (CSV sem compressão; a fonte não publica ZIP).
- `meta.source_details["ultima_atualizacao_relatorio"]` traz a edição lida (horário de Brasília).
- `embargos_geo(bbox=...)` filtra pela interseção do polígono com a caixa; antes filtrava pelo ponto de referência do
  termo e devolvia polígonos fora da caixa. `embargos(bbox=...)` continua pelo ponto, e o ponto com latitude e longitude zeradas (não informado na fonte) fica fora do filtro; na 1.1.0, a caixa que contém (0, 0) devolvia esses termos. Veja a [página da fonte](../sources/ibama.md).

## 37. Censo Agropecuário: linha `Total` publicada

`ibge.censo_agro()` e `datasets.censo_agropecuario` passam a publicar a linha `categoria = "Total"` quando a fonte
a publica. A 1.x descartava essa linha em todos os temas. Número de estabelecimentos não soma entre categorias, então o
total oficial não se recompunha: irrigação, Brasília 2017, 2.726 estabelecimentos no Total e 3.224 somando os métodos.

- Para somar categorias, filtre `categoria != "Total"`; para o total oficial, use a linha `Total`.
- Contrato `censo_agropecuario` 1.2 e parser 3 do censo SIDRA atual. Veja o
  [contrato](../contracts/censo_agropecuario.md).

## 38. B3: mês do contrato nas opções e filtro `vencimento`

`b3.posicoes_abertas_historico(..., vencimento="V26")` e `datasets.futuros_agricolas(..., tipo="oi_historico", vencimento=...)` passam a
trazer o futuro **e as opções** do mês do contrato. A 1.x comparava o código bruto e devolvia só o futuro: quem soma
`posicoes_abertas` por vencimento passa a somar também as opções.

- Para manter o comportamento da 1.x, passe `tipo="futuro"` em `b3.posicoes_abertas_historico`; no
  `datasets.futuros_agricolas`, em que `tipo` escolhe a consulta, filtre a coluna `tipo` do resultado
  (`df[df["tipo"] == "futuro"]`). O código publicado de uma opção (MYOA, ex.: `VVJK`) casa só aquela série.
- `vencimento_mes` e `vencimento_ano` das opções deixam de ser nulos. São o mês e o ano **do contrato**, não os da expiração, que
  pode cair no mês anterior: opções de café arábica e conillon e de soja cross e FOB (ex.: `ICFH27C035000` expira em
  12/02/2027). Contrato `b3.posicoes_abertas` 1.1, com os dois campos não nulos.
- Ticker ou `XprtnCd` fora do padrão levanta `ParseError` (na 1.x o valor ficava nulo). Filtro `vencimento` em outro formato
  levanta `InvalidParameterError` antes da rede. Veja a [página da fonte](../sources/b3.md).

## 39. ABIOVE: edição mais recente e `edicao`

`abiove.exportacao(ano, mes=...)` e o fallback ABIOVE de `datasets.exportacao` passam a ler a edição mais recente da
planilha que publica `ano`. Na 1.x, `ano` e `mes` escolhiam o arquivo (`exp_{ano}{mes}.xlsx`, ou a última edição do
próprio ano): o número de um ano passado ficava preso à edição de dezembro, sem as revisões da ABIOVE (em setembro/2026,
19 das 96 células de 2025; farelo, dez/2025: 2.020.365,023 t na 1.x e 1.990.304,323 t na edição corrente), e um mês sem
edição própria (ex.: `ano=2026, mes=1`) levantava `SourceUnavailableError`.

- `mes` só filtra o mês dos dados. Fora de 1-12 levanta `InvalidParameterError` antes da rede; mês ainda não publicado
  devolve DataFrame vazio.
- Para o número original, passe `edicao="AAAA-MM"` (ex.: `edicao="2025-12"`); valem as edições de `ano` e de `ano + 1`.
- `MetaInfo.source_details["edicao"]` traz o arquivo e o mês da edição lida, e `raw_content_hash`, o SHA-256 da
  planilha. Veja a [API](../api/abiove.md).
- `datasets.exportacao` não aceita `mes`. Na 1.1.0, o argumento chegava ao fallback ABIOVE; na 2.0, levanta `TypeError`
  (seção 78). Para um mês, use `abiove.exportacao(ano, mes=...)` ou filtre a coluna `mes` do resultado.
- `abiove.exportacao(ano, produto="total")` levanta `InvalidParameterError`: para o total dos produtos, use
  `agregacao="mensal"` (com ou sem `produto="total"`). Na soma mensal com `produto="grao"` (ou outro), a coluna `produto`
  traz o produto filtrado; na 1.1.0, trazia `"total"`.

## 40. Comtrade: aliases com o mesmo significado da ComexStat

Os aliases de `comtrade.comercio`, `comtrade.trade_mirror` e `datasets.comercio_internacional` passam a somar só os códigos
HS do produto, com o mesmo significado da ComexStat (seção 28). A tabela completa (entra / não entra) está na
[API Comtrade](../api/comtrade.md).

| Alias | Antes | Agora |
|-------|-------|-------|
| `suco_laranja` | `2009` (sucos de todas as frutas: +US$ 357 mi, +11,4 %, em 2025) | `200911`, `200912`, `200919` |
| `carne_frango` | `0207` (aves em geral: +US$ 212 mi, +2,5 %, em 2025) | `020711` a `020714`; antes de 1996, `InvalidParameterError`, e em 1996 no Brasil, que reportou na HS 1992 (H0) |
| `cafe` | `0901` (com cascas e sucedâneos) | `090111`, `090112`, `090121`, `090122` |
| `algodao` | `5201` | `5201` + `5203` |
| `soja` | `1201` (com a semente) | `120190` (desde 2012) + `120100` (até 2011) |
| `complexo_soja` | `1201` + `1507` + `2304` | `soja` + `1507` + `2304` |

Para reproduzir um recorte antigo, passe o código HS em `produto` (ex.: `comtrade.comercio("2009")`). `comtrade.produtos()`
devolve o mapa novo. `celulose` (`4703`) e `tabaco` (`2401`) não mudam; a doc passa a dizer o que fica de fora.

## 41. PSR: `seguradora` na chave das apólices e registro publicado em dobro

`mapa_psr.apolices` e `datasets.seguro_rural(tipo="apolices")` passam a usar o contrato `mapa_psr_apolices` 2.1, com
`seguradora` na chave primária (não nula). Na 1.1.0, o dataset levantava `ContractViolationError` em 2007, 2008, 2009, 2011 e
2012, porque o MAPA publica o mesmo número de apólice em duas seguradoras; agora esses anos saem inteiros. Quem junta apólices
pela chave antiga precisa incluir a `seguradora`.

O registro publicado duas vezes e igual em todas as colunas (1 caso, em 2009) sai uma vez só, com aviso e a contagem em
`source_details["duplicatas_colapsadas"]`. A fonte passa a levantar `ContractViolationError`, como o dataset, se a chave repetir
com valores diferentes.

Colunas novas no fim: `inicio_vigencia`, `fim_vigencia` e `data_apolice` (`datetime64[ns]`). A vigência de 2006 a 2015 sai nula, com aviso: nesses anos o MAPA publica início igual ao fim, ou seja, não publica a vigência. Quem seleciona colunas por posição ou compara o conjunto de colunas inteiro precisa contar com as três.

## 42. PSR: geocódigo publicado como "-" sai nulo

Em `mapa_psr.apolices`, `mapa_psr.sinistros` e `datasets.seguro_rural`, `cd_ibge` sai nulo quando o MAPA publica "-" no
lugar do geocódigo (1.516 apólices entre 2006 e 2025) ou deixa a célula vazia. Na 1.1.0, esses casos saíam como os textos "-"
e "". Filtros e junções por `cd_ibge` precisam tratar o nulo.

Para o município inteiro, passe o código ou o nome em `municipio`: o filtro compara o código, e entram as apólices rotuladas
com o nome do distrito ([§86](#86-municipio-nome-inteiro-ou-codigo-ibge)). Em `mapa_psr.apolices` e
`mapa_psr.sinistros`, `as_polars` e `return_meta` são só por nome ([§88](#88-flags-e-filtros-secundarios-so-por-nome)).

## 43. CONAB: cereais de inverno nas edições de out/2019 a jan/2022

Nessas edições, as abas de trigo, aveia, canola, centeio, cevada e triticale levam o ano no nome ("Trigo 2021"). Na 1.1.0, `conab.safras` (e `datasets.estimativa_safra` com `fonte="conab"`) procurava a aba sem o ano e levantava erro para os seis cereais. Agora a aba é escolhida pelo cabeçalho: vale a mais recente que publica a safra pedida. Quando a edição ainda não publica o inverno da própria safra (levantamentos 1 a 4 de 2019/20, 3 e 4 de 2020/21 e 2 a 4 de 2021/22), a consulta sai vazia, como nos levantamentos de outros anos que não publicam o ano, e não mais com erro.

## 44. Progresso CONAB: a linha "N estados" deixa de sair como `BR`

Em `conab.progresso_safra` e `datasets.progresso_safra` (contrato 2.0), a última linha de cada bloco da planilha, "7 estados",
"12 estados" etc., sai com `uf = "MEDIA_ESTADOS"` (a coluna `estado` passa a `uf`; [§87](#87-colunas-renomeadas-na-saida)). Na 1.1.0 ela saía como `BR`, mas é a média da própria CONAB dos
estados monitorados (de 88% a 99,9% da área, conforme a cultura), não o Brasil, e não se reproduz com as áreas do
levantamento. As colunas novas `n_estados` e `cobertura_area_pct` trazem o número de estados e a cobertura lidos da nota
"(Esses N estados correspondem a X% da área cultivada)", sem recálculo (0,98 = 98%), e são nulas nas linhas de UF.

O filtro `uf="BR"` levanta `InvalidParameterError` com a cobertura publicada quando a planilha não traz a linha "Brasil"
(nenhum boletim conferido traz). Troque por `uf="MEDIA_ESTADOS"` e leve em conta a cobertura. O `parser_version` do
progresso passa de 1 para 3.

## 45. CEPEA: trigo nas duas praças e variação do dia no `meta`

`cepea.indicador("trigo")` sem `praca` traz o Paraná e o Rio Grande do Sul. Na 1.1.0, a coleta no CEPEA trazia só o Paraná
(o fallback do Notícias Agrícolas já trazia as duas). `cepea.ultimo("trigo")` sem `praca` devolve a data mais recente entre as
duas praças e, no empate, o Paraná, porque a ordem é pela praça. Para a série da 1.1.0, passe `praca="parana"`. O
`datasets.preco_diario("trigo")` segue no Paraná e, numa data sem o Paraná, usa o Rio Grande do Sul, rotulado na coluna
`praca`, pelo [desempate dos produtos regionais](../contracts/preco_diario.md).

No `Indicador` do CEPEA, `meta["variacao"]` passa a ser a variação do dia ("Var./Dia"). Na 1.1.0, o parser guardava ali a
última coluna de variação da tabela, que nas páginas diárias é a do mês. A do mês vai em `meta["variacao_mes"]` e a da semana
em `meta["variacao_semana"]`. No etanol, cujas páginas são semanais, a variação da semana deixa `variacao` e sai só em
`variacao_semana`. Como na 1.1.0, as variações vêm só no `Indicador` recém-coletado da fonte: a leitura do cache, morna ou
`offline`, não as traz.

## 46. Custos CONAB: itens pelo subtotal publicado e categoria pela seção

Na 1.1.0, o `custo_producao` somava ao custeio o grupo valorado junto dos subitens, lia como item o cabeçalho de seção com
zeros e o bloco "Gestão da propriedade familiar" depois do H, e classificava a `categoria` pelo rótulo exato, que muda com o
ano. Na 2.0.0 (parser 5, contrato 3.0 mantido):

- os itens de cada seção fecham com o subtotal publicado sempre que a planilha fecha. Vale a leitura que fecha (grupo,
  subitens ou os dois; agregado de gestão ou os componentes dele), e a linha que sai fica em
  `meta.source_details["parser"]["notes"]` com o valor publicado;
- o cabeçalho romano com zeros abre a seção, e a linha depois do subtotal ou do total da seção vira a nota
  `memo_after_total`;
- subtotal ou total de fórmula que não fecha na própria planilha gera aviso (`UserWarning` e `meta.validation_warnings`)
  com os números publicados; nenhuma linha é recalculada;
- `categoria` sai da seção publicada (IV e V: `custos_fixos`; II, III e VI: `outros`) e, no custeio, do rótulo
  normalizado. Mão de obra, agrotóxicos, mudas e máquinas próprias deixam de cair em `outros` conforme a grafia do ano.

Quem somava `valor_ha` dos itens de uma seção passa a obter o subtotal publicado. Quem filtrava `categoria == "outros"`
para achar mão de obra ou agrotóxicos passa a usar `mao_de_obra` e `insumos`. Detalhes e as 116 abas recusadas no
[contrato](../contracts/custo_producao.md).

## 47. IMEA: registro publicado em duplicata sai uma vez só

`imea.cotacoes` passa a colapsar o registro que o IMEA publica mais de uma vez, igual em todas as colunas: ele sai uma vez só,
com aviso e a contagem em `source_details["duplicatas_colapsadas"]` (`linhas` e `indicadores`). Na 1.1.0, as cópias saíam
todas: em 25/09/2026, 69 linhas a mais na soja (`R$/sc`). A chave (indicador, localidade, data, safra e unidade) repetida com
valores diferentes segue saindo inteira, agora com aviso e a contagem em `source_details["chaves_repetidas"]`. As duas chaves
entram no `source_details` de toda consulta.

## 48. MetaInfo: hash, tamanho e localizador do corpo que deu o dado

O `MetaInfo` das fontes passa a identificar o corpo adquirido, pela regra dos
[contratos](../contracts/index.md#metainfo):

- `raw_content_hash` e `raw_content_size` deixam de sair nulo e zero quando a resposta vem de um corpo só: ABIOVE, ANDA,
  INMET, Acervo Fundiário, ANA, ANTT (praças), B3, CFTC, CONAB, DERAL, IBAMA, IBGE, ICMBio (`ucs_geo`), IMEA, PSR,
  Queimadas, SICAR, UNICA e Rio Verde.
- No CEPEA, `raw_content_hash` deixa o formato `sha256:` seguido de 16 dígitos hexadecimais, um prefixo de 64 bits, e passa
  ao SHA-256 completo, com 64 dígitos e sem prefixo. Quem comparava com o formato antigo precisa mudar. O `fetch_timestamp`
  passa a vir preenchido na coleta.
- Na ANEC, `raw_content_hash` deixa de ser o fingerprint da estrutura do PDF (MD5 com 16 dígitos hexadecimais) e passa ao
  SHA-256 completo do PDF, com o tamanho em `raw_content_size`. O fingerprint vai para `source_details["layout_fingerprint"]`.
  Quem comparava com o formato antigo precisa mudar.
- `source_url` passa a ser o recurso que deu o dado:
  - no CFTC, a consulta com os filtros; antes, o endpoint sem filtros;
  - na B3, o download do CSV com o token como `[REDACTED]`. Antes, era o gerador de ticket, que agora fica em
    `source_details["ticket_url"]`;
  - no IBGE, a consulta à SIDRA quando há uma só. Antes, era a página da SIDRA, que agora fica em
    `source_details["pagina"]`. O `fetch_timestamp` do IBGE passa a vir preenchido.

## 49. USDA PSD: gateway novo e rótulos pelos catálogos oficiais

Na 1.1.0, `usda.psd` e o `oferta_demanda_global` consultavam a OpenData antiga da FAS, que responde 500, e rotulavam
errado 6 dos 9 atributos, o farelo de soja e a UE. Na 2.0.0 (parser 2; contrato `oferta_demanda_global` 2.0, seção 87):

- o host é o gateway `https://api.fas.usda.gov/api/psd`, com a chave no cabeçalho `X-Api-Key` (a mesma
  `AGROBR_USDA_API_KEY` do api.data.gov);
- `attribute`, `unit`, `country` e o nome da commodity fora do cadastro vêm dos catálogos oficiais guardados no pacote, e
  `attribute_br` usa os IDs certos:

  | Rótulo | ID na 1.1.0 | ID na 2.0.0 |
  |---|---|---|
  | `producao` | 125 (Domestic Consumption) | 28 (Production) |
  | `estoque_inicial` | 28 (Production) | 20 (Beginning Stocks) |
  | `consumo_domestico` | 57 (Imports) | 125; 126 no açúcar; 142 no algodão |
  | `importacao` | 130 (Feed Dom. Consumption) | 57 (Imports) |
  | `estoque_final` | 84 (TY Imp. from U.S.) | 176 (Ending Stocks) |
  | `oferta_total` | 176 (Ending Stocks) | 86 (Total Supply) |

  Entram também `distribuicao_total` (178) e, no algodão, `perdas` (150);
- `farelo_soja` passa de `4233000` (Oil, Cottonseed) a `0813100` (Meal, Soybean), e `ue`/`eu` de `E2` (EU-15) a `E4`
  (European Union);
- colunas novas: `attribute_id`, `unit_id`, `last_update_year` e `last_update_month`. As duas últimas são a última
  atualização da série, não a edição consultada, e `last_update_month` sai nulo quando o PSD publica `00`;
- commodity, país ou atributo fora dos catálogos levanta `InvalidParameterError` antes da rede. Antes, código de 7
  dígitos ou país de até 3 letras ia direto ao servidor, que responde `[]` sem erro;
- `pivot=True` nomeia cada coluna pelo `attribute_br` ou, sem ele, pelo nome oficial. Rótulo repetido na mesma série
  levanta `ParseError`, e falha no pivot deixa de devolver o formato longo em silêncio;
- em consulta por código direto fora do cadastro, `commodity` passa do próprio código ao nome oficial do catálogo (ex.:
  `0430000` → `Barley`); código fora do catálogo continua voltando igual em `models.commodity_name`;
- nomes públicos de `agrobr.usda`: `models.PSD_COLUMNS_MAP` sai, porque mapeava o PascalCase da OpenData antiga, que o
  gateway não usa; `client.fetch_psd_country`, `fetch_psd_world` e `fetch_psd_all_countries` passam a devolver
  `RespostaPSD(url, corpo, dados, status)` em vez da lista de registros, que agora está em `.dados`.

Quem filtrava por `attribute_br` recebe agora o atributo certo. Quem usava os IDs do `PSD_ATTRIBUTES` ou o código
`4233000` como farelo precisa trocar pelos da tabela. Detalhes na [fonte](../sources/usda.md) e no
[contrato](../contracts/oferta_demanda_global.md). No `datasets.oferta_demanda_global` (contrato 2.0), os filtros e as
colunas saem em português; a fonte `usda.psd` mantém os nomes em inglês
([§85](#85-nomes-de-parametro-o-mesmo-vocabulario-em-toda-a-api), [§87](#87-colunas-renomeadas-na-saida)).

## 50. Limpeza de código morto

A limpeza tira código sem efeito e não muda dado. Mudam erros de entrada inválida, e saem nomes públicos sem uso na produção:

- `utils.validate_bbox`, usado por SFB, Acervo Fundiário, ANA, IBAMA, ICMBio e MapBiomas Alerta, levanta
  `InvalidParameterError` em vez de `ValueError`. Como é subclasse de `ValueError`, quem captura `ValueError` continua
  pegando, e quem captura `AgrobrError` passa a pegar. No IBAMA, área publicada com ponto levanta `ParseError`, em vez de
  "12.5" virar 125.
- Saem nomes públicos sem uso na produção: `utils.concat_csv_pages` (sem substituto);
  `alt.sicar.parser.parse_imoveis_csv` (use `alt.sicar.imoveis`); `anec.models.normalize_produto` (use `resolve_produto`,
  que recusa produto desconhecido); o `bcb.parser.parse_credito_rural` deixa de resolver `fonte_recurso`, `modalidade`
  e `atividade`, que a chamada padrão da 1.1.0 publicava e as agregações `uf` e `programa` da 2.0 não publicam (os
  códigos `cd_*` seguem). Os 3 nomes voltam no `agregacao="registro"` (seção 4), e `resolve_fonte_recurso`,
  `resolve_modalidade`, `resolve_atividade`, `SICOR_FONTES_RECURSO`, `SICOR_MODALIDADES` e `SICOR_ATIVIDADES` ficam em
  `bcb.models`, com os nomes das tabelas oficiais e `None` para código fora da tabela (a 1.1.0 devolvia
  `Desconhecido (<código>)`);
  `defensivos.parser.parse_formulados_csv` e `parse_tecnicos_csv` (use `parse_*_bundle`, que devolve as tabelas e os
  detalhes) e `defensivos.models.FORMULADOS_COLS_DROP`/`TECNICOS_COLS_DROP`; `lista_suja.models.DOWNLOAD_URL` (use
  `constants.URLS[Fonte.LISTA_SUJA]["download"]`), `RENAME_MAP` e `PDF_HEADER_ROW_MARKER`;
  `rnc.client.fetch_registradas`/`fetch_protegidas` (use `fetch_*_bundle`, com `.content` e `.resource.url`) e
  `rnc.parser.parse_registradas_csv`/`parse_protegidas_csv` (use `parse_*_bundle(...).frame`);
  `alt.mapa_psr.client.download_csv` e `fetch_periodo` (use `open_periodo`). Na chamada direta a
  `bcb.bigquery_client.fetch_credito_rural_bigquery`, safra inválida levanta `ValueError` em vez de sumir do filtro.
- Também saem nomes públicos sem uso na produção: `ibge.ftp_client.extract_xls_from_zip` (sem substituto); em
  `desmatamento.parser`, o parser v1 CSV/GeoJSON (`parse_prodes_csv`, `parse_deter_csv`, `parse_deter_geojson` e
  `parse_prodes_geojson`; use `desmatamento.prodes`, `deter`, `prodes_geo` e `deter_geo`), e as 12 constantes de colunas
  do v1 em `desmatamento.models` (`PRODES_COLUNAS_WFS*`, `DETER_COLUNAS_WFS*`, `COLUNAS_SAIDA_PRODES*`,
  `COLUNAS_SAIDA_DETER*`, `PRODES_GEOM_COLUMN` e `MAX_FEATURES_GEO`); `b3.models.parse_numero_br` (use
  `normalize.numeric.safe_float`); `anda.models.ANDA_UFS` (sem substituto); `deral.models.DERAL_PRODUTOS` e
  `normalize_condicao` (as culturas publicadas ficam em `DERAL_PRODUTOS_PUBLICADOS`). `normalize.encoding.ENCODING_CHAIN`
  perde `utf-16` e `ascii`, que nunca rodavam depois do `iso-8859-1`. `anda.entregas` e `datasets.fertilizante` perdem o
  parâmetro `uf`: os PDFs publicados só trazem o total nacional (`uf="BR"`), e passar `uf` levanta `TypeError`. Na
  ABIOVE, `produto` fora da lista levanta `InvalidParameterError` (subclasse de `ValueError`) antes da rede, e
  `agregacao` fora de `"detalhado"` e `"mensal"` levanta `InvalidParameterError` em vez de virar `"detalhado"`.
  Layouts sem publicação deixam de ser lidos: tabela por UF na ANDA, aba por cultura no DERAL e, na ABIOVE, produtos
  em colunas, tabela com cabeçalho nomeado e produto pelo nome da aba (a planilha levanta `ParseError`).

## 51. ANEC: boletins com 4 produtos e coluna sem nome

As edições até a W2/2026 publicam 4 produtos por período (soja, farelo, milho e trigo); DDGS e sorgo entram na W3/2026.
Na 1.1.0, a W1 e a W2/2026 saíam erradas: soja, farelo e milho das duas semanas vinham como `last_week`, o quadro mensal
era lido por posição (em janeiro de 2025, trigo com 6 t e sorgo com o total, 6.614.352 t) e, na W1, o trigo da semana
corrente (144.290 t) e o farelo de São Francisco do Sul (65.000 t) sumiam. Na 2.0, as duas edições saem lidas pelo nome e
fecham com a linha TOTAL do boletim.

Coluna sem nome de produto passa a `ParseError`. Na W14/2026, a primeira coluna da semana corrente tem dados e nenhum
rótulo: a 1.1.0 descartava esses valores, e a 2.0 recusa a edição, porque o agrobr não adivinha o produto.

A soma dos portos de cada coluna semanal é conferida com a linha TOTAL publicada. Divergência acima do arredondamento sai
em `UserWarning` e em `meta.validation_warnings`, sem mudar os valores. Veja a [fonte](../sources/anec.md).

## 52. Datas das fontes: a mesma regra no pandas 2 e no 3

Na 1.1.0, IBAMA, Acervo Fundiário, CFTC, INMET, MapBiomas Alerta e Queimadas convertiam datas com
`pd.to_datetime(errors="coerce")`, e o resultado dependia da versão do pandas: a data fora do intervalo do `datetime64[ns]`
(antes de 1677 ou depois de 2262) virava `NaT` no pandas 2 e saía como publicada no pandas 3. No CSV vigente do IBAMA, os
embargos datados de 1667 e 2925 mudam de valor com o ambiente.

Na 2.0, as 6 fontes seguem uma regra só, nas 2 versões:

- valor ilegível ou com ano fora de 1900–2099 vira `NaT`, inclusive as datas de 1677 a 1899 e de 2100 a 2262, que a 1.1.0
  publicava nas 2 versões;
- no IBAMA, também vira `NaT` a data de embargo ou de desembargo de dia posterior à edição do próprio arquivo
  (`ULTIMA_ATUALIZACAO_RELATORIO`): na edição de 23/09/2026, os termos datados de 2063, 2080 e 2090. A edição é a maior data válida da coluna; sem nenhuma, a consulta avisa e não aplica esta regra;
- a consulta que descarta valores emite `UserWarning` e põe a mesma mensagem em `meta.validation_warnings`, com a fonte, a
  coluna e a quantidade.

**Toda coluna de data das saídas públicas, de fontes e datasets, sai em `datetime64[ns]`** (`Datetime("ns")` no polars). Com
o pandas 3, a 1.1.0 devolvia `s`, `ms`, `us` ou `ns` conforme a fonte e a entrada, e juntar 2 datasets pela data no polars
falhava por unidade.

O INMET descarta a observação sem data e o CFTC recusa a resposta com `ParseError`, como já faziam com data ilegível. O
texto publicado de uma data descartada continua no corpo bruto da fonte. Veja [Normalização](normalizacao.md#datas-das-fontes).

## 53. MetaInfo: `fetch_timestamp` é a hora da aquisição, também no dataset

Na 1.1.0, o `fetch_timestamp` de todo dataset era a hora em que ele montava o `MetaInfo`, e não a da aquisição do dado. No
acerto de cache, o dataset declarava uma aquisição que não houve: o `datasets.preco_diario` lido do DuckDB do CEPEA e o
`datasets.clima` pelo ZIP do INMET saíam com a hora da chamada.

Na 2.0, o `fetch_timestamp` é o horário UTC da aquisição do corpo que o topo do `MetaInfo` descreve, e o dataset repassa o
da fonte (regra nos [contratos](../contracts/index.md#metainfo)):

- corpo recebido agora: a hora dessa aquisição;
- corpo lido do cache (INMET, Acervo Fundiário, ANEC, ZARC e Agrofit): a hora da aquisição original, igual ao `fetched_at`;
  antes, essas fontes publicavam a hora da chamada;
- vários corpos (BCB Focus, PTAX e SGS, Comtrade, PRODES/DETER, Embrapa Solos, FUNAI e INCRA): a aquisição mais recente,
  igual ao `fetched_at`; antes, a hora da montagem;
- registros do cache DuckDB do CEPEA: a coleta original (o `parsed_at` mais recente das linhas devolvidas), igual ao
  `fetched_at`, na fonte e no `datasets.preco_diario`.

Quem media o frescor pelo `fetch_timestamp` do dataset passa a ler a aquisição. A hora da montagem continua em `timestamp`.

O `datasets.preco_diario` não tem mais a fonte `cache` (ver Changed): quando a coleta falha, o `cepea.indicador` lê o cache
DuckDB e devolve `fetched_at` e `fetch_timestamp` com a coleta original dos registros; antes, a fonte `cache` do dataset
publicava a hora da chamada.

## 54. B3: proveniência de `ajustes`, `historico` e `posicoes_abertas_historico`

Na 1.1.0, `b3.ajustes` saía com `raw_content_hash` nulo e `raw_content_size` zero, e `b3.historico`, sem os corpos de cada
dia. Na 2.0:

- `b3.ajustes` e `datasets.futuros_agricolas(tipo="ajustes")`: SHA-256, tamanho e hora da aquisição do `PRyymmdd.zip`
  recebido, no topo. A B3 remonta o ZIP externo a cada pedido, então o hash do topo muda entre 2 downloads do mesmo pregão.
  A identidade estável fica em `source_details["zip_interno"]` e `source_details["xml"]` (nome, SHA-256 e bytes);
- `b3.historico`: `source_details["corpos"]` traz cada dia recebido. Com um dia só, o topo é o desse corpo; com vários, o
  topo fica nulo e zero, e `fetched_at`/`fetch_timestamp` são a aquisição mais recente, pela regra da seção 53.
- `b3.posicoes_abertas_historico`: `source_details["corpos"]` traz cada dia com arquivo, com a URL do download (token como
  `[REDACTED]`) e o `ticket_url`, pela mesma regra do `historico`. Dia sem arquivo não entra.
- Dia sem pregão (feriado, fim de semana ou pregão ainda não publicado): `b3.ajustes` devolve o vazio do contrato, sem
  aviso, com o SHA-256 e o tamanho do ZIP vazio que a B3 responde; no `historico`, o dia entra em
  `coverage["empty_dates"]` e não no aviso de histórico incompleto. Na 1.1.0, levantava `SourceUnavailableError`.

Quem comparava o hash de 2 downloads do mesmo pregão deve comparar o do XML.

## 55. `as_polars`: o tipo de cada coluna vem do contrato

Na 1.1.0, o `as_polars=True` convertia o DataFrame pelo dado. A coluna preenchida com `pd.NA` saía com o tipo `Null` do
polars, e a mesma coluna saía `String` numa consulta e `Null` noutra: o `pl.concat` de `producao_anual("soja")` com
`producao_anual("cafe")` quebrava no `condicao_produto`, e o da `exportacao` pela ComexStat com o do fallback ABIOVE, no
`uf`.

Na 2.0, todos os datasets tipam pelo [contrato](../contracts/index.md#garantias-globais): `int` → `Int64`, `float` → `Float64`,
`str` → `String` e `bool` → `Boolean`, mesmo com a coluna toda nula; a coluna de data toda nula sai `Datetime("ns")`. A coluna
fora do contrato segue o tipo do dado. Por isso o extra exige `polars>=0.20.3`, a primeira versão com o tipo `String`.

## 56. Embrapa Solos: texto com dupla codificação reparado

Na 1.1.0, os perfis saíam com o texto como a Embrapa publica, parte dele com dupla codificação (UTF-8 lido como Latin-1):
"BrasÃ­lia", "SÃ£o Carlos", "AptidÃ£o". Filtro e junção por nome de município saíam vazios.

Na 2.0, o texto que volta inteiro por Latin-1 → UTF-8 e tem a assinatura ("Ã" ou "Â" seguido de um caractere entre U+0080 e
U+00BF) sai reparado ("Brasília", "São Carlos"), com a contagem por coluna em `MetaInfo.validation_warnings`. Texto legítimo
com "Ã" fica como está. Quem já tratava a dupla codificação por conta própria deve tirar esse passo, para não aplicá-lo 2
vezes. O texto com a assinatura que a fonte publica sem volta (cortado no meio de uma sequência UTF-8, com "�" ou "€") fica como publicado e ganha contagem própria no mesmo aviso. O `parser_version` da Embrapa passa a 3.

## 57. MetaInfo: `source_url`, manifestos e cache

- `bcb.focus`: o `source_url` deixa de ser a última página da consulta (vazia, com `$skip`) e passa a ser a consulta, sem
  `$top` e `$skip`. As páginas seguem em `source_details["resources"]`.
- `antt_pedagio.fluxo_pedagio`: `raw_content_size` deixa de somar todos os bytes recebidos e passa ao tamanho do manifesto,
  o mesmo objeto do `raw_content_hash`. O total recebido, com tentativas e falhas, vai em
  `source_details["received_bytes"]`, e o dos CSVs que entraram no dado, em `source_details["data_file_bytes"]`.
- `bcb.sgs` e `bcb.ptax`/`bcb.ptax_moedas`: o `source_url` passa a ser a consulta, e não o último recurso. No SGS, o
  intervalo inteiro pedido (antes, o último bloco de datas); na PTAX, a consulta sem `$top` e `$skip`.
- `conab.custo_producao` e `custo_producao_total`: `raw_content_size` passa ao tamanho do manifesto, como na ANTT. O
  total recebido vai em `source_details["received_bytes"]`, e o da planilha, em `source_details["data_file_bytes"]`.
- `bcb.credito_rural`, `bcb.credito_rural_total` e `datasets.credito_rural`: o `source_url` deixa de ser a entidade OData
  e passa a ser a consulta, com `$filter` e `$select` e sem `$top`; o `raw_content_hash` e o `raw_content_size` passam a
  ser os do manifesto `{query, resources}`, com cada página em `source_details["resources"]`.
- `ibge.*` (PAM, LSPA, PPM, abate, censos, PEVS, leite e PIB agro): o `cache_expires_at` passa a nulo. Na 1.1.0, a PAM, o
  LSPA, a PPM e o abate carimbavam um vencimento sem cache nenhum; cada chamada consulta o IBGE.
- `conab.safras`: o `cache_expires_at` passa a nulo. Na 1.1.0, carimbava 24 h sem cache nenhum; cada chamada baixa a
  publicação da CONAB.

## 58. Comtrade: marcas de estimativa da ONU

`comtrade.comercio` (contrato `comercio_bilateral` 2.1) e `datasets.comercio_internacional` (contrato 3.0) ganham `peso_liquido_estimado`, `peso_bruto_estimado`
e `quantidade_estimada`, flags anuláveis da ONU. `True` diz que a medida publicada é estimada pela ONU, e não declarada pelo
país. Com peso líquido estimado no resultado, `meta.validation_warnings` traz o HS e o período, e `peso_liquido_kg`,
`volume_ton` e o `ratio_peso` do `trade_mirror` usam o valor estimado. O `trade_mirror` lista essas células por perna em
`source_details["peso_estimado"]`. Quem compara com a ComexStat pode filtrar ou marcar essas linhas.

## 59. CONAB: `safras` e `balanco` recusam produto desconhecido antes da rede

`conab.safras` com um produto fora de `CONAB_PRODUTOS` levantava `ParseError` ("Produto não suportado") depois de 4
requisições. Na 2.0, levanta `InvalidParameterError` antes da rede, com a lista dos válidos. Quem capturava `ParseError` para
esse caso passa a capturar `InvalidParameterError`.

`conab.balanco` com um produto fora dos 6 da aba Suprimento (`soja`, `milho`, `arroz`, `feijao`, `trigo` e `algodao`, com ou
sem acento) baixava a planilha e devolvia um quadro vazio. Na 2.0, também levanta `InvalidParameterError` antes da rede.

## 60. `exportacao` e `importacao`: as colunas do contrato, pelas 2 fontes

Na 1.1.0, a `exportacao` saía com colunas diferentes conforme a fonte: pela ComexStat, com o `volume_ton`, fora do contrato;
pelo fallback ABIOVE, também com o `receita_usd_mil` e em outra ordem. No pandas o `concat` passava; no polars, só com
`how="diagonal"`.

Na 2.0, a `exportacao` (contrato 1.1) e a `importacao` (1.2) saem com as colunas do contrato, na ordem dele. O `volume_ton`
(t = `kg_liquido` / 1000) passa a ser coluna opcional do contrato, e o `receita_usd_mil` sai do dataset (é o `valor_fob_usd` em
mil e segue em `agrobr.abiove`).

## 61. ANDA: `agregacao` desconhecida recusada antes da rede

`anda.entregas` com `agregacao` fora de `"detalhado"` e `"mensal"` (por exemplo, `"semanal"`) baixava o PDF e devolvia o
detalhado em silêncio. Na 2.0, levanta `InvalidParameterError` (subclasse de `ValueError`) antes da rede, como a ABIOVE
(seção 50). O dataset `fertilizante` não tem o parâmetro e não muda.

## 62. Queimadas: FRP negativo e foco repetido não derrubam mais o mês

`datasets.queimadas` recusava o mês inteiro com `ContractViolationError` quando o INPE publicava um foco com FRP negativo ou o
mesmo foco 2 vezes (7 dos 20 meses lidos de 2023 a 2026, entre eles ago/2024, o exemplo da doc). Na 2.0, `queimadas.focos` e o
dataset entregam o mês: a cópia igual sai uma vez; a chave repetida que difere só no FRP sai em 1 linha, com o `frp` nulo; a que
difere em outra coluna sai do resultado; e o FRP negativo sai nulo. Cada caso vem com aviso e a contagem em
`meta.validation_warnings` e `source_details`. Quem lia pela fonte o FRP negativo ou as 2 linhas do foco repetido passa a
recebê-los assim. Veja o [contrato](../contracts/queimadas.md#frp-negativo-e-foco-repetido).

## 63. MapBiomas: classe fora da legenda sai nula, com aviso

Na 1.1.0, `mapbiomas.cobertura`, `mapbiomas.transicao` e o `uso_do_solo` estadual devolviam `Classe {id}` quando a
planilha trazia um código fora da legenda conhecida. Na 2.0, o rótulo (`classe`, `classe_de` ou `classe_para`) sai nulo,
o `classe_id` publicado fica, e a consulta emite `UserWarning` e põe a mesma mensagem em `meta.validation_warnings`,
com os códigos. O recorte municipal segue a mesma regra. Por isso os contratos `mapbiomas_cobertura` e
`mapbiomas_transicao` passam a 2.0 (`MAPBIOMAS_COBERTURA_V2` e `MAPBIOMAS_TRANSICAO_V2`): coluna obrigatória que vira
anulável é mudança major. Quem filtrava `classe.str.startswith("Classe ")` passa a filtrar `classe.isna()`.

**Coleção 11 por padrão.** Sem `colecao`, `mapbiomas.cobertura`, `mapbiomas.transicao` e `datasets.uso_do_solo` leem a
coleção 11 (1985–2025), e não mais a 10: a mesma chamada devolve outros números, e entram classes novas, como
"Savana Alagada (beta)". Para o recorte da 1.1.0, passe `colecao=10`.

## 64. CEPEA: sem rede e sem cache, erro em vez de tabela vazia

Na 1.1.0, `cepea.indicador` sem rede e sem nada no cache para o período devolvia a tabela vazia, sem erro nem aviso, e
o `MetaInfo` dizia `from_cache=True`; `cepea.ultimo` levantava `ParseError` (erro de layout). Na 2.0, os 2 levantam
`SourceUnavailableError`, com `attempted_sources` (as fontes tentadas e o cache). Erro de layout da página continua
`ParseError`. Com cache, a resposta sai dele com `StaleDataWarning`, como antes. Com `offline=True` e o cache vazio,
`indicador` segue devolvendo a tabela vazia, agora com `from_cache=False` e `cache_expires_at` nulo. Quem tratava a
tabela vazia como falta de rede passa a capturar `SourceUnavailableError`. `cepea.ultimo(produto, offline=True)` sem dado no cache levanta o mesmo `SourceUnavailableError`, com o motivo "offline sem dado no cache" (antes, `ParseError`).

## 65. IBGE: `uf` com nível sem filtro é recusada

Na 1.1.0, `ibge.pam`, `ibge.ppm`, `ibge.censo_agro`, `ibge.censo_agro_historico`, `ibge.silvicultura`,
`ibge.extracao_vegetal` e o `datasets.producao_anual` com `uf` e `nivel="brasil"` (ou `"regiao"` no histórico)
devolviam o agregado do país ou da região, sem o filtro e sem aviso. Na 2.0, levantam `InvalidParameterError` antes da
rede. Com `uf`, use `nivel="uf"` ou `"municipio"`; para o agregado, tire a `uf`.

## 66. Parâmetro impossível recusado antes da rede

Na 1.1.0, estas consultas devolviam um quadro vazio (ou o dado sem o filtro) sem erro, ou levantavam erro de
indisponibilidade depois da rede. Na 2.0, levantam `InvalidParameterError`:
- `conab.serie_historica` com `ano_inicio > ano_fim` ou UF inexistente, antes da rede;
- `desmatamento.prodes` (e o dataset) com ano posterior ao corrente, antes da rede; com um ano válido sem feição, o
  resultado segue vazio, agora com aviso;
- `conab.ceasa_precos` e o `preco_atacado` com produto ou CEASA fora do que a CONAB/PROHORT publica, com os válidos
  (depois da rede, contra a resposta: produto publicado fora dos 48 de `ceasa_produtos()` filtra);
- `ibge.abate` e `ibge.leite_trimestral` com UF desconhecida (antes, `SourceUnavailableError`), antes da rede;
- `cftc.cot` com `inicio > fim`, `conab.custo_sociobiodiversidade` com ano posterior ao corrente e `antaq.movimentacao`
  com UF inexistente, antes da rede ou da descarga;
- `datasets.futuros_agricolas` com `data` e `tipo="historico"`, ou com `inicio`, `fim` ou `vencimento` e `tipo="ajustes"`/`"posicoes"`;
- `b3.historico` e `futuros_agricolas(tipo="historico")` com o código de uma opção no `vencimento` (ex.: `VVJK`): os ajustes só trazem futuros, e a 1.1.0 devolvia vazio. Use o código do mês do contrato (ex.: `V26`);
- UF inexistente (ex.: `uf="XX"`), antes da rede:
  - `bcb.credito_rural` e `datasets.credito_rural`: a 1.1.0 filtrava a UF depois da consulta e devolvia vazio;
  - `conab.safras` e os datasets `estimativa_safra` e `producao_anual`: vazio pelo filtro local da CONAB (no
    `producao_anual`, a 1.1.0 passava o código inexistente ao IBGE antes);
  - `queimadas.focos` e `comexstat.exportacao`/`importacao`: vazio pelo filtro local, depois da descarga;
  - `datasets.clima`: na 1.1.0, o INMET e a NASA POWER recusavam a UF, e o dataset levantava `SourceUnavailableError`.

Já eram erro na 1.1.0, com `ValueError`, a UF inexistente na ANTT (pedágio), na ANP (diesel), no Desmatamento, na Embrapa
Solos, na FUNAI, no INCRA, na NASA POWER e no `inmet.clima_uf` (este, depois de baixar o catálogo de estações). Na 2.0,
levantam `InvalidParameterError`, que é subclasse de `ValueError`, antes da rede: quem capturava `ValueError` continua
pegando.

Quem tratava o vazio (ou o `SourceUnavailableError` do abate e do leite) como "sem dado" passa a capturar
`InvalidParameterError`.

## 67. USDA: sem `market_year`, o ano anterior até o WASDE de maio

Na 1.1.0, `usda.psd` sem `market_year` e `datasets.oferta_demanda_global` sem o ano (`ano_comercial` na 2.0) pediam o ano-calendário corrente, que o PSD
só publica a partir do WASDE de maio: de janeiro a abril, o resultado vinha vazio, sem aviso. Na 2.0, se o ano corrente
vem vazio, a consulta pede o anterior, e `source_details["market_year"]` diz qual foi usado (`market_year_tentados`
traz os pedidos). Quem precisa de um ano fixo passa o ano (`market_year` na fonte, `ano_comercial` no dataset); com ele, não há recuo.

## 68. CLI: datas do JSON em ISO 8601

Na 1.1.0, `--formato json` saía com as datas em milissegundos desde 1970 (`1790035200000`), o padrão do
`DataFrame.to_json`. Na 2.0, em todos os comandos, as datas saem em ISO 8601 (`"2026-09-22T00:00:00.000"`). Quem lia o
número passa a ler o texto: `pd.to_datetime(...)`, `datetime.fromisoformat(...)` ou, no `jq`, `.data[:10]`.

## 69. MapBiomas Alerta: `max_registros` no lugar de `limit`, ordem por código e `tipo_data`

Na 1.1.0, `alertas(limit=...)` e `alertas_geo(limit=...)` definiam o tamanho da página, e a consulta parava calada em 50
páginas (5.000 alertas no padrão). A ordem padrão da API repetia e perdia alertas entre as páginas, e o `bbox` ia na ordem
errada e voltava vazio. Na 2.0:
- `limit` sai. `max_registros` (padrão 5000) é o teto de linhas, com aviso quando corta, e `max_registros=None` traz a
  coleção inteira. `limit=` levanta `TypeError`;
- a paginação vai em ordem de código e é conferida contra o `totalCount`: código repetido ou coleção incompleta levantam
  `ParseError`;
- o `bbox` passa a filtrar (antes, toda consulta com caixa voltava vazia);
- `tipo_data="publicacao"` filtra pela data de publicação. O padrão segue a detecção, com aviso quando o período termina a
  menos de 293 dias de hoje;
- data invertida ou fora de `AAAA-MM-DD`/`DD/MM/AAAA` levanta `InvalidParameterError` antes da rede.

Quem passava `limit=100` para ter 100 alertas por página tira o argumento (ou usa `max_registros=None` para a coleção inteira).

## 70. CEPEA: o período antigo vem da série histórica

Na 1.1.0, `cepea.indicador` e `datasets.preco_diario` só tinham a janela recente da página (cerca de 15 pregões) e o que o
cache tinha acumulado: um período anterior vinha vazio ou incompleto, sem aviso. Na 2.0, esse período vem da série
histórica do CEPEA. A primeira consulta que precisa dela baixa a série inteira do produto (uma planilha de até ~0,6 MB por
indicador) e grava no cache; o padrão sem `inicio` (365 dias) também passa por ela. Série indisponível avisa em
`validation_warnings`, e período que fica sem dado levanta `SourceUnavailableError`. Para usar só o cache, passe
`offline=True`. A laranja não tem série.

## 71. Modo determinístico: aviso onde o modo não se aplica

Na 1.1.0, todo dataset aceitava `datasets.deterministic(...)`, mas só o `preco_diario` o honrava (cache local, sem rede,
corte na data). Os que vão à rede consultavam a fonte corrente calados, com `meta.snapshot` preenchido, como se o dado
fosse o da data. Na 2.0, esses datasets avisam em `validation_warnings` e com `UserWarning` que o dado é o corrente. Seis
deles passam a recusar o contexto com `InvalidParameterError`, antes da rede, porque a fonte não tem como devolver o dado
como estava na data: `cadastro_rural`, `desmatamento`, `exportacao`, `importacao`, `uso_do_solo` e `zoneamento_agricola`.
Na 1.1.0, eles devolviam o dado corrente: tire essas chamadas do bloco `deterministic(...)`. O `preco_diario` determinístico sem o produto no cache local
levanta `SourceUnavailableError`, em vez de devolver vazio: popule o cache (ou copie `~/.agrobr/cache/`) antes de
reproduzir noutra máquina. Um período sem dado, com o produto no cache, segue vazio.

## 72. Censo de 1995: `informantes` nas lavouras

Em `ibge.censo_agro` e `datasets.censo_agropecuario`, a variável 151 das lavouras temporárias e permanentes de 1995
(tabelas 492 e 504 da SIDRA) sai como `informantes`, e não mais como `estabelecimentos`: a SIDRA a chama de "Número de
informantes". Em 2017, `estabelecimentos` segue vindo do número de estabelecimentos agropecuários (variáveis 10084 e
9504). Quem filtrava `variavel == "estabelecimentos"` nas lavouras de 1995 passa a filtrar `"informantes"`.

## 73. Logs fora da saída padrão

Na 1.1.0, usado como biblioteca, o agrobr imprimia os logs do structlog (debug e info) na saída padrão, e o
`logging.basicConfig` não os controlava: `python exporta.py > dados.csv` saía com log no topo do CSV. Na 2.0, os logs do agrobr
passam pelo `logging` da biblioteca padrão, em JSON, no logger do módulo que os emite (`agrobr.cepea.parsers.v1`, por exemplo).
Sem configuração, só avisos e erros saem, na saída de erro; `logging.basicConfig(level=logging.DEBUG)` liga todos, e
`logging.getLogger("agrobr")` controla só o agrobr. O `import agrobr` não configura o structlog nem o `logging`.

**Aplicação que usa o structlog.** A configuração do structlog da aplicação, feita antes ou depois do import, vale só para os
logs dela; os do agrobr seguem no `logging`, em JSON. Na 1.1.0, ela valia também para os do agrobr: para vê-los, configure o
`logging` (`logging.basicConfig` ou um handler no logger `"agrobr"`).

**CLI.** `--verbose` mostra os logs INFO na saída de erro; sem ele, só avisos e erros. Os logs da CLI saem legíveis, no
formato de console do structlog, sem cor e um por linha, como na 1.1.0; o JSON vale para o uso como biblioteca.

## 74. Datasets: colunas na ordem do contrato e data em `datetime64`

Na 1.1.0, 8 datasets (`balanco`, `censo_agropecuario_legado`, `comparacao_anual_anec`, `pib_agro`, `producao_anual`,
`queimadas`, `serie_historica_safra` e `clima`) devolviam as colunas na ordem da fonte quando havia dado, e na ordem do
contrato quando vinham vazios. Na 2.0, todo dataset devolve as colunas do contrato na ordem dele, com as colunas extras
ao fim, na ordem da fonte, e o `MetaInfo.columns` acompanha. Quem lia por posição (`df.iloc[:, 3]`, `df.columns[0]`) passa
a ler pelo nome. A coluna `data` do `queimadas` (e das fontes `queimadas.focos` e `focos_geo`) sai em `datetime64[ns]`
(`Datetime("ns")` no polars), e não mais como `datetime.date` numa coluna `object`: `df["data"].dt.date` devolve a data
de antes.

## 75. Status HTTP de erro: `SourceUnavailableError`, não `httpx.HTTPStatusError`

Na 1.1.0, um 403 ou um 404 da fonte saía em várias fontes como `httpx.HTTPStatusError`, cru, e em outras como
`SourceUnavailableError`. Na 2.0, todo status HTTP de erro sai como `SourceUnavailableError`, com a fonte e o status
na mensagem: "HTTP 403" ou "HTTP 404", na maioria das fontes seguido do motivo
("HTTP 403: a fonte recusou o pedido (bloqueio de WAF ou permissão)", "HTTP 404: o recurso não existe na URL").
Quem capturava `httpx.HTTPStatusError` destas fontes passa a capturar `SourceUnavailableError` e a ler o status na
mensagem (`"HTTP 404" in str(erro)`): ABIOVE, ANDA, ANEC, ANP (diesel), ANTT (pedágio), B3, BCB (SGS, PTAX e Focus), CFTC, ComexStat, Defensivos, DERAL, Desmatamento, Embrapa Solos, FUNAI, IBAMA, IBGE (Censo legado, pelo FTP), ICMBio, IMEA, INCRA, INMET (arquivo histórico e API), Lista Suja, MapBiomas, MAPA PSR, NASA POWER, Queimadas, RNC, SFB, SICAR, UNICA, USDA e ZARC. Na 1.1.0,
elas levantavam `httpx.HTTPStatusError` no 403, no 404 ou nos dois. As consultas da SIDRA (IBGE) levantavam `ValueError`
com o corpo da página de erro, e agora levantam o mesmo `SourceUnavailableError`.

- **USDA, 404 do PSD:** o gateway responde 404 para um ano sem dado (soja do Brasil em 1950, por exemplo). A tabela vazia
  segue, mas agora com aviso em `validation_warnings` e `UserWarning`, porque o mesmo 404 sai se a URL da API mudar.
- **Datasets:** o `SourceUnavailableError` final traz `attempted_sources` com as fontes tentadas, na ordem, e `__cause__`
  com o erro da última. Em `errors`, uma fonte que falhou por status HTTP passa a ter o tipo `"unavailable"` (na 1.1.0,
  `"network"`).
- **`NetworkError`** segue exportado, mas nada o levanta na 2.0.

## 76. Crédito rural: a safra sai "AAAA/AA"

Na 1.1.0, o `bcb.credito_rural` e o `datasets.credito_rural` publicavam a safra como "2023/2024"; o novo
`bcb.credito_rural_total` nasceu no mesmo formato. Na 2.0, os 3 publicam "2023/24", o formato dos outros datasets
(`estimativa_safra`, `balanco`, `progresso_safra`, `serie_historica_safra`), e o `merge` pela `safra` volta a casar. A entrada
continua aceitando "2023/24", "2023/2024" e "2024". Para converter o que foi gravado com a 1.1.0:
`df["safra"] = df["safra"].map(agrobr.normalize.dates.normalizar_safra)`.

## 77. Entrada hostil: teto de expansão, `semana_url` e nome de snapshot

- **Teto de expansão:** ZIP e XLSX que expandem além do teto da fonte levantam `ResourceLimitError` antes de descomprimir. Os
  tetos ficam acima do maior arquivo que cada fonte publica (a tabela está em
  [Resiliência](../advanced/resilience.md#teto-de-expansao-zip-e-xlsx)); o dado legítimo não muda.
- **`semana_url`:** `conab.progresso_safra` e `datasets.progresso_safra` só aceitam páginas em `https://www.gov.br/conab/`.
  Outra URL (inclusive `http://` ou outra porta) levanta `InvalidParameterError`, antes do pedido.
- **Nome de snapshot:** os nomes reservados do Windows (`CON`, `PRN`, `AUX`, `NUL`, `COM1`–`COM9` e `LPT1`–`LPT9`, com ou sem
  extensão, em qualquer caixa) e os terminados em ponto levantam `ValueError` em todo sistema, para o snapshot abrir no Windows.

## 78. Argumento fora da assinatura levanta `TypeError`

Na 1.1.0, as 110 funções públicas abaixo aceitavam `**kwargs`: argumento desconhecido ou com nome errado era descartado em silêncio,
ou repassado ao fetcher, que lia só as chaves que conhecia. O recorte saía mais largo que o pedido, sem erro:
`ibama.embargos(municipio="X")` devolvia o Brasil inteiro, e `datasets.progresso_safra("soja", uf="MT")` também. Na 2.0, 77
delas passam à assinatura explícita, e o argumento fora dela levanta `TypeError` antes da rede; as outras 33 mantêm o
`**kwargs` e recusam o nome desconhecido.

- **Datasets (32):** `abate_trimestral`, `balanco`, `cadastro_rural`, `censo_agropecuario`, `censo_agropecuario_historico`, `censo_agropecuario_legado`, `censo_agropecuario_municipal_1985`, `clima`, `comercio_internacional`, `condicao_lavouras`, `credito_rural`, `embarques_anec`, `estimativa_safra`, `exportacao`, `extrativismo_vegetal`, `fertilizante`, `futuros_agricolas`, `importacao`, `leite_industrial`, `movimentacao_portuaria`, `oferta_demanda_global`, `pecuaria_municipal`, `pib_agro`, `posicionamento_fundos`, `preco_atacado`, `producao_anual`, `progresso_safra`, `queimadas`, `seguro_rural`, `serie_historica_safra`, `silvicultura`, `uso_do_solo`.
- **Funções de fonte (45):** `abiove.exportacao`, `acervo_fundiario.assentamentos`, `acervo_fundiario.assentamentos_geo`, `acervo_fundiario.sigef`, `acervo_fundiario.sigef_geo`, `acervo_fundiario.snci`, `acervo_fundiario.snci_geo`, `alt.sicar.imoveis`, `alt.sicar.imoveis_geo`, `alt.sicar.resumo`, `ana.demanda_irrigacao`, `ana.demanda_irrigacao_geo`, `ana.disponibilidade_hidrica`, `ana.disponibilidade_hidrica_geo`, `ana.hidrografia`, `ana.hidrografia_geo`, `ana.pivos_irrigacao`, `ana.pivos_irrigacao_geo`, `anda.entregas`, `b3.ajustes`, `b3.historico`, `b3.posicoes_abertas`, `b3.posicoes_abertas_historico`, `conab.ceasa_precos`, `conab.progresso_safra`, `deral.condicao_lavouras`, `ibama.embargos`, `ibama.embargos_geo`, `icmbio.ucs_geo`, `imea.cotacoes`, `inmet.clima_uf`, `inmet.estacao`, `inmet.historico`, `mapbiomas_alerta.alertas`, `mapbiomas_alerta.alertas_geo`, `queimadas.focos`, `queimadas.focos_geo`, `rio_verde.ensaio_soja`, `sfb.cnfp`, `sfb.cnfp_geo`, `sfb.concessoes`, `sfb.concessoes_geo`, `sfb.ifn_conglomerados`, `sfb.ifn_conglomerados_geo`, `usda.psd`.
- **Funções que mantêm `**kwargs` (33):** na 1.1.0, também descartavam o nome desconhecido; na 2.0, recusam antes da rede. Levantam `TypeError`: `anec.comparacao_anual`, `anec.destinos`, `anec.embarques`, `anec.embarques_mensais`, `desmatamento.prodes`, `prodes_geo`, `deter`, `deter_geo`, `embrapa_solos.perfis`, `perfis_geo`, `mapa_solos`, `mapa_solos_geo`, `funai.terras_indigenas`, `terras_indigenas_geo`, `icmbio.ucs`, `incra.quilombolas`, `quilombolas_geo` e os datasets `custo_producao`, `desmatamento` e `zoneamento_agricola`. Levantam `InvalidParameterError`: `comtrade.comercio`, `comtrade.trade_mirror`, `defensivos.formulados`, `autorizacoes`, `tecnicos`, `lista_suja.empregadores`, `mapbiomas.cobertura`, `mapbiomas.transicao`, `nasa_power.clima_ponto`, `clima_uf`, `rnc.registradas`, `protegidas` e `zarc.zoneamento`.
- **Argumentos que só chegavam ao fallback:** `datasets.exportacao(mes=...)` ia à ABIOVE (seção 39), e
  `datasets.producao_anual(safra=...)`, à CONAB. Na 2.0, os 2 levantam `TypeError`. Use `abiove.exportacao(ano, mes=...)`
  ou filtre o `mes` do resultado, e `conab.safras(produto, safra=...)` ou o `ano` do `producao_anual`.

O que fazer: tire o argumento ou use o nome da assinatura (`help(função)` mostra os aceitos). O `CHANGELOG` lista as mudanças
por fonte.

## 79. ZARC: contrato 2.1, sem chave primária

O contrato `zarc.zoneamento`, do dataset `zoneamento_agricola`, passa de 1.0 a 2.1. O 1.0 declarava a chave
`[cultura, safra, geocodigo, solo_codigo, ciclo_codigo]`; o 2.1 não declara chave: a fonte publica mais de uma linha para a
mesma combinação (duplicatas literais e riscos distintos), e o agrobr preserva cada ocorrência publicada.

O que fazer: quem deduplicava pelas 5 colunas descarta linhas que a fonte publica. Para identificar a linha, use
`registro_origem` (a posição no CSV) junto com `meta.raw_content_hash` (o SHA-256 do corpo): a posição só vale dentro do
mesmo corpo e não identifica a observação entre revisões. Veja o [contrato](../contracts/zoneamento_agricola.md).

## 80. Corpo fora do formato: `ParseError`, não `SourceUnavailableError`

Com uma resposta HTTP 200 em outro formato (por exemplo, um JSON `{"value": []}` onde a fonte publica outra estrutura),
o SGS, a ComexStat, o ZARC e o Desmatamento levantavam `SourceUnavailableError` na 1.1.0. Na 2.0, levantam `ParseError`. No
Desmatamento, o corpo vazio também passa a `ParseError`.

O que fazer: nada, para quem captura `AgrobrError`, a base das 2. Quem capturava só `SourceUnavailableError` para tratar
esse caso passa a capturar também `ParseError`.

## 81. Censo municipal 1985: casa a casa, com o `status` de cada uma (contrato 2.0)

Na 1.1.0, o `ibge.censo_agro_municipal_1985` lia 53 CSVs extraídos por OCR de 22 UFs. Esses CSVs tinham temas
trocados, moeda errada, colunas perdidas e níveis confundidos, e a 2.0.0 refez a extração inteira a partir dos 28 PDFs
do IBGE.

**A API** tem a mesma assinatura (`tema`, e `uf`, `nivel`, `as_polars` e `return_meta` só por nome), mas o que volta muda:

- **O nível estadual passa de `total` a `uf`**, no argumento e na coluna `nivel`. `nivel="total"` devolvia as linhas da UF e agora levanta `InvalidParameterError`; `nivel="uf"` levantava `ValueError` e agora devolve essas linhas. Troque
  `"total"` por `"uf"` no argumento e nos filtros da coluna `nivel`.
- **1 linha por casa do PDF**, com a chave `(volume, tabela, pagina_pdf, linha, coluna)`. Não há mais 1 linha por localidade e
  variável. Pivote pela `coluna` (ou pelo `coluna_nome`) se precisar do formato largo.
- **`valor` só vem na casa confirmada pelas somas impressas.** `valor_lido` traz a leitura sempre, e o `status` diz o nível de
  confiança. Quem usava todos os valores da 1.1.0 escolhe o `status` aceito: `df[df["valor"].notna()]` (só as confirmadas) ou
  `df[df["status"].isin([...])]`.
- **O tema segue o título impresso**, igual em todos os volumes. 19 nomes mudaram, porque o mapa antigo estava deslocado (por
  exemplo, a 91 é `depositos_producao`, e não `meios_transporte`). Consulte `temas_censo_agro_municipal_1985()`.
- **27 UFs** (eram 22): entram MA, PI, CE, RN e TO.
- **Recusas:** tema, UF ou nível inválidos levantam `InvalidParameterError`, e a UF cujo volume não traz a tabela também, com as
  UFs que a têm. A tabela que está no volume, mas de que a extração não leu nenhuma casa (AM 80, AP 80, RR 80 e RR 119), levanta
  `ParseError`. Na 1.1.0, tema, UF e nível inválidos e a UF sem a tabela levantavam `ValueError`. Das 4 tabelas que hoje
  levantam `ParseError`, AM 80, AP 80 e RR 80 davam `ValueError`, e a RR 119 devolvia 36 linhas.

**O contrato vai de 1.0 a 2.0**:

| 1.1.0 (contrato 1.0) | 2.0 (contrato 2.0) |
|---|---|
| `ano`, `uf`, `nivel`, `tema`, `localidade` | iguais; `localidade` é o nome como lido, com o ruído da leitura |
| `uf_cod`, `localidade_cod` | saem: os municípios de 1985 não correspondem 1:1 aos códigos atuais |
| `categoria`, `variavel` | `coluna_nome` (só confirmado), `coluna_nome_lido`, `coluna_nome_status` e `variavel` |
| `valor` | `valor` (só confirmado) e `valor_lido` |
| `unidade` | `unidade` (só com a folha confirmada) e `unidade_lida` |
| `confianca` | `status` da casa, com a precisão medida no contrato |
| `fonte` | sai; a proveniência está no `MetaInfo` (o PDF do IBGE e o SHA-256 dele) |
| — | entram `volume`, `tabela`, `pagina_pdf`, `pagina_impressa`, `linha`, `coluna`, `marcador` e `reparado` |

O contrato 1.0 segue em `agrobr.contracts.ibge.IBGE_CENSO_AGRO_MUNICIPAL_V1`, o mesmo import da 1.1.0, agora com
`DeprecationWarning`.

**Quem lia os CSVs do pacote direto** (`agrobr/data/censo_1985/tab_067.csv` a `tab_119.csv` e o `_index.csv`): eles saem. O
pacote passa a ser um Parquet com 1 linha por casa, a `cobertura.parquet` e o `manifesto.json`, que são internos: leia pela API
ou pelo dataset `censo_agropecuario_municipal_1985`.

Veja o [contrato 2.0](../contracts/censo_agropecuario_municipal_1985.md).

## 82. CLI: `--formato` inválido sai com erro, e o CSV do Windows sai em UTF-8

Na 1.1.0, `-o`/`--formato` com um valor fora da lista (`-o xml`) saía com código 0 e a tabela, e `health --output` fazia o
mesmo com o texto. Na 2.0, os 8 comandos de dados (os 7 da 1.1.0 e `conab levantamentos`, que ganha `--formato`) aceitam só `table`, `csv` e `json` em `--formato`, e `health`, `doctor` e `snapshot list`, só
`text` e `json` ([§92](#92-cli-formato-em-todos-os-comandos-e-snapshot-use-retirado)): outro valor sai com código 2, a mensagem na saída de erro e a saída padrão vazia. Quem testava o código de
saída de um script passa a ver o erro.

No Windows, o CSV da 1.1.0 saía com `\r\r\n` em cada linha e, redirecionado para arquivo ou pipe, em cp1252: o
`pandas.read_csv` padrão falhava em "Paranaguá", e o `csv.reader` via linhas vazias intercaladas. Na 2.0, a saída padrão da
CLI sai em UTF-8, e o CSV com uma quebra por linha (`\r\n` no Windows, `\n` no Linux e no macOS). No Linux e no macOS, os
bytes do CSV não mudam.

## 83. Cache: a política só do CEPEA, e sai o `load_baseline_fingerprint`

Na 1.1.0, o `agrobr.cache.get_policy` tinha 21 políticas (CEPEA, IBGE, CONAB, BCB, ComexStat, INMET, ANDA, NASA POWER e
Notícias Agrícolas) e devolvia a do CEPEA, calada, para fonte fora da tabela ou texto qualquer. Só o cache de indicadores
do CEPEA vence por ela; as outras fontes não têm esse cache (ver a seção 57). Na 2.0, o `POLICIES` e o `SOURCE_POLICY_MAP`
ficam só com o CEPEA (`cepea_diario`), e o `get_policy`, o `calculate_expiry` e o `get_next_update_info` de outra fonte
levantam `InvalidParameterError` ("não tem cache no agrobr"), com as fontes que têm. O `endpoint="semanal"` do CEPEA passa
à política diária, a que o cache usa para todos os produtos. O `agrobr doctor` mostra a expiração só do CEPEA, no texto e
no `cache_expiry` do `--formato json`; antes, 1 linha por fonte, com TTL de fonte sem cache.

O `load_baseline_fingerprint` e o `save_baseline_fingerprint` saem do `agrobr.cepea.parsers`, sem uso no agrobr. Para
gravar e ler o `Fingerprint` em JSON: `fingerprint.model_dump(mode="json")` e `Fingerprint.model_validate(dados)`
(`agrobr.models`).

## 84. Desmatamento: paginação, corte e custo da chamada padrão

Na 1.1.0, `desmatamento.prodes`, `deter`, `prodes_geo` e `deter_geo` faziam 1 requisição WFS com até 50.000 feições
(10.000 nas `_geo`). Na 2.0, o agrobr pagina:

- `tamanho_pagina`: 500 feições por página (100 em `prodes_geo` e `deter_geo`), até 2.000 (500 nas `_geo`);
- `max_registros`: 50.000 (10.000 nas `_geo`); `max_registros=None` lê a seleção inteira;
- cada requisição espera 2 s (o ritmo do TerraBrasilis no agrobr). Sem filtro, a chamada padrão faz até 100 páginas, mais de
  3 min só de espera. Exemplos: 497 s em `prodes(bioma="Amazônia")`, 187 s em `prodes_geo` do Cerrado, MT, 2023, e
  mais de 600 s em `deter_geo` da Amazônia, no PA. Com `tamanho_pagina=2000`, o PRODES da
  Amazônia caiu para 142 s.

**O corte.** Quando a seleção passa de `max_registros`, o agrobr lê o prefixo em ordem crescente de `fid` (PRODES) ou de
`gid` (DETER), e não por data, e emite `UserWarning` ("retornadas N de M ocorrências por limite local"). Os 50.000 da chamada
padrão são uma parte da camada: em 27/09/2026, eram 802.281 feições no PRODES da Amazônia e 460.092 no DETER.

**A receita.** `ano` (PRODES), `uf`, `inicio`, `fim` e `classe` (DETER) vão no filtro CQL do servidor e reduzem as
páginas: filtre antes de subir o limite. Suba `tamanho_pagina` até o máximo e use `max_registros=None` só com filtro. Veja a
[página da fonte](../sources/desmatamento.md).

## 85. Nomes de parâmetro: o mesmo vocabulário em toda a API

Na 2.0, o mesmo conceito tem o mesmo nome em todas as funções: `inicio`/`fim` para período por data, `ano_inicio`/`ano_fim`
para período por ano, `uf` para a unidade da federação, `produto` para o produto e `municipio` para o município (seção 86).
Os nomes antigos levantam `TypeError`, com o nome recusado também em `desmatamento.*`, `datasets.desmatamento` e
`datasets.zoneamento_agricola`, que aceitam `**kwargs`. Em `zarc.zoneamento`, `mapbiomas.cobertura` e
`mapbiomas.transicao`, levantam `InvalidParameterError` com o nome recusado. Por posição, nada muda, salvo onde a seção 88 diz.

| Função | 1.1.0 | 2.0 |
|---|---|---|
| `bcb.sgs`, `bcb.ptax` | `data_inicial`, `data_final` | `inicio`, `fim` |
| `bcb.focus` | `data_inicial` | `inicio` |
| `cftc.cot` | `commodity`, `start`, `end` | `produto`, `inicio`, `fim` |
| `datasets.posicionamento_fundos` | `start`, `end`, `combined` | `inicio`, `fim`, `combinado` |
| `desmatamento.deter`, `deter_geo`, `datasets.desmatamento` | `data_inicio`, `data_fim` | `inicio`, `fim` |
| `mapbiomas_alerta.alertas`, `alertas_geo` | `start_date`, `end_date` | `inicio`, `fim` |
| `alt.antt_pedagio.fluxo_pedagio` | `data_inicio`, `data_fim` (versões de desenvolvimento) | `inicio`, `fim` |
| `conab.serie_historica`, `datasets.serie_historica_safra` | `inicio`, `fim` (anos) | `ano_inicio`, `ano_fim` |
| `normalize.lista_safras` | `inicio`, `fim` (safras) | `safra_inicio`, `safra_fim` |
| `conab.progresso_safra` | `cultura`, `estado` | `produto`, `uf` |
| `datasets.progresso_safra` | `estado` | `uf` |
| `mapbiomas.cobertura`, `mapbiomas.transicao`, `datasets.uso_do_solo` | `estado` | `uf` |
| `conab.custo_producao`, `custo_producao_total` | `cultura` | `produto` |
| `usda.psd` | `commodity` | `produto` |
| `alt.mapa_psr.apolices`, `sinistros` | `cultura` | `produto` |
| `zarc.zoneamento`, `datasets.zoneamento_agricola` | `cultura` | `produto` |
| `alt.sicar.imoveis`, `imoveis_geo`, `imoveis_geo_stream`, `resumo` | `cod_municipio` | `municipio` |
| `alt.sicar.imoveis_geo` | `max_features` | `max_registros` |
| `ana.hidrografia`, `pivos_irrigacao`, `demanda_irrigacao`, `disponibilidade_hidrica` e os 4 `_geo` | `max_features` | `max_registros` |
| `datasets.oferta_demanda_global` | `country`, `market_year`, `attributes`, `pivot` | `pais`, `ano_comercial`, `atributos`, `pivotar` |
| `datasets.comercio_internacional` | `reporter`, `partner`, `freq` | `declarante`, `parceiro`, `frequencia` |

No BCB, na B3, no CEPEA (`cepea.indicador`, `datasets.preco_diario` e a CLI), no CFTC, no Desmatamento, no MapBiomas
Alerta, no fluxo da ANTT e nos datasets deles, `inicio` e `fim` aceitam `date`, `datetime` (a hora é descartada) e texto
`AAAA-MM-DD` ou `DD/MM/AAAA` (`"01/02/2024"` é 1º de fevereiro). `alt.anp_diesel.*` e `datasets.precos_diesel` aceitam
`date` e `datetime`, mas, para texto, exigem `AAAA-MM-DD`. `inmet.estacao`, `inmet.historico_periodo`, `datasets.clima`
no modo estação e `nasa_power.clima_ponto` aceitam `date` ou texto `AAAA-MM-DD` e recusam `datetime`. Nessas fontes ANP,
INMET e NASA POWER, texto `DD/MM/AAAA` levanta `InvalidParameterError`. No CEPEA, o mês e o dia do texto têm dois dígitos:
`"2024-1-5"`, que a 1.1.0 aceitava, passa a levantar `InvalidParameterError`; use `"2024-01-05"` ou `"05/01/2024"`.
Antes, cada fonte aceitava um formato. O filtro `ano_inicio`/`ano_fim` da série histórica recusa tipo inválido e intervalo
invertido antes da rede.

Ficam os nomes que são termo técnico da fonte ou que não têm par na 2.0: na fonte Comtrade, `reporter`, `partner`, `freq` e
`require_complete`; na fonte USDA, `country`, `market_year`, `attributes` e `pivot`, e as colunas em inglês; `parameters`
na NASA POWER; `year` na ANEC; `lat`/`lon`; `sources` no MapBiomas Alerta; `especie` em `ibge.ppm` e `ibge.abate`; `setor`
em `ibge.pib_agro`; `data` (um dia) no `bcb.ptax`; `nivel="estado"` no MapBiomas; `combined` no `cftc.cot`, termo do relatório COT Disaggregated Combined (no dataset, `posicionamento_fundos(..., combinado=True)`); e `cultura` nos defensivos (AGROFIT),
em que o produto é o defensivo e a cultura é o alvo. A coluna de saída `cultura` do ZARC, do PSR e dos custos também fica.

**Proveniência.** No SGS e na PTAX, `source_details["query"]["defaulted_fields"]` traz `inicio`/`fim` no lugar de
`data_inicial`/`data_final`. No SICAR, a chave `source_details["sicar"]["max_features"]` passa a `max_registros`. Quem lê essas
chaves troca o nome.

## 86. Município: nome inteiro ou código IBGE

`municipio` aceita o código IBGE de 7 dígitos (`int` ou texto) ou o **nome inteiro** do município, sem diferenciar caixa,
acento e espaços repetidos; nomes anteriores conhecidos (`"Açu"`) levam ao atual. Pedaço de nome, nome de mais de um município
sem `uf`, nome de outra UF e código fora do cadastro levantam `InvalidParameterError` com os candidatos, antes da rede.
A resolução é a de `normalize.resolver_municipio(valor, uf=None)`, nova na 2.0, que devolve `codigo_ibge`, `nome` e `uf`.

Vale em `alt.sicar` (as 4 funções) e `datasets.cadastro_rural`, `mapbiomas.cobertura` e `datasets.uso_do_solo` municipal,
`alt.mapa_psr.apolices`, `sinistros` e `datasets.seguro_rural`, `zarc.zoneamento` e `datasets.zoneamento_agricola`, e
`alt.anp_diesel.precos_diesel` e `datasets.precos_diesel`. O filtro `cod_municipio` do SICAR, que já existia na 1.1.0, sai, e
também os que só as versões de desenvolvimento da 2.0 tinham (`cod_municipio` no `cadastro_rural`, `geocodigo` no MapBiomas
e `cd_ibge` no PSR): o código vai em `municipio`.

**Muda calado.** Na 1.1.0, o nome casava por pedaço. Agora casa por inteiro, e a mesma chamada pode trazer outro recorte sem
erro:

| Fonte | 1.1.0 | 2.0 |
|---|---|---|
| SICAR | pedaço do nome, com o acento exato (`ILIKE '%nome%'`) | o município de nome exato; onde a UF tem o nome exato e outros que o contêm, só o exato |
| MapBiomas | `municipio="Pinheiro"` trazia os 7 municípios com "pinheiro" no nome | só Pinheiro (MA) |
| PSR e `seguro_rural` | pedaço do rótulo publicado, em qualquer UF | o código do município: entram as apólices rotuladas com o nome do distrito (Caxias do Sul, 2024: 274 → 693 apólices) e saem os homônimos de outras UFs |
| ZARC | pedaço do nome | o nome inteiro: `municipio="herval"` trazia Santa Maria do Herval e agora traz Herval (RS, 4307104) |
| ANP | o nome da planilha, em qualquer UF | o código ou o nome do IBGE, com a UF do município no filtro: `"Santana do Livramento"` passa a `"Sant'Ana do Livramento"` ou `4317103` |

**O que fazer:** use o código IBGE ou o nome completo, com `uf` quando o nome se repete. Para achar o nome a partir de um pedaço,
`normalize.buscar_municipios("sorr", uf="MT")`. Para o comportamento antigo do PSR (rótulo publicado), filtre a coluna
`municipio` do resultado.

## 87. Colunas renomeadas na saída

**Muda calado para quem exporta.** Quem lê a coluna pelo nome antigo recebe `KeyError`; quem só grava o frame (`to_csv`,
Parquet, banco) ou itera as colunas recebe outro schema, sem erro.

**`estado` → `uf`.** Em `conab.progresso_safra` e `datasets.progresso_safra` (contrato `progresso_safra` 2.0), e em
`mapbiomas.cobertura`, `mapbiomas.transicao` e `datasets.uso_do_solo`, estadual e municipal (contratos `mapbiomas_cobertura`
2.0, `mapbiomas_transicao` 2.0 e `mapbiomas_cobertura_municipal` 1.1), inclusive na chave primária. No progresso,
`MEDIA_ESTADOS` e `BR` seguem como valores. O `queimadas.focos` mantém a coluna `estado`, com o nome do estado publicado pelo
INPE, ao lado de `uf`, com a sigla.

**`datasets.posicionamento_fundos`, contrato 2.0.** A fonte `cftc.cot` mantém os nomes do relatório do CFTC; o dataset sai em
português. O mapa está em `agrobr.contracts.datasets.POSICIONAMENTO_FUNDOS_COLUNAS_V2` (nome antigo → novo).

| 1.1.0 | 2.0 |
|---|---|
| `commodity` | `produto` |
| `open_interest` | `posicoes_abertas` |
| `managed_money_long` / `_short` / `_spread` / `_net` | `fundos_compra` / `fundos_venda` / `fundos_spread` / `fundos_saldo` |
| `producer_long` / `_short` | `produtores_compra` / `produtores_venda` |
| `swap_long` / `_short` | `swap_compra` / `swap_venda` |
| `other_long` / `_short` | `outros_compra` / `outros_venda` |
| `nonreportable_long` / `_short` | `nao_reportaveis_compra` / `nao_reportaveis_venda` |
| `change_managed_money_long` / `_short` | `variacao_fundos_compra` / `variacao_fundos_venda` |
| `change_open_interest` | `variacao_posicoes` |

`swap_spread` e `outros_spread` são colunas novas na 2.0 (nas versões de desenvolvimento, `swap_spread` e `other_spread`).

**`datasets.oferta_demanda_global`, contrato 2.0.** A fonte `usda.psd` mantém as colunas em inglês.

| 1.1.0 | 2.0 |
|---|---|
| `commodity_code` | `codigo_produto` |
| `commodity` | `produto` |
| `country_code` | `codigo_pais` |
| `country` | `pais` |
| `market_year` | `ano_comercial` |
| `attribute` | `atributo` |
| `attribute_br` | `atributo_br` |
| `value` | `valor` |
| `unit` | `unidade` |

`codigo_atributo`, `codigo_unidade`, `ano_atualizacao` e `mes_atualizacao` são colunas novas na 2.0 (nas versões de
desenvolvimento, `attribute_id`, `unit_id`, `last_update_year` e `last_update_month`; seção 49).

No modo pivotado, os identificadores fixos usam os nomes novos, e as colunas dos atributos seguem com o rótulo de cada um.

**`datasets.comercio_internacional`, contrato 3.0.** A fonte `comtrade.comercio` mantém os nomes técnicos.

| 1.1.0 | 2.0 |
|---|---|
| `reporter_code` | `codigo_declarante` |
| `reporter_iso` | `iso_declarante` |
| `reporter` | `declarante` |
| `partner_code` | `codigo_parceiro` |
| `partner_iso` | `iso_parceiro` |
| `partner` | `parceiro` |
| `fluxo_code` | `codigo_fluxo` |
| `hs_code` | `codigo_hs` |
| `produto_desc` | `descricao_produto` |

**`conab.brasil_total`.** `produto` passa a ser o identificador normalizado, sem as notas de rodapé, e a coluna nova `rotulo`
traz o texto publicado: o filtro `produto == "SOJA"` volta vazio, e o certo é `produto == "soja"`. `feijao_cores_1`, `_2` e
`_3` distinguem as 3 safras.

**O que fazer:** troque os nomes no consumidor. Para ler arquivos gravados antes, renomeie com o mapa
(`df.rename(columns={novo: antigo ...})`).

## 88. Flags e filtros secundários só por nome

`as_polars`, `return_meta` e as opções que vêm depois deles passam a ser só por nome em todas as fontes e datasets: por posição,
`TypeError`, antes de qualquer download. Exemplos: `bcb.credito_rural`, `cepea.indicador` (também `validate_sanity`,
`force_refresh` e `offline`), `comexstat.exportacao`/`importacao`, `conab.safras` (o `levantamento` segue na 4ª posição),
`balanco` e `brasil_total` (também `levantamento`), `conab.custo_producao`, `inmet.*`, `nasa_power.*` (também `parameters`), `alt.anp_diesel.*`,
`alt.antt_pedagio.*`, `alt.mapa_psr.*` e os datasets correspondentes. Em `datasets.cadastro_rural`, os 7 primeiros
argumentos (`uf` a `criado_apos`) seguem posicionais.

No IBGE, só os filtros principais seguem posicionais, iguais na fonte e no dataset:

| 1.1.0 | 2.0 |
|---|---|
| `ibge.pam("soja", 2023, "MT")` | `ibge.pam("soja", 2023, uf="MT")` |
| `datasets.producao_anual("soja", 2023, "municipio", "MT")` | `datasets.producao_anual("soja", 2023, uf="MT", nivel="municipio")` |
| `ibge.censo_agro("efetivo_rebanho", 2017)` | `ibge.censo_agro("efetivo_rebanho", ano=2017)` |
| `ibge.pib_agro("2024T1")` | `ibge.pib_agro(trimestre="2024T1")` |
| `datasets.leite_industrial("leite", "2024T1")` | `datasets.leite_industrial("2024T1", produto="leite")` |

Pesquisas anuais mantêm produto (ou espécie) e ano por posição; o abate, espécie e trimestre; os censos, só o tema; o leite,
só o trimestre; o PIB, só o setor (na fonte) ou o produto (no dataset). Um trimestre na posição do setor
(`ibge.pib_agro("2024T1")`) levanta `InvalidParameterError` com os setores válidos.

## 89. Tipos da saída

A 2.0 fixa uma regra para todas as fontes: data em `datetime64[ns]` (instante com fuso em `datetime64[ns, UTC]`), inteiro por
natureza (ano, código, contagem) em `Int64`, medida em `float64`, texto no dtype padrão do pandas instalado (`object` no pandas
2, `str` no 3), e o resultado vazio com os mesmos tipos do cheio. `Column.validate` e `validate_dataset` exigem `datetime64`
nas colunas de data: texto conversível e objeto `date` passam a ser erro de contrato. Rótulo de período (safra, `AAAA-MM`,
trimestre) segue em texto.

**Muda calado: colunas de data que eram texto.**

| Função | Coluna | 1.1.0 | 2.0 |
|---|---|---|---|
| `deral.condicao_lavouras`, `datasets.condicao_lavouras` | `data` | texto com o nome da aba: `dd-mm-aaaa`, `dd-mm-aa`, `"Atual"` e `"Anterior"` | `datetime64[ns]` com a data da célula (contrato 2.0) |
| `imea.cotacoes` | `data_publicacao` | texto `AAAA-MM-DD HH:MM:SS` | `datetime64[ns]` |
| `inmet.estacoes` | `inicio_operacao`, `DT_FIM_OPERACAO` | texto ISO com fuso | `datetime64[ns, UTC]`, o mesmo instante |
| `antaq.movimentacao`, `datasets.movimentacao_portuaria` | `data_atracacao` | texto | `datetime64[ns]`, com a hora (contrato 2.0) |
| `alt.antt_pedagio.pracas_pedagio` | `data_da_inativacao` | texto, `""` quando vazia | `datetime64[ns]`, `NaT` quando vazia (contrato 2.0) |

Data inexistente na fonte (como `31/02/2024`) vira `NaT`, com aviso em `meta.validation_warnings`; texto que não é data levanta
`ParseError`. Na FUNAI e no INCRA, as datas já saíam em `datetime64` na 1.1.0 e seguem assim: a `data_atualizacao` da FUNAI,
que a 1.1.0 lia com o mês primeiro, sai com o dia primeiro (seção 34), e o marcador `0001-01-01` do INCRA vira `NaT`
(seção 25). No INMET, o mesmo instante em UTC pode cair no dia seguinte: para a data local publicada, use
`.dt.tz_convert("America/Sao_Paulo").dt.date`.

**A armadilha do filtro por texto.** Comparar uma coluna `datetime64` com texto `dd/mm/aaaa` não dá erro: o pandas lê o texto
com o mês primeiro quando o dia vai até 12. `df[df.data == "01/02/2026"]` traz 2 de janeiro, sem aviso. Compare com
`pd.Timestamp("2026-02-01")` ou `date(2026, 2, 1)`. Para o texto antigo, `df["data"].dt.strftime("%d/%m/%Y")`. Exportado,
o texto também muda: `to_csv` e `astype(str)` dão `2026-09-14`, e não `14/09/2026`.

No DERAL, o filtro pela semana corrente (`data == "Atual"`) volta vazio, e o filtro pelo nome da aba (`"02-10-23"`) pode
voltar vazio ou casar outra data, sem aviso: use
`df[df.data == df.data.max()]` ou `pd.Timestamp`. O `-` publicado em `pct`, `plantio_pct` e `colheita_pct` passa a 0 (na
1.1.0, nulo), e a aba cujo nome difere da data da célula segue a célula, com aviso.

**Muda calado: inteiros e medidas.**

| Função | Coluna | 1.1.0 | 2.0 |
|---|---|---|---|
| `ibge.abate`, `datasets.abate_trimestral` | `animais_abatidos` | `float64` | `Int64` (contrato 2.1) |
| `bcb.sgs`, `datasets.series_economicas` | `codigo` | `int64` | `Int64` (contrato `bcb_sgs` 3.0) |
| `alt.antt_pedagio.pracas_pedagio` | `km_m`, `ano_do_pnv_snv` | texto | `float64`, `Int64` |
| `mapbiomas_alerta.alertas`, `alertas_geo` | `alert_code` | `int64` | `Int64` |
| `b3.ajustes`, `b3.historico` | `vencimento_mes`, `vencimento_ano` | `int64` | `Int64` |
| `alt.mapa_psr.*`, `datasets.seguro_rural` | `ano_apolice` | `int64` | `Int64` |
| `conab.brasil_total` | métricas | `Decimal`/`object` | `float64` |

Códigos e anos do IBGE, da ANA, do SFB e da Embrapa Solos também saem em `Int64`, no cheio e no vazio. Não há truncamento:
a fração numa contagem levanta `ParseError`.

**Muda calado: texto.** O texto sai no dtype padrão do pandas instalado, e não em `string[python]`; nos nulos, `NaN` no pandas
3 e `None` no 2, em vez de `pd.NA`. Ficam em `string[python]`, de propósito, a fonte e os dicionários da ComexStat e o
`antt_pedagio.fluxo` (contrato 3.0), porque o teto de memória dessas consultas conta cada texto guardado. A coluna
`anomalies` do CEPEA sai em `object`, com o JSON ou `None` (no Polars, `String` ou `null`). `ibge.pib_agro` sai com
`precos` na forma canônica (`"corrente"`, e não `" CORRENTE "`).

**Muda calado: o vazio.** Consulta sem linhas devolve as colunas e os tipos do contrato, e não mais tudo `object`: B3,
MapBiomas Alerta (com o CRS `EPSG:4326` no `_geo`), `ibama.embargos_geo`, `icmbio.ucs_geo`, IBGE, CONAB e as demais. A
`ibge.pam` devolve sempre as 14 colunas declaradas, com as medidas não pedidas nulas.

**O que fazer:** teste a família do dtype com `pd.api.types` (`is_integer_dtype`, `is_string_dtype`,
`is_datetime64_any_dtype`), o nulo com `isna()`, e não com `is pd.NA` ou `== "NULL"`; compare datas com `Timestamp` ou
`date`; para ter `int64`, `astype("int64")` quando não houver nulo.

**Contratos que mudam de versão por essas regras:**

| Contrato | 1.1.0 | 2.0 |
|---|---|---|
| `abate_trimestral` | 1.0 | 2.1 |
| `antt_pedagio_pracas` | 1.0 | 2.0 |
| `condicao_lavouras` | 1.0 | 2.0 |
| `movimentacao_portuaria` | 1.0 | 2.0 |
| `oferta_demanda_global` | 1.0 | 2.0 |
| `posicionamento_fundos` | 1.0 | 2.0 |
| `comercio_internacional` | 1.0 | 3.0 |
| `bcb_sgs` | — | 3.0 |
| `embrapa_solos_perfis` | — | 3.0 |

Nos contratos que já existiam na 1.1.0, a constante antiga segue com o schema anterior (menos `POSICIONAMENTO_FUNDOS_V1`,
que passa a 1.1, com `swap_spread` e `other_spread`), e a nova tem outro nome:
`IBGE_ABATE_V2` (`agrobr.contracts.ibge`), `ANTT_PEDAGIO_PRACAS_V2`, `CONDICAO_LAVOURAS_V2`, `MOVIMENTACAO_PORTUARIA_V2`,
`OFERTA_DEMANDA_GLOBAL_V2` e `POSICIONAMENTO_FUNDOS_V2` (`agrobr.contracts.datasets`), `COMERCIO_INTERNACIONAL_V3`
(`agrobr.contracts.comtrade`). O `bcb_sgs`, sem contrato na 1.1.0, está em `BCB_SGS_V3` (`agrobr.contracts.bcb_sgs`). O registro
(`get_contract(nome)`) devolve sempre o atual.

## 90. Erros: a classe diz a causa

**Falha de layout em todas as fontes de um dataset levanta `ParseError`**, com `errors`, `attempted_sources` e a causa
original em `__cause__`, e não mais `SourceUnavailableError`. Falha mista (rede numa fonte, layout noutra) segue como
`SourceUnavailableError`. Erro de programação (um `TypeError` interno, por exemplo) propaga como está, sem acionar o fallback.
Quem fazia `except SourceUnavailableError` para "a fonte falhou" passa a tratar `ParseError` à parte, ou `AgrobrError` na
borda da aplicação.

**Nome desconhecido num catálogo levanta `UnknownNameError`**, subclasse de `InvalidParameterError` e de `KeyError`, com os
nomes válidos: `datasets.get_dataset`, `contracts.get_contract`, `normalize.uf_para_nome`, `uf_para_regiao` e
`uf_para_ibge`. `except KeyError` e `except ValueError` seguem pegando. A exceção é exportada em `agrobr`.

**Entrada inválida levanta `InvalidParameterError` antes da rede**, com os valores válidos na mensagem, onde a 1.1.0 devolvia
vazio, o dado sem filtro ou um erro cru (`ValueError`, `TypeError`, `AttributeError`). A regra da seção 66 vale agora em
todas as fontes. Exemplos: contrato fora de `b3.contratos()`, produto sem contrato no CFTC, cadeia desconhecida no IMEA,
`uf` ou `tipo` fora do catálogo no `inmet.estacoes`, produto fora da planilha no DERAL, safra malformada na UNICA, `ano` em
texto na ABIOVE, produto desconhecido no Notícias Agrícolas, ano fora do intervalo no USDA, no Comtrade e na ANDA, filtros da
ANTAQ, `tipo_veiculo` da ANTT, `evento` com `tipo="apolices"` no `seguro_rural`, `uf` que não é texto, `bbox` fora do
intervalo, safra inválida no `normalize` e `max_pages` ≤ 0 nas semanas do progresso. O satélite das queimadas e a classe do
MapBiomas são conferidos depois do download, contra o que o arquivo publica. `InvalidParameterError` é subclasse de
`ValueError`: quem tratava `df.empty` depois de uma entrada do usuário passa a capturar a exceção. No `normalize.municipio_para_ibge`, nome de mais de um município sem `uf` levanta o mesmo erro do `resolver_municipio`, com os candidatos; a 1.1.0 devolvia o código do primeiro da lista.

**Resposta fora do formato levanta `ParseError`**, e não mais um vazio que parece "sem dado": contagem do ArcGIS sem `count`
(ANA e SFB), envelope do SICOR sem `value`, preços da CEASA sem `resultset`, página da ANEC sem a lista de artigos, lista vazia
do IMEA, catálogo do INMET que não é lista, `alerta_info` do MapBiomas Alerta sem o período, aba dos totais da CONAB sem
cabeçalho, baseline estrutural corrompido e consenso do CEPEA sem registros. Os vazios legítimos (`count` zero, `value=[]`,
dia sem preço) seguem vazios. Campo pedido ausente numa camada ArcGIS (ANA e SFB) também levanta `ParseError`, com o nome do campo; antes, a coluna saía do resultado sem erro.

**INMET sem observação.** `inmet.estacao` e `inmet.clima_uf` sem nenhuma observação no período levantam
`SourceUnavailableError`, e não mais `ParseError`. Quem capturava `ParseError` para "sem dado" troca a classe.

**Mensagens.** "UF invalida" passa a "UF inválida: 'XX'. Valores válidos: AC, AL, …", e "after N retries" passa a
"after N attempts". Quem casava o texto troca o termo.

**Avisos novos**, no `UserWarning` e em `meta.validation_warnings`: consulta SIDRA vazia (a cada chamada), ano de criação
ambíguo no CNFP do SFB, filtro de praça da ANTT sem resultado, linhas repetidas idênticas na ANP (removidas), `ordem` da
Embrapa sem correspondência numa leitura completa e data digitada errado na fonte (vira `NaT`). Quem trata aviso como erro
precisa acomodá-los.

## 91. Variáveis de ambiente

**Muda calado.** A [página das variáveis de ambiente](../advanced/ambiente.md) lista todas, com o padrão e os valores aceitos.

- **Pasta do cache.** Na 1.1.0, só `AGROBR_CACHE_CACHE_DIR` valia; `AGROBR_CACHE_DIR` era ignorada, e a variável vazia
  apontava a pasta corrente. Na 2.0, `AGROBR_CACHE_DIR` é o nome principal, e o antigo segue como alias; com as duas
  definidas e diferentes, vale a nova, com `UserWarning`; vazia, vale o padrão `~/.agrobr/cache`. Quem já tinha
  `AGROBR_CACHE_DIR` definida com outra pasta passa a gravar o cache nela. Defina só uma; prefira `AGROBR_CACHE_DIR`.
- **Cache desligado.** `AGROBR_ANEC_CACHE_DISABLED` e `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED` aceitam `1`, `true`, `yes` e
  `on` (desligam) e `0`, `false`, `no`, `off` e vazio (mantêm), sem caixa. Na 1.1.0, só `1` desligava, e `true` era ignorado.
  Outro valor (`sim`) levanta `InvalidParameterError`.
- **`AGROBR_HTTP_MAX_RETRIES`** conta o total de tentativas por pedido, como sempre contou: o padrão 3 faz até 3 pedidos.
  `0` passa a fazer 1 pedido, sem repetição (na 1.1.0, quebrava toda requisição), e negativo é recusado com
  `ValidationError` ao carregar as configurações.
- **`AGROBR_HTTP_MAX_CONCURRENT_<FONTE>`** menor que 1 é recusado com `ValidationError` (na 1.1.0, `0` travava a chamada).
- **`AGROBR_HTTP_RATE_LIMIT_<FONTE>`** e o `rate_limit_*` passado ao `HTTPSettings` recusam negativo, `inf` e `nan` com `ValidationError` (na 1.1.0, eram aceitos); `0` segue permitido.

No código, o mesmo vale para `retry_async` e `with_retry`: `max_attempts=0` faz 1 tentativa, e espera `0` é espera zero
(antes, os dois viravam os padrões); para o padrão, passe `None` ou omita. `run_all_checks` com `concurrency` menor que 1,
`generate_report` com formato fora de `json`, `html` e `md`, `send_alert` com `level` inválido, `benchmark_*` com
`iterations` menor que 1 e `cache.get_policy` com `endpoint` desconhecido levantam `InvalidParameterError`.

## 92. CLI: `--formato` em todos os comandos e `snapshot use` retirado

A [referência da CLI](../advanced/cli.md) traz todos os comandos e opções.

- `health`, `doctor` e `snapshot list` usam `--formato text|json` (`-o`), como os comandos de dados usam
  `--formato table|csv|json`. `health --output json`, `doctor --json` e `snapshot list --json` saem com código 2: troque por
  `--formato json`.
- `snapshot use` sai: o comando não ativava nada. O modo determinístico se configura no código, por processo
  (`async with datasets.deterministic("AAAA-MM-DD")`), e o snapshot se lê com
  `snapshots.load_from_snapshot(..., snapshot_name=...)`.
- **Muda calado:** `ibge produtos --pesquisa PAM` consultava o LSPA (a caixa alta caía no padrão). Na 2.0, `pam` e `lspa`
  valem sem diferenciar caixa, e outro nome sai com código 2. Quem usava `PAM` para obter o LSPA passa a `--pesquisa lspa`.
- Ano inválido, pesquisa e fonte desconhecidas saem com código 2 antes da consulta; falha de coleta segue com código 1. Os
  logs saem legíveis na saída de erro (`WARNING`; `INFO` com `--verbose`), e o JSON e o CSV ficam sozinhos na saída padrão.
- Opções novas: `cepea indicador --praca`, `conab safras --levantamento` e `conab balanco --safra/--levantamento`.
- **Muda calado:** `agrobr conab levantamentos` lista todos os levantamentos, e não só os 10 primeiros, e o aviso `Listando levantamentos...` sai na saída de erro. Para ler a lista num script, use `--formato csv` ou `--formato json`.

## 93. CONAB: um caminho por função

Os subpacotes `agrobr.conab.custo_producao` e `agrobr.conab.serie_historica` saem; os nomes de mesmo nome em `agrobr.conab`
são as funções. `import agrobr.conab.custo_producao` e `from agrobr.conab.serie_historica import produtos_disponiveis`
levantam `ModuleNotFoundError`: use `from agrobr.conab import custo_producao, serie_historica` e
`conab.produtos_serie_historica()`, que lista os produtos, as categorias e as URLs da série sem requisição. O
`agrobr.conab.ceasa` fica com o `__all__` vazio (`from agrobr.conab.ceasa import *` não exporta nada): use
`conab.ceasa_precos`, `ceasa_produtos`, `ceasa_categorias` e `lista_ceasas`.

## 94. Downloads pesados e cache

- **Muda calado:** `ibama.embargos` e `embargos_geo` guardam o CSV (~208 MB) na pasta de cache por 1 hora. Dentro da hora, a
  mesma chamada devolve a coleta anterior, com `meta.from_cache=True` e `meta.fetched_at` na hora da coleta; chamadas
  simultâneas baixam uma vez só. `use_cache=False`, argumento novo, baixa de novo sem ler nem gravar o cache.
- `acervo_fundiario` com `use_cache=False` ou `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED` não grava mais nada no cache: o ZIP
  (até 766 MB por UF) vai para uma pasta temporária, apagada no fim da consulta. Na 1.1.0, a opção só pulava a leitura.
- `b3.historico` baixa no máximo `AGROBR_HTTP_MAX_CONCURRENT_B3` dias ao mesmo tempo (padrão 3), em vez de um download por dia
  útil de uma vez. Período longo fica mais lento no pico; suba a variável para mais paralelismo.
- `validators.save_baseline`, `load_baseline` e `validate_against_baseline` gravam e leem em `structures` dentro da pasta do
  cache, e não mais em `.structures` no diretório corrente. Para o caminho antigo, passe `baselines_dir=".structures"`.

## 95. Retornos como cópia

**Muda calado.** `datasets.get_dataset`, `contracts.get_contract`, `datasets.list_products` e `datasets.info`,
`normalize.ibge_para_municipio`, `buscar_municipios`, `coordenada_para_municipio` e `listar_ufs`,
`health.get_affected_datasets` e os artigos da ANEC devolvem cópias. Alterar o objeto devolvido deixa de mudar o catálogo
ou a chamada seguinte. Quem usava a mutação como configuração global passa a usar a instância devolvida: por exemplo,
`get_dataset(nome)` e o `fetch` dela.

No `utils.parse_links_from_html` com `base_url`, um `href` relativo sem barra (`"arquivo.pdf"`) passa a sair como URL absoluta;
antes, saía como estava.

## 96. O que é API pública

A 2.0 declara o que segue o versionamento semântico: as funções das fontes, `datasets`, `contracts`, as exceções, o `MetaInfo`,
o `normalize`, o `agrobr.sync` e os comandos da CLI. A lista nominal está na [página da API pública](../api/index.md). O resto
(`cache`, `http`, `utils`, `health`, `alerts`, `benchmark`, `validators`, `constants` e os subpacotes internos de cada fonte)
é interno, salvo os poucos nomes que a doc ensina e a página lista, e pode mudar numa versão menor. Nenhum import muda na 2.0; quem usa um nome interno deve fixar a versão do agrobr.

## 97. Outras mudanças por fonte

- `datasets.clima`: `agregacao` passa a `None`. No modo UF, `None` e `"mensal"` devolvem meses, e `"diario"` levanta
  `InvalidParameterError` (na 1.1.0, era o padrão, aceito e ignorado: a saída já era mensal); para dados diários, use o modo
  estação. No modo UF sem `fonte`, ano anterior a 2000 vai direto à NASA POWER, e `fonte="inmet"` com esse ano levanta
  `InvalidParameterError`.
- `zarc.zoneamento(cultura=...)` levanta `InvalidParameterError: Argumentos desconhecidos: ['cultura']`: use `produto=`.
- `imea.cotacoes` aceita a safra como `"2024/25"` e `"2024/2025"`, além de `"24/25"`; `unica.producao_historica`, como
  `"2018/19"` e `"18/19"`, além de `"2018/2019"`. `deral.condicao_lavouras` aceita os sinônimos de produto do agrobr, e
  `datasets.condicao_lavouras` aceita `"milho"` e `"feijao"` (as duas safras) e declara `as_polars`.
- `datasets.preco_diario` declara `as_polars`.
- ANP: linhas semanais idênticas são removidas, com aviso e a quantidade em `meta.validation_warnings`; valores conflitantes
  seguem levantando `ParseError`.
- ANP, `vendas_diesel`: **muda calado.** `produto` usa o rótulo de `precos_diesel` (`DIESEL S-10` sai `DIESEL S10`), e o
  join por produto entre vendas e preços casa; os outros combustíveis (`DIESEL S-500`, `DIESEL S-1800`, `DIESEL MARÍTIMO`,
  `DIESEL (OUTROS )`) seguem como publicados. `regiao` sai com o nome canônico: `REGIÃO CENTRO-OESTE` vira `Centro-Oeste`
  (idem `Norte`, `Nordeste`, `Sudeste` e `Sul`). Quem filtrava pelo texto publicado troca o valor; os volumes não mudam.
- ANP, `precos_diesel`: período que começa depois de hoje levanta `InvalidParameterError` antes da rede, em todos os níveis
  (na 1.1.0, vazio sem aviso na UF e no Brasil); no município, limite fora de 2022 até o ano corrente também. Ano dentro
  do intervalo cujo arquivo a ANP ainda não publicou segue `SourceUnavailableError`.
- Embrapa Solos: `mapa_solos` e `mapa_solos_geo` casam `ordem` com a classe inteira de `ordem1` (uma das 15 publicadas),
  aceitando caixa, acento e singular (`"latossolo"`). Um trecho do nome (`"latos"`) deixa de casar e levanta
  `InvalidParameterError` com a lista das classes: troque pelo nome da classe (`"latossolos"`).
- ANTT: `rodovia` compara sem caixa, espaço, hífen e zero à esquerda (`"BR 40"`, `"br-040"`); `tipo_veiculo` e
  `tipo_cobranca`, sem caixa e acento.
- RNC e cultivares: os filtros de texto ignoram caixa e acento (`especie="feijao"` acha `"Feijão"`).
- Queimadas: `satelite` sem diferenciar caixa.
- Anos padrão e limites de ano seguem o calendário de Brasília, e não o relógio da máquina, no SGS, na PTAX, nas queimadas,
  no PRODES, no USDA, no Comtrade, na ComexStat, na ANEC, na ANTT e na ANP. Num servidor em UTC, das 21h às 24h de 31/12, o
  ano seguinte deixa de ser aceito.
- `defensivos`: o formato do cache sobe para 2; a primeira consulta baixa de novo.
- SICAR, `resumo(uf)` sem município: a contagem é de feições publicadas, e versões do mesmo `cod_imovel` contam separado; o `MetaInfo` traz `source_details["sicar"]["unidade"] = "feicoes_publicadas"` e um aviso em `validation_warnings`. Para contar imóveis, use o resumo por município.
- SFB, IFN: `sfb.ifn_conglomerados` e o `_geo` leem as camadas ativas do IFN (o serviço `Conglomerado` saiu do ar e as duas
  funções falhavam); `lote` vem pela junção com o cadastro de lotes em `co_lote` (código órfão ou duplicado levanta `ParseError`),
  e entra a coluna `ciclo` (texto anulável, schema IFN 1.1): ajuste seleção por posição. O filtro de `bioma` deixa de voltar vazio
  por diferença de caixa. A proveniência por página está na [página da fonte](../sources/sfb.md).

## 98. B3: `oi_historico` passa a se chamar `posicoes_abertas_historico`

`b3.oi_historico`, da 1.1.0, passa a se chamar `b3.posicoes_abertas_historico`, também em `sync.b3`. As opções não mudam;
só sai o `**kwargs` (seção 78), e o nome antigo deixa de existir (`AttributeError`):

```python
df = await b3.posicoes_abertas_historico(contrato="boi", inicio="2026-09-01", fim="2026-09-04")
```

O dataset continua com `datasets.futuros_agricolas(..., tipo="oi_historico")`: os valores de `tipo` não mudam. Colunas,
unidades e a política para dia sem arquivo também ficam.

## 99. CFTC: recorte sem relatório devolve vazio tipado

`cftc.cot` com um recorte sem relatório devolve o DataFrame vazio, com as colunas e os tipos do preenchido, e não mais
`SourceUnavailableError`. Quem usava a exceção para detectar "sem observação" passa a conferir `df.empty` (`is_empty()` no
Polars); falha de transporte segue `SourceUnavailableError`, e resposta fora do formato, `ParseError`. As 18 contagens e
variações saem em `Int64`, no vazio e no cheio (na 1.1.0, as 13 contagens saíam em `int64` e as 3 variações já em `Int64`; `swap_spread` e `other_spread` são novas). Acima de 50.000 registros, a consulta avisa que a completude
não foi comprovada (`UserWarning` e `meta.validation_warnings`): reduza o período.
