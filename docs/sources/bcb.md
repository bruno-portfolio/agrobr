# BCB/SICOR — Credito Rural

Dados de credito rural do Sistema de Operacoes do Credito Rural (SICOR),
disponibilizados via API OData do Banco Central.

## API

```python
from agrobr import bcb

# Credito de custeio para soja, safra 2024/25
df = await bcb.credito_rural(produto="soja", safra="2024/25", finalidade="custeio")

# Filtrar por UF
df = await bcb.credito_rural(produto="soja", safra="2024/25", uf="MT")

# Agregacao por UF (soma municipios)
df = await bcb.credito_rural(produto="soja", safra="2024/25", agregacao="uf")

# Agregacao por programa
df = await bcb.credito_rural(produto="soja", safra="2024/25", agregacao="programa")

# Filtrar por programa
df = await bcb.credito_rural(produto="soja", safra="2024/25", programa="Pronamp")

# Filtrar por tipo de seguro
df = await bcb.credito_rural(produto="soja", safra="2024/25", tipo_seguro="Proagro tradicional")

# Registro a registro, com mes, fonte de recursos, modalidade e atividade
df = await bcb.credito_rural(produto="soja", safra="2024/25", uf="MT", agregacao="registro")
```

## Colunas — `credito_rural`

| Coluna | Tipo | Descricao |
|---|---|---|
| `safra` | str | Safra no formato "2024/25" (AAAA/AA) |
| `produto` | str | Chave do produto pedido, sem acento e em minúsculas (ex.: `algodao`, `cana`), igual nas duas fontes |
| `uf` | str | UF |
| `finalidade` | str | Finalidade pedida, em minúsculas nas duas fontes (custeio, investimento, comercializacao) |
| `agregacao` | str | Nivel da saida: `uf` ou `programa` |
| `programa` | str | Programa SICOR pela tabela oficial (PRONAMP, PRONAF, RenovAgro etc.); nulo na agregacao por UF e quando o codigo vem nulo |
| `cd_programa` | str | Codigo do programa; nulo na agregacao por UF |
| `qtd_contratos` | int | Quantidade de contratos |
| `valor` | float | Valor financiado (R$) |
| `area_financiada` | float | Área financiada (ha); nula quando nenhum registro do grupo publica área, o que no OData vale sempre (custeio publica `AreaCusteio` vazio; investimento e comercialização não trazem área); só o fallback BigQuery a preenche |
| `fonte` | str | `bcb_odata` ou `bcb_bigquery` |

Sao as 11 colunas do contrato 2.0 (`docs/contracts/credito_rural.md`), nas agregacoes `uf` e `programa`: nelas o tipo
de seguro serve so ao filtro `tipo_seguro`, e ano e mes de emissao, a safra. Com `agregacao="registro"`, a saida e
registro a registro, sem agregar, com 23 colunas: as 11 e mais `ano_emissao`, `mes_emissao`, `regiao`, `cd_sub_programa`,
`cd_fonte_recurso`, `fonte_recurso`, `cd_tipo_seguro`, `tipo_seguro`, `cd_modalidade`, `modalidade`, `cd_atividade` e
`atividade`, no contrato [bcb.credito_rural_registro](../contracts/bcb_credito_rural_registro.md) 1.0.

## Dimensoes SICOR

A API retorna codigos de dimensao (`cdPrograma`, `cdTipoSeguro`, etc.). O nome do programa e o do tipo de seguro
seguem as tabelas de dominio oficiais do BCB (`https://www.bcb.gov.br/htms/sicor/Programa.csv` e
`TipoGarantiaEmpreendimento.csv`): o programa publicado e o trecho da descricao oficial
antes do primeiro " - " (a descricao inteira quando nao ha esse separador; aspas soltas da fonte removidas); o tipo
de seguro e a descricao oficial. O filtro por nome nao diferencia maiusculas de minusculas (`programa="pronamp"`).
Codigos desconhecidos geram `"Desconhecido ({code})"` com log warning; codigo nulo fica com nome nulo, sem aviso.
A descricao oficial do `0152` registra que ele era o Moderinfra ate 30/06/2021; o nome publicado e o atual.

| Codigo | Programa publicado | Descricao oficial | Vigencia oficial |
|---|---|---|---|
| `0001` | PRONAF | PRONAF - PROGRAMA NACIONAL DE FORTALECIMENTO DA AGRICULTURA FAMILIAR | 01/11/2011 a 31/12/2099 |
| `0050` | PRONAMP | PRONAMP - PROGRAMA NACIONAL DE APOIO AO MÉDIO PRODUTOR RURAL | 01/11/2011 a 31/12/2099 |
| `0070` | FUNCAFÉ (PROGRAMA DE DEFESA DA ECONOMIA CAFEEIRA) | FUNCAFÉ (PROGRAMA DE DEFESA DA ECONOMIA CAFEEIRA) | 02/07/2012 a 31/12/2099 |
| `0100` | PRLC-BA (PROG RECUP LAVOURA CACAUEIRA BAIANA) ENCERRADO | PRLC-BA (PROG RECUP LAVOURA CACAUEIRA BAIANA) ENCERRADO | 01/11/2011 a 30/09/2022 |
| `0110` | PRODECER III | PRODECER III - PROG COOP NIPO-BRASILEIRA P DESENV DOS CERRADOS - ENCERRADO | 01/11/2011 a 30/09/2022 |
| `0151` | PROCAP-AGRO (PROGRAMA DE CAPITALIZAÇÃO DAS COOPERATIVAS DE PRODUÇÃO AGROPECUÁRIAS) | PROCAP-AGRO (PROGRAMA DE CAPITALIZAÇÃO DAS COOPERATIVAS DE PRODUÇÃO AGROPECUÁRIAS) | 01/11/2011 a 31/12/2099 |
| `0152` | PROIRRIGA | PROIRRIGA - antigo Moderinfra, alterado em 01/07/2021 | 01/11/2011 a 31/12/2099 |
| `0153` | MODERAGRO | MODERAGRO - PROGRAMA DE MODERNIZAÇÃO DA AGRICULTURA E CONSERVAÇÃO DE RECURSOS NATURAIS | 01/11/2011 a 30/06/2025 |
| `0154` | MODERFROTA | MODERFROTA - PROGRAMA DE MODERNIZAÇÃO DA FROTA DE TRATORES AGRÍCOLAS E IMPL ASSOC E COLHEITADEIRAS | 01/11/2011 a 31/12/2099 |
| `0155` | PRODECOOP | PRODECOOP - PROGRAMA DE DESENVOLVIMENTO COOPERATIVO PARA AGREGAÇÃO DE VALOR À PRODUÇÃO AGROPECUÁRIA | 01/11/2011 a 31/12/2099 |
| `0156` | ABC + Programa para a Adaptação à Mudança do Clima e Baixa Emissão de Carbono | ABC + Programa para a Adaptação à Mudança do Clima e Baixa Emissão de Carbono | 01/11/2011 a 30/06/2023 |
| `0157` | PSI-RURAL | PSI-RURAL - PROG SUSTENTAÇÃO  INVESTIMENTO ENCERRADO | 01/11/2011 a 31/05/2016 |
| `0158` | PROCAP-CRED (PROG CAPIT COOP CRÉDITO) ENCERRADO | PROCAP-CRED (PROG CAPIT COOP CRÉDITO) ENCERRADO | 01/11/2011 a 05/04/2016 |
| `0159` | MODERMAQ | MODERMAQ - PROG MOD PARQUE IND NACIONAL - ENCERRADO | 06/08/2004 a 30/06/2016 |
| `0160` | PRI | PRI - PROGRAMA DE REFORÇO DO INVESTIMENTO (CIRC 3.745) - ENCERRADO | 01/01/2013 a 31/12/2015 |
| `0161` | PRORENOVA-RURAL- PROG APOIO  RENOV IMPLANTAÇÃO NOVOS CANAVIAIS- ENCERRADO | PRORENOVA-RURAL- PROG APOIO  RENOV IMPLANTAÇÃO NOVOS CANAVIAIS- ENCERRADO | 18/06/2013 a 31/12/2018 |
| `0162` | INOVAGRO | INOVAGRO - Programa de Incentivo à Inovação Tecnológica na Produção Agropecuária | 01/07/2013 a 31/12/2099 |
| `0163` | PCA | PCA - Programa para Construção e Ampliação de Armazéns | 01/07/2013 a 31/12/2099 |
| `0164` | PRORENOVA-IND- PROG APOIO RENOV IMPLANT NOVOS CANAVIAIS | PRORENOVA-IND- PROG APOIO RENOV IMPLANT NOVOS CANAVIAIS - ENCERRADO | 18/06/2013 a 07/07/2017 |
| `0165` | PROAQÜICULTURA-PROG APOIO DESENVSETOR AQUÍCOLA | PROAQÜICULTURA-PROG APOIO DESENVSETOR AQUÍCOLA - ENCERRADO | 01/07/2013 a 30/09/2022 |
| `0180` | FNO-ABC (PROG FINANC AGRICULTURA BAIXO CARBONO) ENCERRADO | FNO-ABC (PROG FINANC AGRICULTURA BAIXO CARBONO) ENCERRADO | 01/01/2015 a 30/06/2015 |
| `0200` | PROCERA | PROCERA - PROG ESPECIAL DE CRÉDITO PARA A REFORMA AGRÁRIA - ENCERRADO | 01/01/1984 a 30/09/2022 |
| `0201` | PROGRAMA NACIONAL DE CRÉDITO FUNDIÁRIO (FTRA) | PROGRAMA NACIONAL DE CRÉDITO FUNDIÁRIO (FTRA) | 01/01/2013 a 31/12/2099 |
| `0222` | RenovAgro | RenovAgro - Programa de Financiamento a Sistemas de Produção Agropecuária Sustentáveis | 01/07/2023 a 31/12/2099 |
| `0240` | ANF | ANF - ATIVIDADE NÃO FINANCIADA ENQUADRADA NO PROAGRO | 01/01/1984 a 31/12/2099 |
| `0721` | Linha Crédito Rural instit Res. 4.028/2011 (Dívidas Composição e Renegoc PRONAF) | Linha Crédito Rural instit Res. 4.028/2011 (Dívidas Composição e Renegoc PRONAF) - ENCERRADO | 01/01/2013 a 15/10/2014 |
| `0722` | Linha Crédito Rural inst Res. 4.029/2011 (Reneg Crédito Fundiário) ENCERRADO | Linha Crédito Rural inst Res. 4.029/2011 (Reneg Crédito Fundiário) ENCERRADO | 01/01/2013 a 31/12/2013 |
| `0730` | Linha Crédito Rural inst Res. 4.083/2012 (Enchentes Reg Norte) ENCERRADO | Linha Crédito Rural inst Res. 4.083/2012 (Enchentes Reg Norte) ENCERRADO | 01/01/2013 a 31/12/2013 |
| `0735` | Linha de Crédito Rural instituida pela Res. 4.126/2012 (Produtores de Maçã) ENCERRADO | Linha de Crédito Rural instituida pela Res. 4.126/2012 (Produtores de Maçã) ENCERRADO | 01/01/2013 a 31/12/2013 |
| `0776` | Linha Crédito Rural Inst Res. 4.147/2012 e 4.260/2013 (Agricultores Familiares) ENCERRADO | Linha Crédito Rural Inst Res. 4.147/2012 e 4.260/2013 (Agricultores Familiares) ENCERRADO | 01/01/2013 a 31/12/2015 |
| `0777` | Linha Crédito Rural inst Res. 4.147/2012 e 4.260/2013 (Demais Agricultores) ENCERRADO | Linha Crédito Rural inst Res. 4.147/2012 e 4.260/2013 (Demais Agricultores) ENCERRADO | 26/10/2012 a 31/12/2015 |
| `0779` | Linha de Crédito Rural instituida pela Res. 4.161/2012 (Produtores de Arroz) ENCERRADO | Linha de Crédito Rural instituida pela Res. 4.161/2012 (Produtores de Arroz) ENCERRADO | 01/01/2013 a 31/12/2013 |
| `0783` | Linha Crédito Rural inst pelas Res 4.189 e 4.212/2013-PRONAF (Estiagem Area Sudene) ENCERRADO | Linha Crédito Rural inst pelas Res 4.189 e 4.212/2013-PRONAF (Estiagem Area Sudene) ENCERRADO | 01/01/2013 a 31/12/2014 |
| `0784` | Linha Credito Rural inst Res. 4.188 e 4.211/2013-Demais Produtores (Estiagem Area Sudene) ENCERRADO | Linha Credito Rural inst Res. 4.188 e 4.211/2013-Demais Produtores (Estiagem Area Sudene) ENCERRADO | 01/01/2013 a 31/12/2014 |
| `0785` | Linha Crédito Rural inst  Res. 4.220/2013 (Recursos BNDES-Estiagem Área da Sudene) ENCERRADO | Linha Crédito Rural inst  Res. 4.220/2013 (Recursos BNDES-Estiagem Área da Sudene) ENCERRADO | 02/05/2013 a 30/06/2014 |
| `0786` | Linha de Crédito Rural Instituída pela Res. 4.289/2013 (Renegociação Café Arábica) ENCERRADO | Linha de Crédito Rural Instituída pela Res. 4.289/2013 (Renegociação Café Arábica) ENCERRADO | 25/11/2013 a 31/07/2014 |
| `0790` | Linha de Crédito Rural inst Res 5.120/2024 (Linha emergencial Custeio Pecuário) | Linha de Crédito Rural inst Res 5.120/2024 (Linha emergencial Custeio Pecuário) | 07/02/2024 a 30/06/2024 |
| `0888` | Outras Linhas de Crédito Rural não Especificadas | Outras Linhas de Crédito Rural não Especificadas - ENCERRADO | 01/01/2014 a 31/01/2014 |
| `0901` | Eco Invest Brasil | Eco Invest Brasil - RES CMN Nº 5.130/2024 | 24/03/2026 a 01/01/2099 |
| `0999` | FINANCIAMENTO SEM VÍNCULO A PROGRAMA ESPECÍFICO | FINANCIAMENTO SEM VÍNCULO A PROGRAMA ESPECÍFICO | 02/07/2012 a 31/12/2099 |

| Codigo | Tipo de seguro publicado (descricao oficial) |
|---|---|
| `0` | Não se aplica |
| `1` | Proagro tradicional |
| `2` | Proagro mais |
| `3` | Outro seguro |
| `9` | Sem adesão a seguro |

No `agregacao="registro"`, a fonte de recursos, a modalidade e a atividade saem com a descricao oficial inteira de
`FonteRecursos.csv` (37 codigos), `Modalidade.csv` (64) e `Atividade.csv` (2). Na fonte de
recursos, o trecho antes do " - " juntaria codigos distintos (4 fontes virariam "POUPANÇA RURAL"). Codigo fora da
tabela fica com nome nulo, sem palpite e sem aviso. `cd_modalidade` sai como a fonte publica (`"01"`), e o nome e
resolvido pelo numero da tabela (`"1"` = LAVOURA). O subprograma sai so com o codigo, como na 1.1.0.

## Finalidades

- `custeio` — financiamento da producao
- `investimento` — aquisicao de maquinas, infraestrutura
- `comercializacao` — financiamento da comercializacao
- `industrializacao` — financiamento da industrializacao, que o SICOR publica só no total por UF, sem produto

> **Nota:** o `credito_rural` (por produto) aceita as três primeiras. A industrialização está na entidade `RegiaoUF`, lida pelo `bcb.credito_rural_total`, com as quatro finalidades por UF, programa e mês.

## Produtos

Soja, milho, cafe, algodao, arroz, trigo, feijao, cana-de-acucar, mandioca,
sorgo, aveia, cevada, entre outros. Use o nome canonico do agrobr.

## MetaInfo

```python
df, meta = await bcb.credito_rural(produto="soja", safra="2024/25", return_meta=True)
print(meta.source)          # "bcb_credito"
print(meta.schema_version)  # "2.0"
```

Com `agregacao="registro"`, `schema_version` e `contract_version` sao `"1.0"`, e `source_details["contract"]` e
`"bcb.credito_rural_registro"`.

O `source_url` é a consulta OData pedida, com `$filter` e `$select` e sem `$top`. O `raw_content_hash` é o SHA-256 do
manifesto canônico `{query, resources}` (`source_details["hash_kind"] = "resource_manifest_sha256"`), e o `raw_content_size`,
o tamanho dele; `source_details["resources"]` lista cada página recebida (URL, SHA-256, bytes e hora), inclusive as 12 do
fatiamento por mês quando o volume passa do limite da Olinda. O `fetched_at` é o da página mais recente. O BCB revê meses
fechados (RS, jun/2023: 55 → 54 contratos). O hash do manifesto muda a cada chamada, porque leva a hora de cada página: ele
identifica a aquisição. Para saber se a base mudou entre 2 consultas, compare o `sha256` de cada item de
`source_details["resources"]`. O fallback BigQuery não declara a data da base.

Sem cache: cada chamada consulta a fonte (o OData do BCB ou, no fallback, a BigQuery). O mesmo vale para SGS, PTAX e
Focus.

## Status (set/2026)

A API SICOR foi reestruturada (~2024). O agrobr lê `CusteioRegiaoUFProduto`,
`InvestRegiaoUFProduto` e `ComercRegiaoUFProduto` (por produto, no `credito_rural`) e
`RegiaoUF` (total por UF e finalidade, no `credito_rural_total`). O serviço também publica
município por produto (`CusteioMunicipioProduto`, `InvestMunicipioProduto`) e município sem
produto (`CusteioInvestimentoComercialIndustrialSemFiltros`), que o agrobr não lê.

Notas da fonte, conferidas em 2022 e 2023:

- não há total nem linha Brasil: o total do país é a soma das UFs;
- a `RegiaoUF` é igual à soma dos municípios da `SemFiltros` por UF e finalidade, e à soma por
  produto nas três finalidades que têm produto;
- os códigos de programa, subprograma e fonte de recursos podem vir com ou sem zeros à
  esquerda conforme a entidade ("1" × "0001"): normalizar antes de comparar;
- no investimento, o subprograma difere entre `RegiaoUF` e `SemFiltros`: nunca juntar as duas
  por subprograma;
- `RegiaoUF` e `SemFiltros` cobrem de jan/2013 ao último mês fechado; `200` com `value: []` só
  aparece fora da cobertura (dez/2012 e o mês corrente).

O client usa igualdade em `nomeProduto`, incluindo as aspas duplas literais
do SICOR, e em `nomeUF` para UF. A safra também é limitada no servidor por
`AnoEmissao` e `MesEmissao`; os dois filtros são conferidos novamente no client para evitar
que uma resposta inconsistente da fonte atravesse o parser. Filtros por
programa e tipo de seguro permanecem client-side.

Retry com backoff exponencial (6 tentativas, timeout read 120s).
A API retorna HTTP 500 de forma intermitente. Desde v0.8.0, o agrobr
utiliza Base dos Dados (BigQuery) como fallback automatico quando a API
OData falha. Instale com `pip install agrobr[bigquery]`. O fallback vale so para
`agregacao="uf"` sem `programa` nem `tipo_seguro`: a tabela agrega por municipio e nao traz
programa, fonte de recursos, tipo de seguro, modalidade nem atividade. Nos outros casos, o OData
fora levanta `SourceUnavailableError`, com o motivo na mensagem.

## SGS — Series Temporais

O SGS publica observações por código em `api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json`, sem autenticação. A resposta de observações contém data e valor (algumas séries, como a TR, trazem também `dataFim`, o fim do período), sem catálogo de frequência, unidade ou contagem global.

O [catálogo oficial da série 1](https://dadosabertos.bcb.gov.br/dataset/1-taxa-de-cambio---livre---dolar-americano-venda---diario) informa limite de dez anos para consultas diárias, vigente desde 26/03/2025. O agrobr divide intervalos em blocos encerrados por ano civil e valida a união. A rota `/ultimos/N` da série 1 limita a 20; para cortes maiores, use datas e `ultimos` na [API SGS](../api/bcb.md#sgs).

```python
from agrobr import bcb

df, meta = await bcb.sgs(
    1, inicio="01/01/2010", fim="31/12/2024", return_meta=True,
)
ipca = await bcb.sgs("ipca", inicio="01/01/2024", fim="31/12/2024")
```

A consulta do exemplo devolve 3.767 observações diárias em 2010–2024, em dois blocos. A fonte também pode devolver referências mensais e trimestrais anteriores ao limite diário solicitado e repetir uma referência mensal entre janelas diárias disjuntas. A implementação preserva as referências publicadas, diagnostica os limites e só reconcilia valores iguais entre blocos. Ela não infere frequência nem preenche datas.

O [contrato 3.0](../contracts/bcb_sgs.md) mantém `data`, `valor`, `codigo` e `nome_serie`, inclusive em vazio, e acrescenta `data_fim` quando a série publica `dataFim`. Os 17 aliases continuam disponíveis; outros códigos inteiros podem ser consultados sem receber um nome inventado. O IPCA pode ser negativo. Unidade monetária e frequência devem ser verificadas no cadastro específico; um código de câmbio histórico não implica BRL em toda a série.

Metadados registram cada resposta, hash, status, aquisição UTC e referências fora da janela. Todos os blocos obtidos não comprovam completude da série, pois o endpoint não informa total independente. O 404 oficial de ausência de valores não comprova existência do código. A [licença verificada](../licenses.md#bcb-sgs) ODbL pertence ao catálogo da série 1; não foi generalizada para qualquer código.

---

## PTAX — Cotações, moedas e boletins

O [conjunto oficial de boletins diários](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios) oferece cotações, paridades e metadados de moedas. `bcb.ptax` passa a selecionar moeda e `fechamento`, `todos`, `abertura` ou `intermediario`. O fechamento USD permanece como padrão. `bcb.ptax_moedas` expõe o catálogo corrente `Moedas`.

O catálogo `Moedas` traz AUD, CAD, CHF, DKK, EUR, GBP, JPY, NOK, SEK e USD. Essas entradas descrevem o serviço OData, sem comprovar todo o universo histórico de moedas. A [tabela geral do portal](https://ptax.bcb.gov.br/ptax_internet/consultarTabelaMoedas.do?method=consultaTabelaMoedas) contém outros códigos e datas de exclusão; é uma família distinta.

```python
from agrobr import bcb

moedas = await bcb.ptax_moedas()
df, meta = await bcb.ptax(
    moeda="EUR", boletim="todos",
    inicio="03/09/2026", fim="06/09/2026", return_meta=True,
)
```

Os [contratos de cotações 2.0 e catálogo 1.0](../contracts/bcb_ptax.md) conservam oito e três colunas, respectivamente. As cotações preservam quatro medidas, moeda, texto do boletim, horário e data civil. Não há conversão monetária nem agregação entre boletins.

**Distinções entre rotas:** o fechamento USD genérico coincidiu com a rota dólar nos recortes recentes e de junho de 1994. A rota por dia denomina o fechamento `Fechamento PTAX`; a de período usa `Fechamento`, com os mesmos valores e horários. O seletor reconhece ambos e a saída conserva o texto original. Num mesmo intervalo, a rota específica de fechamento trouxe um fechamento onde a genérica trouxe dois; a rota específica de abertura/intermediário trouxe apenas o último intermediário. Elas não substituem as rotas genéricas na implementação.

Horários fracionários permanecem distintos: intermediário e fechamento podem compartilhar o mesmo segundo e diferir nos microssegundos. `data_hora` continua sem fuso, com dtype ns; `data` é seu dia civil. A aquisição UTC é registrada separadamente. Cotações se referem à unidade monetária doméstica da data histórica; o SDK não rotula todos os valores como BRL. Paridades tipo A expressam moeda/USD; tipo B expressa USD/moeda, com contexto do catálogo preservado.

Nas consultas conhecidas, fim de semana e seleções não suportadas ZZZ/ARS devolveram o mesmo envelope vazio. Por isso, a seleção da moeda é validada no catálogo adquirido antes de consultar as cotações. A coleta usa páginas ordenadas e valida todos os registros antes do filtro de boletim. As consultas conhecidas não trouxeram contagem independente; a página vazia terminal deixa cobertura desconhecida. Count/nextLink, se vierem, são validados, sem garantia de revisão atômica.

O conjunto e os recursos [moedas](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios/resource/9d07b9dc-c2bc-47ca-af92-10b18bcd0d69), [dia](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios/resource/db9b40bf-9b8f-47c4-a82d-3a3afab52e90) e [período](https://dadosabertos.bcb.gov.br/dataset/taxas-de-cambio-todos-os-boletins-diarios/resource/0439af6a-d9be-4bf7-bf1a-60583e5f4c1c) declaram ODbL; veja [licenças](../licenses.md#bcb-ptax). CSV de todas as moedas, exclusões e histórico de revisões ficam fora desta API. Consulte [parâmetros, erros, tipos e proveniência](../api/bcb.md#ptax).

---

## Focus — Expectativas de Mercado

O [conjunto oficial Expectativas de Mercado](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado) publica estatísticas agregadas de previsões de participantes da pesquisa. O catálogo descreve cálculo diário e divulgação no primeiro dia útil da semana. O módulo consulta o histórico estatístico pelo OData, com entidade anual ou mensal.

| Periodicidade | Entidade |
|---------------|----------|
| anual, padrão | `ExpectativasMercadoAnuais` |
| mensal | `ExpectativaMercadoMensais` |

```python
from agrobr import bcb

df, meta = await bcb.focus(
    "IPCA", periodicidade="mensal", inicio="2026-08-28",
    top=100, max_registros=30, return_meta=True,
)
```

A API mantém dez colunas e acrescenta `periodicidade` e `indicador_detalhe`, com [contrato 2.0](../contracts/bcb_focus.md). A data da pesquisa e o horizonte textual da previsão são distintos. O detalhe anual preserva, por exemplo, Exportações, Importações e Saldo de Balança comercial. Bases 0/1 com mesma data e referência permanecem separadas. A função não oferece um catálogo genérico de indicadores nem garante que todo indicador exista nas duas entidades.

Nas consultas conhecidas, com desempate por referência/base/detalhe, uma página de seis registros coincidiu com duas páginas de três nas duas entidades, e não veio contagem independente nem nextLink: `$count=true` foi ignorado, `$inlinecount` foi recusado e `/$count` retornou 403. Metadados distinguem limite local e encerramento observado de completude. A união pode sofrer revisões concorrentes; não é um snapshot atômico.

O catálogo omite períodos sem estatísticas. Uma resposta vazia não valida o indicador; a consulta minúscula `ipca` foi vazia no recorte em que `IPCA` tinha dados. A [API](../api/bcb.md#focus) documenta regras de seleção, paginação, tipos, erros e avisos estatísticos, sem preencher períodos ou inferir unidades.

O catálogo e os recursos [mensal](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado/resource/c26059cc-2b28-41c5-a258-88a7c2b664d0) e [anual](https://dadosabertos.bcb.gov.br/dataset/expectativas-mercado/resource/57d46ecb-4d27-45e9-8145-c173e0b94ff5) declaram ODbL; veja [licenças](../licenses.md#bcb-focus). Outras entidades, como trimestral, Selic e Top5, permanecem fora deste seletor. Microdados institucionais também ficam fora.

---

## Fonte

- API SICOR: `https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata`
- API SGS: `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados`
- API PTAX: `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`
- API Focus: `https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/`
- Atualizacao: mensal (SICOR), várias vezes ao dia (PTAX), variável por série (SGS); Focus: cálculo diário e publicação semanal
- Historico: 2013+ (SICOR), variavel (SGS)
- Contratos: SGS 3.0; Focus, PTAX cotações e crédito rural 2.0; PTAX moedas 1.0; consulte cada API

## Produtos e respostas vazias

O filtro SICOR usa igualdade exata com o nome publicado, incluindo as aspas duplas literais. Isso exclui milho silagem das consultas de milho e trigo silagem ou sarraceno das consultas de trigo. A API normaliza aliases como `cafe`, `feijao`, `algodao`, `cana` e `mandioca` para a grafia da fonte no filtro e publica em `produto` a chave pedida, sem acento e em minúsculas. `cafe_arabica` e `cafe_conilon` foram removidos: o SICOR consultado não distingue esses tipos; use `cafe`.

A finalidade `custeio` usa produtos agrícolas. `investimento` usa itens como BOVINOS, CAFÉ, CANA-DE-AÇUCAR, BANANA e tratores; soja e milho podem não ter registros. Uma consulta válida sem registros devolve DataFrame vazio com o contrato de crédito rural 2.0 e um aviso explicativo, não `ParseError`.
