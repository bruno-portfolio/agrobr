# CONAB CEASA/PROHORT

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Provedor** | CONAB — Companhia Nacional de Abastecimento |
| **Dados** | Precos diarios de atacado hortifruti em CEASAs |
| **Acesso** | Pentaho CDA REST API (JSON) |
| **Formato** | JSON (doQuery endpoint) |
| **Autenticacao** | Credenciais publicas embutidas no frontend |
| **Licenca** | zona_cinza |
| **Frequencia** | Diaria |

## Origem dos Dados

O sistema PROHORT (Programa Brasileiro de Modernizacao do Mercado Hortigranjeiro) da CONAB coleta precos diarios de atacado de hortifruti em 43 CEASAs (Centrais de Abastecimento) do Brasil. Os dados alimentam o dashboard publico do Portal de Informacoes da CONAB.

O agrobr acessa os dados via Pentaho BA Server (backend do portal), usando a API CDA doQuery para obter a matriz de precos (48 produtos x 43 CEASAs) em formato JSON.

A credencial vai no cabeçalho `Authorization: Basic`, nunca na URL: a URL registrada (log do httpx, erro, `MetaInfo`)
não a leva. O padrão é a credencial pública do portal; uma credencial própria vem de `AGROBR_CONAB_CEASA_USER` e
`AGROBR_CONAB_CEASA_PASS` e segue o mesmo caminho.

## Produtos Monitorados

| Categoria | Quantidade | Exemplos |
|-----------|------------|----------|
| Frutas | 21 | Abacaxi, Banana Nanica, Coco Verde, Laranja Pera, Manga, Melancia, Uva |
| Hortalicas | 26 | Alface, Batata, Cebola, Cenoura, Mandioca, Milho Verde, Repolho, Tomate |
| Ovos | 1 | Ovos |

Frutas e hortaliças seguem os grupos do painel oficial "Hortaliças e Frutas" do PROHORT (Portal de Informações da Conab); ovos ficam fora dos dois grupos oficiais. Brócolo, cará, couve, jiló, mandioquinha, quiabo e vagem não aparecem no painel e ficam em Hortaliças por classificação do agrobr. Produto novo no PROHORT, fora dessa tabela, sai com `categoria` nula e um aviso.

**Unidades:** KG (maioria), UN (abacaxi, coco verde, couve-flor), DZ (alface, ovos)

## CEASAs Cobertas

43 CEASAs em 20 UFs, incluindo:
- CEAGESP (12 unidades em SP)
- CEASAMINAS (3 unidades em MG)
- CEASAs estaduais (PR, RS, SC, RJ, BA, CE, GO, DF, etc.)

## Estrutura dos Dados

A API retorna uma matriz pivot (48 linhas x 44 colunas):
- Coluna 0: nome do produto com unidade (ex: "TOMATE (KG)")
- Colunas 1-43: preco por CEASA (null = nao comercializado)
- Headers das colunas contem data por CEASA (ex: "CEAGESP - SAO PAULO\r(13/02/2026)")

O parser unpivota a matriz para formato long-form com 7 colunas.

## Limitacoes

- Cada `colIndex` deve ser um inteiro igual à posição da coluna em `metadata`, inclusive a coluna do produto; índices ausentes, repetidos ou fora de ordem levantam `ParseError`.

- A CEASA de cada coluna de precos vem do cabecalho da propria coluna (`colName`), no formato `<instituicao> \r<cidade>\r(dd/mm/aaaa)/Preco (R$)`, com instituicao e cidade nao vazias, data valida e sem CEASA duplicada; fora disso, levanta `ParseError` em vez de atribuir o preco a outra praca. O catalogo `MDXceasa` deixou de listar todas as CEASAs com preco e nao e mais consultado. Resposta de precos sem a lista `resultset` tambem levanta `ParseError`; so `resultset` vazio vira tabela vazia.

- Apenas precos mais recentes (snapshot diario, sem serie temporal nesta versao)
- Datas variam por CEASA (algumas inativas desde 2023)
- Credenciais Pentaho embutidas no frontend publico, mas API nao documentada oficialmente
- Corrupcao textual ocasional nos headers (ex: "ARACAT UBA" -> "ARACATUBA")

## Cache e Atualizacao

- Não há cache local: cada chamada baixa os preços da CONAB.
- A fonte atualiza os preços diariamente; recomenda-se uma chamada por dia para obter o snapshot.

## Datasets

- [`preco_atacado`](../contracts/preco_atacado.md) — wraps `ceasa.precos()` (48+ produtos PROHORT)

## Links

- [Portal de Informacoes CONAB](https://portaldeinformacoes.conab.gov.br/mercado-atacadista-hortigranjeiro.html)
- [CONAB](https://www.gov.br/conab/pt-br)
