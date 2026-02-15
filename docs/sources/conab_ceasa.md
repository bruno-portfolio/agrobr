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

## Produtos Monitorados

| Categoria | Quantidade | Exemplos |
|-----------|------------|----------|
| Frutas | 20 | Abacaxi, Banana Nanica, Laranja Pera, Manga, Melancia, Tomate, Uva |
| Hortalicas | 28 | Alface, Batata, Cebola, Cenoura, Mandioca, Milho Verde, Ovos, Repolho |

**Unidades:** KG (maioria), UN (abacaxi, coco verde, couve-flor), DZ (alface, ovos)

## CEASAs Cobertas

43 CEASAs em 21 UFs, incluindo:
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

- Apenas precos mais recentes (snapshot diario, sem serie temporal nesta versao)
- Datas variam por CEASA (algumas inativas desde 2023)
- Credenciais Pentaho embutidas no frontend publico, mas API nao documentada oficialmente
- Corrupcao textual ocasional nos headers (ex: "ARACAT UBA" -> "ARACATUBA")

## Cache e Atualizacao

- **TTL:** 4 horas (precos atualizados diariamente)
- Recomendado: usar uma vez por dia para snapshot de precos

## Links

- [Portal de Informacoes CONAB](https://portaldeinformacoes.conab.gov.br/mercado-atacadista-hortigranjeiro.html)
- [CONAB](https://www.gov.br/conab/pt-br)
