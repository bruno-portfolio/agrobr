# ANTT Pedágio

## Sobre a fonte

A ANTT publica recursos CSV e metadados no [catálogo oficial de dados abertos](https://dados.antt.gov.br/dataset/volume-trafego-praca-pedagio). A disponibilidade depende de ano e frequência. Há recursos mensais desde 2010; os recursos diários atuais cobrem 2024 em diante.

## Contrato de fluxo 3.0

A saída preserva 13 colunas e a modalidade de cobrança, categoria tarifária e frequência publicadas. A chave é `data`, `concessionaria`, `praca`, `sentido`, `tipo_veiculo`, `categoria_eixo`, `tipo_cobranca`, `frequencia`. Modalidades de cobrança não são juntadas. `n_eixos` exige evidência textual explícita; códigos históricos de categoria não são contagens físicas universais. Os rótulos saem como publicados, sem os espaços externos, e `sentido` sai em maiúsculas (`Crescente `, `Crescente` e `CRESCENTE` viram `CRESCENTE`): no CSV mensal de 2023, o volume total e o volume por sentido não mudam.

`frequencia="mensal"` e `"diaria"` selecionam famílias separadas de recursos. O client não substitui uma frequência ausente por outra. O catálogo determina ano/revisão selecionados; bytes, hashes e estatísticas descrevem a aquisição efetiva.

## Anomalias conhecidas da fonte

Nos CSVs mensais, 2020, 2021 e 2023 trazem `mes_ano` fora do dia 1 (fim de mês, série de preenchimento, digitação; 56, 3.258 e 734 linhas), atribuídos ao seu mês; 2013, 2015 e 2023 trazem volumes fracionários ou negativos (4, 3 e 36 linhas), excluídos; ECOSUL dez/2021 está publicado duas vezes com linhas idênticas e conta uma vez. CONCEBRA 2021–2023 publica linhas diárias rotuladas no dia 1 do mês, somadas no mês. Cada caso gera aviso e fica em `source_details`.

## Cadastro de praças

O cadastro corrente pode fornecer UF, rodovia e município por correspondência única de concessionária e praça, sem diferença de caixa nem de espaços (acento conta; nada de aproximação): `Ecosul`, como o tráfego de abril de 2021 publica, casa `ECOSUL` do cadastro. O nome anterior de uma concessionária casa o nome atual do cadastro quando a ANTT publica a troca e as praças mantêm o nome: `CRO` (Nova Rota do Oeste), `MSVIA` (Pantanal), `ECO050` (Ecovias Minas Goiás), `ECO101` (Ecovias Capixaba), `ECOPONTE` (Ecovias Ponte), `ECORIOMINAS` (Ecovias Rio Minas) e `AUTOPISTA FERNÃO DIAS` (Motiva Minas SP). A coluna `concessionaria` sai como o tráfego publica, e a geografia vem da praça: a CRO tem praças na BR-163 e na BR-364. Não reconstrói geografia histórica; em 2023, 35 pares (CONCEBRA, VIA 040, VIA BAHIA e praças que o cadastro não tem, como DELTA da ECO050) seguem sem vínculo. Com filtro de UF/rodovia, praça sem esse vínculo (em 2026, 15 pares em GO, MG e PR) sai do resultado com aviso. Se nenhuma praça do cadastro tem a UF/rodovia pedida, o resultado sai vazio com outro aviso, que diz isso, e não com erro de transporte. Na frequência mensal, `inicio` e `fim` usam o dia 1 do mês, e o erro diz qual dos dois e o valor. O cabeçalho oficial `municipal` alimenta `municipio`; a coluna canônica tem precedência quando ambas existem. No `pracas_pedagio`, a coluna `municipal` ainda sai, com `FutureWarning` e aviso no `MetaInfo`, e deixa de sair na próxima versão major: use `municipio`. Contrato do cadastro: 2.0.

## Transporte e orçamentos

Downloads são lidos em blocos para arquivos temporários em disco. CSV: 512 MiB; spool retido: 1 GiB; bytes transferidos em todas as tentativas: 3 GiB. A sessão é reutilizada dentro da aquisição, sem cache persistente de CSV. Redirecionamentos são rejeitados; HTML HTTP 200 de WAF/manutenção gera `SourceUnavailableError`. Arquivos são fechados em sucesso, falha e cancelamento. Limites de linhas/memória do parser estão na [referência da API](../api/antt_pedagio.md).

Contagens de veículos pesados podem apoiar estudos de transporte, mas não identificam a carga transportada.

## Licença e dicionário

CC-BY conforme declaração CKAN; não exige autenticação. Veja as [licenças](../licenses.md) e o [dicionário oficial](https://dados.antt.gov.br/dataset/5bf70ec3-b24e-4f73-99a0-78b200f5e915/resource/5cec4e90-24d4-4a43-84e1-121a422bbcd5/download/dicionario_dados_surod_volume-de-trafego-nas-pracas-de-pedagio.pdf).
