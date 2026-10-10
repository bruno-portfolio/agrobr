# custo_sociobiodiversidade v1.0

Custos extrativistas da CONAB com valores e unidades publicados. Contrato ativo: `CONAB_SOCIOBIO_V1`. Sem conversão por hectare/safra/unidade de produto. Filtre por `unidade_valor` antes de comparar valores. O dataset usa catálogo e parser próprios; custos agrícolas permanecem em `custo_producao`.

**Chave primária:** nenhuma. Seções, itens e totais publicados são preservados.

## Produtos

20 produtos, conforme a aba oficial:

| Código | Produto |
|---|---|
| `acai` | Açaí |
| `andiroba` | Andiroba (amêndoa) |
| `babacu` | Babaçu (amêndoa) |
| `baru` | Baru (amêndoa) |
| `borracha` | Borracha |
| `buriti` | Buriti (fruto) |
| `cacau` | Cacau |
| `carnauba` | Carnaúba |
| `castanha_do_brasil` | Castanha-do-brasil |
| `fava_danta` | Fava d'anta |
| `jucara` | Juçara (fruto) |
| `licuri` | Licuri |
| `macauba` | Macaúba |
| `mangaba` | Mangaba |
| `murumuru` | Murumuru |
| `pequi` | Pequi |
| `piacava` | Piaçava |
| `pirarucu` | Pirarucu |
| `pinhao` | Pinhão de Araucária |
| `umbu` | Umbu |

## Seleção e cobertura

`conab.catalogo_sociobiodiversidade()` lista os 37 recursos capturados e marca os 20 links ativos em `ativo`. Com produto, inventaria o arquivo apontado pela aba oficial. `planilha=` seleciona uma revisão histórica exata. Mais de um recurso ativo para um produto causa erro explícito; revisões nunca são mescladas.

`uf`, `ano`, `local` e `aba` filtram os contextos publicados. Todas as abas identificadas correspondentes são retornadas na ordem do workbook. Se uma aba não resolvida puder corresponder aos filtros, a consulta falha; use `aba` exata para escolher um contexto conhecido. `ano` é o primeiro ano da safra publicada. `safra_publicada` conserva o token literal com barra (`2018/19`, `2016/2017`, inclusive `2018/18`); fica nula quando há um único ano publicado. Nome do arquivo e data de preços não completam safra ausente. O inventário de contexto não valida todas as células do corpo.

Locais em forma de região ou sem UF terminal reconhecida conservam o texto publicado, retirando apenas o prefixo `REGIÃO:` e espaços nas bordas. Parênteses descritivos permanecem em `local`, inclusive eventual texto de UF dentro da descrição. A UF então vem do nome da aba, registrada em `celulas_contexto["uf_origem"] = "nome_da_aba"`. UF reconhecida no cabeçalho tem precedência. Nome do arquivo e data de preços nunca suprem ano de safra ausente.

## Revisões e percentuais

O parser 2 preserva a distinção entre percentual numérico com formato Excel `%`
(multiplicado por 100) e número textual ou já percentual (sem essa escala).
Cabeçalhos monetários preservam a unidade publicada, normalizando apenas espaços.
O contrato permanece 1.0. Três medidas monetárias, colunas extras sem cabeçalho,
erro Excel e safra ausente continuam sendo recusados, sem saída parcial.

`planilha="acai_serie_historica_2008-2024.xlsx"` alcança a revisão arquivada,
inclusive a aba `Codajás-AM-2008` com `ano=2008`. Sem `planilha`, a revisão
ativa é escolhida pelo link oficial; pedir um ano antigo não troca automaticamente
para o arquivo arquivado. Nos arquivos atuais dos 20 produtos e nessa revisão de
açaí, os layouts antigos e novos têm duas colunas monetárias; nenhum publica uma
única coluna. Nas outras 16 revisões arquivadas do catálogo, a leitura não é garantida.

## Schema

| Coluna | Tipo | Nulo | Descrição |
|---|---|---|---|
| `produto` | str | Não | Produto canônico identificado no catálogo oficial da família sociobiodiversidade. |
| `local` | str | Não | Local publicado no contexto da aba. |
| `uf` | str | Não | UF publicada; uso do nome da aba como fallback registrado na proveniência. |
| `ano` | int | Não | Primeiro ano da safra publicada no cabeçalho. |
| `safra_publicada` | str | Sim | Token literal de safra com barra; nulo quando há um ano único. |
| `sistema` | str | Não | Descrição publicada do sistema de produção ou extrativismo. |
| `tipo_relatorio` | str | Sim | Tipo de relatório publicado; nulo quando ausente. |
| `mes_ano_referencia` | str | Sim | Referência textual publicada, sem inventar um dia. |
| `data_precos` | datetime | Sim | Data publicada em célula datada de preços; nula quando só há referência textual. |
| `produtividade` | float | Sim | Produtividade publicada, sem conversão entre bases. |
| `unidade_produtividade` | str | Sim | Unidade literal publicada da produtividade, sem enum ou conversão. |
| `secao` | str | Sim | Cabeçalho publicado da seção, romano ou gestão da propriedade familiar; o primeiro total após o cabeçalho fecha a seção e a mantém; os totais seguintes (CUSTO ...), que somam várias seções, saem nulos. |
| `item` | str | Não | Rótulo publicado da linha, preservando espaços e sinais. |
| `tipo_linha` | str | Não | item, total ou seção; somar linhas indiscriminadamente duplica componentes. |
| `linha` | int | Não | Número físico da linha na aba, base 1. |
| `valor` | float | Sim | Valor da primeira coluna monetária na base publicada; sem conversão. |
| `unidade_valor` | str | Não | Cabeçalho literal da primeira coluna monetária, com espaços colapsados; consumidor filtra por esta coluna. |
| `valor_unidade_produto` | float | Sim | Valor da segunda coluna monetária na unidade publicada, quando presente. |
| `unidade_produto` | str | Sim | Cabeçalho literal da segunda coluna monetária, com espaços colapsados. |
| `participacao_pct` | float | Sim | Participação publicada; no layout novo a base é CV, no antigo a base é a publicada no cabeçalho genérico. |
| `participacao_ct_pct` | float | Sim | Participação publicada na base custo total, quando presente. |
| `planilha` | str | Não | Identificador exato do recurso selecionado no catálogo. |
| `aba` | str | Não | Nome literal da aba de origem. |

Pandas usa o dtype de texto padrão da versão instalada (`object` no pandas 2, `str` no 3), `Int64` nulável, `float64` e `datetime64[ns]`, na ordem acima. Percentuais Excel viram pontos percentuais somente quando a célula tem formato percentual. Zeros, negativos e nulos são preservados. Seções e totais não são recalculados. O primeiro total depois de um cabeçalho de seção fecha essa seção e conserva `secao` (por exemplo, `TOTAL DAS DESPESAS FINANCEIRAS (C)` em `III - DESPESAS FINANCEIRAS`); os totais seguintes, até o próximo cabeçalho, somam várias seções (`CUSTO VARIÁVEL (A+B+C=D)`, `CUSTO TOTAL (H+I=J)` e semelhantes) e saem com `secao` nula.

## Bases observadas

Cabeçalhos monetários dos 20 workbooks ativos, incluindo as duas colunas de valor. São rótulos observados, não um enum de bases permitidas.

| Produto | Cabeçalhos monetários publicados |
|---|---|
| acai | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| andiroba | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| babacu | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/safra` |
| baru | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/dia`; `R$/safra` |
| borracha | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/kg`; `R$/safra` |
| buriti | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/kg`; `R$/safra` |
| cacau | `CUSTO / kg`; `CUSTO / kg/ha`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/safra` |
| carnauba | `(R$/60 kg)`; `(R$/70 kg)`; `(R$/80 kg)`; `(R$/safra ano)`; `CUSTO / 15 kg`; `CUSTO / kg`; `CUSTO POR HA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/kg`; `R$/safra` |
| castanha_do_brasil | `CUSTO / 10 kg`; `CUSTO / hl`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/1 lata`; `R$/Safra`; `R$/ha`; `R$/safra`; `R$/safra/família (1 pessoa)` |
| fava_danta | `CUSTO / kg`; `CUSTO POR HA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra/pessoa` |
| jucara | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| licuri | `CUSTO / kg`; `CUSTO POR HA`; `R$/1 kg`; `R$/safra` |
| macauba | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| mangaba | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/Safra`; `R$/ha`; `R$/safra`; `R$/safra ano` |
| murumuru | `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/safra` |
| pequi | `CUSTO / 25 kg`; `CUSTO / 28 kg`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/1kg`; `R$/25 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| piacava | `CUSTO / 15 kg`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/15 @`; `R$/KG`; `R$/Safra`; `R$/extrativista/safra-ano`; `R$/ha`; `R$/safra`; `R$/safra ano` |
| pirarucu | `CUSTO / kg`; `CUSTO POR HA`; `R$/1 kg`; `R$/Safra/ano`; `R$/ano`; `R$/safra` |
| pinhao | `CUSTO / 15 kg`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/50 kg`; `R$/Safra`; `R$/ha`; `R$/safra` |
| umbu | `CUSTO / 1 kg`; `CUSTO / 25 kg`; `CUSTO / kg`; `CUSTO POR HA`; `CUSTO POR SAFRA`; `R$/1 kg`; `R$/25 kg`; `R$/Safra`; `R$/ha`; `R$/safra`; `R$/safra ano` |

Produtividade também conserva unidades literais, como `kg`, `kg/ha`, `kg/safra`, `kg/safra ano` e `kg/10 milheiros palha`. Não há equivalência presumida entre elas.

## Abas não reconhecidas

Inventário nominal dos recursos ativos capturados. `unresolved` indica falha na identificação de contexto/cabeçalho; `parse` indica contexto identificado, mas células do corpo sem representação segura. O catálogo expõe a razão completa dos contextos pendentes.

| Produto | Etapa | Abas | Motivo exato do erro |
|---|---|---|---|
| acai | parse | `Igarapé-Miri-PA-2008`; `Igarapé-Miri-PA-2009` | `Medida sem descrição na linha 49` |
| baru | unresolved | `Amêndoa-Iporá-GO-2010`; `Amêndoa-Iporá-GO-2011`; `Amêndoa-Pirenópolis-GO-2010`; `Amêndoa-Pirenópolis-GO-2011`; `Fruto-Pirenópolis-GO-2010`; `Fruto-Pirenópolis-GO-2011` | `Safra/local não reconhecidos: 'SAFRA'` |
| borracha | unresolved | `Sena Madureira-AC-2016` | `Título de custo ausente ou ambíguo` |
| castanha_do_brasil | parse | `Sena Madureira-AC-2009`; `Sena Madureira-AC-2010`; `Sena Madureira-AC-2011`; `Sena Madureira-AC-2012`; `Sena Madureira-AC-2013`; `Sena Madureira-AC-2014`; `Sena Madureira-AC-2015` | `Medida fora dos cabeçalhos mapeados em R22C5: 0` |
| castanha_do_brasil | parse | `Sena Madureira-AC-2016` | `Medida fora dos cabeçalhos mapeados em R13C5: 0` |
| piacava | unresolved | `Belmonte-BA-2011`; `Belmonte-BA-2012`; `Belmonte-BA-2013`; `Belmonte-BA-2014`; `Belmonte-BA-2015`; `Belmonte-BA-2016`; `Cairu-BA-2011`; `Cairu-BA-2012`; `Cairu-BA-2013`; `Cairu-BA-2014`; `Cairu-BA-2015`; `Cairu-BA-2016` | `Cabeçalhos monetários não reconhecidos: 3` |
| pirarucu | parse | `Carauari-AM-2022` | `Medida fora dos cabeçalhos mapeados em R34C5: 0` |
| pinhao | unresolved | `São Joaquim-SC-2015`; `São Joaquim-SC-2016` | `Coluna sem mapeamento em R8C4: '1 kg'` |
| umbu | parse | `Uauá-BA-2011` | `Medida sem descrição na linha 55` |

Números órfãos são recusados mesmo quando zero. Pinhão São Joaquim 2015/2016 publica uma coluna adicional `1 kg`, além das duas colunas monetárias representadas: ambas entram em `unresolved` com `Coluna sem mapeamento em R8C4`, um limite de representação, não recusa da base monetária. A coluna extra não é descartada nem convertida.


Inventário: **955 contextos/cabeçalhos identificados e 21 abas pendentes nominais**, em 976 abas de dados. Seis abas de baru publicam apenas `SAFRA`, sem ano; uma de borracha tem título de custo não reconhecido; duas de pinhão mantêm uma limitação conhecida. As outras 12 são piaçava Belmonte/Cairu 2011–2016: contexto geográfico/safra reconhecido, mas três colunas monetárias publicadas excedem a representação de duas colunas. Por exemplo, Belmonte 2016 publica `R$/Safra`, `R$/15 @` e `R$/KG`. Nenhuma é descartada ou convertida. As 12 recusas de parse do corpo continuam separadas.

## Abas cujo nome diverge do cabeçalho

O cabeçalho publicado determina `local`, `uf` e `ano`; `aba` conserva o nome literal. `celulas_contexto["conflito_rotulo"]` registra `nome da aba: …` quando o nome diverge. A tabela inclui diferenças de grafia, abreviação e abrangência; divergência por si só não comprova erro factual. Em Juçara 2025, os dois rótulos geográficos estão cruzados: os valores permanecem associados ao contexto publicado, sem afirmar qual atribuição é factualmente correta.

| Produto | Nome da aba | Local / UF / ano publicados |
|---|---|---|
| acai | `Abaetuba-PA-2016` | Abaetetuba / PA / 2016 |
| acai | `Abaetuba-PA-2017` | ABAETETUBA / PA / 2017 |
| acai | `Abaetuba-PA-2018` | Abaetetuba / PA / 2018 |
| acai | `Abaetuba-PA-2019` | Abaetetuba / PA / 2019 |
| acai | `Abaetuba-PA-2020` | Abaetetuba / PA / 2020 |
| acai | `Abaetuba-PA-2021` | Abaetetuba / PA / 2021 |
| acai | `Abaetuba-PA-2022` | Abaetetuba / PA / 2022 |
| acai | `Cametá-PA-2009` | Cametá / PA / 2008 |
| acai | `Igarapé-Miri-PA-2015` | Igarapé - Miri / PA / 2015 |
| acai | `Igarapé-Miri-PA-2016` | Igarapé - Miri / PA / 2016 |
| andiroba | `Santarém (F. Tapajós) -PA-2018` | Santarém / PA / 2018 |
| andiroba | `Santarém (F. Tapajós) -PA-2019` | Santarém / PA / 2019 |
| andiroba | `Santarém (F. Tapajós) -PA-2020` | Santarém / PA / 2020 |
| andiroba | `Santarém (F. Tapajós) -PA-2021` | Santarém / PA / 2021 |
| babacu | `Vargem Grande-MA-2010` | Vargem Grande / MA / 2008 |
| babacu | `S. Miguel do TO-TO-2011` | São Miguel / TO / 2011 |
| babacu | `S. Miguel do TO-TO-2012` | São Miguel / TO / 2012 |
| babacu | `S. Miguel do TO-TO-2013` | São Miguel / TO / 2013 |
| babacu | `S. Miguel do TO-TO-2014` | São Miguel / TO / 2014 |
| babacu | `S. Miguel do TO-TO-2015` | São Miguel / TO / 2015 |
| babacu | `S. Miguel do TO-TO-2016` | SÃO MIGUEL DO TOCANTINS / TO / 2016 |
| babacu | `S. Miguel do TO-TO-2018` | São Miguel do Tocantins / TO / 2018 |
| baru | `Amêndoa-B. Jardim de GO-GO-2018` | BOM JARDIM DE GOIÁS / GO / 2018 |
| baru | `Amêndoa-B. Jardim de GO-GO-2019` | Bom Jardim de Goiás / GO / 2019 |
| baru | `Amêndoa-B. Jardim de GO-GO-2020` | Bom Jardim de Goiás / GO / 2020 |
| baru | `Amêndoa-B. Jardim de GO-GO-2021` | Bom Jardim de Goiás / GO / 2021 |
| baru | `Amêndoa-B. Jardim de GO-GO-2022` | Bom Jardim de Goiás / GO / 2022 |
| baru | `Amêndoa-B. Jardim de GO-GO-2023` | Bom Jardim de Goiás / GO / 2023 |
| baru | `Amêndoa-B. Jardim de GO-GO-2024` | Bom Jardim de Goiás / GO / 2024 |
| baru | `Amêndoa-B. Jardim de GO-GO-2025` | Bom Jardim de Goiás / GO / 2025 |
| baru | `Amêndoa-Poconé-MT-2010` | Poconé/S.Sra.do Livramento / MT / 2010 |
| baru | `Amêndoa-Poconé-MT-2011` | Poconé/S.Sra.do Livramento / MT / 2011 |
| baru | `Amêndoa-Poconé-MT-2012` | Poconé/S.Sra.do Livramento / MT / 2012 |
| baru | `Amêndoa-Poconé-MT-2013` | Poconé/S.Sra.do Livramento / MT / 2013 |
| baru | `Amêndoa-Poconé-MT-2014` | Poconé/S.Sra.do Livramento / MT / 2014 |
| baru | `Amêndoa-Poconé-MT-2015` | Poconé/S.Sra.do Livramento / MT / 2015 |
| baru | `Amêndoa-Poconé-MT-2016` | Poconé/S.Sra.do Livramento / MT / 2016 |
| buriti | `Fruto-Buritizeiro-MG-2015` | Burutizeiro / MG / 2015 |
| buriti | `Fruto-Buritizeiro-MG-2016` | Burutizeiro / MG / 2016 |
| buriti | `Fruto-Buritizeiro-MG-2017` | Burutizeiro / MG / 2017 |
| buriti | `Polpa-Buritizeiro-MG-2013` | Buritizeiro / MG / 2012 |
| buriti | `Polpa-Buritizeiro-MG-2015` | Burutizeiro / MG / 2015 |
| buriti | `Polpa-Buritizeiro-MG-2016` | Burutizeiro / MG / 2016 |
| buriti | `Polpa-Buritizeiro-MG-2017` | Burutizeiro / MG / 2017 |
| buriti | `Fruto-Igarapé-Miri-PA-2015` | Igarapé - Miri / PA / 2015 |
| buriti | `Fruto-Igarapé-Miri-PA-2016` | Igarapé - Miri / PA / 2016 |
| buriti | `Fruto-Igarapé-Miri-PA-2017` | Igarapé - Miri / PA / 2017 |
| buriti | `Fruto-Iagarapé-Miri-PA-2019` | Igarapé-Miri / PA / 2019 |
| carnauba | `Pó Cerífero-Campo Maior-PI-2016` | CAMPO MAIOR PI / PI / 2016 |
| carnauba | `Pó Cerífero-Piripiri-PI-2016` | PIRIPIRI PI / PI / 2016 |
| carnauba | `Cera-Açu-RN-2015` | ASSÚ / RN / 2015 |
| carnauba | `Cera-Açu-RN-2016` | ASSÚ / RN / 2016 |
| carnauba | `Pó-Cerífero-Açu-RN-2014` | ASSÚ / RN / 2014 |
| carnauba | `Pó-Cerífero-Açu-RN-2015` | ASSÚ / RN / 2015 |
| carnauba | `Pó-Cerífero-Açu-RN-2016` | ASSÚ / RN / 2016 |
| carnauba | `Pó Cerífero-Mossoró-RN-2009` | MOSSORÓ/APODI / RN / 2009 |
| carnauba | `Pó Cerífero-Mossoró-RN-2010` | MOSSORÓ/APODI / RN / 2010 |
| carnauba | `Pó Cerífero-Mossoró-RN-2011` | MOSSORÓ/APODI / RN / 2011 |
| carnauba | `Pó Cerífero-Mossoró-RN-2012` | MOSSORÓ/APODI / RN / 2012 |
| carnauba | `Pó Cerífero-Mossoró-RN-2013` | MOSSORÓ/APODI / RN / 2013 |
| jucara | `Fruto-Três Cachoeiras-RS-2025` | Ubatuba / SP / 2025 |
| jucara | `Fruto-Ubatuba-SP-2023` | TRÊS CACHOEIRAS / RS / 2023 |
| jucara | `Fruto-Ubatuba-SP-2025` | Três Cachoeiras / RS / 2025 |
| macauba | `Mirabela-MG_2014` | MIRABELA / MG / 2013 |
| macauba | `Corumbá-MS-2014` | CORUMBÁ / MS / 2013 |
| mangaba | `Barra dos Coqueiros-SE-2010` | Barra do Coqueiros / SE / 2010 |
| mangaba | `Barra dos Coqueiros-SE-2011` | Barra do Coqueiros / SE / 2011 |
| mangaba | `Barra dos Coqueiros-SE-2012` | Barra do Coqueiros / SE / 2012 |
| mangaba | `Barra dos Coqueiros-SE-2013` | Barra do Coqueiros / SE / 2013 |
| mangaba | `Barra dos Coqueiros-SE-2014` | Barra do Coqueiros / SE / 2014 |
| murumuru | `Carauari-AM-2018` | CARAUARI-AM (Comunidade do Roque) / AM / 2018 |
| pequi | `Crato-CE-2008` | Crato - CE (Distrito Horizonte(Cacimba) - Município : Jardim / CE / 2008 |
| pequi | `Crato-CE-2010` | Crato - CE (Distrito Horizonte(Cacimba) - Município : Jardim / CE / 2010 |
| pequi | `Crato-CE-2011` | Crato - CE (Distrito Horizonte(Cacimba) - Município : Jardim / CE / 2011 |
| pequi | `Crato-CE-2012` | Jardim/Crato / CE / 2012 |
| pequi | `Crato-CE-2013` | Jardim/Crato / CE / 2013 |
| pequi | `Crato-CE-2014` | Jardim/Crato / CE / 2014 |
| pequi | `Crato-CE-2015` | Jardim/Crato / CE / 2015 |
| pequi | `Janpovar-MG-2008` | Japonvar / MG / 2008 |
| pequi | `Janpovar-MG-2010` | Japonvar / MG / 2010 |
| pequi | `Janpovar-MG-2011` | Japonvar / MG / 2011 |
| pequi | `Janpovar-MG-2012` | Japonvar / MG / 2012 |
| pequi | `Janpovar-MG-2013` | Japonvar / MG / 2013 |
| pequi | `Janpovar-MG-2014` | Japonvar / MG / 2014 |
| pequi | `Janpovar-MG-2015` | Japonvar / MG / 2015 |
| pequi | `Janpovar-MG-2016` | Japonvar / MG / 2016 |
| pequi | `Janpovar-MG-2017` | JAPONVAR / MG / 2017 |
| pequi | `Janpovar-MG-2018` | Japonvar / MG / 2018 |
| pequi | `Janpovar-MG-2019` | Japonvar / MG / 2019 |
| pequi | `Janpovar-MG-2020` | Japonvar / MG / 2020 |
| pequi | `Janpovar-MG-2021` | Japonvar / MG / 2021 |
| pequi | `Janpovar-MG-2022` | Japonvar / MG / 2022 |
| pequi | `Janpovar-MG-2023` | Japonvar / MG / 2023 |
| pequi | `Janpovar-MG-2024` | Japonvar / MG / 2024 |
| pequi | `Janpovar-MG-2025` | Japonvar / MG / 2025 |
| pequi | `Poconé-MT-2010` | Poconé\Ns.Sra.Livramento / MT / 2010 |
| pequi | `Poconé-MT-2011` | Poconé\Ns.Sra.Livramento / MT / 2011 |
| pequi | `Poconé-MT-2012` | Poconé\Ns.Sra.Livramento / MT / 2012 |
| pequi | `Poconé-MT-2013` | Poconé\Ns.Sra.Livramento / MT / 2013 |
| pequi | `Poconé-MT-2014` | Poconé\Ns.Sra.Livramento / MT / 2014 |
| pequi | `Poconé-MT-2015` | Poconé\Ns.Sra.Livramento / MT / 2015 |
| pequi | `Poconé-MT-2016` | Poconé\Ns.Sra.Livramento / MT / 2016 |
| pequi | `N. S. do Livramento-MT-2019` | Nossa Senhora do Livramento / MT / 2019 |
| pequi | `N. S. do Livramento-MT-2020` | Nossa Senhora do Livramento / MT / 2020 |
| pequi | `N. S. do Livramento-MT-2021` | Nossa Senhora do Livramento / MT / 2021 |
| pequi | `N. S. do Livramento-MT-2022` | Nossa Senhora do Livramento / MT / 2022 |
| pequi | `N. S. do Livramento-MT-2023` | Nossa Senhora do Livramento / MT / 2023 |
| pequi | `N. S. do Livramento-MT-2024` | Nossa Senhora do Livramento / MT / 2024 |
| pirarucu | `Tefé-AM-2015` | Reservas de Mamirauá e Maraã - Tefé / AM / 2015 |
| pirarucu | `Tefé-AM-2016` | Reservas de Mamirauá e Maraã - Tefé / AM / 2016 |
| pirarucu | `Tefé-AM-2024` | Tefé / AM / 2023 |
| pinhao | `S. J. dos Pinhais-PR-2012` | São José dos Pinhais / PR / 2012 |
| pinhao | `S. J. dos Pinhais-PR-2013` | São José dos Pinhais / PR / 2013 |
| pinhao | `S. J. dos Pinhais-PR-2014` | São José dos Pinhais / PR / 2014 |
| pinhao | `S. J. dos Pinhais-PR-2015` | São José dos Pinhais / PR / 2015 |
| pinhao | `S. J. dos Pinhais-PR-2016` | São José dos Pinhais / PR / 2016 |
| pinhao | `S. J. dos Pinhais-PR-2017` | São José dos Pinhais / PR / 2016 |
| pinhao | `S. F. de Paula-RS-2017` | SÃO FRANCISCO DE PAULA / RS / 2017 |
| pinhao | `S. F. de Paula-RS-2019` | São Francisco de Paula / RS / 2019 |
| pinhao | `S. F. de Paula-RS-2020` | São Francisco de Paula / RS / 2020 |
| pinhao | `S. F. de Paula-RS-2021` | São Francisco de Paula / RS / 2021 |
| pinhao | `S. F. de Paula-RS-2022` | São Francisco de Paula / RS / 2022 |
| pinhao | `S. F. de Paula-RS-2023` | São Francisco de Paula / RS / 2023 |
| pinhao | `S. F. de Paula-RS-2024` | São Francisco de Paula / RS / 2024 |
| umbu | `S. M. do Gostoso-RN-2017` | São Miguel do Gostoso / RN / 2017 |
| umbu | `S. M. do Gostoso-RN-2018` | São Miguel do Gostoso / RN / 2018 |
| umbu | `S. M. do Gostoso-RN-2019` | São Miguel do Gostoso / RN / 2019 |
| umbu | `S. M. do Gostoso-RN-2020` | São Miguel do Gostoso / RN / 2020 |
| umbu | `S. M. do Gostoso-RN-2021` | São Miguel do Gostoso / RN / 2021 |
| umbu | `S. M. do Gostoso-RN-2022` | São Miguel do Gostoso / RN / 2022 |
| umbu | `S. M. do Gostoso-RN-2023` | São Miguel do Gostoso / RN / 2023 |
| umbu | `S. M. do Gostoso-RN-2024` | São Miguel do Gostoso / RN / 2024 |

## Cache e proveniência

Cache do catálogo: 1 h em processo, isolado dos custos agrícolas. `use_cache=False` ignora leitura e gravação. Workbooks são baixados a cada consulta. Metadados conservam recibos, SHA-256 dos bytes, contextos selecionados, células físicas dos valores, schema 1.0 e fonte selecionada. Há suporte a `as_polars=True`, `return_meta=True` e ao wrapper sync; não há snapshot imutável e `deterministic` é recusado antes da rede. Licença: dados públicos federais CONAB (`livre`), conforme [licenças das fontes](../licenses.md).

## Exemplo

```python
from agrobr import conab, datasets

catalog = await conab.catalogo_sociobiodiversidade("acai")
df, meta = await datasets.custo_sociobiodiversidade(
    "acai", uf="AM", ano=2024, return_meta=True
)
old = await datasets.custo_sociobiodiversidade("acai", uf="AM", ano=2008)
carnauba = await datasets.custo_sociobiodiversidade("carnauba", ano=2022)
babacu = await datasets.custo_sociobiodiversidade("babacu", ano=2018)
```
