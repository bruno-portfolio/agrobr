# custo_producao v3.0

Custos de produção CONAB, preservando planilha, aba, referência de preços e cada item, subtotal e total publicado. O contrato ativo é `CONAB_CUSTOS_V3`, em `agrobr.contracts.conab_custos`.

Selecione uma planilha e uma aba sem ambiguidade com `planilha` e `aba`; seleções amplas informam candidatos em vez de escolher silenciosamente. `tecnologia` é qualificação de saída, não parâmetro.

## Produtos

O dataset aceita 8 produtos:

| Código | Produto |
|---|---|
| `soja` | Soja |
| `milho` | Milho |
| `arroz` | Arroz |
| `feijao` | Feijão |
| `trigo` | Trigo |
| `algodao` | Algodão |
| `cafe_arabica` | Café arábica |
| `cafe_conilon` | Café conilon |

Milho, arroz e feijão têm duas planilhas cada: milho de 1ª/2ª safra, arroz
irrigado/sequeiro e feijão de 1ª/2ª+3ª safras. Consulte
`conab.catalogo_custos(cultura)` e informe `planilha=`; depois consulte
`conab.catalogo_custos(cultura, planilha=...)` para escolher uma `aba=` reconhecida.
Os filtros devem identificar um único contexto.

Para café, informe `cafe_arabica` ou `cafe_conilon`. O pedido genérico `cafe`
informa essas alternativas. O catálogo da fonte também inclui outras culturas
agrícolas, além dos oito produtos do dataset.

## Cache

Cache do catálogo: **1 h em processo**. O parâmetro nomeado `use_cache=False` em `conab.catalogo_custos`, `conab.custo_producao` e `datasets.custo_producao` força nova leitura sem consultar ou atualizar o cache. As planilhas são baixadas a cada consulta. `meta.source_details["catalog_cache"]` informa `hit`, `miss` ou `bypass`; os recibos originais do catálogo ficam em `manifest.acquisition.catalog_acquisition`, separados das requisições da chamada atual.

## Leitura de planilhas e limites

O parser 5 lê custos agrícolas XLS/BIFF e XLSX com cabeçalhos mesclados.
Cada faixa de cabeçalho aceita no máximo um valor por linha; colisões,
medidas fora das colunas reconhecidas e erros do Excel causam recusa explícita.
As unidades monetárias permanecem publicadas, sem converter tonelada, saca ou hectare.
Abas antigas de trigo com três medidas monetárias ficam recusadas: o contrato
3.0 representa duas medidas e nenhuma terceira coluna é descartada.

Café arábica/conilon é identificado pelo recurso oficial, mesmo quando o
sistema imprime apenas `CAFÉ`. `safra` conserva tanto o ano único quanto o
biênio publicado. Referências textuais como `13/12/2013` continuam textuais;
`data_referencia` fica nula quando a célula não é uma data Excel.

Percentuais numéricos com formato Excel `%` são multiplicados por 100.
Números já expressos em percentual e números textuais, como `100,00`, não
recebem essa escala. Itens nulos e zeros permanecem distintos. CV, CT e demais
totais são as linhas publicadas, sem somar subtotais novamente. Cabeçalhos
repetidos, paginação, autoria e notas cambiais ficam nos diagnósticos, fora
das observações de custo. O contrato permanece 3.0.

### Subtotais, grupos e memorandos

Desde o parser 5, os itens de cada seção fecham com o subtotal publicado sempre que a planilha fecha:

- o cabeçalho romano (`I -` a `VI -`) abre a seção mesmo quando a planilha imprime zeros nele; os zeros ficam nos
  diagnósticos;
- linha de custo depois do subtotal ou do total da mesma seção, como o bloco "Gestão da propriedade familiar" depois do H
  ou do I, sai das observações e fica nos diagnósticos como `memo_after_total`;
- em cada seção vale a leitura que fecha com o subtotal publicado. O grupo `N - …` com subitens `N.M - …` (inclusive
  `' N.M`) entra só com os subitens, só com o grupo ou com os dois, e o agregado "Gestão da propriedade familiar" entra ou
  sai. A linha que sai fica nos diagnósticos (`group_header`, `group_component` ou `aggregate_row`) com o valor
  publicado, e `meta.source_details["parser"]["subtotal_checks"]` registra a leitura de cada seção;
- quando nenhuma leitura fecha, todas as linhas publicadas ficam, e sai um aviso (`UserWarning` e
  `meta.validation_warnings`) com o subtotal publicado, a soma dos itens e a diferença. O mesmo vale para os totais de
  fórmula, como `(E+F = G)`. O agrobr repassa os números publicados, sem recalcular.

Nas 11 séries de café, milho, algodão, soja, arroz, feijão e trigo, 45 subtotais ou totais não
fecham na própria planilha, por exemplo Barreiras-BA-2011 (algodão, B), Patrocínio-MG-2022 (café arábica, E) e S. Mateus
do Sul-PR-2008 (feijão, G e H).

### Categoria

`categoria` sai da seção publicada: `IV - DEPRECIAÇÕES` e `V - OUTROS CUSTOS FIXOS` dão `custos_fixos`; `II`, `III` e
`VI` dão `outros`. No custeio, o rótulo decide pelo mapa, com acento, hífen e plural normalizados. Assim,
"Mão-de-obra temporária c/encargos", "Mão-de-obra" e "Mão de obra" dão `mao_de_obra`; "Defensivos", "Agrotóxicos",
"Mudas de Café", "Sementes e mudas", "Semente de arroz" e "Royalties" dão `insumos`; "Tratores e Colheitadeiras" e
"Máquinas Próprias" dão `operacoes`. As linhas de total (`tipo_linha = "total"`) não herdam a seção. Rótulo de custeio
fora do mapa fica `outros` em todos os anos.

A irrigação muda de categoria na troca de layout. Na planilha antiga, ela vem dentro da linha única "Operação com
máquinas próprias" (cana, S. M. dos Campos-AL-2017), que dá `operacoes`; no layout novo, ela é o subitem "Conjunto de
Irrigação", que dá `outros`. Assim, a série de `operacoes` muda de conteúdo quando o layout muda.

### Abas identificadas e recusadas

Nas mesmas 11 séries, 116 abas têm o contexto reconhecido e o corpo recusado, sem quadro parcial:

| Série | Abas | Motivo e abas |
|---|---:|---|
| arroz sequeiro | 40 | cabeçalho `kg/sc 60 kg` não reconhecido: Inhumas-GO, Itapuranga-GO, Palmeiras-GO e Piranhas-GO 2007–2014; Bacabal-MA 2007 e 2009–2014; Esperantina-PI-2007 (`kg/sc 50 kg`) |
| feijão 1ª safra | 28 | cabeçalho `kg/sc 60 kg`: Brejo Santo-CE e Crateús-CE 2007–2014, Icó-CE 2012–2014; medida inválida (`#REF!` ou `.`): Estrela-RS 2008–2014, Canoinhas-SC-2013; medida sem descrição: Unaí-MG-2009 |
| trigo | 16 | coluna duplicada de custo por unidade: Toledo-PR 2002–2004, Ubiratã-PR 2005–2007, Londrina-PR 2002–2007, Cascavel-PR 2004–2007 |
| soja | 9 | medida em coluna sem cabeçalho: OGM-Toledo-PR-2008, Cruz Alta-RS 2008–2010; medida inválida (`#REF!`): 1-Ijuí-RS 2007–2010; rótulo fora das colunas: Sorriso-MT-2014 |
| milho 1ª safra | 8 | medida em coluna sem cabeçalho: Rio Verde-GO 2007–2009, Passo Fundo-RS 2008–2010; medida inválida (`#REF!`): Rio Verde-GO-2001; medida sem descrição: Passo Fundo-RS-2012 |
| café arábica | 7 | medida em coluna sem cabeçalho: Patrocínio-MG 2006–2008, Franca-SP 2006–2008, Três Pontas-MG-2022 |
| arroz irrigado | 7 | medida em coluna sem cabeçalho: Cachoeira do Sul-RS 2009–2010; medida inválida (`#REF!` ou `.`): Camaquã-RS 2014–2016, Massaranduba-SC-2013, Meleiro-SC-2013 |
| café conilon | 1 | medida em coluna sem cabeçalho: Ji-Paraná-RO-2014 |

Milho 2ª safra, algodão e feijão 2ª/3ª safras não têm aba recusada nessas séries. O cabeçalho de rendimento
`kg/sc 60 kg`, não reconhecido, responde por 59 das recusas.

A lista cobre só essas 11 séries. Outras culturas têm recusas pelos mesmos motivos, por exemplo
C. de Camaragibe-AL 2014–2016 e S. L. do Quitunde-AL-2017 na cana (medida em coluna sem cabeçalho) e
Cruz das Almas-BA 2008 e 2010–2013 na mandioca (cabeçalho `R$t` não reconhecido).

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `cultura` | str | Não | — | Cultura canônica do recurso oficial selecionado. |
| `uf` | str | Não | — | UF publicada no contexto; se ausente em LOCAL:, usa o nome da aba com origem registrada. |
| `safra` | str | Sim | — | Token de safra publicado, inclusive grafias anômalas; nunca derivado do ano da referência. |
| `tecnologia` | str | Sim | — | Qualificação alta/média/baixa quando explícita no sistema; nula se ausente. |
| `categoria` | str | Não | — | Classificação auxiliar pela seção publicada (IV e V: custos_fixos; II, III e VI: outros) e, no custeio, pelo rótulo normalizado; item e seção conservam os rótulos publicados. |
| `item` | str | Não | — | Descrição literal da linha, inclusive espaços, receitas e totais. |
| `unidade` | str | Não | — | Cabeçalho literal da coluna de custo por hectare. |
| `quantidade_ha` | float | Sim | — | Coeficiente físico por hectare, nulo quando não publicado; nunca derivado de custo. |
| `preco_unitario` | float | Sim | — | Preço unitário publicado, nulo quando ausente; nunca custo por unidade de produção. |
| `valor_ha` | float | Sim | BRL/ha | Valor publicado por hectare; admite zero, receitas negativas e ausência. |
| `participacao_pct` | float | Sim | % | Participação genérica somente quando publicada sem base CV/CT específica. |
| `local` | str | Não | — | Local literal identificado na aba; não restrito a município IBGE. |
| `ano_referencia` | int | Não | — | Ano extraído da referência de preços publicada; distinto de safra. |
| `referencia` | str | Não | — | Referência textual publicada ou ISO da célula Excel datada. |
| `data_referencia` | datetime | Sim | — | Data somente quando a célula Excel publica data; mês/ano não inventa dia. |
| `planilha` | str | Não | — | Identificador exato entre candidatos do catálogo oficial. |
| `aba` | str | Não | — | Nome literal da aba selecionada de forma única. |
| `sistema` | str | Não | — | Descrição publicada do sistema de produção. |
| `linha` | int | Não | — | Número físico da linha na aba, base 1. |
| `tipo_linha` | str | Não | — | item, subtotal ou total publicado; somar indiscriminadamente duplica componentes. |
| `secao` | str | Sim | — | Último cabeçalho romano de seção publicado, quando identificado. |
| `unidade_produto` | str | Sim | — | Cabeçalho literal da unidade de produção, por exemplo CUSTO /  60 kg, R$/1 kg ou (R$/t); sem equivalência presumida. |
| `valor_unidade_produto` | float | Sim | — | Custo pela unidade de produção publicada, não preço unitário de insumo. |
| `participacao_cv_pct` | float | Sim | % CV | Participação publicada na base custo variável; CV não é COE. |
| `participacao_ct_pct` | float | Sim | % CT | Participação publicada na base custo total. |

## Semântica e proveniência

Não há chave primária definida. A saída preserva ocorrências e números físicos de linha; não deduplique por cultura/UF/safra/item.

`safra` é um token publicado anulável, inclusive com grafia anômala, e não é inferida de `ano_referencia`. `referencia` preserva a referência de preços; `data_referencia` só é preenchida quando a planilha publica uma data. `local` não é necessariamente município IBGE.

`valor_ha` preserva zeros, receitas negativas e ausências. `quantidade_ha` e `preco_unitario` ficam nulos quando não publicados; o custo por unidade de produção pertence a `valor_unidade_produto`, com unidade literal em `unidade_produto`. `participacao_cv_pct` e `participacao_ct_pct` preservam bases CV/CT distintas; CV não é COE.

`tipo_linha` distingue `item`, `subtotal` e `total`; somar todas as linhas duplica componentes. `linha` é o número físico na aba, começando em 1. Metadados preservam seleção de planilha/aba, hashes, aquisição e diagnósticos de parsing.


Quando `LOCAL:` não publica a UF, usa-se a UF do nome da aba no padrão
`Local-UF-Ano`. O nome do local continua vindo da célula, removendo o parêntese
descritivo quando presente. A seleção nos metadados registra
`celulas_contexto["uf_origem"] = "nome_da_aba"`; a coordenada de `local` continua
apontando para a célula. Data de referência e safra não são inferidas do nome.

### Limitações de contexto no milho

Em `milho_1a_safra_serie_historica_1997-2025.xls`,
246 contextos são reconhecidos. `P. do Leste-MT-1997` e
`Campo Mourão-PR-1997` continuam sem contexto completo. `Balsas-MA-2013`
conserva o local regional publicado; `Unaí-MG-2005` e `Unaí-MG-2006`
conservam o sistema literal `MILHO1 - PLANTIO DIRETO (100%)`.

Reconhecer o contexto não valida todo o corpo. `Rio Verde-GO-2007`,
`Rio Verde-GO-2008` e `Rio Verde-GO-2009` ainda publicam zero em E29
sem cabeçalho de medida reconhecido e continuam recusadas.

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.custo_producao(
    "soja", uf="BA",
    planilha="serie-historica-custos-soja-1997-a-2025.xls",
    aba="Barreiras-BA-2025", return_meta=True,
)
contracts.validate_dataset(df, "custo_producao")
```

`agrobr/schemas/custo_producao.json` · `get_contract("custo_producao")`.

Veja [API CONAB](../api/conab.md), [migração](../guides/migracao-2.md) e [licença](../licenses.md).
