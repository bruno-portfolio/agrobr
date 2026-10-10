# CONAB - Companhia Nacional de Abastecimento

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Instituicao** | Ministerio da Agricultura |
| **Website** | [conab.gov.br](https://www.conab.gov.br) |
| **Acesso agrobr** | Direto (planilhas Excel públicas) |

## Origem dos Dados

### Fonte

- **URL**: `https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/safra-de-graos/boletim-da-safra-de-graos`
- **Formato**: Planilhas Excel (XLSX e anexos legados XLS)
- **Acesso**: Publico, sem restricoes

## Requisito de Navegador

As funções `safras`, `balanco`, `brasil_total` e `levantamentos` tentam HTTP
primeiro. Se a página ou planilha estiver indisponivel ou invalida por HTTP,
usam Playwright com Chromium como fallback:

```bash
pip install agrobr[browser]
python -m playwright install chromium
```

Quando esse fallback e necessário, a ausência do navegador causa
`SourceUnavailableError`. O dataset
`estimativa_safra` pode tentar IBGE LSPA quando não há fonte ou referência explícita; com `fonte="conab"` ou `levantamento`, não troca de origem. `balanco` não possui fallback.

Nos metadados, `source_method` identifica o transporte da planilha: `httpx` ou
`playwright`, independentemente do transporte usado para descobrir seu link.

## Levantamentos

A CONAB publica levantamentos mensais de safra:

| Mes | Levantamento |
|-----|--------------|
| Outubro | 1o Levantamento |
| Novembro | 2o Levantamento |
| Dezembro | 3o Levantamento |
| Janeiro | 4o Levantamento |
| Fevereiro | 5o Levantamento |
| Marco | 6o Levantamento |
| Abril | 7o Levantamento |
| Maio | 8o Levantamento |
| Junho | 9o Levantamento |
| Julho | 10o Levantamento |
| Agosto | 11o Levantamento |
| Setembro | 12o Levantamento |

## Dados Disponíveis

### Seleção de edição no dataset

`datasets.estimativa_safra("soja", safra="2024/25", uf="MT", levantamento=1)` seleciona o primeiro levantamento CONAB; `levantamento=11` seleciona o décimo primeiro. `fonte="conab"` sem levantamento seleciona a publicação mais recente que traz a safra: para uma safra passada, o último levantamento da safra seguinte, que a revisa. O `levantamento` e a `data_publicacao` são do boletim CONAB que publicou o número, e não da safra: sem `levantamento`, a safra passada vem da publicação mais recente que a traz, revisada. A safra 2024/25 servida pelo 12º levantamento de 2025/26 sai com `levantamento=12` e `data_publicacao=2026-09-15`; o 12º levantamento de 2024/25 é outro boletim, com outro número. A safra do boletim está em `meta.source_details["publicacao"]["safra"]`. Duas ou mais safras atrás da edição mais recente, sem `levantamento`, o número vem da série histórica, e `levantamento` e `data_publicacao` saem nulos (ver `conab.safras`).

O mês civil do LSPA é outro seletor: `mes` direciona o dataset ao IBGE LSPA e não representa o número de levantamento CONAB. Seletores incompatíveis são rejeitados antes da rede. Sem `uf`, a CONAB retorna linhas estaduais; para comparação com LSPA, informe a mesma UF nas duas consultas.

O catálogo percorre páginas e reconhece links de download mesmo sem extensão no caminho. A disponibilidade depende das edições descobertas; não há garantia de arquivo histórico completo. Datas de publicação são preservadas quando informadas pela origem, sem usar a data da consulta como substituta.

O [dataset `estimativa_safra`](../contracts/estimativa_safra.md) usa contrato 3.1 e acrescenta `ano_lspa`/`mes_lspa` nulos e as unidades (`mil_ton`, `mil_ha`) nas linhas CONAB. A API `conab.safras` e `CONAB_SAFRA_V2` permanecem em 2.0.

### Safras

A CONAB publica uma única área, rotulada "ÁREA (Em mil ha)", mantida em `area_plantada`. `area_colhida` fica nula na rota CONAB; área colhida distinta só existe na rota LSPA. Não há cópia nem imputação entre as duas áreas. O contrato CONAB V2 já permite essa nulidade.

Nas abas com cabeçalho em ano civil (trigo, aveia, canola, centeio, cevada e triticale), o ano publicado é o ano de encerramento do biênio do contrato (`Safra 2026` → `2025/26`). Um levantamento que ainda não publica esse ano não fornece estimativa para a safra solicitada. A série histórica conserva seu período anual próprio.

De out/2019 a jan/2022, as abas desses seis cereais levam o ano no nome ("Trigo 2021"), e a edição pode trazer também a aba do ano anterior, às vezes com o cabeçalho de safra quebrado. O agrobr lê a aba mais recente cujo cabeçalho publica a safra pedida: no 12º levantamento de 2020/21 (set/2021), o trigo 2019/20 sai da coluna "Safra 2020" de "Trigo 2021", e não da cópia "Trigo 2020", cujo cabeçalho traz "23". Nos levantamentos 1 a 4 de 2019/20, 3 e 4 de 2020/21 e 2 a 4 de 2021/22, a aba ainda não traz o inverno da própria safra, e a consulta sai vazia. A CONAB publicou só em PDF os levantamentos 7, 8, 9 e 12 de 2019/20, 1 e 2 de 2020/21 e 1 de 2021/22, que por isso não estão no catálogo.


- Area plantada (mil hectares)
- Area colhida nula: o levantamento publica uma única área
- Produtividade (kg/ha)
- Produção (mil toneladas)

### Balanco de Oferta e Demanda

- Estoque inicial
- Produção
- Importacao
- Consumo
- Exportacao
- Estoque final

## Uso

### Safras por Produto

```python
import asyncio
from agrobr import conab

async def main():
    # Dados de safra da soja
    df = await conab.safras('soja')

    # Safra especifica
    df = await conab.safras('milho', safra='2025/26')

    # Filtrar por UF
    df = await conab.safras('soja', uf='MT')

    # Com metadados
    df, meta = await conab.safras('soja', return_meta=True)

asyncio.run(main())
```

### Balanco de Oferta/Demanda

```python
# Balanco de todos os produtos
df = await conab.balanco()

# Balanco de produto especifico
df = await conab.balanco(produto='soja')
```

### Totais Brasil

```python
# Totais nacionais por produto
df = await conab.brasil_total()
```

`produto` contém identificadores normalizados (`soja`, `algodao_caroco`, `brasil`), e `rotulo` preserva o texto original, inclusive notas de rodapé. `grupo` mantém a hierarquia dos detalhes e subtotais. Área, produção e produtividade são `float64`; a área usa `mil_ha`, a produção `mil_ton` e a produtividade `kg/ha`. O schema 2.0 de totais Brasil mantém as nove colunas também no vazio; cabeçalho irreconhecível gera `ParseError`.

As flags `as_polars` e `return_meta` são somente nomeadas. As saídas vazias de `safras`, `balanco` e `serie_historica` também preservam colunas e tipos. Datas reais usam `datetime64[ns]`, números de levantamento usam `Int64`, medidas usam `float64` e o texto segue o dtype padrão do pandas.

## Schema - Safras

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `fonte` | str | "conab" |
| `produto` | str | Nome do produto |
| `safra` | str | Safra (ex: "2024/25") |
| `uf` | str | Sigla da UF |
| `area_plantada` | float64 | Mil hectares |
| `area_colhida` | float64 | Nula: não publicada separadamente no levantamento |
| `produtividade` | float64 | kg/ha |
| `producao` | float64 | Mil toneladas |
| `levantamento` | int | Número do levantamento (1-12) |
| `data_publicacao` | date | Data de publicação |

## Produtos Disponíveis

```python
produtos = await conab.produtos()
# ['soja', 'milho', 'milho_1', 'milho_2', 'milho_3', 'arroz', ...]
```

## UFs Disponíveis

```python
ufs = await conab.ufs()
# ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', ...]
```

## Levantamentos Disponíveis

```python
levs = await conab.levantamentos()
for lev in levs[:5]:
    print(f"{lev['safra']} - {lev['levantamento']}o levantamento")
```

## Custos de produção

Contrato **3.0**, com 25 colunas que preservam planilha, aba, local, sistema, referência de preços e linhas físicas. Selecione `planilha` e `aba` sem ambiguidade; seleções amplas listam candidatos em vez de devolver a primeira aba.

```python
from agrobr import conab

catalogo = await conab.catalogo_custos("soja")
df, meta = await conab.custo_producao(
    "soja", uf="BA",
    planilha=catalogo["planilha"].iloc[-1],
    aba="Barreiras-BA-2025", return_meta=True,
)
```

Não há chave primária definida. Repetições e rótulos literais de itens são preservados. `safra` é anulável e vem da aba; o ano da referência de preços é separado. `tipo_linha` distingue itens, subtotais e totais, que não devem ser somados indiscriminadamente. Receitas negativas e valores ausentes permanecem como publicados.

Veja o [contrato de 25 colunas](../contracts/custo_producao.md) para unidades, nulabilidade, bases CV/CT e proveniência. `custo_producao_total` informa totais publicados selecionados, sem reconstruí-los pela soma de todas as linhas.

## Serie Histórica (v0.8.0)

Dados históricos de safras desde ~1976, disponibilizados em planilhas Excel (.xls legacy).
O parser detecta automaticamente o formato (OLE2/BIFF → xlrd, OOXML → openpyxl com fallback calamine).

```python
# Serie historica de soja
df = await conab.serie_historica("soja", ano_inicio=2020, ano_fim=2025)

# Filtrar por UF
df = await conab.serie_historica("soja", ano_inicio=2020, uf="MT")
```

`conab.produtos_serie_historica()` lista produto, categoria e URL de cada série. Use as funções pela fachada `agrobr.conab`; os antigos subpacotes públicos `custo_producao` e `serie_historica` foram removidos.

### Schema - serie_historica

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `safra` | str | Safra (ex: "2024/25") |
| `produto` | str | Nome do produto |
| `uf` | str | Sigla da UF |
| `regiao` | str | Região da UF |
| `area_plantada_mil_ha` | float | Mil hectares |
| `producao_mil_ton` | float | Mil toneladas |
| `produtividade_kg_ha` | float | kg/ha |
| `area_em_producao_mil_ha` | float, opcional | Café: área em produção, em mil hectares |
| `area_formacao_mil_ha` | float, opcional | Café: área em formação, em mil hectares |
| `area_colhida_mil_ha` | float, opcional | Cana: área colhida, em mil hectares |

Os nomes com unidade permanecem no contrato 1.1. Para comparar as métricas com `safras` e `estimativa_safra`, use esta correspondência; a publicação e o período também precisam coincidir:

| Série histórica | Safras / estimativa | Unidade |
|---|---|---|
| `area_plantada_mil_ha` | `area_plantada` | mil ha |
| `producao_mil_ton` | `producao` | mil t |
| `produtividade_kg_ha` | `produtividade` | kg/ha |
| `area_colhida_mil_ha` | `area_colhida` | mil ha; não implica equivalência de cobertura entre produtos/fontes |

`regiao` é a macrorregião sob a qual a planilha lista a UF, reconhecida só pelo rótulo exato (NORTE, NORDESTE, CENTRO-OESTE, SUDESTE, SUL). Sub-regiões, como as do café na Bahia e em Minas Gerais ("Sul e Centro-Oeste", "Norte, Jequitinhonha e Mucuri"), e agregados ("NORTE/NORDESTE", "CENTRO-SUL", "OUTROS") não mudam a região e não são publicados.

Para café, a área plantada é a soma das áreas em produção e formação quando ambas estão disponíveis. Mil sacas beneficiadas de 60 kg são convertidas para mil toneladas (× 0,06), e sacas/ha para kg/ha (× 60). A produtividade se refere à área em produção; veja o [contrato 1.1](../contracts/serie_historica_safra.md).

Cada produto é uma série da CONAB: cultura, safra (`milho_1` a `milho_3`, feijões) ou recorte (`cana_area_total`, `algodao`, `algodao_pluma`, `algodao_caroco`).

O período `safra` segue o publicado: ano civil `YYYY` para `cafe`, `cafe_arabica`, `cafe_conilon`, `trigo`, `aveia`, `cevada`, `canola`, `centeio`, `triticale`; os demais produtos usam `YYYY/YY`. Os filtros inclusivos `ano_inicio`/`ano_fim` usam o ano inicial e exigem inteiros; intervalos invertidos geram `InvalidParameterError` antes da rede.

A coluna de previsão (rótulos como `Previsão` ou `(¹)`) não entra na série histórica; sua exclusão fica registrada por produto, aba e rótulo. Para a safra em curso, use `estimativa_safra` nos produtos disponíveis nesse dataset.

`algodao` representa algodão em caroço; `algodao_pluma`, pluma; `algodao_caroco`, semente (caroço de algodão). Os três usam a mesma área, com produção e produtividade do respectivo recorte.

`cana` publica a aba Área em `area_colhida_mil_ha`: o título dela na planilha oficial é "Série Histórica de Área Colhida". `area_plantada_mil_ha` fica nula nesse produto; para a área total, use `cana_area_total`.

`cana_area_total` usa somente a aba Área Total, cuja composição muda ao longo da série: colhida + plantio + mudas até 2021/22 (exceto 2016/17), e igual à colhida em 19 a 23 UFs em 2016/17 e de 2023/24 a 2025/26 (detalhe no [contrato](../contracts/serie_historica_safra.md)). Células vazias não recebem valores de Área Colhida; produção e produtividade permanecem nulas. Abas de mudas e modalidades de colheita não integram essa série.

Abas selecionadas ilegíveis, ausentes ou ambíguas geram `ParseError`; filtros sem observações retornam uma tabela vazia com schema. Abas desconhecidas geram aviso.

`arroz_sequeiro`: o rótulo de unidade das abas Produtividade e Produção está errado na planilha oficial (set/2026); os valores são kg/ha e mil t, conforme a relação produção/área.

## Série histórica industrial da cana

`conab.cana_industria(ano_inicio=None, ano_fim=None, uf=None)` lê a `canaseriehist-industria.xls` (Séries Históricas, cana-de-açúcar, indústria): sete abas, uma coluna por aba, uma linha por safra e UF, nas unidades publicadas. É uma API própria porque o esquema não é o da série agrícola; `conab.serie_historica("cana_industria")` levanta `InvalidParameterError` apontando para ela.

```python
df = await conab.cana_industria(ano_inicio=2020, ano_fim=2025, uf="MT")
etanol_cana = df["etanol_anidro_cana_mil_l"] + df["etanol_hidratado_cana_mil_l"]
```

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `safra` | str | Safra (ex: "2024/25") |
| `regiao` | str | Região da UF |
| `uf` | str | Sigla da UF |
| `acucar_mil_ton` | float | Açúcar, mil toneladas |
| `etanol_anidro_cana_mil_l` | float | Etanol anidro de cana, mil litros |
| `etanol_hidratado_cana_mil_l` | float | Etanol hidratado de cana, mil litros |
| `etanol_anidro_milho_mil_l` | float | Etanol anidro de milho, mil litros |
| `etanol_hidratado_milho_mil_l` | float | Etanol hidratado de milho, mil litros |
| `etanol_total_mil_l` | float | Etanol total publicado, de cana e de milho, mil litros |
| `atr_kg_t` | float | ATR médio, kg/t de cana |

A aba "Etanol Total (cana e milho)" inclui o etanol de milho, publicado a partir de 2018/19: ler `etanol_total_mil_l` como etanol de cana superestima o MT em 2024/25 5,7 vezes. Zero publicado sai `0.0`; traço, vazio e erro do Excel saem nulos. A estimativa (última coluna, marcada com `(¹)`) e as linhas de região e BRASIL ficam fora. Total diferente das quatro parcelas, soma das UFs diferente do BRASIL, erro do Excel e a safra fechada mais recente geram aviso, sem alterar os números; layout diferente do medido gera `ParseError`. Detalhes no [contrato 1.0](../contracts/producao_acucar_etanol.md).

## Cache

As consultas da CONAB não guardam cópia local: cada chamada baixa a publicação. `meta.cache_expires_at` sai nulo; em `conab.safras`, o
`meta.cache_key` identifica a consulta (produto, safra, publicação, levantamento e UF), e nas demais funções sai nulo.

## Atualização

| Aspecto | Valor |
|---------|-------|
| **Frequência** | Mensal |
| **Publicação** | Geralmente entre dias 10-15 |

## Datasets

- [`estimativa_safra`](../contracts/estimativa_safra.md) — contrato 3.1 com levantamento CONAB ou mês LSPA explícito

- [`serie_historica_safra`](../contracts/serie_historica_safra.md) — wraps `conab.serie_historica()` (45 produtos; a série industrial da cana tem API própria, `conab.cana_industria()`)

- [`producao_acucar_etanol`](../contracts/producao_acucar_etanol.md) — wraps `conab.cana_industria()` (açúcar, etanol de cana e de milho e ATR por safra e UF)

## Semântica na versão 2.0

O balanço reconhece os nomes acentuados de algodão e feijão, preserva o ano civil do trigo e seleciona a última revisão de cada safra, inclusive nas linhas com células mescladas. O progresso distingue cabeçalhos `Safra AAAA` e `Safra AAAA/AA`, evitando misturar trigo com milho de segunda safra.

`semana_url` recebe a URL da página semanal (`acompanhamento-das-lavouras-…`); o client resolve a ficha e o link da planilha a partir dela. Boletins históricos de progresso: o link do arquivo é resolvido na página oficial, incluindo a ficha de download. Conteúdo sem assinatura de planilha XLSX/XLS é recusado antes do parse, com URL e trecho inicial na mensagem. Só páginas em `https://www.gov.br/conab/` são seguidas: outra URL, ou redirecionamento e link que saiam dela, levantam `InvalidParameterError` antes do pedido.

`tecnologia` não é seletor; a qualificação de saída só é preenchida quando explicitamente publicada. O contrato ativo de custos é 3.0.

A data de publicação é nula quando o download não traz essa informação. A data de consulta (`MetaInfo.fetched_at`) nunca é usada como substituta. `Safra.data_publicacao` aceita `None`; se fornecida pelo metadado da fonte, a data é preservada.

## Sociobiodiversidade

`conab.custo_sociobiodiversidade(produto, uf=None, ano=None, *, local=None, planilha=None, aba=None, use_cache=True, as_polars=False, return_meta=False)` retorna os custos extrativistas publicados. O dataset tem os mesmos seletores. `conab.catalogo_sociobiodiversidade()` lista todas as revisões de recursos com indicação de ativo; com produto, inventaria o workbook ativo, incluindo contextos pendentes. `planilha=` seleciona um recurso histórico exato. Os 20 produtos capturados, unidades literais, seleção e limitações nominais estão no [contrato 1.0](../contracts/custo_sociobiodiversidade.md). Sem conversão hectare/safra nem mescla de revisões. Cache do catálogo de 1 h, separado dos custos agrícolas; workbooks sempre baixados. `use_cache=False` ignora o cache do catálogo.

Custos agrícolas usam parser 5: cabeçalhos mesclados, café por recurso oficial,
safra anual ou bienal literal e notas cambiais separadas dos itens. Percentuais
só recebem escala Excel quando a célula é numérica. Custos da sociobiodiversidade
usam parser 2 e preservam a seleção explícita de revisões arquivadas. Contratos
3.0 e 1.0, com o texto no dtype padrão do pandas instalado; terceira medida monetária continua recusada.
