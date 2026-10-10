# API BCB/SICOR

O módulo BCB fornece dados do Banco Central do Brasil: crédito rural (SICOR), séries temporais (SGS), cotação do dólar (PTAX) e expectativas de mercado (Focus).

## Funções

### `credito_rural`

Dados de financiamento rural por produto, safra e UF, agregados por UF ou por programa, ou registro a registro.

```python
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa", "registro"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Chave do produto (soja, milho, arroz, feijao, trigo, algodao, cafe, cana, mandioca, sorgo) ou item de investimento do SICOR (ex.: "BOVINOS"); nas chaves, acento, caixa e espaços nas pontas não importam; em outro item, caixa e espaços nas pontas não importam, e o acento tem de ser o do SICOR |
| `safra` | `str \| None` | Safra `"AAAA/AA"`, `"AAAA/AAAA"` (anos consecutivos) ou `"AAAA"` (ano final: `"2025"` = 2024/2025); outro formato levanta `InvalidParameterError` antes da rede. `None` (padrão) não filtra safra |
| `finalidade` | `str` | `"custeio"`, `"investimento"` ou `"comercializacao"`; a industrialização não sai por produto e levanta `InvalidParameterError` apontando o `credito_rural_total` |
| `uf` | `str \| None` | Sigla da UF (ex: "MT", "PR"); espaços/caixa são normalizados e valores inválidos levantam `ValueError` |
| `agregacao` | `str` | `"uf"` (padrão), `"programa"` ou `"registro"` (os registros do SICOR, sem agregar; outras colunas e outro contrato, abaixo) |
| `programa` | `str \| None` | Filtrar pelo nome publicado do programa, sem diferenciar maiúsculas (ex: "PRONAMP", "RenovAgro"; tabela em [fontes/BCB](../sources/bcb.md#dimensoes-sicor)) |
| `tipo_seguro` | `str \| None` | Filtrar pela descrição oficial do tipo de seguro, sem diferenciar maiúsculas (ex: "Proagro tradicional", "Sem adesão a seguro") |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `safra` | str | Safra "2024/25" (AAAA/AA, de julho a junho), como nos outros datasets |
| `produto` | str | Chave do produto pedido, sem acento e em minúsculas (ex.: `algodao`, `cana`), igual nas duas fontes; o filtro usa a grafia oficial do SICOR (`"ALGODÃO"`, `"CANA-DE-AÇUCAR"`) |
| `uf` | str | UF |
| `finalidade` | str | Finalidade pedida, em minúsculas nas duas fontes (`custeio`, `investimento` ou `comercializacao`) |
| `agregacao` | str | Nível da saída: `uf` ou `programa` |
| `programa` | str | Programa SICOR; nulo na agregação por UF |
| `cd_programa` | str | Código do programa; nulo na agregação por UF |
| `qtd_contratos` | int | Quantidade de contratos |
| `valor` | float | Valor financiado (R$) |
| `area_financiada` | float | Área financiada (ha). Pelo OData sai nula: no custeio a fonte publica `AreaCusteio` vazio (nenhum dos 2.583 registros de 10 consultas da safra 2024/25, em set/2026, traz área), e investimento e comercialização não trazem área; só o fallback BigQuery a preenche |
| `fonte` | str | `bcb_odata` ou `bcb_bigquery` |

`programa` usa o nome vigente da tabela oficial em todas as safras: o `0152` sai como PROIRRIGA também antes de 07/2021, quando o código era o Moderinfra (a descrição oficial registra a troca em 01/07/2021).

**Ausência não é zero.** Filtro `uf`, `programa` ou `tipo_seguro` com o corpo da fonte sem a coluna correspondente levanta `ParseError`, em vez de devolver o total de todos. Na agregação por UF ou por programa, `valor`, `area_financiada` e `qtd_contratos` saem nulos no grupo em que algum registro não traz o valor; quando o mesmo grupo tem valores conhecidos e ausentes, `MetaInfo.validation_warnings` registra o aviso. Valor ausente sai nulo, mas ano, mês ou quantidade de contratos publicados com texto não numérico ou fração levantam `ParseError` com a coluna, o registro e o valor publicado.

**Safra em curso.** A safra que contém a data de hoje (julho a junho) ainda recebe contratos, e o total dela muda até o fim da safra. Quando o resultado a traz, o `credito_rural` avisa em `validation_warnings` e em `UserWarning` e registra em `source_details` a safra (`safra_em_curso`) e os meses de emissão cobertos (`meses_cobertos`, `"AAAA-MM"`).

**Registro a registro (`agregacao="registro"`).** Devolve os registros das entidades `*RegiaoUFProduto` depois dos filtros de `uf`, `programa` e `tipo_seguro`, sem agregar: o recorte que a chamada padrão da 1.1.0 devolvia. São 23 colunas, as 11 acima e mais `ano_emissao`, `mes_emissao`, `regiao`, `cd_sub_programa`, `cd_fonte_recurso`, `fonte_recurso`, `cd_tipo_seguro`, `tipo_seguro`, `cd_modalidade`, `modalidade`, `cd_atividade` e `atividade`, no contrato [bcb.credito_rural_registro](../contracts/bcb_credito_rural_registro.md) 1.0, com o `MetaInfo` desse contrato. Os nomes de fonte de recursos, modalidade e atividade saem da descrição das tabelas de domínio do BCB, e o código fora da tabela fica com nome nulo. Somado por safra, UF, produto e finalidade, é igual à `agregacao="uf"`.

**Fallback BigQuery só na agregação por UF sem filtro de programa ou seguro.** A tabela da Base dos Dados agrega por município e não traz programa, fonte de recursos, tipo de seguro, modalidade nem atividade. Com `agregacao="programa"`, `agregacao="registro"`, `programa=` ou `tipo_seguro=`, o OData fora levanta `SourceUnavailableError`, com o motivo na mensagem, em vez de devolver o total de todos os programas.

**Exemplo:**

```python
from agrobr import bcb

# Credito custeio soja MT
df = await bcb.credito_rural("soja", safra="2024/25", uf="MT")

# Agregado por UF
df = await bcb.credito_rural("milho", agregacao="uf")

# Agregado por programa
df = await bcb.credito_rural("soja", safra="2024/25", agregacao="programa")

# Filtrar por programa
df = await bcb.credito_rural("soja", safra="2024/25", programa="Pronamp")

# Filtrar por tipo de seguro
df = await bcb.credito_rural("soja", safra="2024/25", tipo_seguro="Proagro tradicional")

# Registro a registro, com mês, fonte de recursos, modalidade e atividade
df = await bcb.credito_rural("soja", safra="2024/25", uf="MT", agregacao="registro")

# Com metadados
df, meta = await bcb.credito_rural("soja", return_meta=True)
print(meta.schema_version)  # "2.0"
```

`agregacao="municipio"` levanta `InvalidParameterError`. As entidades por produto que o agrobr lê (`*RegiaoUFProduto`) não têm município. O SICOR publica município por produto (`CusteioMunicipioProduto` e `InvestMunicipioProduto`), que o agrobr não lê; o extra `agrobr[bigquery]` traz dados municipais.

### `credito_rural_total`

Crédito rural por UF e finalidade, sem produto: a entidade `RegiaoUF` do SICOR, com as quatro finalidades, inclusive a industrialização, que o SICOR não publica por produto. Contrato [bcb.credito_rural_total](../contracts/bcb_credito_rural_total.md) 1.0.

```python
async def credito_rural_total(
    safra: str | None = None,
    finalidade: str | None = None,
    uf: str | None = None,
    agregacao: Literal["uf", "programa"] = "uf",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `safra` | `str \| None` | Mesmos formatos do `credito_rural`; a safra vai de julho a junho. `None` (padrão) não filtra safra e lê a série inteira, desde jan/2013 |
| `finalidade` | `str \| None` | `"custeio"`, `"investimento"`, `"comercializacao"` ou `"industrializacao"`; `None` (padrão) traz as quatro. Outro valor levanta `InvalidParameterError` antes da rede |
| `uf` | `str \| None` | Sigla da UF; `None` (padrão) traz as 27 |
| `agregacao` | `str` | `"uf"` (padrão) ou `"programa"` |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:** uma linha por safra, UF e finalidade (e programa, com `agregacao="programa"`), com as colunas do `credito_rural` sem `produto` e sem `area_financiada`: `safra`, `uf`, `finalidade`, `agregacao`, `programa`, `cd_programa`, `qtd_contratos`, `valor` (R$, ao centavo) e `fonte` (`bcb_odata`).

- **Finalidade sem operação fica ausente.** Na linha larga, a fonte preenche com 0 as finalidades sem operação; esse par não vira linha. Na safra 2022/23, faltam a industrialização no AM, no AP e em RR e a comercialização no AP.
- **Sem linha Brasil.** O SICOR não publica total do país: o total do Brasil é a soma das UFs.
- **Safra parcial.** A safra corrente é parcial; `MetaInfo.source_details["meses"]` traz o primeiro e o último mês com dado e a quantidade de meses.
- **Consulta por safra × soma das mensais.** A função pede a safra numa consulta só (fatiada por mês só quando a resposta bate no limite de registros da Olinda), e o SICOR pode devolver números diferentes da soma das consultas mês a mês. Em 26/09/2026, na safra 2026/27 (julho e agosto), 51 pares UF × finalidade divergiram: no custeio do AC, 274 contratos e R$ 56.788.261,98 na consulta por safra, contra 272 e R$ 56.541.830,82 nas mensais. A causa não foi identificada, e o agrobr reproduz o corpo recebido.
- **Ano, mês ou quantidade que não é inteiro.** `AnoEmissao`, `MesEmissao` ou a quantidade de contratos publicados como texto não numérico ou fração levantam `ParseError` com a coluna, o registro e o valor publicado, em vez de truncar a fração ou cair com erro cru. No filtro de safra (aqui e no `credito_rural`), ano ou mês ausente também levanta `ParseError`: sem eles, o registro não tem safra.
- **Sem fallback BigQuery**: `attempted_sources` é `["bcb_odata"]`.
- O total por UF e finalidade fecha com a soma dos municípios (`CusteioInvestimentoComercialIndustrialSemFiltros`) e, em custeio, investimento e comercialização, com a soma por produto do `credito_rural` (conferido em 2022 e 2023).

**Exemplo:**

```python
from agrobr import bcb

# As quatro finalidades por UF
df = await bcb.credito_rural_total(safra="2022/23")

# Industrialização, que não sai por produto
df = await bcb.credito_rural_total(safra="2022/23", finalidade="industrializacao")

# Uma UF por programa
df = await bcb.credito_rural_total(safra="2022/23", uf="MT", agregacao="programa")
```

### `sgs`

Séries temporais do BCB, por código inteiro positivo ou um dos 17 aliases existentes. Intervalos longos são adquiridos em blocos de calendário, com união validada e proveniência por resposta.

```python
async def sgs(
    codigo: int | str,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

| Parâmetro | Regra |
|-----------|-------|
| `codigo` | Inteiro entre 1 e 2**63−1 ou alias com caixa/espaços normalizados; strings numéricas e bool não são códigos |
| `inicio`, `fim` | Datas civis inclusivas: `date`, `datetime`, ISO ou DD/MM/AAAA; intervalo invertido é inválido |
| `ultimos` | Inteiro positivo; sem datas usa a rota `/ultimos/N`; com datas aplica o corte após a união ordenada |
| `as_polars` | Booleano; exige `agrobr[polars]` quando True |
| `return_meta` | Booleano; True retorna também MetaInfo |

Sem datas e sem `ultimos`, a janela padrão vai da data civil de Brasília (UTC−3) da consulta menos dez anos até essa mesma data, com ajuste de 29/02 para 28/02 quando necessário. Informando apenas o início, o fim usa a data civil de Brasília (UTC−3) da consulta. Informando apenas o fim, o início continua omitido na requisição: a fonte pode recusar essa seleção. Não há seleção de uma revisão histórica congelada.

O planejamento encerra cada bloco em 31/12 do ano inicial + 9, ou no fim pedido se anterior; o próximo começa em 01/01. Isso respeita o limite de dez anos por chamada das consultas diárias, sem pressupor que todo código seja diário. Blocos fecham em anos civis e uma janela de dez anos pode usar duas chamadas. Falha em qualquer bloco interrompe a consulta, sem resultado parcial.

A rota de últimos valores tem limite de 20 documentado e confirmado para a série 1. O agrobr preserva a recusa remota, sem impor esse máximo a códigos de frequência desconhecida. Para solicitar mais observações, informe as duas datas e `ultimos`.

**Aliases:** `selic`, `ipca`, `ipca_alimentacao`, `ipa_agricola`, `pib_agropecuaria`, `credito_rural_concessoes_pf`, `credito_rural_saldo_pf`, `dolar_ptax_venda`, `dolar_ptax_compra`, `cambio_mensal_compra`, `cambio_mensal_venda`, `igpm`, `igpdi`, `inpc`, `cdi`, `tjlp`, `tr`. `ipa_agricola` é a série 7460 (IPA-DI por origem, produtos agrícolas, sem os pecuários);
o nome antigo `ipa_agropecuario` segue aceito com `FutureWarning` e sai como `nome_serie="ipa_agricola"`.

**Retorno — contrato 3.0:** `data` é referência civil `datetime64[ns]` sem fuso; `valor` usa float64 e permite nulo explícito e valores negativos; `codigo` usa Int64; `nome_serie` contém o alias conhecido ou nulo. As quatro colunas também existem no vazio. Quando o corpo publica `dataFim`, o fim do período da taxa (ex.: TR, código 226), a saída ganha `data_fim` (`datetime64[ns]`, opcional) depois delas; linhas sem o campo ficam nulas. Só campos fora de `data`, `valor` e `dataFim` geram aviso. Frequência e unidade dependem da série e não são inferidas pelo espaçamento entre datas.

Referências publicadas anteriores ou posteriores à janela diária são preservadas com aviso e diagnóstico por bloco. Duplicatas dentro de um corpo geram `ParseError`. Entre blocos, referências com valor idêntico são reconciliadas com todas as origens; valores conflitantes geram erro. O corte `ultimos` ocorre após essa união.

Uma lista JSON vazia é válida. O envelope oficial com `SGSNegocioException: Value(s) not found` também retorna vazio com aviso, venha com HTTP 404 ou com HTTP 200 (o aviso traz o status recebido); ele não comprova a existência do código. Outros erros HTTP não são convertidos em ausência. Parâmetros inválidos geram `InvalidParameterError`, falhas de rede/serviço geram `SourceUnavailableError` e dados incompatíveis geram `ParseError`.

**Proveniência:** `source_details` contém a seleção original/efetiva, blocos, URLs, parâmetros, hashes e tamanhos dos corpos, coleta UTC, layout, diagnósticos de referência e reconciliação. `coverage.request_status="all_blocks_succeeded"` indica aquisição dos blocos planejados; `completeness="unknown"` e `expected_count=None` registram que não há total independente da fonte. Extremos observados e contagem única referem-se à união antes do corte; `returned_count` descreve a saída. O hash e tamanho superiores identificam o manifesto canônico de query/recursos, não a concatenação dos corpos. Avisos também são emitidos sem `return_meta`.

```python
from agrobr import bcb

df, meta = await bcb.sgs(
    1, inicio="01/01/2010", fim="31/12/2024",
    return_meta=True,
)
recentes = await bcb.sgs(1, ultimos=3)
recorte = await bcb.sgs(
    1, inicio="01/01/2024", fim="31/12/2024", ultimos=30,
)
```

Veja o [contrato SGS](../contracts/bcb_sgs.md), a [fonte](../sources/bcb.md#sgs-series-temporais) e a [migração](../guides/migracao-2.md).

Na camada semântica, [`datasets.series_economicas`](series_economicas.md) oferece a mesma seleção e reutiliza o contrato 3.0, preservando a proveniência. O dataset recusa contexto `deterministic`, pois a consulta atual não recupera revisões anteriores.

---

### `ptax`

Cotações e paridades PTAX publicadas para uma moeda, com seleção explícita de boletim.

```python
async def ptax(
    *,
    data: str | date | datetime | None = None,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    moeda: str = "USD",
    boletim: Literal["todos", "fechamento", "abertura", "intermediario"] = "fechamento",
    top: int = 1000,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

| Parâmetro | Regra |
|-----------|-------|
| `data` | Data civil única: `date`, `datetime`, ISO ou DD/MM/AAAA; exclusiva de qualquer limite de intervalo |
| `inicio`, `fim` | Limites inclusivos; apenas início preenche fim com hoje (data civil de Brasília, UTC−3), apenas fim preenche início com fim menos 30 dias |
| Sem datas | Hoje (data civil de Brasília, UTC−3) menos 30 dias até hoje, usando uma única referência |
| `moeda` | Três letras ASCII; normaliza caixa para maiúsculas, sem remover espaços, aliases por nome ou códigos numéricos; padrão USD |
| `boletim` | `fechamento` (padrão), `todos`, `abertura` ou `intermediario` |
| `top` | Inteiro positivo estrito; tamanho pedido por página de cotações, padrão 1000 |
| `as_polars` | Booleano; True exige `agrobr[polars]` |
| `return_meta` | Booleano; True retorna também MetaInfo |

Calendários inválidos, datas invertidas/conflitantes, tamanhos de página fracionários/booleanos e moedas malformadas falham antes da rede. Datas futuras explícitas podem produzir vazio. Nenhum limite informado é ignorado. Argumentos desconhecidos geram TypeError. A janela padrão tem extremos separados por 30 dias; não promete 30 datas ou registros.

Cada aquisição de cotações lê primeiro o catálogo OData corrente, com paginação própria e top 1000. Símbolo ausente em catálogo não vazio gera InvalidParameterError antes do GET de cotações. Isso registra ausência no catálogo atual do serviço, sem provar invalidade histórica. Catálogo indisponível ou vazio impede validar a cotação; não há substituição silenciosa de moeda. `value:[]` não valida uma moeda, pois a fonte também o devolve para símbolos não suportados.

**Retorno — contrato 2.0:** oito colunas, mantendo as quatro anteriores como prefixo: `cotacao_compra`, `cotacao_venda`, `data_hora`, `data`, `moeda`, `paridade_compra`, `paridade_venda`, `tipo_boletim`. Quatro medidas usam float64 finito anulável. As duas datas usam datetime64[ns] sem fuso; `data` é o dia civil de `data_hora`. O horário conserva até nove dígitos fracionários, sem truncamento. Moeda é textual não nula; tipo de boletim é texto publicado anulável. Vazio mantém colunas e tipos.

O fechamento USD padrão conserva valores e horários legados nos casos recentes e de 1994. `todos` acrescenta os boletins de abertura e intermediários. A rota genérica por dia publica `Fechamento PTAX`, e a rota por período publica `Fechamento`; o seletor de fechamento reconhece ambos e conserva o rótulo original. Essa variação precisa ser considerada em junções entre dia e período. Boletim novo, vazio ou null é preservado com aviso em `todos`; um filtro específico gera ParseError quando não puder classificar a linha.

**Unidades:** cotações usam a unidade monetária doméstica da data por unidade da moeda selecionada. Não rotule todo o histórico como BRL. Paridades tipo A usam moeda/USD; tipo B usa USD/moeda. Tipo do catálogo e unidades aplicáveis constam dos metadados; o SDK não calcula conversões, inverte paridades ou recompõe fechamentos. O relógio publicado permanece sem fuso e distinto da aquisição UTC.

**Paginação e cobertura:** o catálogo solicita símbolos crescentes; cotações usam horário e rótulo de boletim crescentes. A página inteira é validada antes do filtro de boletim. Sem total independente, páginas curtas avançam pelo número recebido até uma página vazia. Duplicatas, alterações de seleção, contradições de contagem e falhas de página interrompem a aquisição. Não há limite local de linhas ou retorno automático de intervalo incompleto.

`source_details.coverage` descreve cotações antes/depois da seleção de boletim; `catalog.coverage` descreve o catálogo separadamente. Filtro intencional de boletim não é truncamento. `complete` exige contagem da fonte reconciliada; sem ela, inclusive após vazio, a cobertura fica `unknown`. As consultas conhecidas não trouxeram count/nextLink; se vierem, essas anotações são validadas. Não há snapshot atômico de revisão.

**Proveniência e erros:** recursos são listados na ordem catálogo→cotações, com papel, índice da página, URL, parâmetros, hash/bytes do corpo, coleta UTC, quantidades recebidas/retidas e layout. Hash/tamanho superiores identificam manifesto canônico UTF-8 de query e recursos; a soma dos corpos é separada. Erros HTTP/rede geram SourceUnavailableError; HTTP200 malformado, JSON ambíguo, números não finitos e campos obrigatórios inválidos geram ParseError. Valores finitos não positivos ou compra/venda invertidas permanecem com diagnóstico. Avisos saem mesmo sem metadados.

```python
from agrobr import bcb

usd = await bcb.ptax(data="04/09/2026")
eur, meta = await bcb.ptax(
    moeda="EUR", boletim="todos",
    inicio="03/09/2026", fim="06/09/2026",
    top=3, return_meta=True,
)
jpy = await bcb.ptax(moeda="JPY", boletim="intermediario", data="04/09/2026")
```

### `ptax_moedas`

```python
async def ptax_moedas(
    *, top: int = 1000, as_polars: bool = False, return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

Retorna `moeda`, `nome` e `tipo_moeda`, todos textos não nulos, com contrato 1.0 e parser 2. Símbolos são únicos e ordenados. Nomes/tipos permanecem publicados; tipo novo gera diagnóstico sem receber unidade de paridade inventada. Vazio mantém três colunas. Esta API não aceita filtros de cotação/data e descreve o catálogo OData corrente, distinto da tabela histórica mais ampla do portal.

```python
moedas, meta = await bcb.ptax_moedas(return_meta=True)
```

Veja [contratos e identidade](../contracts/bcb_ptax.md), [fonte e licença](../sources/bcb.md#ptax-cotacoes-moedas-e-boletins) e [migração](../guides/migracao-2.md).

---

### `focus`

Estatísticas agregadas do Sistema Expectativas de Mercado do BCB, por indicador e horizonte anual ou mensal. São previsões dos participantes da pesquisa.

```python
async def focus(
    indicador: str = "PIB Agropecuária",
    *,
    periodicidade: Literal["anual", "mensal"] = "anual",
    top: int = 1000,
    inicio: str | date | datetime | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

| Parâmetro | Regra |
|-----------|-------|
| `indicador` | Texto exato não vazio; padrão `"PIB Agropecuária"`. Não normaliza caixa/acento nem inventa aliases |
| `periodicidade` | `"anual"` ou `"mensal"`; seleciona a entidade e a granularidade do horizonte da previsão |
| `top` | Inteiro positivo estrito; tamanho solicitado por página, padrão 1000 |
| `inicio` | Data civil (`date`, `datetime`, ISO ou DD/MM/AAAA), filtro inclusivo na data da pesquisa; não filtra o horizonte previsto |
| `max_registros` | Inteiro positivo ou None; limita a saída após validar integralmente a página recebida |
| `as_polars` | Booleano; True exige `agrobr[polars]` |
| `return_meta` | Booleano; True retorna também MetaInfo |

Bool/float usados como quantidades, datas impossíveis e opções desconhecidas são rejeitados. Não há seleção automática de outra periodicidade para preencher um resultado vazio. Datas futuras são válidas como filtro e podem não ter pesquisas. A unidade depende do indicador e de seu detalhe; a função não converte unidades nem congela revisões históricas.

**Retorno — contrato 2.0:** as dez colunas anteriores permanecem: `indicador`, `data`, `data_referencia`, `media`, `mediana`, `desvio_padrao`, `minimo`, `maximo`, `numero_respondentes`, `base_calculo`. Acrescenta `periodicidade` e `indicador_detalhe`. Data usa datetime64[ns] civil, sem fuso; cinco estatísticas usam float64 anulável; contagem/base usam Int64 anulável. Detalhe é textual anulável, preservado no anual e nulo no mensal; vazio textual publicado é distinto de null. O quadro vazio mantém as doze colunas e tipos.

`data` identifica a pesquisa. `data_referencia` conserva o horizonte textual YYYY ou MM/YYYY, inclusive futuro. Duas bases de cálculo na mesma data/horizonte permanecem separadas. Em Balança comercial anual, Exportações, Importações e Saldo são detalhes distintos; não some ou deduplique essas linhas como uma única expectativa.

**Paginação e limites:** a ordem é Data decrescente, DataReferencia crescente, baseCalculo crescente e, no anual, IndicadorDetalhe crescente. O desempate MM/YYYY é textual; não é uma ordenação cronológica dos horizontes. `max_registros` escolhe os primeiros registros nessa ordem. Página curta não basta para encerrar: sem total/continuação, o cliente avança o offset pelo número recebido até uma página vazia. Duplicatas de identidade dentro ou entre páginas, resposta maior que top, falha de página e envelope sem `value` geram erro, sem retorno parcial.

`source_details.coverage` distingue:

| Estado | Evidência |
|--------|-----------|
| `unknown` | Sem total independente; término observado ou limite local exato sem prova de restante |
| `partial` | Limite local menor que total declarado, ou descarte/continuação comprova dados adicionais |
| `complete` | Contagem declarada reconciliada com a união de chaves e a saída integral |

As consultas conhecidas não trouxeram contagem: `$count=true` não acrescentou total e `/$count` foi recusado. Anotações de total e continuação, se vierem, são validadas. A contagem também não garante uma revisão atômica entre páginas.

**Proveniência e qualidade:** seleção, entidade, filtro, ordem, URLs, offsets, tamanhos pedidos/recebidos/retidos, status, hashes dos corpos, coleta UTC e layout ficam em `source_details`. O hash/tamanho superiores identificam um manifesto canônico de query e recursos. Limite local e inconsistências estatísticas geram avisos mesmo sem `return_meta`. Valores finitos negativos são válidos; relações inconsistentes entre média, mediana, extremos ou desvio são preservadas com diagnóstico. Ausências não viram zero, e JSON/valores não finitos ou campos obrigatórios inválidos geram `ParseError`. Vazio não confirma a existência do indicador.

```python
from agrobr import bcb

anual = await bcb.focus(
    "Balança comercial", inicio="2026-08-28", max_registros=6,
)
mensal, meta = await bcb.focus(
    "IPCA", periodicidade="mensal",
    inicio="2026-08-28", top=100, max_registros=30, return_meta=True,
)
```

Veja [contrato e identidade](../contracts/bcb_focus.md), [fonte e licença](../sources/bcb.md#focus-expectativas-de-mercado) e [migração](../guides/migracao-2.md).

---

## Versão Síncrona

```python
from agrobr.sync import bcb

df = bcb.credito_rural("soja", safra="2024/25")
serie = bcb.sgs("ipca", inicio="01/01/2024")
cambio = bcb.ptax(inicio="01/01/2024", fim="31/01/2024")
expectativas = bcb.focus("PIB Agropecuária")
```

## Fallback

Quando a API OData do SICOR falha, o agrobr usa automaticamente BigQuery (Base dos Dados) como fallback, só no `credito_rural` com `agregacao="uf"` e sem `programa` nem `tipo_seguro` (a tabela não traz essas dimensões). Requer `pip install agrobr[bigquery]` e um projeto GCP para billing: defina `AGROBR_BQ_BILLING_PROJECT=<project-id>` ou configure `billing_project_id` no basedosdados (`~/.basedosdados/config.toml`).

## Notas

- Fonte: [BCB/SICOR](https://olinda.bcb.gov.br) — licença livre
- Dados disponíveis a partir de 2013
- Contrato v2.0 — saída alinhada às agregações reais do SICOR

`inicio` e `fim` substituem os antigos nomes de período, sem aliases. Aceitam ISO, DD/MM/AAAA, `date` e `datetime`; a hora é descartada e `01/02/2024` é 1º de fevereiro. O Focus usa apenas `inicio`; a PTAX mantém `data` para um dia. `as_polars` e `return_meta` exigem nome. Periodicidade Focus e boletim PTAX normalizam caixa; o boletim também aceita acento. No SICOR, envelope sem `value` ou com tipo incorreto levanta `ParseError`; `value=[]` continua sendo um resultado vazio tipado.
