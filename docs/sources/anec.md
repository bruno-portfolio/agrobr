# ANEC — Embarques, Volumes Mensais e Destinos

> **Licença:** `zona_cinza`. Boletins públicos sem termos públicos explícitos de reutilização localizados.

!!! warning "Zona cinzenta"
    A primeira chamada ANEC emite `UserWarning`. Uso ou redistribuição comercial pode exigir autorização da ANEC. Acesso público não comprova permissão para redistribuição comercial.

A Associação Nacional dos Exportadores de Cereais publica boletins PDF com embarques semanais por porto, volumes mensais, comparação anual e participação dos destinos.

## Cobertura e seleção da edição

- **Catálogo:** edições de **2026 ao ano corrente**, com descoberta de categorias anuais no catálogo oficial. Em 6 de setembro de 2026, o catálogo público de 2026 tinha 34 PDFs; o mais recente era W34, criado em 2 de setembro de 2026. Publicação e compatibilidade de layout em anos seguintes não são garantidas.
- **`ano`:** ano da edição, obrigatório e nomeado. Uma edição pode conter dados de outro ano; o parâmetro não consulta um ano histórico independentemente do boletim.
- **`semana`:** semana do boletim, de 1 a 53; `None` seleciona a edição mais recente disponível no catálogo escolhido. O número da semana não é a data de publicação.
- **Produtos:** soja, farelo de soja, milho, DDGS, sorgo e trigo, conforme a tabela selecionada. A comparação anual pode incluir `total_products`. Até a W2/2026, os quadros semanal e mensal publicam só soja, farelo, milho e trigo; DDGS e sorgo entram na W3/2026.
- **Portos semanais:** 19 portos brasileiros reconhecidos, incluindo Santos, Paranaguá e Rio Grande.
- **Dependência:** `pip install agrobr[pdf]`.

## API

```python
from agrobr import anec, datasets

# API e dataset semanais: schema 1.1, com ano, semana e as datas de cada período
df = await anec.embarques(ano=2026, semana=13, porto="paranagua", produto="soja")
df = await anec.embarques(ano=2026, tipo="efetivado")  # ou "programado"

# Schemas de fonte: mensal/destinos 1.1; comparação anual 1.2
mensais = await anec.embarques_mensais(ano=2026, semana=34, produto="soja")
comparacao = await anec.comparacao_anual(ano=2026, produto="total_products")
destinos = await anec.destinos(ano=2026, semana=34, produto="soja")

# Novos datasets semânticos: contratos 1.0
mensais, meta = await datasets.embarques_mensais_anec(
    ano=2026, semana=34, produto="soja", return_meta=True
)
comparacao = await datasets.comparacao_anual_anec(ano=2026)
destinos, meta = await datasets.destinos_anec(ano=2026, return_meta=True)
print(meta.validation_warnings)

itens = await anec.articles_disponiveis(2026)
```

Os parâmetros principais dos três novos datasets são nomeados: `ano: int` obrigatório, `semana: int | None = None`, `produto: str | None = None`, `use_cache: bool = True`, `as_polars: bool = False` e `return_meta: bool = False`. `as_polars=True` requer `pip install agrobr[polars]`. A comparação também aceita `produto="total_products"`. `produto=None` seleciona todos os produtos disponíveis na tabela.

## Schema semanal — `embarques()`

| Coluna | Tipo | Significado |
|---|---|---|
| `porto` | str | Porto canônico, em maiúsculas |
| `produto` | str | Produto canônico |
| `periodo` | str | `last_week` (efetivado) ou `current_week` (programado) |
| `valor_ton` | Float64, nullable | Volume em toneladas |
| `ano` | Int64 | Ano da edição impresso no boletim ("Week 36/2026") |
| `semana` | Int64 | Semana da edição impressa no boletim |
| `data_inicio` | datetime | Primeiro dia do período, lido do rótulo do boletim |
| `data_fim` | datetime | Último dia do período, lido do rótulo do boletim |

O [contrato `embarques_anec`](../contracts/embarques_anec.md) é 1.1, e as quatro colunas acima são opcionais nele. As datas vêm dos rótulos do boletim, como "Last Week (13th to 19th Sep)", nunca da semana ISO. O boletim da semana w traz a semana w em `last_week` e a seguinte em `current_week`, e a semana da ANEC vai de domingo a sábado. No rótulo que cruza o mês, o boletim ora dá o mês do fim ("26th to 01st Aug" é 26/07 a 01/08, em W30/2026), ora o do início ("30th to 05th Aug" é 30/08 a 05/09, em W34/2026). O agrobr fica com a leitura em que as duas semanas se seguem e em que o início da `last_week` fica a até 7 dias da semana da edição (1º de janeiro mais semana − 1 semanas), e o ano da virada de dezembro sai da edição. Quando nenhuma leitura cumpre as duas condições, `data_inicio` e `data_fim` ficam nulas, com `UserWarning` e a mesma mensagem em `MetaInfo.validation_warnings`. A W35/2026 imprime agosto nas duas semanas, que são de setembro ("30th to 05th Aug" e "06th to 12th Aug"): a única leitura seguida, de 30/07 a 12/08, fica a 28 dias da edição, e as datas saem nulas.

Para juntar edições, use `ano`, `semana` e `periodo`, ou as datas: a mesma semana sai como `current_week` (programado) numa edição e como `last_week` (efetivado) na seguinte. A linha TOTAL do quadro não entra no resultado e pode diferir em 1 t da soma dos portos (3 casos em W30/2026 e 1 em W36/2026).

## Volumes mensais — `embarques_mensais()`

As colunas `ano`, `mes`, `produto`, `valor_ton` e `eh_estimativa` são preservadas. A fonte acrescenta `valor_min_ton` e `valor_max_ton`, nullable, além da proveniência abaixo.

Os volumes são **toneladas mensais**, não acumulados de vários meses. Faixas publicadas preservam os dois limites e deixam `valor_ton` nulo, sem ponto médio inventado. Ausência de valor não equivale a zero. Os marcadores de estimativa são preservados; `eh_estimativa=False` não garante volume realizado. O mês sem "*" ainda pode ser revisto: a soja de julho de 2026 saiu 12.182.916 t no boletim 30, 12.040.279 t nos boletins 31 a 34 e 12.108.521 t nos 35 a 37, sempre sem "*". As semanas efetivadas não fecham o mês: rateadas pelos dias, as da soja somam 1,5% (julho) e 3,4% (agosto) a menos que o mensal publicado, e as do milho, 0,4% a mais e 2,5% a menos.

O gráfico mensal de 2025 na página 3 de W34/2026 não é extraído pela tabela mensal atual. A presença de dados de 2025 em `comparacao_anual()` não implica cobertura de 2025 em `embarques_mensais()`.

Veja o [contrato completo `embarques_mensais_anec`](../contracts/embarques_mensais_anec.md).

## Comparação anual — `comparacao_anual()`

O schema de fonte **1.2** inclui `valor_base_ton`, `valor_comparacao_ton`, `ano_base`, `ano_comparacao` e `eh_estimativa`, além de `mes`, `produto` e das legadas `valor_2025`/`valor_2026`. API de fonte e dataset expõem volumes independentes do ano. Os campos legados se referem aos anos literais e ficam nulos quando aquele ano não aparece.

Os anos vêm de pares consecutivos no cabeçalho da tabela. `eh_estimativa` refere-se ao ano comparado. Linhas editoriais são ignoradas, mas cabeçalhos incompletos ou incompatíveis geram `ParseError`. Os valores são toneladas mensais; `total_products` é agregado publicado e não deve ser somado novamente aos produtos individuais. Veja o [contrato do dataset](../contracts/comparacao_anual_anec.md).

## Destinos — `destinos()`

A fonte preserva `produto`, `destino` e `share_pct` nullable (0–100). Acrescenta `ano`, `mes_inicio` e `mes_fim`, nullable, extraídos do cabeçalho do período.

As participações descrevem o **período acumulado do cabeçalho**, não tonelagem mensal. W34/2026 informa janeiro–julho de 2026. Períodos não identificados ficam nulos, sem inferência pela semana da edição. `OTHERS` é válido e percentuais arredondados podem somar 99% ou 101%.

Algumas edições usam gráficos que o parser não extrai, como W08 e W12 de 2026. Quando nenhuma participação é extraída da edição, o retorno inclui aviso em `meta.validation_warnings`. O resultado vazio não comprova ausência de embarques ou destinos. Use `return_meta=True` para inspecionar essa limitação.

Veja o [contrato completo `destinos_anec`](../contracts/destinos_anec.md).

## Proveniência de edição e revisão

As três tabelas de fonte (mensal/destinos 1.1; comparação anual 1.2) e os novos datasets (contratos 1.0) incluem:

| Coluna | Significado |
|---|---|
| `ano_relatorio` | Ano da edição |
| `semana_relatorio` | Semana da edição |
| `edicao_id` | `cuid` do artigo ANEC |
| `publicado_em` | `created_at` do artigo, datetime UTC |
| `revisado_em` | `media_updated_at` do arquivo, datetime UTC |

As chaves dos datasets incluem `edicao_id` e `revisado_em`. Preserve essas dimensões ao armazenar retratos dos boletins para não sobrescrever projeções anteriores. A atualização do arquivo pode anteceder a criação do artigo; os campos descrevem objetos distintos na origem.

## Aliases de produto aceitos

| Input | Canônico |
|---|---|
| `soja`, `soja grão`, `soja grao`, `soja em grão`, `soja em grao`, `soybean`, `soybeans` | `soybean` |
| `farelo`, `farelo de soja`, `soybean meal`, `soybean_meal`, `soybeanmeal`, `soymeal`, `meal` | `soybean_meal` |
| `milho`, `maize`, `corn` | `maize` |
| `trigo`, `wheat` | `wheat` |
| `sorgo`, `sorghum` | `sorghum` |
| `ddgs` | `ddgs` |

## Correspondência com os nomes do agrobr

| Código ANEC (`produto`) | Produto | Nome no agrobr |
|---|---|---|
| `soybean` | soja em grão | `soja` |
| `soybean_meal` | farelo de soja | `farelo_soja` |
| `maize` | milho | `milho` |
| `wheat` | trigo | `trigo` |
| `sorghum` | sorgo | `sorgo` |
| `ddgs` | DDGS (grãos secos de destilaria) | sem equivalente |

`produto` conserva o código da ANEC, em inglês. O nome no agrobr é o canônico de `normalize.crops`, o mesmo de `exportacao` e `estimativa_safra`; o `normalizar_cultura` converte cada código no nome da tabela, e `ddgs` fica como está.

## Cache

PDF cacheado em `~/.agrobr/cache/anec/{year}/week_{NN}/` com:
- `shipment.pdf` — bytes do PDF
- `meta.json` — metadata + SHA256 + `media_updated_at` da fonte

Onde fica e como limpar: [O que o agrobr grava no disco](../advanced/disco.md).

Cada escrita usa um temporário exclusivo e substitui o arquivo de destino
atomicamente. A limpeza remove somente o temporário dessa operação; arquivos
temporários de outras chamadas ou execuções anteriores são preservados.

Stale detection compara `media_updated_at` cached vs ANEC. ANEC revisa
retroativamente: cache antigo invalida automaticamente quando `updated_at`
remoto avança.

O cache do relatório interpretado também identifica a revisão por artigo,
`media_updated_at`, URL e SHA-256 dos bytes recebidos. Uma revisão atualiza
os dados e a URL de proveniência, mesmo mantendo o mesmo `cuid`.

Listagem JSON é cacheada em memória por 5 minutos (configurável via
`AGROBR_ANEC_LIST_TTL`). Cache de PDF disk pode ser desabilitado via
`AGROBR_ANEC_CACHE_DISABLED=1`.

Quando o PDF vem do cache de disco, o `MetaInfo` traz `from_cache=True` e, em `fetched_at`, a coleta original
gravada no `meta.json`, não o instante da chamada; `source_details["media_updated_at"]` é a revisão da edição
conferida contra a listagem. Num download, `from_cache=False` e `fetched_at` é o próprio download.

## MetaInfo

```python
df, meta = await anec.embarques_mensais(ano=2026, return_meta=True)
print(meta.source)              # "anec"
print(meta.schema_version)      # "1.1" nesta API de fonte; "1.0" no dataset
print(meta.source_url)          # PDF selecionado
print(meta.validation_warnings)
```

`raw_content_hash` é o SHA-256 completo do PDF, e `raw_content_size`, o tamanho dele em bytes, do download ou do cache de disco (o mesmo `pdf_sha256` do `meta.json`). O fingerprint do layout do PDF (MD5 da estrutura) fica em `source_details["layout_fingerprint"]`.

## Limites e exemplos oficiais

Layouts PDF e produtos publicados variam entre edições. A ANEC revisa valores retroativamente; consultar o último boletim não reconstrói todas as projeções passadas. Retratos históricos exigem preservar cada edição e revisão.

Os quadros mensal e de comparação anual não são intercambiáveis: em W13/2026, por exemplo, trigo em janeiro aparece com 279.699 t no quadro mensal e 279.499 t no comparativo. O agrobr preserva os valores publicados em cada quadro, sem reconciliação automática. O total impresso na linha de janeiro do quadro mensal (7.727.420 t) fecha com os valores do comparativo (trigo 279.499 t e DDGS 80.057 t), não com os do próprio quadro mensal (279.699 t e 80.141 t), que somam 284 t a mais (W35 e W37/2026).

PDFs oficiais citados nesta página: [W13/2026](https://www.anec.com.br/uploads/cmnrsz3eu00004htx396a7you.pdf) e [W34/2026](https://www.anec.com.br/uploads/cmtkhj4p50000y9tx5nnn6hhi.pdf). Fonte: [ANEC](https://www.anec.com.br/).

Categorias anuais ausentes do mapa configurado são descobertas no catálogo oficial. Anos explícitos são exclusivos: ausência de publicação não recua para ano anterior. Anos vazios usam o TTL da listagem (padrão 300 segundos; `AGROBR_ANEC_LIST_TTL=0` desativa). Edições suportadas vão de 2026 ao ano corrente; descobrir a categoria não garante por si só um layout PDF compatível.

## Leitura dos boletins

Os valores de cada quadro são mantidos separadamente. Toneladas são literais;
células vazias e traços não viram zero. Somatórios percentuais apresentados nos
destinos podem resultar em 99%, 101% ou 102% por arredondamento da publicação;
o agrobr não redistribui essas participações para forçar 100%.

O parser aceita os dois cabeçalhos publicados: os seis produtos nos dois períodos
do quadro semanal (e mais Total Products no mensal), da W3/2026 em diante, e os
quatro das edições até a W2/2026, lidos pelo nome. Coluna sem nome de produto, como
a da soja na semana corrente da W14/2026, gera `ParseError`: o agrobr não adivinha
o produto. Cabeçalho incompleto também gera `ParseError`; o dataset preserva a
causa em `SourceUnavailableError`. A soma dos portos de cada coluna do quadro
semanal é conferida com a linha TOTAL do boletim: divergência acima do
arredondamento (0,5 t por porto) sai em aviso (`UserWarning` e
`meta.validation_warnings`), com os valores por porto repassados sem ajuste. O
pdfplumber às vezes parte um número em 2 palavras coladas (`3` e `72.958`, na linha
TOTAL da [W36/2026](https://www.anec.com.br/uploads/cmu47h0m500016vtxg76q4eeb.pdf)) e, na fonte pequena de 2025, funde o BELÉM com o RIO da linha de
baixo. O parser junta 2 palavras numéricas vizinhas quando formam um número com
separador de milhar e lê as palavras com tolerância vertical de 1 pt. Em 89
edições (W1/2025 a W37/2026, menos a W14/2026), as 16.112 células do quadro
semanal batem com o PDF e nenhum aviso sai; a W25/2026 publicou a linha
TOTAL em branco, e nela não há conferência. Nas
W1 e W2/2026, o quadro "Monthly shipments 2026" está na segunda página e não é
lido: `embarques_mensais` dessas edições traz só 2025, e janeiro de 2026 aparece
nos boletins seguintes. A extração
de destinos usa apenas a tabela à direita do mapa: rótulos percentuais desenhados
no mapa não são destinos. Nas edições 08 e 12/2026 os quadros são imagens, sem
tabela de texto extraível: o vazio acompanhado de aviso permanece uma limitação,
sem comprovar ausência de embarques.

Outros layouts não são garantidos, nem a equivalência entre embarques ANEC e
exportações aduaneiras ComexStat.
De janeiro a agosto de 2026, as duas séries divergem mês a mês em até 49% (milho: −44% em abril e +49% em
julho) e fecham no acumulado dentro de 2,5% (soja +1,6%, farelo +0,2% e milho −2,4%).
