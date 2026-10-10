# API CONAB

O módulo CONAB fornece acesso a safras, balanço oferta/demanda, totais Brasil, custos de produção, série histórica, progresso de safra e preços de atacado (CEASA) da Companhia Nacional de Abastecimento.

## Transporte HTTP e navegador opcional

As APIs de safras e balanço usam HTTP primeiro. Playwright e Chromium são opcionais para fallback de transporte:

```bash
pip install agrobr[browser]
python -m playwright install chromium
```

Se HTTP e o transporte opcional falharem, as APIs levantam `SourceUnavailableError`. Na camada de datasets, `estimativa_safra` pode tentar IBGE LSPA quando a seleção permite fallback; `balanco` não possui fonte alternativa.

## Funções

### `safras`

Obtém dados de safra por produto e UF.

```python
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Produto: 'soja', 'milho', 'arroz', etc. |
| `safra` | `str \| None` | Safra no formato '2024/25'. Default: última |
| `uf` | `str \| None` | UF (ex: 'MT', 'PR'). Default: todas |
| `levantamento` | `int \| None` | Levantamento da própria safra (1-12). Default: publicação mais recente que traz a safra |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna tupla `(df, MetaInfo)` com proveniência |

**Retorno:**

DataFrame com colunas:
- `fonte`: Fonte dos dados
- `produto`: Produto
- `safra`: Ano-safra
- `uf`: Unidade federativa
- `area_plantada`: Área plantada (mil ha)
- `area_colhida`: Nula; o levantamento não publica área colhida separada
- `produtividade`: Produtividade (kg/ha)
- `producao`: Produção (mil t)
- `levantamento`: Número do levantamento da publicação usada
- `data_publicacao`: Data da publicação usada

A CONAB publica uma única área, rotulada "ÁREA (Em mil ha)", mantida em `area_plantada`. `area_colhida` fica nula na rota CONAB; área colhida distinta só existe na rota LSPA. Não há cópia nem imputação entre as duas áreas. O contrato CONAB V2 já permite essa nulidade.

Nas abas com cabeçalho em ano civil (trigo, aveia, canola, centeio, cevada e triticale), o ano publicado é o ano de encerramento do biênio do contrato (`Safra 2026` → `2025/26`). Um levantamento que ainda não publica esse ano não fornece estimativa para a safra solicitada. A série histórica conserva seu período anual próprio.

De out/2019 a jan/2022, as abas desses seis cereais levam o ano no nome ("Trigo 2021"), e a edição pode trazer também a aba do ano anterior, às vezes com o cabeçalho de safra quebrado. O agrobr lê a aba mais recente cujo cabeçalho publica a safra pedida: no 12º levantamento de 2020/21 (set/2021), o trigo 2019/20 sai da coluna "Safra 2020" de "Trigo 2021", e não da cópia "Trigo 2020", cujo cabeçalho traz "23". Nos levantamentos 1 a 4 de 2019/20, 3 e 4 de 2020/21 e 2 a 4 de 2021/22, a aba ainda não traz o inverno da própria safra, e a consulta sai vazia. A CONAB publicou só em PDF os levantamentos 7, 8, 9 e 12 de 2019/20, 1 e 2 de 2020/21 e 1 de 2021/22, que por isso não estão no catálogo.

**Safra passada.** Sem `levantamento`, a safra vem da publicação mais recente que a traz. A CONAB republica a safra anterior, revisada, nos levantamentos da safra seguinte: `conab.safras('gergelim', safra='2024/25', uf='MT')` devolve 695 mil ha (12º levantamento de 2025/26, set/2026); com `levantamento=12`, vem o 12º levantamento da própria safra 2024/25 (set/2025, 401,2 mil ha) e um aviso de que existe publicação mais recente. `levantamento` e `data_publicacao` são sempre os da publicação usada; `MetaInfo.source_details["publicacao"]` registra `levantamento`, `safra` (da publicação), `data_publicacao` e `url`. `brasil_total(safra=...)` e `balanco(safra=...)` seguem a mesma regra.

**Safra antiga.** A aba de produto de cada edição traz só a safra corrente e a anterior. Duas ou mais safras atrás da edição mais recente do catálogo, a revisão da CONAB só aparece na série histórica (por UF) e na aba Suprimento (Brasil, só a produção de seis produtos). Nesse caso, sem `levantamento`, `safras` lê a série histórica do mesmo produto, a mesma de `serie_historica_safra`, se a referência dela for posterior à edição do boletim que traria a safra. A referência é a legenda da planilha ("Estimativa em setembro/2026"), porque a série não publica data. Se a série não for mais nova, vale a edição do boletim, com aviso. Nas linhas da série, `levantamento` e `data_publicacao` ficam nulos, e `MetaInfo.source_details["publicacao"]` traz `origem="serie_historica"` e, por série, `produto`, `url`, `sha256` e `referencia`. Exemplo: soja 2022/23 sai com 159.154,3 mil t (série e aba Suprimento), e não com os 154.609,5 do 12º levantamento de 2023/24. Com `levantamento=N`, vem a edição original. `datasets.estimativa_safra` e a rota CONAB de `datasets.producao_anual` seguem a mesma regra. Na safra que acabou de sair do boletim, a série é conferida com a última edição que a publicou (ver `brasil_total`).

**Soma das UFs × BRASIL.** Quando a soma das UFs entregues não fecha com a linha BRASIL publicada além do arredondamento (0,05 por UF somada, mais 0,05 do próprio BRASIL), o agrobr emite um aviso por safra e coluna (`area_plantada`, `producao`) e repassa os números publicados. Planilha que lista só parte das UFs deixa as outras no BRASIL (o café da série lista 10 ou 11); ali, só a soma acima dele é incoerente. Com `uf`, a conferência é a mesma, sobre a UF entregue. Exemplos: milho 2ª safra 2020/21 no 12º levantamento de 2021/22 (as 27 UFs somam 59.981,5 mil t, e o BRASIL publicado é 60.741,6) e trigo 2003/04 pela série histórica (o BRASIL publicado de 2004 não fecha com as UFs). `serie_historica` confere da mesma forma cada período e coluna de área e de produção que entrega.


**Exemplo:**

```python
from agrobr import conab

# Todas as UFs
df = await conab.safras('soja', safra='2024/25')

# Apenas Mato Grosso
df = await conab.safras('soja', safra='2024/25', uf='MT')

# Levantamento específico
df = await conab.safras('soja', safra='2024/25', levantamento=5)
```

---

### `balanco`

Obtém balanço de oferta e demanda.

Sem `levantamento`, `safra` escolhe a publicação mais recente cuja aba Suprimento traz essa safra: a edição corrente cobre as últimas sete safras (seis na soja), já revisadas (trigo 2024/25: 7.873,4 mil t em set/2026, contra 7.536,1 na edição de set/2025). A tabela devolvida é a dessa publicação e pode conter linhas de vários períodos. Com `levantamento=N`, vem o N-ésimo levantamento da própria safra, que é a edição original, com aviso quando há publicação mais recente. Para o trigo, os períodos do balanço permanecem anuais (`2025` corresponde a 2024/25). A última revisão publicada de cada produto e período prevalece. `MetaInfo.source_details["publicacao"]` registra a edição usada. Quando uma linha publicada não fecha uma das identidades do balanço além do arredondamento (0,05 mil t por termo), o agrobr emite um aviso por linha e repassa os números publicados. As identidades são: estoque inicial + produção + importação = suprimento; consumo + exportação = demanda total; suprimento − consumo − exportação = estoque final. Exemplo: arroz 2024/25 em set/2026, com −363,8 mil t na última.


```python
async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    *,
    as_polars: bool = False,
    levantamento: int | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str \| None` | Produto específico ou todos; sem ele, a soja vem da aba própria "Suprimento - Soja". Aceita `soja`, `milho`, `arroz`, `feijao`, `trigo` e `algodao`, com ou sem acento; outro valor levanta `InvalidParameterError` antes da rede |
| `safra` | `str \| None` | Safra. Default: publicação mais recente |
| `levantamento` | `int \| None` | Levantamento da própria safra (1-12), para a edição original |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna tupla `(df, MetaInfo)` com proveniência |

**Retorno:**

DataFrame com colunas:
- `produto`: Produto
- `safra`: Ano-safra
- `levantamento`: Rótulo da revisão da linha, quando publicado (texto/data; não é seletor)
- `estoque_inicial`: Estoque inicial (mil t)
- `producao`: Produção (mil t)
- `importacao`: Importação (mil t)
- `suprimento`: Suprimento total (mil t), como publicado na aba Suprimento; na aba própria da soja, estoque inicial + produção + importação, somados pelo agrobr
- `consumo`: Consumo (mil t), como publicado; na aba da soja, sementes/outros + processamento, somados pelo agrobr
- `exportacao`: Exportação (mil t)
- `demanda_total`: Demanda total (mil t); nula no wide da soja e no layout longo legado sem essa coluna
- `estoque_final`: Estoque final (mil t)
- `unidade`: Unidade (`mil_ton`)

As oito métricas são float64, inclusive em resultados vazios. `levantamento` e `demanda_total` estão sempre presentes, com nulo quando não publicados. O metadado `schema_version` do balanço é `1.1`.

**Exemplo:**

```python
from agrobr import conab

# Balanço de soja
df = await conab.balanco('soja')

# Todos os produtos
df = await conab.balanco()
```

---

### `brasil_total`

Obtém totais nacionais de produção.

```python
async def brasil_total(
    safra: str | None = None,
    *,
    as_polars: bool = False,
    levantamento: int | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Retorno:**

DataFrame com totais Brasil para todos os produtos. Com `safra`, os números vêm da publicação mais recente que traz a safra, como em `safras`; com `levantamento=N`, da edição original.

Para safra antiga (duas ou mais atrás da edição mais recente), sem `levantamento`, cada uma das 36 linhas de produto sai da linha BRASIL da série histórica correspondente. Os dois `SUBTOTAL` e o `BRASIL (2)` são recalculados como soma, na regra da própria CONAB: linhas de produto de topo, sem a pluma. A produtividade dessas três linhas é produção ÷ área. Isso custa cerca de 35 downloads de série por chamada, um por vez pelo limitador da CONAB. Se uma série falhar, a chamada inteira levanta `SourceUnavailableError`, com a linha de produto que faltou e a sugestão de `levantamento=`; nunca volta com linhas faltando. `MetaInfo.source_details["publicacao"]` traz `origem="serie_historica"` e a `url`, o `sha256` e a `referencia` de cada série.

Na soma, ausência não é zero. Cultura cuja série começa depois da safra pedida (ainda não levantada, como o gergelim, cuja série começa em 2018/19) fica fora do subtotal, com aviso em `MetaInfo.validation_warnings` que a nomeia com o primeiro período da série. Qualquer outra parte sem a safra deixa o subtotal e o `BRASIL (2)` nulos, com aviso; subtotal sem nenhuma parte também sai nulo, nunca 0. Exemplo: 1975/76 é anterior a todas as séries de verão, e o subtotal de verão e o `BRASIL (2)` saem nulos, com o de inverno em 142,5 mil ha.

Nesse caminho, as partes são conferidas com o total publicado: Cores + Preto + Caupi = cada safra do feijão, as três safras = FEIJÃO TOTAL, e as safras do amendoim e do milho e os sistemas do arroz = o total de cada um. A linha BRASIL de cada série é a soma das UFs arredondadas, e a identidade herda esse arredondamento: além de 1,4 (0,05 por UF, mais 0,05 do total), o agrobr emite um aviso por identidade e coluna e repassa os números publicados. Exemplo: 2021/22, em que a série do feijão 3ª safra publica 707,2 mil t e os tipos somam 748,0 (Cores 700,6).

Na safra que acabou de sair do boletim (duas atrás da edição mais recente, a mais nova servida pela série), `safras` e `brasil_total` também baixam a última edição do boletim que a publicou e conferem o BRASIL de cada linha com o da série: a legenda da série pode ser posterior sem que a coluna tenha sido revista. O dado continua saindo da série; divergência além de 0,1 gera aviso com os dois valores, e `MetaInfo.source_details["publicacao"]["conferencia"]` registra a edição conferida e as `divergencias` (lista vazia quando bate). Exemplo, com a série de setembro/2026 e o 12º levantamento de 2025/26: gergelim 2024/25 com 399,4 mil t na série e 610,9 no boletim.

`produto` é o identificador normalizado, sem notas de rodapé (`soja`, `algodao_caroco`, `feijao_cores_1`, `subtotal`, `brasil`). `rotulo` preserva o texto original da coluna A, como `ALGODÃO - CAROÇO (1)` e `BRASIL (2)`. `grupo` mantém o título do bloco publicado: a época do feijão para `Cores`, `Preto` e `Caupi`, o produto pai para detalhes como `Milho 1ª Safra` e `CULTURAS DE INVERNO` para culturas e subtotal de inverno; é nulo nas demais linhas. `produto`, `grupo` e `safra` identificam a linha.

**Tipos e vazio:** `area_plantada`, `produtividade` e `producao` usam `float64`, nas unidades `mil_ha`, `kg/ha` e `mil_ton`. Valores ausentes continuam nulos. O schema 2.0, também descrito por `CONAB_BRASIL_TOTAL_V2`, inclui as mesmas nove colunas em resultados vazios. Cabeçalho ausente ou alterado gera `ParseError`.

### `serie_historica`

```python
async def serie_historica(
    produto: str,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

Os limites são anos inteiros inclusivos, aplicados ao ano inicial de `safra`. Substitua `inicio=`/`fim=` por `ano_inicio=`/`ano_fim=`. Os nomes das métricas e suas unidades permanecem no [contrato 1.1](../contracts/serie_historica_safra.md).

### `cana_industria`

```python
async def cana_industria(
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    uf: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

Série histórica industrial da cana (`canaseriehist-industria.xls`): uma linha por safra e UF com açúcar (mil t), etanol anidro e hidratado de cana e de milho e etanol total (mil litros) e ATR médio (kg/t de cana), nas unidades publicadas. `etanol_total_mil_l` inclui o etanol de milho: não é o etanol de cana. Filtros e validação antes da rede são os de `serie_historica`; `serie_historica("cana_industria")` levanta `InvalidParameterError` apontando para esta função. Zero × vazio, avisos e erros de layout estão no [contrato 1.0](../contracts/producao_acucar_etanol.md); o dataset é `datasets.producao_acucar_etanol`.

### `produtos_serie_historica`

`conab.produtos_serie_historica()` devolve uma lista de dicionários com `produto`, `categoria` e `url`, sem requisição. É o catálogo da série histórica; `conab.produtos()` lista os produtos dos levantamentos mensais.

Use as funções em `agrobr.conab`, inclusive `custo_producao` e `serie_historica`. Os antigos subpacotes homônimos foram removidos. As funções CEASA canônicas são `ceasa_precos`, `ceasa_produtos`, `ceasa_categorias` e `lista_ceasas`; os atributos equivalentes em `conab.ceasa` continuam acessíveis, fora de `__all__`.

---

### `levantamentos`

Lista levantamentos disponíveis.

```python
async def levantamentos() -> list[dict]
```

**Retorno:**

Lista de dicionários descrevendo cada levantamento publicado (safra, número e metadados).

---

### `produtos`

Lista produtos disponíveis (códigos aceitos em `safras()`).

```python
async def produtos() -> list[str]
```

---

### `ufs`

Lista as 27 UFs disponíveis.

```python
async def ufs() -> list[str]
```

---

## Funções relacionadas

O módulo CONAB também expõe (documentadas em páginas próprias ou nos contratos):

- `custo_producao(produto, uf=..., planilha=..., aba=...)` / `custo_producao_total(...)` — custos de produção por hectare; `as_polars` e `return_meta` só por nome. `catalogo_custos(produto)` lista as planilhas agrícolas, e produto sem planilha no catálogo levanta `InvalidParameterError` com as culturas publicadas; com `planilha=...`, lista abas e contextos reconhecidos ou pendentes, com `data_referencia` em `datetime64[ns]`. Qualquer produto com múltiplas planilhas ou contextos candidatos exige seleção explícita por `planilha` e `aba`, até identificar um contexto único; a API informa os candidatos e não escolhe uma revisão automaticamente. Café usa `cafe_arabica` ou `cafe_conilon`. Subtotal ou total de fórmula que não fecha com os itens publicados gera aviso em `meta.validation_warnings`. Ver os oito produtos do dataset e sua semântica no contrato [custo_producao](../contracts/custo_producao.md)
- `serie_historica(produto, ...)` — série histórica de safras (45 produtos, com início conforme produto). Café inclui áreas em produção/formação e conversões explícitas para mil ha, mil toneladas e kg/ha; cana publica a área colhida em `area_colhida_mil_ha`. Avisa quando a soma das UFs não fecha com o BRASIL publicado, na regra de `safras`. Ver contrato [serie_historica_safra](../contracts/serie_historica_safra.md)
- `cana_industria(ano_inicio, ano_fim, uf)` — açúcar, etanol de cana e de milho e ATR por safra e UF; `etanol_total_mil_l` inclui o milho. Avisa quando o total não fecha com as quatro parcelas ou a soma das UFs não fecha com o BRASIL, e repassa os números publicados. Ver contrato [producao_acucar_etanol](../contracts/producao_acucar_etanol.md)
- `progresso_safra(...)` / `semanas_disponiveis()` — progresso semanal de plantio/colheita. Ver [API CONAB Progresso](conab_progresso.md)
- `ceasa_precos(...)` / `ceasa_produtos()` / `ceasa_categorias()` / `lista_ceasas()` — preços de atacado hortifrúti. Ver [API CONAB CEASA](conab_ceasa.md)

### Cache

Cache do catálogo: **1 h em processo**. O parâmetro nomeado `use_cache=False` em `conab.catalogo_custos`, `conab.custo_producao` e `datasets.custo_producao` força nova leitura sem consultar ou atualizar o cache. As planilhas são baixadas a cada consulta. `meta.source_details["catalog_cache"]` informa `hit`, `miss` ou `bypass`; os recibos originais do catálogo ficam em `manifest.acquisition.catalog_acquisition`, separados das requisições da chamada atual.

---

## Modelos

### `Safra`

```python
class Safra(BaseModel):
    fonte: Fonte
    produto: str
    safra: str = Field(..., pattern=r"^\d{4}/\d{2}$")
    uf: str | None = Field(None, min_length=2, max_length=2)
    area_plantada: Decimal | None = Field(None, ge=0)
    producao: Decimal | None = Field(None, ge=0)
    produtividade: Decimal | None = Field(None, ge=0)
    unidade_area: str = Field(default="mil_ha")
    unidade_producao: str = Field(default="mil_ton")
    levantamento: int = Field(..., ge=1, le=12)
    data_publicacao: date | None = None
    meta: dict[str, Any] = Field(default_factory=dict)
    parsed_at: datetime = Field(default_factory=utcnow)
    parser_version: int = Field(default=1)
    anomalies: list[str] = Field(default_factory=list)
```

## Produtos Disponíveis

`produtos()` retorna 25 códigos (incluindo aliases agregados e sub-safras):

| Código | Produto |
|--------|---------|
| `soja` | Soja |
| `milho` | Milho (total) |
| `milho_1` | Milho 1ª safra |
| `milho_2` | Milho 2ª safra |
| `milho_3` | Milho 3ª safra |
| `arroz` | Arroz (total) |
| `arroz_irrigado` | Arroz irrigado |
| `arroz_sequeiro` | Arroz sequeiro |
| `feijao` | Feijão (total) |
| `feijao_1` | Feijão 1ª safra |
| `feijao_2` | Feijão 2ª safra |
| `feijao_3` | Feijão 3ª safra |
| `algodao` | Algodão (total) |
| `algodao_pluma` | Algodão em pluma |
| `trigo` | Trigo |
| `sorgo` | Sorgo |
| `aveia` | Aveia |
| `cevada` | Cevada |
| `canola` | Canola |
| `girassol` | Girassol |
| `mamona` | Mamona |
| `amendoim` | Amendoim |
| `centeio` | Centeio |
| `triticale` | Triticale |
| `gergelim` | Gergelim |

## Versão Síncrona

```python
from agrobr.sync import conab

df = conab.safras('soja', safra='2024/25')
df = conab.balanco('milho')
```

## Sociobiodiversidade

`conab.custo_sociobiodiversidade(produto, uf=None, ano=None, *, local=None, planilha=None, aba=None, use_cache=True, as_polars=False, return_meta=False)` retorna os custos extrativistas publicados. O dataset tem os mesmos seletores. `conab.catalogo_sociobiodiversidade()` lista todas as revisões de recursos com indicação de ativo; com produto, inventaria o workbook ativo, incluindo contextos pendentes. `planilha=` seleciona um recurso histórico exato. Os 20 produtos capturados, unidades literais, seleção e limitações nominais estão no [contrato 1.0](../contracts/custo_sociobiodiversidade.md). Sem conversão hectare/safra nem mescla de revisões. Cache do catálogo de 1 h, separado dos custos agrícolas; workbooks sempre baixados. `use_cache=False` ignora o cache do catálogo.

Custos agrícolas usam parser 5: cabeçalhos mesclados, café por recurso oficial,
safra anual ou bienal literal e notas cambiais separadas dos itens. Percentuais
só recebem escala Excel quando a célula é numérica. Custos da sociobiodiversidade
usam parser 2 e preservam a seleção explícita de revisões arquivadas. Contratos
3.0 e 1.0, com o texto no dtype padrão do pandas instalado; terceira medida monetária continua recusada.

O parser de levantamento 3 reconhece o trigo histórico em abas como `Trigo 2021`,
selecionadas pelo ano de encerramento da safra solicitada. Se duas abas correspondem
à mesma seleção, a leitura falha com `ParseError`; a ordem das abas não decide o ano.

## Catálogos e normalização de produtos

`produtos()` e `ufs()` são catálogos locais, sem acesso à rede, e mantêm a assinatura assíncrona:
use `await conab.produtos()` e `await conab.ufs()`. Na fachada síncrona, use `sync.conab.produtos()`
e `sync.conab.ufs()`.

`safras()` normaliza caixa, espaços externos e aliases com acento, como `" FEIJÃO "`, antes da
consulta e da seleção das linhas. `ceasa_precos(produto=...)` confere o produto depois da rede, contra a publicação recebida, sem
acento e sem caixa nos 2 lados: produto publicado fora de `ceasa_produtos()` também filtra, e o que
não consta da publicação levanta `InvalidParameterError` com os publicados.
