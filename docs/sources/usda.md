# USDA PSD — Estimativas Internacionais

United States Department of Agriculture — Production, Supply, Distribution.
Estimativas internacionais de produção, oferta e demanda agrícola.

## Configuração

Requer API key gratuita. A página OpenData da FAS manda obter a chave no api.data.gov:

1. Registre-se em [api.data.gov/signup](https://api.data.gov/signup/)
2. Configure a variável de ambiente:

```bash
export AGROBR_USDA_API_KEY="sua-key-aqui"
```

Ou passe diretamente:

```python
df = await usda.psd("soja", api_key="sua-key-aqui")
```

A chave vai só no cabeçalho `X-Api-Key`, nunca na URL. Sem chave, `usda.psd` levanta `SourceUnavailableError` antes
de qualquer requisição. Chave recusada pelo gateway (HTTP 403 `API_KEY_INVALID` ou `API_KEY_MISSING`) também sai como
`SourceUnavailableError`, com o código do gateway.

## API

```python
from agrobr import usda

# Dados PSD do Brasil para soja
df = await usda.psd("soja", country="BR", market_year=2024)

# Todos os países
df = await usda.psd("soja", country="all", market_year=2024)

# Dados mundiais agregados
df = await usda.psd("milho", country="world")

# Filtrar por atributos: nome oficial do PSD ou rótulo do agrobr
df = await usda.psd("soja", attributes=["Production", "exportacao"])

# Pivotar atributos como colunas
df = await usda.psd("soja", pivot=True)
```

Sem `market_year`, a consulta pede o ano-calendário corrente e, se o PSD ainda não publicou nada dele, o ano
anterior: de janeiro até o WASDE de maio, que abre o ano-safra novo, o ano corrente vem vazio. O `MetaInfo` registra
em `source_details` o ano usado (`market_year`), os pedidos (`market_year_tentados`) e se o padrão valeu
(`market_year_padrao`). Com `market_year` explícito, não há recuo: ano sem publicação devolve a tabela vazia.
O gateway responde HTTP 404 para um ano sem dado (soja do Brasil em 1950, por exemplo): a tabela vazia vem com aviso
em `validation_warnings` e `UserWarning`, porque o mesmo 404 sai se a URL da API mudar.

## Colunas — `psd`

| Coluna | Tipo | Descrição |
|---|---|---|
| `commodity_code` | str | Código PSD da commodity (7 dígitos) |
| `commodity` | str | Nome do agrobr (`soja`, `milho`...); fora do cadastro, o nome oficial do catálogo |
| `country_code` | str | Código de país do PSD, que não é ISO (`CH` é a China, `E4` a União Europeia); `00` é o mundo |
| `country` | str | Nome oficial do catálogo de países; `World` no agregado mundial |
| `market_year` | int | Ano-safra do USDA, literal do corpo (`2024` é a safra 2024/25) |
| `attribute` | str | Nome oficial do atributo (`Production`, `Exports`...) |
| `attribute_br` | str | Rótulo do agrobr para os atributos do balanço (tabela abaixo); nulo nos demais |
| `value` | float | Valor publicado, na unidade de `unit` |
| `unit` | str | Unidade oficial do catálogo: `(1000 MT)`, `(1000 HA)`, `(MT/HA)`, `1000 480 lb. Bales`, `(1000 60 KG BAGS)`... |
| `attribute_id` | int | `attributeId` do PSD |
| `unit_id` | int | `unitId` do PSD |
| `last_update_year` | int | Ano da última atualização da série (país × ano-safra); não é a edição consultada |
| `last_update_month` | Int64 | Mês da última atualização da série; nulo nas séries antigas, em que o PSD publica `00` |

## Atributos do balanço

| `attribute_br` | ID | Nome oficial |
|---|---:|---|
| `area_colhida` | 4 | Area Harvested |
| `estoque_inicial` | 20 | Beginning Stocks |
| `producao` | 28 | Production |
| `importacao` | 57 | Imports |
| `oferta_total` | 86 | Total Supply |
| `exportacao` | 88 | Exports |
| `consumo_domestico` | 125 | Domestic Consumption |
| `consumo_domestico` (açúcar) | 126 | Total Disappearance |
| `consumo_domestico` (algodão) | 142 | Domestic Use |
| `perdas` (algodão) | 150 | Loss |
| `estoque_final` | 176 | Ending Stocks |
| `distribuicao_total` | 178 | Total Distribution |
| `produtividade` | 184 | Yield |

Nos 9 produtos da tabela de commodities, `estoque_inicial + producao + importacao = oferta_total` e
`exportacao + consumo_domestico + perdas + estoque_final = distribuicao_total = oferta_total`. Os outros atributos
publicados (esmagamento, uso para ração, estoque sobre uso etc.) saem com o nome oficial em `attribute` e `attribute_br`
nulo.

## Commodities

| Nome agrobr | Código | Commodity USDA | Unidade |
|---|---|---|---|
| `soja` | 2222000 | Oilseed, Soybean | 1000 t |
| `milho` | 0440000 | Corn | 1000 t |
| `trigo` | 0410000 | Wheat | 1000 t |
| `algodao` | 2631000 | Cotton | 1000 fardos de 480 lb |
| `arroz` | 0422110 | Rice, Milled | 1000 t |
| `cafe` / `coffee` | 0711100 | Coffee, Green | 1000 sacas de 60 kg |
| `acucar` / `sugar` | 0612000 | Sugar, Centrifugal | 1000 t |
| `farelo_soja` / `soybean_meal` | 0813100 | Meal, Soybean | 1000 t |
| `oleo_soja` / `soybean_oil` | 4232000 | Oil, Soybean | 1000 t |

Área em 1000 ha e produtividade em t/ha (algodão em kg/ha). Qualquer outro `commodityCode` do catálogo oficial também é
aceito. Código de commodity, país ou atributo fora dos catálogos levanta `InvalidParameterError` antes da rede; o
gateway devolveria `[]` sem erro.

Pecuária também sai pela fonte, pelo código do catálogo: rebanho bovino (`0011000`) e suíno (`0013000`), em
`(1000 HEAD)`, e carnes bovina (`0111000`), suína (`0113000`) e de frango (`0115000` e `0114200`), em
`(1000 MT CWE)`. No rebanho, `producao` conta cabeças, não carne, e o abate (`Total Slaughter`) e as perdas saem com
`attribute_br` nulo: a identidade do balanço acima não fecha pelos rótulos. O dataset `oferta_demanda_global` aceita
só os 9 produtos da tabela.

O rebanho bovino do USDA não é o da PPM do IBGE (`datasets.pecuaria_municipal`): o `Beginning Stocks` é a estimativa do USDA para o início do ano, e a PPM é o efetivo levantado pelo IBGE por município para o ano de referência. A diferença é de ordem de grandeza relevante: 186,9 milhões de cabeças no `Beginning Stocks` de 2025 contra 238,2 milhões na PPM de 2024 (21,5 % a menos no USDA). Não compare nem junte as 2 séries como se fossem a mesma medida.

## Catálogos

Os nomes vêm dos catálogos oficiais do gateway (`commodityAttributes`, `commodities`, `countries` e `unitsOfMeasure`),
guardados no pacote em `agrobr/usda/catalogos/`, com os bytes capturados em 25/09/2026 e o SHA no golden. Nenhuma
chamada extra vai à rede. Código que o USDA publicar depois e que não esteja no catálogo local levanta `ParseError`,
nunca rótulo vazio.

`python -m scripts.reconciliar_usda --output resultado.json` confere ao vivo os catálogos e 34 recortes (9 produtos)
contra a saída do agrobr e a identidade do balanço. Precisa da `AGROBR_USDA_API_KEY`; sem ela, a reconciliação semanal
marca o USDA como "não verificado".

## MetaInfo

```python
df, meta = await usda.psd("soja", return_meta=True)
print(meta.source)  # "usda"
print(meta.source_url)  # URL exata consultada no gateway, sem a chave
print(meta.raw_content_hash, meta.raw_content_size)  # SHA-256 e tamanho do corpo recebido
```

## Fonte

- API: `https://api.fas.usda.gov/api/psd` (gateway da FAS; a antiga `apps.fas.usda.gov/OpenData/api` responde 500)
- Formato: JSON (REST), em camelCase e só com IDs
- Atualização: mensal (WASDE); cada série guarda o mês da própria última atualização
- Histórico: 1960+
