# futuros_agricolas

Futuros agrícolas B3 — ajustes diários, histórico e posições abertas.

## Fonte

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | B3 | Bolsa de Valores do Brasil |

## Modos (`tipo=`)

### Ajustes (default)

```python
from agrobr import datasets

df = await datasets.futuros_agricolas("boi", data="2025-03-05")
```

### Histórico

```python
df = await datasets.futuros_agricolas("boi", tipo="historico", inicio="2025-01-01", fim="2025-03-05")
```

`vencimento` aceita só o código do mês do contrato (ex.: `V26`): os ajustes trazem só futuros, e outro formato,
inclusive o código de uma opção, é recusado antes da rede.

### Posições abertas

```python
df = await datasets.futuros_agricolas("boi", tipo="posicoes", data="2025-03-05")
```

### Histórico de posições abertas

```python
df, meta = await datasets.futuros_agricolas(
    "boi", tipo="oi_historico", inicio="2026-09-03", fim="2026-09-04", return_meta=True
)
```

Informe produto, início e fim inclusivos em `AAAA-MM-DD` ou `DD/MM/AAAA` (ou `date`); `data` não se aplica
a esse modo. `vencimento` aceita o código do mês do contrato (ex.: `V26`), que
traz o futuro e as opções daquele mês, ou o código publicado de uma opção
(ex.: `VVJK`); outro formato é recusado antes da rede. As consultas percorrem
dias úteis sequencialmente e retornam tanto
futuros quanto opções, identificados pela coluna `tipo`.

`data` com `tipo="historico"` ou `"oi_historico"`, e `inicio`, `fim` ou `vencimento` com `"ajustes"` ou `"posicoes"`,
levantam `InvalidParameterError` antes da rede: o parâmetro que não se aplica ao tipo era descartado em silêncio. Esses
dois tipos trazem todos os vencimentos do pregão; para um só, filtre a coluna `vencimento_codigo`. Em todos os
tipos, `data`, `inicio` e `fim` fora dos formatos aceitos, e `inicio` depois de `fim`, também levantam
`InvalidParameterError` antes da rede.

A fonte mantém uma janela recente, sem garantir a recuperação de datas antigas.
Use datas recentes ao executar o exemplo. Dias úteis sem posições para o filtro
são relacionados em `meta.validation_warnings`; isso pode refletir arquivo não
publicado, ausência do instrumento ou vencimento, e não comprova a existência
de pregão. Um intervalo só de fim de semana retorna vazio. Datas invertidas
são rejeitadas antes da rede.

Falhas de rede, HTTP 400 no download e erros de parsing interrompem a consulta,
mesmo quando outros dias já foram obtidos. O contexto de snapshot não recupera
arquivos expirados nem altera o intervalo explicitamente informado.

## Produtos

`boi`, `milho`, `cafe_arabica`, `cafe_conillon`, `etanol`, `soja_cross`, `soja_fob`

> `soja_fob` não possui dados de posições abertas (SOY ausente de `TICKERS_AGRO_OI`).

## Contratos

### `tipo="ajustes"` / `tipo="historico"` → `AJUSTE_DIARIO_V1`

PK: `[data, ticker, vencimento_codigo]`

| Coluna | Tipo | Nullable |
|--------|------|----------|
| `data` | DATE | N |
| `ticker` | STRING | N |
| `descricao` | STRING | Y |
| `vencimento_codigo` | STRING | N |
| `vencimento_mes` | INTEGER | N |
| `vencimento_ano` | INTEGER | N |
| `ajuste_anterior` | FLOAT | Y |
| `ajuste_atual` | FLOAT | Y |
| `variacao` | FLOAT | Y |
| `ajuste_por_contrato` | FLOAT | Y |
| `unidade` | STRING | Y |

No boi gordo (BGI), o `ajuste_atual` do último dia de negociação não é a liquidação do contrato, que usa a média de 5
publicações do índice de liquidação: o Indicador do Boi DATAGRO a partir do vencimento de fevereiro de 2025 (BGIG25) e o
Indicador do Boi Gordo CEPEA/B3 até o de janeiro de 2025 (BGIF25), pelo Ofício Circular B3 135/2024-PRE. O `preco_diario`
do agrobr publica o indicador CEPEA, não o DATAGRO. No milho (CCM), o ajuste do vencimento é a liquidação.

`ajuste_por_contrato` é o valor do ajuste por contrato em reais em todos os contratos, inclusive nos cotados em dólar (ICF,
SJC e SOY): é o valor que a B3 publica, já convertido. Em 15/09/2026, o ICF K27 teve variação de −7,75 USD/sc (100 sacas,
US$ −775) e `ajuste_por_contrato` de −3.991,79. O prefixo de `unidade` (`BRL` ou `USD`) é a moeda de `ajuste_anterior`,
`ajuste_atual` e `variacao`, não a desta coluna. O valor é por contrato, e não por unidade de cotação.

### `tipo="posicoes"` / `tipo="oi_historico"` → `POSICOES_ABERTAS_V1`

PK: `[data, ticker_completo]`

| Coluna | Tipo | Nullable |
|--------|------|----------|
| `data` | DATE | N |
| `ticker` | STRING | N |
| `descricao` | STRING | Y |
| `ticker_completo` | STRING | N |
| `vencimento_codigo` | STRING | N |
| `vencimento_mes` | INTEGER | N |
| `vencimento_ano` | INTEGER | N |
| `tipo` | STRING | N |
| `posicoes_abertas` | INTEGER | N |
| `variacao_posicoes` | INTEGER | Y |
| `unidade` | STRING | Y |

`unidade` é a unidade de cotação do contrato (ex.: `BRL/@`). `posicoes_abertas` e `variacao_posicoes` contam contratos e não estão nessa unidade.

## Licença

Classificação: `zona_cinza`. A dispensa D-1 da FAQ e os termos do website têm alcances distintos; confira canal, uso e vigência da política em [Licenças](../licenses.md#b3-brasil-bolsa-balcao).
