# Contrato: oferta_demanda_global v1.1

Oferta e demanda global de commodities agrícolas — USDA PSD, pelo gateway `https://api.fas.usda.gov/api/psd`.

## Schema (long format)

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `commodity_code` | STRING | Não | — | código PSD de 7 dígitos |
| `commodity` | STRING | Não | — | nome do agrobr (`soja`...); fora do cadastro, nome oficial do catálogo |
| `country_code` | STRING | Não | — | código de país do PSD, não ISO (`CH` China, `E4` UE, `00` mundo) |
| `country` | STRING | Sim | — | nome oficial do catálogo de países; `World` no agregado |
| `market_year` | INTEGER | Não | — | >= 1960; ano-safra do USDA literal (`2024` = 2024/25) |
| `attribute` | STRING | Não | — | nome oficial do catálogo `commodityAttributes` |
| `attribute_br` | STRING | Sim | — | rótulo do agrobr nos atributos do balanço; nulo nos demais |
| `value` | FLOAT | Sim | coluna `unit` | — |
| `unit` | STRING | Sim | — | unidade oficial do catálogo `unitsOfMeasure` |
| `attribute_id` | INTEGER | Não | — | `attributeId` do PSD (1.1) |
| `unit_id` | INTEGER | Não | — | `unitId` do PSD (1.1) |
| `last_update_year` | INTEGER | Não | — | >= 1960; ano da última atualização da série (1.1) |
| `last_update_month` | INTEGER | Sim | — | 1 a 12; mês da última atualização; nulo quando o PSD publica `00` (1.1) |

**PK:** `(commodity_code, country_code, market_year, attribute)`

Unidade por linha: `(1000 MT)` na maioria dos atributos, `(1000 HA)` na área, `(MT/HA)` na produtividade; algodão em
`1000 480 lb. Bales` (produtividade em `(KG/HA)`) e café em `(1000 60 KG BAGS)`.

`last_update_year`/`last_update_month` é o mês em que o USDA atualizou pela última vez a série (país × ano-safra), igual
ao `dataReleaseDates` do gateway. Não é a edição do WASDE consultada: séries que o relatório do mês não revisou mantêm o
mês antigo.

`attribute_br` e a identidade do balanço (estoque inicial + produção + importação = oferta = exportação + consumo +
perdas + estoque final) estão na [página da fonte](../sources/usda.md#atributos-do-balanco). O consumo é o 125 na
maioria dos produtos, o 126 no açúcar e o 142 no algodão, que também tem as perdas (150).

Sem `market_year`, vale o padrão da fonte: o ano-calendário corrente ou, enquanto o PSD não publica nada dele (de
janeiro até o WASDE de maio), o anterior, com o ano usado em `source_details["market_year"]`
([página da fonte](../sources/usda.md#api)).

## Histórico

- **1.1 (2.0.0):** colunas `attribute_id`, `unit_id`, `last_update_year` e `last_update_month`. Rótulos pelos catálogos
  oficiais do gateway. A 1.1.0 lia a OpenData antiga, que dá 500, e rotulava errado 6 dos 9 atributos (a produção saía
  como estoque inicial), o farelo de soja (era o óleo de algodão) e a UE (era a EU-15). Ver o
  [guia de migração](../guides/migracao-2.md).
- **1.0 (0.13.0):** versão inicial.

## Pivot mode

Quando `pivot=True`, cada atributo vira uma coluna: o `attribute_br` quando existe, senão o nome oficial. Dois atributos
com o mesmo rótulo na mesma série levantam `ParseError`. A validação de contrato é pulada nesse caso.

## Exemplo

```python
from agrobr import datasets

# Soja Brasil — long format
df = await datasets.oferta_demanda_global("soja")

# Pivot (attributes como colunas)
df = await datasets.oferta_demanda_global("soja", pivot=True)

# Outro país + ano específico
df = await datasets.oferta_demanda_global("milho", country="US", market_year=2023)

# Com metadados
df, meta = await datasets.oferta_demanda_global("soja", return_meta=True)
```
