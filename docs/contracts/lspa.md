# lspa v2.0

Levantamento Sistemático da Produção Agrícola — estimativas mensais do IBGE.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | IBGE LSPA | Levantamento Sistemático da Produção Agrícola |

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `ano` | int | ❌ | Ano de referência (>= 1974) |
| `mes` | int | ❌ | Mês de referência (1-12), inclusive em consultas anuais |
| `localidade` | str | ❌ | Nome da localidade |
| `localidade_cod` | int | ❌ | Código IBGE da localidade |
| `produto` | str | ❌ | Nome do produto |
| `variavel` | str | ❌ | Variável medida: área plantada, área colhida, produção ou rendimento |
| `variavel_cod` | int | ❌ | Código SIDRA da variável |
| `valor` | float64 | ✅ | Valor na unidade indicada na linha |
| `unidade` | str | ❌ | Unidade publicada pelo SIDRA, por exemplo Hectares ou Toneladas |
| `fonte` | str | ❌ | Sempre `ibge_lspa` |

## Primary Key

`[ano, mes, produto, localidade, variavel]`

## Garantias

- Nomes de colunas nunca mudam (apenas adições)
- `ano` é sempre um ano válido
- `mes` está sempre presente e é entre 1 e 12
- Cada linha identifica um mês, produto, localidade e variável; medidas de unidades distintas permanecem separadas
- `fonte` é sempre `ibge_lspa`

A versão 2.0 corrige a identidade dos eixos da tabela 6588 e amplia a chave primária.
`mes=None` retorna os meses disponíveis do ano solicitado. O dataset
`estimativa_safra` continua entregando suas métricas agregadas nas unidades do contrato próprio.

Desde 2012, `produto="cafe"` consulta arábica e canephora, preservando cada espécie na coluna
`produto`, assim como `milho` preserva as duas safras. Para o total de café, somam-se
produção e área das duas espécies no mesmo mês e território. O rendimento total
deve ser calculado como produção em toneladas × 1.000 / área colhida em hectares;
os rendimentos das espécies não são aditivos.

Antes de 2012, `produto="cafe"` retorna o total oficial com `produto="cafe"`, sem
inventar uma divisão entre espécies. Consultas explícitas às espécies nesse período
preservam os valores indisponíveis do SIDRA. A separação começou em janeiro de 2012,
conforme a [nota oficial do IBGE sobre a série histórica](https://www.ibge.gov.br/en/highlights/20651-ibge-makes-available-monthly-time-series-of-systematic-survey-of-agricultural-production-lspa.html?lang=en-GB).

`produto="batata"` consulta as três safras de batata-inglesa (`batata_1`, `batata_2`,
`batata_3`) e preserva cada uma na coluna `produto`. `algodao` corresponde a algodão
herbáceo em caroço; a produção não representa apenas a pluma.

## Schema JSON

Disponível em `agrobr/schemas/lspa.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("lspa")
print(contract.to_json())
```
