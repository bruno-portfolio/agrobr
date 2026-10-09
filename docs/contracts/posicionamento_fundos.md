# posicionamento_fundos

Posicionamento semanal de traders nos futuros agropecuários de Chicago/NY,
via relatório Commitments of Traders (COT Disaggregated) do CFTC.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CFTC | API Socrata pública (publicreporting.cftc.gov), sem autenticação |

## Uso

```python
from agrobr import datasets

df = await datasets.posicionamento_fundos("soja")
df = await datasets.posicionamento_fundos("milho", inicio="2026-01-01")
df = await datasets.posicionamento_fundos("acucar", combinado=True)  # futuros + opções
```

`inicio` e `fim` aceitam `date`, `datetime` e texto `AAAA-MM-DD` ou `DD/MM/AAAA`; formato fora disso ou `inicio` depois
de `fim` gera `InvalidParameterError` antes da consulta.

## Contrato `posicionamento_fundos` v2.0

PK: `[data, codigo_cftc]` — effective from 2.0.0

| Coluna | Tipo | Nullable | Unidade |
|--------|------|----------|---------|
| `data` | DATE | N | — |
| `produto` | STRING | N | — |
| `contrato` | STRING | N | — |
| `codigo_cftc` | STRING | N | — |
| `posicoes_abertas` | INTEGER | N | contratos |
| `fundos_compra` | INTEGER | N | contratos |
| `fundos_venda` | INTEGER | N | contratos |
| `fundos_spread` | INTEGER | N | contratos |
| `fundos_saldo` | INTEGER | N | contratos |
| `produtores_compra` | INTEGER | N | contratos |
| `produtores_venda` | INTEGER | N | contratos |
| `swap_compra` | INTEGER | N | contratos |
| `swap_venda` | INTEGER | N | contratos |
| `swap_spread` | INTEGER | N | contratos |
| `outros_compra` | INTEGER | N | contratos |
| `outros_venda` | INTEGER | N | contratos |
| `outros_spread` | INTEGER | N | contratos |
| `nao_reportaveis_compra` | INTEGER | N | contratos |
| `nao_reportaveis_venda` | INTEGER | N | contratos |
| `variacao_fundos_compra` | INTEGER | S | contratos |
| `variacao_fundos_venda` | INTEGER | S | contratos |
| `variacao_posicoes` | INTEGER | S | contratos |

As colunas `variacao_*` são nulas na primeira semana de cada contrato na série
(não há semana anterior para o delta).

Na 2.0, as colunas passaram ao português. A fonte `cftc.cot` segue com os nomes do relatório (`open_interest`,
`managed_money_long`…); o mapa entre os dois é `agrobr.contracts.datasets.POSICIONAMENTO_FUNDOS_COLUNAS_V2`, e a
tabela de/para está no guia de migração.

## Semântica

Recortes sem relatório retornam um quadro vazio com as mesmas colunas e dtypes do quadro preenchido.
No pandas, as 18 contagens e variações usam `Int64`, a data usa `datetime64[ns]` e os textos usam o
dtype nativo. Ao atingir o teto de 50.000 registros da fonte, a consulta emite `UserWarning` e
preserva o aviso em `meta.validation_warnings`, com `completeness="unknown"` e `row_limit` em
`meta.source_details`. Reduza o período para conferir a cobertura.

- `fundos_*` — managed money, os fundos (a "posição dos fundos" citada pelo mercado agro)
- `produtores_*` — producer/merchant, os hedgers comerciais (produtores, processadores, tradings)
- `swap_*` — swap dealers; `outros_*` — other reportables; `nao_reportaveis_*` — nonreportable
- `compra`/`venda` são as posições compradas e vendidas; `spread`, as posições casadas
- `fundos_saldo` = `fundos_compra` − `fundos_venda` (calculado)
- `posicoes_abertas` = compradas (`produtores_compra` + `swap_compra` + `fundos_compra` + `outros_compra` +
  `nao_reportaveis_compra`) + spreads (`swap_spread` + `fundos_spread` + `outros_spread`), e o mesmo com as vendidas.
  A identidade é exata nos futuros; no relatório combinado (`combinado=True`) a própria CFTC deixa até 1 contrato de
  resíduo. `swap_spread` e `outros_spread` entraram na versão 1.1 (antes, o OI não fechava com as categorias)
- Posições em número de contratos; `data` é a terça-feira de referência do relatório

## Determinismo

Em modo determinístico (`datasets.deterministic()`), o snapshot define o `fim`
da consulta quando não informado — `fim` explícito tem precedência.
