# posicionamento_fundos

Posicionamento semanal de traders nos futuros agropecuários de Chicago/NY,
via relatório Commitments of Traders (COT Disaggregated) do CFTC.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CFTC | API Socrata pública (publicreporting.cftc.gov), sem autenticação |

## Uso

```python
df = await datasets.posicionamento_fundos("soja")
df = await datasets.posicionamento_fundos("milho", start="2026-01-01")
df = await datasets.posicionamento_fundos("acucar", combined=True)  # futures + options
```

## Contrato `cftc.cot` v1.1

PK: `[data, codigo_cftc]` — effective from 1.1.0

| Coluna | Tipo | Nullable | Unidade |
|--------|------|----------|---------|
| `data` | DATE | N | — |
| `commodity` | STRING | N | — |
| `contrato` | STRING | N | — |
| `codigo_cftc` | STRING | N | — |
| `open_interest` | INTEGER | N | contratos |
| `managed_money_long` | INTEGER | N | contratos |
| `managed_money_short` | INTEGER | N | contratos |
| `managed_money_spread` | INTEGER | N | contratos |
| `managed_money_net` | INTEGER | N | contratos |
| `producer_long` | INTEGER | N | contratos |
| `producer_short` | INTEGER | N | contratos |
| `swap_long` | INTEGER | N | contratos |
| `swap_short` | INTEGER | N | contratos |
| `swap_spread` | INTEGER | N | contratos |
| `other_long` | INTEGER | N | contratos |
| `other_short` | INTEGER | N | contratos |
| `other_spread` | INTEGER | N | contratos |
| `nonreportable_long` | INTEGER | N | contratos |
| `nonreportable_short` | INTEGER | N | contratos |
| `change_managed_money_long` | INTEGER | S | contratos |
| `change_managed_money_short` | INTEGER | S | contratos |
| `change_open_interest` | INTEGER | S | contratos |

As colunas `change_*` são nulas na primeira semana de cada contrato na série
(não há semana anterior para o delta).

## Semântica

- `managed_money_*` — fundos (a "posição dos fundos" citada pelo mercado agro)
- `producer_*` — hedgers comerciais (produtores, processadores, tradings)
- `swap_*` — swap dealers
- `managed_money_net` = `managed_money_long` − `managed_money_short` (calculado)
- `open_interest` = compradas (`producer_long` + `swap_long` + `managed_money_long` + `other_long` +
  `nonreportable_long`) + spreads (`swap_spread` + `managed_money_spread` + `other_spread`), e o mesmo com as vendidas.
  A identidade é exata nos futuros; no relatório combinado (`combined=True`) a própria CFTC deixa até 1 contrato de
  resíduo. `swap_spread` e `other_spread` entraram na versão 1.1 (antes, o OI não fechava com as categorias)
- Posições em número de contratos; `data` é a terça-feira de referência do relatório

## Determinismo

Em modo determinístico (`datasets.deterministic()`), o snapshot define o `end`
da consulta quando não informado — `end` explícito tem precedência.
