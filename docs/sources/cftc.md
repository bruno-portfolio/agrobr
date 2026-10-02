# CFTC COT — Posicionamento de Fundos

Commodity Futures Trading Commission — relatório semanal Commitments of Traders
(formato Disaggregated), com o posicionamento por categoria de trader nos contratos
agropecuários de CBOT, CME e ICE. Managed money é a categoria que o mercado agro
cita como "posição dos fundos".

## API

```python
from agrobr import cftc

# Fundos em soja desde maio
df = await cftc.cot("soja", inicio="2026-05-01")

# Todos os 12 contratos agro mapeados
df = await cftc.cot()

# Futures + options combinados
df = await cftc.cot("milho", combined=True)
```

Dataset semântico com contrato versionado:

```python
from agrobr import datasets

df = await datasets.posicionamento_fundos("soja")
```

No dataset, as colunas saem em português (`fundos_compra`, `fundos_saldo`, `posicoes_abertas`…), e as opções entram
com `combinado=True`; a tabela está em [posicionamento_fundos](../contracts/posicionamento_fundos.md).

## Colunas — `cot`

| Coluna | Tipo | Descrição |
|---|---|---|
| `data` | datetime | Terça-feira de referência do relatório |
| `commodity` | str | Nome canônico agrobr (`soja`, `milho`, ...) |
| `contrato` | str | Nome do contrato e bolsa (ex.: `SOYBEANS - CHICAGO BOARD OF TRADE`) |
| `codigo_cftc` | str | Código CFTC do contrato |
| `open_interest` | Int64 | Contratos em aberto |
| `managed_money_long/short/spread` | Int64 | Posições dos fundos |
| `managed_money_net` | Int64 | Long − short (calculado) |
| `producer_long/short` | Int64 | Hedgers comerciais |
| `swap_long/short/spread` | Int64 | Swap dealers |
| `other_long/short/spread` | Int64 | Other reportables |
| `nonreportable_long/short` | Int64 | Posições não reportáveis |
| `change_managed_money_long/short` | Int64 | Variação semanal (nullable) |
| `change_open_interest` | Int64 | Variação semanal do OI (nullable) |

## Contratos

`soja`, `farelo_soja`, `oleo_soja`, `milho`, `trigo` (SRW), `acucar` (nº 11),
`cafe` (C), `algodao` (nº 2), `boi` (live cattle), `suino` (lean hogs),
`laranja` (FCOJ-A), `arroz` (rough rice). Aceita nome canônico, alias EN ou código CFTC; outro valor gera
`InvalidParameterError` com a lista. `inicio` e `fim` aceitam `date`, `datetime` e texto `AAAA-MM-DD` ou
`DD/MM/AAAA`.

Um período sem relatórios retorna um quadro vazio com as mesmas 22 colunas e dtypes do resultado
preenchido: contagens e variações em `Int64`, data em `datetime64[ns]` e textos no dtype nativo do
pandas. Um envelope inválido continua gerando `ParseError`.

A consulta tem limite de 50.000 registros. Ao atingir esse limite, a API emite `UserWarning` e
registra o aviso em `meta.validation_warnings`; `meta.source_details` informa `row_limit` e
`completeness="unknown"`. Isso indica que a completude não foi comprovada. Reduza o período
solicitado. O dataset preserva o aviso e esses metadados.

## MetaInfo

```python
df, meta = await cftc.cot("soja", return_meta=True)
print(meta.source)  # "cftc"
```

## Fonte

- API: `https://publicreporting.cftc.gov/resource/72hh-3qpy.json` (Socrata, sem autenticação)
- Combined (futures+options): `kh3c-gbw2`
- Atualização: semanal — sexta-feira 15:30 ET, dado de terça
- Histórico: junho/2006 em diante
- Licença: `livre` (domínio público, governo EUA)
