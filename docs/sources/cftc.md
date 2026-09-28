# CFTC COT — Posicionamento de Fundos

Commodity Futures Trading Commission — relatório semanal Commitments of Traders
(formato Disaggregated), com o posicionamento por categoria de trader nos contratos
agropecuários de CBOT, CME e ICE. Managed money é a categoria que o mercado agro
cita como "posição dos fundos".

## API

```python
from agrobr import cftc

# Fundos em soja desde maio
df = await cftc.cot("soja", start="2026-05-01")

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

## Colunas — `cot`

| Coluna | Tipo | Descrição |
|---|---|---|
| `data` | datetime | Terça-feira de referência do relatório |
| `commodity` | str | Nome canônico agrobr (`soja`, `milho`, ...) |
| `contrato` | str | Nome do contrato e bolsa (ex.: `SOYBEANS - CHICAGO BOARD OF TRADE`) |
| `codigo_cftc` | str | Código CFTC do contrato |
| `open_interest` | int64 | Contratos em aberto |
| `managed_money_long/short/spread` | int64 | Posições dos fundos |
| `managed_money_net` | int64 | Long − short (calculado) |
| `producer_long/short` | int64 | Hedgers comerciais |
| `swap_long/short/spread` | int64 | Swap dealers |
| `other_long/short/spread` | int64 | Other reportables |
| `nonreportable_long/short` | int64 | Posições não reportáveis |
| `change_managed_money_long/short` | Int64 | Variação semanal (nullable) |
| `change_open_interest` | Int64 | Variação semanal do OI (nullable) |

## Contratos

`soja`, `farelo_soja`, `oleo_soja`, `milho`, `trigo` (SRW), `acucar` (nº 11),
`cafe` (C), `algodao` (nº 2), `boi` (live cattle), `suino` (lean hogs),
`laranja` (FCOJ-A), `arroz` (rough rice). Aceita nome canônico, alias EN ou código CFTC.

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
