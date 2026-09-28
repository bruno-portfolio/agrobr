# CFTC — Commitments of Traders

Posicionamento semanal de traders em contratos agropecuários de Chicago e Nova York,
via relatório COT (formato Disaggregated) da Commodity Futures Trading Commission.

### `cot`

Posições semanais por categoria de trader: managed money (fundos), producer/merchant
(hedgers comerciais), swap dealers, other reportables e non-reportable.

```python
async def cot(
    commodity: str | None = None,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    combined: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `commodity` | `str \| None` | Nome canônico (`"soja"`), alias EN (`"soybeans"`) ou código CFTC (`"005602"`). `None` retorna os 12 contratos agro mapeados |
| `start` | `str \| date \| None` | Data inicial (`YYYY-MM-DD`). `None` retorna desde 2006 |
| `end` | `str \| date \| None` | Data final. `None` até o relatório mais recente |
| `combined` | `bool` | `True` inclui opções (futures+options); default só futuros |
| `as_polars` | `bool` | Se True, retorna `polars.DataFrame` |
| `return_meta` | `bool` | Se True, retorna tupla `(DataFrame, MetaInfo)` |

**Retorno:**

DataFrame com colunas: `data`, `commodity`, `contrato`, `codigo_cftc`, `open_interest`,
`managed_money_long`, `managed_money_short`, `managed_money_spread`, `managed_money_net`,
`producer_long`, `producer_short`, `swap_long`, `swap_short`, `swap_spread`, `other_long`, `other_short`,
`other_spread`, `nonreportable_long`, `nonreportable_short`, `change_managed_money_long`,
`change_managed_money_short`, `change_open_interest`.

Posições em número de contratos (int64). Colunas `change_*` são nullable (Int64) —
nulas na primeira semana de cada contrato na série.

**Exemplo:**

```python
from agrobr import cftc

# Posicionamento dos fundos em soja desde maio
df = await cftc.cot("soja", start="2026-05-01")

# Net dos fundos (long - short), já calculado
df[["data", "managed_money_net", "open_interest"]]

# Todos os 12 contratos agro, futures+options
df = await cftc.cot(combined=True)

# Por código CFTC direto
df = await cftc.cot("005602")
```

## Contratos mapeados

| Canônico | Contrato CFTC | Código |
|---|---|---|
| `soja` | SOYBEANS (CBOT) | 005602 |
| `farelo_soja` | SOYBEAN MEAL (CBOT) | 026603 |
| `oleo_soja` | SOYBEAN OIL (CBOT) | 007601 |
| `milho` | CORN (CBOT) | 002602 |
| `trigo` | WHEAT-SRW (CBOT) | 001602 |
| `acucar` | SUGAR NO. 11 (ICE) | 080732 |
| `cafe` | COFFEE C (ICE) | 083731 |
| `algodao` | COTTON NO. 2 (ICE) | 033661 |
| `boi` | LIVE CATTLE (CME) | 057642 |
| `suino` | LEAN HOGS (CME) | 054642 |
| `laranja` | FCOJ-A (ICE) | 040701 |
| `arroz` | ROUGH RICE (CBOT) | 039601 |

## Dataset semântico

```python
from agrobr import datasets

df = await datasets.posicionamento_fundos("milho")
df = await datasets.posicionamento_fundos("soja", start="2026-01-01")
```

Contrato `cftc.cot` v1.1 — primary key `data` + `codigo_cftc`, 22 colunas validadas (as 2 de spread, novas na 1.1, são opcionais).

## Versão Síncrona

```python
from agrobr.sync import cftc

df = cftc.cot("soja", start="2026-05-01")
```

## Notas

- Fonte: [CFTC Public Reporting](https://publicreporting.cftc.gov) — domínio público (governo EUA)
- Publicação: sextas-feiras 15:30 ET, com dado de terça-feira da mesma semana
- Histórico: junho/2006 em diante (formato Disaggregated)
- Sem autenticação; rate limit interno de 2s entre requests
