# CFTC — Commitments of Traders

Posicionamento semanal de traders em contratos agropecuários de Chicago e Nova York,
via relatório COT (formato Disaggregated) da Commodity Futures Trading Commission.

### `cot`

Posições semanais por categoria de trader: managed money (fundos), producer/merchant
(hedgers comerciais), swap dealers, other reportables e non-reportable.

```python
async def cot(
    produto: str | None = None,
    *,
    inicio: str | date | datetime | None = None,
    fim: str | date | datetime | None = None,
    combined: bool = False,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str \| None` | Nome canônico (`"soja"`), alias EN (`"soybeans"`) ou código CFTC (`"005602"`). `None` retorna os 12 contratos agro mapeados |
| `inicio` | `str \| date \| datetime \| None` | Data inicial (`AAAA-MM-DD` ou `DD/MM/AAAA`). `None` retorna desde 2006 |
| `fim` | `str \| date \| datetime \| None` | Data final. `None` até o relatório mais recente |
| `combined` | `bool` | `True` inclui opções (futures+options); default só futuros |
| `as_polars` | `bool` | Se True, retorna `polars.DataFrame` |
| `return_meta` | `bool` | Se True, retorna tupla `(DataFrame, MetaInfo)` |

**Retorno:**

DataFrame com colunas: `data`, `commodity`, `contrato`, `codigo_cftc`, `open_interest`,
`managed_money_long`, `managed_money_short`, `managed_money_spread`, `managed_money_net`,
`producer_long`, `producer_short`, `swap_long`, `swap_short`, `swap_spread`, `other_long`, `other_short`,
`other_spread`, `nonreportable_long`, `nonreportable_short`, `change_managed_money_long`,
`change_managed_money_short`, `change_open_interest`.

Posições em número de contratos (`Int64`, como as variações). Colunas `change_*` são
nulas na primeira semana de cada contrato na série, mesmo quando o Socrata omite esses campos
e a consulta inclui só essa semana. Campos obrigatórios ausentes continuam gerando `ParseError`.
As colunas seguem os nomes do relatório do CFTC; o dataset
`posicionamento_fundos` as entrega em português.

`produto` sem contrato mapeado, data fora dos formatos aceitos e `inicio` depois de `fim` geram
`InvalidParameterError` antes da consulta, com os valores válidos na mensagem.

**Exemplo:**

```python
from agrobr import cftc

# Posicionamento dos fundos em soja desde maio
df = await cftc.cot("soja", inicio="2026-05-01")

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

A tabela está em `cftc.CFTC_CONTRACTS` (código → nome canônico). `cftc.resolve_contract_codes` devolve os códigos de um
produto, com a mesma regra do `produto` do `cot`: nome canônico, alias EN ou código; `None` devolve os 12.

```python
from agrobr import cftc

cftc.CFTC_CONTRACTS["005602"]            # 'soja'
cftc.resolve_contract_codes("soybeans")  # ['005602']
cftc.resolve_contract_codes(None)        # os 12 códigos
```

Produto sem contrato mapeado levanta `InvalidParameterError`, com os valores válidos na mensagem.

## Dataset semântico

```python
from agrobr import datasets

df = await datasets.posicionamento_fundos("milho")
df = await datasets.posicionamento_fundos("soja", inicio="2026-01-01", combinado=True)
```

Contrato `posicionamento_fundos` 2.0 (`contracts.get_contract("posicionamento_fundos")`): chave primária `data` + `codigo_cftc`, 22 colunas validadas (`swap_spread` e `outros_spread`, novas na 2.0, são opcionais).
No dataset, as colunas estão em português (`fundos_compra`, `fundos_saldo`, `posicoes_abertas`…), e `combined` se chama
`combinado`; o mapa está em `agrobr.contracts.datasets.POSICIONAMENTO_FUNDOS_COLUNAS_V2`.

## Versão Síncrona

```python
from agrobr.sync import cftc

df = cftc.cot("soja", inicio="2026-05-01")
```

## Notas

- Fonte: [CFTC Public Reporting](https://publicreporting.cftc.gov) — domínio público (governo EUA)
- Publicação: sextas-feiras 15:30 ET, com dado de terça-feira da mesma semana
- Histórico: junho/2006 em diante (formato Disaggregated)
- Sem autenticação; rate limit interno de 2s entre requests
