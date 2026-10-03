# UNICA — Safra de Cana Centro-Sul

União da Indústria de Cana-de-Açúcar e Bioenergia. Acompanhamento quinzenal da
safra do Centro-Sul (moagem, açúcar, etanol, mix de produção, ATR) via relatório
PDF público, e histórico anual de produção por estado via export do site clássico.

> Classificação `zona_cinza`: sem termos de uso públicos localizados. O módulo
> emite `UserWarning` na primeira chamada. Veja [licenças](../licenses.md#unica).

## API

```python
from agrobr import unica

# Moagem acumulada da safra corrente (relatório quinzenal mais recente)
df = await unica.moagem_quinzenal("cana", regiao="centro_sul")

# Posição da safra: produção, ATR, mix açúcar/etanol
df = await unica.safra_resumo(periodo="acumulado")
df = await unica.safra_resumo(periodo="mensal")  # ou "quinzena", conforme a edição

# Histórico anual por estado (1980/1981 a 2020/2021)
df = await unica.producao_historica("acucar", safra_inicio="2010/2011")
```

Requer o extra `[pdf]` para o relatório quinzenal: `pip install agrobr[pdf]`.

## Parâmetros

| Função | Parâmetro | Tipo | Default | Descrição |
|--------|-----------|------|---------|-----------|
| `moagem_quinzenal` | `produto` | str | `"cana"` | `cana`, `acucar`, `etanol_total`, `etanol_anidro` ou `etanol_hidratado` |
| `moagem_quinzenal` | `regiao` | str \| None | None | `sao_paulo`, `centro_sul`, `demais_estados` ou todas |
| `safra_resumo` | `periodo` | str | `"acumulado"` | `acumulado`, `quinzena` ou `mensal`; período que a edição corrente não publica → `InvalidParameterError` com os períodos da edição |
| `producao_historica` | `produto` | str | `"cana"` | `cana`, `acucar`, `etanol_anidro`, `etanol_hidratado` ou `etanol_total` |
| `producao_historica` | `safra_inicio` | str \| None | None | Safra inicial (`YYYY/YYYY`, `YYYY/YY` ou `YY/YY`), de 1980/1981 a 2020/2021 |
| `producao_historica` | `safra_fim` | str \| None | None | Safra final, nos mesmos formatos e intervalo, não anterior à inicial |
| Todas | `as_polars` | bool | False | Se True, retorna `polars.DataFrame` |
| Todas | `return_meta` | bool | False | Se True, retorna `(DataFrame, MetaInfo)` |

## Colunas — `moagem_quinzenal`

| Coluna | Tipo | Descrição |
|---|---|---|
| `data` | datetime | Data da posição (quinzena) |
| `quinzena` | str | Rótulo da quinzena (ex.: `01/05`) |
| `safra` | str | Safra do relatório (ex.: `2026/2027`) |
| `produto` | str | `cana`, `acucar`, `etanol_total`, `etanol_anidro`, `etanol_hidratado` |
| `regiao` | str | `sao_paulo`, `centro_sul`, `demais_estados` |
| `valor` | float | Acumulado da safra corrente até a quinzena |
| `valor_safra_anterior` | float | Acumulado equivalente da safra anterior |
| `variacao_pct` | float | Variação percentual |
| `unidade` | str | `t` (cana/açúcar) ou `m3` (etanol) |

## Colunas — `safra_resumo`

| Coluna | Tipo | Descrição |
|---|---|---|
| `produto` | str | `cana`, `acucar`, `etanol_anidro`, `etanol_hidratado`, `etanol_total`, `atr`, `atr_por_tonelada`, `mix_acucar`, `mix_etanol`, `litros_etanol_por_tonelada`, `kg_acucar_por_tonelada` |
| `regiao` | str | `centro_sul`, `sao_paulo`, `demais_estados` |
| `safra` | str | Safra do relatório (ex.: `2026/2027`) |
| `periodo` | str | `acumulado`, `quinzena` ou `mensal`, lido do título da tabela |
| `data_inicio` | datetime | Início do período, lido do título (ex.: `2026-06-01` para "junho de 2026") |
| `data_fim` | datetime | Fim do período, lido do título (ex.: `2026-07-01` no acumulado "até 01 de julho de 2026") |
| `valor` | float | Valor da safra corrente no período |
| `valor_safra_anterior` | float | Valor equivalente da safra anterior |
| `variacao_pct` | float | Variação percentual; nula no mix, que a fonte não compara |
| `unidade` | str | `mil_t`, `mi_litros`, `kg_t` (ATR ou açúcar por tonelada), `l_t` ou `pct` (mix) |

No acumulado, `data_fim` é a data do título, que a UNICA usa como limite exclusivo: o acumulado "até 01 de agosto de 2026" vai até 31/07 e é igual ao acumulado "até 01 de julho" mais o mensal de julho, que sai com `data_fim=2026-07-31`. O dado segue como publicado.

## Colunas — `producao_historica`

| Coluna | Tipo | Descrição |
|---|---|---|
| `safra` | str | Ex.: `2019/2020` |
| `localidade` | str | UF ou agregados `centro_sul`, `norte_nordeste`, `brasil` |
| `produto` | str | Produto solicitado |
| `valor` | float | Produção da safra |
| `unidade` | str | `mil_t` ou `mil_m3` |

## Limitações

- **Quinzenal**: o PDF cobre a safra corrente + comparativo com a anterior; a fonte
  não disponibiliza histórico longo quinzenal.
- **Edição quinzenal ou mensal**: a Tabela 2 do resumo traz a quinzena (ex.: "2ª quinzena de abril de 2026", edição de
  01/05/2026) ou o mês (ex.: "junho de 2026", edição de 01/07/2026). O agrobr lê o período e as datas do título de cada
  tabela, e um título que não reconhece vira `ParseError`. A listagem guarda só a edição corrente: em 23/09/2026, a
  posição até 01/07/2026, publicada em 06/08/2026.
- **Revisões**: a UNICA revisa as quinzenas passadas a cada edição. Exemplo: cana do Centro-Sul acumulada até 01/05,
  60.457.836 t na edição de 01/05 e 60.412.599 t na de 01/07. O agrobr serve sempre a edição corrente.
- **Valor ausente**: marca de ausência (`n/d`, `-`) no lugar de um número do resumo ou das séries quinzenais vira valor
  nulo, com a linha mantida.
  Produto obrigatório (cana, açúcar, etanol total, mix) que falte num período vira `ParseError`.
- **Fora do escopo**: as Tabelas 8 (etanol de milho) e 9 (vendas mensais de etanol) do relatório.
- **Histórico**: o banco do site clássico está **congelado em 2020/2021** — safras
  posteriores retornam vazio na fonte. Para a safra corrente use as funções quinzenais.

## MetaInfo

```python
df, meta = await unica.moagem_quinzenal("cana", return_meta=True)
print(meta.source)  # "unica"
```

## Fonte

- Relatório quinzenal: `https://unicadata.com.br/listagem.php?idMn=63` (PDF, URL rotativa)
- Histórico: `https://unicadata.com.br/xlsHPM.php` (XLSX)
- Atualização: por edição do relatório de safra, quinzenal ou mensal; a listagem traz só a edição corrente
- Licença: `zona_cinza` — licença de reutilização das séries e relatórios não localizada; atribuição não substitui eventual permissão.
