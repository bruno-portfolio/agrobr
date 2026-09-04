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

# Histórico anual por estado (1980/1981 a 2020/2021)
df = await unica.producao_historica("acucar", safra_inicio="2010/2011")
```

Requer o extra `[pdf]` para o relatório quinzenal: `pip install agrobr[pdf]`.

## Parâmetros

| Função | Parâmetro | Tipo | Default | Descrição |
|--------|-----------|------|---------|-----------|
| `moagem_quinzenal` | `produto` | str | `"cana"` | `cana`, `acucar`, `etanol_total`, `etanol_anidro` ou `etanol_hidratado` |
| `moagem_quinzenal` | `regiao` | str \| None | None | `sao_paulo`, `centro_sul`, `demais_estados` ou todas |
| `safra_resumo` | `periodo` | str | `"acumulado"` | `acumulado` ou `quinzena` |
| `producao_historica` | `produto` | str | `"cana"` | `cana`, `acucar`, `etanol_anidro`, `etanol_hidratado` ou `etanol_total` |
| `producao_historica` | `safra_inicio` | str \| None | None | Safra inicial no formato `YYYY/YYYY` |
| `producao_historica` | `safra_fim` | str \| None | None | Safra final no formato `YYYY/YYYY` |
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
- Atualização: quinzenal durante a safra (posições em 1º e 16 de cada mês)
- Licença: `zona_cinza` — uso educacional/pesquisa; comercial, consultar a UNICA
