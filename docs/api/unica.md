# UNICA — Moagem e Produção Centro-Sul

Acompanhamento quinzenal da safra de cana do Centro-Sul (moagem, açúcar, etanol, mix, ATR)
e histórico anual de produção por estado, da União da Indústria de Cana-de-Açúcar e Bioenergia.

> Dados classificados como `zona_cinza` (sem termos de uso públicos) — o módulo emite
> `UserWarning` na primeira chamada. Veja [licenças](../licenses.md).

### `moagem_quinzenal`

Série quinzenal acumulada da safra corrente, extraída do relatório PDF mais recente.

```python
async def moagem_quinzenal(
    produto: str = "cana",
    *,
    regiao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | `cana`, `acucar`, `etanol_total`, `etanol_anidro`, `etanol_hidratado` |
| `regiao` | `str \| None` | `sao_paulo`, `centro_sul` ou `demais_estados`. None retorna todas |
| `as_polars` | `bool` | Se True, retorna `polars.DataFrame` |
| `return_meta` | `bool` | Se True, retorna tupla `(DataFrame, MetaInfo)` |

**Retorno:**

DataFrame com colunas: `data`, `quinzena`, `safra`, `produto`, `regiao`, `valor`,
`valor_safra_anterior`, `variacao_pct`, `unidade` (t para cana/açúcar, m³ para etanol).

Valores **acumulados** da safra até cada quinzena. Cobre apenas a safra do relatório
vigente + comparativo com a anterior — a fonte não publica histórico longo quinzenal.

### `safra_resumo`

Resumo da posição da safra (Tabelas 1-2 do relatório): moagem, produção, ATR,
mix açúcar/etanol e rendimentos por região.

```python
async def safra_resumo(
    *,
    periodo: Literal["acumulado", "quinzena", "mensal"] = "acumulado",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Retorno:**

DataFrame com colunas: `produto`, `regiao`, `safra`, `periodo`, `data_inicio`, `data_fim`,
`valor`, `valor_safra_anterior`, `variacao_pct`, `unidade`. Produtos incluem `cana`, `acucar`,
`etanol_*`, `atr`, `atr_por_tonelada`, `kg_acucar_por_tonelada`, `mix_acucar`, `mix_etanol`.

O período e as datas vêm do título das Tabelas 1-2 da edição corrente, e o período publicado muda com a edição: a de
01/07/2026 traz o acumulado e o mês (junho), e a de 01/05/2026, o acumulado e a 2ª quinzena de abril. Pedir um período
que a edição não publica levanta `InvalidParameterError` com os períodos da edição.

### `producao_historica`

Produção anual por estado, safras 1980/1981 a 2020/2021 (export XLSX do site clássico).

```python
async def producao_historica(
    produto: str = "cana",
    *,
    safra_inicio: str | None = None,
    safra_fim: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | `cana`, `acucar`, `etanol_anidro`, `etanol_hidratado`, `etanol_total` |
| `safra_inicio` | `str \| None` | `"2015/2016"`, `"2015/16"` ou `"15/16"`; None é 1980/1981 |
| `safra_fim` | `str \| None` | Idem; None é 2020/2021 |

Safra fora do formato, com anos não consecutivos, fora de 1980/1981–2020/2021 ou com
`safra_inicio` depois de `safra_fim` levanta `InvalidParameterError` antes da rede.

**Retorno:**

DataFrame com colunas: `safra`, `localidade` (UF ou agregados `centro_sul`,
`norte_nordeste`, `brasil`), `produto`, `valor`, `unidade` (`mil_t` ou `mil_m3`).

> O banco da fonte está **congelado**: safras a partir de 2021/2022 não foram
> publicadas nesse formato. Para a safra corrente use `moagem_quinzenal`/`safra_resumo`.

**Exemplo:**

```python
from agrobr import unica

# Moagem acumulada da safra corrente, Centro-Sul
df = await unica.moagem_quinzenal("cana", regiao="centro_sul")

# Mix açúcar/etanol e ATR da quinzena (só em edição quinzenal; na mensal, periodo="mensal")
df = await unica.safra_resumo(periodo="quinzena")

# Histórico de açúcar por estado
df = await unica.producao_historica("acucar", safra_inicio="2010/2011")
```

## Versão Síncrona

```python
from agrobr.sync import unica

df = unica.moagem_quinzenal("cana")
df = unica.safra_resumo()
df = unica.producao_historica("etanol_total")
```

## Notas

- Fonte: [unicadata.com.br](https://unicadata.com.br) — relatório quinzenal (PDF) e histórico (XLSX)
- O PDF quinzenal requer o extra `[pdf]`: `pip install agrobr[pdf]`
- Publicação quinzenal durante a safra (posições em 1º e 16 de cada mês)
- Sanidade validada no parser: moagem CS < 700M t, mix 0-100%, ATR 60-160 kg/t
