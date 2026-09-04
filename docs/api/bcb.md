# API BCB/SICOR

O modulo BCB fornece dados do Banco Central do Brasil: crédito rural (SICOR), séries temporais (SGS), cotação do dólar (PTAX) e expectativas de mercado (Focus).

## Funcoes

### `credito_rural`

Dados de financiamento rural por produto, safra e UF, agregados por UF ou por programa.

```python
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: Literal["uf", "programa"] = "uf",
    programa: str | None = None,
    tipo_seguro: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str` | Produto (soja, milho, arroz, feijao, trigo, algodao, cafe, cana, sorgo) |
| `safra` | `str \| None` | Safra formato "2024/25". Default: safra mais recente |
| `finalidade` | `str` | `"custeio"`, `"investimento"` ou `"comercializacao"` |
| `uf` | `str \| None` | Sigla da UF (ex: "MT", "PR"); espaços/caixa são normalizados e valores inválidos levantam `ValueError` |
| `agregacao` | `str` | `"uf"` (padrão) ou `"programa"`; o OData não possui dimensão municipal |
| `programa` | `str \| None` | Filtrar por programa (ex: "Pronamp", "Pronaf") |
| `tipo_seguro` | `str \| None` | Filtrar por tipo de seguro (ex: "Proagro", "Seguro privado") |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas:

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `safra` | str | Safra "2024/2025" |
| `produto` | str | Produto financiado |
| `uf` | str | UF |
| `finalidade` | str | Finalidade (`custeio`, `investimento` ou `comercializacao`) |
| `agregacao` | str | Nível da saída: `uf` ou `programa` |
| `programa` | str | Programa SICOR; nulo na agregação por UF |
| `cd_programa` | str | Código do programa; nulo na agregação por UF |
| `qtd_contratos` | int | Quantidade de contratos |
| `valor` | float | Valor financiado (R$) |
| `area_financiada` | float | Área financiada (ha) |
| `fonte` | str | `bcb_odata` ou `bcb_bigquery` |

**Exemplo:**

```python
from agrobr import bcb

# Credito custeio soja MT
df = await bcb.credito_rural("soja", safra="2024/25", uf="MT")

# Agregado por UF
df = await bcb.credito_rural("milho", agregacao="uf")

# Agregado por programa
df = await bcb.credito_rural("soja", safra="2024/25", agregacao="programa")

# Filtrar por programa
df = await bcb.credito_rural("soja", safra="2024/25", programa="Pronamp")

# Filtrar por tipo de seguro
df = await bcb.credito_rural("soja", safra="2024/25", tipo_seguro="Proagro")

# Com metadados
df, meta = await bcb.credito_rural("soja", return_meta=True)
print(meta.schema_version)  # "2.0"
```

`agregacao="municipio"` levanta `InvalidParameterError`. O endpoint OData consultado não contém município; dados municipais podem ser acessados diretamente pelo extra `agrobr[bigquery]`.

### `sgs`

Séries temporais do SGS (Sistema Gerenciador de Séries Temporais) do BCB. Aceita o código numérico da série ou um dos 17 aliases pré-mapeados.

```python
async def sgs(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `codigo` | `int \| str` | Código SGS (ex: `433`) ou alias pré-mapeado (ex: `"ipca"`) |
| `data_inicial` | `str \| None` | Data inicial (DD/MM/YYYY); sem janela e sem `ultimos`, usa hoje menos 10 anos |
| `data_final` | `str \| None` | Data final (DD/MM/YYYY) |
| `ultimos` | `int \| None` | Retorna apenas os N registros mais recentes |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Aliases pré-mapeados:** `selic`, `ipca`, `ipca_alimentacao`, `ipa_agropecuario`, `pib_agropecuaria`, `credito_rural_concessoes_pf`, `credito_rural_saldo_pf`, `dolar_ptax_venda`, `dolar_ptax_compra`, `cambio_mensal_compra`, `cambio_mensal_venda`, `igpm`, `igpdi`, `inpc`, `cdi`, `tjlp`, `tr`

**Retorno:**

DataFrame com colunas: `data`, `valor`, `codigo`, `nome_serie`

**Exemplo:**

```python
from agrobr import bcb

# Por alias
df = await bcb.sgs("ipca", data_inicial="01/01/2024")

# Por código + últimos N registros
df = await bcb.sgs(432, ultimos=30)  # Selic
```

---

### `ptax`

Cotação do dólar PTAX (compra e venda) do BCB.

```python
async def ptax(
    *,
    data: str | None = None,
    data_inicial: str | None = None,
    data_final: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `data` | `str \| None` | Dia único, DD/MM/YYYY (cotação de uma data específica) |
| `data_inicial` | `str \| None` | Data inicial de um período (DD/MM/YYYY) |
| `data_final` | `str \| None` | Data final de um período (DD/MM/YYYY) |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas principais (normalizadas; demais campos retornados pela API, como paridade e tipo de boletim, são preservados): `data`, `data_hora`, `cotacao_compra`, `cotacao_venda`

**Exemplo:**

```python
from agrobr import bcb

# Período
df = await bcb.ptax(data_inicial="01/01/2024", data_final="31/01/2024")
```

---

### `focus`

Expectativas de mercado do Boletim Focus (BCB) por indicador.

```python
async def focus(
    indicador: str = "PIB Agropecuária",
    *,
    top: int = 1000,
    data_inicial: str | None = None,
    max_registros: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `indicador` | `str` | Indicador (ex: `"PIB Agropecuária"`, `"IPCA"`). Default: `"PIB Agropecuária"` |
| `top` | `int` | Máximo de registros por página (default 1000) |
| `data_inicial` | `str \| None` | Filtro server-side (`Data ge 'YYYY-MM-DD'`) |
| `max_registros` | `int \| None` | Interrompe a paginação nos N mais recentes |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `indicador`, `data`, `data_referencia`, `media`, `mediana`, `desvio_padrao`, `minimo`, `maximo`, `numero_respondentes`, `base_calculo`

**Exemplo:**

```python
from agrobr import bcb

# Expectativas do PIB Agropecuária a partir de junho/2026
df = await bcb.focus("PIB Agropecuária", data_inicial="2026-06-01")
```

---

## Versao Sincrona

```python
from agrobr.sync import bcb

df = bcb.credito_rural("soja", safra="2024/25")
serie = bcb.sgs("ipca", data_inicial="01/01/2024")
cambio = bcb.ptax(data_inicial="01/01/2024", data_final="31/01/2024")
expectativas = bcb.focus("PIB Agropecuária")
```

## Fallback

Quando a API OData do BCB falha, o agrobr usa automaticamente BigQuery (Base dos Dados) como fallback. Requer `pip install agrobr[bigquery]` e um projeto GCP para billing: defina `AGROBR_BQ_BILLING_PROJECT=<project-id>` ou configure `billing_project_id` no basedosdados (`~/.basedosdados/config.toml`).

## Notas

- Fonte: [BCB/SICOR](https://olinda.bcb.gov.br) — licenca livre
- Dados disponiveis a partir de 2013
- Contrato v2.0 — saída alinhada às agregações reais do SICOR
