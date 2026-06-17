# API BCB/SICOR

O modulo BCB fornece dados do Banco Central do Brasil: crédito rural (SICOR), séries temporais (SGS), cotação do dólar (PTAX) e expectativas de mercado (Focus).

## Funcoes

### `credito_rural`

Dados de financiamento rural por produto, safra, UF e municipio, com dimensoes de programa, fonte de recurso, tipo de seguro, modalidade e atividade.

```python
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: str = "municipio",
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
| `uf` | `str \| None` | Filtrar por UF (ex: "MT", "PR") |
| `agregacao` | `str` | `"municipio"` (default), `"uf"` ou `"programa"` |
| `programa` | `str \| None` | Filtrar por programa (ex: "Pronamp", "Pronaf") |
| `tipo_seguro` | `str \| None` | Filtrar por tipo de seguro (ex: "Proagro", "Seguro privado") |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas:

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `safra` | str | Safra "2024/2025" |
| `ano_emissao` | int | Ano de emissao |
| `mes_emissao` | int | Mes de emissao |
| `uf` | str | UF do municipio |
| `municipio` | str | Nome do municipio |
| `produto` | str | Produto financiado |
| `finalidade` | str | Finalidade (custeio, investimento, comercializacao) |
| `valor` | float | Valor financiado (R$) |
| `area_financiada` | float | Area financiada (ha) |
| `qtd_contratos` | int | Quantidade de contratos |
| `cd_programa` | str | Codigo do programa SICOR |
| `programa` | str | Nome do programa (ex: "Pronamp", "Pronaf") |
| `cd_sub_programa` | str | Codigo do sub-programa |
| `cd_fonte_recurso` | str | Codigo da fonte de recurso |
| `fonte_recurso` | str | Nome da fonte (ex: "LCA", "FNE", "Poupanca rural controlados") |
| `cd_tipo_seguro` | str | Codigo do tipo de seguro |
| `tipo_seguro` | str | Nome do seguro (ex: "Proagro", "Seguro privado") |
| `cd_modalidade` | str | Codigo da modalidade |
| `modalidade` | str | Nome da modalidade (ex: "Individual", "Coletiva") |
| `cd_atividade` | str | Codigo da atividade |
| `atividade` | str | Nome da atividade (ex: "Agricola", "Pecuaria") |
| `regiao` | str | Regiao (ex: "SUL", "CENTRO-OESTE") |

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
print(meta.schema_version)  # "1.1"
```

## Dimensoes SICOR

As dimensoes sao enriquecidas automaticamente pelo parser com dicionarios hardcoded. Codigos desconhecidos geram `"Desconhecido ({code})"` com log warning.

| Dimensao | Codigos conhecidos |
|----------|-------------------|
| Programa | Pronaf, Pronamp, Funcafe, Moderfrota, ABC, Inovagro, etc. |
| Fonte de recurso | Recursos obrigatorios, Poupanca rural, LCA, FNO/FNE/FCO, Funcafe, etc. |
| Tipo de seguro | Proagro, Sem seguro, Seguro privado, Nao se aplica |
| Modalidade | Individual, Coletiva |
| Atividade | Agricola, Pecuaria |

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
| `data_inicial` | `str \| None` | Data inicial (DD/MM/YYYY) |
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
- Contract v1.1 — 11 novas colunas nullable desde v0.10.1
