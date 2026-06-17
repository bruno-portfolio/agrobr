# BCB/SICOR API

The BCB module provides Banco Central do Brasil data: rural credit (SICOR), time series (SGS), USD exchange rate (PTAX) and market expectations (Focus).

## Functions

### `credito_rural`

Rural financing data by product, crop year, state and municipality, with program, funding source, insurance type, modality and activity dimensions.

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

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product (soja, milho, arroz, feijao, trigo, algodao, cafe, cana, sorgo) |
| `safra` | `str \| None` | Crop year, "2024/25" format. Default: latest crop year |
| `finalidade` | `str` | `"custeio"`, `"investimento"` or `"comercializacao"` |
| `uf` | `str \| None` | Filter by state (e.g. "MT", "PR") |
| `agregacao` | `str` | `"municipio"` (default), `"uf"` or `"programa"` |
| `programa` | `str \| None` | Filter by program (e.g. "Pronamp", "Pronaf") |
| `tipo_seguro` | `str \| None` | Filter by insurance type (e.g. "Proagro", "Seguro privado") |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns:

| Column | Type | Description |
|--------|------|-------------|
| `safra` | str | Crop year "2024/2025" |
| `ano_emissao` | int | Issue year |
| `mes_emissao` | int | Issue month |
| `uf` | str | Municipality's state |
| `municipio` | str | Municipality name |
| `produto` | str | Financed product |
| `finalidade` | str | Purpose (custeio, investimento, comercializacao) |
| `valor` | float | Financed amount (BRL) |
| `area_financiada` | float | Financed area (ha) |
| `qtd_contratos` | int | Number of contracts |
| `cd_programa` | str | SICOR program code |
| `programa` | str | Program name (e.g. "Pronamp", "Pronaf") |
| `cd_sub_programa` | str | Sub-program code |
| `cd_fonte_recurso` | str | Funding source code |
| `fonte_recurso` | str | Funding source name (e.g. "LCA", "FNE", "Poupanca rural controlados") |
| `cd_tipo_seguro` | str | Insurance type code |
| `tipo_seguro` | str | Insurance name (e.g. "Proagro", "Seguro privado") |
| `cd_modalidade` | str | Modality code |
| `modalidade` | str | Modality name (e.g. "Individual", "Coletiva") |
| `cd_atividade` | str | Activity code |
| `atividade` | str | Activity name (e.g. "Agricola", "Pecuaria") |
| `regiao` | str | Region (e.g. "SUL", "CENTRO-OESTE") |

**Example:**

```python
from agrobr import bcb

# Working-capital credit, soybean, MT
df = await bcb.credito_rural("soja", safra="2024/25", uf="MT")

# Aggregated by state
df = await bcb.credito_rural("milho", agregacao="uf")

# Aggregated by program
df = await bcb.credito_rural("soja", safra="2024/25", agregacao="programa")

# Filter by program
df = await bcb.credito_rural("soja", safra="2024/25", programa="Pronamp")

# Filter by insurance type
df = await bcb.credito_rural("soja", safra="2024/25", tipo_seguro="Proagro")

# With metadata
df, meta = await bcb.credito_rural("soja", return_meta=True)
print(meta.schema_version)  # "1.1"
```

## SICOR Dimensions

The dimensions are automatically enriched by the parser with hardcoded dictionaries. Unknown codes produce `"Desconhecido ({code})"` with a log warning.

| Dimension | Known codes |
|-----------|-------------|
| Program | Pronaf, Pronamp, Funcafe, Moderfrota, ABC, Inovagro, etc. |
| Funding source | Recursos obrigatorios, Poupanca rural, LCA, FNO/FNE/FCO, Funcafe, etc. |
| Insurance type | Proagro, Sem seguro, Seguro privado, Nao se aplica |
| Modality | Individual, Coletiva |
| Activity | Agricola, Pecuaria |

### `sgs`

Time series from the BCB's SGS (Time Series Management System). Accepts the numeric series code or one of 17 pre-mapped aliases.

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

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `codigo` | `int \| str` | SGS code (e.g. `433`) or pre-mapped alias (e.g. `"ipca"`) |
| `data_inicial` | `str \| None` | Start date (DD/MM/YYYY) |
| `data_final` | `str \| None` | End date (DD/MM/YYYY) |
| `ultimos` | `int \| None` | Returns only the N most recent records |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Pre-mapped aliases:** `selic`, `ipca`, `ipca_alimentacao`, `ipa_agropecuario`, `pib_agropecuaria`, `credito_rural_concessoes_pf`, `credito_rural_saldo_pf`, `dolar_ptax_venda`, `dolar_ptax_compra`, `cambio_mensal_compra`, `cambio_mensal_venda`, `igpm`, `igpdi`, `inpc`, `cdi`, `tjlp`, `tr`

**Returns:**

DataFrame with columns: `data`, `valor`, `codigo`, `nome_serie`

**Example:**

```python
from agrobr import bcb

# By alias
df = await bcb.sgs("ipca", data_inicial="01/01/2024")

# By code + N most recent records
df = await bcb.sgs(432, ultimos=30)  # Selic
```

---

### `ptax`

USD PTAX exchange rate (buy and sell) from the BCB.

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

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `data` | `str \| None` | Single day, DD/MM/YYYY (quote for a specific date) |
| `data_inicial` | `str \| None` | Period start date (DD/MM/YYYY) |
| `data_final` | `str \| None` | Period end date (DD/MM/YYYY) |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with the main (normalized) columns; other fields returned by the API (such as parity and bulletin type) are preserved: `data`, `data_hora`, `cotacao_compra`, `cotacao_venda`

**Example:**

```python
from agrobr import bcb

# Period
df = await bcb.ptax(data_inicial="01/01/2024", data_final="31/01/2024")
```

---

### `focus`

Market expectations from the BCB Focus Bulletin by indicator.

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

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `indicador` | `str` | Indicator (e.g. `"PIB Agropecuária"`, `"IPCA"`). Default: `"PIB Agropecuária"` |
| `top` | `int` | Max records per page (default 1000) |
| `data_inicial` | `str \| None` | Server-side filter (`Data ge 'YYYY-MM-DD'`) |
| `max_registros` | `int \| None` | Stops pagination at the N most recent |
| `as_polars` | `bool` | Return as polars.DataFrame |
| `return_meta` | `bool` | If True, returns a (DataFrame, MetaInfo) tuple |

**Returns:**

DataFrame with columns: `indicador`, `data`, `data_referencia`, `media`, `mediana`, `desvio_padrao`, `minimo`, `maximo`, `numero_respondentes`, `base_calculo`

**Example:**

```python
from agrobr import bcb

# PIB Agropecuária expectations from June 2026 onwards
df = await bcb.focus("PIB Agropecuária", data_inicial="2026-06-01")
```

---

## Synchronous Version

```python
from agrobr.sync import bcb

df = bcb.credito_rural("soja", safra="2024/25")
serie = bcb.sgs("ipca", data_inicial="01/01/2024")
cambio = bcb.ptax(data_inicial="01/01/2024", data_final="31/01/2024")
expectativas = bcb.focus("PIB Agropecuária")
```

## Fallback

When the BCB OData API fails, agrobr automatically uses BigQuery (Base dos Dados) as a fallback. Requires `pip install agrobr[bigquery]` and a GCP project for billing: set `AGROBR_BQ_BILLING_PROJECT=<project-id>` or configure `billing_project_id` in basedosdados (`~/.basedosdados/config.toml`).

## Notes

- Source: [BCB/SICOR](https://olinda.bcb.gov.br) — free license
- Data available from 2013
- Contract v1.1 — 11 new nullable columns since v0.10.1
