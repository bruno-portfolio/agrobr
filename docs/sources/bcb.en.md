# BCB/SICOR — Rural Credit

Rural credit data from the Rural Credit Operations System (SICOR),
made available through the Central Bank's OData API.

## API

```python
from agrobr import bcb

# Working capital (custeio) credit for soybean, 2024/25 crop year
df = await bcb.credito_rural(produto="soja", safra="2024/25", finalidade="custeio")

# Filter by state
df = await bcb.credito_rural(produto="soja", safra="2024/25", uf="MT")

# Aggregate by state (sums municipalities)
df = await bcb.credito_rural(produto="soja", safra="2024/25", agregacao="uf")

# Aggregate by program
df = await bcb.credito_rural(produto="soja", safra="2024/25", agregacao="programa")

# Filter by program
df = await bcb.credito_rural(produto="soja", safra="2024/25", programa="Pronamp")

# Filter by insurance type
df = await bcb.credito_rural(produto="soja", safra="2024/25", tipo_seguro="Proagro")
```

## Columns — `credito_rural`

| Column | Type | Description |
|---|---|---|
| `safra` | str | Crop year in the format "2024/2025" |
| `ano_emissao` | int | Contract issue year |
| `mes_emissao` | int | Issue month |
| `uf` | str | State of the municipality |
| `municipio` | str | Municipality name |
| `produto` | str | Financed product |
| `finalidade` | str | Purpose (custeio, investimento, comercializacao) |
| `valor` | float | Financed amount (R$) |
| `area_financiada` | float | Financed area (ha) |
| `qtd_contratos` | int | Number of contracts |
| `cd_programa` | str | SICOR program code |
| `programa` | str | Program name (Pronamp, Pronaf, etc.) |
| `cd_sub_programa` | str | Sub-program code |
| `cd_fonte_recurso` | str | Funding source code |
| `fonte_recurso` | str | Source name (LCA, FNE, Poupanca rural, etc.) |
| `cd_tipo_seguro` | str | Insurance type code |
| `tipo_seguro` | str | Insurance name (Proagro, Seguro privado, etc.) |
| `cd_modalidade` | str | Modality code |
| `modalidade` | str | Name (Individual, Coletiva) |
| `cd_atividade` | str | Activity code |
| `atividade` | str | Name (Agricola, Pecuaria) |
| `regiao` | str | Region (SUL, CENTRO-OESTE, etc.) |

## SICOR Dimensions

The API returns dimension codes (`cdPrograma`, `cdFonteRecurso`, etc.)
that the parser automatically enriches with readable names via hardcoded
dictionaries. Unknown codes produce `"Desconhecido ({code})"` with a log warning.

## Purposes

- `custeio` — production financing
- `investimento` — purchase of machinery, infrastructure
- `comercializacao` — marketing financing

> **Note:** `industrializacao` was removed — the endpoint no longer exists in the restructured API.

## Products

Soybean, corn, coffee, cotton, rice, wheat, beans, sugarcane, cassava,
sorghum, oats, barley, among others. Use the agrobr canonical name.

## MetaInfo

```python
df, meta = await bcb.credito_rural(produto="soja", safra="2024/25", return_meta=True)
print(meta.source)          # "bcb"
print(meta.schema_version)  # "1.1"
```

## Status (Feb/2026)

The SICOR API was restructured (~2024). Old endpoints (`CusteioMunicipio`,
`InvestimentoMunicipio`) were replaced by `CusteioRegiaoUFProduto`,
`InvestRegiaoUFProduto`, `ComercRegiaoUFProduto`.

The OData operator `$filter eq` does not work on the new endpoints. The client uses
`contains(nomeProduto,'...')` for server-side filtering by product, and filters
year/state client-side after paginated download. Filters by program and insurance
type are also client-side (the API does not support `$filter` on those fields).

Retry with exponential backoff (6 attempts, 120s read timeout).
The API returns HTTP 500 intermittently. Since v0.8.0, agrobr
uses Base dos Dados (BigQuery) as an automatic fallback when the OData
API fails. Install with `pip install agrobr[bigquery]`.

## SGS — Time Series

Access to time series from the BCB Time Series Management System (Sistema
Gerenciador de Series Temporais).

### Endpoint

`api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json`

No authentication, no pagination.

### Example

```python
from agrobr import bcb

# By canonical name
df = await bcb.sgs("selic")

# By numeric code
df = await bcb.sgs(432)
```

### Pre-mapped series

| Name | Code | Description |
|------|--------|-----------|
| selic | 432 | Selic rate |
| ipca | 433 | Monthly IPCA |
| ipca_alimentacao | 1635 | IPCA food |
| pib_agropecuaria | 22083 | Agricultural GDP |
| dolar_ptax_venda | 1 | Dollar PTAX sell |
| igpm | 189 | IGP-M |
| cdi | 4392 | CDI |

### Columns

| Column | Type | Description |
|--------|------|-----------|
| data | datetime | Observation date |
| valor | float | Series value |
| codigo | int | SGS code |
| nome_serie | str | Series name |

---

## PTAX — Dollar Exchange Rate

Daily PTAX dollar exchange rate (buy and sell).

### Endpoint

`olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`

### Example

```python
from agrobr import bcb

# Period
df = await bcb.ptax(data_inicial="01/01/2026", data_final="31/01/2026")
```

### Columns

| Column | Type | Description |
|--------|------|-----------|
| data | date | Quote date |
| cotacao_compra | float | Buy quote (R$/US$) |
| cotacao_venda | float | Sell quote (R$/US$) |
| data_hora | datetime | Quote date and time |

---

## Focus — Market Expectations

Market expectations collected by the BCB (Focus report).

### Endpoint

`olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/`

### Example

```python
from agrobr import bcb

df = await bcb.focus("PIB Agropecuária")
```

### Indicators

- PIB Agropecuária
- IPCA
- IGP-DI
- IGP-M
- Taxa de cambio
- Meta para taxa over-selic

### Columns

| Column | Type | Description |
|--------|------|-----------|
| indicador | str | Indicator name |
| data | date | Collection date |
| data_referencia | str | Reference period |
| media | float | Mean of expectations |
| mediana | float | Median |
| desvio_padrao | float | Standard deviation |
| minimo | float | Minimum value |
| maximo | float | Maximum value |
| numero_respondentes | int | Number of respondents |
| base_calculo | int | Calculation basis (0 or 1) |

---

## Source

- SICOR API: `https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata`
- SGS API: `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados`
- PTAX API: `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`
- Focus API: `https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/`
- Update frequency: monthly (SICOR), daily (SGS, PTAX, Focus)
- History: 2013+ (SICOR), variable (SGS)
- Contract: v1.1 (11 new nullable columns since v0.10.1)
