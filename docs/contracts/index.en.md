# Data Contracts

agrobr guarantees schema stability. Your pipeline won't break.

Each contract is defined in Python (`agrobr/contracts/`) and exported as JSON (`agrobr/schemas/`).
Validation is automatic: every dataset `fetch()` validates the DataFrame against the registered contract.

## Global Guarantees

| Guarantee | Description |
|-----------|-------------|
| **Stable names** | Columns are never renamed (only added) |
| **Stable columns present** | Every `stable` column exists in the DataFrame; `nullable` permits null values, not an absent column |
| **Types only widen** | int→float ok, float→int never |
| **ISO-8601 dates** | Always YYYY-MM-DD |
| **Explicit units** | Dedicated column |
| **Breaking = Major** | Breaking changes only in major versions |
| **Primary keys** | Where a key is defined, its columns exist and their combination is unique |
| **Min/max constraints** | Numeric values validated against bounds |
| **Unknown arguments** | Every dataset rejects an argument outside its signature with `TypeError`, before network access |
| **Polars types** | With `as_polars=True`, each column has its contract type (`int` → `Int64`, `float` → `Float64`, `str` → `String`, `bool` → `Boolean`), even when all null; an all-null date column is `Datetime("ns")` |

## Datasets

> This table lists the **documented** contracts — it is not identical to `datasets.list_datasets()`. `bcb_focus`, `bcb_ptax` and `bcb_ptax_moedas` are source contracts reused by `expectativas_mercado`, `cotacoes_cambio` and `moedas_cambio`; their dataset names do not add contract aliases. The PTAX page covers both quotes and the catalogue.

The four Agrofit dataset names reuse the existing `agrofit_*` contracts through `_contract_name`; they do not register contract aliases. There are 54 datasets and 89 registered contracts. `bcb_credito_rural_total` is the source contract of the `bcb.credito_rural_total` function, without a dataset, and `bcb_credito_rural_registro` that of `agregacao="registro"` in `bcb.credito_rural` and in the `credito_rural` dataset. `autorizacoes_defensivos` preserves published duplicate rows and has no artificial primary key.

`series_economicas` reuses `bcb_sgs` 3.0 without a contract alias. Selection uses an SGS code or alias, with units and frequency depending on the series. The dataset preserves query provenance and does not reconstruct historical revisions.

The two [cultivar datasets](../api/cultivares.en.md) reuse the new `rnc_registradas` and `rnc_protegidas` 1.0 source contracts. Keys are the RNC registration number and the SNPC application number; certificates shared by distinct applications are preserved. Queries use the current registry, with a 24-hour acquisition cache and no historical reconstruction.

[`uso_do_solo`](uso_do_solo.en.md) validates municipal coverage from collections 10 and 11 with `mapbiomas_cobertura_municipal` 1.1. Its eleven columns include the published `geocodigo` and `id_registro` and the `cod_municipio` taken from `geocodigo`; distinct records sharing a territorial classification are preserved. State coverage and transitions keep their 2.0 contracts.

`empregadores_lista_suja` also reuses an existing source contract, `lista_suja_empregadores` 2.0, without an alias. Its ID key applies within one publication content hash; repeated documents are preserved.

| Dataset | Description | Sources |
|---------|-------------|---------|
| [unidades_conservacao](./unidades_conservacao.en.md) | Federal, state and municipal conservation units, including private reserves, from CNUC. | CNUC/MMA |
| [unidades_conservacao_federais](./unidades_conservacao_federais.en.md) | Federal conservation-unit attributes published by ICMBio. | ICMBio |
| [precos_diesel](./precos_diesel.en.md) | Weekly diesel prices and explicit monthly aggregates from ANP. | ANP |
| [moedas_cambio](./moedas_cambio.en.md) | Current currency catalogue from the BCB PTAX OData service. | BCB |
| [expectativas_mercado](./expectativas_mercado.en.md) | Annual or monthly market expectations from BCB Focus surveys. | BCB |
| [cotacoes_cambio](./cotacoes_cambio.en.md) | Published PTAX exchange-rate quotes by currency and bulletin. | BCB |
| [autorizacoes_defensivos](./autorizacoes_defensivos.en.md) | Use authorizations preserving published multiplicity — `agrofit_autorizacoes` v1.1 | Agrofit/MAPA |
| [composicao_defensivos](./composicao_defensivos.en.md) | Components and concentrations by family and registration — `agrofit_composicao` v1.0 | Agrofit/MAPA |
| [defensivos_formulados](./defensivos_formulados.en.md) | Formulated products by registration — `agrofit_formulados` v1.1 | Agrofit/MAPA |
| [defensivos_tecnicos](./defensivos_tecnicos.en.md) | Technical products by registration — `agrofit_tecnicos` v1.1 | Agrofit/MAPA |
| [preco_diario](./preco_diario.md) | Daily spot prices | CEPEA → cache |
| [producao_anual](./producao_anual.md) | Consolidated annual output | IBGE PAM → CONAB |
| [empregadores_lista_suja](./empregadores_lista_suja.en.md) | Current MTE employer registry — `lista_suja_empregadores` v2.0 | MTE / Lista Suja |
| [estimativa_safra](./estimativa_safra.md) | Estimates v3.1 by CONAB survey or LSPA month | CONAB → IBGE LSPA; explicit selection |
| [balanco](./balanco.md) | Supply/demand balance | CONAB |
| [credito_rural](./credito_rural.md) | Rural credit by crop | BCB/SICOR → BigQuery |
| [cultivares_registradas](./cultivares_registradas.en.md) | RNC registry with textual registration IDs — `rnc_registradas` v1.0 | CultivarWeb/MAPA |
| [cultivares_protegidas](./cultivares_protegidas.en.md) | SNPC registry by application, preserving end-date text — `rnc_protegidas` v1.0 | CultivarWeb/MAPA |
| [bcb_sgs](./bcb_sgs.en.md) | Time series by code, reference dates and historical blocks | BCB SGS |
| [bcb_focus](./bcb_focus.en.md) | Annual/monthly expectations, detail, base, and coverage | BCB Focus |
| [bcb_ptax / bcb_ptax_moedas](./bcb_ptax.en.md) | Quotes by currency/bulletin and current OData catalogue | BCB PTAX |
| [bcb_credito_rural_total](./bcb_credito_rural_total.en.md) | Rural credit by state and purpose, without product, with agro-industrialisation | BCB/SICOR (`RegiaoUF`) |
| [bcb_credito_rural_registro](./bcb_credito_rural_registro.en.md) | Rural credit record by record (`agregacao="registro"`), with month, funding source, modality and activity | BCB/SICOR (`*RegiaoUFProduto`) |
| [exportacao](./exportacao.md) | Agricultural exports | ComexStat → ABIOVE |
| [fertilizante](./fertilizante.md) | Fertilizer deliveries | ANDA |
| [importacao](./importacao.md) | Agricultural imports | ComexStat |
| [custo_producao](./custo_producao.md) | Production costs | CONAB |
| [custo_sociobiodiversidade](./custo_sociobiodiversidade.md) | Sociobiodiversity costs in published units | CONAB |
| [pecuaria_municipal](./pecuaria_municipal.md) | Herds and animal production | IBGE PPM |
| [abate_trimestral](./abate_trimestral.md) | Slaughter of cattle, hogs and poultry | IBGE Slaughter |
| [censo_agropecuario](./censo_agropecuario.md) | Agricultural Census 1995/2006/2017 (11 themes) | IBGE Agri Census |
| [censo_agropecuario_legado](./censo_agropecuario_legado.md) | Agricultural Census 1995/96 — 6 legacy themes (FTP) | IBGE FTP |
| [censo_agropecuario_historico](./censo_agropecuario_historico.md) | Agricultural Census historical series 1920-2006 (9 themes, up to state) | IBGE SIDRA |
| [censo_agropecuario_municipal_1985](./censo_agropecuario_municipal_1985.md) | 1985 census — 53 municipal tables, cell by cell, with each cell's status | IBGE PDFs |
| [cadastro_rural](./cadastro_rural.md) | Rural Environmental Registry | SICAR |
| [clima](./clima.md) | Monthly state climate; daily/hourly station observations | INMET API → INMET ZIP → NASA POWER (state) |
| [comercio_internacional](./comercio_internacional.md) | Bilateral international trade (HS codes) | UN Comtrade |
| [condicao_lavouras](./condicao_lavouras.md) | Paraná crop conditions | SEAB/DERAL |
| [desmatamento](./desmatamento.md) | PRODES deforestation and DETER alerts by biome | INPE |
| [embarques_anec](./embarques_anec.md) | Weekly shipments by port and product | ANEC |
| [embarques_mensais_anec](./embarques_mensais_anec.md) | Monthly volumes, estimates and ranges by edition | ANEC |
| [comparacao_anual_anec](./comparacao_anual_anec.md) | Monthly comparison between years by edition | ANEC |
| [destinos_anec](./destinos_anec.md) | Destination shares for the cumulative period | ANEC |
| [silvicultura](./silvicultura.md) | Silvicultural output (IBGE PEVS) | IBGE PEVS |
| [extrativismo_vegetal](./extrativismo_vegetal.md) | Extractive plant production (IBGE PEVS) | IBGE PEVS |
| [leite_industrial](./leite_industrial.md) | Quarterly milk (acquisition/processing) | IBGE Milk |
| [lspa](./lspa.md) | Monthly agricultural production estimates | IBGE LSPA |
| [oferta_demanda_global](./oferta_demanda_global.md) | Global supply/demand (USDA PSD) | USDA |
| [pib_agro](./pib_agro.md) | Agricultural GDP by sector and quarter | IBGE SIDRA |
| [preco_atacado](./preco_atacado.md) | Wholesale prices at CEASAs | CONAB CEASA/PROHORT |
| [progresso_safra](./progresso_safra.md) | Weekly sowing/harvest progress | CONAB |
| [queimadas](./queimadas.md) | Satellite fire hotspots | INPE |
| [futuros_agricolas](./futuros_agricolas.md) | B3 agricultural futures (settlements, history, positions) | B3 |
| [posicionamento_fundos](./posicionamento_fundos.md) | Fund positioning by trader category (COT) | CFTC |
| [movimentacao_portuaria](./movimentacao_portuaria.md) | Port cargo movement ⚠️ (source offline) | ANTAQ |
| [seguro_rural](./seguro_rural.md) | Rural insurance — policies and claims | MAPA PSR |
| [serie_historica_safra](./serie_historica_safra.md) | Crop historical series (45 products) | CONAB |
| [series_economicas](./series_economicas.en.md) | Series by SGS code or alias, date range and latest observations | BCB SGS |
| [uso_do_solo](./uso_do_solo.md) | Land cover and use (MapBiomas) | MapBiomas |
| [zoneamento_agricola](./zoneamento_agricola.md) | Agricultural climate risk zoning (ZARC) | MAPA/Embrapa |

## JSON Schemas

Each contract automatically generates a JSON file in `agrobr/schemas/`:

```python
from agrobr.contracts import get_contract, list_contracts, generate_json_schemas

# List registered contracts
list_contracts()

# Access a contract
contract = get_contract("preco_diario")
print(contract.primary_key)   # ['data', 'produto']
print(contract.to_json())     # Full JSON schema

# Validation (automatic on every fetch, or manual)
from agrobr.contracts import validate_dataset
validate_dataset(df, "preco_diario")  # raises ContractViolationError

# Generate all JSONs
generate_json_schemas("agrobr/schemas/")
```

## Usage

```python
from agrobr import datasets

# List datasets
print(datasets.list_datasets())
# 54 datasets

# List a dataset's products
datasets.list_products("preco_diario")
# ['soja', 'milho', 'boi', 'bezerro', 'cafe', 'cafe_robusta', 'trigo', 'algodao']

# Dataset info
datasets.info("preco_diario")
# {'name': 'preco_diario', 'sources': ['cepea', 'cache'], ...}

# Text sheet of one dataset and of all of them
print(datasets.describe("preco_diario"))
print(datasets.describe_all())

# The dataset object (a copy): info and fetch with the arguments of datasets.preco_diario
ds = datasets.get_dataset("preco_diario")
df = await ds.fetch("soja", inicio="2024-01-01")
```

An unknown name in `get_dataset`, `info`, `list_products` or `describe` raises `UnknownNameError`, which inherits from
`InvalidParameterError` and `KeyError`, with the valid names in the message.

## Automatic Fallback

Automatic fallback applies to datasets with configured alternative sources, respecting query selection and coverage. Single-source datasets, including all four Agrofit datasets, have no fallback to another institution:

```
preco_diario: CEPEA → local cache
producao_anual: IBGE PAM → CONAB
estimativa_safra: CONAB → IBGE LSPA
balanco: CONAB
credito_rural: BCB/SICOR → BigQuery (basedosdados)
exportacao: ComexStat → ABIOVE
fertilizante: ANDA
custo_producao: CONAB
custo_sociobiodiversidade: CONAB
clima: INMET API → INMET ZIP → NASA POWER (state mode only)
futuros_agricolas: B3
```

## MetaInfo

Every call with `return_meta=True` returns provenance metadata:

```python
df, meta = await datasets.preco_diario("soja", return_meta=True)

print(meta.source)            # Source used
print(meta.dataset)           # Dataset name
print(meta.contract_version)  # Contract version
print(meta.records_count)     # Records returned
print(meta.from_cache)        # Whether it came from cache
print(meta.attempted_sources) # Sources tried, including internal cascades
print(meta.selected_source)   # Actual source that supplied the data
print(meta.snapshot)          # Cutoff date (deterministic mode)
print(meta.license)           # License classification of the data (docs/licenses.md)
```

If an adapter invokes an internal cascade, `attempted_sources`,
`selected_source`, and `from_cache` preserve that provenance. A simple source
keeps the dataset adapter name.

`license` is the license classification of the data (`livre`, `nc`, `zona_cinza` or `restrito`, from the
[license table](../licenses.md)): the most restrictive among the `data_sources` (CEPEA with the Notícias
Agrícolas fallback comes out `restrito`) or, without them, the selected source's. It comes from
`agrobr.constants.LICENCAS`, the same table as the documentation; a source outside it gives `None`.

**Physical provenance.**

- `source_url` is the resource that supplied the data, such as the query with
  its filters or the downloaded file. Tokens and credentials come out as
  `[REDACTED]`. The institutional page, when useful, goes in `source_details`.
- With a single body, `raw_content_hash` is its full SHA-256 and
  `raw_content_size` is its size in bytes.
- `fetch_timestamp` is the UTC time of the acquisition of the body the top level
  describes, and datasets pass on the source's value:
  - a body received now: the time of that acquisition;
  - a body read from cache (for example, INMET's ZIP, Acervo Fundiário, ANEC,
    RNC, ZARC, and Agrofit): the time of the original acquisition, equal to
    `fetched_at`;
  - several bodies: the most recent acquisition among them, equal to
    `fetched_at`;
  - records from CEPEA's DuckDB cache: null (see below).
- With several bodies (pages, years, periods, or queries), `raw_content_hash`
  is null and `raw_content_size` is zero. INMET (`resources`), PSR (`corpos`),
  IBGE (`consultas`, on SIDRA), and the CONAB series (`publicacao.series`) list
  each body in `source_details`, with URL and SHA-256.
- An auxiliary body that feeds the data goes in `source_details`, with URL,
  SHA-256, and bytes, and the top level describes the records body. For IMEA, the
  catalog that names the indicators is in `indicadores_url`,
  `indicadores_sha256`, and `indicadores_bytes`.
- Data that CEPEA reads from the DuckDB cache (`source` equal to `cache` or
  `cache_fallback`) comes with a null `raw_content_hash`, a zero
  `raw_content_size`, and a null `fetch_timestamp`, because the cache stores
  records, not the body. `fetched_at` is the time of the original collection.
- When `source_details` has `hash_kind` or `raw_content_hash_kind` equal to
  `resource_manifest_sha256` (BCB, Comtrade, Embrapa Solos, FUNAI, PRODES/DETER,
  INCRA, and SICAR), `raw_content_hash` is the SHA-256 of the query manifest, not
  of an HTTP body, and `raw_content_size` is the size of that manifest. The
  manifest carries the time of each resource (`fetched_at`) in these sources
  except SICAR, and also in ANP with more than one file (`raw_hash_kind`): the
  hash identifies the acquisition and changes on every call, even with the same
  content. To know
  whether the content changed, compare the `sha256` of each item of
  `source_details["resources"]`.
- For ANTT (`fluxo_pedagio`) and CONAB costs (`custo_producao`), `hash_kind` is their own
  (`sha256_canonical_utf8_query_and_acquisition_manifest` and
  `sha256_canonical_utf8_query_acquisition_selection_manifest`), under the same rule: hash
  and size are those of the manifest. Total bytes received, including retries and failures,
  are in `source_details["received_bytes"]`, and the bytes of the files that went into the
  data (CSVs for ANTT, the workbook for CONAB), in `source_details["data_file_bytes"]`.
- For NASA POWER, the hash is that of the receipt list in
  `source_details["http_receipts"]`, and the size adds up the HTTP bodies.
