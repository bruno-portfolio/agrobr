# Data Contracts

agrobr guarantees schema stability. Your pipeline won't break.

Each contract is defined in Python (`agrobr/contracts/`) and exported as JSON (`agrobr/schemas/`).
Validation is automatic: every dataset `fetch()` validates the DataFrame against the registered contract.

## Global Guarantees

| Guarantee | Description |
|-----------|-------------|
| **Stable names** | Columns are never renamed (only added) |
| **Types only widen** | int→float ok, float→int never |
| **ISO-8601 dates** | Always YYYY-MM-DD |
| **Explicit units** | Dedicated column |
| **Breaking = Major** | Breaking changes only in major versions |
| **Primary keys** | Each dataset has a defined primary key (no duplicates) |
| **Min/max constraints** | Numeric values validated against bounds |

## Datasets

> This table lists the **documented** contracts (one page each) — it is not identical to `datasets.list_datasets()`. `lspa` is a source-API contract (via `ibge.lspa()`, no dataset wrapper); `embarques_anec` is a registered dataset + contract but has no page yet.

| Dataset | Description | Sources |
|---------|-------------|---------|
| [preco_diario](./preco_diario.md) | Daily spot prices | CEPEA → cache |
| [producao_anual](./producao_anual.md) | Consolidated annual output | IBGE PAM → CONAB |
| [estimativa_safra](./estimativa_safra.md) | Current-season estimates | CONAB → IBGE LSPA |
| [balanco](./balanco.md) | Supply/demand balance | CONAB |
| [credito_rural](./credito_rural.md) | Rural credit by crop | BCB/SICOR → BigQuery |
| [exportacao](./exportacao.md) | Agricultural exports | ComexStat → ABIOVE |
| [fertilizante](./fertilizante.md) | Fertilizer deliveries | ANDA |
| [importacao](./importacao.md) | Agricultural imports | ComexStat |
| [custo_producao](./custo_producao.md) | Production costs | CONAB |
| [pecuaria_municipal](./pecuaria_municipal.md) | Herds and animal production | IBGE PPM |
| [abate_trimestral](./abate_trimestral.md) | Slaughter of cattle, hogs and poultry | IBGE Slaughter |
| [censo_agropecuario](./censo_agropecuario.md) | Agricultural Census 1995/2006/2017 (10 themes) | IBGE Agri Census |
| [censo_agropecuario_legado](./censo_agropecuario_legado.md) | Agricultural Census 1995/96 — 6 legacy themes (FTP) | IBGE FTP |
| [censo_agropecuario_historico](./censo_agropecuario_historico.md) | Agricultural Census historical series 1920-2006 (9 themes, up to state) | IBGE SIDRA |
| [censo_agropecuario_municipal_1985](./censo_agropecuario_municipal_1985.md) | 1985 municipal census — 53 themes via OCR of PDFs (22 states) | IBGE PDFs |
| [cadastro_rural](./cadastro_rural.md) | Rural Environmental Registry | SICAR |
| [clima](./clima.md) | Monthly climate data by state or station | INMET → NASA POWER |
| [comercio_internacional](./comercio_internacional.md) | Bilateral international trade (HS codes) | UN Comtrade |
| [condicao_lavouras](./condicao_lavouras.md) | Paraná crop conditions | SEAB/DERAL |
| [desmatamento](./desmatamento.md) | PRODES deforestation and DETER alerts by biome | INPE |
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
| [movimentacao_portuaria](./movimentacao_portuaria.md) | Port cargo movement | ANTAQ |
| [seguro_rural](./seguro_rural.md) | Rural insurance — policies and claims | MAPA PSR |
| [serie_historica_safra](./serie_historica_safra.md) | Crop historical series (32 crops) | CONAB |
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
datasets.list_datasets()
# ['abate_trimestral', 'balanco', 'cadastro_rural', 'censo_agropecuario',
#  'censo_agropecuario_historico', 'censo_agropecuario_legado',
#  'censo_agropecuario_municipal_1985', 'clima', 'comercio_internacional',
#  'condicao_lavouras', 'credito_rural', 'custo_producao', 'desmatamento',
#  'embarques_anec', 'estimativa_safra', 'exportacao', 'extrativismo_vegetal',
#  'fertilizante', 'futuros_agricolas', 'importacao', 'leite_industrial',
#  'movimentacao_portuaria', 'oferta_demanda_global', 'pecuaria_municipal',
#  'pib_agro', 'posicionamento_fundos', 'preco_atacado', 'preco_diario',
#  'producao_anual', 'progresso_safra', 'queimadas', 'seguro_rural',
#  'serie_historica_safra', 'silvicultura', 'uso_do_solo', 'zoneamento_agricola']
# (36 datasets)

# List a dataset's products
datasets.list_products("preco_diario")
# ['soja', 'milho', 'boi', 'cafe', 'cafe_robusta', 'trigo', 'algodao']

# Dataset info
datasets.info("preco_diario")
# {'name': 'preco_diario', 'sources': ['cepea', 'cache'], ...}
```

## Automatic Fallback

Each dataset has multiple prioritized sources. If the primary source
fails, agrobr automatically tries the next:

```
preco_diario: CEPEA → local cache
producao_anual: IBGE PAM → CONAB
estimativa_safra: CONAB → IBGE LSPA
balanco: CONAB
credito_rural: BCB/SICOR → BigQuery (basedosdados)
exportacao: ComexStat → ABIOVE
fertilizante: ANDA
custo_producao: CONAB
clima: INMET → NASA POWER
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
print(meta.snapshot)          # Cutoff date (deterministic mode)
```
