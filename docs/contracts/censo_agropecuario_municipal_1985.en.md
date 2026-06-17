# censo_agropecuario_municipal_1985 v1.0

Agricultural Census 1985 — municipal data extracted via OCR of IBGE PDFs (22 states, 53 themes).

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | IBGE Agri Census Municipal 1985 | Local CSVs extracted via OCR of state PDFs |

## Themes

53 themes covering land structure, land use, labor, mechanization, livestock, crops and production:

`assistencia_tecnica`, `bens_despesas`, `classe_atividade_economica`, `colheita_lav_temporaria`, `condicao_legal_terras`, `condicao_produtor`, `conservacao_solo`, `cooperativas`, `despesas`, `efetivo_asininos`, `efetivo_aves`, `efetivo_bovinos`, `efetivo_bubalinos_femeas`, `efetivo_bubalinos_machos`, `efetivo_caprinos`, `efetivo_coelhos`, `efetivo_equinos`, `efetivo_muares`, `efetivo_ovinos`, `efetivo_suinos`, `energia_eletrica`, `fertilizantes_defensivos`, `forma_administracao`, `grupos_area_complemento`, `grupos_area_total`, `grupos_pessoal_ocupado`, `horticultura`, `inseminacao_ordenha`, `investimentos`, `irrigacao`, `lavoura_permanente`, `maquinas_instrumentos`, `meios_transporte`, `parcelas`, `pessoal_ocupado`, `producao_animal_cont`, `producao_la_mel`, `producao_leite_ovos`, `producao_particular`, `produtos_extrativos`, `propriedade_terras`, `residencia_produtor`, `servicos_empreitada`, `silos_armazenamento`, `silvicultura`, `silvicultura_cont`, `terras_fora_area`, `terras_proprias_terceiros`, `transformacao_beneficiamento`, `transformacao_cont`, `uso_forca_trabalho`, `utilizacao_terras`, `valor_bens_invest_financ`

Use `temas_censo_agro_municipal_1985()` for the full list.

## Schema

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ano` | int | ❌ | Always 1985 |
| `uf` | str | ❌ | State abbreviation (22 states) |
| `uf_cod` | int | ❌ | IBGE state code (11-53) |
| `localidade` | str | ❌ | Municipality/mesoregion/microregion name |
| `localidade_cod` | int | ✅ | IBGE code (resolved via OCR, may be None) |
| `nivel` | str | ❌ | total, mesorregiao, microrregiao, municipio |
| `tema` | str | ❌ | Table theme |
| `categoria` | str | ❌ | Always "geral" |
| `variavel` | str | ❌ | Variable name (semantic or val_N) |
| `valor` | float64 | ✅ | Numeric value |
| `unidade` | str | ❌ | Unit of measure |
| `confianca` | str | ❌ | OCR quality: alta, media or baixa |
| `fonte` | str | ❌ | Always "ibge_censo_agro_municipal_1985" |

## Primary Key

Empty (`[]`). OCR data can produce homonymous labels (e.g. 3 "Santo Antonio" municipalities in MG belonging to distinct microregions). Uniqueness is not guaranteed at the name level.

## Format

Long format: each row holds one variable/value pair.

### Semantic columns vs val_N

12 tables have columns with semantic names (e.g. `estab_total`, `area_ha_total`). The rest use `val_1`, `val_2`, etc.

### Units by prefix

| Prefix | Unit |
|---------|------|
| `estab_` | estabelecimentos |
| `area_ha_` | hectares |
| `inform_` | informantes |
| `num_pessoas_` | pessoas |
| `efetivo_` | cabeças |
| `qtde_` | toneladas |
| `valor_` | mil_cruzeiros |
| `val_` | unidades |

## Territorial Levels

| Level | Description |
|-------|-------------|
| `total` | State total |
| `mesorregiao` | Mesoregion |
| `microrregiao` | Microregion |
| `municipio` | Municipality |

## Available States (22)

AC, AL, AM, AP, BA, DF, ES, GO, MG, MS, MT, PA, PB, PE, PR, RJ, RO, RR, RS, SC, SE, SP

**Excluded:** MA, PI, CE, RN — PDFs without an OCR layer (Tesseract insufficient).

## Guarantees

- `ano` is always 1985
- Numeric values are always >= 0
- `fonte` is always "ibge_censo_agro_municipal_1985"
- `confianca` indicates OCR quality: alta, media or baixa
- `localidade_cod` may be None when OCR does not allow an exact match
- Empty PK: OCR may produce homonymous labels
- Data extracted from 28 state PDFs + 1 national (cross-validation 77.9%)
- Update frequency: never (historical data)

## Quirks

- **OCR labels**: ~4.2% of municipality labels contain residual OCR artifacts
- **0.08% unrecoverable**: labels with digits embedded in the name are not resolvable
- **val_N columns**: 41 tables use generic names (consult _index.csv for meaning)
- **Cross-validation**: 77.9% match between state and national totals (96.8% exact when there is a match)

## Example

```python
from agrobr import ibge

# Land ownership in São Paulo
df = await ibge.censo_agro_municipal_1985('propriedade_terras', uf='SP')

# Municipalities only
df = await ibge.censo_agro_municipal_1985('efetivo_bovinos', nivel='municipio')

# List themes
temas = await ibge.temas_censo_agro_municipal_1985()

# Via the semantic dataset
from agrobr import datasets
df = await datasets.censo_agropecuario_municipal_1985('utilizacao_terras', uf='MG')

# With metadata
df, meta = await ibge.censo_agro_municipal_1985('propriedade_terras', return_meta=True)
```

### CLI

```bash
# Land ownership data in SP
agrobr ibge censo-municipal-1985 propriedade_terras --uf SP

# CSV format
agrobr ibge censo-municipal-1985 efetivo_bovinos --formato csv

# List available themes
agrobr ibge temas-municipal-1985
```

## JSON Schema

Available at `agrobr/schemas/censo_agropecuario_municipal_1985.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("censo_agropecuario_municipal_1985")
print(contract.to_json())
```

## Relationship with other contracts

| Contract | Scope | Periods |
|----------|-------|---------|
| `censo_agropecuario` | 10 thematic themes (SIDRA) | 1995, 2006, 2017 |
| `censo_agropecuario_legado` | 6 legacy themes (FTP) | 1995 |
| `censo_agropecuario_historico` | 9 historical-series themes (SIDRA, up to state) | 1920-2006 |
| **`censo_agropecuario_municipal_1985`** | **53 municipal themes (OCR of PDFs)** | **1985** |

These are separate contracts, no conflict. This is the only one with municipal 1985 data.
