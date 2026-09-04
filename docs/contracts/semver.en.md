# Versioning Policy (Semver)

agrobr follows [Semantic Versioning 2.0.0](https://semver.org/) with per-dataset
granularity. Each dataset has its own `schema_version` (independent of `lib_version`).

## Rules

| Change type | Bump | Example |
|---|---|---|
| Field removed or renamed | **Major** | Rename `preco` → `price` |
| Data type changed (narrowing) | **Major** | `price: float64` → `price: str` |
| Required column becomes optional | **Major** | `uf: required` → `uf: nullable` |
| New optional column added | Minor | Add `latitude` |
| Constraint added | Minor | Add `price_min: 0` |
| New fallback source | Patch | Add ABIOVE as backup |
| Parsing fix (same columns) | Patch | Fix municipality encoding |
| Data type widened | Patch | `int` → `float` (compatible) |
| New data source (module) | Minor | `agrobr.bcb` |

**Principle:** a dataset's `schema_version` only bumps major when the change
can break downstream code that depends on the current schema.

## Per-Dataset Guarantees

### `preco_diario`

| Column | Type | Guarantee | Since |
|---|---|---|---|
| `data` | `date` | required | v0.4.0 |
| `produto` | `str` | required | v0.4.0 |
| `valor` | `float` | required, > 0 | v0.4.0 |
| `unidade` | `str` | required | v0.4.0 |
| `fonte` | `str` | required | v0.6.0 |

### `estimativa_safra`

| Column | Type | Guarantee | Since |
|---|---|---|---|
| `produto` | `str` | required | v0.4.0 |
| `safra` | `str` | required, format `YYYY/YY` | v0.4.0 |
| `uf` | `str` | optional | v0.4.0 |
| `area_plantada` | `float` | optional, >= 0 | v0.4.0 |
| `area_colhida` | `float` | optional, >= 0 | v0.4.0 |
| `produtividade` | `float` | optional, >= 0 | v0.4.0 |
| `producao` | `float` | optional, >= 0 | v0.4.0 |
| `levantamento` | `int` | optional, 1-12; null when the source is LSPA | v1.2.0 |
| `data_publicacao` | `date` | optional; null when the source is LSPA | v1.2.0 |
| `fonte` | `str` | required | v0.6.0 |

### `producao_anual` (IBGE PAM)

| Column | Type | Guarantee | Since |
|---|---|---|---|
| `ano` | `int` | required, >= 1974 | v0.4.0 |
| `localidade` | `str` | optional | v0.4.0 |
| `produto` | `str` | required | v0.4.0 |
| `area_plantada` | `float` | optional, >= 0 | v0.4.0 |
| `area_colhida` | `float` | optional, >= 0 | v0.4.0 |
| `producao` | `float` | optional, >= 0 | v0.4.0 |
| `rendimento` | `float` | optional, >= 0 | v0.4.0 |
| `valor_producao` | `float` | optional, >= 0 | v0.4.0 |
| `fonte` | `str` | required | v0.6.0 |

### Source Layer — Per-module Contracts

Source-layer modules (`agrobr.cepea`, `agrobr.conab`, etc.) return
DataFrames with documented columns, but with a **weaker** guarantee than the
datasets layer. The datasets layer normalizes and validates.

#### `comexstat.exportacao` (v1.0)

| Column | Type | Guarantee |
|---|---|---|
| `ano` | `int` | required |
| `mes` | `int` | required, 1-12 |
| `produto` | `str` | required |
| `uf` | `str` | optional |
| `kg_liquido` | `float` | optional, >= 0 |
| `valor_fob_usd` | `float` | optional, >= 0 |

#### `bcb.credito_rural` (v2.0)

| Column | Type | Guarantee |
|---|---|---|
| `safra` | `str` | required |
| `produto` | `str` | required |
| `uf` | `str` | optional |
| `finalidade` | `str` | required |
| `agregacao` | `str` | required (`uf` or `programa`) |
| `programa` | `str` | optional |
| `cd_programa` | `str` | optional |
| `qtd_contratos` | `int` | optional, >= 0 |
| `valor` | `float` | optional, >= 0 |
| `area_financiada` | `float` | optional, >= 0 |
| `fonte` | `str` | required |

#### `inmet.clima_uf` (v1.0)

| Column | Type | Guarantee |
|---|---|---|
| `mes` | `date` | required |
| `uf` | `str` | required |
| `precip_acum_mm` | `float` | required, >= 0 |
| `temp_media` | `float` | required |
| `temp_max_media` | `float` | required |
| `temp_min_media` | `float` | required |
| `num_estacoes` | `int` | optional, >= 0 |
| `umidade_media` | `float` | optional, 0-100 |
| `radiacao_media_mj` | `float` | optional, >= 0 |
| `vento_medio_ms` | `float` | optional, >= 0 |
| `fonte` | `str` | required |

#### `anda.entregas` (v2.0)

| Column | Type | Guarantee |
|---|---|---|
| `ano` | `int` | required |
| `mes` | `int` | required, 1-12 |
| `uf` | `str` | optional |
| `produto_fertilizante` | `str` | required |
| `volume_ton` | `float` | optional, >= 0 |

## MetaInfo

All functions with `return_meta=True` return `MetaInfo` with provenance
fields. MetaInfo fields are **additive** (never removed), so they do not
constitute a breaking change.

## Deprecation

Before removing a column or changing a type (breaking change):

1. Column marked as `deprecated` for at least 1 minor release
2. Warning emitted via `DeprecationWarning` at runtime
3. Documented in the CHANGELOG
4. Removed in the next major
