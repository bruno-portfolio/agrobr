# BCB PTAX — quotes 2.0 and currencies 1.0

Source-API contracts for `bcb.ptax()` and `bcb.ptax_moedas()`, registered as `bcb_ptax` and `bcb_ptax_moedas`. Constants `BCB_PTAX_V2` and `BCB_PTAX_MOEDAS_V1` live in `agrobr.contracts.bcb_ptax`. Exported schemas are `agrobr/schemas/bcb_ptax.json` and `agrobr/schemas/bcb_ptax_moedas.json`. The `cotacoes_cambio` and `moedas_cambio` datasets reuse these source contracts.

## Quotes

| Column | pandas type | Nullable | Meaning |
|--------|-------------|----------|---------|
| `cotacao_compra` | float64 | Yes | Published bid quote, without conversion |
| `cotacao_venda` | float64 | Yes | Published ask quote, without conversion |
| `data_hora` | datetime64[ns], timezone-naive | No | Published clock time, retaining up to nine fractional digits |
| `data` | datetime64[ns], timezone-naive | No | Civil date of data_hora, without time of day |
| `moeda` | text | No | Uppercase ASCII symbol, validated against the acquired catalogue |
| `paridade_compra` | float64 | Yes | Published bid parity |
| `paridade_venda` | float64 | Yes | Published ask parity |
| `tipo_boletim` | text | Yes | Original published label, without output normalization |

The eight-column order retains the previous four as its prefix. Empty output keeps all columns and dtypes. The key is `moeda, data_hora, tipo_boletim` within the selected acquisition/route. Null is a missing dimension. Do not key only by date or truncated seconds: intermediate and closing bulletins may have different fractions within the same second. Acquisition orders timestamps ascending; the civil date must match timestamp normalization.

Day and period routes publish different closing labels for the same event: `Fechamento PTAX` and `Fechamento`. The selector recognizes both, while the contract retains raw text; account for this difference when joining outputs from different routes. Unknown, empty/whitespace, or null labels are allowed in todos with a warning; empty text differs from null; specific selectors raise when safe classification is unavailable.

Four measures require finite JSON numbers or explicit nulls; missing required fields, numeric text, booleans, infinities, ambiguous JSON, and underflow raise errors. There is no imputation, parity inversion, or closing calculation. Finite non-positive values or bids greater than asks remain with diagnostics. Invalid, timezone-bearing, or out-of-ns timestamps are rejected without truncating precision. The contract does not certify the financial quality of source values.

Quotes use the domestic monetary unit at the reference date per unit of the selected currency, without claiming BRL across all history. Parity depends on catalogue type A/B; context and units are retained in metadata. Acquisition uses UTC; no timezone is inferred for published clock time.

## Currency catalogue

| Column | pandas type | Nullable | Meaning |
|--------|-------------|----------|---------|
| `moeda` | text | No | Three-letter uppercase ASCII symbol |
| `nome` | text | No | Published name, retaining its spelling |
| `tipo_moeda` | text | No | Published type; A/B recognized, new types retained with diagnostics |

The currency key is unique and sorted. Text must be non-empty. Empty output retains three columns. Records describe the current OData catalogue without proving historical validity or the universe of the portal's broader currency table. A missing symbol differs from catalogue unavailability.

## Acquisition and provenance

Quote acquisition reads the catalogue with separate pagination and validates every bulletin before selection. Duplicates within/across pages, selection changes, and failed pages raise without partial returns. Without count, a short page advances by the received number until an empty page. Quote `coverage` distinguishes received, returned, and filtered rows; `catalog.coverage` describes the catalogue. Intentional filtering is not truncation. Complete requires a reconciled total; without one, coverage is unknown. There is no revision snapshot.

MetaInfo uses schema/contract 2.0 for quotes, 1.0 for currencies, and parser 2 for both. Sources are `bcb_ptax` and `bcb_ptax_moedas`. Flattened resources identify catalog/quotes role, zero-based page index within each role, URL/parameters, counts, UTC acquisition, body hashes/bytes, and layout. Top-level hash/size represent a canonical UTF-8 query/resources manifest; total body bytes are separate. Selected catalogue entry, units, dtypes, nulls, and warnings accompany output.

```python
from agrobr import bcb, contracts

df, meta = await bcb.ptax(moeda="EUR", boletim="todos", data="04/09/2026", return_meta=True)
contracts.validate_dataset(df, "bcb_ptax")
moedas = await bcb.ptax_moedas()
contracts.validate_dataset(moedas, "bcb_ptax_moedas")
```

See the [API](../api/bcb.en.md#ptax), [source](../sources/bcb.en.md#ptax-currencies-quotes-and-bulletins), and [migration guide](../guides/migracao-2.en.md).
