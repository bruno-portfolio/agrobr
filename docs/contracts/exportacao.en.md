# exportacao v1.1

Brazilian agricultural exports by product, state and month.

## Sources

| Priority | Source | Description |
|----------|--------|-------------|
| 1 | ComexStat | Official MDIC data by NCM |
| 2 | ABIOVE | National fallback with unit normalization; unavailable with a state filter |

## Products

`soja`, `milho`, `cafe`, `algodao`, `acucar`, `farelo_soja`, `oleo_soja`

## Schema

| Column | Type | Nullable | Unit | Stable |
|--------|------|----------|------|--------|
| `ano` | int | ❌ | - | Yes |
| `mes` | int | ❌ | - | Yes |
| `produto` | str | ❌ | - | Yes |
| `uf` | str | ✅ | - | Yes |
| `kg_liquido` | float | ✅ | kg | Yes |
| `valor_fob_usd` | float | ✅ | USD | Yes |
| `volume_ton` | float | ✅ | t (`kg_liquido` / 1000) | No |

**Primary key:** `[ano, mes, produto, uf]`

The dataset returns only these columns, in this order, from both sources. ABIOVE's `receita_usd_mil`, which is `valor_fob_usd` in thousands, stays in the [source](../api/abiove.md).

**Constraints:** `ano >= 1997`, `mes` between 1 and 12, `kg_liquido >= 0`, `valor_fob_usd >= 0`

## Product semantics

Each product sums every NCM code that makes it up, with the codes in force in each year
("included / not included" table in the [ComexStat API](../api/comexstat.md)):

- `soja`: soybeans, whether or not broken, except seed (`12019000`; `12010090` until 2013).
- `milho`: the whole of heading `1005`.
- `cafe`: not roasted and roasted, decaffeinated or not (`09011`, `09012`); husks and
  substitutes (`09019000`) and soluble coffee (`2101`) are excluded.
- `algodao`: `5201` (not carded or combed) and `5203` (carded or combed); waste (`5202`),
  yarn and fabrics are excluded.
- `acucar`: the whole of heading `1701` (raw cane and beet sugar and refined sugar).
- `farelo_soja`: the whole of heading `2304` (flours and pellets and cake and other residues).
- `oleo_soja`: the whole of heading `1507` (crude, refined and other).

The adapter consolidates each product's codes by `ano`, `mes`, and `uf` before validating
the primary key; the `ncm` column is not part of the dataset output (per-NCM detail is in
`agrobr.comexstat`). `oleo_soja_bruto` remains available in the standalone ComexStat API
as the specific code `15071000`, but it is not part of this dataset's vocabulary. The
ABIOVE fallback uses the generic `farelo` and `oleo` categories, with the same scope as
`farelo_soja` and `oleo_soja` (2025 meal: ABIOVE 23.30 Mt × heading `2304` 23.27 Mt),
and returns the dataset's canonical product name.

## Guarantees

- Column names never change (additions only)
- `ano` is always >= 1997
- `mes` between 1 and 12
- Numeric values are always >= 0

## Fallback behavior

- When `ano` is omitted, the latest complete calendar year is used.
- ABIOVE publishes national totals only. When `uf` is provided, the fallback
  does not replace the requested state breakdown with those totals; if
  ComexStat is unavailable, the call raises `SourceUnavailableError`.
- The adapter maps `soja`, `farelo_soja`, `oleo_soja`, and `milho` to ABIOVE's
  product names and restores the canonical name in the output. The `uf` column
  is null for these national totals.
- ABIOVE does not provide `cafe`, `algodao`, or `acucar`. For these products, a
  ComexStat failure ends the cascade without downloading the fallback file.
- The license changes on fallback: ComexStat is `livre`, and ABIOVE is `zona_cinza`. `meta.license` gives
  the one of the delivered data, and `datasets.info("exportacao")["licenses"]` lists both.

## Example

```python
from agrobr import datasets

# Async
df = await datasets.exportacao("soja", ano=2024)
df = await datasets.exportacao("soja", ano=2024, uf="MT")

# With metadata
df, meta = await datasets.exportacao("soja", ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.exportacao("soja", ano=2024)
```

## JSON Schema

Available at `agrobr/schemas/exportacao.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("exportacao")
print(contract.to_json())
```

Output flags are keyword-only. `kg_liquido` and monetary values use `float64`; text uses the installed pandas default in populated and empty outputs.
