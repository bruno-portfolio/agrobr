# CONAB CEASA/PROHORT

Most recent wholesale produce prices across 43 CEASAs in Brazil (48 products).

## `conab.ceasa_precos()`

Most recent wholesale prices per product x CEASA.

```python
import agrobr

df = await agrobr.conab.ceasa_precos(produto="tomate")
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `produto` | `str` | No | Filter by product (e.g. "tomate", "ABACAXI"). Case-insensitive |
| `ceasa` | `str` | No | Filter by CEASA (e.g. "CEAGESP - SAO PAULO", "SAO PAULO"). Case-insensitive, partial match |
| `as_polars` | `bool` | No | If True, returns a `polars.DataFrame` |
| `return_meta` | `bool` | No | If True, returns `(DataFrame, MetaInfo)` |

### Returned Columns

| Column | Type | Description |
|--------|------|-------------|
| `data` | datetime | Price date (per CEASA, extracted from the header) |
| `produto` | str | Product name (e.g. TOMATE, ABACAXI) |
| `categoria` | str | FRUTAS or HORTALICAS |
| `unidade` | str | KG, UN or DZ |
| `ceasa` | str | CEASA name (e.g. CEAGESP - SAO PAULO) |
| `ceasa_uf` | str | CEASA state (e.g. SP) |
| `preco` | float | Price in BRL (nulls filtered out) |

---

## `conab.ceasa_produtos()`

List of the 48 products monitored by PROHORT.

```python
import agrobr

produtos = agrobr.conab.ceasa_produtos()
```

### Returns

Sorted list of strings (e.g. `["ABACATE", "ABACAXI", ..., "VAGEM"]`).

---

## `conab.lista_ceasas()`

List of the 43 CEASAs with their state.

```python
import agrobr

ceasas = agrobr.conab.lista_ceasas()
for c in ceasas[:3]:
    print(c["nome"], c["uf"])
```

### Returns

List of dicts with `nome` and `uf` for each CEASA, sorted by name.

---

## `conab.ceasa_categorias()`

Product categories (FRUTAS, HORTALICAS).

```python
import agrobr

cats = agrobr.conab.ceasa_categorias()
print(f"Frutas: {len(cats['FRUTAS'])}")
print(f"Hortalicas: {len(cats['HORTALICAS'])}")
```

---

## Synchronous Usage

```python
from agrobr import sync

df = sync.conab.ceasa_precos(produto="tomate")
produtos = sync.conab.ceasa_produtos()
ceasas = sync.conab.lista_ceasas()
```

## Examples

### Tomato price in SP

```python
import agrobr

df = await agrobr.conab.ceasa_precos(produto="tomate", ceasa="SAO PAULO")
print(df[["ceasa", "preco", "unidade"]])
```

### All fruits

```python
import agrobr

df = await agrobr.conab.ceasa_precos()
frutas = df[df["categoria"] == "FRUTAS"]
print(frutas.groupby("produto")["preco"].mean().sort_values(ascending=False))
```

### Compare prices across CEASAs

```python
import agrobr

df = await agrobr.conab.ceasa_precos(produto="tomate")
print(df[["ceasa", "ceasa_uf", "preco"]].sort_values("preco"))
```

## Data Source

- **Provider:** CONAB — Companhia Nacional de Abastecimento
- **System:** PROHORT (Programa Brasileiro de Modernizacao do Mercado Hortigranjeiro)
- **Frequency:** Daily (wholesale prices)
- **Coverage:** 48 products (20 fruits, 28 vegetables), 43 CEASAs, 20 states
- **Format:** JSON (Pentaho CDA REST API)
- **License:** zona_cinza (embedded public credentials, API not officially documented)
- **Portal:** [Portal de Informacoes CONAB](https://portaldeinformacoes.conab.gov.br/mercado-atacadista-hortigranjeiro.html)
