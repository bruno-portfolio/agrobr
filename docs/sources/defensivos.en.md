# Agrofit/MAPA — Agricultural Pesticides

> **License:** CC-BY 4.0 (MAPA Open Data Portal).
> Classification: `livre`

Data on pesticides registered in Brazil via the Agrofit system of the Ministerio
da Agricultura, Pecuaria e Abastecimento (MAPA).

## Overview

| Field | Value |
|-------|-------|
| **Operator** | MAPA — Ministerio da Agricultura |
| **Website** | [dados.agricultura.gov.br](https://dados.agricultura.gov.br) |
| **License** | `livre` — CC-BY 4.0 |
| **Format** | CSV (`;` separator) |
| **Update** | Continuous (24h cache) |
| **Coverage** | ~8K formulated products, ~267K authorizations, ~2.8K technical products |

## Available Data

### Formulated Products

Registered commercial pesticides. Each record is a unique product
identified by `nr_registro`.

**Columns:** `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`,
`classe`, `formulacao`, `classe_toxicologica`, `classe_ambiental`, `organicos`,
`modo_de_acao`

### Use Authorizations

1:N relationship with formulated products — each authorization links a product to a
specific crop and pest.

**Columns:** `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`,
`classe`, `cultura`, `praga`, `praga_nome_comum`, `modalidade_de_emprego`

### Technical Products

Active ingredients before commercial formulation.

**Columns:** `nr_registro`, `marca_comercial`, `ingrediente_ativo`, `titular`,
`classe`, `grupo_quimico`, `nome_cientifico`, `classe_toxicologica`, `classe_ambiental`

## API

```python
from agrobr import defensivos

# All formulated products
df = await defensivos.formulados()

# Filter by active ingredient
df = await defensivos.formulados(ingrediente_ativo="glifosato")

# Organic products only
df = await defensivos.formulados(organicos="SIM")

# Authorizations for soybean
df = await defensivos.autorizacoes(cultura="soja")

# Technical products
df = await defensivos.tecnicos()

# With metadata
df, meta = await defensivos.formulados(return_meta=True)
```

## Technical Notes

- The formulated-products CSV is large (~100MB+). Download timeout: 300s
- The parser fixes Windows-1252 encoding (en-dash `\x96` → UTF-8)
- The composite column `INGREDIENTE_ATIVO(GRUPO_QUIMICO)(CONCENTRACAO)` in technical products
  is split automatically via regex
- Filters use `str.contains()` case-insensitive (except `organicos`, which is exact)
- Local CSV cache with a 24h TTL

## Source

- URL: `https://dados.agricultura.gov.br/dataset/6c913699-e82e-4da3-a0a1-fb6c431e367f`
- Format: CSV (`;`)
- Update: continuous
- License: `livre` — CC-BY 4.0
