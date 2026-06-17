# RNC/CultivarWeb — Registro Nacional de Cultivares

> **License:** Brazilian federal government public data (Lei 12.527/2011).
> Classification: `livre`

Data on registered and protected cultivars in Brazil via the CultivarWeb system
of the Ministerio da Agricultura, Pecuaria e Abastecimento (MAPA).

## Overview

| Field | Value |
|-------|-------|
| **Operator** | MAPA — Ministerio da Agricultura |
| **Website** | [sistemas.agricultura.gov.br/snpc/cultivarweb](https://sistemas.agricultura.gov.br/snpc/cultivarweb) |
| **License** | `livre` — Brazilian federal government public data |
| **Format** | CSV (comma, UTF-8) |
| **Update** | Continuous |
| **Coverage** | ~37K registered cultivars, ~5K protected cultivars |

## Available Data

### Registered Cultivars

Cultivars with an active or closed registration in the RNC/MAPA.

**Columns:** `cultivar`, `nome_comum`, `nome_cientifico`, `grupo`, `situacao`,
`nr_formulario`, `nr_registro`, `data_registro`, `data_validade`, `mantenedor`

### Protected Cultivars

Cultivars with intellectual property protection (SNPC).

**Columns:** `cultivar`, `nome_cientifico`, `nome_comum`, `nr_processo`, `situacao`,
`nr_certificado`, `inicio_protecao`, `termino_protecao`, `titular`,
`representante_legal`, `melhoristas`

## API

```python
import asyncio
from agrobr import rnc

async def main():
    # All registered cultivars
    df = await rnc.registradas()

    # Filter by species
    df = await rnc.registradas(especie="soja")

    # Filter by maintainer
    df = await rnc.registradas(mantenedor="Embrapa")

    # Protected cultivars
    df = await rnc.protegidas()

    # Filter by holder
    df = await rnc.protegidas(titular="Embrapa")

    # With metadata
    df, meta = await rnc.registradas(return_meta=True)

    # Polars
    df = await rnc.registradas(as_polars=True)

asyncio.run(main())
```

## Technical Notes

- Access via 2 POSTs with an HTTP session (empty search + CSV export)
- User-Agent required (the server rejects requests without the header)
- Dates in DD/MM/YYYY format (the parser converts them to datetime)
- Filters applied post-download via `str.contains()` case-insensitive
- Local CSV cache with a 24h TTL

## Source

- URL: `https://sistemas.agricultura.gov.br/snpc/cultivarweb`
- Format: CSV (`,`)
- Update: continuous
- License: `livre` — Brazilian federal government public data (Lei 12.527/2011)
