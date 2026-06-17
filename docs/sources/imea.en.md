# IMEA — MT Quotes and Indicators

> **License:** IMEA terms of use prohibit redistribution of data without
> written authorization. Personal/educational use only.
> Ref: [Terms of Use](https://imea.com.br/imea-site/termo-de-uso.html)
> Classification: `restrito`

!!! warning "Redistribution restriction"
    IMEA explicitly prohibits sharing data without written
    authorization. A `warnings.warn()` is emitted on first use of the module.
    Do not redistribute data obtained via this module without authorization from IMEA.

Instituto Mato-Grossense de Economia Agropecuária.
Daily quotes, price indicators and crop-year data for Mato Grosso.

## API

```python
from agrobr import imea

# Soybean quotes in MT
df = await imea.cotacoes("soja")

# Filter by crop year
df = await imea.cotacoes("soja", safra="24/25")

# Filter by unit
df = await imea.cotacoes("soja", unidade="R$/sc")

# Other production chains
df = await imea.cotacoes("milho")
df = await imea.cotacoes("algodao")
df = await imea.cotacoes("bovinocultura")
```

## Columns — `cotacoes`

| Column | Type | Description |
|---|---|---|
| `cadeia` | str | Production chain |
| `localidade` | str | MT macroregion/locality |
| `valor` | float | Quote value |
| `variacao` | float | Change (%) |
| `safra` | str | Crop year (e.g.: "24/25") |
| `unidade` | str | Unit (R$/sc, R$/t, %) |
| `unidade_descricao` | str | Unit description |
| `data_publicacao` | str | Publication date |

## Production Chains

| agrobr name | IMEA chain |
|---|---|
| `soja` | Soja |
| `milho` | Milho |
| `algodao` | Algodão |
| `bovinocultura` / `boi` / `boi_gordo` / `bovinos` | Bovinocultura |
| `suinocultura` | Suinocultura |
| `leite` | Leite |

## MetaInfo

```python
df, meta = await imea.cotacoes("soja", return_meta=True)
print(meta.source)  # "imea"
print(meta.source_method)  # "httpx"
```

## Source

- API: `https://api1.imea.com.br/api/v2/mobile/cadeias`
- Format: JSON (REST API)
- Update: daily
- Coverage: Mato Grosso
- Authentication: none (public API)
- License: `restrito` — redistribution requires written authorization from IMEA
