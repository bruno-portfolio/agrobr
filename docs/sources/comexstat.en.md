# ComexStat — Exports and Imports

Foreign trade data from MDIC/SECEX. Exports and imports
by product (NCM), state and country.

## API

```python
from agrobr import comexstat

# Monthly soybean exports in 2024
df = await comexstat.exportacao("soja", ano=2024, agregacao="mensal")

# Monthly soybean imports in 2024
df = await comexstat.importacao("soja", ano=2024, agregacao="mensal")

# Detailed exports (by country/state/transport mode)
df = await comexstat.exportacao("soja", ano=2024, agregacao="detalhado")

# Filter by state
df = await comexstat.exportacao("soja", ano=2024, uf="MT")
```

## Columns — `exportacao` / `importacao` (monthly)

| Column | Type | Description |
|---|---|---|
| `ano` | int | Year |
| `mes` | int | Month (1-12) |
| `ncm` | str | NCM code (8 digits) |
| `uf` (exports) | str | State where the goods were produced, regardless of the exporter's seat ([MDIC FAQ 10](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/perguntas-frequentes-faq/12-por-que-a)) |
| `uf` (imports) | str | State of the importer's tax domicile, not the goods' destination in the country ([MDIC FAQ 10](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/perguntas-frequentes-faq/12-por-que-a)) |
| `kg_liquido` | float | Net weight (kg) |
| `valor_fob_usd` | float | FOB value (USD) |
| `volume_ton` | float | Volume in tonnes |
| `valor_frete_usd` (imports only) | float | Freight (USD) |
| `valor_seguro_usd` (imports only) | float | Insurance (USD) |

## Products

`produto` accepts an alias or an NCM prefix of 2 to 8 digits. The alias table, with
what each alias includes and excludes, is in the [ComexStat API](../api/comexstat.md).

> **Note:** the filter uses `str.startswith` with the alias prefixes (an alias may have
> several, and `defensivos`/`agrotoxicos` exclude the codes put up exclusively for
> household sanitation use). Each alias sums the codes in force in each year: when the
> nomenclature splits or renumbers a code (ethanol, soybeans, wheat, sugar, DAP,
> chicken), the series has no gap, including the transition year. `ssp` and `tsp`
> only have an equivalent code since 2017 and reject earlier years.

The standalone API preserves one row per NCM code. The `exportacao` and `importacao`
datasets consolidate each product's codes by year, month, and state; `oleo_soja_bruto`
remains limited to code `15071000`.

## MetaInfo

```python
df, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)
print(meta.source)  # "comexstat"
```

## Technical notes

- The site `balanca.mdic.gov.br` does not send the complete certificate chain. The client verifies
  TLS in full (hostname included), with SERPRO's intermediate certificate checked by SHA-256 and added
  to the authorities (`certifi`, `SSL_CERT_FILE` or `SSL_CERT_DIR`).
- No cache: every call downloads the flow's annual CSV (~100 MB) and filters it in memory, and several
  queries for the same year download the file again. `produto` takes 1 alias or 1 NCM prefix per call,
  not a list; for several codes in a single download, use the common prefix (the standalone API returns
  1 row per NCM).
- Each annual CSV is ~100 MB. The download goes to a temporary file and is checked against the GET's
  `Content-Length` or, without it, the HEAD of the same file; without either, the result warns in
  `validation_warnings` ("tamanho do arquivo não conferido").
- The whole download, retries included, has a 300 s ceiling; past it, `SourceUnavailableError`
  ("TimeoutError"). `AGROBR_HTTP_TIMEOUT_READ` does not change that ceiling; on a slow network, raise
  `AGROBR_HTTP_TIMEOUT_DOWNLOAD_COMEXSTAT` ([environment variables](../advanced/ambiente.md)).

## Source

- Bulk CSV: `https://balanca.mdic.gov.br/balanca/bd/comexstat-bd/ncm`
- Update: weekly/monthly
- History: 1997+

`kg_liquido`, monetary values and `volume_ton` use `float64`; years, months and statistical quantities use `Int64`. Source and dictionary text deliberately retain `string[python]`: the memory guard counts pooled Python strings. This exception applies to populated and empty results. The `exportacao` and `importacao` datasets convert final selected text columns to the installed pandas default. Output flags are keyword-only.
