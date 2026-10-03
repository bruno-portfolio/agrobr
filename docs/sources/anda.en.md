# ANDA — Fertilizers

> ANDA is a private organization, and no reuse license was located for fertilizer-delivery statistics. The classification is zona_cinza. The portal's generic rights reservation was not treated as an NC clause for numerical facts; the absence of a license also does not authorize copying entire reports or a protected database. Record the source, month/year and extraction. The category records the absence of a published license.

Associação Nacional para Difusão de Adubos. Monthly fertilizer deliveries
to the Brazilian market (national total, `uf="BR"`).

## Installation

ANDA requires `pdfplumber` as an optional dependency:

```bash
pip install agrobr[pdf]
```

## API

```python
from agrobr import anda

# Monthly fertilizer deliveries
df = await anda.entregas(ano=2024)

# Monthly aggregation (without the uf column)
df = await anda.entregas(ano=2024, agregacao="mensal")
```

## Columns — `entregas`

| Column | Type | Description |
|---|---|---|
| `ano` | int | Year |
| `mes` | int | Month (1-12) |
| `uf` | str | Always `BR` (national total) |
| `produto_fertilizante` | str | Always `total`; the source does not publish deliveries broken down by formulation |
| `volume_ton` | float | Delivered volume (tonnes) |

## Risk Note

ANDA publishes data in PDF. The layout may change without notice between years.
The available delivery bulletins contain only aggregated fertilizer totals.
Therefore, `produto="total"` is the only accepted value; formulations such as
`ureia`, `map`, or `kcl` raise `InvalidParameterError` before download.

The agrobr parser reads the "Principais Indicadores" layout (aggregated
national data, with months and values sometimes in cells concatenated with
`\n`). No published PDF has a state table, and the parser does not try to
read one. Drastic format changes may require a parser update.

There is no year fallback. If no PDF link matches the requested year, the client
raises `InvalidParameterError` and reports the years available on the site.
For the selected link, the actual year is extracted from the link text or file
name and passed to the parser.

In agrobr-insights, ANDA data is handled with dynamic weighting: when it
looks distorted, its weight in the SCI is automatically reduced.

## MetaInfo

```python
df, meta = await anda.entregas(ano=2024, return_meta=True)
print(meta.source)  # "anda"
print(meta.source_method)  # "httpx+pdfplumber"
print(meta.source_url)  # PDF used, e.g. .../Principais_Indicadores_2026.pdf
print(meta.source_details["pdf"])
# {"url", "rotulo_catalogo" (e.g. "Dados 2026"), "edicao_impressa" (e.g. "Janeiro a Junho"; "Total do Ano"
#  for a closed year), "sha256", "bytes", "pagina_de_recursos"}
```

`source_url` is the concrete PDF picked from the catalog; the printed edition is the label of the cumulative
row of the deliveries section (it tells up to which month the PDF goes). `raw_content_hash` is the PDF SHA-256.

## Source

- URL: `https://anda.org.br/recursos/`
- Format: PDF/Excel
- Update: monthly
- Public catalog: 2016–2026
- License: `zona_cinza` — no reuse license located.


## Publication coverage and validation

The public catalog contains 11 PDFs covering 2016–2026, all with monthly national deliveries (`uf="BR"`). The 2026 bulletin publishes January through June; blank later months are not zero. Since none of them publishes a state breakdown, 2.0.0 removed the `uf` argument from the source and from the `fertilizante` dataset (2.0 migration guide, section 50).

Parser 3 requires the `Fertilizantes Entregues ao Mercado (em toneladas de produto)` section and searches for the year only within it. If that year or section identity is missing, the source raises `ParseError`, and so does the dataset (`"Todas as fontes falharam por layout"`), with the source's reason in `errors`. Production, imports, exports and exchange ratios from the same PDF cannot substitute for deliveries. Published values and contract 2.0 are unchanged.

`ano` must be an integer from 2000 through the current year. `ano` and `mes` use nullable `Int64`; `volume_ton` uses `float64`. Invalid parameters fail before downloading the bulletin.
