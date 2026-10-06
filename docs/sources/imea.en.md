# IMEA — MT Quotes and Indicators

> IMEA's public series are classified as `zona_cinza`: no reuse license was verified, nor was the non-public-file clause shown to cover this scope. The terms require prior written authorization to share non-public files; that restriction remains for those files. Reservations concerning databases and other assets do not constitute an open license. The module warns on the first call. [IMEA Terms of Use](https://imea.com.br/imea-site/termo-de-uso.html).

!!! warning "License of the public scope"
    First use emits a `UserWarning` for the `zona_cinza` classification. The express sharing restriction still applies to non-public files.

Instituto Mato-Grossense de Economia Agropecuária.
Daily quotes, price indicators and crop-year data for Mato Grosso.

## API

```python
from agrobr import imea

# Soybean quotes in MT
df = await imea.cotacoes("soja")

# Filter by crop year
df = await imea.cotacoes("soja", safra="24/25")

# Filter by unit (see "Unit and indicator" below)
df = await imea.cotacoes("soja", unidade="R$/sc")

# Other production chains
df = await imea.cotacoes("milho")
df = await imea.cotacoes("algodao")
df = await imea.cotacoes("bovinocultura")
df = await imea.cotacoes("custo_producao")
```

An unknown argument (e.g. `municipio=`) raises `TypeError` before any request.

## Columns — `cotacoes`

| Column | Type | Description |
|---|---|---|
| `cadeia` | str | Requested chain (see "Grain freight" below) |
| `indicador_id` | str | Indicator identifier at the source (`IndicadorFinalId`, text as published; some ids have 18 digits) |
| `indicador` | str | Official indicator name (e.g. "Preço soja disponível compra"); null if the id is not in the chain's catalog |
| `localidade` | str | Market, macroregion or category, depending on the indicator (in the economy chain, "Algodão", "Total MT"; for soybean seed, "Convencional"/"Transgênica") |
| `valor` | float | Published value (null when the source publishes none) |
| `variacao` | float | Change (%) |
| `safra` | str | Crop year (e.g. "24/25"; null for indicators without a crop year) |
| `unidade` | str | Unit (R$/sc, R$/t, R$/ha, %...) |
| `unidade_descricao` | str | Unit description |
| `data_publicacao` | datetime64[ns] | Publication date and time (null in some records) |

A row is identified by `indicador_id` + `localidade` + `data_publicacao` + `safra` + `unidade`. Without the indicator,
thousands of rows of the same chain repeat the other four columns (e.g. soybeans, 4,140 of 4,568 on September 23, 2026).

**Record published more than once.** The source sometimes publishes the same record more than once: on September 25,
2026, 23 records of indicator `708192508838936580` (R$/sc, Mato Grosso and 22 municipalities) came out four times each.

- A record identical in every column is returned once, with a warning, and the count goes to
  `source_details["duplicatas_colapsadas"]` (`linhas` and `indicadores`).
- A key repeated with different values is not collapsed: all its rows are returned, with a warning and the count in
  `source_details["chaves_repetidas"]`. An error would drop the whole chain because of one indicator.
- Both warnings go to `meta.validation_warnings` on every call; the `UserWarning` is emitted only on the first call for each chain in the process.

**Conflicting catalog names.** If the catalog repeats an `Id` with different names, the request raises `ParseError`: the correct indicator name cannot be determined. Repetitions with the same name remain accepted.

### Unit and indicator

`unidade` alone does not identify the product. For soybeans, `unidade="R$/sc"` returns grain (60 kg bag, e.g. "Preço soja
disponível compra" in Sorriso) **and seed** ("SEMENTE SOJA BRANCA (R$/sc 40kg)", locality "Convencional" or
"Transgênica"). Split them by `indicador`:

```python
df = await imea.cotacoes("soja", unidade="R$/sc")
grain = df[~df["indicador"].str.contains("semente", case=False, na=False)]
```

### Grain freight

Freight ("Preço disponível do Frete de Grãos", R$/t) is published in both the soybean and corn chains; each query returns
it with the requested chain. When combining soybeans and corn, deduplicate by `indicador_id` + `data_publicacao` +
`localidade`.

## Production Chains

| agrobr name | IMEA chain (id) |
|---|---|
| `soja` / `soybeans` | Soja (4) |
| `milho` / `corn` | Milho (3) |
| `algodao` / `cotton` | Algodão (1) |
| `bovinocultura` / `boi` / `boi_gordo` / `bovinos` / `cattle` | Bovinocultura de Corte (2) |
| `suinocultura` / `pork` | Suinocultura (7) |
| `leite` / `dairy` | Leite (8) |
| `conjuntura` | Conjuntura Econômica (5): gross production value, food basket and retail price indices (Cuiabá) |
| `custo_producao` | Custo de Produção (10): input prices (seed, seed treatment) |

The chain number is also accepted (`"5"`, `"10"`). Chains inactive at the source (6 Madeira Nativa, 9 Aves,
11 Geoprocessamento), an unknown name or a non-text value raise `InvalidParameterError`, with the
options, before any request.

## MetaInfo

```python
df, meta = await imea.cotacoes("soja", return_meta=True)
print(meta.source)  # "imea"
print(meta.source_url)  # .../v2/mobile/cadeias/4/cotacoes
print(meta.source_details["indicadores_url"])  # .../v2/mobile/cadeias/4/indicadores
print(meta.source_details["indicadores_sha256"])  # SHA-256 of the indicator catalog
print(meta.source_details["indicadores_bytes"])  # catalog size in bytes
print(meta.source_details["duplicatas_colapsadas"])  # {"linhas": 0, "indicadores": []}
print(meta.source_details["chaves_repetidas"])  # {"linhas": 0, "indicadores": []}
```

## Source

- API: `https://api1.imea.com.br/api/v2/mobile/cadeias/{id}/cotacoes` and, for indicator names,
  `.../cadeias/{id}/indicadores` (two requests per query)
- Format: JSON (REST API); a missing published key raises `ParseError`
- Update: daily
- Coverage: Mato Grosso
- Authentication: none (public API)
- License: `zona_cinza` for public series; non-public files require written authorization.
