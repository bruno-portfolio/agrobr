# Normalization

The `agrobr.normalize` module standardizes Brazilian agricultural data to enable cross-referencing across different sources. It has 39 functions organized into 7 sub-modules.

## IBGE Municipalities

5,571 municipalities with 7-digit IBGE codes. Accent/case-insensitive lookup.

```python
from agrobr.normalize import municipio_para_ibge, ibge_para_municipio, buscar_municipios

# Name to IBGE code
municipio_para_ibge("Rondonópolis")        # 5107602
municipio_para_ibge("RONDONOPOLIS")        # 5107602 (no accent, uppercase)
municipio_para_ibge("rondonopolis", "MT")  # 5107602 (disambiguate by state)

# IBGE code to info
ibge_para_municipio(5107602)
# {'codigo_ibge': 5107602, 'nome': 'Rondonópolis', 'uf': 'MT'}

# Partial search
buscar_municipios("sorriso", uf="MT")
# [{'codigo_ibge': 5107925, 'nome': 'Sorriso', 'uf': 'MT'}]
buscar_municipios("sorriso", uf="XX")  # InvalidParameterError, listing the valid codes
buscar_municipios("santo", limite=-1)  # InvalidParameterError (negative limit)

# Use the full municipality name and the state when available
municipio_para_ibge("Brasília")                 # 5300108 (DF)
municipio_para_ibge("Brasília de Minas", "MG")  # 3108602
```

`ibge_para_municipio`, `buscar_municipios` and `coordenada_para_municipio` return copies: changing the returned
dict does not affect the next query.

Data from the [IBGE Localities API](https://servicodados.ibge.gov.br/api/docs/localidades) — free to use.

## Joining by municipality

Municipal datasets carry the `cod_municipio` column (`Int64`, the 7-digit IBGE code), the same in all of them, so the join
needs no conversion:

```python
from agrobr import datasets

pam = await datasets.producao_anual("soja", ano=2023, nivel="municipio", uf="MT")
zarc = await datasets.zoneamento_agricola(cultura="soja", uf="MT")
joined = pam.merge(zarc, on="cod_municipio")
```

Where a row is not a municipality, `cod_municipio` is null: state and Brazil rows of the IBGE surveys, the CONAB fallback (by
state) and PSR policies without a code. The previous columns stay:

| Dataset | Previous column | Type | `cod_municipio` |
|---|---|---|---|
| `producao_anual`, `pecuaria_municipal`, `extrativismo_vegetal`, `silvicultura`, `censo_agropecuario`, `censo_agropecuario_historico`, `censo_agropecuario_legado` | `localidade_cod` | int, at any level | municipality rows only |
| `cadastro_rural` | `cod_municipio_ibge` | int | the same value |
| `queimadas` | `municipio_id` | int | the same value |
| `desmatamento` (DETER) | `municipio_id` | text | as an integer |
| `seguro_rural` | `cd_ibge` | text | as an integer; null without a code |
| `uso_do_solo` (municipal) | `geocodigo` | text | as an integer; null when the code has no state prefix |
| `zoneamento_agricola` | `geocodigo` | text | as an integer |

`censo_agropecuario_municipal_1985` stays out: 1985 municipalities do not map 1:1 to today's codes, and the name comes as read. `precos_diesel` and `movimentacao_portuaria`
carry only the municipality name; to join them, use `municipio_para_ibge(nome, uf)` (above) and check the names that do not
match.

## Reverse Geocoding

Lookup `(lat, lon) → municipality` via nearest centroid. Zero HTTP, sub-ms. 5,571 municipalities with centroids from the [IBGE Meshes API](https://servicodados.ibge.gov.br/api/v3/malhas/).

```python
from agrobr.normalize import coordenada_para_municipio

# Coordinate to nearest municipality
coordenada_para_municipio(-12.74, -55.68)
# {'codigo_ibge': 5107925, 'nome': 'Sorriso', 'uf': 'MT'}

coordenada_para_municipio(-15.78, -47.93)
# {'codigo_ibge': 5300108, 'nome': 'Brasília', 'uf': 'DF'}

# Ocean / outside Brazil → None (threshold 1.5° ~167km)
coordenada_para_municipio(0, -30)
# None
```

Typical use case — filter SICAR by municipality from a coordinate:

```python
from agrobr.normalize import coordenada_para_municipio
from agrobr.alt import sicar

info = coordenada_para_municipio(lat, lon)
gdf = await sicar.imoveis_geo(info["uf"], municipio=info["nome"])
```

## Crops

158 variants mapping to 43 canonical crops. Accepts Portuguese, English, with/without accents.

```python
from agrobr.normalize import normalizar_cultura, listar_culturas, is_cultura_valida

# Standardization
normalizar_cultura("SOJA")             # "soja"
normalizar_cultura("Soja em Grão")     # "soja"
normalizar_cultura("soybean")          # "soja"
normalizar_cultura("milho 2ª safra")   # "milho_2"
normalizar_cultura("café arábica")     # "cafe_arabica"
normalizar_cultura("boi gordo")        # "boi"
normalizar_cultura("cotton")           # "algodao"
normalizar_cultura("cafe_conillon")    # "cafe_robusta"
normalizar_cultura("castanha do pará") # "castanha_do_brasil"

# List canonical
listar_culturas()
# ['acucar', 'acucar_cristal', 'acucar_refinado', 'algodao', 'algodao_pluma',
#  'amendoim', 'arroz', 'aveia', 'batata', 'bezerro', 'boi', 'cafe',
#  'cafe_arabica', 'cafe_robusta', 'cana', 'castanha_do_brasil', 'cebola',
#  'centeio', 'cevada', 'etanol_anidro', 'etanol_hidratado', 'farelo_soja',
#  'feijao', 'feijao_1', 'feijao_2', 'feijao_3', 'frango_congelado',
#  'frango_resfriado', 'laranja', 'laranja_in_natura', 'laranja_industria',
#  'leite', 'mandioca', 'milho', 'milho_1', 'milho_2', 'milho_3', 'oleo_soja',
#  'soja', 'sorgo', 'suino', 'tomate', 'trigo']

# Validation
is_cultura_valida("soja em grão")  # True
is_cultura_valida("batata doce")   # False
```

## States and Regions

27 states with IBGE code, full name and region. Accepts abbreviation, full name, with/without accents.

```python
from agrobr.normalize import (
    normalizar_uf, validar_uf, uf_para_nome, uf_para_regiao,
    uf_para_ibge, ibge_para_uf, listar_ufs, listar_regioes,
)

normalizar_uf("São Paulo")     # "SP"
normalizar_uf("sp")            # "SP"
normalizar_uf("SAO PAULO")     # "SP"
normalizar_uf("mato grosso")   # "MT"

uf_para_nome("MT")             # "Mato Grosso"
uf_para_regiao("MT")           # "Centro-Oeste"
uf_para_ibge("MT")             # 51
ibge_para_uf(51)               # "MT"
uf_para_nome("XX")             # UnknownNameError, listing the valid codes

validar_uf("SP")               # True
validar_uf("XX")               # False

listar_ufs()                   # ['AC', 'AL', 'AP', 'AM', ..., 'TO']
listar_ufs("Sul")              # ['PR', 'RS', 'SC']
listar_ufs("sul")              # InvalidParameterError, listing the valid regions
listar_regioes()               # ['Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul']
```

`UnknownNameError` is an `InvalidParameterError` and also a `KeyError`: an existing `except KeyError` still catches it.

## Biomes

6 Brazilian biomes. Accepts with/without accents, case-insensitive. Used automatically in `desmatamento`, `queimadas` and `mapbiomas`.

```python
from agrobr.normalize import normalizar_bioma, BIOMAS_VALIDOS

normalizar_bioma("amazonia")        # "Amazônia"
normalizar_bioma("cerrado")         # "Cerrado"
normalizar_bioma("mata atlantica")  # "Mata Atlântica"
normalizar_bioma("  Caatinga  ")    # "Caatinga"
normalizar_bioma("desconhecido")    # "desconhecido" (passthrough)

BIOMAS_VALIDOS
# {'Amazônia', 'Caatinga', 'Cerrado', 'Mata Atlântica', 'Pampa', 'Pantanal'}
```

## Crop Years

Crop-year dates in the Brazilian `YYYY/YY` format. The crop year runs from July to June.

```python
from agrobr.normalize import (
    safra_atual, normalizar_safra, validar_safra,
    safra_para_anos, anos_para_safra, safra_anterior, safra_posterior,
    periodo_safra, lista_safras,
)

safra_atual()                    # "2025/26" (between Jul/2025 and Jun/2026, by the Brasília date)
normalizar_safra("24/25")        # "2024/25"
normalizar_safra("2024/2025")    # "2024/25"
validar_safra("2024/25")         # True

safra_para_anos("2024/25")       # (2024, 2025)
anos_para_safra(2024, 2025)      # "2024/25"
safra_anterior("2024/25")        # "2023/24"
safra_posterior("2024/25")       # "2025/26"

periodo_safra("2024/25")         # (date(2024, 7, 1), date(2025, 6, 30))
lista_safras("2020/21", "2024/25")
# ['2020/21', '2021/22', '2022/23', '2023/24', '2024/25']
lista_safras(safra_inicio="2023/24", safra_fim="2024/25")  # ['2023/24', '2024/25']

normalizar_safra("abc")          # InvalidParameterError, listing the accepted formats
anos_para_safra(2024, 2026)      # InvalidParameterError (a season spans two consecutive years)
lista_safras("2025/26", "2024/25")  # InvalidParameterError (reversed range)
```

## Source dates

Dates that arrive from outside as text go through `dates.converter_coluna`, with the same rule on pandas 2 and 3: IBAMA
(`data_embargo`, `data_desembargo`), Acervo Fundiário (SIGEF, SNCI and settlement dates), CFTC (`data`), INMET (`data` of
the observations), MapBiomas Alerta (`data_deteccao`, `data_publicacao`) and Queimadas (`data_hora_gmt`).

- An unreadable value, or one with a year outside 1900–2099 (`DATA_ANO_MINIMO` and `DATA_ANO_MAXIMO`), becomes `NaT`. For
  IBAMA, so does an act date on a day after the file's own edition (`ULTIMA_ATUALIZACAO_RELATORIO`).
- When it discards any value, the query raises a `UserWarning` and puts the same message in `meta.validation_warnings`, with
  the source, the column and the number of present values that became `NaT` (an empty cell does not count).
- What happens next depends on the source: INMET drops the observation without a date (and the message says so), and CFTC
  rejects the response with `ParseError`.

**Every date column of the public outputs, from sources and datasets, comes out as `datetime64[ns]`**, with the timezone
when the source publishes one, in pandas and in polars (`Datetime("ns")`). Without this, the unit varied by source and by
pandas version (`ns` on 2; `s`, `ms` or `us` on 3), and joining datasets on the date failed in polars. Without the date
rule, a date outside the `datetime64[ns]` range (before 1677 or after 2262) became `NaT` on pandas 2 and came out as
published on pandas 3.

```python
import pandas as pd
from agrobr.normalize import dates

dates.converter_datas(pd.Series(["2024-01-02", "1667-05-31"]), fonte="exemplo")
# DatasConvertidas(datas=[Timestamp('2024-01-02'), NaT] as datetime64[ns], descartadas=1)
```

## Units

Conversion between Brazilian agricultural units: bags, tonnes, bushels, arrobas, hectares.

```python
from agrobr.normalize import (
    converter, sacas_para_toneladas, toneladas_para_sacas,
    preco_saca_para_tonelada, preco_tonelada_para_saca,
)

# Generic conversion
converter(1, "ton", "sc60kg")           # 16.6667 (60kg bags)
converter(100, "sc60kg", "ton")         # 6.0
converter(1, "ton", "bu", produto="soja")  # 36.7437 (bushels)
converter(1, "arroba", "kg")            # 15.0

# Price shortcuts
preco_saca_para_tonelada(145.50)        # 2425.0 (BRL/ton from BRL/sc60kg)
preco_tonelada_para_saca(2425.0)        # 145.5  (BRL/sc60kg from BRL/ton)

# Weight to volume
sacas_para_toneladas(1000)              # 60.0
toneladas_para_sacas(60)                # 1000.0

# Invalid input
converter(1, "galao", "kg")             # InvalidParameterError, listing the mass units
converter(1, "ton", "bu")               # InvalidParameterError: bushel requires a product (milho, soja, trigo)
sacas_para_toneladas(1000, peso_saca_kg=0)  # InvalidParameterError (bag weight must be positive)
```

## Encoding

Encoding detection and decoding for HTML/CSV from Brazilian sources (ISO-8859-1, Windows-1252, UTF-8).

```python
from agrobr.normalize import detect_encoding, decode_content, detect_encoding_chain

# Detect encoding of bytes (chardet)
encoding, confidence = detect_encoding(raw_bytes)   # ("iso-8859-1", 0.95)

# Decode with full fallback chain
text, enc = decode_content(raw_bytes)               # (str, "utf-8")

# Fast chain without chardet (UTF-8-sig → UTF-8 → Windows-1252 → ISO-8859-1)
enc = detect_encoding_chain(raw_bytes)              # "windows-1252"
```

`detect_encoding_chain` returns `utf-8-sig` when there is a BOM and, otherwise, the first of UTF-8, Windows-1252 and ISO-8859-1 that decodes the whole content. ISO-8859-1 decodes any byte and ends the chain. `decode_content` follows the same chain after the declared encoding. Both are used internally by the parsers for government CSVs and HTMLs.

## Brazilian Numbers

Parsing of numeric values in the Brazilian format (dot as thousands, comma as decimal).

```python
from agrobr.normalize import parse_numeric_br

parse_numeric_br("1.234,56")     # 1234.56
parse_numeric_br("1234,56")      # 1234.56
parse_numeric_br("500.000,50")   # 500000.5
parse_numeric_br(42)             # 42.0 (int/float passthrough)
parse_numeric_br("-")            # None (missing-data marker)
parse_numeric_br(None)           # None
parse_numeric_br("abc")          # None (invalid returns None)
```

## Quick Reference

| Sub-module | Functions | Data |
|---|---|---|
| `municipalities` | `municipio_para_ibge`, `ibge_para_municipio`, `buscar_municipios`, `coordenada_para_municipio`, `total_municipios` | 5,571 municipalities + centroids |
| `crops` | `normalizar_cultura`, `listar_culturas`, `is_cultura_valida` | 158 variants, 43 canonical |
| `regions` | `normalizar_uf`, `validar_uf`, `uf_para_nome`, `uf_para_regiao`, `uf_para_ibge`, `ibge_para_uf`, `listar_ufs`, `listar_regioes`, `normalizar_municipio`, `normalizar_praca`, `normalizar_bioma` | 27 states, 6 biomes |
| `dates` | `safra_atual`, `normalizar_safra`, `validar_safra`, `safra_para_anos`, `anos_para_safra`, `safra_anterior`, `safra_posterior`, `periodo_safra`, `lista_safras`, `converter_datas` | Jul-Jun crop years; dates from 1900 to 2099 |
| `units` | `converter`, `sacas_para_toneladas`, `toneladas_para_sacas`, `preco_saca_para_tonelada`, `preco_tonelada_para_saca` | sc, ton, bu, @, ha |
| `encoding` | `detect_encoding`, `decode_content`, `detect_encoding_chain` | ISO-8859-1, CP1252, UTF-8 |
| `numeric` | `parse_numeric_br` | BR format (1.234,56) |
