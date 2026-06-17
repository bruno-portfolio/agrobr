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

# Homonyms — without state returns the first; with state it disambiguates
municipio_para_ibge("Brasília")            # 5300108 (DF)
municipio_para_ibge("Brasília", "MG")      # 3108909 (Brasília de Minas)
```

Data from the [IBGE Localities API](https://servicodados.ibge.gov.br/api/docs/localidades) — free to use.

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

144 variants mapping to 41 canonical crops. Accepts Portuguese, English, with/without accents.

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

# List canonical
listar_culturas()
# ['acucar', 'acucar_cristal', 'acucar_refinado', 'algodao', 'algodao_pluma',
#  'amendoim', 'arroz', 'aveia', 'batata', 'boi', 'cafe', 'cafe_arabica',
#  'cafe_robusta', 'cana', 'cebola', 'centeio', 'cevada', 'etanol_anidro',
#  'etanol_hidratado', 'farelo_soja', 'feijao', 'feijao_1', 'feijao_2',
#  'feijao_3', 'frango_congelado', 'frango_resfriado', 'laranja',
#  'laranja_in_natura', 'laranja_industria', 'leite', 'mandioca', 'milho',
#  'milho_1', 'milho_2', 'milho_3', 'oleo_soja', 'soja', 'sorgo', 'suino',
#  'tomate', 'trigo']

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

validar_uf("SP")               # True
validar_uf("XX")               # False

listar_ufs()                   # ['AC', 'AL', 'AM', ..., 'TO']
listar_regioes()               # ['Centro-Oeste', 'Nordeste', 'Norte', 'Sudeste', 'Sul']
```

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

safra_atual()                    # "2025/26" (if between Jul/2025 and Jun/2026)
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
```

## Encoding

Encoding detection and decoding for HTML/CSV from Brazilian sources (ISO-8859-1, Windows-1252, UTF-8).

```python
from agrobr.normalize import detect_encoding, decode_content, detect_encoding_chain

# Detect encoding of bytes (chardet)
encoding, confidence = detect_encoding(raw_bytes)   # ("iso-8859-1", 0.95)

# Decode with full fallback chain
text, enc = decode_content(raw_bytes)               # (str, "utf-8")

# Fast chain without chardet (UTF-8 → UTF-8-sig → Windows-1252 → ISO-8859-1)
enc = detect_encoding_chain(raw_bytes)              # "windows-1252"
```

`detect_encoding_chain` probes the first 4KB in the order UTF-8, UTF-8-sig, Windows-1252, ISO-8859-1 — with chardet as the final fallback. Used internally by the `alt/` parsers for government CSVs.

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
| `crops` | `normalizar_cultura`, `listar_culturas`, `is_cultura_valida` | 144 variants, 41 canonical |
| `regions` | `normalizar_uf`, `validar_uf`, `uf_para_nome`, `uf_para_regiao`, `uf_para_ibge`, `ibge_para_uf`, `listar_ufs`, `listar_regioes`, `normalizar_municipio`, `normalizar_praca`, `normalizar_bioma` | 27 states, 6 biomes |
| `dates` | `safra_atual`, `normalizar_safra`, `validar_safra`, `safra_para_anos`, `anos_para_safra`, `safra_anterior`, `safra_posterior`, `periodo_safra`, `lista_safras` | Jul-Jun crop years |
| `units` | `converter`, `sacas_para_toneladas`, `toneladas_para_sacas`, `preco_saca_para_tonelada`, `preco_tonelada_para_saca` | sc, ton, bu, @, ha |
| `encoding` | `detect_encoding`, `decode_content`, `detect_encoding_chain` | ISO-8859-1, CP1252, UTF-8 |
| `numeric` | `parse_numeric_br` | BR format (1.234,56) |
