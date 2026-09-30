# Normalização

O módulo `agrobr.normalize` padroniza dados agrícolas brasileiros para garantir cruzamento entre fontes diferentes. São 40 funções organizadas em 7 sub-módulos.

## Municípios IBGE

5571 municípios com código IBGE de 7 dígitos. Busca accent/case insensitive.

```python
from agrobr.normalize import (
    buscar_municipios,
    ibge_para_municipio,
    municipio_para_ibge,
    resolver_municipio,
)

# Nome para código IBGE
municipio_para_ibge("Rondonópolis")        # 5107602
municipio_para_ibge("RONDONOPOLIS")        # 5107602 (sem acento, uppercase)
municipio_para_ibge("rondonopolis", "MT")  # 5107602 (desambiguar por UF)

# Código IBGE para info
ibge_para_municipio(5107602)
# {'codigo_ibge': 5107602, 'nome': 'Rondonópolis', 'uf': 'MT'}

# Busca parcial
buscar_municipios("sorriso", uf="MT")
# [{'codigo_ibge': 5107925, 'nome': 'Sorriso', 'uf': 'MT'}]
buscar_municipios("sorriso", uf="XX")  # InvalidParameterError, com as siglas válidas
buscar_municipios("santo", limite=-1)  # InvalidParameterError (limite negativo)

# Use o nome completo do município e a UF quando disponível
municipio_para_ibge("Brasília")                 # 5300108 (DF)
municipio_para_ibge("Brasília de Minas", "MG")  # 3108602

# Nome inteiro ou código IBGE, com erro em vez de palpite
resolver_municipio("sorriso")           # {'codigo_ibge': 5107925, 'nome': 'Sorriso', 'uf': 'MT'}
resolver_municipio("5107925")           # o mesmo (código em int ou str)
resolver_municipio("Bom Jesus", "PI")   # {'codigo_ibge': 2201903, 'nome': 'Bom Jesus', 'uf': 'PI'}
resolver_municipio("Bom Jesus")         # InvalidParameterError: ambíguo, lista os 5 e pede a uf
resolver_municipio("Santa Rita", "MG")  # InvalidParameterError: não encontrado, lista os candidatos
```

`resolver_municipio` é a regra do parâmetro `municipio` das funções que filtram por município: o nome casa
por inteiro, sem caixa, acento e espaços repetidos, e nunca por pedaço (`"Santa Rita"` não é `"Santa Rita do
Sapucaí"`). Nome de mais de um município, nome inexistente, código fora do cadastro ou município de outra UF
levantam `InvalidParameterError` com os candidatos.

`ibge_para_municipio`, `buscar_municipios`, `coordenada_para_municipio` e `resolver_municipio` devolvem cópias: alterar o dicionário
devolvido não muda a consulta seguinte.

Dados da [API IBGE Localidades](https://servicodados.ibge.gov.br/api/docs/localidades) — livre para uso.

## Cruzar por município

Os datasets municipais trazem a coluna `cod_municipio` (`Int64`, o código IBGE de 7 dígitos), igual em todos, e o join sai
sem conversão:

```python
from agrobr import datasets

pam = await datasets.producao_anual("soja", ano=2023, nivel="municipio", uf="MT")
zarc = await datasets.zoneamento_agricola(produto="soja", uf="MT")
cruzado = pam.merge(zarc, on="cod_municipio")
```

Onde a linha não é de município, `cod_municipio` é nulo: UF e Brasil das pesquisas do IBGE, o fallback da CONAB (por UF) e a
apólice do PSR sem código. As colunas de antes ficam:

| Dataset | Coluna de antes | Tipo | `cod_municipio` |
|---|---|---|---|
| `producao_anual`, `pecuaria_municipal`, `extrativismo_vegetal`, `silvicultura`, `censo_agropecuario`, `censo_agropecuario_historico`, `censo_agropecuario_legado` | `localidade_cod` | int, em qualquer nível | só nas linhas de município |
| `cadastro_rural` | `cod_municipio_ibge` | int | o mesmo valor |
| `queimadas` | `municipio_id` | int | o mesmo valor |
| `desmatamento` (DETER) | `municipio_id` | texto | em inteiro |
| `seguro_rural` | `cd_ibge` | texto | em inteiro; nulo sem código |
| `uso_do_solo` (municipal) | `geocodigo` | texto | em inteiro; nulo quando o código não tem o prefixo de uma UF |
| `zoneamento_agricola` | `geocodigo` | texto | em inteiro |

O `censo_agropecuario_municipal_1985` fica fora: os municípios de 1985 não correspondem 1:1 aos códigos atuais, e o nome vem como lido. `precos_diesel` e
`movimentacao_portuaria` trazem só o nome do município; para cruzar, use `municipio_para_ibge(nome, uf)` (acima) e confira os
nomes que não casarem.

## Geocodificação Reversa

Lookup `(lat, lon) → município` via centroide mais próximo. Zero HTTP, sub-ms. 5571 municípios com centroides da [API IBGE Malhas](https://servicodados.ibge.gov.br/api/v3/malhas/).

```python
from agrobr.normalize import coordenada_para_municipio

# Coordenada para município mais próximo
coordenada_para_municipio(-12.74, -55.68)
# {'codigo_ibge': 5107925, 'nome': 'Sorriso', 'uf': 'MT'}

coordenada_para_municipio(-15.78, -47.93)
# {'codigo_ibge': 5300108, 'nome': 'Brasília', 'uf': 'DF'}

# Oceano / fora do Brasil → None (threshold 1.5° ~167km)
coordenada_para_municipio(0, -30)
# None
```

Caso de uso típico — filtrar SICAR por município a partir de coordenada:

```python
from agrobr.normalize import coordenada_para_municipio
from agrobr.alt import sicar

info = coordenada_para_municipio(lat, lon)
gdf = await sicar.imoveis_geo(info["uf"], municipio=info["nome"])
```

## Culturas

158 variantes mapeando para 43 culturas canônicas. Aceita português, inglês, com/sem acento.

```python
from agrobr.normalize import normalizar_cultura, listar_culturas, is_cultura_valida

# Padronização
normalizar_cultura("SOJA")             # "soja"
normalizar_cultura("Soja em Grão")     # "soja"
normalizar_cultura("soybean")          # "soja"
normalizar_cultura("milho 2ª safra")   # "milho_2"
normalizar_cultura("café arábica")     # "cafe_arabica"
normalizar_cultura("boi gordo")        # "boi"
normalizar_cultura("cotton")           # "algodao"
normalizar_cultura("cafe_conillon")    # "cafe_robusta"
normalizar_cultura("castanha do pará") # "castanha_do_brasil"

# Listar canônicas
listar_culturas()
# ['acucar', 'acucar_cristal', 'acucar_refinado', 'algodao', 'algodao_pluma',
#  'amendoim', 'arroz', 'aveia', 'batata', 'bezerro', 'boi', 'cafe',
#  'cafe_arabica', 'cafe_robusta', 'cana', 'castanha_do_brasil', 'cebola',
#  'centeio', 'cevada', 'etanol_anidro', 'etanol_hidratado', 'farelo_soja',
#  'feijao', 'feijao_1', 'feijao_2', 'feijao_3', 'frango_congelado',
#  'frango_resfriado', 'laranja', 'laranja_in_natura', 'laranja_industria',
#  'leite', 'mandioca', 'milho', 'milho_1', 'milho_2', 'milho_3', 'oleo_soja',
#  'soja', 'sorgo', 'suino', 'tomate', 'trigo']

# Validação
is_cultura_valida("soja em grão")  # True
is_cultura_valida("batata doce")   # False
```

## UFs e Regiões

27 UFs com código IBGE, nome completo e região. Aceita sigla, nome completo, com/sem acento.

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
uf_para_nome("XX")             # UnknownNameError, com as siglas válidas

validar_uf("SP")               # True
validar_uf("XX")               # False

listar_ufs()                   # ['AC', 'AL', 'AP', 'AM', ..., 'TO']
listar_ufs("Sul")              # ['PR', 'RS', 'SC']
listar_ufs("sul")              # InvalidParameterError, com as regiões válidas
listar_regioes()               # ['Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul']
```

`UnknownNameError` é `InvalidParameterError` e também `KeyError`: o `except KeyError` de antes segue pegando.

## Biomas

6 biomas brasileiros. Aceita com/sem acento, case insensitive. Usada automaticamente em `desmatamento`, `queimadas` e `mapbiomas`.

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

## Safras

Datas de safra agrícola no formato brasileiro `YYYY/YY`. A safra agrícola vai de julho a junho.

```python
from agrobr.normalize import (
    safra_atual, normalizar_safra, validar_safra,
    safra_para_anos, anos_para_safra, safra_anterior, safra_posterior,
    periodo_safra, lista_safras,
)

safra_atual()                    # "2025/26" (entre jul/2025 e jun/2026, pela data de Brasília)
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

normalizar_safra("abc")          # InvalidParameterError, com os formatos aceitos
anos_para_safra(2024, 2026)      # InvalidParameterError (safra cobre dois anos consecutivos)
lista_safras("2025/26", "2024/25")  # InvalidParameterError (intervalo invertido)
```

## Datas das fontes

As datas que chegam de fora como texto passam por `dates.converter_coluna`, com a mesma regra no pandas 2 e no 3: IBAMA
(`data_embargo`, `data_desembargo`), Acervo Fundiário (datas do SIGEF, do SNCI e dos assentamentos), CFTC (`data`), INMET
(`data` das observações), MapBiomas Alerta (`data_deteccao`, `data_publicacao`) e Queimadas (`data_hora_gmt`).

- Valor ilegível, ou com ano fora de 1900–2099 (`DATA_ANO_MINIMO` e `DATA_ANO_MAXIMO`), vira `NaT`. No IBAMA, também a data
  de ato de dia posterior à edição do próprio arquivo (`ULTIMA_ATUALIZACAO_RELATORIO`).
- Quando descarta algum valor, a consulta emite `UserWarning` e põe a mesma mensagem em `meta.validation_warnings`, com a
  fonte, a coluna e a quantidade de valores presentes que viraram `NaT` (célula vazia não conta).
- O que vem depois é de cada fonte: o INMET descarta a observação sem data (e a mensagem diz isso), e o CFTC recusa a resposta
  com `ParseError`.

**Toda coluna de data das saídas públicas, de fontes e datasets, sai em `datetime64[ns]`**, com o fuso quando a fonte publica
um, em pandas e em polars (`Datetime("ns")`). Sem isso, a unidade variava por fonte e por versão do pandas (`ns` no 2; `s`,
`ms` ou `us` no 3), e o join entre datasets pela data falhava no polars. Sem a regra de datas, a data fora do intervalo do
`datetime64[ns]` (antes de 1677 ou depois de 2262) virava `NaT` no pandas 2 e saía como publicada no pandas 3.

```python
import pandas as pd
from agrobr.normalize import dates

dates.converter_datas(pd.Series(["2024-01-02", "1667-05-31"]), fonte="exemplo")
# DatasConvertidas(datas=[Timestamp('2024-01-02'), NaT] em datetime64[ns], descartadas=1)
```

## Unidades

Conversão entre unidades agrícolas brasileiras: sacas, toneladas, bushels, arrobas, hectares.

```python
from agrobr.normalize import (
    converter, sacas_para_toneladas, toneladas_para_sacas,
    preco_saca_para_tonelada, preco_tonelada_para_saca,
)

# Conversão genérica
converter(1, "ton", "sc60kg")           # 16.6667 (sacas de 60kg)
converter(100, "sc60kg", "ton")         # 6.0
converter(1, "ton", "bu", produto="soja")  # 36.7437 (bushels)
converter(1, "arroba", "kg")            # 15.0

# Atalhos para preços
preco_saca_para_tonelada(145.50)        # 2425.0 (R$/ton a partir de R$/sc60kg)
preco_tonelada_para_saca(2425.0)        # 145.5  (R$/sc60kg a partir de R$/ton)

# Peso para volume
sacas_para_toneladas(1000)              # 60.0
toneladas_para_sacas(60)                # 1000.0

# Entrada inválida
converter(1, "galao", "kg")             # InvalidParameterError, com as unidades de massa
converter(1, "ton", "bu")               # InvalidParameterError: bushel exige produto (milho, soja, trigo)
sacas_para_toneladas(1000, peso_saca_kg=0)  # InvalidParameterError (peso da saca deve ser positivo)
```

## Encoding

Detecção e decodificação de encoding para HTML/CSV de fontes brasileiras (ISO-8859-1, Windows-1252, UTF-8).

```python
from agrobr.normalize import detect_encoding, decode_content, detect_encoding_chain

# Detectar encoding de bytes (chardet)
encoding, confidence = detect_encoding(raw_bytes)   # ("iso-8859-1", 0.95)

# Decodificar com fallback chain completa
text, enc = decode_content(raw_bytes)               # (str, "utf-8")

# Chain rápida sem chardet (UTF-8-sig → UTF-8 → Windows-1252 → ISO-8859-1)
enc = detect_encoding_chain(raw_bytes)              # "windows-1252"
```

`detect_encoding_chain` devolve `utf-8-sig` quando há BOM e, senão, o primeiro de UTF-8, Windows-1252 e ISO-8859-1 que decodifica o conteúdo inteiro. O ISO-8859-1 decodifica qualquer byte e encerra a chain. `decode_content` segue a mesma chain depois do encoding declarado. Usadas internamente pelos parsers para CSVs e HTMLs de governo.

## Numérico BR

Parsing de valores numéricos no formato brasileiro (ponto como milhar, vírgula como decimal).

```python
from agrobr.normalize import parse_numeric_br

parse_numeric_br("1.234,56")     # 1234.56
parse_numeric_br("1234,56")      # 1234.56
parse_numeric_br("500.000,50")   # 500000.5
parse_numeric_br(42)             # 42.0 (passthrough int/float)
parse_numeric_br("-")            # None (marcador de dado ausente)
parse_numeric_br(None)           # None
parse_numeric_br("abc")          # None (inválido retorna None)
```

## Referência Rápida

| Sub-módulo | Funções | Dados |
|---|---|---|
| `municipalities` | `municipio_para_ibge`, `ibge_para_municipio`, `buscar_municipios`, `coordenada_para_municipio`, `total_municipios` | 5571 municípios + centroides |
| `crops` | `normalizar_cultura`, `listar_culturas`, `is_cultura_valida` | 158 variantes, 43 canônicas |
| `regions` | `normalizar_uf`, `validar_uf`, `uf_para_nome`, `uf_para_regiao`, `uf_para_ibge`, `ibge_para_uf`, `listar_ufs`, `listar_regioes`, `normalizar_municipio`, `normalizar_praca`, `normalizar_bioma` | 27 UFs, 6 biomas |
| `dates` | `safra_atual`, `normalizar_safra`, `validar_safra`, `safra_para_anos`, `anos_para_safra`, `safra_anterior`, `safra_posterior`, `periodo_safra`, `lista_safras`, `converter_datas` | Safras Jul-Jun; datas de 1900 a 2099 |
| `units` | `converter`, `sacas_para_toneladas`, `toneladas_para_sacas`, `preco_saca_para_tonelada`, `preco_tonelada_para_saca` | sc, ton, bu, @, ha |
| `encoding` | `detect_encoding`, `decode_content`, `detect_encoding_chain` | ISO-8859-1, CP1252, UTF-8 |
| `numeric` | `parse_numeric_br` | Formato BR (1.234,56) |
