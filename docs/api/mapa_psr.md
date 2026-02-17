# API MAPA PSR

O modulo MAPA PSR fornece dados de apolices e sinistros do seguro rural brasileiro com subvencao federal, publicados pelo SISSER/MAPA. Namespace: `agrobr.alt.mapa_psr`.

## Funcoes

### `sinistros`

Sinistros de seguro rural — indenizacoes pagas por cultura/municipio.

```python
async def sinistros(
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: str | None = None,
    evento: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `cultura` | `str \| None` | Filtro por cultura (busca parcial, ex: "SOJA") |
| `uf` | `str \| None` | Filtro por UF (sigla, ex: "MT") |
| `ano` | `int \| None` | Filtro de ano unico (ex: 2023) |
| `ano_inicio` | `int \| None` | Ano inicial do range (inclusive) |
| `ano_fim` | `int \| None` | Ano final do range (inclusive) |
| `municipio` | `str \| None` | Filtro por municipio (busca parcial) |
| `evento` | `str \| None` | Filtro por evento preponderante (ex: "seca") |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `nr_apolice`, `ano_apolice`, `uf`, `municipio`, `cd_ibge`,
`cultura`, `classificacao`, `evento`, `area_total`, `valor_indenizacao`, `valor_premio`,
`valor_subvencao`, `valor_limite_garantia`, `produtividade_estimada`,
`produtividade_segurada`, `nivel_cobertura`, `seguradora`

**Exemplo:**

```python
from agrobr.alt import mapa_psr

# Todos os sinistros
df = await mapa_psr.sinistros()

# Sinistros de soja em MT
df = await mapa_psr.sinistros(cultura="SOJA", uf="MT")

# Sinistros por seca em 2023
df = await mapa_psr.sinistros(evento="seca", ano=2023)

# Range de anos
df = await mapa_psr.sinistros(ano_inicio=2020, ano_fim=2024)
```

### `apolices`

Todas as apolices de seguro rural com subvencao federal.

```python
async def apolices(
    cultura: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `cultura` | `str \| None` | Filtro por cultura (busca parcial, ex: "SOJA") |
| `uf` | `str \| None` | Filtro por UF (sigla, ex: "MT") |
| `ano` | `int \| None` | Filtro de ano unico (ex: 2023) |
| `ano_inicio` | `int \| None` | Ano inicial do range (inclusive) |
| `ano_fim` | `int \| None` | Ano final do range (inclusive) |
| `municipio` | `str \| None` | Filtro por municipio (busca parcial) |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `nr_apolice`, `ano_apolice`, `uf`, `municipio`, `cd_ibge`,
`cultura`, `classificacao`, `area_total`, `valor_premio`, `valor_subvencao`,
`valor_limite_garantia`, `valor_indenizacao`, `evento`, `produtividade_estimada`,
`produtividade_segurada`, `nivel_cobertura`, `taxa`, `seguradora`

**Exemplo:**

```python
from agrobr.alt import mapa_psr

# Todas as apolices
df = await mapa_psr.apolices()

# Apolices de milho no PR
df = await mapa_psr.apolices(cultura="MILHO", uf="PR")

# Apolices de 2023
df = await mapa_psr.apolices(ano=2023)
```

## Versao Sincrona

```python
from agrobr.sync import alt

df = alt.mapa_psr.sinistros(cultura="SOJA", uf="MT")
df = alt.mapa_psr.apolices(ano=2023)
```

## Notas

- Fonte: [SISSER/MAPA](https://dados.agricultura.gov.br/dataset/sisser3) — licenca `livre` (CC-BY)
- Dados: CSV bulk (3 arquivos: 2006-2015, 2016-2024, 2025)
- PII removido automaticamente (NM_SEGURADO, NR_DOCUMENTO_SEGURADO)
- Geolocalizacao removida (LATITUDE, LONGITUDE, graus/min/seg)
- CSVs podem ser grandes (~500k linhas no periodo 2006-2015)
- Timeout de leitura: 180 segundos
