# API RNC

O modulo rnc fornece dados de cultivares registradas e protegidas no Brasil via CultivarWeb/MAPA.

## Funcoes

### `registradas`

Cultivares com registro no RNC (Registro Nacional de Cultivares).

```python
async def registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `cultivar` | `str \| None` | Filtro por nome da cultivar (contains, case-insensitive) |
| `especie` | `str \| None` | Filtro por especie / nome comum |
| `grupo` | `str \| None` | Filtro por grupo |
| `situacao` | `str \| None` | Filtro por situacao (ex: "REGISTRADA") |
| `mantenedor` | `str \| None` | Filtro por mantenedor |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

**Retorno:** DataFrame com colunas: `cultivar`, `nome_comum`, `nome_cientifico`, `grupo`, `situacao`, `nr_formulario`, `nr_registro`, `data_registro`, `data_validade`, `mantenedor`

**Exemplo:**

```python
from agrobr import rnc

# Todas as cultivares de soja
df = await rnc.registradas(especie="soja")

# Cultivares Embrapa
df = await rnc.registradas(mantenedor="Embrapa")
```

---

### `protegidas`

Cultivares com protecao de propriedade intelectual (SNPC).

```python
async def protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `cultivar` | `str \| None` | Filtro por nome da cultivar (contains, case-insensitive) |
| `especie` | `str \| None` | Filtro por especie / nome comum |
| `situacao` | `str \| None` | Filtro por situacao (ex: "PROTECAO DEFINITIVA") |
| `titular` | `str \| None` | Filtro por titular da protecao |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

**Retorno:** DataFrame com colunas: `cultivar`, `nome_cientifico`, `nome_comum`, `nr_processo`, `situacao`, `nr_certificado`, `inicio_protecao`, `termino_protecao`, `titular`, `representante_legal`, `melhoristas`

**Exemplo:**

```python
from agrobr import rnc

# Todas as protegidas
df = await rnc.protegidas()

# Filtrar por titular
df = await rnc.protegidas(titular="Embrapa")
```

## Versao Sincrona

```python
from agrobr.sync import rnc

df = rnc.registradas(especie="soja")
df = rnc.protegidas(titular="Embrapa")
```

## Notas

- Fonte: [CultivarWeb/MAPA](https://sistemas.agricultura.gov.br/snpc/cultivarweb) — licenca `livre` (dados publicos governo federal)
- Acesso via 2 POSTs com sessao (pesquisa vazia + export CSV)
- User-Agent obrigatorio
- Datas no formato DD/MM/YYYY (convertidas automaticamente)
- ~37K cultivares registradas, ~5K cultivares protegidas
