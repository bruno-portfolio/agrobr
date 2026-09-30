# API MAPA PSR

O modulo MAPA PSR fornece dados de apolices e sinistros do seguro rural brasileiro com subvencao federal, publicados pelo SISSER/MAPA. Namespace: `agrobr.alt.mapa_psr`.

## Funcoes

### `sinistros`

Sinistros de seguro rural — indenizacoes pagas por cultura/municipio.

```python
async def sinistros(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    evento: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str \| None` | Filtro por cultura (busca parcial, accent-insensitive, ex: "cafe" matcha "CAFE ARABICA") |
| `uf` | `str \| None` | Filtro por UF (sigla, ex: "MT") |
| `ano` | `int \| None` | Filtro de ano unico (ex: 2023) |
| `ano_inicio` | `int \| None` | Ano inicial do range (inclusive) |
| `ano_fim` | `int \| None` | Ano final do range (inclusive) |
| `municipio` | `int \| str \| None` | Município pelo código IBGE de 7 dígitos (`int` ou `str`) ou pelo nome inteiro, sem caixa e acento (ex.: `4305108` ou `"Caxias do Sul"`); pedaço de nome, nome de outra UF ou nome repetido sem `uf` geram `InvalidParameterError` com os candidatos |
| `evento` | `str \| None` | Filtro por evento preponderante (ex: "seca") |
| `as_polars` | `bool` | Se True, retorna polars.DataFrame; somente nomeado, como `return_meta` |
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
df = await mapa_psr.sinistros(produto="SOJA", uf="MT")

# Sinistros por seca em 2023
df = await mapa_psr.sinistros(evento="seca", ano=2023)

# Range de anos
df = await mapa_psr.sinistros(ano_inicio=2020, ano_fim=2024)
```

### `apolices`

Todas as apolices de seguro rural com subvencao federal.

```python
async def apolices(
    produto: str | None = None,
    uf: str | None = None,
    ano: int | None = None,
    ano_inicio: int | None = None,
    ano_fim: int | None = None,
    municipio: int | str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> DataFrameResult
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str \| None` | Filtro por cultura (busca parcial, accent-insensitive, ex: "cafe" matcha "CAFE ARABICA") |
| `uf` | `str \| None` | Filtro por UF (sigla, ex: "MT") |
| `ano` | `int \| None` | Filtro de ano unico (ex: 2023) |
| `ano_inicio` | `int \| None` | Ano inicial do range (inclusive) |
| `ano_fim` | `int \| None` | Ano final do range (inclusive) |
| `municipio` | `int \| str \| None` | Município pelo código IBGE de 7 dígitos (`int` ou `str`) ou pelo nome inteiro, sem caixa e acento (ex.: `4305108` ou `"Caxias do Sul"`); pedaço de nome, nome de outra UF ou nome repetido sem `uf` geram `InvalidParameterError` com os candidatos |
| `as_polars` | `bool` | Se True, retorna polars.DataFrame; somente nomeado, como `return_meta` |
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
df = await mapa_psr.apolices(produto="MILHO", uf="PR")

# Apolices de 2023
df = await mapa_psr.apolices(ano=2023)
```

## Versao Sincrona

```python
from agrobr.sync import alt

df = alt.mapa_psr.sinistros(produto="SOJA", uf="MT")
df = alt.mapa_psr.apolices(ano=2023)
```

## Notas

- Fonte: [SISSER/MAPA](https://dados.agricultura.gov.br/dataset/sisser3) — licenca `livre` (CC-BY)
- Dados: CSV bulk (3 arquivos: 2006-2015, 2016-2024, 2025)
- PII removido automaticamente (NM_SEGURADO, NR_DOCUMENTO_SEGURADO)
- Geolocalizacao removida (LATITUDE, LONGITUDE, graus/min/seg)
- Download em arquivo temporário e leitura por blocos de 10 mil linhas, com filtros antes das conversões numéricas
- O download de cada período continua integral; espaço temporário em disco é necessário, e a memória do resultado cresce com as linhas selecionadas
- Timeout de leitura: 180 segundos
- `municipio` filtra pelo código IBGE publicado em `CD_GEOCMU`, que cobre também as apólices rotuladas com o nome do distrito; a linha sem código entra quando o rótulo é o nome inteiro do município na mesma UF (ver [MAPA PSR](../sources/mapa_psr.md#municipio-e-codigo-ibge))

## Integridade e período das apólices

O CSV inteiro é validado antes da aplicação dos filtros. Cabeçalho duplicado, registro com campos a mais ou a menos e ano de apólice inválido geram `ParseError` com a posição do registro; a leitura não descarta essas linhas silenciosamente. Campos entre aspas podem conter separadores e quebras de linha. O parser é versão 4; o contrato de apólices está em 1.2 e o de sinistros em 1.1. `ano_apolice` sai em `Int64`, com linhas e vazio.

`ano_apolice` é o ano de contratação da apólice, conforme o dicionário SISSER; não identifica a data do evento ou do pagamento. `sinistros` seleciona indenização positiva com evento não vazio. Zero publicado continua zero em `apolices`; valores ausentes continuam nulos. Não se arredondam valores monetários a centavos. Números de apólice e códigos geográficos conservam seus zeros iniciais. Só o registro publicado em dobro e idêntico em todas as colunas sai uma vez (ver [MAPA PSR](../sources/mapa_psr.md)); nenhuma outra linha é deduplicada.

Na captura de 18/09/2026, o catálogo disponibilizava três CSVs, até 2025. O arquivo 2025 tinha indenizações ausentes, o que não demonstra ausência de sinistros. O EOF comprova a leitura completa do arquivo publicado, sem garantir cobertura completa do programa ou atualização dos pagamentos.

Campos textuais preservam literais como `NULL`, `NA`, `None` e `N/A`, sujeitos apenas às normalizações de espaços e caixa já documentadas; eles não são convertidos em ausência pelo leitor CSV. Campo textual vazio continua vazio, exceto `cd_ibge`, que sai nulo. Campos numéricos mantêm a conversão vigente: valores ausentes ou não interpretáveis ficam nulos, sem transformar tokens textuais em zero.
