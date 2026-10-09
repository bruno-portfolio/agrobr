# API ANP Diesel

O modulo ANP Diesel fornece dados de precos de revenda e volumes de venda de diesel no Brasil, publicados pela Agencia Nacional do Petroleo. Namespace: `agrobr.alt.anp_diesel`.

## Funcoes

### `precos_diesel`

Precos de revenda de diesel por municipio, UF ou nivel Brasil.

```python
async def precos_diesel(
    uf: str | None = None,
    municipio: int | str | None = None,
    produto: str = "DIESEL S10",
    inicio: str | date | None = None,
    fim: str | date | None = None,
    agregacao: str = "semanal",
    nivel: str = "municipio",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame | tuple[pd.DataFrame | pl.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str \| None` | Filtro por UF (ex: SP, MT, PR) |
| `municipio` | `int \| str \| None` | Município pelo código IBGE de 7 dígitos ou pelo nome inteiro, resolvido por `normalize.resolver_municipio` antes da rede e comparado com o nome da planilha sem caixa, acento e pontuação (`"Sant'Ana do Livramento"` casa com `SANTANA DO LIVRAMENTO`); pedaço de nome gera `InvalidParameterError` com os candidatos |
| `produto` | `str` | "DIESEL" ou "DIESEL S10" (default) |
| `inicio` | `str \| date \| None` | Data inicial (YYYY-MM-DD) |
| `fim` | `str \| date \| None` | Data final (YYYY-MM-DD) |
| `agregacao` | `str` | "semanal" (default) ou "mensal" |
| `nivel` | `str` | "municipio" (default), "uf" ou "brasil" |
| `as_polars` | `bool` | Retorna polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

Contrato de fonte `anp_diesel_precos` 2.0, com 15 colunas: `data`, `uf`, `municipio`, `produto`, `preco_venda`, `preco_compra`, `n_postos`, `margem`, `periodo_inicio`, `periodo_fim`, `nivel`, `unidade`, `agregacao`, `n_semanas`, `n_postos_media`. O [dataset `precos_diesel`](../contracts/precos_diesel.md) tem contrato próprio 1.0, com as mesmas colunas.

**Exemplo:**

```python
from agrobr.alt import anp_diesel

# Precos de DIESEL S10
df = await anp_diesel.precos_diesel()

# Filtrar por UF e periodo
df = await anp_diesel.precos_diesel(
    uf="MT",
    inicio="2024-01-01",
    fim="2024-06-30",
)

# Nivel UF com agregacao mensal
df = await anp_diesel.precos_diesel(nivel="uf", agregacao="mensal")
```

### `vendas_diesel`

Volumes de venda de diesel por UF (mensal).

```python
async def vendas_diesel(
    uf: str | None = None,
    inicio: str | date | None = None,
    fim: str | date | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame | tuple[pd.DataFrame | pl.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str \| None` | Filtro por UF (ex: SP, MT, PR) |
| `inicio` | `str \| date \| None` | Data inicial |
| `fim` | `str \| date \| None` | Data final |
| `as_polars` | `bool` | Retorna polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `data`, `uf`, `regiao`, `produto`, `volume_m3`

**Exemplo:**

```python
from agrobr.alt import anp_diesel

# Volumes de diesel
df = await anp_diesel.vendas_diesel()

# Filtrar por UF
df = await anp_diesel.vendas_diesel(uf="MT")
```

## Versao Sincrona

```python
from agrobr.sync import alt

df = alt.anp_diesel.precos_diesel(uf="MT")
df = alt.anp_diesel.vendas_diesel()
```

## Notas

- Fonte: [ANP Gov.br](https://www.gov.br/anp/) — licenca `livre` (Decreto 8.777/2016)
- Dados: XLSX bulk (preços municipais desde 2022; por UF e Brasil desde 2013), CSV (volumes). No nível municipal, `inicio` ou `fim` fora de 2022 até o ano corrente levanta `InvalidParameterError` antes da rede
- Planilhas municipais grandes são baixadas integralmente; não há cache persistente.

## Períodos de preços e catálogo

`data` é o início publicado da semana, não o instante de aquisição. Filtros de data selecionam esse início e preservam o fim publicado. O mensal usa o mês do início semanal e a média simples dos preços semanais disponíveis, sem ponderação por postos ou dias. `n_postos` fica nulo; `n_postos_media` e `n_semanas` descrevem as observações selecionadas. Semanas ausentes não são inventadas.

No nível municipal, intervalos de anos são resolvidos pelos links publicados; anos além do catálogo configurado acionam descoberta no catálogo oficial. Período válido sem recurso publicado gera `SourceUnavailableError` com a cobertura do catálogo. Datas malformadas ou invertidas geram `InvalidParameterError`.

Metadados preservam URLs pedida/final dos recursos, aquisição, hashes, população semanal e cobertura selecionada. Veja o [contrato do dataset](../contracts/precos_diesel.md).

## Preço de venda por nível

`preco_venda` não tem a mesma natureza em todos os níveis. No município, é a média aritmética simples dos preços dos postos da amostra. Na UF e no Brasil, desde 31/10/2004, é a média ponderada pelas vendas que as distribuidoras informam à ANP (nota da [página da série histórica](https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/serie-historica-do-levantamento-de-precos)). `n_postos` é o tamanho da amostra e fecha entre os níveis: a UF soma os postos dos seus municípios, e o Brasil, os das UFs. Mas não é o peso, e recompor a UF pela média dos municípios ponderada por `n_postos` erra: na semana de 06/09/2026, o diesel S10 de AL sai a 6,84 R$/l com 25 postos, e a média dos 4 municípios ponderada por postos dá 7,20 R$/l. `produto="DIESEL"` é o óleo diesel B S500 comum, como diz a própria planilha; `"DIESEL S10"` é o S10.

## Semanas na virada do ano

Nas planilhas municipais, uma semana iniciada no fim de dezembro pode estar no arquivo do período seguinte. A seleção inclui o arquivo adjacente disponível quando necessário: a semana de 31/12/2023 a 06/01/2024 está em `2024–2025`, pertence ao filtro de dezembro de 2023 e entra na média desse mês. A consulta pode baixar dois arquivos mesmo com início e fim no mesmo ano.

O mensal desta API é calculado a partir das semanas selecionadas; não usa as planilhas mensais separadas que a ANP também publica. A ausência de preço de distribuição nos arquivos municipais mantém `preco_compra` e `margem` nulos. Os recibos de aquisição identificam todos os arquivos usados.

## Repetições e tipos de saída

Linhas semanais inteiramente idênticas são removidas da seleção antes da média mensal, com
`UserWarning` e registro em `MetaInfo.validation_warnings`. Valores conflitantes para a mesma
semana continuam gerando `ParseError`. Os recibos e o hash dos arquivos brutos são preservados.

Nos preços, texto usa o dtype padrão do pandas instalado (`str` no pandas 3 e `object` no pandas 2),
inclusive no vazio. Datas usam `datetime64[ns]`, valores monetários `float64` e contagens `Int64`.
`as_polars` e `return_meta` devem ser passados por nome em ambas as funções.
O limite anual do catálogo municipal segue a data civil de Brasília.
