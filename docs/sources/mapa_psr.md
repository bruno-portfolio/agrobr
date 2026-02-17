# MAPA PSR — Seguro Rural

> **Licenca:** CC-BY (dados publicos governo federal).
> Classificacao: `livre`

Dados abertos do SISSER/MAPA — Sistema de Subvencao Economica ao Premio do
Seguro Rural. Apolices e sinistros (indenizacoes) do seguro rural brasileiro
com subvencao federal, publicados pelo Ministerio da Agricultura.

Proxy de revisao de producao: sinistros elevados em soja Q1 antecedem
cortes na estimativa CONAB Q2.

## Instalacao

Nao requer dependencias opcionais. Usa apenas httpx + pandas (core).

## API

```python
from agrobr.alt import mapa_psr

# Sinistros de seguro rural (indenizacoes pagas)
df = await mapa_psr.sinistros()

# Filtrar por cultura e UF
df = await mapa_psr.sinistros(cultura="SOJA", uf="MT")

# Filtrar por ano ou range
df = await mapa_psr.sinistros(ano=2023)
df = await mapa_psr.sinistros(ano_inicio=2020, ano_fim=2024)

# Filtrar por evento preponderante
df = await mapa_psr.sinistros(evento="seca")

# Filtrar por municipio
df = await mapa_psr.sinistros(municipio="SORRISO")

# Todas as apolices (incluindo sem sinistro)
df = await mapa_psr.apolices()

# Apolices filtradas
df = await mapa_psr.apolices(cultura="MILHO", uf="PR", ano=2023)

# API sincrona
from agrobr.sync import alt
df = alt.mapa_psr.sinistros(cultura="SOJA")
df = alt.mapa_psr.apolices(uf="MT")
```

## Parametros — `sinistros`

| Parametro | Tipo | Default | Descricao |
|---|---|---|---|
| `cultura` | str \| None | None | Filtro por cultura (busca parcial, ex: "SOJA") |
| `uf` | str \| None | None | Filtro por UF (sigla, ex: "MT") |
| `ano` | int \| None | None | Filtro de ano unico (ex: 2023) |
| `ano_inicio` | int \| None | None | Ano inicial do range (inclusive) |
| `ano_fim` | int \| None | None | Ano final do range (inclusive) |
| `municipio` | str \| None | None | Filtro por municipio (busca parcial) |
| `evento` | str \| None | None | Filtro por evento preponderante (ex: "seca") |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

## Colunas — `sinistros`

| Coluna | Tipo | Nullable | Descricao |
|---|---|---|---|
| `nr_apolice` | str | Nao | Numero da apolice |
| `ano_apolice` | int | Nao | Ano da apolice |
| `uf` | str | Nao | Sigla UF da propriedade |
| `municipio` | str | Sim | Nome do municipio |
| `cd_ibge` | str | Sim | Codigo IBGE do municipio |
| `cultura` | str | Nao | Cultura segurada (uppercase) |
| `classificacao` | str | Sim | Classificacao do produto (AGRICOLA, PECUARIO, etc.) |
| `evento` | str | Nao | Evento preponderante (lowercase) |
| `area_total` | float | Sim | Area total segurada (ha) |
| `valor_indenizacao` | float | Nao | Valor da indenizacao (R$) — sempre > 0 |
| `valor_premio` | float | Sim | Premio liquido (R$) |
| `valor_subvencao` | float | Sim | Subvencao federal (R$) |
| `valor_limite_garantia` | float | Sim | Limite de garantia (R$) |
| `produtividade_estimada` | float | Sim | Produtividade estimada |
| `produtividade_segurada` | float | Sim | Produtividade segurada |
| `nivel_cobertura` | float | Sim | Nivel de cobertura (%) |
| `seguradora` | str | Sim | Razao social da seguradora |

## Parametros — `apolices`

| Parametro | Tipo | Default | Descricao |
|---|---|---|---|
| `cultura` | str \| None | None | Filtro por cultura |
| `uf` | str \| None | None | Filtro por UF |
| `ano` | int \| None | None | Filtro de ano unico |
| `ano_inicio` | int \| None | None | Ano inicial do range |
| `ano_fim` | int \| None | None | Ano final do range |
| `municipio` | str \| None | None | Filtro por municipio |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

## Colunas — `apolices`

Mesmas colunas de `sinistros`, mais:

| Coluna | Tipo | Nullable | Descricao |
|---|---|---|---|
| `taxa` | float | Sim | Taxa do premio (%) |

## Pipeline de dados

1. Resolve periodos necessarios (2006-2015, 2016-2024, 2025) com base nos filtros de ano
2. Download CSV bulk do portal dados.agricultura.gov.br (3 arquivos, encoding variavel)
3. Detecta encoding (UTF-8 → ISO-8859-1 → chardet) e separador (`;` ou `,`)
4. Remove colunas PII (NM_SEGURADO, NR_DOCUMENTO_SEGURADO) e geolocalizacao
5. Normaliza nomes de colunas (snake_case padronizado)
6. Converte tipos (float64 para monetarios, int para ano)
7. Para sinistros: filtra VALOR_INDENIZACAO > 0 e EVENTO_PREPONDERANTE nao vazio

## MetaInfo

```python
df, meta = await mapa_psr.sinistros(return_meta=True)
print(meta.source)           # "mapa_psr"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 1
print(meta.records_count)    # varia por filtro
```

## Nota de desempenho

Os CSVs do SISSER podem ser grandes (o arquivo 2006-2015 tem ~500k linhas).
O modulo baixa apenas os periodos necessarios com base nos filtros de ano.
Timeout de leitura: 180 segundos.

## Fonte

- URL: `https://dados.agricultura.gov.br/dataset/sisser3`
- Formato: CSV (3 arquivos por periodo)
- Atualizacao: anual
- Historico: 2006+
- Licenca: `livre` (CC-BY, dados publicos governo federal)
