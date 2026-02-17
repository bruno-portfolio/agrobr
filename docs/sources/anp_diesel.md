# ANP Diesel — Precos e Volumes

> **Licenca:** Dados publicos do governo federal (Decreto 8.777/2016).
> Classificacao: `livre`

Agencia Nacional do Petroleo, Gas Natural e Biocombustiveis. Dados de precos
de revenda e volumes de venda de diesel no Brasil. Proxy de atividade
mecanizada agricola.

## Instalacao

Nao requer dependencias opcionais. Usa apenas httpx + pandas + openpyxl (core).

## API

```python
from agrobr.alt import anp_diesel

# Precos de diesel S10 — nivel municipio
df = await anp_diesel.precos_diesel(produto="DIESEL S10")

# Precos de diesel por UF
df = await anp_diesel.precos_diesel(nivel="uf")

# Precos filtrados por UF e periodo
df = await anp_diesel.precos_diesel(
    uf="MT",
    inicio="2024-01-01",
    fim="2024-06-30",
)

# Precos agregados mensalmente
df = await anp_diesel.precos_diesel(agregacao="mensal")

# Volumes de venda por UF
df = await anp_diesel.vendas_diesel()

# Volumes de venda filtrados
df = await anp_diesel.vendas_diesel(uf="SP", inicio="2024-01-01")

# API sincrona
from agrobr.sync import alt
df = alt.anp_diesel.precos_diesel(uf="MT")
df = alt.anp_diesel.vendas_diesel()
```

## Parametros — `precos_diesel`

| Parametro | Tipo | Default | Descricao |
|---|---|---|---|
| `uf` | str \| None | None | Filtro por UF (ex: SP, MT, PR) |
| `municipio` | str \| None | None | Filtro por municipio (substring) |
| `produto` | str | "DIESEL S10" | "DIESEL" ou "DIESEL S10" |
| `inicio` | str \| date \| None | None | Data inicial (YYYY-MM-DD) |
| `fim` | str \| date \| None | None | Data final (YYYY-MM-DD) |
| `agregacao` | str | "semanal" | "semanal" ou "mensal" |
| `nivel` | str | "municipio" | "municipio", "uf" ou "brasil" |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

## Colunas — `precos_diesel`

| Coluna | Tipo | Nullable | Descricao |
|---|---|---|---|
| `data` | datetime | Nao | Data da coleta |
| `uf` | str | Sim | Sigla UF (2 chars) |
| `municipio` | str | Sim | Nome do municipio |
| `produto` | str | Sim | "DIESEL" ou "DIESEL S10" |
| `preco_venda` | float | Sim | Preco medio revenda (R$/litro) |
| `preco_compra` | float | Sim | Preco medio distribuicao (R$/litro) |
| `margem` | float | Sim | preco_venda - preco_compra |
| `n_postos` | int | Sim | Numero de postos pesquisados |

## Parametros — `vendas_diesel`

| Parametro | Tipo | Default | Descricao |
|---|---|---|---|
| `uf` | str \| None | None | Filtro por UF (ex: SP, MT, PR) |
| `inicio` | str \| date \| None | None | Data inicial |
| `fim` | str \| date \| None | None | Data final |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

## Colunas — `vendas_diesel`

| Coluna | Tipo | Nullable | Descricao |
|---|---|---|---|
| `data` | datetime | Nao | Primeiro dia do mes |
| `uf` | str | Sim | Sigla UF |
| `regiao` | str | Sim | Regiao geografica |
| `produto` | str | Sim | Tipo diesel |
| `volume_m3` | float | Sim | Volume vendido em m3 |

## Pipeline de dados

### Precos
1. Download XLSX bulk do portal gov.br (arquivos por periodo: 2022-2023, 2024-2025, 2026)
2. Parse com openpyxl, filtro de produtos diesel (DIESEL, DIESEL S10)
3. Calculo de margem (preco_venda - preco_compra)
4. Agregacao semanal ou mensal conforme parametro

### Volumes
1. Download XLS de vendas de combustiveis em m3
2. Parse formato long (ANO, MES, VOLUME) ou wide (meses como colunas)
3. Filtro de diesel (OLEO DIESEL e variantes)
4. Conversao para formato padrao (data, uf, regiao, produto, volume_m3)

## MetaInfo

```python
df, meta = await anp_diesel.precos_diesel(return_meta=True)
print(meta.source)           # "anp_diesel"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 1
print(meta.records_count)    # varia por filtro
```

## Nota de desempenho

Os XLSX da ANP podem ser grandes (50-100MB para precos por municipio).
O modulo cacheia por periodo do arquivo (ex: 2022-2023), nao por parametro
de filtro. Filtros de UF/municipio/produto sao aplicados no parse apos download.
TTL de cache: 7 dias.

## Fonte

- URL precos: `https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/`
- URL volumes: `https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/vdpb/vendas-combustiveis-m3.xls`
- Formato: XLSX (precos 2013+), XLS (volumes)
- Atualizacao: semanal (precos), mensal (volumes)
- Historico: 2013+ (precos), variavel (volumes)
- Licenca: `livre` (dados publicos governo federal, Decreto 8.777/2016)
