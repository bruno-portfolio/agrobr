# ANTAQ — Movimentacao Portuaria

> **Licenca:** Dados publicos do governo federal.
> Classificacao: `livre`

Agencia Nacional de Transportes Aquaviarios. Dados de movimentacao
portuaria de carga (granel solido, liquido, geral, conteiner) desde 2010.

## Instalacao

Nao requer dependencias opcionais. Usa apenas httpx + pandas (core).

## API

```python
from agrobr import antaq

# Movimentacao portuaria de um ano
df = await antaq.movimentacao(2024)

# Filtrar por tipo de navegacao
df = await antaq.movimentacao(2024, tipo_navegacao="longo_curso")

# Filtrar por natureza da carga
df = await antaq.movimentacao(2024, natureza_carga="granel_solido")

# Filtrar por mercadoria (substring case-insensitive)
df = await antaq.movimentacao(2024, mercadoria="soja")

# Filtrar por porto
df = await antaq.movimentacao(2024, porto="Santos")

# Filtrar por UF
df = await antaq.movimentacao(2024, uf="SP")

# Filtrar por sentido
df = await antaq.movimentacao(2024, sentido="embarque")

# Multiplos filtros
df = await antaq.movimentacao(
    2024,
    tipo_navegacao="longo_curso",
    natureza_carga="granel_solido",
    mercadoria="soja",
    uf="PR",
    sentido="embarque",
)

# API sincrona
from agrobr.sync import antaq as antaq_sync
df = antaq_sync.movimentacao(2024, uf="SP")
```

## Parametros — `movimentacao`

| Parametro | Tipo | Default | Descricao |
|---|---|---|---|
| `ano` | int | obrigatorio | Ano dos dados (2010-2025) |
| `tipo_navegacao` | str \| None | None | longo_curso, cabotagem, interior, apoio_maritimo, apoio_portuario |
| `natureza_carga` | str \| None | None | granel_solido, granel_liquido, carga_geral, conteiner |
| `mercadoria` | str \| None | None | Filtro por mercadoria (substring case-insensitive) |
| `porto` | str \| None | None | Filtro por porto (substring case-insensitive) |
| `uf` | str \| None | None | Filtro por UF (ex: SP, PR, MT) |
| `sentido` | str \| None | None | embarque ou desembarque |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

## Colunas — `movimentacao`

| Coluna | Tipo | Nullable | Descricao |
|---|---|---|---|
| `ano` | int | Nao | Ano |
| `mes` | int | Nao | Mes (1-12) |
| `data_atracacao` | str | Sim | Data de atracacao |
| `tipo_navegacao` | str | Sim | Tipo de navegacao |
| `tipo_operacao` | str | Sim | Tipo de operacao da carga |
| `natureza_carga` | str | Sim | Natureza da carga |
| `sentido` | str | Sim | Embarcados ou Desembarcados |
| `porto` | str | Sim | Nome do porto |
| `complexo_portuario` | str | Sim | Complexo portuario |
| `terminal` | str | Sim | Terminal |
| `municipio` | str | Sim | Municipio |
| `uf` | str | Sim | UF do porto |
| `regiao` | str | Sim | Regiao geografica |
| `cd_mercadoria` | str | Sim | Codigo NCM SH4 da mercadoria |
| `mercadoria` | str | Sim | Nomenclatura simplificada |
| `grupo_mercadoria` | str | Sim | Grupo da mercadoria |
| `origem` | str | Sim | Origem da carga |
| `destino` | str | Sim | Destino da carga |
| `peso_bruto_ton` | float | Sim | Peso bruto em toneladas |
| `qt_carga` | float | Sim | Quantidade de carga |
| `teu` | int | Sim | TEU (conteineres) |

## Pipeline de dados

O modulo faz join de 3 tabelas do Estatistico Aquaviario:

1. **Atracacao** — dados do porto, terminal, municipio, UF, data
2. **Carga** — peso, tipo navegacao, natureza carga, sentido, mercadoria
3. **Mercadoria** — tabela de referencia NCM SH4

Join via `IDAtracacao` (FK Carga → Atracacao), lookup via `CDMercadoria`.

## MetaInfo

```python
df, meta = await antaq.movimentacao(2024, return_meta=True)
print(meta.source)           # "antaq"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 1
print(meta.records_count)    # ~2.4M para ano completo
```

## Nota de desempenho

Os ZIPs anuais da ANTAQ sao grandes (~80MB comprimidos, ~450MB descomprimidos
para Carga.txt). O download pode levar alguns segundos. O parser usa
`usecols` para carregar apenas colunas necessarias, otimizando memoria.

## Fonte

- URL: `https://web3.antaq.gov.br/ea/sense/download.html`
- Download: `https://web3.antaq.gov.br/ea/txt/{ANO}.zip`
- Formato: TXT (CSV com separador `;`, encoding UTF-8-sig, decimal `,`)
- Atualizacao: anual (dados consolidados)
- Historico: 2010+
- Licenca: `livre` (dados publicos governo federal)
