# ANTAQ — Movimentacao Portuaria

> **Licenca:** Dados publicos do governo federal.
> Classificacao: `livre`

!!! warning "Fonte indisponivel desde 23/06/2026"
    A ANTAQ tirou o Estatistico Aquaviario do ar ([aviso oficial](https://www.gov.br/antaq/pt-br/central-de-conteudos/publicacoes-da-antaq/publicacoes-off/painel-estatistico-aquaviario-indisponivel)).
    O host `estatistica.antaq.gov.br` nao serve mais os arquivos: responde `403` (Cloudflare
    challenge) ou redireciona para o aviso de indisponibilidade, conforme o cliente.
    Chamadas a `antaq.movimentacao()` levantam `SourceUnavailableError`. Nao ha fonte
    alternativa com cobertura equivalente — a Base dos Dados cobre apenas 2014-2020.
    Ultima verificacao: 31/08/2026.

Agencia Nacional de Transportes Aquaviarios. Dados de movimentacao
portuaria de carga (granel solido, liquido, geral, conteiner) desde 2010.

## Instalacao

Nao requer dependencias opcionais. Usa requests + pandas (core) — o WAF da ANTAQ rejeita clients httpx.
O download segue `AGROBR_HTTP_TIMEOUT_CONNECT` e `AGROBR_HTTP_TIMEOUT_READ`, com piso de 180 s na leitura (valor maior
prevalece); `AGROBR_HTTP_TIMEOUT_WRITE` e `AGROBR_HTTP_TIMEOUT_POOL` são do httpx e não valem para este transporte.

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
| `ano` | int | obrigatorio | Ano dos dados, de 2010 ao último ano publicado; o ano que a ANTAQ ainda não publicou levanta `SourceUnavailableError` |
| `tipo_navegacao` | str \| None | None | longo_curso, cabotagem, interior, apoio_maritimo, apoio_portuario |
| `natureza_carga` | str \| None | None | granel_solido, granel_liquido, carga_geral, conteiner |
| `mercadoria` | str \| None | None | Filtro por mercadoria (substring case-insensitive); sem catálogo estático, a lista vem no `Mercadoria.txt` do ZIP, e um nome fora dela só aparece como resultado vazio |
| `porto` | str \| None | None | Filtro por porto (substring case-insensitive) |
| `uf` | str \| None | None | Filtro por UF (ex: SP, PR, MT); UF inexistente é recusada antes da descarga |
| `sentido` | str \| None | None | embarque ou desembarque |
| `as_polars` | bool | False | Se True, retorna `polars.DataFrame` |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

## Colunas — `movimentacao`

| Coluna | Tipo | Nullable | Descricao |
|---|---|---|---|
| `ano` | Int64 | Sim (carga sem atracacao) | Ano |
| `mes` | Int64 | Sim (carga sem atracacao) | Mes (1-12) |
| `data_atracacao` | datetime64[ns] | Sim | Data e hora de atracacao |
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
print(meta.source_method)    # "requests+zip"
print(meta.parser_version)   # 2
print(meta.records_count)    # ~2.4M para ano completo
```

## Campos, joins e unidades

Dos 62 campos publicados nos tres TXT (29 em atracacao, 27 em carga, 6 em mercadoria), 21 viram
coluna de saida, 3 sao chave de join (`IDAtracacao` duas vezes, `CDMercadoria`) e 38 sao ignorados.

**Joins e cardinalidade.** A saida parte da carga: `carga -> atracacao` por `IDAtracacao` e
`carga -> mercadoria` por `CDMercadoria`, ambos `left`. Uma atracacao pode ter varias cargas (num
recorte de janeiro de 2024, a atracacao `1406197` tem 5), entao o numero de linhas e o numero de cargas, nao de
atracacoes; atracacao sem carga nao aparece. Carga sem atracacao mantem a linha com `ano`/`mes`
nulos na API da fonte e e descartada pelo dataset, que exige `ano` e `mes`. Carga com
`CDMercadoria` fora da tabela mantem a linha com `mercadoria`/`grupo_mercadoria` nulos.

**Coluna ausente.** Se um dos TXT vier sem uma coluna que o join, os filtros ou a chave do dataset usam,
`antaq.movimentacao()` e `datasets.movimentacao_portuaria()` levantam `ParseError` com o nome da coluna.
São elas: na atracação, `IDAtracacao`, `Porto Atracação`, `Complexo Portuário`, `Terminal`, `Município`,
`SGUF`, `Região Geográfica`, `Ano`, `Mes` e `Data Atracação`; na carga, `IDAtracacao`, `CDMercadoria`,
`Tipo Navegação`, `Natureza da Carga`, `Sentido` e `VLPesoCargaBruta`; na mercadoria, `CDMercadoria` e
`Nomenclatura Simplificada Mercadoria`.

**Colunas com duas origens.** `tipo_navegacao` vem de `Tipo Navegacao` (carga); a atracacao publica
`Tipo de Navegacao da Atracacao`, que e lida e descartada na projecao do join - as duas divergem
quando a mesma atracacao movimenta cargas de naturezas diferentes. `uf` vem de `SGUF` (sigla), nao
de `UF` (nome por extenso). `mercadoria` e a `Nomenclatura Simplificada Mercadoria`; a descricao NCM
completa (`Mercadoria`) nao e publicada, e o filtro `mercadoria` casa apenas com o nome curto.

**Unidades e rotulos.** `peso_bruto_ton` esta em toneladas: o texto publicado perde o ponto de
milhar e troca a virgula por ponto. `QTCarga` nao tem unidade publicada pela ANTAQ nem informada no corpo
(a escala sugere quilos em fertilizantes e unidades em carga de apoio; e inferencia, nao unidade
publicada) - `qt_carga` e copiado sem conversao.
`TEU` ausente vira 0. `ano`/`mes` sao o periodo publicado da atracacao (`Ano` e `Mes`, este ultimo
em texto pt-BR como `jan`), nao a data: uma atracacao iniciada em 22/12/2023 aparece com `ano=2024`
e `mes=1`.

**Limites.** O recorte dos exemplos cobre janeiro de 2024 em AM e PA; `apoio_maritimo`, carga
conteinerizada e `TEU > 0` nao tem caso positivo nele.

## Nota de desempenho

Os ZIPs anuais da ANTAQ sao grandes (~80MB comprimidos, ~450MB descomprimidos
para Carga.txt). O download pode levar alguns segundos. O parser usa
`usecols` para carregar apenas colunas necessarias, otimizando memoria.

## Fonte

- URL: `https://estatistica.antaq.gov.br/ea/sense/download.html`
- Download: `https://estatistica.antaq.gov.br/ea/txt/{ANO}.zip`
- Formato: TXT (CSV com separador `;`, encoding UTF-8-sig, decimal `,`)
- Atualizacao: anual (dados consolidados)
- Historico: 2010+
- Licenca: `livre` (dados publicos governo federal)

`data_atracacao` preserva a data e a hora publicadas em `datetime64[ns]`; erro de calendário vira `NaT` com aviso em `MetaInfo.validation_warnings`. Texto que não é data levanta `ParseError`. `ano`, `mes` e `teu` usam `Int64`; `peso_bruto_ton` e `qt_carga` usam `float64`. Cheio e vazio têm os mesmos tipos. `sentido`, `tipo_navegacao` e `natureza_carga` aceitam os aliases documentados e os rótulos publicados inteiros, ignorando caixa, acento e espaço nas pontas. Valor não suportado levanta `InvalidParameterError` antes de baixar os ZIPs.
