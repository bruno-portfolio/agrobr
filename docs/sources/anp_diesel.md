# ANP Diesel — Precos e Volumes

> **Licença:** Dados publicos do governo federal (Decreto 8.777/2016).
> Classificação: `livre`

Agencia Nacional do Petroleo, Gas Natural e Biocombustiveis. Dados de precos
de revenda e volumes de venda de diesel no Brasil. Proxy de atividade
mecanizada agrícola.

O CSV oficial de vendas pode trazer cabeçalhos acentuados; filtros por UF reconhecem a grafia publicada. Zeros e valores negativos publicados são preservados: o CSV de setembro/2026 traz −70 m³ de diesel marítimo em Sergipe, dezembro/2025, sem explicação no CSV. Não trate esses registros automaticamente como volume físico válido nem os converta silenciosamente para zero.

## Instalação

Não requer dependências opcionais. Usa httpx + pandas + calamine, com openpyxl como fallback.

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

## Parâmetros — `precos_diesel`

| Parâmetro | Tipo | Default | Descrição |
|---|---|---|---|
| `uf` | str \| None | None | Filtro por UF (ex: SP, MT, PR) |
| `municipio` | int \| str \| None | None | Município pelo código IBGE de 7 dígitos ou pelo nome inteiro, resolvido por `normalize.resolver_municipio` antes da rede e comparado com o nome da planilha sem caixa, acento e pontuação (`"Sant'Ana do Livramento"` casa com `SANTANA DO LIVRAMENTO`); pedaço de nome gera `InvalidParameterError` com os candidatos |
| `produto` | str | "DIESEL S10" | `DIESEL`, `OLEO DIESEL`, `OLEO DIESEL S10` ou `DIESEL S10` |
| `inicio` | str \| date \| None | None | Data inicial (YYYY-MM-DD) |
| `fim` | str \| date \| None | None | Data final (YYYY-MM-DD) |
| `agregacao` | str | "semanal" | "semanal" ou "mensal" |
| `nivel` | str | "municipio" | "municipio", "uf" ou "brasil" |
| `as_polars` | bool | False | Se True, retorna `polars.DataFrame` |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

## Colunas — `precos_diesel`

O contrato de fonte `anp_diesel_precos` 2.0 tem 15 colunas; o dataset `precos_diesel` mantém contrato próprio 1.0 com as mesmas colunas. `data` é o início publicado da semana ou a referência mensal; não é o instante da aquisição. Intervalo semanal, nível geográfico, unidade e cobertura da agregação são explícitos. Veja o [contrato do dataset](../contracts/precos_diesel.md) e a [semântica dos períodos](../api/anp_diesel.md).

## Parâmetros — `vendas_diesel`

| Parâmetro | Tipo | Default | Descrição |
|---|---|---|---|
| `uf` | str \| None | None | Filtro por UF (ex: SP, MT, PR) |
| `inicio` | str \| date \| None | None | Data inicial |
| `fim` | str \| date \| None | None | Data final |
| `as_polars` | bool | False | Se True, retorna `polars.DataFrame` |
| `return_meta` | bool | False | Retorna tupla (DataFrame, MetaInfo) |

## Colunas — `vendas_diesel`

| Coluna | Tipo | Nullable | Descrição |
|---|---|---|---|
| `data` | datetime | Não | Primeiro dia do mes |
| `uf` | str | Sim | Sigla UF |
| `regiao` | str | Sim | Região, com o nome canônico (`Norte`, `Nordeste`, `Centro-Oeste`, `Sudeste`, `Sul`) |
| `produto` | str | Sim | Tipo diesel |
| `volume_m3` | float | Sim | Volume vendido em m3 |

## Pipeline de dados

### Precos
1. Download XLSX bulk do portal gov.br (arquivos por período: 2022-2023, 2024-2025, 2026)
2. Parse com calamine (fallback openpyxl), filtro de produtos diesel (DIESEL, DIESEL S10, OLEO DIESEL, OLEO DIESEL S10); linha com rótulo de agregado ("TOTAL", "SUBTOTAL") na coluna de município sai, com aviso em `validation_warnings` e `UserWarning`
3. Normalizacao: prefixo "OLEO"/"ÓLEO" removido, nomes de estado convertidos para sigla UF
4. Cálculo de margem (preco_venda - preco_compra)
5. Agregação semanal ou mensal conforme parâmetro

### Volumes
1. Download CSV de vendas de diesel por tipo (dados abertos ANP)
2. Parse CSV semicolon-delimited (ANO, MES, GRANDE REGIAO, UNIDADE DA FEDERACAO, PRODUTO, VENDAS)
3. Filtro de diesel (OLEO DIESEL e variantes)
4. Normalizacao: prefixo "OLEO"/"ÓLEO" removido do produto e `DIESEL S-10` escrito `DIESEL S10`, como nos preços; os outros combustíveis (`DIESEL S-500`, `DIESEL S-1800`, `DIESEL MARÍTIMO`, `DIESEL (OUTROS )`) ficam como publicados; `REGIÃO CENTRO-OESTE` vira `Centro-Oeste`; nomes de estado convertidos para sigla UF
5. Conversão para formato padrão (data, uf, região, produto, volume_m3)

## MetaInfo

```python
df, meta = await anp_diesel.precos_diesel(return_meta=True)
print(meta.source)           # "anp_diesel"
print(meta.source_method)    # "httpx"
print(meta.parser_version)   # 4
print(meta.records_count)    # varia por filtro
```

## Nota de desempenho

Os XLSX da ANP podem ser grandes (50-100MB para precos por município).
Os períodos necessários são baixados concorrentemente e processados com
calamine. Não há cache persistente; os filtros são aplicados após o download.

## Fonte

- URL precos: `https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/shlp/`
- URL volumes: `https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2025.csv`
- Formato: XLSX (precos 2013+), CSV (volumes 2013+)
- Atualização: semanal (precos), mensal (volumes)
- Historico: 2013+ (precos e volumes)
- Licença: `livre` (dados publicos governo federal, Decreto 8.777/2016)

## Preço de venda por nível

`preco_venda` não tem a mesma natureza em todos os níveis. No município, é a média aritmética simples dos preços dos postos da amostra. Na UF e no Brasil, desde 31/10/2004, é a média ponderada pelas vendas que as distribuidoras informam à ANP (nota da [página da série histórica](https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/precos-revenda-e-de-distribuicao-combustiveis/serie-historica-do-levantamento-de-precos)). `n_postos` é o tamanho da amostra e fecha entre os níveis: a UF soma os postos dos seus municípios, e o Brasil, os das UFs. Mas não é o peso, e recompor a UF pela média dos municípios ponderada por `n_postos` erra: na semana de 06/09/2026, o diesel S10 de AL sai a 6,84 R$/l com 25 postos, e a média dos 4 municípios ponderada por postos dá 7,20 R$/l. `produto="DIESEL"` é o óleo diesel B S500 comum, como diz a própria planilha; `"DIESEL S10"` é o S10.

## Semanas na virada do ano

Nas planilhas municipais, uma semana iniciada no fim de dezembro pode estar no arquivo do período seguinte. A seleção inclui o arquivo adjacente disponível quando necessário: a semana de 31/12/2023 a 06/01/2024 está em `2024–2025`, pertence ao filtro de dezembro de 2023 e entra na média desse mês. A consulta pode baixar dois arquivos mesmo com início e fim no mesmo ano.

O mensal desta API é calculado a partir das semanas selecionadas; não usa as planilhas mensais separadas que a ANP também publica. A ausência de preço de distribuição nos arquivos municipais mantém `preco_compra` e `margem` nulos. Os recibos de aquisição identificam todos os arquivos usados.

A semana entra no mês da sua data de início, mesmo quando termina no mês seguinte: a de 29/03 a 04/04/2026, com 4 dos 7 dias em abril, entra inteira em março. Por isso o mensal pode diferir da planilha mensal da ANP. De jan/2025 a ago/2026 (Brasil, MT e SP, diesel S500 e S10), a diferença média fica entre R$ 0,006 e R$ 0,018/l por série. Num mês de choque de preço, chega a 2%: MT S500, mar/2026, 7,138 R$/l no agrobr × 7,00 R$/l na ANP (7,045 R$/l se a semana contasse pelo mês do fim).

## Repetições publicadas

A ANP pode repetir uma linha semanal inteira em seus arquivos. O agrobr mantém uma ocorrência na
seleção, registra a quantidade removida em `MetaInfo.validation_warnings` e emite `UserWarning`.
Isso evita contar a mesma semana duas vezes na média mensal. Uma identidade semanal com valores
conflitantes continua gerando `ParseError`, inclusive entre arquivos sobrepostos.

Os preços usam texto no dtype padrão do pandas instalado (`str` no pandas 3 e `object` no pandas 2),
datas em `datetime64[ns]`, valores em `float64` e contagens em `Int64`, inclusive em resultados vazios.
As flags `as_polars` e `return_meta` são passadas por nome. A virada anual do catálogo municipal
segue o dia civil de Brasília.

`inicio` e `fim` aceitam `date`, `datetime` (vale a data civil) e texto `AAAA-MM-DD`. Período que começa depois de hoje levanta `InvalidParameterError` antes da rede, em todos os níveis; no município, limite fora de 2022 até o ano corrente também. Ano dentro desse intervalo cujo arquivo a ANP ainda não publicou continua `SourceUnavailableError`.
