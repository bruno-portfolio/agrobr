# ComexStat — Exportações e Importações

Dados de comércio exterior do MDIC/SECEX. Exportações e importações
por produto (NCM), UF e país.

## API

```python
from agrobr import comexstat

# Exportações mensais de soja em 2024
df = await comexstat.exportacao("soja", ano=2024, agregacao="mensal")

# Importações mensais de soja em 2024
df = await comexstat.importacao("soja", ano=2024, agregacao="mensal")

# Exportações detalhadas (por país/UF/via)
df = await comexstat.exportacao("soja", ano=2024, agregacao="detalhado")

# Filtrar por UF
df = await comexstat.exportacao("soja", ano=2024, uf="MT")
```

## Colunas — `exportacao` / `importacao` (mensal)

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano |
| `mes` | int | Mês (1-12) |
| `ncm` | str | Código NCM (8 dígitos) |
| `uf` (exportação) | str | UF produtora da mercadoria, independente da sede do exportador ([FAQ 10 do MDIC](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/perguntas-frequentes-faq/12-por-que-a)) |
| `uf` (importação) | str | UF do domicílio fiscal do importador, não o destino da mercadoria no país ([FAQ 10 do MDIC](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/perguntas-frequentes-faq/12-por-que-a)) |
| `kg_liquido` | float | Peso líquido (kg) |
| `valor_fob_usd` | float | Valor FOB (USD) |
| `volume_ton` | float | Volume em toneladas |
| `valor_frete_usd` (só importação) | float | Frete (USD) |
| `valor_seguro_usd` (só importação) | float | Seguro (USD) |

## Produtos

`produto` aceita um alias ou um prefixo NCM de 2 a 8 dígitos. A tabela dos aliases, com
o que entra e o que não entra em cada um, está na [API ComexStat](../api/comexstat.md).

> **Nota:** o filtro usa `str.startswith` com os prefixos do alias (um alias pode ter
> vários, e `defensivos`/`agrotoxicos` excluem os códigos de uso exclusivamente
> domissanitário). Cada alias soma os códigos vigentes em cada ano: quando a
> nomenclatura desdobra ou renumera um código (etanol, soja, trigo, açúcar, DAP,
> frango), a série segue sem buraco, inclusive no ano de transição. `ssp` e `tsp`
> só têm código equivalente desde 2017 e recusam anos anteriores.

A API autônoma preserva uma linha por NCM. Os datasets `exportacao` e `importacao`
consolidam os códigos de cada produto por ano, mês e UF; `oleo_soja_bruto` permanece
restrito ao código `15071000`.

## MetaInfo

```python
df, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)
print(meta.source)  # "comexstat"
```

## Notas tecnicas

- O site `balanca.mdic.gov.br` não envia a cadeia completa do certificado. O client verifica o TLS
  por inteiro (hostname incluso), com o certificado intermediário da SERPRO conferido por SHA-256 e
  acrescentado às autoridades (`certifi`, `SSL_CERT_FILE` ou `SSL_CERT_DIR`).
- Sem cache: cada chamada baixa o CSV anual do fluxo (~100 MB), valida todas as linhas e filtra em memória (da ordem
  de 1 minuto por ano consultado), e várias consultas
  do mesmo ano baixam o arquivo de novo. `produto` aceita 1 alias ou 1 prefixo NCM por chamada, sem
  lista; para vários códigos num download só, use o prefixo comum (a API autônoma devolve 1 linha por NCM).
- Cada CSV anual tem ~100 MB. O download vai para um arquivo temporário e é conferido contra o
  `Content-Length` do GET ou, sem ele, do HEAD do mesmo arquivo; sem nenhum dos 2, o resultado avisa
  em `validation_warnings` ("tamanho do arquivo não conferido").
- O download inteiro, novas tentativas incluídas, tem teto de 300 s; acima dele, `SourceUnavailableError`
  ("TimeoutError"). O `AGROBR_HTTP_TIMEOUT_READ` não mexe nesse teto; em rede lenta, suba
  `AGROBR_HTTP_TIMEOUT_DOWNLOAD_COMEXSTAT` ([variáveis de ambiente](../advanced/ambiente.md)).

## Fonte

- Bulk CSV: `https://balanca.mdic.gov.br/balanca/bd/comexstat-bd/ncm`
- Atualização: semanal/mensal
- Historico: 1997+

`kg_liquido`, valores monetários e `volume_ton` usam `float64`; anos, meses e quantidades estatísticas usam `Int64`. O texto da fonte e dos dicionários mantém `string[python]`: o guarda de memória conta os objetos Python do pool de textos. Essa exceção vale no cheio e no vazio. Os datasets `exportacao` e `importacao` convertem o texto do recorte final para o padrão do pandas instalado. As flags de saída são somente por nome.
