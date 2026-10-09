# Variáveis de ambiente

Todas as variáveis do agrobr começam com `AGROBR_`. Nenhuma é obrigatória, exceto as credenciais das fontes que
exigem cadastro (USDA, MapBiomas Alerta e a rota observacional do INMET).

## Quando valem

Defina as variáveis antes de importar o agrobr. Em detalhe:

- os timeouts (`AGROBR_HTTP_TIMEOUT_*`) e o usuário e a senha da CEASA são lidos na importação de cada cliente, exceto o
  teto do download do Comex Stat, lido a cada download;
- a pasta e o nome do banco do CEPEA são lidos no primeiro uso do cache e valem até o fim do processo; a conexão com o
  banco abre e fecha a cada operação. Os outros caches (ZARC, ANEC, Acervo Fundiário, IBAMA, RNC e Agrofit) leem a
  pasta a cada consulta;
- o limite de concorrência de uma fonte (`AGROBR_HTTP_MAX_CONCURRENT_*`) é lido no primeiro pedido a ela no processo;
- as demais são lidas a cada chamada.

## Cache e disco

| Variável | Padrão | O que faz |
|---|---|---|
| `AGROBR_CACHE_DIR` | `~/.agrobr/cache` | Pasta de todos os caches ([o que vai para o disco](disco.md)). Vazia vale o padrão |
| `AGROBR_CACHE_CACHE_DIR` | — | Nome antigo de `AGROBR_CACHE_DIR`, aceito como alias. Com as duas definidas, vale `AGROBR_CACHE_DIR`; se apontarem para pastas diferentes, sai um `UserWarning` com a pasta usada. O argumento `cache_dir` do `CacheSettings` vence as duas, e o aviso diz isso |
| `AGROBR_CACHE_DB_NAME` | `agrobr.duckdb` | Nome do banco DuckDB do CEPEA, dentro da pasta de cache |
| `AGROBR_ANEC_CACHE_DISABLED` | desligada | Booleano. Ligada, a ANEC não lê nem grava os PDFs no cache |
| `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED` | desligada | Booleano. Ligada, o Acervo Fundiário não lê nem grava os ZIPs no cache, como `use_cache=False` |
| `AGROBR_ANEC_LIST_TTL` | `300` | Segundos em que a lista de boletins da ANEC fica em memória; `0` desliga. Valor que não é número, negativo ou infinito vale `300`, com aviso |

`CacheSettings(cache_dir=...)` no código vence as variáveis.

## HTTP

| Variável | Padrão | O que faz |
|---|---|---|
| `AGROBR_HTTP_TIMEOUT_CONNECT` | `10` | Segundos para abrir a conexão |
| `AGROBR_HTTP_TIMEOUT_READ` | `30` | Segundos de leitura. Vale como mínimo: o cliente da fonte que fixa um tempo maior fica com o dele ([resiliência](resilience.md#configuracao-http-centralizada)). Vale também no download da ANTAQ, que usa o requests (os timeouts de escrita e de pool não se aplicam a ele) |
| `AGROBR_HTTP_TIMEOUT_WRITE` | `10` | Segundos de escrita |
| `AGROBR_HTTP_TIMEOUT_POOL` | `10` | Segundos de espera por uma conexão livre |
| `AGROBR_HTTP_TIMEOUT_DOWNLOAD_COMEXSTAT` | `300` | Teto, em segundos, do download inteiro de um arquivo do Comex Stat, novas tentativas incluídas. O `AGROBR_HTTP_TIMEOUT_READ` não mexe nele; suba este em rede lenta. Número finito maior que `0`; o resto é recusado com `pydantic.ValidationError` |
| `AGROBR_HTTP_MAX_RETRIES` | `3` | **Total** de tentativas por pedido, contando a primeira. `0` vale `1` (sem retry); negativo é recusado com `pydantic.ValidationError` |
| `AGROBR_HTTP_RETRY_BASE_DELAY` | `1.0` | Primeira espera entre tentativas, em segundos |
| `AGROBR_HTTP_RETRY_MAX_DELAY` | `30.0` | Teto da espera entre tentativas, em segundos |
| `AGROBR_HTTP_RETRY_EXPONENTIAL_BASE` | `2` | Fator da espera a cada nova tentativa |
| `AGROBR_HTTP_RATE_LIMIT_<FONTE>` | por fonte | Intervalo mínimo, em segundos, entre pedidos à fonte, no processo inteiro. Número finito maior ou igual a `0` (`0` não espera); negativo, `inf` ou `nan` é recusado com `pydantic.ValidationError` |
| `AGROBR_HTTP_RATE_LIMIT_DEFAULT` | `1.0` | Intervalo das fontes sem variável própria, com a mesma validação |
| `AGROBR_HTTP_MAX_CONCURRENT_<FONTE>` | por fonte | Pedidos simultâneos à fonte. Só existe para `ANA` (1), `ANP_DIESEL` (3), `B3` (3) e `IBGE` (3); valor menor que 1 é recusado com `pydantic.ValidationError` |
| `AGROBR_HTTP_MAX_CONCURRENT_DEFAULT` | `1` | Pedidos simultâneos das demais fontes; menor que 1 é recusado com `pydantic.ValidationError` |

`<FONTE>` do intervalo, com o padrão em segundos: `ABIOVE` (3), `ACERVO_FUNDIARIO` (3), `ANA` (2), `ANDA` (3), `ANEC` (3),
`ANP_DIESEL` (2), `ANTAQ` (1), `ANTT_PEDAGIO` (2), `B3` (1), `B3_ARQUIVOS` (5), `BCB` (1), `CEPEA` (5), `CFTC` (2), `CNUC` (2),
`COMEXSTAT` (2), `COMTRADE` (2), `CONAB` (3), `CONAB_CEASA` (2), `DEFENSIVOS` (2), `DERAL` (3), `DESMATAMENTO` (2),
`EMBRAPA_SOLOS` (2), `FUNAI` (2), `IBAMA` (2), `IBGE` (1), `ICMBIO` (2), `IMEA` (1), `INCRA` (2), `INMET` (0.5),
`LISTA_SUJA` (2), `MAPBIOMAS` (2), `MAPBIOMAS_ALERTA` (3), `NASA_POWER` (1), `NOTICIAS_AGRICOLAS` (2), `QUEIMADAS` (1),
`RIO_VERDE` (3), `RNC` (3), `SFB` (2), `SICAR` (2), `UNICA` (3), `USDA` (1) e `ZARC` (2). Variável de fonte fora das listas
não tem efeito. O detalhe do retry e da concorrência está em [Resiliência](resilience.md).

O `HTTPSettings` recusa texto em variável numérica, `AGROBR_HTTP_MAX_RETRIES` negativo, `AGROBR_HTTP_RATE_LIMIT_<FONTE>`
negativo ou não finito, `AGROBR_HTTP_MAX_CONCURRENT_<FONTE>` menor que 1 e `AGROBR_HTTP_TIMEOUT_DOWNLOAD_COMEXSTAT` que não
seja positivo e finito. Essa recusa vem já no `import agrobr`, porque cada cliente monta as configurações HTTP ao ser
importado; definido depois do import, o valor é recusado no pedido seguinte. A exceção é a `pydantic.ValidationError`, que
não herda de `AgrobrError` e não é a `agrobr.exceptions.ValidationError`. `AGROBR_HTTP_RETRY_BASE_DELAY` e
`AGROBR_HTTP_RETRY_MAX_DELAY` negativos passam no import e levantam `InvalidParameterError` no primeiro pedido que passa
pelo retry.

## Credenciais

| Variável | Fonte | O que faz |
|---|---|---|
| `AGROBR_INMET_TOKEN` | INMET | Token da API observacional (`inmet.estacao`, `inmet.clima_uf`). Os ZIPs históricos não precisam dele |
| `AGROBR_USDA_API_KEY` | USDA PSD | Chave obrigatória do `usda.psd`. O argumento `api_key=` vence a variável |
| `AGROBR_COMTRADE_API_KEY` | UN Comtrade | Chave opcional; sem ela, a consulta usa o preview público. O argumento `api_key=` vence a variável |
| `AGROBR_MAPBIOMAS_ALERTA_TOKEN` | MapBiomas Alerta | Token obrigatório. O argumento `token=` vence a variável |
| `AGROBR_BQ_BILLING_PROJECT` | BCB/SICOR | Projeto GCP de cobrança do BigQuery, no fallback do crédito rural (extra `[bigquery]`). Sem ela, vale o `billing_project_id` do basedosdados |
| `AGROBR_CONAB_CEASA_USER` e `AGROBR_CONAB_CEASA_PASS` | CONAB CEASA | Usuário e senha do serviço de preços da CEASA. O padrão é o acesso público; defina só se o seu ambiente exigir outro |

O agrobr não grava credencial em disco. Os valores destas variáveis (menos o `AGROBR_BQ_BILLING_PROJECT` e o
`AGROBR_CONAB_CEASA_USER`) e os dos webhooks e da chave de e-mail dos alertas saem como `[REDACTED]` nas mensagens de erro.

## Certificados

`SSL_CERT_FILE` (arquivo) ou `SSL_CERT_DIR` (pasta) troca as autoridades certificadoras; sem elas, vale o `certifi`. Os
clientes seguem a convenção do httpx, e o SICAR, a ComexStat e a FUNAI, que montam um contexto TLS próprio, leem as mesmas
variáveis. A ANTAQ baixa pelo `requests`, que não as lê: para ela, use `REQUESTS_CA_BUNDLE` (arquivo). Use em rede com
proxy que reassina o TLS.

## Alertas

Usadas pelo health check e pelo envio de alertas, que são infraestrutura interna, sem garantia de SemVer
([API pública](../api/index.md)).

| Variável | Padrão | O que faz |
|---|---|---|
| `AGROBR_ALERT_ENABLED` | `true` | Booleano. Desligada, nenhum alerta é enviado |
| `AGROBR_ALERT_SLACK_WEBHOOK` | — | URL do webhook do Slack |
| `AGROBR_ALERT_DISCORD_WEBHOOK` | — | URL do webhook do Discord |
| `AGROBR_ALERT_SENDGRID_API_KEY` | — | Chave do SendGrid para o e-mail |
| `AGROBR_ALERT_EMAIL_FROM` | `alerts@agrobr.dev` | Remetente do e-mail |
| `AGROBR_ALERT_EMAIL_TO` | vazia | Destinatários, em lista JSON: `'["a@exemplo.com"]'` |
| `AGROBR_ALERT_ALERT_ON_PARSE_ERROR`, `_LAYOUT_CHANGE`, `_SOURCE_DOWN`, `_ANOMALY`, `_SOFT_BLOCK` | `true` | Booleanos: alerta por categoria de falha. O nome repete `ALERT` (prefixo mais campo) |
| `AGROBR_ALERT_ALERT_ON_RECOVERY` | `true` | Booleano: alerta quando a fonte volta |
| `AGROBR_ALERT_CONSECUTIVE_FAILURES_WARNING` | `2` | Falhas seguidas até o alerta de aviso |
| `AGROBR_ALERT_CONSECUTIVE_FAILURES_CRITICAL` | `3` | Falhas seguidas até o alerta crítico |
| `AGROBR_ALERT_DISCORD_EMBED_CHAR_LIMIT` | `3900` | Teto de caracteres da mensagem no Discord |

## Booleanos

`AGROBR_ANEC_CACHE_DISABLED` e `AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED` ligam com `1`, `true`, `yes` ou `on` (e `t` ou `y`) e
desligam com `0`, `false`, `no` ou `off` (e `f` ou `n`), sem distinção de caixa. Só nessas 2 variáveis os espaços das pontas
saem, e ausente ou vazia vale desligada. Outro valor (por exemplo, `sim`) levanta `InvalidParameterError` na consulta.

Os booleanos dos alertas (`AGROBR_ALERT_ENABLED` e `AGROBR_ALERT_ALERT_ON_*`) aceitam os mesmos valores, sem distinção de
caixa, mas não tiram os espaços nem aceitam o vazio: `' YES '`, a variável vazia e qualquer outro valor levantam
`pydantic.ValidationError`.

## Ver a configuração

`agrobr config show` mostra a pasta do cache, o nome do banco, o timeout de leitura e o total de tentativas em vigor
([CLI](cli.md)).
