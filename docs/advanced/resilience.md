# Resiliência e Fallbacks

O agrobr foi projetado para ser robusto e resiliente a falhas. Este documento explica as camadas de defesa implementadas.

## Camadas de Defesa

```
┌─────────────────────────────────────────────────────────────────┐
│                    CAMADAS DE DEFESA - AGROBR                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  CAMADA 1: PREVENÇÃO                                            │
│  ├─ Structure Monitor (6h)     → Detecta mudanças antecipadas   │
│  ├─ Golden Data Tests (CI)     → Garante parsing não regride    │
│  └─ Fingerprint Baseline       → Referência do health --deep    │
│                                                                  │
│  CAMADA 2: DETECÇÃO                                             │
│  ├─ Coluna de valor em R$      → Parser recusa a página sem ela │
│  ├─ Fingerprint                → Só no health --deep e no CI    │
│  ├─ can_parse() Confidence     → Parser reconhece estrutura?    │
│  └─ User-Agent Rotation        → Evita bloqueio de IP           │
│                                                                  │
│  CAMADA 3: VALIDAÇÃO                                            │
│  ├─ Pydantic Validation        → Tipos e formatos corretos?     │
│  └─ Sanity Check (opcional)    → validate_sanity=True           │
│                                                                  │
│  CAMADA 4: FALLBACK                                             │
│  ├─ Parser Cascade             → Tenta próximo parser           │
│  ├─ Cache Fallback             → Retorna cache stale            │
│  └─ Source Fallback            → Fonte alternativa (NA)         │
│                                                                  │
│  CAMADA 5: ALERTAS                                              │
│  ├─ Multi-canal                → Slack, Discord, Email          │
│  ├─ GitHub Issue               → Tracking automático            │
│  └─ Logging Estruturado        → Debug facilitado               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Retry com Exponential Backoff

Todas as requisições HTTP usam retry automático:

```python
# Configuração padrão
max_retries = 3  # total de tentativas por pedido, contando a primeira
base_delay = 1.0  # segundos
max_delay = 30.0  # segundos
exponential_base = 2

# 3 tentativas, com esperas de 1s → 2s (máx 30s)
```

`max_retries` conta o total de tentativas, e não só as novas: o padrão 3 faz até 3 pedidos, com 2 esperas.
`AGROBR_HTTP_MAX_RETRIES=0` (ou `1`) faz um pedido só, sem retry; valor negativo é recusado na validação, com
`ValidationError`. No `retry_async` e no `with_retry`, `max_attempts=0` também vale uma tentativa, `base_delay=0` e
`max_delay=0` valem espera zero, e valor negativo levanta `InvalidParameterError`. Esgotadas as tentativas, a mensagem diz
`after N attempts`.

**Status codes que acionam retry:**
- 408 Request Timeout
- 429 Too Many Requests
- 500 Internal Server Error
- 502 Bad Gateway
- 503 Service Unavailable
- 504 Gateway Timeout

Além do status HTTP e do tamanho mínimo, as respostas são validadas antes de
chegar aos parsers. Clientes JSON convertem HTML de manutenção, bloqueios de WAF
e corpos vazios em `SourceUnavailableError`. Downloads conferem os magic bytes
de ZIP/XLSX, XLS e PDF; arquivos CSV rejeitam conteúdo que começa como HTML.
Assim, uma indisponibilidade da fonte não é confundida com quebra de layout.

## Rate Limiting

Cada fonte tem seu próprio rate limit, configurável via env vars:

| Fonte | Intervalo | Env var |
|-------|-----------|---------|
| ABIOVE | 3 segundos | `AGROBR_HTTP_RATE_LIMIT_ABIOVE` |
| ANDA | 3 segundos | `AGROBR_HTTP_RATE_LIMIT_ANDA` |
| BCB | 1 segundo | `AGROBR_HTTP_RATE_LIMIT_BCB` |
| CEPEA | 5 segundos | `AGROBR_HTTP_RATE_LIMIT_CEPEA` |
| ComexStat | 2 segundos | `AGROBR_HTTP_RATE_LIMIT_COMEXSTAT` |
| CONAB | 3 segundos | `AGROBR_HTTP_RATE_LIMIT_CONAB` |
| DERAL | 3 segundos | `AGROBR_HTTP_RATE_LIMIT_DERAL` |
| IBGE | 1 segundo | `AGROBR_HTTP_RATE_LIMIT_IBGE` |
| IMEA | 1 segundo | `AGROBR_HTTP_RATE_LIMIT_IMEA` |
| INMET | 0.5 segundo | `AGROBR_HTTP_RATE_LIMIT_INMET` |
| NASA POWER | 1 segundo | `AGROBR_HTTP_RATE_LIMIT_NASA_POWER` |
| Notícias Agrícolas | 2 segundos | `AGROBR_HTTP_RATE_LIMIT_NOTICIAS_AGRICOLAS` |
| USDA | 1 segundo | `AGROBR_HTTP_RATE_LIMIT_USDA` |
| ZARC | 2 segundos | `AGROBR_HTTP_RATE_LIMIT_ZARC` |
| Default | 1 segundo | `AGROBR_HTTP_RATE_LIMIT_DEFAULT` |

A tabela mostra as principais fontes; cada uma das fontes suportadas tem seu próprio rate limit (default 1 segundo). Quatro pedidos internos não têm variável própria e usam o `AGROBR_HTTP_RATE_LIMIT_DEFAULT`: o custo de produção e a série histórica da CONAB, o Censo Agro legado do IBGE (FTP) e o MAPA PSR; o `AGROBR_HTTP_RATE_LIMIT_CONAB` e o `AGROBR_HTTP_RATE_LIMIT_IBGE` não valem para eles. Além do intervalo entre requisições, a concorrência por fonte é controlada por `AGROBR_HTTP_MAX_CONCURRENT_<FONTE>` só em quatro fontes: ANA (1), ANP Diesel (3), B3 (3) e IBGE (3); as demais usam `AGROBR_HTTP_MAX_CONCURRENT_DEFAULT` (1), e a variável de outra fonte (por exemplo, `AGROBR_HTTP_MAX_CONCURRENT_CFTC`) não tem efeito. Valor menor que 1 é recusado na validação, com `ValidationError`. A concorrência vale via semáforos que permitem requests paralelos a fontes diferentes. O intervalo e a concorrência valem para o processo inteiro: entre chamadas do `agrobr.sync` (cada uma com o seu `asyncio.run`), entre loops e entre threads. A espera por vaga entre threads tem teto (`AGROBR_HTTP_TIMEOUT_READ`); no teto, o pedido segue com aviso, e o intervalo continua valendo.

## Configuração HTTP Centralizada

Todos os clients usam `HTTPSettings` (env prefix `AGROBR_HTTP_`). A lista completa, com os padrões, está em
[Variáveis de ambiente](ambiente.md).

```bash
# Timeouts (segundos)
export AGROBR_HTTP_TIMEOUT_CONNECT=10
export AGROBR_HTTP_TIMEOUT_READ=30
export AGROBR_HTTP_TIMEOUT_WRITE=10
export AGROBR_HTTP_TIMEOUT_POOL=10

# Retry (MAX_RETRIES é o total de tentativas; 0 ou 1 = sem retry)
export AGROBR_HTTP_MAX_RETRIES=3
export AGROBR_HTTP_RETRY_BASE_DELAY=1.0
export AGROBR_HTTP_RETRY_MAX_DELAY=30.0
```

Os clientes das fontes fixam a leitura acima do padrão de 30 s: ComexStat 120 s; ZARC, PSR e SICAR 180 s;
INMET 600 s; as demais, entre 30 e 300 s, no `get_timeout(read=...)` de cada `client.py`.
`AGROBR_HTTP_TIMEOUT_READ` vale como mínimo: maior que o do cliente, prevalece; menor, fica o do cliente.
`CONNECT`, `WRITE` e `POOL` valem em todos os clientes HTTP das fontes.

Via código:

```python
from agrobr.http import get_timeout

timeout = get_timeout()             # httpx.Timeout (defaults)
timeout = get_timeout(read=60.0)    # leitura de 60 s, ou a do AGROBR_HTTP_TIMEOUT_READ, se maior
```

## User-Agent Rotativo

Pool de User-Agents reais e atuais:

- Chrome Windows (múltiplas versões)
- Chrome Mac
- Firefox Windows/Mac
- Edge
- Safari

Rotação determinística por fonte para parecer tráfego natural.

## Fallback de Encoding

Chain de fallback para encoding:

1. UTF-8 (padrão)
2. Windows-1252 (CP1252, padrão Excel BR — superset do Latin-1, vem antes porque ISO-8859-1 decodifica qualquer byte e mataria o resto da chain)
3. ISO-8859-1 (Latin-1, comum em sites BR antigos). Decodifica qualquer sequência de bytes, então a chain termina nele:
   nenhum passo depois dele rodaria.

## Fallback de Engine Excel

Planilhas XLSX de fontes governamentais podem conter estilos/fills malformados
que crasham o openpyxl (bug conhecido desde 2021, sem fix upstream).

O agrobr usa fallback automático para `python-calamine` (engine Rust, MIT):

```
openpyxl (estilos + dados)
        ↓ falhou (stylesheet malformado)?
calamine (ignora estilos, extrai só dados)
        ↓ falhou?
ParseError
```

Guard xlrd: arquivos OLE2/BIFF (.xls) usam xlrd direto, sem fallback calamine.

Helpers: `open_excel_safe()` (multi-sheet) e `read_excel_safe()` (single-sheet)
em `agrobr/utils/io.py`.

## Teto de expansão (ZIP e XLSX)

Um arquivo compactado pequeno pode expandir para gigabytes. Antes de descomprimir, o agrobr confere quanto cada membro de ZIP
e cada XLSX expande, contra um teto por fonte (`constants.MAX_EXPANDED_BYTES`), e levanta `ResourceLimitError` se passar:

- **ZIP** (Queimadas, B3, MapBiomas, ANTAQ e o Censo legado do IBGE): o membro passa por `read_zip_member` ou
  `open_zip_member`, que conferem o tamanho declarado. O `zipfile` não entrega mais do que o declarado: um membro que expande
  além dele falha no CRC.
- **XLSX** (`read_excel_safe`, `open_excel_safe`, a série do CEPEA, o MapBiomas municipal e a UNICA): `check_xlsx_expansion`
  soma o tamanho declarado dos membros e confere o CRC de cada um em stream, antes do leitor de planilha. O calamine não
  respeita o tamanho declarado: sem o CRC, um XLSX com o tamanho forjado passaria.

| Fonte | Teto | Maior arquivo publicado (em 27/09/2026) |
|---|---|---|
| Queimadas | 2 GiB | CSV anual de 2024: 905 MB |
| B3 | 512 MiB | ZIP interno de 13 MB; XML de 144 MB |
| MapBiomas | 1 GiB | XLSX municipal: 267 MB |
| ANTAQ | 4 GiB | não medido: o site está fora do ar desde 23/06/2026 |
| IBGE (Censo legado, FTP) | 16 MiB | 122 KB |
| ANP | 512 MiB | planilha de preços 2022–2023: 152 MB |
| ABIOVE, UNICA, CONAB e progresso da CONAB | 64 MiB | 0,8 MB, 1,0 MB, 3,9 MB e 0,2 MB |
| Demais fontes | 256 MiB | a série do CEPEA, a DERAL e a série histórica da CONAB publicam XLS, que não é compactado |

O custo de produção da CONAB e o histórico do INMET têm teto próprio (`CONAB_CUSTOS_MAX_EXPANDED_BYTES` e
`INMET_HISTORICO_MAX_*`). PDF não tem teto: o pdfplumber descomprime cada stream inteiro.

## Pedido fora do host da fonte

`conab.progresso_safra(semana_url=...)` só segue páginas em `https://www.gov.br/conab/`. Outra URL, ou um redirecionamento ou
link que saia dela, levanta `InvalidParameterError` antes de o pedido sair. Numa aplicação que repassa a URL do usuário, isso
fecha o pedido a endereço interno.

## Fallback de Fonte

Quando a fonte primária de um dataset falha e uma fonte seguinte responde, o
agrobr emite `SourceFallbackWarning` com a fonte primária, a categoria e o resumo
do erro, além do fallback selecionado. O aviso usa `warnings.warn`, portanto pode
ser capturado ou filtrado pelas ferramentas padrão do Python e segue para stderr.

### CEPEA

```
CEPEA (www.cepea.org.br)
        ↓ bloqueado (Cloudflare)?
Notícias Agrícolas (httpx direto, SSR)
        ↓ soft block (consent/challenge page)?
        ↓ falhou (HTTP error)?
Cache local (DuckDB)
```

O Notícias Agrícolas republica os mesmos indicadores CEPEA/ESALQ via HTML server-side rendered, sem necessidade de Playwright.

Cada etapa retorna um `FetchResult(html, source)` que identifica explicitamente a origem do HTML ("cepea", "browser" ou "noticias_agricolas"), evitando detecção frágil por markers no conteúdo.

**Soft block detection:** Alguns usuários recebem do NA uma página de consent/challenge (HTTP 200, ~10KB sem tabela) em vez da página de dados (~75KB com tabela). O client NA valida o conteúdo antes de retornar: se o HTML é < 20KB e não contém `<table`, levanta `SourceUnavailableError`, ativando o cache fallback.

## Cache

O cache local usa DuckDB e guarda só os indicadores do CEPEA, com as linhas do fallback Notícias Agrícolas. Eles vencem na primeira virada das 18h BRT (21h UTC) em dia útil depois da coleta (smart TTL). As demais fontes não usam esse cache: IBGE, BCB, ComexStat e SICAR consultam a fonte a cada chamada, e as que têm cache próprio (INMET, ZARC, RNC e o catálogo de custos da CONAB, entre outras) o descrevem na página delas. Quando o fetch do CEPEA falha, o cache stale é retornado com `StaleDataWarning`. Sem rede e sem cache, o comportamento depende da camada: a fonte direta `cepea.indicador()` levanta `SourceUnavailableError` (até a 1.1.0, devolvia a tabela vazia); os datasets (`datasets.*`, que tentam fontes em cascata) levantam `SourceUnavailableError` quando todas as fontes se esgotam.

### Fluxo de Cache

O fallback interno para cache também emite `SourceFallbackWarning` nos datasets;
convertê-lo em erro interrompe a consulta. Cache quente/offline não gera uma nova
tentativa de fallback. `MetaInfo.selected_source="cache"` identifica o caminho
local, enquanto `data_sources` preserva as fontes das linhas retornadas.

Falhas de migração são diferentes de indisponibilidade de abertura do cache:
`CacheMigrationError` interrompe o acesso, sem degradar silenciosamente para vazio.
A conexão fica aberta só durante cada operação, e outro processo (um 2º notebook,
um worker) usa o mesmo cache; o upsert grava tudo ou nada. Sem acesso ao arquivo
(pasta sem escrita, disco cheio, outro processo gravando), a operação segue sem
cache, e o agrobr avisa uma vez (`UserWarning`) com o caminho, o motivo e a dica
`AGROBR_CACHE_DIR`. Banco ilegível (leitura incompleta, checksum ou arquivo inválido) vai para o lado
como `agrobr.duckdb.corrompido-<AAAAMMDDHHMM>`, com aviso, e a consulta seguinte cria um banco
novo ([o que o agrobr grava no disco](disco.md)).
Migrações pendentes preservam os originais em quarentena e só retiram linhas da
área ativa na mesma transação que registra a versão. Veja [preservação e
recuperação](../guides/migracao-2.md#18-preservacao-automatica-do-cache-existente).

```
Request
   │
   ▼
Cache fresh? ──yes──→ Retorna cache
   │no
   ▼
Fetch fonte
   │
   ├─success──→ Atualiza cache
   │
   └─fail──→ Cache stale? ──yes──→ Retorna stale + warning
                 │no
                 ▼
           cepea.indicador() → DataFrame vazio
           datasets.* → SourceUnavailableError
```

## Fingerprinting de Layout

Compara a estrutura da página do CEPEA com uma baseline. Roda só no `agrobr health --deep` e no
Structure Monitor do CI: a coleta (`cepea.indicador`, `datasets.preco_diario`) não compara
fingerprint. Na coleta, a defesa contra mudança de layout é o parser: sem a coluna de valor em R$
reconhecida pelo cabeçalho, ele levanta `ParseError`, e a consulta segue para a Notícias Agrícolas
(quando habilitada) e depois para o cache. Um cabeçalho em US$ nunca vira preço em reais.

**Componentes da fingerprint:**
- Classes CSS das tabelas
- IDs relevantes (preço, indicador, etc.)
- Headers de tabelas
- Contagem de elementos estruturais
- Hash da hierarquia de tags

**Thresholds do `health --deep`:**

| Similaridade | Resultado do check |
|--------------|------|
| > 85% | `ok` |
| 70-85% | `warning` (drift) |
| < 70% | `failed` (layout mudou muito) |

A baseline do `health --deep` vai no pacote (`agrobr/health/baselines/cepea_baseline.json`, a página
da soja de 27/09/2026), e o check vale de qualquer pasta. Sem ela, ou quando a página veio da
Notícias Agrícolas, o check sai `warning` com o motivo, sem comparar.

## Validação Estatística

Os 22 identificadores CEPEA têm regras de unidade e faixa de valor. Para ativar
essa conferência na API, use `cepea.indicador(produto, validate_sanity=True)`.
Uma unidade incompatível gera `unit_mismatch` antes da comparação numérica;
não há conversão implícita de moeda, peso ou centavos.

```python
from agrobr.validators.sanity import PRICE_RULES

regra = PRICE_RULES["soja"]
print(regra.expected_unit)         # BRL/sc60kg
print(regra.min_value)             # 30
print(regra.max_value)             # 300
print(regra.max_daily_change_pct)  # 15
```

| Produto | Unidade esperada | Faixa inclusiva | Variação temporal máxima |
|---|---|---|---|
| `soja`, `soja_parana` | BRL/sc60kg | 30–300 | 15% |
| `milho` | BRL/sc60kg | 15–150 | 15% |
| `cafe`, `cafe_arabica` | BRL/sc60kg | 200–3000 | 10% |
| `cafe_robusta` | BRL/sc60kg | 100–3000 | 10% |
| `bezerro` | BRL/cabeca | 800–8000 | 10% |
| `boi`, `boi_gordo` | BRL/@ | 100–500 | 10% |
| `trigo` | BRL/ton | 20 × 1000/60 a 150 × 1000/60 | 15% |
| `algodao` | cBRL/lb | 50 × 100 × 0,45359237/15 a 250 × 100 × 0,45359237/15 | 10% |
| `arroz` | BRL/sc50kg | 8–300 | sem limite |
| `acucar` | BRL/sc50kg | 8–400 | sem limite |
| `acucar_refinado` | BRL/kg | 0,2–8 | sem limite |
| `frango_congelado`, `frango_resfriado` | BRL/kg | 0,6–20 | sem limite |
| `suino` | BRL/kg | 0,8–30 | sem limite |
| `etanol_hidratado` | BRL/L | 0,1–8 | sem limite diário; série semanal |
| `etanol_anidro` | BRL/L | 0,1–10 | sem limite diário; série semanal |
| `leite` | BRL/L | 0,1–8 | sem limite diário; série mensal |
| `laranja_industria`, `laranja_in_natura` | BRL/cx40.8kg | 4–300 | sem limite |

As faixas são escolhas de engenharia e precisam ser revistas quando o mercado
muda. Para arroz, açúcares, frangos, suíno, etanóis e leite, os novos limites usam
metade do mínimo positivo e o dobro do máximo do histórico oficial disponível
em setembro de 2026, arredondados para fora a um algarismo significativo. Os
zeros publicados no histórico de leite não entram nessa calibração: o modelo
`Indicador` já exige preço positivo.

Citros usa referências pontuais oficiais de 2014/2015 e 2024; sua faixa não
representa uma varredura histórica completa. As nove regras preexistentes não
foram recalibradas. Soja Paraná herda a política de faixa da soja, conservando
sua série regional; café arábica é um alias de café. Nenhum limite de variação
foi inferido para os outros onze produtos novos.

A comparação temporal separa produto, praça e unidade; frangos, etanóis e
laranjas continuam séries distintas. As faixas não são intervalos de confiança
nem garantem detectar todo erro de escala. `validate_sanity=True` acrescenta
anomalias ao retorno sem bloqueá-lo, com um resumo das linhas marcadas (quantas e quais
regras) em `MetaInfo.validation_warnings` e `UserWarning`; o helper `validate_batch(..., strict=True)`
rejeita anomalias críticas. Regras customizadas podem omitir `expected_unit` para
preservar o comportamento anterior.

## Health Checks

Verificações automáticas:

1. **Conectividade**: HTTP GET responde?
2. **Latência**: < 5 segundos?
3. **Parsing**: Parser extrai dados?
4. **Fingerprint** (CEPEA, só com `--deep`): estrutura similar à baseline do pacote? Sem
   baseline, ou com a página vinda da Notícias Agrícolas, sai `warning` com o motivo.

Consultas HTTP que levam mais de 5 segundos são repetidas uma vez. O health usa
a menor latência e registra a primeira medição em `cold_start_ms`, evitando que o
cold start de serviços como o ArcGIS da ANA seja contado como falha. O Comtrade é
sondado pelo endpoint público guest, sem exigir chave de API.

O workflow persiste os contadores no DuckDB, fecha o store antes de salvar o cache
e envia a quantidade de falhas anterior nos alertas de recuperação. Se o store não
puder ser aberto por corrupção, lock ou permissão, o run informa a degradação e
termina com erro em vez de zerar os contadores silenciosamente.

### GitHub Actions

- **Daily Health Check**: 2x ao dia (9h e 21h BRT)
- **Structure Monitor**: A cada 6 horas
- **Tests**: Em cada PR
- **Reconciliação semanal**: segundas às 6h BRT (abaixo)

### Reconciliação semanal

O workflow `reconciliacao.yml` roda os `scripts/reconciliar_*.py` contra as fontes oficiais, um
por vez, e dá a cada fonte um estado:

- `ok`;
- `mismatch`;
- `indisponível`: a fonte está fora do ar ou bloqueou o runner;
- `não verificado`: falta a credencial, ou o script está fora da execução semanal (sem modo ao
  vivo, por exemplo). Nunca é sucesso;
- `erro do script`.

O resumo fica como artefato da execução. O job falha com `mismatch` ou `erro do script`.
Localmente, `python -m scripts.reconciliacao_semanal imea` roda uma fonte e grava em
`reports/reconciliacao_semanal/` o resumo e o JSON completo do script.

Cada fonte com `mismatch` ganha a issue "Reconciliação semanal: mismatch em <fonte>". Enquanto
ela estiver aberta, as semanas seguintes comentam nela. A issue traz só o estado, a contagem, os
identificadores dos casos e o link do artefato. O detalhe, que pode ter valores da fonte, sai da
execução local.

## Alertas

### Canais Suportados

```bash
# Slack
export AGROBR_ALERT_SLACK_WEBHOOK=https://hooks.slack.com/...

# Discord
export AGROBR_ALERT_DISCORD_WEBHOOK=https://discord.com/api/webhooks/...

# Email (SendGrid); a lista vai em JSON, entre aspas simples no shell
export AGROBR_ALERT_SENDGRID_API_KEY=SG...
export AGROBR_ALERT_EMAIL_TO='["admin@example.com"]'
```

A URL do webhook do Slack e do Discord é a própria credencial. Com o log de `INFO` do `httpx` ligado pela aplicação, o
agrobr a troca por `[REDACTED]` na linha `HTTP Request` do envio do alerta. A mensagem de erro de resposta não JSON também
mascara as credenciais do agrobr, do ambiente ou passadas por argumento.

### Níveis de Alerta

| Nível | Trigger | Canais |
|-------|---------|--------|
| Info | Health check OK | Logs apenas |
| Warning | Fingerprint drift, cache stale | Slack/Discord |
| Critical | Parse failed, fonte down | Todos + GitHub Issue |

## Modo Offline

Para trabalhar sem conexão:

```python
df = await cepea.indicador('soja', offline=True)
```

Usa apenas o cache local.

## Comando Doctor

Use o comando `doctor` para diagnosticar saúde do sistema:

```bash
agrobr doctor
```

### Exemplo de saída

```
agrobr diagnostics v2.0.0
==================================================

Sources Connectivity
  [OK] CEPEA (Noticias Agricolas)             142ms
  [OK] CONAB                                   89ms
  [OK] IBGE/SIDRA                              67ms

Cache Status
  Location:      ~/.agrobr/cache/agrobr.duckdb
  Size:          2.40 MB
  Total records: 1,152

  By source:
    CEPEA: 847 records (2025-01-21 to 2026-02-04)
    NOTICIAS_AGRICOLAS: 305 records (2024-01-01 to 2026-02-04)

Cache Expiry
  CEPEA: Expira às 18h BRT (atualização CEPEA)

Configuration
  Alternative source: enabled (Notícias Agrícolas via httpx)

[OK] All systems operational
```

### Output JSON

Para integração com sistemas de monitoramento:

```bash
agrobr doctor --formato json
```

### Verbose

Para informações detalhadas:

```bash
agrobr doctor --verbose
```
