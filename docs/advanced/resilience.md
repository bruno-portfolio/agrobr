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
│  └─ Fingerprint Baseline       → Referência para comparação     │
│                                                                  │
│  CAMADA 2: DETECÇÃO                                             │
│  ├─ Fingerprint Check          → HTML estruturalmente diferente?│
│  ├─ can_parse() Confidence     → Parser reconhece estrutura?    │
│  └─ User-Agent Rotation        → Evita bloqueio de IP           │
│                                                                  │
│  CAMADA 3: VALIDAÇÃO                                            │
│  ├─ Pydantic Validation        → Tipos e formatos corretos?     │
│  ├─ Sanity Check               → Valores dentro do range?       │
│  └─ Completeness Check         → Dados parciais (< 80%)?        │
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
max_retries = 3
base_delay = 1.0  # segundos
max_delay = 30.0  # segundos
exponential_base = 2

# Delays: 1s → 2s → 4s (máx 30s)
```

**Status codes que acionam retry:**
- 408 Request Timeout
- 429 Too Many Requests
- 500 Internal Server Error
- 502 Bad Gateway
- 503 Service Unavailable
- 504 Gateway Timeout

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

A tabela mostra as principais fontes; cada uma das fontes suportadas tem seu próprio rate limit (default 1 segundo). Além do intervalo entre requisições, a concorrência por fonte é controlada por `max_concurrent_<fonte>` (default 1; B3 e IBGE usam 3), via semáforos que permitem requests paralelos a fontes diferentes.

## Configuração HTTP Centralizada

Todos os clients usam `HTTPSettings` (env prefix `AGROBR_HTTP_`):

```bash
# Timeouts (segundos)
export AGROBR_HTTP_TIMEOUT_CONNECT=10
export AGROBR_HTTP_TIMEOUT_READ=30
export AGROBR_HTTP_TIMEOUT_WRITE=10
export AGROBR_HTTP_TIMEOUT_POOL=10

# Retry
export AGROBR_HTTP_MAX_RETRIES=3
export AGROBR_HTTP_RETRY_BASE_DELAY=1.0
export AGROBR_HTTP_RETRY_MAX_DELAY=30.0
```

Via código:

```python
from agrobr.http import get_timeout

timeout = get_timeout()             # httpx.Timeout (defaults)
timeout = get_timeout(read=60.0)    # sobrescreve só o read timeout
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
3. ISO-8859-1 (Latin-1, comum em sites BR antigos)
4. UTF-16 (raro)
5. ASCII
6. Detecção automática (chardet, confiança > 0.7)
7. UTF-8 com replacement (último recurso)

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

## Fallback de Fonte

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

O cache local usa DuckDB. CEPEA expira às 18h (smart TTL); demais fontes usam TTL fixo por política (ex.: CONAB 24h, IBGE 7 dias). Quando o fetch falha, o cache stale é retornado com warning. Sem cache stale, o comportamento depende da camada: a fonte direta `cepea.indicador()` retorna um DataFrame vazio (com aviso no `MetaInfo`); os datasets (`datasets.*`, que tentam fontes em cascata) levantam `SourceUnavailableError` quando todas as fontes se esgotam.

### Fluxo de Cache

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

Detecta mudanças de layout antes que causem erros:

**Componentes da fingerprint:**
- Classes CSS das tabelas
- IDs relevantes (preço, indicador, etc.)
- Headers de tabelas
- Contagem de elementos estruturais
- Hash da hierarquia de tags

**Thresholds:**

| Similaridade | Ação |
|--------------|------|
| > 85% | OK, parsing normal |
| 70-85% | Warning, tenta parsing |
| < 70% | Erro, layout mudou muito |

## Validação Estatística

Sanity checks baseados em ranges históricos:

```python
# Exemplo: Soja
min_value = 30   # R$/sc (mínimo histórico ~R$40)
max_value = 300  # R$/sc (máximo histórico ~R$200)
max_daily_change = 15%  # Variação diária máxima
```

Anomalias são marcadas nos dados mas não bloqueiam retorno (soft validation).

## Health Checks

Verificações automáticas:

1. **Conectividade**: HTTP GET responde?
2. **Latência**: < 5 segundos?
3. **Parsing**: Parser extrai dados?
4. **Fingerprint**: Estrutura similar ao baseline?

### GitHub Actions

- **Daily Health Check**: 2x ao dia (9h e 21h BRT)
- **Structure Monitor**: A cada 6 horas
- **Tests**: Em cada PR

## Alertas

### Canais Suportados

```python
# Slack
export AGROBR_ALERT_SLACK_WEBHOOK=https://hooks.slack.com/...

# Discord
export AGROBR_ALERT_DISCORD_WEBHOOK=https://discord.com/api/webhooks/...

# Email (SendGrid)
export AGROBR_ALERT_SENDGRID_API_KEY=SG...
export AGROBR_ALERT_EMAIL_TO=["admin@example.com"]
```

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
agrobr diagnostics v1.0.2
==================================================

Sources Connectivity
  [OK] CEPEA (Noticias Agricolas)             142ms
  [OK] CONAB                                   89ms
  [OK] IBGE/SIDRA                              67ms

Cache Status
  Location:      ~/.agrobr/cache/agrobr.duckdb
  Size:          2.40 MB
  Total records: 1,247

  By source:
    CEPEA: 847 records (2025-01-21 to 2026-02-04)
    CONAB: 305 records (2024-01-01 to 2026-02-04)
    IBGE: 95 records (2020-01-01 to 2023-12-31)

Cache Expiry
  CEPEA: Expira as 18h (atualizacao CEPEA)
  CONAB: TTL 24 horas
  IBGE: TTL 7 dias

Configuration
  Alternative source: enabled (Notícias Agrícolas via httpx)

[OK] All systems operational
```

### Output JSON

Para integração com sistemas de monitoramento:

```bash
agrobr doctor --json
```

### Verbose

Para informações detalhadas:

```bash
agrobr doctor --verbose
```
