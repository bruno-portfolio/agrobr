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
│  ├─ History Fallback           → Busca histórico permanente     │
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

Cada fonte tem seu próprio rate limit:

| Fonte | Intervalo |
|-------|-----------|
| CEPEA | 2 segundos |
| CONAB | 3 segundos |
| IBGE | 1 segundo |
| Notícias Agrícolas | 2 segundos |

O rate limiter usa semáforos por fonte, permitindo requests paralelos a fontes diferentes.

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
2. ISO-8859-1 (Latin-1, comum em sites BR antigos)
3. Windows-1252 (CP1252, padrão Excel BR)
4. UTF-16 (raro)
5. Detecção automática (chardet)
6. UTF-8 com replacement (último recurso)

## Fallback de Fonte

### CEPEA

```
CEPEA (www.cepea.org.br)
        ↓ bloqueado?
Playwright (browser headless)
        ↓ ainda bloqueado?
Notícias Agrícolas (fallback)
```

O Notícias Agrícolas republica os mesmos indicadores CEPEA/ESALQ.

## Cache e Histórico

### Cache Volátil
- TTL configurável por fonte
- Usado para respostas rápidas
- Expira automaticamente

### Histórico Permanente
- Nunca expira
- Acumula dados progressivamente
- Permite reconstruir séries históricas
- Útil em modo offline

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
   ├─success──→ Atualiza cache + histórico
   │
   └─fail──→ Cache stale? ──yes──→ Retorna stale + warning
                 │no
                 ▼
           Histórico? ──yes──→ Retorna histórico + warning
                 │no
                 ▼
           SourceUnavailableError
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
# Via código
df = await cepea.indicador('soja', offline=True)

# Via ambiente
export AGROBR_CACHE_OFFLINE_MODE=true
```

Usa apenas cache e histórico local.

## Telemetria (Opt-in)

Coleta anônima para melhorar o pacote:

```bash
export AGROBR_TELEMETRY_ENABLED=true
```

**O que é coletado:**
- Fontes mais usadas
- Produtos mais consultados
- Taxa de cache hit/miss
- Erros de parsing (tipo, frequência)
- Latência de requests

**Nunca coletamos:**
- IPs ou dados pessoais
- Conteúdo dos dados
- Informações identificáveis
