# ANEC — Embarques Semanais de Grãos e Oleaginosas

> **Licença:** Sem termos de uso públicos localizados. Sem contato formal com a
> associação. Classificação: `zona_cinza`

!!! warning "Status zona-cinza"
    A primeira chamada por sessão emite um `UserWarning` alertando que ANEC
    publica os dados sem termos explícitos. Verifique diretamente com a ANEC
    antes de uso comercial. ANEC não está em fallback automático de outros
    datasets.

Associação Nacional dos Exportadores de Cereais. Publica relatórios semanais em
PDF com embarques por porto de soja, farelo, milho, DDGS, sorgo e trigo.

## Cobertura

- **Frequência:** semanal (publicação às quartas-feiras)
- **Anos suportados:** 2026+ (`MIN_YEAR=2026`). Anos anteriores têm layout
  diferente — `NotImplementedError` com instrução de update
- **Produtos:** Soybean, Soybean Meal, Maize, DDGS, Sorghum, Wheat
- **Portos:** 19 portos brasileiros (Santos, Paranaguá, Rio Grande, etc.)

## API

```python
from agrobr import anec

# Embarques semanais (porto x produto x período: efetivado/programado)
df = await anec.embarques(ano=2026)

# Semana específica
df = await anec.embarques(ano=2026, semana=13)

# Filtros: porto (case/acento-insensitive), produto, tipo
df = await anec.embarques(ano=2026, porto="paranagua", produto="soja")
df = await anec.embarques(ano=2026, tipo="efetivado")  # ou "programado"

# Agregados mensais
df = await anec.embarques_mensais(ano=2026, produto="soja")

# Comparação 2025 x 2026 por mês x produto
df = await anec.comparacao_anual(ano=2026)

# Top destinos por produto (share %)
df = await anec.destinos(ano=2026, produto="soybean")

# Listar artigos disponíveis no ano
items = await anec.articles_disponiveis(2026)
```

## Schema — `embarques()`

| Coluna | Tipo | Descrição |
|---|---|---|
| `porto` | str | Nome canônico do porto (UPPER) |
| `produto` | str | `soybean`, `soybean_meal`, `maize`, `wheat`, `ddgs`, `sorghum` |
| `periodo` | str | `last_week` (efetivado) ou `current_week` (programado) |
| `valor_ton` | float | Volume embarcado (toneladas), `NaN` quando vazio |

## Schema — `embarques_mensais()`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência |
| `mes` | int | Mês (1-12) |
| `produto` | str | Mesma lista de embarques() |
| `valor_ton` | float | Volume mensal acumulado |
| `eh_estimativa` | bool | `True` quando o mês traz `*` (programação ainda em curso) |

## Schema — `comparacao_anual()`

| Coluna | Tipo | Descrição |
|---|---|---|
| `mes` | int | Mês (1-12) |
| `produto` | str | Produto canônico |
| `valor_2025` | float | Volume mensal 2025 (referência) |
| `valor_2026` | float | Volume mensal 2026 (atual ou estimativa) |

## Schema — `destinos()`

| Coluna | Tipo | Descrição |
|---|---|---|
| `produto` | str | Produto canônico |
| `destino` | str | País de destino (UPPER) |
| `share_pct` | float | % do total exportado do produto (0-100) |

## Aliases de produto aceitos

| Input | Canônico |
|---|---|
| `soja`, `soybean`, `soybeans`, `soja grao` | `soybean` |
| `farelo`, `farelo de soja`, `meal`, `soybean meal` | `soybean_meal` |
| `milho`, `maize`, `corn` | `maize` |
| `trigo`, `wheat` | `wheat` |
| `sorgo`, `sorghum` | `sorghum` |
| `ddgs` | `ddgs` |

## Cache

PDF cacheado em `~/.agrobr/cache/anec/{year}/week_{NN}/` com:
- `shipment.pdf` — bytes do PDF
- `meta.json` — metadata + SHA256 + `media_updated_at` da fonte

Stale detection compara `media_updated_at` cached vs ANEC. ANEC revisa
retroativamente: cache antigo invalida automaticamente quando `updated_at`
remoto avança.

Listagem JSON é cacheada em memória por 5 minutos (configurável via
`AGROBR_ANEC_LIST_TTL`). Cache de PDF disk pode ser desabilitado via
`AGROBR_ANEC_CACHE_DISABLED=1`.

## MetaInfo

```python
df, meta = await anec.embarques(ano=2026, return_meta=True)
print(meta.source)              # "anec"
print(meta.source_method)       # "httpx+pdfplumber"
print(meta.parser_version)      # 1
print(meta.raw_content_hash)    # fingerprint do PDF (md5 dos headers)
```

## Notas de risco

- **Layout do PDF**: pode mudar em release futura da ANEC. O parser computa
  uma fingerprint MD5 da estrutura — divergência aciona `ParseError` em vez
  de retornar dados incorretos.
- **Revisão retroativa**: ANEC informa que números são revisados ao fim de
  cada mês. O cache invalida automaticamente quando `media_updated_at` avança.
- **DDGS/Sorgo no YoY**: ANEC mudou layout entre semanas — ora aparece DDGS+Total
  Products (W04), ora DDGS+Sorgo (W08+). Parser detecta ambos os layouts.
- **`destinations` pode vir vazio** em semanas antes do mês fechar (caso
  legítimo, não erro do parser).

## Fonte

- URL: `https://www.anec.com.br/`
- Formato: PDF (14 páginas em 2026)
- Atualização: semanal (quartas-feiras)
- Histórico no agrobr: 2026+
- Licença: `zona_cinza`
