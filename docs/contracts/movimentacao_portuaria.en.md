# Contract: movimentacao_portuaria

Port cargo movement — ANTAQ.

!!! warning "Source unavailable since 2026-06-23"
    ANTAQ took the Estatistico Aquaviario offline ([official notice](https://www.gov.br/antaq/pt-br/central-de-conteudos/publicacoes-da-antaq/publicacoes-off/painel-estatistico-aquaviario-indisponivel)).
    The `estatistica.antaq.gov.br` host no longer serves the files: it returns `403`
    (Cloudflare challenge) or redirects to the unavailability notice, depending on the client.
    Calls to `antaq.movimentacao()` raise `SourceUnavailableError`. No alternative source
    offers equivalent coverage — Base dos Dados only covers 2014-2020.
    Last checked: 2026-08-31.

## Schema

| Column | Type | Nullable | Unit | Constraints |
|--------|------|----------|------|-------------|
| `ano` | INTEGER | No | — | ≥ 2010 |
| `mes` | INTEGER | No | — | 1-12 |
| `data_atracacao` | STRING | Yes | — | — |
| `tipo_navegacao` | STRING | Yes | — | — |
| `tipo_operacao` | STRING | Yes | — | — |
| `natureza_carga` | STRING | Yes | — | — |
| `sentido` | STRING | Yes | — | Embarcados/Desembarcados |
| `porto` | STRING | Yes | — | — |
| `complexo_portuario` | STRING | Yes | — | — |
| `terminal` | STRING | Yes | — | — |
| `municipio` | STRING | Yes | — | — |
| `uf` | STRING | Yes | — | valid state |
| `regiao` | STRING | Yes | — | — |
| `cd_mercadoria` | STRING | Yes | — | — |
| `mercadoria` | STRING | Yes | — | — |
| `grupo_mercadoria` | STRING | Yes | — | — |
| `origem` | STRING | Yes | — | — |
| `destino` | STRING | Yes | — | — |
| `peso_bruto_ton` | FLOAT | Yes | ton | ≥ 0 |
| `qt_carga` | FLOAT | Yes | — | ≥ 0 |
| `teu` | INTEGER | Yes | — | ≥ 0 |

**PK:** `(ano, mes, porto, cd_mercadoria, sentido, tipo_navegacao)`

## Parameters

- `ano: int` — movement year (required, ≥ 2010)
- `mercadoria: str | None` — filter by commodity (substring, case-insensitive)
- `porto: str | None` — filter by port (substring, case-insensitive)
- `uf: str | None` — filter by state (exact match, uppercase)
- `sentido: str | None` — "embarque" or "desembarque"
- `tipo_navegacao: str | None` — navigation type
- `natureza_carga: str | None` — cargo nature

## Example

```python
from agrobr import datasets

# 2024 movement
df = await datasets.movimentacao_portuaria(ano=2024)

# Soybeans shipped from Santos
df = await datasets.movimentacao_portuaria(
    ano=2024, mercadoria="Soja", porto="Santos", sentido="embarque"
)

# With metadata
df, meta = await datasets.movimentacao_portuaria(ano=2024, return_meta=True)
```
