# agrobr 0.9.0 — Quality & Resilience

## Highlights

- **1640 testes** (era 949), cobertura **~78%** (era 57.5%)
- **13/13 fontes** com golden tests (era 2/13)
- **10 bugs corrigidos** incluindo histórico DuckDB que nunca salvava dados
- **Resiliência HTTP** completa: retry centralizado, 429 handling, Retry-After
- **INMET** atualizado: novo endpoint + suporte a token de autenticação
- **Benchmark de escalabilidade** validado: memory, volume, cache, async

## Breaking Changes

Nenhum. API pública mantida.

## Upgrade

```bash
pip install --upgrade agrobr
```

## Added

- 1640 testes (era 949), cobertura ~78% (era 57.5%)
- Golden tests para todas as 13 fontes de dados
- Cobertura CLI (51 testes), alerts (17 testes), health (39 testes)
- Benchmark de escalabilidade (memory, volume, cache, async, rate limiting)
- Suporte a token INMET (`AGROBR_INMET_TOKEN` via env var)
- `retry_on_status()` e `retry_async()` centralizados em `http/retry.py`
- Retry-After header respeitado em respostas 429
- Testes de resiliência HTTP para todos os 13 clients (timeout, 429, 500, 403, resposta vazia)
- Testes para API pública: `cepea.indicador()`/`ultimo()`, `conab.safras()`/`balanco()`/`brasil_total()`/`levantamentos()`
- Pre-commit hooks atualizados (ruff v0.15, mypy v1.19)

## Fixed

- **Cache DuckDB** — `history_entries.id` sem autoincrement: histórico permanente nunca salvava dados
- **normalize/dates** — `normalizar_safra()` não fazia strip no input
- **6 clients sem retry para HTTP 429**: inmet, nasa_power, conab_custo, conab_serie, conab main, ibge
- **Graceful degradation silenciosa** trocada por `SourceUnavailableError` quando retry esgota
- **except Exception genérico** em `duckdb_store.py` restringido para exceções específicas
- **INMET** — endpoint `/estacao/dados/` atualizado para `/estacao/` (API mudou)
- **INMET** — tratamento de HTTP 204 (No Content) retorna DataFrame vazio

## Changed

- Retry loops de todos 13 clients migrados para `http/retry.py` centralizado
- Testes de datasets refatorados: 98 funções duplicadas → 27 parametrizadas (115 cenários)
- mypy override para `tests.*` (`ignore_errors = true`, strict mantido no core)

## Known Issues

- 4 golden tests com dados sintéticos (INMET, USDA, NA, ANDA) — `needs_real_data`
  (BCB, IBGE, ComexStat, DERAL, ABIOVE migrados para dados reais na issue #10)
- DuckDB 1.4.4 incompatível com coverage no Python 3.14

## Links

- [Documentação](https://www.agrobr.dev/docs/)
- [PyPI](https://pypi.org/project/agrobr/)
- [Changelog completo](https://github.com/bruno-portfolio/agrobr/blob/main/CHANGELOG.md)
- [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/agrobr_demo.ipynb)
