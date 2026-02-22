# agrobr 0.11.2 — Test Coverage Boost

## Highlights

- **3658 testes** (era 3501), cobertura **84%** (era 80%)
- **157 novos testes** cobrindo 15 módulos, 462 linhas adicionais
- **12 módulos** com ganho significativo de cobertura
- Nenhuma dependência nova — todos os testes usam `unittest.mock`

## Breaking Changes

Nenhum. API pública mantida.

## Upgrade

```bash
pip install --upgrade agrobr
```

## Added

- **Cobertura de testes 80% → 84%** — 157 novos testes em 15 módulos:
  - `telemetry/collector` 0% → 100% (async track, flush, conveniences)
  - `utils/logging` 0% → 100% (structlog configure, get_logger)
  - `validators/sanity` 59% → 100% (validate_safra, validate_batch)
  - `mapbiomas/client` 39% → 100% (URL building, fetch mock)
  - `desmatamento/client` 22% → 97% (WFS URL, UF mapping, fetch)
  - `cache/policies` 56% → 96% (format_ttl, next_update, should_refresh)
  - `cache/duckdb_store` 83% → 94% (chunks, _to_row, error paths)
  - `validators/structural` 18% → 85% (Jaccard, fingerprint, baseline)
  - `http/browser` 23% → 77% (Playwright mock, Cloudflare detect)
  - `plugins/__init__` 58% → 87% (load from file/dir, lifecycle)
  - `cepea/parsers/consensus` 72% → 100% (analyze, select_best, validator)
  - `cepea/parsers/detector` 92% → 100% (fallback chain, confidence)

## Fixed

- **CONAB serie_historica**: URL corrigida — `/conab/conab/pt-br/` duplicado removido
- **MapBiomas**: URLs migradas de GCS (404) para Dataverse (`data.mapbiomas.org`)
- **SICAR**: SSLContext customizado com `@SECLEVEL=1` para TLS handshake failure
- **ANTT Pedagio**: slugs CKAN atualizados, parser V2 ajustado para novo layout CSV
- **ANP Diesel**: `vendas_diesel` migrado de XLS pivot (quebrado) para CSV dados abertos

## Links

- [Documentação](https://www.agrobr.dev/docs/)
- [PyPI](https://pypi.org/project/agrobr/)
- [Changelog completo](https://github.com/bruno-portfolio/agrobr/blob/main/CHANGELOG.md)
- [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/agrobr_demo.ipynb)
