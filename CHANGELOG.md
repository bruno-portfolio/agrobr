# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Unreleased]

### Added
- Suporte inicial para IBGE (PAM e LSPA)
- Golden data tests para CONAB

## [0.1.0] - 2026-02-04

### Added
- **CEPEA**: Indicadores de preços agrícolas (soja, milho, boi, café, algodão, trigo)
  - Fallback automático para Notícias Agrícolas quando CEPEA bloqueado
  - Acumulação progressiva de histórico no DuckDB
- **CONAB**: Dados de safras e balanço oferta/demanda
  - Parser para planilhas XLSX do boletim de safras
  - Suporte a todos os produtos principais (soja, milho, arroz, feijão, etc.)
- **IBGE**: Integração com API SIDRA
  - PAM (Produção Agrícola Municipal) - dados anuais
  - LSPA (Levantamento Sistemático) - estimativas mensais
- **Cache**: Sistema de cache com DuckDB
  - Separação entre cache volátil e histórico permanente
  - TTL configurável por fonte
  - Acumulação progressiva de dados
- **HTTP**: Cliente robusto com resiliência
  - Retry com exponential backoff
  - Rate limiting por fonte
  - User-agent rotativo
  - Fallback para Playwright quando necessário
- **CLI**: Interface de linha de comando completa
  - Comandos para CEPEA, CONAB e IBGE
  - Exportação em CSV, JSON e Parquet
- **Validação**: Sistema de validação multinível
  - Pydantic v2 para validação de tipos
  - Validação estatística (sanity checks)
  - Fingerprinting de layout para detecção de mudanças
- **Monitoramento**: Health checks e alertas
  - Health check por fonte
  - Alertas multi-canal (Slack, Discord, Email)
  - Monitoramento de estrutura (6h)
- **Suporte Polars**: Todas as APIs suportam `as_polars=True`
- **Testes**: 96 testes passando (~80% cobertura)
- **CI/CD**: GitHub Actions configurados
  - Testes automatizados
  - Health check diário
  - Monitoramento de estrutura

### Technical Details
- Python 3.11+ required
- Async-first design com sync wrapper
- Type hints completos
- Logging estruturado com structlog

[Unreleased]: https://github.com/bruno-portfolio/agrobr/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/bruno-portfolio/agrobr/releases/tag/v0.1.0
