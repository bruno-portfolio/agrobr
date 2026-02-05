# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Unreleased]

## [0.6.1] - 2026-02-05

### Fixed
- Playwright graceful degradation — import com try/except, não crasha em Python 3.14+
- Parser Notícias Agrícolas levanta `ParseError` ao invés de retornar lista vazia silenciosamente
- Cache fallback automático com `StaleDataWarning` quando todas as fontes falham

## [0.6.0] - 2026-02-05

### Added
- **Camada Semântica** - 4 datasets padronizados com fallback automático entre fontes
  - `datasets.preco_diario()` - Preços diários (CEPEA → cache)
  - `datasets.producao_anual()` - Produção anual (IBGE PAM → CONAB)
  - `datasets.estimativa_safra()` - Estimativas safra corrente (CONAB → IBGE LSPA)
  - `datasets.balanco()` - Balanço oferta/demanda (CONAB)
  - `datasets.list_datasets()` / `datasets.list_products()` / `datasets.info()`
- **Contratos Públicos** - Garantias formais de schema versionado
  - Documentação em `docs/contracts/` para cada dataset
  - Colunas estáveis, tipos só alargam, breaking changes só em major
- **Modo Determinístico Aprimorado** - Context manager async com contextvars
  - `async with datasets.deterministic("2025-12-31"):` - Isolado por task
  - `@deterministic_decorator("2025-12-31")` - Decorator para funções
  - `is_deterministic()` / `get_snapshot()` - Verificar estado atual
- **Hierarquia de Exceções Expandida**
  - `NetworkError` - Erros de rede (timeout, HTTP error, DNS)
  - `ContractViolationError` - DataFrame não atende contrato do dataset
- **MetaInfo Expandido** - Novos campos de proveniência
  - `dataset` - Nome do dataset
  - `contract_version` - Versão do contrato
  - `snapshot` - Data de corte (modo determinístico)
- **Documentação Avançada**
  - `docs/advanced/reproducibility.md` - Guia de reprodutibilidade
  - `docs/advanced/pipelines.md` - Integração Airflow, Prefect, Dagster
- **Notebook Demo** - Google Colab com exemplos executáveis

### Changed
- `agrobr.sync.datasets` - API síncrona para datasets
- README atualizado com seção de datasets e status das fontes

## [0.5.0] - 2026-02-04

### Added
- **Plugin System** - Arquitetura extensível para fontes e validadores
  - `SourcePlugin` - Interface para novas fontes de dados
  - `ParserPlugin` - Interface para parsers customizados
  - `ExporterPlugin` - Interface para exportadores customizados
  - `ValidatorPlugin` - Interface para validadores customizados
  - `register()`, `get_plugin()`, `list_plugins()` - Gerenciamento de plugins
- **API Stability Decorators** - Marcadores de estabilidade de API
  - `@stable(since="x.y.z")` - Marca API como estável
  - `@experimental(since="x.y.z")` - Marca API como experimental
  - `@deprecated(since, removed_in, replacement)` - Marca API como deprecated
  - `@internal` - Marca API como interna (não pública)
  - `list_stable_apis()`, `list_experimental_apis()`, `list_deprecated_apis()`
- **SLA Documentado** - Contratos de nível de serviço por fonte
  - `SourceSLA` - Definição de SLA com tier, freshness, latency, availability
  - `CEPEA_SLA` - Tier CRITICAL, atualização diária 18h, 99% uptime
  - `CONAB_SLA` - Tier STANDARD, atualização mensal, 98% uptime
  - `IBGE_SLA` - Tier STANDARD, varia por pesquisa
  - `get_sla()`, `list_slas()`, `get_sla_summary()`
- **Certificação de Qualidade** - Sistema de certificação de dados
  - `QualityLevel` - GOLD, SILVER, BRONZE, UNCERTIFIED
  - `QualityCheck` - Check individual com status e detalhes
  - `QualityCertificate` - Certificado completo com score e validade
  - `certify(df)` - Executa checks (completeness, duplicates, schema, freshness, range)
  - `quick_check(df)` - Retorna (level, score) rapidamente

## [0.4.0] - 2026-02-04

### Added
- **Modo Determinístico** - Reprodutibilidade absoluta para backtests
  - `agrobr.set_mode("deterministic", snapshot="2025-01-01")`
  - `agrobr.configure()` para opções globais
  - `agrobr.get_config()` para consultar configuração atual
  - `agrobr.reset_config()` para resetar ao padrão
- **Sistema de Snapshots** - Gerenciamento de versões de dados
  - `create_snapshot()` - Cria snapshot dos dados atuais
  - `load_from_snapshot()` - Carrega dados de um snapshot
  - `list_snapshots()` / `delete_snapshot()` - Gerenciamento
  - CLI: `agrobr snapshot create/list/delete/use`
- **Export Auditável** - Formatos com metadados de proveniência
  - `export_parquet()` - Parquet com metadata embutido
  - `export_csv()` - CSV com arquivo sidecar .meta.json
  - `export_json()` - JSON com metadados opcionais
  - `verify_export()` - Verificação de integridade

## [0.3.0] - 2026-02-04

### Added
- **Stability Contracts** - Garantias formais de schema para todas as fontes
  - `CEPEA_INDICADOR_V1` - Contrato para indicadores de preço CEPEA
  - `CONAB_SAFRA_V1` - Contrato para dados de safra CONAB
  - `CONAB_BALANCO_V1` - Contrato para balanço oferta/demanda CONAB
  - `IBGE_PAM_V1` - Contrato para dados PAM do IBGE
  - `IBGE_LSPA_V1` - Contrato para dados LSPA do IBGE
  - `contract.validate(df)` - Validação automática contra contrato
  - `contract.to_markdown()` - Documentação automática
- **Validação Semântica** - Verificações avançadas de qualidade
  - Validação de preços positivos
  - Validação de faixas de produtividade por cultura
  - Detecção de anomalias em variação diária (>20%)
  - Consistência de sequência de datas
  - Consistência de áreas (colhida <= plantada)
  - Validação de formato de safra
  - `validate_semantic(df)` - Executa todas as regras
  - `get_validation_summary(df)` - Resumo das violações
- **Benchmark Suite** - Ferramentas para medição de performance
  - `benchmark_async()` / `benchmark_sync()` - Benchmark de funções
  - `run_api_benchmarks()` - Benchmark das APIs
  - `run_contract_benchmarks()` - Benchmark de validação de contratos
  - `run_semantic_benchmarks()` - Benchmark de validação semântica

### Changed
- Changelog reestruturado seguindo Keep a Changelog

## [0.2.0] - 2026-02-04

### Added
- **`agrobr doctor`** - Comando CLI para diagnóstico do sistema
  - Verificação de conectividade das fontes
  - Estatísticas do cache (tamanho, registros, por fonte)
  - Status de configuração
  - Output JSON (`--json`) e formatado Rich
- **Parâmetro `return_meta`** - Suporte a data lineage em todas as APIs
  - `cepea.indicador(return_meta=True)` retorna `(DataFrame, MetaInfo)`
  - `conab.safras(return_meta=True)` retorna `(DataFrame, MetaInfo)`
  - `ibge.pam(return_meta=True)` retorna `(DataFrame, MetaInfo)`
  - `ibge.lspa(return_meta=True)` retorna `(DataFrame, MetaInfo)`
- **Classe `MetaInfo`** - Metadados de proveniência e rastreabilidade
  - Informações da fonte (nome, URL, método)
  - Timing (duração fetch, duração parse)
  - Status do cache (from_cache, cache_key, expires_at)
  - Integridade do conteúdo (hash SHA256, tamanho)
  - Versões (agrobr, parser, schema, python)
  - `to_dict()` / `to_json()` para serialização
  - `verify_hash(df)` para verificação de integridade
- **Documentação** - Guias de proveniência e resiliência
  - `docs/sources/cepea.md` - Documentação da fonte CEPEA
  - `docs/sources/conab.md` - Documentação da fonte CONAB
  - `docs/sources/ibge.md` - Documentação da fonte IBGE
  - `docs/advanced/resilience.md` - Documentação de resiliência

### Changed
- `MetaInfo` exportado do pacote principal

## [0.1.2] - 2026-02-04

### Changed
- **Smart TTL** para cache CEPEA - expira às 18:00 (horário de atualização CEPEA)
- Reduz requests desnecessários em ~90%

## [0.1.1] - 2026-02-04

### Fixed
- Browser fallback desabilitado para CEPEA (Cloudflare bloqueia)
- CEPEA agora vai direto para Notícias Agrícolas, evitando timeout

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
  - Monitoramento de estrutura
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

[Unreleased]: https://github.com/bruno-portfolio/agrobr/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/bruno-portfolio/agrobr/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/bruno-portfolio/agrobr/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/bruno-portfolio/agrobr/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/bruno-portfolio/agrobr/releases/tag/v0.1.0