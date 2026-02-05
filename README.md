# agrobr

> Dados agrícolas brasileiros em uma linha de código

[![PyPI version](https://img.shields.io/pypi/v/agrobr)](https://pypi.org/project/agrobr/)
[![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml)
[![Daily Health Check](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml)
[![Docs](https://github.com/bruno-portfolio/agrobr/actions/workflows/docs.yml/badge.svg)](https://bruno-portfolio.github.io/agrobr/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/demo_colab.ipynb)

Infraestrutura Python para dados agrícolas brasileiros com camada semântica sobre **CEPEA**, **CONAB** e **IBGE**.

## Instalação

```bash
pip install agrobr
```

Com suporte a Polars e Playwright (para fontes que requerem JavaScript):
```bash
pip install agrobr[polars,browser]
playwright install chromium
```

## Uso Rápido

### CEPEA - Indicadores de Preços

```python
import asyncio
from agrobr import cepea

async def main():
    # Série histórica de soja
    df = await cepea.indicador('soja', periodo='2024')
    print(df.head())

    # Último valor disponível
    ultimo = await cepea.ultimo('soja')
    print(f"Soja: R$ {ultimo.valor}/sc em {ultimo.data}")

    # Produtos disponíveis
    print(cepea.produtos())  # ['soja', 'milho', 'boi_gordo', 'cafe', ...]

asyncio.run(main())
```

### CONAB - Safras e Balanço

```python
from agrobr import conab

async def main():
    # Dados de safra por UF
    df = await conab.safras('soja', safra='2024/25')
    print(df[['uf', 'area_plantada', 'producao', 'produtividade']])

    # Balanço oferta/demanda
    balanco = await conab.balanco('soja')
    print(balanco)

    # Total Brasil
    brasil = await conab.brasil_total()
    print(brasil)
```

### IBGE - PAM e LSPA

```python
from agrobr import ibge

async def main():
    # PAM - Produção Agrícola Municipal (anual)
    df = await ibge.pam('soja', ano=2023, nivel='uf')
    print(df[['localidade', 'area_plantada', 'producao']])

    # LSPA - Levantamento Sistemático (mensal)
    df = await ibge.lspa('soja', ano=2024, mes=6)
    print(df)

    # Múltiplos anos
    df = await ibge.pam('milho', ano=[2020, 2021, 2022, 2023])
```

### Datasets (v0.6.0) - Camada Semântica

Peça o que quer, fonte é detalhe interno:

```python
from agrobr import datasets

async def main():
    # Preço diário (CEPEA com fallback automático)
    df = await datasets.preco_diario("soja")

    # Produção anual (IBGE PAM → CONAB)
    df = await datasets.producao_anual("soja", ano=2023)

    # Estimativa de safra corrente (CONAB → IBGE LSPA)
    df = await datasets.estimativa_safra("soja", safra="2024/25")

    # Balanço oferta/demanda (CONAB)
    df = await datasets.balanco("soja")

    # Com metadados de proveniência
    df, meta = await datasets.preco_diario("soja", return_meta=True)
    print(meta.source, meta.contract_version)

    # Listar datasets disponíveis
    print(datasets.list_datasets())
    # ['balanco', 'estimativa_safra', 'preco_diario', 'producao_anual']
```

### Modo Determinístico (Reprodutibilidade)

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    # Todas as consultas filtram data <= snapshot
    # Usa apenas cache local (sem rede)
    df = await datasets.preco_diario("soja")
```

### Modo Síncrono

```python
from agrobr.sync import cepea, conab, ibge, datasets

# Mesmo API, sem async/await
df = cepea.indicador('soja', periodo='2024')
safras = conab.safras('milho')
pam = ibge.pam('soja', ano=2023)
df = datasets.preco_diario('soja')
```

### Suporte Polars

```python
# Retorna polars.DataFrame em vez de pandas
df = await cepea.indicador('soja', as_polars=True)
df = await conab.safras('milho', as_polars=True)
df = await ibge.pam('soja', ano=2023, as_polars=True)
```

### CLI

```bash
# CEPEA
agrobr cepea soja --ultimo
agrobr cepea milho --inicio 2024-01-01 --formato csv

# CONAB
agrobr conab safras soja --safra 2024/25
agrobr conab balanco milho

# IBGE
agrobr ibge pam soja --ano 2023 --nivel uf
agrobr ibge lspa milho --ano 2024 --mes 6

# Health check
agrobr health --all
```

## Status das Fontes

| Fonte | Status |
|-------|--------|
| CEPEA | [![Health](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml) |
| Testes | [![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml) |

O agrobr monitora automaticamente a disponibilidade das fontes.
Use `agrobr health --all` para verificar localmente.

## Datasets Disponíveis

| Dataset | Descrição | Fontes |
|---------|-----------|--------|
| `preco_diario` | Preços diários spot | CEPEA → cache |
| `producao_anual` | Produção anual consolidada | IBGE PAM → CONAB |
| `estimativa_safra` | Estimativas safra corrente | CONAB → IBGE LSPA |
| `balanco` | Oferta/demanda | CONAB |

## Fontes Suportadas

| Fonte | Dados | Status |
|-------|-------|--------|
| CEPEA | Indicadores de preços (soja, milho, café, boi, algodão, trigo) | ✅ Funcional |
| CONAB | Safras, balanço oferta/demanda | ✅ Funcional |
| IBGE | PAM (anual), LSPA (mensal) | ✅ Funcional |

## Diferenciais

- **Camada semântica** - datasets padronizados com fallback automático
- **Contratos públicos** - schema versionado e garantias de estabilidade
- **Modo determinístico** - reprodutibilidade total para papers/auditorias
- **Async-first** para pipelines de alta performance
- **Cache inteligente** com DuckDB (analytics nativo)
- **Histórico permanente** - acumula dados automaticamente
- **Suporte pandas + polars**
- **Validação com Pydantic v2**
- **Validação estatística** de sanidade (detecta anomalias)
- **Fingerprinting de layout** para detecção proativa de mudanças
- **Alertas multi-canal** (Slack, Discord, Email)
- **CLI completo** para debug e automação
- **Fallback automático** entre fontes

## Como Funciona

O agrobr mantém um cache local em DuckDB que acumula dados ao longo do tempo:

```
Dia 1:   Coleta 10 dias de dados → salva no DuckDB
Dia 30:  30 dias de histórico acumulado
Dia 365: 1 ano completo de dados locais
```

Consultas a períodos antigos são instantâneas (cache). Apenas dados recentes precisam de request HTTP.

## Documentação

 [Documentação completa](https://bruno-portfolio.github.io/agrobr/)

- [Guia Rápido](https://bruno-portfolio.github.io/agrobr/quickstart/)
- [API CEPEA](https://bruno-portfolio.github.io/agrobr/api/cepea/)
- [API CONAB](https://bruno-portfolio.github.io/agrobr/api/conab/)
- [API IBGE](https://bruno-portfolio.github.io/agrobr/api/ibge/)
- [Resiliência](https://bruno-portfolio.github.io/agrobr/advanced/resilience/)

## Contribuindo

Contribuições são bem-vindas! Veja [CONTRIBUTING.md](CONTRIBUTING.md) para detalhes.

## Licença

MIT - veja [LICENSE](LICENSE) para detalhes.
