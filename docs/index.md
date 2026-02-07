# agrobr

**Dados agrícolas brasileiros em uma linha de código**

[![PyPI version](https://badge.fury.io/py/agrobr.svg)](https://pypi.org/project/agrobr/)
[![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml)
[![Health Check](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## O que é o agrobr?

Infraestrutura Python para dados agrícolas brasileiros com **camada semântica** sobre CEPEA, CONAB e IBGE.

- **CEPEA/ESALQ**: 20 indicadores de preços (soja, milho, boi, café, algodão, trigo, arroz, açúcar, etanol, frango, suíno, leite, laranja)
- **CONAB**: Safras e balanço oferta/demanda
- **IBGE/SIDRA**: PAM (anual) e LSPA (mensal)

## Datasets — Camada Semântica

Peça o que quer, a fonte é detalhe interno:

| Dataset | Descrição | Fontes (fallback automático) |
|---------|-----------|------------------------------|
| `preco_diario` | Preços diários spot | CEPEA → Notícias Agrícolas → cache |
| `producao_anual` | Produção anual consolidada | IBGE PAM → CONAB |
| `estimativa_safra` | Estimativas safra corrente | CONAB → IBGE LSPA |
| `balanco` | Oferta/demanda | CONAB |

```python
from agrobr import datasets

df = await datasets.preco_diario("soja")
df = await datasets.producao_anual("soja", ano=2023)
df = await datasets.estimativa_safra("soja", safra="2024/25")
df = await datasets.balanco("soja")
```

## Instalação

```bash
pip install agrobr

# Com Playwright (para fontes que requerem JavaScript)
pip install agrobr[browser]
playwright install chromium
```

## Uso Rápido

```python
from agrobr import cepea, conab, ibge

# CEPEA - Indicadores de preços
df = await cepea.indicador('soja', inicio='2024-01-01')

# CONAB - Safras
df = await conab.safras('soja', safra='2024/25')

# IBGE - PAM
df = await ibge.pam('soja', ano=2023, nivel='uf')
```

### Versão Síncrona

```python
from agrobr.sync import cepea

df = cepea.indicador('soja')
```

## Diferenciais

| Problema | Solução agrobr |
|----------|----------------|
| Download manual de planilhas | Uma linha de código |
| Layouts inconsistentes | Parsing robusto com fallback |
| Scripts que quebram | Fingerprinting detecta mudanças |
| Sem histórico | Cache DuckDB com acumulação |
| Encoding caótico | Fallback chain automático |
| Escolher fonte | Datasets abstraem a fonte |

## Features

- **Camada semântica** — datasets com fallback automático entre fontes
- **Contratos públicos** — schema versionado com garantias de estabilidade
- **Modo determinístico** — reprodutibilidade total para papers/auditorias
- **Async-first** com sync wrapper para uso simples
- **Cache DuckDB** com histórico permanente
- **Suporte pandas + polars** (`as_polars=True`)
- **CLI completa** (`agrobr cepea indicador soja --formato csv`)
- **Validação** — Pydantic v2 + sanity checks estatísticos + fingerprinting
- **Monitoramento** — health checks diários + alertas multi-canal

## Próximos Passos

- [Guia Rápido](quickstart.md) — Tutorial completo
- [Datasets](contracts/index.md) — Contratos e garantias
- [API Reference](api/cepea.md) — Documentação detalhada
- [Fontes](sources/index.md) — Proveniência e rastreabilidade
- [Exemplos](https://github.com/bruno-portfolio/agrobr/tree/main/examples) — Scripts de exemplo

## Licença

MIT License — veja [LICENSE](https://github.com/bruno-portfolio/agrobr/blob/main/LICENSE)
