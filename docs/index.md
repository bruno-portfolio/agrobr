# agrobr

**Dados agrícolas brasileiros em uma linha de código**

[![PyPI version](https://badge.fury.io/py/agrobr.svg)](https://pypi.org/project/agrobr/)
[![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml)
[![Health Check](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## O que é o agrobr?

O **agrobr** é um pacote Python que fornece acesso simplificado aos principais dados agrícolas brasileiros:

- **CEPEA/ESALQ**: Indicadores de preços (soja, milho, boi, café, etc.)
- **CONAB**: Dados de safras e balanço oferta/demanda
- **IBGE/SIDRA**: PAM (Produção Agrícola Municipal) e LSPA (Levantamento Sistemático)

## Por que usar o agrobr?

| Problema | Solução agrobr |
|----------|----------------|
| Download manual de planilhas | Uma linha de código |
| Layouts inconsistentes | Parsing robusto com fallback |
| Scripts que quebram | Fingerprinting detecta mudanças |
| Sem histórico | Cache DuckDB com acumulação |
| Encoding caótico | Fallback chain automático |

## Instalação

```bash
pip install agrobr

# Com suporte a Polars (opcional)
pip install agrobr[polars]

# Instalar Playwright para scraping avançado
playwright install chromium
```

## Uso Rápido

```python
from agrobr import cepea, conab, ibge

# CEPEA - Indicadores de preços
df = await cepea.indicador('soja')
print(df.head())

# CONAB - Safras
df = await conab.safras('soja', safra='2024/25')

# IBGE - PAM
df = await ibge.pam('soja', ano=2023, nivel='uf')
```

### Versão Síncrona

```python
from agrobr.sync import cepea

# Mesma API, sem async/await
df = cepea.indicador('soja')
```

## Features

- **Async-first**: Performance para pipelines de dados
- **Sync wrapper**: Uso simples quando async não é necessário
- **Cache inteligente**: DuckDB com histórico permanente
- **Suporte Pandas + Polars**: `as_polars=True` em todas as APIs
- **CLI completa**: `agrobr cepea soja --formato csv`
- **Resiliência**: Retry, rate limiting, fallback automático
- **Validação**: Pydantic v2 + sanity checks estatísticos
- **Monitoramento**: Health checks e alertas

## Próximos Passos

- [Guia Rápido](quickstart.md) - Tutorial completo
- [API Reference](api/cepea.md) - Documentação detalhada
- [Exemplos](https://github.com/bruno-portfolio/agrobr/tree/main/examples) - Scripts de exemplo

## Licença

MIT License - veja [LICENSE](https://github.com/bruno-portfolio/agrobr/blob/main/LICENSE)
