# agrobr

**Dados agrícolas brasileiros em uma linha de código**

[![PyPI version](https://badge.fury.io/py/agrobr.svg)](https://pypi.org/project/agrobr/)
[![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml)
[![Health Check](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## O que é o agrobr?

Infraestrutura Python para dados agrícolas brasileiros com **camada semântica** sobre 25 fontes públicas.

**v0.10.1** — 2778 testes | ~78% cobertura | 19/19 golden tests | retry centralizado 19/19 clients

- **CEPEA/ESALQ**: 20 indicadores de preços (soja, milho, boi, café, algodão, trigo, arroz, açúcar, etanol, frango, suíno, leite, laranja)
- **CONAB**: Safras, balanço oferta/demanda, custos de produção e série histórica
- **IBGE/SIDRA**: PAM (anual) e LSPA (mensal)
- **NASA POWER**: Climatologia gridded diária (temperatura, precipitação, radiação, umidade, vento)
- **BCB/SICOR**: Crédito rural por cultura e UF com dimensões SICOR (programa, fonte, seguro, modalidade, atividade) + fallback BigQuery
- **ComexStat**: Exportações agrícolas por NCM
- **ANDA**: Entregas de fertilizantes por UF
- **ABIOVE**: Exportação do complexo soja (volume e receita mensal)
- **USDA PSD**: Estimativas internacionais de produção/oferta/demanda
- **IMEA**: Cotações e indicadores para Mato Grosso (6 cadeias produtivas)
- **DERAL**: Condição das lavouras do Paraná (semanal)
- **INMET**: Dados meteorológicos por estação (requer token `AGROBR_INMET_TOKEN`)
- **Notícias Agrícolas**: Cotações agrícolas (fallback CEPEA)
- **Queimadas/INPE**: Focos de calor por satélite (6 biomas, 13 satélites)
- **Desmatamento PRODES/DETER**: Desmatamento consolidado + alertas em tempo real
- **MapBiomas**: Cobertura e uso da terra por município (1985-presente)
- **CONAB Progresso**: Progresso semanal de plantio/colheita por cultura e UF
- **CONAB CEASA/PROHORT**: Precos diarios de atacado hortifruti em 43 CEASAs (48 produtos)
- **B3 Futuros Agro**: Ajustes diarios (settlement) + posicoes em aberto (open interest) de futuros e opcoes agro
- **UN Comtrade**: Comercio bilateral + trade mirror (exportacoes vs importacoes por HS code, ~200 paises)
- **ANTAQ**: Movimentacao portuaria de carga (granel solido/liquido, carga geral, conteiner, 2010+)
- **ANP Diesel**: Precos de revenda e volumes de venda de diesel por UF/municipio (proxy atividade mecanizada)
- **ANTT Pedagio**: Fluxo de veiculos em pracas de pedagio rodoviario (ANTT Dados Abertos, CC-BY, 2010+)
- **MAPA PSR**: Apolices e sinistros do seguro rural com subvencao federal (SISSER/MAPA, 2006+)
- **SICAR**: Cadastro Ambiental Rural — registros de imoveis rurais por UF via WFS (7.4M+ imoveis, 27 UFs)

## Datasets — Camada Semântica

Peça o que quer, a fonte é detalhe interno:

| Dataset | Descrição | Fontes (fallback automático) |
|---------|-----------|------------------------------|
| `preco_diario` | Preços diários spot | CEPEA → Notícias Agrícolas → cache |
| `producao_anual` | Produção anual consolidada | IBGE PAM → CONAB |
| `estimativa_safra` | Estimativas safra corrente | CONAB → IBGE LSPA |
| `balanco` | Oferta/demanda | CONAB |
| `credito_rural` | Crédito rural por cultura (programa, seguro, modalidade) | BCB/SICOR → BigQuery |
| `exportacao` | Exportações agrícolas | ComexStat → ABIOVE |
| `fertilizante` | Entregas de fertilizantes | ANDA |
| `custo_producao` | Custos de produção | CONAB |
| `pecuaria_municipal` | Rebanhos e produção animal | IBGE PPM |
| `abate_trimestral` | Abate de bovinos, suínos e frangos | IBGE Abate |
| `cadastro_rural` | Cadastro Ambiental Rural (imóveis rurais) | SICAR/GeoServer WFS |
| `censo_agropecuario` | Censo Agropecuário 2017 | IBGE Censo Agro |

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
from agrobr import cepea, conab, ibge, nasa_power

# CEPEA - Indicadores de preços
df = await cepea.indicador('soja', inicio='2024-01-01')

# CONAB - Safras
df = await conab.safras('soja', safra='2024/25')

# IBGE - PAM
df = await ibge.pam('soja', ano=2023, nivel='uf')

# NASA POWER - Clima
df = await nasa_power.clima_uf('MT', ano=2025)
```

### Versão Síncrona

```python
from agrobr.sync import cepea, nasa_power

df = cepea.indicador('soja')
df = nasa_power.clima_uf('MT', ano=2025)
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

## Quality & Reliability

| Métrica | Valor |
|---------|-------|
| Testes | 2778 passando |
| Cobertura | ~78% |
| Golden tests | 19/19 fontes |
| Resiliência HTTP | Retry centralizado + 429/Retry-After |
| Benchmarks | Memory, volume, cache, async, rate limiting |
| Bugs corrigidos (v0.10.1) | DuckDB thread-safety, parser NA semanal, ANDA ano_real |

## Features

- **25 fontes públicas** — CEPEA, CONAB, IBGE, NASA POWER, BCB/SICOR, ComexStat, ANDA, ABIOVE, USDA, IMEA, DERAL, INMET, Notícias Agrícolas, Queimadas/INPE, Desmatamento, MapBiomas, CONAB Progresso, CONAB CEASA/PROHORT, B3 Futuros Agro, UN Comtrade, ANTAQ, ANP Diesel, MAPA PSR, ANTT Pedagio, SICAR
- **21/21 golden tests** — validação automatizada contra dados de referência
- **Resiliência HTTP** — `retry_on_status()`/`retry_async()` centralizado, Retry-After, 429 handling
- **Camada semântica** — datasets com fallback automático entre fontes
- **Contratos públicos** — schema versionado com garantias de estabilidade
- **Modo determinístico** — reprodutibilidade total para papers/auditorias
- **Async-first** com sync wrapper para uso simples
- **Cache DuckDB** com histórico permanente
- **Suporte pandas + polars** (`as_polars=True`)
- **CLI completa** (`agrobr cepea indicador soja --formato csv`)
- **Validação** — Pydantic v2 + sanity checks estatísticos + fingerprinting
- **Monitoramento** — health checks diários + alertas Discord/Slack

## Próximos Passos

- [Guia Rápido](quickstart.md) — Tutorial completo
- [Datasets](contracts/index.md) — Contratos e garantias
- [API Reference](api/cepea.md) — Documentação detalhada
- [Fontes](sources/index.md) — Proveniência e rastreabilidade
- [Exemplos](https://github.com/bruno-portfolio/agrobr/tree/main/examples) — Scripts de exemplo
- [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/agrobr_demo.ipynb) — Notebook interativo com todas as fontes

## Licença

MIT License — veja [LICENSE](https://github.com/bruno-portfolio/agrobr/blob/main/LICENSE)
