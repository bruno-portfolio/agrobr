# agrobr

> Dados agrícolas brasileiros em uma linha de código

[![PyPI version](https://img.shields.io/pypi/v/agrobr)](https://pypi.org/project/agrobr/)
[![Downloads](https://static.pepy.tech/badge/agrobr)](https://pepy.tech/project/agrobr)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/agrobr)](https://pypi.org/project/agrobr/)
[![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml)
[![Daily Health Check](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml)
[![Docs](https://github.com/bruno-portfolio/agrobr/actions/workflows/docs.yml/badge.svg)](https://www.agrobr.dev/docs/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/agrobr_demo.ipynb)

Infraestrutura Python para dados agrícolas brasileiros com camada semântica sobre **15 fontes públicas**: CEPEA, CONAB, IBGE, NASA POWER, BCB/SICOR, ComexStat, ANDA, ABIOVE, USDA PSD, IMEA, DERAL, INMET, Notícias Agrícolas, Queimadas/INPE e Desmatamento PRODES/DETER.

**v0.10.0-dev** — 2227 testes, ~78% cobertura, 15/15 fontes com golden tests, retry centralizado em 15/15 clients.

## Demo
![Animation](https://github.com/user-attachments/assets/40e1341e-f47b-4eb5-b18e-55b49c63ee97)

## Instalação

```bash
pip install agrobr
```

Com extras opcionais:
```bash
pip install agrobr[pdf]             # pdfplumber para ANDA (fertilizantes)
pip install agrobr[polars]          # Suporte a Polars
pip install agrobr[browser]         # Playwright (opcional, para fontes com JS)
pip install agrobr[bigquery]        # Base dos Dados (fallback BCB/SICOR)
pip install agrobr[all]             # Tudo incluído
```

## Uso Rápido

### CEPEA - Indicadores de Preços

```python
import asyncio
from agrobr import cepea

async def main():
    # Série histórica de soja
    df = await cepea.indicador('soja', inicio='2024-01-01')
    print(df.head())

    # Último valor disponível
    ultimo = await cepea.ultimo('soja')
    print(f"Soja: R$ {ultimo.valor}/sc em {ultimo.data}")

    # Produtos disponíveis
    print(await cepea.produtos())  # ['soja', 'milho', 'boi_gordo', 'cafe', ...]

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

### Datasets - Camada Semântica

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

    # Crédito rural (BCB/SICOR com fallback BigQuery)
    df = await datasets.credito_rural("soja", safra="2024/25")

    # Exportações (ComexStat → ABIOVE)
    df = await datasets.exportacao("soja", ano=2024)

    # Fertilizantes (ANDA)
    df = await datasets.fertilizante(ano=2024, uf="MT")

    # Custos de produção (CONAB)
    df = await datasets.custo_producao("soja", uf="MT", safra="2024/25")

    # Com metadados de proveniência
    df, meta = await datasets.preco_diario("soja", return_meta=True)
    print(meta.source, meta.contract_version)

    # Listar datasets disponíveis
    print(datasets.list_datasets())
    # ['balanco', 'credito_rural', 'custo_producao', 'estimativa_safra',
    #  'exportacao', 'fertilizante', 'preco_diario', 'producao_anual']
```

### Modo Determinístico (Reprodutibilidade)

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    # Todas as consultas filtram data <= snapshot
    # Usa apenas cache local (sem rede)
    df = await datasets.preco_diario("soja")
```

### Novas Fontes v0.7.0+

```python
from agrobr import nasa_power, bcb, comexstat, anda

async def main():
    # NASA POWER — climatologia por ponto ou UF (v0.7.1)
    df = await nasa_power.clima_ponto(-12.6, -56.1, "2024-01-01", "2024-12-31")
    df = await nasa_power.clima_uf("MT", ano=2024)

    # BCB/SICOR — crédito rural
    df = await bcb.credito_rural(produto="soja", safra="2024/25")

    # ComexStat — exportações mensais
    df = await comexstat.exportacao("soja", ano=2024, agregacao="mensal")

    # ANDA — entregas de fertilizantes (requer pip install agrobr[pdf])
    df = await anda.entregas(ano=2024, uf="MT")

    # CONAB — custos de produção
    from agrobr import conab
    df = await conab.custo_producao(cultura="soja", uf="MT", safra="2024/25")
```

### Novas Fontes v0.8.0

```python
from agrobr import abiove, usda, imea, deral, conab

async def main():
    # ABIOVE — exportação do complexo soja
    df = await abiove.exportacao(ano=2024, produto="grao")

    # USDA PSD — estimativas internacionais (requer API key gratuita)
    df = await usda.psd("soja", country="BR", market_year=2024)

    # IMEA — cotações e indicadores Mato Grosso
    df = await imea.cotacoes("soja", safra="24/25")

    # DERAL — condição das lavouras Paraná
    df = await deral.condicao_lavouras("soja")

    # CONAB — série histórica de safras (2010+)
    df = await conab.serie_historica("soja", inicio=2020, fim=2025, uf="MT")
```

### INMET — Token de Autenticação

A API de dados observacionais do INMET requer token. Configure via variável de ambiente:

```bash
export AGROBR_INMET_TOKEN="seu-token-aqui"
```

Sem o token, requisições de dados retornam HTTP 204 (sem conteúdo). A listagem de estações funciona sem token. Para dados climáticos sem token, use [NASA POWER](#novas-fontes-v070) como alternativa.

### Queimadas/INPE — Focos de Calor (v0.10.0)

```python
from agrobr import queimadas

async def main():
    # Focos de calor em setembro/2024
    df = await queimadas.focos(ano=2024, mes=9)

    # Filtrar por UF e bioma
    df = await queimadas.focos(ano=2024, mes=9, uf="MT", bioma="Amazonia")

    # Dia especifico
    df = await queimadas.focos(ano=2024, mes=9, dia=15)

    # Com metadados
    df, meta = await queimadas.focos(ano=2024, mes=9, return_meta=True)
```

### Desmatamento PRODES/DETER (v0.10.0)

```python
from agrobr import desmatamento

async def main():
    # PRODES — desmatamento anual consolidado (Cerrado)
    df = await desmatamento.prodes(bioma="Cerrado", ano=2022, uf="MT")

    # DETER — alertas em tempo real (Amazônia)
    df = await desmatamento.deter(
        bioma="Amazônia", uf="PA",
        data_inicio="2024-01-01", data_fim="2024-06-30",
    )

    # Filtrar por classe de alerta
    df = await desmatamento.deter(bioma="Amazônia", classe="DESMATAMENTO_CR")

    # Com metadados
    df, meta = await desmatamento.prodes(bioma="Cerrado", ano=2022, return_meta=True)
```

### Modo Síncrono

```python
from agrobr.sync import cepea, conab, ibge, datasets, nasa_power, bcb, comexstat
from agrobr.sync import abiove, usda, imea, deral, queimadas, desmatamento

# Mesmo API, sem async/await
df = cepea.indicador('soja', inicio='2024-01-01')
safras = conab.safras('milho')
pam = ibge.pam('soja', ano=2023)
df = datasets.preco_diario('soja')
clima = nasa_power.clima_uf('MT', ano=2024)
credito = bcb.credito_rural(produto='soja', safra='2024/25')
exportacao = comexstat.exportacao('soja', ano=2024)

# v0.8.0
df = abiove.exportacao(ano=2024)
df = usda.psd('soja', market_year=2024)
df = imea.cotacoes('soja')
df = deral.condicao_lavouras('soja')
df = conab.serie_historica('soja', inicio=2020)
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
agrobr cepea indicador soja --ultimo
agrobr cepea indicador milho --inicio 2024-01-01 --formato csv

# CONAB
agrobr conab safras soja --safra 2024/25
agrobr conab balanco milho

# IBGE
agrobr ibge pam soja --ano 2023 --nivel uf
agrobr ibge lspa milho --ano 2024 --mes 6

# Health check & diagnóstico
agrobr health --all
agrobr doctor --verbose
agrobr config show
```

## Status das Fontes

| Fonte | Status |
|-------|--------|
| CEPEA | [![Health](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml) |
| Testes | [![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml) |
| Integração | [![Integration](https://github.com/bruno-portfolio/agrobr/actions/workflows/integration_tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/integration_tests.yml) |

O agrobr monitora automaticamente a disponibilidade das fontes.
Use `agrobr health --all` para verificar localmente.

## Datasets Disponíveis

| Dataset | Descrição | Fontes |
|---------|-----------|--------|
| `preco_diario` | Preços diários spot | CEPEA → cache |
| `producao_anual` | Produção anual consolidada | IBGE PAM → CONAB |
| `estimativa_safra` | Estimativas safra corrente | CONAB → IBGE LSPA |
| `balanco` | Oferta/demanda | CONAB |
| `credito_rural` | Crédito rural por cultura | BCB/SICOR → BigQuery |
| `exportacao` | Exportações agrícolas | ComexStat → ABIOVE |
| `fertilizante` | Entregas de fertilizantes | ANDA |
| `custo_producao` | Custos de produção | CONAB |

## Fontes Suportadas

| Fonte | Dados | Golden Test | Status |
|-------|-------|:-----------:|--------|
| CEPEA | Indicadores de preços (20 produtos) | ✅ | Funcional |
| CONAB | Safras, balanço, custos, série histórica | ✅ | Funcional |
| IBGE | PAM (anual), LSPA (mensal) | ✅ | Funcional |
| NASA POWER | Climatologia diária/mensal (grid 0.5°) | ✅ | Funcional |
| BCB/SICOR | Crédito rural por cultura (+ fallback BigQuery) | ✅¹ | Funcional |
| ComexStat | Exportações por NCM/UF | ✅¹ | Funcional |
| ANDA | Entregas de fertilizantes | ✅ | Funcional |
| ABIOVE | Exportação complexo soja (volume/receita) | ✅ | Funcional |
| USDA PSD | Estimativas internacionais (produção/oferta/demanda) | ✅¹ | Funcional |
| IMEA | Cotações e indicadores Mato Grosso | ✅ | Funcional |
| DERAL | Condição das lavouras Paraná | ✅ | Funcional |
| INMET | Meteorologia (600+ estações) | ✅¹ | Requer token (`AGROBR_INMET_TOKEN`) |
| Notícias Agrícolas | Cotações (fallback CEPEA) | ✅¹ | Funcional |
| Queimadas/INPE | Focos de calor por satelite (6 biomas, 13 satelites) | ✅ | Funcional |
| Desmatamento PRODES/DETER | Desmatamento consolidado + alertas (TerraBrasilis WFS) | ✅ | Funcional |

> ¹ Golden test com dados sintéticos — `needs_real_data` para validação com API real.

## Contratos & Schemas

Cada dataset tem um contrato formal com validação automática. Schemas JSON gerados em `agrobr/schemas/`:

```python
from agrobr.contracts import get_contract, list_contracts, validate_dataset

# Listar contratos registrados
list_contracts()
# ['balanco', 'credito_rural', 'custo_producao', 'estimativa_safra',
#  'exportacao', 'fertilizante', 'preco_diario', 'producao_anual']

# Inspecionar contrato
contract = get_contract("preco_diario")
print(contract.primary_key)   # ['data', 'produto']
print(contract.to_json())     # Schema JSON completo

# Validação explícita (automática em todo fetch)
validate_dataset(df, "preco_diario")  # raises ContractViolationError
```

Garantias globais: nomes estáveis (só adicionam), tipos só alargam (int→float ok, float→int nunca), datas ISO-8601, breaking changes só em major version. Veja [docs/contracts/](https://www.agrobr.dev/docs/contracts/) para detalhes por dataset.

## Normalização Transversal

Funções para padronizar dados entre fontes:

```python
from agrobr.normalize import (
    normalizar_cultura, municipio_para_ibge, normalizar_uf, normalizar_safra,
)

normalizar_cultura("Soja em Grão")    # "soja"
normalizar_cultura("milho 2ª safra")  # "milho_2"
normalizar_cultura("coffee")          # "cafe"

municipio_para_ibge("Sorriso", "MT")  # 5107925
municipio_para_ibge("SAO PAULO", "SP")  # 3550308

normalizar_uf("São Paulo")            # "SP"
normalizar_safra("24/25")             # "2024/25"
```

5571 municípios IBGE, 35 culturas canônicas, 27 UFs. Dados de municípios via API IBGE Localidades (livre para uso).

## Diferenciais

- **15/15 fontes com golden tests** — validação automatizada contra dados de referência
- **Resiliência HTTP completa** — retry centralizado em 15/15 clients, 429 handling, Retry-After
- **2227 testes, ~78% cobertura** — benchmarks de escalabilidade (memory, volume, cache, async)
- **Camada semântica** — datasets padronizados com fallback automático
- **Contratos formais** — schema versionado com validação automática, primary keys e constraints
- **Schemas JSON** — contratos exportados como JSON em `agrobr/schemas/`
- **Normalização transversal** — municípios IBGE, culturas, UFs, safras padronizados
- **Modo determinístico** — reprodutibilidade total para papers/auditorias
- **Async-first** para pipelines de alta performance
- **Cache inteligente** com DuckDB (analytics nativo + histórico permanente)
- **Suporte pandas + polars**
- **Validação** — Pydantic v2 + sanity checks estatísticos + fingerprinting de layout
- **Alertas multi-canal** (Slack, Discord, Email)
- **CLI completo** para debug e automação

## Como Funciona

O agrobr mantém um cache local em DuckDB que acumula dados ao longo do tempo:

```
Dia 1:   Coleta 10 dias de dados → salva no DuckDB
Dia 30:  30 dias de histórico acumulado
Dia 365: 1 ano completo de dados locais
```

Consultas a períodos antigos são instantâneas (cache). Apenas dados recentes precisam de request HTTP.

## Manter Dados Atualizados

Integre com Airflow, Prefect ou Dagster usando a API sync:

```python
# Airflow task
@task
def extract_soja():
    from agrobr.sync import datasets
    df = datasets.preco_diario("soja")
    df.to_parquet("/data/soja.parquet")
```

Veja o [guia completo de pipelines](https://www.agrobr.dev/docs/advanced/pipelines/) e o [guia de ergonomia async](https://www.agrobr.dev/docs/guides/async/).

## Documentação

 [Documentação completa](https://www.agrobr.dev/docs/)

- [Guia Rápido](https://www.agrobr.dev/docs/quickstart/)
- [Datasets](https://www.agrobr.dev/docs/contracts/) — Contratos e garantias
- [Fontes](https://www.agrobr.dev/docs/sources/) — 15 fontes documentadas
- [API Reference](https://www.agrobr.dev/docs/api/cepea/)
- [Resiliência](https://www.agrobr.dev/docs/advanced/resilience/)

## Contribuindo

Contribuições são bem-vindas! Veja [CONTRIBUTING.md](CONTRIBUTING.md) para detalhes.

## Licenças dos Dados

> **Importante:** O agrobr é licenciado sob MIT, mas os **dados** acessados
> pertencem às suas respectivas fontes e possuem licenças próprias.
> Dados CEPEA/ESALQ, por exemplo, são CC BY-NC 4.0 (uso comercial requer
> autorização). Consulte **[docs/licenses.md](docs/licenses.md)** para a tabela
> completa de fontes, licenças e classificações.

## Licença

MIT - veja [LICENSE](LICENSE) para detalhes.
