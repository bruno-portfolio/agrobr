# agrobr

**Dados agrícolas brasileiros em uma linha de código**

[![PyPI version](https://badge.fury.io/py/agrobr.svg)](https://pypi.org/project/agrobr/)
[![Tests](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/tests.yml)
[![Health Check](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml/badge.svg)](https://github.com/bruno-portfolio/agrobr/actions/workflows/health_check.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

!!! warning "A 2.0 tem mudanças incompatíveis"
    Antes de atualizar, leia o [resumo do que quebra](guides/migracao-2.md#resumo-o-que-quebra) no guia de migração.
    Para ficar na série 1.x enquanto migra: `pip install "agrobr<2"`.

## O que é o agrobr?

Infraestrutura Python para dados agrícolas brasileiros com **camada semântica** sobre 41 fontes públicas.

**v2.0.0** — 54 datasets | 89 contratos versionados | validação de parâmetros antes da rede | golden tests por fonte

- **CEPEA/ESALQ**: 22 indicadores de preços (soja, milho, boi, café arábica, café robusta, algodão, trigo, arroz, açúcar, etanol, frango, suíno, leite, laranja)
- **CONAB**: Safras, balanço oferta/demanda, custos de produção, série histórica, progresso semanal de plantio/colheita e preços atacado hortifruti (CEASA/PROHORT)
- **IBGE/SIDRA**: PAM (anual), LSPA (mensal), PPM, Abate, PEVS (silvicultura + extracao vegetal), Leite Trimestral, PIB Agro, Censo Agro
- **NASA POWER**: Climatologia gridded diária (temperatura, precipitação, radiação, umidade, vento)
- **BCB/SICOR**: Crédito rural por cultura e UF + séries temporais SGS (Selic, IPCA, PIB agro) + cotação PTAX + expectativas Focus
- **ComexStat**: Exportações agrícolas por NCM
- **ANDA**: Entregas mensais de fertilizantes (total nacional)
- **ABIOVE**: Exportação do complexo soja (volume e receita mensal)
- **USDA PSD**: Estimativas internacionais de produção/oferta/demanda
- **IMEA**: Cotações e indicadores para Mato Grosso (8 cadeias, com o nome oficial de cada indicador)
- **DERAL**: Condição das lavouras do Paraná (semanal)
- **INMET**: Dados meteorológicos por estação (requer token `AGROBR_INMET_TOKEN`)
- **Notícias Agrícolas**: Cotações agrícolas (fallback CEPEA)
- **Queimadas/INPE**: Focos de calor por satélite (6 biomas, 13 satélites)
- **Desmatamento PRODES/DETER**: Desmatamento consolidado + alertas em tempo real + geometria (GeoDataFrame)
- **MapBiomas**: Cobertura e uso da terra por município (1985-presente)
- **B3 Futuros Agro**: Ajustes diarios (settlement) + posicoes em aberto (open interest) de futuros e opcoes agro
- **UN Comtrade**: Comercio bilateral + trade mirror (exportacoes vs importacoes por HS code, ~200 paises)
- **ANTAQ**: Movimentacao portuaria de carga (granel solido/liquido, carga geral, conteiner, 2010+) — ⚠️ fonte fora do ar desde 23/06/2026
- **ANP Diesel**: Precos de revenda e volumes de venda de diesel por UF/municipio (proxy atividade mecanizada)
- **ANTT Pedagio**: Fluxo de veiculos em pracas de pedagio rodoviario (ANTT Dados Abertos, CC-BY, 2010+)
- **MAPA PSR**: Apolices e sinistros do seguro rural com subvencao federal (SISSER/MAPA, 2006+)
- **SICAR**: Cadastro Ambiental Rural — registros de imoveis rurais por UF via WFS (7.4M+ imoveis, 27 UFs)
- **ZARC**: Zoneamento Agricola de Risco Climatico — janelas de plantio por municipio/cultura/solo/ciclo (MAPA/Embrapa, CC-BY)
- **Agrofit/MAPA (Defensivos)**: Agrotoxicos registrados no Brasil — produtos formulados, autorizações de uso, produtos técnicos e composição (Creative Commons Attribution, versão não indicada)
- **FUNAI**: Terras indigenas via WFS (665 TIs, reprodução com citação)
- **ICMBio**: Unidades de conservacao federais via WFS (347 UCs, sem RPPN)
- **INCRA**: Territorios quilombolas via WFS (~426 territorios)
- **IBAMA**: Embargos ambientais do portal de dados abertos (~116 mil termos, atualização diária)
- **MapBiomas Alerta**: Alertas de desmatamento via GraphQL (citacao obrigatoria)
- **Lista Suja**: Cadastro corrente de empregadores do MTE, CSV no core e alternativa PDF; API de fonte e dataset semântico
- **ANA/SNIRH**: Hidrografia, pivos de irrigacao, demanda e disponibilidade hidrica via ArcGIS REST
- **SFB**: Florestas publicas, concessoes florestais e IFN via ArcGIS REST
- **RNC/CultivarWeb**: Registro Nacional de Cultivares — ~37K registradas + ~5K protegidas (MAPA, dados publicos)
- **EMBRAPA Solos**: Perfis de solo PronaSolos (34 mil horizontes de ~9 mil pontos) + mapa pedologico SiBCS (2.8K poligonos) via WFS (CC BY-NC 3.0 BR)
- **Fundacao Rio Verde**: Ensaios de cultivares de soja — safras 2023/24 a 2025/26, até 4 épocas de semeio (PDF, pdfplumber)

## Datasets — Camada Semântica

54 datasets disponíveis, com proveniência. O fallback depende das fontes alternativas configuradas para cada dataset:

| Dataset | Descrição | Fontes |
|---------|-----------|------------------------------|
| `cotacoes_cambio` | Cotações e paridades cambiais dos boletins PTAX/BCB | BCB |
| `cultivares_protegidas` | Cadastro corrente de cultivares protegidas no SNPC/MAPA | MAPA |
| `cultivares_registradas` | Cadastro corrente de cultivares registradas no RNC/MAPA | MAPA |
| `expectativas_mercado` | Expectativas anuais e mensais de mercado do Focus/BCB | BCB |
| `moedas_cambio` | Catálogo corrente de moedas do serviço PTAX/BCB | BCB |
| `precos_diesel` | Preços semanais de diesel da ANP e médias mensais derivadas | ANP |
| `series_economicas` | Séries econômicas do Sistema Gerenciador de Séries Temporais do Banco Central | BCB |
| `unidades_conservacao` | Unidades de conservação federais, estaduais e municipais, com RPPNs, do CNUC | CNUC/MMA |
| `unidades_conservacao_federais` | Cadastro corrente de unidades de conservação federais da camada ICMBio/INDE | ICMBio |
| `abate_trimestral` | Abate de bovinos, suínos e frangos por UF | IBGE Abate |
| `autorizacoes_defensivos` | Autorizações de uso com multiplicidade publicada | Agrofit/MAPA |
| `balanco` | Oferta/demanda | CONAB |
| `cadastro_rural` | Cadastro Ambiental Rural (imóveis rurais) | SICAR/GeoServer WFS |
| `censo_agropecuario` | Censo Agropecuário 1995/2006/2017 (11 temas) | IBGE Censo Agro |
| `censo_agropecuario_historico` | Série histórica Censo Agropecuário 1920-2006 (9 temas) | IBGE SIDRA |
| `censo_agropecuario_legado` | Censo Agropecuário 1995/96 — 6 temas legados | IBGE FTP |
| `censo_agropecuario_municipal_1985` | Censo 1985 — 53 tabelas municipais, casa a casa, com o status de cada casa | IBGE PDFs |
| `clima` | Dados climáticos mensais/diários por UF ou estação | INMET → NASA POWER |
| `comercio_internacional` | Comércio internacional bilateral por HS code | UN Comtrade |
| `comparacao_anual_anec` | Comparação mensal entre anos por edição | ANEC |
| `composicao_defensivos` | Componentes e concentrações por família e registro | Agrofit/MAPA |
| `condicao_lavouras` | Condição semanal das lavouras do Paraná | DERAL |
| `credito_rural` | Crédito rural por cultura (programa, seguro, modalidade) | BCB/SICOR → BigQuery |
| `custo_producao` | Custos de produção | CONAB |
| `custo_sociobiodiversidade` | Custos da sociobiodiversidade nas unidades publicadas | CONAB |
| `defensivos_formulados` | Produtos formulados por registro | Agrofit/MAPA |
| `defensivos_tecnicos` | Produtos técnicos por registro | Agrofit/MAPA |
| `desmatamento` | Desmatamento PRODES/DETER — consolidado + alertas | INPE TerraBrasilis |
| `destinos_anec` | Participação dos destinos no período acumulado por edição | ANEC |
| `embarques_anec` | Embarques semanais por porto e produto | ANEC |
| `embarques_mensais_anec` | Volumes mensais, estimativas e faixas por edição | ANEC |
| `empregadores_lista_suja` | Cadastro nacional corrente de empregadores do MTE, sem recorte agropecuário | MTE / Lista Suja |
| `estimativa_safra` | Estimativas safra corrente | CONAB → IBGE LSPA |
| `exportacao` | Exportações agrícolas | ComexStat → ABIOVE |
| `extrativismo_vegetal` | Produção extrativista vegetal (açaí, castanha, erva-mate) | IBGE PEVS |
| `fertilizante` | Entregas de fertilizantes | ANDA |
| `futuros_agricolas` | Futuros agrícolas B3 (ajustes, histórico, posições) | B3 |
| `importacao` | Importações agrícolas | ComexStat |
| `leite_industrial` | Aquisição e industrialização trimestral de leite por UF | IBGE Leite |
| `movimentacao_portuaria` | Movimentação portuária de carga (granel, geral, contêiner) | ANTAQ |
| `oferta_demanda_global` | Oferta/demanda global de commodities (USDA PSD) | USDA |
| `pecuaria_municipal` | Rebanhos e produção animal | IBGE PPM |
| `pib_agro` | PIB agropecuário por setor e trimestre | IBGE SIDRA |
| `posicionamento_fundos` | Posicionamento de fundos por categoria de trader (COT) | CFTC |
| `preco_atacado` | Preços de atacado hortifrúti em CEASAs | CONAB CEASA/PROHORT |
| `preco_diario` | Preços diários spot | CEPEA → cache |
| `producao_anual` | Produção anual consolidada | IBGE PAM → CONAB |
| `progresso_safra` | Progresso semanal semeadura/colheita | CONAB |
| `queimadas` | Focos de calor por satélite (6 biomas) | INPE |
| `seguro_rural` | Apólices e sinistros do seguro rural | MAPA PSR |
| `serie_historica_safra` | Série histórica de safras — 45 produtos, cobertura conforme cultura | CONAB |
| `silvicultura` | Produção silvicultural (eucalipto, pinus, carvão vegetal) | IBGE PEVS |
| `uso_do_solo` | Cobertura e uso da terra anual por UF/município | MapBiomas |
| `zoneamento_agricola` | Zoneamento agrícola de risco climático (ZARC) | MAPA/Embrapa |

O [dataset Lista Suja](api/empregadores_lista_suja.md) reutiliza a publicação corrente do MTE e suas rotas CSV/PDF, sem snapshot histórico ou segunda fonte institucional.

Os quatro [datasets Agrofit](api/defensivos_datasets.md) reutilizam a fonte única `defensivos` e seus contratos existentes. Não há fallback para outra fonte.

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
| Sem histórico | Snapshots em parquet sob demanda; cache DuckDB só para os indicadores CEPEA |
| Encoding caótico | Fallback chain automático |
| Escolher fonte | Datasets abstraem a fonte |

## Quality & Reliability

| Métrica | Valor |
|---------|-------|
| Validação | Escopo e limites documentados |
| Golden tests | fixtures por fonte (dados reais ou sintéticos) |
| Resiliência HTTP | Retry centralizado + 429/Retry-After |
| Benchmarks | Memory, volume, cache, async, rate limiting |

## Features

- **41 fontes públicas** — CEPEA, CONAB, IBGE, NASA POWER, BCB/SICOR, ComexStat, ANDA, ABIOVE, ANEC, USDA, IMEA, DERAL, INMET, Notícias Agrícolas, Queimadas/INPE, Desmatamento, MapBiomas, B3 Futuros Agro, UN Comtrade, ANTAQ, ANP Diesel, MAPA PSR, ANTT Pedagio, SICAR, ZARC, Agrofit/MAPA (Defensivos), FUNAI, ICMBio, CNUC/MMA, INCRA, IBAMA, MapBiomas Alerta, Lista Suja, ANA/SNIRH, SFB, RNC/CultivarWeb, EMBRAPA Solos, Fundação Rio Verde, Acervo Fundiário/INCRA, CFTC COT, UNICA
- **Golden tests** — fixtures de referência por fonte (dados reais ou sintéticos documentados)
- **Resiliência HTTP** — `retry_on_status()`/`retry_async()` centralizado, Retry-After, 429 handling
- **Camada semântica** — datasets com proveniência e fallback quando há fontes alternativas configuradas
- **Contratos públicos** — schema versionado com garantias de estabilidade
- **Modo determinístico + snapshots** — reprodutibilidade para papers e auditorias (modo determinístico no `preco_diario`; snapshots CEPEA/CONAB/IBGE) ([guia](guides/snapshots.md))
- **Async-first** com sync wrapper para uso simples
- **Cache DuckDB** dos indicadores CEPEA, com TTL inteligente (expira às 18h)
- **Suporte pandas + polars** (`as_polars=True`)
- **CLI completa** (`agrobr cepea indicador soja --formato csv`)
- **Validação** — Pydantic v2 + sanity checks estatísticos + fingerprinting
- **Monitoramento** — health checks diários + alertas Discord/Slack

## Próximos Passos

- [Guia Rápido](quickstart.md) — Tutorial completo
- [Datasets](contracts/index.md) — Contratos e garantias
- [API pública](api/index.md) — Módulos públicos e referência detalhada
- [Fontes](sources/index.md) — Proveniência e rastreabilidade
- [Exemplos](https://github.com/bruno-portfolio/agrobr/tree/main/examples) — Scripts de exemplo
- [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/bruno-portfolio/agrobr/blob/main/examples/agrobr_demo.ipynb) — Notebook interativo com todas as fontes

## Licença

MIT License — veja [LICENSE](https://github.com/bruno-portfolio/agrobr/blob/main/LICENSE)
