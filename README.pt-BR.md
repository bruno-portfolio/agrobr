# agrobr

> Dados agrícolas brasileiros em uma linha de código

**⚠️ O agrobr 2.0 tem mudanças incompatíveis.** Antes de atualizar, leia o [resumo do que quebra](https://www.agrobr.dev/docs/guides/migracao-2/#resumo-o-que-quebra) no guia de migração. Para ficar na série 1.x por enquanto: `pip install "agrobr<2"`.

**🇺🇸 [Read in English](https://github.com/bruno-portfolio/agrobr/blob/main/README.md)**

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

<p align="center">
  <a href="https://htmlpreview.github.io/?https://github.com/bruno-portfolio/agrobr/blob/main/docs/canopy.html">
    <img src="https://raw.githubusercontent.com/bruno-portfolio/agrobr/main/docs/canopy.svg" width="100%" />
  </a>
</p>

Infraestrutura Python para dados agrícolas brasileiros com camada semântica sobre **41 fontes públicas** — preços de mercado, produção e safras, comércio exterior, crédito rural, clima, monitoramento ambiental, cadastros territoriais e regulatório.

O Brasil é um dos maiores produtores agrícolas do mundo, mas os dados públicos estão espalhados por dezenas de portais do governo, cada um com seu formato, codificação e peculiaridades. O agrobr transforma tudo isso em DataFrames limpos e validados.

**v2.0.0** — 54 datasets | 89 contratos versionados | evidências de validação por incremento | validação de parâmetros antes da rede | golden tests por fonte

## Demo
![Animation](https://github.com/user-attachments/assets/40e1341e-f47b-4eb5-b18e-55b49c63ee97)

## Instalação

```bash
pip install agrobr
```

Com extras opcionais:
```bash
pip install agrobr[pdf]             # pdfplumber para ANDA, ANEC, UNICA (relatório quinzenal), Lista Suja (rota PDF) e Rio Verde
pip install agrobr[polars]          # Suporte a Polars
pip install agrobr[browser]         # Playwright (opcional, para fontes com JS)
pip install agrobr[bigquery]        # Base dos Dados (fallback BCB/SICOR)
pip install agrobr[geo]             # GeoPandas — habilita variantes _geo (PRODES, DETER, SICAR, FUNAI, ICMBio, CNUC, IBGE (malha municipal e áreas urbanizadas), INCRA, IBAMA, Queimadas, MapBiomas Alerta, ANA, SFB, EMBRAPA Solos, Acervo Fundiário)
pip install agrobr[all]             # Todas as integrações opcionais de runtime
```

Os levantamentos de safra e balanços de oferta/demanda da CONAB usam HTTP primeiro; Playwright
e Chromium são fallback opcional de transporte:

```bash
pip install agrobr[browser]
python -m playwright install chromium
```

### Docker

```bash
docker build -t agrobr .
docker run -it --rm agrobr
```

```python
>>> from agrobr.sync import cepea
>>> df = cepea.indicador('soja', inicio='2024-01-01')
```

```bash
# CLI
docker run --rm agrobr agrobr cepea indicador boi

# Persistir cache entre execuções
docker run -it --rm -v agrobr-cache:/home/agrobr/.agrobr agrobr

# Com extras adicionais (EXTRAS substitui o default "browser,pdf")
docker build --build-arg EXTRAS="browser,pdf,polars" -t agrobr:extras .

# Rodar script local
docker run --rm -v "$(pwd)":/work agrobr python /work/analise.py
```

> A imagem default inclui Playwright + Chromium e pdfplumber. Veja o [guia Docker](https://www.agrobr.dev/docs/guides/docker/) para extras adicionais.

## Uso por categoria

Os exemplos abaixo usam a forma `async`. Para a equivalente sem `async/await`, veja [Modo síncrono](#modo-síncrono). Funções que retornam DataFrame aceitam `as_polars=True` e `return_meta=True` (proveniência); as variantes `_geo` retornam GeoDataFrame e aceitam só `return_meta`.

### Preços e mercado

CEPEA (spot diário), B3 (futuros agro), IMEA (Mato Grosso), CONAB CEASA/PROHORT (atacado hortifruti), ANP Diesel.

```python
from agrobr import cepea

# Indicadores diários CEPEA — soja, milho, café, boi, trigo, algodão, arroz, etc.
df = await cepea.indicador('soja', inicio='2024-01-01')
ultimo = await cepea.ultimo('soja')
print(f"Soja: R$ {ultimo.valor}/sc em {ultimo.data}")

print(await cepea.produtos())       # 22 produtos disponíveis
print(await cepea.pracas('soja'))   # praças de comercialização por produto
```

| Fonte | Função carro-chefe | Doc |
|-------|--------------------|-----|
| **B3** futuros agro | `b3.ajustes(data="13/02/2025")`, `b3.posicoes_abertas(data=...)`, `b3.historico(contrato="boi", inicio=..., fim=...)` | [docs/sources/b3.md](https://www.agrobr.dev/docs/sources/b3/) |
| **CFTC COT** posicionamento de fundos (Chicago/NY) | `cftc.cot("soja", inicio="2026-05-01")` | [docs/sources/cftc.md](https://www.agrobr.dev/docs/sources/cftc/) |
| **IMEA** Mato Grosso | `imea.cotacoes("soja", safra="24/25")` | [docs/sources/imea.md](https://www.agrobr.dev/docs/sources/imea/) |
| **CONAB CEASA** | `conab.ceasa_precos(produto="tomate", ceasa="SAO PAULO")` | [docs/sources/conab_ceasa.md](https://www.agrobr.dev/docs/sources/conab_ceasa/) |
| **ANP Diesel** | `alt.anp_diesel.precos_diesel(uf="MT")`, `alt.anp_diesel.vendas_diesel(uf="MT")` | [docs/sources/anp_diesel.md](https://www.agrobr.dev/docs/sources/anp_diesel/) |

### Produção e safras

CONAB (safras, balanço, custo, série histórica, progresso), IBGE (PAM, LSPA, PPM, Abate, PEVS, Leite, PIB, Censo Agro), DERAL, USDA PSD, ABIOVE, ANEC, UNICA, Rio Verde.

```python
from agrobr import conab, ibge

# CONAB — safra atual + balanço oferta/demanda
df = await conab.safras('soja', safra='2024/25')
df = await conab.balanco('soja')
df = await conab.serie_historica('soja', ano_inicio=2010, ano_fim=2024)
df = await conab.progresso_safra(produto='Soja', uf='MT', operacao='Colheita')
catalogo = await conab.catalogo_custos("soja")
planilha = catalogo["planilha"].max()
contextos = await conab.catalogo_custos("soja", planilha=planilha)
mt = contextos[contextos["uf"].eq("MT") & contextos["status"].eq("identified")]
aba = mt.sort_values("ano_referencia")["aba"].iloc[-1]
df = await conab.custo_producao("soja", uf="MT", planilha=planilha, aba=aba)

# IBGE — Produção Agrícola Municipal (anual)
df = await ibge.pam('soja', ano=2023, nivel='uf')
df = await ibge.pam('cafe', ano=2023, nivel='municipio', uf='MG')
df = await ibge.lspa('soja', ano=2024, mes=6)        # Levantamento Sistemático mensal
df = await ibge.ppm('bovino', ano=2023)               # Pecuária Municipal
df = await ibge.abate('frango', trimestre='202303', uf='PR')

# Censo Agropecuário — 1995/2006/2017 + série histórica 1920-2006 + 1985 municipal
df = await ibge.censo_agro('efetivo_rebanho')
df = await ibge.censo_agro_historico('estabelecimentos_area')
temas_1985 = await ibge.temas_censo_agro_municipal_1985()
```

**Pacote local, 1 linha por casa do PDF.** O agrobr extraiu os números dos 53 temas (tabelas 67 a 119 dos 28 volumes estaduais do IBGE) e os traz em `agrobr/data/censo_1985/`: a consulta não acessa a rede. `valor` só vem quando a casa foi confirmada pelas somas impressas (município → microrregião → mesorregião → UF); `valor_lido` traz a leitura sempre, e o `status` dá o nível de confiança, com a precisão medida contra oráculos cegos. Veja o [contrato](https://www.agrobr.dev/docs/contracts/censo_agropecuario_municipal_1985/).

| Fonte | Função carro-chefe | Doc |
|-------|--------------------|-----|
| **IBGE PEVS** | `ibge.silvicultura('madeira_tora', ano=2023)`, `ibge.extracao_vegetal('acai', ano=2023)` | [docs/sources/ibge.md](https://www.agrobr.dev/docs/sources/ibge/) |
| **IBGE Leite** | `ibge.leite_trimestral(trimestre='202303', uf='MG')` | [docs/sources/ibge.md](https://www.agrobr.dev/docs/sources/ibge/) |
| **IBGE PIB Agro** | `ibge.pib_agro(trimestre='202501', setor='agropecuaria')` | [docs/sources/ibge.md](https://www.agrobr.dev/docs/sources/ibge/) |
| **DERAL** condição PR | `deral.condicao_lavouras('soja')` | [docs/sources/deral.md](https://www.agrobr.dev/docs/sources/deral/) |
| **USDA PSD** internacional | `usda.psd('soja', country='BR', market_year=2024)` (requer `AGROBR_USDA_API_KEY`) | [docs/sources/usda.md](https://www.agrobr.dev/docs/sources/usda/) |
| **ABIOVE** complexo soja | `abiove.exportacao(ano=2024, produto='grao')` | [docs/sources/abiove.md](https://www.agrobr.dev/docs/sources/abiove/) |
| **ANEC** embarques semanais | `anec.embarques(ano=2026)`, `anec.destinos(ano=2026)` | [docs/sources/anec.md](https://www.agrobr.dev/docs/sources/anec/) |
| **UNICA** moagem Centro-Sul | `unica.moagem_quinzenal('cana')`, `unica.safra_resumo()`, `unica.producao_historica('acucar')` | [docs/sources/unica.md](https://www.agrobr.dev/docs/sources/unica/) |
| **Rio Verde** ensaios cultivares MT | `rio_verde.ensaio_soja(safra='2025/2026')` | [docs/sources/rio_verde.md](https://www.agrobr.dev/docs/sources/rio_verde/) |

### Comércio e logística

ComexStat (BR), UN Comtrade (mundial bilateral), ANTAQ (portos), ANTT Pedágio (rodovias).

```python
from agrobr import comexstat, comtrade

# Exportações/importações brasileiras por NCM/UF, mensal
df = await comexstat.exportacao('soja', ano=2024, agregacao='mensal')
df = await comexstat.importacao('fertilizantes', ano=2024)

# Comércio bilateral mundial (UN Comtrade)
df = await comtrade.comercio('soja', reporter='BR')
df = await comtrade.trade_mirror('soja', reporter='BR')   # validação cruzada exportador/importador
```

| Fonte | Função carro-chefe | Doc |
|-------|--------------------|-----|
| **ANTAQ** portos | `antaq.movimentacao(ano=2024)` | [docs/sources/antaq.md](https://www.agrobr.dev/docs/sources/antaq/) |
| **ANTT Pedágio** | `alt.antt_pedagio.fluxo_pedagio(ano=2024)`, `alt.antt_pedagio.pracas_pedagio(uf='SP')` | [docs/sources/antt_pedagio.md](https://www.agrobr.dev/docs/sources/antt_pedagio/) |

### Crédito, câmbio e seguro

BCB (SICOR + SGS + PTAX + Focus), MAPA PSR.

```python
from agrobr import bcb, alt

# BCB SICOR — crédito rural
df = await bcb.credito_rural('soja', safra='2024/25')
df = await bcb.credito_rural('soja', safra='2024/25', programa='Pronamp')

# BCB SGS — séries temporais (Selic, IPCA, IPA agro, câmbio, etc.)
df = await bcb.sgs('selic', ultimos=12)
df = await bcb.sgs('ipa_agricola', inicio='01/01/2020')
df = await bcb.sgs('pib_agropecuaria')                    # também: ipca, igpm, cdi, tjlp, dolar_ptax_venda...

# BCB PTAX — cotação dólar
df = await bcb.ptax(inicio='01/01/2024', fim='31/12/2024')

# BCB Focus — expectativas de mercado
df = await bcb.focus('PIB Agropecuária')

# MAPA PSR — apólices e sinistros do seguro rural
df = await alt.mapa_psr.apolices(produto='soja', ano=2023)
df = await alt.mapa_psr.sinistros(produto='soja', uf='MT')
```

### Clima e água

NASA POWER (climatologia global), INMET (estações brasileiras, requer token), ANA/SNIRH (hidrografia, irrigação).

```python
from agrobr import nasa_power, inmet, ana

# NASA POWER — climatologia por ponto ou UF (sem auth)
df = await nasa_power.clima_uf('MT', ano=2024)
df = await nasa_power.clima_ponto(-12.6, -56.1, '2024-01-01', '2024-12-31')

# INMET — estações observacionais (requer AGROBR_INMET_TOKEN)
df = await inmet.estacao('A001', '2024-01-01', '2024-01-31')
df = await inmet.clima_uf('SP', ano=2024)

# ANA/SNIRH — pivôs de irrigação por UF
df = await ana.pivos_irrigacao(uf='MT')
gdf = await ana.pivos_irrigacao_geo(uf='MT')   # requer agrobr[geo]
```

> A API observacional do INMET levanta `SourceUnavailableError` sem token. Configure: `export AGROBR_INMET_TOKEN=seu_token`. Para clima sem token, use NASA POWER.

### Ambiental

Queimadas (focos INPE), Desmatamento (PRODES + DETER), MapBiomas (cobertura/transição), MapBiomas Alerta, IBAMA (embargos), ICMBio e CNUC (UCs), SFB (florestas públicas).

```python
from agrobr import queimadas, desmatamento, mapbiomas

# Queimadas — focos de calor por satélite (6 biomas, 13 satélites)
df = await queimadas.focos(ano=2024, mes=9, uf='MT', bioma='Amazonia')

# Desmatamento — PRODES (anual consolidado) + DETER (alertas em tempo real)
df = await desmatamento.prodes(bioma='Cerrado', ano=2022, uf='MT')
df = await desmatamento.deter(
    bioma='Amazônia', uf='PA',
    inicio='2024-01-01', fim='2024-06-30',
)

# MapBiomas — uso e cobertura da terra (1985-presente)
df = await mapbiomas.cobertura(uf='MT', ano=2022)
df = await mapbiomas.transicao(uf='PA')
df = await mapbiomas.cobertura(nivel='municipio', municipio='5107925', ano=2025)

# Variantes geo (requerem agrobr[geo])
gdf = await desmatamento.prodes_geo(bioma='Cerrado', ano=2022, uf='MT')
gdf = await queimadas.focos_geo(ano=2024, mes=9, uf='MT')
```

| Fonte | Função carro-chefe | Doc |
|-------|--------------------|-----|
| **MapBiomas Alerta** | `mapbiomas_alerta.alertas(inicio='2024-01-01')` (requer `AGROBR_MAPBIOMAS_ALERTA_TOKEN`) | [docs/sources/mapbiomas_alerta.md](https://www.agrobr.dev/docs/sources/mapbiomas_alerta/) |
| **IBAMA** embargos | `ibama.embargos(uf='PA')` | [docs/sources/ibama.md](https://www.agrobr.dev/docs/sources/ibama/) |
| **ICMBio** UCs federais | `icmbio.ucs(uf='AM', grupo='PI')` | [docs/sources/icmbio.md](https://www.agrobr.dev/docs/sources/icmbio/) |
| **CNUC** UCs das 3 esferas, com RPPNs | `cnuc.ucs(uf='SE', esfera='municipal')` | [docs/sources/cnuc.md](https://www.agrobr.dev/docs/sources/cnuc/) |
| **SFB** florestas públicas | `sfb.cnfp(uf='AM')`, `sfb.concessoes(uf='AM')`, `sfb.ifn_conglomerados(uf='MT')` | [docs/sources/sfb.md](https://www.agrobr.dev/docs/sources/sfb/) |

### Cadastros territoriais

SICAR (CAR), Acervo Fundiário INCRA (SIGEF/SNCI/assentamentos), FUNAI (terras indígenas), INCRA (quilombolas), EMBRAPA Solos (PronaSolos + SiBCS).

```python
from agrobr import alt, acervo_fundiario, funai, incra, embrapa_solos

# SICAR — Cadastro Ambiental Rural (imóveis rurais por UF)
df = await alt.sicar.imoveis('DF')
df = await alt.sicar.resumo('MT', municipio='Sorriso')

# Acervo Fundiário/INCRA — parcelas certificadas e assentamentos
df = await acervo_fundiario.sigef('MT')
df = await acervo_fundiario.snci('PA')
df = await acervo_fundiario.assentamentos(uf='PA')

# FUNAI — terras indígenas
df = await funai.terras_indigenas(uf='AM', fase='Regularizada')

# INCRA — territórios quilombolas
df = await incra.quilombolas(uf='BA')

# EMBRAPA Solos — perfis pedológicos PronaSolos + mapa SiBCS
df = await embrapa_solos.perfis(uf='SP')
df = await embrapa_solos.mapa_solos(ordem='LATOSSOLO')

# Variantes geo (requer agrobr[geo])
gdf = await alt.sicar.imoveis_geo('DF')
gdf = await funai.terras_indigenas_geo(uf='AM')
gdf = await acervo_fundiario.sigef_geo('MT')
gdf = await embrapa_solos.mapa_solos_geo(ordem='LATOSSOLO')
```

### Insumos e regulatório

ANDA (fertilizantes), Defensivos/Agrofit (agrotóxicos), RNC (cultivares), Lista Suja (trabalho escravo), ZARC (zoneamento).

```python
from agrobr import anda, defensivos, rnc, lista_suja, zarc

# ANDA — entregas de fertilizantes (requer agrobr[pdf])
df = await anda.entregas(ano=2024)

# Defensivos/Agrofit — agrotóxicos registrados no Brasil
df = await defensivos.formulados(ingrediente_ativo='glifosato')
df = await defensivos.tecnicos(titular='Bayer')
df = await defensivos.autorizacoes(cultura='soja')

# RNC/CultivarWeb — cadastros correntes de registradas e protegidas
df = await rnc.registradas(especie='Soja')
df = await rnc.protegidas(titular='Embrapa')

# Lista Suja — empregadores em condição análoga à escravidão
df = await lista_suja.empregadores(uf='PA')

# ZARC — Zoneamento Agrícola de Risco Climático
df = await zarc.zoneamento(produto='soja', uf='MT')
print(zarc.culturas())   # 107 culturas (tábuas anual, perene e rótulos legados)
```

## Camada semântica — datasets

Use `datasets` para o dado normalizado, com proveniência rastreada. O fallback automático vale onde o dataset tem fonte alternativa compatível configurada; o dataset de fonte única usa só essa fonte.

```python
from agrobr import datasets

# Preço diário (CEPEA → fallback)
df = await datasets.preco_diario('soja')

# Produção anual (IBGE PAM → CONAB)
df = await datasets.producao_anual('soja', ano=2023)

# Estimativa safra corrente (CONAB → IBGE LSPA)
df = await datasets.estimativa_safra('soja', safra='2024/25')

# Crédito rural (BCB SICOR → BigQuery via basedosdados)
df = await datasets.credito_rural('soja', safra='2024/25')

# Clima (INMET → NASA POWER)
df = await datasets.clima(uf='SP', ano=2024)

# Com proveniência: meta inclui qual fonte foi usada e quais foram tentadas
df, meta = await datasets.preco_diario('soja', return_meta=True)
print(meta.selected_source, meta.attempted_sources, meta.contract_version)

# Listar todos os datasets disponíveis
print(datasets.list_datasets())
```

54 datasets disponíveis. Veja a [lista completa](#datasets-disponíveis) abaixo.

## Reprodutibilidade — snapshots e modo determinístico

Snapshots exportam dados em parquet para análises reproduzíveis — papers, auditorias e pipelines de CI.

```python
from agrobr import datasets
from agrobr.snapshots import create_snapshot, list_snapshots, delete_snapshot

# Criar snapshot (salva dados atuais em ~/.agrobr/snapshots/; CONAB e IBGE vão à rede)
info = await create_snapshot("2025-Q4")
info = await create_snapshot(sources=["cepea", "conab"])

# Listar e remover
for s in list_snapshots():
    print(s.name, s.file_count, f"{s.size_bytes/1024/1024:.1f} MB")
delete_snapshot("2025-Q4")

# Modo determinístico — fixa a data de referência; só o preco_diario lê do cache local, sem rede
async with datasets.deterministic("2025-12-31"):
    df = await datasets.preco_diario("soja")
```

O modo determinístico não lê snapshots: ele fixa a data de referência. Só o `preco_diario` a honra, lendo do cache
local, sem rede, até essa data (sem o produto no cache, levanta `SourceUnavailableError`). Os demais datasets recusam o
contexto antes da rede ou consultam a fonte corrente e avisam, em `validation_warnings` e com `UserWarning`, que o dado
não é o da data. Veja o [guia de snapshots](https://www.agrobr.dev/docs/guides/snapshots/).

Via CLI:

```bash
agrobr snapshot create 2025-Q4 --sources cepea,conab,ibge
agrobr snapshot list
agrobr snapshot delete 2025-Q4
```

## Modo síncrono

```python
from agrobr.sync import cepea, conab, ibge, datasets, alt

# Mesmo API, sem async/await
df = cepea.indicador('soja', inicio='2024-01-01')
df = conab.safras('soja', safra='2024/25')
df = ibge.pam('soja', ano=2023)
df = datasets.preco_diario('soja')
df = alt.sicar.imoveis('DF')
```

Qualquer fonte top-level + `alt` está disponível em `agrobr.sync` com a mesma assinatura em tempo de execução. O checker de tipos e o editor não a veem, porque o espelho é dinâmico e devolve `Any`; para código tipado, use a API assíncrona.

## Suporte Polars

```python
df = await cepea.indicador('soja', as_polars=True)
df = await datasets.preco_diario('soja', as_polars=True)
df = await ibge.pam('soja', ano=2023, as_polars=True)
```

Suportado nas source APIs e datasets, exceto nas variantes `_geo` (GeoDataFrame).

## CLI

```bash
# Fontes
agrobr cepea indicador soja --ultimo
agrobr cepea indicador milho --inicio 2024-01-01 --formato csv
agrobr conab safras soja --safra 2024/25
agrobr conab balanco milho
agrobr ibge pam soja --ano 2023 --nivel uf
agrobr ibge lspa milho --ano 2024 --mes 6

# Diagnóstico e configuração
agrobr health
agrobr health --source cepea --deep
agrobr doctor --verbose
agrobr config show

# Snapshots
agrobr snapshot create 2025-Q4 --sources cepea,conab,ibge
agrobr snapshot list
```

## Datasets disponíveis

| Dataset | Descrição | Fontes |
|---------|-----------|--------|
| `cotacoes_cambio` | Cotações e paridades cambiais dos boletins PTAX/BCB | BCB |
| `expectativas_mercado` | Expectativas anuais e mensais de mercado do Focus/BCB | BCB |
| `moedas_cambio` | Catálogo corrente de moedas do serviço PTAX/BCB | BCB |
| `precos_diesel` | Preços semanais de diesel da ANP e médias mensais derivadas | ANP |
| `unidades_conservacao` | Unidades de conservação federais, estaduais e municipais, com RPPNs, do CNUC | CNUC/MMA |
| `unidades_conservacao_federais` | Cadastro corrente de unidades de conservação federais da camada ICMBio/INDE | ICMBio |
| `abate_trimestral` | Abate de bovinos, suínos e frangos por UF | IBGE Abate |
| `autorizacoes_defensivos` | Autorizações de uso com multiplicidade publicada | Agrofit/MAPA |
| `balanco` | Oferta/demanda | CONAB |
| `cadastro_rural` | Cadastro Ambiental Rural (imóveis rurais por UF) | SICAR/GeoServer WFS |
| `censo_agropecuario` | Censo Agropecuário 1995/2006/2017 (11 temas) | IBGE Censo Agro |
| `censo_agropecuario_historico` | Série histórica Censo Agropecuário 1920-2006 (9 temas) | IBGE SIDRA |
| `censo_agropecuario_legado` | Censo 1995/96 — 6 temas legados (FTP) | IBGE FTP |
| `censo_agropecuario_municipal_1985` | Censo 1985 — 53 temas, 1 linha por casa do PDF, `valor` só quando confirmado pelas somas impressas | IBGE PDFs |
| `clima` | Dados climáticos mensais/diários por UF ou estação | INMET → NASA POWER |
| `comercio_internacional` | Comércio internacional bilateral por HS code | UN Comtrade |
| `comparacao_anual_anec` | Comparação mensal entre anos explícitos por edição | ANEC |
| `composicao_defensivos` | Componentes e concentrações por família e registro | Agrofit/MAPA |
| `condicao_lavouras` | Condição semanal das lavouras do Paraná | DERAL |
| `credito_rural` | Crédito rural por cultura (programa, seguro, modalidade) | BCB/SICOR → BigQuery |
| `cultivares_protegidas` | Cultivares protegidas por processo, preservando término textual | CultivarWeb/SNPC |
| `cultivares_registradas` | Cultivares registradas com identificador textual exato | CultivarWeb/RNC |
| `custo_producao` | Custos de produção | CONAB |
| `custo_sociobiodiversidade` | Custos da sociobiodiversidade nas unidades publicadas | CONAB |
| `defensivos_formulados` | Produtos formulados por registro | Agrofit/MAPA |
| `defensivos_tecnicos` | Produtos técnicos por registro | Agrofit/MAPA |
| `desmatamento` | Desmatamento PRODES/DETER — consolidado + alertas | INPE TerraBrasilis |
| `destinos_anec` | Participação acumulada dos destinos com período e edição | ANEC |
| `embarques_anec` | Embarques semanais por porto (soja, farelo, milho, DDGS, sorgo, trigo) | ANEC |
| `embarques_mensais_anec` | Volumes mensais e faixas de estimativa por edição | ANEC |
| `empregadores_lista_suja` | Cadastro nacional corrente de empregadores do MTE, sem restrição ao agro | MTE / Lista Suja |
| `estimativa_safra` | Estimativas safra corrente | CONAB → IBGE LSPA |
| `exportacao` | Exportações agrícolas | ComexStat → ABIOVE |
| `extrativismo_vegetal` | Produção extrativista vegetal (açaí, castanha, erva-mate) | IBGE PEVS |
| `fertilizante` | Entregas de fertilizantes | ANDA |
| `futuros_agricolas` | Futuros agrícolas B3 (ajustes, histórico, posições) | B3 |
| `importacao` | Importações agrícolas | ComexStat |
| `leite_industrial` | Aquisição e industrialização trimestral de leite por UF | IBGE Leite |
| `movimentacao_portuaria` | Movimentação portuária de carga (granel, geral, contêiner) | ANTAQ |
| `oferta_demanda_global` | Oferta/demanda global de commodities | USDA PSD |
| `pecuaria_municipal` | Pecuária municipal (rebanhos e produção animal) | IBGE PPM |
| `posicionamento_fundos` | Posicionamento semanal de traders nos futuros agro de Chicago/NY (COT) | CFTC |
| `pib_agro` | PIB Agropecuária por setor e trimestre | IBGE SIDRA |
| `preco_atacado` | Preços de atacado hortifrúti em CEASAs | CONAB CEASA/PROHORT |
| `preco_diario` | Preços diários spot | CEPEA → cache |
| `producao_anual` | Produção anual consolidada | IBGE PAM → CONAB |
| `progresso_safra` | Progresso semanal semeadura/colheita | CONAB |
| `queimadas` | Focos de calor por satélite (6 biomas) | INPE |
| `seguro_rural` | Apólices e sinistros do seguro rural | MAPA PSR |
| `serie_historica_safra` | Série histórica de safras — 45 produtos, cobertura conforme cultura | CONAB |
| `series_economicas` | Séries econômicas por código ou alias SGS, intervalo e últimas observações | BCB SGS |
| `silvicultura` | Produção silvicultural (eucalipto, pinus, carvão vegetal) | IBGE PEVS |
| `uso_do_solo` | Cobertura e uso da terra anual por UF/município | MapBiomas |
| `zoneamento_agricola` | Zoneamento agrícola de risco climático (ZARC) | MAPA/Embrapa |

Os dois [datasets de cultivares](https://www.agrobr.dev/docs/api/cultivares/) expõem os cadastros correntes RNC/SNPC com filtros exatos por identificador, cache bruto de aquisição de 24 horas e proveniência. Exemplo: `await datasets.cultivares_registradas(nr_registro='42039', return_meta=True)`. Cultivares protegidas conservam o término publicado em `termino_protecao_texto`, incluindo condições sem data definida. Esses datasets não reconstituem snapshots históricos.

O [dataset da Lista Suja](https://www.agrobr.dev/docs/api/empregadores_lista_suja/) reaproveita a publicação corrente do MTE e as rotas CSV/PDF dela, sem snapshot histórico nem segunda fonte institucional.

Os quatro [datasets do Agrofit](https://www.agrobr.dev/docs/api/defensivos_datasets/) reaproveitam a fonte única `defensivos` e os contratos dela. Não têm fallback para outra fonte.

## Fontes suportadas

Disponibilidade monitorada automaticamente. Use `agrobr health` para verificar localmente (ou `agrobr health --source <nome> --deep` pra checagem específica com parse).

| Fonte | Dados | Golden Test | Status |
|-------|-------|:-----------:|--------|
| CEPEA | Indicadores de preços (22 produtos) | ✅ | Funcional |
| CONAB | Safras, balanço, custos, série histórica, progresso semanal, CEASA/PROHORT preços atacado | ✅ | Funcional |
| IBGE | PAM, LSPA, PPM, Abate, PEVS, Leite, PIB, Censo Agro (1995-96/2006/2017 + série histórica) | ✅ | Censo municipal 1985 pelo pacote local (`valor` só quando confirmado); demais rotas funcionais |
| NASA POWER | Climatologia diária/mensal (grid 0.5°) | ✅ | Funcional |
| BCB/SICOR | Crédito rural por cultura + séries SGS + PTAX + Focus | ✅¹ | Funcional |
| ComexStat | Exportações e importações por NCM/UF | ✅¹ | Funcional |
| ANDA | Entregas de fertilizantes | ✅ | Funcional |
| ABIOVE | Exportação complexo soja (volume/receita) | ✅ | Funcional |
| ANEC | Embarques semanais por porto (soja, farelo, milho, DDGS, sorgo, trigo) | ✅ | Funcional |
| USDA PSD | Estimativas internacionais (produção/oferta/demanda) | ✅¹ | Funcional |
| IMEA | Cotações e indicadores Mato Grosso | ✅ | Funcional |
| DERAL | Condição das lavouras Paraná | ✅ | Funcional |
| INMET | Meteorologia (600+ estações) | ✅¹ | Requer `AGROBR_INMET_TOKEN` |
| Notícias Agrícolas | Cotações (fallback CEPEA, uso interno) | ✅¹ | Funcional |
| Queimadas/INPE | Focos de calor por satélite (6 biomas, 13 satélites) | ✅ | Funcional |
| Desmatamento PRODES/DETER | Desmatamento consolidado + alertas (TerraBrasilis WFS) | ✅ | Funcional |
| MapBiomas | Cobertura e uso da terra (1985-presente), nível estado e município | ✅ | Funcional |
| B3 Futuros Agro | Ajustes diários + posições em aberto (7 contratos agro) | ✅ | Funcional |
| UN Comtrade | Comércio bilateral + trade mirror (~200 países, HS codes) | ✅¹ | Funcional |
| ANTAQ | Movimentação portuária de carga (granel, geral, contêiner) | ⚠️ | Fonte fora do ar desde 23/06/2026 |
| ANP Diesel | Preços revenda + volumes diesel por UF/município | ✅ | Funcional |
| MAPA PSR | Apólices e sinistros seguro rural (2006+, 27 UFs) | ✅ | Funcional |
| ANTT Pedágio | Fluxo de veículos em praças de pedágio (2010+, 200+ praças) | ✅ | Funcional |
| SICAR | Cadastro Ambiental Rural — imóveis rurais por UF (7,4M+ registros, WFS) | ✅ | Funcional |
| ZARC | Zoneamento Agrícola de Risco Climático (janelas de plantio por município/cultura/solo) | ✅ | Funcional |
| Agrofit/MAPA (Defensivos) | Agrotóxicos registrados — formulados, autorizações, técnicos (~8K produtos) | ✅ | Funcional |
| MapBiomas Alerta | Alertas de desmatamento via GraphQL (500K+ alertas) | ✅ | Requer `AGROBR_MAPBIOMAS_ALERTA_TOKEN` |
| Lista Suja | Cadastro corrente de empregadores do MTE (trabalho escravo), CSV/TXT com alternativa em PDF | ✅ | Funcional |
| ANA/SNIRH | Hidrografia, pivôs irrigação, demanda irrigação, disponibilidade hídrica (ArcGIS REST) | ✅ | Funcional |
| SFB | Florestas públicas (CNFP), concessões florestais, IFN conglomerados (ArcGIS REST) | ✅ | Funcional (IFN fora do ar desde 02/09/2026) |
| FUNAI | Terras indígenas (WFS geoserver.funai.gov.br) — 665 TIs, filtros uf/fase/bbox | ✅ | Funcional |
| IBAMA | Embargos ambientais (CSV de dados abertos + geometrias WKT) — ~116 mil registros, atualização diária, filtro uf/bbox | ✅ | Funcional |
| ICMBio | Unidades de conservação federais (WFS geoservicos.inde.gov.br) — 347 UCs | ✅ | Funcional |
| CNUC | UCs federais, estaduais e municipais, com RPPNs (WFS cnuc-mapserv.mma.gov.br) — 3.450 UCs | ✅ | Funcional |
| INCRA | Territórios quilombolas (WFS cmr.funai.gov.br) — ~426 territórios | ✅ | Funcional |
| Acervo Fundiário/INCRA | Parcelas certificadas SIGEF (27 UFs) + SNCI (27 UFs) + assentamentos Brasil — shapefile ZIP | ✅ | Funcional |
| RNC/CultivarWeb | Cadastros correntes de registradas e protegidas; IDs exatos e término textual — MAPA/SNPC | ✅ | Funcional |
| EMBRAPA Solos | Perfis de solo PronaSolos (34 mil+ horizontes de ~9 mil pontos) + mapa pedológico SiBCS (2,8K polígonos) | ✅ | Funcional |
| Fundação Rio Verde | Ensaios cultivares soja MT — safras 2023/24 a 2025/26, até 4 épocas (PDF) | ✅ | Funcional |
| CFTC COT | Posicionamento semanal de traders — managed money, produtores, swaps (12 contratos agro, 2006+) | ✅ | Funcional |
| UNICA | Moagem Centro-Sul, produção açúcar/etanol, mix e ATR (PDF quinzenal + XLSX histórico) | ✅ | Funcional |

> ¹ Golden test com dados sintéticos — `needs_real_data` para validação com API real.
>
> Várias fontes têm licença restritiva ou zona cinzenta — CEPEA `nc`, UN Comtrade `restrito` e IMEA/Notícias Agrícolas/B3/ABIOVE/ANDA/ANEC/UNICA `zona_cinza`. Emitem `warnings.warn` na primeira chamada. Veja [docs/licenses.md](https://www.agrobr.dev/docs/licenses/) para a tabela completa.

## Contratos & Schemas

Cada dataset tem um contrato formal com validação automática. Schemas JSON gerados em `agrobr/schemas/`:

```python
from agrobr.contracts import get_contract, list_contracts, validate_dataset

# Listar contratos registrados
list_contracts()

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
    normalizar_cultura, municipio_para_ibge, coordenada_para_municipio,
    normalizar_uf, normalizar_safra,
)

normalizar_cultura("Soja em Grão")        # "soja"
normalizar_cultura("milho 2ª safra")      # "milho_2"
normalizar_cultura("coffee")              # "cafe"

municipio_para_ibge("Sorriso", "MT")      # 5107925
municipio_para_ibge("SAO PAULO", "SP")    # 3550308

coordenada_para_municipio(-12.74, -55.68)
# {'codigo_ibge': 5107925, 'nome': 'Sorriso', 'uf': 'MT'}

normalizar_uf("São Paulo")                # "SP"
normalizar_safra("24/25")                 # "2024/25"
```

5571 municípios IBGE com centroides (geocodificação reversa offline), 43 culturas canônicas, 27 UFs. Dados via API IBGE Localidades e Malhas (livre para uso).

## Diferenciais

- **Golden tests com fixtures de referência por fonte** — validação automatizada contra dados reais ou sintéticos documentados
- **Resiliência HTTP completa** — retry centralizado em todos os clients, 429 handling, Retry-After
- **Validação por incremento** — escopo de teste registrado, evidência das chamadas públicas e limitações; os benchmarks de escalabilidade cobrem memória, volume e async
- **Camada semântica** — datasets padronizados com proveniência rastreada e fallback onde há fonte alternativa configurada
- **Contratos formais** — schema versionado com validação automática, primary keys e constraints
- **Schemas JSON** exportados em `agrobr/schemas/`
- **Modo determinístico + snapshots** — reprodutibilidade para papers e auditorias (modo determinístico no `preco_diario`; snapshots CEPEA/CONAB/IBGE)
- **Normalização transversal** — municípios IBGE, culturas, UFs, safras padronizados
- **Async-first** com wrapper síncrono pra pipelines (Airflow, Prefect, Dagster)
- **Suporte pandas + polars** nas APIs e datasets (exceto as variantes `_geo`, que retornam GeoDataFrame)
- **Validação** — Pydantic v2 + sanity checks estatísticos + fingerprinting de layout
- **Cache CEPEA com smart TTL** (DuckDB local, expira às 18h hora oficial CEPEA)
- **Alertas multi-canal** (Slack, Discord, Email)
- **CLI completo** para debug e automação

## Como funciona

agrobr é uma biblioteca de **coleta + normalização**, não um framework de armazenamento. Cada chamada vai à fonte (com retry, fingerprinting de layout e validação de contrato). Não há acúmulo automático de histórico:

- **Cache CEPEA indicadores** — única fonte com cache local persistente. DuckDB com smart TTL: expira às 18h (hora oficial CEPEA), evitando chamadas redundantes durante o dia.
- **Snapshots opcionais** — você cria explicitamente via `create_snapshot()` para reprodutibilidade.
- **Histórico permanente é responsabilidade do consumidor** — APIs históricas entregam as observações publicadas atualmente para o intervalo solicitado. Uma chamada agrobr pode adquirir vários recursos; o BCB SGS divide intervalos longos em blocos de calendário. Isso não reconstitui revisões anteriores. Para guardar publicações sucessivas, use scheduler + parquet (próxima seção).

## Manter dados atualizados

Para acumular histórico ou rodar coleta agendada, integre com Airflow, Prefect ou Dagster usando a API sync:

```python
from datetime import date

# Airflow task
@task
def extract_soja_diario():
    from agrobr.sync import datasets
    df = datasets.preco_diario("soja")
    df.to_parquet(f"/data/soja/{date.today()}.parquet")
```

Veja o [guia completo de pipelines](https://www.agrobr.dev/docs/advanced/pipelines/) e o [guia de ergonomia async](https://www.agrobr.dev/docs/guides/async/).

## Documentação

[Documentação completa](https://www.agrobr.dev/docs/)

- [Guia Rápido](https://www.agrobr.dev/docs/quickstart/)
- [Datasets](https://www.agrobr.dev/docs/contracts/) — Contratos e garantias
- [Fontes](https://www.agrobr.dev/docs/sources/) — 41 fontes documentadas
- [API pública e referência](https://www.agrobr.dev/docs/api/)
- [Resiliência](https://www.agrobr.dev/docs/advanced/resilience/)
- [Portabilidade](https://www.agrobr.dev/docs/porting/) — Guia para portar o agrobr para R, Julia ou outras linguagens

## Contribuindo

Contribuições são bem-vindas! Veja [CONTRIBUTING.md](https://github.com/bruno-portfolio/agrobr/blob/main/CONTRIBUTING.md) para detalhes.

## Licenças dos dados

> **Importante:** O agrobr é licenciado sob MIT, mas os **dados** acessados
> pertencem às suas respectivas fontes e possuem licenças próprias.
> Dados CEPEA/ESALQ, por exemplo, são CC BY-NC 4.0 (uso comercial requer
> autorização). Consulte **[docs/licenses.md](https://www.agrobr.dev/docs/licenses/)** para a tabela
> completa de fontes, licenças e classificações.

## Licença

MIT - veja [LICENSE](https://github.com/bruno-portfolio/agrobr/blob/main/LICENSE) para detalhes.
