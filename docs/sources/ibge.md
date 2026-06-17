# IBGE - Instituto Brasileiro de Geografia e Estatistica

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Instituicao** | Governo Federal |
| **Website** | [ibge.gov.br](https://www.ibge.gov.br) |
| **API** | [SIDRA](https://sidra.ibge.gov.br) |
| **Acesso agrobr** | Via API SIDRA (JSON) |

## Origem dos Dados

### Fonte

- **API**: `https://sidra.ibge.gov.br/`
- **Formato**: JSON
- **Acesso**: Publico, sem autenticacao

## Pesquisas Disponiveis

### PAM - Producao Agricola Municipal

- **Tabela SIDRA**: 5457 (nova serie 2018+)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual

### LSPA - Levantamento Sistematico da Producao Agricola

- **Tabela SIDRA**: 6588
- **Cobertura**: Nacional/UF
- **Frequencia**: Mensal

### PPM - Pesquisa da Pecuaria Municipal

- **Tabelas SIDRA**: 3939 (rebanhos), 74 (producao de origem animal)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual
- **Serie**: 1974-presente (51 anos)

### Abate - Pesquisa Trimestral do Abate de Animais

- **Tabelas SIDRA**: 1092 (bovinos), 1093 (suinos), 1094 (frangos)
- **Cobertura**: Brasil + UF (27 UFs)
- **Frequencia**: Trimestral
- **Serie**: 1997-presente
- **Especies**: bovino, suino, frango
- **Variaveis**: animais abatidos (cabecas), peso das carcacas (kg)

### Censo Agropecuario 1995/2006/2017

- **Tabelas SIDRA 2017**: 6907 (efetivo rebanho), 6881 (uso terra), 6957 (lavoura temporaria), 6956 (lavoura permanente), 6855 (preparo solo), 6848 (adubacao), 6849 (calagem), 6851 (agrotoxicos), 8561 (praticas agricolas), 6857 (irrigacao)
- **Tabelas SIDRA 2006**: 791 (preparo solo), 1249 (adubacao), 1245 (calagem), 1459 (agrotoxicos), 837 (praticas agricolas), 855 (irrigacao)
- **Tabelas SIDRA 1995**: 323 (efetivo rebanho), 316/311 (uso terra), 497/492/503 (lavoura temporaria), 509/504/510 (lavoura permanente)
- **Cobertura**: Brasil + UF + municipio
- **Frequencia**: Decenial
- **Periodos**: 1995, 2006 e 2017 (conforme tema)
- **Temas**: efetivo_rebanho, uso_terra, lavoura_temporaria, lavoura_permanente, preparo_solo, adubacao, calagem, agrotoxicos, praticas_agricolas, irrigacao
- **Formato**: Long format (variavel/valor por linha)

### Censo Agropecuario — Serie Historica (1920-2006)

- **Tabelas SIDRA**: 263 (estabelecimentos/area), 264 (uso terra), 265 (pessoal/tratores), 280 (condicao produtor), 281 (efetivo animais), 282 (producao animal), 283 (producao vegetal), 1730 (lavoura permanente), 1731 (lavoura temporaria)
- **Cobertura**: Brasil + Regiao + UF (municipal NAO disponivel no SIDRA)
- **Frequencia**: Censos decenais (1920-2006, conforme tabela)
- **Periodos**: ate 10 censos por tema (1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006)
- **Temas**: 9 temas com serie historica longa
- **Quirks**: Aves em mil cabecas (tab 281), unidades mistas por categoria (tabs 282/283/1730/1731), classificacoes sem Total (tabs 281/282/283/1730/1731)

### Censo Agropecuario 1985 — Dados Municipais (PDFs OCR)

- **Fonte**: PDFs estaduais da Biblioteca IBGE
- **Formato**: CSVs extraidos via OCR hibrida (PyMuPDF coords + correcao OCR)
- **Cobertura**: 22 UFs, ate municipio (mesorregiao, microrregiao, municipio)
- **Frequencia**: Unica (Censo 1985)
- **Temas**: 53 temas (propriedade, uso da terra, pessoal, mecanizacao, pecuaria, lavouras, producao)
- **UFs excluidas**: MA, PI, CE, RN (PDFs sem camada OCR)
- **Acesso**: Dados bundled no pacote (agrobr/data/censo_1985/)
- **Qualidade**: campo `confianca` (alta/media/baixa), 77.9% cross-validation estadual↔nacional
- **URL catalogo**: https://biblioteca.ibge.gov.br/index.php/biblioteca-catalogo?view=detalhes&id=768

### Censo Agropecuario 1995/96 — Temas Legados (FTP)

- **Fonte**: FTP IBGE (`ftp.ibge.gov.br`)
- **Formato**: XLS legado (xlrd)
- **Cobertura**: Brasil (mesorregioes, microrregioes, municipios)
- **Frequencia**: Unica (Censo 1995/96)
- **Temas**: tecnologia, pessoal_ocupado, maquinas, producao_animal, valor_producao, financeiro
- **Acesso**: Publico, sem autenticacao

### PEVS — Silvicultura

- **Tabelas SIDRA**: 291 (producao, classificacao c194) + 5930 (area plantada, classificacao c734)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual
- **Serie**: 1986-presente
- **Produtos**: carvao, lenha, madeira_tora, madeira_celulose, acacia_negra, eucalipto_folha, resina (14 total)
- **Especies area**: eucalipto, pinus, outras
- **Variaveis**: quantidade_produzida (var 142), valor_producao (var 143), area (var 6549)
- **Unidades**: Toneladas ou Metros cubicos (conforme produto)

### PEVS — Extracao Vegetal

- **Tabela SIDRA**: 289 (classificacao c193)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual
- **Serie**: 1986-presente
- **Produtos**: acai, castanha_caju, castanha_para, erva_mate, mangaba, palmito, pequi_fruto, pinhao, umbu, hevea_coagulado, hevea_liquido, carnauba_cera, carnauba_po, piacava, carvao, lenha, madeira_tora, babacu, copaiba, cumaru, pequi_amendoa (21 total)
- **Variaveis**: quantidade_produzida (var 144), valor_producao (var 145)
- **Unidades**: Toneladas (maioria) ou Metros cubicos (lenha, madeira_tora)

### Leite Trimestral — Pesquisa Trimestral do Leite

- **Tabela SIDRA**: 1086
- **Cobertura**: Brasil + UF (27 UFs)
- **Frequencia**: Trimestral
- **Serie**: 1997-presente
- **Variaveis**: leite adquirido (var 282, mil litros), leite industrializado (var 283, mil litros), preco medio (var 2522, R$/litro)
- **Output**: Formato wide (3 variaveis como colunas)

### PIB Agropecuario — Contas Nacionais Trimestrais

- **Tabelas SIDRA**: 1846 (precos correntes, var 585) + 6612 (precos reais base 1995, var 9318)
- **Cobertura**: Brasil (nivel nacional)
- **Frequencia**: Trimestral
- **Serie**: 1996-presente
- **Setores**: agropecuaria (90687), industria (90691), servicos (90696), pib_total (90707)
- **Classificacao**: c11255
- **Unidade**: Milhoes de Reais

## Variaveis

| Codigo | Nome | Unidade |
|--------|------|---------|
| 214 | Area plantada | hectares |
| 215 | Area colhida | hectares |
| 216 | Quantidade produzida | toneladas |
| 112 | Rendimento medio | kg/ha |

## Uso - PAM

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Dados de soja por UF
    df = await ibge.pam('soja', ano=2023)

    # Multiplos anos
    df = await ibge.pam('milho', ano=[2020, 2021, 2022, 2023])

    # Filtrar por UF
    df = await ibge.pam('soja', ano=2023, uf='MT')

    # Nivel municipal
    df = await ibge.pam('arroz', ano=2023, nivel='municipio', uf='RS')

    # Com metadados
    df, meta = await ibge.pam('soja', ano=2023, return_meta=True)

asyncio.run(main())
```

### Niveis Territoriais

| Nivel | Descricao |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa |
| `municipio` | Por municipio |

## Uso - LSPA

### Basico

```python
# Estimativas do ano
df = await ibge.lspa('soja', ano=2024)

# Mes especifico
df = await ibge.lspa('milho_1', ano=2024, mes=6)

# Filtrar por UF
df = await ibge.lspa('soja', ano=2024, uf='PR')

# Com metadados
df, meta = await ibge.lspa('soja', ano=2024, return_meta=True)
```

## Schema - PAM

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `produto` | str | Nome do produto |
| `area_plantada` | float | Hectares |
| `area_colhida` | float | Hectares |
| `producao` | float | Toneladas |
| `rendimento` | float | kg/ha |
| `valor_producao` | float | Mil reais |
| `fonte` | str | "ibge_pam" |

## Schema - LSPA

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `mes` | int | Mes de referencia |
| `variavel` | str | Nome da variavel |
| `valor` | float | Valor da variavel |
| `produto` | str | Nome do produto |
| `fonte` | str | "ibge_lspa" |

## Produtos PAM

```python
produtos = await ibge.produtos_pam()
# ['soja', 'milho', 'arroz', 'feijao', 'trigo', 'cafe', ...]
```

## Produtos LSPA

```python
produtos = await ibge.produtos_lspa()
# ['soja', 'milho_1', 'milho_2', 'arroz', 'feijao_1', 'feijao_2', ...]
```

Nota: No LSPA, `milho_1` e `milho_2` referem-se a primeira e segunda safras.

## UFs Disponiveis

```python
ufs = await ibge.ufs()
# ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', ...]
```

## Uso - PPM

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Rebanho bovino por UF
    df = await ibge.ppm('bovino', ano=2023)

    # Producao de leite
    df = await ibge.ppm('leite', ano=2023)

    # Multiplos anos
    df = await ibge.ppm('bovino', ano=[2020, 2021, 2022, 2023])

    # Filtrar por UF
    df = await ibge.ppm('bovino', ano=2023, uf='MT')

    # Nivel municipal
    df = await ibge.ppm('bovino', ano=2023, nivel='municipio', uf='MS')

    # Com metadados
    df, meta = await ibge.ppm('bovino', ano=2023, return_meta=True)

asyncio.run(main())
```

## Schema - PPM

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `especie` | str | Nome da especie/produto |
| `valor` | float | Valor (cabecas, mil litros, etc) |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_ppm" |

## Especies/Produtos PPM

### Rebanhos (tabela 3939)

| Codigo | Especie | Unidade |
|--------|---------|---------|
| `bovino` | Bovino | cabecas |
| `bubalino` | Bubalino | cabecas |
| `equino` | Equino | cabecas |
| `suino_total` | Suino (total) | cabecas |
| `suino_matrizes` | Suino matrizes | cabecas |
| `caprino` | Caprino | cabecas |
| `ovino` | Ovino | cabecas |
| `galinaceos_total` | Galinaceos (total) | cabecas |
| `galinhas_poedeiras` | Galinhas poedeiras | cabecas |
| `codornas` | Codornas | cabecas |

### Producao de origem animal (tabela 74)

| Codigo | Produto | Unidade |
|--------|---------|---------|
| `leite` | Leite | mil litros |
| `ovos_galinha` | Ovos de galinha | mil duzias |
| `ovos_codorna` | Ovos de codorna | mil duzias |
| `mel` | Mel de abelha | kg |
| `casulos` | Casulos de bicho-da-seda | kg |
| `la` | La | kg |

```python
especies = await ibge.especies_ppm()
# ['bovino', 'bubalino', 'caprino', 'casulos', 'codornas', ...]
```

## Uso - Abate Trimestral

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Abate bovino por UF
    df = await ibge.abate('bovino', trimestre='202303')

    # Abate de frango no Parana
    df = await ibge.abate('frango', trimestre='202303', uf='PR')

    # Abate de suinos — Brasil
    df = await ibge.abate('suino', trimestre='202304')

    # Com metadados
    df, meta = await ibge.abate('bovino', trimestre='202303', return_meta=True)

asyncio.run(main())
```

## Schema - Abate Trimestral

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `trimestre` | str | Trimestre no formato YYYYQQ |
| `localidade` | str | UF |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `especie` | str | bovino, suino ou frango |
| `animais_abatidos` | float | Quantidade abatida (cabecas) |
| `peso_carcacas` | float | Peso total das carcacas (kg) |
| `fonte` | str | "ibge_abate" |

## Especies Abate

| Codigo | Especie | Tabela SIDRA |
|--------|---------|--------------|
| `bovino` | Bovino | 1092 |
| `suino` | Suino | 1093 |
| `frango` | Frango | 1094 |

```python
especies = await ibge.especies_abate()
# ['bovino', 'suino', 'frango']
```

## Uso - Censo Agropecuario

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Efetivo de rebanho por UF
    df = await ibge.censo_agro('efetivo_rebanho')

    # Uso da terra em Mato Grosso
    df = await ibge.censo_agro('uso_terra', uf='MT')

    # Lavoura temporaria por municipio
    df = await ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')

    # Lavoura permanente — Brasil
    df = await ibge.censo_agro('lavoura_permanente')

    # Com metadados
    df, meta = await ibge.censo_agro('efetivo_rebanho', return_meta=True)

asyncio.run(main())
```

## Schema - Censo Agropecuario

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia (1995, 2006 ou 2017) |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `tema` | str | Tema do censo |
| `categoria` | str | Categoria dentro do tema |
| `variavel` | str | Nome da variavel |
| `valor` | float | Valor da variavel |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_censo_agro" |

## Temas Censo Agropecuario

| Codigo | Tema | Tabela SIDRA 1995 | Tabela SIDRA 2006 | Tabela SIDRA 2017 |
|--------|------|-------------------|-------------------|-------------------|
| `efetivo_rebanho` | Efetivo de rebanho | 323 | — | 6907 |
| `uso_terra` | Uso da terra | 316/311 | — | 6881 |
| `lavoura_temporaria` | Lavoura temporaria | 497/492/503 | — | 6957 |
| `lavoura_permanente` | Lavoura permanente | 509/504/510 | — | 6956 |
| `preparo_solo` | Preparo do solo | — | 791 | 6855 |
| `adubacao` | Adubacao | — | 1249 | 6848 |
| `calagem` | Calagem | — | 1245 | 6849 |
| `agrotoxicos` | Uso de agrotoxicos | — | 1459 | 6851 |
| `praticas_agricolas` | Praticas agricolas | — | 837 | 8561 |
| `irrigacao` | Irrigacao | — | 855 | 6857 |

```python
temas = await ibge.temas_censo_agro()
# ['efetivo_rebanho', 'uso_terra', 'lavoura_temporaria', 'lavoura_permanente',
#  'preparo_solo', 'adubacao', 'calagem', 'agrotoxicos', 'praticas_agricolas', 'irrigacao']
```

## Cache

| Pesquisa | TTL | Stale maximo |
|----------|-----|--------------|
| PAM | 7 dias | 90 dias |
| LSPA | 24 horas | 30 dias |
| PPM | 7 dias | 90 dias |
| Abate | 7 dias | 90 dias |
| Censo Agro | 30 dias | 90 dias |
| Censo Agro Legado | 90 dias | 90 dias |
| Silvicultura (PEVS) | 7 dias | 90 dias |
| Extracao Vegetal (PEVS) | 7 dias | 90 dias |
| Leite Trimestral | 7 dias | 90 dias |
| PIB Agro | 7 dias | 90 dias |

## Atualizacao

| Pesquisa | Frequencia |
|----------|------------|
| PAM | Anual (agosto-setembro) |
| LSPA | Mensal |
| PPM | Anual (setembro) |
| Abate | Trimestral (T+2 meses) |
| Censo Agro | Decenial (ultimo: 2017) |
| Silvicultura (PEVS) | Anual (agosto-setembro) |
| Extracao Vegetal (PEVS) | Anual (agosto-setembro) |
| Leite Trimestral | Trimestral (T+2 meses) |
| PIB Agro | Trimestral (T+2 meses) |

## Uso - Silvicultura (PEVS)

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Producao de madeira em tora por UF
    df = await ibge.silvicultura('madeira_tora', ano=2023)

    # Area plantada de eucalipto
    df = await ibge.silvicultura('eucalipto', variavel='area')

    # Carvao vegetal em MG
    df = await ibge.silvicultura('carvao', ano=2023, uf='MG')

    # Com metadados
    df, meta = await ibge.silvicultura('madeira_tora', return_meta=True)

asyncio.run(main())
```

## Schema - Silvicultura

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `produto` | str | Nome do produto |
| `valor` | float | Valor (Toneladas, Metros cubicos ou Hectares) |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_silvicultura" |

## Uso - Extracao Vegetal (PEVS)

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Producao de acai por UF
    df = await ibge.extracao_vegetal('acai', ano=2023)

    # Castanha-do-Para no Amazonas
    df = await ibge.extracao_vegetal('castanha_para', ano=2023, uf='AM')

    # Valor da producao
    df = await ibge.extracao_vegetal('acai', variavel='valor_producao')

    # Com metadados
    df, meta = await ibge.extracao_vegetal('acai', return_meta=True)

asyncio.run(main())
```

## Schema - Extracao Vegetal

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `produto` | str | Nome do produto |
| `valor` | float | Valor (Toneladas ou Metros cubicos) |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_extracao_vegetal" |

## Uso - Leite Trimestral

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Leite trimestral por UF
    df = await ibge.leite_trimestral(trimestre='202303')

    # Filtrar por UF
    df = await ibge.leite_trimestral(trimestre='202303', uf='MG')

    # Multiplos trimestres
    df = await ibge.leite_trimestral(trimestre=['202301', '202302', '202303'])

    # Com metadados
    df, meta = await ibge.leite_trimestral(return_meta=True)

asyncio.run(main())
```

## Schema - Leite Trimestral

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `trimestre` | str | Trimestre YYYYQQ |
| `localidade` | str | UF |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `leite_adquirido` | float | Leite cru adquirido (mil litros) |
| `leite_industrializado` | float | Leite cru industrializado (mil litros) |
| `preco_medio` | float | Preco medio pago ao produtor (R$/litro) |
| `fonte` | str | "ibge_leite_trimestral" |

## Uso - PIB Agropecuario

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # PIB agropecuario a precos correntes
    df = await ibge.pib_agro(trimestre='202501')

    # PIB a precos reais (base 1995)
    df = await ibge.pib_agro(trimestre='202501', precos='real_1995')

    # PIB total
    df = await ibge.pib_agro(trimestre='202501', setor='pib_total')

    # Com metadados
    df, meta = await ibge.pib_agro(return_meta=True)

asyncio.run(main())
```

## Schema - PIB Agropecuario

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `trimestre` | str | Trimestre YYYYQQ |
| `valor` | float | Valor (Milhoes de Reais) |
| `unidade` | str | Unidade de medida |
| `setor` | str | Setor economico |
| `fonte` | str | "ibge_pib" |

## Notas

- PEVS Silvicultura: 14 produtos, dados anuais desde 1986. Area plantada (tab 5930) com 3 especies. Cache 7 dias
- PEVS Extracao Vegetal: 21 produtos, dados anuais desde 1986. Unidades mistas (Toneladas vs Metros cubicos). Cache 7 dias
- Leite Trimestral: tabela 1086, 3 variaveis pivotadas em colunas wide. Serie desde 1997. Cache 7 dias
- PIB Agropecuario: tabs 1846/6612, 4 setores, nivel Brasil. Serie desde 1996. Sem contrato (macro view). Cache 7 dias
