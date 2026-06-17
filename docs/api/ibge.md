# API IBGE

O módulo IBGE fornece acesso aos dados do Sistema IBGE de Recuperação Automática (SIDRA).

## Funções

### `pam`

Obtém dados da Produção Agrícola Municipal.

```python
async def pam(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
    variaveis: list[str] | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Código do produto (ex: 'soja', 'milho') |
| `ano` | `int \| str \| list[int] \| None` | Ano(s). Default: último disponível |
| `uf` | `str \| None` | Filtrar por UF (ex: 'MT') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Nível: 'brasil', 'uf', 'municipio' |
| `variaveis` | `list[str] \| None` | Variáveis específicas |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna `(df, MetaInfo)` com proveniência |

**Variáveis disponíveis:**

| Código | Variável |
|--------|----------|
| `area_plantada` | Área plantada (hectares) |
| `area_colhida` | Área colhida (hectares) |
| `producao` | Quantidade produzida (toneladas) |
| `rendimento` | Rendimento médio (kg/ha) |

**Exemplo:**

```python
from agrobr import ibge

# PAM por UF
df = await ibge.pam('soja', ano=2023, nivel='uf')

# Múltiplos anos
df = await ibge.pam('soja', ano=[2020, 2021, 2022, 2023])

# Por município (filtrar UF para reduzir volume)
df = await ibge.pam('soja', ano=2023, nivel='municipio', uf='MT')

# Variáveis específicas
df = await ibge.pam('soja', ano=2023, variaveis=['producao', 'area_plantada'])
```

---

### `lspa`

Obtém dados do Levantamento Sistemático da Produção Agrícola.

```python
async def lspa(
    produto: str,
    ano: int | str | None = None,
    mes: int | str | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Código do produto |
| `ano` | `int \| str \| None` | Ano. Default: atual |
| `mes` | `int \| str \| None` | Mês (1-12). Default: último |
| `uf` | `str \| None` | Filtrar por UF |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna `(df, MetaInfo)` com proveniência |

**Produtos LSPA:**

| Código | Produto |
|--------|---------|
| `soja` | Soja |
| `milho_1` | Milho 1ª safra |
| `milho_2` | Milho 2ª safra |
| `arroz` | Arroz |
| `feijao_1` | Feijão 1ª safra |
| `feijao_2` | Feijão 2ª safra |
| `feijao_3` | Feijão 3ª safra |
| `trigo` | Trigo |
| `algodao` | Algodão herbáceo |
| `amendoim_1` | Amendoim 1ª safra |
| `amendoim_2` | Amendoim 2ª safra |
| `batata_1` | Batata-inglesa 1ª safra |
| `batata_2` | Batata-inglesa 2ª safra |

**Aliases genéricos:**

Nomes genéricos expandem automaticamente para sub-safras e retornam um DataFrame concatenado:

| Alias | Expande para |
|-------|-------------|
| `milho` | `milho_1` + `milho_2` |
| `feijao` | `feijao_1` + `feijao_2` + `feijao_3` |
| `amendoim` | `amendoim_1` + `amendoim_2` |
| `batata` | `batata_1` + `batata_2` |

**Exemplo:**

```python
from agrobr import ibge

# LSPA mensal
df = await ibge.lspa('soja', ano=2024, mes=6)

# Milho 2ª safra
df = await ibge.lspa('milho_2', ano=2024)

# Alias genérico — retorna milho_1 + milho_2 concatenados
df = await ibge.lspa('milho', ano=2024)

# Por UF
df = await ibge.lspa('soja', ano=2024, uf='MT')
```

---

### `produtos_pam`

Lista produtos disponíveis na PAM.

```python
async def produtos_pam() -> list[str]
```

---

### `produtos_lspa`

Lista produtos disponíveis no LSPA.

```python
async def produtos_lspa() -> list[str]
```

---

### `ppm`

Obtém dados da Pesquisa da Pecuária Municipal.

```python
async def ppm(
    especie: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `especie` | `str` | Espécie ou produto (ex: 'bovino', 'leite') |
| `ano` | `int \| str \| list[int] \| None` | Ano(s). Default: último disponível |
| `uf` | `str \| None` | Filtrar por UF (ex: 'MT') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Nível: 'brasil', 'uf', 'municipio' |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna `(df, MetaInfo)` com proveniência |

**Espécies disponíveis (rebanhos):**

| Código | Espécie |
|--------|---------|
| `bovino` | Bovino |
| `bubalino` | Bubalino |
| `equino` | Equino |
| `suino_total` | Suíno (total) |
| `suino_matrizes` | Suíno matrizes |
| `caprino` | Caprino |
| `ovino` | Ovino |
| `galinaceos_total` | Galináceos (total) |
| `galinhas_poedeiras` | Galinhas poedeiras |
| `codornas` | Codornas |

**Produtos de origem animal:**

| Código | Produto | Unidade |
|--------|---------|---------|
| `leite` | Leite | mil litros |
| `ovos_galinha` | Ovos de galinha | mil dúzias |
| `ovos_codorna` | Ovos de codorna | mil dúzias |
| `mel` | Mel de abelha | kg |
| `casulos` | Casulos bicho-da-seda | kg |
| `la` | Lã | kg |

**Exemplo:**

```python
from agrobr import ibge

# Rebanho bovino por UF
df = await ibge.ppm('bovino', ano=2023, nivel='uf')

# Produção de leite por município em MG
df = await ibge.ppm('leite', ano=2023, nivel='municipio', uf='MG')

# Série histórica
df = await ibge.ppm('bovino', ano=[2019, 2020, 2021, 2022, 2023])

# Com metadados
df, meta = await ibge.ppm('bovino', ano=2023, return_meta=True)
```

---

### `especies_ppm`

Lista espécies e produtos disponíveis na PPM.

```python
async def especies_ppm() -> list[str]
```

---

### `abate`

Obtém dados da Pesquisa Trimestral do Abate de Animais.

```python
async def abate(
    especie: str,
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `especie` | `str` | Espécie: 'bovino', 'suino', 'frango' |
| `trimestre` | `str \| list[str] \| None` | Trimestre YYYYQQ (ex: '202303'). Default: último disponível |
| `uf` | `str \| None` | Filtrar por UF (ex: 'PR') |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna `(df, MetaInfo)` com proveniência |

**Espécies disponíveis:**

| Código | Espécie | Tabela SIDRA |
|--------|---------|--------------|
| `bovino` | Bovino | 1092 |
| `suino` | Suíno | 1093 |
| `frango` | Frango | 1094 |

**Variáveis retornadas:**

| Variável | Descrição | Unidade |
|----------|-----------|---------|
| `animais_abatidos` | Quantidade de animais abatidos | cabeças |
| `peso_carcacas` | Peso total das carcaças | kg |

**Exemplo:**

```python
from agrobr import ibge

# Abate bovino por UF
df = await ibge.abate('bovino', trimestre='202303')

# Abate de frango no Paraná
df = await ibge.abate('frango', trimestre='202303', uf='PR')

# Abate de suínos — Brasil
df = await ibge.abate('suino', trimestre='202304')

# Com metadados
df, meta = await ibge.abate('bovino', trimestre='202303', return_meta=True)
```

---

### `especies_abate`

Lista espécies disponíveis no Abate Trimestral.

```python
async def especies_abate() -> list[str]
```

---

### `censo_agro`

Obtém dados do Censo Agropecuário (1995, 2006 e 2017).

```python
async def censo_agro(
    tema: str,
    ano: int | str | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `tema` | `str` | Tema do censo (ver tabela abaixo) |
| `ano` | `int \| str \| None` | Ano censal (1995, 2006 ou 2017). Default: todos os anos disponíveis |
| `uf` | `str \| None` | Filtrar por UF (ex: 'MT') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Nível: 'brasil', 'uf', 'municipio' |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna `(df, MetaInfo)` com proveniência |

**Temas disponíveis:**

| Código | Tema | Tabela 1995 | Tabela 2006 | Tabela 2017 |
|--------|------|:-----------:|:-----------:|:-----------:|
| `efetivo_rebanho` | Efetivo de rebanho | 323 | — | 6907 |
| `uso_terra` | Uso da terra | 316/311 | — | 6881 |
| `lavoura_temporaria` | Lavoura temporária | 497/492/503 | — | 6957 |
| `lavoura_permanente` | Lavoura permanente | 509/504/510 | — | 6956 |
| `preparo_solo` | Preparo do solo | — | 791 | 6855 |
| `adubacao` | Adubação | — | 1249 | 6848 |
| `calagem` | Calagem | — | 1245 | 6849 |
| `agrotoxicos` | Uso de agrotóxicos | — | 1459 | 6851 |
| `praticas_agricolas` | Práticas agrícolas | — | 837 | 8561 |
| `irrigacao` | Irrigação | — | 855 | 6857 |

**Variáveis retornadas por tema (temas originais):**

| Tema | Variável | Unidade |
|------|----------|---------|
| `efetivo_rebanho` | `estabelecimentos` | unidades |
| `efetivo_rebanho` | `cabecas` | cabeças |
| `uso_terra` | `estabelecimentos` | unidades |
| `uso_terra` | `area` | hectares |
| `lavoura_temporaria` | `estabelecimentos` | unidades |
| `lavoura_temporaria` | `producao` | varia |
| `lavoura_temporaria` | `area_colhida` | hectares |
| `lavoura_permanente` | `estabelecimentos` | unidades |
| `lavoura_permanente` | `producao` | varia |
| `lavoura_permanente` | `area_colhida` | hectares |

**Categorias dos novos temas (exemplos):**

| Tema | Categorias (exemplos) |
|------|----------------------|
| `preparo_solo` | Cultivo convencional, Cultivo mínimo, Plantio direto na palha |
| `adubacao` | Química, Orgânica, Adubação verde |
| `calagem` | Fez aplicação, Não fez aplicação |
| `agrotoxicos` | Utilizou, Não utilizou |
| `praticas_agricolas` | Plantio em nível, Rotação de culturas, Pousio |
| `irrigacao` | Gotejamento, Pivô central, Inundação, Aspersão |

**Exemplo:**

```python
from agrobr import ibge

# Efetivo de rebanho por UF (2017)
df = await ibge.censo_agro('efetivo_rebanho')

# Uso da terra em Mato Grosso
df = await ibge.censo_agro('uso_terra', uf='MT')

# Lavoura temporária por município
df = await ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')

# Preparo do solo — ambos os anos (2006 + 2017)
df = await ibge.censo_agro('preparo_solo')

# Irrigação apenas 2017
df = await ibge.censo_agro('irrigacao', ano=2017)

# Adubação em 2006, filtrado por UF
df = await ibge.censo_agro('adubacao', ano=2006, uf='SP')

# Com metadados
df, meta = await ibge.censo_agro('efetivo_rebanho', return_meta=True)
```

---

### `temas_censo_agro`

Lista temas disponíveis no Censo Agropecuário.

```python
async def temas_censo_agro() -> list[str]
```

---

### `censo_agro_legado`

Obtém dados do Censo Agropecuário 1995/96 — 6 temas legados via FTP (XLS).

> **Nota:** `nivel='uf'` retorna dados por **mesorregião** (não por UF individual) — particularidade do formato legado; `nivel='municipio'` e `nivel='brasil'` funcionam normalmente.

```python
async def censo_agro_legado(
    tema: str,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `tema` | `str` | Tema legado (ver tabela abaixo) |
| `uf` | `str \| None` | Filtrar por UF (ex: 'SP') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Nível: 'brasil', 'uf', 'municipio' |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna `(df, MetaInfo)` com proveniência |

**Temas disponíveis:**

| Código | Tema |
|--------|------|
| `tecnologia` | Tecnologia (assistência técnica, irrigação, adubos, etc.) |
| `pessoal_ocupado` | Pessoal ocupado (total, familiar, permanentes, temporários) |
| `maquinas` | Máquinas e equipamentos (tratores por faixa de CV) |
| `producao_animal` | Produção animal (leite, lã, ovos) |
| `valor_producao` | Valor da produção (vegetal, animal, subtipos) |
| `financeiro` | Dados financeiros (investimentos, financiamentos, despesas, receitas) |

**Exemplo:**

```python
from agrobr import ibge

# Tecnologia por mesorregião
df = await ibge.censo_agro_legado('tecnologia')

# Pessoal ocupado em São Paulo
df = await ibge.censo_agro_legado('pessoal_ocupado', uf='SP')

# Máquinas — nível município
df = await ibge.censo_agro_legado('maquinas', nivel='municipio')

# Com metadados
df, meta = await ibge.censo_agro_legado('tecnologia', return_meta=True)
```

---

### `temas_censo_agro_legado`

Lista temas disponíveis no Censo Agropecuário Legado (FTP).

```python
async def temas_censo_agro_legado() -> list[str]
```

---

### `censo_agro_historico`

Obtém dados da série histórica do Censo Agropecuário (1920-2006, nível UF máximo).

```python
async def censo_agro_historico(
    tema: str,
    ano: int | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'regiao', 'uf'] = 'uf',
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `tema` | `str` | Tema da série histórica (ver tabela abaixo) |
| `ano` | `int \| list[int] \| None` | Ano(s) censal(ais). Default: todos os disponíveis |
| `uf` | `str \| None` | Filtrar por UF (ex: 'SP'). Só aplicado em nivel='uf' |
| `nivel` | `Literal['brasil', 'regiao', 'uf']` | Nível: 'brasil', 'regiao', 'uf' (municipal NÃO disponível) |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna `(df, MetaInfo)` com proveniência |

**Temas disponíveis:**

| Código | Tema | Tabela SIDRA | Períodos |
|--------|------|:------------:|----------|
| `estabelecimentos_area` | Estabelecimentos e área por grupo de área | 263 | 1920-2006 (10 censos) |
| `uso_terra` | Área por utilização das terras | 264 | 1970-2006 (6 censos) |
| `pessoal_tratores` | Pessoal ocupado e tratores | 265 | 1970-2006 (6 censos) |
| `condicao_produtor` | Estabelecimentos por condição do produtor | 280 | 1920-2006 (10 censos) |
| `efetivo_animais` | Efetivo de animais por espécie | 281 | 1970-2006 (6 censos) |
| `producao_animal` | Produção animal por tipo | 282 | 1920-2006 (10 censos) |
| `producao_vegetal` | Produção vegetal e área colhida | 283 | 1920-2006 (10 censos) |
| `lavoura_permanente` | Quantidade produzida — lavouras permanentes | 1730 | 1940-2006 (9 censos) |
| `lavoura_temporaria` | Quantidade produzida — lavouras temporárias | 1731 | 1940-2006 (9 censos) |

**Exemplo:**

```python
from agrobr import ibge

# Estabelecimentos e área, Brasil, 1985
df = await ibge.censo_agro_historico('estabelecimentos_area', ano=1985, nivel='brasil')

# Efetivo de animais, todas as UFs, todos os censos
df = await ibge.censo_agro_historico('efetivo_animais')

# Pessoal e tratores em São Paulo, 1980 e 1985
df = await ibge.censo_agro_historico('pessoal_tratores', ano=[1980, 1985], uf='SP')

# Produção vegetal, nível região
df = await ibge.censo_agro_historico('producao_vegetal', nivel='regiao')

# Com metadados
df, meta = await ibge.censo_agro_historico('uso_terra', ano=1985, return_meta=True)
```

---

### `temas_censo_agro_historico`

Lista temas disponíveis na série histórica do Censo Agropecuário.

```python
async def temas_censo_agro_historico() -> list[str]
```

#### CLI

```bash
# Todos os dados de estabelecimentos/área por UF
agrobr ibge censo-historico estabelecimentos_area

# Ano específico, formato CSV
agrobr ibge censo-historico uso_terra --ano 1985 --formato csv

# Múltiplos anos, nível Brasil
agrobr ibge censo-historico efetivo_animais --ano 1970,1985,2006 --nivel brasil

# Filtrar por UF
agrobr ibge censo-historico pessoal_tratores --ano 1985 --uf SP

# Listar temas disponíveis
agrobr ibge temas-historico
```

---

### `censo_agro_municipal_1985`

Obtém dados do Censo Agropecuário 1985 a nível municipal (extraídos via OCR de PDFs IBGE).

```python
async def censo_agro_municipal_1985(
    tema: str,
    *,
    uf: str | None = None,
    nivel: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `tema` | `str` | Tema (53 disponíveis — use `temas_censo_agro_municipal_1985()`) |
| `uf` | `str \| None` | Filtrar por UF (22 UFs disponíveis) |
| `nivel` | `str \| None` | Filtrar: total, mesorregiao, microrregiao, municipio |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retornar MetaInfo |

**Exemplo:**

```python
from agrobr import ibge

# Propriedade de terras em São Paulo
df = await ibge.censo_agro_municipal_1985('propriedade_terras', uf='SP')

# Apenas municípios
df = await ibge.censo_agro_municipal_1985('bovinos', nivel='municipio')

# Com metadados
df, meta = await ibge.censo_agro_municipal_1985('propriedade_terras', return_meta=True)
```

---

### `temas_censo_agro_municipal_1985`

Lista temas disponíveis no Censo Agropecuário Municipal 1985.

```python
async def temas_censo_agro_municipal_1985() -> list[str]
```

#### CLI

```bash
# Dados de propriedade de terras em SP
agrobr ibge censo-municipal-1985 propriedade_terras --uf SP

# Formato CSV
agrobr ibge censo-municipal-1985 bovinos --formato csv

# Filtrar por nível
agrobr ibge censo-municipal-1985 uso_terra_lavoura --nivel municipio --uf MG

# Listar temas disponíveis
agrobr ibge temas-municipal-1985
```

---

### `ufs`

Lista UFs disponíveis.

```python
async def ufs() -> list[str]
```

---

### `silvicultura`

Obtém dados da Produção da Extração Vegetal e da Silvicultura (PEVS) — silvicultura.

```python
async def silvicultura(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
    variavel: str = 'quantidade_produzida',
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Código do produto (ex: 'madeira_tora', 'carvao') ou espécie de área (ex: 'eucalipto') |
| `ano` | `int \| str \| list[int] \| None` | Ano(s). Default: último disponível |
| `uf` | `str \| None` | Filtrar por UF (ex: 'MG') |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Nível: 'brasil', 'uf', 'municipio' |
| `variavel` | `str` | 'quantidade_produzida', 'valor_producao' (tab 291) ou 'area' (tab 5930) |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retornar MetaInfo |

**Produtos (tab 291, classificação c194):**

`carvao`, `carvao_eucalipto`, `carvao_pinus`, `carvao_outras`, `lenha`, `lenha_eucalipto`, `lenha_pinus`, `lenha_outras`, `madeira_tora`, `madeira_celulose`, `madeira_outras_finalidades`, `acacia_negra`, `eucalipto_folha`, `resina`

**Espécies de área (tab 5930, classificação c734) — variavel='area':**

`eucalipto`, `pinus`, `outras`

**Exemplo:**

```python
from agrobr import ibge

# Produção de madeira em tora por UF
df = await ibge.silvicultura('madeira_tora', ano=2023)

# Área plantada de eucalipto
df = await ibge.silvicultura('eucalipto', variavel='area')

# Carvão vegetal em MG
df = await ibge.silvicultura('carvao', ano=2023, uf='MG')

# Com metadados
df, meta = await ibge.silvicultura('madeira_tora', ano=2023, return_meta=True)
```

---

### `produtos_silvicultura`

Lista produtos disponíveis na silvicultura (tab 291).

```python
async def produtos_silvicultura() -> list[str]
```

---

### `especies_silvicultura_area`

Lista espécies disponíveis para área plantada (tab 5930).

```python
async def especies_silvicultura_area() -> list[str]
```

---

### `extracao_vegetal`

Obtém dados da Produção da Extração Vegetal e da Silvicultura (PEVS) — extração vegetal.

```python
async def extracao_vegetal(
    produto: str,
    ano: int | str | list[int] | None = None,
    uf: str | None = None,
    nivel: Literal['brasil', 'uf', 'municipio'] = 'uf',
    variavel: str = 'quantidade_produzida',
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Código do produto (ex: 'acai', 'castanha_para') |
| `ano` | `int \| str \| list[int] \| None` | Ano(s). Default: último disponível |
| `uf` | `str \| None` | Filtrar por UF |
| `nivel` | `Literal['brasil', 'uf', 'municipio']` | Nível: 'brasil', 'uf', 'municipio' |
| `variavel` | `str` | 'quantidade_produzida' ou 'valor_producao' |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retornar MetaInfo |

**Produtos (tab 289, classificação c193):**

`acai`, `castanha_caju`, `castanha_para`, `erva_mate`, `mangaba`, `palmito`, `pequi_fruto`, `pinhao`, `umbu`, `hevea_coagulado`, `hevea_liquido`, `carnauba_cera`, `carnauba_po`, `piacava`, `carvao`, `lenha`, `madeira_tora`, `babacu`, `copaiba`, `cumaru`, `pequi_amendoa`

**Exemplo:**

```python
from agrobr import ibge

# Produção de açaí por UF
df = await ibge.extracao_vegetal('acai', ano=2023)

# Castanha-do-Pará no Amazonas
df = await ibge.extracao_vegetal('castanha_para', ano=2023, uf='AM')

# Valor da produção
df = await ibge.extracao_vegetal('acai', ano=2023, variavel='valor_producao')

# Com metadados
df, meta = await ibge.extracao_vegetal('acai', ano=2023, return_meta=True)
```

---

### `produtos_extracao_vegetal`

Lista produtos disponíveis na extração vegetal (tab 289).

```python
async def produtos_extracao_vegetal() -> list[str]
```

---

### `leite_trimestral`

Obtém dados da Pesquisa Trimestral do Leite — aquisição, industrialização e preço médio.

```python
async def leite_trimestral(
    trimestre: str | list[str] | None = None,
    uf: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `trimestre` | `str \| list[str] \| None` | Trimestre YYYYQQ (ex: '202303'). Default: último |
| `uf` | `str \| None` | Filtrar por UF |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retornar MetaInfo |

**Colunas retornadas (pivot wide):**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `trimestre` | str | Trimestre YYYYQQ |
| `localidade` | str | UF |
| `localidade_cod` | int | Código IBGE |
| `leite_adquirido` | float | Leite cru adquirido (mil litros) |
| `leite_industrializado` | float | Leite cru industrializado (mil litros) |
| `preco_medio` | float | Preço médio pago ao produtor (R$/litro) |
| `fonte` | str | "ibge_leite_trimestral" |

**Exemplo:**

```python
from agrobr import ibge

# Leite trimestral por UF
df = await ibge.leite_trimestral(trimestre='202303')

# Filtrar por UF
df = await ibge.leite_trimestral(trimestre='202303', uf='MG')

# Múltiplos trimestres
df = await ibge.leite_trimestral(trimestre=['202301', '202302', '202303'])

# Com metadados
df, meta = await ibge.leite_trimestral(trimestre='202303', return_meta=True)
```

---

### `pib_agro`

Obtém o PIB agropecuário trimestral (Contas Nacionais Trimestrais).

```python
async def pib_agro(
    trimestre: str | list[str] | None = None,
    precos: str = 'corrente',
    setor: str = 'agropecuaria',
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `trimestre` | `str \| list[str] \| None` | Trimestre YYYYQQ. Default: último |
| `precos` | `str` | 'corrente' (tab 1846) ou 'real_1995' (tab 6612) |
| `setor` | `str` | 'agropecuaria', 'industria', 'servicos' ou 'pib_total' |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retornar MetaInfo |

**Exemplo:**

```python
from agrobr import ibge

# PIB agropecuário a preços correntes
df = await ibge.pib_agro(trimestre='202501')

# PIB a preços reais (base 1995)
df = await ibge.pib_agro(trimestre='202501', precos='real_1995')

# PIB total (todos os setores)
df = await ibge.pib_agro(trimestre='202501', setor='pib_total')

# Com metadados
df, meta = await ibge.pib_agro(return_meta=True)
```

---

## Diferenças PAM vs LSPA vs PPM vs Abate vs PEVS vs Leite vs PIB vs Censo Agro vs Série Histórica vs Municipal 1985

| Aspecto | PAM | LSPA | PPM | Abate | PEVS | Leite | PIB | Censo Agro | Censo Legado | Série Histórica | Municipal 1985 |
|---------|-----|------|-----|-------|------|-------|-----|------------|--------------|-----------------|----------------|
| Frequência | Anual | Mensal | Anual | Trimestral | Anual | Trimestral | Trimestral | Decenial | Única (1995/96) | Decenial | Única (1985) |
| Granularidade | Até município | Até UF | Até município | Brasil + UF | Até município | UF | Brasil | Até município | Até município | Brasil/Região/UF | Até município |
| Tipo | Consolidados | Estimativas | Consolidados | Consolidados | Consolidados | Consolidados | Estimativas | Censitários | Censitários (FTP) | Censitários | Censitários (OCR) |
| Disponibilidade | T+1 ano | T+1 mês | T+1 ano | T+2 meses | T+1 ano | T+2 meses | T+2 meses | Pós-censo | Estático | Estático | Estático |
| Escopo | Lavouras | Lavouras | Pecuária | Abate | Silvicultura + Extr. vegetal | Leite (aquisição, industrialização) | PIB setorial | Estrutura agro | 6 temas legados | 9 temas (1920-2006) | 53 temas (1985) |

## Tabelas SIDRA Utilizadas

| Tabela | Descrição |
|--------|-----------|
| 5457 | PAM - Nova série (2018+) |
| 6588 | LSPA - Estimativas mensais |
| 1612 | PAM - Lavouras temporárias (histórico) |
| 3939 | PPM - Efetivo de rebanhos |
| 74 | PPM - Produção de origem animal |
| 1092 | Abate - Bovinos |
| 1093 | Abate - Suínos |
| 1094 | Abate - Frangos |
| 323 | Censo Agro 1995 - Efetivo de rebanho |
| 316 / 311 | Censo Agro 1995 - Uso da terra |
| 497 / 492 / 503 | Censo Agro 1995 - Lavoura temporária |
| 509 / 504 / 510 | Censo Agro 1995 - Lavoura permanente |
| 6907 | Censo Agro 2017 - Efetivo de rebanho |
| 6881 | Censo Agro 2017 - Uso da terra |
| 6957 | Censo Agro 2017 - Lavoura temporária |
| 6956 | Censo Agro 2017 - Lavoura permanente |
| 791 / 6855 | Censo Agro 2006/2017 - Preparo do solo |
| 1249 / 6848 | Censo Agro 2006/2017 - Adubação |
| 1245 / 6849 | Censo Agro 2006/2017 - Calagem |
| 1459 / 6851 | Censo Agro 2006/2017 - Agrotóxicos |
| 837 / 8561 | Censo Agro 2006/2017 - Práticas agrícolas |
| 855 / 6857 | Censo Agro 2006/2017 - Irrigação |
| 263 | Série Histórica - Estabelecimentos e área |
| 264 | Série Histórica - Uso da terra |
| 265 | Série Histórica - Pessoal e tratores |
| 280 | Série Histórica - Condição do produtor |
| 281 | Série Histórica - Efetivo de animais |
| 282 | Série Histórica - Produção animal |
| 283 | Série Histórica - Produção vegetal |
| 1730 | Série Histórica - Lavoura permanente |
| 1731 | Série Histórica - Lavoura temporária |
| 289 | PEVS - Extração vegetal (c193) |
| 291 | PEVS - Silvicultura produção (c194) |
| 5930 | PEVS - Silvicultura área (c734) |
| 1086 | Leite - Pesquisa Trimestral do Leite |
| 1846 | PIB - Contas Nacionais a preços correntes |
| 6612 | PIB - Contas Nacionais a preços reais (1995) |

## Versão Síncrona

```python
from agrobr.sync import ibge

df = ibge.pam('soja', ano=2023)
df = ibge.lspa('milho_1', ano=2024, mes=6)
df = ibge.ppm('bovino', ano=2023)
df = ibge.abate('bovino', trimestre='202303')
df = ibge.censo_agro('efetivo_rebanho')
df = ibge.censo_agro('preparo_solo', ano=2017)
df = ibge.censo_agro_legado('tecnologia')
df = ibge.censo_agro_legado('pessoal_ocupado', uf='SP')
df = ibge.censo_agro_historico('estabelecimentos_area', ano=1985)
df = ibge.censo_agro_municipal_1985('propriedade_terras', uf='SP')
df = ibge.silvicultura('madeira_tora', ano=2023)
df = ibge.extracao_vegetal('acai', ano=2023)
df = ibge.leite_trimestral(trimestre='202303')
df = ibge.pib_agro(trimestre='202501')
```

## Notas

- Consultas por município geram grande volume de dados
- Recomenda-se filtrar por UF quando usar nível município
- LSPA é atualizado mensalmente pelo IBGE
- PAM é consolidada anualmente após colheita
- PPM é consolidada anualmente (setembro), série desde 1974
- Abate Trimestral disponível desde 1997, atualizado a cada trimestre (T+2 meses)
- Censo Agropecuário: 10 temas, dados de 1995, 2006 e/ou 2017 conforme disponibilidade. Referência 2017: out/2016 a set/2017. Cache 30 dias
- Censo Agropecuário Legado: 6 temas FTP (tecnologia, pessoal_ocupado, maquinas, producao_animal, valor_producao, financeiro). Ano fixo 1995. Cache 90 dias
- Série Histórica: 9 temas, 1920-2006, até UF (municipal NÃO disponível). Unidades mistas por categoria (Aves=Mil cabeças, etc). Cache 30 dias
- Censo Municipal 1985: 53 temas, dados municipais de 22 UFs, extraídos via OCR de PDFs estaduais do IBGE. Dados estáticos (bundled). Campo `confianca` indica qualidade OCR
