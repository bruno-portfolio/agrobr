# API IBGE

O módulo IBGE fornece acesso aos dados do Sistema IBGE de Recuperação Automática (SIDRA).

## Funções

### `pam`

Obtém dados da Produção Agrícola Municipal.

```python
async def pam(
    produto: str,
    ano: int | list[int] | None = None,
    uf: str | None = None,
    nivel: str = 'uf',
    variaveis: list[str] | None = None,
    as_polars: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Código do produto (ex: 'soja', 'milho') |
| `ano` | `int \| list[int] \| None` | Ano(s). Default: último disponível |
| `uf` | `str \| None` | Filtrar por UF (ex: 'MT') |
| `nivel` | `str` | Nível: 'brasil', 'uf', 'municipio' |
| `variaveis` | `list[str] \| None` | Variáveis específicas |
| `as_polars` | `bool` | Retornar como polars.DataFrame |

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
    ano: int | None = None,
    mes: int | None = None,
    uf: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Código do produto |
| `ano` | `int \| None` | Ano. Default: atual |
| `mes` | `int \| None` | Mês (1-12). Default: último |
| `uf` | `str \| None` | Filtrar por UF |
| `as_polars` | `bool` | Retornar como polars.DataFrame |

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
    ano: int | list[int] | None = None,
    uf: str | None = None,
    nivel: str = 'uf',
    as_polars: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `especie` | `str` | Espécie ou produto (ex: 'bovino', 'leite') |
| `ano` | `int \| list[int] \| None` | Ano(s). Default: último disponível |
| `uf` | `str \| None` | Filtrar por UF (ex: 'MT') |
| `nivel` | `str` | Nível: 'brasil', 'uf', 'municipio' |
| `as_polars` | `bool` | Retornar como polars.DataFrame |

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

### `ufs`

Lista UFs disponíveis.

```python
async def ufs() -> list[str]
```

---

## Diferenças PAM vs LSPA vs PPM

| Aspecto | PAM | LSPA | PPM |
|---------|-----|------|-----|
| Frequência | Anual | Mensal | Anual |
| Granularidade | Até município | Até UF | Até município |
| Tipo | Dados consolidados | Estimativas | Dados consolidados |
| Disponibilidade | T+1 ano | T+1 mês | T+1 ano |
| Escopo | Lavouras | Lavouras | Pecuária |

## Tabelas SIDRA Utilizadas

| Tabela | Descrição |
|--------|-----------|
| 5457 | PAM - Nova série (2018+) |
| 6588 | LSPA - Estimativas mensais |
| 1612 | PAM - Lavouras temporárias (histórico) |
| 3939 | PPM - Efetivo de rebanhos |
| 74 | PPM - Produção de origem animal |

## Versão Síncrona

```python
from agrobr.sync import ibge

df = ibge.pam('soja', ano=2023)
df = ibge.lspa('milho_1', ano=2024, mes=6)
df = ibge.ppm('bovino', ano=2023)
```

## Notas

- Consultas por município geram grande volume de dados
- Recomenda-se filtrar por UF quando usar nível município
- LSPA é atualizado mensalmente pelo IBGE
- PAM é consolidada anualmente após colheita
- PPM é consolidada anualmente (setembro), série desde 1974
