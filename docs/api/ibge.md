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

**Exemplo:**

```python
from agrobr import ibge

# LSPA mensal
df = await ibge.lspa('soja', ano=2024, mes=6)

# Milho 2ª safra
df = await ibge.lspa('milho_2', ano=2024)

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

### `ufs`

Lista UFs disponíveis.

```python
async def ufs() -> list[str]
```

---

## Diferenças PAM vs LSPA

| Aspecto | PAM | LSPA |
|---------|-----|------|
| Frequência | Anual | Mensal |
| Granularidade | Até município | Até UF |
| Tipo | Dados consolidados | Estimativas |
| Disponibilidade | T+1 ano | T+1 mês |

## Tabelas SIDRA Utilizadas

| Tabela | Descrição |
|--------|-----------|
| 5457 | PAM - Nova série (2018+) |
| 6588 | LSPA - Estimativas mensais |
| 1612 | PAM - Lavouras temporárias (histórico) |

## Versão Síncrona

```python
from agrobr.sync import ibge

df = ibge.pam('soja', ano=2023)
df = ibge.lspa('milho_1', ano=2024, mes=6)
```

## Notas

- Consultas por município geram grande volume de dados
- Recomenda-se filtrar por UF quando usar nível município
- LSPA é atualizado mensalmente pelo IBGE
- PAM é consolidada anualmente após colheita
