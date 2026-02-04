# API CONAB

O módulo CONAB fornece acesso aos dados de safras e balanço oferta/demanda da Companhia Nacional de Abastecimento.

## Funções

### `safras`

Obtém dados de safra por produto e UF.

```python
async def safras(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    levantamento: int | None = None,
    as_polars: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Produto: 'soja', 'milho', 'arroz', etc. |
| `safra` | `str \| None` | Safra no formato '2024/25'. Default: última |
| `uf` | `str \| None` | UF (ex: 'MT', 'PR'). Default: todas |
| `levantamento` | `int \| None` | Número do levantamento (1-12). Default: último |
| `as_polars` | `bool` | Retornar como polars.DataFrame |

**Retorno:**

DataFrame com colunas:
- `regiao`: Região (Norte, Nordeste, etc.)
- `uf`: Unidade federativa
- `safra`: Ano-safra
- `area_mil_ha`: Área plantada (mil hectares)
- `produtividade_kg_ha`: Produtividade (kg/ha)
- `producao_mil_t`: Produção (mil toneladas)

**Exemplo:**

```python
from agrobr import conab

# Todas as UFs
df = await conab.safras('soja', safra='2024/25')

# Apenas Mato Grosso
df = await conab.safras('soja', safra='2024/25', uf='MT')

# Levantamento específico
df = await conab.safras('soja', safra='2024/25', levantamento=5)
```

---

### `balanco`

Obtém balanço de oferta e demanda.

```python
async def balanco(
    produto: str | None = None,
    safra: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str \| None` | Produto específico ou todos |
| `safra` | `str \| None` | Safra. Default: última |
| `as_polars` | `bool` | Retornar como polars.DataFrame |

**Retorno:**

DataFrame com componentes do balanço:
- Estoque inicial
- Produção
- Importação
- Suprimento total
- Consumo
- Exportação
- Estoque final

**Exemplo:**

```python
from agrobr import conab

# Balanço de soja
df = await conab.balanco('soja')

# Todos os produtos
df = await conab.balanco()
```

---

### `brasil_total`

Obtém totais nacionais de produção.

```python
async def brasil_total(
    safra: str | None = None,
    as_polars: bool = False,
) -> pd.DataFrame | pl.DataFrame
```

**Retorno:**

DataFrame com totais Brasil para todos os produtos.

---

### `levantamentos`

Lista levantamentos disponíveis.

```python
async def levantamentos() -> list[dict]
```

**Retorno:**

Lista de dicionários com:
- `safra`: Ano-safra
- `levantamento`: Número do levantamento
- `data`: Data de publicação

---

### `produtos`

Lista produtos disponíveis.

```python
async def produtos() -> list[str]
```

---

### `ufs`

Lista UFs disponíveis.

```python
async def ufs() -> list[str]
```

---

## Modelos

### `Safra`

```python
class Safra(BaseModel):
    fonte: Fonte
    produto: str
    safra: str  # Formato: '2024/25'
    uf: str | None
    area_plantada: Decimal | None
    producao: Decimal | None
    produtividade: Decimal | None
    unidade_area: str = 'mil_ha'
    unidade_producao: str = 'mil_ton'
    levantamento: int
    data_publicacao: date
    meta: dict[str, Any] = {}
```

## Produtos Disponíveis

| Código | Produto |
|--------|---------|
| `soja` | Soja |
| `milho` | Milho (1ª + 2ª safra) |
| `milho_1` | Milho 1ª safra |
| `milho_2` | Milho 2ª safra |
| `arroz` | Arroz |
| `feijao` | Feijão (total) |
| `algodao` | Algodão |
| `trigo` | Trigo |
| `sorgo` | Sorgo |
| `aveia` | Aveia |
| `cevada` | Cevada |
| `girassol` | Girassol |
| `amendoim` | Amendoim |
| `mamona` | Mamona |
| `canola` | Canola |

## Versão Síncrona

```python
from agrobr.sync import conab

df = conab.safras('soja', safra='2024/25')
df = conab.balanco('milho')
```
