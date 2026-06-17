# API CONAB

O módulo CONAB fornece acesso a safras, balanço oferta/demanda, totais Brasil, custos de produção, série histórica, progresso de safra e preços de atacado (CEASA) da Companhia Nacional de Abastecimento.

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
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str` | Produto: 'soja', 'milho', 'arroz', etc. |
| `safra` | `str \| None` | Safra no formato '2024/25'. Default: última |
| `uf` | `str \| None` | UF (ex: 'MT', 'PR'). Default: todas |
| `levantamento` | `int \| None` | Número do levantamento (1-12). Default: último |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna tupla `(df, MetaInfo)` com proveniência |

**Retorno:**

DataFrame com colunas:
- `fonte`: Fonte dos dados
- `produto`: Produto
- `safra`: Ano-safra
- `uf`: Unidade federativa
- `area_plantada`: Área plantada (mil ha)
- `area_colhida`: Área colhida (mil ha)
- `produtividade`: Produtividade (kg/ha)
- `producao`: Produção (mil t)
- `levantamento`: Número do levantamento
- `data_publicacao`: Data de publicação

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
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `produto` | `str \| None` | Produto específico ou todos |
| `safra` | `str \| None` | Safra. Default: última |
| `as_polars` | `bool` | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Retorna tupla `(df, MetaInfo)` com proveniência |

**Retorno:**

DataFrame com colunas:
- `produto`: Produto
- `safra`: Ano-safra
- `levantamento`: Número do levantamento
- `estoque_inicial`: Estoque inicial (mil t)
- `producao`: Produção (mil t)
- `importacao`: Importação (mil t)
- `suprimento`: Suprimento total (mil t)
- `consumo`: Consumo (mil t)
- `exportacao`: Exportação (mil t)
- `demanda_total`: Demanda total (mil t)
- `estoque_final`: Estoque final (mil t)
- `unidade`: Unidade (`mil_ton`)

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
    return_meta: bool = False,
) -> pd.DataFrame | pl.DataFrame  # (df, MetaInfo) se return_meta=True
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

Lista de dicionários descrevendo cada levantamento publicado (safra, número e metadados).

---

### `produtos`

Lista produtos disponíveis (códigos aceitos em `safras()`).

```python
async def produtos() -> list[str]
```

---

### `ufs`

Lista as 27 UFs disponíveis.

```python
async def ufs() -> list[str]
```

---

## Funções relacionadas

O módulo CONAB também expõe (documentadas em páginas próprias ou nos contratos):

- `custo_producao(cultura, uf=...)` / `custo_producao_total(cultura, uf=...)` — custos de produção por hectare. Ver contrato [custo_producao](../contracts/custo_producao.md)
- `serie_historica(produto, ...)` — série histórica de safras (32 culturas desde 1976). Ver contrato [serie_historica_safra](../contracts/serie_historica_safra.md)
- `progresso_safra(...)` / `semanas_disponiveis()` — progresso semanal de plantio/colheita. Ver [API CONAB Progresso](conab_progresso.md)
- `ceasa_precos(...)` / `ceasa_produtos()` / `ceasa_categorias()` / `lista_ceasas()` — preços de atacado hortifrúti. Ver [API CONAB CEASA](conab_ceasa.md)

---

## Modelos

### `Safra`

```python
class Safra(BaseModel):
    fonte: Fonte
    produto: str
    safra: str = Field(..., pattern=r"^\d{4}/\d{2}$")
    uf: str | None = Field(None, min_length=2, max_length=2)
    area_plantada: Decimal | None = Field(None, ge=0)
    producao: Decimal | None = Field(None, ge=0)
    produtividade: Decimal | None = Field(None, ge=0)
    unidade_area: str = Field(default="mil_ha")
    unidade_producao: str = Field(default="mil_ton")
    levantamento: int = Field(..., ge=1, le=12)
    data_publicacao: date
    meta: dict[str, Any] = Field(default_factory=dict)
    parsed_at: datetime = Field(default_factory=utcnow)
    parser_version: int = Field(default=1)
    anomalies: list[str] = Field(default_factory=list)
```

## Produtos Disponíveis

`produtos()` retorna 25 códigos (incluindo aliases agregados e sub-safras):

| Código | Produto |
|--------|---------|
| `soja` | Soja |
| `milho` | Milho (total) |
| `milho_1` | Milho 1ª safra |
| `milho_2` | Milho 2ª safra |
| `milho_3` | Milho 3ª safra |
| `arroz` | Arroz (total) |
| `arroz_irrigado` | Arroz irrigado |
| `arroz_sequeiro` | Arroz sequeiro |
| `feijao` | Feijão (total) |
| `feijao_1` | Feijão 1ª safra |
| `feijao_2` | Feijão 2ª safra |
| `feijao_3` | Feijão 3ª safra |
| `algodao` | Algodão (total) |
| `algodao_pluma` | Algodão em pluma |
| `trigo` | Trigo |
| `sorgo` | Sorgo |
| `aveia` | Aveia |
| `cevada` | Cevada |
| `canola` | Canola |
| `girassol` | Girassol |
| `mamona` | Mamona |
| `amendoim` | Amendoim |
| `centeio` | Centeio |
| `triticale` | Triticale |
| `gergelim` | Gergelim |

## Versão Síncrona

```python
from agrobr.sync import conab

df = conab.safras('soja', safra='2024/25')
df = conab.balanco('milho')
```
