# API Rio Verde

O modulo rio_verde fornece resultados de ensaios de cultivares de soja da Fundacao Rio Verde (Lucas do Rio Verde, MT).

## Funcoes

### `ensaio_soja`

Resultados de produtividade por cultivar e epoca de semeio.

```python
async def ensaio_soja(
    safra: str,
    *,
    cultivar: str | None = None,
    empresa: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `safra` | `str` | Safra (ex: "2025/2026"). **Obrigatorio** |
| `cultivar` | `str \| None` | Filtro por cultivar (contains, case-insensitive) |
| `empresa` | `str \| None` | Filtro por empresa obtentora |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Retorna tupla (DataFrame, MetaInfo) |

**Retorno:** DataFrame com colunas: `safra`, `empresa`, `cultivar`, `grupo_maturacao`, `ciclo_dias`, `produtividade_1_epoca_sc_ha`, `produtividade_2_epoca_sc_ha`, `produtividade_3_epoca_sc_ha`, `produtividade_4_epoca_sc_ha`, `produtividade_media_sc_ha`

**Exemplo:**

```python
from agrobr import rio_verde

# Ensaio da safra 2025/2026
df = await rio_verde.ensaio_soja("2025/2026")

# Filtrar por empresa
df = await rio_verde.ensaio_soja("2025/2026", empresa="Brasmax")

# Safras disponiveis
safras = await rio_verde.safras_disponiveis()
```

---

### `safras_disponiveis`

Lista as safras com dados disponíveis.

```python
async def safras_disponiveis() -> list[str]
```

**Retorno:** Lista de strings com as safras disponiveis (ex: `["2024/2025", "2025/2026"]`)

**Exemplo:**

```python
from agrobr import rio_verde

safras = await rio_verde.safras_disponiveis()
```

## Versao Sincrona

```python
from agrobr.sync import rio_verde

df = rio_verde.ensaio_soja(safra="2025/2026")
safras = rio_verde.safras_disponiveis()
```

## Notas

- Fonte: [Fundacao Rio Verde](https://fundacaorioverde.com.br) — licenca `zona_cinza`
- Requer `pip install agrobr[pdf]` (pdfplumber)
- ~97 cultivares x 4 epocas de semeio por safra
- Produtividade em sacas/hectare (sc/ha)
- PDF text-based (nao requer OCR)
