# API UN Comtrade

Comercio internacional bilateral via UN Comtrade API. Feature principal: trade mirror — comparar exportacoes declaradas pelo reporter vs importacoes declaradas pelo parceiro.

## API Key (Opcional)

Funciona em guest mode (sem key, 500 records/call). Para mais capacidade:

1. Registre em [comtradeplus.un.org](https://comtradeplus.un.org)
2. Configure: `export AGROBR_COMTRADE_API_KEY=sua_chave`

## Funcoes

### `comercio`

Dados de comercio bilateral por produto, pais e periodo.

```python
async def comercio(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str | None = None,
    fluxo: str = "X",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str` | Produto: `"soja"`, `"complexo_soja"`, `"carne_bovina"` ou HS code |
| `reporter` | `str` | Pais reporter: `"BR"`, `"CN"`, `"US"`. Default: `"BR"` |
| `partner` | `str \| None` | Pais parceiro. None = World (todos). Default: None |
| `fluxo` | `str` | `"X"` (exportacao) ou `"M"` (importacao). Default: `"X"` |
| `periodo` | `str \| int \| None` | Ano, mes ou range: `2024`, `202401`, `"2022-2024"`. None = ano corrente |
| `freq` | `str` | `"A"` (anual) ou `"M"` (mensal). Default: `"A"` |
| `api_key` | `str \| None` | API key (ou usa `AGROBR_COMTRADE_API_KEY`) |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `periodo`, `ano`, `mes`, `reporter_iso`, `partner_iso`, `fluxo_code`, `hs_code`, `produto_desc`, `peso_liquido_kg`, `volume_ton`, `valor_fob_usd`, `valor_cif_usd`, `valor_primario_usd`

### `trade_mirror`

Comparacao bilateral: o que o reporter exportou vs o que o parceiro importou.

```python
async def trade_mirror(
    produto: str,
    *,
    reporter: str = "BR",
    partner: str = "CN",
    periodo: str | int | None = None,
    freq: str = "A",
    api_key: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Retorno:**

DataFrame com colunas de ambos os lados + discrepancias:

| Coluna | Descricao |
|--------|-----------|
| `peso_liquido_kg_reporter` | Peso declarado pelo reporter (exportacao) |
| `valor_fob_usd_reporter` | Valor FOB declarado pelo reporter |
| `peso_liquido_kg_partner` | Peso declarado pelo partner (importacao) |
| `valor_cif_usd_partner` | Valor CIF declarado pelo partner |
| `diff_peso_kg` | Diferenca de peso (reporter - partner) |
| `diff_valor_fob_usd` | Diferenca de valor FOB (reporter - partner) |
| `ratio_valor` | FOB reporter / CIF partner (esperado ~0.85-0.95) |
| `ratio_peso` | Peso reporter / peso partner (esperado ~1.0) |

### `paises`

```python
def paises() -> list[str]
```

Retorna lista de codigos ISO3 aceitos.

### `produtos`

```python
def produtos() -> dict[str, list[str]]
```

Retorna mapeamento nome canonico -> lista de HS codes.

## Exemplos

```python
from agrobr import comtrade

# Exportacao BR de soja para China
df = await comtrade.comercio("soja", reporter="BR", partner="CN", periodo=2024)

# Trade mirror — o killer feature
df = await comtrade.trade_mirror("soja", reporter="BR", partner="CN", periodo=2024)

# Mensal com chunking automatico
df = await comtrade.trade_mirror("soja", partner="CN", freq="M", periodo="2022-2024")

# Complexo soja (grao + oleo + farelo)
df = await comtrade.comercio("complexo_soja", partner="CN")

# Outros parceiros
df = await comtrade.trade_mirror("carne_bovina", partner="US")
```

## Versao Sincrona

```python
from agrobr.sync import comtrade

df = comtrade.trade_mirror("soja", partner="CN")
```

## Notas

- Fonte: [UN Comtrade](https://comtradeapi.un.org) — licenca livre (dados publicos ONU)
- Guest mode: 500 records/call, ~100 req/hora
- Free key: 500 calls/dia, 100k records/call
- Periodos > 12 meses sao divididos em chunks automaticamente
- Dados mensais desde 2000, anuais desde 1988
