# API ComexStat

O modulo ComexStat fornece dados de exportacao brasileira do MDIC/SECEX — volumes, valores FOB (USD) por produto, UF e pais de destino.

## Funcoes

### `exportacao`

Dados de exportacao por produto agricola.

```python
async def exportacao(
    produto: str,
    ano: int | None = None,
    uf: str | None = None,
    agregacao: str = "mensal",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str` | Produto (soja, milho, cafe, algodao, acucar, farelo_soja, oleo_soja) |
| `ano` | `int \| None` | Ano de referencia. Default: ano atual |
| `uf` | `str \| None` | Filtrar por UF de origem |
| `agregacao` | `str` | `"mensal"` (default) ou `"detalhado"` |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `ano`, `mes`, `ncm`, `uf`, `kg_liquido`, `valor_fob_usd`, `volume_ton` (apenas em agregacao mensal)

**Exemplo:**

```python
from agrobr import comexstat

# Exportacao soja 2024
df = await comexstat.exportacao("soja", ano=2024)

# Filtrar por UF
df = await comexstat.exportacao("milho", ano=2024, uf="MT")

# Detalhado (por registro)
df = await comexstat.exportacao("cafe", ano=2024, agregacao="detalhado")
```

## Versao Sincrona

```python
from agrobr.sync import comexstat

df = comexstat.exportacao("soja", ano=2024)
```

## Notas

- Fonte: [ComexStat/MDIC](https://comexstat.mdic.gov.br) — licenca livre
- 19 produtos mapeados por prefixo NCM
- Arquivos CSV anuais de ~100MB cada
- Dados disponiveis a partir de 1997
