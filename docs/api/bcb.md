# API BCB/SICOR

O modulo BCB fornece dados de credito rural do SICOR (Sistema de Operacoes do Credito Rural e do Proagro) do Banco Central do Brasil.

## Funcoes

### `credito_rural`

Dados de financiamento rural por produto, safra, UF e municipio.

```python
async def credito_rural(
    produto: str,
    safra: str | None = None,
    finalidade: str = "custeio",
    uf: str | None = None,
    agregacao: str = "municipio",
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str` | Produto (soja, milho, arroz, feijao, trigo, algodao, cafe, cana, sorgo) |
| `safra` | `str \| None` | Safra formato "2024/25". Default: safra mais recente |
| `finalidade` | `str` | `"custeio"`, `"investimento"` ou `"comercializacao"` |
| `uf` | `str \| None` | Filtrar por UF (ex: "MT", "PR") |
| `agregacao` | `str` | `"municipio"` (default) ou `"uf"` |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `safra`, `uf`, `cd_municipio`, `municipio`, `produto`, `valor`, `area_financiada`, `qtd_contratos`

**Exemplo:**

```python
from agrobr import bcb

# Credito custeio soja MT
df = await bcb.credito_rural("soja", safra="2024/25", uf="MT")

# Agregado por UF
df = await bcb.credito_rural("milho", agregacao="uf")

# Com metadados
df, meta = await bcb.credito_rural("soja", return_meta=True)
```

## Versao Sincrona

```python
from agrobr.sync import bcb

df = bcb.credito_rural("soja", safra="2024/25")
```

## Fallback

Quando a API OData do BCB falha, o agrobr usa automaticamente BigQuery (Base dos Dados) como fallback. Requer `pip install agrobr[bigquery]`.

## Notas

- Fonte: [BCB/SICOR](https://olinda.bcb.gov.br) — licenca livre
- Dados disponiveis a partir de 2013
