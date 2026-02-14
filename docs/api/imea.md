# API IMEA

O modulo IMEA fornece cotacoes diarias, indicadores de precos e dados de comercializacao do Instituto Mato-Grossense de Economia Agropecuaria.

!!! danger "Licenca restrito"
    Termos de uso do IMEA proibem redistribuicao sem autorizacao escrita. Uso pessoal/educacional apenas. Ref: [imea.com.br/termo-de-uso](https://imea.com.br/imea-site/termo-de-uso.html)

## Funcoes

### `cotacoes`

Cotacoes e indicadores de precos de Mato Grosso.

```python
async def cotacoes(
    cadeia: str = "soja",
    *,
    safra: str | None = None,
    unidade: str | None = None,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `cadeia` | `str` | Cadeia produtiva: `"soja"`, `"milho"`, `"algodao"`, `"bovinocultura"` |
| `safra` | `str \| None` | Filtrar por safra (ex: `"24/25"`). None retorna todas |
| `unidade` | `str \| None` | Filtrar por unidade (ex: `"R$/sc"`, `"R$/t"`, `"%"`) |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `cadeia`, `localidade`, `valor`, `variacao`, `safra`, `unidade`, `unidade_descricao`, `data_publicacao`

**Exemplo:**

```python
from agrobr import imea

# Cotacoes soja MT
df = await imea.cotacoes("soja")

# Safra especifica
df = await imea.cotacoes("milho", safra="24/25")
```

## Versao Sincrona

```python
from agrobr.sync import imea

df = imea.cotacoes("soja")
```

## Notas

- Fonte: [IMEA](https://imea.com.br) — licenca `restrito`
- Dados exclusivos de Mato Grosso
- Warning emitido no primeiro uso
