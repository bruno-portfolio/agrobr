# API IMEA

O módulo IMEA fornece cotações diarias, indicadores de precos e dados de comercializacao do Instituto Mato-Grossense de Economia Agropecuária.

!!! warning "zona_cinza"
    As séries públicas do IMEA são classificadas como `zona_cinza`: não foi comprovada licença de reutilização nem que a cláusula de arquivos não públicos alcance esse recorte. Os termos condicionam o compartilhamento de arquivos não públicos à autorização prévia por escrito; essa restrição permanece para tais arquivos. As reservas sobre bases de dados e outros ativos não são uma licença aberta. O módulo avisa na primeira chamada. [Termo de Uso do IMEA](https://imea.com.br/imea-site/termo-de-uso.html).

## Funções

### `cotacoes`

Cotações e indicadores de precos de Mato Grosso.

```python
async def cotacoes(
    cadeia: str = "soja",
    *,
    safra: str | None = None,
    unidade: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `cadeia` | `str` | Cadeia produtiva: `"soja"`, `"milho"`, `"algodao"`, `"bovinocultura"`, `"suinocultura"`, `"leite"` |
| `safra` | `str \| None` | Filtrar por safra: `"24/25"`, `"2024/25"` ou `"2024/2025"` (o IMEA publica `"24/25"`). None retorna todas; formato inválido levanta `InvalidParameterError` antes da rede |
| `unidade` | `str \| None` | Filtrar por unidade (ex: `"R$/sc"`, `"R$/t"`, `"%"`) |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `cadeia`, `indicador_id`, `indicador`, `localidade`, `valor`, `variacao`, `safra`, `unidade`, `unidade_descricao`, `data_publicacao`. `indicador_id` é o código do indicador no IMEA, e `indicador`, o nome dele no catálogo da cadeia. `data_publicacao` sai em `datetime64[ns]`. Filtro sem correspondência devolve DataFrame vazio com os mesmos dtypes; lista de cotações ou catálogo vazio na fonte levanta `ParseError`.

**Exemplo:**

```python
from agrobr import imea

# Cotacoes soja MT
df = await imea.cotacoes("soja")

# Safra especifica
df = await imea.cotacoes("milho", safra="24/25")
```

## Versão Síncrona

```python
from agrobr.sync import imea

df = imea.cotacoes("soja")
```

## Notas

- Fonte: [IMEA](https://imea.com.br) — licença `zona_cinza`
- Dados exclusivos de Mato Grosso
- Warning emitido no primeiro uso
