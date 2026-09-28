# API ANDA

O modulo ANDA fornece as entregas mensais de fertilizantes ao mercado brasileiro (total nacional), publicadas pela Associacao Nacional para Difusao de Adubos.

!!! warning "Licenca zona_cinza"
    Termos de uso nao localizados publicamente. Autorizacao formal solicitada em fev/2026 — aguardando resposta.

## Dependencia

Requer `pdfplumber`:

```bash
pip install agrobr[pdf]
```

## Funcoes

### `entregas`

Volume mensal de entregas de fertilizantes no Brasil.

```python
async def entregas(
    ano: int,
    *,
    produto: str = "total",
    agregacao: str = "detalhado",
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `ano` | `int` | Ano de referencia. Ano indisponivel no site levanta `InvalidParameterError` listando os anos disponíveis |
| `produto` | `str` | Mantido por compatibilidade. O único valor disponível é `"total"`; outros valores levantam `ValueError` antes do download |
| `agregacao` | `str` | `"detalhado"` (uma linha por mês) ou `"mensal"` (soma por mês, sem a coluna `uf`); outros valores levantam `InvalidParameterError` antes do download |
| `as_polars` | `bool` | Se True, retorna `polars.DataFrame` |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `ano`, `mes`, `uf` (sempre `"BR"`), `produto_fertilizante`, `volume_ton`

`produto_fertilizante` é sempre `"total"`: os boletins de entregas da ANDA
não publicam esse indicador separado por formulação. Versões anteriores
apenas copiavam o parâmetro `produto` para essa coluna, sem filtrar os dados.

**Exemplo:**

```python
from agrobr import anda

# Entregas 2024
df = await anda.entregas(2024)

# Equivalente; o parâmetro é mantido por compatibilidade
df = await anda.entregas(2024, produto="total")

# Agregado mensal
df = await anda.entregas(2024, agregacao="mensal")
```

## Versao Sincrona

```python
from agrobr.sync import anda

df = anda.entregas(2024)
```

## Notas

- Fonte: [ANDA](https://anda.org.br) — licenca `zona_cinza`
- Dados extraidos de PDF via `pdfplumber`
- Catálogo público conferido em 18/09/2026: PDFs de 2016 a 2026


## Cobertura e validação da publicação

O catálogo público conferido em 18/09/2026 contém 11 PDFs, de 2016 a 2026, todos com entregas nacionais mensais (`uf="BR"`). O boletim 2026 publica janeiro a junho; meses posteriores vazios não são zero. No ano corrente (data de Brasília), `entregas` e o dataset `fertilizante` avisam que o boletim é parcial, em `validation_warnings` e em `UserWarning`, e registram `ano_em_curso` e `meses_cobertos` (os meses publicados) em `source_details`. Como nenhum deles publica recorte estadual, a 2.0.0 tirou o parâmetro `uf` da fonte e do dataset `fertilizante` (guia de migração 2.0, seção 50).

O parser 3 exige a seção `Fertilizantes Entregues ao Mercado (em toneladas de produto)` e procura o ano somente nela. Se o ano ou essa identificação estiver ausente, a fonte levanta `ParseError`; o dataset preserva o motivo em `SourceUnavailableError`. Produção, importação, exportação e relações de troca do mesmo PDF não podem substituir entregas. Valores publicados e o contrato 2.0 permanecem iguais.
