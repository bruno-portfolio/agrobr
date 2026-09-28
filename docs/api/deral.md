# API DERAL

O modulo DERAL fornece dados de condicao de lavouras, progresso de plantio e colheita do Departamento de Economia Rural do Parana.

## Funcoes

### `condicao_lavouras`

Condicao semanal das lavouras paranaenses.

```python
async def condicao_lavouras(
    produto: str | None = None,
    *,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str \| None` | Filtrar por produto (`"soja"`, `"milho"`, `"milho_1"`, `"milho_2"`, `"trigo"`, `"feijao"`, `"cana"`, `"cafe"`, etc.). None retorna todos |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `produto`, `data`, `condicao`, `pct`, `plantio_pct`, `colheita_pct`

**Exemplo:**

```python
from agrobr import deral

# Todas as lavouras
df = await deral.condicao_lavouras()

# Apenas soja
df = await deral.condicao_lavouras("soja")

# Com metadados
df, meta = await deral.condicao_lavouras("milho", return_meta=True)
```

## Versao Sincrona

```python
from agrobr.sync import deral

df = deral.condicao_lavouras("soja")
```

## Notas

- Fonte: [DERAL/SEAB-PR](https://www.agricultura.pr.gov.br) — licenca livre
- Dados exclusivos do Parana
- Publicado em Excel (PC.xls) — layout pode variar entre safras

## Reconciliação das planilhas de fevereiro e setembro de 2026

As duas capturas originais de PC.xls são BIFF/XLS: 26 abas, 438 registros de
condição e 730 células numéricas de condição, plantio e colheita conferidas
diretamente. A extensão `.xlsx` do arquivo antigo preservado no golden não
indica o formato real. Não foi localizada uma publicação XLSX original para
certificar essa variante do parser.

Os percentuais publicados nessas capturas estão em pontos percentuais (0–100),
sem conversão de frações formatadas como porcentagem. As colunas de fase
fenológica e comercialização, as linhas de batata e de soja de segunda safra
ficam fora do contrato atual. Uma aba que informa feriado sem observações não
produz registros ou zeros. O nome `18-12-2017` contém referência publicada de
08/01/2018; a data vem da célula, conforme a publicação.

O parser 2 exige os cabeçalhos Ruim, Média, Boa, Plantada e Colhida nas tabelas
com várias culturas. Se faltar um deles, a fonte levanta `ParseError` e o dataset
propaga `SourceUnavailableError` com o motivo, evitando sucesso parcial com
apenas as abas históricas. O contrato permanece na versão 1.0.
