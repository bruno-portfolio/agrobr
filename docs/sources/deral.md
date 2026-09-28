# DERAL — Condição das Lavouras PR

Departamento de Economia Rural da Secretaria de Agricultura do Paraná (SEAB/PR).
Dados semanais de condição das lavouras, progresso de plantio e colheita.

## API

```python
from agrobr import deral

# Condição de todas as lavouras
df = await deral.condicao_lavouras()

# Filtrar por produto
df = await deral.condicao_lavouras("soja")
df = await deral.condicao_lavouras("milho")
df = await deral.condicao_lavouras("trigo")
```

## Colunas — `condicao_lavouras`

| Coluna | Tipo | Descrição |
|---|---|---|
| `produto` | str | Cultura monitorada; feijão e milho preservam a safra em `feijao_1`, `feijao_2`, `milho_1` ou `milho_2` |
| `data` | str | Data de referência (dd/mm/yyyy) |
| `condicao` | str | `boa`, `media` ou `ruim`; o progresso de plantio e colheita vem nas colunas abaixo, no mesmo registro |
| `pct` | float | Percentual da lavoura nessa condição |
| `plantio_pct` | float | Progresso do plantio (%) |
| `colheita_pct` | float | Progresso da colheita (%) |

## Data e percentuais publicados

`data` vem da célula de referência da aba, inclusive quando ela fica fora do
cabeçalho, e é normalizada para `dd/mm/yyyy`. Datas Excel, `dd/mm/yyyy`,
`dd-mm-yyyy` e `dd-mm-yy` são reconhecidas; anos de dois dígitos usam 2000+.
O nome da aba nunca substitui uma data ausente: nesse caso, o parser levanta
`ParseError` com o nome da aba.
Uma aba que não puder ser lida interrompe a leitura com `ParseError`; não há resultado parcial.

O quadro sai ordenado por `produto`, pela data em ordem cronológica e por `condicao`: a última
linha de cada produto é a referência mais recente. `data` continua texto `dd/mm/yyyy`, como no
contrato, e o `max()` ou a ordenação dessa coluna como texto não são cronológicos; converta com
`pd.to_datetime(df["data"], format="%d/%m/%Y")`.

A nota do PC.xls define `"-"` como zero absoluto. Nas colunas de percentual,
esse traço, inclusive com espaços, vira `0.0`; células vazias continuam nulas.
O `source_method` informa o leitor realmente usado, como `httpx+xlrd` para
o arquivo BIFF de setembro de 2026, incluindo eventual leitor alternativo.

## Produtos

8 culturas publicadas nas edições de fevereiro e setembro de 2026:
cafe, cevada, feijao_1, feijao_2, milho_1 (verão), milho_2 (safrinha), soja, trigo.

Aveia, cana, canola, mandioca e os totais milho/feijão têm alias no parser, mas não
aparecem nessas edições do relatório semanal. A API da fonte conserva
esses aliases; os filtros `milho` e `feijao` selecionam as respectivas safras
publicadas. O dataset anuncia somente as oito culturas acima, cuja disponibilidade
varia conforme a edição.

## Nota de Risco

DERAL publica dados em planilhas Excel (PC.xls). O layout pode mudar
sem aviso entre safras. O parser lê as abas de condição (uma linha por
cultura, com as colunas ruim, média, boa, plantada e colhida) e ignora as
demais. Mudanças drásticas de formato podem exigir atualização do parser.

## MetaInfo

```python
df, meta = await deral.condicao_lavouras("soja", return_meta=True)
print(meta.source)  # "deral"
print(meta.source_method)  # "httpx+xlrd"
```

## Fonte

- URL: `https://www.agricultura.pr.gov.br/system/files/publico/Safras/PC.xls`
- Formato: Excel (.xls)
- Atualização: semanal
- Cobertura: Paraná

## Leitura da planilha PC.xls

O PC.xls publicado em fevereiro e em setembro de 2026 é BIFF/XLS: 26 abas, 438
registros de condição e 730 células numéricas de condição, plantio e colheita. A
extensão `.xlsx` de um arquivo antigo não indica o formato real.

Os percentuais publicados nessas edições estão em pontos percentuais (0–100),
sem conversão de frações formatadas como porcentagem. As colunas de fase
fenológica e comercialização, as linhas de batata e de soja de segunda safra
ficam fora do contrato atual. Uma aba que informa feriado sem observações não
produz registros ou zeros. O nome `18-12-2017` contém referência publicada de
08/01/2018; a data vem da célula, conforme a publicação. Quando o nome de uma aba
datada (`dd-mm-aa` ou `dd-mm-aaaa`) difere da data da célula, como nesse caso e em
`19-09-2021`, com 20/09/2021, a leitura segue a célula e avisa em `validation_warnings`
e em `UserWarning`.

O parser 2 exige os cabeçalhos Ruim, Média, Boa, Plantada e Colhida nas tabelas
com várias culturas. Se faltar um deles, a fonte levanta `ParseError` e o dataset
propaga `SourceUnavailableError` com o motivo, evitando sucesso parcial com
apenas as abas históricas. O contrato permanece na versão 1.0.
