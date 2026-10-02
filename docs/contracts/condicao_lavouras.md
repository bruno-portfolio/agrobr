# Contrato: condicao_lavouras

Condição das lavouras paranaenses — SEAB/DERAL.

## Schema

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `produto` | STRING | Não | — | Key normalizado DERAL |
| `data` | DATE | Não | — | data de referência publicada na planilha |
| `condicao` | STRING | Não | — | boa, media, ruim, plantio, colheita |
| `pct` | FLOAT | Sim | % | 0-100 |
| `plantio_pct` | FLOAT | Sim | % | 0-100 |
| `colheita_pct` | FLOAT | Sim | % | 0-100 |

**PK:** `(produto, data, condicao)`

## Produtos

10 culturas: cafe, cevada, feijao, feijao_1, feijao_2, milho, milho_1, milho_2, soja, trigo.

`feijao` e `milho` juntam a 1ª e a 2ª safra: o filtro devolve as linhas de `feijao_1`/`feijao_2` (ou `milho_1`/`milho_2`),
e a coluna `produto` traz a safra. Aveia, cana, canola e mandioca têm alias no parser, mas não aparecem no relatório
semanal nas edições de fevereiro e setembro de 2026. A disponibilidade de cada cultura varia conforme a edição.

## Escopo geográfico

Dados cobrem exclusivamente o estado do Paraná (PR).

## Normalização

Cada registro traz a condição (`boa`, `media` ou `ruim`) e, na mesma linha, o progresso de
plantio e colheita da cultura. Nenhum caminho da 2.0.0 produz `plantio` ou `colheita` em
`condicao`: a única fonte é o PC.xls do DERAL, que publica só boa, média e ruim. O contrato
continua a admiti-los, para não recusar quadro externo validado com ele.

A fonte lê a data publicada em cada aba e a entrega em `datetime64[ns]`; os nomes
`Atual` e `Anterior` nunca são usados como datas. Sem referência publicada
reconhecível, a leitura falha com `ParseError` que identifica a aba.
O traço `"-"` representa zero absoluto segundo a nota do PC.xls e vira
`0.0` em `pct`, `plantio_pct` e `colheita_pct`. Células vazias permanecem nulas.

O quadro sai ordenado por `produto`, pela data em ordem cronológica e por `condicao`: a última
linha de cada produto é a referência mais recente. `data` sai em `datetime64[ns]` (até a 1.1.0, texto
`dd/mm/yyyy`); para filtrar uma data, compare com `pd.Timestamp("2026-02-01")`: o texto `"01/02/2026"` é
lido pelo pandas com o mês primeiro e casa 2 de janeiro, sem aviso.

## Exemplo

```python
from agrobr import datasets

# Todas as culturas
df = await datasets.condicao_lavouras()

# Apenas soja
df = await datasets.condicao_lavouras("soja")

# Com metadados
df, meta = await datasets.condicao_lavouras(return_meta=True)
```

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
também levanta `ParseError` ("Todas as fontes falharam por layout", com o motivo da
fonte em `errors`), evitando sucesso parcial com apenas as abas históricas. O contrato é a versão 2.0: `data` passou de texto `dd/mm/yyyy` a
`datetime64[ns]`.
