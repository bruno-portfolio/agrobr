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
| `produto` | str | Cultura monitorada |
| `data` | str | Data de referência (dd/mm/yyyy) |
| `condicao` | str | Condição: "boa", "media", "ruim" |
| `pct` | float | Percentual da lavoura nessa condição |
| `plantio_pct` | float | Progresso do plantio (%) |
| `colheita_pct` | float | Progresso da colheita (%) |

## Produtos

soja, milho, milho_1 (verão), milho_2 (safrinha), trigo, feijao,
feijao_1, feijao_2, feijao_3, aveia, cevada, canola, sorgo, mandioca.

## Nota de Risco

DERAL publica dados em planilhas Excel (PC.xls). O layout pode mudar
sem aviso entre safras. O parser detecta automaticamente os produtos
nas abas da planilha e extrai condições e progresso. Mudanças drásticas
de formato podem exigir atualização do parser.

## MetaInfo

```python
df, meta = await deral.condicao_lavouras("soja", return_meta=True)
print(meta.source)  # "deral"
print(meta.source_method)  # "httpx+openpyxl"
```

## Fonte

- URL: `https://www.agricultura.pr.gov.br/system/files/publico/Safras/PC.xls`
- Formato: Excel (.xls)
- Atualização: semanal
- Cobertura: Paraná
