# ANDA — Fertilizantes

Associação Nacional para Difusão de Adubos. Dados de entregas de
fertilizantes por UF e mês.

## Instalação

ANDA requer `pdfplumber` como dependência opcional:

```bash
pip install agrobr[pdf]
```

## API

```python
from agrobr import anda

# Entregas de fertilizantes por UF/mês
df = await anda.entregas(ano=2024)

# Filtrar por UF
df = await anda.entregas(ano=2024, uf="MT")

# Agregação mensal (soma todas as UFs)
df = await anda.entregas(ano=2024, agregacao="mensal")
```

## Colunas — `entregas`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano |
| `mes` | int | Mês (1-12) |
| `uf` | str | UF |
| `produto_fertilizante` | str | Tipo de fertilizante |
| `volume_ton` | float | Volume entregue (toneladas) |

## Nota de Risco

ANDA publica dados em PDF. O layout pode mudar sem aviso entre anos.
O parser do agrobr detecta automaticamente a orientacao das tabelas
(UFs nas linhas vs colunas), e tambem suporta o layout "Principais
Indicadores" (dados nacionais agregados com meses/valores em celulas
concatenadas com `\n`). Mudancas drasticas de formato podem exigir
atualizacao do parser.

No agrobr-insights, dados ANDA sao tratados com peso dinamico: quando
parecem distorcidos, o peso no SCI e reduzido automaticamente.

## MetaInfo

```python
df, meta = await anda.entregas(ano=2024, return_meta=True)
print(meta.source)  # "anda"
print(meta.source_method)  # "httpx+pdfplumber"
```

## Fonte

- URL: `https://anda.org.br/recursos/`
- Formato: PDF/Excel
- Atualizacao: mensal
- Historico: 2010+
