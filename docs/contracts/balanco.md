# balanco v1.1

Balanço de oferta e demanda de commodities.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CONAB | Balanço de Oferta e Demanda |

A fonte CONAB usa HTTP primeiro. Playwright e Chromium são opcionais para fallback de transporte:

```bash
pip install agrobr[browser]
python -m playwright install chromium
```

`balanco` não possui fonte alternativa de fallback. Se HTTP e o transporte opcional falharem, o dataset levanta `SourceUnavailableError`.

Sem `levantamento`, `safra` escolhe a publicação mais recente cuja aba Suprimento traz essa safra; a edição corrente cobre as últimas sete safras (seis na soja), já revisadas. A tabela devolvida é a dessa publicação e pode conter linhas de vários períodos. Com `levantamento=N`, vem o N-ésimo levantamento da própria safra, que é a edição original. Para o trigo, os períodos do balanço permanecem anuais. A última revisão publicada de cada produto e período prevalece.

## Produtos

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `safra` | str | ❌ | Biênio publicado, como "2024/25", ou ano civil para trigo |
| `produto` | str | ❌ | Nome do produto |
| `estoque_inicial` | float64 | ✅ | Estoque inicial (mil ton) |
| `producao` | float64 | ✅ | Produção (mil ton) |
| `importacao` | float64 | ✅ | Importação (mil ton) |
| `suprimento` | float64 | ✅ | Suprimento total (mil ton): estoque inicial + produção + importação, somados pelo agrobr; nulo se faltar uma parcela |
| `consumo` | float64 | ✅ | Consumo interno (mil ton): sementes/outros + processamento, somados pelo agrobr (soja 2025/26, set/26: 3.766 + 62.137,7 = 65.903,7); nulo se faltar uma parcela |
| `exportacao` | float64 | ✅ | Exportação (mil ton) |
| `estoque_final` | float64 | ✅ | Estoque final (mil ton) |
| `demanda_total` | float64 | ✅ | Demanda publicada (mil ton); nula no wide e no long antigo |
| `levantamento` | str | ✅ | Rótulo textual da revisão, como `set/26`; nulo quando não publicado |
| `unidade` | str | ❌ | Unidade das métricas: `mil_ton` |
| `fonte` | str | ❌ | Fonte selecionada no dataset: `conab` |

Todas as colunas numéricas usam float64, inclusive em resultados vazios. O contrato 1.1 adiciona colunas opcionais sem alterar as obrigatórias da versão 1.0; `CONAB_BALANCO_V1` permanece disponível e `CONAB_BALANCO_V1_1` é o ativo. Demanda não publicada fica nula, sem cálculo substituto; `levantamento` é o rótulo da revisão, não o número do levantamento.

## Garantias

- Balanço completo de oferta/demanda
- Atualizado mensalmente junto com levantamentos CONAB

## Exemplo

```python
from agrobr import datasets

# Balanço safra corrente
df = await datasets.balanco("soja")

# Balanço safra específica (revisão mais recente)
df = await datasets.balanco("soja", safra="2024/25")

# Edição original: 12º levantamento da própria safra
df = await datasets.balanco("soja", safra="2024/25", levantamento=12)

# Com metadados
df, meta = await datasets.balanco("soja", return_meta=True)
```

## Componentes do Balanço

```
Suprimento = Estoque Inicial + Produção + Importação
Demanda = Consumo + Exportação
Estoque Final = Suprimento - Demanda
```

## Schema JSON

Disponível em `agrobr/schemas/balanco.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("balanco")
print(contract.primary_key)  # ['safra', 'produto']
print(contract.to_json())
```

## Unidades

Todos os valores numéricos em **mil toneladas**.
