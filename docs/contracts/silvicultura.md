# silvicultura v1.1

Producao silvicultural (eucalipto, pinus, carvao vegetal, madeira) por UF ou municipio.

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | IBGE PEVS | Producao da Extracao Vegetal e da Silvicultura |

## Produtos

`carvao`, `lenha`, `madeira_tora`, `madeira_celulose`, `madeira_outras_finalidades`, `acacia_negra`, `eucalipto_folha`, `resina`

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `ano` | int | ❌ | Ano de referencia |
| `localidade` | str | ✅ | UF ou municipio |
| `localidade_cod` | int | ✅ | Codigo IBGE |
| `cod_municipio` | int | ✅ | Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo fora da linha de município |
| `produto` | str | ❌ | Nome do produto |
| `valor` | float64 | ✅ | Quantidade produzida (toneladas ou metros cúbicos) ou, com `variavel="valor_producao"`, valor da produção em mil reais; a escala vem em `unidade` |
| `unidade` | str | ❌ | Unidade de medida |
| `fonte` | str | ❌ | Origem dos dados |

## Primary Key

`[ano, produto, localidade]`

## Garantias

- Dados consolidados anuais
- Latencia tipica: Y+1 (dados disponiveis no ano seguinte)
- Serie historica desde 1986

## Exemplo

```python
from agrobr import datasets

# Producao de madeira em tora por UF
df = await datasets.silvicultura("madeira_tora", ano=2023)

# Carvao vegetal em Minas Gerais
df = await datasets.silvicultura("carvao", ano=2023, uf="MG")

# Area plantada de eucalipto (via ibge direto)
from agrobr import ibge
df = await ibge.silvicultura("eucalipto", variavel="area")

# Com metadados
df, meta = await datasets.silvicultura("madeira_tora", ano=2023, return_meta=True)
```

## Schema JSON

Disponivel em `agrobr/schemas/silvicultura.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("silvicultura")
print(contract.to_json())
```

## Niveis Territoriais

| Nivel | Descricao |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa (default) |
| `municipio` | Por municipio |
