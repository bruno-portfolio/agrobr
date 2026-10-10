# pecuaria_municipal v1.1

Efetivo de rebanhos e produção de origem animal por UF ou município.

Na API 2.0, somente `produto`, `ano` aceitam posição; os demais filtros e flags são passados por nome. Retornos vazios preservam os dtypes do contrato: inteiros em `Int64`, medidas em `float64` e texto no padrão do pandas instalado.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | IBGE PPM | Pesquisa da Pecuaria Municipal |

## Produtos

### Rebanhos

`bovino`, `bubalino`, `equino`, `suino_total`, `suino_matrizes`, `caprino`, `ovino`, `galinaceos_total`, `galinhas`, `codornas`

O `bovino` da PPM não é o rebanho do USDA (`usda.psd`, código `0011000`): a PPM é o efetivo levantado pelo IBGE por município para o ano de referência, e o `Beginning Stocks` do USDA é a estimativa dele para o início do ano. A PPM de 2024 traz 238,2 milhões de cabeças, e o `Beginning Stocks` de 2025, 186,9 milhões (21,5 % a menos). As 2 séries não são a mesma medida.

`galinhas` é a categoria do IBGE "Galináceos - galinhas", que inclui poedeiras e matrizeiras. `galinhas_poedeiras` continua aceito como alias depreciado (`FutureWarning`) e devolve `especie="galinhas"`.

### Produção de origem animal

`leite`, `ovos_galinha`, `ovos_codorna`, `mel`, `casulos`, `la`

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `ano` | Int64 | ❌ | Ano de referencia |
| `localidade` | str | ✅ | UF ou município |
| `localidade_cod` | Int64 | ✅ | Código IBGE |
| `cod_municipio` | Int64 | ✅ | Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo fora da linha de município |
| `especie` | str | ❌ | Nome da especie/produto |
| `valor` | float64 | ✅ | Valor (unidade varia por espécie) |
| `unidade` | str | ❌ | Unidade de medida |
| `fonte` | str | ❌ | Origem dos dados |

## Primary Key

`[ano, especie, localidade]`

## Garantias

- Dados consolidados do ano civil (referencia 31/dez)
- Latencia tipica: Y+1 (dados disponíveis no ano seguinte)
- Serie histórica desde 1974

## Exemplo

```python
from agrobr import datasets

# Rebanho bovino por UF
df = await datasets.pecuaria_municipal("bovino", ano=2023)

# Producao de leite por municipio
df = await datasets.pecuaria_municipal("leite", ano=2023, nivel="municipio", uf="MG")

# Filtrar por UF
df = await datasets.pecuaria_municipal("bovino", ano=2023, uf="MT")

# Com metadados
df, meta = await datasets.pecuaria_municipal("bovino", ano=2023, return_meta=True)
```

## Schema JSON

Disponível em `agrobr/schemas/pecuaria_municipal.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("pecuaria_municipal")
print(contract.to_json())
```

## Níveis Territoriais

| Nível | Descrição |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa (default) |
| `municipio` | Por município |
