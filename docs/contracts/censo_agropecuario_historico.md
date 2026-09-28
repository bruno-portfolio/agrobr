# censo_agropecuario_historico v1.1

Serie historica do Censo Agropecuario (1920-2006) por tema e UF via SIDRA.

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | IBGE Censo Agro Historico | Serie historica via SIDRA (9 tabelas, ate UF) |

## Temas

`estabelecimentos_area`, `uso_terra`, `pessoal_tratores`, `condicao_produtor`, `efetivo_animais`, `producao_animal`, `producao_vegetal`, `lavoura_permanente`, `lavoura_temporaria`

### Cobertura temporal por tema

| Tema | Censos disponiveis | Total |
|------|--------------------|:-----:|
| `estabelecimentos_area` | 1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 10 |
| `uso_terra` | 1970, 1975, 1980, 1985, 1995, 2006 | 6 |
| `pessoal_tratores` | 1970, 1975, 1980, 1985, 1995, 2006 | 6 |
| `condicao_produtor` | 1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 10 |
| `efetivo_animais` | 1970, 1975, 1980, 1985, 1995, 2006 | 6 |
| `producao_animal` | 1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 10 |
| `producao_vegetal` | 1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 10 |
| `lavoura_permanente` | 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 9 |
| `lavoura_temporaria` | 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006 | 9 |

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `ano` | int | ❌ | Ano censitario (1920-2006) |
| `localidade` | str | ✅ | UF ou regiao |
| `localidade_cod` | int | ✅ | Codigo IBGE |
| `cod_municipio` | int | ✅ | Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo fora da linha de município |
| `tema` | str | ❌ | Tema da serie historica |
| `categoria` | str | ❌ | Categoria dentro do tema (ou "total") |
| `variavel` | str | ❌ | Nome da variavel |
| `valor` | float64 | ✅ | Valor da variavel |
| `unidade` | str | ❌ | Unidade de medida |
| `fonte` | str | ❌ | Sempre "ibge_censo_agro_historico" |

## Primary Key

`[ano, tema, categoria, variavel, localidade]`

## Formato

Long format: cada linha tem um par variavel/valor.

### Variaveis por tema

| Tema | Variavel | Unidade |
|------|----------|---------|
| `estabelecimentos_area` | `estabelecimentos`, `area`, `estabelecimentos_pct`, `area_pct` | Unidades, Hectares, % |
| `uso_terra` | `area`, `area_pct` | Hectares, % |
| `pessoal_tratores` | `pessoal_ocupado`, `tratores` | Pessoas, Unidades |
| `condicao_produtor` | `estabelecimentos`, `area`, `estabelecimentos_pct`, `area_pct` | Unidades, Hectares, % |
| `efetivo_animais` | `efetivo` | Cabecas / Mil cabecas (Aves) |
| `producao_animal` | `producao` | Mil litros / Mil duzias / Toneladas |
| `producao_vegetal` | `producao`, `area_colhida` | varia por cultura, Hectares |
| `lavoura_permanente` | `quantidade_produzida` | varia por cultura |
| `lavoura_temporaria` | `quantidade_produzida` | varia por cultura |

## Niveis Territoriais

| Nivel | Descricao |
|-------|-----------|
| `brasil` | Total nacional |
| `regiao` | Por regiao (Norte, Nordeste, etc) |
| `uf` | Por Unidade Federativa (default) |

**Municipal NAO disponivel** — dados municipais nao existem no SIDRA para serie historica.

## Quirks

- **Aves**: unidade "Mil cabecas" (tabela 281), demais animais em "Cabecas"
- **Unidades mistas**: producao animal/vegetal e lavouras tem unidades que variam por categoria (litros, duzias, toneladas, frutos, cachos, etc)
- **Classificacoes sem Total**: tabelas 281/282/283/1730/1731 nao tem categoria "Total"
- **Missing values**: `".."` = indisponivel, `"..."` = suprimido, `"-"` = nao aplicavel → todos convertidos para NaN

## Garantias

- Anos validos sao apenas anos censitarios (1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006)
- Valores numericos sempre >= 0
- Fonte sempre "ibge_censo_agro_historico"
- Nivel territorial maximo e UF (sem dados municipais)
- Sem cache: cada chamada consulta o IBGE

## Exemplo

```python
from agrobr import ibge

# Estabelecimentos e area, Brasil, 1985
df = await ibge.censo_agro_historico('estabelecimentos_area', ano=1985, nivel='brasil')

# Efetivo de animais, todas as UFs, todos os censos
df = await ibge.censo_agro_historico('efetivo_animais')

# Pessoal e tratores em Sao Paulo, 1980 e 1985
df = await ibge.censo_agro_historico('pessoal_tratores', ano=[1980, 1985], uf='SP')

# Via dataset semantico
from agrobr import datasets
df = await datasets.censo_agropecuario_historico('producao_vegetal')

# Com metadados
df, meta = await ibge.censo_agro_historico('uso_terra', ano=1985, return_meta=True)
```

## Schema JSON

Disponivel em `agrobr/schemas/censo_agropecuario_historico.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("censo_agropecuario_historico")
print(contract.to_json())
```

## Relacao com outros contratos

| Contrato | Escopo | Periodos |
|----------|--------|----------|
| `censo_agropecuario` | 11 temas temáticos (SIDRA) | 1995, 2006, 2017 |
| `censo_agropecuario_legado` | 6 temas legados (FTP) | 1995 |
| **`censo_agropecuario_historico`** | **9 temas serie historica (SIDRA)** | **1920-2006** |

São contratos separados, cada um com seu dataset wrapper e registry entry. O ano de 1995 aparece em dois deles com números
diferentes: o `censo_agropecuario` lê as tabelas do Censo 1995-96 no SIDRA (316, 323, 497 e 509), e o
`censo_agropecuario_historico`, as da série histórica (264, 281, 1730 e 1731).

- Na cana da PB, são 8,7 Mt no `censo_agropecuario` e 3,5 Mt no `censo_agropecuario_historico`.
- O arquivo original do Censo no FTP do IBGE (1998) confirma a série histórica na cana da PB e nas áreas totais de MT e MA.
  Na soja do PR, as três fontes divergem.
- A nota da tabela 1731 do IBGE diz que "pequenas correções foram feitas após a publicação".
- Para a série longa, use o `censo_agropecuario_historico`.
