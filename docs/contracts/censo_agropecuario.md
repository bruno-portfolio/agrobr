# censo_agropecuario v1.2

Dados do Censo Agropecuário 1995/2006/2017 por tema, UF e nível territorial.

Na API 2.0, somente `tema` aceita posição; os demais filtros e flags são passados por nome. Retornos vazios preservam os dtypes do contrato: inteiros em `Int64`, medidas em `float64` e texto no padrão do pandas instalado.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | IBGE Censo Agro | Censo Agropecuário 1995, 2006 e 2017 |

## Temas

`efetivo_rebanho`, `uso_terra`, `lavoura_temporaria`, `lavoura_permanente`, `preparo_solo`, `adubacao`, `calagem`, `agrotoxicos`, `praticas_agricolas`, `irrigacao`, `despesa_adubos`

### Cobertura temporal por tema

| Tema | 1995 | 2006 | 2017 |
|------|:----:|:----:|:----:|
| `efetivo_rebanho` | ✅ | — | ✅ |
| `uso_terra` | ✅ | — | ✅ |
| `lavoura_temporaria` | ✅ | — | ✅ |
| `lavoura_permanente` | ✅ | — | ✅ |
| `preparo_solo` | — | ✅ | ✅ |
| `adubacao` | — | ✅ | ✅ |
| `calagem` | — | ✅ | ✅ |
| `agrotoxicos` | — | ✅ | ✅ |
| `praticas_agricolas` | — | ✅ | ✅ |
| `irrigacao` | — | ✅ | ✅ |
| `despesa_adubos` | — | — | ✅ |

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `ano` | Int64 | ❌ | Ano de referencia (1995, 2006 ou 2017) |
| `localidade` | str | ✅ | UF ou município |
| `localidade_cod` | Int64 | ✅ | Código IBGE |
| `cod_municipio` | Int64 | ✅ | Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo fora da linha de município |
| `tema` | str | ❌ | Tema do censo |
| `categoria` | str | ❌ | Categoria dentro do tema |
| `variavel` | str | ❌ | Nome da variável |
| `valor` | float64 | ✅ | Valor da variável |
| `unidade` | str | ❌ | Unidade de medida |
| `fonte` | str | ❌ | Origem dos dados |

## Primary Key

`[ano, tema, categoria, variavel, localidade]`

## Formato

Long format: cada linha tem um par variavel/valor.

### Variáveis por tema (temas originais)

| Tema | Variável | Unidade |
|------|----------|---------|
| `efetivo_rebanho` | `estabelecimentos` (só 2017) | unidades |
| `efetivo_rebanho` | `cabecas` | cabecas |
| `uso_terra` | `estabelecimentos` | unidades |
| `uso_terra` | `area` | hectares |
| `lavoura_temporaria` | `estabelecimentos` (2017) ou `informantes` (1995) | unidades |
| `lavoura_temporaria` | `producao` | varia |
| `lavoura_temporaria` | `area_colhida` | hectares |
| `lavoura_permanente` | `estabelecimentos` (2017) ou `informantes` (1995) | unidades |
| `lavoura_permanente` | `producao` | varia |
| `lavoura_permanente` | `area_colhida` | hectares |

Em 1995, as lavouras publicam `informantes`: a variável 151 da SIDRA (tabelas 492 e 504), que a SIDRA chama de
"Número de informantes". Em 2017, `estabelecimentos` é o "Número de estabelecimentos agropecuários com lavoura
temporária" (10084) e, na permanente, "com 50 pés e mais existentes" (9504).

### Novos temas — categorias

| Tema | Categorias (exemplos) |
|------|----------------------|
| `preparo_solo` | Cultivo convencional, Cultivo mínimo, Plantio direto na palha |
| `adubacao` | Quimica, Organica, Adubacao verde (2006); Fez adubacao, Quimica, Organica (2017) |
| `calagem` | Fez aplicação, Não fez aplicação |
| `agrotoxicos` | Utilizou, Não utilizou |
| `praticas_agricolas` | Plantio em nível, Rotacao de culturas, Pousio |
| `irrigacao` | Gotejamento, Pivo central, Inundacao, Aspersao |

## Garantias

- Dados decenais consolidados (Censo Agropecuário 1995, 2006 e 2017)
- Período de referencia 2017: outubro/2016 a setembro/2017
- Sem cache: cada chamada consulta o IBGE
- Parâmetro `ano` filtra por ano censal; `ano=None` retorna todos os anos disponíveis
- `categoria = "Total"` é a linha que a fonte publica como total da classificação do tema (ex.: todos os métodos de
  irrigação, todas as espécies do efetivo). Ela não se soma às demais categorias.
- `estabelecimentos` não soma entre categorias: um estabelecimento pode entrar em mais de uma. Irrigação, Brasília 2017:
  2.726 estabelecimentos no Total e 3.224 somando os 11 métodos. Medidas aditivas, como a área, fecham com o Total
  (25.626 ha nos dois) quando nenhuma categoria está em sigilo. Categoria em sigilo sai nula ("X" na fonte), e a soma
  fica abaixo do Total: no efetivo de AL, faltam 747 cabeças, porque Bubalinos e Avestruzes saem "X".
- O Censo conta só os estabelecimentos agropecuários e não bate com a PAM e a PPM, mesmo com os mesmos nomes: em 2017,
  fica cerca de 10% abaixo da PAM em soja e milho (até 20% no PR) e cerca de 20% abaixo da PPM no efetivo bovino. A data de referência também difere: o efetivo do Censo 2017 é o de 30/09/2017, e o da PPM, o de 31/12 de cada ano.
- O ano de 1995 também está no `censo_agropecuario_historico`, com números diferentes, porque as tabelas do SIDRA são
  outras ([detalhes](./censo_agropecuario_historico.md#relacao-com-outros-contratos)).

## Exemplo

```python
from agrobr import ibge

# Efetivo de rebanho por UF (1995 e 2017)
df = await ibge.censo_agro('efetivo_rebanho')

# Uso da terra em Mato Grosso
df = await ibge.censo_agro('uso_terra', uf='MT')

# Preparo do solo — ambos os anos
df = await ibge.censo_agro('preparo_solo')

# Irrigacao apenas 2017
df = await ibge.censo_agro('irrigacao', ano=2017)

# Lavoura temporaria por municipio
df = await ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')

# Com metadados
df, meta = await ibge.censo_agro('efetivo_rebanho', return_meta=True)
```

Pelo dataset, com o `MetaInfo` da camada semântica:

```python
from agrobr import datasets

df, meta = await datasets.censo_agropecuario("efetivo_rebanho", uf="MT", return_meta=True)
```

## Schema JSON

Disponível em `agrobr/schemas/censo_agropecuario.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("censo_agropecuario")
print(contract.to_json())
```

## Níveis Territoriais

| Nível | Descrição |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa (default) |
| `municipio` | Por município |

## Temas Legados (FTP)

6 temas adicionais do Censo 1995/96 estão disponíveis via `censo_agro_legado()` com contrato separado. Ver [censo_agropecuario_legado](./censo_agropecuario_legado.md).
