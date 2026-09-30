# producao_anual v2.2

Produção agrícola anual consolidada por UF ou município.

Na API 2.0, somente `produto`, `ano` aceitam posição; os demais filtros e flags são passados por nome. Retornos vazios preservam os dtypes do contrato: inteiros em `Int64`, medidas em `float64` e texto no padrão do pandas instalado.

Se o IBGE estiver indisponível, listas de anos não acionam uma chamada inválida à CONAB: esse fallback informa `SourceUnavailableError` porque não cobre múltiplos anos.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | IBGE PAM | Produção Agrícola Municipal |
| 2 | CONAB | Acompanhamento de Safras |

No fallback CONAB, o ano civil corresponde ao segundo ano da safra:
`ano=2023` consulta a safra `2022/23`. A fonte fornece dados estaduais; o nível
`brasil` é calculado pela soma das UFs e o nível `municipio` não possui fallback.
Como o boletim não publica área colhida, `area_colhida` fica nula nesse fallback.

O fallback usa o boletim de grãos e atende `soja`, `milho`, `arroz`, `feijao`,
`trigo` e `algodao`. `cafe`, `cacau`, `cana`, `mandioca` e `laranja` dependem da
PAM neste dataset; a série histórica de cana da CONAB é uma API distinta.

Sem `ano`, a PAM entrega o último ano publicado. O fallback CONAB entrega o ano
civil anterior ao corrente, ou seja, a safra `(Y-2)/(Y-1)`, já colhida nas culturas
de verão e de inverno; nunca a estimativa da safra em curso. Entre janeiro e a
publicação da PAM (setembro/outubro), as duas fontes podem diferir em um ano.
Safra duas ou mais atrás da edição mais recente do boletim sai da série histórica
da CONAB, que traz a revisão mais recente (ver `conab.safras`).

O `rendimento` muda de denominador com a fonte. Na PAM, é a produção ÷ área
colhida. Na CONAB, é a produtividade publicada por UF, calculada sobre a única
área que o boletim publica; no nível `brasil`, é a produção ÷ área plantada, somadas
as UFs.

A CONAB e o IBGE estimam níveis diferentes. Na soja, a CONAB ficou 3,8 % acima da PAM em 2025 (171,5 contra 165,3
milhões de t) e 3,1 % acima do LSPA em 2025/26; as áreas diferem menos de 1 %. Uma série que mistura as 2 fontes (o ano
em que a PAM falha, ou o ano ainda sem PAM) muda de nível no ano da troca, e o salto não é variação da safra: confira
`fonte` antes de comparar anos.

## Produtos

`soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`, `cafe`, `cacau`, `cana`, `mandioca`, `laranja`

Use as chaves canônicas acima. Para cana e mandioca, culturas temporárias de
longa duração, `area_plantada` representa a área destinada à colheita no ano
civil; para laranja, cultura permanente, também representa a área destinada
à colheita. A série começa em 1974, com `area_plantada` ausente antes de 1988.
O dataset consulta área, produção e rendimento; `valor_producao` permanece
nulo porque não é solicitado à PAM. A API `ibge.pam` permite solicitar essa
variável explicitamente.

## Schema

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| `ano` | Int64 | ❌ | Ano de referência |
| `produto` | str | ❌ | Nome do produto |
| `localidade` | str | ✅ | UF ou município |
| `localidade_cod` | Int64 | ❌ | Código IBGE da localidade (D1C do SIDRA); coluna opcional, só nas linhas do IBGE |
| `cod_municipio` | Int64 | ✅ | Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo fora da linha de município (e no fallback da CONAB, que é por UF) |
| `area_plantada` | float64 | ✅ | Área plantada (ha) |
| `area_colhida` | float64 | ✅ | Área colhida (ha) |
| `producao` | float64 | ✅ | Produção (ver `unidade_producao`) |
| `rendimento` | float64 | ✅ | Rendimento (ver `unidade_rendimento`) |
| `valor_producao` | float64 | ✅ | Valor da produção (ver `unidade_valor_producao`) |
| `fonte` | str | ❌ | Origem dos dados: `ibge_pam` ou `conab` |
| `unidade_producao` | str | ✅ | Unidade da produção: `ton`; laranja anterior a 2001 usa `mil_frutos` |
| `unidade_rendimento` | str | ✅ | Unidade do rendimento: `kg/ha`; laranja anterior a 2001 usa `frutos/ha` |
| `unidade_valor_producao` | str | ✅ | Moeda e escala: `mil_reais` ou moedas históricas conforme o ano |
| `condicao_produto` | str | ✅ | Café `em_coco` até 2001 e `beneficiado` desde 2002; ausente nos demais produtos |

## Primary Key

`[ano, produto, localidade]`

## Garantias

- Dados consolidados do ano agrícola completo
- Latência típica: Y+1 (dados disponíveis no ano seguinte)
- Área e produção da CONAB são convertidas de mil ha/mil ton para ha/ton

## Exemplo

```python
from agrobr import datasets

# Produção por UF
df = await datasets.producao_anual("soja", ano=2023)

# Produção por município
df = await datasets.producao_anual("milho", ano=2023, nivel="municipio")

# Filtrar por UF
df = await datasets.producao_anual("soja", ano=2023, uf="MT")

# Com metadados
df, meta = await datasets.producao_anual("soja", ano=2023, return_meta=True)
```

```python
df = await datasets.producao_anual("cana", ano=2024, uf="SP")
df = await datasets.producao_anual("mandioca", ano=2024, nivel="municipio", uf="RO")
df, meta = await datasets.producao_anual("laranja", ano=2000, nivel="brasil", return_meta=True)
```

## Schema JSON

Disponível em `agrobr/schemas/producao_anual.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("producao_anual")
print(contract.to_json())
```

## Níveis Territoriais

| Nível | Descrição |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa (default) |
| `municipio` | Por município; disponível apenas na fonte primária IBGE PAM |

## Unidades e quebras históricas da PAM

Os valores publicados não são convertidos implicitamente. `unidade_producao`, `unidade_rendimento` e `unidade_valor_producao` identificam a escala de cada linha. Laranja anterior a 2001 usa `mil_frutos` e `frutos/ha`; desde 2001, `ton` e `kg/ha`. `condicao_produto` distingue café `em_coco` até 2001 e `beneficiado` desde 2002. As moedas históricas permanecem identificadas, sem conversão para reais nem correção de inflação. Consulte as [notas metodológicas do IBGE](https://sidra.ibge.gov.br/pesquisa/pam/tabelas/).

O símbolo SIDRA `-` significa zero numérico e é preservado como zero; `..`, `...` e `X` permanecem ausentes. Municípios com produção zero não são eliminados. O contrato `producao_anual` é 2.1; as quatro colunas descritivas e o `localidade_cod` são opcionais no contrato e entregues pela API PAM.

O parser PAM 2 preserva também localidades e medidas inteiramente ausentes ou suprimidas. Duas observações para a mesma localidade, ano e medida, inclusive aliases de variável que colidem, geram `ParseError`; variáveis sem mapeamento também são recusadas. Não há seleção silenciosa do primeiro valor. O schema é 2.1 (2.0 mais o `localidade_cod`).

No fallback CONAB, Brasil é a soma das UFs e o rendimento é recalculado como
produção em toneladas × 1.000 / área em hectares. A produtividade nacional
publicada pela CONAB é arredondada e pode diferir desse quociente. A reconciliação
compara a soma de área e produção com a linha `BRASIL` publicada; não soma
regiões ou outros agregados.
