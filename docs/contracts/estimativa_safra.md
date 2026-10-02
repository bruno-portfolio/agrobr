# estimativa_safra v3.1

Estimativas de safra com levantamento CONAB ou mês de referência LSPA explícito. O dataset tem contrato próprio, `ESTIMATIVA_SAFRA_V3_1`; a fonte CONAB e `CONAB_SAFRA_V2` permanecem em 2.0.

## Fontes e seletores

| Seleção | Fonte usada | Referência |
|---|---|---|
| Sem fonte ou seletor de referência | CONAB → IBGE LSPA | Observação mais recente disponível na fonte selecionada |
| `fonte="conab"` | Somente CONAB | Publicação mais recente que traz a safra (a safra anterior vem da safra seguinte, revisada; duas ou mais safras atrás, da série histórica da CONAB, com `levantamento` e `data_publicacao` nulos), ou `levantamento` explícito |
| `levantamento=1` | Somente CONAB | Levantamento 1 da safra solicitada |
| `fonte="ibge_lspa"` | Somente LSPA | Mês mais recente com observações, ou `mes` explícito |
| `mes="01"` | Somente LSPA | Janeiro do ano civil final da safra |

`levantamento` e `mes` são seletores distintos. Usar ambos, ou combinar um deles com `fonte` incompatível, gera erro antes da rede. Uma fonte forçada não é substituída por outra. Referência explícita indisponível gera erro, sem seleção silenciosa de outro mês, levantamento ou fonte.

Quando todas as fontes consultadas respondem sem observações para o recorte (produto, safra, UF e seletor sem linha), o resultado é o vazio do contrato, com os mesmos dtypes, um `UserWarning` e o aviso em `MetaInfo.validation_warnings`. Falha de rede ou de layout de alguma fonte mantém a classe do erro: com uma fonte vazia e a outra fora do ar sai `SourceUnavailableError`, e com a outra falhando por layout sai `ParseError`.

Sem mês explícito, o LSPA seleciona o período mais recente disponível com observações. Não completa componentes ausentes do mês solicitado com meses anteriores.

Com `uf=None`, a CONAB retorna linhas por UF e o LSPA retorna o agregado Brasil (`uf` nulo). Informe uma UF nos comparativos entre fontes. O fallback não garante cobertura geográfica ou metodologia idêntica: a CONAB e o IBGE estimam níveis diferentes (na soja de 2025/26, a CONAB ficou 3,1 % acima do LSPA), e uma série que mistura as 2 fontes muda de nível no ano da troca.

A CONAB tenta HTTP primeiro. Playwright e Chromium são dependências opcionais para fallback de transporte:

```bash
pip install agrobr[browser]
python -m playwright install chromium
```

A disponibilidade dos levantamentos depende do catálogo descoberto; a interface não garante arquivo completo de todas as safras e edições.

## Parâmetros

`produto`, `safra` e `uf` aceitam posição; as flags e os seletores restantes são somente nomeados:

```python
async def estimativa_safra(
    produto: str,
    safra: str | None = None,
    uf: str | None = None,
    *,
    return_meta: bool = False,
    fonte: Literal["conab", "ibge_lspa"] | None = None,
    levantamento: int | None = None,
    mes: int | str | None = None,
    as_polars: bool = False,
) -> DataFrameResult: ...
```

Produtos: `soja`, `milho`, `arroz`, `feijao`, `trigo`, `algodao`. `levantamento` deve ser inteiro de 1 a 12. `mes` aceita inteiro ou string inteira de 1 a 12, incluindo `"01"`; não aceita período como `"202501"` nem lista de meses.

`as_polars=True` retorna um DataFrame Polars, após a validação do contrato, e requer `pip install agrobr[polars]`.

`DataFrameResult` inclui DataFrames pandas ou Polars e a tupla com `MetaInfo` quando `return_meta=True`. Em pandas, `data_publicacao` usa `datetime64[ns]`; anos, meses e números de levantamento usam `Int64`, medidas usam `float64` e o texto segue o dtype padrão do pandas. Os vazios preservam esses tipos.

A safra é normalizada para anos consecutivos no formato `YYYY/YY`. Para LSPA, `safra="2024/25"` seleciona o ano civil **2025**. O rótulo de dois anos é convenção de compatibilidade do dataset, não campo nativo LSPA nem consulta ao prognóstico do ano seguinte.

## Schema

| Coluna | Tipo | Nullable | Significado |
|---|---|---|---|
| `fonte` | str | Não | `conab` ou `ibge_lspa` |
| `produto` | str | Não | Nome do produto |
| `safra` | str | Não | Safra normalizada, por exemplo `2024/25` |
| `uf` | str | Sim | UF; nula para o agregado Brasil do LSPA |
| `area_plantada` | float64 | Sim | Área plantada, mil ha |
| `area_colhida` | float64 | Sim | Área colhida LSPA, mil ha; nula na CONAB |
| `produtividade` | float64 | Sim | Produtividade, kg/ha |
| `producao` | float64 | Sim | Produção, mil toneladas |
| `levantamento` | Int64 | Sim | Número do levantamento CONAB (1–12) do boletim que publicou o número; nulo para LSPA e para safra servida pela série histórica |
| `data_publicacao` | date | Sim | Data do boletim CONAB que publicou o número, quando disponível; nula para LSPA e para safra servida pela série histórica |
| `ano_lspa` | Int64 | Sim | Ano civil observado no LSPA; nulo para CONAB |
| `mes_lspa` | Int64 | Sim | Mês observado no LSPA (1–12); nulo para CONAB |
| `unidade_producao` | str | Não | Unidade de `producao`: `mil_ton` nas 2 fontes |
| `unidade_area` | str | Não | Unidade de `area_plantada` e `area_colhida`: `mil_ha` nas 2 fontes |

**Chave primária:** `[fonte, safra, produto, uf, levantamento, ano_lspa, mes_lspa]`.

O `levantamento` e a `data_publicacao` são do boletim CONAB que publicou o número, e não da safra: sem `levantamento`, a safra passada vem da publicação mais recente que a traz, revisada. A safra 2024/25 servida pelo 12º levantamento de 2025/26 sai com `levantamento=12` e `data_publicacao=2026-09-15`; o 12º levantamento de 2024/25 é outro boletim, com outro número. A safra do boletim está em `meta.source_details["publicacao"]["safra"]`. Duas ou mais safras atrás da edição mais recente, sem `levantamento`, o número vem da série histórica, e `levantamento` e `data_publicacao` saem nulos (ver `conab.safras`).

Todas as colunas estão presentes, inclusive as nullable. A escala é a mesma nas 2 rotas: produção em mil toneladas e áreas em mil hectares, declaradas em `unidade_producao` (`mil_ton`) e `unidade_area` (`mil_ha`), os mesmos valores de `conab.brasil_total`. O [`producao_anual`](./producao_anual.md) sai em toneladas e hectares: para comparar, multiplique a estimativa por 1.000. A chave distingue origens e meses LSPA que antes colidiam. Preserve a chave completa ao armazenar ou deduplicar observações.

A CONAB publica uma única área, rotulada "ÁREA (Em mil ha)", mantida em `area_plantada`. `area_colhida` fica nula na rota CONAB; área colhida distinta só existe na rota LSPA. Não há cópia nem imputação entre as duas áreas. O contrato CONAB V2 já permite essa nulidade.

No levantamento de trigo, o ano publicado é o ano de encerramento do biênio do contrato (`Safra 2026` → `2025/26`). Um levantamento que ainda não publica esse ano não fornece estimativa para a safra solicitada. A série histórica conserva seu período anual próprio.

## Normalização LSPA

O dataset usa área plantada, área colhida e produção do período selecionado. Agrega os componentes esperados do produto (duas safras de milho ou três de feijão), verifica variáveis, unidades e geografia e rejeita componentes ausentes ou duplicatas.

Hectares e toneladas são convertidos para mil ha e mil toneladas. Um valor ausente propaga nulidade ao agregado correspondente, sem virar zero. A produtividade é recalculada pela produção total dividida pela área colhida total, em kg/ha. Área colhida zero resulta em produtividade nula. Rendimentos publicados por componente não são somados nem promediados sem ponderação.

O [contrato da fonte LSPA](./lspa.md) permanece em 2.0 e retorna a tabela longa com ano, mês, variável e unidade originais.

## Exemplos

```python
from agrobr import datasets

# Preservar seleção automática de fonte
atual = await datasets.estimativa_safra("soja", uf="MT")

# Selecionar edições CONAB
primeiro = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", levantamento=1
)
decimo_primeiro = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", levantamento=11
)

# Selecionar dois meses LSPA do mesmo ano civil
janeiro, meta = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT",
    fonte="ibge_lspa", mes="01", return_meta=True,
)
dezembro = await datasets.estimativa_safra(
    "soja", safra="2024/25", uf="MT", mes=12
)
print(janeiro[["ano_lspa", "mes_lspa", "producao"]])
print(meta.selected_source)
```

As capturas oficiais LSPA de 2025 contêm produção de soja em Mato Grosso de **45.586,022 mil toneladas em janeiro** e **50.175,032 mil toneladas em dezembro**. São estimativas sucessivas da mesma safra no ano civil, não fluxos de produção para somar entre meses. [SIDRA janeiro](https://apisidra.ibge.gov.br/values/t/6588/n3/51/h/n/p/202501/c48/39443), [SIDRA dezembro](https://apisidra.ibge.gov.br/values/t/6588/n3/51/h/n/p/202512/c48/39443).

## Migração e schema JSON

A versão 3.0 mantém as dez colunas anteriores e adiciona duas colunas LSPA nullable, mas altera a chave primária: a mudança é **major**, não apenas uma versão minor aditiva. Atualize chaves persistidas e verificações de schema. A 3.1 acrescenta `unidade_producao` e `unidade_area`, opcionais no contrato (minor). Não use `CONAB_SAFRA_V2` para validar este dataset; ele continua sendo o contrato da fonte.

```python
from agrobr.contracts import get_contract

contract = get_contract("estimativa_safra")
print(contract.version)  # "3.1"
print(contract.primary_key)
```

JSON: `agrobr/schemas/estimativa_safra.json`. Consulte o [guia de migração](../guides/migracao-2.md) e a [política SemVer](./semver.md).
