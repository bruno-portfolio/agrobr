# unidades_conservacao_federais v1.0

Cadastro corrente de unidades de conservação federais da camada ICMBio/INDE

Fonte: **ICMBio**. Registro do contrato: `unidades_conservacao_federais`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `codigo` | str | Não | — | Código CNUC publicado |
| `nome` | str | Não | — | — |
| `categoria` | str | Não | — | — |
| `grupo` | str | Não | — | — |
| `uf` | str | Não | — | Pode conter várias UFs |
| `bioma` | str | Não | — | Pode conter vários biomas |
| `area_ha` | float | Sim | ha | Área da UC inteira, não a parte dentro da UF |
| `ano_criacao` | int | Sim | — | — |
| `ato_criacao` | str | Não | — | — |

**Chave primária:** Não definida; repetições da fonte são preservadas.

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

Retorno tabular sem geometria. UF e bioma podem conter mais de uma classificação publicada; códigos não recebem unicidade artificial. `bbox` usa longitude/latitude (oeste, sul, leste, norte). Cobertura, rota escolhida, recursos e hashes da fonte acompanham os metadados.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte. Estas publicações correntes não permitem selecionar uma revisão histórica via `deterministic`.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `uf` | `str \| None` | `None` |
| `grupo` | `str \| None` | `None` |
| `bioma` | `str \| None` | `None` |
| `bbox` | `tuple[float, float, float, float] \| None` | `None` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.unidades_conservacao_federais(uf="DF", return_meta=True)
contracts.validate_dataset(df, "unidades_conservacao_federais")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/unidades_conservacao_federais.json` · `get_contract("unidades_conservacao_federais")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da API/fonte](../sources/icmbio.md).

## Conteúdo da camada

Em 18/09/2026, a camada tinha 347 UCs na consulta sem filtros, 50 com `bioma="Cerrado"` e 22 com `uf="SP"`, seleções sobrepostas com 347 códigos CNUC distintos. O contrato não impõe chave primária nem remove eventuais repetições futuras.

`areahaalb` é preservado como `area_ha`, sem soma ou conversão de escala; `criacaoano` é atributo da UC, não edição da camada. Os textos de UF/bioma compostos permanecem integrais: 43 UCs da consulta sem filtros têm várias UFs. `area_ha` é a área da UC inteira, não a parte dentro da UF: o filtro `uf=` devolve a UC compartilhada com a área toda, e somar por UF conta essa UC mais de uma vez. Nessa data, nenhum campo vinha vazio; área e ano continuam anuláveis.

O WFS devolve 11 campos; `FID` e `ogc_fid` ficam fora da saída pública. O XSD da camada declara 22 propriedades, 13 delas fora do contrato tabular, inclusive a geometria. A concordância com `numberOfFeatures` consultado antes do CSV é registrada como `count_reconciled`, sem snapshot transacional.
