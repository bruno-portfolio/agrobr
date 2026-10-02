# unidades_conservacao v1.0

Unidades de conservação federais, estaduais e municipais, com RPPNs, do CNUC

Fonte: **CNUC/MMA**. Registro do contrato: `unidades_conservacao`.

## Schema

| Coluna | Tipo | Nulo | Unidade | Descrição |
|---|---|---|---|---|
| `codigo` | str | Não | — | Código CNUC publicado |
| `nome` | str | Não | — | — |
| `esfera` | str | Não | — | `federal`, `estadual` ou `municipal` |
| `categoria` | str | Não | — | Categoria de manejo do SNUC, como publicada (12 valores) |
| `grupo` | str | Não | — | `PI` (proteção integral) ou `US` (uso sustentável) |
| `categoria_iucn` | str | Sim | — | Categoria IUCN publicada (`Category Ia` a `Category VI`) |
| `uf` | str | Não | — | Siglas em ordem alfabética, separadas por `/` |
| `municipios` | str | Não | — | Lista publicada, no formato `NOME (UF), …`; a fonte corta textos longos com `...` |
| `bioma` | str | Sim | — | Biomas com área publicada na UC, separados por `/` |
| `area_ha` | float | Sim | ha | Área da UC inteira, não a parte dentro da UF |
| `data_criacao` | date | Sim | — | Data de criação |
| `ato_criacao` | str | Sim | — | — |
| `orgao_gestor` | str | Sim | — | — |
| `qualidade_poligono` | str | Sim | — | Se o polígono segue o memorial descritivo, é estimativa ou é esquemático |
| `wdpa_id` | str | Sim | — | Identificador na base mundial de áreas protegidas (WDPA) |

**Chave primária:** `codigo`

Todas as colunas estáveis devem existir, inclusive anuláveis e em resultados vazios. Mudanças incompatíveis exigem versão major do contrato.

## Semântica e proveniência

Retorno tabular sem geometria; a geometria está em `cnuc.ucs_geo`. A camada só tem as UCs com limite cadastrado no CNUC: as UCs sem polígono e as zonas de amortecimento não entram. `uf` pode ter mais de uma sigla, e o filtro `uf=` casa a sigla dentro da lista. O filtro `municipio=` casa o nome inteiro de cada município publicado com o cadastro do IBGE, nunca por pedaço; listas cortadas pela fonte e grafias que não casam com o IBGE vão para `MetaInfo.validation_warnings` e para um `UserWarning`. `bbox` usa longitude/latitude (oeste, sul, leste, norte).

`bioma` é derivado: são os biomas (Amazônia, Caatinga, Cerrado, Mata Atlântica, Pampa e Pantanal) que têm área maior que zero nas colunas de área por bioma da camada. A área marinha fica de fora, e a UC sem área por bioma publicada sai com `bioma` nulo.

`return_meta=True` retorna dados e `MetaInfo`, com fontes tentadas/selecionada, aquisição, versão contratual e diagnósticos da fonte (contagem do serviço antes do download, conciliada com o retorno). A camada é corrente e não permite selecionar uma revisão histórica via `deterministic`.

## Parâmetros

| Parâmetro | Tipo | Padrão |
|---|---|---|
| `uf` | `str \| None` | `None` |
| `municipio` | `str \| int \| None` | `None` |
| `esfera` | `str \| None` | `None` |
| `categoria` | `str \| None` | `None` |
| `grupo` | `str \| None` | `None` |
| `bioma` | `str \| None` | `None` |
| `bbox` | `tuple[float, float, float, float] \| None` | `None` |
| `max_registros` | `int \| None` | `None` |
| `as_polars` | `bool` | `False` |
| `return_meta` | `bool` | `False` |

Seleção acima de 10.000 UCs levanta `ResourceLimitError` antes do download. `max_registros` devolve as primeiras UCs em ordem de `codigo`.

## Exemplo

```python
from agrobr import contracts, datasets

df, meta = await datasets.unidades_conservacao(uf="SE", return_meta=True)
contracts.validate_dataset(df, "unidades_conservacao")
```

Para chamadas síncronas, use `from agrobr.sync import datasets` e remova `await`. `as_polars=True` requer `pip install agrobr[polars]`.

## Schema JSON e licença

`agrobr/schemas/unidades_conservacao.json` · `get_contract("unidades_conservacao")`.

`livre` — consulte as [licenças](../licenses.md) e os [detalhes da fonte](../sources/cnuc.md).

## Conteúdo da camada

Em 01/10/2026, a camada tinha 3.450 UCs com limite: 1.086 federais, 1.458 estaduais e 906 municipais, com 1.426 RPPNs (736 federais, 669 estaduais e 21 municipais). O código CNUC não se repetiu, e 65 UCs não tinham área por bioma publicada. O cadastro CSV do CNUC de julho de 2026 tem 3.576 UCs: a diferença são, sobretudo, UCs sem polígono no CNUC.

`unidades_conservacao_federais` continua com a camada do ICMBio (só UCs federais, sem RPPN) e não muda.
