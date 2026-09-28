# Séries econômicas — SGS

`datasets.series_economicas` consulta uma série do Sistema Gerenciador de Séries Temporais do Banco Central e aplica o contrato [`bcb_sgs` 2.1](../contracts/series_economicas.md). Reutiliza a [API de fonte `bcb.sgs`](bcb.md), sem combinar séries, converter unidades ou inferir frequência pelas datas observadas.

```python
from agrobr import datasets

historico, meta = await datasets.series_economicas(
    1, data_inicial="01/01/2010", data_final="31/12/2024", return_meta=True,
)
ipca = await datasets.series_economicas(
    "ipca", data_inicial="01/01/2024", data_final="31/12/2024",
)
```

## Parâmetros

```python
async def series_economicas(
    codigo: int | str,
    *,
    data_inicial: str | None = None,
    data_final: str | None = None,
    ultimos: int | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
): ...
```

`codigo` aceita inteiro positivo representável em int64 ou um alias exato já oferecido por `bcb.sgs`, como `"ipca"`. Uma string numérica (`"1"`) não substitui o inteiro `1`. Não há strip, normalização de caixa ou conversão de código no wrapper: a fonte recebe o valor original. Unidade, periodicidade e significado de cada código devem ser conferidos no [catálogo SGS](https://www3.bcb.gov.br/sgspub/).

| Seleção | Comportamento da fonte |
|---|---|
| Duas datas | Intervalo `DD/MM/AAAA`, inclusivo na solicitação |
| Apenas início | Final efetivo é a data corrente UTC |
| Apenas final | Início delegado ao servidor; um limite remoto pode impedir uma consulta extensa |
| Sem datas nem `ultimos` | Início padrão dez anos antes da data corrente UTC; final corrente |
| `ultimos` sem datas | Endpoint remoto de últimas observações |
| Datas e `ultimos` | Aquisição de todos os blocos do período, reconciliação, ordenação e recorte final local |

`ultimos` exige inteiro positivo. O limite remoto observado de vinte valores na série 1 não é aplicado como teto universal pelo SDK; uma rejeição remota documentada gera `InvalidParameterError`. Com datas, `ultimos=1300` é um recorte local legítimo e não evita baixar os blocos anteriores.

Flags exigem booleanos estritos. Código, calendário, intervalo invertido e seletores inválidos são rejeitados antes de I/O. Argumentos desconhecidos, incluindo `produto`, `use_cache` e `fonte`, geram `TypeError`. Somente `codigo` pode ser posicional. O dataset não introduz cache nem opções diferentes das oferecidas por SGS.

## Histórico, ausências e erros

Períodos longos são divididos pela fonte em blocos de até dez anos, encerrados em fronteiras anuais. Uma falha de transporte, parsing ou contrato em qualquer bloco interrompe a consulta; o dataset não entrega o primeiro bloco como resultado completo nem oculta corrupção por meio de `ultimos`.

As datas são referências publicadas. Uma série mensal pode retornar a referência `01/01/2024` ao consultar a partir de `02/01/2024`; o SDK preserva essa referência e emite diagnóstico, inclusive sem metadados. Sábados, valores negativos e nulos JSON explícitos não são descartados. Não há preenchimento diário, interpolação ou substituição de ausência por zero. Séries que publicam `dataFim` (ex.: TR) ganham a coluna opcional `data_fim` com o fim do período.

Duplicata dentro de um corpo é erro. Referências repetidas entre blocos somente são reconciliadas quando o valor coincide, preservando as origens; conflito falha. `[]` válido ou o envelope específico de ausência de valores pode produzir o esquema vazio tipado. O HTTP 404 reconhecido emite aviso e não comprova que o código exista.

A base do dataset encapsula indisponibilidade, erro de layout e violação de contrato da fonte em `SourceUnavailableError`, com classificação em `errors`. Parâmetros inválidos mantêm `InvalidParameterError`. Existe somente a fonte `bcb_sgs`, sem fallback. O contrato é validado também sem `return_meta` e no resultado vazio.

## Metadados e reprodução

`meta.source="datasets.series_economicas/bcb_sgs"`, `dataset="series_economicas"`, `source_method="dataset"` e `selected_source="bcb_sgs"`. A versão do contrato e do schema é 2.1. `fetched_at` preserva a coleta da fonte em UTC com fuso; `fetch_timestamp` é o mesmo instante.

`source_details` mantém a consulta efetiva, defaults, recursos por bloco, reconciliação, diagnósticos e cobertura. `coverage.completeness="unknown"` expressa ausência de uma contagem global comprovada; todos os blocos solicitados terem sido obtidos não prova completude histórica da série. Os detalhes e listas são copiados independentemente.

`raw_content_hash` é o SHA-256 do manifesto canônico de `query` e `resources`, e `raw_content_size` é o tamanho desse manifesto. Os hashes/tamanhos de cada corpo ficam nos recursos; `source_details.resource_bytes` soma bytes HTTP. Durações de aquisição/parsing são preservadas. A camada não transforma esse hash em uma revisão congelada.

Qualquer contexto `datasets.deterministic(...)` é recusado antes de I/O, mesmo com datas explícitas: selecionar referências históricas não seleciona a revisão da série disponível em outra data. `snapshot=None`. `update_frequency="varies_by_series"`, unidade e data mínima não fixadas descrevem esse escopo heterogêneo.

```python
from agrobr import sync

ultimas = sync.datasets.series_economicas(1, ultimos=3)
```

`as_polars=True` converte após a validação pandas e exige apenas o extra Polars. O core continua independente dos extras. Consulte também as [licenças](../licenses.md).
