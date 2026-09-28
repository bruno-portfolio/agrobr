# Contrato — series_economicas

O dataset [`series_economicas`](../api/series_economicas.md) reutiliza o contrato registrado como **`bcb_sgs`, versão 2.1**, sem alias próprio no registry de contratos. A fonte única é `bcb.sgs`; não há normalização de unidade, frequência ou valor na camada de datasets.

| Coluna | dtype pandas | Nula | Semântica |
|---|---|---|---|
| `data` | `datetime64[ns]` | Não | Referência civil publicada, sem horário/fuso |
| `valor` | `float64` | Sim | Valor finito na unidade da série; negativos são permitidos |
| `codigo` | `int64` | Não | Código positivo, até `2**63 - 1` |
| `nome_serie` | texto | Sim | Alias conhecido pelo SDK; não é título obtido do catálogo |
| `data_fim` | `datetime64[ns]` | Sim | Opcional: fim do período publicado em `dataFim` (ex.: TR); só existe quando a fonte publica o campo |

São quatro colunas estáveis, nessa ordem, seguidas de `data_fim` quando a série publica `dataFim`. Chave primária: **`codigo, data`**. As linhas são ordenadas por código e referência; `ultimos`, quando local, é aplicado depois da reconciliação. Um código numérico sem alias conhecido conserva `nome_serie` nulo, sem fabricar um nome. Texto não nulo nessa coluna deve ser não vazio.

Nulos explícitos em valores permanecem ausências, sem zero ou interpolação. `NaN`/infinito publicados como texto inválido não são confundidos com null JSON. Duplicatas no mesmo corpo e conflitos entre blocos falham; apenas coincidências entre blocos são reconciliadas, com origens nos metadados.

Datas mensais/trimestrais podem anteceder o início diário pedido. O SDK mantém a referência e seu diagnóstico; não presume que ela represente uma observação diária. Sábado não é um erro de calendário. Unidade e periodicidade dependem do código e devem ser consultadas no catálogo SGS.

O contrato é obrigatório mesmo sem metadados, antes de converter para Polars e em resultados vazios. Vazios usam o quadro vazio do contrato: as quatro colunas com os dtypes acima e `data_fim`. Um envelope de ausência de valores não prova existência da série; erro de layout não é tratado como vazio legítimo.

```python
from agrobr import contracts, datasets

df = await datasets.series_economicas("ipca", data_inicial="01/01/2024", data_final="31/12/2024")
contracts.validate_dataset(df, "bcb_sgs")
```

`meta.contract_version` e `schema_version` são 2.1. Hash/tamanho brutos identificam um manifesto de consulta e recursos, não os bytes de um único corpo; estes estão em `source_details.resources`. Cobertura desconhecida não é promovida a completude histórica. O período não representa revisão as-of; `deterministic` é recusado antes de I/O.
