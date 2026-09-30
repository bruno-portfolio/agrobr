# BCB SGS — contrato 3.0

Contrato da API de fonte `bcb.sgs()`, registrado como `bcb_sgs` e reutilizado pelo dataset [`series_economicas`](series_economicas.md). A constante é `agrobr.contracts.bcb_sgs.BCB_SGS_V3`; o schema exportado fica em `agrobr/schemas/bcb_sgs.json`.

| Coluna | Tipo pandas | Nulo | Significado |
|--------|-------------|------|-------------|
| `data` | datetime64[ns], sem fuso | Não | Referência civil publicada pela fonte, sem horário |
| `valor` | float64 | Sim | Medida na unidade própria da série; finita, pode ser negativa |
| `codigo` | Int64 | Não | Código SGS selecionado, inteiro positivo |
| `nome_serie` | texto | Sim | Alias do agrobr quando conhecido; nulo para outros códigos |
| `data_fim` | datetime64[ns], sem fuso | Sim | Opcional: fim do período publicado em `dataFim` (ex.: TR, código 226); só existe quando a fonte publica o campo |

A chave é `codigo, data` e a saída é ordenada nessa ordem, inclusive após `ultimos`. As quatro colunas estáveis e os tipos numéricos/temporais permanecem no vazio; `data_fim` vem depois delas quando alguma linha traz `dataFim`, e os demais campos do corpo geram aviso. O quadro vazio do contrato (`empty_frame()`, usado pelo dataset) também declara `data_fim`. Nulo explícito da fonte preserva ausência; texto malformado, infinito e perda total de magnitude na conversão para float64 geram erro. Datas devem caber no domínio datetime64[ns].

`data` não é a data de publicação ou coleta. A frequência e a unidade não constam do corpo de observações; o contrato não as infere pelo alias ou pela distância entre referências. Referências fora dos limites diários solicitados são preservadas com aviso. Duplicatas dentro do mesmo corpo são inválidas; entre blocos, valores iguais podem ser reconciliados conservando todas as origens. Conflitos interrompem a aquisição.

A ausência declarada pelo envelope oficial HTTP404 e uma lista JSON vazia produzem quadro vazio tipado. O 404 não comprova a existência do código, e um erro de transporte ou layout não é vazio. A consulta só retorna quando todos os blocos planejados são obtidos, mas `coverage.completeness="unknown"` registra a ausência de contagem global independente.

`MetaInfo` usa schema/contrato 3.0 e parser 2. A seleção, cada resposta final, SHA256, status, parâmetros, coleta UTC e reconciliação estão em `source_details`. O hash/tamanho superiores representam um manifesto canônico de query e recursos; corpos têm hashes próprios. Índices de recursos e linhas nas origens começam em zero. Os extremos de cobertura antecedem o corte `ultimos`; a contagem retornada descreve a saída final. Revisões não são congeladas.

```python
from agrobr import bcb, contracts

df, meta = await bcb.sgs(
    "ipca", inicio="01/01/2024", fim="31/12/2024", return_meta=True,
)
contracts.validate_dataset(df, "bcb_sgs")
```

Veja a [API](../api/bcb.md#sgs) e as [mudanças de migração](../guides/migracao-2.md).

A versão 3.0 troca `codigo` de `int64` para `Int64`. A constante `BCB_SGS_V2` preserva o contrato 2.1 para validação histórica. Texto segue o padrão do pandas instalado; a mudança de dtype pode afetar comparações com nomes de tipos.
