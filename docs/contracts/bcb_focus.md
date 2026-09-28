# BCB Focus — contrato 2.0

Contrato da API de fonte `bcb.focus()`, registrado como `bcb_focus`. A constante é `agrobr.contracts.bcb_focus.BCB_FOCUS_V2`; o schema exportado fica em `agrobr/schemas/bcb_focus.json`. O dataset `expectativas_mercado` reutiliza este contrato de fonte.

| Coluna | Tipo pandas | Nulo | Significado |
|--------|-------------|------|-------------|
| `indicador` | texto | Não | Nome exato publicado pelo BCB |
| `data` | datetime64[ns], sem fuso | Não | Data civil da pesquisa, sem horário |
| `data_referencia` | texto | Não | Horizonte anual YYYY ou mensal MM/YYYY |
| `media`, `mediana`, `desvio_padrao`, `minimo`, `maximo` | float64 | Sim | Estatísticas finitas publicadas, sem arredondamento adicional |
| `numero_respondentes` | Int64 | Sim | Inteiro publicado entre 0 e 2³¹−1 |
| `base_calculo` | Int64 | Sim | Código publicado entre 0 e 2³¹−1, sem remapeamento |
| `periodicidade` | texto | Não | `anual` ou `mensal`, conforme a entidade selecionada |
| `indicador_detalhe` | texto | Sim | Dimensão anual publicada; nulo na entidade mensal |

As doze colunas e seus tipos numéricos/temporais permanecem no vazio. A chave é `periodicidade, indicador, indicador_detalhe, data, data_referencia, base_calculo`. Nulo na chave identifica uma dimensão ausente; detalhe vazio é distinto de nulo. Exportações, Importações e Saldo da Balança comercial não podem ser fundidos, nem bases diferentes do mesmo indicador.

`data` é a data da pesquisa, distinta da coleta UTC. `data_referencia` é um horizonte de projeção e pode ser futuro; não é o dia de uma observação realizada. O formato precisa ter ano e mês válidos. A saída conserva a ordem adquirida: pesquisa decrescente, depois referência textual e base crescentes; no anual, detalhe também participa do desempate. MM/YYYY não está em ordem cronológica de horizontes.

Os campos externos obrigatórios precisam estar presentes, mesmo quando aceitam null. Ausências explícitas não viram zero. Estatísticas exigem números JSON; contagem/base exigem inteiros JSON, sem bool ou coerção de texto/fração. Valores não finitos, JSON ambíguo e datas fora do domínio datetime64[ns] geram erro. Valores negativos finitos são preservados. Desvio negativo, extremos invertidos e média/mediana fora dos extremos disponíveis geram diagnóstico com origem, conservando os valores publicados. O contrato não certifica a consistência científica da pesquisa.

Cada página é validada integralmente antes de aplicar `max_registros`. Duplicatas, inclusive idênticas ou com dimensões nulas, alterações de seleção e falhas de página interrompem a aquisição. Resposta curta sem contagem independente continua pelo número efetivamente recebido até página vazia. `coverage.completeness` fica `unknown` sem total independente; descarte ou continuação pendente ao atingir o limite local permitem `partial`. `complete` exige contagem reconciliada, sem descarte nem continuação contraditória. Contagem e nextLink são validados quando presentes; as consultas conhecidas não os trouxeram. Não há snapshot de revisões entre páginas.

`MetaInfo` usa schema/contrato 2.0, parser 2 e fonte `bcb_focus`. `source_details` contém query, recursos, cobertura, avisos e tipos/nulos. Recursos preservam URLs, parâmetros, offset, quantidades recebidas/retidas, status, coleta UTC, SHA256 e bytes do corpo. O hash/tamanho superiores identificam um manifesto canônico UTF-8 de query e recursos; a soma dos corpos é informada separadamente. Avisos também são emitidos sem `return_meta=True`.

```python
from agrobr import bcb, contracts

df, meta = await bcb.focus(
    "IPCA", periodicidade="mensal", data_inicial="2026-08-28", return_meta=True,
)
contracts.validate_dataset(df, "bcb_focus")
```

Veja a [API](../api/bcb.md#focus), a [fonte e seus limites](../sources/bcb.md#focus-expectativas-de-mercado) e a [migração](../guides/migracao-2.md).
