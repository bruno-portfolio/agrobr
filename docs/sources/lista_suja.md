# Lista Suja — Cadastro de Empregadores do MTE

A tabela da fonte também está disponível em [`datasets.empregadores_lista_suja`](../api/empregadores_lista_suja.md), que reutiliza o contrato 2.0 e conserva a proveniência das rotas CSV/PDF. O cadastro é nacional, sem seleção automática de atividade agropecuária. Os exemplos abaixo chamam a API de fonte; guardas da assinatura e encapsulamento de erros do wrapper estão descritos na página do dataset.

## Acesso

A API consulta o cadastro principal publicado pelo Ministério do Trabalho e Emprego na [página oficial](https://www.gov.br/trabalho-e-emprego/pt-br/assuntos/inspecao-do-trabalho/areas-de-atuacao/combate-ao-trabalho-escravo-e-analogo-ao-de-escravo), sem autenticação. A descoberta distingue esse cadastro do Cadastro de Empregadores em Ajustamento de Conduta (CEAC), que é outra coleção.

O padrão `formato="auto"` usa CSV e confere o TXT companheiro para obter o contexto da publicação. Esse caminho funciona com a instalação core. PDF é alternativa quando o CSV não está anunciado ou ocorre falha de transporte elegível. Uma resposta bem-sucedida com arquivo inválido gera `ParseError`. `formato="csv"` e `formato="pdf"` são escolhas exclusivas.

Somente o PDF requer `pip install agrobr[pdf]`. Polars requer `pip install agrobr[polars]`.

## Uso

```python
from agrobr import lista_suja

df, meta = await lista_suja.empregadores(return_meta=True)
para = await lista_suja.empregadores(uf="PA", formato="csv")
registro = await lista_suja.empregadores(id_registro="41")
pdf = await lista_suja.empregadores(formato="pdf")
```

Para uso síncrono:

```python
from agrobr.sync import lista_suja

df = lista_suja.empregadores(uf="PA")
```

| Argumento | Padrão | Comportamento |
|---|---|---|
| `uf` | `None` | Sigla de UF, normalizada; filtro local após validar o arquivo completo |
| `id_registro` | `None` | Texto exato do ID nessa exportação; não recebe conversão numérica |
| `formato` | `"auto"` | `"auto"`, `"csv"` ou `"pdf"` |
| `as_polars` | `False` | Converte após validar o contrato |
| `return_meta` | `False` | Retorna `(df, MetaInfo)` |

Filtros podem ser combinados. Ausência de correspondência retorna um quadro vazio com os mesmos tipos. Tipos inválidos, UF inválida, filtro textual vazio e argumentos desconhecidos geram `InvalidParameterError` antes de aviso ou rede. O arquivo completo é baixado a cada chamada, sem paginação ou cache persistente.

## Contrato 2.0

Disponível como `get_contract("lista_suja_empregadores")` e `LISTA_SUJA_EMPREGADORES_V2`, de `agrobr.contracts.lista_suja`. As doze colunas são estáveis.

| Coluna | Tipo pandas | Anulável | Conteúdo |
|---|---|---|---|
| `empregador` | texto | Não | Nome publicado |
| `cpf_cnpj` | texto | Não | Documento com pontuação e zeros preservados |
| `estabelecimento` | texto | Sim | Estabelecimento publicado |
| `uf` | texto | Sim | UF informada |
| `cnae` | texto | Sim | Código com zeros preservados |
| `data_inclusao` | `datetime64[ns]` | Sim | Inclusão quando a célula contém uma única data |
| `trabalhadores_resgatados` | `Int64` | Sim | Campo oficial “Trabalhadores envolvidos”, com nome legado |
| `ano_acao_fiscal` | `Int64` | Sim | Ano informado |
| `id_registro` | texto | Não | ID da linha na exportação |
| `data_decisao` | `datetime64[ns]` | Sim | Data de decisão administrativa informada |
| `data_atualizacao` | `datetime64[ns]` | Sim | Atualização do cadastro comprovada no corpo da publicação |
| `data_inclusao_texto` | texto | Não | Célula original, incluindo intervalos e múltiplas datas |

A chave `[id_registro]` vale dentro de uma exportação identificada por `meta.raw_content_hash`. Documentos repetidos não são deduplicados. Não se declara um identificador permanente de empregador.

Há células como `05/04/2024 a 10/05/2024, 09/04/2025`. Nesses casos, `data_inclusao` fica `NaT`, o texto é preservado e `source_details.compound_inclusion_ids` identifica os registros. O parser não escolhe primeira ou última data nem infere o motivo jurídico do intervalo. Datas e números presentes inválidos geram erro.

Ausências textuais são nulas, nunca preenchidas com outros campos. Contagens e anos usam `Int64`, inclusive em resultados vazios. Essas mudanças de tipos e sentinelas justificam o contrato major **2.0**; veja a [migração](../guides/migracao-2.md).

## Publicação e proveniência

`meta.source_details.publication` distingue `periodic_update` de `registry_updated_at`. Na captura de 06/09/2026, eram 06/04/2026 e 04/09/2026, respectivamente. A segunda alimenta `data_atualizacao`; a data da página, o relógio da consulta e `Last-Modified` não a substituem.

O CSV não declara a edição. Para usar a data do TXT, o parser compara os dez campos originais de todas as linhas por ID. TXT divergente ou inválido gera erro. TXT ausente ou indisponível deixa a data nula e registra o diagnóstico. O PDF fornece o contexto no próprio corpo. Datas da publicação são civis; instantes de aquisição têm fuso UTC.

Os metadados incluem:

- Rotas realmente tentadas e selecionada, formato e causa de fallback.
- URL pedida/final, hash, tamanho, horário e cabeçalhos do arquivo, da página e do TXT recebido.
- Título, texto de edição e notas da publicação, sem atribuir causa a campos ausentes.
- Contagens original e final, nulos, assinatura do layout e filtros aplicados.

Um erro na descoberta não autoriza reutilizar um endereço antigo. Fallback automático considera HTTP 403, 404, 408, 410, 429, 5xx ou falhas de transporte, aplicando a política de retry quando elegível; outros erros propagam.

## Escopo e licença

A implementação entrega a exportação corrente. CEAC, seleção por edição histórica e armazenamento de revisões não fazem parte dessa API. Hash e data documentam a coleta, mas não constituem um histórico.

O dataset semântico recusa `deterministic` antes de I/O e retorna `snapshot=None`; nem o arquivo corrente nem seu hash reconstituem outra edição.

A classificação existente é `livre`. O rodapé oficial informa CC BY-ND 3.0; não foi localizada uma licença separada dos arquivos. Acesso público e classificação interna não comprovam permissão irrestrita de reutilização: veja a [verificação da licença](../licenses.md#lista-suja). O aviso existente sobre CPF/CNPJ é emitido na primeira chamada.

## Edição de 06/04/2026 e formatos

Em 18/09/2026, os arquivos CSV, TXT e PDF eram os da edição periódica de 06/04/2026, com o cadastro atualizado em 04/09/2026; a data da aquisição não indica uma nova edição. São 579 registros, 567 documentos distintos e 4.706 trabalhadores no campo publicado, com dez inclusões compostas. Os nulos e documentos repetidos são preservados.

CSV/TXT e PDF são representações da mesma publicação, e cada formato conserva seu texto: duas quebras de linha após hífen no estabelecimento permanecem diferentes entre CSV e PDF, sem reparo implícito.

Nos campos textuais derivados, espaços internos são colapsados. Isso afeta seis células de `estabelecimento` no CSV dessa edição: IDs 170, 180, 356, 368, 410 e 525. `data_inclusao_texto` via PDF preserva as quebras de linha da célula. Entre CSV e PDF, as doze colunas finais diferem literalmente em 12 células: dez textos de inclusão composta e os dois estabelecimentos citados. Nos dez campos de origem, depois de normalizar os espaços, restam duas diferenças.
