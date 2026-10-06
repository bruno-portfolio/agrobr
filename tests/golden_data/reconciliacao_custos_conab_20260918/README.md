# Reconciliação CONAB custos

Manifesto v2 independente de custos agrícolas e da sociobiodiversidade.
`manifest.json` contém células Excel A1, valores crus, formatos numéricos,
contextos publicados, extremos físicos, decisões por linha/coluna e amostras
da API de fonte e dos dois datasets. Os números físicos de linha completam
`fixed_key` (planilha/aba); cada caso seleciona uma única aba.

Os esperados vêm de xlrd/openpyxl, sem chamar parser, contrato ou normalizador
do agrobr. A geração e a reconferência estão em
{oracle_io,oracle_costs,prepare_cases,build_manifest,verify_manifest}.py`.
O manifesto usa JSON compacto para permanecer abaixo de 2.000.000 bytes.

`receipts.json` conserva URL, data, hash, tamanho e cabeçalhos das respostas
HTTP completas. Doze workbooks já existentes são reutilizados por caminho
relativo, sem cópia ou alteração. Os cinco novos são:

| Arquivo | Evidência necessária |
|---|---|
| `feb4999ec69b7274.xls` | Café arábica, safra anual, cabeçalhos mesclados e referência textual |
| `aceab784e7d4a6f2.xls` | Café conilon identificado pelo recurso oficial |
| `a6cdcd1877333e68.xlsx` | Família agrícola OOXML publicada, algodão 2026 |
| `b9b97fe929cb851f.xls` | Trigo por tonelada e recusa de terceira medida monetária |
| `122e85180138ccc3.xls` | Milho e nota cambial numérica/vazia fora dos itens de custo |

Outros 17 workbooks novos ficam fora do repositório, com recibos e replay público
reproduzível; os testes do repositório não dependem desses arquivos. O relatório
cobre os 34 corpos e os 88 recursos catalogados. Dezesseis revisões sociobio
arquivadas têm registro nominal, sem certificação N2 de seus bytes.

Não foi encontrado recurso agrícola XLSX de café nem layout sociobio com uma
única coluna monetária nas capturas. Esses casos não são fabricados. Três
medidas monetárias, erro Excel, unidade não reconhecida e contexto ausente
continuam como recusas. N3 não se aplica a revisões ou métodos locais de custos.

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_conab/test_custo_producao/test_reconciliacao.py tests/test_conab/test_custo_producao/test_reconciliacao_manifest.py
```
