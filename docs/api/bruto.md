# Coleta bruta (`agrobr.bruto`)

Guarda em disco o arquivo ou as páginas originais da fonte, como ela publica, e registra cada aquisição numa linha
do `manifesto.jsonl`. Não devolve DataFrame nem normaliza nada: é para quem precisa do dado original, com hash,
cabeçalhos e cobertura conferida. O formato do manifesto, o layout em disco, os status e a retomada estão no
[contrato da coleta bruta](../contracts/bruto.md).

## coletar

```python
from agrobr import bruto

coleta = await bruto.coletar("ibge", "malha_municipal", uf="AL", destino="dados/bruto")
coleta.entrada.status          # "ok"
coleta.manifesto               # Path de dados/bruto/manifesto.jsonl
```

Versão síncrona: `agrobr.sync.bruto.coletar(...)`, com os mesmos argumentos.

### Parâmetros

| Parâmetro | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| fonte | str | Sim | Fonte da tabela de recursos (`ana`, `cnuc`, `funai`, `ibama`, `ibge`, `incra`, `acervo_fundiario`, `sfb`, `sicar`) |
| recurso | str | Sim | Recurso da fonte, exatamente como na tabela |
| destino | str \| PathLike | Sim | Pasta da coleta; recebe o `manifesto.jsonl` e os arquivos |
| nome | str | Não | Identifica a seleção no manifesto; padrão: a UF, ou `brasil` sem recorte. Obrigatório com bbox |
| uf | str | Depende | Sigla da UF; obrigatória, opcional ou recusada conforme o recurso |
| bbox | tuple[float, float, float, float] | Não | `(minx, miny, maxx, maxy)` em longitude/latitude; recusada nos recursos de arquivo (ZIP e CSV) |
| bbox_crs | `"EPSG:4674"` \| `"EPSG:4326"` | Não | CRS da bbox; padrão `EPSG:4674` |
| tamanho_pagina | int | Não | Feições por página nos recursos paginados, de 1 a 1.000; padrão 100 (20 em `funai`/`terras_indigenas`); `None` nos recursos de arquivo e em `incra`/`quilombolas` (página única) |
| compactar | bool | Não | Gzip local das páginas e controles; padrão `True`; o arquivo (ZIP ou CSV) é guardado como veio |
| retomar | bool | Não | Reaproveita a entrada `ok` da mesma consulta depois de conferir os hashes; padrão `False` |
| limites | `bruto.LimitesBrutos` | Não | Orçamento da chamada; padrão `LimitesBrutos()` |

### Recursos

| Fonte | Recurso | Seleção | Formato / chave da cobertura |
|---|---|---|---|
| `ana` | `massas_dagua` | UF e/ou bbox obrigatórios | `esri_json` / `FID` |
| `cnuc` | `ucs` | UF, bbox, ambos ou Brasil; sempre `limite=uc` | `gml` / `cd_cnuc` |
| `ibge` | `malha_municipal` | UF, bbox, ambos ou Brasil | `geojson` / `cd_mun` |
| `ibge` | `areas_urbanizadas` | bbox ou Brasil; UF recusada | `geojson` / `fid` |
| `funai` | `terras_indigenas`, `terras_indigenas_pontos` | Brasil; UF e bbox recusadas; polígonos com 20 por página no padrão | `geojson` / `gid` |
| `incra` | `quilombolas` | Brasil; UF, bbox e `tamanho_pagina` recusados; página única, sem campo ordenável | `geojson` / `feature.id` |
| `acervo_fundiario` | `sigef_publico`, `sigef_privado` | UF obrigatória | `zip` |
| `acervo_fundiario` | `snci_publico`, `snci_privado`, `snci_brasil` | UF obrigatória | `zip` |
| `acervo_fundiario` | `assentamentos` | Brasil; UF e bbox recusadas; o ZIP nacional do INCRA | `zip` |
| `sfb` | `cnfp` | Brasil; UF e bbox recusadas; o Brasil inteiro pede `tamanho_pagina=20` e `max_bytes_pagina` de 24 MiB | `esri_json` / `fid` |
| `sicar` | `imoveis` | UF obrigatória; bbox opcional | `geojson` / `feature.id` |
| `cnuc` | `cadastro` | Brasil; UF e bbox recusadas; edição `202607` conferida no catálogo do MMA | `csv` |
| `ibama` | `termos_embargo` | Brasil; UF e bbox recusadas; traz dado pessoal (nome e CPF/CNPJ) | `csv` |
| `ibge` | `malha_municipal_zip`, `areas_urbanizadas_zip` | Brasil; UF e bbox recusadas | `zip` |

### Retorno

`bruto.ColetaBruta`, com `manifesto` (caminho absoluto do `manifesto.jsonl`), `entrada` (`bruto.RecursoBruto`, a linha
do manifesto) e `reutilizado` (`True` só quando `retomar=True` reaproveitou uma entrada `ok` sem ir à rede).

### Limites (`bruto.LimitesBrutos`)

| Campo | Padrão | Aplicação |
|---|---|---|
| max_bytes_recurso | 4 GiB | Bytes do recurso inteiro; o arquivo tem teto de 4 GiB |
| max_bytes_pagina | 8 MiB | Cada página ou controle |
| max_paginas | 10.000 | Páginas por coleta |
| max_ids | 500.000 | IDs retidos para a conferência |
| max_bytes_ids | 64 MiB | Memória das estruturas de IDs |
| max_segundos | 3.600 | Prazo da chamada, incluindo esperas de retry e do rate limiter |

### Erros

| Situação | Resultado |
|---|---|
| Argumento, seleção, destino ou nome inválidos | `InvalidParameterError`, antes da rede e sem mudar o manifesto |
| 404 no arquivo (ZIP ou CSV) | Retorna com `status="ausente_na_fonte"`; a próxima retomada tenta de novo |
| Falha HTTP ou de rede depois das tentativas | `SourceUnavailableError`, com a entrada `erro` no manifesto |
| Envelope inválido, erro OGC em HTTP 200, arquivo 200 fora do formato ou cobertura divergente | `ParseError`, com a entrada `erro` |
| Limite excedido | `ResourceLimitError` |
| Manifesto existente inconsistente | `ContractViolationError`; o manifesto anterior fica intacto |

A coleta não tem retorno parcial de sucesso: nos recursos paginados, `ok` exige a cobertura inteira conferida
(contagens antes e depois, IDs, CRS e a ordem da paginação); nos de arquivo, o corpo inteiro recebido e o início
conferido (assinatura do ZIP ou cabeçalho do CSV). Veja o [contrato](../contracts/bruto.md) para a regra de cada recurso e o
exemplo completo em `examples/bruto.py`.
