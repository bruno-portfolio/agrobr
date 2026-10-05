# Raw collection (`agrobr.bruto`)

Stores on disk the source's original file or pages, as published, and records each acquisition as one line of
`manifesto.jsonl`. It returns no DataFrame and normalizes nothing: it is for whoever needs the original data, with
hashes, headers and checked coverage. The manifest format, disk layout, statuses and resumption are in the
[raw collection contract](../contracts/bruto.md).

## coletar

```python
from agrobr import bruto

coleta = await bruto.coletar("ibge", "malha_municipal", uf="AL", destino="data/raw")
coleta.entrada.status          # "ok"
coleta.manifesto               # Path of data/raw/manifesto.jsonl
```

Synchronous version: `agrobr.sync.bruto.coletar(...)`, with the same arguments.

### Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| fonte | str | Yes | Source from the resource table (`ana`, `cnuc`, `funai`, `ibama`, `ibge`, `incra`, `acervo_fundiario`, `sfb`, `sicar`) |
| recurso | str | Yes | The source's resource, exactly as in the table |
| destino | str \| PathLike | Yes | Collection folder; receives `manifesto.jsonl` and the files |
| nome | str | No | Identifies the selection in the manifest; default: the state, or `brasil` with no cut. Required with bbox |
| uf | str | Depends | State abbreviation; required, optional or refused depending on the resource |
| bbox | tuple[float, float, float, float] | No | `(minx, miny, maxx, maxy)` in longitude/latitude; refused for file resources (ZIP and CSV) |
| bbox_crs | `"EPSG:4674"` \| `"EPSG:4326"` | No | CRS of the bbox; default `EPSG:4674` |
| tamanho_pagina | int | No | Features per page for paginated resources, 1 to 1,000; default 100 (20 for `funai`/`terras_indigenas`); `None` for file resources and for `incra`/`quilombolas` (single page) |
| compactar | bool | No | Local gzip of pages and controls; default `True`; the file (ZIP or CSV) is stored as received |
| retomar | bool | No | Reuses the `ok` entry of the same query after checking the hashes; default `False` |
| limites | `bruto.LimitesBrutos` | No | Budget of the call; default `LimitesBrutos()` |

### Resources

| Source | Resource | Selection | Format / coverage key |
|---|---|---|---|
| `ana` | `massas_dagua` | State and/or bbox required | `esri_json` / `FID` |
| `cnuc` | `ucs` | State, bbox, both or Brazil; always `limite=uc` | `gml` / `cd_cnuc` |
| `ibge` | `malha_municipal` | State, bbox, both or Brazil | `geojson` / `cd_mun` |
| `ibge` | `areas_urbanizadas` | bbox or Brazil; state refused | `geojson` / `fid` |
| `funai` | `terras_indigenas`, `terras_indigenas_pontos` | Brazil; state and bbox refused; polygons default to 20 per page | `geojson` / `gid` |
| `incra` | `quilombolas` | Brazil; state, bbox and `tamanho_pagina` refused; single page, no sortable field | `geojson` / `feature.id` |
| `acervo_fundiario` | `sigef_publico`, `sigef_privado` | State required | `zip` |
| `acervo_fundiario` | `snci_publico`, `snci_privado`, `snci_brasil` | State required | `zip` |
| `acervo_fundiario` | `assentamentos` | Brazil; state and bbox refused; INCRA's national ZIP | `zip` |
| `sfb` | `cnfp` | Brazil; state and bbox refused; the whole country needs `tamanho_pagina=20` and a 24 MiB `max_bytes_pagina` | `esri_json` / `fid` |
| `sicar` | `imoveis` | State required; bbox optional | `geojson` / `feature.id` |
| `cnuc` | `cadastro` | Brazil; state and bbox refused; edition `202607` checked in MMA's catalog | `csv` |
| `ibama` | `termos_embargo` | Brazil; state and bbox refused; contains personal data (name and CPF/CNPJ) | `csv` |
| `ibge` | `malha_municipal_zip`, `areas_urbanizadas_zip` | Brazil; state and bbox refused | `zip` |

### Return

`bruto.ColetaBruta`, with `manifesto` (absolute path of `manifesto.jsonl`), `entrada` (`bruto.RecursoBruto`, the
manifest line) and `reutilizado` (`True` only when `retomar=True` reused an `ok` entry without going to the network).

### Limits (`bruto.LimitesBrutos`)

| Field | Default | Applies to |
|---|---|---|
| max_bytes_recurso | 4 GiB | Bytes of the whole resource; the file is capped at 4 GiB |
| max_bytes_pagina | 8 MiB | Each page or control |
| max_paginas | 10,000 | Pages per collection |
| max_ids | 500,000 | IDs kept for the check |
| max_bytes_ids | 64 MiB | Memory of the ID structures |
| max_segundos | 3,600 | Deadline of the call, including retry and rate-limiter waits |

### Errors

| Situation | Result |
|---|---|
| Invalid argument, selection, destination or name | `InvalidParameterError`, before the network and without changing the manifest |
| 404 on a file (ZIP or CSV) | Returns with `status="ausente_na_fonte"`; the next resumption tries again |
| HTTP or network failure after the retries | `SourceUnavailableError`, with an `erro` entry in the manifest |
| Invalid envelope, OGC error in HTTP 200, HTTP 200 file in another format or divergent coverage | `ParseError`, with an `erro` entry |
| Limit exceeded | `ResourceLimitError` |
| Inconsistent existing manifest | `ContractViolationError`; the previous manifest stays intact |

There is no partial success: for paginated resources, `ok` requires the whole coverage checked (counts before and
after, IDs, CRS and the paging order); for file resources, the whole body received and its beginning checked (ZIP
signature or CSV header). See the [contract](../contracts/bruto.md) for each resource's rule and the full example in
`examples/bruto.py`.
