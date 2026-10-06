# Notícias Agrícolas — CEPEA Fallback

> Notícias Agrícolas is classified as `zona_cinza` because no publisher-specific reuse license was found for its quotations. A generic rights reservation does not establish a specific prohibition on reusing every numerical fact, nor does it grant permission over protected reports or databases. CEPEA-origin data retains CC BY-NC 4.0, with attribution and permission for commercial use. Automatic fallback remains and emits the publisher's warning in addition to the original source's warning. When CEPEA and Notícias Agrícolas appear in `MetaInfo.data_sources`, `MetaInfo.license` is `nc`.

!!! info "Fallback active"
    This module is the primary fallback to work around Cloudflare protection
    on the CEPEA site. While CEPEA is protected by Cloudflare,
    NA is the effective data source. A `warnings.warn()` is emitted on first use.

## Overview

| Field | Value |
|-------|-------|
| **Operator** | Olivi Produções de Vídeo e Comunicação LTDA |
| **Website** | [noticiasagricolas.com.br](https://www.noticiasagricolas.com.br) |
| **License** | `zona_cinza`; CEPEA origin CC BY-NC 4.0 |
| **Role in agrobr** | CEPEA fallback (2nd option, after direct access) |
| **Data** | 100% CEPEA/ESALQ republication — no exclusive data |

## How it works in agrobr

Milk is excluded from the CEPEA API fallback. The NA page contains a closing
date and a reference-month note, but the standalone NA parser exposes the
closing date. That behavior remains available in the NA module and must not
be combined as if it were the reference month returned by CEPEA.

The Notícias Agrícolas module is **not called directly by the user**. It is
triggered automatically by the CEPEA module when:

1. Direct access to CEPEA fails (Cloudflare 403, network or HTTP status)
2. The CEPEA page arrives but the parser finds no indicator (`ParseError`)

## Weekly Data

Some NA tables contain weekly averages in the format `09 - 13/02/2026`.
The parser extracts the end date of the interval and marks these records with
`anomalies=["media_semanal"]` and `meta["tipo"]="media_semanal"`,
`meta["periodo"]="09 - 13/02/2026"`. This allows distinguishing daily
quotes from weekly averages in the returned DataFrame. The `anomalies` marker is stored
in the cache and comes back on `offline` reads and within the expiry (cache migration
11); `tipo` and `periodo` stay only in the collected `Indicador`.

## Content Validation (Soft Block)

Some users receive from NA a consent/challenge page (HTTP 200,
~10KB without a table) instead of the data page (~75KB with a table). The client
validates the content before returning: if the HTML is < 20KB and does not contain
`<table`, it raises `SourceUnavailableError` with the message "soft block",
activating the cache fallback in the CEPEA module.

## Source

- Page: Notícias Agrícolas quotations
- Format: HTML (server-side rendered, no JavaScript)
- Update: daily (follows CEPEA)
- License: `zona_cinza`; CEPEA origin `nc`
