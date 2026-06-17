# Notícias Agrícolas — CEPEA Fallback

> **License:** All rights reserved (Law 9.610/98). Private company
> with no public terms of use on the republication of quotes. Data originating
> from CEPEA is subject to CC BY-NC 4.0.
> Classification: `restrito`

!!! info "Fallback active"
    This module is the primary fallback to work around Cloudflare protection
    on the CEPEA site. While CEPEA is protected by Cloudflare,
    NA is the effective data source. A `warnings.warn()` is emitted on first use.

## Overview

| Field | Value |
|-------|-------|
| **Operator** | Olivi Produções de Vídeo e Comunicação LTDA |
| **Website** | [noticiasagricolas.com.br](https://www.noticiasagricolas.com.br) |
| **License** | `restrito` — all rights reserved |
| **Role in agrobr** | CEPEA fallback (3rd option after direct httpx and Playwright) |
| **Data** | 100% CEPEA/ESALQ republication — no exclusive data |

## How it works in agrobr

The Notícias Agrícolas module is **not called directly by the user**. It is
triggered automatically by the CEPEA module when:

1. The direct httpx request to CEPEA fails (Cloudflare 403)
2. Playwright is not installed or also fails
3. The circuit breaker opens for CEPEA httpx

## Weekly Data

Some NA tables contain weekly averages in the format `09 - 13/02/2026`.
The parser extracts the end date of the interval and marks these records with
`anomalies=["media_semanal"]` and `meta["tipo"]="media_semanal"`,
`meta["periodo"]="09 - 13/02/2026"`. This allows distinguishing daily
quotes from weekly averages in the returned DataFrame.

## Content Validation (Soft Block)

Some users receive from NA a consent/challenge page (HTTP 200,
~10KB without a table) instead of the data page (~75KB with a table). The client
validates the content before returning: if the HTML is < 20KB and does not contain
`<table`, it raises `SourceUnavailableError` with the message "soft block",
activating the cache fallback in the CEPEA module.

## Source

- URL: `https://www.noticiasagricolas.com.br/cotacoes/`
- Format: HTML (server-side rendered, no JavaScript)
- Update: daily (follows CEPEA)
- License: `restrito`
