# Noticias Agricolas API

The Noticias Agricolas module republishes CEPEA/ESALQ indicators and serves as an automatic fallback when direct access to CEPEA fails (Cloudflare).

!!! warning "zona_cinza"
    Notícias Agrícolas is classified as `zona_cinza` because no publisher-specific reuse license was found for its quotations. A generic rights reservation does not establish a specific prohibition on reusing every numerical fact, nor does it grant permission over protected reports or databases. CEPEA-origin data retains CC BY-NC 4.0: attribution is required, and commercial use requires the holder's authorization. Automatic fallback remains and emits the publisher's warning in addition to the original source's warning. When CEPEA and Notícias Agrícolas appear in `MetaInfo.data_sources`, `MetaInfo.license` is `nc`.

!!! note "Internal use"
    This module is **not called directly by the user**. It is invoked automatically by the CEPEA module as a fallback. Documented here for technical reference.

## Functions

### `fetch_indicador_page`

Fetches the HTML page with the indicators for a product.

```python
async def fetch_indicador_page(produto: str) -> str
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `produto` | `str` | Product (soja, milho, boi, cafe, algodao, trigo, etc.) |

**Returns:** Page HTML as a string.

---

### `parse_indicador`

Extracts indicators from the HTML.

```python
def parse_indicador(html: str, produto: str) -> list[Indicador]
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `html` | `str` | HTML content of the page |
| `produto` | `str` | Product name |

**Returns:** List of `Indicador` objects.

## Notes

- Source: Notícias Agrícolas — `zona_cinza`; CEPEA origin `nc`.
- Automatic CEPEA fallback — the user does not need to call it directly
- Warning emitted on first use
- Fallback active while CEPEA remains protected by Cloudflare

Unknown products or invalid argument types raise `InvalidParameterError` with the accepted product list before any request. Case and surrounding whitespace are normalized.

`parse_indicador()` validates the product before interpreting HTML and accepts accented aliases,
such as `" CAFÉ "`. An unknown product is rejected with the list of options instead of receiving
a generic unit, preserving the units and locations of published products.
