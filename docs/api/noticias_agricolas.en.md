# Noticias Agricolas API

The Noticias Agricolas module republishes CEPEA/ESALQ indicators and serves as an automatic fallback when direct access to CEPEA fails (Cloudflare).

!!! danger "restrito license"
    All rights reserved (Law 9.610/98). Data originating from CEPEA (CC BY-NC 4.0).

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

- Source: [Noticias Agricolas](https://noticiasagricolas.com.br) — `restrito` license
- Automatic CEPEA fallback — the user does not need to call it directly
- Warning emitted on first use
- Fallback active while CEPEA remains protected by Cloudflare
