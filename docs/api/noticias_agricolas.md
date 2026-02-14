# API Noticias Agricolas

O modulo Noticias Agricolas republica indicadores CEPEA/ESALQ e serve como fallback automatico quando o acesso direto ao CEPEA falha (Cloudflare).

!!! danger "Licenca restrito"
    Todos os direitos reservados (Lei 9.610/98). Dados originarios do CEPEA (CC BY-NC 4.0). Deprecacao planejada para v0.10.0.

!!! note "Uso interno"
    Este modulo **nao e chamado diretamente pelo usuario**. E invocado automaticamente pelo modulo CEPEA como fallback. Documentado aqui para referencia tecnica.

## Funcoes

### `fetch_indicador_page`

Busca pagina HTML com indicadores de um produto.

```python
async def fetch_indicador_page(produto: str) -> str
```

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `produto` | `str` | Produto (soja, milho, boi, cafe, algodao, trigo, etc.) |

**Retorno:** HTML da pagina como string.

---

### `parse_indicador`

Extrai indicadores do HTML.

```python
def parse_indicador(html: str, produto: str) -> list[Indicador]
```

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `html` | `str` | Conteudo HTML da pagina |
| `produto` | `str` | Nome do produto |

**Retorno:** Lista de objetos `Indicador`.

## Notas

- Fonte: [Noticias Agricolas](https://noticiasagricolas.com.br) — licenca `restrito`
- Fallback automatico do CEPEA — usuario nao precisa chamar diretamente
- Warning emitido no primeiro uso
- Deprecacao planejada: sera substituido por acesso direto CEPEA com browser headless
