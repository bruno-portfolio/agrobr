# API Noticias Agricolas

O modulo Noticias Agricolas republica indicadores CEPEA/ESALQ e serve como fallback automatico quando o acesso direto ao CEPEA falha (Cloudflare).

!!! warning "zona_cinza"
    Notícias Agrícolas é classificado como `zona_cinza` porque não foi localizada licença própria de reutilização das cotações. A reserva genérica de direitos não comprova uma proibição específica de reutilizar todo fato numérico, nem concede permissão sobre relatórios ou bases protegidas. Dados de origem CEPEA conservam a CC BY-NC 4.0, com atribuição e autorização para uso comercial. O fallback automático permanece e emite o aviso do publicador, além do aviso da origem. Quando CEPEA e Notícias Agrícolas constam em `MetaInfo.data_sources`, prevalece `nc` em `MetaInfo.license`.

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

- Fonte: Notícias Agrícolas — `zona_cinza`; origem CEPEA `nc`.
- Fallback automatico do CEPEA — usuario nao precisa chamar diretamente
- Warning emitido no primeiro uso
- Fallback ativo enquanto CEPEA estiver protegido por Cloudflare

Produto desconhecido ou de tipo inválido levanta `InvalidParameterError` com a lista de produtos aceitos, antes de qualquer requisição. Caixa e espaços externos são normalizados.

`parse_indicador()` aplica essa validação antes de interpretar o HTML e aceita aliases com acento,
como `" CAFÉ "`. Produto desconhecido não recebe uma unidade genérica: ele é recusado com a lista
de opções, preservando as unidades e praças dos produtos publicados.
