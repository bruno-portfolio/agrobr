# Notícias Agrícolas — Fallback CEPEA

> **Licença:** Todos os direitos reservados (Lei 9.610/98). Empresa privada
> sem termos de uso públicos sobre republicação de cotações. Dados originários
> do CEPEA estão sujeitos a CC BY-NC 4.0.
> Classificação: `restrito` — deprecação planejada para v0.10.0

!!! warning "Deprecação planejada"
    Este módulo é um fallback temporário para contornar proteção Cloudflare
    no site do CEPEA. Será removido na v0.10.0 quando o acesso direto ao
    CEPEA for resolvido. Um `warnings.warn()` é emitido no primeiro uso.

## Visão Geral

| Campo | Valor |
|-------|-------|
| **Operador** | Olivi Produções de Vídeo e Comunicação LTDA |
| **Website** | [noticiasagricolas.com.br](https://www.noticiasagricolas.com.br) |
| **Licença** | `restrito` — todos os direitos reservados |
| **Papel no agrobr** | Fallback do CEPEA (3ª opção após httpx direto e Playwright) |
| **Dados** | 100% republicação CEPEA/ESALQ — sem dado exclusivo |

## Como funciona no agrobr

O módulo Notícias Agrícolas **não é chamado diretamente pelo usuário**. Ele é
acionado automaticamente pelo módulo CEPEA quando:

1. Requisição httpx direta ao CEPEA falha (Cloudflare 403)
2. Playwright não está instalado ou também falha
3. Circuit breaker abre para CEPEA httpx

## Fonte

- URL: `https://www.noticiasagricolas.com.br/cotacoes/`
- Formato: HTML (server-side rendered, sem JavaScript)
- Atualização: diária (segue CEPEA)
- Licença: `restrito`
