# Notícias Agrícolas — Fallback CEPEA

> Notícias Agrícolas é classificado como `zona_cinza` porque não foi localizada licença própria de reutilização das cotações. A reserva genérica de direitos não comprova uma proibição específica de reutilizar todo fato numérico, nem concede permissão sobre relatórios ou bases protegidas. Dados de origem CEPEA conservam a CC BY-NC 4.0, com atribuição e autorização para uso comercial. O fallback automático permanece e emite o aviso do publicador, além do aviso da origem. Quando CEPEA e Notícias Agrícolas constam em `MetaInfo.data_sources`, prevalece `nc` em `MetaInfo.license`.

!!! info "Fallback ativo"
    Este módulo é o fallback principal para contornar proteção Cloudflare
    no site do CEPEA. Enquanto o CEPEA estiver protegido por Cloudflare,
    o NA é a fonte efetiva de dados. Um `warnings.warn()` é emitido no primeiro uso.

## Visão Geral

| Campo | Valor |
|-------|-------|
| **Operador** | Olivi Produções de Vídeo e Comunicação LTDA |
| **Website** | [noticiasagricolas.com.br](https://www.noticiasagricolas.com.br) |
| **Licença** | `zona_cinza`; origem CEPEA CC BY-NC 4.0 |
| **Papel no agrobr** | Fallback do CEPEA (2ª opção, depois do acesso direto) |
| **Dados** | 100% republicação CEPEA/ESALQ — sem dado exclusivo |

## Como funciona no agrobr

Leite está excluído do fallback da API CEPEA. A página NA contém a data de
fechamento e uma nota de mês de referência, mas o parser autônomo NA expõe
o fechamento. Essa semântica permanece disponível no módulo NA e não deve
ser combinada como se fosse o mês de referência retornado por CEPEA.

O módulo Notícias Agrícolas **não é chamado diretamente pelo usuário**. Ele é
acionado automaticamente pelo módulo CEPEA quando:

1. O acesso direto ao CEPEA falha (Cloudflare 403, rede ou status HTTP)
2. A página do CEPEA chega, mas o parser não reconhece indicador (`ParseError`)

## Dados Semanais

Algumas tabelas do NA contêm médias semanais no formato `09 - 13/02/2026`.
O parser extrai a data final do intervalo e marca esses registros com
`anomalies=["media_semanal"]` e `meta["tipo"]="media_semanal"`,
`meta["periodo"]="09 - 13/02/2026"`. Isso permite distinguir cotações
diárias de médias semanais no DataFrame retornado. A marca `anomalies` é gravada no
cache e volta na leitura `offline` e dentro do prazo (migração 11 do cache); `tipo` e
`periodo` ficam só no `Indicador` da coleta.

## Validação de Conteúdo (Soft Block)

Alguns usuários recebem do NA uma página de consent/challenge (HTTP 200,
~10KB sem tabela) em vez da página de dados (~75KB com tabela). O client
valida o conteúdo antes de retornar: se o HTML é < 20KB e não contém
`<table`, levanta `SourceUnavailableError` com mensagem "soft block",
ativando o cache fallback no módulo CEPEA.

## Fonte

- URL: `https://www.noticiasagricolas.com.br/cotacoes/`
- Formato: HTML (server-side rendered, sem JavaScript)
- Atualização: diária (segue CEPEA)
- Licença: `zona_cinza`; origem CEPEA `nc`
