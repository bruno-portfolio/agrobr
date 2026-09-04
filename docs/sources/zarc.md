# ZARC (Zoneamento Agricola de Risco Climatico)

## Sobre

O **ZARC** e o sistema oficial do MAPA (Ministerio da Agricultura) e Embrapa
que define janelas de plantio recomendadas por municipio, cultura, tipo de solo
e ciclo do cultivar. Publicado como Portaria no Diario Oficial da Uniao,
o ZARC e requisito para acesso ao credito rural subsidiado (Proagro, PSR).

Dados publicados como CSV no portal [dados.agricultura.gov.br](https://dados.agricultura.gov.br)
(CKAN), licenca CC-BY, atualizacao semanal.

## Dados disponiveis

- **Tabua de Risco:** janelas de plantio (36 decendios) por municipio/cultura/solo/ciclo
- **Culturas:** 32 culturas (soja, milho, trigo, café, cana, feijão, arroz, etc.)
- **Safras:** 2016/2017 a atual + perene (cafe, cana, banana, etc.)
- **Solos:** 3 tipos classicos (arenoso/medio/argiloso) + 6 niveis AD (agua disponivel)
- **Cobertura:** todos os municipios brasileiros (~5.600)

## Campos retornados

| Campo | Tipo | Descricao |
|-------|------|-----------|
| cultura | string | Nome canonico da cultura (ex: "soja", "milho_1", "trigo") |
| safra | string | "2025/2026" ou "perene" |
| geocodigo | string | Codigo IBGE do municipio (7 digitos) |
| uf | string | Sigla da UF |
| municipio | string | Nome do municipio |
| solo_codigo | int | Tipo de solo (1-3 classico, 11-16 AD) |
| ciclo_codigo | int | Ciclo do cultivar (20, 21, 22, 24) |
| clima | string | Restricao climatica (ex: "Sem restricao") |
| manejo | string | Manejo especifico (ex: "Sem restricao", "Irrigado") |
| portaria | string | Numero da portaria MAPA |
| dec1-dec36 | int | Risco por decendio (0=nao recomendado, 20/30/40=% risco) |

## Decendios

Cada mes e dividido em 3 decendios:

- dec1-dec3: janeiro
- dec4-dec6: fevereiro
- ...
- dec34-dec36: dezembro

Valores: 0 (nao recomendado), 20 (alto risco), 30 (medio risco), 40 (baixo risco).

## Notas

- **CSV grande:** arquivos de ~224MB por safra. Session cache evita re-download na mesma sessao Python
- **CKAN discovery:** URLs mudam a cada publicacao; o client faz discovery via API CKAN
- **User-Agent:** portal requer headers browser-like (retorna 403 com bot UA)
- **Encoding:** UTF-8 com BOM, separador `;`
- **Produtividade:** campo quase sempre 0 no dataset atual (reservado para ZarcPRO futuro), dropado intencionalmente

## Licenca

Dados publicos do governo federal brasileiro (CC-BY). Uso livre com citacao da fonte.

## Links

- [Portal CKAN](https://dados.agricultura.gov.br/dataset/tabua-de-risco-zoneamento-agricola-de-risco-climatico)
- [ZARC - MAPA](https://www.gov.br/agricultura/pt-br/assuntos/riscos-seguro/programa-nacional-de-zoneamento-agricola-de-risco-climatico)
