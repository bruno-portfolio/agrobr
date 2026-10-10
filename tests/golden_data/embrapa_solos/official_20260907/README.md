# Embrapa Solos: respostas oficiais de 07/09/2026

Os quatro JSON são cópias byte a byte dos corpos HTTP decodificados da continuação documental e de paginação. O manifesto registra URL completa, horário observado com fuso, status, cabeçalhos públicos, SHA-256, tamanho e caminho do recibo original. Os testes usam somente estes arquivos locais; não consultam a rede.

- `perfis_reference.json`: seis ocorrências, IDs publicados de 1 a 6, 83 atributos solicitados, geometria projetada como null.
- `mapa_reference.json`: seis ocorrências, IDs publicados de 1 a 6, 18 atributos solicitados, geometria projetada como null.
- `perfis_geo.json`: uma ocorrência com Point e CRS explícito EPSG:4326.
- `mapa_geo.json`: uma ocorrência com MultiPolygon e CRS explícito EPSG:4326; corresponde ao ID 6, com terceira classificação preenchida.

As referências abrangem 606 células escalares originais. Valores textuais, inclusive `NULL`, `<1`, quebras de linha e texto com codificação visual incomum, não foram corrigidos. Ano, data da coleta e profundidades permanecem atributos distintos. A seleção limitada não caracteriza a população nem comprova unicidade de Feature.id, fid ou codigo_pon.

Os arquivos CSV históricos no diretório pai permanecem preservados como evidência do formato anterior. O caminho público da versão 2 utiliza JSON e preserva os atributos químicos como texto; nenhuma coerção dos CSV antigos é atribuída ao novo contrato.
