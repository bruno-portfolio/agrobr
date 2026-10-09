# ICMBio — CRS declarado pelo WFS (23/09/2026)

Duas respostas oficiais do WFS `ICMBio:limiteucsfederais_a` para o mesmo recorte (PARNA da Chapada dos Guimarães),
copiadas byte a byte da conferência de 23/09/2026: `geo_4674.json` sem `srsName` (o servidor declara
`urn:ogc:def:crs:EPSG::4674`) e `geo_4326.json` com `srsName=EPSG:4326` (declara `urn:ogc:def:crs:EPSG::4326`).
URL, data, status e SHA-256 em `manifest.json`. Oráculo: o agrobr pede 4326 e recusa corpo cujo CRS
declarado diverge do pedido.
