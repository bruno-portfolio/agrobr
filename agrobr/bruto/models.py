from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from agrobr import constants

SCHEMA_VERSION: Literal["1.0.0"] = "1.0.0"
LISTA_OFICIAL_DE_IDS = frozenset({("ana", "massas_dagua")})
CABECALHOS_PERMITIDOS = frozenset(
    {
        "last-modified",
        "etag",
        "content-type",
        "content-length",
        "content-encoding",
        "date",
        "retry-after",
        "location",
    }
)

Modo = Literal["arquivo", "paginado"]
Formato = Literal["zip", "esri_json", "gml", "geojson"]
FormatoPagina = Literal["esri_json", "gml", "geojson"]
FormatoControle = Literal["json", "xml"]
Papel = Literal["arquivo", "pagina", "contagem_antes", "contagem_depois", "ids", "crs"]
PapelControle = Literal["contagem_antes", "contagem_depois", "ids", "crs"]
CrsBbox = Literal["EPSG:4674", "EPSG:4326"]
Status = Literal["ok", "erro", "ausente_na_fonte"]
TipoErro = Literal[
    "HTTP404",
    "SourceUnavailableError",
    "ParseError",
    "ResourceLimitError",
    "ContractViolationError",
    "OSError",
    "CancelledError",
    "KeyboardInterrupt",
]
EstadoCobertura = Literal["conferida", "divergente", "nao_comprovada", "nao_aplicavel"]
IdFeicao = str | int


def _caminho_relativo(valor: str) -> str:
    caminho = PurePosixPath(valor)
    if (
        not valor
        or "\\" in valor
        or caminho.is_absolute()
        or re.match(r"^[A-Za-z]:", valor)
        or any(parte in ("", ".", "..") for parte in valor.split("/"))
    ):
        raise ValueError(f"caminho de artefato inválido: {valor!r}")
    return valor


def _cabecalhos(valor: dict[str, str]) -> dict[str, str]:
    fora = sorted(chave for chave in valor if chave not in CABECALHOS_PERMITIDOS)
    if fora:
        raise ValueError(f"cabeçalhos fora da lista permitida: {fora}")
    return valor


def instante(valor: str) -> datetime:
    return datetime.fromisoformat(valor.replace("Z", "+00:00"))


def _data_real(valor: str) -> str:
    instante(valor)
    return valor


def _numero_json(valor: object) -> object:
    if isinstance(valor, bool) or not isinstance(valor, int | float):
        raise ValueError(f"esperado número JSON, recebeu {valor!r}")
    return valor


def _falso(valor: bool) -> bool:
    if valor is not False:
        raise ValueError("snapshot_transacional deve ser false")
    return valor


Numero = Annotated[float, Field(allow_inf_nan=False), BeforeValidator(_numero_json)]
Prazo = Annotated[float, Field(gt=0, allow_inf_nan=False), BeforeValidator(_numero_json)]
Falso = Annotated[StrictBool, AfterValidator(_falso), Field(json_schema_extra={"const": False})]
Utc = Annotated[
    StrictStr,
    Field(
        pattern=r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d{1,6})?Z$",
        json_schema_extra={"format": "date-time"},
    ),
    AfterValidator(_data_real),
]
Hash = Annotated[StrictStr, Field(pattern=r"^[0-9a-f]{64}$")]
UrlHttps = Annotated[StrictStr, Field(pattern=r"^https://[^\s]+$")]
Caminho = Annotated[StrictStr, AfterValidator(_caminho_relativo)]
Texto = Annotated[StrictStr, Field(min_length=1)]
NaoNegativo = Annotated[StrictInt, Field(ge=0)]
Positivo = Annotated[StrictInt, Field(ge=1)]
StatusHttp = Annotated[StrictInt, Field(ge=100, le=599)]
Cabecalhos = Annotated[dict[StrictStr, StrictStr], AfterValidator(_cabecalhos)]
Parametros = dict[StrictStr, StrictStr]
Total = NaoNegativo | Literal["unknown"] | None


class _Modelo(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class LimitesManifesto(_Modelo):
    """Os seis limites efetivos de uma tentativa, como ficam em ``opcoes.limites``: todos obrigatórios."""

    max_bytes_recurso: Positivo
    max_bytes_pagina: Positivo
    max_paginas: Positivo
    max_ids: Positivo
    max_bytes_ids: Positivo
    max_segundos: Prazo


class LimitesBrutos(LimitesManifesto):
    """Orçamento de uma chamada de ``coletar``; os padrões valem aqui, e o manifesto guarda os seis valores efetivos."""

    max_bytes_recurso: Positivo = constants.BRUTO_MAX_BYTES_RECURSO
    max_bytes_pagina: Positivo = constants.BRUTO_MAX_BYTES_PAGINA
    max_paginas: Positivo = constants.BRUTO_MAX_PAGINAS
    max_ids: Positivo = constants.BRUTO_MAX_IDS
    max_bytes_ids: Positivo = constants.BRUTO_MAX_BYTES_IDS
    max_segundos: Prazo = constants.BRUTO_MAX_SEGUNDOS


class Selecao(_Modelo):
    uf: StrictStr | None
    bbox: tuple[Numero, Numero, Numero, Numero] | None
    bbox_crs: CrsBbox | None
    camada: StrictStr | None
    edicao: StrictInt | None
    natureza: Literal["publico", "privado"] | None


class Opcoes(_Modelo):
    tamanho_pagina: Annotated[StrictInt, Field(ge=1, le=constants.BRUTO_TAMANHO_PAGINA_MAX)] | None
    compactar: StrictBool
    limites: LimitesManifesto

    @field_validator("limites", mode="before")
    @classmethod
    def _limites_efetivos(cls, valor: object) -> object:
        return LimitesManifesto(**valor.model_dump()) if isinstance(valor, LimitesBrutos) else valor


class EvidenciaCRS(_Modelo):
    tipo: Literal["pagina", "controle", "prj"]
    arquivo: Caminho
    localizador: Texto
    valor: Texto


class PaginacaoOffset(_Modelo):
    tipo: Literal["offset"]
    inicio: NaoNegativo
    quantidade: Positivo


class PaginacaoFID(_Modelo):
    tipo: Literal["fid"]
    min: StrictInt
    max: StrictInt
    quantidade: Positivo

    @model_validator(mode="after")
    def _faixa(self) -> PaginacaoFID:
        if self.min > self.max:
            raise ValueError("paginação FID com min maior que max")
        return self


Paginacao = Annotated[PaginacaoOffset | PaginacaoFID, Field(discriminator="tipo")]


class _Artefato(_Modelo):
    numero: Positivo
    url_solicitada: UrlHttps
    url: UrlHttps
    parametros: Parametros
    inicio: Utc
    fim: Utc
    http_status: Literal[200]
    arquivo: Caminho
    sha256: Hash
    bytes: NaoNegativo
    bytes_armazenados: NaoNegativo
    compressao: Literal["gzip", "nenhuma"]
    cabecalhos: Cabecalhos


class PaginaBruta(_Artefato):
    formato: FormatoPagina
    paginacao: Paginacao
    feicoes_recebidas: NaoNegativo
    ids_distintos: NaoNegativo
    total_declarado: Total
    crs: StrictStr | None


class ControleBruto(_Artefato):
    formato: FormatoControle
    papel: PapelControle
    valor_declarado: Total


class Cobertura(_Modelo):
    total_antes: NaoNegativo | None
    total_depois: NaoNegativo | None
    recebidas: NaoNegativo | None
    ids_distintos: NaoNegativo | None
    ids_repetidos: NaoNegativo | None
    campo_id: StrictStr | None
    estado: EstadoCobertura
    completa: StrictBool
    snapshot_transacional: Falso
    controles: list[Caminho]


class ErroBruto(_Modelo):
    tipo: TipoErro
    mensagem: Texto
    http_status: StatusHttp | None
    url: UrlHttps | None


class RecursoBruto(_Modelo):
    """Uma linha do ``manifesto.jsonl`` (schema 1.0.0); a validação confere também os invariantes cruzados do contrato."""

    schema_version: Literal["1.0.0"]
    tipo: Literal["recurso"]
    fonte: Texto
    recurso: Texto
    nome: Texto
    consulta_id: Hash
    coleta_id: Texto
    status: Status
    inicio: Utc
    fim: Utc
    registrado_em: Utc
    url_solicitada: UrlHttps
    url: UrlHttps
    parametros: Parametros
    selecao: Selecao
    opcoes: Opcoes
    modo: Modo
    formato: Formato
    crs: StrictStr | None
    crs_evidencia: EvidenciaCRS | None
    http_status: StatusHttp | None
    http_inicio: Utc | None
    http_fim: Utc | None
    arquivo: Caminho | None
    sha256: Hash | None
    bytes: NaoNegativo | None
    bytes_armazenados: NaoNegativo | None
    compressao: Literal["gzip", "nenhuma"]
    cabecalhos: Cabecalhos
    paginas: list[PaginaBruta]
    controles: list[ControleBruto]
    feicoes: NaoNegativo | None
    cobertura: Cobertura
    erro: ErroBruto | None
    avisos: list[StrictStr]
    agrobr_version: Texto

    @model_validator(mode="after")
    def _invariantes(self) -> RecursoBruto:
        violacoes = invariantes(self)
        if violacoes:
            raise ValueError("; ".join(violacoes))
        return self

    @property
    def chave(self) -> tuple[str, str, str]:
        return self.fonte, self.recurso, self.nome

    def linha(self) -> str:
        return json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )


class ColetaBruta(_Modelo):
    manifesto: Path
    entrada: RecursoBruto
    reutilizado: StrictBool


class PedidoBruto(_Modelo):
    fonte: str
    recurso: str
    nome: str
    modo: Modo
    formato: Formato
    uf: str | None
    bbox: tuple[float, float, float, float] | None
    bbox_crs: CrsBbox | None
    tamanho_pagina: int | None
    compactar: bool
    limites: LimitesBrutos


class PlanoBruto(_Modelo):
    fonte: Texto
    recurso: Texto
    nome: Texto
    url_solicitada: UrlHttps
    parametros: Parametros
    selecao: Selecao
    modo: Modo
    formato: Formato
    opcoes: Opcoes
    campo_id: StrictStr | None
    crs_esperado: StrictStr | None


class PedidoHTTP(_Modelo):
    url: UrlHttps
    parametros: Parametros
    papel: Papel
    numero: Positivo
    formato: Formato | FormatoControle
    paginacao: PaginacaoOffset | PaginacaoFID | None = None


@dataclass(frozen=True)
class RespostaCapturada:
    """Uma resposta GET: o corpo de página ou controle fica em ``corpo``; o do arquivo, em ``temporario``."""

    pedido: PedidoHTTP
    url_solicitada: str
    url: str
    http_status: int
    cabecalhos: dict[str, str]
    inicio: str
    fim: str
    sha256: str | None
    tamanho: int | None
    corpo: bytes | None
    temporario: Path | None
    completo: bool


@dataclass(frozen=True)
class LeituraPagina:
    feicoes_recebidas: int
    ids: tuple[IdFeicao, ...]
    total_declarado: int | Literal["unknown"] | None
    crs: str | None
    crs_localizador: str | None = None
    crs_valor: str | None = None


@dataclass(frozen=True)
class LeituraControle:
    papel: PapelControle
    valor_declarado: int | Literal["unknown"] | None


@dataclass(frozen=True)
class ConclusaoBruta:
    """O que o adaptador afirma ao fim; a cobertura, as contagens e o CRS são conferidos e preenchidos pelo núcleo."""

    status: Literal["ok", "ausente_na_fonte"]
    crs_evidencia: EvidenciaCRS | None = None
    avisos: tuple[str, ...] = ()


def consulta_id(
    *,
    fonte: str,
    recurso: str,
    nome: str,
    url_solicitada: str,
    parametros: dict[str, str],
    selecao: Selecao,
    modo: str,
    formato: str,
    tamanho_pagina: int | None,
    compactar: bool,
) -> str:
    canonico = json.dumps(
        {
            "fonte": fonte,
            "recurso": recurso,
            "nome": nome,
            "url_solicitada": url_solicitada,
            "parametros": parametros,
            "selecao": selecao.model_dump(mode="json"),
            "modo": modo,
            "formato": formato,
            "tamanho_pagina": tamanho_pagina,
            "compactar": compactar,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(canonico.encode("utf-8")).hexdigest()


def artefatos(entrada: RecursoBruto) -> list[str]:
    caminhos = [p.arquivo for p in entrada.paginas] + [c.arquivo for c in entrada.controles]
    return caminhos + ([entrada.arquivo] if entrada.arquivo is not None else [])


def invariantes(e: RecursoBruto) -> list[str]:
    """Regras do contrato 1.0.0 que o schema campo a campo não expressa; lista vazia quando a entrada é válida."""
    erros: list[str] = []

    def exigir(condicao: bool, mensagem: str) -> None:
        if not condicao:
            erros.append(mensagem)

    calculado = consulta_id(
        fonte=e.fonte,
        recurso=e.recurso,
        nome=e.nome,
        url_solicitada=e.url_solicitada,
        parametros=e.parametros,
        selecao=e.selecao,
        modo=e.modo,
        formato=e.formato,
        tamanho_pagina=e.opcoes.tamanho_pagina,
        compactar=e.opcoes.compactar,
    )
    exigir(e.consulta_id == calculado, "consulta_id não confere com a consulta normalizada")
    exigir(
        instante(e.inicio) <= instante(e.fim) <= instante(e.registrado_em),
        "horários fora de ordem (inicio <= fim <= registrado_em)",
    )
    exigir((e.erro is None) == (e.status == "ok"), "erro deve ser null exatamente em ok")
    exigir(e.cobertura.completa == (e.status == "ok"), "cobertura.completa deve ser true só em ok")
    exigir(e.compressao == "nenhuma", "compressao do topo deve ser nenhuma")
    exigir(
        (e.crs is None) == (e.crs_evidencia is None),
        "crs e crs_evidencia: ambos null ou ambos preenchidos",
    )
    quatro = (e.arquivo, e.sha256, e.bytes, e.bytes_armazenados)
    exigir(
        all(v is None for v in quatro) or all(v is not None for v in quatro),
        "arquivo, sha256, bytes e bytes_armazenados: todos preenchidos ou todos null",
    )
    if e.selecao.bbox is None:
        exigir(e.selecao.bbox_crs is None, "selecao.bbox_crs deve ser null sem bbox")
    else:
        exigir(e.selecao.bbox_crs is not None, "selecao.bbox_crs obrigatório com bbox")
    prefixo = f"{e.fonte}/{e.recurso}/{e.nome}/{e.coleta_id}/"
    todos = artefatos(e)
    exigir(all(c.startswith(prefixo) for c in todos), f"artefato fora de {prefixo}")
    exigir(len(set(todos)) == len(todos), "caminho de artefato repetido")
    if e.crs_evidencia is not None:
        exigir(
            e.crs_evidencia.arquivo in todos, "crs_evidencia.arquivo não é artefato desta entrada"
        )
    for nome_lista, lista in (("paginas", e.paginas), ("controles", e.controles)):
        exigir(
            [item.numero for item in lista] == list(range(1, len(lista) + 1)),
            f"{nome_lista}: números devem ser contíguos a partir de 1",
        )
    for item in [*e.paginas, *e.controles]:
        exigir(
            instante(e.inicio) <= instante(item.inicio) <= instante(item.fim) <= instante(e.fim),
            f"{item.arquivo}: horário fora da tentativa",
        )
        if item.compressao == "nenhuma":
            exigir(
                item.bytes == item.bytes_armazenados, f"{item.arquivo}: bytes_armazenados sem gzip"
            )
    exigir(
        set(e.cobertura.controles) <= {c.arquivo for c in e.controles},
        "cobertura.controles referencia controle inexistente",
    )
    if e.status == "erro":
        exigir(
            e.erro is not None and e.erro.tipo != "HTTP404",
            "erro com tipo HTTP404 é ausente_na_fonte",
        )
    if e.status == "ausente_na_fonte":
        exigir(e.modo == "arquivo", "ausente_na_fonte só existe em modo arquivo")
        exigir(
            e.erro is not None and e.erro.tipo == "HTTP404", "ausente_na_fonte exige erro HTTP404"
        )
        exigir(e.http_status == 404, "ausente_na_fonte exige http_status 404")
    if e.modo == "arquivo":
        _invariantes_arquivo(e, exigir)
    else:
        _invariantes_paginado(e, exigir)
    return erros


def _invariantes_arquivo(e: RecursoBruto, exigir: Callable[[bool, str], None]) -> None:
    exigir(e.formato == "zip", "modo arquivo exige formato zip")
    exigir(e.parametros == {}, "modo arquivo tem parametros {}")
    exigir(e.paginas == [] and e.controles == [], "modo arquivo não tem páginas nem controles")
    exigir(e.opcoes.tamanho_pagina is None, "modo arquivo tem opcoes.tamanho_pagina null")
    exigir(e.opcoes.compactar is False, "modo arquivo tem opcoes.compactar false")
    exigir(e.feicoes is None, "modo arquivo tem feicoes null")
    c = e.cobertura
    exigir(
        (c.total_antes, c.total_depois, c.recebidas, c.ids_distintos, c.ids_repetidos, c.campo_id)
        == (None,) * 6,
        "modo arquivo tem contagens e campo_id null na cobertura",
    )
    exigir(
        c.estado == "nao_aplicavel" and c.controles == [],
        "modo arquivo tem cobertura nao_aplicavel",
    )
    exigir((e.http_inicio is None) == (e.http_fim is None), "http_inicio e http_fim andam juntos")
    if e.http_inicio is not None and e.http_fim is not None:
        exigir(
            instante(e.inicio)
            <= instante(e.http_inicio)
            <= instante(e.http_fim)
            <= instante(e.fim),
            "GET do arquivo fora da tentativa",
        )
    if e.arquivo is not None:
        exigir(
            e.arquivo == f"{e.fonte}/{e.recurso}/{e.nome}/{e.coleta_id}/original.zip",
            "arquivo fora do layout",
        )
        exigir(
            e.bytes == e.bytes_armazenados,
            "ZIP sem compactação adicional: bytes_armazenados = bytes",
        )
    if e.status == "ok":
        exigir(
            e.arquivo is not None and e.http_status == 200,
            "arquivo ok exige o arquivo e http_status 200",
        )


def _invariantes_paginado(e: RecursoBruto, exigir: Callable[[bool, str], None]) -> None:
    exigir(e.formato != "zip", "modo paginado não usa formato zip")
    exigir(e.arquivo is None, "modo paginado não tem arquivo concatenado")
    exigir(
        (e.http_status, e.http_inicio, e.http_fim) == (None, None, None) and e.cabecalhos == {},
        "modo paginado tem http_status, http_inicio, http_fim null e cabecalhos {}",
    )
    exigir(e.opcoes.tamanho_pagina is not None, "modo paginado tem opcoes.tamanho_pagina")
    exigir(e.url_solicitada == e.url, "modo paginado: url igual à url_solicitada do endpoint")
    exigir("?" not in e.url_solicitada, "modo paginado: url do topo sem query string")
    exigir(
        all(p.formato == e.formato for p in e.paginas), "página com formato diferente do recurso"
    )
    c = e.cobertura
    exigir(
        None not in (c.recebidas, c.ids_distintos, c.ids_repetidos) and c.campo_id is not None,
        "modo paginado tem recebidas, ids_distintos, ids_repetidos e campo_id na cobertura",
    )
    exigir(c.estado != "nao_aplicavel", "modo paginado não tem cobertura nao_aplicavel")
    exigir(
        c.recebidas == sum(p.feicoes_recebidas for p in e.paginas),
        "cobertura.recebidas é a soma das feições das páginas",
    )
    if e.status != "ok":
        exigir(c.estado != "conferida", "cobertura conferida só em ok")
        return
    total = c.total_antes
    exigir(c.estado == "conferida", "paginado ok exige cobertura conferida")
    exigir(
        total is not None
        and total == c.total_depois == e.feicoes == c.recebidas == c.ids_distintos,
        "paginado ok exige total_antes = total_depois = feicoes = recebidas = ids_distintos",
    )
    exigir(c.ids_repetidos == 0, "paginado ok exige ids_repetidos 0")
    referenciados = [ctl for ctl in e.controles if ctl.arquivo in c.controles]
    antes = [ctl.valor_declarado for ctl in referenciados if ctl.papel == "contagem_antes"]
    depois = [ctl.valor_declarado for ctl in referenciados if ctl.papel == "contagem_depois"]
    exigir(len(antes) == len(depois) == 1, "paginado ok referencia uma contagem antes e uma depois")
    exigir(
        antes == [c.total_antes] and depois == [c.total_depois],
        "contagens dos controles diferem da cobertura",
    )
    exigir(
        all(p.ids_distintos == p.feicoes_recebidas for p in e.paginas)
        and sum(p.ids_distintos for p in e.paginas) == c.ids_distintos,
        "ids_distintos das páginas diferem das feições ou da cobertura",
    )
    if (e.fonte, e.recurso) in LISTA_OFICIAL_DE_IDS:
        exigir(
            any(ctl.papel == "ids" for ctl in referenciados),
            "ok sem a lista oficial de IDs referenciada",
        )
    exigir(
        all(p.total_declarado in ("unknown", None, total) for p in e.paginas),
        "total numérico declarado em página difere da contagem de controle",
    )
    if total == 0:
        exigir(e.paginas == [], "zero feições confirmado tem paginas []")
    if e.paginas:
        exigir(e.crs is not None, "paginado ok com feições exige crs verificado")
        exigir(all(p.crs == e.crs for p in e.paginas), "páginas com CRS diferente do recurso")
