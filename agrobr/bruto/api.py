from __future__ import annotations

import asyncio
import json
import sys
import time
import uuid
from os import PathLike
from pathlib import Path
from typing import Literal

import httpx
from pydantic import ValidationError

from agrobr import _log, constants
from agrobr.bruto import models, protocols, registry, storage, transport, validation
from agrobr.exceptions import (
    ContractViolationError,
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
    SourceUnavailableError,
)
from agrobr.utils import tasks

logger = _log.get_logger(__name__)

_PAPEIS_CONTROLE = ("contagem_antes", "contagem_depois", "ids", "crs")


async def coletar(
    fonte: str,
    recurso: str,
    *,
    destino: str | PathLike[str],
    nome: str | None = None,
    uf: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    bbox_crs: Literal["EPSG:4674", "EPSG:4326"] = "EPSG:4674",
    tamanho_pagina: int | None = None,
    compactar: bool = True,
    retomar: bool = False,
    limites: models.LimitesBrutos | None = None,
) -> models.ColetaBruta:
    """Guarda em ``destino`` o arquivo ou as páginas originais da fonte e registra a aquisição no ``manifesto.jsonl``.

    O contrato do manifesto (schema 1.0.0), os recursos aceitos e as regras de retomada estão na página
    "Coleta bruta" da documentação. Retorna para ``ok`` e ``ausente_na_fonte`` (404 do arquivo); as demais
    falhas registram ``erro`` no manifesto, quando possível, e levantam a exceção da categoria. Argumento,
    destino ou colisão de chave inválidos levantam ``InvalidParameterError`` antes da rede.
    """
    comeco = time.monotonic()
    registrado = registry.recurso(fonte, recurso)
    pedido = validation.pedido(
        registrado,
        nome=nome,
        uf=uf,
        bbox=bbox,
        bbox_crs=bbox_crs,
        tamanho_pagina=tamanho_pagina,
        compactar=compactar,
        retomar=retomar,
        limites=limites,
    )
    adaptador = registry.adaptador(registrado)
    plano = adaptador.planejar(pedido)
    validation.conferir_plano(plano, pedido, registrado)
    consulta = models.consulta_id(
        fonte=plano.fonte,
        recurso=plano.recurso,
        nome=plano.nome,
        url_solicitada=plano.url_solicitada,
        parametros=plano.parametros,
        selecao=plano.selecao,
        modo=plano.modo,
        formato=plano.formato,
        tamanho_pagina=plano.opcoes.tamanho_pagina,
        compactar=plano.opcoes.compactar,
    )
    raiz = storage.preparar_destino(destino)
    orcamento = transport.Orcamento(
        plano.fonte, pedido.limites, prazo=comeco + pedido.limites.max_segundos
    )
    with storage.trava(raiz):
        entradas = storage.ler_manifesto(raiz)
        existente = _existente(entradas, plano, consulta, retomar=bool(retomar))
        if existente is not None and existente.status == "ok":
            await tasks.to_thread_ate_o_fim(
                _verificar_entrada, raiz, existente, pedido.limites, orcamento
            )
            return models.ColetaBruta(
                manifesto=raiz / storage.MANIFESTO, entrada=existente, reutilizado=True
            )
        coleta = Coleta(raiz, plano, consulta, orcamento)
        entrada = await coleta.executar(adaptador, entradas)
    return models.ColetaBruta(
        manifesto=raiz / storage.MANIFESTO, entrada=entrada, reutilizado=False
    )


def _existente(
    entradas: list[models.RecursoBruto],
    plano: models.PlanoBruto,
    consulta: str,
    *,
    retomar: bool,
) -> models.RecursoBruto | None:
    for entrada in entradas:
        if (entrada.fonte, entrada.recurso) != (plano.fonte, plano.recurso):
            continue
        if entrada.nome.casefold() != plano.nome.casefold():
            continue
        if entrada.nome != plano.nome:
            raise InvalidParameterError(
                f"bruto: o nome {plano.nome!r} colide com {entrada.nome!r} (só difere na caixa)"
            )
        if not retomar:
            raise InvalidParameterError(
                f"bruto: {plano.fonte}/{plano.recurso}/{plano.nome} já está no manifesto; use "
                "retomar=True, outro nome ou outro destino"
            )
        if entrada.consulta_id != consulta:
            raise InvalidParameterError(
                f"bruto: {plano.fonte}/{plano.recurso}/{plano.nome} está no manifesto com outra "
                "consulta (seleção, camada, parâmetros, tamanho_pagina ou compactar); use outro "
                "nome ou destino"
            )
        return entrada
    return None


def _verificar_entrada(
    raiz: Path,
    entrada: models.RecursoBruto,
    limites: models.LimitesBrutos,
    orcamento: transport.Orcamento,
) -> None:
    artefatos: list[tuple[str, str, int, int, str, int]] = [
        (a.arquivo, a.compressao, a.bytes, a.bytes_armazenados, a.sha256, limites.max_bytes_pagina)
        for a in [*entrada.paginas, *entrada.controles]
    ]
    if entrada.arquivo is not None:
        assert entrada.bytes is not None and entrada.bytes_armazenados is not None
        assert entrada.sha256 is not None
        teto = min(limites.max_bytes_recurso, constants.BRUTO_TETO_ARQUIVO_BYTES)
        artefatos.append(
            (
                entrada.arquivo,
                "nenhuma",
                entrada.bytes,
                entrada.bytes_armazenados,
                entrada.sha256,
                teto,
            )
        )
    total = sum(artefato[2] for artefato in artefatos)
    if total > limites.max_bytes_recurso:
        raise ResourceLimitError(
            entrada.fonte,
            f"conferir {total} bytes da entrada passa de max_bytes_recurso ({limites.max_bytes_recurso})",
        )
    for relativo, compressao, tamanho, armazenado, sha256, teto in artefatos:
        storage.verificar(
            raiz,
            relativo,
            compressao=compressao,
            tamanho=tamanho,
            armazenado=armazenado,
            sha256=sha256,
            teto=teto,
            conferir_prazo=orcamento.conferir_prazo,
        )


def _tipo_erro(exc: BaseException) -> models.TipoErro | None:
    for classe, tipo in (
        (asyncio.CancelledError, "CancelledError"),
        (KeyboardInterrupt, "KeyboardInterrupt"),
        (SourceUnavailableError, "SourceUnavailableError"),
        (ParseError, "ParseError"),
        (ResourceLimitError, "ResourceLimitError"),
        (ContractViolationError, "ContractViolationError"),
        (OSError, "OSError"),
    ):
        if isinstance(exc, classe):
            return tipo  # type: ignore[return-value]
    return None


class Coleta:
    """Estado de uma tentativa: implementa o ``ContextoBruto`` que o adaptador usa e monta a linha do manifesto."""

    def __init__(
        self,
        raiz: Path,
        plano: models.PlanoBruto,
        consulta: str,
        orcamento: transport.Orcamento,
    ) -> None:
        self.raiz = raiz
        self.plano = plano
        self.consulta = consulta
        self.orcamento = orcamento
        self.limites = plano.opcoes.limites
        self.relogio = transport.Relogio()
        self.coleta_id = uuid.uuid4().hex
        self.prefixo = f"{plano.fonte}/{plano.recurso}/{plano.nome}/{self.coleta_id}"
        self.original = f"{self.prefixo}/original.{plano.formato}"
        self.inicio = self.relogio.agora()
        self.paginas: list[models.PaginaBruta] = []
        self.controles: list[models.ControleBruto] = []
        self.recebidos: set[models.IdFeicao] = set()
        self.esperados: set[models.IdFeicao] | None = None
        self.repetidos = 0
        self._bytes_chaves = 0
        self.divergente = False
        self.evidencia: models.EvidenciaCRS | None = None
        self.ultimo_arquivo: models.RespostaCapturada | None = None
        self.arquivo: tuple[str, models.RespostaCapturada, int] | None = None
        self._pendente: models.RespostaCapturada | None = None
        self._falha: tuple[int | None, str | None] = (None, None)

    @property
    def memoria_ids(self) -> int:
        """Bytes contabilizados das estruturas de IDs (chaves e conjuntos), não o RSS do processo."""
        recipientes = sys.getsizeof(self.recebidos) + sys.getsizeof(self.esperados or set())
        return self._bytes_chaves + recipientes

    def conferir_prazo(self) -> None:
        self.orcamento.conferir_prazo()

    def _violacao(self, mensagem: str) -> ContractViolationError:
        return ContractViolationError(
            "bruto", f"{self.plano.fonte}/{self.plano.recurso}: {mensagem}"
        )

    def divergencia(self, mensagem: str) -> ParseError:
        return self._divergencia(mensagem)

    def _divergencia(self, mensagem: str) -> ParseError:
        self.divergente = True
        return ParseError(self.plano.fonte, 1, mensagem)

    async def obter(
        self, http: httpx.AsyncClient, pedido: models.PedidoHTTP
    ) -> models.RespostaCapturada:
        self.conferir_prazo()
        self._despejar_pendente()
        arquivo = pedido.papel == "arquivo"
        if arquivo != (self.plano.modo == "arquivo"):
            raise self._violacao(f"pedido de papel {pedido.papel!r} em recurso {self.plano.modo}")
        temporario = None
        teto = self.limites.max_bytes_pagina
        if arquivo:
            teto = min(self.limites.max_bytes_recurso, constants.BRUTO_TETO_ARQUIVO_BYTES)
            temporario = storage.caminho(self.raiz, f"{self.original}.part")
            temporario.parent.mkdir(parents=True, exist_ok=True)
        self._falha = (None, pedido.url)
        resposta = await transport.baixar(
            http,
            pedido,
            fonte=self.plano.fonte,
            orcamento=self.orcamento,
            relogio=self.relogio,
            teto=teto,
            temporario=temporario,
        )
        self._falha = (None, resposta.url_solicitada)
        if arquivo:
            self.ultimo_arquivo = resposta
        if resposta.http_status == 200:
            self._pendente = None if arquivo else resposta
            return resposta
        if resposta.corpo is not None:
            nome = f"diagnostico/http{resposta.http_status}_{pedido.papel}_{pedido.numero:06d}.bin"
            storage.gravar_privado(self.raiz, self.coleta_id, nome, resposta.corpo)
        if arquivo and resposta.http_status == 404:
            return resposta
        self._falha = (resposta.http_status, resposta.url_solicitada)
        raise SourceUnavailableError(
            source=self.plano.fonte,
            url=resposta.url_solicitada,
            last_error=f"HTTP {resposta.http_status} em {pedido.papel}",
        )

    async def consultar(self, http: httpx.AsyncClient, url: str) -> bytes:
        """GET auxiliar (o catálogo que confirma a edição): conta no orçamento, no prazo e nas tentativas da coleta,
        com o teto de ``max_bytes_pagina``, e não vira artefato nem entra no manifesto."""
        self.conferir_prazo()
        pedido = models.PedidoHTTP(
            url=url, parametros={}, papel="catalogo", numero=1, formato="json"
        )
        self._falha = (None, url)
        resposta = await transport.baixar(
            http,
            pedido,
            fonte=self.plano.fonte,
            orcamento=self.orcamento,
            relogio=self.relogio,
            teto=self.limites.max_bytes_pagina,
        )
        if resposta.http_status != 200 or resposta.corpo is None:
            self._falha = (resposta.http_status, resposta.url_solicitada)
            raise SourceUnavailableError(
                source=self.plano.fonte,
                url=resposta.url_solicitada,
                last_error=f"HTTP {resposta.http_status} no catálogo",
            )
        return resposta.corpo

    def _despejar_pendente(self) -> None:
        pendente, self._pendente = self._pendente, None
        if pendente is not None and pendente.corpo is not None:
            nome = f"diagnostico/{pendente.pedido.papel}_{pendente.pedido.numero:06d}.bin"
            storage.gravar_privado(self.raiz, self.coleta_id, nome, pendente.corpo)

    def _conferir_resposta(
        self, resposta: models.RespostaCapturada, papeis: tuple[str, ...]
    ) -> bytes:
        if (
            resposta.pedido.papel not in papeis
            or resposta.http_status != 200
            or not resposta.completo
            or resposta.corpo is None
            or resposta.sha256 is None
            or resposta.tamanho is None
        ):
            raise self._violacao("registro de resposta incompleta ou de outro papel")
        return resposta.corpo

    def _nome_artefato(self, base: str, formato: str) -> str:
        sufixo = ".gz" if self.plano.opcoes.compactar else ""
        return f"{self.prefixo}/{base}.{storage.EXTENSOES[formato]}{sufixo}"

    def _total_antes(self) -> int | None:
        valores = [c.valor_declarado for c in self.controles if c.papel == "contagem_antes"]
        return valores[0] if valores and isinstance(valores[0], int) else None

    def registrar_pagina(
        self, resposta: models.RespostaCapturada, leitura: models.LeituraPagina
    ) -> models.PaginaBruta:
        self.conferir_prazo()
        corpo = self._conferir_resposta(resposta, ("pagina",))
        assert resposta.sha256 is not None and resposta.tamanho is not None
        pedido = resposta.pedido
        numero = len(self.paginas) + 1
        if (
            self.plano.modo != "paginado"
            or pedido.formato != self.plano.formato
            or pedido.paginacao is None
            or pedido.numero != numero
        ):
            raise self._violacao(
                f"página {pedido.numero} fora de ordem, sem paginação ou de outro formato"
            )
        if numero > self.limites.max_paginas:
            raise ResourceLimitError(
                self.plano.fonte, f"a coleta passou de max_paginas ({self.limites.max_paginas})"
            )
        if leitura.feicoes_recebidas != len(leitura.ids):
            raise self._divergencia(
                f"página {numero}: {leitura.feicoes_recebidas} feições e {len(leitura.ids)} IDs; "
                f"toda feição precisa de {self.plano.campo_id}"
            )
        if leitura.feicoes_recebidas and leitura.crs != self.plano.crs_esperado:
            raise self._divergencia(
                f"página {numero}: CRS {leitura.crs!r}, esperado {self.plano.crs_esperado!r}"
            )
        total = self._total_antes()
        if (
            isinstance(leitura.total_declarado, int)
            and total is not None
            and leitura.total_declarado != total
        ):
            raise self._divergencia(
                f"página {numero} declara {leitura.total_declarado} feições e a contagem é {total}"
            )
        self.conferir_ids(leitura.ids, papel="recebidos")
        relativo = self._nome_artefato(f"p{numero:06d}", pedido.formato)
        armazenado = storage.gravar_corpo(
            self.raiz, relativo, corpo, gzip_local=self.plano.opcoes.compactar
        )
        pagina = models.PaginaBruta(
            numero=numero,
            url_solicitada=resposta.url_solicitada,
            url=resposta.url,
            parametros=pedido.parametros,
            inicio=resposta.inicio,
            fim=resposta.fim,
            http_status=200,
            arquivo=relativo,
            sha256=resposta.sha256,
            bytes=resposta.tamanho,
            bytes_armazenados=armazenado,
            compressao="gzip" if self.plano.opcoes.compactar else "nenhuma",
            cabecalhos=resposta.cabecalhos,
            formato=pedido.formato,  # type: ignore[arg-type]
            paginacao=pedido.paginacao,
            feicoes_recebidas=leitura.feicoes_recebidas,
            ids_distintos=len(set(leitura.ids)),
            total_declarado=leitura.total_declarado,
            crs=leitura.crs,
        )
        self.paginas.append(pagina)
        self._pendente = None
        if (
            self.evidencia is None
            and leitura.feicoes_recebidas
            and leitura.crs_localizador
            and leitura.crs_valor
        ):
            self.evidencia = models.EvidenciaCRS(
                tipo="pagina",
                arquivo=relativo,
                localizador=leitura.crs_localizador,
                valor=leitura.crs_valor,
            )
        return pagina

    def registrar_controle(
        self, resposta: models.RespostaCapturada, leitura: models.LeituraControle
    ) -> models.ControleBruto:
        self.conferir_prazo()
        corpo = self._conferir_resposta(resposta, _PAPEIS_CONTROLE)
        pedido = resposta.pedido
        numero = len(self.controles) + 1
        if (
            self.plano.modo != "paginado"
            or pedido.papel != leitura.papel
            or pedido.formato not in ("json", "xml")
            or pedido.numero != numero
        ):
            raise self._violacao(
                f"controle {pedido.numero} fora de ordem ou de outro papel/formato"
            )
        contagem = leitura.papel in ("contagem_antes", "contagem_depois")
        if not contagem and leitura.valor_declarado is not None:
            raise self._violacao(f"controle {leitura.papel} não declara valor")
        if (
            contagem
            and isinstance(leitura.valor_declarado, int)
            and leitura.valor_declarado > self.limites.max_ids
        ):
            raise ResourceLimitError(
                self.plano.fonte,
                f"a fonte declara {leitura.valor_declarado} feições, acima de max_ids "
                f"({self.limites.max_ids}); reduza o recorte ou ajuste os limites",
            )
        relativo = self._nome_artefato(f"controles/c{numero:06d}", pedido.formato)
        armazenado = storage.gravar_corpo(
            self.raiz, relativo, corpo, gzip_local=self.plano.opcoes.compactar
        )
        assert resposta.sha256 is not None and resposta.tamanho is not None
        controle = models.ControleBruto(
            numero=numero,
            url_solicitada=resposta.url_solicitada,
            url=resposta.url,
            parametros=pedido.parametros,
            inicio=resposta.inicio,
            fim=resposta.fim,
            http_status=200,
            arquivo=relativo,
            sha256=resposta.sha256,
            bytes=resposta.tamanho,
            bytes_armazenados=armazenado,
            compressao="gzip" if self.plano.opcoes.compactar else "nenhuma",
            cabecalhos=resposta.cabecalhos,
            formato=pedido.formato,  # type: ignore[arg-type]
            papel=leitura.papel,
            valor_declarado=leitura.valor_declarado,
        )
        self.controles.append(controle)
        self._pendente = None
        return controle

    def registrar_arquivo(self, resposta: models.RespostaCapturada) -> None:
        self.conferir_prazo()
        if (
            self.plano.modo != "arquivo"
            or self.arquivo is not None
            or resposta.pedido.papel != "arquivo"
            or resposta.http_status != 200
            or not resposta.completo
            or resposta.temporario is None
            or resposta.tamanho is None
        ):
            raise self._violacao("registro de arquivo sem resposta 200 completa ou repetido")
        relativo = self.original
        armazenado = storage.publicar_arquivo(self.raiz, relativo, resposta.temporario)
        if armazenado != resposta.tamanho:
            raise self._violacao(
                f"{relativo}: {armazenado} bytes no disco e {resposta.tamanho} lidos"
            )
        self.arquivo = (relativo, resposta, armazenado)

    def conferir_ids(
        self,
        ids: tuple[models.IdFeicao, ...],
        *,
        papel: Literal["esperados", "recebidos"],
    ) -> None:
        for valor in ids:
            if isinstance(valor, bool) or not isinstance(valor, str | int) or valor == "":
                raise self._divergencia(f"{self.plano.campo_id} nulo ou inválido: {valor!r}")
        if papel == "esperados":
            if self.esperados is not None:
                raise self._violacao("a lista oficial de IDs já foi registrada")
            esperados = set(ids)
            if len(esperados) != len(ids):
                raise self._divergencia("a lista oficial de IDs tem ID repetido")
            self._conferir_quantidade(len(esperados))
            self.esperados = esperados
            self._bytes_chaves += sum(sys.getsizeof(valor) for valor in esperados)
        else:
            for valor in ids:
                if valor in self.recebidos:
                    self.repetidos += 1
                    continue
                self.recebidos.add(valor)
                self._bytes_chaves += sys.getsizeof(valor)
            self._conferir_quantidade(len(self.recebidos))
            if self.repetidos:
                raise self._divergencia(
                    f"{self.plano.campo_id} repetido entre feições recebidas ({self.repetidos} vez(es))"
                )
            if self.esperados is not None and not self.recebidos <= self.esperados:
                raise self._divergencia(f"{self.plano.campo_id} recebido fora da lista oficial")
        if self.memoria_ids > self.limites.max_bytes_ids:
            raise ResourceLimitError(
                self.plano.fonte,
                f"as estruturas de IDs passaram de max_bytes_ids ({self.limites.max_bytes_ids} bytes)",
            )

    def _conferir_quantidade(self, quantidade: int) -> None:
        if quantidade > self.limites.max_ids:
            raise ResourceLimitError(
                self.plano.fonte,
                f"{quantidade} IDs retidos, acima de max_ids ({self.limites.max_ids})",
            )

    async def executar(
        self, adaptador: protocols.AdaptadorBruto, entradas: list[models.RecursoBruto]
    ) -> models.RecursoBruto:
        checkpoint = {
            "fonte": self.plano.fonte,
            "recurso": self.plano.recurso,
            "nome": self.plano.nome,
            "consulta_id": self.consulta,
            "inicio": self.inicio,
        }
        storage.gravar_privado(
            self.raiz, self.coleta_id, "checkpoint.json", json.dumps(checkpoint).encode()
        )
        try:
            conclusao = await adaptador.adquirir(self.plano, contexto=self)
            entrada = self._concluir(conclusao)
            self.conferir_prazo()
            storage.gravar_manifesto(self.raiz, storage.substituir(entradas, entrada))
        except BaseException as exc:
            self._publicar_erro(exc, entradas)
            raise
        storage.apagar_privado(self.raiz, self.coleta_id, "checkpoint.json")
        return entrada

    def _concluir(self, conclusao: models.ConclusaoBruta) -> models.RecursoBruto:
        if not isinstance(conclusao, models.ConclusaoBruta):
            raise self._violacao("o adaptador não devolveu ConclusaoBruta")
        self._despejar_pendente()
        if conclusao.status == "ausente_na_fonte":
            resposta = self.ultimo_arquivo
            if (
                self.plano.modo != "arquivo"
                or self.arquivo is not None
                or resposta is None
                or resposta.http_status != 404
            ):
                raise self._violacao("ausente_na_fonte só vale depois de 404 no GET do arquivo")
            erro = models.ErroBruto(
                tipo="HTTP404",
                mensagem=f"a fonte respondeu 404 para o arquivo em {resposta.url_solicitada}",
                http_status=404,
                url=resposta.url_solicitada,
            )
            return self._entrada("ausente_na_fonte", erro, conclusao)
        if self.plano.modo == "arquivo":
            if self.arquivo is None:
                raise self._violacao("ok sem arquivo registrado")
        else:
            falhas = self._falhas_de_cobertura()
            if falhas:
                raise self._divergencia("; ".join(falhas))
        return self._entrada("ok", None, conclusao)

    def _falhas_de_cobertura(self) -> list[str]:
        antes = [c for c in self.controles if c.papel == "contagem_antes"]
        depois = [c for c in self.controles if c.papel == "contagem_depois"]
        if len(antes) != 1 or len(depois) != 1:
            return ["a cobertura exige uma contagem antes e uma depois das páginas"]
        total, final = antes[0].valor_declarado, depois[0].valor_declarado
        if not isinstance(total, int) or not isinstance(final, int):
            return ["contagem desconhecida não fecha a coleta (unknown não vale zero)"]
        falhas = []
        if self.paginas and not (
            antes[0].fim <= self.paginas[0].inicio and self.paginas[-1].fim <= depois[0].inicio
        ):
            falhas.append("as contagens não cercam as páginas (antes da 1ª e depois da última)")
        recebidas = sum(p.feicoes_recebidas for p in self.paginas)
        if not total == final == recebidas == len(self.recebidos):
            falhas.append(
                f"contagem antes {total}, depois {final}, recebidas {recebidas}, "
                f"IDs distintos {len(self.recebidos)}"
            )
        if self.repetidos:
            falhas.append(f"{self.repetidos} ID(s) repetido(s)")
        if any(c.papel == "ids" for c in self.controles) and self.esperados is None:
            falhas.append("controle de IDs registrado sem a lista oficial conferida")
        lista = (self.plano.fonte, self.plano.recurso) in models.LISTA_OFICIAL_DE_IDS
        if lista and (self.esperados is None or not any(c.papel == "ids" for c in self.controles)):
            falhas.append(
                "a lista oficial de IDs (controle e conferência) é obrigatória, inclusive vazia"
            )
        if self.esperados is not None and self.esperados != self.recebidos:
            falhas.append("IDs recebidos diferem da lista oficial")
        if any(
            isinstance(p.total_declarado, int) and p.total_declarado != total for p in self.paginas
        ):
            falhas.append("total declarado em página difere da contagem de controle")
        if total == 0 and self.paginas:
            falhas.append("zero feições confirmado não tem páginas")
        return falhas

    def _cobertura(self, status: models.Status) -> models.Cobertura:
        ok = status == "ok"
        if self.plano.modo == "arquivo":
            return models.Cobertura(
                total_antes=None,
                total_depois=None,
                recebidas=None,
                ids_distintos=None,
                ids_repetidos=None,
                campo_id=None,
                estado="nao_aplicavel",
                completa=ok,
                snapshot_transacional=False,
                controles=[],
            )

        def total(papel: str) -> int | None:
            valores = [c.valor_declarado for c in self.controles if c.papel == papel]
            return valores[0] if len(valores) == 1 and isinstance(valores[0], int) else None

        estado: models.EstadoCobertura = (
            "conferida" if ok else "divergente" if self.divergente else "nao_comprovada"
        )
        return models.Cobertura(
            total_antes=total("contagem_antes"),
            total_depois=total("contagem_depois"),
            recebidas=sum(p.feicoes_recebidas for p in self.paginas),
            ids_distintos=len(self.recebidos),
            ids_repetidos=self.repetidos,
            campo_id=self.plano.campo_id,
            estado=estado,
            completa=ok,
            snapshot_transacional=False,
            controles=[c.arquivo for c in self.controles if c.papel != "crs"],
        )

    def _evidencia(self, conclusao: models.ConclusaoBruta | None) -> models.EvidenciaCRS | None:
        if conclusao is not None and conclusao.crs_evidencia is not None:
            return conclusao.crs_evidencia
        return self.evidencia

    def _entrada(
        self,
        status: models.Status,
        erro: models.ErroBruto | None,
        conclusao: models.ConclusaoBruta | None,
    ) -> models.RecursoBruto:
        from agrobr import __version__

        fim = self.relogio.agora()
        evidencia = self._evidencia(conclusao)
        resposta = self.ultimo_arquivo if self.plano.modo == "arquivo" else None
        relativo, armazenado = (
            (self.arquivo[0], self.arquivo[2]) if self.arquivo is not None else (None, None)
        )
        recebido = self.arquivo[1] if self.arquivo is not None else None
        try:
            return models.RecursoBruto(
                schema_version=models.SCHEMA_VERSION,
                tipo="recurso",
                fonte=self.plano.fonte,
                recurso=self.plano.recurso,
                nome=self.plano.nome,
                consulta_id=self.consulta,
                coleta_id=self.coleta_id,
                status=status,
                inicio=self.inicio,
                fim=fim,
                registrado_em=self.relogio.agora(),
                url_solicitada=self.plano.url_solicitada,
                url=resposta.url if resposta is not None else self.plano.url_solicitada,
                parametros=self.plano.parametros,
                selecao=self.plano.selecao,
                opcoes=self.plano.opcoes,
                modo=self.plano.modo,
                formato=self.plano.formato,
                crs=None if evidencia is None else self.plano.crs_esperado,
                crs_evidencia=evidencia if self.plano.crs_esperado is not None else None,
                http_status=resposta.http_status if resposta is not None else None,
                http_inicio=resposta.inicio if resposta is not None else None,
                http_fim=resposta.fim if resposta is not None else None,
                arquivo=relativo,
                sha256=recebido.sha256 if recebido is not None else None,
                bytes=recebido.tamanho if recebido is not None else None,
                bytes_armazenados=armazenado,
                compressao="nenhuma",
                cabecalhos=resposta.cabecalhos if resposta is not None else {},
                paginas=self.paginas,
                controles=self.controles,
                feicoes=self._cobertura(status).total_antes
                if self.plano.modo == "paginado"
                else None,
                cobertura=self._cobertura(status),
                erro=erro,
                avisos=list(conclusao.avisos) if conclusao is not None else [],
                agrobr_version=__version__,
            )
        except ValidationError as exc:
            raise self._violacao(f"a entrada viola o contrato do manifesto: {exc}") from None

    def _publicar_erro(self, exc: BaseException, entradas: list[models.RecursoBruto]) -> None:
        tipo = _tipo_erro(exc)
        if tipo is None:
            return
        status_http, url = self._falha
        if tipo in ("ContractViolationError", "OSError", "CancelledError", "KeyboardInterrupt"):
            status_http, url = None, None
        if tipo == "ResourceLimitError":
            status_http, url = None, getattr(exc, "url", "") or url
        mensagem = (getattr(exc, "last_error", "") or str(exc) or type(exc).__name__).strip()
        try:
            self._despejar_pendente()
            storage.caminho(self.raiz, f"{self.original}.part").unlink(missing_ok=True)
            erro = models.ErroBruto(
                tipo=tipo,
                mensagem=mensagem,
                http_status=status_http,
                url=url if url and url.startswith("https://") else None,
            )
            entrada = self._entrada("erro", erro, None)
            storage.gravar_manifesto(self.raiz, storage.substituir(entradas, entrada))
            storage.apagar_privado(self.raiz, self.coleta_id, "checkpoint.json")
        except Exception as falha:
            logger.warning(
                "bruto_erro_nao_registrado",
                fonte=self.plano.fonte,
                recurso=self.plano.recurso,
                erro=f"{type(falha).__name__}: {falha}",
            )
