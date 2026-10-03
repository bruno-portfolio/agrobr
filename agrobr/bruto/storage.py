from __future__ import annotations

import gzip
import hashlib
import json
import os
import stat
import sys
import threading
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from pydantic import ValidationError

from agrobr import constants
from agrobr.bruto import models
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ResourceLimitError
from agrobr.utils.atomic import atomic_output

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

MANIFESTO = "manifesto.jsonl"
PRIVADO = ".bruto"
EXTENSOES = {
    "esri_json": "esri.json",
    "gml": "gml",
    "geojson": "geojson",
    "json": "json",
    "xml": "xml",
}
_BLOCO = 64 * 1024
_travados: set[str] = set()
_travados_lock = threading.Lock()


def preparar_destino(destino: object) -> Path:
    if not isinstance(destino, str | os.PathLike):
        raise InvalidParameterError(
            f"bruto: destino deve ser caminho (str ou PathLike), recebeu {type(destino).__name__}"
        )
    raiz = Path(os.fspath(destino)).absolute()
    if raiz.exists() and not raiz.is_dir():
        raise InvalidParameterError(f"bruto: destino {raiz} existe e não é pasta")
    raiz.mkdir(parents=True, exist_ok=True)
    return raiz


def _travar(fd: int) -> None:
    if sys.platform == "win32":
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _destravar(fd: int) -> None:
    if sys.platform == "win32":
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(fd, fcntl.LOCK_UN)


@contextmanager
def trava(raiz: Path) -> Iterator[None]:
    """Trava exclusiva do destino entre threads e processos; o sistema operacional a solta se o processo morrer."""
    chave = os.path.normcase(str(raiz.resolve()))
    ocupado = ResourceLimitError("bruto", f"outra coleta está gravando em {raiz}")
    with _travados_lock:
        if chave in _travados:
            raise ocupado
        _travados.add(chave)
    try:
        privado = raiz / PRIVADO
        privado.mkdir(exist_ok=True)
        fd = os.open(privado / "lock", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            try:
                _travar(fd)
            except OSError:
                raise ocupado from None
            try:
                yield
            finally:
                _destravar(fd)
        finally:
            os.close(fd)
    finally:
        with _travados_lock:
            _travados.discard(chave)


def _violacao(mensagem: str) -> ContractViolationError:
    return ContractViolationError("bruto", mensagem)


def ler_manifesto(raiz: Path) -> list[models.RecursoBruto]:
    """Lê e valida o manifesto inteiro; versão de schema que o gravador não sabe preservar é recusada antes da rede."""
    caminho = raiz / MANIFESTO
    if not caminho.exists():
        return []
    sem_link(raiz, caminho)
    teto = constants.BRUTO_MAX_BYTES_MANIFESTO
    with open(caminho, "rb") as arquivo:
        dados = arquivo.read(teto + 1)
    if len(dados) > teto:
        raise ResourceLimitError("bruto", f"manifesto acima de {teto} bytes: {caminho}")
    if not dados:
        return []
    try:
        texto = dados.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _violacao(f"manifesto não é UTF-8: {exc}") from None
    if texto.startswith("﻿") or "\r" in texto or not texto.endswith("\n"):
        raise _violacao("manifesto deve ser UTF-8 sem BOM, com LF e LF no fim da última linha")
    entradas: list[models.RecursoBruto] = []
    for numero, linha in enumerate(texto[:-1].split("\n"), start=1):
        try:
            bruto = json.loads(linha)
        except json.JSONDecodeError as exc:
            raise _violacao(f"linha {numero} do manifesto não é JSON: {exc}") from None
        versao = bruto.get("schema_version") if isinstance(bruto, dict) else None
        if versao != models.SCHEMA_VERSION:
            raise InvalidParameterError(
                f"bruto: linha {numero} do manifesto tem schema_version {versao!r}; esta versão do "
                f"agrobr só grava manifestos {models.SCHEMA_VERSION}. Use outro destino."
            )
        try:
            entradas.append(models.RecursoBruto.model_validate_json(linha))
        except ValidationError as exc:
            raise _violacao(f"linha {numero} do manifesto inválida: {exc}") from None
    chaves = [entrada.chave for entrada in entradas]
    if len(set(chaves)) != len(chaves):
        raise _violacao("manifesto com chave (fonte, recurso, nome) repetida")
    return entradas


def gravar_manifesto(raiz: Path, entradas: list[models.RecursoBruto]) -> None:
    dados = "".join(entrada.linha() + "\n" for entrada in entradas).encode("utf-8")
    teto = constants.BRUTO_MAX_BYTES_MANIFESTO
    if len(dados) > teto:
        raise ResourceLimitError("bruto", f"o manifesto passaria de {teto} bytes; nada foi gravado")
    with atomic_output(raiz / MANIFESTO) as temporario, open(temporario, "wb") as arquivo:
        arquivo.write(dados)
        arquivo.flush()
        os.fsync(arquivo.fileno())


def substituir(
    entradas: list[models.RecursoBruto], nova: models.RecursoBruto
) -> list[models.RecursoBruto]:
    resultado = [nova if entrada.chave == nova.chave else entrada for entrada in entradas]
    return resultado if nova.chave in {e.chave for e in entradas} else [*resultado, nova]


def _ponto_de_reparse(caminho: Path) -> bool:
    try:
        atributos = getattr(os.lstat(caminho), "st_file_attributes", 0)
    except FileNotFoundError:
        return False
    return bool(atributos & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def sem_link(raiz: Path, alvo: Path) -> None:
    atual = raiz
    for parte in alvo.relative_to(raiz).parts:
        atual = atual / parte
        if atual.is_symlink() or _ponto_de_reparse(atual):
            raise _violacao(f"{atual} é link ou junção; o bruto não segue links dentro do destino")


def caminho(raiz: Path, relativo: str) -> Path:
    alvo = raiz.joinpath(*relativo.split("/"))
    sem_link(raiz, alvo)
    return alvo


def gravar_corpo(raiz: Path, relativo: str, corpo: bytes, *, gzip_local: bool) -> int:
    """Grava em ``.part`` e publica com ``os.replace`` depois de fechado; devolve ``bytes_armazenados``."""
    alvo = caminho(raiz, relativo)
    alvo.parent.mkdir(parents=True, exist_ok=True)
    parcial = alvo.with_name(alvo.name + ".part")
    with open(parcial, "wb") as arquivo:
        if gzip_local:
            with gzip.GzipFile(filename="", mode="wb", fileobj=arquivo, mtime=0) as compactado:
                compactado.write(corpo)
        else:
            arquivo.write(corpo)
        arquivo.flush()
        os.fsync(arquivo.fileno())
    os.replace(parcial, alvo)
    return alvo.stat().st_size


def publicar_arquivo(raiz: Path, relativo: str, temporario: Path) -> int:
    alvo = caminho(raiz, relativo)
    os.replace(temporario, alvo)
    return alvo.stat().st_size


def gravar_privado(raiz: Path, coleta_id: str, relativo: str, dados: bytes) -> None:
    alvo = raiz / PRIVADO / coleta_id / relativo
    alvo.parent.mkdir(parents=True, exist_ok=True)
    alvo.write_bytes(dados)


def apagar_privado(raiz: Path, coleta_id: str, relativo: str) -> None:
    (raiz / PRIVADO / coleta_id / relativo).unlink(missing_ok=True)


def verificar(
    raiz: Path,
    relativo: str,
    *,
    compressao: str,
    tamanho: int,
    armazenado: int,
    sha256: str,
    teto: int,
    conferir_prazo: Callable[[], None],
) -> None:
    """Confere tamanho guardado, tamanho original e hash de um artefato, lendo o gzip em blocos limitados."""
    if tamanho > teto:
        raise ResourceLimitError(
            "bruto", f"{relativo}: conferir {tamanho} bytes passa do limite de {teto}"
        )
    alvo = caminho(raiz, relativo)
    if not alvo.is_file():
        raise _violacao(f"{relativo}: artefato não existe")
    if alvo.stat().st_size != armazenado:
        raise _violacao(f"{relativo}: tamanho guardado difere de bytes_armazenados")
    hasher = hashlib.sha256()
    lidos = 0
    try:
        with open(alvo, "rb") as arquivo:
            leitor = gzip.GzipFile(fileobj=arquivo, mode="rb") if compressao == "gzip" else arquivo
            while bloco := leitor.read(_BLOCO):
                lidos += len(bloco)
                if lidos > tamanho:
                    raise _violacao(f"{relativo}: corpo maior que bytes declarado")
                hasher.update(bloco)
                conferir_prazo()
    except (OSError, EOFError, zlib.error) as exc:
        raise _violacao(f"{relativo}: gzip ilegível ({type(exc).__name__}: {exc})") from None
    if lidos != tamanho or hasher.hexdigest() != sha256:
        raise _violacao(f"{relativo}: tamanho ou sha256 difere do manifesto")
