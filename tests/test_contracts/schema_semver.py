from __future__ import annotations

import json
import pathlib
import re
import subprocess
import typing

Schema = dict[str, typing.Any]
VersionKey = tuple[int, int, int, bool, tuple[tuple[int, int | str], ...]]

_VERSION = re.compile(
    r"(0|[1-9]\d*)\.(0|[1-9]\d*)(?:\.(0|[1-9]\d*))?"
    r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
)
_WIDENINGS = {("int", "float"), ("int", "Decimal"), ("date", "datetime")}


class BaselineUnavailable(RuntimeError):
    pass


def version_key(version: str) -> VersionKey:
    match = _VERSION.fullmatch(version)
    if match is None:
        raise ValueError(f"Versão de contrato/tag inválida: {version!r}")
    major, minor, patch, prerelease = match.groups()
    identifiers = prerelease.split(".") if prerelease else []
    if any(part.isdigit() and len(part) > 1 and part.startswith("0") for part in identifiers):
        raise ValueError(f"Pré-lançamento inválido: {version!r}")
    return (
        int(major),
        int(minor),
        int(patch or "0"),
        prerelease is None,
        tuple((0, int(part)) if part.isdigit() else (1, part) for part in identifiers),
    )


def run_git(root: pathlib.Path, *arguments: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *arguments],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=30,
        )
    except FileNotFoundError as exc:
        raise BaselineUnavailable("Gate semver: executável git indisponível.") from exc
    return result.stdout


def previous_tag(root: pathlib.Path) -> str:
    root.stat()
    try:
        (root / ".git").lstat()
    except FileNotFoundError as exc:
        raise BaselineUnavailable("Gate semver: checkout sem .git (por exemplo, sdist).") from exc
    checkout = pathlib.Path(run_git(root, "rev-parse", "--show-toplevel").strip())
    if checkout.resolve() != root.resolve():
        raise ValueError(f"Checkout git inesperado: {checkout}; esperado: {root}")
    head = run_git(root, "rev-parse", "--verify", "HEAD").strip()
    tags = run_git(root, "tag", "--merged", "HEAD", "--list", "v*").splitlines()
    versions = []
    for tag in tags:
        try:
            key = version_key(tag.removeprefix("v"))
        except ValueError:
            continue
        versions.append((key, tag))
    for _, tag in sorted(versions, reverse=True):
        commit = run_git(root, "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}").strip()
        if commit != head:
            return tag
    raise BaselineUnavailable("Gate semver: nenhuma tag local v* anterior e alcançável de HEAD.")


def parse_schema(text: str, source: str) -> Schema:
    schema = json.loads(text)
    if not isinstance(schema, dict):
        raise ValueError(f"{source}: schema deve ser um objeto JSON")
    version_key(schema["schema_version"])
    if not isinstance(schema["primary_key"], list) or not all(
        isinstance(name, str) for name in schema["primary_key"]
    ):
        raise ValueError(f"{source}: primary_key inválida")
    columns = schema["columns"]
    if not isinstance(columns, list):
        raise ValueError(f"{source}: columns deve ser uma lista")
    names = []
    for column in columns:
        if (
            not isinstance(column["name"], str)
            or not isinstance(column["type"], str)
            or not isinstance(column["nullable"], bool)
        ):
            raise ValueError(f"{source}: definição de coluna inválida")
        names.append(column["name"])
    if len(set(names)) != len(names):
        raise ValueError(f"{source}: nomes de colunas duplicados")
    for field, attribute in (("dtypes", "type"), ("nullable", "nullable")):
        expected = {column["name"]: column[attribute] for column in columns}
        if schema[field] != expected:
            raise ValueError(f"{source}: {field} diverge de columns")
    return schema


def tag_schemas(root: pathlib.Path, tag: str) -> dict[str, Schema]:
    paths = run_git(
        root,
        "ls-tree",
        "-r",
        "--name-only",
        "--full-tree",
        f"refs/tags/{tag}",
        "--",
        "agrobr/schemas/",
    ).splitlines()
    schemas = {}
    for path in paths:
        relative = pathlib.Path(path)
        if relative.parent.as_posix() != "agrobr/schemas" or relative.suffix != ".json":
            continue
        source = f"refs/tags/{tag}:{path}"
        schemas[relative.name] = parse_schema(run_git(root, "show", source), source)
    if not schemas:
        raise ValueError(f"Tag {tag}: nenhum agrobr/schemas/*.json encontrado")
    return schemas


def checkout_schemas(root: pathlib.Path) -> dict[str, Schema]:
    directory = root / "agrobr" / "schemas"
    directory.stat()
    if not directory.is_dir():
        raise NotADirectoryError(str(directory))
    schemas = {
        path.name: parse_schema(path.read_text(encoding="utf-8"), str(path))
        for path in sorted(directory.glob("*.json"))
    }
    if not schemas:
        raise ValueError(f"{directory}: nenhum schema JSON encontrado")
    return schemas


def breaking_changes(previous: dict[str, Schema], current: dict[str, Schema]) -> list[str]:
    errors = []
    for filename in sorted(previous.keys() & current.keys()):
        old, new = previous[filename], current[filename]
        old_version, new_version = old["schema_version"], new["schema_version"]
        if version_key(new_version)[0] > version_key(old_version)[0]:
            continue
        prefix = f"{filename} ({old_version} -> {new_version}, sem aumento de major)"
        if old["primary_key"] != new["primary_key"]:
            errors.append(f"{prefix}: primary_key {old['primary_key']} -> {new['primary_key']}")
        old_columns = {column["name"]: column for column in old["columns"]}
        new_columns = {column["name"]: column for column in new["columns"]}
        for name, old_column in old_columns.items():
            if name not in new_columns:
                errors.append(f"{prefix}: coluna removida: {name}")
                continue
            new_column = new_columns[name]
            types = old_column["type"], new_column["type"]
            if types[0] != types[1] and types not in _WIDENINGS:
                errors.append(f"{prefix}: {name}: tipo incompatível {types[0]} -> {types[1]}")
            if old_column["nullable"] is False and new_column["nullable"] is True:
                errors.append(f"{prefix}: {name}: nullable False -> True")
    return errors


def check_checkout(root: pathlib.Path) -> tuple[str, list[str]]:
    tag = previous_tag(root)
    return tag, breaking_changes(tag_schemas(root, tag), checkout_schemas(root))
