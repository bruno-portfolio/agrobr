"""Sistema de plugins para extensibilidade do agrobr."""

from __future__ import annotations

import importlib
import importlib.util
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

import structlog

logger = structlog.get_logger()

T = TypeVar("T", bound="Plugin")

_registry: dict[str, type[Plugin]] = {}
_instances: dict[str, Plugin] = {}


@dataclass
class PluginMeta:
    name: str
    version: str
    description: str
    author: str = ""
    requires: list[str] = field(default_factory=list)
    provides: list[str] = field(default_factory=list)


class Plugin(ABC):
    meta: PluginMeta

    @abstractmethod
    def setup(self) -> None:
        pass

    @abstractmethod
    def teardown(self) -> None:
        pass

    def is_enabled(self) -> bool:
        return True


class SourcePlugin(Plugin):
    @abstractmethod
    async def fetch(self, **kwargs: Any) -> Any:
        pass

    @abstractmethod
    async def parse(self, content: Any, **kwargs: Any) -> Any:
        pass

    def get_source_name(self) -> str:
        return self.meta.name


class ParserPlugin(Plugin):
    @abstractmethod
    def can_parse(self, content: str) -> bool:
        pass

    @abstractmethod
    def parse(self, content: str, **kwargs: Any) -> Any:
        pass

    @property
    def priority(self) -> int:
        return 0


class ExporterPlugin(Plugin):
    @abstractmethod
    def export(self, data: Any, path: Path, **kwargs: Any) -> Path:
        pass

    @abstractmethod
    def get_extension(self) -> str:
        pass


class ValidatorPlugin(Plugin):
    @abstractmethod
    def validate(self, data: Any, **kwargs: Any) -> tuple[bool, list[str]]:
        pass


def register(plugin_class: type[T]) -> type[T]:
    if not hasattr(plugin_class, "meta"):
        raise ValueError(f"Plugin {plugin_class.__name__} must have 'meta' attribute")

    name = plugin_class.meta.name
    if name in _registry:
        logger.warning("plugin_override", name=name, old=_registry[name].__name__, new=plugin_class.__name__)

    _registry[name] = plugin_class
    logger.info("plugin_registered", name=name, version=plugin_class.meta.version)
    return plugin_class


def get_plugin(name: str) -> Plugin | None:
    if name in _instances:
        return _instances[name]

    if name not in _registry:
        return None

    plugin_class = _registry[name]
    instance = plugin_class()
    instance.setup()
    _instances[name] = instance
    return instance


def list_plugins() -> list[PluginMeta]:
    return [cls.meta for cls in _registry.values()]


def list_plugins_by_type(plugin_type: type[Plugin]) -> list[PluginMeta]:
    return [cls.meta for cls in _registry.values() if issubclass(cls, plugin_type)]


def load_plugin_from_file(path: Path) -> type[Plugin] | None:
    if not path.exists():
        logger.error("plugin_file_not_found", path=str(path))
        return None

    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        logger.error("plugin_spec_failed", path=str(path))
        return None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (
            isinstance(attr, type)
            and issubclass(attr, Plugin)
            and attr is not Plugin
            and hasattr(attr, "meta")
        ):
            register(attr)
            return attr

    logger.warning("no_plugin_found", path=str(path))
    return None


def load_plugins_from_dir(directory: Path) -> list[type[Plugin]]:
    loaded = []
    if not directory.exists():
        return loaded

    for path in directory.glob("*.py"):
        if path.name.startswith("_"):
            continue
        plugin_class = load_plugin_from_file(path)
        if plugin_class:
            loaded.append(plugin_class)

    return loaded


def unload_plugin(name: str) -> bool:
    if name in _instances:
        _instances[name].teardown()
        del _instances[name]

    if name in _registry:
        del _registry[name]
        logger.info("plugin_unloaded", name=name)
        return True

    return False


def unload_all() -> None:
    for name in list(_instances.keys()):
        _instances[name].teardown()
    _instances.clear()
    _registry.clear()


__all__ = [
    "Plugin",
    "PluginMeta",
    "SourcePlugin",
    "ParserPlugin",
    "ExporterPlugin",
    "ValidatorPlugin",
    "register",
    "get_plugin",
    "list_plugins",
    "list_plugins_by_type",
    "load_plugin_from_file",
    "load_plugins_from_dir",
    "unload_plugin",
    "unload_all",
]
