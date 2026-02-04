"""Tests for plugins module."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from agrobr.plugins import (
    ExporterPlugin,
    ParserPlugin,
    Plugin,
    PluginMeta,
    SourcePlugin,
    ValidatorPlugin,
    get_plugin,
    list_plugins,
    list_plugins_by_type,
    register,
    unload_all,
    unload_plugin,
)


class TestPluginMeta:
    def test_meta_creation(self):
        meta = PluginMeta(
            name="test-plugin",
            version="1.0.0",
            description="Test plugin",
            author="Test Author",
        )
        assert meta.name == "test-plugin"
        assert meta.version == "1.0.0"
        assert meta.requires == []
        assert meta.provides == []

    def test_meta_with_dependencies(self):
        meta = PluginMeta(
            name="dependent-plugin",
            version="1.0.0",
            description="Plugin with deps",
            requires=["other-plugin"],
            provides=["feature-x"],
        )
        assert "other-plugin" in meta.requires
        assert "feature-x" in meta.provides


class TestPluginRegistry:
    def setup_method(self):
        unload_all()

    def teardown_method(self):
        unload_all()

    def test_register_plugin(self):
        @register
        class TestPlugin(Plugin):
            meta = PluginMeta(name="test", version="1.0", description="Test")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

        plugins = list_plugins()
        assert any(p.name == "test" for p in plugins)

    def test_get_plugin(self):
        @register
        class MyPlugin(Plugin):
            meta = PluginMeta(name="my-plugin", version="1.0", description="My plugin")

            def setup(self) -> None:
                self.initialized = True

            def teardown(self) -> None:
                pass

        plugin = get_plugin("my-plugin")
        assert plugin is not None
        assert hasattr(plugin, "initialized")
        assert plugin.initialized is True

    def test_get_plugin_not_found(self):
        plugin = get_plugin("nonexistent")
        assert plugin is None

    def test_unload_plugin(self):
        @register
        class ToUnload(Plugin):
            meta = PluginMeta(name="to-unload", version="1.0", description="Will be unloaded")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

        assert any(p.name == "to-unload" for p in list_plugins())
        unload_plugin("to-unload")
        assert not any(p.name == "to-unload" for p in list_plugins())


class TestPluginTypes:
    def setup_method(self):
        unload_all()

    def teardown_method(self):
        unload_all()

    def test_source_plugin(self):
        @register
        class MySource(SourcePlugin):
            meta = PluginMeta(name="my-source", version="1.0", description="Source plugin")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

            async def fetch(self, **_kwargs: Any) -> Any:
                return "data"

            async def parse(self, content: Any, **_kwargs: Any) -> Any:
                return {"parsed": content}

        sources = list_plugins_by_type(SourcePlugin)
        assert len(sources) == 1
        assert sources[0].name == "my-source"

    def test_parser_plugin(self):
        @register
        class MyParser(ParserPlugin):
            meta = PluginMeta(name="my-parser", version="1.0", description="Parser plugin")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

            def can_parse(self, content: str) -> bool:
                return "special" in content

            def parse(self, content: str, **_kwargs: Any) -> Any:
                return {"content": content}

            @property
            def priority(self) -> int:
                return 10

        parsers = list_plugins_by_type(ParserPlugin)
        assert len(parsers) == 1

        plugin = get_plugin("my-parser")
        assert plugin is not None
        assert plugin.priority == 10

    def test_exporter_plugin(self):
        @register
        class MyExporter(ExporterPlugin):
            meta = PluginMeta(name="my-exporter", version="1.0", description="Exporter")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

            def export(self, _data: Any, path: Path, **_kwargs: Any) -> Path:
                return path

            def get_extension(self) -> str:
                return ".custom"

        exporters = list_plugins_by_type(ExporterPlugin)
        assert len(exporters) == 1

        plugin = get_plugin("my-exporter")
        assert plugin is not None
        assert plugin.get_extension() == ".custom"

    def test_validator_plugin(self):
        @register
        class MyValidator(ValidatorPlugin):
            meta = PluginMeta(name="my-validator", version="1.0", description="Validator")

            def setup(self) -> None:
                pass

            def teardown(self) -> None:
                pass

            def validate(self, _data: Any, **_kwargs: Any) -> tuple[bool, list[str]]:
                return True, []

        validators = list_plugins_by_type(ValidatorPlugin)
        assert len(validators) == 1

        plugin = get_plugin("my-validator")
        assert plugin is not None
        valid, errors = plugin.validate({"test": "data"})
        assert valid is True
        assert errors == []
