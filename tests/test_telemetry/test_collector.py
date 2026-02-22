from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.telemetry.collector import (
    TelemetryCollector,
    track_cache_operation,
    track_fetch,
    track_parse_error,
)


@pytest.fixture(autouse=True)
def _reset_collector():
    TelemetryCollector.reset()
    yield
    TelemetryCollector.reset()


class TestGetInstanceId:
    def test_returns_string(self):
        instance_id = TelemetryCollector.get_instance_id()
        assert isinstance(instance_id, str)
        assert len(instance_id) == 16

    def test_deterministic(self):
        id1 = TelemetryCollector.get_instance_id()
        id2 = TelemetryCollector.get_instance_id()
        assert id1 == id2


class TestGetContext:
    def test_contains_required_fields(self):
        ctx = TelemetryCollector.get_context()
        assert "instance_id" in ctx
        assert "package_version" in ctx
        assert "python_version" in ctx
        assert "os" in ctx
        assert "timestamp" in ctx

    def test_instance_id_matches(self):
        ctx = TelemetryCollector.get_context()
        assert ctx["instance_id"] == TelemetryCollector.get_instance_id()


class TestTrack:
    @pytest.mark.asyncio
    async def test_disabled_does_not_buffer(self):
        with patch("agrobr.telemetry.collector.TelemetrySettings") as mock_settings:
            mock_settings.return_value.enabled = False
            await TelemetryCollector.track("test_event", {"key": "val"})
        assert len(TelemetryCollector._buffer) == 0

    @pytest.mark.asyncio
    async def test_enabled_adds_to_buffer(self):
        with patch("agrobr.telemetry.collector.TelemetrySettings") as mock_settings:
            mock_settings.return_value.enabled = True
            mock_settings.return_value.batch_size = 100
            await TelemetryCollector.track("test_event", {"key": "val"})
        assert len(TelemetryCollector._buffer) == 1
        assert TelemetryCollector._buffer[0]["event"] == "test_event"

    @pytest.mark.asyncio
    async def test_auto_flush_on_batch_size(self):
        with (
            patch("agrobr.telemetry.collector.TelemetrySettings") as mock_settings,
            patch.object(TelemetryCollector, "_flush", new_callable=AsyncMock),
            patch("asyncio.create_task") as mock_create_task,
        ):
            mock_settings.return_value.enabled = True
            mock_settings.return_value.batch_size = 2
            await TelemetryCollector.track("e1")
            await TelemetryCollector.track("e2")
            mock_create_task.assert_called()


class TestFlush:
    @pytest.mark.asyncio
    async def test_empty_buffer_noop(self):
        await TelemetryCollector._flush()

    @pytest.mark.asyncio
    async def test_sends_payload(self):
        TelemetryCollector._buffer = [{"event": "test", "context": {}, "properties": {}}]

        mock_response = MagicMock()
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.telemetry.collector.httpx.AsyncClient", return_value=mock_client):
            await TelemetryCollector._flush()

        mock_client.post.assert_called_once()
        assert len(TelemetryCollector._buffer) == 0

    @pytest.mark.asyncio
    async def test_network_error_logged(self):
        TelemetryCollector._buffer = [{"event": "test", "context": {}, "properties": {}}]

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("connection refused"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("agrobr.telemetry.collector.httpx.AsyncClient", return_value=mock_client):
            await TelemetryCollector._flush()

        assert len(TelemetryCollector._buffer) == 0


class TestReset:
    def test_clears_buffer_and_id(self):
        TelemetryCollector._buffer = [{"event": "x"}]
        TelemetryCollector._instance_id = "abc"
        TelemetryCollector.reset()
        assert TelemetryCollector._buffer == []
        assert TelemetryCollector._instance_id is None


class TestConveniences:
    @pytest.mark.asyncio
    async def test_track_fetch(self):
        with patch("agrobr.telemetry.collector.TelemetrySettings") as mock_settings:
            mock_settings.return_value.enabled = True
            mock_settings.return_value.batch_size = 100
            await track_fetch("cepea", "soja", 150.0, False)
        assert len(TelemetryCollector._buffer) == 1
        assert TelemetryCollector._buffer[0]["event"] == "fetch"

    @pytest.mark.asyncio
    async def test_track_parse_error(self):
        with patch("agrobr.telemetry.collector.TelemetrySettings") as mock_settings:
            mock_settings.return_value.enabled = True
            mock_settings.return_value.batch_size = 100
            await track_parse_error("cepea", 1, "ValueError")
        assert len(TelemetryCollector._buffer) == 1
        assert TelemetryCollector._buffer[0]["event"] == "parse_error"

    @pytest.mark.asyncio
    async def test_track_cache_operation(self):
        with patch("agrobr.telemetry.collector.TelemetrySettings") as mock_settings:
            mock_settings.return_value.enabled = True
            mock_settings.return_value.batch_size = 100
            await track_cache_operation("get", True)
        assert len(TelemetryCollector._buffer) == 1
        assert TelemetryCollector._buffer[0]["event"] == "cache"
