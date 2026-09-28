from __future__ import annotations

import asyncio
import json
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from agrobr.alt.antt_pedagio import acquisition, client
from agrobr.exceptions import ParseError


class CatalogFile:
    def __init__(
        self,
        body: bytes,
        *,
        read_error: BaseException | None = None,
        close_error: OSError | None = None,
    ) -> None:
        self.body, self.read_error, self.close_error = body, read_error, close_error
        self.closed = False
        self.close_attempts = 0

    def read(self) -> bytes:
        if self.read_error is not None:
            raise self.read_error
        return self.body

    def close(self) -> None:
        self.close_attempts += 1
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class TestCatalogCloseIntegrity(unittest.TestCase):
    def setUp(self) -> None:
        self.bundle = acquisition.TrafegoAcquisition([], None)
        self.bundle.attempts.append(SimpleNamespace(sha256="received", size_bytes=77))
        self.close_error = OSError("catalog spool close failed")
        self.started = datetime.now(UTC)

    def discover(self, file: CatalogFile) -> Any:
        async def fetch(*_args: Any, **_kwargs: Any) -> tuple[CatalogFile, int]:
            return file, 0

        transport = SimpleNamespace(fetch=fetch, bundle=self.bundle)
        return asyncio.run(client._discover(None, transport, "expected-package"))

    def assert_close_failure(self, file: CatalogFile) -> None:
        self.assertEqual(file.close_attempts, 1)
        self.assertFalse(file.closed)
        self.assertEqual(len(self.bundle.spool_close_errors), 1)
        detail = self.bundle.spool_close_errors[0]
        self.assertEqual(detail["role"], "catalog")
        self.assertEqual(detail["resource_index"], 0)
        self.assertIsNone(detail["file_index"])
        self.assertIsNone(detail["resource_id"])
        self.assertEqual(detail["error_type"], "OSError")
        self.assertLessEqual(self.started, datetime.fromisoformat(detail["at"]))
        self.assertLessEqual(datetime.fromisoformat(detail["at"]), datetime.now(UTC))
        self.assertEqual(self.bundle.catalog_sha256, "received")

    def body(self, name: str = "expected-package") -> bytes:
        return json.dumps(
            {"success": True, "result": {"id": "synthetic", "name": name, "resources": []}}
        ).encode()

    def test_invalid_identity_preserves_normalized_error_when_close_fails(self):
        file = CatalogFile(self.body("another-package"), close_error=self.close_error)
        with self.assertRaises(ParseError) as caught:
            self.discover(file)
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assert_close_failure(file)

    def test_read_error_remains_primary(self):
        primary = OSError("catalog read failed")
        file = CatalogFile(b"", read_error=primary, close_error=self.close_error)
        with self.assertRaises(OSError) as caught:
            self.discover(file)
        self.assertIs(caught.exception, primary)
        self.assert_close_failure(file)

    def test_valid_catalog_propagates_close_error(self):
        file = CatalogFile(self.body(), close_error=self.close_error)
        with self.assertRaises(OSError) as caught:
            self.discover(file)
        self.assertIs(caught.exception, self.close_error)
        self.assertEqual(self.bundle.catalog.name, "expected-package")
        self.assert_close_failure(file)
