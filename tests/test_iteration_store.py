"""Tests for locked ingest iteration claims."""

from __future__ import annotations

import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from frosty.iteration import IngestIterationStore, bump_iteration


class IngestIterationStoreTests(unittest.TestCase):
    def test_claim_advances_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = IngestIterationStore(Path(tmp) / ".frosty-iteration-next", initial="2.0")
            self.assertEqual(store.claim(), "2.0")
            self.assertEqual(store.peek(), "2.1")
            self.assertEqual(store.claim(), "2.1")
            self.assertEqual(store.peek(), "2.2")

    def test_reset_is_locked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = IngestIterationStore(Path(tmp) / ".frosty-iteration-next", initial="3.0")
            self.assertEqual(store.reset("5.0"), "5.0")
            self.assertEqual(store.peek(), "5.0")
            self.assertEqual(store.claim(), "5.0")
            self.assertEqual(store.peek(), "5.1")

    def test_concurrent_claims_are_unique(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = IngestIterationStore(Path(tmp) / ".frosty-iteration-next", initial="1.0")
            claimed: list[str] = []
            lock = threading.Lock()

            def worker() -> None:
                value = store.claim()
                with lock:
                    claimed.append(value)

            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(lambda _: worker(), range(20)))

            self.assertEqual(len(claimed), 20)
            self.assertEqual(len(set(claimed)), 20)
            expected = []
            current = "1.0"
            for _ in range(20):
                expected.append(current)
                current = bump_iteration(current)
            self.assertEqual(sorted(claimed), sorted(expected))
            self.assertEqual(store.peek(), bump_iteration(expected[-1]))


if __name__ == "__main__":
    unittest.main()
