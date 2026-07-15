"""Integration-style tests for the Splunk journal decoder."""

from __future__ import annotations

import io
import struct
import unittest
from unittest import mock

from frosty.splunk_journal.decoder import (
    JournalDecoder,
    Opcode,
    require_uvarint_from_bytes,
)


def _header(index_time: int = 1_700_000_000) -> bytes:
    # Opcode.HEADER + 6-byte payload (version, pad, index_time LE)
    return bytes([Opcode.HEADER, 1, 0]) + struct.pack("<i", index_time)


def _oldstyle_event(*, message: bytes = b"hi", stream_id: int = 1) -> bytes:
    """Minimal OLDSTYLE_EVENT (opcode 1): no hash, no extended storage, no metadata.

    The leading message_length varint encodes (bytes remaining after that varint
    through end of message), not the body size alone — see JournalDecoder._decode_event.
    """
    tail = bytearray()
    tail.extend(struct.pack("<Q", stream_id))
    tail.extend([0, 0, 0])  # stream_offset, stream_sub_offset, index_time_diff
    tail.append(0)  # time_sub_seconds (shifted varint)
    tail.append(0)  # metadata_count
    tail.extend(message)
    # message_length varint = len(tail)
    return bytes([Opcode.OLDSTYLE_EVENT, len(tail)]) + bytes(tail)


def _length_prefixed(opcode: int, payload: bytes = b"\x00\x01") -> bytes:
    return bytes([opcode, len(payload)]) + payload


class RequireVarintTests(unittest.TestCase):
    def test_require_uvarint_rejects_truncated(self) -> None:
        with self.assertRaises(ValueError):
            require_uvarint_from_bytes(bytes([0x80, 0x80]), 0)


class DecoderOpcodeIntegrationTests(unittest.TestCase):
    def _scan_all(self, raw: bytes, *, skip_metadata: bool = False) -> list[bytes]:
        decoder = JournalDecoder(io.BytesIO(raw), skip_metadata=skip_metadata)
        messages: list[bytes] = []
        while decoder.scan():
            messages.append(decoder.get_event().message)
        self.assertIsNone(decoder.err())
        return messages

    def test_delete_between_events_does_not_abort(self) -> None:
        raw = (
            _header()
            + _oldstyle_event(message=b"one", stream_id=1)
            + _length_prefixed(Opcode.DELETE, b"dead")
            + _oldstyle_event(message=b"two", stream_id=2)
        )
        messages = self._scan_all(raw)
        self.assertEqual(messages, [b"one", b"two"])

    def test_hash_slice_is_skipped(self) -> None:
        raw = (
            _header()
            + _length_prefixed(Opcode.HASH_SLICE, b"x" * 8)
            + _oldstyle_event(message=b"ok", stream_id=3)
        )
        self.assertEqual(self._scan_all(raw), [b"ok"])

    def test_truncated_varint_in_event_sets_error(self) -> None:
        # Opcode + incomplete multi-byte varint (continuation bits set).
        raw = _header() + bytes([Opcode.OLDSTYLE_EVENT, 0x80, 0x80])
        decoder = JournalDecoder(io.BytesIO(raw))
        self.assertFalse(decoder.scan())
        self.assertIsNotNone(decoder.err())
        self.assertIn("truncated", str(decoder.err()).lower())

    def test_skip_metadata_raises_on_truncated_entry(self) -> None:
        decoder = JournalDecoder(reader=mock.Mock(), skip_metadata=True)
        decoder.opcode = 2
        with self.assertRaises(ValueError):
            # Incomplete first metadata entry (continuation-only byte).
            decoder._skip_metadata_bytes(bytes([0x80]), 1)

    def test_decode_metadata_raises_instead_of_plus_one(self) -> None:
        decoder = JournalDecoder(reader=mock.Mock(), skip_metadata=False)
        decoder.opcode = 2
        decoder.event.metadata_count = 1
        with self.assertRaises(ValueError):
            decoder.decode_metadata(bytes([0x80]))


class SetDefaultPipelineTests(unittest.TestCase):
    def test_ensure_index_called_before_settings(self) -> None:
        from frosty import elastic

        calls: list[str] = []

        def _fake_ensure(*_a, **_k):
            calls.append("ensure")

        def _fake_request(*_a, **_k):
            calls.append("settings")
            return 200, {}

        with mock.patch.object(elastic, "ensure_index", side_effect=_fake_ensure):
            with mock.patch.object(elastic, "elastic_request", side_effect=_fake_request):
                elastic.set_default_pipeline(
                    "https://example.es.io",
                    "key",
                    "frosty-apache-test",
                    "frosty-router-apache",
                )
        self.assertEqual(calls, ["ensure", "settings"])


if __name__ == "__main__":
    unittest.main()
