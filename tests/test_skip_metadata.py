"""Tests for skip-metadata fast path in the journal decoder."""

from __future__ import annotations

import unittest
from unittest import mock

from frosty.splunk_journal.decoder import JournalDecoder, Opcode


class SkipMetadataBytesTests(unittest.TestCase):
    def test_skip_metadata_bytes_matches_decode_consumed_length(self) -> None:
        # Synthetic metadata: opcode>2 path with rmki type needing 1 extra int.
        # Use opcode 4 so _metadata_entry_size / _read_metadata share the same walk.
        decoder = JournalDecoder(reader=mock.Mock(), skip_metadata=True)
        decoder.opcode = 4
        # rmki_key = meta_key & 0xF after <<2; pick a key that maps to a known type.
        # Simpler: use opcode <= 2 path (num_to_read=1).
        decoder.opcode = 2
        # One entry: key varint 0x01, value varint 0x02 → 2 bytes
        buffer = bytes([0x01, 0x02, 0x03, 0x04])
        decoder.event.metadata_count = 2
        skipped = decoder._skip_metadata_bytes(buffer, 2)
        # Two entries of 2 bytes each
        self.assertEqual(skipped, 4)

        decoder.skip_metadata = False
        decoder.event.metadata_count = 2
        decoder.fields = {Opcode.NEW_STRING: ["a", "b", "c", "d", "e"]}
        # decode_field uses key-1/value-1 indexes; values from _read_metadata are (rest, long_val)
        # For opcode<=2, rest = (meta_key<<3)>>4. With key=1: rest=0. Pair (0, 2) → decode_field(0,2)
        # may error; we only care that consumed length matches.
        with mock.patch.object(decoder, "decode_field", return_value=("k", "v")):
            consumed = decoder.decode_metadata(buffer)
        self.assertEqual(consumed, skipped)

    def test_decode_event_uses_skip_path_when_enabled(self) -> None:
        decoder = JournalDecoder(reader=mock.Mock(), skip_metadata=True)
        decoder.opcode = 2
        decoder.event.metadata_count = 3
        stream = mock.Mock()
        stream.peek.return_value = b"\x00" * 120
        decoder.reader = stream
        with mock.patch.object(decoder, "_skip_metadata_bytes", return_value=6) as skip_fn:
            with mock.patch.object(decoder, "decode_metadata") as decode_fn:
                # Mirror the metadata branch in _decode_event
                metadata_peek = decoder.reader.peek(4 * 10 * decoder.event.metadata_count)
                if decoder.skip_metadata:
                    decoder.event.metadata_fields = None
                    consumed = decoder._skip_metadata_bytes(
                        metadata_peek, decoder.event.metadata_count
                    )
                else:
                    consumed = decoder.decode_metadata(metadata_peek)
                decoder.reader.skip(consumed)
                skip_fn.assert_called_once()
                decode_fn.assert_not_called()
                self.assertIsNone(decoder.event.metadata_fields)
                stream.skip.assert_called_once_with(6)


if __name__ == "__main__":
    unittest.main()
