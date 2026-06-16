# Vendored from splunk-ddss-extractor (MIT License)
# https://github.com/ponquersohn/splunk_ddss_extractor
# Based on fionera/splunker (Apache-2.0): https://github.com/fionera/splunker

"""Splunk journal.zst binary format decoder (pure Python)."""

from __future__ import annotations

import io
import logging
import struct
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, List, Optional, Tuple

from frosty.splunk_journal.stream import JournalStream

logger = logging.getLogger(__name__)


class Opcode(IntEnum):
    NOP = 0
    OLDSTYLE_EVENT = 1
    OLDSTYLE_EVENT_WITH_HASH = 2
    NEW_HOST = 3
    NEW_SOURCE = 4
    NEW_SOURCE_TYPE = 5
    NEW_STRING = 6
    DELETE = 8
    SPLUNK_PRIVATE = 9
    HEADER = 10
    HASH_SLICE = 11


@dataclass
class RawdataMetaKeyItemType:
    representation: int
    extra_ints_needed: int

    def is_float_type(self) -> bool:
        return (self.representation & 0x2) != 0


RMKI_TYPES = {
    0: RawdataMetaKeyItemType(0, 1),
    2: RawdataMetaKeyItemType(2, 1),
    3: RawdataMetaKeyItemType(3, 2),
    4: RawdataMetaKeyItemType(4, 2),
    6: RawdataMetaKeyItemType(6, 2),
    7: RawdataMetaKeyItemType(7, 3),
    8: RawdataMetaKeyItemType(8, 1),
    9: RawdataMetaKeyItemType(9, 1),
    10: RawdataMetaKeyItemType(10, 1),
    11: RawdataMetaKeyItemType(11, 2),
    12: RawdataMetaKeyItemType(12, 3),
    14: RawdataMetaKeyItemType(14, 2),
    15: RawdataMetaKeyItemType(15, 0),
}


def decode_bytes_as_text(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


@dataclass
class Event:
    message_length: int = 0
    has_extended_storage: bool = False
    extended_storage_len: int = 0
    has_hash: bool = False
    hash: bytes = field(default_factory=lambda: b"\x00" * 20)
    stream_id: int = 0
    stream_offset: int = 0
    stream_sub_offset: int = 0
    index_time_diff: int = 0
    time_sub_seconds: int = 0
    metadata_count: int = 0
    message: bytes = b""
    include_punctuation: bool = False
    metadata_fields: Dict[str, str] | None = None
    index_time: int = 0
    event_time: int = 0
    host: str = ""
    sourcetype: str = ""
    source: str = ""

    def reset(self):
        self.host = ""
        self.sourcetype = ""
        self.source = ""
        self.message_length = 0
        self.has_extended_storage = False
        self.extended_storage_len = 0
        self.has_hash = False
        self.hash = b"\x00" * 20
        self.stream_id = 0
        self.stream_offset = 0
        self.stream_sub_offset = 0
        self.index_time_diff = 0
        self.time_sub_seconds = 0
        self.metadata_count = 0
        self.message = b""
        self.include_punctuation = False
        self.metadata_fields = {}
        self.index_time = 0
        self.event_time = 0

    def message_string(self) -> str:
        return decode_bytes_as_text(self.message)

    def to_normalized_dict(self) -> dict:
        return {
            "index_time": self.index_time,
            "time": self.event_time,
            "event": self.message_string(),
            "host": self.host,
            "sourcetype": self.sourcetype,
            "source": self.source,
            "fields": self.metadata_fields or {},
        }


def decode_uvarint_from_bytes(data: bytes, offset: int = 0) -> Tuple[int, int]:
    result = 0
    shift = 0
    n = 0
    for i in range(offset, len(data)):
        b = data[i]
        n += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return result, n
        shift += 7
    return 0, -1


def decode_shifted_varint_from_bytes(data: bytes, offset: int = 0):
    u, n = decode_uvarint_from_bytes(data, offset)
    if n == -1:
        return 0, -1
    return u >> 1, n


class MetadataError(Exception):
    pass


class JournalDecoder:
    HASH_SIZE = 20

    def __init__(self, reader: Optional[io.BufferedReader], trace: bool = False):
        self.reader = JournalStream(reader)
        self.trace = trace
        self.opcode = 0
        self.event = Event()
        self.error: Optional[Exception] = None
        self.fields: Dict[int, List[str]] = {}
        self.base_event_time = 0
        self.base_index_time = 0
        self.active_host = 0
        self.active_source = 0
        self.active_source_type = 0
        self.metadata_error_counts: dict[str, int] = {}
        self.total_metadata_errors = 0
        self.events_with_errors = 0

    def host(self) -> str:
        if Opcode.NEW_HOST in self.fields and self.active_host > 0:
            return self.fields[Opcode.NEW_HOST][self.active_host - 1]
        return ""

    def source(self) -> str:
        if Opcode.NEW_SOURCE in self.fields and self.active_source > 0:
            return self.fields[Opcode.NEW_SOURCE][self.active_source - 1]
        return ""

    def source_type(self) -> str:
        if Opcode.NEW_SOURCE_TYPE in self.fields and self.active_source_type > 0:
            return self.fields[Opcode.NEW_SOURCE_TYPE][self.active_source_type - 1]
        return ""

    def scan(self) -> bool:
        while True:
            try:
                self.opcode = self.reader.read_byte()
            except EOFError:
                self.error = None
                return False
            except Exception as exc:
                self.error = exc
                return False

            if self._is_event_opcode(self.opcode):
                self.event.reset()

            try:
                self._decode_next()
            except MetadataError as exc:
                self._warn_metadata_error("scan", exc)
            except Exception as exc:
                self.error = exc
                return False

            if self._is_event_opcode(self.opcode):
                return True

    def err(self) -> Optional[Exception]:
        return self.error

    def get_event(self) -> Event:
        return self.event

    def _is_event_opcode(self, opcode: int) -> bool:
        return (
            opcode == Opcode.OLDSTYLE_EVENT
            or opcode == Opcode.OLDSTYLE_EVENT_WITH_HASH
            or (32 <= opcode <= 43)
        )

    def _decode_next(self):
        if self.opcode == Opcode.HEADER:
            self._decode_header()
        elif self.opcode == Opcode.SPLUNK_PRIVATE:
            self._decode_splunk_private()
        elif self.opcode == Opcode.NEW_HOST:
            self._decode_host()
        elif self.opcode == Opcode.NEW_SOURCE:
            self._decode_source()
        elif self.opcode == Opcode.NEW_SOURCE_TYPE:
            self._decode_source_type()
        elif self.opcode == Opcode.NEW_STRING:
            self._decode_string()
        elif self.opcode == Opcode.NOP:
            pass
        elif 17 <= self.opcode <= 31:
            self._decode_new_state()
        elif self._is_event_opcode(self.opcode):
            self._decode_event()
        else:
            raise ValueError(f"Unknown opcode: 0x{self.opcode:02x}")

    def _decode_header(self):
        data = self.reader.read(6)
        version = data[0]
        self.base_index_time = struct.unpack("<i", data[2:6])[0]
        logger.debug("Journal version: %s", version)

    def _decode_splunk_private(self):
        length = self.reader.read_uvarint()
        self.reader.skip(length)

    def _read_string_field(self) -> str:
        length = self.reader.read_uvarint()
        data = self.reader.read(length)
        return decode_bytes_as_text(data)

    def _decode_host(self):
        s = self._read_string_field()
        self.fields.setdefault(Opcode.NEW_HOST, []).append(s)

    def _decode_source(self):
        s = self._read_string_field()
        self.fields.setdefault(Opcode.NEW_SOURCE, []).append(s)

    def _decode_source_type(self):
        s = self._read_string_field()
        self.fields.setdefault(Opcode.NEW_SOURCE_TYPE, []).append(s)

    def _decode_string(self):
        s = self._read_string_field()
        self.fields.setdefault(Opcode.NEW_STRING, []).append(s)

    def _decode_new_state(self):
        if self.opcode & 0x8:
            self.active_host = self.reader.read_uvarint()
        if self.opcode & 0x4:
            self.active_source = self.reader.read_uvarint()
        if self.opcode & 0x2:
            self.active_source_type = self.reader.read_uvarint()
        if self.opcode & 0x1:
            data = self.reader.read(4)
            self.base_event_time = struct.unpack("<i", data)[0]

    def _decode_event(self):
        event_info_size = 8 * 10 + 8 + self.HASH_SIZE
        peek = self.reader.peek(event_info_size)
        offset = 0

        self.event.message_length, n = decode_uvarint_from_bytes(peek, offset)
        offset += n
        self.event.message_length += self.reader.pos + offset

        if self.opcode & 0x4:
            self.event.has_extended_storage = True
            self.event.extended_storage_len, n = decode_uvarint_from_bytes(peek, offset)
            offset += n

        if self.opcode & 0x01 == 0:
            self.event.has_hash = True
            self.event.hash = peek[offset : offset + self.HASH_SIZE]
            offset += self.HASH_SIZE

        self.event.stream_id = struct.unpack("<Q", peek[offset : offset + 8])[0]
        offset += 8

        self.event.stream_offset, n = decode_uvarint_from_bytes(peek, offset)
        offset += n

        self.event.stream_sub_offset, n = decode_uvarint_from_bytes(peek, offset)
        offset += n

        self.event.index_time_diff, n = decode_uvarint_from_bytes(peek, offset)
        offset += n
        self.event.index_time = self.base_index_time + self.event.index_time_diff

        self.event.time_sub_seconds, n = decode_shifted_varint_from_bytes(peek, offset)
        offset += n
        self.event.event_time = self.base_event_time * 1000 + self.event.time_sub_seconds

        self.event.metadata_count, n = decode_uvarint_from_bytes(peek, offset)
        offset += n
        self.reader.skip(offset)

        if self.event.metadata_count > 0:
            metadata_peek = self.reader.peek(4 * 10 * self.event.metadata_count)
            consumed = self.decode_metadata(metadata_peek)
            self.reader.skip(consumed)

        if self.event.has_extended_storage:
            self.reader.read(self.event.extended_storage_len)

        self.event.message_length -= self.reader.pos
        self.event.message = self.reader.read(self.event.message_length)
        self.event.include_punctuation = (self.opcode & 0x22) == 34
        self.event.source = self.source()
        self.event.sourcetype = self.source_type()
        self.event.host = self.host()

    def decode_metadata(self, buffer: bytes) -> int:
        metadata_offset = 0
        self.event.metadata_fields = {}
        extraction_errors: list[str] = []

        for i in range(self.event.metadata_count):
            try:
                n, meta_index = self._read_metadata(buffer, metadata_offset)
                metadata_offset += n
                for field_index, value_index in meta_index:
                    field, value = self.decode_field(field_index, value_index)
                    if field == "__field_error__":
                        extraction_errors.append(value)
                        continue
                    if field not in self.event.metadata_fields:
                        self.event.metadata_fields[field] = value
                    elif isinstance(self.event.metadata_fields[field], list):
                        self.event.metadata_fields[field].append(value)
                    else:
                        self.event.metadata_fields[field] = [
                            self.event.metadata_fields[field],
                            value,
                        ]
            except Exception as exc:
                extraction_errors.append(f"metadata entry {i}: {exc}")
                self._warn_metadata_error(f"decode_metadata entry {i}", exc)
                metadata_offset += 1

        if extraction_errors:
            self.event.metadata_fields["__extraction_errors__"] = extraction_errors
            self.events_with_errors += 1

        return metadata_offset

    def decode_field(self, key, value):
        key -= 1
        value -= 1
        fields = self.fields[Opcode.NEW_STRING]
        try:
            return fields[key], fields[value]
        except Exception as exc:
            self._warn_metadata_error(f"decode_field(key={key + 1}, value={value + 1})", exc)
            return "__field_error__", f"key={key + 1}, value={value + 1}: {exc}"

    def _read_metadata(self, peek: bytes, offset: int):
        meta_key, n = decode_uvarint_from_bytes(peek, offset)
        if n == -1:
            raise ValueError("Cannot read varint for metadata key")
        peek_offset = n

        if self.opcode <= 2:
            meta_key <<= 3
            num_to_read = 1
            rest = meta_key >> 4
        else:
            if self.opcode < 36:
                meta_key <<= 2
            rmki_key = int(meta_key & 0xF)
            rest = meta_key >> 4
            type_val = RMKI_TYPES.get(rmki_key)
            num_to_read = type_val.extra_ints_needed if type_val else 0

        ret = []
        for _ in range(num_to_read):
            long_val, n = decode_uvarint_from_bytes(peek, offset + peek_offset)
            if n == -1:
                raise ValueError("Cannot read varint for metadata value")
            ret.append((rest, long_val))
            peek_offset += n

        return peek_offset, ret

    def _warn_metadata_error(self, context: str, error: Exception):
        error_key = f"{context}: {type(error).__name__}"
        self.metadata_error_counts[error_key] = self.metadata_error_counts.get(error_key, 0) + 1
        self.total_metadata_errors += 1
        if self.trace:
            logger.debug("Metadata error in %s: %s", context, error)
