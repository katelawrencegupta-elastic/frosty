# Vendored from splunk-ddss-extractor (MIT License)
# https://github.com/ponquersohn/splunk_ddss_extractor

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class JournalStream:
    def __init__(self, reader, chunk_size=64 * 1024):
        self.reader = reader
        self.chunk_size = chunk_size
        self.buffer = bytearray()
        self._eof = False
        self.pos = 0

    def _fill(self, n: int):
        while len(self.buffer) < n and not self._eof:
            chunk = self.reader.read(max(self.chunk_size, n - len(self.buffer)))
            if not chunk:
                self._eof = True
                break
            self.buffer.extend(chunk)

        if len(self.buffer) < n:
            raise EOFError("End of stream")

    def tell(self) -> int:
        return self.pos

    def read(self, n: int) -> bytes:
        if n <= 0:
            return b""
        self._fill(n)
        data = bytes(self.buffer[:n])
        del self.buffer[:n]
        self.pos += n
        return data

    def read_byte(self) -> int:
        self._fill(1)
        b = self.buffer[0]
        del self.buffer[0]
        self.pos += 1
        return b

    def peek(self, n: int) -> bytes:
        if n <= 0:
            return b""
        try:
            self._fill(n)
        except EOFError:
            logger.warning("Reached end of stream while peeking")
        return bytes(self.buffer[:n])

    def skip(self, n: int) -> int:
        self._fill(n)
        del self.buffer[:n]
        self.pos += n
        return n

    def discard(self, n: int | None = None):
        if n is None or n > len(self.buffer):
            n = len(self.buffer)
        if n > 0:
            del self.buffer[:n]

    def read_uvarint(self) -> int:
        result = 0
        shift = 0
        while True:
            b = self.read_byte()
            result |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
                break
            shift += 7
        return result

    def read_varint(self) -> int:
        u = self.read_uvarint()
        return (u >> 1) ^ -(u & 1)
