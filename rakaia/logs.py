import os
import os.path
import io
import collections
import asyncio

from .util import LocklessCondition

# Utility classes for streaming a file onto disk, while letting other code in
# the same process simultaneously stream the file back off of disk.
# Only supports bytes and Unix-style filesystems (because it assumes we can
# unlink a file while there may still be readers outstanding)

class TailableFileWriter:
    def __init__(self, path):
        self._path = path
        self._write_handle = open(path, "wb")
        self._closed = False
        self._bytes_available = 0
        # for signalling that we have written something
        self._condition = LocklessCondition()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        self._closed = True
        self._condition.notify_all()

    def write(self, data):
        assert not self._closed
        self._write_handle.write(data)
        self._write_handle.flush()
        self._bytes_available += len(data)
        self._condition.notify_all()

    def unlink(self):
        self.close()
        assert self._closed
        if os.path.exists(self._path):
            os.unlink(self._path)

    def aiter_any(self, *, max_chunk_size=16384):
        return _TailableFileChunks(self,
                                   open(self._path, "rb"),
                                   max_chunk_size)

    def aiter_lines(self, max_chunk_size=16384):
        return _TailableFileLines(self.aiter_any(max_chunk_size=max_chunk_size))

# Just an async iterator, no fancy stream interface with read/close/etc.
class _TailableFileChunks:
    def __init__(self, writer, read_handle, max_chunk_size):
        self._writer = writer
        self._read_handle = read_handle
        self._max_chunk_size = max_chunk_size

    async def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self._writer._bytes_available > self._read_handle.tell():
                return self._read_handle.read(self._max_chunk_size)
            elif self._writer._closed:
                raise StopAsyncIteration
            else:
                await self._writer._condition.wait()

class _TailableFileLines:
    def __init__(self, chunk_aiter):
        self._chunk_aiter = chunk_aiter
        # already complete lines, ordered from last-to-first (so .pop() will
        # return the next one)
        self._pending_queue = []
        # partial line carried over between chunks
        self._partial = io.BytesIO()

    async def __aiter__(self):
        return self

    async def _refill(self):
        "After calling this, _pending_queue will be empty iff we are at EOF"
        # no complete lines available -- need to get more data
        async for chunk in self._chunk_aiter:
            lines = chunk.split(b"\n")
            self._partial.write(lines[0])
            if len(lines) == 1:
                # no line break here
                continue
            else:
                lines[0] = self._partial.getvalue()
                self._partial = io.BytesIO()
                self._partial.write(lines.pop())
                lines.reverse()
                self._pending_queue = lines
                return
        # hit end-of-file
        last_line = self._partial.getvalue()
        self._partial = io.BytesIO()
        if last_line:
            self._pending_queue = [last_line]
        else:
            self._pending_queue = []

    async def __anext__(self):
        if not self._pending_queue:
            await self._refill()
        if not self._pending_queue:
            raise StopAsyncIteration
        else:
            return self._pending_queue.pop()


