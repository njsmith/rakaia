import os
import os.path
import io
import collections
import asyncio

from .util import LocklessCondition
from .async_generator import async_generator, yield_

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
        # Have to make sure the file gets opened before we enter the
        # coroutine, because if we do it in the coroutine then the actual
        # opening might be delayed for an arbitrarily long time, by which
        # point the file could have been unlinked.
        read_handle = open(self._path, "rb")
        return self._aiter_any_impl(read_handle, max_chunk_size)

    @async_generator
    async def _aiter_any_impl(self, read_handle, max_chunk_size):
        with read_handle:
            while True:
                if self._bytes_available > read_handle.tell():
                    await yield_(read_handle.read(max_chunk_size))
                elif self._closed:
                    raise StopAsyncIteration
                else:
                    await self._condition.wait()

    def aiter_lines(self, *, max_chunk_size=16384):
        # See comment on aiter_any
        aiter_any = self.aiter_any(max_chunk_size=max_chunk_size)
        return self._aiter_lines_impl(aiter_any)

    @async_generator
    async def _aiter_lines_impl(self, aiter_any):
        # The code with BytesIO is a bit over-complicated, because we're
        # trying to avoid quadratic behavior when we get a very long line
        # split up into lots and lots of independent chunks (e.g., a giant
        # string of "......" as output by a test suite).
        trailing = io.BytesIO()
        async for chunk in aiter_any:
            lines = chunk.split(b"\n")
            trailing.write(lines[0])
            if len(lines) == 1:
                continue
            else:
                lines[0] = trailing.getvalue()
                trailing = io.BytesIO()
                trailing.write(lines.pop())
            for line in lines:
                await yield_(line)
        if trailing.getvalue():
            await yield_(trailing.getvalue())
