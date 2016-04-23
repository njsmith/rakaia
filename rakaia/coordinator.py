import os
import hmac
import asyncio
from functools import wraps

import aiohttp
from aiohttp import web

# Mechanism for tasks to prove that they are who they say they are
class _Mint:
    def __init__(self):
        self._secret = os.urandom(64)

    def mint_token(self, task_id):
        return (hmac.new(self._secret,
                         task_id.encode("ascii"),
                         "sha512")
                .hexdigest())

    def validate(self, task_id, purported_token):
        gold_token = self.mint_token(task_id)
        # hmac.compare_digest is constant-time, unlike ==
        return hmac.compare_digest(purported_token, gold_token)

MINT = _Mint()

class LogInFlight:
    def __init__(self, tmpdir, identifier):
        self._identifier = identifier

    def append(self, chunk):
        # save chunk and fire off event for any readers

    async def finalize(self):
        # upload to blob storage

    async def
        # delete from tmpdir

# want the get_log thing to hand out an async stream
# maybe asyncio.StreamReader
#   https://docs.python.org/3/library/asyncio-stream.html
# or maybe aiohttp.streams.StreamReader

def validate_task_id(handler):
    @wraps(handler)
    async def handler_with_validated_task_id(request, task_id):
        task_id = request.match_info['task_id']
        task_token = request.match_info["task_token"]
        if not MINT.validate(task_id, task_token):
            raise web.HTTPUnauthorized("invalid task_token")
        return (await handler(request, task_id))

@validate_task_id
async def log_put(request, task_id):
    # if there's already a log, error out
    # otherwise, create one
    # wake up everyone who was waiting for this to even exist
    # write everything over
    # save the final thing to blob storage
    log = LogInFlight(TMPDIR, identifier)
    logs_in_flight[identifier] = log
    async for chunk in request.content.iter_any():
        log.append(chunk)
    await log.finalize()
    del logs_in_flight[identifier]

    # FIXME: scan for "headline" markers and update the task status with them
    # (ideally with some API to stream this to listeners too)
    # break into lines, with some care to handle lines that fall across
    # chunks, and process each line in order...
    # I guess we can most cleanly do this from another async for loop.
    # or just do it in the original loop above
    # FIXME: make this robust against very very long lines that trickle in one
    # byte at a time (like: "......" in tests). Specifically, not quadratic.
    # XX alternatively, aiohttp.stream.StreamReader already has equivalent
    # logic.
    # well... the StreamReader logic is way more complicated, with limits and
    # all kinds of things
    trailing = BytesIO()
    async for chunk in log:
        assert chunk
        lines = chunk.split(b"\n")
        trailing.write(lines[0])
        # Cases:
        # - chunk has no newline -> just append to trailing and we're done
        # - chunk has a newline: trailing is finished
        if len(lines) == 1:
            continue
        else:
            lines[0] = trailing.value()
            trailing = BytesIO()
            trailing.write(lines.pop(-1))
            for line in lines:
                ...

# Rackspace doesn't have any py3 api to cloud files + CDN
# the openstack sdk (https://pypi.python.org/pypi/openstacksdk) does claim to
# support python 3, but then I guess it doesn't support rackspace's CDN
# so I guess it's s3 for us?
# Oh, it looks like the "enable CDN" switch is just a single boolean flag we
# set once on the container (= bucket)?

# abstract interface
class BlobStorage:
    def __init__(self, credentials, bucket, whatever):
        ...

    async def add(self, name, file_handle):
        ...
        return public_url


class StreamingLogAsyncIter:
    def __init__(self, condition, file_handle, max_chunk_size=65536):
        self._condition = condition
        self._file_handle = file_handle

    async def __aiter__(self):
        return self

    def _file_size(self):
        return os.fstat(self._file_handle.fileno()).st_size

    async def __anext__(self):
        size = self._file_size()
        if size == self._file_handle.tell():
            await self._condition.acquire()
            await self._condition.wait()
            self._condition.release()
        chunk = self._file_handle.read(max_chunk_size)
        if not chunk:
            raise AsyncStopIteration
        self._bytes_returned += len(chunk)
        return chunk

async def log_get(request):
    task_id = request.match_info['task_id']
    # if it's already in blob storage, just redirect
    response = web.StreamResponse()
    response.enable_compression()
    response.headers["foo"] = "bar"
    await response.prepare()
    async for chunk in stream.iter_any():
        response.write(chunk)
        await response.drain()
    await response.write_eof()
    # otherwise wait for log to start existing
    # then stream it out bit by bit

app = web.Application()
app.router.add_route("GET", "/log/{task_id:.+}", log_get)
app.router.add_route("PUT", "/_task_api/{task_token}/log/{task_id:.+}", log_put)

if __name__ == "__main__":
    web.run_app