import os
import hmac
import asyncio
from functools import wraps

import aiohttp
from aiohttp import web

from .tailable_file import TailableFileWriter

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

def validate_task_id(handler):
    @wraps(handler)
    async def handler_with_validated_task_id(request):
        task_id = request.match_info['task_id']
        task_token = request.match_info["task_token"]
        if not MINT.validate(task_id, task_token):
            raise web.HTTPUnauthorized(reason="invalid task_token")
        return (await handler(request, task_id))
    return handler_with_validated_task_id

# FIXME: temporary for testing
async def token_for_task_id(request):
    task_id = request.match_info["task_id"]
    token = MINT.mint_token(task_id)
    return web.Response(body=token.encode("ascii"))

# Rackspace doesn't have any py3 api to cloud files + CDN
# the openstack sdk (https://pypi.python.org/pypi/openstacksdk) does claim to
# support python 3, but then I guess it doesn't support rackspace's CDN
# so I guess it's s3 for us?
# Oh, it looks like the "enable CDN" switch is just a single boolean flag we
# set once on the container (= bucket)?

# abstract interface
# class BlobStorage:
#     def __init__(self, credentials, bucket, whatever):
#         ...

#     async def add(self, name, file_handle):
#         ...
#         return public_url


TMPDIR = "/home/njs/rakaia/tmp/"
LOGS = {}

@validate_task_id
async def log_put(request, task_id):
    # Could already be there if anyone is blocked waiting to read it
    if task_id not in LOGS:
        LOGS[task_id] = TailableFileWriter(os.path.join(TMPDIR, task_id))
    writer = LOGS[task_id]
    print("uploading for {}".format(task_id))
    # XX if client drops connection, then this just hangs forever :-(
    async for chunk in request.content.iter_any():
        print("got chunk ({} bytes)".format(len(chunk)))
        writer.write(chunk)
    print("done")
    writer.close()
    # FIXME: once finished, upload to blob storage and remove from disk
    return web.Response(body=b"thanks")

async def copy_logs_to_http(writer, response):
    async for chunk in writer.aiter_any():
        response.write(chunk)
        await response.drain()
    await response.write_eof()

async def log_get(request):
    task_id = request.match_info['task_id']
    if task_id not in LOGS:
        LOGS[task_id] = TailableFileWriter(os.path.join(TMPDIR, task_id))
    writer = LOGS[task_id]

    response = web.StreamResponse()
    response.enable_compression()
    response.headers["Content-Type"] = "text/plain; charset=utf-8"
    await response.prepare(request)
    async for chunk in writer.aiter_any():
        response.write(chunk)
        await response.drain()
    await response.write_eof()
    return response

app = web.Application()
app.router.add_route("GET", "/_fixme/token/{task_id:.+}", token_for_task_id)
app.router.add_route("GET", "/log/{task_id:.+}", log_get)
app.router.add_route("PUT", "/_task_api/{task_token}/log/{task_id:.+}", log_put)

if __name__ == "__main__":
    web.run_app(app)