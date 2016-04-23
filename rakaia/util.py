import asyncio

class LocklessCondition:
    def __init__(self, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self._waiters = []

    async def wait(self):
        fut = asyncio.Future(loop=self._loop)
        self._waiters.append(fut)
        await fut

    def notify_all(self):
        for fut in self._waiters:
            if not fut.done():
                fut.set_result(False)
        self._waiters = []