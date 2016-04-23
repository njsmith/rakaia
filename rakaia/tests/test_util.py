import asyncio
import pytest

from rakaia.util import LocklessCondition

@pytest.mark.asyncio
async def test_lockless_condition(event_loop):
    condition = LocklessCondition(loop=event_loop)

    async def wait_then_notify(q):
        print("waiting...")
        await condition.wait()
        print("...notified!")
        await q.put("notified")
        print("done")

    q1 = asyncio.Queue()
    event_loop.create_task(wait_then_notify(q1))

    q2 = asyncio.Queue()
    event_loop.create_task(wait_then_notify(q2))

    await asyncio.sleep(0.01)
    assert q1.empty()
    assert q2.empty()

    condition.notify_all()

    await asyncio.sleep(0.01)
    assert not q1.empty()
    assert not q2.empty()

    assert (await q1.get()) == "notified"
    assert (await q2.get()) == "notified"
