import asyncio
import pytest

from rakaia.async_generator import async_generator, yield_

@async_generator
async def async_range(count):
    for i in range(count):
        print("Calling yield_({})".format(i))
        await yield_(i)

@async_generator
async def double(ait):
    async for value in ait:
        await yield_(value * 2)
        await asyncio.sleep(0.001)

# like list(it) but works on async iterators
async def collect(ait):
    items = []
    async for value in ait:
        items.append(value)
    return items

@pytest.mark.asyncio
async def test_async_generator():
    assert (await collect(async_range(10))) == list(range(10))
    assert (await collect(double(async_range(10)))
            == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18])
