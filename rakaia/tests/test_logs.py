import io
import random
import asyncio
from itertools import count

import pytest

from rakaia.util import LocklessCondition
from rakaia.logs import TailableFileWriter

@pytest.mark.asyncio
async def test_tailable_file(event_loop, tmpdir):
    path = tmpdir.join("test.log")
    writer = TailableFileWriter(str(path))

    assert path.exists()
    with pytest.raises(TypeError):
        # this is a binary file
        writer.write("1")
    writer.write(b"1")

    async def read_chunks(q):
        async for chunk in writer.aiter_any():
            print("Got: {!r}".format(chunk))
            await q.put(chunk)
        await q.put(None)

    chunks1 = asyncio.Queue(loop=event_loop)
    event_loop.create_task(read_chunks(chunks1))

    assert (await chunks1.get()) == b"1"
    assert chunks1.empty()

    writer.write(b"2")

    assert (await chunks1.get()) == b"2"
    assert chunks1.empty()

    chunks2 = asyncio.Queue(loop=event_loop)
    event_loop.create_task(read_chunks(chunks2))

    all2 = b""
    all2 += await chunks2.get()
    assert all2 == b"12"

    writer.close()

    assert (await chunks1.get()) is None
    assert (await chunks2.get()) is None

    all3 = b""
    async for chunk in writer.aiter_any():
        all3 += chunk
    assert all3 == b"12"

@pytest.mark.asyncio
async def test_tailable_file_stresstest(event_loop, tmpdir):
    path = tmpdir.join("test.log")
    writer = TailableFileWriter(str(path))
    gold = io.BytesIO()
    r = random.Random(0)

    def random_chunk(r):
        length = r.choice([0, 1, 10, 65536])
        newlines = r.choice([0, 0, 0, 1, 1, 1, 4])
        newlines = min(length, newlines)
        newline = b"\n"[0]
        chunk = bytearray([r.randrange(256) for i in range(length)])
        for i in range(length):
            if chunk[i] == newline:
                chunk[i] += 1
        for i in range(newlines):
            chunk[r.randrange(length)] = newline
        return chunk

    async def write_chunks(count):
        for i in range(count):
            chunk = random_chunk(r)
            print("Writing chunk ({} bytes, {} newlines)"
                  .format(len(chunk), chunk.count(b"\n")))
            writer.write(chunk)
            gold.write(chunk)
            if r.randrange(2):
                print("  (sleeping)")
                await asyncio.sleep(0.01)

    counter = count()
    running = set()
    condition = LocklessCondition()

    async def read_items(aiter, out_list):
        token = next(counter)
        print("-> Entering {}".format(token))
        running.add(token)
        my_list = []
        out_list.append(my_list)
        async for item in aiter:
            print("  {}: got item ({} bytes)".format(token, len(item)))
            my_list.append(item)
        print("-> Leaving {}".format(token))
        running.remove(token)
        condition.notify_all()

    chunk_lists = []
    line_lists = []

    await write_chunks(10)
    event_loop.create_task(read_items(writer.aiter_any(), chunk_lists))
    event_loop.create_task(read_items(writer.aiter_lines(), line_lists))
    await write_chunks(10)
    event_loop.create_task(read_items(writer.aiter_any(), chunk_lists))
    event_loop.create_task(read_items(writer.aiter_lines(), line_lists))
    await write_chunks(10)
    event_loop.create_task(read_items(writer.aiter_any(), chunk_lists))
    event_loop.create_task(read_items(writer.aiter_lines(), line_lists))
    await write_chunks(10)
    # make sure there's some data remaining to be processed when unlink is
    # called
    writer.write(b"asdf")
    gold.write(b"asdf")
    print("Closing")
    writer.close()
    event_loop.create_task(read_items(writer.aiter_any(), chunk_lists))
    event_loop.create_task(read_items(writer.aiter_lines(), line_lists))
    writer.unlink()

    while running:
        print("Done writing, waiting for {} to exit".format(len(running)))
        await condition.wait()

    for chunk_list in chunk_lists:
        assert b"".join(chunk_list) == gold.getvalue()

    gold_line_list = gold.getvalue().split(b"\n")
    for line_list in line_lists:
        assert line_list == gold_line_list
