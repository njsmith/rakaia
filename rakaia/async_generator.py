# In python we have:
#   functions
#   iterators
#   generators as a convenient way to implement iterators like functions
#   async functions
#   async iterators
#   async generators as a convenient ... wait, no, this doesn't exist
#
# The only built-in way to make an async iterator is to manually implement
# __aiter__ and __anext__. This sucks -- have you ever thought "well, I could
# use a generator... instead I'm going to manually define a class with
# __iter__ and __next__ (and send and throw and close)"? Of course not,
# generators do the same thing but are way easier.
#
# So, here's how you define an async generator:
#
# from this_file import async_generator, yield_
#
# @async_generator
# async def double_all(ait):
#     # you can use async constructs like in any async def function
#     async for x in ait:
#         # where in a normal generator you'd write
#         #   yield foo
#         # instead write
#         #   await yield_(foo)
#         await yield_(2 * x)

import warnings
from functools import wraps

__all__ = ["yield_", "async_generator"]

class YieldWrapper:
    def __init__(self, payload):
        self.payload = payload

class YieldAwaitable:
    def __init__(self, payload):
        self._payload = payload
        self._awaited = False

    def __await__(self):
        self._awaited = True
        yield YieldWrapper(self._payload)

    def __del__(self):
        if not self._awaited:
            warnings.warn(RuntimeWarning(
                "result of yield_(...) was never awaited"))

def yield_(value):
    return YieldAwaitable(value)

# This is the awaitable / iterator returned from asynciter.__anext__()
class ANextIter:
    def __init__(self, it):
        self._it = it

    def __await__(self):
        return self

    def __next__(self):
        return self.send(None)

    def send(self, value):
        try:
            result = self._it.send(value)
        except StopIteration as e:
            # The underlying generator returned, so we should signal the end
            # of iteration.
            if e.value is not None:
                raise RuntimeError(
                    "@async_generator functions must return None")
            raise StopAsyncIteration
        if isinstance(result, YieldWrapper):
            raise StopIteration(result.payload)
        else:
            return result

    def throw(self, type, value=None, traceback=None):
        return self._it.throw(type, value, traceback)

    def close(self):
        return self._it.close()

class AsyncGenerator:
    def __init__(self, coroutine):
        self._coroutine = coroutine

    async def __aiter__(self):
        return self

    def __anext__(self):
        return ANextIter(self._coroutine)

def async_generator(coroutine_maker):
    @wraps(coroutine_maker)
    def async_generator_maker(*args, **kwargs):
        return AsyncGenerator(coroutine_maker(*args, **kwargs))
    return async_generator_maker
