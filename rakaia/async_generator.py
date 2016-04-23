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
