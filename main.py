import asyncio
import queue
import threading
from typing import Callable

from aiohttp import web, http


def is_integer(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


class Queue:
    def __init__(self) -> None:
        self.msg_queue = asyncio.Queue()

    async def push(self, s: str) -> None:
        await self.msg_queue.put(s)

    async def pop(self, timeout: int | None) -> str | None:
        if not timeout:
            try:
                msg = self.msg_queue.get_nowait()
            except asyncio.QueueEmpty:
                msg = None
            return msg
        else:
            try:
                msg = await asyncio.wait_for(self.msg_queue.get(), timeout=timeout)
            except (asyncio.QueueEmpty, asyncio.TimeoutError):
                msg = None
            return msg
        


class QueueManager:
    def __init__(self) -> None:
        self._queues: dict[str, Queue] = {}
        self._lock = threading.RLock()

    async def get_queue(self, name: str) -> Queue | None:
        with self._lock:
            return self._queues.get(name)

    async def create_queue(self, name: str) -> Queue:
        with self._lock:
            _queue = self._queues.get(name)
            if _queue is None:
                _queue = Queue()
                self._queues[name] = _queue
            return _queue


qm = QueueManager()


@web.middleware
async def error_middleware(request: web.Request, handler: Callable):
    try:
        return await handler(request)
    except Exception:
        return web.Response(status=http.HTTPStatus.INTERNAL_SERVER_ERROR)


async def get_message(request):
    name = request.match_info.get('queue', "")
    if not name:
        return web.Response(status=http.HTTPStatus.BAD_REQUEST)

    _queue = await qm.get_queue(name)
    if not _queue:
        return web.Response(status=http.HTTPStatus.BAD_REQUEST)

    timeout = request.query.get('timeout', 0)
    if not is_integer(timeout) or int(timeout) < 0:
        timeout = 0
    timeout = int(timeout)

    message = await _queue.pop(timeout)
    if not message:
        return web.Response(status=http.HTTPStatus.NOT_FOUND)

    return web.Response(text=message, status=http.HTTPStatus.OK)


async def send_message(request):
    name = request.match_info.get('queue', "")
    if not name:
        return web.Response(status=http.HTTPStatus.BAD_REQUEST)

    message = request.query.get('v', "")
    if not message:
        return web.Response(status=http.HTTPStatus.BAD_REQUEST)

    _queue = await qm.get_queue(name)
    if not _queue:
        _queue = await qm.create_queue(name)

    await _queue.push(message)
    return web.Response(status=http.HTTPStatus.OK)


app = web.Application()
app.add_routes(
    [
        web.get('/{queue}', get_message),
        web.put('/{queue}', send_message)
    ]
)
app.middlewares.append(error_middleware)

if __name__ == '__main__':
    web.run_app(app)
