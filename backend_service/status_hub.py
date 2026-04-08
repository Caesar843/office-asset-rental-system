from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import Future

from fastapi import WebSocket

from api_schemas import StatusMessageResponse

LOGGER = logging.getLogger(__name__)


class StatusHub:
    """
    Thread-safe websocket broadcaster.

    Service callbacks run in normal worker threads, while websocket sends must
    happen on the FastAPI event loop. The hub bridges the two without leaking
    websocket concerns into the service layer.
    """

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    async def startup(self) -> None:
        self._loop = asyncio.get_running_loop()

    async def shutdown(self) -> None:
        with self._lock:
            sockets = list(self._connections)
            self._connections.clear()

        for websocket in sockets:
            try:
                await websocket.close()
            except Exception:
                LOGGER.debug("websocket close ignored during shutdown", exc_info=True)

        self._loop = None

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        with self._lock:
            self._connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        with self._lock:
            self._connections.discard(websocket)

    def publish(self, message: StatusMessageResponse) -> None:
        loop = self._loop
        if loop is None:
            LOGGER.debug("status hub dropped payload before startup: %s", message.model_dump())
            return

        try:
            future = asyncio.run_coroutine_threadsafe(self._broadcast(message), loop)
        except Exception:
            LOGGER.warning("status hub failed to schedule websocket broadcast", exc_info=True)
            return

        future.add_done_callback(self._log_publish_failure)

    @staticmethod
    def _log_publish_failure(future: Future[None]) -> None:
        try:
            future.result()
        except Exception:
            LOGGER.warning("status hub websocket broadcast failed", exc_info=True)

    async def _broadcast(self, message: StatusMessageResponse) -> None:
        payload = message.model_dump()
        with self._lock:
            sockets = list(self._connections)

        stale: list[WebSocket] = []
        for websocket in sockets:
            try:
                await websocket.send_json(payload)
            except Exception:
                stale.append(websocket)

        if stale:
            with self._lock:
                for websocket in stale:
                    self._connections.discard(websocket)
