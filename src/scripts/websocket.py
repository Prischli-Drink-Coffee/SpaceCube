# orchestrator/src/scripts/websocket.py
import asyncio
import websockets
import json
import signal
from typing import Set
from src.utils.custom_logging import setup_logging
from env import Env

log = setup_logging()
env = Env()


class WebSocketServer:
    def __init__(self, host='0.0.0.0', port=9003):
        self.host = host
        self.port = port
        self.connected_clients: Set[websockets.WebSocketServerProtocol] = set()
        self.server = None
        self.loop = asyncio.get_event_loop()
        self._stop = asyncio.Event()

        # Обработка сигналов
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, sig, frame):
        log.info(f"Received exit signal {sig}, initiating shutdown...")
        self.loop.call_soon_threadsafe(self._stop.set)

    async def _handler(self, websocket: websockets.WebSocketServerProtocol):
        self.connected_clients.add(websocket)
        try:
            log.info(f"New client connected. Total: {len(self.connected_clients)}")
            await websocket.wait_closed()  # Ожидаем закрытие соединения
        finally:
            self.connected_clients.remove(websocket)
            log.info(f"Client disconnected. Remaining: {len(self.connected_clients)}")

    async def _broadcast_task(self):
        while not self._stop.is_set():
            try:
                await asyncio.sleep(1)  # Заглушка, реальная логика будет через очередь
            except asyncio.CancelledError:
                break

    async def run_server(self):
        try:
            async with websockets.serve(
                self._handler,
                self.host,
                self.port,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=5
            ) as self.server:
                log.info(f"WebSocket server started on ws://{self.host}:{self.port}")
                await self._stop.wait()
        except Exception as e:
            log.error(f"WebSocket server failed: {e}")
            raise
        finally:
            await self.shutdown()

    async def broadcast(self, data: dict):
        if not self.connected_clients:
            return

        message = json.dumps(data)
        dead_clients = []

        for client in self.connected_clients:
            try:
                await client.send(message)
            except (
                websockets.ConnectionClosed,
                websockets.ConnectionClosedOK,
                websockets.ConnectionClosedError
            ):
                dead_clients.append(client)
            except Exception as e:
                log.error(f"Error sending to client: {e}")
                dead_clients.append(client)

        # Удаляем отключенных клиентов
        for client in dead_clients:
            try:
                self.connected_clients.remove(client)
            except KeyError:
                pass

    async def shutdown(self):
        log.info("Shutting down WebSocket server...")
        
        # Закрываем все подключения
        if self.connected_clients:
            await asyncio.gather(
                *[client.close() for client in self.connected_clients],
                return_exceptions=True
            )
            self.connected_clients.clear()
        
        # Останавливаем сервер
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        log.info("WebSocket server stopped")
