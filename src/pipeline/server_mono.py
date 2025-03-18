import asyncio
import websockets
import json
import signal
from src.scripts.particle_sim import SimParams, ParticleSimulator
from src.utils.custom_logging import setup_logging
from env import Env


log = setup_logging()
env = Env()


class WebSocketServer:
    def __init__(self, host='0.0.0.0', port=9003):
        self.host = host
        self.port = port
        self.clients = set()
        self.running = True
        
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
    def signal_handler(self, sig, frame):
        log.info(f"Received signal {sig}, shutting down...")
        self.running = False
        asyncio.get_event_loop().stop()
        
    async def handler(self, websocket):
        self.clients.add(websocket)
        try:
            async for msg in websocket:
                pass  # Обработка входящих сообщений не требуется
        finally:
            self.clients.remove(websocket)
            
    async def run_server(self):
        async with websockets.serve(self.handler, self.host, self.port):
            log.info(f"WebSocket server started on ws://{self.host}:{self.port}")
            await asyncio.Future()
            
    async def broadcast(self, data):
        if self.clients:
            try:
                message = json.dumps(data)
                await asyncio.gather(
                    *[client.send(message) for client in self.clients],
                    return_exceptions=True
                )
            except Exception as e:
                log.error(f"Broadcast error: {e}")
            

async def main():
    params = SimParams(N=10000, dt=0.01, box_size=100.0)
    log.info(f"Параметры симуляции: {params}")
    simulator = ParticleSimulator(params)
    
    server = WebSocketServer(
        host=env.__getattr__("HOST"),
        port=env.__getattr__("PORT")
    )
    log.info(f"Websocket: {server}")
    
    server_task = asyncio.create_task(server.run_server())
    
    try:
        while server.running:
            simulator.step()
            frame = simulator.get_frame()
            # log.info(frame)
            await server.broadcast({
                'type': 'frame',
                't': 0,
                'particles': [
                    {
                        'x': pos[0], 
                        'y': pos[1], 
                        'z': pos[2],
                        'mass': mass,
                        'velocity': {'x': vel[0], 'y': vel[1], 'z': vel[2]},
                        'radius': radii
                    }
                    for pos, vel, mass, radii in zip(
                        frame['positions'], 
                        frame['velocities'],
                        frame['masses'], 
                        frame['radii']
                    )
                ]
            })
            await asyncio.sleep(0)  # Даем возможность другим задачам выполняться
            
    except asyncio.CancelledError:
        pass
    finally:
        server_task.cancel()
        await server_task
        

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Server stopped by user")