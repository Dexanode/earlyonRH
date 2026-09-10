import asyncio
import json
import unittest
from websockets.asyncio.server import serve
from listener import ws_wakeup


class WebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_and_head_notification(self):
        connections = 0
        async def handler(ws):
            nonlocal connections
            connections += 1
            request = json.loads(await ws.recv())
            self.assertEqual(request['method'], 'eth_chainId')
            await ws.send(json.dumps({'id': 1, 'result': '0x1237'}))
            request = json.loads(await ws.recv())
            self.assertEqual(request['params'], ['newHeads'])
            await ws.send(json.dumps({'id': 2, 'result': 'subscription'}))
            if connections == 1:
                await ws.close(); return
            await ws.send(json.dumps({'method': 'eth_subscription', 'params': {'result': {'number': '0x10'}}}))
            await ws.wait_closed()
        async with serve(handler, '127.0.0.1', 0) as server:
            port = server.sockets[0].getsockname()[1]
            wake = asyncio.Event()
            task = asyncio.create_task(ws_wakeup(f'ws://127.0.0.1:{port}', wake))
            try:
                await asyncio.wait_for(wake.wait(), 10)
                self.assertGreaterEqual(connections, 2)
            finally:
                task.cancel(); await asyncio.gather(task, return_exceptions=True)


if __name__ == '__main__': unittest.main()
