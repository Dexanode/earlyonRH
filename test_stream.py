import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from websockets.asyncio.server import serve
from stream import StreamStore, consume
from listener import database, get_meta, set_meta
from test_listener import FakeRPC, make_log, LAUNCH, BUY, V2, addr, h
from dashboard import read


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / 'live.sqlite'
        self.db = database(self.path)
        self.s = StreamStore(self.db)
        self.s.connected = True
        for n in range(1, 6): self.s.header(FakeRPC().block(n))

    def tearDown(self):
        self.db.close(); self.tmp.cleanup()

    def test_reordered_launch_and_buy_duplicate_and_no_receipt(self):
        buy = make_log('curve', 0, BUY, addr(2)); buy['logIndex'] = '0x1'
        launch = make_log('pons_v2', 0, LAUNCH, V2)
        self.s.log(buy); self.s.log(launch); self.s.log(launch)
        self.s.flush()
        self.assertEqual(self.db.execute('select count(*) from events').fetchone()[0], 2)
        self.assertEqual(self.db.execute('select count(*) from receipts').fetchone()[0], 0)
        self.assertEqual(read(self.path)['health']['transport'], 'websocket-logs')
        self.assertGreater(read(self.path)['health']['recovery_blocks'], 0)

    def test_pending_survives_restart_and_missing_header_marks_gap(self):
        row = make_log('pons_v2', 0, LAUNCH, V2)
        self.s.log(row)
        s = StreamStore(self.db); s.head = 5; s.connected = True
        s.flush()
        self.assertEqual(self.db.execute('select count(*) from stream_pending').fetchone()[0], 1)
        self.assertEqual(get_meta(self.db, 'recovery_next'), '2')
        s.header(FakeRPC().block(2)); s.flush()
        self.assertEqual(self.db.execute('select count(*) from events').fetchone()[0], 1)

    def test_removed_log_rolls_back_and_records_recovery(self):
        row = make_log('pons_v2', 0, LAUNCH, V2)
        self.s.log(row); self.s.flush()
        self.s.log(dict(row, removed=True))
        self.assertEqual(self.db.execute('select count(*) from events').fetchone()[0], 0)
        self.assertEqual(self.db.execute('select count(*) from watches').fetchone()[0], 0)
        self.assertIsNotNone(get_meta(self.db, 'last_reorg'))

    def test_replacement_header_rolls_back(self):
        row = make_log('pons_v2', 0, LAUNCH, V2)
        self.s.log(row); self.s.flush()
        self.s.header(dict(FakeRPC().block(2), hash=h(200)))
        self.assertEqual(self.db.execute('select count(*) from events').fetchone()[0], 0)


class SubscriptionTests(unittest.IsolatedAsyncioTestCase):
    async def test_log_subscription_and_restart_overlap(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = database(Path(tmp) / 'live.sqlite')
            with db: set_meta(db, 'cursor', 1)
            state = StreamStore(db)
            async def handler(ws):
                await ws.recv(); await ws.send(json.dumps({'id': 1, 'result': '0x1237'}))
                req = json.loads(await ws.recv()); self.assertEqual(req['params'][0], 'logs')
                await ws.send(json.dumps({'id': 2, 'result': 'logs'}))
                await ws.recv(); await ws.send(json.dumps({'id': 3, 'result': 'heads'}))
                row = make_log('pons_v2', 0, LAUNCH, V2)
                await ws.send(json.dumps({'params': {'subscription': 'logs', 'result': row}}))
                for n in range(2, 6):
                    await ws.send(json.dumps({'params': {'subscription': 'heads', 'result': FakeRPC().block(n)}}))
                await ws.wait_closed()
            async with serve(handler, '127.0.0.1', 0) as server:
                task = asyncio.create_task(consume(f'ws://127.0.0.1:{server.sockets[0].getsockname()[1]}', state))
                try:
                    async with asyncio.timeout(5):
                        while state.head != 5: await asyncio.sleep(.01)
                    state.flush()
                    self.assertEqual(db.execute('select count(*) from events').fetchone()[0], 1)
                    self.assertEqual(get_meta(db, 'recovery_next'), '1')
                finally:
                    task.cancel(); await asyncio.gather(task, return_exceptions=True)
            db.close()
