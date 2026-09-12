import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from websockets.asyncio.server import serve
from stream import StreamStore, consume
from listener import REGISTRY, database, get_meta, set_meta
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
        self.assertEqual(read(self.path)['health']['recovery_blocks'], 0)

    def test_long_v4_pool_maps_swap_to_asset_side(self):
        airlock=next(a for a,k in REGISTRY.items() if k=='long');manager=next(a for a,k in REGISTRY.items() if k=='v4')
        asset,quote,pool=addr(20),addr(21),h(22)
        create=make_log('long',0,{'asset':asset,'numeraire':quote,'initializer':addr(23),'poolOrHook':addr(24)},airlock);create['logIndex']='0x2'
        init=make_log('v4',0,{'id':pool,'currency0':asset,'currency1':quote,'fee':3000,'tickSpacing':60,'hooks':addr(24),'sqrtPriceX96':2**96,'tick':0},manager);init['logIndex']='0x0'
        swap=make_log('v4',2,{'id':pool,'sender':addr(25),'amount0':-100,'amount1':10,'sqrtPriceX96':2**96,'liquidity':1000,'tick':0,'fee':3000},manager);swap['logIndex']='0x1'
        for row in (swap,create,init):self.s.log(row)
        self.s.flush()
        event=self.db.execute("SELECT asset,name,decoded FROM events WHERE name='DexBuy'").fetchone()
        self.assertEqual((event['asset'],event['name']),(asset,'DexBuy'))
        self.assertEqual(json.loads(event['decoded'])['buyer'],addr(25))

    def test_pending_survives_restart_and_commits_after_three_heads(self):
        row = make_log('pons_v2', 0, LAUNCH, V2)
        self.s.log(row)
        s = StreamStore(self.db); s.head = 5; s.connected = True
        s.flush()
        self.assertEqual(self.db.execute('select count(*) from events').fetchone()[0], 1)
        self.assertEqual(self.db.execute('select count(*) from stream_pending').fetchone()[0], 0)

    def test_removed_log_rolls_back_and_records_recovery(self):
        row = make_log('pons_v2', 0, LAUNCH, V2)
        self.s.log(row); self.s.flush()
        self.s.log(dict(row, removed=True))
        self.assertEqual(self.db.execute('select count(*) from events').fetchone()[0], 0)
        self.assertEqual(self.db.execute('select count(*) from watches').fetchone()[0], 0)
        self.assertIsNotNone(get_meta(self.db, 'last_reorg'))

    def test_large_gap_is_recorded_but_only_recent_tail_is_recovered(self):
        with self.db: self.s.gap(100, 1000)
        self.assertEqual(get_meta(self.db, 'uncovered_from'), '100')
        self.assertEqual(get_meta(self.db, 'uncovered_to'), '900')
        self.assertEqual(get_meta(self.db, 'recovery_next'), '901')
        self.assertEqual(get_meta(self.db, 'recovery_target'), '1000')

    def test_coalesced_heads_do_not_create_false_gap(self):
        with self.db:
            set_meta(self.db, 'recovery_next', 1)
            set_meta(self.db, 'recovery_target', 0)
        self.s.header(FakeRPC().block(5))
        jumped = FakeRPC().block(9)
        jumped['parentHash'] = h(8)
        self.s.header(jumped)
        self.assertEqual(get_meta(self.db, 'recovery_target'), '0')

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
