import json
import tempfile
import unittest
from pathlib import Path
from events import SPECS, decode, topic
from listener import Collector, REGISTRY, RpcError, database, export, get_meta


def addr(n): return '0x' + f'{n:040x}'
def h(n): return '0x' + f'{n:064x}'
def word(v):
    return int(v, 16).to_bytes(32, 'big') if isinstance(v, str) else (v % (1 << 256)).to_bytes(32, 'big')


def make_log(kind, index, values, address, block=2, tx=None):
    spec = SPECS[kind][index]
    topics, data = [spec['topic']], b''
    for field in spec['fields']:
        w = word(values[field[1]])
        if len(field) == 3: topics.append('0x' + w.hex())
        else: data += w
    return {'address': address, 'topics': topics, 'data': '0x' + data.hex(), 'blockNumber': hex(block), 'blockHash': h(block), 'transactionHash': tx or h(100), 'logIndex': hex(index), 'removed': False}


V2 = next(a for a, k in REGISTRY.items() if k == 'pons_v2')
LAUNCH = dict(token=addr(1), curve=addr(2), deployer=addr(3), pairToken=addr(4), launchConfigId=1, graduationThreshold=10**30)
BUY = dict(buyer=addr(5), recipient=addr(6), quoteIn=10**25, tokensOut=10**35, fee=7, tax=8)


class FakeRPC:
    def __init__(self, logs=None):
        self.rows = logs or []
        self.hashes = {n: h(n) for n in range(10)}
        self.fail_receipt = False
        self.bad_receipt = False
        self.chain = '0x1237'
    def block(self, n):
        return {'number': hex(n), 'hash': self.hashes[n], 'parentHash': self.hashes.get(n-1, h(0)), 'timestamp': hex(1700000000+n)}
    def call(self, method, params):
        if method == 'eth_chainId': return self.chain
        if method == 'eth_getCode': return '0x60006000'
        if method == 'eth_blockNumber': return hex(4)
        if method == 'eth_getLogs':
            f = params[0]
            return [r for r in self.rows if r['address'] in f['address'] and int(f['fromBlock'],16) <= int(r['blockNumber'],16) <= int(f['toBlock'],16)]
        if method == 'eth_getTransactionReceipt':
            if self.fail_receipt: return None
            logs = [r.copy() for r in self.rows if r['transactionHash']==params[0]]
            if self.bad_receipt: logs[0]['data'] = '0x'
            return {'transactionHash':params[0], 'status':'0x1', 'blockNumber':'0x2', 'blockHash':h(2), 'logs':logs}
        raise AssertionError(method)


class Tests(unittest.TestCase):
    def test_live_starts_near_head_and_resumes_without_reset(self):
        rpc = FakeRPC()
        c = Collector(self.db, rpc, factory_first=True)
        c.setup()
        self.assertEqual(get_meta(self.db, 'start'), '2')
        self.assertNotIn('v4', c.registry.values())
        c.tick()
        cursor = get_meta(self.db, 'cursor')
        Collector(self.db, rpc, factory_first=True).setup()
        self.assertEqual(get_meta(self.db, 'cursor'), cursor)
        with self.assertRaises(RpcError): Collector(self.db, rpc).setup()

    def test_live_same_block_launch_and_buy(self):
        launch = make_log('pons_v2', 0, LAUNCH, V2)
        buy = make_log('curve', 0, BUY, addr(2)); buy['logIndex'] = '0x1'
        c = Collector(self.db, FakeRPC([launch, buy]), factory_first=True)
        c.setup(); c.tick()
        self.assertEqual(self.db.execute('SELECT count(*) FROM events').fetchone()[0], 2)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = database(Path(self.tmp.name)/'test.sqlite')
    def tearDown(self): self.db.close(); self.tmp.cleanup()
    def collector(self, rows=None):
        rpc=FakeRPC(rows); c=Collector(self.db,rpc,start=2,chunk=1); c.setup(); return c,rpc
    def test_known_keccak_vector(self):
        self.assertEqual(topic('Transfer(address,address,uint256)'), '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
    def test_v1_documented_topic(self):
        self.assertEqual(SPECS['pons_v1'][0]['topic'],'0xdb51ea9ad51ab453a65a4cb7e60c3cb378c9501bb002609f8f97778fb6c4235a')
    def test_large_uint_and_actual_curve_amounts(self):
        _,v=decode('curve',make_log('curve',0,BUY,addr(2)))
        self.assertEqual(v['tokensOut'],str(10**35)); self.assertEqual(v['tax'],'8')
    def test_negative_tick_and_liquidity(self):
        v=dict(id=h(8),sender=addr(9),tickLower=-400,tickUpper=800,liquidityDelta=-10**20,salt=h(7))
        _,got=decode('v4',make_log('v4',1,v,addr(10)))
        self.assertEqual(got['liquidityDelta'],str(-10**20)); self.assertEqual(got['tickLower'],'-400')
    def test_malformed_abi_rejected(self):
        row=make_log('curve',0,BUY,addr(2)); row['data']='0x'
        with self.assertRaises(ValueError): decode('curve',row)
    def test_unknown_event_preserved(self):
        self.assertEqual(decode('curve',{'topics':[h(42)],'data':'0x'}),('Unknown',{}))
    def test_same_block_launch_and_buy_discovery(self):
        launch=make_log('pons_v2',0,LAUNCH,V2)
        buy=make_log('curve',0,BUY,addr(2)); buy['logIndex']='0x1'
        c,_=self.collector([launch,buy]); c.tick()
        self.assertEqual(self.db.execute('SELECT count(*) FROM events').fetchone()[0],2)
        self.assertEqual(self.db.execute('SELECT count(*) FROM watches').fetchone()[0],1)
    def test_duplicate_and_restart(self):
        row=make_log('pons_v2',0,LAUNCH,V2)
        c,rpc=self.collector([row,row]); c.tick()
        c2=Collector(self.db,rpc,start=1); c2.setup(); c2.tick()
        self.assertEqual(get_meta(self.db,'start'),'2')
        self.assertEqual(self.db.execute('SELECT count(*) FROM events').fetchone()[0],1)
    def test_receipt_failure_does_not_advance_or_register_child(self):
        c,rpc=self.collector([make_log('pons_v2',0,LAUNCH,V2)]); rpc.fail_receipt=True
        with self.assertRaises(RpcError): c.tick()
        self.assertEqual(get_meta(self.db,'cursor'),'1')
        self.assertEqual(self.db.execute('SELECT count(*) FROM watches').fetchone()[0],0)
    def test_receipt_mismatch_rejected(self):
        c,rpc=self.collector([make_log('pons_v2',0,LAUNCH,V2)]); rpc.bad_receipt=True
        with self.assertRaises(RpcError): c.tick()
    def test_reorg_removes_events_receipts_and_dynamic_watches(self):
        c,rpc=self.collector([make_log('pons_v2',0,LAUNCH,V2)]); c.tick()
        rpc.hashes[2]=h(222); rpc.rows=[]; c.reconcile()
        for t in ['events','receipts','watches']:
            self.assertEqual(self.db.execute('SELECT count(*) FROM '+t).fetchone()[0],0)
        self.assertEqual(get_meta(self.db,'cursor'),'1')
        c.tick(); self.assertEqual(get_meta(self.db,'cursor'),'2')
    def test_deep_reorg_stops(self):
        c,rpc=self.collector(); rpc.hashes[1]=h(99)
        with self.assertRaises(RpcError): c.reconcile()
    def test_wrong_chain_stops(self):
        rpc=FakeRPC(); rpc.chain='0x1'
        with self.assertRaises(RpcError): Collector(self.db,rpc,start=2).setup()
    def test_export_has_evidence_and_precision(self):
        c,_=self.collector([make_log('pons_v2',0,LAUNCH,V2)]); c.tick()
        path=Path(self.tmp.name)/'out.json'; export(self.db,path)
        data=json.loads(path.read_text())
        self.assertTrue(data['events'][0]['explorer_url'].endswith(h(100)))
        self.assertEqual(data['events'][0]['decoded']['graduationThreshold'],str(10**30))


if __name__=='__main__': unittest.main()
