"""Stream-first Pons radar. HTTP is reserved for reconnect gap recovery.

Events are provider-observed logs matched to block headers, not independently
receipt-verified. Existing receipt-verified historical rows remain unchanged.
"""
import asyncio
import json
import logging
import os
import time
from collections import OrderedDict

from events import SPECS, decode
from listener import CHAIN, REGISTRY, RPC, RpcError, database, get_meta, set_meta, now

LOG = logging.getLogger('stream')
FACTORIES = {a: k for a, k in REGISTRY.items() if k.startswith('pons_') or k=='long'}
TOPICS = list(dict.fromkeys(s['topic'] for k in ('pons_v1', 'pons_v2', 'long', 'curve', 'v3_pool', 'v2_factory', 'v3_factory', 'erc6551_registry') for s in SPECS[k]))
GENERIC_BIRTH_TOPICS = {s['topic']: kind for kind in ('pons_v1','pons_v2','v2_factory','v3_factory','erc6551_registry') for s in SPECS[kind] if s['name'] in ('TokenLaunched','PairCreated','PoolCreated','ERC6551AccountCreated')}
MAX_AUTO_RECOVERY = 100


class StreamStore:
    def __init__(self, db):
        self.db = db
        self.headers = OrderedDict()
        self.head = None
        self.connected = False
        self.last_message = 0
        db.execute('CREATE TABLE IF NOT EXISTS stream_pending(tx TEXT,idx INTEGER,block INTEGER,body TEXT,PRIMARY KEY(tx,idx))')
        with db:
            set_meta(db, 'transport', 'websocket-logs')
            set_meta(db, 'validation', 'provider stream log held for three heads; no separate header/receipt verification')
            set_meta(db, 'scope', 'factory-first')

    def gap(self, lo, hi):
        if hi < lo: return
        if hi - lo + 1 > MAX_AUTO_RECOVERY:
            set_meta(self.db, 'uncovered_from', lo)
            set_meta(self.db, 'uncovered_to', hi - MAX_AUTO_RECOVERY)
            lo = hi - MAX_AUTO_RECOVERY + 1
        oldlo = get_meta(self.db, 'recovery_next')
        oldhi = get_meta(self.db, 'recovery_target')
        if oldlo and oldhi and int(oldlo) <= int(oldhi):
            lo, hi = min(lo, int(oldlo)), max(hi, int(oldhi))
            if hi - lo + 1 > MAX_AUTO_RECOVERY:
                set_meta(self.db, 'uncovered_from', lo)
                set_meta(self.db, 'uncovered_to', hi - MAX_AUTO_RECOVERY)
                lo = hi - MAX_AUTO_RECOVERY + 1
        set_meta(self.db, 'recovery_next', lo)
        set_meta(self.db, 'recovery_target', hi)

    def header(self, b):
        n = int(b['number'], 16)
        int(b['timestamp'], 16)
        if len(b['hash']) != 66 or len(b['parentHash']) != 66:
            raise RpcError('invalid stream header')
        previous = self.headers.get(n - 1)
        replaced = self.headers.get(n)
        if (previous and previous['hash'] != b['parentHash']) or (replaced and replaced['hash'] != b['hash']):
            ancestors = [h for h, v in self.headers.items() if v['hash'] == b['parentHash'] and h == n - 1]
            if not ancestors: raise RpcError('stream reorg ancestry unavailable; recovery required')
            self.rollback(n)
        # Providers may coalesce newHeads while the independent log subscription
        # continues delivering every matching log. A head-number jump alone is
        # therefore not evidence of a data gap; missing event headers and reconnects
        # are recovered explicitly elsewhere.
        self.headers[n] = dict(b)
        self.headers.move_to_end(n)
        while len(self.headers) > 4096: self.headers.popitem(last=False)
        self.head = max(self.head or n, n)
        self.last_message = time.monotonic()

    def rollback(self, n):
        with self.db:
            self.db.execute('DELETE FROM events WHERE block_number>=?', (n,))
            self.db.execute('DELETE FROM watches WHERE created_block>=?', (n,))
            self.db.execute('DELETE FROM stream_pending WHERE block>=?', (n,))
            self.gap(n, max(n, self.head or n))
            set_meta(self.db, 'last_reorg', now())
        for h in list(self.headers):
            if h >= n: del self.headers[h]

    def log(self, row):
        n, idx = int(row['blockNumber'], 16), int(row['logIndex'], 16)
        if row.get('removed'):
            self.rollback(n)
            return
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO stream_pending VALUES (?,?,?,?)',
                            (row['transactionHash'], idx, n, json.dumps(dict(row, _received_at=now()))))

    def flush(self):
        if self.head is None: return
        target = self.head - 3
        watches = {r['address']: dict(r) for r in self.db.execute('SELECT * FROM watches')}
        rows = [json.loads(r[0]) for r in self.db.execute('SELECT body FROM stream_pending WHERE block<=? ORDER BY block,idx LIMIT 5000', (target,))]
        # Launches precede child events even if subscription messages were reordered.
        rows.sort(key=lambda r: (int(r['blockNumber'], 16), r['address'].lower() not in FACTORIES, int(r['logIndex'], 16)))
        count = 0
        with self.db:
            for row in rows:
                n, idx = int(row['blockNumber'], 16), int(row['logIndex'], 16)
                header = self.headers.get(n)
                if header and header['hash'] != row['blockHash']:
                    self.db.execute('DELETE FROM stream_pending WHERE tx=? AND idx=?', (row['transactionHash'], idx))
                    self.gap(n, n)
                    continue
                address = row['address'].lower()
                kind = FACTORIES.get(address) or watches.get(address, {}).get('kind')
                if not kind and row.get('topics'):
                    kind = GENERIC_BIRTH_TOPICS.get(row['topics'][0].lower())
                if kind:
                    name, values = decode(kind, row)
                    if name == 'TokenLaunched':
                        child = values['pool'] if kind == 'pons_v1' else values['curve']
                        w = dict(address=child, kind='v3_pool' if kind == 'pons_v1' else 'curve', asset=values['token'], created_block=n)
                        watches[child] = w
                        self.db.execute('INSERT OR IGNORE INTO watches VALUES (?,?,?,?)', tuple(w.values()))
                    if name == 'Create' and kind == 'long':
                        w=dict(address=values['poolOrHook'],kind='v3_pool',asset=values['asset'],created_block=n)
                        watches[w['address']]=w
                        self.db.execute('INSERT OR IGNORE INTO watches VALUES (?,?,?,?)',tuple(w.values()))
                    if name in ('PairCreated','PoolCreated'):
                        child=values.get('pair') or values.get('pool')
                        w=dict(address=child,kind='v3_pool',asset=child,created_block=n)
                        watches[child]=w
                        self.db.execute('INSERT OR IGNORE INTO watches VALUES (?,?,?,?)',tuple(w.values()))
                    asset = values.get('token') or values.get('asset') or values.get('pair') or values.get('pool') or values.get('account') or watches.get(address, {}).get('asset')
                    values['_validation'] = 'provider-stream-confirmed-3-heads'
                    self.db.execute('INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        (row['transactionHash'], idx, n, row['blockHash'], address, kind, name, asset,
                         row['_received_at'], int(header['timestamp'], 16) if header else int(time.time()), json.dumps(values), json.dumps(row), None))
                    count += 1
                self.db.execute('DELETE FROM stream_pending WHERE tx=? AND idx=?', (row['transactionHash'], idx))
            if get_meta(self.db, 'start') is None: set_meta(self.db, 'start', target)
            set_meta(self.db, 'cursor', max(int(get_meta(self.db, 'cursor') or 0), target))
            set_meta(self.db, 'head', self.head)
            set_meta(self.db, 'heartbeat', now())
            set_meta(self.db, 'last_success', now())
            set_meta(self.db, 'status', 'healthy' if self.connected else 'degraded')
            if self.connected: set_meta(self.db, 'last_error', '')
            set_meta(self.db, 'chain', CHAIN)
        if count: LOG.info('stream committed %s relevant logs; head=%s', count, self.head)


def fetch_gap(rpc, lo, hi):
    rows = rpc.call('eth_getLogs', [dict(fromBlock=hex(lo), toBlock=hex(hi), topics=[TOPICS])])
    if not isinstance(rows, list) or len(rows) > 10000: raise RpcError('gap response exceeds safe limit')
    heights = sorted({int(r['blockNumber'], 16) for r in rows})
    headers = rpc.many([('eth_getBlockByNumber', [hex(n), False]) for n in heights])
    if any(not b or int(b['number'], 16) != n for n, b in zip(heights, headers)):
        raise RpcError('gap header mismatch')
    return rows, headers


async def consume(url, state):
    from websockets.asyncio.client import connect
    while True:
        try:
            async with connect(url, open_timeout=20, ping_interval=20, max_size=8*1024*1024) as ws:
                await ws.send(json.dumps(dict(jsonrpc='2.0', id=1, method='eth_chainId', params=[])))
                response = json.loads(await asyncio.wait_for(ws.recv(), 20))
                if response.get('result') != hex(CHAIN): raise RpcError('WebSocket chain mismatch')
                # Install logs before heads; all relevant signatures ensure same-block
                # child buys are received before the new child address is known.
                await ws.send(json.dumps(dict(jsonrpc='2.0', id=2, method='eth_subscribe', params=['logs', {'topics': [TOPICS]}])))
                response = json.loads(await asyncio.wait_for(ws.recv(), 20))
                if not response.get('result'): raise RpcError('log subscription rejected')
                log_id = response['result']
                await ws.send(json.dumps(dict(jsonrpc='2.0', id=3, method='eth_subscribe', params=['newHeads'])))
                head_id = None
                first_head = True
                state.connected = True
                LOG.info('direct log subscription connected')
                async for message in ws:
                    payload = json.loads(message)
                    if payload.get('id') == 3:
                        head_id = payload.get('result')
                        if not head_id: raise RpcError('head subscription rejected')
                        continue
                    params = payload.get('params', {})
                    if params.get('subscription') == log_id:
                        state.log(params['result'])
                    elif head_id and params.get('subscription') == head_id:
                        b = params['result']
                        if first_head:
                            with state.db:
                                old = int(get_meta(state.db, 'cursor') or int(b['number'], 16) - 1)
                                state.gap(max(1, old - 3), int(b['number'], 16))
                            first_head = False
                        state.header(b)
        except asyncio.CancelledError: raise
        except Exception as exc:
            state.connected = False
            with state.db:
                set_meta(state.db, 'status', 'degraded')
                set_meta(state.db, 'last_error', str(exc) if isinstance(exc, RpcError) else type(exc).__name__)
            LOG.warning('stream disconnected (%s); reconnecting', type(exc).__name__)
            await asyncio.sleep(5)


async def maintain(state, rpc):
    retry_at = 0
    while True:
        await asyncio.sleep(1)
        try:
            if state.connected and time.monotonic() - state.last_message < 30:
                state.flush()
            lo = int(get_meta(state.db, 'recovery_next') or 1)
            end = int(get_meta(state.db, 'recovery_target') or 0)
            if lo <= end and state.head and time.monotonic() >= retry_at:
                hi = min(end, lo + 9, state.head - 3)
                if hi < lo: continue
                rows, headers = await asyncio.to_thread(fetch_gap, rpc, lo, hi)
                for b in headers: state.headers[int(b['number'], 16)] = b
                # Recovery can discover a launch after live child logs arrived:
                # ordered historical replay covers those child logs too.
                for row in rows: state.log(row)
                state.flush()
                with state.db:
                    set_meta(state.db, 'recovery_next', hi + 1)
                    set_meta(state.db, 'recovery_error', '')
                LOG.info('gap recovered %s-%s; remaining=%s', lo, hi, max(0, end-hi))
        except asyncio.CancelledError: raise
        except Exception as exc:
            retry_at = time.monotonic() + 15
            with state.db:
                set_meta(state.db, 'recovery_error', str(exc) if isinstance(exc, RpcError) else type(exc).__name__)
            LOG.warning('gap recovery delayed (%s); live subscription remains active', type(exc).__name__)


async def run():
    url, http = os.environ.get('RPC_WS_URL'), os.environ.get('RPC_HTTP_URL')
    if not url or not http: raise ValueError('RPC_WS_URL and RPC_HTTP_URL are required')
    db = database('data/live.sqlite')
    if get_meta(db, 'chain') not in (None, str(CHAIN)): raise ValueError('wrong database chain')
    state = StreamStore(db)
    try: await asyncio.gather(consume(url, state), maintain(state, RPC(http, attempts=1, spacing=0.2)))
    finally: db.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try: asyncio.run(run())
    except KeyboardInterrupt: pass
