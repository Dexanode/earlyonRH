"""Read-only Robinhood event collector. Never signs or broadcasts transactions."""
import argparse
import asyncio
import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from threading import Lock

from events import SPECS, decode

CHAIN = 4663
REGISTRY = {
    '0xa5aab3f0c6eeadf30ef1d3eb997108e976351feb': 'pons_v1',
    '0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e': 'pons_v2',
    '0xeb7c034704ef8dcd2d32324c1545f62fb4ad0862': 'long',
    '0x8366a39cc670b4001a1121b8f6a443a643e40951': 'v4',
}
EXPLORER = 'https://robinhoodchain.blockscout.com/tx/'
LOG = logging.getLogger('listener')


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


class RpcError(Exception):
    pass


class RateLimited(RpcError):
    pass


class RPC:
    def __init__(self, url, attempts=3, spacing=0.25):
        if not url.startswith(('https://', 'http://')):
            raise ValueError('RPC_HTTP_URL must be HTTP(S)')
        self.url, self.attempts, self.spacing = url, attempts, spacing
        self.last = 0
        self.headers = OrderedDict()
        self.header_lock = Lock()

    def remember_header(self, header):
        try:
            height = int(header['number'], 16)
            int(header['timestamp'], 16)
            if any(not isinstance(header[k], str) or len(header[k]) != 66 for k in ('hash', 'parentHash')):
                return
        except (KeyError, TypeError, ValueError):
            return
        with self.header_lock:
            self.headers[height] = dict(header)
            self.headers.move_to_end(height)
            while len(self.headers) > 4096:
                self.headers.popitem(last=False)

    def range_headers(self, heights):
        with self.header_lock:
            found = {n: self.headers[n] for n in heights if n in self.headers}
        missing = [n for n in heights if n not in found]
        found.update(zip(missing, self.many([('eth_getBlockByNumber', [hex(n), False]) for n in missing])))
        LOG.info('headers websocket=%s http=%s', len(heights) - len(missing), len(missing))
        return [found[n] for n in heights]

    def clear_headers(self):
        with self.header_lock:
            self.headers.clear()

    def call(self, method, params):
        # Error messages intentionally exclude endpoints: URLs can contain keys.
        for attempt in range(self.attempts):
            time.sleep(max(0, self.spacing - (time.monotonic() - self.last)))
            self.last = time.monotonic()
            body = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}).encode()
            req = urllib.request.Request(self.url, data=body, headers={
                'Content-Type': 'application/json',
                'User-Agent': 'earlyonrh/1.0',
            })
            try:
                with urllib.request.urlopen(req, timeout=20) as res:
                    result = json.load(res)
                if result.get('error'):
                    raise RpcError(f'{method}: JSON-RPC error code {result["error"].get("code")}')
                if 'result' not in result:
                    raise RpcError(f'{method}: missing result')
                return result['result']
            except (OSError, ValueError, RpcError) as exc:
                status = getattr(exc, 'code', None)
                if status == 429:
                    raise RateLimited(f'{method}: provider rate limit') from None
                if attempt + 1 == self.attempts:
                    raise RpcError(f'{method} failed ({status or type(exc).__name__})') from None
                time.sleep(min(2 ** attempt, 8))

    def block(self, height):
        b = self.call('eth_getBlockByNumber', [hex(height), False])
        if not b or int(b['number'], 16) != height:
            raise RpcError('missing or mismatched block')
        return b

    def many(self, calls):
        """Bounded JSON-RPC batches; match by ID, never response order."""
        output = []
        for offset in range(0, len(calls), 10):
            group = calls[offset:offset + 10]
            if getattr(self, 'batch_disabled', False):
                output.extend(self.parallel(group))
                continue
            time.sleep(max(0, self.spacing - (time.monotonic() - self.last)))
            self.last = time.monotonic()
            body = json.dumps([dict(jsonrpc='2.0', id=i, method=m, params=p)
                               for i, (m, p) in enumerate(group)]).encode()
            req = urllib.request.Request(self.url, data=body, headers={'Content-Type': 'application/json'})
            try:
                with urllib.request.urlopen(req, timeout=20) as res:
                    rows = json.load(res)
                if not isinstance(rows, list): raise RpcError('batch unsupported')
                mapped = {r.get('id'): r for r in rows}
                if len(rows) != len(group) or set(mapped) != set(range(len(group))):
                    raise RpcError('incomplete or duplicate batch response')
                if any('result' not in r or r.get('error') for r in rows):
                    raise RpcError('batch item failed')
                output.extend(mapped[i]['result'] for i in range(len(group)))
            except (OSError, ValueError, RpcError, TypeError, AttributeError) as exc:
                if getattr(exc, 'code', None) == 429:
                    raise RateLimited('batch: provider rate limit') from None
                LOG.warning('RPC batch unavailable (%s); falling back to individual reads', getattr(exc, 'code', None) or type(exc).__name__)
                self.batch_disabled = True
                output.extend(self.parallel(group))
        return output

    def parallel(self, calls):
        # Each worker has its own timing state. Only the collector writes SQLite.
        def read(item):
            return RPC(self.url, self.attempts, self.spacing).call(*item)
        with ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(read, calls))


def database(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.executescript('''
      PRAGMA busy_timeout=60000;
      PRAGMA journal_mode=WAL;
      CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS blocks(number INTEGER PRIMARY KEY,hash TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS watches(address TEXT PRIMARY KEY,kind TEXT,asset TEXT,created_block INTEGER);
      CREATE TABLE IF NOT EXISTS events(
        tx_hash TEXT,log_index INTEGER,block_number INTEGER,block_hash TEXT,address TEXT,
        kind TEXT,name TEXT,asset TEXT,observed_at TEXT,event_timestamp INTEGER,
        decoded TEXT,raw TEXT,decode_error TEXT,
        PRIMARY KEY(tx_hash,log_index));
      CREATE INDEX IF NOT EXISTS events_asset ON events(asset,block_number);
      CREATE INDEX IF NOT EXISTS events_recent ON events(block_number DESC,log_index DESC);
      CREATE TABLE IF NOT EXISTS receipts(tx_hash TEXT PRIMARY KEY,block_number INTEGER,body TEXT);
      CREATE TABLE IF NOT EXISTS validations(address TEXT PRIMARY KEY,checked_at TEXT,code_sha256 TEXT,bytes INTEGER);
    ''')
    return db


def get_meta(db, key):
    r = db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    return r[0] if r else None


def set_meta(db, key, value):
    db.execute('INSERT OR REPLACE INTO meta VALUES (?,?)', (key, str(value)))


class Collector:
    def __init__(self, db, rpc, start=None, confirmations=2, chunk=10, max_logs=500, max_reorg=64, factory_first=False):
        self.db, self.rpc, self.start = db, rpc, start
        self.confirmations, self.chunk = confirmations, chunk
        self.max_logs, self.max_reorg = max_logs, max_reorg
        self.active_chunk = chunk
        self.successful_ranges = 0
        self.factory_first = factory_first
        self.registry = {a: k for a, k in REGISTRY.items() if not factory_first or k != 'v4'}

    def setup(self):
        if int(self.rpc.call('eth_chainId', []), 16) != CHAIN:
            raise RpcError('wrong chain; expected 4663')
        if get_meta(self.db, 'chain') not in (None, str(CHAIN)):
            raise RpcError('database belongs to another chain')
        scope = 'factory-first' if self.factory_first else 'all-registry'
        previous_scope = get_meta(self.db, 'scope') or ('all-registry' if get_meta(self.db, 'cursor') else scope)
        if previous_scope != scope:
            raise RpcError('scan scope changed; use a separate database')
        fingerprint = hashlib.sha256(json.dumps({'registry': REGISTRY, 'specs': SPECS}, sort_keys=True).encode()).hexdigest()
        if get_meta(self.db, 'decoder_fingerprint') not in (None, fingerprint):
            raise RpcError('registry/decoder changed; replay into a new database')
        for address in self.registry:
            code = self.rpc.call('eth_getCode', [address, 'latest'])
            raw = bytes.fromhex(code.removeprefix('0x'))
            if not raw: raise RpcError(f'no bytecode at registry address {address}')
            previous = self.db.execute('SELECT code_sha256 FROM validations WHERE address=?', (address,)).fetchone()
            digest = hashlib.sha256(raw).hexdigest()
            if previous and previous[0] != digest:
                raise RpcError(f'bytecode changed at {address}; manual registry review required')
            self.db.execute('INSERT OR REPLACE INTO validations VALUES (?,?,?,?)', (address, now(), digest, len(raw)))
        with self.db:
            set_meta(self.db, 'chain', CHAIN)
            set_meta(self.db, 'scope', scope)
            set_meta(self.db, 'decoder_fingerprint', fingerprint)
            if get_meta(self.db, 'cursor') is None:
                height = self.start
                if height is None:
                    height = max(1, int(self.rpc.call('eth_blockNumber', []), 16) - self.confirmations)
                if height < 1: raise ValueError('start block must be >=1')
                anchor = self.rpc.block(height - 1)
                self.db.execute('INSERT INTO blocks VALUES (?,?)', (height - 1, anchor['hash']))
                set_meta(self.db, 'start', height)
                set_meta(self.db, 'cursor', height - 1)
            set_meta(self.db, 'status', 'ready')

    def reconcile(self):
        cursor = int(get_meta(self.db, 'cursor'))
        stored = self.db.execute('SELECT hash FROM blocks WHERE number=?', (cursor,)).fetchone()
        if self.rpc.block(cursor)['hash'] == stored[0]: return
        rows = self.db.execute('SELECT number,hash FROM blocks WHERE number<? ORDER BY number DESC LIMIT ?', (cursor, self.max_reorg)).fetchall()
        ancestor = next((r['number'] for r in rows if self.rpc.block(r['number'])['hash'] == r['hash']), None)
        if ancestor is None: raise RpcError('reorg exceeds retained ancestry; use a new database and earlier start block')
        with self.db:
            for table, field in [('events', 'block_number'), ('receipts', 'block_number'), ('watches', 'created_block'), ('blocks', 'number')]:
                self.db.execute(f'DELETE FROM {table} WHERE {field}>?', (ancestor,))
            set_meta(self.db, 'cursor', ancestor)
            set_meta(self.db, 'last_reorg', now())
        LOG.warning('reorg rollback to block %s', ancestor)
        if hasattr(self.rpc, 'clear_headers'): self.rpc.clear_headers()

    def logs(self, addresses, lo, hi, topics=None):
        out = []
        # The provider accepts the full watch set in one filter. Splitting every
        # 50 addresses made catch-up slower as new launches accumulated.
        groups = [addresses] if addresses else []
        while groups:
            group = groups.pop()
            f = {'address': group, 'fromBlock': hex(lo), 'toBlock': hex(hi)}
            if topics: f['topics'] = topics
            try:
                rows = self.rpc.call('eth_getLogs', [f])
            except RateLimited:
                raise
            except RpcError:
                if len(group) <= 50: raise
                middle = len(group) // 2
                groups.extend([group[middle:], group[:middle]])
                continue
            if not isinstance(rows, list): raise RpcError('invalid logs response')
            received = now()
            rows = [dict(r, _received_at=received) for r in rows]
            out.extend(rows)
            if len(out) > self.max_logs: raise RpcError('too many logs; reduce block chunk')
        return out

    def ingest(self, lo, hi):
        started = time.monotonic()
        heights = list(range(lo, hi + 1))
        if hasattr(self.rpc, 'many'):
            blocks = self.rpc.range_headers(heights) if hasattr(self.rpc, 'range_headers') else self.rpc.many([('eth_getBlockByNumber', [hex(n), False]) for n in heights])
            if any(not b or int(b['number'], 16) != n for n, b in zip(heights, blocks)):
                raise RpcError('missing or mismatched block')
            headers = dict(zip(heights, blocks))
        else:
            headers = {n: self.rpc.block(n) for n in heights}
        headers_done = time.monotonic()
        parent = self.db.execute('SELECT hash FROM blocks WHERE number=?', (lo - 1,)).fetchone()[0]
        for n, b in headers.items():
            if b['parentHash'] != parent: raise RpcError('chain changed during block read')
            parent = b['hash']
        # Fetch factory events before their children, including same-block curve buys.
        rows = self.logs(list(self.registry), lo, hi)
        watches = {r['address']: dict(r) for r in self.db.execute('SELECT * FROM watches')}
        discoveries = []
        for row in rows:
            kind = REGISTRY.get(row['address'].lower())
            if kind not in ('pons_v1', 'pons_v2', 'long'): continue
            try: name, values = decode(kind, row)
            except ValueError:
                # Cannot advance past an undecodable launch: it would lose child events.
                raise RpcError('factory ABI mismatch; checkpoint not advanced') from None
            if name == 'TokenLaunched':
                child = values['pool'] if kind == 'pons_v1' else values['curve']
                entry = {'address': child, 'kind': 'v3_pool' if kind == 'pons_v1' else 'curve', 'asset': values['token'], 'created_block': int(row['blockNumber'], 16)}
                watches[child] = entry; discoveries.append(entry)
            elif name == 'Create':
                entry={'address':values['poolOrHook'],'kind':'v3_pool','asset':values['asset'],'created_block':int(row['blockNumber'],16)}
                watches[entry['address']]=entry;discoveries.append(entry)
        if watches:
            rows.extend(self.logs(list(watches), lo, hi))
        unique = {(r['transactionHash'], int(r['logIndex'], 16)): r for r in rows}
        if len(unique) > self.max_logs: raise RpcError('too many combined logs; reduce chunk')
        logs_done = time.monotonic()
        transactions = list(dict.fromkeys(r['transactionHash'] for r in unique.values()))
        receipts = dict(zip(transactions, self.rpc.many([
            ('eth_getTransactionReceipt', [tx]) for tx in transactions
        ]))) if hasattr(self.rpc, 'many') else {}
        prepared = []
        receipts_done = time.monotonic()
        for key, row in sorted(unique.items(), key=lambda x: (int(x[1]['blockNumber'], 16), x[0][1])):
            n = int(row['blockNumber'], 16)
            if n not in headers or row['blockHash'] != headers[n]['hash'] or row.get('removed'):
                raise RpcError('log block mismatch or removed log')
            tx = row['transactionHash']
            if tx not in receipts:
                receipt = self.rpc.call('eth_getTransactionReceipt', [tx])
                receipts[tx] = receipt
            receipt = receipts[tx]
            if not receipt or receipt['blockHash'] != row['blockHash'] or int(receipt['status'], 16) != 1 or receipt.get('transactionHash') != tx:
                raise RpcError('receipt unavailable, reverted, or reorged')
            matches = [r for r in receipts[tx]['logs'] if int(r['logIndex'], 16) == key[1]]
            if not matches or any(matches[0].get(k) != row.get(k) for k in ('address', 'data', 'topics', 'blockHash', 'transactionHash')):
                raise RpcError('log does not match transaction receipt')
            address = row['address'].lower()
            kind = REGISTRY.get(address) or watches[address]['kind']
            error = None
            try: name, values = decode(kind, row)
            except ValueError as exc: name, values, error = 'DecodeError', {}, str(exc)
            asset = values.get('token') or values.get('id') or watches.get(address, {}).get('asset')
            raw = {k: v for k, v in row.items() if k != '_received_at'}
            prepared.append((tx, key[1], n, row['blockHash'], address, kind, name, asset, row['_received_at'], int(headers[n]['timestamp'], 16), json.dumps(values), json.dumps(raw), error))
        # Detect tip replacement after receipts before committing a range.
        if self.rpc.block(hi)['hash'] != headers[hi]['hash']:
            raise RpcError('range changed before commit')
        with self.db:
            self.db.executemany('INSERT OR REPLACE INTO blocks VALUES (?,?)', [(n, b['hash']) for n, b in headers.items()])
            for w in discoveries:
                self.db.execute('INSERT OR IGNORE INTO watches VALUES (?,?,?,?)', (w['address'], w['kind'], w['asset'], w['created_block']))
            self.db.executemany('INSERT OR IGNORE INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', prepared)
            self.db.executemany('INSERT OR REPLACE INTO receipts VALUES (?,?,?)', [(tx, int(r['blockNumber'], 16), json.dumps(r)) for tx, r in receipts.items()])
            set_meta(self.db, 'cursor', hi)
            set_meta(self.db, 'last_success', now())
            set_meta(self.db, 'status', 'healthy')
        LOG.info('committed blocks %s-%s events=%s child_watches=%s', lo, hi, len(prepared), len(watches))
        LOG.info('range throughput %.2f blocks/sec', (hi - lo + 1) / max(time.monotonic() - started, 0.001))
        LOG.info('RPC timing headers=%.2fs logs=%.2fs receipts=%.2fs commit=%.2fs', headers_done-started, logs_done-headers_done, receipts_done-logs_done, time.monotonic()-receipts_done)
        return len(prepared)

    def tick(self):
        with self.db: set_meta(self.db, 'heartbeat', now())
        self.reconcile()
        head = int(self.rpc.call('eth_blockNumber', []), 16)
        with self.db: set_meta(self.db, 'head', head)
        target = head - self.confirmations
        lo = int(get_meta(self.db, 'cursor')) + 1
        if lo > target:
            with self.db: set_meta(self.db, 'status', 'healthy')
            return False
        span = min(self.active_chunk, target - lo + 1)
        while True:
            try:
                self.ingest(lo, lo + span - 1)
                self.successful_ranges += 1
                if self.successful_ranges >= 100:
                    self.active_chunk = min(self.chunk, self.active_chunk + 1)
                    self.successful_ranges = 0
                return lo + span - 1 < target
            except RateLimited:
                raise
            except RpcError as exc:
                if hasattr(self.rpc, 'clear_headers'): self.rpc.clear_headers()
                if span <= 1: raise
                span = max(1, span // 2)
                self.active_chunk = span
                self.successful_ranges = 0
                LOG.warning('reducing range to %s blocks: %s', span, exc)


def export(db, path):
    items = []
    for row in db.execute('SELECT * FROM events ORDER BY block_number,log_index'):
        r = dict(row); r['decoded'] = json.loads(r['decoded']); r['raw'] = json.loads(r['raw'])
        r['explorer_url'] = EXPLORER + r['tx_hash']
        r['actor_attribution'] = 'unresolved; sender/buyer may be router or executor'
        items.append(r)
    payload = {'chain_id': CHAIN, 'exported_at': now(), 'status': dict(db.execute('SELECT key,value FROM meta')), 'events': items,
               'limitations': ['No USD conversion, buy recommendation, wallet-quality score, or quote execution.', 'Raw repeat-buy events are not proof of independent economic decisions.', 'Only registered contracts and children discovered since start are covered.']}
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    temp = Path(str(path) + '.tmp'); temp.write_text(json.dumps(payload, indent=2)); temp.replace(path)


async def ws_wakeup(url, wake, rpc=None):
    from websockets.asyncio.client import connect
    while True:
        try:
            async with connect(url, open_timeout=20, ping_interval=20) as ws:
                await ws.send(json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'eth_chainId', 'params': []}))
                response = json.loads(await asyncio.wait_for(ws.recv(), 20))
                if response.get('result') != hex(CHAIN): raise RpcError('WebSocket chain mismatch')
                await ws.send(json.dumps({'jsonrpc': '2.0', 'id': 2, 'method': 'eth_subscribe', 'params': ['newHeads']}))
                response = json.loads(await asyncio.wait_for(ws.recv(), 20))
                if not response.get('result'): raise RpcError('WebSocket subscription refused')
                LOG.info('WebSocket heads connected')
                async for message in ws:
                    payload = json.loads(message)
                    if payload.get('method') == 'eth_subscription':
                        if rpc is not None:
                            rpc.remember_header(payload.get('params', {}).get('result', {}))
                        wake.set()
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning('WebSocket unavailable (%s); HTTP recovery remains active', type(exc).__name__)
            await asyncio.sleep(5)


async def run(args):
    # A single sequential worker owns collection; WebSocket stays responsive during HTTP reads.
    db = database(args.db)
    if args.command == 'export':
        export(db, args.output)
        db.close()
        return
    url = os.environ.get('RPC_HTTP_URL')
    if not url: raise ValueError('Set RPC_HTTP_URL in the environment')
    rpc = RPC(url, spacing=args.request_spacing)
    c = Collector(db, rpc, args.start_block, args.confirmations, args.chunk, factory_first=args.factory_first)
    try:
        await asyncio.to_thread(c.setup)
    except Exception as exc:
        with db:
            set_meta(db, 'status', 'degraded')
            set_meta(db, 'heartbeat', now())
            set_meta(db, 'last_error', str(exc) if isinstance(exc, (RpcError, ValueError)) else type(exc).__name__)
        db.close()
        raise
    wake = asyncio.Event()
    task = asyncio.create_task(ws_wakeup(os.environ['RPC_WS_URL'], wake, rpc)) if os.environ.get('RPC_WS_URL') else None
    try:
        while True:
            try:
                more = await asyncio.to_thread(c.tick)
                if args.command == 'once': break
                if more:
                    await asyncio.sleep(0); continue
            except RpcError as exc:
                with db: set_meta(db, 'status', 'degraded'); set_meta(db, 'last_error', str(exc))
                if args.command == 'once': raise
                LOG.warning('%s; checkpoint retained', exc)
                if isinstance(exc, RateLimited):
                    await asyncio.sleep(5)
            try: await asyncio.wait_for(wake.wait(), args.poll)
            except asyncio.TimeoutError: pass
            wake.clear()
            # Coalesce rapid head notifications into one scan near the tip.
            # Backlog processing above still runs without this delay.
            await asyncio.sleep(0.75)
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        db.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command', choices=['run', 'once', 'export'])
    p.add_argument('--db', default='data/listener.sqlite')
    p.add_argument('--start-block', type=int)
    p.add_argument('--factory-first', action='store_true', help='Only factory launches and their discovered child contracts; separate database required')
    p.add_argument('--confirmations', type=int, default=2)
    p.add_argument('--chunk', type=int, default=10)
    p.add_argument('--poll', type=float, default=2)
    p.add_argument('--request-spacing', type=float, default=0.05)
    p.add_argument('--output', default='data/timeline.json')
    a = p.parse_args()
    if a.chunk < 1 or a.confirmations < 0 or a.poll <= 0 or a.request_spacing < 0: p.error('invalid numeric setting')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try: asyncio.run(run(a))
    except (RpcError, ValueError) as exc: p.exit(1, str(exc) + '\n')
    except KeyboardInterrupt: pass


if __name__ == '__main__': main()
