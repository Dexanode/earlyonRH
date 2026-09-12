"""Budgeted enrichment for active candidates; read-only chain calls only."""
import datetime as dt
import hashlib
import json
import logging
import os
import sqlite3
import time

from listener import RPC, RpcError, RateLimited, database, get_meta, now

LOG = logging.getLogger('enricher')
ZERO = '0x' + '0' * 40
OWNER_SELECTORS = ('0x8da5cb5b', '0x893d20e8')
PAUSED = '0x5c975abb'
TOTAL_SUPPLY = '0x18160ddd'
BALANCE_OF = '0x70a08231'
IMPLEMENTATION_SLOT = '0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc'
ADMIN_SLOT = '0x0000000b53127684a568b3173ae13b9f8a6016e019881a10d6a717850b5d6103'


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS asset_analysis(
        asset TEXT PRIMARY KEY, analyzed_at TEXT NOT NULL, deployer TEXT,
        code_sha256 TEXT, code_bytes INTEGER, owner TEXT, paused INTEGER,
        total_supply TEXT, deployer_balance TEXT, implementation TEXT,
        proxy_admin TEXT, safety_score REAL, safety_status TEXT, findings TEXT,
        error TEXT, source_block INTEGER);
      CREATE TABLE IF NOT EXISTS tx_attributions(
        tx_hash TEXT PRIMARY KEY, asset TEXT NOT NULL, sender TEXT,
        event_actor TEXT, relation TEXT, checked_at TEXT NOT NULL, error TEXT);
      CREATE INDEX IF NOT EXISTS tx_attributions_asset ON tx_attributions(asset);
      CREATE TABLE IF NOT EXISTS enrichment_budget(day TEXT PRIMARY KEY, requests INTEGER NOT NULL);
    ''')


class BudgetRPC:
    def __init__(self, db, rpc, daily): self.db, self.rpc, self.daily = db, rpc, daily
    def call(self, method, params):
        day = dt.datetime.now(dt.timezone.utc).date().isoformat()
        used = self.db.execute('SELECT requests FROM enrichment_budget WHERE day=?', (day,)).fetchone()
        if used and used[0] >= self.daily: raise RpcError('daily enrichment RPC budget reached')
        with self.db:
            self.db.execute('INSERT INTO enrichment_budget VALUES (?,1) ON CONFLICT(day) DO UPDATE SET requests=requests+1', (day,))
        return self.rpc.call(method, params)


def word_address(value):
    if not isinstance(value, str) or not value.startswith('0x'): return None
    raw = value[2:].rjust(64, '0')
    if len(raw) != 64: return None
    address = '0x' + raw[-40:].lower()
    return None if address == ZERO else address


def uint(value):
    try: return int(value, 16) if isinstance(value, str) and value.startswith('0x') else None
    except ValueError: return None


def optional_call(rpc, address, data):
    try: return rpc.call('eth_call', [{'to': address, 'data': data}, 'latest'])
    except RpcError: return None


def analyze_asset(db, rpc, asset):
    launch = db.execute("SELECT decoded,block_number FROM events WHERE asset=? AND name='TokenLaunched' ORDER BY block_number LIMIT 1", (asset,)).fetchone()
    deployer = json.loads(launch['decoded']).get('deployer') if launch else None
    code = rpc.call('eth_getCode', [asset, 'latest'])
    raw = bytes.fromhex(code.removeprefix('0x')) if isinstance(code, str) else b''
    owner = None
    for selector in OWNER_SELECTORS:
        owner = word_address(optional_call(rpc, asset, selector))
        if owner: break
    paused_raw = optional_call(rpc, asset, PAUSED)
    paused = uint(paused_raw)
    paused = paused if paused in (0, 1) else None
    supply = uint(optional_call(rpc, asset, TOTAL_SUPPLY))
    balance = uint(optional_call(rpc, asset, BALANCE_OF + ('0' * 24 + deployer[2:] if deployer else '0' * 64))) if deployer else None
    implementation = word_address(rpc.call('eth_getStorageAt', [asset, IMPLEMENTATION_SLOT, 'latest']))
    admin = word_address(rpc.call('eth_getStorageAt', [asset, ADMIN_SLOT, 'latest']))
    concentration = balance / supply if balance is not None and supply else None
    findings, score = [], 0
    if raw: score += 25
    else: findings.append('no_runtime_code')
    if implementation or admin: findings.append('proxy_or_admin_slot_nonzero')
    else: score += 20
    if paused == 1: findings.append('paused_true')
    elif paused == 0: score += 15
    else: findings.append('paused_unknown')
    if owner is None: findings.append('owner_unknown_or_renounced')
    elif owner == deployer: findings.append('owner_is_deployer')
    else: findings.append('owner_present')
    if concentration is None: findings.append('deployer_concentration_unknown')
    elif concentration > .5: findings.append('deployer_holds_over_50pct')
    elif concentration > .2: score += 5; findings.append('deployer_holds_over_20pct')
    else: score += 20
    # Unknown capabilities never earn points. This is screening, not an audit.
    status = 'higher-risk' if paused == 1 or concentration is not None and concentration > .5 else 'screened' if score >= 60 else 'incomplete'
    with db:
        db.execute('INSERT OR REPLACE INTO asset_analysis VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
          (asset, now(), deployer, hashlib.sha256(raw).hexdigest() if raw else None, len(raw), owner, paused,
           str(supply) if supply is not None else None, str(balance) if balance is not None else None,
           implementation, admin, score, status, json.dumps(findings), None, launch['block_number'] if launch else None))


def attribute_transactions(db, rpc, assets, limit):
    marks = ','.join('?' for _ in assets)
    if not marks: return 0
    rows = db.execute(f'''SELECT e.tx_hash,e.asset,e.decoded FROM events e
      LEFT JOIN tx_attributions t ON t.tx_hash=e.tx_hash
      WHERE e.asset IN ({marks}) AND e.name IN ('CurveBuy','CurveSell','DexBuy','DexSell')
        AND (t.tx_hash IS NULL OR t.error IS NOT NULL)
      ORDER BY e.block_number DESC LIMIT ?''', (*assets, limit)).fetchall()
    for row in rows:
        actor_values = json.loads(row['decoded'])
        actor = actor_values.get('buyer') or actor_values.get('seller')
        sender = relation = error = None
        try:
            tx = rpc.call('eth_getTransactionByHash', [row['tx_hash']])
            sender = tx.get('from', '').lower() if tx else None
            relation = 'direct' if sender and sender == actor else 'v4-tx-sender' if sender and actor_values.get('_pool_id') else 'routed' if sender else 'unknown'
        except RateLimited:
            break
        except RpcError as exc: error = str(exc)
        with db:
            db.execute('INSERT OR REPLACE INTO tx_attributions VALUES (?,?,?,?,?,?,?)',
                       (row['tx_hash'], row['asset'], sender, actor, relation, now(), error))
    return len(rows)


def top_assets(db, limit=10):
    head = int(dict(db.execute('SELECT key,value FROM meta')).get('head', 0))
    return [r[0] for r in db.execute('''SELECT asset FROM events
      WHERE asset IS NOT NULL AND block_number>? GROUP BY asset
      HAVING SUM(name IN ('CurveBuy','DexBuy'))>=3 ORDER BY COUNT(*) DESC LIMIT ?''', (head-3000, limit))]


def cycle(db, rpc, tx_limit=5):
    heartbeat = get_meta(db, 'heartbeat')
    try: fresh = (dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(heartbeat)).total_seconds() < 30
    except (TypeError, ValueError): fresh = False
    if get_meta(db, 'status') != 'healthy' or not fresh:
        LOG.info('live listener is not healthy; enrichment paused')
        return
    assets = top_assets(db)
    cutoff = (dt.datetime.now(dt.timezone.utc)-dt.timedelta(hours=6)).isoformat()
    for asset in assets:
        old = db.execute('SELECT analyzed_at FROM asset_analysis WHERE asset=?', (asset,)).fetchone()
        if not old or old[0] < cutoff:
            try: analyze_asset(db, rpc, asset)
            except RpcError as exc:
                LOG.warning('asset analysis delayed: %s', exc)
                break
    count = attribute_transactions(db, rpc, assets, tx_limit)
    LOG.info('enrichment cycle assets=%s tx=%s', len(assets), count)


def main():
    url = os.environ.get('ENRICHMENT_RPC_HTTP_URL') or os.environ.get('RPC_HTTP_URL')
    if not url: raise ValueError('RPC_HTTP_URL required')
    daily = int(os.environ.get('ENRICHMENT_DAILY_RPC_BUDGET', '250'))
    if not 1 <= daily <= 100000: raise ValueError('invalid ENRICHMENT_DAILY_RPC_BUDGET')
    db = database('data/live.sqlite'); schema(db)
    rpc = BudgetRPC(db, RPC(url, attempts=1, spacing=.25), daily)
    try:
        while True:
            try: cycle(db, rpc)
            except (RpcError, sqlite3.Error) as exc: LOG.warning('cycle delayed: %s', exc)
            time.sleep(60)
    finally: db.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    main()
