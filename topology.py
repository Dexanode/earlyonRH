"""Append-only birth and topology projector for the early-discovery radar."""
import argparse
import json
import sqlite3
import time

from listener import database, get_meta, now, set_meta


PROTOCOLS = (
    ('pons_v1', 'Pons V1', 'launchpad', 'active', 'listener', 'Factory launch and V3 child pool'),
    ('pons_v2', 'Pons V2', 'launchpad', 'active', 'listener', 'Factory launch and bonding curve'),
    ('dex_factories', 'Permissionless V2/V3 factories', 'dex', 'active', 'signature-sensor', 'PairCreated and PoolCreated across any emitter'),
    ('uniswap_v4', 'Uniswap V4', 'dex', 'partial', 'listener', 'Registered PoolManager initialization and liquidity'),
    ('hood_fun', 'hood.fun', 'launchpad', 'planned', 'adapter', 'Contract registry requires verification'),
    ('hood_dev', 'hood.dev', 'launchpad', 'planned', 'adapter', 'V3 single-sided launch topology'),
    ('hoodpad', 'HoodPad', 'launchpad', 'partial', 'signature-sensor', 'Pons V2-compatible factory births; strategy adapter pending'),
    ('erc721', 'ERC-721', 'nft', 'planned', 'standard', 'Mint and transfer sensor'),
    ('erc1155', 'ERC-1155', 'nft', 'planned', 'standard', 'Mint and transfer sensor'),
    ('erc6551', 'ERC-6551', 'nft-account', 'active', 'signature-sensor', 'Token-bound account creation across registry emitters'),
    ('stonkbrokers', 'StonkBrokers / Anvil', 'nft-market', 'planned', 'adapter', 'Collection, TBA, AMM and token linkage'),
    ('stock_tokens', 'Stock Token registry', 'financial', 'planned', 'official-api', 'Canonical deployment and status registry'),
    ('bridges', 'Canonical bridges', 'capital-flow', 'planned', 'adapter', 'Inbound capital migration'),
)


def ensure_schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS protocol_sources(
        id TEXT PRIMARY KEY,name TEXT NOT NULL,category TEXT NOT NULL,status TEXT NOT NULL,
        sensor TEXT NOT NULL,notes TEXT,updated_at TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS topology_entities(
        id TEXT PRIMARY KEY,entity_type TEXT NOT NULL,protocol TEXT,label TEXT,
        first_block INTEGER,first_timestamp INTEGER,first_tx TEXT,confidence TEXT NOT NULL,
        attributes TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS topology_edges(
        source TEXT NOT NULL,relation TEXT NOT NULL,target TEXT NOT NULL,protocol TEXT,
        first_block INTEGER,first_timestamp INTEGER,first_tx TEXT,confidence TEXT NOT NULL,
        attributes TEXT NOT NULL,created_at TEXT NOT NULL,
        PRIMARY KEY(source,relation,target));
      CREATE TABLE IF NOT EXISTS topology_observations(
        id INTEGER PRIMARY KEY AUTOINCREMENT,observation_type TEXT NOT NULL,entity TEXT,
        protocol TEXT,block_number INTEGER,event_timestamp INTEGER,tx_hash TEXT,log_index INTEGER,
        evidence_status TEXT NOT NULL,payload TEXT NOT NULL,observed_at TEXT NOT NULL,
        UNIQUE(tx_hash,log_index,observation_type));
      CREATE INDEX IF NOT EXISTS topology_birth_recent ON topology_observations(event_timestamp DESC,id DESC);
      CREATE INDEX IF NOT EXISTS topology_edges_target ON topology_edges(target,relation);
      CREATE TABLE IF NOT EXISTS topology_processed(
        tx_hash TEXT NOT NULL,log_index INTEGER NOT NULL,processed_at TEXT NOT NULL,
        PRIMARY KEY(tx_hash,log_index));
    ''')
    stamp = now()
    db.executemany('''INSERT INTO protocol_sources VALUES(?,?,?,?,?,?,?)
      ON CONFLICT(id) DO UPDATE SET name=excluded.name,category=excluded.category,
      status=excluded.status,sensor=excluded.sensor,notes=excluded.notes,updated_at=excluded.updated_at''',
      [p + (stamp,) for p in PROTOCOLS])


def entity(db, ident, typ, protocol, block, timestamp, tx, confidence='observed', attributes=None, label=None):
    if not ident: return
    stamp = now(); payload = json.dumps(attributes or {}, sort_keys=True)
    db.execute('''INSERT INTO topology_entities VALUES(?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(id) DO UPDATE SET entity_type=excluded.entity_type,
      protocol=COALESCE(topology_entities.protocol,excluded.protocol),
      label=COALESCE(topology_entities.label,excluded.label),
      first_block=MIN(topology_entities.first_block,excluded.first_block),
      first_timestamp=MIN(topology_entities.first_timestamp,excluded.first_timestamp),
      confidence=excluded.confidence,attributes=excluded.attributes,updated_at=excluded.updated_at''',
      (ident, typ, protocol, label, block, timestamp, tx, confidence, payload, stamp, stamp))


def edge(db, source, relation, target, protocol, block, timestamp, tx, confidence='observed', attributes=None):
    if not source or not target: return
    db.execute('INSERT OR IGNORE INTO topology_edges VALUES(?,?,?,?,?,?,?,?,?,?)',
      (source, relation, target, protocol, block, timestamp, tx, confidence,
       json.dumps(attributes or {}, sort_keys=True), now()))


def observation(db, typ, subject, protocol, row, payload):
    db.execute('INSERT OR IGNORE INTO topology_observations(observation_type,entity,protocol,block_number,event_timestamp,tx_hash,log_index,evidence_status,payload,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
      (typ, subject, protocol, row['block_number'], row['event_timestamp'], row['tx_hash'],
       row['log_index'], 'observed', json.dumps(payload, sort_keys=True), row['observed_at']))


def project_row(db, row):
    values = json.loads(row['decoded']); kind, name = row['kind'], row['name']
    protocol = 'uniswap_v4' if kind == 'v4' else kind if kind in ('pons_v1', 'pons_v2') else None
    block, ts, tx = row['block_number'], row['event_timestamp'], row['tx_hash']
    entity(db, row['address'], 'contract', protocol or kind, block, ts, tx,
           attributes={'listener_kind': kind})
    if name == 'TokenLaunched':
        token, deployer = values.get('token'), values.get('deployer')
        child = values.get('curve') or values.get('pool')
        child_type = 'bonding_curve' if values.get('curve') else 'pool'
        entity(db, token, 'token', protocol, block, ts, tx)
        entity(db, deployer, 'wallet', protocol, block, ts, tx)
        entity(db, child, child_type, protocol, block, ts, tx)
        edge(db, row['address'], 'created', token, protocol, block, ts, tx)
        edge(db, deployer, 'deployed', token, protocol, block, ts, tx)
        edge(db, token, 'trades_on', child, protocol, block, ts, tx)
        if values.get('pairToken'):
            entity(db, values['pairToken'], 'token', protocol, block, ts, tx)
            edge(db, token, 'paired_with', values['pairToken'], protocol, block, ts, tx)
        observation(db, 'NEW_MARKET_BIRTH', token, protocol, row, values)
    elif name == 'Initialize' and kind == 'v4':
        pool = values.get('id')
        entity(db, pool, 'pool', protocol, block, ts, tx, attributes={'hooks': values.get('hooks')})
        for currency in (values.get('currency0'), values.get('currency1')):
            entity(db, currency, 'token', protocol, block, ts, tx)
            edge(db, pool, 'contains', currency, protocol, block, ts, tx)
        if values.get('hooks'):
            entity(db, values['hooks'], 'hook', protocol, block, ts, tx)
            edge(db, pool, 'uses_hook', values['hooks'], protocol, block, ts, tx)
        observation(db, 'NEW_POOL_BIRTH', pool, protocol, row, values)
    elif name in ('PairCreated','PoolCreated'):
        protocol='dex_factories';pool=values.get('pair') or values.get('pool')
        entity(db,pool,'pool',protocol,block,ts,tx,attributes={'factory':row['address'],'fee':values.get('fee')})
        entity(db,row['address'],'factory',protocol,block,ts,tx)
        edge(db,row['address'],'created',pool,protocol,block,ts,tx)
        for token in (values.get('token0'),values.get('token1')):
            entity(db,token,'token',protocol,block,ts,tx)
            edge(db,pool,'contains',token,protocol,block,ts,tx)
        observation(db,'NEW_POOL_BIRTH',pool,protocol,row,values)
    elif name == 'ERC6551AccountCreated':
        protocol='erc6551';account=values.get('account');collection=values.get('tokenContract')
        entity(db,account,'token_bound_account',protocol,block,ts,tx,attributes={'tokenId':values.get('tokenId'),'implementation':values.get('implementation')})
        entity(db,collection,'nft_collection',protocol,block,ts,tx)
        edge(db,collection,'owns_account',account,protocol,block,ts,tx,attributes={'tokenId':values.get('tokenId')})
        observation(db,'NEW_TOKEN_BOUND_ACCOUNT',account,protocol,row,values)
    elif name == 'CurveCompleted':
        observation(db, 'MARKET_GRADUATED', row['asset'], 'pons_v2', row, values)


def project(db, limit=2000):
    ensure_schema(db)
    rows = db.execute('''SELECT e.* FROM events e LEFT JOIN topology_processed p
      ON p.tx_hash=e.tx_hash AND p.log_index=e.log_index
      WHERE p.tx_hash IS NULL ORDER BY e.block_number,e.log_index LIMIT ?''', (limit,)).fetchall()
    with db:
        for row in rows:
            project_row(db, row)
            db.execute('INSERT OR IGNORE INTO topology_processed VALUES(?,?,?)',(row['tx_hash'],row['log_index'],now()))
        set_meta(db, 'topology_heartbeat', now())
        set_meta(db, 'topology_entities', db.execute('SELECT COUNT(*) FROM topology_entities').fetchone()[0])
        set_meta(db, 'topology_edges', db.execute('SELECT COUNT(*) FROM topology_edges').fetchone()[0])
        set_meta(db, 'topology_births', db.execute("SELECT COUNT(*) FROM topology_observations WHERE observation_type LIKE '%BIRTH'").fetchone()[0])
    return len(rows)


def main():
    p=argparse.ArgumentParser();p.add_argument('--db',default='data/listener.sqlite');p.add_argument('--interval',type=float,default=5);p.add_argument('--once',action='store_true');a=p.parse_args()
    db=database(a.db)
    try:
        while True:
            count=project(db)
            if a.once: break
            time.sleep(.1 if count else a.interval)
    finally: db.close()


if __name__ == '__main__': main()
