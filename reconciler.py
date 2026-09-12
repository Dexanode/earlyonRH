"""Reconcile GMGN observations and derive Pons V2/Long launch lifecycles."""
import argparse
import datetime as dt
import json
import logging
import os
import sqlite3
import time
from urllib import parse, request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from listener import database, now, set_meta
from market_normalizer import schema as market_schema

LOG=logging.getLogger('reconciler')
GMGN='https://openapi.gmgn.ai'


def schema(db):
    market_schema(db)
    db.executescript('''
      CREATE TABLE IF NOT EXISTS gmgn_reconciliation(
        asset TEXT PRIMARY KEY, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
        symbol TEXT, name TEXT, price_usd REAL, market_cap_usd REAL, liquidity_usd REAL,
        holder_count INTEGER, security_status TEXT, price_delta_pct REAL,
        market_cap_delta_pct REAL, liquidity_delta_pct REAL,
        info_json TEXT, pool_json TEXT, security_json TEXT,
        holders_json TEXT, traders_json TEXT, participants_checked_at TEXT, error TEXT);
      CREATE TABLE IF NOT EXISTS source_timings(
        asset TEXT NOT NULL, source TEXT NOT NULL, first_seen_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL, first_price_usd REAL, first_market_cap_usd REAL,
        PRIMARY KEY(asset,source));
      CREATE TABLE IF NOT EXISTS launchpad_lifecycle(
        asset TEXT PRIMARY KEY, protocol TEXT NOT NULL, stage TEXT NOT NULL,
        created_at TEXT NOT NULL, created_block INTEGER NOT NULL, creator TEXT,
        first_buy_at TEXT, first_buy_block INTEGER, seconds_to_first_buy INTEGER,
        buys INTEGER NOT NULL, sells INTEGER NOT NULL, unique_buyers INTEGER NOT NULL,
        net_quote_raw TEXT, graduation_threshold_raw TEXT, curve_progress_pct REAL,
        migrated_at TEXT, migrated_block INTEGER, seconds_to_migration INTEGER,
        metadata_seen_at TEXT, gmgn_seen_at TEXT, updated_at TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS launchpad_lifecycle_stage ON launchpad_lifecycle(stage,created_block DESC);
    ''')


def gmgn(path,address,extra=None,timeout=5):
    key=os.environ.get('GMGN_API_KEY')
    if not key:raise ValueError('GMGN_API_KEY missing')
    params={'chain':'robinhood','address':address,'timestamp':int(time.time()),'client_id':str(uuid.uuid4())}
    params.update(extra or {})
    req=request.Request(GMGN+path+'?'+parse.urlencode(params),headers={'Accept':'application/json','X-APIKEY':key,'User-Agent':'earlyonRH/1'})
    with request.urlopen(req,timeout=timeout) as response:payload=json.load(response)
    if str(payload.get('code')) not in ('0','0.0'):raise ValueError(str(payload.get('error') or payload.get('message') or 'GMGN error'))
    return payload.get('data') or {}


def values(payload):
    """Flatten common GMGN fields without depending on one response nesting."""
    found=[]
    def walk(item):
        if isinstance(item,dict):
            found.append(item)
            for child in item.values():walk(child)
        elif isinstance(item,list):
            for child in item:walk(child)
    walk(payload)
    def pick(*keys):
        for item in found:
            for key in keys:
                if item.get(key) not in (None,''):return item[key]
        return None
    def number(*keys):
        try:return float(pick(*keys))
        except (TypeError,ValueError):return None
    return {'symbol':pick('symbol','token_symbol'),'name':pick('name','token_name'),
            'price_usd':number('price_usd','price'),'market_cap_usd':number('market_cap','market_cap_usd','marketcap','fdv'),
            'liquidity_usd':number('liquidity','liquidity_usd'),'holder_count':number('holder_count','holders'),
            'security_status':pick('security_status','risk_level','status')}


def delta(external,local):
    return round((external/local-1)*100,2) if external is not None and local else None


def reconcile_asset(db,asset,participants=False):
    old=db.execute('SELECT * FROM gmgn_reconciliation WHERE asset=?',(asset,)).fetchone();stamp=now()
    documents={}
    errors=[]
    jobs=[('info','/v1/token/info',None),('pool','/v1/token/pool_info',None),('security','/v1/token/security',None)]
    if participants:
        extra={'limit':20,'order_by':'amount_percentage','direction':'desc'}
        jobs.extend([('holders','/v1/market/token_top_holders',extra),('traders','/v1/market/token_top_traders',extra)])
    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        futures={pool.submit(gmgn,path,asset,extra):label for label,path,extra in jobs}
        for future in as_completed(futures):
            label=futures[future]
            try:documents[label]=future.result()
            except (OSError,ValueError) as exc:errors.append(label+':'+type(exc).__name__)
    merged=values(documents);local=db.execute('SELECT price_usd,market_cap_usd,liquidity_usd FROM market_snapshots WHERE asset=?',(asset,)).fetchone()
    local=dict(local) if local else {};first=old['first_seen_at'] if old else stamp
    keep=lambda key:merged.get(key) if merged.get(key) is not None else (old[key] if old else None)
    def document(label):
        if documents.get(label) is not None:return documents[label]
        if old and old[label+'_json']:
            try:return json.loads(old[label+'_json'])
            except json.JSONDecodeError:return {}
        return {}
    row=(asset,first,stamp,keep('symbol'),keep('name'),keep('price_usd'),keep('market_cap_usd'),keep('liquidity_usd'),
         int(keep('holder_count')) if keep('holder_count') is not None else None,keep('security_status'),
         delta(keep('price_usd'),local.get('price_usd')),delta(keep('market_cap_usd'),local.get('market_cap_usd')),delta(keep('liquidity_usd'),local.get('liquidity_usd')),
         json.dumps(document('info')),json.dumps(document('pool')),json.dumps(document('security')),
         json.dumps(document('holders')),json.dumps(document('traders')),
         stamp if participants else (old['participants_checked_at'] if old else None),','.join(errors) or None)
    with db:
        db.execute('INSERT OR REPLACE INTO gmgn_reconciliation VALUES('+','.join('?'*20)+')',row)
        db.execute('INSERT INTO source_timings VALUES(?,?,?,?,?,?) ON CONFLICT(asset,source) DO UPDATE SET last_seen_at=excluded.last_seen_at',(asset,'gmgn',first,stamp,keep('price_usd'),keep('market_cap_usd')))
        if keep('symbol') or keep('name'):
            db.execute('''INSERT INTO token_metadata(address,checked_at,symbol,name,decimals,total_supply,error) VALUES(?,?,?,?,NULL,NULL,NULL)
              ON CONFLICT(address) DO UPDATE SET checked_at=excluded.checked_at,
              symbol=coalesce(token_metadata.symbol,excluded.symbol),name=coalesce(token_metadata.name,excluded.name)''',(asset,stamp,keep('symbol'),keep('name')))
    return not errors


def launch_lifecycle(db,limit=500):
    launches=db.execute("""SELECT * FROM events WHERE kind IN ('pons_v2','long') AND name IN ('TokenLaunched','Create')
      AND asset IS NOT NULL ORDER BY block_number DESC LIMIT ?""",(limit,)).fetchall();updated=0
    for launch in launches:
        asset=launch['asset'];data=json.loads(launch['decoded']);created_at=dt.datetime.fromtimestamp(launch['event_timestamp'],dt.timezone.utc).isoformat()
        trades=db.execute("SELECT name,block_number,event_timestamp,decoded FROM events WHERE asset=? AND name IN ('CurveBuy','CurveSell','DexBuy','DexSell') ORDER BY block_number,log_index",(asset,)).fetchall()
        buys=[row for row in trades if row['name'] in ('CurveBuy','DexBuy')];sells=[row for row in trades if row['name'] in ('CurveSell','DexSell')]
        buyers={(json.loads(row['decoded']).get('buyer') or '').lower() for row in buys};buyers.discard('')
        net=sum(int(json.loads(row['decoded']).get('quoteIn') or 0) for row in buys)-sum(int(json.loads(row['decoded']).get('quoteOut') or 0) for row in sells)
        threshold=int(data.get('graduationThreshold') or 0);progress=round(max(0,net)/threshold*100,2) if threshold else None
        migration=db.execute("SELECT block_number,event_timestamp FROM events WHERE asset=? AND name IN ('LaunchSwept','CurveCompleted','Migrate') ORDER BY block_number LIMIT 1",(asset,)).fetchone()
        first=buys[0] if buys else None
        stage='migrated' if migration else 'trading' if trades else 'created'
        metadata_row=db.execute("SELECT checked_at FROM token_metadata WHERE address=? AND (coalesce(symbol,'')!='' OR coalesce(name,'')!='')",(asset,)).fetchone()
        gmgn_row=db.execute('SELECT first_seen_at FROM gmgn_reconciliation WHERE asset=?',(asset,)).fetchone()
        migrated_at=dt.datetime.fromtimestamp(migration['event_timestamp'],dt.timezone.utc).isoformat() if migration else None
        first_at=dt.datetime.fromtimestamp(first['event_timestamp'],dt.timezone.utc).isoformat() if first else None
        creator=(data.get('deployer') or data.get('initializer') or '').lower() or None
        row=(asset,launch['kind'],stage,created_at,launch['block_number'],creator,first_at,first['block_number'] if first else None,
             first['event_timestamp']-launch['event_timestamp'] if first else None,len(buys),len(sells),len(buyers),str(net),str(threshold) if threshold else None,progress,
             migrated_at,migration['block_number'] if migration else None,migration['event_timestamp']-launch['event_timestamp'] if migration else None,
             metadata_row['checked_at'] if metadata_row else None,gmgn_row['first_seen_at'] if gmgn_row else None,now())
        with db:db.execute('INSERT OR REPLACE INTO launchpad_lifecycle VALUES('+','.join('?'*21)+')',row)
        updated+=1
    return updated


def cycle(db,limit=10):
    with db:set_meta(db,'reconciler_heartbeat',now());set_meta(db,'reconciler_status','running')
    lifecycle=launch_lifecycle(db)
    assets=[row[0] for row in db.execute("""SELECT asset FROM launchpad_lifecycle
      ORDER BY CASE stage WHEN 'trading' THEN 0 WHEN 'created' THEN 1 ELSE 2 END,created_block DESC LIMIT ?""",(limit,))]
    ok=0
    for index,asset in enumerate(assets):
        try:ok+=reconcile_asset(db,asset,participants=index<2)
        except sqlite3.Error as exc:LOG.warning('reconcile %s delayed: %s',asset,exc)
        time.sleep(.08)
    with db:
        set_meta(db,'reconciler_heartbeat',now());set_meta(db,'reconciler_status','healthy');set_meta(db,'reconciled_assets',ok);set_meta(db,'launchpad_lifecycles',lifecycle)
    return ok,lifecycle


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--db',default='data/live.sqlite');parser.add_argument('--interval',type=int,default=30);parser.add_argument('--limit',type=int,default=10);args=parser.parse_args()
    db=database(args.db);schema(db)
    try:
        while True:
            try:LOG.info('reconciled=%s lifecycles=%s',*cycle(db,args.limit))
            except (OSError,ValueError,sqlite3.Error) as exc:LOG.warning('cycle delayed: %s',type(exc).__name__)
            time.sleep(max(15,args.interval))
    finally:db.close()


if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s');main()
